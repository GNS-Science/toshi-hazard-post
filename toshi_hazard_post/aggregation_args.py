import csv
from collections import namedtuple
from pathlib import Path
from typing import Any, Optional, Union

import tomlkit
from nzshm_model import all_model_versions
from pydantic import AfterValidator, BaseModel, FilePath, PositiveInt, ValidationInfo, field_validator, model_validator
from toshi_hazard_store.model.constraints import AggregationEnum, IntensityMeasureTypeEnum
from toshi_hazard_store.scripts.ths_import import chc_manager
from typing_extensions import Annotated, Self


def load_input_args(filepath: Union[str, Path]) -> 'AggregationArgs':
    config = tomlkit.parse(Path(filepath).read_text()).unwrap()
    config['filepath'] = Path(filepath)
    return AggregationArgs(**config)


def resolve_path(path: Union[Path, str], reference_filepath: Union[Path, str]) -> str:
    path = Path(path)
    if not path.is_absolute():
        return str(Path(reference_filepath).parent / path)
    return str(path)


def is_model_version(value: str) -> str:
    if value not in all_model_versions():
        raise ValueError("must specify valid nshm_model_version ({})".format(all_model_versions()))
    return value


def is_compat_calc_id(compat_calc_id: str) -> str:
    try:
        chc_manager.load(compat_calc_id)
    except FileNotFoundError:
        raise ValueError("Compatible Hazard Calculation with unique ID {value} does not exist.")

    return compat_calc_id


class GeneralArgs(BaseModel):
    compatibility_key: Annotated[str, AfterValidator(is_compat_calc_id)]
    hazard_model_id: str


class HazardModelArgs(BaseModel):
    nshm_model_version: Annotated[Optional[str], AfterValidator(is_model_version)] = None
    srm_logic_tree: Optional[FilePath] = None
    gmcm_logic_tree: Optional[FilePath] = None

    @model_validator(mode='after')
    def check_logic_trees(self) -> Self:
        if not self.nshm_model_version and not (self.srm_logic_tree and self.gmcm_logic_tree):
            raise ValueError(
                """if nshm_model_version not specified, must provide both
                gmcm_logic_tree and srm_logic_tree"""
            )
        return self


class SiteArgs(BaseModel):
    vs30s: Optional[list[PositiveInt]] = None
    locations: Optional[list[str]] = None
    locations_file: Optional[FilePath] = None

    @staticmethod
    def has_vs30(filepath: Path):
        with filepath.open() as lf:
            header = lf.readline()
            if "vs30" in header:
                return True
        return False

    @field_validator('locations_file', mode='after')
    @classmethod
    def check_file_vs30s(cls, value: FilePath) -> FilePath:
        with value.open() as loc_file:
            site_reader = csv.reader(loc_file)
            Site = namedtuple("Site", next(site_reader), rename=True)  # type:ignore
            if 'vs30' in Site._fields:
                for row in site_reader:
                    site = Site(*row)
                    try:
                        vs30 = int(site.vs30)  # type:ignore
                        assert vs30 > 0
                    except ValueError:
                        raise ValueError("not all vs30 values are valid {}".format(row))
        return value

    @model_validator(mode='after')
    def check_locations(self) -> Self:
        if self.locations_file and self.locations:
            raise ValueError("cannot specify both locations and locations_file")

        if (not self.locations) and (not self.locations_file):
            raise ValueError("must specify locations or locations_file")

        file_has_vs30 = self.locations_file and self.has_vs30(self.locations_file)
        if file_has_vs30 and self.vs30s:
            raise ValueError("cannot specify both uniform and site-specific vs30s")
        elif not file_has_vs30 and not self.vs30s:
            raise ValueError("locations file must have vs30 column if uniform vs30s not provided")

        return self


class CalculationArgs(BaseModel):
    imts: list[IntensityMeasureTypeEnum] = [e for e in IntensityMeasureTypeEnum]
    agg_types: Optional[list[AggregationEnum]] = [e for e in AggregationEnum]


class DebugArgs(BaseModel):
    skip_save: bool = False
    restart: Optional[tuple[FilePath, FilePath]] = None


class AggregationArgs(BaseModel):
    filepath: FilePath
    general: GeneralArgs
    hazard_model: HazardModelArgs
    site_params: SiteArgs
    calculation: CalculationArgs
    debug: DebugArgs = DebugArgs()

    # resolve absolute paths (relative to input file) for optional logic tree and config fields
    @field_validator('hazard_model', mode='before')
    @classmethod
    def absolute_model_paths(cls, data: Any, info: ValidationInfo) -> Any:
        if isinstance(data, dict):
            for key in ["srm_logic_tree", "gmcm_logic_tree"]:
                if data.get(key):
                    data[key] = resolve_path(data[key], info.data["filepath"])
        return data

    # resolve absolute paths (relative to input file) for optional site file
    @field_validator('site_params', mode='before')
    @classmethod
    def absolute_site_path(cls, data: Any, info: ValidationInfo) -> Any:
        if isinstance(data, dict):
            if data.get("locations_file"):
                data["locations_file"] = resolve_path(data["locations_file"], info.data["filepath"])
        return data

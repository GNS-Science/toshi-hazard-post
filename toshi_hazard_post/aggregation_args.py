"""This module defines pydantic classes and functions for the aggregation calculation configuration.

Members:
    AggregationArgs: pydantic class for aggregation configuration arguments.
    load_input_args: function to return an AggregationArgs object built from a toml file.
"""

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
    """Load the input arguments of a hazard aggregation calculation.

    Args:
        filepath: the path to the configuration toml file.

    Returns:
        The aggregation arguments.
    """
    config = tomlkit.parse(Path(filepath).read_text()).unwrap()
    config['filepath'] = Path(filepath)
    return AggregationArgs(**config)


def _resolve_path(path: Union[Path, str], reference_filepath: Union[Path, str]) -> str:
    path = Path(path)
    if not path.is_absolute():
        return str(Path(reference_filepath).parent / path)
    return str(path)


def _is_model_version(value: str) -> str:
    if value not in all_model_versions():
        raise ValueError("must specify valid nshm_model_version ({})".format(all_model_versions()))
    return value


def _is_compat_calc_id(compat_calc_id: str) -> str:
    try:
        chc_manager.load(compat_calc_id)
    except FileNotFoundError:
        raise ValueError("Compatible Hazard Calculation with unique ID {value} does not exist.")

    return compat_calc_id


class GeneralArgs(BaseModel):
    """The general parameters of a hazard model.

    Attributes:
        compatibility_key: the toshi-hazard-store compatibility key
        hazard_model_id: the name of the hazard model to use when storing result.
    """

    compatibility_key: Annotated[str, AfterValidator(_is_compat_calc_id)]
    hazard_model_id: str


class HazardModelArgs(BaseModel):
    """The PSHA hazard model parameters.

    Logic tree filepaths will override nshm_model_version logic trees.

    Attributes:
        nshm_model_version: the nzshm-model model version.
        srm_logic_tree: the source logic tree filepath.
        gmcm_logic_tree: the ground motion logic tree filepath.

    """

    nshm_model_version: Annotated[Optional[str], AfterValidator(_is_model_version)] = None
    srm_logic_tree: Optional[FilePath] = None
    gmcm_logic_tree: Optional[FilePath] = None

    @model_validator(mode='after')
    def _check_logic_trees(self) -> Self:
        if not self.nshm_model_version and not (self.srm_logic_tree and self.gmcm_logic_tree):
            raise ValueError(
                """if nshm_model_version not specified, must provide both
                gmcm_logic_tree and srm_logic_tree"""
            )
        return self


class SiteArgs(BaseModel):
    """Site parameters.

    Restrictions:
        - Must provide one of locations or locations_file but not both.
        - vs30s must be provided as an attribute or in the locations file but not both.
        - if vs30s are provided, they are treated as uniform for all sites.

    Attributes:
        vs30s:
        locations:
        locations_file:
    """

    vs30s: Optional[list[PositiveInt]] = None
    locations: Optional[list[str]] = None
    locations_file: Optional[FilePath] = None

    @staticmethod
    def _has_vs30(filepath: Path):
        with filepath.open() as lf:
            header = lf.readline()
            if "vs30" in header:
                return True
        return False

    @field_validator('locations_file', mode='after')
    @classmethod
    def _check_file_vs30s(cls, value: FilePath) -> FilePath:
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
    def _check_locations(self) -> Self:
        if self.locations_file and self.locations:
            raise ValueError("cannot specify both locations and locations_file")

        if (not self.locations) and (not self.locations_file):
            raise ValueError("must specify locations or locations_file")

        file_has_vs30 = self.locations_file and self._has_vs30(self.locations_file)
        if file_has_vs30 and self.vs30s:
            raise ValueError("cannot specify both uniform and site-specific vs30s")
        elif not file_has_vs30 and not self.vs30s:
            raise ValueError("locations file must have vs30 column if uniform vs30s not provided")

        return self


class CalculationArgs(BaseModel):
    """The calculation parameters.

    Attributes:
        imts: the intensity measure types.
        agg_types: the aggregation types.
    """

    imts: list[IntensityMeasureTypeEnum] = [e for e in IntensityMeasureTypeEnum]
    agg_types: list[AggregationEnum] = [e for e in AggregationEnum]


class DebugArgs(BaseModel):
    """The debugging parameters.

    Attributes:
        skip_save: set to True to skip saving the aggregations.
        restart: tuple of the paths to branch_hash_table and weights numpy files used to quickly start a
            calculation without needing to rebuild the logic tree.
    """

    skip_save: bool = False
    restart: Optional[tuple[FilePath, FilePath]] = None


class AggregationArgs(BaseModel):
    """The arguments needed to setup a suite of aggregation calculations.

    Attributes:
        filepath: the location of the config file, used to resolve relative paths.
        general: the general arguments.
        hazard_model: the arguments for the hazard model specification.
        site_params: the site parameters.
        calculation: the calculation parameters.
        debug: the debugging parameters.
    """

    filepath: FilePath
    general: GeneralArgs
    hazard_model: HazardModelArgs
    site_params: SiteArgs
    calculation: CalculationArgs
    debug: DebugArgs = DebugArgs()

    # resolve absolute paths (relative to input file) for optional logic tree and config fields
    @field_validator('hazard_model', mode='before')
    @classmethod
    def _absolute_model_paths(cls, data: Any, info: ValidationInfo) -> Any:
        if isinstance(data, dict):
            for key in ["srm_logic_tree", "gmcm_logic_tree"]:
                if data.get(key):
                    data[key] = _resolve_path(data[key], info.data["filepath"])
        return data

    # resolve absolute paths (relative to input file) for optional site file
    @field_validator('site_params', mode='before')
    @classmethod
    def _absolute_site_path(cls, data: Any, info: ValidationInfo) -> Any:
        if isinstance(data, dict):
            if data.get("locations_file"):
                data["locations_file"] = _resolve_path(data["locations_file"], info.data["filepath"])
        return data

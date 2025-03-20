import csv
from collections import namedtuple
from pathlib import Path
from typing import Optional, Union

import toml
from nzshm_model import all_model_versions, get_model_version
from nzshm_model.logic_tree import GMCMLogicTree, SourceLogicTree
from toshi_hazard_store.model.constraints import AggregationEnum, IntensityMeasureTypeEnum

from toshi_hazard_post.ths_mock import query_compatibility


class AggregationArgs:
    def __init__(self, input_filepath: Union[str, Path]) -> None:
        self.filepath = Path(input_filepath).resolve()
        self._config = toml.load(self.filepath)
        self._validate_vs30s()
        self._validate_list('site', 'locations', str)
        self._validate_compatibility()
        self._set_logic_trees()

        self.locations = self._config['site']['locations']
        self.vs30s = self._config['site'].get('vs30s')
        self.compat_key = self._config['general']['compatibility_key']
        self.hazard_model_id = self._config['general']['hazard_model_id']

        self._validate_list('calculation', 'imts', str, IntensityMeasureTypeEnum)
        self._validate_list('calculation', 'agg_types', str, AggregationEnum)
        self.imts = self._config['calculation'].get('imts', list(v.value for v in IntensityMeasureTypeEnum))
        self.agg_types = self._config['calculation'].get('agg_types', list(v.value for v in AggregationEnum))
    
        self.skip_save = False
        if self._config.get('debug'):
            self.skip_save = self._config['debug'].get('skip_save')

    def _validate_compatibility(self) -> None:
        res = list(query_compatibility(self._config['general']['compatibility_key']))
        if not res:
            raise ValueError(
                "compatibility key {} does not exist in the database".format(
                    self._config['general']['compatibility_key']
                )
            )

    def _validate_agg_types(self) -> None:
        for agg_type in self._config['calculation']['agg_types']:
            if agg_type not in ("cov", "std", "mean"):
                try:
                    fractile = float(agg_type)
                except ValueError:
                    raise ValueError(
                        """
                        aggregate types must be 'cov', 'std', 'mean',
                        or a string representation of a floating point value: {}
                        """.format(
                            agg_type
                        )
                    )
                else:
                    if not (0 < fractile < 1):
                        raise ValueError(
                            "fractile aggregate types must be between 0 and 1 exclusive: {}".format(agg_type)
                        )

    def _validate_list(
        self,
        table,
        name,
        element_type,
        constraint_enum: Optional[Union[AggregationEnum, IntensityMeasureTypeEnum]] = None,
    ) -> None:

        if constraint_enum:
            items = self._config[table].get(name)
            if not items:
                return  # empty list is OK for constraints
            for item in items:
                assert constraint_enum(item)  # ensure each value is a valid enum value
            return

        if not self._config[table].get(name):
            raise KeyError("must specify [{}][{}]".format(table, name))
        if not isinstance(self._config[table][name], list):
            raise ValueError("[{}][{}] must be a list".format(table, name))
        for loc in self._config[table][name]:
            if not isinstance(loc, element_type):
                raise ValueError("all location identifiers in [{}][{}] must be {}".format(table, name, element_type))

    def _set_logic_trees(self):
        lt_config = self._config["logic_trees"]
        model_spec = bool(lt_config.get("model_version"))
        file_spec = bool(lt_config.get("srm_file") or lt_config.get("gmcm_file"))

        if (not model_spec) and (not file_spec):
            raise KeyError("must specify a model_version or srm_file and gmcm_file")
        elif model_spec and file_spec:
            raise KeyError("specify EITHER a model_version or logic tree files, not both")
        elif model_spec:
            if model_spec and lt_config["model_version"] not in all_model_versions():
                raise KeyError("%s is not a valid model version" % lt_config["model_version"])
            model_version = self._config['logic_trees']['model_version']
            model = get_model_version(model_version)
            self.srm_logic_tree = model.source_logic_tree
            self.gmcm_logic_tree = model.gmm_logic_tree
            return
        else:
            if not lt_config.get("srm_file"):
                raise KeyError("must specify srm_file")
            if not lt_config.get("gmcm_file"):
                raise KeyError("must specify gmcm_file")
            for lt_file in ("srm_file", "gmcm_file"):
                if not Path(lt_config[lt_file]).is_absolute():
                    lt_config[lt_file] = self.filepath.parent / lt_config[lt_file]
                if not Path(lt_config[lt_file]).exists():
                    raise FileNotFoundError("{} {} does not exist".format(lt_file, lt_config["srm_file"]))
            srm_file = self._config['logic_trees']['srm_file']
            gmcm_file = self._config['logic_trees']['gmcm_file']
            self.srm_logic_tree = SourceLogicTree.from_json(srm_file)
            self.gmcm_logic_tree = GMCMLogicTree.from_json(gmcm_file)

    def _validate_vs30s(self) -> None:
        if self._config['site'].get('vs30s'):
            self._validate_list('site', 'vs30s', int)
        else:
            for location_id in self._config['site']['locations']:
                fpath = Path(location_id)
                if not fpath.exists():
                    raise RuntimeError("if vs30s not specified, all locations must be files with vs30 column")
                with Path(location_id).open() as loc_file:
                    site_reader = csv.reader(loc_file)
                    Site = namedtuple("Site", next(site_reader), rename=True)  # type:ignore
                    if 'vs30' not in Site._fields:
                        raise ValueError("if vs30s not specified, all locations must be files with vs30 column")
                    for row in site_reader:
                        site = Site(*row)
                        try:
                            vs30 = int(site.vs30)  # type:ignore
                            assert vs30 > 0
                        except ValueError:
                            raise ValueError("not all vs30 values in {} are not valid row:{}".format(location_id, row))

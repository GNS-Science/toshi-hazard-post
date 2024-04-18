import csv
from collections import namedtuple
from pathlib import Path
from typing import Union

import toml
from nzshm_model import all_model_versions

from toshi_hazard_post.version2.ths_mock import query_compatibility


class AggregationConfig:
    def __init__(self, config_filepath: Union[str, Path]) -> None:
        self._config = toml.load(config_filepath)
        model_spec = self._validate_logic_trees()
        self._validate_vs30s()
        self._validate_list('site', 'locations', str)
        self._validate_list('calculation', 'imts', str)
        self._validate_list('calculation', 'agg_types', str)
        self._validate_agg_types()
        self._validate_compatibility()

        if model_spec:
            self.model_version = self._config['logic_trees']['model_version']
            self.srm_file = None
            self.gmcm_file = None
        else:
            self.model_version = None
            self.srm_file = self._config['logic_trees']['srm_file']
            self.gmcm_file = self._config['logic_trees']['gmcm_file']

        self.locations = self._config['site']['locations']
        self.vs30s = self._config['site'].get('vs30s')
        self.compat_key = self._config['general']['compatibility_key']
        self.hazard_model_id = self._config['general']['hazard_model_id']
        self.imts = self._config['calculation']['imts']
        self.agg_types = self._config['calculation']['agg_types']

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

    def _validate_list(self, table, name, element_type) -> None:
        if not self._config[table].get(name):
            raise KeyError("must specify [{}][{}]".format(table, name))
        if not isinstance(self._config[table][name], list):
            raise ValueError("[{}][{}] must be a list".format(table, name))
        for loc in self._config[table][name]:
            if not isinstance(loc, element_type):
                raise ValueError("all location identifiers in [{}][{}] must be {}".format(table, name, element_type))

    def _validate_logic_trees(self) -> bool:
        lt_config = self._config["logic_trees"]
        model_spec = bool(lt_config.get("model_version"))
        file_spec = bool(lt_config.get("srm_file") or lt_config.get("gmcm_file"))

        if model_spec and file_spec:
            raise KeyError("specify EITHER a model_version or logic tree files, not both")
        if (not model_spec) and (not file_spec):
            raise KeyError("must specify a model_version or srm_file and gmcm_file")
        if file_spec:
            if not lt_config.get("srm_file"):
                raise KeyError("must specify srm_file")
            if not lt_config.get("gmcm_file"):
                raise KeyError("must specify gmcm_file")
            for lt_file in ("srm_file", "gmcm_file"):
                if not Path(lt_config[lt_file]).exists():
                    raise FileNotFoundError("{} {} does not exist".format(lt_file, lt_config["srm_file"]))
        if model_spec and lt_config["model_version"] not in all_model_versions():
            raise KeyError("{} is not a valid model version".format(lt_config["model_version"]))

        return model_spec

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

import copy
from pathlib import Path

import pytest
import tomlkit

from toshi_hazard_store.model.constraints import AggregationEnum, IntensityMeasureTypeEnum

from toshi_hazard_post.aggregation_args import AggregationArgs

config_filepath = Path(__file__).parent / 'fixtures/hazard.toml'


def get_config():
    config = tomlkit.parse(config_filepath.read_text()).unwrap()
    config['filepath'] = config_filepath
    return copy.deepcopy(config)


# can specify model version
config1 = get_config()

# can specify logic tree files
config2 = get_config()
del config2['hazard_model']['nshm_model_version']
config2['hazard_model']['srm_logic_tree'] = config_filepath
config2['hazard_model']['gmcm_logic_tree'] = config_filepath

# can specify vs30 in location file
config3 = get_config()
del config3['site_params']['vs30s']
del config3['site_params']['locations']
config3['site_params']['locations_file'] = str(Path(__file__).parent / 'fixtures/sites_vs30s.csv')

# imts is optional
config4 = get_config()
del config4['calculation']['imts']

# can specify sites in a file and uniform vs30
config5 = get_config()
del config5['site_params']['locations']
config5['site_params']['locations_file'] = str(Path(__file__).parent / 'fixtures/sites.csv')

# if specifying a model version, it must exist
config_keyerror1 = get_config()
config_keyerror1['hazard_model']['nshm_model_version'] = 'NOT A MODEL VERSION'

# must specify a model version or logic tree files
config_keyerror2 = get_config()
del config_keyerror2['hazard_model']['nshm_model_version']

# if specifying logic tree files but no nshm_model, must specify both srm and gmcm
config_keyerror3 = get_config()
del config_keyerror3['hazard_model']['nshm_model_version']
config_keyerror3['hazard_model']['srm_logic_tree'] = config_filepath

# the compatability key must exist
config_error4 = get_config()
config_error4['general']['compatibility_key'] = "NOT A COMPAT KEY"

# cannot specify both locations and a locations file
config_error5 = get_config()
config_error5['site_params']['locations_file'] = str(Path(__file__).parent / 'fixtures/sites_vs30s.csv')

# cannot specify both uniform and site specific vs30
config_error6 = get_config()
del config_error6['site_params']['locations']
config_error6['site_params']['locations_file'] = str(Path(__file__).parent / 'fixtures/sites_vs30s.csv')


# if specifying logic tree files, they must exist
config_fnferror1 = get_config()
del config_fnferror1['hazard_model']['nshm_model_version']
config_fnferror1['hazard_model']['srm_logic_tree'] = 'foobar.toml'
config_fnferror1['hazard_model']['gmcm_logic_tree'] = 'foobar.toml'

# if vs30 is missing, must specify vs30 in location file
config_rterror1 = get_config()
del config_rterror1['site_params']['vs30s']

# if vs30 is in file, all must be valid
config_aerror1 = get_config()
del config_aerror1['site_params']['vs30s']
del config_aerror1['site_params']['locations']
config_aerror1['site_params']['locations_file'] = str(Path(__file__).parent / 'fixtures/sites_vs30s_lt0.csv')

config_verror1 = get_config()
del config_verror1['site_params']['vs30s']
del config_verror1['site_params']['locations']
config_verror1['site_params']['locations_file'] = str(Path(__file__).parent / 'fixtures/sites_vs30s_str.csv')

# imts is now optional
# config_keyerror5 = get_config()
# del config_keyerror5['calculation']['imts']

# must specifiy locations
config_keyerror6 = get_config()
del config_keyerror6['site_params']['locations']

# imts must be list of strings
config_verror2 = get_config()
config_verror2['calculation']['imts'] = [1, 2, 3]

config_verror3 = get_config()
config_verror3['calculation']['imts'] = "SA(1.5)"

# agg values must be valid
config_verror4 = get_config()
config_verror4['calculation']['agg_types'] = ["mean", "0.5", "1.1"]


@pytest.mark.parametrize("config", [config1, config2, config3, config4, config5])
def test_args_valid(config):
    assert AggregationArgs(**config)


@pytest.mark.parametrize(
    "config",
    [
        config_keyerror1,
        config_keyerror2,
        config_keyerror3,
        config_error4,
        config_error5,
        config_error6,
        # config_keyerror4,
        # config_keyerror5,
        config_keyerror6,
        config_fnferror1,
        config_rterror1,
        config_aerror1,
        config_verror1,
        config_verror2,
        config_verror3,
        config_verror4,
    ],
)
def test_args_error(config):
    with pytest.raises(ValueError):
        AggregationArgs(**config)


def test_default_imts():
    config = get_config()
    del config['calculation']['imts']
    agg_args = AggregationArgs(**config)
    assert agg_args.calculation.imts == [imt for imt in IntensityMeasureTypeEnum]


def test_default_aggs():
    config = get_config()
    del config['calculation']['agg_types']
    agg_args = AggregationArgs(**config)
    assert agg_args.calculation.agg_types == [agg for agg in AggregationEnum]

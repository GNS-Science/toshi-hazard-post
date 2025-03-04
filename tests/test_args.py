import copy
from pathlib import Path
from unittest import mock

import pytest
import toml

from toshi_hazard_post.aggregation_args import AggregationArgs

config_filepath = Path(__file__).parent / 'fixtures/hazard.toml'


def get_config():
    config = toml.load(config_filepath)
    return copy.deepcopy(config)


# can specify model version
config1 = get_config()

# can specify logic tree files
config2 = get_config()
del config2['logic_trees']['model_version']
config2['logic_trees']['srm_file'] = config_filepath
config2['logic_trees']['gmcm_file'] = config_filepath

# can specify vs30 in location file
config3 = get_config()
del config3['site']['vs30s']
config3['site']['locations'] = [str(Path(__file__).parent / 'fixtures/sites_vs30s.csv')]

# if specifying a model version, it must exist
config_keyerror1 = get_config()
config_keyerror1['logic_trees']['model_version'] = 'NOT A MODEL VERSION'

# must specify a model version or logic tree files
config_keyerror2 = get_config()
del config_keyerror2['logic_trees']['model_version']

# if specifying logic tree files, must specify both srm and gmcm
config_keyerror3 = get_config()
del config_keyerror3['logic_trees']['model_version']
config_keyerror3['logic_trees']['srm_file'] = config_filepath

# cannot specify both model version and logic tree files(s)
config_keyerror4 = get_config()
config_keyerror4['logic_trees']['srm_file'] = config_filepath
config_keyerror4['logic_trees']['gmcm_file'] = config_filepath

# if specifying logic tree files, they must exist
config_fnferror1 = get_config()
del config_fnferror1['logic_trees']['model_version']
config_fnferror1['logic_trees']['srm_file'] = 'foobar.toml'
config_fnferror1['logic_trees']['gmcm_file'] = 'foobar.toml'

# if vs30 is missing, must specify vs30 in location file
config_rterror1 = get_config()
del config_rterror1['site']['vs30s']

# if vs30 is in file, all must be valid
config_aerror1 = get_config()
del config_aerror1['site']['vs30s']
config_aerror1['site']['locations'] = [str(Path(__file__).parent / 'fixtures/sites_vs30s_lt0.csv')]

config_verror1 = get_config()
del config_verror1['site']['vs30s']
config_verror1['site']['locations'] = [str(Path(__file__).parent / 'fixtures/sites_vs30s_str.csv')]

# imts is now optional
# config_keyerror5 = get_config()
# del config_keyerror5['calculation']['imts']

# must specifiy locations
config_keyerror6 = get_config()
del config_keyerror6['site']['locations']

# imts must be list of strings
config_verror2 = get_config()
config_verror2['calculation']['imts'] = [1, 2, 3]

config_verror3 = get_config()
config_verror3['calculation']['imts'] = "SA(1.5)"

# agg values must be valid
config_verror4 = get_config()
config_verror4['calculation']['agg_types'] = ["mean", "0.5", "1.1"]

# compatibility key must exist
config_verror5 = get_config()
config_verror5['general']['compatibility_key'] = "Z"


@pytest.mark.parametrize("config", [config1, config2, config3])
@mock.patch('toshi_hazard_post.aggregation_args.toml.load')
@mock.patch('toshi_hazard_post.aggregation_args.SourceLogicTree.from_json')
@mock.patch('toshi_hazard_post.aggregation_args.GMCMLogicTree.from_json')
def test_logic_tree_valid(mock_gmcm, mock_srm, mock_load, config):
    mock_load.return_value = config
    assert AggregationArgs('dummy')


@pytest.mark.parametrize(
    "config,errortype",
    [
        (config_keyerror1, KeyError),
        (config_keyerror2, KeyError),
        (config_keyerror3, KeyError),
        (config_keyerror4, KeyError),
        # (config_keyerror5, KeyError),
        (config_keyerror6, KeyError),
        (config_fnferror1, FileNotFoundError),
        (config_rterror1, RuntimeError),
        (config_aerror1, AssertionError),
        (config_verror1, ValueError),
        (config_verror2, ValueError),
        (config_verror3, ValueError),
        (config_verror4, ValueError),
        (config_verror5, ValueError),
    ],
)
@mock.patch('toshi_hazard_post.aggregation_args.toml.load')
def test_logic_tree_error(mock_load, config, errortype):
    mock_load.return_value = config
    with pytest.raises(errortype):
        AggregationArgs('dummy')

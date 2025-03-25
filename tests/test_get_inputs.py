from pathlib import Path

import toml
from nzshm_common.location.location import LOCATION_LISTS
from nzshm_model import get_model_version
from nzshm_model.logic_tree import GMCMLogicTree, SourceLogicTree

from toshi_hazard_post.aggregation_args import load_input_args
from toshi_hazard_post.aggregation_setup import get_logic_trees, get_sites


def test_model():
    args_filepath = Path(__file__).parent / 'fixtures' / 'hazard.toml'
    args = load_input_args(args_filepath)

    slt, glt = get_logic_trees(
        args.hazard_model.nshm_model_version,
        args.hazard_model.srm_logic_tree,
        args.hazard_model.gmcm_logic_tree,
    )
    args_raw = toml.load(args_filepath)

    model_expected = get_model_version(args_raw["hazard_model"]["nshm_model_version"])
    assert slt == model_expected.source_logic_tree
    assert glt == model_expected.gmm_logic_tree


def test_model_from_paths():

    args_filepath = Path(__file__).parent / 'fixtures' / 'hazard_lt_files.toml'
    args_raw = toml.load(args_filepath)
    args = load_input_args(args_filepath)

    slt_expected = SourceLogicTree.from_json(args_filepath.parent / args_raw['hazard_model']['srm_logic_tree'])
    gmcm_expected = GMCMLogicTree.from_json(args_filepath.parent / args_raw['hazard_model']['gmcm_logic_tree'])

    slt, glt = get_logic_trees(
        args.hazard_model.nshm_model_version,
        args.hazard_model.srm_logic_tree,
        args.hazard_model.gmcm_logic_tree,
    )

    assert slt == slt_expected
    assert glt == gmcm_expected


def test_get_sites():
    vs30s = [200, 400]
    locations = ["NZ"]

    # all combinations of location and vs30
    sites = get_sites(locations=locations, vs30s=vs30s)
    assert len(sites) == len(LOCATION_LISTS[locations[0]]['locations']) * len(vs30s)

    locations_file = Path(__file__).parent / 'fixtures/sites_w_vs30.csv'
    sites = get_sites(locations_file=locations_file)
    assert len(sites) == 2
    assert sites[0].vs30 == 250
    assert sites[1].vs30 == 400

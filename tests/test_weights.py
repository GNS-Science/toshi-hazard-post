from nzshm_model.logic_tree import SourceLogicTree, GMCMLogicTree, LogicTreeCorrelations
import importlib.resources as resources
from pathlib import Path
from toshi_hazard_post.logic_tree import HazardLogicTree
import pytest

fixtures_dir = resources.files('tests.fixtures.end_to_end')
srm_filepath = Path(fixtures_dir) / 'srm_logic_tree_no_slab.json'
gmcm_filepath = Path(fixtures_dir) / 'gmcm_logic_tree_medium.json'

def test_weights_nocorrelations():
    srm = SourceLogicTree.from_json(srm_filepath)
    gmcm = GMCMLogicTree.from_json(gmcm_filepath)

    srm.correlations = LogicTreeCorrelations()

    logic_tree = HazardLogicTree(srm, gmcm)

    weights = logic_tree.weights
    assert sum(weights) == pytest.approx(1.0)


def test_weights_correlations():
    srm = SourceLogicTree.from_json(srm_filepath)
    gmcm = GMCMLogicTree.from_json(gmcm_filepath)

    logic_tree = HazardLogicTree(srm, gmcm)

    weights = logic_tree.weights
    assert sum(weights) == pytest.approx(1.0)

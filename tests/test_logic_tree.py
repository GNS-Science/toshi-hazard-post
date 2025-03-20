import math
from pathlib import Path

import pytest
from nzshm_model.logic_tree import GMCMLogicTree, SourceLogicTree
from nzshm_model.logic_tree.correlation import LogicTreeCorrelations

from toshi_hazard_post.logic_tree import HazardLogicTree


@pytest.fixture(scope='function')
def source_logic_tree():
    slt_filepath = Path(__file__).parent / 'fixtures/slt.json'
    return SourceLogicTree.from_json(slt_filepath)


@pytest.fixture(scope='function')
def gmcm_logic_tree():
    gmcm_filepath = Path(__file__).parent / 'fixtures/glt.json'
    return GMCMLogicTree.from_json(gmcm_filepath)


def test_logic_tree_trt(source_logic_tree, gmcm_logic_tree):

    # HazardLogicTree should remove gmcm branch sets that do not have TRTs used by the source logic tree
    hazard_logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    assert len(hazard_logic_tree.gmcm_logic_tree.branch_sets) == 3

    source_logic_tree.branch_sets = [
        bs for bs in source_logic_tree.branch_sets if 'Active Shallow Crust' not in bs.tectonic_region_types
    ]
    hazard_logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    assert len(hazard_logic_tree.gmcm_logic_tree.branch_sets) == 2


def test_logic_tree_weights(source_logic_tree, gmcm_logic_tree):

    hazard_logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    component_branch = list(hazard_logic_tree.component_branches)[0]
    weight_expected = component_branch.source_branch.weight * math.prod(
        [branch.weight for branch in component_branch.gmcm_branches]
    )
    assert list(hazard_logic_tree.component_branches)[0].weight == pytest.approx(weight_expected)

    assert sum(hazard_logic_tree.weights) == pytest.approx(1.0)


def test_logic_tree_branches(source_logic_tree, gmcm_logic_tree):

    # correct number of component and composite branches with correlations
    hazard_logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    assert len(list(hazard_logic_tree.component_branches)) == 36 * 3 + 9 * 2 + 3 * 2 + 1 * 2
    assert len(list(hazard_logic_tree.composite_branches)) == 36 * 9 * 3 * 2 * 2

    # correct number of component and composite branches without correlations
    source_logic_tree.correlations = LogicTreeCorrelations()
    hazard_logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    assert len(list(hazard_logic_tree.component_branches)) == 36 * 3 + 9 * 2 + 3 * 2 + 1 * 2
    assert len(list(hazard_logic_tree.composite_branches)) == 36 * 9 * 3 * 3 * 2 * 2


def test_weights_nocorrelations(source_logic_tree, gmcm_logic_tree):

    source_logic_tree.correlations = LogicTreeCorrelations()
    logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    assert sum(logic_tree.weights) == pytest.approx(1.0)


def test_weights_correlations(source_logic_tree, gmcm_logic_tree):

    logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    assert sum(logic_tree.weights) == pytest.approx(1.0)

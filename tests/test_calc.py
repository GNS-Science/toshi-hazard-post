from pathlib import Path

import numpy as np
import pytest
from nzshm_model.logic_tree import GMCMLogicTree, SourceLogicTree

from toshi_hazard_post.aggregation_calc import build_branch_rates, calc_composite_rates, calculate_aggs
from toshi_hazard_post.logic_tree import HazardLogicTree

# from toshi_hazard_post.data import ValueStore

NLEVELS = 10


@pytest.fixture(scope='function')
def logic_tree():
    slt_filepath = Path(__file__).parent / 'fixtures/slt.json'
    gmcm_filepath = Path(__file__).parent / 'fixtures/glt.json'
    slt = SourceLogicTree.from_json(slt_filepath)
    glt = GMCMLogicTree.from_json(gmcm_filepath)
    return HazardLogicTree(slt, glt)


@pytest.fixture(scope='function')
def component_rates_all(logic_tree):

    component_rates = dict()
    for i, branch in enumerate(logic_tree.component_branches):
        values = np.linspace(0, 1, 10) * i
        component_rates[branch.hash_digest] = values
    return component_rates


@pytest.fixture(scope='function')
def value_store_small(logic_tree):

    component_rates = dict()
    for i, branch in enumerate(logic_tree.composite_branches[0].branches):
        values = np.linspace(0, 1, 10) * i
        component_rates[branch.hash_digest] = values
    return component_rates


@pytest.fixture(scope='function')
def branch_hashes(logic_tree):
    return logic_tree.branch_hash_table


@pytest.fixture(scope='function')
def branch_rates():
    branch_rates_filepath = Path(__file__).parent / 'fixtures/calc/branch_rates.npy'
    return np.load(branch_rates_filepath)


# here we use value_store_all to make sure that component_branches and composite_branches.branches load into
# ValueStore the same way
def test_calc_composite_rates1(branch_hashes, component_rates_all):
    rates = calc_composite_rates(branch_hashes[0], component_rates_all, NLEVELS)
    assert rates.shape == (NLEVELS,)


# here we use values_store_small to make sure that the calculated rate is correct
def test_calc_composite_rates2(logic_tree, branch_hashes, value_store_small):
    rates_expected = np.zeros((NLEVELS,))
    rates = calc_composite_rates(branch_hashes[0], value_store_small, NLEVELS)
    for i in range(len(logic_tree.composite_branches[0].branches)):
        rates_expected += np.linspace(0, 1, NLEVELS) * i

    assert np.array_equal(rates, rates_expected)


def test_build_branch_rates1(logic_tree, branch_hashes, component_rates_all):

    rates = build_branch_rates(branch_hashes, component_rates_all)
    nbranches = len(list(logic_tree.composite_branches))
    assert rates.shape == (nbranches, NLEVELS)


@pytest.fixture(scope='module')
def stats():
    probs = np.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])
    aggs = ["0.1", "0.5", "mean", "cov", "std", "0.9"]
    weights = np.array([1, 2, 1, 4, 1, 2, 2, 3, 3])
    return dict(
        probs=probs,
        aggs=aggs,
        weights=weights,
    )


def test_calculate_aggs(branch_rates):

    hazard_aggs_filepath = Path(__file__).parent / 'fixtures/calc/agg_rates.npy'
    expected = np.load(hazard_aggs_filepath)

    weights = np.array([0.1, 0.1, 0.2, 0.3, 0.1, 0.2])
    agg_types = ['mean', 'std', 'cov', '0.6']
    hazard_agg = calculate_aggs(branch_rates, weights, agg_types)
    assert np.allclose(hazard_agg, expected)

    # agg_types can be in any order
    sorter = [1, 0, 3, 2]
    agg_types = list(np.array(agg_types)[sorter])
    expected = expected[sorter]
    hazard_agg = calculate_aggs(branch_rates, weights, agg_types)
    assert np.allclose(hazard_agg, expected)


# test for bug that didn't handle agg_types list that didn't include 'mean', 'std', and 'cov'
@pytest.mark.parametrize(
    "agg_types",
    [
        ['0.2', '0.9'],
        ['mean', '0.2', '0.5', '0.9'],
        ['mean', 'std', '0.2', '0.9'],
        ['mean', 'std', 'cov', '0.2'],
        ['std', '0.2'],
        ['0.2', 'cov', '0.9'],
    ],
)
def test_agg_types(branch_rates, agg_types):
    weights = np.array([0.1, 0.1, 0.2, 0.3, 0.1, 0.2])
    hazard_agg = calculate_aggs(branch_rates, weights, agg_types)
    assert hazard_agg.shape == (len(agg_types), branch_rates.shape[1])

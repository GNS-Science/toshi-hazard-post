import numpy as np
import pytest
from pathlib import Path

from toshi_hazard_post.version2.logic_tree import HazardLogicTree
from nzshm_model.logic_tree import SourceLogicTree, GMCMLogicTree

from toshi_hazard_post.version2.aggregation_calc import (
    calc_composite_rates,
    build_branch_rates,
    weighted_stats,
    calculate_aggs,
)
from toshi_hazard_post.version2.data import ValueStore

NLEVELS = 10


@pytest.fixture(scope='function')
def logic_tree():
    slt_filepath = Path(__file__).parent / 'fixtures/slt.json'
    gmcm_filepath = Path(__file__).parent / 'fixtures/glt.json'
    slt = SourceLogicTree.from_json(slt_filepath)
    glt = GMCMLogicTree.from_json(gmcm_filepath)
    return HazardLogicTree(slt, glt)


@pytest.fixture(scope='function')
def value_store_all(logic_tree):

    value_store = ValueStore()
    for i, branch in enumerate(logic_tree.component_branches):
        values = np.linspace(0, 1, 10) * i
        value_store.set_values(values, branch)
    return value_store


@pytest.fixture(scope='function')
def value_store_small(logic_tree):

    value_store = ValueStore()
    for i, branch in enumerate(next(logic_tree.composite_branches).branches):
        values = np.linspace(0, 1, 10) * i
        value_store.set_values(values, branch)
    return value_store


# here we use value_store_all to make sure that component_branches and composite_branches.branches load into
# ValueStore the same way
def test_calc_composite_rates1(logic_tree, value_store_all):
    rates = calc_composite_rates(next(logic_tree.composite_branches), value_store_all, NLEVELS)
    assert rates.shape == (NLEVELS,)


# here we use values_store_small to make sure that the calculated rate is correct
def test_calc_composite_rates2(logic_tree, value_store_small):
    rates_expected = np.zeros((NLEVELS,))
    rates = calc_composite_rates(next(logic_tree.composite_branches), value_store_small, NLEVELS)
    for i in range(len(next(logic_tree.composite_branches).branches)):
        rates_expected += np.linspace(0, 1, NLEVELS) * i

    assert np.array_equal(rates, rates_expected)


def test_build_branch_rates1(logic_tree, value_store_all):

    rates = build_branch_rates(logic_tree, value_store_all, NLEVELS)
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


def test_weighted_stats(stats):
    stats_file = Path(__file__).parent / 'fixtures/calc/quantiles_expected.npy'
    stats = weighted_stats(stats['probs'], stats['aggs'], stats['weights'])

    print(stats)

    stats_expected = np.load(stats_file)
    assert np.allclose(stats, stats_expected)


def test_zero_mean(stats):
    probs = np.zeros(stats['probs'].shape)
    stats_rec = weighted_stats(probs, stats['aggs'], stats['weights'])
    assert stats_rec[stats['aggs'].index("cov")] == 0


def test_calculate_aggs():

    branch_probs_filepath = Path(__file__).parent / 'fixtures/calc/branch_rates.npy'
    hazard_aggs_filepath = Path(__file__).parent / 'fixtures/calc/agg_rates.npy'
    branch_rates = np.load(branch_probs_filepath)
    expected = np.load(hazard_aggs_filepath)

    weights = np.array([0.1, 0.1, 0.2, 0.3, 0.1, 0.2])
    aggs = ['mean', 'std', 'cov', '0.6']
    hazard_agg = calculate_aggs(branch_rates, weights, aggs)
    assert np.allclose(hazard_agg, expected)

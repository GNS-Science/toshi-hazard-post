from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from nzshm_model.logic_tree import GMCMLogicTree, SourceLogicTree

import toshi_hazard_post.aggregation_calc as aggregation_calc
from toshi_hazard_post.logic_tree import HazardLogicTree, build_branch_index_table

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
def component_digests(logic_tree):
    return logic_tree.component_digests


@pytest.fixture(scope='function')
def component_rates_all(component_digests):
    # row i is the rate curve of component branch i
    return np.outer(np.arange(len(component_digests)), np.linspace(0, 1, NLEVELS))


@pytest.fixture(scope='function')
def branch_hashes(logic_tree):
    return logic_tree.branch_hash_table


@pytest.fixture(scope='function')
def branch_rates():
    # the fixture is stored as (branch, level); calculate_aggs takes (level, branch)
    branch_rates_filepath = Path(__file__).parent / 'fixtures/calc/branch_rates.npy'
    return np.ascontiguousarray(np.load(branch_rates_filepath).T)


def test_build_branch_rates():
    component_rates = np.array([[1.0, 2.0, 3.0], [10.0, 20.0, 30.0], [100.0, 200.0, 300.0]])
    index_table = np.array([[0, 1], [2, 2]], dtype=np.uint16)
    rates = aggregation_calc.build_branch_rates(index_table, component_rates)
    # (level, composite branch)
    assert rates.tolist() == [[11.0, 200.0], [22.0, 400.0], [33.0, 600.0]]


def test_build_branch_rates_index_out_of_range():
    component_rates = np.array([[1.0, 2.0], [10.0, 20.0]])
    index_table = np.array([[0, 2]], dtype=np.uint16)
    with pytest.raises(IndexError):
        aggregation_calc.build_branch_rates(index_table, component_rates)


# use a real logic tree to make sure that component_branches and composite_branches.branches index the same way
def test_build_branch_rates_logic_tree(logic_tree, branch_hashes, component_digests, component_rates_all):
    index_table = build_branch_index_table(branch_hashes, component_digests)
    rates = aggregation_calc.build_branch_rates(index_table, component_rates_all)

    nbranches = len(logic_tree.composite_branches)
    assert rates.shape == (NLEVELS, nbranches)

    for i_composite in (0, nbranches - 1):
        rates_expected = np.zeros((NLEVELS,))
        for branch in logic_tree.composite_branches[i_composite].branches:
            rates_expected += np.linspace(0, 1, NLEVELS) * component_digests.index(branch.hash_digest)
        assert np.array_equal(rates[:, i_composite], rates_expected)


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
    hazard_agg = aggregation_calc.calculate_aggs(branch_rates, weights, agg_types)
    assert np.allclose(hazard_agg, expected)

    # agg_types can be in any order
    sorter = [1, 0, 3, 2]
    agg_types = list(np.array(agg_types)[sorter])
    expected = expected[sorter]
    hazard_agg = aggregation_calc.calculate_aggs(branch_rates, weights, agg_types)
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
    hazard_agg = aggregation_calc.calculate_aggs(branch_rates, weights, agg_types)
    assert hazard_agg.shape == (len(agg_types), branch_rates.shape[0])


def test_convert_p2r():
    d = {'values': [np.array([1, 1, 1]), np.array([0, 0, 0])]}
    df_in = pd.DataFrame(d)
    df_out = aggregation_calc.convert_probs_to_rates(df_in)

    assert all(df_out.columns == ['rates'])
    assert all(df_out.loc[0]['rates'] == [np.inf, np.inf, np.inf])
    assert all(df_out.loc[1]['rates'] == [0, 0, 0])


def test_component_array():
    d = {
        'sources_digest': ['def', 'abc', 'ghi'],
        'gmms_digest': ['456', '123', '789'],
        'rates': [np.array([2.0, 20.0]), np.array([1.0, 10.0]), np.array([3.0, 30.0])],
    }
    df = pd.DataFrame(d)

    component_array = aggregation_calc.create_component_array(df, ['abc123', 'def456', 'ghi789'])

    assert component_array.tolist() == [[1.0, 10.0], [2.0, 20.0], [3.0, 30.0]]


@pytest.mark.parametrize("component_digests", [['abc123', 'def456', 'xyz000'], ['abc123']])
def test_component_array_digest_mismatch(component_digests):
    d = {
        'sources_digest': ['def', 'abc'],
        'gmms_digest': ['456', '123'],
        'rates': [np.array([2.0, 20.0]), np.array([1.0, 10.0])],
    }
    df = pd.DataFrame(d)

    with pytest.raises(KeyError):
        aggregation_calc.create_component_array(df, component_digests)

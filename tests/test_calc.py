from pathlib import Path

import numpy as np
import pytest
from nzshm_common.location.coded_location import CodedLocation
from nzshm_model.logic_tree import GMCMLogicTree, SourceLogicTree

import toshi_hazard_post.aggregation_calc as aggregation_calc
import toshi_hazard_post.calculators as calculators
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


@pytest.mark.parametrize("agg_types", [['mean', 'std', 'cov', '0.6', '0.1'], ['0.9', 'cov', '0.005'], ['std']])
def test_calculate_aggs_from_components(agg_types):
    rng = np.random.default_rng(1)
    nbranches = 6000  # enough that the rates are not all sorted at once
    component_rates = np.exp(rng.normal(0.0, 1.0, (8, NLEVELS))) * np.logspace(-1, -6, NLEVELS)[None, :]
    columns = [np.arange(0, 4), np.arange(4, 8)]
    index_table = np.stack([rng.choice(column, nbranches) for column in columns], axis=1).astype(np.uint16)
    weights = rng.random(nbranches)

    aggs = aggregation_calc.calculate_aggs_from_components(index_table, component_rates, weights, agg_types, columns)

    # the aggregates of the composite rates, with the quantiles found with the bounds that the fused kernel uses
    rates = aggregation_calc.build_branch_rates(index_table, component_rates)
    mean, std = calculators.weighted_avg_and_std(rates, weights)
    bounds = calculators.composite_rate_bounds(columns, component_rates)
    quantiles = calculators.weighted_quantiles(
        rates, weights, aggregation_calc._quantile_points(agg_types), bounds=bounds
    )
    expected = aggregation_calc._assemble_aggs(agg_types, NLEVELS, mean, std, quantiles)
    assert aggs.tolist() == expected.tolist()


def test_calculate_aggs_from_components_index_out_of_range():
    component_rates = np.array([[1.0, 2.0], [10.0, 20.0]])
    index_table = np.array([[0, 2]], dtype=np.uint16)
    with pytest.raises(IndexError):
        aggregation_calc.calculate_aggs_from_components(
            index_table, component_rates, np.ones(1), ['mean'], [np.array([0]), np.array([2])]
        )


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


def test_prob_to_rate_whole_array_matches_rows(monkeypatch):
    """Converting a job's probabilities at once gives exactly what converting them row by row did."""
    import toshi_hazard_post.data as data
    from tests.test_data import compatibility_key, gmms_digests, imts, location, logic_tree, nloc_0, sources_digests
    from tests.test_data import vs30 as fixture_vs30

    monkeypatch.setattr(data, 'RLZ_DIR', str(Path(__file__).parent / 'fixtures/end_to_end/rlz'))
    batch = data.get_batch_table(
        data.get_realizations_dataset(fixture_vs30, nloc_0),
        compatibility_key,
        sources_digests,
        gmms_digests,
        fixture_vs30,
        nloc_0,
        imts,
    )
    digests = logic_tree.component_digests
    ids = data.component_row_ids(batch, digests)
    probs = data.get_job_probs(batch, data.group_batch_rows(batch), ids, digests, location, imts[0])

    whole = calculators.prob_to_rate(probs, 1.0)
    rows = np.stack([calculators.prob_to_rate(row, 1.0) for row in probs])
    assert whole.dtype == rows.dtype
    assert np.array_equal(whole, rows)


def test_task_args_repr_leaves_out_probs():
    args = aggregation_calc.AggTaskArgs(
        location=CodedLocation(-41.3, 174.78, 0.001), vs30=400, imt="PGA", probs=np.zeros((912, 44), np.float32)
    )
    text = repr(args)
    assert "(912, 44)" in text and "float32" in text and "-41.300~174.780" in text
    assert len(text) < 200

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


def test_build_branch_rates_into_buffer():
    component_rates = np.array([[1.0, 2.0, 3.0], [10.0, 20.0, 30.0], [100.0, 200.0, 300.0]])
    index_table = np.array([[0, 1], [2, 2]], dtype=np.uint16)
    out = np.full((3, 2), np.nan)

    rates = aggregation_calc.build_branch_rates(index_table, component_rates, out=out)

    assert rates is out
    assert rates.tolist() == [[11.0, 200.0], [22.0, 400.0], [33.0, 600.0]]


@pytest.mark.parametrize(
    "out", [np.empty((2, 2)), np.empty((3, 2), dtype=np.float32), np.empty((2, 3)).T], ids=["shape", "dtype", "order"]
)
def test_build_branch_rates_rejects_unusable_buffer(out):
    component_rates = np.array([[1.0, 2.0, 3.0], [10.0, 20.0, 30.0], [100.0, 200.0, 300.0]])
    index_table = np.array([[0, 1], [2, 2]], dtype=np.uint16)
    with pytest.raises(ValueError, match="out"):
        aggregation_calc.build_branch_rates(index_table, component_rates, out=out)


def test_composite_rates_buffer_is_reused_within_a_thread():
    buffer = aggregation_calc._composite_rates_buffer((3, 4))

    assert aggregation_calc._composite_rates_buffer((3, 4)) is buffer
    assert aggregation_calc._composite_rates_buffer((3, 5)).shape == (3, 5)


def test_composite_rates_buffer_is_not_shared_between_threads():
    # test_end_to_end runs the tasks on a ThreadPoolExecutor, so two tasks can run at once in one process
    from concurrent.futures import ThreadPoolExecutor

    buffer = aggregation_calc._composite_rates_buffer((3, 4))
    with ThreadPoolExecutor(max_workers=1) as executor:
        other = executor.submit(aggregation_calc._composite_rates_buffer, (3, 4)).result()

    assert other is not buffer


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


def test_calculate_aggs_with_bounds(branch_rates):
    weights = np.array([0.1, 0.1, 0.2, 0.3, 0.1, 0.2])
    agg_types = ['mean', 'std', 'cov', '0.6', '0.1']
    # any bounds that hold the rates of each level, here wider than they need to be
    bounds = (branch_rates.min(axis=1) - 1.0, branch_rates.max(axis=1) + 1.0, branch_rates.max(axis=1) + 1.0)

    with_bounds = aggregation_calc.calculate_aggs(branch_rates, weights, agg_types, bounds)

    np.testing.assert_allclose(
        with_bounds, aggregation_calc.calculate_aggs(branch_rates, weights, agg_types), rtol=1e-12
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

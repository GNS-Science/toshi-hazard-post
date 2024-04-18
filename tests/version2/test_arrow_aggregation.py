"""
Some tests that explore various  approaches to data wrangling with arrow/numpy/pandas and duckdb

final test is a slow performance comparison of the four main methods

"""
import pathlib
import time
import random

import pytest
import pyarrow as pa
import pandas as pd
import numpy as np
import duckdb

from toshi_hazard_post.version2.aggregation_config import AggregationConfig
from toshi_hazard_post.version2.aggregation_calc_arrow import calc_aggregation_arrow
from toshi_hazard_post.version2.logic_tree import HazardLogicTree
from toshi_hazard_post.version2.aggregation_setup import get_lts, get_sites
from toshi_hazard_post.version2 import calculators

## for original tests
from toshi_hazard_post.version2.aggregation_calc import (
    build_branch_rates,
    calculate_aggs,
)
from toshi_hazard_post.version2.data import load_realizations

DEMO_PATH = pathlib.Path(__file__).parent.parent.parent / "demo"


@pytest.fixture(scope='module')
def config():
    toml = DEMO_PATH / 'hazard_v2_micro.toml'
    assert toml.exists()
    yield AggregationConfig(toml)


@pytest.fixture(scope='module')
def hazard_logic_tree(config):
    srm_lt, gmcm_lt = get_lts(config)
    yield HazardLogicTree(srm_lt, gmcm_lt)


@pytest.fixture(scope='module')
def rates_weights(config, hazard_logic_tree):
    sites = get_sites(config.locations, config.vs30s)
    weight_table = hazard_logic_tree.weight_table()
    rates_weights = calc_aggregation_arrow(
        site=sites[0],
        imts=[config.imts[0]],
        agg_types=config.agg_types,
        weights=weight_table,
        logic_tree=hazard_logic_tree,
        compatibility_key=config.compat_key,
        hazard_model_id=config.hazard_model_id,
    )
    yield rates_weights


def test_rates_weights_dimensions(rates_weights, hazard_logic_tree):
    assert rates_weights.shape == (96, 4)
    # there are four ComponentBranches in each CompositeBranch
    assert rates_weights.shape[0] == len(list(hazard_logic_tree.composite_branches)) * 4


def test_rates_weights_groupby(rates_weights):
    rwtg = rates_weights.group_by(['sources_digest', 'gmms_digest']).aggregate([('weight', 'sum')])
    assert rwtg.column(0).unique() == rates_weights.column(0).unique()  # source_digest
    assert rwtg.column(1).unique() == rates_weights.column(1).unique()  # gmm_digest
    assert rwtg.shape == (12, 3)


def test_rates_weights_values_numpy_sum(rates_weights, hazard_logic_tree):
    """nice but it's not what we need, see test_pandas_with_filter_groupby"""
    # rwtg = rates_weights.group_by(['sources_digest', 'gmms_digest']).aggregate([('weight', 'sum')])
    values_arr = rates_weights.column(2).to_numpy()
    # print(values_arr)
    summed = np.sum(values_arr, axis=0)

    print(values_arr.shape)
    print(summed.mean().shape)
    assert summed.shape[0] == values_arr[0].shape[0]
    # assert 0


def test_duckdb_can_sum_scalars(rates_weights):
    """Sum scalars works, but not for our values array field"""
    con = duckdb.connect()
    summed = con.execute(
        "SELECT sources_digest, gmms_digest, sum(weight) FROM rates_weights GROUP BY sources_digest, gmms_digest;"
    )
    assert summed.arrow().shape == (12, 3)


def test_duckdb_cannot_sum_arrays(rates_weights):
    """this shows there's not a cute duckie option, keep trying"""
    con = duckdb.connect()
    with pytest.raises(Exception):
        summed = con.execute(
            "SELECT sources_digest, gmms_digest, sum(values), sum(weight) FROM rates_weights "
            " GROUP BY sources_digest, gmms_digest;"
        )
        print(summed)


def test_pandas_with_filter_groupby(rates_weights, hazard_logic_tree):
    rwdf = rates_weights.to_pandas()
    digest_keys = rwdf.gmms_digest + "|" + rwdf.sources_digest
    digest_keys.name = "digest_keys"
    rwdf2 = pd.concat([rwdf.loc[:, ["rates", "weight"]], digest_keys], axis=1)

    summed = rwdf2.groupby('digest_keys').sum()
    assert summed.shape == (12, 2)

    # TODO sum of weights should be 4, but no it's 2 check with CDC
    # assert summed['weight'].sum() == 4.0


def test_pandas_equals_original(rates_weights, hazard_logic_tree):
    rwdf = rates_weights.to_pandas()
    digest_keys = rwdf.gmms_digest + "|" + rwdf.sources_digest
    digest_keys.name = "digest_keys"
    rwdf2 = pd.concat([rwdf.loc[:, ["rates", "weight"]], digest_keys], axis=1)
    summed = rwdf2.groupby('digest_keys').sum()
    assert summed.shape == (12, 2)


def test_original_aggregation(hazard_logic_tree, config):
    aggs = ['mean', 'std', 'cov', '0.6']
    sites = get_sites(config.locations, config.vs30s)
    weights = hazard_logic_tree.weights
    levels = range(44)
    site = sites[0]
    value_store = load_realizations(
        hazard_logic_tree, imt='PGA', location=site.location, vs30=site.vs30, compatibility_key=config.compat_key
    )
    branch_rates = build_branch_rates(hazard_logic_tree, value_store, len(levels))
    hazard = calculate_aggs(branch_rates, weights=weights, agg_types=aggs)
    print(hazard.shape)
    assert hazard.shape[1] == len(aggs)
    assert hazard.shape[0] == len(levels)


def test_apply_fn_with_pandas_concat(rates_weights):
    """apply calc functions usign vectorization. and check the values survive the round-trip

    NB this uses pandas as I hadn't figured out the numpy vectroisation trick yet
    """

    vpr = np.vectorize(calculators.prob_to_rate)
    vrp = np.vectorize(calculators.rate_to_prob)
    rw_df = rates_weights.to_pandas()

    # print(rw_df.info())
    val_series = rw_df['rates']
    val_series = val_series.apply(vpr, inv_time=1.0)
    val_series = val_series.apply(vrp, inv_time=1.0)

    _rw_df = pd.concat([rw_df[['sources_digest', 'gmms_digest', 'weight']], val_series], axis=1)

    assert _rw_df.shape == rw_df.shape

    og_values = rates_weights.column(2).to_numpy()
    rates_weights = rates_weights.set_column(2, 'rates', pa.array(_rw_df['rates']))

    new_values = rates_weights.column(2).to_numpy()

    # check em all..
    for idx in range(len(og_values)):
        assert np.allclose(og_values[idx], new_values[idx])


def test_numpy_nested_array_understanding():
    """trying different optinos to modify nested array values"""
    _2d_array = []
    for n in range(2):
        _2d_array.append(np.arange(0.08, 0.01, -0.01))

    print(_2d_array)
    tbl = pa.table([pa.array(_2d_array)], ['rates'])

    # `otypes=[object]` stops ValueError: setting an array element with a sequence. with np.apply_along_axis
    vpr = np.vectorize(calculators.prob_to_rate, otypes=[object])

    val_series_apply = tbl.to_pandas().rates.apply(vpr, inv_time=1.0)  # noqa: F841

    val_series_along_np = np.apply_along_axis(vpr, 0, tbl.column(0).to_numpy(), inv_time=1.0)
    print(val_series_along_np)
    val_series_along = np.apply_along_axis(vpr, 0, tbl.to_pandas().rates, inv_time=1.0)
    print(val_series_along)
    # assert 0


def test_no_apply_fn_with_numpy(rates_weights):
    """
    we can stay in numpy
    """
    vpr = np.vectorize(calculators.prob_to_rate, otypes=[object])
    rates = rates_weights.column(2).to_numpy()
    probs = np.apply_along_axis(vpr, 0, rates, inv_time=1.0)
    print(probs.sum() / 2)


def test_numpy_nested_array_and_vectorized_perf():
    """
    this shows us that pandas is actualy quicker than numpy until array sizes get larger than 912 * 10,
    ... thereafter numpy trumps pandas
    """
    _2d_array = []
    for n in range(912 * 50):
        _2d_array.append(np.arange(0.09, 0.001, -0.002, dtype=np.float32))

    # _2d_array = np.array(_2d_array)
    # assert _2d_array.shape == (912, 45)

    tbl = pa.table([pa.array(_2d_array)], ['rates'])
    vpr = np.vectorize(calculators.prob_to_rate, otypes=[object])

    ## fastest to slowest (above  912 * 10,)
    tic = time.perf_counter()
    val_series_along_np = np.apply_along_axis(vpr, 0, tbl.column(0).to_numpy(), inv_time=1.0)
    toc = time.perf_counter()
    print(toc - tic, "numpy")

    tic = time.perf_counter()
    val_series_apply = tbl.to_pandas().rates.apply(vpr, inv_time=1.0)
    toc = time.perf_counter()
    print(toc - tic, "pandas apply")

    tic = time.perf_counter()
    val_series_apply = tbl.to_pandas().rates.apply(vpr, inv_time=1.0)
    val_series_along_pd = np.apply_along_axis(vpr, 0, tbl.to_pandas().rates, inv_time=1.0)
    toc = time.perf_counter()
    print(toc - tic, "pandas + numpy")

    tic = time.perf_counter()
    pd_series_iterable = tbl.to_pandas().rates
    for n in range(pd_series_iterable.shape[0]):
        pd_series_iterable[n] = vpr(pd_series_iterable[n], inv_time=1.0)
    toc = time.perf_counter()
    print(toc - tic, "for loop")

    for idx in random.sample(range(pd_series_iterable.shape[0]), 10):
        for x in random.sample(range(45), 10):
            assert val_series_along_np[idx][x] == val_series_apply[idx][x]
            assert val_series_along_pd[idx][x] == val_series_apply[idx][x]
            assert pd_series_iterable[idx][x] == val_series_apply[idx][x]

    # print(val_series_along_np[idx])
    # print(val_series_apply[idx])
    # # assert np.allclose(val_series_along_pd[idx], val_series_apply[idx])
    # assert 0

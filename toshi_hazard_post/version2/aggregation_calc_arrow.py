import logging
import time
from typing import TYPE_CHECKING, List, Sequence

import duckdb
import numpy as np
import pyarrow as pa

import toshi_hazard_post.version2.calculators as calculators

# from toshi_hazard_post.version2.data import load_realizations, save_aggregations
from toshi_hazard_post.version2.data_arrow import load_realizations as load_arrow_realizations

if TYPE_CHECKING:
    from toshi_hazard_post.version2.aggregation_setup import Site
    from toshi_hazard_post.version2.logic_tree import HazardLogicTree

log = logging.getLogger(__name__)


def join_rates_weights(rlz_table: pa.Table, weights: pa.Table) -> pa.Table:
    """join the tables using the two digest columns

    Using duckdb for now because ...
      - still using our in-memory dataframes
      - SQL syntax
      - hides some complexity
    """
    con = duckdb.connect()
    joined = con.execute(
        f"SELECT r.sources_digest, r.gmms_digest, r.rates, w.weight FROM rlz_table r JOIN "
        f"weights w ON r.sources_digest = w.sources_digest AND r.gmms_digest = w.gmms_digest;"
    )
    rates_weights_joined = joined.arrow()

    if True:
        # af9ec2b004d7 380a95154af2
        smpl = con.execute(
            "SELECT * FROM rates_weights_joined WHERE sources_digest = 'af9ec2b004d7' "
            "AND gmms_digest = '380a95154af2';"
        )
        smpl_df = smpl.arrow().to_pandas()
        print(smpl_df)

    log.info(f"rates_weights_joined shape: {rates_weights_joined.shape}")
    # log.debug(rates_weights_joined.to_pandas()) # Don't do this for logging it takes significant time
    log.info(
        "RSS: {}MB".format(pa.total_allocated_bytes() >> 20)
    )  #     value_store.set_values(values, component_branch)
    return rates_weights_joined


def convert_probs_to_rates(rlz_table):
    """all aggregations must be performed in rates space, but rlz have probablities

    here we're only vectorising internally to the row, maybe this could be done over the entire columns ??
    """
    probs_array = rlz_table.column(2).to_numpy()
    print(probs_array.shape)

    vpr = np.vectorize(calculators.prob_to_rate, otypes=[object])

    rates_array = np.apply_along_axis(vpr, 0, probs_array, inv_time=1.0)
    print(rates_array.shape)
    return rlz_table.set_column(2, 'rates', pa.array(rates_array))


def calculate_aggs(rates_weights_table: pa.Table, agg_types: Sequence[str]):
    """

    Calculate weighted aggregate statistics of the composite realizations

    modified from OG, but parked for now until we've figured out the data shapes with CDC
    """
    # flake8: noqa
    branch_rates = rates_weights_table.column(2).to_numpy()
    weights = rates_weights_table.column(3).to_numpy()

    # rw_df = rates_weights_table.to_pandas()
    # rates_series = rw_df['rates']
    # weight_series = rw_df['weight']

    # log.debug(f"calculate_aggs(): branch_rates with shape {rates_series.shape}")
    # log.debug(f"calculate_aggs(): weights with shape {rates_series.shape}")
    log.debug(f"calculate_aggs(): agg_types {agg_types}")

    # vectorize it
    vect_wtd_avg = np.vectorize(calculators.weighted_avg_and_std, otypes=[object])

    # tic = time.perf_counter()
    # val_series_along_np = np.apply_along_axis(vpr, 0, rates, inv_time=1.0)
    # toc = time.perf_counter()
    # print(toc - tic, "numpy")
    # # wa_std = rates_series.apply(vect_wtd_avg, weights=weight_series)
    assert 0
    # nrows = rates_series.shape[0]

    # log.debug(f"{rates_series[:, 0]}")

    # OG code block: for loop
    # ncols = len(agg_types)
    # aggs = np.empty((nrows, ncols)) # (IMTL, agg_type)
    # for i in range(nrows):
    #     quantiles = aggregation_calc.weighted_stats(rates_series[:, i], list(agg_types), sample_weight=weight_series)
    #     aggs[i, :] = np.array(quantiles)


def calc_aggregation_arrow(
    site: 'Site',
    imts: List[str],
    agg_types: List[str],
    weights: 'pa.table',
    logic_tree: 'HazardLogicTree',
    compatibility_key: str,
    hazard_model_id: str,
) -> pa.table:
    """
    Calculate hazard aggregation for a single site and imt and save result

    Parameters:
        site: location, vs30 pair
        imt: Intensity measure type (e.g. "PGA", "SA(1.5)")
        agg_types: the aggregate statistics to be calculated (e.g., "mean", "0.5")
        levels: IMTLs for the hazard curve
        weights: weights for the branches of the logic tree
        logic_tree: the complete (srm + gmcm combined) logic tree
        compatibility_key: the key identifying the hazard calculation compatibility entry
        hazard_model_id: the id of the hazard model for storing results in the database

    Returns:
        exception: the raised exception if any part of the calculation fails
    """
    location = site.location
    vs30 = site.vs30

    log.info("loading realizations . . .")
    tic = time.perf_counter()
    rlz_table = load_arrow_realizations(logic_tree, imts, location, vs30, compatibility_key)
    toc = time.perf_counter()
    log.debug(f'time to load realizations {toc-tic:.2f} seconds')
    log.debug(f"rlz_table {rlz_table.shape}")
    # log.debug(rlz_table.to_pandas())
    # print(weights)

    # convert probabilities to rates
    tic = time.perf_counter()
    rates_table = convert_probs_to_rates(rlz_table)
    del rlz_table
    toc = time.perf_counter()
    log.debug(f'time to convert_probs_to_rates() {toc-tic:.2f} seconds')
    log.debug(f"rates_table {rates_table.shape}")

    # print(rates_table)

    # join tables (NB this is a bit expensive, esp as we have
    #  four times (tectonic_types) more rows than OG approach
    tic = time.perf_counter()
    rates_weights = join_rates_weights(rates_table, weights)
    toc = time.perf_counter()
    log.debug(f'time to join_rates_weights() {toc-tic:.2f} seconds')
    log.debug(f"rates_weights {rates_weights.shape}")

    return rates_weights  # for now, just just finish up here

    ## TODO: now we need to figure out the math (sum of rates, ett)
    ## Need to figure out vectorization better (OK we have an approach nailed)
    # aggregates = calculate_aggs(rates_weights, agg_types)

    log.info("saving result . . . ")
    # save_aggregations(hazard, location, vs30, imt, agg_types, hazard_model_id)

    return None

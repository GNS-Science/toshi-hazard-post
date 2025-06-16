"""
Functions for loading realizations and saving aggregations
"""

import logging
import time
from typing import TYPE_CHECKING, List

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from toshi_hazard_store.model.hazard_models_pydantic import HazardAggregateCurve
from toshi_hazard_store.model.pyarrow import pyarrow_aggr_dataset, pyarrow_dataset

from toshi_hazard_post.local_config import get_config

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    import numpy.typing as npt
    from nzshm_common.location.coded_location import CodedLocation

    from toshi_hazard_post.logic_tree import HazardComponentBranch


def get_batch_table(
    dataset: ds.Dataset,
    compatibility_key: str,
    sources_digests: list[str],
    gmms_digests: list[str],
    nloc_0: str,
    vs30: int,
    imts: list[str],
) -> pa.Table:
    columns = ['nloc_001', 'imt', 'sources_digest', 'gmms_digest', 'values']
    flt = (
        (pc.field('compatible_calc_id') == pc.scalar(compatibility_key))
        & (pc.is_in(pc.field('sources_digest'), pa.array(sources_digests)))
        & (pc.is_in(pc.field('gmms_digest'), pa.array(gmms_digests)))
        & (pc.field('nloc_0') == pc.scalar(nloc_0))
        & (pc.field('vs30') == pc.scalar(vs30))
        & (pc.is_in(pc.field('imt'), pa.array(imts)))
    )
    t0 = time.perf_counter()
    batch_datatable = dataset.to_table(columns=columns, filter=flt)
    t1 = time.perf_counter()
    log.info("time to create data table 1: %0.1f seconds" % (t1 - t0))
    return batch_datatable


def get_job_datatable(
    batch_datatable: pa.Table,
    location: 'CodedLocation',
    imt: str,
    n_expected: int,
) -> pa.Table:
    t0 = time.perf_counter()
    table = batch_datatable.filter((pc.field("imt") == imt) & (pc.field("nloc_001") == location.downsample(0.001).code))
    t1 = time.perf_counter()
    log.info("time to create job data table 2: %0.5f seconds" % (t1 - t0))
    table = pa.table(
        {
            "sources_digest": table['sources_digest'].to_pylist(),
            "gmms_digest": table['gmms_digest'].to_pylist(),
            "values": table['values'],
        }
    )
    t2 = time.perf_counter()

    if len(table) == 0:
        raise Exception(f"no records found for location: {location}, imt: {imt}")
    if len(table) != n_expected:
        msg = f"incorrect number of records found for location: {location}, imt: {imt}. Expected {n_expected}, got {len(table)}"
        raise Exception(msg)

    log.info("time to create final job data table: %0.5f seconds" % (t2 - t1))
    return table


def save_aggregations(
    hazard: 'npt.NDArray',
    location: 'CodedLocation',
    vs30: int,
    imt: str,
    agg_types: list[str],
    hazard_model_id: str,
    compatability_key: str,
) -> None:
    """
    Save the aggregated hazard to the database. Converts hazard as rates to proabilities before saving.

    Parameters:
        hazard: the aggregate hazard rates (not proabilities)
        location: the site location
        vs30: the site vs30
        imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
        agg_types: the statistical aggregate types (e.g. "mean", "0.5")
        hazard_model_id: the model id for storing in the database
    """

    def generate_models():
        for i, agg in enumerate(agg_types):
            yield HazardAggregateCurve(
                compatible_calc_id=compatability_key,
                hazard_model_id=hazard_model_id,
                nloc_001=location.code,
                nloc_0=location.downsample(1.0).code,
                imt=imt,
                vs30=vs30,
                aggr=agg,
                values=hazard[i, :],
            )

    config = get_config()

    agg_dir, filesystem = pyarrow_dataset.configure_output(config['AGG_DIR'])
    partitioning = ['vs30', 'imt', 'nloc_001']
    pyarrow_aggr_dataset.append_models_to_dataset(
        models=generate_models(), base_dir=agg_dir, filesystem=filesystem, partitioning=partitioning
    )


def get_realizations_dataset() -> ds.Dataset:
    """
    Get a pyarrow Dataset filtered to a location bin (partition), component branches, and compatibility key

    Parameters:
        location_bin: the location bin that the database is partitioned on
        component_branches: the branches to filter into the dataset
        compatibility_key: the hazard engine compatibility ley to filter into the dataset

    Returns:
        dataset: the dataset with the filteres applied
    """

    config = get_config()
    rlz_dir, filesystem = pyarrow_dataset.configure_output(config['RLZ_DIR'])

    t0 = time.monotonic()
    dataset = ds.dataset(rlz_dir, format='parquet', filesystem=filesystem, partitioning='hive')
    t1 = time.monotonic()
    log.info("time to get realizations dataset %0.6f" % (t1 - t0))

    return dataset


# def load_realizations(
#     imt: str,
#     location: 'CodedLocation',
#     vs30: int,
#     component_branches: list['HazardComponentBranch'],
#     compatibility_key: str,
# ) -> pd.DataFrame:
#     """
#     Load component realizations from the database.

#     Parameters:
#         component_branches: list of the component branches that are combined to construct the full logic tree
#         imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
#         location: the site location
#         vs30: the site vs30
#         compatibility_key: the compatibility key used to lookup the correct realizations in the database

#     Returns:
#         values: the component realizations
#     """
#     dataset = get_realizations_dataset()

#     gmms_digests = [branch.gmcm_hash_digest for branch in component_branches]
#     sources_digests = [branch.source_hash_digest for branch in component_branches]

#     flt = (
#         (pc.field('compatible_calc_id') == pc.scalar(compatibility_key))
#         & (pc.is_in(pc.field('sources_digest'), pa.array(sources_digests)))
#         & (pc.is_in(pc.field('gmms_digest'), pa.array(gmms_digests)))
#         & (pc.field('nloc_0') == pc.scalar(location.downsample(1.0).code))
#         & (pc.field('nloc_001') == pc.scalar(location.downsample(0.001).code))
#         & (pc.field('imt') == pc.scalar(imt))
#         & (pc.field('vs30') == pc.scalar(vs30))
#     )

#     t0 = time.monotonic()
#     columns = ['sources_digest', 'gmms_digest', 'values']
#     arrow_scanner = ds.Scanner.from_dataset(dataset, filter=flt, columns=columns, use_threads=False)
#     t1 = time.monotonic()

#     rlz_table = arrow_scanner.to_table()
#     t2 = time.monotonic()
#     if len(rlz_table) == 0:
#         raise Exception(
#             f"no realizations were found in the database for {location=}, {imt=}, {vs30=}, {compatibility_key=}"
#         )

#     log.info("load scanner:%0.6f, to_arrow %0.6fs" % (t1 - t0, t2 - t1))
#     log.info("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
#     log.info("loaded %s realizations in arrow", rlz_table.shape[0])

#     rlz_df = rlz_table.to_pandas()
#     rlz_df['sources_digest'] = rlz_df['sources_digest'].astype(str)
#     rlz_df['gmms_digest'] = rlz_df['gmms_digest'].astype(str)
#     return rlz_df

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


def save_aggregations(
    hazard: 'npt.NDArray',
    location: 'CodedLocation',
    vs30: int,
    imt: str,
    agg_types: List[str],
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


def load_realizations(
    imt: str,
    location: 'CodedLocation',
    vs30: int,
    component_branches: List['HazardComponentBranch'],
    compatibility_key: str,
) -> pd.DataFrame:
    """
    Load component realizations from the database.

    Parameters:
        component_branches: list of the component branches that are combined to construct the full logic tree
        imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
        location: the site location
        vs30: the site vs30
        compatibility_key: the compatibility key used to lookup the correct realizations in the database

    Returns:
        values: the component realizations
    """
    dataset = get_realizations_dataset()

    gmms_digests = [branch.gmcm_hash_digest for branch in component_branches]
    sources_digests = [branch.source_hash_digest for branch in component_branches]

    flt = (
        (pc.field('compatible_calc_id') == pc.scalar(compatibility_key))
        & (pc.is_in(pc.field('sources_digest'), pa.array(sources_digests)))
        & (pc.is_in(pc.field('gmms_digest'), pa.array(gmms_digests)))
        & (pc.field('nloc_0') == pc.scalar(location.downsample(1.0).code))
        & (pc.field('nloc_001') == pc.scalar(location.downsample(0.001).code))
        & (pc.field('imt') == pc.scalar(imt))
        & (pc.field('vs30') == pc.scalar(vs30))
    )

    t0 = time.monotonic()
    columns = ['sources_digest', 'gmms_digest', 'values']
    arrow_scanner = ds.Scanner.from_dataset(dataset, filter=flt, columns=columns, use_threads=False)
    t1 = time.monotonic()

    rlz_table = arrow_scanner.to_table()
    t2 = time.monotonic()
    if len(rlz_table) == 0:
        raise Exception(
            f"no realizations were found in the database for {location=}, {imt=}, {vs30=}, {compatibility_key=}"
        )

    log.info("load scanner:%0.6f, to_arrow %0.6fs" % (t1 - t0, t2 - t1))
    log.info("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
    log.info("loaded %s realizations in arrow", rlz_table.shape[0])

    rlz_df = rlz_table.to_pandas()
    rlz_df['sources_digest'] = rlz_df['sources_digest'].astype(str)
    rlz_df['gmms_digest'] = rlz_df['gmms_digest'].astype(str)
    return rlz_df

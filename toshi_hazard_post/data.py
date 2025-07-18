"""
Functions for loading realizations and saving aggregations
"""

import logging
import time
from typing import TYPE_CHECKING, Optional

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


def get_batch_table(
    dataset: ds.Dataset,
    compatibility_key: str,
    sources_digests: list[str],
    gmms_digests: list[str],
    imts: list[str],
) -> pa.Table:
    t0 = time.perf_counter()
    columns = ['nloc_001', 'imt', 'sources_digest', 'gmms_digest', 'values']
    flt = (
        (pc.field('compatible_calc_id') == pc.scalar(compatibility_key))
        & (pc.is_in(pc.field('sources_digest'), pa.array(sources_digests)))
        & (pc.is_in(pc.field('gmms_digest'), pa.array(gmms_digests)))
        & (pc.is_in(pc.field('imt'), pa.array(imts)))
    )
    batch_datatable = dataset.to_table(columns=columns, filter=flt)
    t1 = time.perf_counter()
    log.debug("time to create batch table: %0.1f seconds" % (t1 - t0))
    return batch_datatable


def get_job_datatable(
    batch_datatable: pa.Table,
    location: 'CodedLocation',
    imt: str,
    n_expected: int,
) -> pa.Table:
    t0 = time.perf_counter()
    table = batch_datatable.filter((pc.field("imt") == imt) & (pc.field("nloc_001") == location.downsample(0.001).code))
    table = pa.table(
        {
            "sources_digest": table['sources_digest'].to_pylist(),
            "gmms_digest": table['gmms_digest'].to_pylist(),
            "values": table['values'],
        }
    )

    if len(table) == 0:
        raise KeyError(f"no records found for location: {location}, imt: {imt}")
    if len(table) != n_expected:
        msg = (
            f"incorrect number of records found for location: "
            f"{location}, imt: {imt}. Expected {n_expected}, got {len(table)}"
        )
        raise KeyError(msg)

    t1 = time.perf_counter()
    log.debug("time to create job table: %0.5f seconds" % (t1 - t0))
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


def get_realizations_dataset(vs30: Optional[int] = None, nloc_0: Optional[str] = None) -> ds.Dataset:
    """
    Get a pyarrow Dataset for realizations.

    Optional parameters take advantage of partitioning of dataset for faster retrieval.

    Parameters:
        vs30:
        component_branches: the branches to filter into the dataset
        compatibility_key: the hazard engine compatibility ley to filter into the dataset

    Returns:
        dataset: the dataset with the filteres applied
    """

    config = get_config()
    rlz_dir_tmp = config['RLZ_DIR']
    if vs30 is not None:
        rlz_dir_tmp += f"/vs30={vs30}"
        if nloc_0 is not None:
            rlz_dir_tmp += f"/nloc_0={nloc_0}"
    rlz_dir, filesystem = pyarrow_dataset.configure_output(rlz_dir_tmp)

    t0 = time.monotonic()
    dataset = ds.dataset(rlz_dir, format='parquet', filesystem=filesystem, partitioning='hive')
    t1 = time.monotonic()
    log.debug("time to get realizations dataset %0.6f" % (t1 - t0))

    return dataset

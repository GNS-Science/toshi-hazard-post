"""Functions for loading realizations and saving aggregations."""

import functools
import itertools
import logging
import posixpath
import time
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
from toshi_hazard_store.model.hazard_models_pydantic import HazardAggregateCurve
from toshi_hazard_store.model.pyarrow import pyarrow_aggr_dataset, pyarrow_dataset

from toshi_hazard_post.local_config import AGG_DIR, RLZ_DIR

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    import numpy.typing as npt
    from nzshm_common.location.coded_location import CodedLocation


@functools.cache
def _s3_filesystem() -> fs.S3FileSystem:
    # building an S3FileSystem can take seconds (e.g. when the AWS SDK probes for EC2 instance credentials) and
    # holds the GIL while it does, so build one per process rather than one per dataset
    return fs.S3FileSystem(region=pyarrow_dataset.REGION)


def resolve_target(target: str) -> tuple[str, fs.FileSystem]:
    """Get the path and filesystem of a dataset, as toshi-hazard-store's configure_output does.

    Every S3 target shares one S3FileSystem per process.

    Args:
        target: a local path or an S3 URI.

    Returns:
        The path within the filesystem, and the filesystem.
    """
    if target.startswith('s3://'):
        return posixpath.normpath(target.removeprefix('s3://')), _s3_filesystem()
    return str(Path(target).resolve()), fs.LocalFileSystem()


def get_batch_table(
    dataset: ds.Dataset,
    compatibility_key: str,
    sources_digests: list[str],
    gmms_digests: list[str],
    vs30: int,
    nloc_0: str,
    imts: list[str],
) -> pa.Table:
    """Get the realization datatable for a batch of aggregation jobs.

    Filtering is done for comatibility key, branch digests, vs30, nloc_0, and (multiple) imts.

    Args:
        dataset: the realization dataset.
        compatibility_key: the toshi-hazard-store compatibility key.
        sources_digests: the digests of the source branches.
        gmms_digests: the digests of the gmcm branches.
        vs30: the vs30 of the sites.
        nloc_0: the nloc_0 (1.0 degree location code).
        imts: the intensity measure types.

    Returns:
        The filtered datatable.
    """
    t0 = time.perf_counter()
    columns = ['nloc_001', 'imt', 'sources_digest', 'gmms_digest', 'values']
    flt = (
        (pc.field('compatible_calc_id') == pc.scalar(compatibility_key))
        & (pc.is_in(pc.field('sources_digest'), pa.array(sources_digests)))  # type: ignore[attr-defined]
        & (pc.is_in(pc.field('gmms_digest'), pa.array(gmms_digests)))  # type: ignore[attr-defined]
        & (pc.is_in(pc.field('imt'), pa.array(imts)))  # type: ignore[attr-defined]
    )

    # if we used the partitioning when fetching the dataset then vs30 and nloc_0 will not be in the
    # schema (we will have already implicitly filtered on them)
    dataset_columns = dataset.schema.names
    if 'vs30' in dataset_columns:
        flt = flt & (pc.field('vs30') == pc.scalar(vs30))
    if 'nloc_0' in dataset_columns:
        flt = flt & (pc.field('nloc_0') == pc.scalar(nloc_0))

    # the table is read in thousands of chunks, and taking the rows of a job from a chunked table is slow
    batch_datatable = dataset.to_table(columns=columns, filter=flt).combine_chunks()
    t1 = time.perf_counter()
    log.debug("time to create batch table: %0.1f seconds" % (t1 - t0))
    return batch_datatable


def _string_ids(column: pa.ChunkedArray) -> tuple['npt.NDArray', list[str]]:
    """Give each string of a column an integer id, and get the string of each id."""
    # chunks of a dictionary encoded column can each have their own dictionary, so encode the strings again
    if pa.types.is_dictionary(column.type):
        column = column.cast(pa.string())
    encoded = column.combine_chunks().dictionary_encode()
    return encoded.indices.to_numpy(), encoded.dictionary.to_pylist()


def group_batch_rows(batch_datatable: pa.Table) -> dict[tuple[str, str], 'npt.NDArray']:
    """Find the rows of the batch datatable for each aggregation job (one IMT and location).

    The whole table is grouped in one pass, so that the table of each job can be taken from it without
    searching the table again.

    Args:
        batch_datatable: the datatable produced by get_batch_table.

    Returns:
        The row indices of each (location code at 0.001 degree resolution, intensity measure type), in the
            order the rows have in the table.
    """
    if len(batch_datatable) == 0:
        return {}
    location_ids, locations = _string_ids(batch_datatable['nloc_001'])
    imt_ids, imts = _string_ids(batch_datatable['imt'])
    job_ids = location_ids.astype(np.int64) * len(imts) + imt_ids

    # a stable sort puts the rows of each job together and leaves them in table order
    order = np.argsort(job_ids, kind='stable')
    sorted_job_ids = job_ids[order]
    starts = np.concatenate(([0], np.flatnonzero(np.diff(sorted_job_ids)) + 1, [len(order)]))
    job_rows = {}
    for start, end in itertools.pairwise(starts):
        location_id, imt_id = divmod(int(sorted_job_ids[start]), len(imts))
        job_rows[(locations[location_id], imts[imt_id])] = order[start:end]
    return job_rows


def component_row_ids(batch_datatable: pa.Table, component_digests: Sequence[str]) -> 'npt.NDArray':
    """Find the component branch of each row of a batch datatable.

    A row's component branch digest is its sources digest followed by its gmms digest.

    Args:
        batch_datatable: the datatable produced by get_batch_table.
        component_digests: the hash digests of the component branches.

    Returns:
        For each row, the index into component_digests of the row's component branch, or -1 if the row is not
            for a component branch.
    """
    if len(batch_datatable) == 0:
        return np.empty(0, dtype=np.int64)
    source_ids, sources = _string_ids(batch_datatable['sources_digest'])
    gmm_ids, gmms = _string_ids(batch_datatable['gmms_digest'])
    # a batch has few distinct (source, gmm) pairs, so look up each pair once rather than each row
    pair_ids = source_ids.astype(np.int64) * len(gmms) + gmm_ids
    pairs, row_pairs = np.unique(pair_ids, return_inverse=True)
    index = {digest: i for i, digest in enumerate(component_digests)}
    pair_components = np.array(
        [index.get(sources[pair // len(gmms)] + gmms[pair % len(gmms)], -1) for pair in pairs.tolist()],
        dtype=np.int64,
    )
    return pair_components[row_pairs.reshape(-1)]


def get_job_probs(
    batch_datatable: pa.Table,
    job_rows: dict[tuple[str, str], 'npt.NDArray'],
    component_ids: 'npt.NDArray',
    component_digests: Sequence[str],
    location: 'CodedLocation',
    imt: str,
) -> 'npt.NDArray':
    """Get the realization probabilities of one aggregation job (one IMT and location), by component branch.

    Args:
        batch_datatable: the datatable produced by get_batch_table.
        job_rows: the rows of batch_datatable for each job, produced by group_batch_rows.
        component_ids: the component branch of each row of batch_datatable, produced by component_row_ids.
        component_digests: the hash digests of the component branches, in the order wanted for the array.
        location: the location of the site.
        imt: the intensity measure type.

    Returns:
        The probabilities with shape (n component branches, n IMTL), where row i is for component_digests[i].

    Raises:
        KeyError: if the job's rows are not exactly one for each component branch.
    """
    rows = job_rows.get((location.downsample(0.001).code, imt))
    if rows is None:
        raise KeyError(f"no records found for location: {location}, imt: {imt}")
    n_components = len(component_digests)
    if len(rows) != n_components:
        msg = (
            f"incorrect number of records found for location: "
            f"{location}, imt: {imt}. Expected {n_components}, got {len(rows)}"
        )
        raise KeyError(msg)

    ids = component_ids[rows]
    counts = np.bincount(ids[ids >= 0], minlength=n_components)
    if (counts != 1).any():
        missing = [component_digests[i] for i in np.flatnonzero(counts == 0).tolist()]
        # every task of a bad run fails the same way, so keep the message short
        n_unexpected = len(rows) - (n_components - len(missing))
        raise KeyError(
            f"expected rates for {n_components} component branches, got {len(rows)}: "
            f"{len(missing)} component branches have no rates (first {missing[:5]}), "
            f"{n_unexpected} rates are duplicates or not for a component branch"
        )
    ordered_rows = np.empty(n_components, dtype=np.int64)
    ordered_rows[ids] = rows
    values = batch_datatable['values'].take(ordered_rows).combine_chunks()
    return values.flatten().to_numpy().reshape(n_components, -1)


def get_job_datatable(
    batch_datatable: pa.Table,
    job_rows: dict[tuple[str, str], 'npt.NDArray'],
    location: 'CodedLocation',
    imt: str,
    n_expected: int,
) -> pa.Table:
    """Get the realization datatable for a specific aggregation job (one IMT, location, etc.).

    The batch_datatable is expected to be produced by get_batch_table which will have applied broader
    filters on the dataset for vs30, nloc_0, etc.

    Args:
        batch_datatable: the pre-filtered datatable to take the rows of a specific aggregation job from.
        job_rows: the rows of batch_datatable for each job, produced by group_batch_rows.
        location: the location of the site.
        imt: the intensity measure type.
        n_expected: the number of records expected (typically the number of branches).

    Returns:
        The data table of the job, with the digests as strings.

    Raises:
        KeyError: if the batch datatable does not have n_expected records for the job.
    """
    t0 = time.perf_counter()
    rows = job_rows.get((location.downsample(0.001).code, imt))
    if rows is None:
        raise KeyError(f"no records found for location: {location}, imt: {imt}")
    if len(rows) != n_expected:
        msg = (
            f"incorrect number of records found for location: "
            f"{location}, imt: {imt}. Expected {n_expected}, got {len(rows)}"
        )
        raise KeyError(msg)

    job_table = batch_datatable.select(['sources_digest', 'gmms_digest', 'values']).take(rows)
    # the workers need the digests as strings, not as dictionary encoded strings
    table = pa.table(
        {
            "sources_digest": job_table['sources_digest'].cast(pa.string()),
            "gmms_digest": job_table['gmms_digest'].cast(pa.string()),
            "values": job_table['values'],
        }
    )

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
    compatibility_key: str,
) -> None:
    """Save the aggregated hazard to the database.

    Converts hazard as rates to proabilities before saving.

    Args:
        hazard: the aggregate hazard rates (not proabilities)
        location: the site location
        vs30: the site vs30
        imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
        agg_types: the statistical aggregate types (e.g. "mean", "0.5")
        hazard_model_id: the model id for storing in the database
        compatibility_key: the toshi-hazard-store compatibility key.
    """

    def generate_models():
        for i, agg in enumerate(agg_types):
            yield HazardAggregateCurve(
                compatible_calc_id=compatibility_key,
                hazard_model_id=hazard_model_id,
                nloc_001=location.code,
                nloc_0=location.downsample(1.0).code,
                imt=imt,
                vs30=vs30,
                aggr=agg,
                values=hazard[i, :],
            )

    agg_dir, filesystem = resolve_target(str(AGG_DIR))
    partitioning = ['vs30', 'imt', 'nloc_001']
    pyarrow_aggr_dataset.append_models_to_dataset(
        models=generate_models(), base_dir=agg_dir, filesystem=filesystem, partitioning=partitioning
    )


def get_realizations_dataset(
    vs30: int | None = None, nloc_0: str | None = None, rlz_dir: str | Path | None = None
) -> ds.Dataset:
    """Get a pyarrow Dataset for realizations.

    Optional parameters take advantage of partitioning of dataset for faster retrieval. The partitioning is
    assumed to be vs30/nloc_0. See toshi-hazard-store documentation for details.

    Args:
        vs30: the site vs30
        nloc_0: the 1 degree grid location (e.g. '-41.0~175.0')
        rlz_dir: location of realization dataset. If not passed, function will use env var.

    Returns:
        dataset: the relization dataset
    """
    rlz_dir_tmp = str(RLZ_DIR) if rlz_dir is None else str(rlz_dir)
    if vs30 is not None:
        rlz_dir_tmp += f"/vs30={vs30}"
        if nloc_0 is not None:
            rlz_dir_tmp += f"/nloc_0={nloc_0}"
    rlz_dir, filesystem = resolve_target(rlz_dir_tmp)

    t0 = time.monotonic()
    dataset = ds.dataset(rlz_dir, format='parquet', filesystem=filesystem, partitioning='hive')
    t1 = time.monotonic()
    log.debug("time to get realizations dataset %0.6f" % (t1 - t0))

    return dataset

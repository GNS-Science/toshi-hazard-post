"""Module for coordinating and launching aggregation jobs."""

import itertools
import logging
import time
from collections.abc import Callable, Generator, Iterable
from concurrent.futures import FIRST_COMPLETED, Executor, Future, ProcessPoolExecutor, wait
from multiprocessing import shared_memory
from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa
from nzshm_common.location.coded_location import bin_locations

import toshi_hazard_post.calculators as calculators
import toshi_hazard_post.constants as constants
from toshi_hazard_post.aggregation_args import AggregationArgs
from toshi_hazard_post.aggregation_calc import AggSharedArgs, AggTaskArgs, calc_aggregation
from toshi_hazard_post.aggregation_setup import Site, get_logic_trees, get_sites
from toshi_hazard_post.data import (
    component_row_ids,
    get_batch_table,
    get_job_probs,
    get_realizations_dataset,
    group_batch_rows,
)
from toshi_hazard_post.local_config import NUM_WORKERS
from toshi_hazard_post.logic_tree import HazardLogicTree, build_branch_index_table

if TYPE_CHECKING:
    import numpy.typing as npt

    from toshi_hazard_post.logic_tree import HazardComponentBranch


log = logging.getLogger(__name__)

PARTITION_RESOLUTION = 1.0


def _load_batch(
    vs30: int,
    nloc_0: str,
    compatibility_key: str,
    sources_digests: list[str],
    gmms_digests: list[str],
    imts: list[str],
    component_digests: list[str],
) -> tuple[pa.Table, dict[tuple[str, str], 'npt.NDArray'], 'npt.NDArray']:
    """Load the realizations of one vs30 and 1 degree location bin, and find the rows and component branches."""
    log.debug("loading batch %d, %s", vs30, nloc_0)
    t0 = time.perf_counter()
    dataset = get_realizations_dataset(vs30, nloc_0)
    batch_datatable = get_batch_table(dataset, compatibility_key, sources_digests, gmms_digests, vs30, nloc_0, imts)
    job_rows = group_batch_rows(batch_datatable)
    component_ids = component_row_ids(batch_datatable, component_digests)
    log.debug("loaded batch %d, %s in %0.1f seconds", vs30, nloc_0, time.perf_counter() - t0)
    return batch_datatable, job_rows, component_ids


def _generate_agg_jobs(
    sites: list[Site],
    imts: list[str],
    compatibility_key: str,
    component_branches: list['HazardComponentBranch'],
) -> Generator[AggTaskArgs, None, None]:

    gmms_digests = [branch.gmcm_hash_digest for branch in component_branches]
    sources_digests = [branch.source_hash_digest for branch in component_branches]
    # the order of the branch index table, which the workers use to find the rates of each component branch
    component_digests = [branch.hash_digest for branch in component_branches]

    # group locations by vs30
    vs30s_unique = set([site.vs30 for site in sites])

    log.info("creating batches from %d sites and %d vs30s", len(sites), len(vs30s_unique))
    batches = []
    for vs30 in vs30s_unique:
        locations = [site.location for site in sites if site.vs30 == vs30]
        location_bins = bin_locations(locations, PARTITION_RESOLUTION)
        batches += [(vs30, nloc_0, location_bin) for nloc_0, location_bin in location_bins.items()]

    for vs30, nloc_0, location_bin in batches:
        batch_datatable, job_rows, component_ids = _load_batch(
            vs30, nloc_0, compatibility_key, sources_digests, gmms_digests, imts, component_digests
        )
        log.info("batch %d, %s", vs30, nloc_0)
        for location, imt in itertools.product(location_bin.locations, imts):
            probs = get_job_probs(batch_datatable, job_rows, component_ids, component_digests, location, imt)
            yield AggTaskArgs(location=location, vs30=vs30, imt=imt, probs=probs)
        # drop this batch before the next is loaded, so that only one is held at a time
        del batch_datatable, job_rows, component_ids


def _run_tasks(
    executor: Executor,
    fn: Callable[[AggTaskArgs, AggSharedArgs], None],
    tasks: Iterable[AggTaskArgs],
    shared_args: AggSharedArgs,
    max_in_flight: int,
) -> tuple[int, int]:
    """Run fn on every task, with no more than max_in_flight tasks submitted and not yet finished.

    Each submitted task holds its data in this process until it runs, so the cap bounds that memory, and also
    stops the tasks being generated (and their batches loaded) far ahead of the workers.

    Returns:
        The number of tasks and the number that failed.
    """
    num_jobs = 0
    num_failed = 0
    pending: dict[Future, AggTaskArgs] = {}

    def collect(done: set[Future]) -> None:
        nonlocal num_failed
        for future in done:
            task_args = pending.pop(future)
            if exception := future.exception():
                num_failed += 1
                log.error("Exception encountered for task args %s: %r", task_args, exception)

    for task_args in tasks:
        if len(pending) >= max_in_flight:
            collect(wait(pending, return_when=FIRST_COMPLETED).done)
        pending[executor.submit(fn, task_args, shared_args)] = task_args
        num_jobs += 1
    collect(wait(pending).done)
    return num_jobs, num_failed


def run_aggregation(args: AggregationArgs, pool_executor: Executor | None = None) -> None:
    """Main entry point for running aggregation caculations.

    Args:
        args: the aggregation configuration
        pool_executor: the Executor (e.g. ProcessPoolExecutor) to use for launching the parallel tasks. If
            none given, ProcessPoolExecutor(max_workers=NUM_WORKERS) is used
    """
    time0 = time.perf_counter()
    # get the sites
    log.info("getting sites . . .")
    sites = get_sites(args.site_params.locations_file, args.site_params.locations, args.site_params.vs30s)
    srm_logic_tree, gmcm_logic_tree = get_logic_trees(
        args.hazard_model.nshm_model_version,
        args.hazard_model.srm_logic_tree,
        args.hazard_model.gmcm_logic_tree,
    )

    # create the logic tree objects and build the full logic tree
    log.info("getting logic trees . . . ")
    logic_tree = HazardLogicTree(srm_logic_tree, gmcm_logic_tree)
    component_branches = logic_tree.component_branches

    if not args.debug.restart:
        log.info("calculating weights and branch hash table . . . ")
        tic = time.perf_counter()
        weights = logic_tree.weights
        branch_hash_table: list | npt.NDArray = logic_tree.branch_hash_table
        toc = time.perf_counter()

        log.info('time to build weight array and hash table %0.2f seconds' % (toc - tic))
    else:
        branch_hash_table = np.load(args.debug.restart[0])
        weights = np.load(args.debug.restart[1])
        if weights.shape != (len(branch_hash_table),):
            raise ValueError(
                f"restart files do not match: {args.debug.restart[0]} has {len(branch_hash_table)} composite branches "
                f"and {args.debug.restart[1]} has weights with shape {weights.shape}"
            )

    agg_types = [a.value for a in args.calculation.agg_types]
    imts = [i.value for i in args.calculation.imts]

    # workers look up component rates by position, so replace each digest with its index in component_digests
    component_digests = logic_tree.component_digests
    branch_index_table = build_branch_index_table(branch_hash_table, component_digests)
    del branch_hash_table
    log.info("Size of weight array: %dMB", weights.nbytes >> 20)
    log.info("Size of branch index table: %dMB", branch_index_table.nbytes >> 20)

    weights_shm = shared_memory.SharedMemory(name=constants.WEIGHTS_SHM_NAME, create=True, size=weights.nbytes)
    branch_index_table_shm = shared_memory.SharedMemory(
        name=constants.BRANCH_INDEX_TABLE_SHM_NAME, create=True, size=branch_index_table.nbytes
    )

    bit: npt.NDArray = np.ndarray(
        branch_index_table.shape, dtype=branch_index_table.dtype, buffer=branch_index_table_shm.buf
    )
    bit[:] = branch_index_table[:]
    wgt: npt.NDArray = np.ndarray(weights.shape, dtype=weights.dtype, buffer=weights_shm.buf)
    wgt[:] = weights[:]

    shared_args = AggSharedArgs(
        weights_shape=weights.shape,
        branch_index_table_shape=branch_index_table.shape,
        branch_index_table_dtype=branch_index_table.dtype.str,
        agg_types=agg_types,
        hazard_model_id=args.general.hazard_model_id,
        compatibility_key=args.general.compatibility_key,
        skip_save=args.debug.skip_save,
    )

    time_parallel_start = time.perf_counter()
    log.info("starting %d calculations with %d workers", len(sites) * len(imts), NUM_WORKERS)

    # compile before the workers are started so that they do not each compile for their first task
    calculators.warm_up()
    pool_executor = pool_executor or ProcessPoolExecutor(max_workers=NUM_WORKERS)
    with pool_executor as executor:
        tasks = _generate_agg_jobs(sites, imts, args.general.compatibility_key, component_branches)
        num_jobs, num_failed = _run_tasks(executor, calc_aggregation, tasks, shared_args, 2 * NUM_WORKERS)

    time_parallel_end = time.perf_counter()
    branch_index_table_shm.close()
    branch_index_table_shm.unlink()
    weights_shm.close()
    weights_shm.unlink()

    time1 = time.perf_counter()
    log.info("total time: processed %d calculations in %0.3f seconds", num_jobs, time1 - time0)
    log.info("time to perform aggregations after job setup %0.3f", time_parallel_end - time_parallel_start)

    print(f"THERE ARE {num_failed} FAILED JOBS . . . ")

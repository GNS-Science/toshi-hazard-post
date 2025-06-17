"""
Module for coordinating and launching aggregation jobs.
"""

from multiprocessing import shared_memory
import numpy as np
import itertools
import logging
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Generator

import psutil
import pyarrow.orc as orc
from nzshm_common.location import get_locations
from nzshm_common.location.coded_location import bin_locations

from toshi_hazard_post.aggregation_args import AggregationArgs
from toshi_hazard_post.aggregation_calc import AggSharedArgs, AggTaskArgs, calc_aggregation
from toshi_hazard_post.aggregation_setup import Site, get_logic_trees, get_sites
from toshi_hazard_post.data import get_batch_table, get_job_datatable, get_realizations_dataset
from toshi_hazard_post.local_config import get_config
from toshi_hazard_post.logic_tree import HazardLogicTree
import toshi_hazard_post.constants as constants

if TYPE_CHECKING:
    from nzshm_common.location import CodedLocation
    import pyarrow.dataset as ds

    from toshi_hazard_post.logic_tree import HazardComponentBranch


log = logging.getLogger(__name__)

PARTITION_RESOLUTION = 1.0



# TODO:
# - [ ] hanlde locations file
# - [x] check that the correct number of records are returned
# - [x] add timing and better memeory log messages
# - [x] review all log messages, enusre correct level and placement
# - [x] make sure resultion is correct when a coded location is used
# - [x] organize into correct modules
# - [ ] test coverage
# - [x] WORKING dir can be set

process = psutil.Process()


def log_memory(state):
    log.debug('memory use at state "%s": %d MB' % (state, process.memory_info().rss / 1024**2))


def generate_agg_jobs(
    sites: list[Site],
    imts: list[str],
    compatibility_key: str,
    component_branches: list['HazardComponentBranch'],
    dataset: 'ds.Dataset',
) -> Generator[tuple[int, 'CodedLocation', str, Path], None, None]:
    gmms_digests = [branch.gmcm_hash_digest for branch in component_branches]
    sources_digests = [branch.source_hash_digest for branch in component_branches]
    n_expected = len(component_branches)

    # group locations by vs30
    vs30s_unique = set([site.vs30 for site in sites])

    log.info(
        "creating batches from %s sites and %s vs30s"
        % (len(sites), len(vs30s_unique))
    )
    log_memory("start generate")
    for vs30 in vs30s_unique:
        locations = [site.location for site in sites if site.vs30 == vs30]
        location_bins = bin_locations(locations, PARTITION_RESOLUTION)
        for nloc_0, location_bin in location_bins.items():
            log.info("batch %d, %s" % (vs30, nloc_0))
            log_memory("got dataset")
            batch_datatable = get_batch_table(
                dataset, compatibility_key, sources_digests, gmms_digests, nloc_0, vs30, imts
            )
            log_memory("batch datatable")

            for location, imt in itertools.product(location_bin.locations, imts):
                job_datatable = get_job_datatable(batch_datatable, location, imt, n_expected)
                log_memory("job datatable")
                working_dir = get_config()['WORKING_DIR']
                filepath = working_dir / f"{vs30}_{nloc_0}_{location.downsample(0.001).code}_{imt}_dataset.dat"
                log.debug("writing file %s for agg job %s, %s" % (filepath, location.code, imt))
                t0 = time.perf_counter()
                orc.write_table(job_datatable, filepath, compression='snappy')
                t1 = time.perf_counter()
                log.debug("time to write data: %0.5f seconds" % (t1 - t0))
                yield vs30, location, imt, filepath


def run_aggregation(args: AggregationArgs) -> None:
    """
    Main entry point for running aggregation caculations.

    Parameters:
        config: the aggregation configuration
    """
    num_workers = get_config()['NUM_WORKERS']

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

    log.info("calculating weights and branch hash table . . . ")
    tic = time.perf_counter()
    weights = logic_tree.weights
    branch_hash_table = logic_tree.branch_hash_table
    toc = time.perf_counter()

    log.info('time to build weight array and hash table %0.2f seconds' % (toc - tic))
    log.info("Size of weight array: {}MB".format(weights.nbytes >> 20))
    log.info("Size of hash table: {}MB".format(sys.getsizeof(branch_hash_table) >> 20))

    component_branches = logic_tree.component_branches

    # TODO: this is not true
    assert args.calculation.agg_types is not None  # guarnteed to not be none by Pydantic validation function
    agg_types = [a.value for a in args.calculation.agg_types]

    assert args.calculation.imts is not None  # guarnteed to not be none by Pydantic validation function
    imts = [i.value for i in args.calculation.imts]

    weights_shm = shared_memory.SharedMemory(name=constants.WEIGHTS_SHM_NAME, create=True, size=weights.nbytes)
    branch_hash_table = np.array(branch_hash_table)
    branch_hash_table_shm = shared_memory.SharedMemory(name=constants.BRANCH_HASH_TABLE_SHM_NAME, create=True, size=branch_hash_table.nbytes)

    bht = np.ndarray(branch_hash_table.shape, dtype=branch_hash_table.dtype, buffer=branch_hash_table_shm.buf)
    bht[:] = branch_hash_table[:]
    wgt = np.ndarray(weights.shape, dtype=weights.dtype, buffer=weights_shm.buf)
    wgt[:] = weights[:]

    shared_args = AggSharedArgs(
        weights_shape=weights.shape,
        branch_hash_table_shape=branch_hash_table.shape,
        agg_types=agg_types,
        hazard_model_id=args.general.hazard_model_id,
        compatibility_key=args.general.compatibility_key,
        skip_save=args.debug.skip_save,
    )

    time_parallel_start = time.perf_counter()
    num_jobs = 0
    log.info("starting %d calculations with %d workers" % (len(sites) * len(imts), num_workers))

    futures = {}
    ds1 = get_realizations_dataset()
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # TODO: add locations file
        for vs30, location, imt, filepath in generate_agg_jobs(
            sites,
            imts,
            args.general.compatibility_key,
            component_branches,
            ds1,
        ):
            task_args = AggTaskArgs(
                location=location,
                vs30=vs30,
                imt=imt,
                table_filepath=filepath,
            )
            future = executor.submit(calc_aggregation, task_args, shared_args)
            futures[future] = task_args
            num_jobs += 1

        num_failed = 0
        for future in as_completed(futures.keys()):
            if exception := future.exception():
                num_failed += 1
                log.error("Exception encountered for task args %s: %s" % (futures[future], repr(exception)))

    time_parallel_end = time.perf_counter()
    branch_hash_table_shm.close()
    branch_hash_table_shm.unlink()
    weights_shm.close()
    weights_shm.unlink()

    time1 = time.perf_counter()
    log.info("total time: processed %d calculations in %0.3f seconds" % (num_jobs, time1 - time0))
    log.info("time to perform aggregations after job setup %0.3f" % (time_parallel_end - time_parallel_start))

    print(f"THERE ARE {num_failed} FAILED JOBS . . . ")

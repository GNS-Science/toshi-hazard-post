"""
Module for coordinating and launching aggregation jobs.
"""

import logging
from pathlib import Path
import itertools
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import TYPE_CHECKING, Generator, List, Tuple

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.orc as orc
import pyarrow.compute as pc
import itertools
import datetime as dt

from nzshm_common.location.coded_location import bin_locations
from nzshm_common.location import get_locations

from toshi_hazard_post.aggregation_args import AggregationArgs
from toshi_hazard_post.aggregation_calc import AggSharedArgs, AggTaskArgs, calc_aggregation
from toshi_hazard_post.aggregation_setup import Site, get_logic_trees, get_sites
from toshi_hazard_post.local_config import get_config
from toshi_hazard_post.logic_tree import HazardLogicTree
from toshi_hazard_post.data import get_realizations_dataset

if TYPE_CHECKING:
    from nzshm_common.location import CodedLocation, CodedLocationBin
    from toshi_hazard_post.logic_tree import HazardComponentBranch


log = logging.getLogger(__name__)

PARTITION_RESOLUTION = 1.0
import psutil
process = psutil.Process()

WORKING = Path("/tmp")

def log_memory(state):
    log.info('memory use at state "%s": %d MB' % (state, process.memory_info().rss / 1024 ** 2))

# TODO:
# - [ ] hanlde locations file
# - [x] check that the correct number of records are returned
# - [x] add timing and better memeory log messages
# - [ ] review all log messages, enusre correct level and placement
# - [ ] make sure resultion is correct when a coded location is used
# - [ ] organize into correct modules
def generate_agg_jobs(
    locations: list['CodedLocation'],
    vs30s: list[int],
    imts: list[str],
    compatibility_key: str,
    component_branches: list['HazardComponentBranch'],
    dataset: ds.Dataset,
):
    gmms_digests = [branch.gmcm_hash_digest for branch in component_branches]
    sources_digests = [branch.source_hash_digest for branch in component_branches]
    location_bins = bin_locations(locations, PARTITION_RESOLUTION)
    log.info("creating %d batches from %d vs30s and %d location bins" % (len(location_bins) * len(vs30s), len(vs30s), len(location_bins)))
    log_memory("start generate")
    for vs30 in vs30s:
        for nloc_0, location_bin in location_bins.items():
            log.info("batch %d, %s" % (vs30, nloc_0))
            log_memory("got dataset")
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
            dt1 = dataset.to_table(columns=columns, filter=flt)
            t1 = time.perf_counter()
            log.info("time to create data table 1: %0.1f seconds" % (t1-t0))
            log_memory("table 1")

            for (location, imt) in itertools.product(location_bin.locations, imts):
                t2 = time.perf_counter()
                dt2 = dt1.filter((pc.field("imt")==imt) & (pc.field("nloc_001") == location.downsample(0.001).code))
                t3 = time.perf_counter()
                log_memory("table 2")
                log.info("time to create data table 2: %0.5f seconds" % (t3-t2))
                t4 = time.perf_counter()
                table = pa.table({
                    "sources_digest": dt2['sources_digest'].to_pylist(),
                    "gmms_digest": dt2['gmms_digest'].to_pylist(),
                    "values":  dt2['values']
                })

                if len(table) == 0:
                    raise Exception(f"no records found for location: {location}, imt: {imt}")
                if len(table) != len(component_branches):
                    msg = f"incorrect number of records found for location: {location}, imt: {imt}. Expected {len(component_branches)}, got {len(table)}"
                    raise Exception(msg)

                t5 = time.perf_counter()
                log_memory("final table")
                log.info("time to create final data table: %0.5f seconds" % (t5-t4))
                filepath = WORKING / f"{vs30}_{nloc_0}_{location.downsample(0.001).code}_{imt}_dataset.dat"
                log.info("writing file %s for agg job %s, %s" % (filepath, location.code, imt))
                t6 = time.perf_counter()
                orc.write_table(table, filepath, compression='snappy')
                t7 = time.perf_counter()
                log.info("time to write data: %0.5f seconds" % (t7-t6))
                yield vs30, location, imt, filepath




# class TaskGenerator:
#     def __init__(
#         self,
#         sites: List[Site],
#         imts: List[str],
#     ):
#         self.imts = imts
#         self.locations = [site.location for site in sites]
#         self.vs30s = [site.vs30 for site in sites]

#     def task_generator(self) -> Generator[Tuple[Site, str, 'CodedLocationBin'], None, None]:
#         for imt in self.imts:
#             locations_tmp = self.locations.copy()
#             vs30s_tmp = self.vs30s.copy()
#             for location_bin in bin_locations(self.locations, PARTITION_RESOLUTION).values():
#                 for location in location_bin:
#                     idx = locations_tmp.index(location)
#                     locations_tmp.pop(idx)
#                     vs30 = vs30s_tmp.pop(idx)
#                     site = Site(location=location, vs30=vs30)
#                     yield site, imt, location_bin


def run_aggregation(args: AggregationArgs) -> None:
    """
    Main entry point for running aggregation caculations.

    Parameters:
        config: the aggregation configuration
    """
    num_workers = get_config()['NUM_WORKERS']
    delay_multiplier = get_config()['DELAY_MULTIPLIER']

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

    assert args.calculation.agg_types is not None  # guarnteed to not be none by Pydantic validation function
    agg_types = [a.value for a in args.calculation.agg_types]

    assert args.calculation.imts is not None  # guarnteed to not be none by Pydantic validation function
    imts = [i.value for i in args.calculation.imts]

    shared_args = AggSharedArgs(
        weights=weights,
        branch_hash_table=branch_hash_table,
        component_branches=component_branches,
        agg_types=agg_types,
        hazard_model_id=args.general.hazard_model_id,
        compatibility_key=args.general.compatibility_key,
        skip_save=args.debug.skip_save,
    )

    time_parallel_start = time.perf_counter()
    # task_generator = TaskGenerator(sites, imts)
    num_jobs = 0
    delay_width = 10
    log.info("starting %d calculations with %d workers" % (len(sites) * len(imts), num_workers))

    futures = {}
    locations = get_locations(args.site_params.locations)
    ds1 = get_realizations_dataset()
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # TODO: add locations file
        for vs30, location, imt, filepath in generate_agg_jobs(
            locations,
            args.site_params.vs30s,
            imts,
            args.general.compatibility_key,
            component_branches,
            ds1,
        ):
            site = Site(location=location, vs30=vs30)
            task_args = AggTaskArgs(
                site=site,
                imt=imt,
                filepath=filepath,
            )
            future = executor.submit(calc_aggregation, task_args, shared_args)
            futures[future] = task_args
            num_jobs += 1

        num_failed = 0
        for future in as_completed(futures.keys()):
            if exception := future.exception():
                num_failed += 1
                log.error("Exception encountered for task args %s: %s" % (futures[future], repr(exception)))

    # futures = {}
    # with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # for site, imt, location_bin in task_generator.task_generator():
    #         if num_workers > 1:
    #             delay = (num_jobs % delay_width) * delay_multiplier
    #         else:
    #             delay = 0
    #         task_args = AggTaskArgs(
    #             site=site,
    #             imt=imt,
    #             delay=delay,
    #         )
    #         num_jobs += 1
    #         future = executor.submit(calc_aggregation, task_args, shared_args)
    #         futures[future] = task_args
    #     total_jobs = num_jobs

    #     num_failed = 0
    #     for future in as_completed(futures.keys()):
    #         if exception := future.exception():
    #             num_failed += 1
    #             print(f"Exception encountered for task args {futures[future]}: {repr(exception)}")

    time_parallel_end = time.perf_counter()

    time1 = time.perf_counter()
    log.info("time to perform parallel tasks %0.3f" % (time_parallel_end - time_parallel_start))
    log.info("processed %d calculations in %0.3f seconds" % (num_jobs, time1 - time0))

    print(f"THERE ARE {num_failed} FAILED JOBS . . . ")

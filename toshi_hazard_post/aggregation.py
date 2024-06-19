"""
Module for coordinating and launching aggregation jobs.
"""
import logging
import sys
import time
from typing import TYPE_CHECKING, Generator, List, Tuple, Union

from nzshm_common.location.coded_location import bin_locations

from toshi_hazard_post.aggregation_args import AggregationArgs
from toshi_hazard_post.aggregation_calc import AggSharedArgs, AggTaskArgs, calc_aggregation
from toshi_hazard_post.aggregation_setup import Site, get_sites
from toshi_hazard_post.local_config import get_config
from toshi_hazard_post.logic_tree import HazardLogicTree
from toshi_hazard_post.parallel import setup_parallel

if TYPE_CHECKING:
    import multiprocessing
    import queue

    from nzshm_common.location.coded_location import CodedLocationBin

log = logging.getLogger(__name__)

PARTITION_RESOLUTION = 1.0


class TaskGenerator:
    def __init__(
        self,
        sites: List[Site],
        imts: List[str],
    ):
        self.imts = imts
        self.locations = [site.location for site in sites]
        self.vs30s = [site.vs30 for site in sites]

    def task_generator(self) -> Generator[Tuple[Site, str, 'CodedLocationBin'], None, None]:
        for imt in self.imts:
            locations_tmp = self.locations.copy()
            vs30s_tmp = self.vs30s.copy()
            for location_bin in bin_locations(self.locations, PARTITION_RESOLUTION).values():
                for location in location_bin:
                    idx = locations_tmp.index(location)
                    locations_tmp.pop(idx)
                    vs30 = vs30s_tmp.pop(idx)
                    site = Site(location=location, vs30=vs30)
                    yield site, imt, location_bin


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
    sites = get_sites(args.locations, args.vs30s)

    # create the logic tree objects and build the full logic tree
    log.info("getting logic trees . . . ")
    logic_tree = HazardLogicTree(args.srm_logic_tree, args.gmcm_logic_tree)

    log.info("calculating weights and branch hash table . . . ")
    tic = time.perf_counter()
    weights = logic_tree.weights
    branch_hash_table = logic_tree.branch_hash_table
    toc = time.perf_counter()

    log.info('time to build weight array and hash table %0.2f seconds' % (toc - tic))
    log.info("Size of weight array: {}MB".format(weights.nbytes >> 20))
    log.info("Size of hash table: {}MB".format(sys.getsizeof(branch_hash_table) >> 20))

    component_branches = logic_tree.component_branches

    shared_args = AggSharedArgs(
        weights=weights,
        branch_hash_table=branch_hash_table,
        component_branches=component_branches,
        agg_types=args.agg_types,
        hazard_model_id=args.hazard_model_id,
        compatibility_key=args.compat_key,
    )

    task_queue: Union['queue.Queue', 'multiprocessing.JoinableQueue']
    result_queue: Union['queue.Queue', 'multiprocessing.Queue']
    task_queue, result_queue = setup_parallel(num_workers, calc_aggregation, shared_args)

    time_parallel_start = time.perf_counter()
    task_generator = TaskGenerator(sites, args.imts)
    num_jobs = 0
    log.info("starting %d calculations" % (len(sites) * len(args.imts)))
    for site, imt, location_bin in task_generator.task_generator():
        task_args = AggTaskArgs(
            location_bin=location_bin,
            site=site,
            imt=imt,
        )
        task_queue.put(task_args)
        num_jobs += 1
    total_jobs = num_jobs

    # Add a poison pill for each to signal we've done everything
    for i in range(num_workers):
        task_queue.put(None)

    # Wait for all of the tasks to finish
    task_queue.join()
    time_parallel_end = time.perf_counter()

    # TODO: catch exceptions and report trace
    results: List[str] = []
    while num_jobs:
        result = result_queue.get()
        results.append(result)
        num_jobs -= 1

    time1 = time.perf_counter()
    log.info("time to perform parallel tasks %0.3f" % (time_parallel_end - time_parallel_start))
    log.info("processed %d calculations in %0.3f seconds" % (total_jobs, time1 - time0))

    n_failed = len(list(filter(lambda s: 'FAILED' in s, results)))
    if n_failed:
        print("")
        print(f"THERE ARE {n_failed} FAILED JOBS . . . ")
        for result in results:
            if 'FAILED' in result:
                print(result)


# if __name__ == "__main__":
#     config_filepath = "tests/version2/fixtures/hazard.toml"
#     config = AggregationConfig(config_filepath)
#     run_aggregation(config)
#     print()
#     print()
#     print()
#     run_aggregation_arrow(config)

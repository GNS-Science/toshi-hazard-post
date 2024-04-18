import logging
import time

from toshi_hazard_post.version2.aggregation_calc import calc_aggregation
from toshi_hazard_post.version2.aggregation_config import AggregationConfig
from toshi_hazard_post.version2.aggregation_setup import get_levels, get_lts, get_sites
from toshi_hazard_post.version2.logic_tree import HazardLogicTree

log = logging.getLogger(__name__)


############
# ORIGINAL
############
def run_aggregation(config: AggregationConfig) -> None:
    """
    Main entry point for running aggregation caculations.

    Parameters:
        config: the aggregation configuration
    """

    # get the sites
    log.info("getting sites . . .")
    sites = get_sites(config.locations, config.vs30s)

    # create the logic tree objects and build the full logic tree
    # TODO: pre-calculating the logic tree will require serialization if dsitributing in cloud. However,
    # the object cannot be serialized due to use of FilteredBranch
    log.info("getting logic trees . . . ")
    srm_lt, gmcm_lt = get_lts(config)
    log.info("building hazard logic tree . . .")
    logic_tree = HazardLogicTree(srm_lt, gmcm_lt)

    # for each independent thing (location, imt, vs30) (do we want to allow looping over vs30s?)
    # each of these can be placed in a multiprocessing queue
    log.info("original method")
    log.info("starting aggregation for %s sites and %s imts . . . ", len(sites), len(config.imts))
    og_0 = time.perf_counter()

    # get the weights (this code could be moved to nzshm-model)
    # TODO: this could be done in calc_aggregation() which would avoid passing the weights array when running in
    # parrallel, however, it may be slow? determine speed and decide
    log.info("calculating weights . . . ")
    tic = time.perf_counter()
    weights = logic_tree.weights
    toc = time.perf_counter()
    log.info(f'time to calculate weights {toc-tic:.2f} seconds')

    print(len(weights))

    # get the levels for the compatibility
    log.info("getting levels . . .")
    levels = get_levels(config.compat_key)

    for site in sites:
        for imt in config.imts:

            log.info("site: %s, imt: %s", site, imt)
            tic = time.perf_counter()
            exception = calc_aggregation(
                site, imt, config.agg_types, levels, weights, logic_tree, config.compat_key, config.hazard_model_id
            )
            toc = time.perf_counter()
            log.info(f'time to perform aggregation for one location-imt pair {toc-tic:.2f} seconds')
            if exception:
                raise exception

    og_1 = time.perf_counter()
    log.info(f"total OG time: {round(og_1 - og_0, 6)}")


if __name__ == "__main__":

    config_filepath = "tests/version2/fixtures/hazard.toml"
    config = AggregationConfig(config_filepath)
    run_aggregation(config)

import logging
import time

import pyarrow as pa

from toshi_hazard_post.version2.aggregation_calc_arrow import calc_aggregation_arrow
from toshi_hazard_post.version2.aggregation_config import AggregationConfig
from toshi_hazard_post.version2.aggregation_setup import get_lts, get_sites  # , get_levels
from toshi_hazard_post.version2.logic_tree import HazardLogicTree

log = logging.getLogger(__name__)


############
# ARROW
############
def run_aggregation_arrow(config: AggregationConfig) -> None:
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

    log.info("arrow method")
    arrow_0 = time.perf_counter()

    ## CBC weight table
    tic = time.perf_counter()
    # for i, cb in enumerate( logic_tree.weight_table):
    #     pass
    # print(i, cb)
    weight_table = logic_tree.weight_table()
    toc = time.perf_counter()
    log.info(f'time to build weight table {toc-tic:.2f} seconds')
    # print(table)
    log.debug(weight_table.shape)
    log.debug(weight_table.to_pandas())
    log.info("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))

    for site in sites:

        log.info("site: %s, imts: %s", site, config.imts)
        tic = time.perf_counter()

        calc_aggregation_arrow(
            site=site,
            imts=config.imts,
            agg_types=config.agg_types,
            weights=weight_table,
            logic_tree=logic_tree,
            compatibility_key=config.compat_key,
            hazard_model_id=config.hazard_model_id,
        )

        toc = time.perf_counter()
        log.info(f'time to perform aggregation for one location, {len(config.imts)} imts: {toc-tic:.2f} seconds')

    arrow_1 = time.perf_counter()
    log.info(f"total arrow time: {round(arrow_1 - arrow_0, 3)}")


# if __name__ == "__main__":
#     config_filepath = "tests/version2/fixtures/hazard.toml"
#     config = AggregationConfig(config_filepath)
#     run_aggregation(config)
#     print()
#     print()
#     print()
#     run_aggregation_arrow(config)

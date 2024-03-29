"""Handler for deaggregation task."""
import argparse
import json
import logging
import logging.config
import time

from toshi_hazard_post.hazard_grid.gridded_hazard import DistributedGridTaskArguments, calc_gridded_hazard
from toshi_hazard_post.local_config import API_KEY, NUM_WORKERS
from toshi_hazard_post.util import decompress_config

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger('toshi_hazard_store').setLevel(logging.ERROR)
logging.getLogger('toshi_hazard_post').setLevel(logging.INFO)


def process_args(args: DistributedGridTaskArguments) -> None:

    log.info("args: %s" % args)
    log.debug("using API_KEY with len: %s" % len(API_KEY))

    results = calc_gridded_hazard(
        location_grid_id=args.location_grid_id,
        poe_levels=args.poe_levels,
        hazard_model_ids=args.hazard_model_ids,
        vs30s=args.vs30s,
        imts=args.imts,
        aggs=args.aggs,
        num_workers=NUM_WORKERS,
        force=args.force,
        filter_locations=args.filter_locations,
        iter_method='zip',
    )

    log.info(results)


# _ __ ___   __ _(_)_ __
#  | '_ ` _ \ / _` | | '_ \
#  | | | | | | (_| | | | | |
#  |_| |_| |_|\__,_|_|_| |_|
#
# This is run in the BATCH environment by ./script/container_task.sh.
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    args = parser.parse_args()

    try:
        # LOCAL and CLUSTER this is a file
        config_file = args.config
        f = open(args.config, 'r', encoding='utf-8')
        config = json.load(f)
    except Exception:
        # for AWS this must now be a compressed JSON string
        config = json.loads(decompress_config(args.config))

    # Wait for some more time, scaled by taskid to avoid S3 consistency issue
    time.sleep(config['job_arguments']['task_id'])

    process_args(args=DistributedGridTaskArguments(**config['task_arguments']))

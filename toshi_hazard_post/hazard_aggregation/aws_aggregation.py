"""Hazard aggregation task dispatch."""
import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

import boto3
from nzshm_common.location.code_location import CodedLocation

import toshi_hazard_post.hazard_aggregation.aggregation_task
from toshi_hazard_post.data_functions import get_imts, get_levels
from toshi_hazard_post.local_config import API_URL, NUM_WORKERS, S3_URL, SNS_AGG_TASK_TOPIC, WORK_PATH
from toshi_hazard_post.locations import get_locations, locations_by_chunk
from toshi_hazard_post.logic_tree.branch_combinator import get_logic_tree
from toshi_hazard_post.logic_tree.logic_tree import HazardLogicTree
from toshi_hazard_post.util import BatchEnvironmentSetting, get_ecs_job_config
from toshi_hazard_post.util.sns import publish_message

from ..toshi_api_support import toshi_api
from .aggregation import DistributedAggregationTaskArguments

# from toshi_hazard_post.util.util import compress_config
from .aggregation_config import AggregationConfig
from .aggregation_task import fetch_logic_trees

log = logging.getLogger(__name__)

TEST_SIZE = None  # 16  # HOW many locations to run MAX (also see TOML limit)
MEMORY = 15360  # 7168 #8192 #30720 #15360 # 10240
NUM_WORKERS = 4  # noqa
NUM_MACHINES = 300
TIME_LIMIT = 1 * 60  # minutes


def batch_job_config(
        task_arguments: Dict=None,
        job_arguments: Dict=None,
        task_id: int=0,
        config_data: Any = None
) -> Dict[str, Any]:
    """Create an AWS Batch job configuration."""

    if task_arguments is None:
        task_arguments = {}
    if job_arguments is None:
        job_arguments = {}

    job_name = f"ToshiHazardPost-HazardAggregation-{task_id}"
    if not config_data:
        config_data = dict(task_arguments=task_arguments, job_arguments=job_arguments)
        use_compression = True
        quote_config_string = False
    else:
        use_compression = False
        quote_config_string = True
    extra_env = [
        BatchEnvironmentSetting(name="NZSHM22_HAZARD_STORE_STAGE", value="PROD"),
        BatchEnvironmentSetting(name="NZSHM22_HAZARD_STORE_REGION", value="ap-southeast-2"),
        BatchEnvironmentSetting(name="NZSHM22_HAZARD_POST_WORKERS", value=str(NUM_WORKERS)),
        # DEPLOYMENT_STAGE: ${self:custom.stage}
    ]
    return get_ecs_job_config(
        job_name,
        config_data,
        toshi_api_url=API_URL,
        toshi_s3_url=S3_URL,
        task_module=toshi_hazard_post.hazard_aggregation.aggregation_task.__name__,
        time_minutes=TIME_LIMIT,
        memory=MEMORY,
        vcpu=NUM_WORKERS,
        job_definition="BigLeverOnDemandEC2-THP-HazardAggregation",
        job_queue="ToshiHazardPost_HazAgg_JQ",  # "BigLever_32GB_8VCPU_v2_JQ", #"BigLeverOnDemandEC2-job-queue"
        extra_env=extra_env,
        use_compression=use_compression,
        quote_config_string=quote_config_string,
    )


def push_test_message() -> None:
    """For local SNS testing only."""
    publish_message({'hello': 'world'}, SNS_AGG_TASK_TOPIC)


def save_logic_trees(logic_trees: Dict[int, HazardLogicTree]) -> str:
    """Save the source_branches.json required by every aggregation task."""

    logic_tree_dict = {k: asdict(v) for k, v in logic_trees.items()}
    filepath = Path(WORK_PATH, 'logic_trees.json')
    with open(filepath, 'w') as sbf:
        sbf.write(json.dumps(logic_tree_dict, indent=2))

    logic_tree_id = toshi_api.save_sources_to_toshi(filepath, tag=None)
    log.debug("Produced logic_tree dict id : %s from file %s" % (logic_tree_id, filepath))
    return logic_tree_id


def distribute_aggregation(config: AggregationConfig, process_mode: str) -> None:
    """Configure the tasks using toshi to store the configuration."""

    # build logic tree or reuse existing (for testin/debugging only)
    if config.reuse_source_branches_id:
        log.info("reuse logic trees id: %s" % config.reuse_source_branches_id)
        logic_tree_id = config.reuse_source_branches_id
        logic_trees = fetch_logic_trees(logic_tree_id)
    else:
        log.info("building the logic trees.")

        logic_trees = {}
        for vs30 in config.vs30s:
            logic_trees[vs30] = get_logic_tree(
                config.lt_config, config.hazard_gts, vs30, gmm_correlations=[], truncate=config.source_branches_truncate
            )
        logic_tree_id = save_logic_trees(logic_trees)
        log.info("saved logic trees to id : %s" % logic_tree_id)

    locations = get_locations(config)
    resolution = 0.001
    example_loc_code = CodedLocation(*locations[0], resolution).downsample(0.001)
    log.debug('example_loc_code code: %s obj: %s' % (example_loc_code, example_loc_code))

    levels = get_levels(
        logic_trees[config.vs30s[0]], [example_loc_code.code], config.vs30s[0]
    )  # TODO: get separate levels for every IMT ?
    avail_imts = get_imts(logic_trees[config.vs30s[0]], config.vs30s[0])
    log.info(f'available imts: {avail_imts}')
    for imt in config.imts:
        assert imt in avail_imts

    if process_mode == 'AWS_BATCH':

        batch_client = boto3.client(
            service_name='batch', region_name='us-east-1', endpoint_url='https://batch.us-east-1.amazonaws.com'
        )
        for job_config in batch_job_configs(config, locations, logic_tree_id, levels):
            print('AWS_CONFIG: ', job_config)
            print()
            res = batch_client.submit_job(**job_config)
            print(res)
            print()

    if process_mode == 'AWS_LAMBDA':
        """Not really tested recently, lambda too puny for this work. TODO: deprecate."""
        pass
        """
        coded_locations = [CodedLocation(*loc) for loc in locations]
        for data in lambda_job_configs(config, coded_locations, toshi_ids, source_branches, levels, vs30):
            print('lamba_CONFIG: ', data)
            # Send message to initiate the process remotely
            publish_message({'aggregation_task_arguments': asdict(data)}, SNS_AGG_TASK_TOPIC)
        """


def batch_job_configs(
    config: AggregationConfig, locations: List[Tuple[float, float]], logic_trees_id: str, levels: List[float]
) -> Iterator[Dict[str, Any]]:

    task_count = 0
    log.debug('len locations %s' % len(locations))
    locs_processed = 0
    num_machines = config.num_machines if config.num_machines else NUM_MACHINES
    for key, coded_locs in locations_by_chunk(locations, point_res=0.001, chunk_size=NUM_WORKERS).items():
        # for key, coded_locs in locations_by_degree(locations, grid_res=1.0, point_res=0.001).items():
        log.info('key: %s coded_locs[:3]: %s len(coded_locs): %s' % (key, coded_locs[:3], len(coded_locs)))
        coded_locs_as_dicts = [asdict(loc) for loc in coded_locs]
        data = DistributedAggregationTaskArguments(
            hazard_model_id=config.hazard_model_id,
            logic_trees_id=logic_trees_id,
            locations=coded_locs_as_dicts,
            levels=levels,
            vs30s=config.vs30s,
            aggs=config.aggs,
            imts=config.imts,
            stride=config.stride,
        )
        locs_processed += NUM_WORKERS
        task_count += 1
        yield batch_job_config(
            task_arguments=asdict(data),
            job_arguments=dict(task_id=task_count, num_machines=num_machines),
            task_id=task_count,
        )
        if TEST_SIZE and locs_processed >= TEST_SIZE:
            break

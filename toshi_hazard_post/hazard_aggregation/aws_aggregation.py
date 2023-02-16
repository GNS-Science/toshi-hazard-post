"""Hazard aggregation task dispatch."""
import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List

import boto3
from nzshm_common.location.code_location import CodedLocation

import toshi_hazard_post.hazard_aggregation.aggregation_task

# from toshi_hazard_store.aggregate_rlzs_mp import build_source_branches
# from toshi_hazard_store.branch_combinator.branch_combinator import merge_ltbs_fromLT
from toshi_hazard_post.branch_combinator import merge_ltbs_fromLT
from toshi_hazard_post.hazard_aggregation.locations import get_locations, locations_by_chunk
from toshi_hazard_post.local_config import API_URL, NUM_WORKERS, S3_URL, SNS_AGG_TASK_TOPIC, WORK_PATH
from toshi_hazard_post.util import BatchEnvironmentSetting, get_ecs_job_config
from toshi_hazard_post.util.sns import publish_message

from .aggregate_rlzs import get_imts, get_levels
from .aggregation import DistributedAggregationTaskArguments, build_source_branches

# from toshi_hazard_post.util.util import compress_config
from .aggregation_config import AggregationConfig
from .aggregation_task import fetch_source_branches
from .toshi_api_support import save_sources_to_toshi, get_multi_gtdata

log = logging.getLogger(__name__)

TEST_SIZE = None  # 16  # HOW many locations to run MAX (also see TOML limit)
MEMORY = 15360  # 7168 #8192 #30720 #15360 # 10240
NUM_WORKERS = 4  # noqa


def batch_job_config(task_arguments: Dict, job_arguments: Dict, task_id: int):
    """Create an AWS Batch job configuration."""
    job_name = f"ToshiHazardPost-HazardAggregation-{task_id}"
    config_data = dict(task_arguments=task_arguments, job_arguments=job_arguments)
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
        time_minutes=240,
        memory=MEMORY,
        vcpu=NUM_WORKERS,
        job_definition="BigLeverOnDemandEC2-THP-HazardAggregation",
        job_queue="ToshiHazardPost_HazAgg_JQ",  # "BigLever_32GB_8VCPU_v2_JQ", #"BigLeverOnDemandEC2-job-queue"
        extra_env=extra_env,
        use_compression=True,
    )


def push_test_message():
    """For local SNS testing only."""
    publish_message({'hello': 'world'}, SNS_AGG_TASK_TOPIC)


def save_source_branches(source_branches):
    """Save the source_branches.json required by every aggregation task."""
    filepath = Path(WORK_PATH, 'source_branches.json')
    with open(filepath, 'w') as sbf:
        sbf.write(json.dumps(source_branches, indent=2))

    source_branches_id = save_sources_to_toshi(filepath, tag=None)
    log.debug("Produced source_branches id : %s from file %s" % (source_branches_id, filepath))
    return source_branches_id


def distribute_aggregation(config: AggregationConfig, process_mode: str):
    """Configure the tasks using toshi to store the configuration."""
    omit: List[str] = []

    gtdata = get_multi_gtdata(config.gtids)

    toshi_ids = {}
    for vs30 in config.vs30s:
        toshi_ids[vs30] = [
            branch.hazard_solution_id
            for branch in merge_ltbs_fromLT(config.logic_tree_permutations, gtdata=gtdata, omit=[])
            if branch.vs30 == vs30
        ]
    log.debug("toshi_ids: %s" % toshi_ids)

    # build source branches or reuse existing (for testin/debugging only)
    if config.reuse_source_branches_id:
        log.info("reuse sources_branches_id: %s" % config.reuse_source_branches_id)
        source_branches_id = config.reuse_source_branches_id
        source_branches = fetch_source_branches(source_branches_id)
        source_branches = {int(k): v for k, v in source_branches.items()}
    else:
        log.info("building the sources branches.")

        source_branches = {}
        for vs30 in config.vs30s:
            source_branches[vs30] = build_source_branches(
                config.logic_tree_permutations,
                gtdata,
                config.src_correlations,
                config.gmm_correlations,
                vs30,
                omit,
                toshi_ids[vs30],
                truncate=config.source_branches_truncate,
            )
        source_branches_id = save_source_branches(source_branches)
        log.info("saved source_branches to id : %s" % source_branches_id)

    locations = get_locations(config)

    resolution = 0.001
    example_loc_code = CodedLocation(*locations[0], resolution).downsample(0.001)

    log.debug('example_loc_code code: %s obj: %s' % (example_loc_code, example_loc_code))

    levels = get_levels(
        source_branches[config.vs30s[0]], [example_loc_code.code], config.vs30s[0]
    )  # TODO: get separate levels for every IMT ?
    avail_imts = get_imts(source_branches[config.vs30s[0]], config.vs30s[0])
    log.info(f'available imts: {avail_imts}')
    for imt in config.imts:
        assert imt in avail_imts

    if process_mode == 'AWS_BATCH':

        batch_client = boto3.client(
            service_name='batch', region_name='us-east-1', endpoint_url='https://batch.us-east-1.amazonaws.com'
        )
        for job_config in batch_job_configs(config, locations, toshi_ids, source_branches_id, levels, config.vs30s):
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


def batch_job_configs(config, locations, toshi_ids, source_branches_id, levels, vs30s):
    task_count = 0
    log.debug('len locations %s' % len(locations))
    locs_processed = 0
    for key, coded_locs in locations_by_chunk(locations, point_res=0.001, chunk_size=NUM_WORKERS).items():
        # for key, coded_locs in locations_by_degree(locations, grid_res=1.0, point_res=0.001).items():
        log.info('key: %s coded_locs[:3]: %s len(coded_locs): %s' % (key, coded_locs[:3], len(coded_locs)))
        coded_locs_as_dicts = [asdict(loc) for loc in coded_locs]
        data = DistributedAggregationTaskArguments(
            config.hazard_model_id,
            source_branches_id,
            toshi_ids,
            coded_locs_as_dicts,
            config.aggs,
            config.imts,
            levels,
            vs30s,
        )
        locs_processed += NUM_WORKERS
        task_count += 1
        yield batch_job_config(task_arguments=asdict(data), job_arguments=dict(task_id=task_count), task_id=task_count)
        if TEST_SIZE and locs_processed >= TEST_SIZE:
            break

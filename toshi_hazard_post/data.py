"""
Functions for loading realizations and saving aggregations
"""
import logging
import time
from typing import TYPE_CHECKING, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pyarrow import fs
from toshi_hazard_store.model.revision_4 import hazard_aggregate_curve, pyarrow_aggr_dataset

from toshi_hazard_post.local_config import ArrowFS, get_config

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    import numpy.typing as npt
    from nzshm_common.location.coded_location import CodedLocation, CodedLocationBin

    from toshi_hazard_post.logic_tree import HazardComponentBranch

try:
    import boto3
except ModuleNotFoundError:
    log.warning("warning boto3 module dependency not available, maybe you want to install with nzshm-model[boto3]")


def save_aggregations(
    hazard: 'npt.NDArray',
    location: 'CodedLocation',
    vs30: int,
    imt: str,
    agg_types: List[str],
    hazard_model_id: str,
    compatability_key: str,
) -> None:
    """
    Save the aggregated hazard to the database. Converts hazard as rates to proabilities before saving.

    Parameters:
        hazard: the aggregate hazard rates (not proabilities)
        location: the site location
        vs30: the site vs30
        imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
        agg_types: the statistical aggregate types (e.g. "mean", "0.5")
        hazard_model_id: the model id for storing in the database
    """

    filesystem, root = get_agg_filesystem()

    def generate_models():
        for i, agg in enumerate(agg_types):
            yield hazard_aggregate_curve.HazardAggregateCurve(
                compatible_calc_fk=('A', compatability_key),
                hazard_model_id=hazard_model_id,
                values=hazard[i, :],
                imt=imt,
                vs30=vs30,
                agg=agg,
            ).set_location(location)

    pyarrow_aggr_dataset.append_models_to_dataset(generate_models(), root, filesystem=filesystem)
    # write_aggs_to_ths(hazard, location, vs30, imt, agg_types, hazard_model_id)


def get_local_fs(local_dir) -> Tuple[fs.FileSystem, str]:
    return fs.LocalFileSystem(), str(local_dir)


def get_s3_fs(region, bucket) -> Tuple[fs.FileSystem, str]:
    session = boto3.session.Session()
    credentials = session.get_credentials()
    filesystem = fs.S3FileSystem(
        secret_key=credentials.secret_key,
        access_key=credentials.access_key,
        region=region,
        session_token=credentials.token,
    )
    root = bucket
    return filesystem, root


def get_rlz_filesystem() -> Tuple[fs.FileSystem, str]:
    config = get_config()
    return get_arrow_filesystem(
        config['RLZ_FS'],
        config['RLZ_LOCAL_DIR'],
        config['RLZ_AWS_REGION'],
        config['RLZ_S3_BUCKET'],
    )


def get_agg_filesystem() -> Tuple[fs.FileSystem, str]:
    config = get_config()
    return get_arrow_filesystem(
        config['AGG_FS'],
        config['AGG_LOCAL_DIR'],
        config['AGG_AWS_REGION'],
        config['AGG_S3_BUCKET'],
    )


def get_arrow_filesystem(
    fs_type: ArrowFS,
    local_dir: Optional[str] = None,
    aws_region: Optional[str] = None,
    s3_bucket: Optional[str] = None,
) -> Tuple[fs.FileSystem, str]:

    if fs_type is ArrowFS.LOCAL:
        log.info("getting local ArrowFS %s" % local_dir)
        filesystem, root = get_local_fs(local_dir)
    elif fs_type is ArrowFS.AWS:
        log.info("getting S3 ArrowFS %s:%s" % (aws_region, s3_bucket))
        filesystem, root = get_s3_fs(aws_region, s3_bucket)
    else:
        filesystem = root = None
    return filesystem, root


def get_realizations_dataset() -> ds.Dataset:
    """
    Get a pyarrow Dataset filtered to a location bin (partition), component branches, and compatibility key

    Parameters:
        location_bin: the location bin that the database is partitioned on
        component_branches: the branches to filter into the dataset
        compatibility_key: the hazard engine compatibility ley to filter into the dataset

    Returns:
        dataset: the dataset with the filteres applied
    """
    filesystem, root = get_rlz_filesystem()

    t0 = time.monotonic()
    dataset = ds.dataset(f'{root}', format='parquet', filesystem=filesystem, partitioning='hive')
    t1 = time.monotonic()
    log.info("time to get realizations dataset %0.6f" % (t1 - t0))

    return dataset


def load_realizations_mock(
    imt: str,
    location: 'CodedLocation',
    vs30: int,
):
    filename = f"/work/chrisdc/NZSHM-WORKING/PROD/tmp_data/component_{location.code}_{vs30}_{imt}"
    return pd.read_pickle(filename).to_dict()['rates']


def load_realizations(
    imt: str,
    location: 'CodedLocation',
    vs30: int,
    location_bin: 'CodedLocationBin',
    component_branches: List['HazardComponentBranch'],
    compatibility_key: str,
) -> pd.DataFrame:
    """
    Load component realizations from the database.

    Parameters:
        component_branches: list of the component branches that are combined to construct the full logic tree
        imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
        location: the site location
        vs30: the site vs30
        compatibility_key: the compatibility key used to lookup the correct realizations in the database

    Returns:
        values: the component realizations
    """
    dataset = get_realizations_dataset()

    gmms_digests = [branch.gmcm_hash_digest for branch in component_branches]
    sources_digests = [branch.source_hash_digest for branch in component_branches]

    flt = (
        (pc.field('compatible_calc_fk') == pc.scalar(compatibility_key))
        & (pc.is_in(pc.field('sources_digest'), pa.array(sources_digests)))
        & (pc.is_in(pc.field('gmms_digest'), pa.array(gmms_digests)))
        & (pc.field('nloc_0') == pc.scalar(location_bin.code))
        & (pc.field('nloc_001') == pc.scalar(location.downsample(0.001).code))
        & (pc.field('imt') == pc.scalar(imt))
        & (pc.field('vs30') == pc.scalar(vs30))
    )

    t0 = time.monotonic()
    columns = ['sources_digest', 'gmms_digest', 'values']
    arrow_scanner = ds.Scanner.from_dataset(dataset, filter=flt, columns=columns, use_threads=False)
    t1 = time.monotonic()

    rlz_table = arrow_scanner.to_table()
    t2 = time.monotonic()
    if len(rlz_table) == 0:
        raise Exception(
            f"no realizations were found in the database for {location=}, {imt=}, {vs30=}, {compatibility_key=}"
        )

    log.info("load scanner:%0.6f, to_arrow %0.6fs" % (t1 - t0, t2 - t1))
    log.info("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))
    log.info("loaded %s realizations in arrow", rlz_table.shape[0])
    # all_values = rlz_table.to_pandas()['values']
    # from itertools import chain
    # import numpy as np
    # foo = [row for row in all_values.to_numpy()]
    # all_values = np.array(list(chain(*[row for row in all_values.to_numpy()])))
    # print(all_values.max())
    # print(np.histogram(all_values, range=(0, .12)))
    # breakpoint()
    # assert 0
    rlz_df = rlz_table.to_pandas()
    rlz_df['sources_digest'] = rlz_df['sources_digest'].astype(str)
    rlz_df['gmms_digest'] = rlz_df['gmms_digest'].astype(str)
    return rlz_df


# def save_aggregations(
#     hazard: 'npt.NDArray',
#     location: 'CodedLocation',
#     vs30: int,
#     imt: str,
#     agg_types: List[str],
#     hazard_model_id: str,
# ) -> None:
#     """
#     Save the aggregated hazard to the database. Converts hazard as rates to proabilities before saving.

#     Parameters:
#         hazard: the aggregate hazard rates (not proabilities)
#         location: the site location
#         vs30: the site vs30
#         imt: the intensity measure type (e.g. "PGA", "SA(1.5)")
#         agg_types: the statistical aggregate types (e.g. "mean", "0.5")
#         hazard_model_id: the model id for storing in the database
#     """
#     hazard = rate_to_prob(hazard, 1.0)
#     write_aggs_to_ths(hazard, location, vs30, imt, agg_types, hazard_model_id)

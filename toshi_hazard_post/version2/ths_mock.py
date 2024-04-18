import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generator, Iterable, List, Sequence

import boto3
import numpy as np
import pyarrow.compute as pc
import pyarrow.dataset as ds
from nzshm_model.branch_registry import identity_digest
from nzshm_model.logic_tree import GMCMBranch, SourceBranch
from pyarrow import fs

from toshi_hazard_post.version2.local_config import ARROW_DIR, ARROW_FS, ArrowFS
from toshi_hazard_post.version2.logic_tree import HazardComponentBranch

if TYPE_CHECKING:
    import numpy.typing as npt
    from nzshm_common.location.code_location import CodedLocation

log = logging.getLogger(__name__)


def write_aggs_to_ths(
    hazard: 'npt.NDArray',
    location: 'CodedLocation',
    vs30: int,
    imt: str,
    agg_types: List[str],
    hazard_model_id: str,
) -> None:
    filepath = f"{hazard_model_id}_{vs30}_{imt}_{location.code}"
    np.save(filepath, hazard)


def query_compatibility(compatibility_key: str) -> Generator[str, None, None]:
    entries = {"A_A": "a", "B": "b", "C": "c"}

    if compatibility_key in entries:
        yield entries[compatibility_key]


def query_levels(compat_key: str) -> 'npt.NDArray':
    """
    Get the intensity measure type levels (IMTLs) for the hazard curve from the compatibility table

    Parameters:
        compatibility_key: the key identifying the hazard calculation compatibility entry

    Returns:
        levels: the IMTLs for the hazard calculation
    """
    return np.array(
        [
            0.0001,
            0.0002,
            0.0004,
            0.0006,
            0.0008,
            0.001,
            0.002,
            0.004,
            0.006,
            0.008,
            0.01,
            0.02,
            0.04,
            0.06,
            0.08,
            0.1,
            0.2,
            0.3,
            0.4,
            0.5,
            0.6,
            0.7,
            0.8,
            0.9,
            1.0,
            1.2,
            1.4,
            1.6,
            1.8,
            2.0,
            2.2,
            2.4,
            2.6,
            2.8,
            3.0,
            3.5,
            4.0,
            4.5,
            5.0,
            6.0,
            7.0,
            8.0,
            9.0,
            10.0,
        ]
    )


@dataclass
class mRLZ:
    values: 'npt.NDArray'
    # loc: str
    # vs30: int
    # imt: str
    source: SourceBranch
    gsims: Sequence[GMCMBranch]


def query_realizations(
    loc: 'CodedLocation', vs30: int, imt: str, branches: Iterable[HazardComponentBranch], compat_key: str
) -> Generator[mRLZ, None, None]:

    session = boto3.session.Session()
    credentials = session.get_credentials()

    if ARROW_FS is ArrowFS.AWS:
        log.info("reading from S3 dataset")
        filesystem = fs.S3FileSystem(
            secret_key=credentials.secret_key,
            access_key=credentials.access_key,
            region='ap-southeast-2',
            session_token=credentials.token,
        )
        root = 'ths-poc-arrow-test/pq-CDC'

        filesystem = fs.S3FileSystem(
            region='ap-southeast-2',
        )
    elif ARROW_FS is ArrowFS.LOCAL:
        log.info("reading from local dataset")
        filesystem = fs.LocalFileSystem()
        root = str(ARROW_DIR)

    partition = f"nloc_0={loc.downsample(1).code}"
    dataset = ds.dataset(f'{root}/{partition}', format='parquet', filesystem=filesystem)
    df = dataset.to_table().to_pandas()
    ind = (
        (df['nloc_001'] == loc.downsample(0.001).code)
        & (df['imt'] == imt)
        & (df['vs30'] == vs30)
        & (df['compatible_calc_fk'] == compat_key)
    )
    df0 = df[ind]

    for branch in branches:
        # sources_digest = 'ef55f8757069'
        # gmms_digest = 'a7d8c5d537e1'

        # sources_digest = hashlib.shake_256(branch.source_branch.registry_identity.encode()).hexdigest(6)
        # gmms_digest = hashlib.shake_256(
        #     '|'.join([b.registry_identity for b in branch.gmcm_branches]).encode()
        # ).hexdigest(6)
        tic = time.perf_counter()
        sources_digest = identity_digest(branch.source_branch.registry_identity)
        gmms_digest = identity_digest(branch.gmcm_branches[0].registry_identity)

        # filter = (
        #     (pc.field('imt')==pc.scalar(imt)) &
        #     (pc.field('vs30') == pc.scalar(vs30)) &
        #     (pc.field('nloc_001') == pc.scalar(loc.downsample(0.001).code)) &
        #     (pc.field('compatible_calc_fk') == pc.scalar(compat_key)) &
        #     (pc.field('sources_digest') == pc.scalar(sources_digest)) &
        #     (pc.field('gmms_digest') == pc.scalar(gmms_digest))
        # )
        # df = dataset.to_table(filter=filter).to_pandas()

        ind = (df0['sources_digest'] == sources_digest) & (df0['gmms_digest'] == gmms_digest)
        df1 = df0[ind]

        if df1.shape[0] != 1:
            breakpoint()
            raise Exception("something's gone wrong")
        values = df1['values'].iloc[0]
        toc = time.perf_counter()
        log.debug(f"time to load one component branch {toc-tic}")

        yield mRLZ(
            values=values,
            source=branch.source_branch,
            gsims=branch.gmcm_branches,
        )

    # for loc, vs30, imt, branch in product(locs, vs30s, imts, branches):
    #     yield mRLZ(
    #         loc=loc,
    #         vs30=vs30,
    #         imt=imt,
    #         source=branch.source_branch,
    #         gsims=branch.gmcm_branches,
    #         values=list(np.linspace(1, 0, 44) * 0.5),
    #     )


def pyarrow_demo(loc, imt, vs30, compat_key):

    sources_digest = 'ef55f8757069'
    gmms_digest = 'a7d8c5d537e1'

    if ARROW_FS is ArrowFS.AWS:
        session = boto3.session.Session()
        credentials = session.get_credentials()

        filesystem = fs.S3FileSystem(
            secret_key=credentials.secret_key,
            access_key=credentials.access_key,
            region='ap-southeast-2',
            session_token=credentials.token,
        )

        filesystem = fs.S3FileSystem(
            region='ap-southeast-2',
        )
    elif ARROW_FS is ArrowFS.LOCAL:
        filesystem = fs.LocalFileSystem()

    partition = f"nloc_0={loc.downsample(1).code}"
    dataset = ds.dataset(f'ths-poc-arrow-test/pq-CDC/{partition}', format='parquet', filesystem=filesystem)
    filter = (
        (pc.field('imt') == pc.scalar(imt))
        & (pc.field('vs30') == pc.scalar(vs30))
        & (pc.field('nloc_001') == pc.scalar(loc.downsample(0.001).code))
        & (pc.field('compatible_calc_fk') == pc.scalar(compat_key))
        & (pc.field('sources_digest') == pc.scalar(sources_digest))
        & (pc.field('gmms_digest') == pc.scalar(gmms_digest))
    )
    # df = dataset.to_table(filter=filter).to_pandas()
    table = dataset.to_table(filter=filter)

    return table

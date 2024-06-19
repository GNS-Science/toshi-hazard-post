"""
Package to set compute configuration. Configuration parameters can be set by default .env file, user specified file,
and/or environment variables in that order of increasing precidence.

Environment varaible parameters are uppercase, config file is case insensitive.

To use the local configuration, set the envvar 'THP_ENV_FILE' to the desired config file path. Then call
get_config() inside a function. Note that get_config() must be called in function scope. If called in module scope,
changes to 'THP_ENV_FILE' will not be effective

Parameters:
    THP_NUM_WORKERS: number of parallel processes. if == 1, will run without spawning new processes
    THP_{RLZ|AGG}_FS: the filesystem to use for the {realization or aggregate} datastore (LOCAL or AWS)
    THP_{RLZ|AGG}_LOCAL_DIR: the path to the local {realization or aggregate} datastore (if using LOCAL)
    THP_{RLZ|AGG}_S3_BUCKET: the S3 bucket where the {realization or aggregate} datastore is kept (if using AWS)
    THP_{RLZ|AGG}_AWS_REGION: the AWS region for {realization or aggregate} if using S3 datastore
"""

import os
from enum import Enum, auto

from dotenv import load_dotenv


class ArrowFS(Enum):
    LOCAL = auto()
    AWS = auto()


def set_fs(fs: str, envvar: str) -> ArrowFS:
    try:
        return ArrowFS[fs.upper()]
    except KeyError:
        raise KeyError("filesystem set to %s, but %s must be in %s" % (fs, envvar, [x.name for x in ArrowFS]))


DEFAULT_NUM_WORKERS = 1
DEFAULT_FS = 'LOCAL'


def get_config():
    load_dotenv(os.getenv('THP_ENV_FILE', '.env'))
    return dict(
        NUM_WORKERS=int(os.getenv('THP_NUM_WORKERS', DEFAULT_NUM_WORKERS)),
        RLZ_FS=set_fs(os.getenv('THP_RLZ_FS', DEFAULT_FS), 'THP_RLZ_FS'),
        AGG_FS=set_fs(os.getenv('THP_AGG_FS', DEFAULT_FS), 'THP_AGG_FS'),
        RLZ_LOCAL_DIR=os.getenv('THP_RLZ_LOCAL_DIR'),
        RLZ_S3_BUCKET=os.getenv('THP_RLZ_S3_BUCKET'),
        RLZ_AWS_REGION=os.getenv('THP_RLZ_AWS_REGION'),
        AGG_LOCAL_DIR=os.getenv('THP_AGG_LOCAL_DIR'),
        AGG_S3_BUCKET=os.getenv('THP_AGG_S3_BUCKET'),
        AGG_AWS_REGION=os.getenv('THP_AGG_AWS_REGION'),
    )

"""
Module for setting the compute configuration. Configuration parameters can be set by default .env
file, user specified file, or environment variable, with  that order of  precedence.

Environment varaible parameters are uppercase, config file is case insensitive.

To use the local configuration, set the envvar 'THP_ENV_FILE' to the desired config file path. Then call
get_config() inside a function. Note that get_config() must be called in function scope. If called in module scope,
changes to 'THP_ENV_FILE' will not be effective

Parameters:
    THP_NUM_WORKERS: number of parallel processes. if == 1, will run without spawning new processes
    THP_{RLZ|AGG}_DIR: the path to the {realization or aggregate} datastore. Can be a local filepath or s3 bucket
"""

import os

from dotenv import load_dotenv

DEFAULT_NUM_WORKERS = 1
DEFAULT_FS = 'LOCAL'


def get_config():
    load_dotenv(os.getenv('THP_ENV_FILE', '.env'))
    return dict(
        NUM_WORKERS=int(os.getenv('THP_NUM_WORKERS', DEFAULT_NUM_WORKERS)),
        RLZ_DIR=os.getenv('THP_RLZ_DIR'),
        AGG_DIR=os.getenv('THP_AGG_DIR'),
        DELAY_MULTIPLIER=float(os.getenv('THP_DELAY_MULTIPLIER', 1.0)),
    )

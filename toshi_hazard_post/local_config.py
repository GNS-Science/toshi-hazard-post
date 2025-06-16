"""
Module for setting the compute configuration. Configuration parameters can be set by default .env
file, user specified file, or environment variable, with  that order of  precedence.

Environment varaible parameters are uppercase, config file is case insensitive.

To use the local configuration, set the envvar 'THP_ENV_FILE' to the desired config file path. Then call
get_config() inside a function. Note that get_config() must be called in function scope. If called in module scope,
changes to 'THP_ENV_FILE' will not be effective

Parameters:
    THP_NUM_WORKERS: number of parallel processes. if == 1, will run without spawning new processes.
    THP_{RLZ|AGG}_DIR: the path to the {realization or aggregate} datastore. Can be a local filepath or s3 bucket.
    THP_WORKING_DIR: the path to the directory to use for writing realization data tables.
"""

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

DEFAULT_NUM_WORKERS = 1
DEFAULT_FS = 'LOCAL'


def get_config() -> dict[str, Any]:
    load_dotenv(os.getenv('THP_ENV_FILE', '.env'))
    config = dict(
        NUM_WORKERS=int(os.getenv('THP_NUM_WORKERS', DEFAULT_NUM_WORKERS)),
        RLZ_DIR=os.getenv('THP_RLZ_DIR'),
        AGG_DIR=os.getenv('THP_AGG_DIR'),
        WORKING_DIR=Path(os.getenv('THP_WORKING_DIR', '/tmp')).expanduser()
    )
    if not config['WORKING_DIR'].is_dir():
        raise FileNotFoundError(f"WORKING_DIR {config['WORKING_DIR']} is not a directory")
    return config

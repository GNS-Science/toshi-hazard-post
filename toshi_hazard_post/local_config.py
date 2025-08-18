"""Module for setting the compute configuration.

Configuration parameters can be set by a file and/or environment variables. Environment variables will take
precedence over values in the file file. The default the file location is ./.env, but can be set with THP_ENV_FILE.

Args:
    THP_NUM_WORKERS: number of parallel processes. if == 1, will run without spawning new processes.
    THP_{RLZ|AGG}_DIR: the path to the {realization or aggregate} datastore. Can be a local filepath or s3 bucket.
    THP_WORKING_DIR: the path to the directory to use for writing realization data tables.
"""

import os
import tempfile
from pathlib import Path

from dotenv import load_dotenv

DEFAULT_NUM_WORKERS = 1
DEFAULT_FS = 'LOCAL'


def _dir_path_env(name, default='', can_be_s3=False) -> str:
    dir_path = os.getenv(name, default)
    if can_be_s3 and dir_path[:5] == "s3://":
        return dir_path
    dir_path = Path(dir_path).expanduser()
    if not dir_path.is_dir():
        raise ValueError(f"{name} must be a directory but {dir_path} was given.")
    return str(dir_path)


load_dotenv(os.getenv('THP_ENV_FILE', '.env'))
NUM_WORKERS = int(os.getenv('THP_NUM_WORKERS', DEFAULT_NUM_WORKERS))
RLZ_DIR = _dir_path_env('THP_RLZ_DIR', can_be_s3=True)
AGG_DIR = _dir_path_env('THP_AGG_DIR', can_be_s3=True)
WORKING_DIR = _dir_path_env('THP_WORKING_DIR', tempfile.gettempdir())

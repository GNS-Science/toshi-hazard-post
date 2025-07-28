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


def _dir_path_env(name, default='') -> Path:
    path = Path(os.getenv(name, default)).expanduser()
    if not path.is_dir():
        raise ValueError("{name} must be a directory but {path} was assigned.")
    return Path(path)


load_dotenv(os.getenv('THP_ENV_FILE', '.env'))
NUM_WORKERS = int(os.getenv('THP_NUM_WORKERS', DEFAULT_NUM_WORKERS))
RLZ_DIR = _dir_path_env('THP_RLZ_DIR')
AGG_DIR = _dir_path_env('THP_AGG_DIR')
WORKING_DIR = _dir_path_env('THP_WORKING_DIR', tempfile.gettempdir())

if not WORKING_DIR.is_dir():
    raise FileNotFoundError(f"{WORKING_DIR=} is not a directory")

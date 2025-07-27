"""
Module for setting the compute configuration. Configuration parameters can be set by default .env
file, user specified file, or environment variable, with  that order of  precedence.

Environment variabiles can be overwritten with a .env file or a file specified by the value of THP_ENV_FILE.

Parameters:
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


def dir_path_env(name, default='') -> Path:
    path = Path(os.getenv(name, default)).expanduser()
    if not path.is_dir():
        raise ValueError("{name} must be a directory but {path} was assigned.")
    return Path(path)


load_dotenv(os.getenv('THP_ENV_FILE', '.env'))
NUM_WORKERS = int(os.getenv('THP_NUM_WORKERS', DEFAULT_NUM_WORKERS))
RLZ_DIR = dir_path_env('THP_RLZ_DIR')
AGG_DIR = dir_path_env('THP_AGG_DIR')
WORKING_DIR = dir_path_env('THP_WORKING_DIR', tempfile.gettempdir())

if not WORKING_DIR.is_dir():
    raise FileNotFoundError(f"{WORKING_DIR=} is not a directory")

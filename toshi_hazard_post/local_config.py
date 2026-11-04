"""Module for setting the compute configuration.

Configuration parameters can be set by a file and/or environment variables. Environment variables will take
precedence over values in the file file. The default the file location is ./.env, but can be set with THP_ENV_FILE.

Args:
    THP_NUM_WORKERS: number of parallel processes. if == 1, will run without spawning new processes.
    THP_NUM_LOAD_THREADS: number of realization batches (one vs30 and 1 degree location bin each) to load at once,
        in threads of the main process. Each batch being loaded or used takes about 1-2 GB.
    THP_{RLZ|AGG}_DIR: the path to the {realization or aggregate} datastore. Can be a local filepath or s3 bucket.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# on Windows we need to tell pyarrow where to find the installed time zone database
# https://arrow.apache.org/docs/python/install.html#tzdata-on-windows
if os.name == 'nt':
    import tzdata

    tzdata_dir = Path(tzdata.__file__).parent / 'zoneinfo'
    os.environ['TZDIR'] = str(tzdata_dir)

DEFAULT_NUM_WORKERS = 1
DEFAULT_NUM_LOAD_THREADS = 4
DEFAULT_FS = 'LOCAL'


def _positive_int_env(name: str, default: int) -> int:
    value = os.getenv(name, str(default))
    try:
        number = int(value)
    except ValueError:
        number = 0
    if number < 1:
        raise ValueError(f"{name} must be a positive integer but {value!r} was given.")
    return number


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
NUM_LOAD_THREADS = _positive_int_env('THP_NUM_LOAD_THREADS', DEFAULT_NUM_LOAD_THREADS)
RLZ_DIR = _dir_path_env('THP_RLZ_DIR', can_be_s3=True)
AGG_DIR = _dir_path_env('THP_AGG_DIR', can_be_s3=True)

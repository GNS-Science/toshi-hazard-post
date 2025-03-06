import os
from pathlib import Path

import pytest

from toshi_hazard_post.local_config import ArrowFS, get_config

default_attrs = [
    ("NUM_WORKERS", 1),
    ("RLZ_FS", ArrowFS.LOCAL),
    ("AGG_FS", ArrowFS.LOCAL),
]

user_attrs = [
    ("NUM_WORKERS", 2),
    ("RLZ_LOCAL_DIR", 'user local dir'),
    ("RLZ_S3_BUCKET", 'user s3 bucket'),
    ("RLZ_AWS_REGION", 'user rlz aws region'),
    # ("RLZ_FS", toshi_hazard_post.local_config.ArrowFS.AWS),
    ("AGG_LOCAL_DIR", 'user agg local dir'),
    ("AGG_S3_BUCKET", 'user agg s3 bucket'),
    ("AGG_AWS_REGION", 'user agg aws region'),
    # ("AGG_FS", toshi_hazard_post.local_config.ArrowFS.AWS),
]


@pytest.fixture(scope='function', params=list(range(7)))
def env_attr_val(request):
    attrs_vals = [
        ["NUM_WORKERS", 3],
        ["RLZ_LOCAL_DIR", 'env local dir'],
        ["RLZ_S3_BUCKET", 'env s3 bucket'],
        ["RLZ_AWS_REGION", 'env aws region'],
        ["AGG_LOCAL_DIR", 'env agg local dir'],
        ["AGG_S3_BUCKET", 'env agg s3 bucket'],
        ["AGG_AWS_REGION", 'env agg aws region'],
    ]
    env_attr_val = [['THP_' + item[0]] + item for item in attrs_vals]
    return env_attr_val[request.param]


user_filepath = Path(__file__).parent / 'fixtures' / 'local_config' / '.env'


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    env_vars = [
        "THP_NUM_WORKERS",
        "THP_RLZ_LOCAL_DIR",
        "THP_RLZ_S3_BUCKET",
        "THP_RLZ_AWS_REGION",
        "THP_RLZ_FS",
        "THP_AGG_LOCAL_DIR",
        "THP_AGG_S3_BUCKET",
        "THP_AGG_AWS_REGION",
        "THP_AGG_FS",
        "THP_ENV_FILE",
    ]
    for var in env_vars:
        monkeypatch.delenv(var, raising=False)


@pytest.mark.skip("doesn't work in GHA")
@pytest.mark.parametrize("attr,value", default_attrs)
def test_default_values(attr, value):
    assert get_config()[attr] == value


@pytest.mark.skip("doesn't work in GHA")
@pytest.mark.parametrize("attr,value", user_attrs)
def test_user_precidence(attr, value, monkeypatch):
    """test that a user defined env file will override default values"""
    monkeypatch.setenv('THP_ENV_FILE', str(user_filepath))
    assert get_config()[attr] == value


@pytest.mark.skip("doesn't work in GHA")
def test_env_precidence(env_attr_val, monkeypatch):
    """test that env vars will take highest precidence"""
    monkeypatch.setenv('THP_ENV_FILE', str(user_filepath))
    env, attr, value = env_attr_val
    os.environ[env] = str(value)
    assert get_config()[attr] == value


def test_arrow_fs():
    with pytest.raises(KeyError):
        os.environ['THP_RLZ_FS'] = 'FOOBAR'
        get_config()

    with pytest.raises(KeyError):
        os.environ['THP_AGG_FS'] = 'FOOBAR'
        get_config()

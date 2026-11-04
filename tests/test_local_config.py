import importlib

import pytest

import toshi_hazard_post.local_config as local_config


@pytest.fixture
def reload_config(monkeypatch):
    yield lambda: importlib.reload(local_config)
    monkeypatch.undo()
    importlib.reload(local_config)


def test_num_load_threads_default(monkeypatch, reload_config):
    monkeypatch.delenv('THP_NUM_LOAD_THREADS', raising=False)
    assert reload_config().NUM_LOAD_THREADS == 4


def test_num_load_threads_from_env(monkeypatch, reload_config):
    monkeypatch.setenv('THP_NUM_LOAD_THREADS', '2')
    assert reload_config().NUM_LOAD_THREADS == 2


@pytest.mark.parametrize("value", ["0", "-1", "x"])
def test_num_load_threads_must_be_positive(monkeypatch, reload_config, value):
    monkeypatch.setenv('THP_NUM_LOAD_THREADS', value)
    with pytest.raises(ValueError, match="THP_NUM_LOAD_THREADS"):
        reload_config()

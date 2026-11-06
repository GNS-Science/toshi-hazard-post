"""The shared memory of a run is removed however the run ends, and runs do not share it (#105)."""

import multiprocessing
import os
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from multiprocessing import shared_memory
from pathlib import Path

import numpy as np
import pytest
from nzshm_common.location.coded_location import CodedLocation

import toshi_hazard_post.aggregation as aggregation
import toshi_hazard_post.data
from toshi_hazard_post.aggregation import run_aggregation
from toshi_hazard_post.aggregation_args import AggregationArgs, load_input_args
from toshi_hazard_post.aggregation_calc import AggSharedArgs, AggTaskArgs, calc_aggregation

fixture_dir = Path(__file__).parent / 'fixtures/end_to_end'


@pytest.fixture
def agg_args(monkeypatch, tmp_path) -> AggregationArgs:
    monkeypatch.setattr(toshi_hazard_post.data, 'AGG_DIR', str(tmp_path))
    monkeypatch.setattr(toshi_hazard_post.data, 'RLZ_DIR', str(fixture_dir / 'rlz'))
    return load_input_args(fixture_dir / 'hazard.toml')


@pytest.fixture
def created_names(monkeypatch) -> Iterator[list[str]]:
    """Record the name of every shared memory segment created, and remove any left behind after the test."""
    names: list[str] = []
    original = shared_memory.SharedMemory

    class Recording(original):  # type: ignore[valid-type, misc]
        def __init__(self, name: str | None = None, create: bool = False, size: int = 0) -> None:
            super().__init__(name=name, create=create, size=size)
            if create:
                names.append(self.name)

    monkeypatch.setattr(shared_memory, 'SharedMemory', Recording)
    yield names
    for name in names:
        if exists(name):
            leftover = original(name=name)
            leftover.close()
            leftover.unlink()


def exists(name: str) -> bool:
    try:
        segment = shared_memory.SharedMemory(name=name)
    except FileNotFoundError:
        return False
    segment.close()
    return True


def fail_job(*args: object) -> None:
    raise ValueError("bad job")


def test_failed_run_removes_its_shared_memory(monkeypatch, agg_args, created_names):
    monkeypatch.setattr(aggregation, 'get_job_probs', fail_job)

    with pytest.raises(ValueError, match="bad job"):
        run_aggregation(agg_args, ThreadPoolExecutor())

    assert len(created_names) == 2
    assert not any(exists(name) for name in created_names)


def test_run_after_a_failed_run_succeeds(monkeypatch, agg_args, created_names):
    get_job_probs = aggregation.get_job_probs
    monkeypatch.setattr(aggregation, 'get_job_probs', fail_job)
    with pytest.raises(ValueError, match="bad job"):
        run_aggregation(agg_args, ThreadPoolExecutor())

    monkeypatch.setattr(aggregation, 'get_job_probs', get_job_probs)
    run_aggregation(agg_args, ThreadPoolExecutor())  # FileExistsError if the first run's segments were left behind


def test_runs_at_the_same_time_do_not_collide(monkeypatch, agg_args, created_names):
    nested_runs: list[str] = []

    def task_that_starts_another_run(task_args: AggTaskArgs, shared_args: AggSharedArgs) -> None:
        # start a second run while the first holds its shared memory
        if not nested_runs:
            nested_runs.append("started")
            run_aggregation(agg_args, ThreadPoolExecutor())
            nested_runs.append("finished")
        calc_aggregation(task_args, shared_args)

    monkeypatch.setattr(aggregation, 'calc_aggregation', task_that_starts_another_run)
    run_aggregation(agg_args, ThreadPoolExecutor())

    assert nested_runs == ["started", "finished"]
    assert len(set(created_names)) == 4


def exit_worker(task_args: AggTaskArgs, shared_args: AggSharedArgs) -> None:
    os._exit(1)


def test_worker_crash_removes_the_shared_memory(monkeypatch, agg_args, created_names):
    def tasks(*args: object) -> Iterator[AggTaskArgs]:
        for i in range(10):
            yield AggTaskArgs(CodedLocation(-41.3, 174.8, 0.001), i, "PGA", np.zeros((1, 1)))

    monkeypatch.setattr(aggregation, 'NUM_WORKERS', 1)
    monkeypatch.setattr(aggregation, '_generate_agg_jobs', tasks)
    monkeypatch.setattr(aggregation, 'calc_aggregation', exit_worker)

    # a submit after the worker dies raises; the cap of 2 tasks in flight makes sure there is one
    with pytest.raises(BrokenProcessPool):
        # forkserver rather than fork: the test process may have threads running
        run_aggregation(agg_args, ProcessPoolExecutor(1, mp_context=multiprocessing.get_context('forkserver')))

    assert len(created_names) == 2
    assert not any(exists(name) for name in created_names)

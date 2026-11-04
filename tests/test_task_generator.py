import threading
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import product

import numpy as np
import pytest
from nzshm_common.location.coded_location import CodedLocation

import toshi_hazard_post.aggregation as aggregation
from toshi_hazard_post.aggregation import _generate_agg_jobs, _prefetch, _run_tasks
from toshi_hazard_post.aggregation_calc import AggSharedArgs, AggTaskArgs
from toshi_hazard_post.aggregation_setup import Site


def double(x: int) -> int:
    return x * 2


def test_prefetch_preserves_order():
    assert list(_prefetch(double, range(10), 3)) == [x * 2 for x in range(10)]


def test_prefetch_more_threads_than_items():
    assert list(_prefetch(double, [1], 4)) == [2]


def test_prefetch_no_items():
    assert list(_prefetch(double, [], 4)) == []


class Tracked:
    """An object that counts how many instances are alive."""

    live = 0

    def __init__(self) -> None:
        Tracked.live += 1

    def __del__(self) -> None:
        Tracked.live -= 1


@pytest.mark.parametrize("n", [1, 2, 4])
def test_prefetch_holds_at_most_n(n):
    lock = threading.Lock()
    loading = 0
    max_loading = 0

    def load(i: int) -> Tracked:
        nonlocal loading, max_loading
        with lock:
            loading += 1
            max_loading = max(max_loading, loading)
        time.sleep(0.01 * (i % 3))
        obj = Tracked()
        with lock:
            loading -= 1
        return obj

    Tracked.live = 0
    seen = []
    for obj in _prefetch(load, range(12), n):
        time.sleep(0.01)  # let the loader threads run ahead if they can
        seen.append(Tracked.live)
        del obj
    assert len(seen) == 12
    assert max(seen) <= n
    assert max_loading <= n


def test_prefetch_raises_at_the_failed_items_turn():
    def load(i: int) -> int:
        if i == 2:
            raise ValueError("bad bin")
        return i

    gen = _prefetch(load, range(5), 2)
    assert next(gen) == 0
    assert next(gen) == 1
    with pytest.raises(ValueError, match="bad bin"):
        next(gen)


def test_prefetch_stops_early_without_waiting_for_pending_loads():
    started = []

    def load(i: int) -> int:
        started.append(i)
        if i > 0:
            time.sleep(0.5)
        return i

    gen = _prefetch(load, range(10), 2)
    assert next(gen) == 0
    t0 = time.perf_counter()
    gen.close()
    assert time.perf_counter() - t0 < 0.4
    assert len(started) < 10


def test_generate_agg_jobs_order_is_the_same_for_any_threads(monkeypatch):
    """Jobs come out in bin order even when later bins finish loading first."""
    lat_lons = [(-41.2, 174.8), (-41.3, 174.7), (-36.9, 174.8), (-45.9, 170.5)]  # 3 bins
    sites = [Site(CodedLocation(*ll, 0.001), vs30) for ll, vs30 in product(lat_lons, [275, 400])]
    imts = ["PGA", "SA(1.0)"]
    bins = []

    def fake_load_batch(vs30, nloc_0, compatibility_key, sources_digests, gmms_digests, imts, component_digests):
        bins.append((vs30, nloc_0))
        time.sleep(0.05 / len(bins))  # later loads finish sooner
        return (vs30, nloc_0), {}, None

    monkeypatch.setattr(aggregation, '_load_batch', fake_load_batch)
    monkeypatch.setattr(aggregation, 'get_job_probs', lambda *args: np.zeros((1, 1)))

    def jobs(k: int) -> list[tuple[int, str, str]]:
        return [(t.vs30, t.location.code, t.imt) for t in _generate_agg_jobs(sites, imts, "key", [], k)]

    serial = jobs(1)
    assert len(serial) == len(sites) * len(imts)
    assert jobs(4) == serial


@pytest.mark.parametrize("k", [1, 2])
def test_generate_agg_jobs_holds_at_most_k_batches(monkeypatch, k):
    lat_lons = [(-41.2, 174.8), (-36.9, 174.8), (-45.9, 170.5), (-39.0, 176.0), (-44.0, 172.0)]  # 5 bins
    sites = [Site(CodedLocation(*ll, 0.001), 275) for ll in lat_lons]

    live_when_loaded = []

    def fake_load_batch(vs30, nloc_0, compatibility_key, sources_digests, gmms_digests, imts, component_digests):
        table = Tracked()
        live_when_loaded.append(Tracked.live)
        return table, {}, None

    monkeypatch.setattr(aggregation, '_load_batch', fake_load_batch)
    monkeypatch.setattr(aggregation, 'get_job_probs', lambda *args: np.zeros((1, 1)))

    Tracked.live = 0
    seen = []
    for _ in _generate_agg_jobs(sites, ["PGA"], "key", [], k):
        time.sleep(0.02)  # let the loader threads run ahead if they can
        seen.append(Tracked.live)
    assert len(seen) == 5
    assert max(seen) <= k
    assert max(live_when_loaded) <= k


def shared_args() -> AggSharedArgs:
    return AggSharedArgs(
        agg_types=["mean"],
        compatibility_key="key",
        hazard_model_id="model",
        weights_shape=(1,),
        branch_index_table_shape=(1, 1),
        branch_index_table_dtype="<u2",
        skip_save=True,
    )


def task(i: int) -> AggTaskArgs:
    return AggTaskArgs(location=CodedLocation(-41.0, 175.0, 0.001), vs30=i, imt="PGA", probs=np.zeros((912, 44)))


def test_run_tasks_caps_tasks_in_flight():
    lock = threading.Lock()
    submitted = 0
    done = 0
    in_flight = []

    def fn(task_args: AggTaskArgs, shared: AggSharedArgs) -> None:
        nonlocal done
        time.sleep(0.005)
        with lock:
            done += 1

    def tasks():
        nonlocal submitted
        for i in range(100):
            with lock:
                in_flight.append(submitted - done)
            submitted += 1
            yield task(i)

    with ThreadPoolExecutor(4) as executor:
        result = _run_tasks(executor, fn, tasks(), shared_args(), max_in_flight=6)
    assert result == (100, 0)
    assert max(in_flight) <= 6


def test_run_tasks_counts_and_logs_failures(caplog):
    def fn(task_args: AggTaskArgs, shared: AggSharedArgs) -> None:
        if task_args.vs30 % 4 == 1:
            raise ValueError("bad task")

    with ThreadPoolExecutor(2) as executor:
        result = _run_tasks(executor, fn, (task(i) for i in range(10)), shared_args(), max_in_flight=4)
    assert result == (10, 3)
    errors = [record for record in caplog.records if "Exception encountered" in record.getMessage()]
    assert len(errors) == 3
    assert all(len(record.getMessage()) < 500 for record in errors)

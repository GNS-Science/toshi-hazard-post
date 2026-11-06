import threading
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import product

import numpy as np
from nzshm_common.location.coded_location import CodedLocation

import toshi_hazard_post.aggregation as aggregation
from toshi_hazard_post.aggregation import _generate_agg_jobs, _run_tasks
from toshi_hazard_post.aggregation_calc import AggSharedArgs, AggTaskArgs
from toshi_hazard_post.aggregation_setup import Site


class Tracked:
    """An object that counts how many instances are alive."""

    live = 0

    def __init__(self) -> None:
        Tracked.live += 1

    def __del__(self) -> None:
        Tracked.live -= 1


def test_generate_agg_jobs_yields_every_job_in_bin_order(monkeypatch):
    lat_lons = [(-41.2, 174.8), (-41.3, 174.7), (-36.9, 174.8), (-45.9, 170.5)]  # 3 bins
    sites = [Site(CodedLocation(*ll, 0.001), vs30) for ll, vs30 in product(lat_lons, [275, 400])]
    imts = ["PGA", "SA(1.0)"]
    bins = []

    def fake_load_batch(vs30, nloc_0, compatibility_key, sources_digests, gmms_digests, imts, component_digests):
        bins.append((vs30, nloc_0))
        return (vs30, nloc_0), {}, None

    monkeypatch.setattr(aggregation, '_load_batch', fake_load_batch)
    monkeypatch.setattr(aggregation, 'get_job_probs', lambda *args: np.zeros((1, 1)))

    jobs = list(_generate_agg_jobs(sites, imts, "key", []))
    assert len(jobs) == len(sites) * len(imts)
    assert len(bins) == len(set(bins)) == 6  # each bin loaded once
    # the jobs of each bin come together, in the order the bins were loaded
    job_bins = [(t.vs30, t.location.downsample(1.0).code) for t in jobs]
    assert list(dict.fromkeys(job_bins)) == bins


def test_generate_agg_jobs_holds_one_batch_at_a_time(monkeypatch):
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
    seen = [Tracked.live for _ in _generate_agg_jobs(sites, ["PGA"], "key", [])]
    assert seen == [1] * 5
    assert live_when_loaded == [1] * 5


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

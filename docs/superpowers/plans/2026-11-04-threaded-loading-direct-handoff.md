# Threaded batch loading and direct job hand-off: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The parent loads up to K bins at once in threads, reusing one S3 filesystem, and hands each job's data to its worker as an array ordered by component index, with at most 2 × workers tasks in flight. The ORC round trip is gone. Output stays bit-identical to `main`.

**Architecture:** `data.py` gets one cached S3 filesystem per process, a per-row component id for each batch, and `get_job_probs()`, which returns a job's `(component, level)` float32 probabilities in component order. `aggregation.py` gets a generic bounded `_prefetch()` used by `_generate_agg_jobs()`, and a `_run_tasks()` submit loop with a cap on tasks in flight. `aggregation_calc.calc_aggregation()` takes the array straight from `AggTaskArgs.probs`.

**Tech Stack:** Python 3.10–3.12, pyarrow, numpy, numba, `concurrent.futures`, pytest, `uv`.

**Spec:** `docs/superpowers/specs/2026-11-04-threaded-loading-direct-handoff-design.md`

## Global Constraints

- Output is bit-identical to `main` (exact `np.array_equal`), not within a tolerance.
- New env var `THP_NUM_LOAD_THREADS`, default **4**, read in `local_config.py` like `THP_NUM_WORKERS`.
- At most K bin tables in memory at once, **counting the one being sliced**. K=1 behaves like today's serial loading.
- At most `2 * NUM_WORKERS` tasks in flight.
- Parent slicing + ordering ≤ **3 ms per job**, measured.
- No hand-written queues, locks, condition variables or producer/consumer classes in product code. Use `concurrent.futures` primitives and `collections.deque`. Tests may use locks.
- Keep the log lines `scripts/calc_time_v2.py` parses, with their wording: `starting %d calculations with %d workers`, `time to perform one aggregation %0.2f seconds`, `time to perform aggregations after job setup %0.3f`, and the `THERE ARE n FAILED JOBS` print.
- `THP_WORKING_DIR` / `WORKING_DIR` are removed; the CHANGELOG notes it.
- Code must not set `AWS_EC2_METADATA_DISABLED`; only the docs mention it.
- Ruff: line length 120, `quote-style = "preserve"`, rules `E,F,I,B,UP,G004` (no f-strings in logging). mypy covers `tests/` too. Always use `uv run`; `make test` must pass before each commit.
- Out of scope: disaggregations / chunking levels, #98, #99, #105, pyarrow I/O pool size. Clear leftover shared memory during test runs with `rm /dev/shm/weights_shm /dev/shm/branch_index_table_shm`.

## Review Focus

1. **A load fails partway through a run.** The error must propagate from `_generate_agg_jobs()` and stop the run, without waiting for the other in-flight loads to finish: `_prefetch` cancels pending futures on exit. Covered by `test_prefetch_raises_at_the_failed_items_turn` and `test_prefetch_stops_early_without_waiting_for_pending_loads` (Task 2).
2. **A failed task's log line contains a 160 KB array.** `AggTaskArgs.__repr__` must leave out the data. Covered by `test_task_args_repr_leaves_out_probs` (Task 4).
3. **`THP_RLZ_DIR` with a trailing slash** (the commented-out `.env` example is `s3://ths-dataset-prod/NZSHM22_RLZ/`) gives `.../NZSHM22_RLZ//vs30=400`. The path must be normalised the way `configure_output()` does it. Covered by a parametrised case in `test_s3_target_path_matches_configure_output` (Task 1).
4. **`THP_NUM_LOAD_THREADS=0`, a negative value or a non-number** must raise a clear `ValueError` at start-up, not hang or silently run serially. Covered by `test_num_load_threads_must_be_positive` (Task 2).
5. **Fewer bins than K, or zero bins** (e.g. one site). `_prefetch` must not hang and must yield everything. Covered by `test_prefetch_more_threads_than_items` and `test_prefetch_no_items` (Task 2).

---

### Task 1: One S3 filesystem per process

**Files:**
- Modify: `toshi_hazard_post/data.py`: `get_realizations_dataset` (line ~228) and `save_aggregations` (line ~199), which both call `pyarrow_dataset.configure_output`.
- Test: `tests/test_data.py`

**Interfaces:**
- Produces: `data.resolve_target(target: str) -> tuple[str, pyarrow.fs.FileSystem]`. It returns the same path as `toshi_hazard_store.model.pyarrow.pyarrow_dataset.configure_output(target)`. For `s3://` targets the filesystem is one `fs.S3FileSystem(region=pyarrow_dataset.REGION)` per process, from a `functools.cache`d zero-argument helper `_s3_filesystem()`. For local targets it is `fs.LocalFileSystem()` and `str(Path(target).resolve())`.

- [ ] **Step 1: Write the failing tests** in `tests/test_data.py`, with a fake `S3FileSystem` class that counts constructions. Monkeypatch `pyarrow.fs.S3FileSystem`: it's the same module object that toshi-hazard-store uses. Call `data._s3_filesystem.cache_clear()` before and after.
  - `test_s3_filesystem_is_built_once`: two `resolve_target("s3://bucket/rlz/vs30=400/nloc_0=-39.0~174.0")` calls plus one `resolve_target("s3://bucket/agg")` → constructions == 1, and the same object is returned.
  - `test_s3_target_path_matches_configure_output`, parametrised over `"s3://bucket/rlz/vs30=400/nloc_0=-39.0~174.0"`, `"s3://bucket/rlz//vs30=400"` and `"s3://bucket/rlz/"`: `resolve_target(t)[0] == pyarrow_dataset.configure_output(t)[0]`.
  - `test_local_target`: `resolve_target(str(tmp_path / "x"))` == `(str((tmp_path / "x").resolve()), <LocalFileSystem>)`.
- [ ] **Step 2: Run them** with `uv run pytest tests/test_data.py -k "s3_ or local_target" -v`. Expected: FAIL, `resolve_target` not defined.
- [ ] **Step 3: Implement `resolve_target` and `_s3_filesystem`** in `data.py`. Normalise the S3 path the way `configure_output` does (`s3path.PureS3Path.from_uri(...)` with the leading `/` stripped). If `s3path` isn't a declared dependency, use `posixpath.normpath` on the part after `s3://` and let the parametrised test prove they match. Replace both `pyarrow_dataset.configure_output(...)` calls in `data.py` with `resolve_target(...)`. The second call, in `save_aggregations`, isn't in the spec: see the note at the end of this plan.
- [ ] **Step 4: Run** `uv run pytest tests/test_data.py tests/test_end_to_end.py -v`. Expected: PASS.
- [ ] **Step 5: Commit** after `make test` passes: `perf: build one S3 filesystem per process instead of one per dataset open (#97)`.

### Task 2: `THP_NUM_LOAD_THREADS` and bounded threaded bin loading

**Files:**
- Modify: `toshi_hazard_post/local_config.py` (next to `NUM_WORKERS`, line ~41)
- Modify: `toshi_hazard_post/aggregation.py`: `_generate_agg_jobs` and its call in `run_aggregation`
- Test: `tests/test_task_generator.py` (replace the skipped WIP test), `tests/test_local_config.py` (new)

**Interfaces:**
- Consumes: `data.get_realizations_dataset`, `data.get_batch_table`, `data.group_batch_rows` (unchanged).
- Produces:
  - `local_config.NUM_LOAD_THREADS: int`, from `THP_NUM_LOAD_THREADS`, default `DEFAULT_NUM_LOAD_THREADS = 4`. A value that isn't an integer ≥ 1 raises `ValueError` naming the variable.
  - `aggregation._prefetch(load: Callable[[T], R], items: Iterable[T], n: int) -> Generator[R, None, None]`: yields `load(item)` in item order, running loads in a `ThreadPoolExecutor(max_workers=n)`.
  - `aggregation._load_batch(vs30: int, nloc_0: str, compatibility_key: str, sources_digests: list[str], gmms_digests: list[str], imts: list[str]) -> tuple[pa.Table, dict[tuple[str, str], npt.NDArray]]`: the work each loader thread does. Task 4 adds the component row ids to its return value.
  - `_generate_agg_jobs(sites, imts, compatibility_key, component_branches, n_load_threads: int)`: same yields as today. `run_aggregation` passes `NUM_LOAD_THREADS`.

`_prefetch` holds at most n results, counting the one the caller is using. This only works if the caller drops its reference before asking for the next one, so `_generate_agg_jobs` must `del` the batch table and job rows at the end of each bin. The algorithm:

```python
executor = ThreadPoolExecutor(max_workers=n)
pending = deque(executor.submit(load, item) for item in islice(it, n))
try:
    while pending:
        result = pending.popleft().result()
        yield result
        del result
        pending.extend(executor.submit(load, item) for item in islice(it, 1))
finally:
    executor.shutdown(wait=False, cancel_futures=True)
```

Build the bin list `(vs30, nloc_0, location_bin)` up front, in the order the loops produce today, so the job order is unchanged. Each load runs `get_realizations_dataset` + `get_batch_table` + `group_batch_rows`. Slicing stays in the generator.

- [ ] **Step 1: Write the failing tests.**
  - `tests/test_local_config.py`:
    - `test_num_load_threads_default`: env var unset → `importlib.reload(local_config).NUM_LOAD_THREADS == 4`.
    - `test_num_load_threads_from_env`: `"2"` → 2.
    - `test_num_load_threads_must_be_positive`, parametrised over `"0"`, `"-1"`, `"x"` → `pytest.raises(ValueError, match="THP_NUM_LOAD_THREADS")`.
    - Reload again in teardown with the env var restored.
  - `tests/test_task_generator.py`:
    - `test_prefetch_preserves_order`: `list(_prefetch(lambda x: x * 2, range(10), 3)) == [x * 2 for x in range(10)]`.
    - `test_prefetch_holds_at_most_n`, parametrised n ∈ {1, 2, 4}: the loader returns instances of a small class whose `__init__`/`__del__` update a live counter, and records concurrent loads with a counter under a `threading.Lock`. The consumer does `for obj in gen: seen.append(live); del obj`. Assert `max(seen) <= n` and max concurrent loads `<= n`.
    - `test_prefetch_raises_at_the_failed_items_turn`: the loader raises `ValueError` for item 2 of 5. The consumer gets 0 and 1, then `pytest.raises(ValueError)`.
    - `test_prefetch_stops_early_without_waiting_for_pending_loads`: n=2, items 0..9, each load sleeps 0.5 s except item 0. Take one item, then `gen.close()`. The close returns in < 0.4 s, and fewer than 10 loads ever started.
    - `test_prefetch_more_threads_than_items`: `list(_prefetch(f, [1], 4)) == [f(1)]`.
    - `test_prefetch_no_items`: `list(_prefetch(f, [], 4)) == []`.
    - `test_generate_agg_jobs_order_is_the_same_for_any_threads`: use the end-to-end fixtures, as `tests/test_data.py` does (`RLZ_DIR` monkeypatched to `tests/fixtures/end_to_end/rlz`, the fixture logic trees). Use sites spread over at least 2 bins that exist in the fixture data; check `fixtures/end_to_end/rlz` for which. `[(vs30, loc.code, imt) for vs30, loc, imt, *_ in _generate_agg_jobs(..., n_load_threads=k)]` is identical for k=1 and k=4. Monkeypatch `aggregation.WORKING_DIR` to `tmp_path` until Task 4 removes it.
- [ ] **Step 2: Run** `uv run pytest tests/test_local_config.py tests/test_task_generator.py -v`. Expected: FAIL, names not defined.
- [ ] **Step 3: Implement** `NUM_LOAD_THREADS`, `_prefetch` and the `_generate_agg_jobs` change. Log `"batch %d, %s"` when the generator starts slicing a bin, as today. Add a debug line for each load's start and finish in the loader.
- [ ] **Step 4: Run** `uv run pytest -v`. Expected: PASS, including `test_end_to_end.py`.
- [ ] **Step 5: Commit** after `make test` passes: `perf: load up to THP_NUM_LOAD_THREADS bins ahead in parent threads (#100 item 7)`.

### Task 3: Job probabilities ordered by component index, from the parent

**Files:**
- Modify: `toshi_hazard_post/data.py`: add the two functions below. `get_job_datatable` stays until Task 4 removes it.
- Test: `tests/test_data.py`, reusing `make_batch_table` / the `batch_table` fixture (strings, dictionary and dictionary-chunk variants).

**Interfaces:**
- Produces:
  - `data.component_row_ids(batch_datatable: pa.Table, component_digests: Sequence[str]) -> npt.NDArray[np.int64]`: for each row of the batch, the index into `component_digests` of `sources_digest + gmms_digest`, or `-1` if the pair isn't a component branch. No per-row Python: encode each digest column with `_string_ids`, map the few unique (source, gmm) pairs to an index, then index with numpy.
  - `data.get_job_probs(batch_datatable: pa.Table, job_rows: dict[tuple[str, str], npt.NDArray], component_ids: npt.NDArray, component_digests: Sequence[str], location: CodedLocation, imt: str) -> npt.NDArray`: shape `(len(component_digests), n_levels)`, dtype float32 (the dataset's `list<float>`). Row i is the probabilities for `component_digests[i]`, taken from `batch_datatable['values']` (flatten the taken list array and reshape). Raises `KeyError` with:
    - `no records found for location: {location}, imt: {imt}` when the job has no rows (as today);
    - `incorrect number of records found for location: {location}, imt: {imt}. Expected {n}, got {m}` when the count is wrong (as today, with `n = len(component_digests)`);
    - the message from today's `aggregation_calc.create_component_array` when the count is right but some components are missing, duplicated or unexpected: `expected rates for {N} component branches, got {M}: {k} component branches have no rates (first {missing[:5]}), {u} rates are duplicates or not for a component branch`.

- [ ] **Step 1: Write the failing tests.**
  - `test_component_row_ids`: on `batch_table`, an unknown pair gives -1 and the known ones give their indices. Run all three fixture variants.
  - `test_job_probs_is_the_job_in_component_order`: shuffle `component_digests`. Row i of `get_job_probs(...)` equals that job's row whose digests concatenate to `component_digests[i]`, built in the test by a plain dict lookup. Dtype is float32.
  - Port `test_job_datatable_no_records`, `test_job_datatable_incorrect_number_of_records` and `test_job_datatable_location_is_downsampled` to `get_job_probs`, with the same messages.
  - `test_job_probs_digest_mismatch`, parametrised over missing, duplicate and unexpected rows, with the same `match=` regexes as `tests/test_calc.py::test_component_array_digest_mismatch`.
- [ ] **Step 2: Run** `uv run pytest tests/test_data.py -v`. Expected: the new tests FAIL.
- [ ] **Step 3: Implement both functions.**
- [ ] **Step 4: Run** `uv run pytest tests/test_data.py -v`. Expected: PASS.
- [ ] **Step 5: Measure the per-job cost.** Write an untracked script `scripts/bench_job_probs.py` that loads bin `-39.0~174.0` (vs30=400, 16 IMTs, NSHM_v1.0.4) from `/data/toshi_hazard_store/NZSHM22_RLZ_BKUP`, times `component_row_ids` once, then times `get_job_probs` for all 944 jobs. Expected: (row ids time / 944) + median `get_job_probs` time ≤ 3 ms. If not, stop and report the numbers.
- [ ] **Step 6: Commit** after `make test` passes, without the script: `perf: take each job's probabilities from the batch in component order (#97)`.

### Task 4: Hand the array to the worker, cap tasks in flight, remove the ORC path

**Files:**
- Modify: `toshi_hazard_post/aggregation_calc.py` (`AggTaskArgs`, `AggSharedArgs`, `calc_aggregation`)
- Modify: `toshi_hazard_post/aggregation.py` (`_generate_agg_jobs` yields, `_load_batch` also computes `component_row_ids`, `run_aggregation` uses `_run_tasks`)
- Modify: `toshi_hazard_post/local_config.py` (remove `WORKING_DIR` and its docstring line)
- Modify: `toshi_hazard_post/data.py` (remove `get_job_datatable`)
- Modify: `CHANGELOG.md`, `docs/usage.md`, `CLAUDE.md`
- Test: `tests/test_calc.py`, `tests/test_task_generator.py`, `tests/test_data.py`

**Interfaces:**
- Consumes: `data.component_row_ids`, `data.get_job_probs` (Task 3); `_prefetch` and `n_load_threads` (Task 2).
- Produces:
  - `AggTaskArgs(location: CodedLocation, vs30: int, imt: str, probs: npt.NDArray)`, whose `__repr__` is `AggTaskArgs(location=<code>, vs30=<vs30>, imt=<imt>, probs=<shape> <dtype>)`.
  - `AggSharedArgs` without `component_digests`.
  - `_generate_agg_jobs(...)` yields `AggTaskArgs`.
  - `aggregation._run_tasks(executor: Executor, fn: Callable[[AggTaskArgs, AggSharedArgs], None], tasks: Iterable[AggTaskArgs], shared_args: AggSharedArgs, max_in_flight: int) -> tuple[int, int]`, returning `(num_jobs, num_failed)`. It submits while fewer than `max_in_flight` tasks are pending, and otherwise waits with `wait(pending, return_when=FIRST_COMPLETED)`. Each failure is logged as today: `log.error("Exception encountered for task args %s: %r", task_args, exception)`. `run_aggregation` calls it with `max_in_flight=2 * NUM_WORKERS`.
  - In `calc_aggregation`: `component_rates_array = calculators.prob_to_rate(task_args.probs, 1.0)`, then the rest is unchanged. No file reads or unlinks. Drop the `loading realizations from %s` log line; keep `time to perform one aggregation`.

- [ ] **Step 1: Write the failing tests.**
  - `tests/test_calc.py::test_prob_to_rate_whole_array_matches_rows`: take one job's float32 probabilities from the end-to-end fixture, via `get_job_probs` on the fixture batch. `np.array_equal(calculators.prob_to_rate(probs, 1.0), np.stack([calculators.prob_to_rate(row, 1.0) for row in probs]))`, and both are float32. This is today's per-row path.
  - `tests/test_calc.py::test_task_args_repr_leaves_out_probs`: with `probs = np.zeros((912, 44), np.float32)`, `repr(args)` contains `(912, 44)` and `float32` and is under 200 characters.
  - `tests/test_task_generator.py::test_run_tasks_caps_tasks_in_flight`: `ThreadPoolExecutor(4)`; `fn` sleeps 5 ms and increments a done counter under a lock; `tasks` is a generator that records `submitted - done` each time it's pulled, over 100 tasks with `max_in_flight=6`. Assert max ≤ 6 and the result == `(100, 0)`.
  - `tests/test_task_generator.py::test_run_tasks_counts_and_logs_failures`: `fn` raises for 3 of 10 tasks; the result == `(10, 3)`; `caplog` has 3 `Exception encountered` records and none longer than 500 characters.
  - Delete `test_convert_p2r`, `test_component_array*` and the `get_job_datatable` tests: Task 3's tests replace them.
- [ ] **Step 2: Run** `uv run pytest tests/test_calc.py tests/test_task_generator.py -v`. Expected: FAIL.
- [ ] **Step 3: Implement.** Remove `convert_probs_to_rates`, `load_realizations`, `create_component_array`, `get_job_datatable`, `WORKING_DIR`, and the `pyarrow.orc` and pandas imports that are no longer used. Drop the `WORKING_DIR` monkeypatch from Task 2's ordering test.
- [ ] **Step 4: Run** `make test`. Expected: PASS, including `test_end_to_end.py` output unchanged.
- [ ] **Step 5: Docs.**
  - `CHANGELOG.md` under `[Unreleased]`:
    - **Changed:** the threaded loading with `THP_NUM_LOAD_THREADS`; the one S3 filesystem per process; jobs handed over as arrays, with at most 2 × workers in flight; `AggTaskArgs.probs` replacing `table_filepath`.
    - **Removed:** `THP_WORKING_DIR` (no longer used), `data.get_job_datatable`, `aggregation_calc.convert_probs_to_rates`, `load_realizations`, `create_component_array`, `AggSharedArgs.component_digests`.
    - Link #97.
  - `docs/usage.md`: document `THP_NUM_LOAD_THREADS` (default 4, about 1–2 GB of parent memory per thread), and note that `AWS_EC2_METADATA_DISABLED=true` avoids an ~8 s start-up stall when reading from S3 off EC2.
  - `CLAUDE.md`: replace the ORC/`WORKING_DIR` sentences in execution-flow steps 3–4 with the threaded loading and array hand-off, and remove `THP_WORKING_DIR` from the env var list.
- [ ] **Step 6: Commit** after `make test` passes: `perf: hand job data to workers as arrays and cap tasks in flight, removing the ORC round trip (#97)`.

### Task 5: Real-run verification (local disk and S3)

**Files:** none in the repo. Everything goes in the session scratchpad (`$SCRATCH` below): the locations file, configs, the comparison script and the logs.

- [ ] **Step 1: Sites.** Write every distinct `nloc_001` at vs30=400 in the 8 bins (`-39.0~174.0`, `-38.0~176.0`, `-41.0~175.0`, `-37.0~175.0`, `-44.0~172.0`, `-46.0~169.0`, `-40.0~176.0`, `-36.0~174.0`) from `/data/toshi_hazard_store/NZSHM22_RLZ_BKUP` to `$SCRATCH/sites_8bins.csv` (`lat,lon`). Expected: 791 rows; per bin 59, 133, 82, 95, 83, 101, 129, 109.
- [ ] **Step 2: Config** `$SCRATCH/hazard_8bins.toml`: as `demo/hazard_perf.toml` (NSHM_v1.0.4, the 11 agg types, `restart` with absolute paths to `demo/NSHM_v1.0.4_{bht,weights}.npy`), but `locations_file = "$SCRATCH/sites_8bins.csv"`, `vs30s = [400]`, all 16 IMTs (the `DEFAULT_IMTS` in `scripts/bench_batch_load.py`), `skip_save = false`.
- [ ] **Step 3: Baseline checkout.** `git worktree add $SCRATCH/thp-main main`, then `uv sync` in it.
- [ ] **Step 4: Four runs**, each `systemd-run --user --scope -q -p MemoryMax=55G -p MemorySwapMax=0 env THP_NUM_WORKERS=30 THP_RLZ_DIR=<src> THP_AGG_DIR=$SCRATCH/agg_<name> uv run thp aggregate $SCRATCH/hazard_8bins.toml`, logging to `$SCRATCH/<name>.log`. Run `main` from the worktree and the branch from the repo:
  - `main-local` and `branch-local`, with `<src>=/data/toshi_hazard_store/NZSHM22_RLZ_BKUP`;
  - `main-s3` and `branch-s3`, with `<src>=s3://ths-dataset-prod/NZSHM22_RLZ`.

  Before each run, `rm -f /dev/shm/weights_shm /dev/shm/branch_index_table_shm`. Before the S3 runs, check `aws sts get-caller-identity`; if it fails, stop and ask the user to refresh the `toshi` profile. While each run goes, sample every 1 s the parent's `VmRSS` from `/proc/<pid>/status` and the machine's `MemTotal - MemAvailable`. Keep the peaks.
- [ ] **Step 5: Compare outputs.** Read each `$SCRATCH/agg_<name>` with `pyarrow.dataset` (hive partitioning), key by `(nloc_001, vs30, imt, aggr)`, and check that `np.array_equal` holds for every key's `values`. Expected: 791 × 16 × 11 = 139,216 keys in each run, and branch == main exactly for local and for S3. Also report whether local == S3.
- [ ] **Step 6: Report** a table for the four runs: total time, time after job setup (from the log line), parent peak RSS, machine peak memory, failed jobs. Expected: both branch runs < 10 min and 0 failed jobs; branch faster than main on each data source; parent peak ≤ ~8 GB. Remove the worktree afterwards (`git worktree remove $SCRATCH/thp-main`).

---

**A change from the spec to confirm (Task 1):** `save_aggregations()` also calls `configure_output()` once per task, in the worker, so with `THP_AGG_DIR` on S3 each task would pay the same 4 s stall. Task 1 points it at the same cached filesystem: one line, using the same helper. The test run saves locally, so it won't show this.

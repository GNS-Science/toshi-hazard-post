# Threaded batch loading and direct job hand-off (#100 item 7, #97)

## Why

After phase 1 of #100, a `(location, imt)` task takes ~0.55 s, and the parent's serial job preparation sets how many workers can be kept busy. Two costs dominate, measured on 8 bins at vs30=400 (12,656 jobs, see the item 6 comment on #100):

- **Loading each `(vs30, nloc_0)` bin, one at a time.** ~20 ms per job from S3. On S3 every load also pays a 4–8 s stall building a new `fs.S3FileSystem()`. That stall holds the GIL, so it also blocks other threads.
- **Handing each job to a worker through an ORC file in `WORKING_DIR`.** ~12 ms to write in the parent, then a read, a pandas conversion, a row-by-row `prob_to_rate` and a dict in the worker.

Serial loading plus the ORC hand-off can keep ~16 workers busy on S3. Loading 4 bins at once in parent threads, plus handing the data over as an array (#97), should keep ~54 busy. #98 (loading in the workers) was considered and not pursued: it costs more memory per worker and makes each task a whole bin.

## Goals

1. **The output doesn't change.** The new code must produce aggregates bit-identical to `main` for the same config.
2. **A real run is faster** on both local disk and S3, measured on the test run below. Each run of the new code must finish in under 10 minutes.
3. **One PR**, with separate commits for the three pieces below so each can be reviewed on its own.

## Anti-goals

These would make the change a failure even with correct output and better timings:

- **Memory growth beyond what's stated here.** The workers' memory must not grow. The parent's extra memory must stay proportional to the number of loader threads: about 1–2 GB per bin loading, up to ~8 GB at the default of 4 on this machine, which has 36 cores and 62 GB.
- **Complexity.** `_generate_agg_jobs()` and `run_aggregation()` must stay short and flat. No hand-written queues, locks, condition variables or producer/consumer classes. Prefer `concurrent.futures` primitives (`submit`, `wait(..., FIRST_COMPLETED)`) and a `collections.deque` of futures.

## Non-goals

- Disaggregations, including running the calculation in chunks of levels. The hand-off below must not rule them out, but nothing is built for them.
- #98, #99, #105 (shared-memory segments left behind after a crash, and concurrent runs colliding), and raising pyarrow's I/O thread pool size. During test runs, clear leftover segments with `rm /dev/shm/weights_shm /dev/shm/branch_index_table_shm`.
- Splitting a run across EC2 machines.

## The three changes

### 1. One S3 filesystem per process

`data.get_realizations_dataset()` calls toshi-hazard-store's `pyarrow_dataset.configure_output()`, which builds a new `fs.S3FileSystem(region=REGION)` on every call. In `data.py`, build the filesystem once per process and reuse it for every bin: a small memoised helper, keyed by whether the target is S3. Local paths keep using `fs.LocalFileSystem()` and the resolved absolute path, exactly as `configure_output()` does. pyarrow filesystems are safe to share across threads.

The first construction in a process still stalls for ~8 s when not on EC2. That's acceptable, and `AWS_EC2_METADATA_DISABLED=true` removes it where wanted. The docs should mention that variable; the code shouldn't set it.

### 2. Threaded bin loading in the parent

`_generate_agg_jobs()` loads up to K bins ahead in a `ThreadPoolExecutor(max_workers=K)` and yields jobs from them in the same order as today: vs30, then bin, then `(location, imt)`.

- K comes from a new environment variable **`THP_NUM_LOAD_THREADS`**, read in `local_config.py` the same way as `THP_NUM_WORKERS`. The default is **4**.
- **At most K bin tables are in memory at once, counting the one being sliced.** Submit the next load only after the current bin's jobs have all been yielded and its table dropped.
- With K=1 the behaviour and memory match today's one-bin-at-a-time loading.
- Each loader thread does `get_realizations_dataset()` + `get_batch_table()` + `group_batch_rows()`. Slicing out each job stays in the generator, on the main thread.
- An exception in a load is raised in the generator when that bin's turn comes, which stops the run, as a load failure does today. A bin with the wrong number of rows for a job still raises `KeyError` from `get_job_datatable()`, as today.

### 3. Direct hand-off with a cap on queued tasks (#97)

- `AggTaskArgs.table_filepath` is replaced by the job's data. That's a float32 probability array of shape `(n component branches, n levels)`, **already ordered by the component index** that the branch index table uses (`component_digests` order). The worker no longer reads a file, uses pandas or builds a digest dict.
- **The check moves to the parent.** Today's check in `create_component_array()`, that the job's rows are exactly the component branches with no duplicates, happens in the parent when the job is sliced, with the same error message. Building the order must not be a per-job string operation: work it out once per bin, or with vectorised integer ids as in `group_batch_rows()`. Budget: **≤ 3 ms per job** in the parent for slicing plus ordering, measured.
- **The worker converts probabilities to rates on the whole array at once**, with `calculators.prob_to_rate`. The result must be bit-identical to today's row-by-row `DataFrame.apply`: same function, same input dtype, same output dtype. If it isn't, stop and report before going further.
- **Cap on tasks in flight.** `run_aggregation()` submits up to `max_in_flight = 2 * NUM_WORKERS` tasks. When that many are pending, it waits with `wait(pending, return_when=FIRST_COMPLETED)` before submitting the next, and records failures from the finished ones as `as_completed` does today. This bounds the parent's queued data, about 10 MB for hazard, and also stops the loader threads from running far ahead of the workers.
- **Failure reporting:** a failed task is logged with its task args, as today, but those must not dump the data array: give `AggTaskArgs` a `__repr__` without it. The `THERE ARE n FAILED JOBS` line and the per-task timing log lines keep their wording, because `scripts/calc_time_v2.py` parses the logs.
- **`WORKING_DIR` and `THP_WORKING_DIR` are removed**, along with the ORC writes, reads and unlinks. The CHANGELOG notes that `THP_WORKING_DIR` is no longer used. Update `CLAUDE.md` (the execution flow and the env var list) and `docs/usage.md` if it lists the env vars. Document `THP_NUM_LOAD_THREADS` there too.

## Verification

### Tests (`make test` must pass)

- Existing tests keep passing, including `test_end_to_end.py`. Its output comparison is the first check that the output hasn't changed.
- New unit tests:
  - the bounded lookahead in `_generate_agg_jobs()`: jobs come out in the same order as with K=1, and no more than K loads are in flight or held at once (check with a fake loader that records how many it holds);
  - the cap in `run_aggregation()`: no more than `2 * NUM_WORKERS` tasks are pending at once (a fake executor or a thread executor with blocking tasks);
  - the parent-side ordering and check: correct order, plus missing, duplicate and unexpected rows raise `KeyError`;
  - the vectorised `prob_to_rate` is bit-identical to the row-by-row version on real fixture data.
- One S3 filesystem: a test that two `get_realizations_dataset()` calls on an `s3://` path build the filesystem once. Mock the constructor; no network.

### The real run

- **Config:** NSHM_v1.0.4, vs30=400, all 16 IMTs, the 11 agg types from `demo/hazard_perf.toml`, `restart` with `demo/NSHM_v1.0.4_{bht,weights}.npy` to skip the 37 s setup, and `skip_save = false` with `THP_AGG_DIR` set to a local directory per run.
- **Sites:** every distinct `nloc_001` that the realization dataset holds at vs30=400 in the 8 benchmark bins: `-39.0~174.0`, `-38.0~176.0`, `-41.0~175.0`, `-37.0~175.0`, `-44.0~172.0`, `-46.0~169.0`, `-40.0~176.0`, `-36.0~174.0`. That's 791 locations (59, 133, 82, 95, 83, 101, 129, 109 per bin) and 12,656 jobs, the same as the benchmark. Don't take them from the 0.1° grid `NZ_0_1_NB_1_1`: it has only 706 sites in these bins, because the dataset also holds sites that aren't on the grid. The locations file and the run configs stay out of the repo.
- **Data:** local `/data/toshi_hazard_store/NZSHM22_RLZ_BKUP` and `s3://ths-dataset-prod/NZSHM22_RLZ`, with `THP_NUM_WORKERS=30`.
- **Runs:** `main` once on each data source (baseline output and timings; the S3 baseline may go over 10 minutes, which is fine), then the branch once on each.
- **Output check:** every aggregate value in the branch's output equals `main`'s for the same `(location, vs30, imt, agg)`, compared exactly with `np.array_equal`. The local and S3 runs should match each other too.
- **Report:** total time, time after setup, the parent's peak RSS, and the whole machine's peak memory use, for all four runs, plus how long workers sat waiting for jobs if the logs show it. Each branch run is under 10 minutes.
- Run under `systemd-run --user --scope -p MemoryMax=...` so a mistake can't take the machine down. Refresh the AWS credentials (`toshi` profile) before the S3 runs.

## Left to the builder

- How the memoised filesystem helper looks, and whether it lives in `data.py` or wraps `configure_output()`.
- Exactly how the per-bin component order is computed, within the 3 ms per job budget.
- Whether the probability array is float32 or the dataset's own dtype; it must match what the worker converts today.
- Log wording for new messages, such as loader start and finish per bin. Existing messages stay as they are.

# Changelog

## [Unreleased]
### Changed
- Composite branch rates are built from a table of integer indices into the component branches, summed in a numba
  kernel, instead of a Python loop with digest string lookups. For NSHM_v1.0.4 this is about 24x faster per
  (location, imt) task (~5.4 s to ~0.24 s) and the shared memory table shrinks from 359 MB to 7.5 MB. Aggregates
  are unchanged apart from floating point rounding in mean, std and cov (up to 7e-14 relative).
  ([#93](https://github.com/GNS-Science/toshi-hazard-post/issues/93))
- The weighted mean and std are calculated in a numba kernel that makes two passes over each level instead of
  allocating two temporaries the size of the composite rates array. For NSHM_v1.0.4 this is about 4x faster per
  (location, imt) task (~0.41 s to ~0.10 s) and removes ~660 MB of transient memory per worker. Mean, std and cov
  change by floating point rounding only (up to 7e-15 relative).
  ([#95](https://github.com/GNS-Science/toshi-hazard-post/issues/95))
- The weighted quantiles are calculated in a numba kernel that puts the values of a level in bins and sorts only
  the values near each quantile, instead of sorting every composite branch of every level. The quantile definition
  is unchanged, including the order of equal values. For NSHM_v1.0.4 this is about 21x faster per (location, imt)
  task (~4.6 s to ~0.22 s for 8 quantiles). Quantiles change by floating point rounding only (up to 6e-11 relative),
  because the cumulative weights are summed in a different order.
  ([#94](https://github.com/GNS-Science/toshi-hazard-post/issues/94))
- The realization table of each (location, imt) job is taken from the batch table using row indices found by
  grouping the batch once, instead of filtering the whole batch table for every job. For a batch of 59 locations
  and 16 imts of NSHM_v1.0.4 (944 jobs) the parent process prepares the jobs about 17x faster (~111 s to ~6.4 s,
  8.5 to 148 jobs per second).
  ([#96](https://github.com/GNS-Science/toshi-hazard-post/issues/96))
- One S3 filesystem is built per process and shared by every dataset read and aggregation write, instead of one
  per call. Building one can take 4-8 s off EC2 (the AWS SDK probing for instance credentials) and blocks other
  threads while it does. `AWS_EC2_METADATA_DISABLED=true` removes the remaining one-off delay.
- Each job's realization probabilities are passed to its worker as an array, already ordered by component branch,
  instead of being written to and read back from an ORC file, and converted to rates in one operation. The parent
  checks that a job has exactly one row per component branch, all of the same length and with no null
  probabilities. A job that fails these checks now stops the run, where before only that task failed and the run
  went on. At most 2 x `THP_NUM_WORKERS` tasks are in flight, so jobs are not prepared far ahead of the
  workers. ([#97](https://github.com/GNS-Science/toshi-hazard-post/issues/97))
- The parent process releases the logic tree, with its cached composite branches, once the weights and branch index
  table are built, instead of holding it for the whole run. For NSHM_v1.0.4 this frees about 1.15 GB of parent memory
  (1267 MB to 105 MB RSS) while batches are loaded. Peak parent memory is unchanged, at about 1.36 GB for
  NSHM_v1.0.4: it is reached while the weights and branch index table are built, before the logic tree is released.
  ([#110](https://github.com/GNS-Science/toshi-hazard-post/issues/110))
- The weighted quantiles read each level of the composite rates twice instead of three times. The bins are set
  from bounds on each level's composite rates, worked out from the component rates, instead of a pass to find the
  smallest and largest rate, and the gathering pass works out each rate's bin again instead of storing it. The
  quantile kernel also uses 16384 bins instead of 65536, so that its bin tables take less cache. For NSHM_v1.0.4 the
  quantile step is about 1.5x faster with 18 to 30 workers (0.36 s to 0.24 s per (location, imt) task at 18). On
  real realizations, with 30 workers on an 18 core machine, a task takes 1.16 s instead of 1.41 s. Quantiles change
  by floating point rounding only (up to 4e-13 relative), because the cumulative weights are summed by bin; the
  saved float32 curves are unchanged.
  ([#107](https://github.com/GNS-Science/toshi-hazard-post/issues/107))
- Each worker writes the composite rates into one buffer kept for all its tasks, instead of new memory for every
  task, and the rates are written a block of composite branches and a level at a time. The operating system no
  longer maps and zeroes ~345 MB for every NSHM_v1.0.4 task, which took about as long as the sum itself. The
  composite rates step is about 2x faster with one worker (0.22 s to 0.11 s) and 1.55x with 18 to 30 workers. On
  real realizations a task takes 0.66 s instead of 0.76 s with 18 workers, and 1.07 s instead of 1.16 s with 30. The
  rates are bit-identical. A worker now holds the buffer between tasks, which raised peak memory by about 0.9 GB
  with 30 workers. ([#107](https://github.com/GNS-Science/toshi-hazard-post/issues/107))
- `aggregation_calc.build_branch_rates` takes an optional `out` array to write the rates into, and the new
  `calculators.composite_rates_into` writes them into a given array.
- `calculators.weighted_quantiles` and `aggregation_calc.calculate_aggs` take optional `bounds` on the values of each
  level, from the new `calculators.composite_rate_bounds`. `aggregation_calc.AggSharedArgs` has the new required
  field `branch_index_columns`, from the new `logic_tree.branch_index_columns`.
- `aggregation_calc.AggTaskArgs` has the job's probabilities in `probs` instead of `table_filepath`.
- New `data.group_batch_rows` finds the rows of each job in a batch table, for `data.get_job_probs`.
- `calculators.weighted_quantiles` takes values with shape (IMTL, branch) and returns the quantiles of every level
  with shape (quantile, IMTL), rather than taking the values of a single level.
- `calculators.weighted_avg_and_std` takes values with shape (IMTL, branch) rather than (branch, IMTL).
- `aggregation_calc.calculate_aggs` takes composite rates with shape (IMTL, branch) rather than (branch, IMTL).
- The error raised when a job's realizations do not match the component branches reports counts and the first few
  missing digests rather than every digest.
- `aggregation_calc.AggSharedArgs` has the names of the shared memory segments in the new required fields
  `weights_shm_name` and `branch_index_table_shm_name`. New `constants.SHM_NAME_PREFIX` starts each name.

### Fixed
- The shared memory segments holding the weights and branch index table are removed however a run ends, so a run
  that fails no longer makes the next one fail with `FileExistsError`. Each run names its segments
  `thp_<pid>_<random>_weights` and `..._index`, so two runs on one machine no longer collide. A run killed with
  `SIGTERM` or `SIGKILL` leaves its pool workers running, and they keep its segments in `/dev/shm` until they are
  killed. When a run stops with an error, the tasks that had already failed are logged first.
  ([#105](https://github.com/GNS-Science/toshi-hazard-post/issues/105))
- A `debug.restart` weights file that is not float64 is converted to float64 when loaded. Before, the workers read
  it as float64 regardless: a float32 file made every task fail with `buffer is too small`, and an integer file of
  the same size would have given wrong aggregates.

### Removed
- `aggregation_calc.calc_composite_rates` and `aggregation_calc.create_component_dict`.
- `THP_WORKING_DIR` is no longer used: jobs are not written to files.
- `data.get_job_datatable` (replaced by `data.get_job_probs` and `data.component_row_ids`),
  `aggregation_calc.convert_probs_to_rates`, `aggregation_calc.load_realizations`,
  `aggregation_calc.create_component_array` and `aggregation_calc.AggSharedArgs.component_digests`.
- `constants.WEIGHTS_SHM_NAME` and `constants.BRANCH_INDEX_TABLE_SHM_NAME`: each run now names its own segments.

## [0.7.4] - 2026-08-17
### Fixed
- Branch hash digests are now computed from the branch registry identity rather than looked up in the nzshm-model
  branch registry. Logic trees containing branches that are not in the published registry (e.g. newly computed
  branches) no longer raise `KeyError`; an unregistered identity emits a `UserWarning` instead. Digests for
  published models are unchanged. ([#91](https://github.com/GNS-Science/toshi-hazard-post/issues/91))

## [0.7.3] - 2026-07-16
### Changed
- Merged the `thp` CLI into the `toshi_hazard_post` package so the project builds and ships a single package. The `thp` console entry point now targets `toshi_hazard_post.cli:thp`.
- Upgraded toshi-hazard-store>=2.1.1
- deps: patch (10 pkgs), minor (30 pkgs), major: mypy 1.20.2→2.2.0, pandas 2.3.3→3.0.3, pymdown-extensions 10.21.2→11.0.1, backrefs 6.2→7.0, cryptography 47.0.0→49.0.0, readme-renderer 44.0→45.0

## [0.7.2] - 2026-05-06
### Changed
- Migrated to uv and ruff
- Upgraded dependencies

## [0.7.1] - 2025-08-18
### Added
 - Optional argument for realization dataset location to override environment variable.
 - Optional argument for parallel Executor to run_aggregation function.

### Fixed
 - time zone database on Windows needed for pyarrow.

## [0.7.0] - 2025-07-28

### Changed
- Batch load data for faster access to toshi-hazard-store
- Save realization data to file to avoid memory copy to processes
- Most timing log messages are debug instead of info level
- Use simpler concurrent.futures instead of multiprocessing
- Improved docstrings.

### Removed
- Pass config file on command line.

## [0.6.0] - 2025-06-06

### Changed
 - Use toshi-hazard-store v1 to retrieve realizations and store aggregate hazard

## [0.5.0] - 2025-03-24

### Changed
  - calculation.imts and .agg_types settings are now optional.
  - upstream nshm libraries updated to latest pypi releases.
  - minor test fixes
  - cli logging level is now INFO.
  - update toshi-hazard-store to 0.9.1 pypi release.
  - update pyarrow to allow all versions >=15.
  
### Fixed
 - Composite branch weight bug
 - Counting and indexing of aggregation types bug

## [0.4.0] - 2024-06-06

### Added
 - Documentation

### Changed
 - Complete refactor taking advantage of nzshm-model functionality
 - Significant performance improvements
 - Use toshi-hazard-post v4 tables and pyarrow / parquet database storage

### Removed
 - Cloud compute support. To be added back later
 - Disaggregation calculations. To be added back later


## [0.3.2] - 2023-08-22

### Changed
 - Use new version of toshi-hazard-store with faster, cheaper queries
 - Update demo configs and SRM logic tree

### Added
 - Transpower locations

## [0.3.1] - 2023-08-11

### Changed
 - Use new disaggregation index format
## [0.3.0] - 2023-07-24

### Added
 - Disaggregation - local and AWS
 - Gridded Hazard - local and AWS

### Changed
 - Reduced number of conversions from probability to rate and back
 - Improved parallelization
 - Use nzhsm-model classes
 - 100s of other updates
## [0.2.0] - 2022-08-03

### Added
 - A working Dockerfile for batch
 - main aggregation code ported from THS;
 - dynamodb via THS
 - runs hazard aggregration in AWS_BATCH mode

## [0.1.0] - 2022-07-20

- First version

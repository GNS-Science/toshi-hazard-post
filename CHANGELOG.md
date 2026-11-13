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
- The weighted mean, std and quantiles of each (location, imt) task are calculated straight from the component rates,
  without building the ~345 MB array of composite rates that NSHM_v1.0.4 needs. Each level takes two passes over
  the composite branches, each rate summed from the component rates as it is needed. The first pass gives the mean
  and std, from blocks of 256 branches combined afterwards, and puts each rate in one of 16384 quantile bins,
  keeping only its bin (2 bytes per branch). The bins are set from bounds on each level's rates, worked out from
  the component rates, so no pass is needed to find the smallest and largest rate. The second pass sums again only
  the rates in the bins next to each quantile. Before, the task wrote the composite rates to new memory and read
  each level back five times, so it was limited by memory bandwidth.
  - On an 18 core / 36 thread machine with NSHM_v1.0.4, a task takes 0.33 s instead of 0.79 s with 18 workers,
    and 0.51 s instead of 1.27 s with 30. Throughput levels off at 55-59 tasks per second from 18 workers, instead
    of about 24. The task now moves about 350 MiB to and from memory instead of about 2.6 GiB, and
    uses about a fifth of the memory bandwidth at 18 workers, so the limit is now the physical cores.
  - On real realizations, a task takes 0.37 s instead of 0.85 s with 18 workers and 0.67 s instead of 1.43 s with
    30, and the run 47 s instead of 77 s and 36 s instead of 70 s. Peak machine memory falls from about 22 GB to
    14 GB with 18 workers, and from about 27 GB to 14 GB with 30.
  - Quantiles change by floating point rounding only (up to 4e-13 relative), because the cumulative weights are
    summed by bin, and the saved float32 curves are unchanged. Mean and std change by rounding only (up to ~1e-15
    relative, closer to a long double reference than before), and a std much smaller than its mean is now accurate
    to ~2e-16 instead of ~9e-12. ([#107](https://github.com/GNS-Science/toshi-hazard-post/issues/107))
- New `calculators.composite_rate_aggs` and `aggregation_calc.calculate_aggs_from_components` calculate the
  aggregates from the component rates and the branch index table; `calc_aggregation` uses them.
- `calculators.weighted_quantiles` takes optional `bounds` on the values of each level, from the new
  `calculators.composite_rate_bounds`. `aggregation_calc.AggSharedArgs` has the new required
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

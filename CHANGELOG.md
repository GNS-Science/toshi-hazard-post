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
- `calculators.weighted_avg_and_std` takes values with shape (IMTL, branch) rather than (branch, IMTL).
- `aggregation_calc.calculate_aggs` takes composite rates with shape (IMTL, branch) rather than (branch, IMTL).
- The error raised when a job's realizations do not match the component branches reports counts and the first few
  missing digests rather than every digest.

### Removed
- `aggregation_calc.calc_composite_rates` and `aggregation_calc.create_component_dict`.

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

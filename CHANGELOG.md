# Changelog

## [0.7.1] - 2025-08-18
### Added
 - Optional argument for realization dataset location to override environment variable.

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
 - Disaggregation- local and AWS
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

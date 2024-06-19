# Changelog

## [0.4.0] - 2024-06-06

### Changed
 * Complete refactor taking advantage of nzshm-model functionality
 * Significant performance improvements
 * Use toshi-hazard-post v4 tables and pyarrow / parquet database storage

### Removed
 * Cloud compute support. To be added back later
 * Disaggregation calculations. Toi be added back later


## [0.3.2] - 2023-08-22

### Changed
 * Use new version of toshi-hazard-store with faster, cheaper queries
 * Update demo configs and SRM logic tree

### Added
 * Transpower locations

## [0.3.1] - 2023-08-11

### Changed
 * Use new disaggregation index format
## [0.3.0] - 2023-07-24

### Added
 * Disaggregation- local and AWS
 * Gridded Hazard - local and AWS

### Changed
 * Reduced number of conversions from probability to rate and back
 * Improved parallelization
 * Use nzhsm-model classes
 * 100s of other updates
## [0.2.0] - 2022-08-03

### Added
 * A working Dockerfile fpor batch
 * main aggregation code ported from THS;
 * dynamodb via THS
 * runs hazard aggregration in AWS_BATCH mode

## [0.1.0] - 2022-07-20

* First version

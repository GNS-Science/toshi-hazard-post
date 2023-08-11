# Use as python package in project

```
import toshi_hazard_post
```

# Run toshi-hazard-post job from the command line
```
thp --help
```

## Running in a Docker Container

```
docker build . -t toshi-hazard-post
docker run --rm -it toshi-hazard-post -s bash
```

In the docker:
```
thp --help
```

# Commands
toshi-hazard-post can be run from the command line via the `thp` script or, if installed from source by running the following command from the source directory:
```
python scripts/cli.py
```

## aggregate
```
thp aggregate CONFIG_FILE
```
`CONFIG_FILE` is a valid path to a [configuration file](#configuration-file) for hazard aggregation.

### Options
* `--help`: print help and exit
* `--mode (-m)`: `AWS_BATCH` or `LOCAL` (default). Run on local machine or launch jobs on AWS Batch service.
* `--deagg (-d)`: Calculate deaggregations rather than hazard curves.
* `--push-sns-test (-pt)`: Ask CBC
* `--migrate-tables (-m)`: Only with `--mode AWS_BATCH`. Ask CBC

### Configuration File
When running in `aggregate` mode, a configuration file is used to specify what aggregations are performed. The configuration file is in `toml` format. All entries are required unless otherwise stated.

#### aggregation
The `[aggregation]` header defines the desired parameters for the aggregation:

* `hazard_model_id`: `str` The string identifier of the model when stored by toshi-hazard-post
* `aggs`: `List[str]` List of aggregate statistics to calculate. Valid values are `"mean"`, `"std"`, `"cov"`, for mean, standard deviation, and coefficient of variation, respectively; or a string representation of a floating point number between 0 and 1, e.g. `"0.4"` for the 0.4 fractile.
* `vs30s`: `List[int]` List of vs30 values for the sites
* `imts`: `List[str]` List of intensity measure types. Valid values are e.g. `"PGA"`, `"SA(0.5)"`, etc.
* `locations`: `List[str]` List of location codes for the sites. See [location codes](#location_codes) for a description.
* `logic_tree_file`: `str` Valid path to python file defining logic tree. See [logic tree](#logic_tree) for a description.
* `gtids`: `List[str]` (only required for hazard curve aggregation, not for disaggregation) List of General Task IDs for finding the realizations to be aggregated. These IDs must have realizations that match the desired `vs30s`, `imts`, `locations`, and sources defined in `logic_tree_file`. It is possible to have multiple `gtids` to cover multiple `vs30s` or source branches in `logic_tree_file`. However, all `imts` and `locations` must be present in all realizations.
* `stride`: `int` Optional. The number of elements in the disaggregation or hazard curve array to be processed at once. This is used for memory management when calculating large arrays (particularly applicable to disaggregations run in parallel).
* `save_rlz`: `bool` Optional. Save all composite realizations to disk. Default is `false`

#### deaggregation
The `[deaggregation]` header defines parameters for aggregation disaggregations. This header is only required if running with the `--deagg` flag.

* `dimensions`: `List[str]` Dimensions along which disaggregation is to be calculated. These names must align with the oq-engine strings used to describe disaggregation (e.g. `"eps"`, `"mag"`, `"dist"`, and `"TRT"` for epsilon, magnitude, distance, and tectonic region type).
* `poes`: `List[float]` Probabilities of exceedance at which to calculate disaggregation. These are related to the `inv_time`.
* `inv_time`: `int` Time in years for which the `poes` apply. E.g. if `poes = [0.02, 0.1]` and `inv_time = 50` then disaggregations for 2% and 10% in 50 years would be calculated.
* `agg_targets`: `List[str]` List of aggregations of the hazard curve to use for finding the intensity measure level at which to perform the disaggregation. For example, `["mean", "0.5"]` would calculate disaggregations at hazard levels determined by the mean and 50th percentile curves.
* `hazard_model_target`: `str` The id of the hazard model that is to be disaggregated. This does not have to be the same as `hazard_model_id` in `[aggregation]`. You can store the disaggregation result with a different id string than the model to be disaggregated.

Note that all parameters must match disaggregation realizations already calculated and found in toshiAPI.

#### debug
The `[debug]` header defines options used when debugging code:

* `skip_save`: `bool` Set to true to avoid saving the result to toshi-hazard-store.
* `run_serial`: `bool` Set to true to run in serial mode rather than multiple aggregations in parallel.
* `location_limit`: `int` Number of locations from `locations` to process
* `source_branches_truncate`: `int` Limit number of source branches to include in logic tree for faster processing.
* `reuse_source_branches_id`: `str` Toshi ID of source branch object to re-use so the full logic tree does not have to be re-constructed.


#### Location Codes
Locations of sites can be specified in three ways:
* Latitude and longitude seperated by `~`, e.g., `"-41.1~175.56"`
* A Location id as defined in `nzshm-commmon`
* A grid name as defined in `nzshm-common`
Any combination and any number of the above formats can be used to define the site locations.

### Logic Tree
See the `nzshm-model` documentation on logic tree file format.

## build-grid


# Environment Variables
toshi-hazard-post and its dependencies use environment variables to control behavior:

## toshi-hazard-post environment variables
`NZSHM22_HAZARD_POST_WORKERS`

## toshi-hazard-store environment variables
`NZSHM22_HAZARD_STORE_STAGE`

`NZSHM22_HAZARD_STORE_REGION`

## toshi-api environment variables
`NZSHM22_TOSHI_API_KEY`

`NZSHM22_TOSHI_S3_URL`

`NZSHM22_TOSHI_API_URL`
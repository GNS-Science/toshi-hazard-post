# Usage

## Configuration

toshi-hazard-post requires some configuration to run. This can either be done via a environment variables and/or a configuration file. Environment variables will override settings in the configuration file.

- THP_NUM_WORKERS: number of parallel processes to run (default: 1)
- THP_{RLZ|AGG}_FS: the filesystem to use for the {realization or aggregate} datastore (default: "LOCAL")
- THP_{RLZ|AGG}_LOCAL_DIR: the path to the local {realization or aggregate} datastore
- THP_{RLZ|AGG}_S3_BUCKET: the S3 bucket where the {realization or aggregate} datastore is kept
- THP_{RLZ|AGG}_AWS_REGION: the AWS region for {realization or aggregate} if using S3 datastore

Values for the filesystem variables can be `"LOCAL"` or `"AWS"` indicating the parquet files are stored on a local disk or in an S3 bucket. By default, toshi-hazard-post will look for a configuration file named `.env`, though you can specify a different file on the command line with the `---config-file` option.

## Using an input file to run the calculation

The standard way to run toshi-hazard-post is to use the `thp` command.

```console
$ thp aggregate [--config-file PATH] INPUT_FILE
```

The input file is a [toml](https://toml.io/en/) file that specifies the calculation arguments:

```
[general]
compatibility_key = "A_A"
hazard_model_id = "DEMO_MODEL"

[logic_trees]
model_version = "NSHM_v1.0.4"

# alternatively, specify a path to logic tree files
# srm_file = "demo/srm_logic_tree_no_slab.json"
# gmcm_file = "demo/gmcm_logic_tree_medium.json"

[site]
vs30s = [275, 400]
locations = ["WLG", "SRWG214", "-41.000~174.700", "myfile.csv"]

[calculation]
imts = ["PGA", "SA(0.2)", "SA(0.5)", "SA(1.5)", "SA(3.0)", "SA(5.0)"]
agg_types = ["mean", "cov", "std", "0.1", "0.005", "0.01", "0.025"]
```

### `[general]`
- `compatibility_key`: this is a string used to identify entries in the realization database that were created using a compatible hazard engine, i.e. all hazard curves created with the same compatibility key can be directly compared to each other. Differences will be due to changes in e.g. location, ground motion models, sources, etc. Differences will not be due to the hazard calculation algorithm. 
- `hazard_model_id`: used to identify the model in the output, aggregation database

### `[logic_trees]`
Logic trees can be specified in one of two ways:

1. Specify an official New Zealand NSHM model defined by the `nzhsm-model` package. This will use the logic trees (both SRM and GMCM) provided by `nzshm-model`. See the [nzhsm-model package documentation](https://gns-science.github.io/nzshm-model/usage/) for details.
2. Specify a path to SRM and GMCM logic tree files. See the [nzhsm-model documentation](https://gns-science.github.io/nzshm-model/file-format/) for the file format.

### `[site]`
- `locations`: Site locations can be specified as a list of strings using the format specified for the `get_locations()` function in [`nzshm-common`](https://gns-science.github.io/nzshm-common-py).
- `vs30s`: Site conditions are specified by vs30 and are specified by a list of ints. All vs30s will be applied to every location to produce `len(vs30s) * len(locations)` sites.

Alternatively, site specific vs30 values can be applied to every site. This is done by omitting the `vs30` entry in the input file and specifying locations as a csv file. The format of the csv file is:
```
lat,lon,vs30
-37.6,175.0,400
-43.8,171.5,200
```

### `[calculation]`

- `imts`: list of strings of intensity measure types to calculate (following OpenQuake IMT naming convention)
- `agg_types`: list of strings of the statistical aggregates to calculate. Options are:
    - `mean`: weighted mean
    - `std`: weighted standard deviation
    - `cov`: weighted coeficint of variation [Meletti et al., 2021](https://doi.org/10.4401/ag-8579)
    - fractile specified by the string representation of a floating point number between 0 and 1


### Manipulating calculation arguments programmatically

Users may want to manipulate arguments in a script to facilitate easy experimentation. Here is an example of altering the logic tree and re-running a calculation:
```py
from toshi_hazard_post.aggregation_args import AggregationArgs
from toshi_hazard_post.aggregation import run_aggregation
from nzshm_model.logic_tree.correlation import LogicTreeCorrelations

# starting model
input_file = "demo/hazard_mini.toml"
args = AggregationArgs(input_file)

# run model
run_aggregation(args)

# extract logic trees
slt = args.srm_logic_tree
glt = args.gmcm_logic_tree

# modify SRM logic tree
for branch_set in slt.branch_sets:
    print(branch_set.tectonic_region_types)
slt.branch_sets = [slt.branch_sets[0]]
slt.branch_sets[0].branches = [slt.branch_sets[0].branches[0]]

# weights of branches of each branch set must sum to 1.0
slt.branch_sets[0].branches[0].weight = 1.0

# remove correlations
slt.correlations = LogicTreeCorrelations()

# modify GMCM logic tree to match the TRT of the new SRM logic tree
for branch_set in glt.branch_sets:
    print(branch_set.tectonic_region_type)
glt.branch_sets = [glt.branch_sets[1]]

# write logic trees to json for later use
slt.to_json('slt_one_branch.json')
glt.to_json('glt_crust_only.json')

args.srm_logic_tree = slt
args.gmcm_logic_tree = glt
args.hazard_model_id = 'ONE_SRM_BRANCH'
run_aggregation(args)
```
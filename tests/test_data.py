import importlib.resources as resources
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from nzshm_common.location import get_locations
from nzshm_common.location.coded_location import CodedLocation

import toshi_hazard_post.data
from toshi_hazard_post.aggregation_setup import get_logic_trees
from toshi_hazard_post.data import get_batch_table, get_job_datatable, get_realizations_dataset, group_batch_rows
from toshi_hazard_post.logic_tree import HazardLogicTree

compatibility_key = "NZSHM22"
fixture_dir = resources.files('tests.fixtures.end_to_end')
srm_filepath = str(fixture_dir / "srm_logic_tree_small.json")
gmcm_filepath = str(fixture_dir / "gmcm_logic_tree_complete.json")
vs30 = 275
location = get_locations(["WLG"])[0]
nloc_0 = location.downsample(1.0).code
imts = ["PGA"]

srm_logic_tree, gmcm_logic_tree = get_logic_trees(
    srm_logic_tree_filepath=srm_filepath, gmcm_logic_tree_filepath=gmcm_filepath
)
logic_tree = HazardLogicTree(srm_logic_tree, gmcm_logic_tree)
component_branches = logic_tree.component_branches
gmms_digests = [branch.gmcm_hash_digest for branch in component_branches]
sources_digests = [branch.source_hash_digest for branch in component_branches]
n_expected = 270


@pytest.fixture
def patch_rlz(monkeypatch):
    monkeypatch.setattr(toshi_hazard_post.data, 'RLZ_DIR', str(Path(__file__).parent / 'fixtures/end_to_end/rlz'))


def test_table_without_partition(patch_rlz):
    """We can retrieve a dataset without using the partitioning and filter on vs30 and nloc_0."""
    dataset = get_realizations_dataset()
    batch_table = get_batch_table(dataset, compatibility_key, sources_digests, gmms_digests, vs30, nloc_0, imts)
    get_job_datatable(batch_table, group_batch_rows(batch_table), location, imts[0], n_expected)


def test_table_with_partition(patch_rlz):
    """We can retrieve a dataset using the partitioning no need to filter on vs30 and nloc_0."""
    dataset = get_realizations_dataset(vs30, nloc_0)
    batch_table = get_batch_table(dataset, compatibility_key, sources_digests, gmms_digests, vs30, nloc_0, imts)
    get_job_datatable(batch_table, group_batch_rows(batch_table), location, imts[0], n_expected)


LOCATIONS = [
    CodedLocation(-41.3, 174.78, 0.001),
    CodedLocation(-36.87, 174.77, 0.001),
    CodedLocation(-40.6, 175.2, 0.001),
]
IMTS = ["PGA", "SA(0.5)"]
N_BRANCHES = 4


def make_batch_table(dictionary_encoded: bool, nchunks: int = 1) -> pa.Table:
    """A batch table of every location, imt and branch with the rows of the jobs shuffled together."""
    rows = [
        (location.code, imt, f"source{branch}", f"gmm{branch}", [float(i), float(j), float(branch)])
        for i, location in enumerate(LOCATIONS)
        for j, imt in enumerate(IMTS)
        for branch in range(N_BRANCHES)
    ]
    order = np.random.default_rng(0).permutation(len(rows))
    names = ['nloc_001', 'imt', 'sources_digest', 'gmms_digest', 'values']
    tables = []
    for chunk in np.array_split(order, nchunks):
        columns = [pa.array([rows[i][c] for i in chunk]) for c in range(4)]
        if dictionary_encoded:
            # each chunk has its own dictionary, as when the batch is read from more than one file
            columns = [column.dictionary_encode() for column in columns]
        columns.append(pa.array([rows[i][4] for i in chunk], type=pa.list_(pa.float32())))
        tables.append(pa.table(columns, names=names))
    return pa.concat_tables(tables, promote_options="permissive")


@pytest.fixture(params=[(False, 1), (True, 1), (True, 3)], ids=["strings", "dictionary", "dictionary chunks"])
def batch_table(request):
    return make_batch_table(*request.param)


def test_group_batch_rows(batch_table):
    job_rows = group_batch_rows(batch_table)

    assert set(job_rows.keys()) == {(location.code, imt) for location in LOCATIONS for imt in IMTS}
    nloc_001 = batch_table['nloc_001'].cast(pa.string()).to_pylist()
    imt = batch_table['imt'].cast(pa.string()).to_pylist()
    for (job_location, job_imt), rows in job_rows.items():
        assert len(rows) == N_BRANCHES
        # the rows of a job are in the order they have in the batch table
        assert rows.tolist() == sorted(rows.tolist())
        assert all(nloc_001[row] == job_location and imt[row] == job_imt for row in rows)


def test_group_batch_rows_of_empty_table(batch_table):
    assert group_batch_rows(batch_table.slice(0, 0)) == {}


@pytest.mark.parametrize("location", LOCATIONS)
@pytest.mark.parametrize("imt", IMTS)
def test_job_datatable_is_the_rows_of_the_job(batch_table, location, imt):
    job_table = get_job_datatable(batch_table, group_batch_rows(batch_table), location, imt, N_BRANCHES)

    # the workers add the digest columns together, which needs strings rather than dictionary encoded strings
    assert job_table.schema.names == ['sources_digest', 'gmms_digest', 'values']
    assert job_table.schema.field('sources_digest').type == pa.string()
    assert job_table.schema.field('gmms_digest').type == pa.string()
    expected = batch_table.filter((pc.field('imt') == imt) & (pc.field('nloc_001') == location.code))
    assert job_table['sources_digest'].to_pylist() == expected['sources_digest'].cast(pa.string()).to_pylist()
    assert job_table['gmms_digest'].to_pylist() == expected['gmms_digest'].cast(pa.string()).to_pylist()
    assert job_table['values'].to_pylist() == expected['values'].to_pylist()


def test_job_datatable_location_is_downsampled(batch_table):
    location = CodedLocation(-41.3001, 174.7801, 0.0001)
    job_table = get_job_datatable(batch_table, group_batch_rows(batch_table), location, "PGA", N_BRANCHES)
    assert len(job_table) == N_BRANCHES


@pytest.mark.parametrize(
    "location,imt",
    [(CodedLocation(-45.0, 170.0, 0.001), "PGA"), (LOCATIONS[0], "SA(3.0)")],
    ids=["location", "imt"],
)
def test_job_datatable_no_records(batch_table, location, imt):
    with pytest.raises(KeyError, match="no records found"):
        get_job_datatable(batch_table, group_batch_rows(batch_table), location, imt, N_BRANCHES)


def test_job_datatable_incorrect_number_of_records(batch_table):
    with pytest.raises(KeyError, match=f"Expected {N_BRANCHES + 1}, got {N_BRANCHES}"):
        get_job_datatable(batch_table, group_batch_rows(batch_table), LOCATIONS[0], "PGA", N_BRANCHES + 1)

import importlib.resources as resources
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs
import pytest
from nzshm_common.location import get_locations
from nzshm_common.location.coded_location import CodedLocation
from toshi_hazard_store.model.pyarrow import pyarrow_dataset

import toshi_hazard_post.data
from toshi_hazard_post.aggregation_setup import get_logic_trees
from toshi_hazard_post.data import (
    component_row_ids,
    get_batch_table,
    get_job_probs,
    get_realizations_dataset,
    group_batch_rows,
    resolve_target,
)
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
    component_digests = logic_tree.component_digests
    component_ids = component_row_ids(batch_table, component_digests)
    probs = get_job_probs(
        batch_table, group_batch_rows(batch_table), component_ids, component_digests, location, imts[0]
    )
    assert probs.shape[0] == n_expected


def test_table_with_partition(patch_rlz):
    """We can retrieve a dataset using the partitioning no need to filter on vs30 and nloc_0."""
    dataset = get_realizations_dataset(vs30, nloc_0)
    batch_table = get_batch_table(dataset, compatibility_key, sources_digests, gmms_digests, vs30, nloc_0, imts)
    component_digests = logic_tree.component_digests
    component_ids = component_row_ids(batch_table, component_digests)
    probs = get_job_probs(
        batch_table, group_batch_rows(batch_table), component_ids, component_digests, location, imts[0]
    )
    assert probs.shape[0] == n_expected


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


class FakeS3FileSystem:
    constructions = 0

    def __init__(self, **kwargs):
        FakeS3FileSystem.constructions += 1


@pytest.fixture
def fake_s3(monkeypatch):
    # toshi-hazard-store and toshi_hazard_post.data use the same pyarrow.fs module
    monkeypatch.setattr(pyarrow.fs, 'S3FileSystem', FakeS3FileSystem)
    FakeS3FileSystem.constructions = 0
    toshi_hazard_post.data._s3_filesystem.cache_clear()
    yield
    toshi_hazard_post.data._s3_filesystem.cache_clear()


def test_s3_filesystem_is_built_once(fake_s3):
    _, fs1 = resolve_target("s3://bucket/rlz/vs30=400/nloc_0=-39.0~174.0")
    _, fs2 = resolve_target("s3://bucket/rlz/vs30=400/nloc_0=-39.0~174.0")
    _, fs3 = resolve_target("s3://bucket/agg")
    assert FakeS3FileSystem.constructions == 1
    assert fs1 is fs2 is fs3


def test_s3_filesystem_is_not_shared_with_forked_workers(fake_s3, monkeypatch):
    """A worker forked from the parent builds its own filesystem rather than using the parent's connections."""
    _, parent_fs = resolve_target("s3://bucket/rlz")
    monkeypatch.setattr(toshi_hazard_post.data.os, 'getpid', lambda: -1)
    _, worker_fs = resolve_target("s3://bucket/agg")
    assert worker_fs is not parent_fs
    assert FakeS3FileSystem.constructions == 2


@pytest.mark.parametrize(
    "target", ["s3://bucket/rlz/vs30=400/nloc_0=-39.0~174.0", "s3://bucket/rlz//vs30=400", "s3://bucket/rlz/"]
)
def test_s3_target_path_matches_configure_output(fake_s3, target):
    assert resolve_target(target)[0] == pyarrow_dataset.configure_output(target)[0]


def test_local_target(tmp_path):
    path, filesystem = resolve_target(str(tmp_path / "x"))
    assert path == str((tmp_path / "x").resolve())
    assert isinstance(filesystem, pyarrow.fs.LocalFileSystem)


COMPONENT_DIGESTS = [f"source{branch}gmm{branch}" for branch in range(N_BRANCHES)]


def job_probs(batch, location, imt, component_digests=COMPONENT_DIGESTS):
    component_ids = component_row_ids(batch, component_digests)
    return get_job_probs(batch, group_batch_rows(batch), component_ids, component_digests, location, imt)


def test_component_row_ids(batch_table):
    component_digests = ["source2gmm2", "source0gmm0", "source1gmm1"]  # source3gmm3 is not a component
    ids = component_row_ids(batch_table, component_digests)

    sources = batch_table['sources_digest'].cast(pa.string()).to_pylist()
    gmms = batch_table['gmms_digest'].cast(pa.string()).to_pylist()
    expected = [
        component_digests.index(s + g) if s + g in component_digests else -1 for s, g in zip(sources, gmms, strict=True)
    ]
    assert ids.tolist() == expected


def test_component_row_ids_of_empty_table(batch_table):
    assert len(component_row_ids(batch_table.slice(0, 0), COMPONENT_DIGESTS)) == 0


@pytest.mark.parametrize("location", LOCATIONS)
@pytest.mark.parametrize("imt", IMTS)
def test_job_probs_is_the_job_in_component_order(batch_table, location, imt):
    component_digests = [COMPONENT_DIGESTS[i] for i in [2, 0, 3, 1]]
    probs = job_probs(batch_table, location, imt, component_digests)

    job = batch_table.filter((pc.field('imt') == imt) & (pc.field('nloc_001') == location.code))
    by_digest = {
        s + g: v
        for s, g, v in zip(
            job['sources_digest'].cast(pa.string()).to_pylist(),
            job['gmms_digest'].cast(pa.string()).to_pylist(),
            job['values'].to_pylist(),
            strict=True,
        )
    }
    assert probs.dtype == np.float32
    assert probs.tolist() == [by_digest[digest] for digest in component_digests]


def test_job_probs_location_is_downsampled(batch_table):
    probs = job_probs(batch_table, CodedLocation(-41.3001, 174.7801, 0.0001), "PGA")
    assert probs.shape == (N_BRANCHES, 3)


@pytest.mark.parametrize(
    "location,imt",
    [(CodedLocation(-45.0, 170.0, 0.001), "PGA"), (LOCATIONS[0], "SA(3.0)")],
    ids=["location", "imt"],
)
def test_job_probs_no_records(batch_table, location, imt):
    with pytest.raises(KeyError, match="no records found"):
        job_probs(batch_table, location, imt)


def test_job_probs_incorrect_number_of_records(batch_table):
    with pytest.raises(KeyError, match=f"Expected {N_BRANCHES + 1}, got {N_BRANCHES}"):
        job_probs(batch_table, LOCATIONS[0], "PGA", COMPONENT_DIGESTS + ["source9gmm9"])


def single_job_table(pairs: list[tuple[str, str]], values: list | None = None) -> pa.Table:
    if values is None:
        values = [[float(i)] for i in range(len(pairs))]
    return pa.table(
        {
            'nloc_001': [LOCATIONS[0].code] * len(pairs),
            'imt': ["PGA"] * len(pairs),
            'sources_digest': [s for s, _ in pairs],
            'gmms_digest': [g for _, g in pairs],
            'values': pa.array(values, type=pa.list_(pa.float32())),
        }
    )


@pytest.mark.parametrize(
    "pairs,message",
    [
        # the right number of rows, but one is not for a component branch
        (
            [("source0", "gmm0"), ("source1", "gmm1"), ("source2", "gmm2"), ("sourceX", "gmmX")],
            r"1 component branches have no rates \(first \['source3gmm3'\]\), 1 rates are",
        ),
        # the right number of rows, but one component branch twice
        (
            [("source0", "gmm0"), ("source0", "gmm0"), ("source2", "gmm2"), ("source3", "gmm3")],
            r"1 component branches have no rates \(first \['source1gmm1'\]\), 1 rates are duplicates",
        ),
    ],
    ids=["unexpected", "duplicate"],
)
def test_job_probs_digest_mismatch(pairs, message):
    with pytest.raises(KeyError, match=message):
        job_probs(single_job_table(pairs), LOCATIONS[0], "PGA")


def test_job_probs_digest_mismatch_message_is_short():
    component_digests = [f"missing{i}" for i in range(100)]
    table = single_job_table([(f"s{i}", f"g{i}") for i in range(100)])
    with pytest.raises(KeyError) as excinfo:
        job_probs(table, LOCATIONS[0], "PGA", component_digests)
    assert len(str(excinfo.value)) < 500


COMPONENT_PAIRS = [(f"source{i}", f"gmm{i}") for i in range(N_BRANCHES)]


@pytest.mark.parametrize(
    "values,message",
    [
        ([[0.1, 0.2], [0.3, None], [0.5, 0.6], [0.7, 0.8]], r"1 null probabilities"),
        ([[0.1, 0.2], None, [0.5, 0.6], [0.7, 0.8]], r"1 component branches have no probabilities"),
    ],
    ids=["null value", "null row"],
)
def test_job_probs_nulls(values, message):
    with pytest.raises(ValueError, match=f"PGA.*{message}"):
        job_probs(single_job_table(COMPONENT_PAIRS, values), LOCATIONS[0], "PGA")


def test_job_probs_nulls_in_other_jobs_are_ignored():
    """Only the job's own rows are checked."""
    job = single_job_table(COMPONENT_PAIRS, [[0.1, 0.2]] * N_BRANCHES)
    other = single_job_table(COMPONENT_PAIRS, [[0.1, None]] * N_BRANCHES).set_column(
        1, 'imt', pa.array(["SA(1.0)"] * N_BRANCHES)
    )
    probs = job_probs(pa.concat_tables([job, other]), LOCATIONS[0], "PGA")
    assert probs.shape == (N_BRANCHES, 2)

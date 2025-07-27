from toshi_hazard_post.data import get_batch_table, get_job_datatable, get_realizations_dataset
import pytest
import toshi_hazard_post.data
from toshi_hazard_post.aggregation_setup import get_logic_trees
from toshi_hazard_post.logic_tree import HazardLogicTree
from nzshm_common.location import get_locations
from pathlib import Path
import importlib.resources as resources

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
    get_job_datatable(batch_table, location, imts[0], n_expected)


def test_table_with_partition(patch_rlz):
    """We can retrieve a dataset using the partitioning no need to filter on vs30 and nloc_0."""
    dataset = get_realizations_dataset(vs30, nloc_0)
    batch_table = get_batch_table(dataset, compatibility_key, sources_digests, gmms_digests, vs30, nloc_0, imts)
    get_job_datatable(batch_table, location, imts[0], n_expected)

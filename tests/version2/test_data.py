import pytest
from pathlib import Path
from nzshm_common.location.location import CodedLocation
from toshi_hazard_post.version2.data import ValueStore, load_realizations
from toshi_hazard_post.version2.logic_tree import HazardComponentBranch, HazardLogicTree
from nzshm_model.logic_tree import SourceBranch, GMCMBranch, SourceLogicTree, GMCMLogicTree
import numpy as np


def test_valuestore():
    values = np.linspace(0, 1, 10)
    gmcm_branch = GMCMBranch("gmcm b1", 1.0, "my_gmm", {}, "Crust")
    source_branch = SourceBranch("source b1", 1.0, tectonic_region_types=("Crust",))
    branch = HazardComponentBranch(source_branch, [gmcm_branch])
    value_store = ValueStore()
    value_store.set_values(values, branch)

    assert np.array_equal(value_store.get_values(branch), values)


@pytest.mark.skip(reason="need to mock the database")
def test_loadrlz():

    slt_filepath = Path(__file__).parent / 'fixtures/slt.json'
    gmcm_filepath = Path(__file__).parent / 'fixtures/glt.json'
    location = CodedLocation(-45.0, 170.0, 0.001)
    slt = SourceLogicTree.from_json(slt_filepath)
    glt = GMCMLogicTree.from_json(gmcm_filepath)

    logic_tree = HazardLogicTree(slt, glt)

    value_store = load_realizations(logic_tree, "PGA", location, 400, "A")

    assert (len(value_store._values)) == 36 * 3 + 9 * 2 + 3 * 2 + 1 * 2

    with pytest.raises(KeyError):
        value_store.get_values(HazardComponentBranch(SourceBranch(), [GMCMBranch()]))

    for branch in logic_tree.component_branches:
        assert isinstance(value_store.get_values(branch), np.ndarray)

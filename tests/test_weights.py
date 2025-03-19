from nzshm_model.logic_tree import SourceLogicTree, GMCMLogicTree, LogicTreeCorrelations
import importlib.resources as resources
from pathlib import Path
from toshi_hazard_post.logic_tree import HazardLogicTree
import pytest

if __name__ == "__main__":
    fixtures_dir = resources.files('fixtures.end_to_end')
else:
    fixtures_dir = resources.files('tests.fixtures.end_to_end')
srm_filepath = Path(fixtures_dir) / 'srm_logic_tree_no_slab.json'
gmcm_filepath = Path(fixtures_dir) / 'gmcm_logic_tree_medium.json'

def test_weights_nocorrelations():
    srm = SourceLogicTree.from_json(srm_filepath)
    gmcm = GMCMLogicTree.from_json(gmcm_filepath)

    srm.correlations = LogicTreeCorrelations()

    logic_tree = HazardLogicTree(srm, gmcm)

    weights = logic_tree.weights
    assert sum(weights) == pytest.approx(1.0)


def test_weights_correlations():
    srm = SourceLogicTree.from_json(srm_filepath)
    gmcm = GMCMLogicTree.from_json(gmcm_filepath)

    logic_tree = HazardLogicTree(srm, gmcm)

    weights = logic_tree.weights
    assert sum(weights) == pytest.approx(1.0)

if __name__ == "__main__":
    srm = SourceLogicTree.from_json(srm_filepath)
    gmcm = GMCMLogicTree.from_json(gmcm_filepath)

    logic_tree = HazardLogicTree(srm, gmcm)
    idx = 0
    print(logic_tree.composite_branches[idx].weight)
    for branch in logic_tree.composite_branches[idx].branches:
        print(f"source branch id: {branch.source_branch.tectonic_region_types} {branch.source_branch.branch_id}")
        print(f"source branch weight: {branch.source_branch.weight}")
        print(f"gmcm branch id {branch.gmcm_branches[0].gsim_name}")
        print(f"gmcm branch weight {branch.gmcm_branches[0].weight}")
        print("")
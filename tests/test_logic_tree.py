import math
import warnings
from pathlib import Path

import numpy as np
import pytest
from nzshm_model.branch_registry import identity_digest
from nzshm_model.logic_tree import (
    GMCMBranch,
    GMCMLogicTree,
    InversionSource,
    SourceBranch,
    SourceBranchSet,
    SourceLogicTree,
)
from nzshm_model.logic_tree.correlation import LogicTreeCorrelations

from toshi_hazard_post.logic_tree import (
    HazardComponentBranch,
    HazardLogicTree,
    _branch_hash_digest,
    build_branch_index_table,
    registry,
)

# a toshi id that is not in the nzshm-model source branch registry
UNREGISTERED_NRML_ID = "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwMDU4MFV3ZzdW"


@pytest.fixture(autouse=True)
def clear_digest_cache():
    """Digests are memoised, so the unregistered-branch warning fires only on the first miss.

    Clear the cache around every test to keep the warning assertions independent of test order.
    """
    _branch_hash_digest.cache_clear()
    yield
    _branch_hash_digest.cache_clear()


@pytest.fixture(scope='function')
def source_logic_tree():
    slt_filepath = Path(__file__).parent / 'fixtures/slt.json'
    return SourceLogicTree.from_json(slt_filepath)


@pytest.fixture(scope='function')
def gmcm_logic_tree():
    gmcm_filepath = Path(__file__).parent / 'fixtures/glt.json'
    return GMCMLogicTree.from_json(gmcm_filepath)


def test_logic_tree_trt(source_logic_tree, gmcm_logic_tree):

    # HazardLogicTree should remove gmcm branch sets that do not have TRTs used by the source logic tree
    hazard_logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    assert len(hazard_logic_tree.gmcm_logic_tree.branch_sets) == 3

    source_logic_tree.branch_sets = [
        bs for bs in source_logic_tree.branch_sets if 'Active Shallow Crust' not in bs.tectonic_region_types
    ]
    hazard_logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    assert len(hazard_logic_tree.gmcm_logic_tree.branch_sets) == 2


def test_logic_tree_weights(source_logic_tree, gmcm_logic_tree):

    hazard_logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    component_branch = list(hazard_logic_tree.component_branches)[0]
    weight_expected = component_branch.source_branch.weight * math.prod(
        [branch.weight for branch in component_branch.gmcm_branches]
    )
    assert list(hazard_logic_tree.component_branches)[0].weight == pytest.approx(weight_expected)

    assert sum(hazard_logic_tree.weights) == pytest.approx(1.0)


def test_logic_tree_branches(source_logic_tree, gmcm_logic_tree):

    # correct number of component and composite branches with correlations
    hazard_logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    assert len(list(hazard_logic_tree.component_branches)) == 36 * 3 + 9 * 2 + 3 * 2 + 1 * 2
    assert len(list(hazard_logic_tree.composite_branches)) == 36 * 9 * 3 * 2 * 2

    # correct number of component and composite branches without correlations
    source_logic_tree.correlations = LogicTreeCorrelations()
    hazard_logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    assert len(list(hazard_logic_tree.component_branches)) == 36 * 3 + 9 * 2 + 3 * 2 + 1 * 2
    assert len(list(hazard_logic_tree.composite_branches)) == 36 * 9 * 3 * 3 * 2 * 2


def test_weights_nocorrelations(source_logic_tree, gmcm_logic_tree):

    source_logic_tree.correlations = LogicTreeCorrelations()
    logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    assert sum(logic_tree.weights) == pytest.approx(1.0)


def test_weights_correlations(source_logic_tree, gmcm_logic_tree):

    logic_tree = HazardLogicTree(source_logic_tree, gmcm_logic_tree)
    assert sum(logic_tree.weights) == pytest.approx(1.0)


def test_unregistered_source_branch_computes_digest_and_warns(gmcm_logic_tree):
    """A source branch that is not in the registry gets a computed digest, not a KeyError."""
    source_logic_tree = SourceLogicTree(
        branch_sets=[
            SourceBranchSet(
                branches=[
                    SourceBranch(
                        sources=[InversionSource(nrml_id=UNREGISTERED_NRML_ID)],
                        tectonic_region_types=("Active Shallow Crust",),
                    )
                ]
            )
        ]
    )

    with pytest.warns(UserWarning, match="unregistered source branch identity"):
        component_branches = HazardLogicTree(source_logic_tree, gmcm_logic_tree).component_branches

    assert component_branches
    for branch in component_branches:
        assert branch.source_hash_digest == identity_digest(UNREGISTERED_NRML_ID)


def test_unregistered_gmcm_branch_computes_digest_and_warns():
    """A gmcm branch that is not in the registry gets a computed digest, not a KeyError."""
    source_branch = SourceBranch(
        sources=[InversionSource(nrml_id=UNREGISTERED_NRML_ID)],
        tectonic_region_types=("Active Shallow Crust",),
    )
    gmcm_branch = GMCMBranch(
        gsim_name="NotARealGSIM",
        gsim_args={"mu_branch": "Upper"},
        tectonic_region_type="Active Shallow Crust",
    )
    with pytest.warns(UserWarning, match="unregistered gmcm branch identity"):
        component_branch = HazardComponentBranch(source_branch=source_branch, gmcm_branches=(gmcm_branch,))

    assert component_branch.gmcm_hash_digest == identity_digest("NotARealGSIM(mu_branch=Upper)")


def test_registered_digests_unchanged(source_logic_tree, gmcm_logic_tree):
    """For a published logic tree the computed digests match the registry, and nothing warns."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        component_branches = HazardLogicTree(source_logic_tree, gmcm_logic_tree).component_branches

        assert component_branches
        for branch in component_branches:
            source_entry = registry.source_registry.get_by_identity(branch.source_branch.registry_identity)
            gmcm_entry = registry.gmm_registry.get_by_identity(branch.gmcm_branches[0].registry_identity)
            assert branch.source_hash_digest == source_entry.hash_digest
            assert branch.gmcm_hash_digest == gmcm_entry.hash_digest


def test_build_branch_index_table():
    branch_hash_table = [['b', 'a'], ['a', 'c'], ['c', 'c']]
    index_table = build_branch_index_table(branch_hash_table, ['a', 'b', 'c'])
    assert index_table.tolist() == [[1, 0], [0, 2], [2, 2]]
    assert np.issubdtype(index_table.dtype, np.integer)


def test_build_branch_index_table_from_array():
    # the restart option loads the branch hash table as a numpy array of strings
    branch_hash_table = np.array([['b', 'a'], ['a', 'c']])
    index_table = build_branch_index_table(branch_hash_table, ['a', 'b', 'c'])
    assert index_table.tolist() == [[1, 0], [0, 2]]


def test_build_branch_index_table_many_components():
    # more component branches than fit in a 16 bit index
    component_digests = [str(i) for i in range(70_000)]
    index_table = build_branch_index_table([['69999', '0'], ['256', '65536']], component_digests)
    assert index_table.tolist() == [[69999, 0], [256, 65536]]


def test_build_branch_index_table_unknown_digest():
    with pytest.raises(KeyError, match='x'):
        build_branch_index_table([['a', 'x']], ['a', 'b'])


def test_component_digests_index_component_branches():
    slt = SourceLogicTree.from_json(Path(__file__).parent / 'fixtures/slt.json')
    glt = GMCMLogicTree.from_json(Path(__file__).parent / 'fixtures/glt.json')
    logic_tree = HazardLogicTree(slt, glt)

    digests = logic_tree.component_digests
    assert len(digests) == len(logic_tree.component_branches)
    for i in (0, len(digests) - 1):
        branch = logic_tree.component_branches[i]
        assert digests[i] == branch.source_hash_digest + branch.gmcm_hash_digest

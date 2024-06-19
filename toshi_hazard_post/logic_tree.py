"""
Classes for the combined SRM + GMCM logic trees used to define a seismic hazard model.
"""
import copy
import logging
from dataclasses import dataclass, field
from functools import reduce
from itertools import chain, product
from operator import mul
from typing import TYPE_CHECKING, List, Tuple

import numpy as np
import nzshm_model.branch_registry

if TYPE_CHECKING:
    import numpy.typing as npt
    from nzshm_model.logic_tree import GMCMBranch, GMCMLogicTree, SourceBranch, SourceLogicTree

log = logging.getLogger(__name__)

registry = nzshm_model.branch_registry.Registry()


# this is a dataclass so that we can use asdict for the __repr__()
@dataclass
class HazardComponentBranch:
    """
    A component branch of the combined (SRM + GMCM) logic tree comprised of an srm branch and a gmcm branch. The
    HazardComposite branch is the smallest unit necessary to create a hazard curve realization.

    Parameters:
        source_branch: the source
        gmcm_branch: the ground motion model
    """

    source_branch: 'SourceBranch'
    gmcm_branches: Tuple['GMCMBranch']
    weight: float = field(init=False)
    hash_digest: str = field(init=False)

    def __post_init__(self):
        self.weight = reduce(mul, [self.source_branch.weight] + [b.weight for b in self.gmcm_branches])
        self.gmcm_branches = tuple(self.gmcm_branches)
        self.hash_digest = self.source_hash_digest + self.gmcm_hash_digest

    @property
    def registry_identity(self) -> str:
        return self.source_branch.registry_identity + '|'.join(
            [branch.registry_identity for branch in self.gmcm_branches]
        )

    # @property
    # def hash_digest(self) -> str:
    #     return self.source_hash_digest + self.gmcm_hash_digest

    @property
    def gmcm_hash_digest(self) -> str:
        assert len(self.gmcm_branches) == 1
        return registry.gmm_registry.get_by_identity(self.gmcm_branches[0].registry_identity).hash_digest

    @property
    def source_hash_digest(self) -> str:
        return registry.source_registry.get_by_identity(self.source_branch.registry_identity).hash_digest


@dataclass
class HazardCompositeBranch:
    """
    A composite branch of the combined (SRM + GMCM) logic tree.

    A HazardCompositeBranch will have multiple sources and
    multiple ground motion models and is formed by taking all combinations of branches from the branch sets. The
    HazardComposite branch is an Iterable and will return HazardComponentBranch when iterated.

    Parameters:
        branches: the source-ground motion pairs that comprise the HazardCompositeBranch
    """

    branches: List[HazardComponentBranch] = field(default_factory=list)
    weight: float = field(init=False)

    def __post_init__(self) -> None:
        self.weight = reduce(mul, [branch.weight for branch in self.branches])

    def __iter__(self) -> 'HazardCompositeBranch':
        self.__counter = 0
        return self

    def __next__(self) -> HazardComponentBranch:
        if self.__counter >= len(self.branches):
            raise StopIteration
        else:
            self.__counter += 1
            return self.branches[self.__counter - 1]


# TODO: move to nzhsm_model?
class HazardLogicTree:
    """
    The combined (SRM + GMCM) logic tree needed to define the complete hazard model.

    Parameters:
        srm_logic_tree: the source (SRM) logic tree
        gmcm_logic_tree: the ground motion (GMCM) logic tree
    """

    def __init__(self, srm_logic_tree: 'SourceLogicTree', gmcm_logic_tree: 'GMCMLogicTree') -> None:
        self.srm_logic_tree = srm_logic_tree

        # remove the TRTs from the GMCM logic tree that are not in the SRM logic tree
        # 1. find which TRTs are included in the source logic tree
        self.trts = set(chain(*[bs.tectonic_region_types for bs in self.srm_logic_tree.branch_sets]))

        # 2. make a copy of the gmcm logic tree. Eliminate any BranchSets with a TRT not included in the source tree
        self.gmcm_logic_tree = copy.deepcopy(gmcm_logic_tree)
        self.gmcm_logic_tree.branch_sets[:] = filter(
            lambda bs: bs.tectonic_region_type in self.trts, gmcm_logic_tree.branch_sets
        )

        self._composite_branches: List[HazardCompositeBranch] = []
        self._component_branches: List[HazardComponentBranch] = []

    @property
    def composite_branches(self) -> List[HazardCompositeBranch]:
        """
        Get the composite branches combining the SRM branches with the appropraite GMCM branches by matching tectonic
        region type.

        Returns:
            composite_branches: the composite branches that make up all full realizations of the complete hazard
            logic tree
        """
        if not self._composite_branches:
            self._generate_composite_branches()
        return self._composite_branches

    @property
    def component_branches(self) -> List[HazardComponentBranch]:
        """
        Get the component branches (each SRM branch with all possible GMCM branch matches)

        Returns:
            component_branches: the component branches that make up the independent realizations of the logic tree
        """
        if not self._component_branches:
            self._generate_component_branches()
        return self._component_branches

    @property
    def weights(self) -> 'npt.NDArray':
        """
        The weights for every enumerated branch (srm + gmcm) of the logic tree.

        Returns:
            weights: one dimensional array of branch weights
        """
        return np.array([branch.weight for branch in self.composite_branches])

    @property
    def branch_hash_table(self) -> List[List[str]]:
        """
        The simplist structure used to iterate though the realization hashes. Each element of the list represents a
        composite branch as a list of hashes of the component branches that make up the composite branch.

        Returns:
            hash_list: the list of composite branches, each of wich is a list of component branch hashes.
        """
        hashes = []
        for composite_branch in self.composite_branches:
            hashes.append([branch.hash_digest for branch in composite_branch])
        return hashes

    def _generate_composite_branches(self) -> None:
        log.debug("generating composite branches")
        self._composite_branches = []
        for srm_composite_branch, gmcm_composite_branch in product(
            self.srm_logic_tree.composite_branches, self.gmcm_logic_tree.composite_branches
        ):
            # for each srm component branch, find the matching GMCM branches (by TRT)
            hbranches = []
            for srm_branch in srm_composite_branch:
                trts = srm_branch.tectonic_region_types
                gmcm_branches = tuple(branch for branch in gmcm_composite_branch if branch.tectonic_region_type in trts)
                hbranches.append(HazardComponentBranch(source_branch=srm_branch, gmcm_branches=gmcm_branches))
            self._composite_branches.append(HazardCompositeBranch(hbranches))

    def _generate_component_branches(self) -> None:
        self._component_branches = []
        for srm_branch in self.srm_logic_tree:
            trts = srm_branch.tectonic_region_types
            branch_sets = [
                branch_set for branch_set in self.gmcm_logic_tree.branch_sets if branch_set.tectonic_region_type in trts
            ]
            for gmcm_branches in product(*[bs.branches for bs in branch_sets]):
                self._component_branches.append(
                    HazardComponentBranch(source_branch=srm_branch.to_branch(), gmcm_branches=gmcm_branches)
                )

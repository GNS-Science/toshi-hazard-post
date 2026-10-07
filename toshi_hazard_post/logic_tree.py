"""Classes for the combined SRM + GMCM logic trees used to define a seismic hazard model."""

from __future__ import annotations

import copy
import logging
import math
import warnings
from functools import cache
from itertools import chain, product
from typing import TYPE_CHECKING

import numpy as np
import nzshm_model.branch_registry
from nzshm_model.branch_registry import identity_digest

if TYPE_CHECKING:
    from collections.abc import Sequence

    import numpy.typing as npt
    from nzshm_model.branch_registry import BranchRegistry
    from nzshm_model.logic_tree import GMCMBranch, GMCMLogicTree, SourceBranch, SourceLogicTree

log = logging.getLogger(__name__)

registry = nzshm_model.branch_registry.Registry()


@cache
def _branch_hash_digest(branch_registry: BranchRegistry, identity: str, kind: str) -> str:
    """The hash digest of a branch identity, warning if the identity is absent from the registry.

    The registry is advisory: a hash digest is a pure function of the identity string, so an
    unregistered branch still has a well defined digest. The warning preserves the registry's
    value (traceability, catching a mistyped toshi id) without making an unpublished branch a
    hard error. Callers can escalate or silence it with warnings.filterwarnings().

    Memoised because a production logic tree holds millions of component branches drawn from only
    a handful of distinct identities: this keeps identity_digest() off the hot path and emits the
    warning once per identity rather than once per branch.

    Args:
        branch_registry: the registry to check the identity against.
        identity: the registry identity string of the branch.
        kind: the kind of branch ("source" or "gmcm"), used in the warning message.

    Returns:
        The hash digest of the identity.
    """
    try:
        branch_registry.get_by_identity(identity)
    except KeyError:
        message = f"unregistered {kind} branch identity: {identity}"
        # warnings.warn() only reaches stderr; also log so the message lands in the job log, where
        # it is the one clue that a downstream "incorrect number of records found" was a bad id.
        log.warning(message)
        # 1: here, 2: the digest property, 3: HazardComponentBranch.__init__, 4: its caller.
        warnings.warn(message, UserWarning, stacklevel=4)
    return identity_digest(identity)


def build_branch_index_table(
    branch_hash_table: Sequence[Sequence[str]] | npt.NDArray, component_digests: Sequence[str]
) -> npt.NDArray:
    """Convert the branch hash table to a table of indices into the component branches.

    Args:
        branch_hash_table: composite branches represented as a list of hashes of the component branches
        component_digests: the hash digests of the component branches. The position of a digest in this
            sequence is its index in the returned table.

    Returns:
        The index table with shape (n composite branches, n component branches per composite branch)

    Raises:
        KeyError: if the branch hash table has a digest that is not in component_digests.
    """
    hash_table = np.asarray(branch_hash_table)
    digests, inverse = np.unique(hash_table, return_inverse=True)
    index = {digest: i for i, digest in enumerate(component_digests)}
    missing = [str(digest) for digest in digests if str(digest) not in index]
    if missing:
        raise KeyError(f"branch hash table digests are not component branches: {missing}")
    dtype = np.min_scalar_type(max(len(component_digests) - 1, 0))
    lookup = np.array([index[str(digest)] for digest in digests], dtype=dtype)
    return lookup[inverse.reshape(hash_table.shape)]


class HazardComponentBranch:
    """A component branch of the combined (SRM + GMCM) logic tree comprised of an srm branch and a gmcm branch.

    The HazardComposite branch is the smallest unit necessary to create a hazard curve realization.
    """

    def __init__(self, source_branch: SourceBranch, gmcm_branches: tuple[GMCMBranch, ...]):
        """Initialize a new HazardComponentBranch object.

        Args:
            source_branch: The source branch of the composite branch.
            gmcm_branches: The GMCM branches that make up the composite branch.
        """
        self.source_branch = source_branch
        self.gmcm_branches: tuple[GMCMBranch, ...] = gmcm_branches
        self.weight = math.prod([self.source_branch.weight] + [b.weight for b in self.gmcm_branches])
        self.gmcm_branches = tuple(self.gmcm_branches)
        self.hash_digest = self.source_hash_digest + self.gmcm_hash_digest

    @property
    def registry_identity(self) -> str:
        """The registry identity of the component branch.

        The registry identity is formed as concatinations of the source and gmcm identities.
        {source branch id}{gmcm branch1}|{gmcm branch2}...
        """
        return self.source_branch.registry_identity + '|'.join(
            [branch.registry_identity for branch in self.gmcm_branches]
        )

    @property
    def gmcm_hash_digest(self) -> str:
        """The hash digest of the gmcm branch."""
        if len(self.gmcm_branches) != 1:
            raise NotImplementedError("multiple gmcm branches for a component branch is not implemented")
        return _branch_hash_digest(registry.gmm_registry, self.gmcm_branches[0].registry_identity, "gmcm")

    @property
    def source_hash_digest(self) -> str:
        """The hash digest of the source branch."""
        return _branch_hash_digest(registry.source_registry, self.source_branch.registry_identity, "source")


class HazardCompositeBranch:
    """A composite branch of the combined (SRM + GMCM) logic tree.

    A HazardCompositeBranch will have multiple sources and
    multiple ground motion models and is formed by taking all combinations of branches from the branch sets. The
    HazardComposite branch is an Iterable and will return HazardComponentBranch when iterated.
    """

    def __init__(self, branches: list[HazardComponentBranch], source_weight: float):
        """Initialize a new HazardCompositeBranch object.

        Args:
            branches: The source-ground motion pairs that comprise the HazardCompositeBranch
            source_weight: The weight of the source branch.
        """
        self.branches = branches

        # to avoid double counting gmcm branches when calculating the weight we find the unique gmcm branches
        # since GMCMBranch objects are not hashable, we cannot use set()
        gsims = []
        for branch in self.branches:
            for gsim in branch.gmcm_branches:
                if gsim not in gsims:
                    gsims.append(gsim)
        gsim_weights = [gsim.weight for gsim in gsims]
        self.weight = math.prod(gsim_weights) * source_weight

    def __iter__(self) -> HazardCompositeBranch:
        """Iterate over all HazardComponentBranches that make up the HazardCompositeBranch."""
        self.__counter = 0
        return self

    def __next__(self) -> HazardComponentBranch:
        """Get the next HazardComponentBranch."""
        if self.__counter >= len(self.branches):
            raise StopIteration
        else:
            self.__counter += 1
            return self.branches[self.__counter - 1]


# TODO: move to nzhsm_model?
class HazardLogicTree:
    """The combined (SRM + GMCM) logic tree needed to define the complete hazard model."""

    def __init__(self, srm_logic_tree: SourceLogicTree, gmcm_logic_tree: GMCMLogicTree) -> None:
        """Initialize a new HazardLogicTree object.

        Args:
            srm_logic_tree: The seismicity rate model logic tree.
            gmcm_logic_tree: The ground motion characterisation model logic tree.
        """
        self.srm_logic_tree = srm_logic_tree

        # remove the TRTs from the GMCM logic tree that are not in the SRM logic tree
        # 1. find which TRTs are included in the source logic tree
        self.trts = set(chain(*[bs.tectonic_region_types for bs in self.srm_logic_tree.branch_sets]))

        # 2. make a copy of the gmcm logic tree. Eliminate any BranchSets with a TRT not included in the source tree
        self.gmcm_logic_tree = copy.deepcopy(gmcm_logic_tree)
        self.gmcm_logic_tree.branch_sets[:] = filter(
            lambda bs: bs.tectonic_region_type in self.trts, gmcm_logic_tree.branch_sets
        )

        self._composite_branches: list[HazardCompositeBranch] = []
        self._component_branches: list[HazardComponentBranch] = []

    @property
    def composite_branches(self) -> list[HazardCompositeBranch]:
        """Get the composite branches combining the SRM branches with the appropraite GMCM branches.

        The tectonic region types will be matched between SRM and GMCM branches.

        Returns:
            composite_branches: the composite branches that make up all full realizations of the complete hazard
            logic tree
        """
        if not self._composite_branches:
            self._generate_composite_branches()
        return self._composite_branches

    @property
    def component_branches(self) -> list[HazardComponentBranch]:
        """Get the component branches (each SRM branch with all possible GMCM branch matches).

        Returns:
            component_branches: the component branches that make up the independent realizations of the logic tree
        """
        if not self._component_branches:
            self._generate_component_branches()
        return self._component_branches

    @property
    def component_digests(self) -> list[str]:
        """The hash digests of the component branches, in the same order as component_branches.

        Returns:
            component_digests: the hash digest of each component branch
        """
        return [branch.hash_digest for branch in self.component_branches]

    @property
    def weights(self) -> npt.NDArray:
        """The weights for every enumerated branch (srm + gmcm) of the logic tree.

        Returns:
            weights: one dimensional array of branch weights
        """
        return np.array([branch.weight for branch in self.composite_branches])

    @property
    def branch_hash_table(self) -> list[list[str]]:
        """The simplest structure used to iterate though the realization hashes.

        Each element of the list represents a composite branch as a list of hashes of the component branches
        that make up the composite branch.

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

            # to use the correct weight for correlated source branches we
            # must use the source branch weight which is correctly set
            self._composite_branches.append(HazardCompositeBranch(hbranches, source_weight=srm_composite_branch.weight))

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

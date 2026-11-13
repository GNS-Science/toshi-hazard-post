"""Primary functions for calculating an aggregation for a single, site, IMT, etc."""

import logging
import os
import time
from collections.abc import Sequence
from dataclasses import dataclass
from multiprocessing import shared_memory
from typing import TYPE_CHECKING

import numpy as np

import toshi_hazard_post.calculators as calculators
from toshi_hazard_post.data import save_aggregations

if TYPE_CHECKING:
    import numpy.typing as npt
    from nzshm_common.location import CodedLocation

log = logging.getLogger(__name__)


@dataclass
class AggTaskArgs:
    """The arguments for a specific aggregation task.

    A aggregation task is for a single location, vs30, imt, etc.

    Attributes:
        location: the site locaiton.
        vs30: the site vs30.
        imt: the intensity measure type.
        probs: the realization probabilities with shape (n component branches, n IMTL), where row i is for the
            component branch at position i of the branch index table.
    """

    location: 'CodedLocation'
    vs30: int
    imt: str
    probs: 'npt.NDArray'

    def __repr__(self) -> str:
        # failed tasks are logged with their arguments, so leave out the data
        return (
            f"AggTaskArgs(location={self.location.code}, vs30={self.vs30}, imt={self.imt}, "
            f"probs={self.probs.shape} {self.probs.dtype})"
        )


@dataclass
class AggSharedArgs:
    """A class to store arguments shared by multiple aggregation jobs (used for parallelization).

    Attribues:
        agg_types: the types of aggregation to perform (e.g. 'mean', '0.9', etc.).
        compatibility_key: the toshi-hazard-store compatibility key.
        hazard_model_id: the name of the model to use when storing the result.
        weights_shape: the shape of the weights array.
        branch_index_table_shape: the shape of the branch index table array.
        branch_index_table_dtype: the data type of the branch index table array.
        weights_shm_name: the name of the shared memory segment holding the weights.
        branch_index_table_shm_name: the name of the shared memory segment holding the branch index table.
        branch_index_columns: the component branches in each column of the branch index table, used to bound the
            composite rates of each level for the quantiles (see logic_tree.branch_index_columns()).
        skip_save: set to True if skipping saving the aggregations. Used when debugging to avoid writing to a database.
    """

    agg_types: list[str]
    compatibility_key: str
    hazard_model_id: str
    weights_shape: tuple[int, ...]
    branch_index_table_shape: tuple[int, ...]
    branch_index_table_dtype: str
    weights_shm_name: str
    branch_index_table_shm_name: str
    branch_index_columns: list['npt.NDArray']
    skip_save: bool


def _is_float(value: str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return False


def _quantile_points(agg_types: Sequence[str]) -> list[float]:
    """The quantiles among the aggregate statistics, in order."""
    return [float(agg) for agg in agg_types if _is_float(agg)]


def _assemble_aggs(
    agg_types: Sequence[str],
    nlevels: int,
    mean: 'npt.NDArray | None',
    std: 'npt.NDArray | None',
    quantiles: 'npt.NDArray | None',
) -> 'npt.NDArray':
    """The aggregates in the order of agg_types (agg_type, IMTL), from the mean, std and quantiles (quantile, IMTL)
    in the order of _quantile_points(). Each is None if no aggregate needs it."""
    aggs = np.empty((len(agg_types), nlevels))
    if quantiles is not None:
        aggs[[_is_float(agg) for agg in agg_types], :] = quantiles
    if mean is not None and std is not None:
        for agg, value in (("mean", mean), ("std", std), ("cov", calculators.cov(mean, std))):
            if agg in agg_types:
                aggs[list(agg_types).index(agg), :] = value
    log.debug("agg with shape %s", aggs.shape)
    return aggs


def _check_branch_index_table(branch_index_table: 'npt.NDArray', component_rates: 'npt.NDArray') -> None:
    if branch_index_table.max() >= len(component_rates):
        raise IndexError(
            f"branch index table refers to component branch {branch_index_table.max()} "
            f"but there are only {len(component_rates)} component branches"
        )


def calculate_aggs_from_components(
    branch_index_table: 'npt.NDArray',
    component_rates: 'npt.NDArray',
    weights: 'npt.NDArray',
    agg_types: Sequence[str],
    columns: Sequence['npt.NDArray'],
) -> 'npt.NDArray':
    """Calculate weighted aggregate statistics of the composite realizations from the component realizations.

    The rate of a composite realization is the sum of the rates of its component realizations. The aggregates are
    calculated without building the array of composite rates (see calculators.composite_rate_aggs()).

    Args:
        branch_index_table: composite branches represented as rows of indices into component_rates
        component_rates: component realization rates with shape (n component branches, n IMTL)
        weights: one dimensional array of weights for composite branches with dimensions (branch,)
        agg_types: the aggregate statistics to be calculated (e.g., "mean", "0.5") with dimension (agg_type,)
        columns: the component branches in each column of the branch index table (see
            logic_tree.branch_index_columns())

    Returns:
        hazard: aggregate rates array with dimension (agg_type, IMTL)

    Raises:
        IndexError: if the branch index table refers to a component branch not in component_rates.
    """
    _check_branch_index_table(branch_index_table, component_rates)
    quantile_points = _quantile_points(agg_types)
    mean, std, quantiles = calculators.composite_rate_aggs(
        branch_index_table, component_rates, weights, quantile_points, columns
    )
    return _assemble_aggs(agg_types, component_rates.shape[1], mean, std, quantiles if quantile_points else None)


def calc_aggregation(task_args: AggTaskArgs, shared_args: AggSharedArgs) -> None:
    """Calculate hazard aggregation for a single site and imt and save result.

    Args:
        task_args: The arguments fot the specific aggregation calculation.
        shared_args: The arguments shared among all workers.
    """
    time0 = time.perf_counter()
    worker_name = os.getpid()

    location = task_args.location
    vs30 = task_args.vs30
    imt = task_args.imt

    agg_types = shared_args.agg_types
    compatibility_key = shared_args.compatibility_key
    hazard_model_id = shared_args.hazard_model_id

    # the arrays below point into the shared memory but do not keep it mapped: never close() these
    # SharedMemory objects while the arrays are alive, or reading them segfaults
    branch_index_table_shm = shared_memory.SharedMemory(name=shared_args.branch_index_table_shm_name)
    branch_index_table: npt.NDArray = np.ndarray(
        shared_args.branch_index_table_shape,
        dtype=shared_args.branch_index_table_dtype,
        buffer=branch_index_table_shm.buf,
    )

    weights_shm = shared_memory.SharedMemory(name=shared_args.weights_shm_name)
    weights: npt.NDArray = np.ndarray(shared_args.weights_shape, dtype=np.float64, buffer=weights_shm.buf)

    # convert probabilities to rates
    time1 = time.perf_counter()
    component_rates_array = calculators.prob_to_rate(task_args.probs, 1.0)
    time2 = time.perf_counter()
    log.debug("worker %s: time to convert probabilities to rates %0.2f seconds", worker_name, time2 - time1)

    log.debug('worker %s: rates_table %s', worker_name, component_rates_array.shape)

    log.info("worker %s:  calculating aggregates . . . ", worker_name)
    hazard = calculate_aggs_from_components(
        branch_index_table, component_rates_array, weights, agg_types, shared_args.branch_index_columns
    )
    time3 = time.perf_counter()
    log.debug("worker %s: time to calculate aggs %0.2f seconds", worker_name, time3 - time2)

    probs = calculators.rate_to_prob(hazard, 1.0)
    if shared_args.skip_save:
        log.info("worker %s SKIPPING SAVE . . . ", worker_name)
    else:
        log.info("worker %s saving result . . . ", worker_name)
        save_aggregations(probs, location, vs30, imt, agg_types, hazard_model_id, compatibility_key)
    time4 = time.perf_counter()
    log.info("worker %s time to perform one aggregation %0.2f seconds", worker_name, time4 - time0)

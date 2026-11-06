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
    skip_save: bool


def calculate_aggs(branch_rates: 'npt.NDArray', weights: 'npt.NDArray', agg_types: Sequence[str]) -> 'npt.NDArray':
    """Calculate weighted aggregate statistics of the composite realizations.

    Args:
        branch_rates: hazard rates for every composite realization of the model with dimensions (IMTL, branch)
        weights: one dimensional array of weights for composite branches with dimensions (branch,)
        agg_types: the aggregate statistics to be calculated (e.g., "mean", "0.5") with dimension (agg_type,)

    Returns:
        hazard: aggregate rates array with dimension (agg_type, IMTL)
    """
    log.debug("branch_rates with shape %s", branch_rates.shape)
    log.debug("weights with shape %s", weights.shape)
    log.debug("agg_types %s", agg_types)

    def is_float(value):
        try:
            float(value)
            return True
        except ValueError:
            return False

    def index(lst, value):
        try:
            return lst.index(value)
        except ValueError:
            pass
        return None

    idx_mean = index(agg_types, "mean")
    idx_std = index(agg_types, "std")
    idx_cov = index(agg_types, "cov")
    idx_quantile = [is_float(agg) for agg in agg_types]
    quantile_points = [float(pt) for pt in agg_types if is_float(pt)]

    nlevels = branch_rates.shape[0]
    naggs = len(agg_types)
    aggs = np.empty((naggs, nlevels))

    if (idx_mean is not None) | (idx_std is not None) | (idx_cov is not None):
        mean, std = calculators.weighted_avg_and_std(branch_rates, weights)
        cov = calculators.cov(mean, std)
    if quantile_points:
        aggs[idx_quantile, :] = calculators.weighted_quantiles(branch_rates, weights, quantile_points)

    if idx_mean is not None:
        aggs[idx_mean, :] = mean
    if idx_std is not None:
        aggs[idx_std, :] = std
    if idx_cov is not None:
        aggs[idx_cov, :] = cov

    log.debug("agg with shape %s", aggs.shape)
    return aggs


def build_branch_rates(branch_index_table: 'npt.NDArray', component_rates: 'npt.NDArray') -> 'npt.NDArray':
    """Calculate the rate for the composite branches in the logic tree.

    The rate for a composite branch is the sum of rates of the component branches.

    Args:
        branch_index_table: composite branches represented as rows of indices into component_rates
        component_rates: component realization rates with shape (n component branches, n IMTL)

    Returns:
        The rates array with shape (n IMTL, n branches)

    Raises:
        IndexError: if the branch index table refers to a component branch not in component_rates.
    """
    if branch_index_table.max() >= len(component_rates):
        raise IndexError(
            f"branch index table refers to component branch {branch_index_table.max()} "
            f"but there are only {len(component_rates)} component branches"
        )
    return calculators.composite_rates(branch_index_table, component_rates)


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

    composite_rates = build_branch_rates(branch_index_table, component_rates_array)
    time3 = time.perf_counter()
    log.debug("worker %s: time to build_branch_rates %0.2f seconds", worker_name, time3 - time2)

    log.info("worker %s:  calculating aggregates . . . ", worker_name)
    hazard = calculate_aggs(composite_rates, weights, agg_types)
    time4 = time.perf_counter()
    log.debug("worker %s: time to calculate aggs %0.2f seconds", worker_name, time4 - time3)

    probs = calculators.rate_to_prob(hazard, 1.0)
    if shared_args.skip_save:
        log.info("worker %s SKIPPING SAVE . . . ", worker_name)
    else:
        log.info("worker %s saving result . . . ", worker_name)
        save_aggregations(probs, location, vs30, imt, agg_types, hazard_model_id, compatibility_key)
    time5 = time.perf_counter()
    log.info("worker %s time to perform one aggregation %0.2f seconds", worker_name, time5 - time0)

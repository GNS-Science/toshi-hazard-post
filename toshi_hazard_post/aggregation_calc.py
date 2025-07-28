"""Primary functions for calculating an aggregation for a single, site, IMT, etc."""

import logging
import os
import time
from dataclasses import dataclass
from multiprocessing import shared_memory
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Sequence

import numpy as np
import pyarrow.orc as orc

import toshi_hazard_post.calculators as calculators
import toshi_hazard_post.constants as constants
from toshi_hazard_post.data import save_aggregations

if TYPE_CHECKING:
    import numpy.typing as npt
    import pandas as pd
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
        table_filepath: the location of the realization data ORC format file.
    """

    location: 'CodedLocation'
    vs30: int
    imt: str
    table_filepath: Path


@dataclass
class AggSharedArgs:
    """A class to store arguments shared by multiple aggregation jobs (used for parallelization).

    Attribues:
        agg_types: the types of aggregation to perform (e.g. 'mean', '0.9', etc.).
        compatibility_key: the toshi-hazard-store compatibility key.
        hazard_model_id: the name of the model to use when storing the result.
        weights_shape: the shape of the weights array.
        branch_hash_table_shape: the shape of the branch hash table array.
        skip_save: set to True if skipping saving the aggregations. Used when debugging to avoid writing to a database.
    """

    agg_types: list[str]
    compatibility_key: str
    hazard_model_id: str
    weights_shape: tuple[int, ...]
    branch_hash_table_shape: tuple[int, ...]
    skip_save: bool


def convert_probs_to_rates(probs: 'pd.DataFrame') -> 'pd.DataFrame':
    """Convert probabilies to rates assuming probabilies are Poissonian.

    The 'values' column in the input dataframe will be used to calculate rates assuming they are probabilities
    in one year. The output dataframe will have a 'rates' column.

    Args:
        probs: the probabilities dataframe.

    Returns:
        the rates dataframe.
    """
    probs['rates'] = probs['values'].apply(calculators.prob_to_rate, inv_time=1.0)
    return probs.drop('values', axis=1)


def load_realizations(filepath: Path) -> 'pd.DataFrame':
    """Load the realizations from an Appache ORC format file.

    Args:
        filepath: the path of the ORC file.

    Returns:
        The realization data.
    """
    data_table = orc.read_table(filepath)
    return data_table.to_pandas()


def calculate_aggs(branch_rates: 'npt.NDArray', weights: 'npt.NDArray', agg_types: Sequence[str]) -> 'npt.NDArray':
    """Calculate weighted aggregate statistics of the composite realizations.

    Args:
        branch_rates: hazard rates for every composite realization of the model with dimensions (branch, IMTL)
        weights: one dimensional array of weights for composite branches with dimensions (branch,)
        agg_types: the aggregate statistics to be calculated (e.g., "mean", "0.5") with dimension (agg_type,)

    Returns:
        hazard: aggregate rates array with dimension (agg_type, IMTL)
    """
    log.debug(f"branch_rates with shape {branch_rates.shape}")
    log.debug(f"weights with shape {weights.shape}")
    log.debug(f"agg_types {agg_types}")

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

    nlevels = branch_rates.shape[1]
    naggs = len(agg_types)
    aggs = np.empty((naggs, nlevels))

    if (idx_mean is not None) | (idx_std is not None) | (idx_cov is not None):
        mean, std = calculators.weighted_avg_and_std(branch_rates, weights)
        cov = calculators.cov(mean, std)
    if quantile_points:
        #  Have not figured out a faster way to do this than a loop. Each level has an independent interpolation
        for i in range(nlevels):
            aggs[idx_quantile, i] = calculators.weighted_quantiles(branch_rates[:, i], weights, quantile_points)

    if idx_mean is not None:
        aggs[idx_mean, :] = mean
    if idx_std is not None:
        aggs[idx_std, :] = std
    if idx_cov is not None:
        aggs[idx_cov, :] = cov

    log.debug(f"agg with shape {aggs.shape}")
    return aggs


def calc_composite_rates(
    branch_hashes: list[str], component_rates: Dict[str, 'npt.NDArray'], nlevels: int
) -> 'npt.NDArray':
    """Calculate the rate for a single composite branch of the logic tree.

    The rate for a composite branch is the sum of rates of the component branches.

    Args:
        branch_hashes: the branch hashes for the component branches that comprise the composite branch
        component_rates: component realization rates keyed by component branch hash
        nlevels: the number of levels (IMTLs) in the rate array

    Returns:
        rates: hazard rates for the composite realization D(nlevels,)
    """
    # option 1, iterate and lookup on dict or pd.Series
    rates = np.zeros((nlevels,))
    for branch_hash in branch_hashes:
        rates += component_rates[branch_hash]
    return rates

    # option 2, use list comprehnsion and np.sum. Slower than 1.
    # rates = np.array([component_rates[branch.hash_digest] for branch in composite_branch])
    # return np.sum(rates, axis=0)

    # option 3, slice and sum in place using pd.Series. Very slow
    # digests = [branch.hash_digest for branch in composite_branch]
    # return component_rates[digests].sum()

    # option 4, use NDArray.sum(). Slightly slower than 1
    # return np.array([component_rates[branch.hash_digest] for branch in composite_branch]).sum(axis=0)
    # breakpoint()

    # option 5, build array and then sum. Slower than 1
    # rates = component_rates[composite_branch.branches[0].hash_digest]
    # for branch in composite_branch.branches[1:]:
    #     rates = np.vstack([rates, component_rates[branch.hash_digest]])
    # return rates.sum(axis=0)


def build_branch_rates(branch_hash_table: 'npt.NDArray', component_rates: Dict[str, 'npt.NDArray']) -> 'npt.NDArray':
    """Calculate the rate for the composite branches in the logic tree.

    Args:
        branch_hash_table: composite branches represented as a list of hashes of the component branches
        component_rates: component realization rates keyed by component branch hash

    Returns:
        The rates array with shape (n branches, n IMTL)
    """
    nimtl = len(next(iter(component_rates.values())))
    return np.array([calc_composite_rates(branch, component_rates, nimtl) for branch in branch_hash_table])


def _create_component_dict(component_rates: 'pd.DataFrame') -> Dict[str, 'npt.NDArray']:
    component_rates['digest'] = component_rates['sources_digest'] + component_rates['gmms_digest']
    component_rates.drop(['sources_digest', 'gmms_digest'], axis=1)
    component_rates.set_index('digest', inplace=True)

    return component_rates['rates'].to_dict()


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

    branch_hash_table_shm = shared_memory.SharedMemory(name=constants.BRANCH_HASH_TABLE_SHM_NAME)
    branch_hash_table: 'npt.NDArray' = np.ndarray(
        shared_args.branch_hash_table_shape, dtype='<U24', buffer=branch_hash_table_shm.buf
    )

    weights_shm = shared_memory.SharedMemory(name=constants.WEIGHTS_SHM_NAME)
    weights: 'npt.NDArray' = np.ndarray(shared_args.weights_shape, dtype=np.float64, buffer=weights_shm.buf)

    log.info("worker %s: loading realizations from %s. . ." % (worker_name, task_args.table_filepath))
    component_probs = load_realizations(task_args.table_filepath)
    log.debug("worker %s: %s rlz_table " % (worker_name, component_probs.shape))

    # convert probabilities to rates
    time1 = time.perf_counter()
    component_rates = convert_probs_to_rates(component_probs)
    del component_probs
    time2 = time.perf_counter()
    log.debug('worker %s: time to convert_probs_to_rates() % 0.2f' % (worker_name, time2 - time1))

    component_rates = _create_component_dict(component_rates)

    time3 = time.perf_counter()
    log.debug('worker %s: time to convert to dict and set digest index %0.2f seconds' % (worker_name, time3 - time2))
    log.debug('worker %s: rates_table %d' % (worker_name, len(component_rates)))

    composite_rates = build_branch_rates(branch_hash_table, component_rates)
    time4 = time.perf_counter()
    log.debug('worker %s: time to build_ranch_rates %0.2f seconds' % (worker_name, time4 - time3))

    log.info("worker %s:  calculating aggregates . . . " % worker_name)
    hazard = calculate_aggs(composite_rates, weights, agg_types)
    time5 = time.perf_counter()
    log.debug('worker %s: time to calculate aggs %0.2f seconds' % (worker_name, time5 - time4))

    probs = calculators.rate_to_prob(hazard, 1.0)
    if shared_args.skip_save:
        log.info("worker %s SKIPPING SAVE . . . " % worker_name)
    else:
        log.info("worker %s saving result . . . " % worker_name)
        save_aggregations(probs, location, vs30, imt, agg_types, hazard_model_id, compatibility_key)
    task_args.table_filepath.unlink()
    time6 = time.perf_counter()
    log.info('worker %s time to perform one aggregation %0.2f seconds' % (worker_name, time6 - time0))

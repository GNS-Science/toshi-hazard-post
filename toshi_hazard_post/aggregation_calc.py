"""
Primary functions for calculating an aggregation for a single, site, IMT, etc.
"""
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Sequence

import numpy as np

import toshi_hazard_post.calculators as calculators
from toshi_hazard_post.data import load_realizations, save_aggregations

if TYPE_CHECKING:
    import numpy.typing as npt
    import pandas as pd
    from nzshm_common.location.coded_location import CodedLocationBin

    from toshi_hazard_post.aggregation_setup import Site
    from toshi_hazard_post.logic_tree import HazardComponentBranch

log = logging.getLogger(__name__)


@dataclass
class AggTaskArgs:
    location_bin: 'CodedLocationBin'
    site: 'Site'
    imt: str


@dataclass
class AggSharedArgs:
    agg_types: List[str]
    compatibility_key: str
    hazard_model_id: str
    weights: 'npt.NDArray'
    component_branches: List['HazardComponentBranch']
    branch_hash_table: List[List[str]]


def convert_probs_to_rates(probs: 'pd.DataFrame') -> 'pd.DataFrame':
    """
    Convert probabilies to rates assuming probabilies are Poissonian
    """
    probs['rates'] = probs['values'].apply(calculators.prob_to_rate, inv_time=1.0)
    probs.drop('values', axis=1)
    return probs


def calculate_aggs(branch_rates: 'npt.NDArray', weights: 'npt.NDArray', agg_types: Sequence[str]) -> 'npt.NDArray':
    """
    Calculate weighted aggregate statistics of the composite realizations

    Parameters:
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
    quantile_points = [float(pt) for pt in filter(is_float, agg_types)]

    nlevels = branch_rates.shape[1]
    naggs = 3 + len(quantile_points)
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
    branch_hashes: List[str], component_rates: Dict[str, 'npt.NDArray'], nlevels: int
) -> 'npt.NDArray':
    """
    Calculate the rate for a single composite branch of the logic tree by summing rates of the component branches

    Parameters:
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


def build_branch_rates(branch_hash_table: List[List[str]], component_rates: Dict[str, 'npt.NDArray']) -> 'npt.NDArray':
    """
    Calculate the rate for the composite branches in the logic tree (all combination of SRM branch sets and applicable
    GMCM models).

    Output is a numpy array with dimensions (branch, IMTL)

    Parameters:
        branch_hash_table: composite branches represented as a list of hashes of the component branches
        component_rates: component realization rates keyed by component branch hash

    Returns:
        rates
    """

    nimtl = len(next(iter(component_rates.values())))
    return np.array([calc_composite_rates(branch, component_rates, nimtl) for branch in branch_hash_table])


def create_component_dict(component_rates: 'pd.DataFrame') -> Dict[str, 'npt.NDArray']:
    component_rates['digest'] = component_rates['sources_digest'] + component_rates['gmms_digest']
    component_rates.drop(['sources_digest', 'gmms_digest'], axis=1)
    component_rates.set_index('digest', inplace=True)

    return component_rates['rates'].to_dict()


def calc_aggregation(task_args: AggTaskArgs, shared_args: AggSharedArgs, worker_name: str) -> None:
    """
    Calculate hazard aggregation for a single site and imt and save result

    Parameters:
        site: location, vs30 pair
        imt: Intensity measure type (e.g. "PGA", "SA(1.5)")
        agg_types: the aggregate statistics to be calculated (e.g., "mean", "0.5")
        levels: IMTLs for the hazard curve
        weights: weights for the branches of the logic tree
        component_branches: list of the component branches that are combined to construct the full logic tree
        branch_hash_table: composite branches represented as a list of hashes of the component branches
        compatibility_key: the key identifying the hazard calculation compatibility entry
        hazard_model_id: the id of the hazard model for storing results in the database

    Returns:
        exception: the raised exception if any part of the calculation fails
    """
    site = task_args.site
    imt = task_args.imt
    location_bin = task_args.location_bin

    agg_types = shared_args.agg_types
    compatibility_key = shared_args.compatibility_key
    hazard_model_id = shared_args.hazard_model_id
    weights = shared_args.weights
    component_branches = shared_args.component_branches
    branch_hash_table = shared_args.branch_hash_table

    time0 = time.perf_counter()
    location = site.location
    vs30 = site.vs30

    log.info("worker %s: loading realizations . . ." % (worker_name))
    time1 = time.perf_counter()

    component_probs = load_realizations(imt, location, vs30, location_bin, component_branches, compatibility_key)
    time2 = time.perf_counter()
    log.debug('worker %s: time to load realizations %0.2f seconds' % (worker_name, time2 - time1))
    log.debug("worker %s: %s rlz_table " % (worker_name, component_probs.shape))

    # convert probabilities to rates
    component_rates = convert_probs_to_rates(component_probs)
    del component_probs
    time3 = time.perf_counter()
    log.debug('worker %s: time to convert_probs_to_rates() % 0.2f' % (worker_name, time3 - time2))

    component_rates = create_component_dict(component_rates)

    time4 = time.perf_counter()
    log.debug('worker %s: time to convert to dict and set digest index %0.2f seconds' % (worker_name, time4 - time3))
    log.debug('worker %s: rates_table %d' % (worker_name, len(component_rates)))

    composite_rates = build_branch_rates(branch_hash_table, component_rates)
    time5 = time.perf_counter()
    log.debug('worker %s: time to build_ranch_rates %0.2f seconds' % (worker_name, time5 - time4))

    log.info("worker %s:  calculating aggregates . . . " % worker_name)
    hazard = calculate_aggs(composite_rates, weights, agg_types)
    time6 = time.perf_counter()
    log.debug('worker %s: time to calculate aggs %0.2f seconds' % (worker_name, time6 - time5))

    log.info("worker %s saving result . . . " % worker_name)
    probs = calculators.rate_to_prob(hazard, 1.0)
    save_aggregations(probs, location, vs30, imt, agg_types, hazard_model_id, compatibility_key)
    time7 = time.perf_counter()
    log.info(
        'worker %s time to perform one aggregation after loading data %0.2f seconds' % (worker_name, time7 - time2)
    )
    log.info('worker %s time to perform one aggregation %0.2f seconds' % (worker_name, time7 - time0))
    # time.sleep(30)

    return None

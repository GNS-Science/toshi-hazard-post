"""A collection of calculation functions for hazard aggregation."""

import logging
from typing import TYPE_CHECKING, Union

import numpy as np
from numba import jit

if TYPE_CHECKING:
    import numpy.typing as npt

log = logging.getLogger(__name__)


@jit(nopython=True)
def prob_to_rate(prob: 'npt.NDArray', inv_time: float) -> 'npt.NDArray':
    """Convert probability of exceedance to rate assuming Poisson distribution.

    Args:
        prob: probability of exceedance
        inv_time: time period for probability (e.g. 1.0 for annual probability)

    Returns:
        rate: return rate in inv_time
    """
    return -np.log(1.0 - prob) / inv_time


@jit(nopython=True)
def rate_to_prob(rate: 'npt.NDArray', inv_time: float) -> 'npt.NDArray':
    """Convert rate to probabiility of exceedance assuming Poisson distribution.

    Args:
        rate: rate over inv_time
        inv_time: time period of rate (e.g. 1.0 for annual rate)

    Returns:
        prob: probability of exceedance in inv_time
    """
    return 1.0 - np.exp(-inv_time * rate)


@jit(nopython=True)
def composite_rates(branch_index_table: 'npt.NDArray', component_rates: 'npt.NDArray') -> 'npt.NDArray':
    """Calculate the rates of the composite branches by summing the rates of their component branches.

    The indices are not bounds checked, every entry of branch_index_table must be a valid row of component_rates.

    Args:
        branch_index_table: composite branches as rows of indices into component_rates (composite branch, component)
        component_rates: rates of the component branches (component branch, IMTL)

    Returns:
        rates: hazard rates for the composite branches (IMTL, composite branch)
    """
    nbranches, ncomponents = branch_index_table.shape
    nlevels = component_rates.shape[1]
    rates = np.empty((nlevels, nbranches))
    for i in range(nbranches):
        for j in range(nlevels):
            rate = 0.0
            for k in range(ncomponents):
                rate += component_rates[branch_index_table[i, k], j]
            rates[j, i] = rate
    return rates


# number of terms in each partial sum of the blocked sums
SUM_BLOCK_SIZE = 256


# The blocked sums accumulate partial sums of SUM_BLOCK_SIZE terms, which are then added together. A running
# total is only ever added to numbers of a similar size, so the rounding error grows with the number of blocks
# rather than with the number of terms.
@jit(nopython=True)
def _blocked_sum(values: 'npt.NDArray') -> float:
    """Calculate the sum of a one dimensional array."""
    total = 0.0
    for start in range(0, values.shape[0], SUM_BLOCK_SIZE):
        partial = 0.0
        for j in range(start, min(start + SUM_BLOCK_SIZE, values.shape[0])):
            partial += values[j]
        total += partial
    return total


@jit(nopython=True)
def _blocked_weighted_sum(weights: 'npt.NDArray', values: 'npt.NDArray') -> float:
    """Calculate the sum of weights * values for one dimensional arrays of the same length."""
    total = 0.0
    for start in range(0, values.shape[0], SUM_BLOCK_SIZE):
        partial = 0.0
        for j in range(start, min(start + SUM_BLOCK_SIZE, values.shape[0])):
            partial += weights[j] * values[j]
        total += partial
    return total


@jit(nopython=True)
def _blocked_weighted_sum_sq_dev(weights: 'npt.NDArray', values: 'npt.NDArray', center: float) -> float:
    """Calculate the sum of weights * (values - center)^2 for one dimensional arrays of the same length."""
    total = 0.0
    for start in range(0, values.shape[0], SUM_BLOCK_SIZE):
        partial = 0.0
        for j in range(start, min(start + SUM_BLOCK_SIZE, values.shape[0])):
            deviation = values[j] - center
            partial += weights[j] * deviation * deviation
        total += partial
    return total


@jit(nopython=True)
def weighted_avg_and_std(values: 'npt.NDArray', weights: 'npt.NDArray') -> tuple['npt.NDArray', 'npt.NDArray']:
    """Calculate weighted average and standard deviation of an array.

    Each level is reduced in two passes over its branches (the mean, then the deviations from the mean) without
    allocating any array the size of values.

    Args:
        values: array of values (IMTL, branch)
        weights: weights of values (branch, ). They do not need to sum to 1.

    Returns:
        A tuple of (mean, std) where mean is the weighted mean and
            std is the standard devaition both with size (IMTL, ).
    """
    nlevels, nbranches = values.shape
    if weights.shape[0] != nbranches:
        raise ValueError("weights must have one entry for each branch of values")
    sum_weights = _blocked_sum(weights)
    average = np.empty(nlevels)
    std = np.empty(nlevels)
    for i in range(nlevels):
        mean = _blocked_weighted_sum(weights, values[i]) / sum_weights
        # the deviations are taken from the mean rather than using E[x^2] - mean^2, which loses precision
        # when the standard deviation is much smaller than the mean
        variance = _blocked_weighted_sum_sq_dev(weights, values[i], mean) / sum_weights
        average[i] = mean
        std[i] = np.sqrt(variance)
    return (average, std)


def cov(mean: 'npt.NDArray', std: 'npt.NDArray') -> 'npt.NDArray':
    """Calculate the coeficient of variation handling zero mean by setting cov to zero.

    Args:
        mean: array of mean values
        std: array of standard deviation values

    Returns:
        cov: array of coeficient of variation values
    """
    cov = std / mean
    cov[mean == 0] = 0
    return cov


def weighted_quantiles(
    values: 'npt.NDArray', weights: 'npt.NDArray', quantiles: Union[list[float], 'npt.NDArray']
) -> 'npt.NDArray':
    """Calculate weighted quantiles of array.

    Args:
        values: values of data
        weights: weights of values. Same length as values
        quantiles: quantiles to be found. Values should be in [0,1]

    Returns:
        weighted quantiles
    """
    sorter = np.argsort(values, kind='stable')
    values = values[sorter]
    weights = weights[sorter]

    quantiles_at_values = np.cumsum(weights) - 0.5 * weights
    quantiles_at_values /= np.sum(weights)

    wq = np.interp(quantiles, quantiles_at_values, values)

    return wq

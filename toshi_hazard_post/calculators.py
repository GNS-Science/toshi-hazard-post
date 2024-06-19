"""A collection of calculation functions for hazard aggregation."""

import logging
from typing import TYPE_CHECKING, List, Tuple, Union

import numpy as np
from numba import jit

if TYPE_CHECKING:
    import numpy.typing as npt

log = logging.getLogger(__name__)


@jit(nopython=True)
def prob_to_rate(prob: 'npt.NDArray', inv_time: float) -> 'npt.NDArray':
    """Convert probability of exceedance to rate assuming Poisson distribution.

    Parameters:
        prob: probability of exceedance
        inv_time: time period for probability (e.g. 1.0 for annual probability)

    Returns:
        rate: return rate in inv_time
    """

    return -np.log(1.0 - prob) / inv_time


@jit(nopython=True)
def rate_to_prob(rate: 'npt.NDArray', inv_time: float) -> 'npt.NDArray':
    """Convert rate to probabiility of exceedance assuming Poisson distribution.

    Parameters:
        rate: rate over inv_time
        inv_time: time period of rate (e.g. 1.0 for annual rate)

    Returns:
        prob: probability of exceedance in inv_time
    """

    return 1.0 - np.exp(-inv_time * rate)


def weighted_avg_and_std(values: 'npt.NDArray', weights: 'npt.NDArray') -> Tuple['npt.NDArray', 'npt.NDArray']:
    """Calculate weighted average and standard deviation of an array.

    Parameters:
        values: array of values (branch, IMTL)
        weights: weights of values (branch, )

    Returns:
        mean: weighted mean (IMTL, )
        std: standard deviation (IMTL, )
    """
    average = np.average(values, weights=weights, axis=0)
    # Fast and numerically precise:
    variance = np.average((values - average) ** 2, weights=weights, axis=0)
    return (average, np.sqrt(variance))


def cov(mean: 'npt.NDArray', std: 'npt.NDArray') -> 'npt.NDArray':
    """
    Calculate the coeficient of variation handling zero mean by setting cov to zero

    Parameters:
        mean: array of mean values
        std: array of standard deviation values

    Returns:
        cov: array of coeficient of variation values
    """
    cov = std / mean
    cov[mean == 0] = 0
    return cov


def weighted_quantiles(
    values: 'npt.NDArray', weights: 'npt.NDArray', quantiles: Union[List[float], 'npt.NDArray']
) -> 'npt.NDArray':
    """Calculate weighted quantiles of array

    Parameters:
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

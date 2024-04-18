"""A collection of calculation functions for hazard aggregation."""

import logging
import math
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


def weighted_avg_and_std(values: 'npt.NDArray', weights: 'npt.NDArray') -> Tuple[np.double, float]:
    """Calculate weighted average and standard deviation of an array.

    Parameters:
        values: array of values
        weights: weights of values. Same length as values.

    Returns:
        mean: weighted mean
        std: standard deviation
    """

    average = np.average(values, weights=weights)
    # Fast and numerically precise:
    variance = np.average((values - average) ** 2, weights=weights)
    return (average, math.sqrt(variance))


def calculate_weighted_quantiles(
    values: 'npt.NDArray', weights: 'npt.NDArray', quantiles: Union[List[float], 'npt.NDArray']
) -> 'npt.NDArray':
    """Calculate weighed quantiles of array

    Parameters:
        values: values of data
        weights: weights of values. Same length as values
        quantiles: quantiles to be found. Values should be in [0,1]

    Returns:
        weighed_quantiles: weighed quantiles
    """

    sorter = np.argsort(values)
    values = values[sorter]
    weights = weights[sorter]

    weighted_quantiles = np.cumsum(weights) - 0.5 * weights
    weighted_quantiles /= np.sum(weights)

    wq = np.interp(quantiles, weighted_quantiles, values)

    return wq

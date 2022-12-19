import logging
import math
import time

import numpy as np

log = logging.getLogger(__name__)


def weighted_avg_and_std(values, weights):
    """
    Return the weighted average and standard deviation.

    values, weights -- Numpy ndarrays with the same shape.
    """
    average = np.average(values, weights=weights)
    # Fast and numerically precise:
    variance = np.average((values - average) ** 2, weights=weights)
    return (average, math.sqrt(variance))


def calculate_mean(sample_weight, values):

    return np.sum(sample_weight * values)


def calculate_weighted_quantiles(values, sample_weight, quantiles):
    """Very close to numpy.percentile, but supports weights.
    NOTE: quantiles should be in [0, 1]!
    :param values: numpy.array with data
    :param quantiles: array-like with many quantiles needed. Can also be string 'mean' to calculate weighted mean
    :param sample_weight: array-like of the same length as `array`
    :param values_sorted: bool, if True, then will avoid sorting of
        initial array
    :param old_style: if True, will correct output to be consistent
        with numpy.percentile.
    :return: numpy.array with computed quantiles.
    """

    sorter = np.argsort(values)
    values = values[sorter]
    sample_weight = sample_weight[sorter]

    weighted_quantiles = np.cumsum(sample_weight) - 0.5 * sample_weight
    weighted_quantiles /= np.sum(sample_weight)

    wq = np.interp(quantiles, weighted_quantiles, values)

    return wq


def weighted_quantile(values, quantiles, sample_weight=None):

    tic = time.perf_counter()

    values = np.array(values)
    if sample_weight is None:
        sample_weight = np.ones(len(values))
    sample_weight = np.array(sample_weight)
    sample_weight = sample_weight / sum(sample_weight)

    get_mean = False
    get_std = False
    get_cov = False
    if ('mean' in quantiles) | ('std' in quantiles) | ('cov' in quantiles):
        mean, std = weighted_avg_and_std(values, sample_weight)
        if 'mean' in quantiles:
            get_mean = True
            mean_ind = quantiles.index('mean')
            quantiles = quantiles[0:mean_ind] + quantiles[mean_ind + 1 :]
        if 'std' in quantiles:
            get_std = True
            std_ind = quantiles.index('std')
            quantiles = quantiles[0:std_ind] + quantiles[std_ind + 1 :]
        if 'cov' in quantiles:
            get_cov = True
            cov_ind = quantiles.index('cov')
            quantiles = quantiles[0:cov_ind] + quantiles[cov_ind + 1 :]
            cov = std / mean

    quantiles = np.array(
        [float(q) for q in quantiles]
    )  # TODO this section is hacky, need to tighten up API with typing
    # print(f'QUANTILES: {quantiles}')

    assert np.all(quantiles >= 0) and np.all(quantiles <= 1), 'quantiles should be in [0, 1]'

    wq = calculate_weighted_quantiles(values, sample_weight, quantiles)

    if get_cov:
        wq = np.append(np.append(wq[0:cov_ind], np.array([cov])), wq[cov_ind:])
    if get_std:
        wq = np.append(np.append(wq[0:std_ind], np.array([std])), wq[std_ind:])
    if get_mean:
        wq = np.append(np.append(wq[0:mean_ind], np.array([mean])), wq[mean_ind:])

    toc = time.perf_counter()
    log.debug(f'time to calculate weighted quantiles {toc-tic} seconds')

    return wq

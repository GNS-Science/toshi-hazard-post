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


# number of bins that the values of a level are put in to find the values either side of each quantile. The
# quantiles do not depend on it, only the time taken: with more bins fewer values are sorted, but the weight
# of each bin is accumulated in a larger table
QUANTILE_NBINS = 65536
# a set of values no larger than this is sorted rather than put in bins
QUANTILE_SORT_SIZE = 4096


@jit(nopython=True)
def _select_quantiles(
    values: 'npt.NDArray',
    weights: 'npt.NDArray',
    weight_below: float,
    targets: 'npt.NDArray',
    members: 'npt.NDArray',
    has_left: bool,
    left_x: float,
    left_v: float,
    has_right: bool,
    right_x: float,
    right_v: float,
    nbins: int,
    sort_size: int,
    out: 'npt.NDArray',
) -> None:
    """Find weighted quantiles of a set of values without sorting all of them.

    The values are a run of the stable sorted order of a level, given in branch order. A small set is sorted.
    A large set is put in bins by value and the weight in each bin is accumulated to find the bin holding each
    quantile. This function is then called for the values of each of those bins, with the values adjacent to
    the bin in sorted order (left and right) passed in so that a quantile can be interpolated across the edge
    of the bin.

    The values must not be NaN.

    Args:
        values: the values in branch order (value, )
        weights: weights of the values (value, )
        weight_below: the weight of all the values of the level that sort before these values
        targets: the cumulative weight at each quantile of the level (quantile, )
        members: indices of the targets that are in this set of values
        has_left: True if a value of the level sorts before these values
        left_x: the cumulative weight position of the value that sorts immediately before these values
        left_v: the value that sorts immediately before these values
        has_right: True if a value of the level sorts after these values
        right_x: the cumulative weight position of the value that sorts immediately after these values
        right_v: the value that sorts immediately after these values
        nbins: the number of bins for a large set of values, at least 4
        sort_size: a set of values no larger than this is sorted
        out: the quantile values, set for the members only (quantile, )
    """
    n = values.shape[0]
    vmin = values[0]
    vmax = values[0]
    # the largest finite value sets the width of the bins, so that an infinite rate does not empty them
    vmax_finite = -np.inf
    for i in range(n):
        v = values[i]
        if v < vmin:
            vmin = v
        if v > vmax:
            vmax = v
        if v > vmax_finite and v != np.inf:
            vmax_finite = v

    if vmin == vmax and not has_left and not has_right:
        for k in members:
            out[k] = vmin
        return

    if n <= sort_size or vmin == vmax:
        # every value sits at the cumulative weight up to and including it, less half its own weight. Equal
        # values are in branch order, which is the order a stable sort leaves them in
        order = np.argsort(values, kind='mergesort') if vmin != vmax else np.arange(n)
        npoints = n + (1 if has_left else 0) + (1 if has_right else 0)
        xp = np.empty(npoints)
        fp = np.empty(npoints)
        p = 0
        if has_left:
            xp[p] = left_x
            fp[p] = left_v
            p += 1
        cumulative = weight_below
        for index in order:
            cumulative += weights[index]
            xp[p] = cumulative - 0.5 * weights[index]
            fp[p] = values[index]
            p += 1
        if has_right:
            xp[p] = right_x
            fp[p] = right_v
        for k in members:
            out[k] = np.interp(targets[k], xp, fp)
        return

    # bin 0 holds the values equal to the minimum (e.g. all the zero rates), which never need sorting. The
    # other bins are linear in value. Any binning that is monotonic in value gives the same quantiles
    last_bin = nbins - 1
    scale = (nbins - 2) / (vmax_finite - vmin) if vmax_finite > vmin else 1.0
    bin_index = np.empty(n, dtype=np.int32)
    bin_weight = np.zeros(nbins)
    bin_count = np.zeros(nbins, dtype=np.int64)
    for i in range(n):
        v = values[i]
        if v == vmin:
            b = 0
        else:
            position = (v - vmin) * scale
            b = 1 + int(position) if position < last_bin - 1 else last_bin
        bin_index[i] = b
        bin_weight[b] += weights[i]
        bin_count[b] += 1

    bin_weight_below = np.empty(nbins)
    cumulative = weight_below
    for b in range(nbins):
        bin_weight_below[b] = cumulative
        cumulative += bin_weight[b]

    # the bin holding each target
    nmembers = members.shape[0]
    member_bin = np.empty(nmembers, dtype=np.int64)
    for m in range(nmembers):
        b = max(np.searchsorted(bin_weight_below, targets[members[m]], side='right') - 1, 0)
        while bin_count[b] == 0:  # bin 0 is never empty
            b -= 1
        member_bin[m] = b

    # the non empty bins either side of each of those bins, -1 if there is none. The values of a level that
    # sort immediately before and after the values of a bin are in these
    target_bins = np.unique(member_bin)
    ntarget_bins = target_bins.shape[0]
    bin_below = np.empty(ntarget_bins, dtype=np.int64)
    bin_above = np.empty(ntarget_bins, dtype=np.int64)
    for g in range(ntarget_bins):
        below = target_bins[g] - 1
        while below >= 0 and bin_count[below] == 0:
            below -= 1
        above = target_bins[g] + 1
        while above < nbins and bin_count[above] == 0:
            above += 1
        bin_below[g] = below
        bin_above[g] = above if above < nbins else -1

    neighbour_bins = np.concatenate((bin_below, bin_above))
    selected = np.unique(np.concatenate((target_bins, neighbour_bins[neighbour_bins >= 0])))
    nselected = selected.shape[0]
    slot = np.full(nbins, -1, dtype=np.int32)
    for s in range(nselected):
        slot[selected[s]] = s
    is_target = np.zeros(nselected, dtype=np.bool_)
    for g in range(ntarget_bins):
        is_target[slot[target_bins[g]]] = True

    # gather the values of the target bins in branch order, and find the values that a stable sort would put
    # first and last in each selected bin
    start = np.zeros(nselected + 1, dtype=np.int64)
    for s in range(nselected):
        start[s + 1] = start[s] + (bin_count[selected[s]] if is_target[s] else 0)
    fill = start[:nselected].copy()
    gathered_values = np.empty(start[nselected])
    gathered_weights = np.empty(start[nselected])
    seen = np.zeros(nselected, dtype=np.bool_)
    first_v = np.empty(nselected)
    first_w = np.empty(nselected)
    last_v = np.empty(nselected)
    last_w = np.empty(nselected)
    for i in range(n):
        s = slot[bin_index[i]]
        if s < 0:
            continue
        v = values[i]
        if not seen[s] or v < first_v[s]:
            first_v[s] = v
            first_w[s] = weights[i]
        if not seen[s] or v >= last_v[s]:
            last_v[s] = v
            last_w[s] = weights[i]
        seen[s] = True
        if is_target[s]:
            gathered_values[fill[s]] = v
            gathered_weights[fill[s]] = weights[i]
            fill[s] += 1

    for g in range(ntarget_bins):
        b = target_bins[g]
        s = slot[b]
        bin_has_left, bin_left_x, bin_left_v = has_left, left_x, left_v
        if bin_below[g] >= 0:
            s_below = slot[bin_below[g]]
            bin_has_left = True
            bin_left_x = bin_weight_below[b] - 0.5 * last_w[s_below]
            bin_left_v = last_v[s_below]
        bin_has_right, bin_right_x, bin_right_v = has_right, right_x, right_v
        if bin_above[g] >= 0:
            s_above = slot[bin_above[g]]
            bin_has_right = True
            bin_right_x = bin_weight_below[bin_above[g]] + 0.5 * first_w[s_above]
            bin_right_v = first_v[s_above]
        count = start[s + 1] - start[s]
        _select_quantiles(
            gathered_values[start[s] : start[s + 1]],
            gathered_weights[start[s] : start[s + 1]],
            bin_weight_below[b],
            targets,
            members[member_bin == b],
            bin_has_left,
            bin_left_x,
            bin_left_v,
            bin_has_right,
            bin_right_x,
            bin_right_v,
            nbins,
            # a bin always holds fewer values than were binned, unless they are NaN: sort to be sure of stopping
            sort_size if count < n else count,
            out,
        )


def weighted_quantiles(
    values: 'npt.NDArray',
    weights: 'npt.NDArray',
    quantiles: Union[list[float], 'npt.NDArray'],
    nbins: int = QUANTILE_NBINS,
    sort_size: int = QUANTILE_SORT_SIZE,
) -> 'npt.NDArray':
    """Calculate weighted quantiles of each level of an array.

    The values of a level are put in ascending order, with equal values kept in branch order. Each value sits
    at the cumulative weight up to and including it, less half its own weight, as a fraction of the total
    weight. A quantile is linearly interpolated between the values either side of it, and is the smallest or
    largest value if it is outside of them.

    Only the values near each quantile are sorted, see _select_quantiles(). nbins and sort_size change the
    time taken, not the quantiles.

    Args:
        values: array of values (IMTL, branch). They must not be NaN.
        weights: weights of values (branch, ). They do not need to sum to 1.
        quantiles: quantiles to be found (quantile, ). Values should be in [0,1]
        nbins: the number of bins that a large set of values are put in, at least 4
        sort_size: a set of values no larger than this is sorted rather than put in bins

    Returns:
        weighted quantiles (quantile, IMTL)
    """
    values = np.ascontiguousarray(values, dtype=np.float64)
    weights = np.ascontiguousarray(weights, dtype=np.float64)
    if values.ndim != 2 or weights.shape != (values.shape[1],):
        raise ValueError("weights must have one entry for each branch of values")
    if values.shape[1] == 0:
        raise ValueError("values must have at least one branch")
    # two of the bins are for the smallest and largest values, the rest have to divide the values in between
    if nbins < 4:
        raise ValueError("nbins must be at least 4")

    quantiles = np.asarray(quantiles, dtype=np.float64)
    targets = quantiles * _blocked_sum(weights)
    # the last quantile is the largest value. If the largest values have no weight they sit at exactly the
    # total weight, which the weights summed in another order may fall short of
    targets[quantiles >= 1.0] = np.inf
    members = np.arange(targets.shape[0])
    level_quantiles = np.empty(targets.shape[0])
    wq = np.empty((targets.shape[0], values.shape[0]))
    for i in range(values.shape[0]):
        _select_quantiles(
            values[i], weights, 0.0, targets, members, False, 0.0, 0.0, False, 0.0, 0.0, nbins, sort_size,
            level_quantiles,
        )  # fmt: skip
        wq[:, i] = level_quantiles
    return wq

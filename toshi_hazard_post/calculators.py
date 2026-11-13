"""A collection of calculation functions for hazard aggregation."""

import logging
from collections.abc import Sequence
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


# number of composite branches whose rates are written one level at a time by composite_rates()
COMPOSITE_RATES_BLOCK = 4096


@jit(nopython=True)
def composite_rates(branch_index_table: 'npt.NDArray', component_rates: 'npt.NDArray') -> 'npt.NDArray':
    """Calculate the rates of the composite branches by summing the rates of their component branches.

    The composite branches are taken a block at a time, and the rates of a block are written one level at a time,
    so that each level is written in order rather than every level at once. The component rates of a level are
    read from a transposed copy, small enough to stay in cache. Each rate is summed over the component branches in
    column order, as composite_rate_bounds() relies on.

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
    component_rates_t = np.ascontiguousarray(component_rates.T)
    for start in range(0, nbranches, COMPOSITE_RATES_BLOCK):
        stop = min(start + COMPOSITE_RATES_BLOCK, nbranches)
        for j in range(nlevels):
            level_rates = component_rates_t[j]
            row = rates[j]
            for i in range(start, stop):
                rate = 0.0
                for k in range(ncomponents):
                    rate += level_rates[branch_index_table[i, k]]
                row[i] = rate
    return rates


# number of terms in each partial sum of the blocked sums
SUM_BLOCK_SIZE = 256


# The blocked sums accumulate partial sums of SUM_BLOCK_SIZE terms, which are then added together. A running
# total is only ever added to numbers of a similar size, so the rounding error grows with the number of blocks
# rather than with the number of terms. weighted_avg_and_std() reduces each level in the same blocks.
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
def _block_weights(weights: 'npt.NDArray') -> 'npt.NDArray':
    """The sum of the weights in each block of SUM_BLOCK_SIZE branches."""
    nbranches = weights.shape[0]
    nblocks = (nbranches + SUM_BLOCK_SIZE - 1) // SUM_BLOCK_SIZE
    block_weight = np.empty(nblocks)
    for b in range(nblocks):
        total = 0.0
        for j in range(b * SUM_BLOCK_SIZE, min((b + 1) * SUM_BLOCK_SIZE, nbranches)):
            total += weights[j]
        block_weight[b] = total
    return block_weight


@jit(nopython=True)
def _shift(values: 'npt.NDArray', weights: 'npt.NDArray', block_weight: 'npt.NDArray') -> float:
    """A value near the weighted mean to take deviations from: the mean of the first block with weight."""
    nbranches = values.shape[0]
    for b in range(block_weight.shape[0]):
        if block_weight[b] > 0.0:
            total = 0.0
            for j in range(b * SUM_BLOCK_SIZE, min((b + 1) * SUM_BLOCK_SIZE, nbranches)):
                total += weights[j] * values[j]
            return total / block_weight[b]
    return 0.0


# The block loops below keep 4 partial sums, so that each addition does not wait for the one before it. They are
# inlined into weighted_avg_and_std(): called once per block, they took about a fifth longer.
@jit(nopython=True, inline='always')
def _block_weighted_sum(values: 'npt.NDArray', weights: 'npt.NDArray', start: int, stop: int, shift: float) -> float:
    """The sum of weights * (values - shift) over branches start to stop."""
    s0 = s1 = s2 = s3 = 0.0
    j = start
    while j + 3 < stop:
        s0 += weights[j] * (values[j] - shift)
        s1 += weights[j + 1] * (values[j + 1] - shift)
        s2 += weights[j + 2] * (values[j + 2] - shift)
        s3 += weights[j + 3] * (values[j + 3] - shift)
        j += 4
    while j < stop:
        s0 += weights[j] * (values[j] - shift)
        j += 1
    return (s0 + s1) + (s2 + s3)


@jit(nopython=True, inline='always')
def _block_weighted_sum_sq_dev(
    values: 'npt.NDArray', weights: 'npt.NDArray', start: int, stop: int, shift: float, center: float
) -> float:
    """The sum of weights * ((values - shift) - center)^2 over branches start to stop."""
    s0 = s1 = s2 = s3 = 0.0
    j = start
    while j + 3 < stop:
        d0 = (values[j] - shift) - center
        d1 = (values[j + 1] - shift) - center
        d2 = (values[j + 2] - shift) - center
        d3 = (values[j + 3] - shift) - center
        s0 += weights[j] * d0 * d0
        s1 += weights[j + 1] * d1 * d1
        s2 += weights[j + 2] * d2 * d2
        s3 += weights[j + 3] * d3 * d3
        j += 4
    while j < stop:
        d0 = (values[j] - shift) - center
        s0 += weights[j] * d0 * d0
        j += 1
    return (s0 + s1) + (s2 + s3)


@jit(nopython=True)
def weighted_avg_and_std(values: 'npt.NDArray', weights: 'npt.NDArray') -> tuple['npt.NDArray', 'npt.NDArray']:
    """Calculate weighted average and standard deviation of an array.

    Each level is read from memory once, a block of SUM_BLOCK_SIZE branches at a time. While a block is in cache
    its weighted sum and the weighted squared deviations from its own mean are found, and the blocks are then
    combined (Chan et al.): the sum of squared deviations from the mean is the sum over the blocks of their own,
    plus each block's weight times the squared difference between its mean and the mean. No array the size of
    values is allocated.

    The deviations are taken from a value near the mean (the mean of the first block with weight) rather than
    from zero, and not as E[x^2] - mean^2: both lose precision when the standard deviation is much smaller than
    the mean.

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
    block_weight = _block_weights(weights)
    nblocks = block_weight.shape[0]
    block_mean = np.empty(nblocks)
    block_sq_dev = np.empty(nblocks)
    average = np.empty(nlevels)
    std = np.empty(nlevels)
    for i in range(nlevels):
        level = values[i]
        shift = _shift(level, weights, block_weight)
        # the means are of the shifted values, values - shift
        total = 0.0
        for b in range(nblocks):
            start = b * SUM_BLOCK_SIZE
            stop = min(start + SUM_BLOCK_SIZE, nbranches)
            block_total = _block_weighted_sum(level, weights, start, stop, shift)
            total += block_total
            mean_b = block_total / block_weight[b] if block_weight[b] > 0.0 else 0.0
            block_mean[b] = mean_b
            block_sq_dev[b] = _block_weighted_sum_sq_dev(level, weights, start, stop, shift, mean_b)
        mean = total / sum_weights
        sq_dev = 0.0
        for b in range(nblocks):
            difference = block_mean[b] - mean
            sq_dev += block_sq_dev[b] + block_weight[b] * difference * difference
        average[i] = shift + mean
        std[i] = np.sqrt(sq_dev / sum_weights)
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
QUANTILE_NBINS = 16384
# a set of values no larger than this is sorted rather than put in bins
QUANTILE_SORT_SIZE = 4096


@jit(nopython=True, inline='always')
def _quantile_bin(value: float, vmin: float, scale: float, last_bin: int) -> int:
    """The bin of a value: 0 for vmin (or less), then linear in value, with infinite values in the last bin."""
    if value <= vmin:
        return 0
    position = (value - vmin) * scale
    return 1 + int(position) if position < last_bin - 1 else last_bin


# The helpers below are steps of _select_quantiles() once the values are in bins, shared with
# _composite_rate_aggs(), which bins and gathers composite rates that it sums rather than reads. They run once per
# set of values; the loops over the values are written out in both.
@jit(nopython=True, inline='always')
def _target_bins(
    bin_weight: 'npt.NDArray', bin_count: 'npt.NDArray', weight_below: float, targets: 'npt.NDArray',
    members: 'npt.NDArray',
) -> tuple:  # fmt: skip
    """Find the bins holding the targets, and the bins to gather values from.

    Returns:
        bin_weight_below: the weight of the values in the bins before each bin, plus weight_below (bin, )
        member_bin: the bin holding each member's target (member, )
        target_bins: the bins holding a target, in order (target bin, )
        bin_below, bin_above: the non empty bins either side of each target bin, -1 if there is none. The values
            that sort immediately before and after the values of a target bin are in these (target bin, )
        slot: the index of each target or neighbouring bin among the selected bins, -1 for the others (bin, )
        is_target: True for each selected bin that holds a target (selected bin, )
        start: where the values of each selected bin start among the gathered values, the target bins' values
            only (selected bin + 1, )
    """
    nbins = bin_weight.shape[0]
    bin_weight_below = np.empty(nbins)
    cumulative = weight_below
    for b in range(nbins):
        bin_weight_below[b] = cumulative
        cumulative += bin_weight[b]

    nmembers = members.shape[0]
    member_bin = np.empty(nmembers, dtype=np.int64)
    for m in range(nmembers):
        b = max(np.searchsorted(bin_weight_below, targets[members[m]], side='right') - 1, 0)
        # only the empty bins after the last value can be landed on
        while b > 0 and bin_count[b] == 0:
            b -= 1
        # bin 0 is empty when vmin is only a bound, then the first non empty bin holds the target
        while bin_count[b] == 0:
            b += 1
        member_bin[m] = b

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
    start = np.zeros(nselected + 1, dtype=np.int64)
    for s in range(nselected):
        start[s + 1] = start[s] + (bin_count[selected[s]] if is_target[s] else 0)
    return bin_weight_below, member_bin, target_bins, bin_below, bin_above, slot, is_target, start


@jit(nopython=True, inline='always')
def _gather_buffers(start: 'npt.NDArray') -> tuple:
    """The arrays that the values of the selected bins are gathered into, for selected bins whose gathered values
    start at start: (fill, gathered_values, gathered_weights, seen, first_v, first_w, last_v, last_w)."""
    nselected = start.shape[0] - 1
    first_v = np.empty(nselected)
    first_w = np.empty(nselected)
    last_v = np.empty(nselected)
    last_w = np.empty(nselected)
    seen = np.zeros(nselected, dtype=np.bool_)
    return (
        start[:nselected].copy(), np.empty(start[nselected]), np.empty(start[nselected]), seen, first_v, first_w,
        last_v, last_w,
    )  # fmt: skip


@jit(nopython=True, inline='always')
def _bin_neighbours(
    g: int, target_bins: 'npt.NDArray', bin_below: 'npt.NDArray', bin_above: 'npt.NDArray', slot: 'npt.NDArray',
    bin_weight_below: 'npt.NDArray', first_v: 'npt.NDArray', first_w: 'npt.NDArray', last_v: 'npt.NDArray',
    last_w: 'npt.NDArray', has_left: bool, left_x: float, left_v: float, has_right: bool, right_x: float,
    right_v: float,
) -> tuple:  # fmt: skip
    """The values either side of target bin g in sorted order, as (has_left, left_x, left_v, has_right, right_x,
    right_v) for _select_quantiles(): from the neighbouring bins, or those of the enclosing set if there is none."""
    b = target_bins[g]
    if bin_below[g] >= 0:
        s_below = slot[bin_below[g]]
        has_left = True
        left_x = bin_weight_below[b] - 0.5 * last_w[s_below]
        left_v = last_v[s_below]
    if bin_above[g] >= 0:
        s_above = slot[bin_above[g]]
        has_right = True
        right_x = bin_weight_below[bin_above[g]] + 0.5 * first_w[s_above]
        right_v = first_v[s_above]
    return has_left, left_x, left_v, has_right, right_x, right_v


# this kernel takes several seconds to compile, so the compiled code is kept on disk (see warm_up())
@jit(nopython=True, cache=True)
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
    has_range: bool,
    range_lo: float,
    range_hi: float,
    range_hi_finite: float,
) -> None:
    """Find weighted quantiles of a set of values without sorting all of them.

    The values are a run of the stable sorted order of a level, given in branch order. A small set is sorted.
    A large set is put in bins by value and the weight in each bin is accumulated to find the bin holding each
    quantile. This function is then called for the values of each of those bins, with the values adjacent to
    the bin in sorted order (left and right) passed in so that a quantile can be interpolated across the edge
    of the bin.

    The values must not be NaN.

    The bins span the smallest to the largest finite value. Each value is read twice: once to put it in a bin,
    and once to gather the values of the bins holding a quantile, working its bin out again rather than storing
    it. If has_range is False the values are first read once more to find the range.

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
        has_range: True if range_lo, range_hi and range_hi_finite are given, False to find them from the values
        range_lo: at most the smallest value. If equal to range_hi, every value is equal to it
        range_hi: at least the largest value
        range_hi_finite: at least the largest finite value
    """
    n = values.shape[0]
    if has_range:
        vmin = range_lo
        vmax = range_hi
        vmax_finite = range_hi_finite
    else:
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

    # bin 0 holds the values equal to the minimum (e.g. all the zero rates), which never need sorting, and is
    # empty if vmin is below every value. The other bins are linear in value. Any binning that is monotonic in
    # value gives the same quantiles, up to the rounding of the cumulative weights, which are summed by bin
    last_bin = nbins - 1
    scale = (nbins - 2) / (vmax_finite - vmin) if vmax_finite > vmin else 1.0
    bin_weight = np.zeros(nbins)
    bin_count = np.zeros(nbins, dtype=np.int64)
    for i in range(n):
        b = _quantile_bin(values[i], vmin, scale, last_bin)
        bin_weight[b] += weights[i]
        bin_count[b] += 1

    bin_weight_below, member_bin, target_bins, bin_below, bin_above, slot, is_target, start = _target_bins(
        bin_weight, bin_count, weight_below, targets, members
    )

    # gather the values of the target bins in branch order, and find the values that a stable sort would put
    # first and last in each selected bin
    fill, gathered_values, gathered_weights, seen, first_v, first_w, last_v, last_w = _gather_buffers(start)
    # the same as _pass_b(). A helper for this, inlined into both, made this kernel take 40% longer
    for i in range(n):
        v = values[i]
        s = slot[_quantile_bin(v, vmin, scale, last_bin)]
        if s < 0:
            continue
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

    for g in range(target_bins.shape[0]):
        b = target_bins[g]
        s = slot[b]
        bin_has_left, bin_left_x, bin_left_v, bin_has_right, bin_right_x, bin_right_v = _bin_neighbours(
            g, target_bins, bin_below, bin_above, slot, bin_weight_below, first_v, first_w, last_v, last_w,
            has_left, left_x, left_v, has_right, right_x, right_v,
        )  # fmt: skip
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
            # the smallest and largest value of the bin are its range, unless the largest is infinite. This is a
            # value rather than a literal False, which numba would compile as a second signature of this
            # recursive kernel, and two cached signatures of a recursive function crash when loaded
            last_v[s] != np.inf,
            first_v[s],
            last_v[s],
            last_v[s],
        )


def weighted_quantiles(
    values: 'npt.NDArray',
    weights: 'npt.NDArray',
    quantiles: Union[list[float], 'npt.NDArray'],
    nbins: int = QUANTILE_NBINS,
    sort_size: int = QUANTILE_SORT_SIZE,
    bounds: tuple['npt.NDArray', 'npt.NDArray', 'npt.NDArray'] | None = None,
) -> 'npt.NDArray':
    """Calculate weighted quantiles of each level of an array.

    The values of a level are put in ascending order, with equal values kept in branch order. Each value sits
    at the cumulative weight up to and including it, less half its own weight, as a fraction of the total
    weight. A quantile is linearly interpolated between the values either side of it, and is the smallest or
    largest value if it is outside of them.

    Only the values near each quantile are sorted, see _select_quantiles(). nbins, sort_size and bounds change
    the time taken, and the quantiles only by rounding (the cumulative weights are summed in another order).

    Given bounds, each level is read twice rather than three times. Use composite_rate_bounds() for the bounds
    of composite rates. Bounds that do not hold every value give wrong quantiles.

    Args:
        values: array of values (IMTL, branch). They must not be NaN.
        weights: weights of values (branch, ). They do not need to sum to 1.
        quantiles: quantiles to be found (quantile, ). Values should be in [0,1]
        nbins: the number of bins that a large set of values are put in, at least 4
        sort_size: a set of values no larger than this is sorted rather than put in bins
        bounds: (lo, hi, hi_finite) each with shape (IMTL, ): at most the smallest value, at least the largest
            value and at least the largest finite value of each level. If lo equals hi, every value of the level
            is equal to it. If None, they are found from the values.

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
    nlevels = values.shape[0]
    lo: npt.NDArray[np.float64]
    hi: npt.NDArray[np.float64]
    hi_finite: npt.NDArray[np.float64]
    if bounds is None:
        has_range = False
        lo = hi = hi_finite = np.zeros(nlevels)
    else:
        has_range = True
        lo, hi, hi_finite = (np.asarray(bound, dtype=np.float64) for bound in bounds)
        if not lo.shape == hi.shape == hi_finite.shape == (nlevels,):
            raise ValueError("bounds must have one entry for each level of values")

    quantiles = np.asarray(quantiles, dtype=np.float64)
    targets = quantiles * _blocked_sum(weights)
    # the last quantile is the largest value. If the largest values have no weight they sit at exactly the
    # total weight, which the weights summed in another order may fall short of
    targets[quantiles >= 1.0] = np.inf
    members = np.arange(targets.shape[0])
    level_quantiles = np.empty(targets.shape[0])
    wq = np.empty((targets.shape[0], values.shape[0]))
    for i in range(nlevels):
        _select_quantiles(
            values[i], weights, 0.0, targets, members, False, 0.0, 0.0, False, 0.0, 0.0, nbins, sort_size,
            level_quantiles, has_range, lo[i], hi[i], hi_finite[i],
        )  # fmt: skip
        wq[:, i] = level_quantiles
    return wq


def composite_rate_bounds(
    columns: Sequence['npt.NDArray'], component_rates: 'npt.NDArray'
) -> tuple['npt.NDArray', 'npt.NDArray', 'npt.NDArray']:
    """Bound the rates of the composite branches of each level, without building them.

    A composite rate is the sum of one component rate from each column of the branch index table, added in
    column order (see composite_rates()). Rounded addition is monotonic, so adding the smallest (largest) rate
    of each column in the same order bounds every composite rate below (above). The bounds are exact if every
    combination of the component branches of the columns is a composite branch.

    Args:
        columns: the component branches in each column of the branch index table, see
            logic_tree.branch_index_columns()
        component_rates: rates of the component branches (component branch, IMTL)

    Returns:
        (lo, hi, hi_finite) each with shape (IMTL, ): at most the smallest, at least the largest and at least the
            largest finite composite rate of each level, as weighted_quantiles() takes for its bounds.
    """
    nlevels = component_rates.shape[1]
    lo = np.zeros(nlevels)
    hi = np.zeros(nlevels)
    hi_finite = np.zeros(nlevels)
    for column in columns:
        rates = component_rates[column]
        lo = lo + rates.min(axis=0)
        hi = hi + rates.max(axis=0)
        hi_finite = hi_finite + np.where(np.isinf(rates), -np.inf, rates).max(axis=0)
    return lo, hi, hi_finite


# the bin of each composite rate of a level is kept as a uint16 by _composite_rate_aggs()
COMPOSITE_RATE_AGGS_MAX_NBINS = 65536


@jit(nopython=True, inline='always')
def _composite_rate(branch_index_table: 'npt.NDArray', i: int, level_rates: 'npt.NDArray', ncomponents: int) -> float:
    """The rate of composite branch i, summed in column order as composite_rates()."""
    if ncomponents == 4:  # the NSHM tables, unrolled: about 40% faster than the loop, with the same additions
        return (
            ((0.0 + level_rates[branch_index_table[i, 0]]) + level_rates[branch_index_table[i, 1]])
            + level_rates[branch_index_table[i, 2]]
        ) + level_rates[branch_index_table[i, 3]]
    rate = 0.0
    for k in range(ncomponents):
        rate += level_rates[branch_index_table[i, k]]
    return rate


# The two passes over the composite branches of a level are kernels of their own, called once per level, so that
# how they are compiled does not depend on the rest of _composite_rate_aggs(): written into it, they took 5 to
# 15% longer, depending on which of the helpers above were inlined beside them.
@jit(nopython=True)
def _pass_a(
    branch_index_table: 'npt.NDArray', level_rates: 'npt.NDArray', weights: 'npt.NDArray',
    block_weight: 'npt.NDArray', shift: float, binned: bool, vmin: float, scale: float, last_bin: int,
    bins: 'npt.NDArray', bin_weight: 'npt.NDArray', bin_count: 'npt.NDArray', block_mean: 'npt.NDArray',
    block_sq_dev: 'npt.NDArray', block_rates: 'npt.NDArray',
) -> float:  # fmt: skip
    """Pass A of _composite_rate_aggs() over the rates of one level.

    Sets the mean and the sum of squared deviations of each block of the rates less shift, as
    weighted_avg_and_std() does, and if binned the bin of each rate, adding its weight and count to the bin.

    Returns:
        the weighted sum of the rates less shift
    """
    nbranches, ncomponents = branch_index_table.shape
    total = 0.0
    for b in range(block_weight.shape[0]):
        start = b * SUM_BLOCK_SIZE
        stop = min(start + SUM_BLOCK_SIZE, nbranches)
        nblock = stop - start
        for t in range(nblock):
            block_rates[t] = _composite_rate(branch_index_table, start + t, level_rates, ncomponents)
        block_weights = weights[start:stop]
        block_total = _block_weighted_sum(block_rates, block_weights, 0, nblock, shift)
        total += block_total
        mean_b = block_total / block_weight[b] if block_weight[b] > 0.0 else 0.0
        block_mean[b] = mean_b
        block_sq_dev[b] = _block_weighted_sum_sq_dev(block_rates, block_weights, 0, nblock, shift, mean_b)
        if binned:
            for t in range(nblock):
                bin_t = _quantile_bin(block_rates[t], vmin, scale, last_bin)
                bins[start + t] = bin_t
                bin_weight[bin_t] += block_weights[t]
                bin_count[bin_t] += 1
    return total


@jit(nopython=True)
def _pass_b(
    branch_index_table: 'npt.NDArray', level_rates: 'npt.NDArray', weights: 'npt.NDArray', bins: 'npt.NDArray',
    slot: 'npt.NDArray', is_target: 'npt.NDArray', fill: 'npt.NDArray', gathered_values: 'npt.NDArray',
    gathered_weights: 'npt.NDArray', seen: 'npt.NDArray', first_v: 'npt.NDArray', first_w: 'npt.NDArray',
    last_v: 'npt.NDArray', last_w: 'npt.NDArray',
) -> None:  # fmt: skip
    """Pass B of _composite_rate_aggs() over the rates of one level: the rates in the selected bins, summed again,
    are gathered in branch order as _select_quantiles() gathers its values."""
    ncomponents = branch_index_table.shape[1]
    for i in range(branch_index_table.shape[0]):
        s = slot[bins[i]]
        if s < 0:
            continue
        v = _composite_rate(branch_index_table, i, level_rates, ncomponents)
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


@jit(nopython=True, cache=True)
def _composite_rate_aggs(
    branch_index_table: 'npt.NDArray',
    component_rates: 'npt.NDArray',
    weights: 'npt.NDArray',
    quantiles: 'npt.NDArray',
    lo: 'npt.NDArray',
    hi: 'npt.NDArray',
    hi_finite: 'npt.NDArray',
    nbins: int,
    sort_size: int,
) -> tuple['npt.NDArray', 'npt.NDArray', 'npt.NDArray']:
    """The weighted mean, std and quantiles of each level of the composite rates, see composite_rate_aggs().

    Each level is done in two passes over the composite branches, each rate summed from the component rates of
    the level (small enough to stay in cache) as it is needed:

    pass A: the rates of a block of SUM_BLOCK_SIZE branches are summed into a small buffer, from which their
            weighted sum and squared deviations are found as weighted_avg_and_std() does, and each rate is put
            in its quantile bin, as _select_quantiles() does. The bin of each branch is kept.
    pass B: the rates in the bins next to the quantiles are summed again and gathered, and the quantiles found
            from them by _select_quantiles().

    Everything is calculated in the same order as composite_rates() followed by weighted_avg_and_std() and
    weighted_quantiles() with the same bounds, so the results are the same to the bit.
    """
    nbranches, ncomponents = branch_index_table.shape
    nlevels = component_rates.shape[1]
    component_rates_t = np.ascontiguousarray(component_rates.T)
    sum_weights = _blocked_sum(weights)
    block_weight = _block_weights(weights)
    nblocks = block_weight.shape[0]
    block_mean = np.empty(nblocks)
    block_sq_dev = np.empty(nblocks)
    block_rates = np.empty(SUM_BLOCK_SIZE)

    nq = quantiles.shape[0]
    targets = quantiles * sum_weights
    for q in range(nq):
        # as weighted_quantiles()
        if quantiles[q] >= 1.0:
            targets[q] = np.inf
    members = np.arange(nq)
    average = np.empty(nlevels)
    std = np.empty(nlevels)
    wq = np.empty((nq, nlevels))
    level_quantiles = np.empty(nq)
    last_bin = nbins - 1
    bin_weight = np.empty(nbins)
    bin_count = np.empty(nbins, dtype=np.int64)
    bins = np.empty(nbranches, dtype=np.uint16)

    for j in range(nlevels):
        level_rates = component_rates_t[j]
        # the shift for the deviations, as _shift()
        shift = 0.0
        for b in range(nblocks):
            if block_weight[b] > 0.0:
                total = 0.0
                for i in range(b * SUM_BLOCK_SIZE, min((b + 1) * SUM_BLOCK_SIZE, nbranches)):
                    total += weights[i] * _composite_rate(branch_index_table, i, level_rates, ncomponents)
                shift = total / block_weight[b]
                break

        vmin = lo[j]
        vmax_finite = hi_finite[j]
        # every rate is equal to vmin, as _select_quantiles() with no values either side
        binned = nq > 0 and vmin != hi[j]
        scale = (nbins - 2) / (vmax_finite - vmin) if vmax_finite > vmin else 1.0
        bin_weight[:] = 0.0
        bin_count[:] = 0

        total = _pass_a(
            branch_index_table, level_rates, weights, block_weight, shift, binned, vmin, scale, last_bin, bins,
            bin_weight, bin_count, block_mean, block_sq_dev, block_rates,
        )  # fmt: skip
        mean = total / sum_weights
        sq_dev = 0.0
        for b in range(nblocks):
            difference = block_mean[b] - mean
            sq_dev += block_sq_dev[b] + block_weight[b] * difference * difference
        average[j] = shift + mean
        std[j] = np.sqrt(sq_dev / sum_weights)

        if not binned:
            wq[:, j] = vmin
            continue

        # the top level of _select_quantiles(), with the rates of the selected bins summed again
        bin_weight_below, member_bin, target_bins, bin_below, bin_above, slot, is_target, start_g = _target_bins(
            bin_weight, bin_count, 0.0, targets, members
        )
        fill, gathered_values, gathered_weights, seen, first_v, first_w, last_v, last_w = _gather_buffers(start_g)
        _pass_b(
            branch_index_table, level_rates, weights, bins, slot, is_target, fill, gathered_values, gathered_weights,
            seen, first_v, first_w, last_v, last_w,
        )  # fmt: skip

        for g in range(target_bins.shape[0]):
            b = target_bins[g]
            s = slot[b]
            bin_has_left, bin_left_x, bin_left_v, bin_has_right, bin_right_x, bin_right_v = _bin_neighbours(
                g, target_bins, bin_below, bin_above, slot, bin_weight_below, first_v, first_w, last_v, last_w,
                False, 0.0, 0.0, False, 0.0, 0.0,
            )  # fmt: skip
            count = start_g[s + 1] - start_g[s]
            _select_quantiles(
                gathered_values[start_g[s] : start_g[s + 1]],
                gathered_weights[start_g[s] : start_g[s + 1]],
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
                sort_size if count < nbranches else count,
                level_quantiles,
                last_v[s] != np.inf,
                first_v[s],
                last_v[s],
                last_v[s],
            )
        wq[:, j] = level_quantiles
    return average, std, wq


def composite_rate_aggs(
    branch_index_table: 'npt.NDArray',
    component_rates: 'npt.NDArray',
    weights: 'npt.NDArray',
    quantiles: Union[list[float], 'npt.NDArray'],
    columns: Sequence['npt.NDArray'],
    nbins: int = QUANTILE_NBINS,
    sort_size: int = QUANTILE_SORT_SIZE,
) -> tuple['npt.NDArray', 'npt.NDArray', 'npt.NDArray']:
    """Calculate the weighted mean, std and quantiles of each level of the composite rates, without building them.

    The results are the same, to the bit, as those of weighted_avg_and_std() and weighted_quantiles() (with the
    bounds of composite_rate_bounds()) of the rates from composite_rates(). The composite rates are summed from
    the component rates as they are needed rather than stored: a (IMTL, composite branch) array is about 345 MB
    for NSHM_v1.0.4, and reading it from memory took most of the time of an aggregation.

    The indices are not bounds checked, every entry of branch_index_table must be a valid row of component_rates.

    Args:
        branch_index_table: composite branches as rows of indices into component_rates (composite branch, component)
        component_rates: rates of the component branches (component branch, IMTL)
        weights: weights of the composite branches (composite branch, ). They do not need to sum to 1.
        quantiles: quantiles to be found (quantile, ). Values should be in [0,1]
        columns: the component branches in each column of branch_index_table, see
            logic_tree.branch_index_columns()
        nbins: the number of bins that a large set of values are put in, 4 to 65536
        sort_size: a set of values no larger than this is sorted rather than put in bins

    Returns:
        (mean, std, quantiles): the weighted mean and std (IMTL, ) and the weighted quantiles (quantile, IMTL)
    """
    nbranches, ncomponents = branch_index_table.shape
    if weights.shape != (nbranches,):
        raise ValueError("weights must have one entry for each composite branch")
    if len(columns) != ncomponents:
        raise ValueError("columns must have one entry for each column of the branch index table")
    if not 4 <= nbins <= COMPOSITE_RATE_AGGS_MAX_NBINS:
        raise ValueError(f"nbins must be from 4 to {COMPOSITE_RATE_AGGS_MAX_NBINS}")
    component_rates = np.ascontiguousarray(component_rates, dtype=np.float64)
    weights = np.ascontiguousarray(weights, dtype=np.float64)
    quantiles = np.asarray(quantiles, dtype=np.float64)
    lo, hi, hi_finite = composite_rate_bounds(columns, component_rates)
    if nbranches <= sort_size:
        # the composite rates would all be sorted at once, which _composite_rate_aggs() does not do
        rates = composite_rates(branch_index_table, component_rates)
        mean, std = weighted_avg_and_std(rates, weights)
        wq = weighted_quantiles(rates, weights, quantiles, nbins=nbins, sort_size=sort_size, bounds=(lo, hi, hi_finite))
        return mean, std, wq
    return _composite_rate_aggs(
        branch_index_table, component_rates, weights, quantiles, lo, hi, hi_finite, nbins, sort_size
    )


def warm_up() -> None:
    """Compile the weighted quantiles and composite rate aggregate kernels, or load them from the numba cache.

    Call this before starting worker processes. Workers that are forked inherit the compiled kernels and workers
    that are spawned load them from the cache, rather than each of them compiling them for their first task.
    """
    weighted_quantiles(np.zeros((1, 2)), np.ones(2), [0.5])
    # with the types of an aggregation: a uint16 branch index table (see logic_tree.build_branch_index_table())
    zeros = np.zeros(1)
    _composite_rate_aggs(
        np.zeros((1, 1), dtype=np.uint16), np.zeros((1, 1)), np.ones(1), np.array([0.5]), zeros, zeros, zeros,
        QUANTILE_NBINS, QUANTILE_SORT_SIZE,
    )  # fmt: skip

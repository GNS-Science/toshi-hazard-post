import json
import math
import unittest
from pathlib import Path

import numpy as np
import pytest

import toshi_hazard_post.calculators as calculators
from toshi_hazard_post.calculators import (
    SUM_BLOCK_SIZE,
    _blocked_sum,
    _blocked_weighted_sum,
    _blocked_weighted_sum_sq_dev,
    composite_rates,
    cov,
    prob_to_rate,
    rate_to_prob,
    weighted_avg_and_std,
    weighted_quantiles,
)


class TestProbRate(unittest.TestCase):
    def setUp(self):
        self._probs = np.arange(0.1, 1.0, 0.1)
        self._inv_time = 5.5

        self._rates_file = Path(Path(__file__).parent, 'fixtures/calculators', 'rates.json')
        self._rates = np.array(json.loads(self._rates_file.read_text()))

    def test_prob_to_rate(self):

        rates = prob_to_rate(self._probs, self._inv_time)

        assert rates.shape == self._rates.shape
        assert np.allclose(rates, self._rates)

    def test_rate_to_prob(self):

        probs = rate_to_prob(self._rates, self._inv_time)

        assert probs.shape == self._probs.shape
        assert np.allclose(probs, self._probs)


@pytest.fixture
def weights_and_values():
    filepath = Path(__file__).parent / 'fixtures' / 'calculators' / 'weights_and_values.json'
    w_and_v = json.loads(filepath.read_text())
    weights = np.array(w_and_v['weights'])
    values = np.array(w_and_v['values'])
    # (level, branch)
    return weights, np.vstack((values, values * 2.0))


# coverage cannot trace numba compiled code, so the kernels are also run as plain Python (py_func)
@pytest.mark.parametrize("func", [weighted_avg_and_std, weighted_avg_and_std.py_func], ids=["jit", "python"])
class TestMeanStd:
    def test_weighted_avg_and_std(self, func, weights_and_values):
        weights, values = weights_and_values
        mean, std = func(values, weights)

        assert mean == pytest.approx(np.array([4.48577620473112, 4.48577620473112 * 2.0]))
        assert std == pytest.approx(np.array([2.6294520822489, 2.6294520822489 * 2.0]))

    def test_matches_numpy(self, func):
        rng = np.random.default_rng(0)
        # (level, branch), with levels that differ by orders of magnitude like a hazard curve
        values = rng.random((5, 1000)) * np.logspace(-1, -7, 5)[:, None]
        weights = rng.random(1000)

        mean, std = func(values, weights)

        mean_expected = np.average(values, weights=weights, axis=1)
        variance_expected = np.average((values - mean_expected[:, None]) ** 2, weights=weights, axis=1)
        assert mean.shape == std.shape == (5,)
        np.testing.assert_allclose(mean, mean_expected, rtol=1e-12)
        np.testing.assert_allclose(std, np.sqrt(variance_expected), rtol=1e-12)

    def test_weights_not_normalized(self, func, weights_and_values):
        weights, values = weights_and_values
        mean, std = func(values, weights)
        mean_scaled, std_scaled = func(values, weights * 7.0)

        assert mean_scaled == pytest.approx(mean)
        assert std_scaled == pytest.approx(std)

    @pytest.mark.parametrize("nweights", [4, 20])
    def test_weights_must_match_branches(self, func, nweights):
        values = np.ones((3, 10))
        with pytest.raises(ValueError, match="one entry for each branch"):
            func(values, np.ones(nweights))

    def test_rejects_branch_major_values(self, func):
        # (branch, level) was the layout before the kernel was written for (level, branch)
        values = np.ones((10, 3))
        with pytest.raises(ValueError, match="one entry for each branch"):
            func(values, np.ones(10))

    def test_zero_mean(self, func, weights_and_values):
        weights, values = weights_and_values
        mean, std = func(values * 0.0, weights)
        weighted_cov = cov(mean, std)
        assert (weighted_cov == 0).all()


def test_mean_std_rounding_error_does_not_grow_with_branches():
    # summing these branches one after the other gives a mean with a relative error of 9e-14
    rng = np.random.default_rng(0)
    values = rng.random((2, 1_000_000))
    weights = rng.random(1_000_000)

    mean, std = weighted_avg_and_std(values, weights)

    sum_weights = math.fsum(weights)
    mean_expected = [math.fsum(weights * level) / sum_weights for level in values]
    std_expected = [
        math.sqrt(math.fsum(weights * (level - m) ** 2) / sum_weights)
        for level, m in zip(values, mean_expected, strict=True)
    ]
    np.testing.assert_allclose(mean, mean_expected, rtol=2e-14)
    np.testing.assert_allclose(std, std_expected, rtol=2e-14)


# the lengths are either side of a whole number of blocks
@pytest.mark.parametrize("nterms", [0, 1, SUM_BLOCK_SIZE - 1, SUM_BLOCK_SIZE, 3 * SUM_BLOCK_SIZE + 7])
class TestBlockedSums:
    @pytest.fixture
    def weights_and_values(self, nterms):
        rng = np.random.default_rng(nterms)
        return rng.random(nterms), rng.random(nterms)

    @pytest.mark.parametrize("func", [_blocked_sum, _blocked_sum.py_func], ids=["jit", "python"])
    def test_blocked_sum(self, func, weights_and_values):
        _, values = weights_and_values
        assert func(values) == pytest.approx(math.fsum(values), rel=1e-14)

    @pytest.mark.parametrize("func", [_blocked_weighted_sum, _blocked_weighted_sum.py_func], ids=["jit", "python"])
    def test_blocked_weighted_sum(self, func, weights_and_values):
        weights, values = weights_and_values
        assert func(weights, values) == pytest.approx(math.fsum(weights * values), rel=1e-14)

    @pytest.mark.parametrize(
        "func", [_blocked_weighted_sum_sq_dev, _blocked_weighted_sum_sq_dev.py_func], ids=["jit", "python"]
    )
    def test_blocked_weighted_sum_sq_dev(self, func, weights_and_values):
        weights, values = weights_and_values
        expected = math.fsum(weights * (values - 0.3) ** 2)
        assert func(weights, values, 0.3) == pytest.approx(expected, rel=1e-14)


def reference_weighted_quantiles(values, weights, quantiles):
    """The weighted quantile definition, by sorting every value of each level. Returns (quantile, level)."""
    wq = []
    for level in values:
        sorter = np.argsort(level, kind='stable')
        quantiles_at_values = np.cumsum(weights[sorter]) - 0.5 * weights[sorter]
        quantiles_at_values /= np.sum(weights)
        wq.append(np.interp(quantiles, quantiles_at_values, level[sorter]))
    return np.array(wq).T


def quantile_cases():
    """Awkward (values with shape (level, branch), weights) pairs, by name."""
    rng = np.random.default_rng(0)
    n = 3000
    weights = rng.random(n)
    lognormal = np.exp(rng.normal(0.0, 1.0, (3, n)))
    return {
        "hazard curve like": (lognormal * np.logspace(-1, -7, 3)[:, None], weights),
        "heavy skew": (np.exp(rng.normal(0.0, 4.0, (3, n))), weights),
        "one huge outlier": (np.concatenate([lognormal, np.full((3, 1), 1e12)], axis=1), np.append(weights, 0.5)),
        "two distant clusters": (1.0 + (rng.random((3, n)) > 0.5) + 1e-9 * rng.random((3, n)), weights),
        "many ties": (rng.integers(0, 20, (3, n)).astype(float), weights),
        "mostly zero": (lognormal * (rng.random((3, n)) > 0.9), weights),
        "all but one zero": (np.concatenate([np.zeros((3, n)), np.ones((3, 1))], axis=1), np.append(weights, 0.5)),
        "all equal": (np.full((3, n), 2.5), weights),
        "some zero weights": (lognormal, weights * (rng.random(n) > 0.3)),
        "few heavy weights": (lognormal, np.exp(rng.normal(0.0, 5.0, n))),
        "weights not normalized": (lognormal, weights * 7.0),
        "an infinite value": (np.concatenate([lognormal, np.full((3, 1), np.inf)], axis=1), np.append(weights, 0.5)),
        "negative values": (rng.normal(0.0, 1.0, (3, n)), weights),
        "5 branches": (rng.random((3, 5)), rng.random(5)),
        "2 branches": (rng.random((3, 2)), rng.random(2)),
        "1 branch": (rng.random((3, 1)), rng.random(1)),
    }


# coverage cannot trace numba compiled code, so the kernel is also run as plain Python (py_func)
@pytest.fixture(params=["jit", "python"])
def quantile_kernel(request, monkeypatch):
    if request.param == "python":
        monkeypatch.setattr(calculators, '_select_quantiles', calculators._select_quantiles.py_func)


@pytest.mark.usefixtures("quantile_kernel")
class TestQuantiles:
    QUANTILES = [0.0, 0.001, 0.005, 0.025, 0.1, 0.25, 0.5, 0.75, 0.9, 0.975, 0.995, 0.999, 1.0]

    def test_weighted_quantiles(self):
        values = np.array([[1.0, 2.0, 3.0, 4.0, 5.0]])
        weights = np.array([1.0, 0.0, 0.0, 1.0, 1.0])

        quantiles = weighted_quantiles(values, weights, [0.5])

        # (quantile, level)
        assert quantiles.shape == (1, 1)
        assert quantiles[0, 0] == pytest.approx(4.0)

    def test_fixture(self):
        fixtures = Path(__file__).parent / 'fixtures' / 'calculators'
        weights = np.load(fixtures / 'weights.npy')
        # the fixture is stored as (branch, level)
        rates = np.load(fixtures / 'branch_rates.npy').T
        aggs_expected = np.load(fixtures / 'agg_0p2.npy')

        aggs = weighted_quantiles(rates, weights, [0.2])

        np.testing.assert_allclose(aggs, aggs_expected, verbose=True)

    # with the default sizes the values of the cases are sorted directly, the smaller sizes put them in bins and
    # then put the values of crowded bins in bins again
    @pytest.mark.parametrize("nbins,sort_size", [(65536, 4096), (64, 16), (4, 1)])
    @pytest.mark.parametrize("case", quantile_cases().keys())
    def test_matches_sorting_every_value(self, case, nbins, sort_size):
        values, weights = quantile_cases()[case]

        quantiles = weighted_quantiles(values, weights, self.QUANTILES, nbins=nbins, sort_size=sort_size)

        expected = reference_weighted_quantiles(values, weights, self.QUANTILES)
        assert quantiles.shape == (len(self.QUANTILES), values.shape[0])
        np.testing.assert_allclose(quantiles, expected, rtol=1e-10)

    def test_equal_values_are_in_branch_order(self):
        # the weight of the last of the equal values sets where interpolation to the next value starts, so
        # the quantile is different if the branches are in another order
        values = np.array([[0.0, 0.0, 0.0, 1.0]])
        weights = np.array([0.1, 0.2, 0.3, 0.4])
        reordered_weights = np.array([0.3, 0.2, 0.1, 0.4])

        quantiles = weighted_quantiles(values, weights, [0.6], nbins=4, sort_size=1)
        reordered = weighted_quantiles(values, reordered_weights, [0.6], nbins=4, sort_size=1)

        assert quantiles[0, 0] == pytest.approx(reference_weighted_quantiles(values, weights, [0.6])[0, 0])
        assert reordered[0, 0] == pytest.approx(reference_weighted_quantiles(values, reordered_weights, [0.6])[0, 0])
        assert quantiles[0, 0] != pytest.approx(reordered[0, 0])

    def test_quantiles_in_any_order(self):
        values, weights = quantile_cases()["hazard curve like"]

        quantiles = weighted_quantiles(values, weights, [0.9, 0.1, 0.5], nbins=64, sort_size=16)

        expected = reference_weighted_quantiles(values, weights, [0.9, 0.1, 0.5])
        np.testing.assert_allclose(quantiles, expected, rtol=1e-10)

    def test_does_not_change_arguments(self):
        values, weights = quantile_cases()["hazard curve like"]
        values_before, weights_before = values.copy(), weights.copy()

        weighted_quantiles(values, weights, self.QUANTILES, nbins=64, sort_size=16)

        assert np.array_equal(values, values_before)
        assert np.array_equal(weights, weights_before)

    @pytest.mark.parametrize("nweights", [4, 20])
    def test_weights_must_match_branches(self, nweights):
        with pytest.raises(ValueError, match="one entry for each branch"):
            weighted_quantiles(np.ones((3, 10)), np.ones(nweights), [0.5])

    def test_rejects_one_dimensional_values(self):
        # a single level was the argument before the quantiles were calculated for every level at once
        with pytest.raises(ValueError, match="one entry for each branch"):
            weighted_quantiles(np.ones(10), np.ones(10), [0.5])

    def test_rejects_no_branches(self):
        with pytest.raises(ValueError, match="at least one branch"):
            weighted_quantiles(np.ones((3, 0)), np.ones(0), [0.5])

    def test_rejects_too_few_bins(self):
        with pytest.raises(ValueError, match="nbins"):
            weighted_quantiles(np.ones((3, 10)), np.ones(10), [0.5], nbins=3)


def test_quantiles_of_many_branches():
    # enough branches that the default sizes put the values in bins, with ties and levels of all zeros
    rng = np.random.default_rng(0)
    n = 200_000
    values = np.exp(rng.normal(0.0, 1.0, (4, n))) * np.logspace(-1, -7, 4)[:, None]
    values[2, rng.random(n) > 0.4] = 0.0
    values[3] = 0.0
    weights = rng.random(n)
    quantiles = [0.005, 0.01, 0.025, 0.5, 0.2, 0.8, 0.1, 0.9]

    wq = weighted_quantiles(values, weights, quantiles)

    np.testing.assert_allclose(wq, reference_weighted_quantiles(values, weights, quantiles), rtol=1e-10)


@pytest.mark.parametrize("func", [composite_rates, composite_rates.py_func], ids=["jit", "python"])
@pytest.mark.parametrize("index_dtype", [np.uint8, np.uint16, np.uint32, np.int64])
def test_composite_rates(func, index_dtype):
    # 3 component branches x 2 levels
    component_rates = np.array([[1.0, 2.0], [10.0, 20.0], [100.0, 200.0]])
    # 4 composite branches x 2 component branches
    branch_index_table = np.array([[0, 1], [2, 2], [1, 0], [0, 2]], dtype=index_dtype)

    rates = func(branch_index_table, component_rates)

    # (level, composite branch)
    assert rates.shape == (2, 4)
    assert rates.tolist() == [[11.0, 200.0, 11.0, 101.0], [22.0, 400.0, 22.0, 202.0]]


@pytest.mark.parametrize("func", [composite_rates, composite_rates.py_func], ids=["jit", "python"])
def test_composite_rates_single_component(func):
    component_rates = np.array([[1.0, 2.0, 3.0], [10.0, 20.0, 30.0]])
    branch_index_table = np.array([[1], [0], [1]], dtype=np.uint16)

    rates = func(branch_index_table, component_rates)

    assert rates.tolist() == [[10.0, 1.0, 10.0], [20.0, 2.0, 20.0], [30.0, 3.0, 30.0]]

import json
import unittest
from pathlib import Path

import numpy as np
import pytest

from toshi_hazard_post.calculators import (
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
        self._rates = np.array(json.load(open(self._rates_file)))

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
    w_and_v = json.load(open(filepath))
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

    def test_does_not_need_contiguous_values(self, func, weights_and_values):
        weights, values = weights_and_values
        mean, std = func(values, weights)
        # a transposed (branch, level) array is a non contiguous view when turned back to (level, branch)
        mean_view, std_view = func(np.ascontiguousarray(values.T).T, weights)

        assert mean_view.tolist() == mean.tolist()
        assert std_view.tolist() == std.tolist()

    def test_zero_mean(self, func, weights_and_values):
        weights, values = weights_and_values
        mean, std = func(values * 0.0, weights)
        weighted_cov = cov(mean, std)
        assert (weighted_cov == 0).all()


class TestQuantiles(unittest.TestCase):
    def setUp(self):
        weights_filepath = Path(__file__).parent / 'fixtures' / 'calculators' / 'weights.npy'
        rates_filepath = Path(__file__).parent / 'fixtures' / 'calculators' / 'branch_rates.npy'
        aggs_expected_filepath = Path(__file__).parent / 'fixtures' / 'calculators' / 'agg_0p2.npy'
        self.weights = np.load(weights_filepath)
        self.rates = np.load(rates_filepath)
        self.aggs_expected = np.load(aggs_expected_filepath)

    def test_calculate_quantiles(self):

        quantiles = [0.5]
        values = np.array((1, 2, 3, 4, 5))
        weights = np.array((1, 0, 0, 1, 1))
        quantiles = weighted_quantiles(np.array(values), np.array(weights), quantiles)
        quantiles_expeced = [4]

        assert np.allclose(quantiles, quantiles_expeced)

    def test_calculate_quantiles_b(self):

        naggs = 1
        _, nlevels = self.rates.shape
        aggs = np.empty((naggs, nlevels))
        quantiles = [0.2]
        for i in range(nlevels):
            aggs[:, i] = weighted_quantiles(self.rates[:, i], self.weights, quantiles)

        np.testing.assert_allclose(aggs, self.aggs_expected, verbose=True)


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

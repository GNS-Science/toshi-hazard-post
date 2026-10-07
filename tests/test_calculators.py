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


class TestMeanStd(unittest.TestCase):
    def setUp(self):
        self._weights_values_file = Path(Path(__file__).parent, 'fixtures/calculators', 'weights_and_values.json')
        self._mean_expected = np.array([4.48577620473112, 4.48577620473112 * 2.0])
        self._std_expected = np.array([2.6294520822489, 2.6294520822489 * 2.0])

        w_and_v = json.load(open(self._weights_values_file))
        self._weights = np.array(w_and_v['weights'])
        self._values = np.array(w_and_v['values'])
        self._values = np.vstack((self._values, self._values * 2.0)).transpose()

    def test_weighted_avg_and_std(self):
        mean, std = weighted_avg_and_std(self._values, self._weights)

        assert mean == pytest.approx(self._mean_expected)
        assert std == pytest.approx(self._std_expected)

    def test_zero_mean(self):
        values = self._values * 0.0
        mean, std = weighted_avg_and_std(values, self._weights)
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


# coverage cannot trace numba compiled code, so the kernel is also run as plain Python (py_func)
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

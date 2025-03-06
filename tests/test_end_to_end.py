from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

import toshi_hazard_post.local_config
from toshi_hazard_post.aggregation import run_aggregation
from toshi_hazard_post.aggregation_args import AggregationArgs

fixture_dir = Path(__file__).parent / 'fixtures' / 'end_to_end'
parquet_filepath = fixture_dir / 'rlz_probs.pq'
csv_filepath = fixture_dir / 'rlz_probs.csv'
args_filepath = fixture_dir / 'hazard.toml'
aggs_expected_filepath = fixture_dir / 'aggregations_275_PGA_-34.500~173.000.npy'


@mock.patch('toshi_hazard_post.aggregation_calc.save_aggregations')
@mock.patch('toshi_hazard_post.aggregation_calc.load_realizations')
def test_end_to_end(load_mock, save_mock, monkeypatch):
    aggs_expected = np.load(aggs_expected_filepath)

    def mock_config():
        return dict(NUM_WORKERS=1)

    monkeypatch.setattr(toshi_hazard_post.local_config, 'get_config', mock_config)
    load_mock.return_value = pd.read_parquet(parquet_filepath)
    agg_args = AggregationArgs(args_filepath)
    run_aggregation(agg_args)
    aggs = save_mock.mock_calls[0].args[0]

    np.testing.assert_allclose(aggs, aggs_expected)

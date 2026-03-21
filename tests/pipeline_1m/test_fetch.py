"""Tests for cex_data_feed.pipeline_1m.fetch."""
from unittest.mock import patch

import pandas as pd
import pytest

from cex_data_feed.binance.api import Kline
from cex_data_feed.pipeline_1m.fetch import fetch_closed_1m_candles


def _make_klines(timestamps_ms, close_times_ms):
    """Create a list of Kline objects for testing."""
    klines = []
    for i, (ot, ct) in enumerate(zip(timestamps_ms, close_times_ms)):
        klines.append(
            Kline(
                open_time_ms=ot,
                open="100.0",
                high="101.0",
                low="99.0",
                close="100.5",
                volume="1000.0",
                close_time_ms=ct,
                quote_asset_volume="50000.0",
                num_trades=100,
                taker_buy_base_volume="500.0",
                taker_buy_quote_volume="25000.0",
            )
        )
    return klines


class TestFetchClosed1mCandles:
    @patch("cex_data_feed.pipeline_1m.fetch.fetch_klines")
    def test_excludes_in_progress_candle(self, mock_fetch):
        """The currently-forming candle (close_time in the future) should be excluded."""
        now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
        one_min_ms = 60_000

        # 3 candles: 2 closed (close_time in the past) + 1 in-progress (close_time in future)
        open_times = [
            now_ms - 3 * one_min_ms,
            now_ms - 2 * one_min_ms,
            now_ms - 1 * one_min_ms,  # in-progress
        ]
        close_times = [
            now_ms - 2 * one_min_ms - 1,  # closed
            now_ms - 1 * one_min_ms - 1,  # closed
            now_ms + one_min_ms,            # in-progress (future)
        ]

        mock_fetch.return_value = _make_klines(open_times, close_times)

        df = fetch_closed_1m_candles(symbol="BTCUSDT", limit=3)

        assert len(df) == 2
        assert "_close_time" not in df.columns

    @patch("cex_data_feed.pipeline_1m.fetch.fetch_klines")
    def test_empty_response(self, mock_fetch):
        mock_fetch.return_value = []
        df = fetch_closed_1m_candles()
        assert df.empty

    @patch("cex_data_feed.pipeline_1m.fetch.fetch_klines")
    def test_all_closed(self, mock_fetch):
        """When all candles are closed, all should be returned."""
        now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
        one_min_ms = 60_000

        open_times = [now_ms - 3 * one_min_ms, now_ms - 2 * one_min_ms]
        close_times = [now_ms - 2 * one_min_ms - 1, now_ms - one_min_ms - 1]

        mock_fetch.return_value = _make_klines(open_times, close_times)

        df = fetch_closed_1m_candles(symbol="BTCUSDT", limit=2)
        assert len(df) == 2

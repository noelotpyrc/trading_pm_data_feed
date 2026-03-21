"""Tests for cex_data_feed.pipeline_1m.fetch."""
from unittest.mock import patch, call

import pandas as pd
import pytest

from cex_data_feed.binance.api import Kline
from cex_data_feed.pipeline_1m.fetch import fetch_closed_1m_since


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


class TestFetchClosed1mSince:
    @patch("cex_data_feed.pipeline_1m.fetch.fetch_klines")
    def test_excludes_in_progress_candle(self, mock_fetch):
        """The currently-forming candle (close_time in the future) should be excluded."""
        now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
        one_min_ms = 60_000

        open_times = [
            now_ms - 3 * one_min_ms,
            now_ms - 2 * one_min_ms,
            now_ms - 1 * one_min_ms,  # in-progress
        ]
        close_times = [
            now_ms - 2 * one_min_ms - 1,
            now_ms - 1 * one_min_ms - 1,
            now_ms + one_min_ms,  # in-progress (future)
        ]

        mock_fetch.return_value = _make_klines(open_times, close_times)
        start = pd.Timestamp(now_ms - 3 * one_min_ms, unit="ms").tz_localize(None)
        df = fetch_closed_1m_since(start_ts=start, symbol="BTCUSDT")

        assert len(df) == 2
        assert "_close_time" not in df.columns

    @patch("cex_data_feed.pipeline_1m.fetch.fetch_klines")
    def test_empty_response(self, mock_fetch):
        mock_fetch.return_value = []
        start = pd.Timestamp("2024-01-01 00:00:00")
        df = fetch_closed_1m_since(start_ts=start)
        assert df.empty

    @patch("cex_data_feed.pipeline_1m.fetch.fetch_klines")
    def test_all_closed(self, mock_fetch):
        """When all candles are closed, all should be returned."""
        now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
        one_min_ms = 60_000

        open_times = [now_ms - 3 * one_min_ms, now_ms - 2 * one_min_ms]
        close_times = [now_ms - 2 * one_min_ms - 1, now_ms - one_min_ms - 1]

        mock_fetch.return_value = _make_klines(open_times, close_times)
        start = pd.Timestamp(now_ms - 3 * one_min_ms, unit="ms").tz_localize(None)
        df = fetch_closed_1m_since(start_ts=start, symbol="BTCUSDT")
        assert len(df) == 2

    @patch("cex_data_feed.pipeline_1m.fetch.fetch_klines")
    def test_pages_when_needed(self, mock_fetch):
        """Should page through multiple API calls when results equal the limit."""
        now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
        one_min_ms = 60_000

        # First call returns _API_MAX_LIMIT items (simulate full page)
        batch1_open = [now_ms - (3 - i) * one_min_ms for i in range(3)]
        batch1_close = [ot + one_min_ms - 1 for ot in batch1_open]

        # Second call returns fewer (last page)
        batch2_open = [now_ms - 0 * one_min_ms]  # but this one is in-progress
        batch2_close = [now_ms + one_min_ms]

        mock_fetch.side_effect = [
            _make_klines(batch1_open, batch1_close),
            _make_klines(batch2_open, batch2_close),
        ]

        # Patch _API_MAX_LIMIT to 3 so we trigger paging
        with patch("cex_data_feed.pipeline_1m.fetch._API_MAX_LIMIT", 3):
            start = pd.Timestamp(batch1_open[0], unit="ms").tz_localize(None)
            df = fetch_closed_1m_since(start_ts=start)

        assert mock_fetch.call_count == 2
        assert len(df) == 3  # 3 closed from first batch, 0 from second

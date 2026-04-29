"""Tests for cex_data_feed.pipeline_1m_coinbase.fetch."""
from unittest.mock import patch

import pandas as pd

from cex_data_feed.coinbase.api import Candle
from cex_data_feed.pipeline_1m_coinbase.fetch import fetch_closed_1m_since


def _make_candles(open_times_s):
    """Create Coinbase Candle objects at the given open times (epoch seconds)."""
    return [
        Candle(
            open_time_s=ot,
            low="99.0",
            high="101.0",
            open="100.0",
            close="100.5",
            volume="1000.0",
        )
        for ot in open_times_s
    ]


class TestFetchClosed1mSince:
    @patch("cex_data_feed.pipeline_1m_coinbase.fetch.time.sleep", lambda *_: None)
    @patch("cex_data_feed.pipeline_1m_coinbase.fetch.fetch_candles")
    def test_excludes_in_progress_candle(self, mock_fetch):
        """The currently-forming candle (close_time >= now) should be excluded."""
        now_s = int(pd.Timestamp.utcnow().timestamp())
        now_s -= now_s % 60  # floor to minute → wall_now > now_s
        opens = [
            now_s - 120,  # closed
            now_s - 60,   # closed
            now_s,        # in-progress (close_time = now_s + 60 > wall_now)
        ]
        mock_fetch.return_value = _make_candles(opens)

        start = pd.Timestamp(now_s - 120, unit="s").tz_localize(None)
        df = fetch_closed_1m_since(start_ts=start, product_id="BTC-USD")

        assert len(df) == 2
        assert "_close_time" not in df.columns

    @patch("cex_data_feed.pipeline_1m_coinbase.fetch.time.sleep", lambda *_: None)
    @patch("cex_data_feed.pipeline_1m_coinbase.fetch.fetch_candles")
    def test_empty_response(self, mock_fetch):
        mock_fetch.return_value = []
        start = pd.Timestamp.utcnow().tz_localize(None).floor("min") - pd.Timedelta(minutes=5)
        df = fetch_closed_1m_since(start_ts=start)
        assert df.empty

    @patch("cex_data_feed.pipeline_1m_coinbase.fetch.time.sleep", lambda *_: None)
    @patch("cex_data_feed.pipeline_1m_coinbase.fetch.fetch_candles")
    def test_pages_across_window(self, mock_fetch):
        """Should make multiple calls when request span exceeds the 5h window."""
        now_floor_s = int(pd.Timestamp.utcnow().tz_localize(None).floor("min").timestamp())
        # Ask for ~7h of history (420 minutes) → forces 2 windows
        start_s = now_floor_s - 420 * 60

        def _side_effect(product_id, granularity, start_iso, end_iso):
            s = int(pd.Timestamp(start_iso).timestamp())
            e = int(pd.Timestamp(end_iso).timestamp())
            opens = list(range(s, e, 60))
            return _make_candles(opens)

        mock_fetch.side_effect = _side_effect

        start = pd.Timestamp(start_s, unit="s").tz_localize(None)
        df = fetch_closed_1m_since(start_ts=start)

        assert mock_fetch.call_count >= 2
        # Should not have duplicate timestamps after dedup
        assert df["timestamp"].is_unique
        assert df["timestamp"].is_monotonic_increasing

"""
Fetch helpers for the 1m Coinbase accumulator.

Wraps coinbase.api to return only *fully closed* 1m candles.
A candle is considered closed when its close_time < now (UTC).

Coinbase Exchange caps a single candles request at 300 rows, so this module
pages through fixed [start, end] windows of 5 hours each (300 * 60s).
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import pandas as pd

from cex_data_feed.coinbase.api import fetch_candles, candles_to_dataframe


DEFAULT_PRODUCT = "BTC-USD"
_GRANULARITY = 60                   # 1m candles
_WINDOW_S = 300 * _GRANULARITY      # 5h window = 300 candles, the API max
_SLEEP_BETWEEN_CALLS_S = 0.4        # public limit is 3 req/s; stay well under


def _to_iso_utc(ts: pd.Timestamp) -> str:
    """Convert a naive (UTC) Timestamp to an ISO-8601 string Coinbase accepts."""
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.isoformat()


def fetch_closed_1m_since(
    start_ts: pd.Timestamp,
    product_id: str = DEFAULT_PRODUCT,
) -> pd.DataFrame:
    """Fetch all closed 1m candles from start_ts to now, paging in 5h windows."""
    now_utc = datetime.now(timezone.utc)
    end_floor = pd.Timestamp(now_utc).tz_convert(None).floor("min")

    cur = pd.Timestamp(start_ts).floor("min")
    all_dfs: list[pd.DataFrame] = []

    while cur < end_floor:
        win_end = min(cur + pd.Timedelta(seconds=_WINDOW_S), end_floor)
        candles = fetch_candles(
            product_id,
            _GRANULARITY,
            start_iso=_to_iso_utc(cur),
            end_iso=_to_iso_utc(win_end),
        )
        if candles:
            df = candles_to_dataframe(candles, _GRANULARITY)
            now_ts = pd.Timestamp(datetime.now(timezone.utc)).tz_convert(None)
            closed = df[df["_close_time"] < now_ts].copy()
            closed = closed.drop(columns=["_close_time"], errors="ignore")
            if not closed.empty:
                all_dfs.append(closed)

        cur = win_end
        if cur < end_floor:
            time.sleep(_SLEEP_BETWEEN_CALLS_S)

    if not all_dfs:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

    result = pd.concat(all_dfs, ignore_index=True)
    result = (
        result.drop_duplicates(subset="timestamp", keep="last")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    return result

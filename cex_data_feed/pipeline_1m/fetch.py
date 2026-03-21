"""
Fetch helpers for the 1m accumulator.

Wraps the existing binance.api module to return only *fully closed* 1m candles.
A candle is considered closed when its close_time < now (UTC).
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from cex_data_feed.binance.api import fetch_klines, klines_to_dataframe


DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_LIMIT = 10  # bars per poll; covers any gap from a few missed runs


def fetch_closed_1m_candles(
    symbol: str = DEFAULT_SYMBOL,
    limit: int = DEFAULT_LIMIT,
) -> pd.DataFrame:
    """Fetch recent 1m candles and return only those that are fully closed.

    A candle is 'fully closed' when its close_time is strictly before the
    current UTC time.  The currently-forming bar is excluded.

    Returns a DataFrame with columns:
        timestamp, open, high, low, close, volume,
        quote_asset_volume, num_trades,
        taker_buy_base_volume, taker_buy_quote_volume
    (sorted ascending by timestamp, _close_time column dropped)
    """
    klines = fetch_klines(symbol, "1m", limit)
    df = klines_to_dataframe(klines)

    if df.empty:
        return df

    now_utc = pd.Timestamp(datetime.now(timezone.utc)).tz_convert(None)
    closed = df[df["_close_time"] < now_utc].copy()
    closed = closed.drop(columns=["_close_time"], errors="ignore")
    closed = closed.sort_values("timestamp").reset_index(drop=True)
    return closed

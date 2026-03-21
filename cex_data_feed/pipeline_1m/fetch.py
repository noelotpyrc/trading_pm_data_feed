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
_API_MAX_LIMIT = 1500  # Binance max per request


def fetch_closed_1m_since(
    start_ts: pd.Timestamp,
    symbol: str = DEFAULT_SYMBOL,
) -> pd.DataFrame:
    """Fetch all closed 1m candles from start_ts to now, paging if needed.

    Pages through Binance API in batches of 1500. Returns all fully closed
    candles (close_time < now UTC) sorted ascending by timestamp.
    """
    start_ms = int(start_ts.timestamp() * 1000)
    all_dfs = []

    while True:
        klines = fetch_klines(symbol, "1m", limit=_API_MAX_LIMIT, start_time_ms=start_ms)
        if not klines:
            break

        df = klines_to_dataframe(klines)
        if df.empty:
            break

        now_utc = pd.Timestamp(datetime.now(timezone.utc)).tz_convert(None)
        closed = df[df["_close_time"] < now_utc].copy()
        closed = closed.drop(columns=["_close_time"], errors="ignore")

        if closed.empty:
            break

        all_dfs.append(closed)

        # If we got fewer than the limit, we've reached the end
        if len(klines) < _API_MAX_LIMIT:
            break

        # Move start forward past the last candle we received
        last_open_ms = klines[-1].open_time_ms
        start_ms = last_open_ms + 60_000  # next minute

    if not all_dfs:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume",
                     "quote_asset_volume", "num_trades",
                     "taker_buy_base_volume", "taker_buy_quote_volume"]
        )

    result = pd.concat(all_dfs, ignore_index=True)
    result = result.sort_values("timestamp").reset_index(drop=True)
    return result

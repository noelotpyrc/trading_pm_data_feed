from __future__ import annotations

from dataclasses import dataclass
from typing import List
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json

import pandas as pd


COINBASE_EXCHANGE = "https://api.exchange.coinbase.com"


@dataclass(frozen=True)
class Candle:
    """One Coinbase Exchange candle.

    Coinbase returns rows as [time, low, high, open, close, volume] where
    `time` is the candle open time in epoch seconds.
    """
    open_time_s: int
    low: str
    high: str
    open: str
    close: str
    volume: str


def _build_candles_url(
    product_id: str,
    granularity: int,
    start_iso: str | None = None,
    end_iso: str | None = None,
) -> str:
    params: dict = {"granularity": granularity}
    if start_iso is not None:
        params["start"] = start_iso
    if end_iso is not None:
        params["end"] = end_iso
    qs = urlencode(params)
    return f"{COINBASE_EXCHANGE}/products/{product_id}/candles?{qs}"


def fetch_candles(
    product_id: str,
    granularity: int,
    start_iso: str | None = None,
    end_iso: str | None = None,
) -> List[Candle]:
    """Fetch candles from Coinbase Exchange public REST API.

    granularity is in seconds; valid values are {60, 300, 900, 3600, 21600, 86400}.
    Coinbase returns at most 300 candles per request; if the [start, end] window
    requires more, the request is rejected with HTTP 400.

    Coinbase returns candles in descending time order. The returned list here
    preserves API order; callers should sort if needed.
    """
    url = _build_candles_url(product_id, granularity, start_iso, end_iso)
    req = Request(url, headers={"User-Agent": "ohlcv-feed/1.0"})
    with urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read())
    candles: List[Candle] = []
    for row in payload:
        candles.append(
            Candle(
                open_time_s=int(row[0]),
                low=str(row[1]),
                high=str(row[2]),
                open=str(row[3]),
                close=str(row[4]),
                volume=str(row[5]),
            )
        )
    return candles


def candles_to_dataframe(candles: List[Candle], granularity: int) -> pd.DataFrame:
    """Map raw candles into canonical DataFrame.

    Columns: timestamp, open, high, low, close, volume, _close_time

    - timestamp: pandas datetime64[ns] (UTC, naive)
    - _close_time: timestamp + granularity seconds (so callers can filter
      in-progress candles, mirroring the binance pipeline)
    - sorted ascending by timestamp
    """
    if not candles:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        ).astype(
            {
                "timestamp": "datetime64[ns]",
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": float,
            }
        )
    df = pd.DataFrame(
        [
            {
                "timestamp": pd.to_datetime(c.open_time_s, unit="s", utc=True).tz_convert(None),
                "open": float(c.open),
                "high": float(c.high),
                "low": float(c.low),
                "close": float(c.close),
                "volume": float(c.volume),
                "_close_time": pd.to_datetime(c.open_time_s + granularity, unit="s", utc=True).tz_convert(None),
            }
            for c in candles
        ]
    )
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    return df

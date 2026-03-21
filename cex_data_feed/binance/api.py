from __future__ import annotations

from dataclasses import dataclass
from typing import List
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json

import pandas as pd


BINANCE_FAPI = "https://fapi.binance.com"


@dataclass(frozen=True)
class Kline:
    open_time_ms: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    close_time_ms: int
    quote_asset_volume: str | None = None
    num_trades: int = 0
    taker_buy_base_volume: str | None = None
    taker_buy_quote_volume: str | None = None


def _build_klines_url(
    symbol: str,
    interval: str,
    limit: int | None = None,
    start_time_ms: int | None = None,
) -> str:
    params: dict = {"symbol": symbol, "interval": interval}
    if limit is not None:
        params["limit"] = limit
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    qs = urlencode(params)
    return f"{BINANCE_FAPI}/fapi/v1/klines?{qs}"


def fetch_klines(
    symbol: str,
    interval: str,
    limit: int | None = None,
    start_time_ms: int | None = None,
) -> List[Kline]:
    """Fetch klines from Binance Futures API.

    If start_time_ms is provided, fetches candles starting from that time.
    Returns a list of Kline with string price/volume fields as returned by the API.
    """
    url = _build_klines_url(symbol, interval, limit, start_time_ms)
    req = Request(url, headers={"User-Agent": "ohlcv-feed/1.0"})
    with urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read())
    klines: List[Kline] = []
    for row in payload:
        klines.append(
            Kline(
                open_time_ms=int(row[0]),
                open=str(row[1]),
                high=str(row[2]),
                low=str(row[3]),
                close=str(row[4]),
                volume=str(row[5]),
                close_time_ms=int(row[6]),
                quote_asset_volume=str(row[7]),
                num_trades=int(row[8]),
                taker_buy_base_volume=str(row[9]),
                taker_buy_quote_volume=str(row[10]),
            )
        )
    return klines


def klines_to_dataframe(klines: List[Kline]) -> pd.DataFrame:
    """Map raw klines into canonical DataFrame.

    Columns: timestamp, open, high, low, close, volume, quote_asset_volume,
             num_trades, taker_buy_base_volume, taker_buy_quote_volume, _close_time

    - timestamp: pandas datetime64[ns] (UTC, naive by convention)
    - numerical columns: float64 (num_trades: int)
    - sorted ascending by timestamp
    """
    if not klines:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_asset_volume",
                "num_trades",
                "taker_buy_base_volume",
                "taker_buy_quote_volume",
            ]
        ).astype(
            {
                "timestamp": "datetime64[ns]",
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": float,
                "quote_asset_volume": float,
                "num_trades": int,
                "taker_buy_base_volume": float,
                "taker_buy_quote_volume": float,
            }
        )
    df = pd.DataFrame(
        [
            {
                "timestamp": pd.to_datetime(k.open_time_ms, unit="ms", utc=True).tz_convert(None),
                "open": float(k.open),
                "high": float(k.high),
                "low": float(k.low),
                "close": float(k.close),
                "volume": float(k.volume),
                "quote_asset_volume": float(k.quote_asset_volume) if k.quote_asset_volume is not None else None,
                "num_trades": k.num_trades,
                "taker_buy_base_volume": float(k.taker_buy_base_volume) if k.taker_buy_base_volume is not None else None,
                "taker_buy_quote_volume": float(k.taker_buy_quote_volume) if k.taker_buy_quote_volume is not None else None,
                "_close_time": pd.to_datetime(k.close_time_ms, unit="ms", utc=True).tz_convert(None),
            }
            for k in klines
        ]
    )
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    return df

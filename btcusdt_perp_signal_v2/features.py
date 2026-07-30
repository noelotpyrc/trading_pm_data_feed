"""
Base features for btcusdt_perp_signal_v2 — SPEC_book25_fcfs.md §2, exact.

Self-owned (does not import the v1 btcusdt_perp_signal.features, which covers only a subset). Every
feature here enters the rules only through its trailing decile (ranks.py); the raw values are the
input to that. Columns required expects: open, high, low, close, volume, taker_buy_base_volume.

The §2.1 vwap_60_cross_rate quirk (one cross = two ternary transitions) is reproduced exactly.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

EPS = 1e-9

# feature names the book ranks (superset of what any single cell needs)
EFFICIENCY_WINDOWS = (60, 240, 360)
CROSS_RATE_WINDOWS = (30, 60, 90, 180)
GAP_WINDOWS = (90, 120, 180)
TAKER_SPANS = (15, 30)
CUM_KS = (3, 4, 5)


def parkinson(high: pd.Series, low: pd.Series, window: int) -> pd.Series:
    log_hl = np.log(high / low)
    return np.sqrt((1.0 / (4.0 * np.log(2.0))) * (log_hl ** 2).rolling(window).mean())


def rolling_vwap(o, h, low, c, volume, window: int) -> pd.Series:
    tp = (o + h + low + c) / 4.0
    return (tp * volume).rolling(window).sum() / volume.rolling(window).sum()


def efficiency_ratio(close: pd.Series, window: int) -> pd.Series:
    net = close - close.shift(window)
    path = close.diff().abs().rolling(window, min_periods=window).sum()
    return (net / (path + EPS)).abs().clip(0.0, 1.0)


def cross_rate(close: pd.Series, vwap_60: pd.Series, window: int) -> pd.Series:
    """§2.1: ternary cross_dir, then count of its state transitions over `window` (min_periods=1).
    A cross returns cross_dir to 0 the next bar, so one cross contributes two transitions."""
    prev_above = close.shift(1) > vwap_60.shift(1)
    curr_above = close > vwap_60
    cross_dir = pd.Series(0, index=close.index, dtype="int64")
    cross_dir[(~prev_above) & curr_above] = 1
    cross_dir[prev_above & (~curr_above)] = -1
    trans = (cross_dir != cross_dir.shift(1)).astype("int64")
    return trans.rolling(window, min_periods=1).sum()


def taker_imb_ema(taker_buy: pd.Series, volume: pd.Series, span: int) -> pd.Series:
    mp = max(2, span // 5)
    net = 2.0 * taker_buy - volume
    num = net.ewm(span=span, adjust=False, min_periods=mp).mean()
    den = volume.ewm(span=span, adjust=False, min_periods=mp).mean().clip(lower=EPS)
    return (num / den).clip(-1.0, 1.0)


def intrabar_return_zscore(open_: pd.Series, close: pd.Series, window: int = 1440) -> pd.Series:
    ir = (close - open_) / open_
    mean = ir.rolling(window, min_periods=100).mean()
    std = ir.rolling(window, min_periods=100).std()
    return (ir - mean) / std


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Attach every §2 feature the book-11 cells + regime need. Mutates and returns df."""
    o, h, low, c = df["open"], df["high"], df["low"], df["close"]
    vol, taker = df["volume"], df["taker_buy_base_volume"]

    df["parkinson_30"] = parkinson(h, low, 30)
    df["parkinson_1440"] = parkinson(h, low, 1440)

    vwap_60 = rolling_vwap(o, h, low, c, vol, 60)
    for r in CROSS_RATE_WINDOWS:
        df[f"vwap_60_cross_rate_{r}"] = cross_rate(c, vwap_60, r)
    for x in GAP_WINDOWS:
        vwap_x = rolling_vwap(o, h, low, c, vol, x)
        df[f"vwap_gap_norm_{x}"] = (c - vwap_x) / c

    for x in EFFICIENCY_WINDOWS:
        df[f"efficiency_ratio_{x}"] = efficiency_ratio(c, x)

    for s in TAKER_SPANS:
        df[f"taker_imb_ema_{s}"] = taker_imb_ema(taker, vol, s)

    df["intrabar_return_zscore_1440"] = intrabar_return_zscore(o, c, 1440)

    p30 = df["parkinson_30"].clip(lower=1e-9)
    for k in CUM_KS:
        cum = c / o.shift(k - 1) - 1.0
        df[f"cum_return_{k}bar_norm_p30"] = cum / p30

    return df

"""
Feature calculations for BTCUSDT perp signal strategy.

All functions take a DataFrame with OHLCV + num_trades columns
and return it with feature columns attached. Designed to work on
the last ~1500 rows of 1m data (1440 needed for parkinson_1440 +
warmup for other rolling windows).
"""
from __future__ import annotations

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def parkinson_volatility(high: pd.Series, low: pd.Series, window: int) -> pd.Series:
    log_hl_ratio = np.log(high / low)
    parkinson_values = log_hl_ratio ** 2
    return np.sqrt((1 / (4 * np.log(2))) * parkinson_values.rolling(window).mean())


def avg_trade_size(volume: pd.Series, num_trades: pd.Series,
                   eps: float = 1e-9) -> pd.Series:
    den = num_trades.replace(0, np.nan)
    return np.log1p(volume / den)


def avg_trade_size_zscore(volume: pd.Series, num_trades: pd.Series,
                          window: int, eps: float = 1e-9) -> pd.Series:
    ats = avg_trade_size(volume, num_trades, eps)
    roll_mean = ats.rolling(window, min_periods=window).mean()
    roll_std = ats.rolling(window, min_periods=window).std()
    z = (ats - roll_mean) / (roll_std + eps)
    return z.clip(-5.0, 5.0)


def rolling_vwap(open_: pd.Series, high: pd.Series, low: pd.Series,
                 close: pd.Series, volume: pd.Series, window: int) -> pd.Series:
    typical_price = (open_ + high + low + close) / 4
    pv = typical_price * volume
    return pv.rolling(window).sum() / volume.rolling(window).sum()


def detect_cross_direction(close: pd.Series, vwap: pd.Series) -> pd.Series:
    prev_above = close.shift(1) > vwap.shift(1)
    curr_above = close > vwap
    cross_direction = pd.Series(None, index=close.index, dtype="object")
    cross_direction[~prev_above & curr_above] = "above"
    cross_direction[prev_above & ~curr_above] = "below"
    return cross_direction


# ---------------------------------------------------------------------------
# Main feature builder
# ---------------------------------------------------------------------------

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Attach all strategy features to df. Expects columns:
    open, high, low, close, volume, num_trades.

    Returns the same DataFrame with feature columns added.
    """
    # 1. Parkinson volatility
    df["parkinson_30"] = parkinson_volatility(df["high"], df["low"], window=30)
    df["parkinson_1440"] = parkinson_volatility(df["high"], df["low"], window=1440)
    df["parkinson_ratio"] = df["parkinson_30"] / df["parkinson_1440"]

    # 2. Volume ratios
    df["volume_sma_30"] = df["volume"].rolling(window=30).mean()
    df["volume_sma_60"] = df["volume"].rolling(window=60).mean()
    df["volume_ratio_30"] = df["volume"] / df["volume_sma_30"]
    df["volume_ratio_60"] = df["volume"] / df["volume_sma_60"]

    # 3. Avg trade size z-score
    df["avg_trade_size_zscore_60"] = avg_trade_size_zscore(
        df["volume"], df["num_trades"], window=60
    )

    # 4. VWAP and cross rate
    df["vwap_60"] = rolling_vwap(
        df["open"], df["high"], df["low"], df["close"], df["volume"], window=60
    )
    cross_dir = detect_cross_direction(df["close"], df["vwap_60"])
    numeric = cross_dir.map({"above": 1, "below": -1}).fillna(0)
    crosses = (numeric != numeric.shift(1)).astype(int)
    df["vwap_60_cross_rate_90"] = crosses.rolling(90, min_periods=1).sum()

    # 5. Cumulative return normalized by vol
    df["cum_return_5bar"] = df["close"] / df["open"].shift(4) - 1
    df["cum_return_5bar_norm_p30"] = df["cum_return_5bar"] / df["parkinson_30"].clip(lower=1e-9)

    # 6. Efficiency ratio
    eps = 1e-9
    net_return = df["close"] - df["close"].shift(360)
    abs_path = df["close"].diff().abs().rolling(360, min_periods=360).sum()
    raw_efficiency = net_return / (abs_path + eps)
    df["efficiency_ratio_360"] = raw_efficiency.abs().clip(0.0, 1.0)

    return df


# ---------------------------------------------------------------------------
# Signal detection
# ---------------------------------------------------------------------------

COMMON_FILTERS = {
    "parkinson_ratio": (">=", 1.5),
    "volume_ratio_30": (">=", 1.0),
    "avg_trade_size_zscore_60": (">=", 1.0),
    "vwap_60_cross_rate_90": (">=", 4.0),
}

LONG_FILTERS = {
    "volume_ratio_60": (">=", 2.0),
    "cum_return_5bar_norm_p30": (">=", 5.0),
}

SHORT_FILTERS = {
    "volume_ratio_60": ("<=", 1.5),
    "efficiency_ratio_360": ("<=", 0.04),
    "cum_return_5bar_norm_p30": ("<=", 0.0),
}


def _check_filters(row: pd.Series, filters: dict) -> bool:
    for col, (op, thresh) in filters.items():
        val = row.get(col)
        if val is None or np.isnan(val):
            return False
        if op == ">=" and val < thresh:
            return False
        if op == "<=" and val > thresh:
            return False
    return True


def check_signal(row: pd.Series) -> str | None:
    """Check if the latest row triggers a signal.

    Returns 'long', 'short', or None.
    """
    if not _check_filters(row, COMMON_FILTERS):
        return None
    if _check_filters(row, LONG_FILTERS):
        return "long"
    if _check_filters(row, SHORT_FILTERS):
        return "short"
    return None

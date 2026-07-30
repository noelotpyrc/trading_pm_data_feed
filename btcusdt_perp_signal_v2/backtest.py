"""
Offline replay — SPEC_dryrun_book11.md §7 reconciliation path.

Computes §2 features, §3 deciles (vectorized pandas rolling rank — the normative reference the live
incremental engine must reconcile against), and drives the §4 FCFS state machine bar-by-bar. Produces
the same FIRING/ENTRY/RESULT events as the live dry run, minus the order-book execution columns.

`ret_research_bps` here is the exact reconciliation target for the live run (§7): any gap beyond
floating-point rounding is a pipeline bug, not edge decay.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from btcusdt_perp_signal_v2 import cells, config, features
from btcusdt_perp_signal_v2.decision import Decision

_OHLCV_TABLE = "ohlcv_btcusdt_1m"


def load_bars(db_path: Path, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """Deduped OHLCV bars (latest ingested_at per timestamp), ascending, with a clean RangeIndex."""
    con = sqlite3.connect(str(db_path))
    try:
        q = f"""
            SELECT timestamp, open, high, low, close, volume, num_trades, taker_buy_base_volume
            FROM {_OHLCV_TABLE}
            WHERE id IN (SELECT id FROM (
                SELECT id, ROW_NUMBER() OVER (
                    PARTITION BY timestamp ORDER BY ingested_at DESC, id DESC) rn FROM {_OHLCV_TABLE})
                WHERE rn = 1)
        """
        params: list = []
        if start:
            q += " AND timestamp >= ?"; params.append(start)
        if end:
            q += " AND timestamp <= ?"; params.append(end)
        q += " ORDER BY timestamp ASC"
        df = pd.read_sql_query(q, con, params=params, parse_dates=["timestamp"])
    finally:
        con.close()
    return df.reset_index(drop=True)


def _pct_to_decile(pct: pd.Series) -> pd.Series:
    d = np.floor(pct * 10.0)
    d = np.clip(d, 0, 9) + 1.0
    return d.where(pct.notna())          # NaN pct -> NaN decile (cell can't fire)


def compute_deciles(df: pd.DataFrame, pairs=None) -> dict:
    """{(feature, window): int-or-None decile Series} via rolling(W, min_periods=W//2).rank(pct)."""
    pairs = config.required_series() if pairs is None else pairs
    out = {}
    for feat, win in pairs:
        w = config.WBARS[win]
        pct = df[feat].rolling(w, min_periods=w // 2).rank(pct=True)
        out[(feat, win)] = _pct_to_decile(pct)
    return out


def _arming_index(df: pd.DataFrame) -> int:
    """First bar with every rank window on a full window (§2 warm-up)."""
    return max(config.WBARS.values()) - 1


def replay(df: pd.DataFrame, buffer: int = config.BUFFER, horizon: int = config.N_HORIZON,
           pairs=None) -> dict:
    """Run features -> deciles -> FCFS. Returns {'firings','entries','results', ...}."""
    df = features.compute_features(df.copy())
    dec = compute_deciles(df, pairs)
    arm = _arming_index(df)
    # cast to ms explicitly: pandas datetime64 may be [ns] or [us] depending on version, so a raw
    # astype(int64)//1e6 is unit-dependent (off by 1000x under pandas 3.0's [us] default).
    ts_ms = df["timestamp"].astype("datetime64[ms]").astype("int64").to_numpy()
    close = df["close"].to_numpy()

    dec_arrays = {k: v.to_numpy() for k, v in dec.items()}

    def _decile(i, k):
        v = dec_arrays[k][i]
        return None if (v is None or (isinstance(v, float) and np.isnan(v))) else int(v)

    d = Decision(buffer=buffer, horizon=horizon)
    firings, entries, results = [], [], []
    for i in range(arm, len(df)):
        deciles = {k: _decile(i, k) for k in dec_arrays}
        ev = d.on_bar(int(i), int(ts_ms[i]), float(close[i]), deciles)
        firings.extend(ev.firings)
        if ev.entry is not None:
            entries.append(ev.entry)
        if ev.result is not None:
            results.append(ev.result)
    return {"firings": firings, "entries": entries, "results": results,
            "arming_index": arm, "n_bars": len(df), "df": df, "deciles": dec}


def sanity_stats(res: dict) -> dict:
    """§7 pipeline-defect checks: regime share, decile balance at the extremes, firing counts."""
    df, dec = res["df"], res["deciles"]
    arm = res["arming_index"]
    reg = dec[(config.REGIME_FEATURE, config.REGIME_WINDOW)].iloc[arm:]
    reg_bool = reg.isin(list(config.REGIME_DECILES))
    n_reg = int(reg_bool.sum())
    firings_by_cell = {}
    for f in res["firings"]:
        firings_by_cell[f.cell_id] = firings_by_cell.get(f.cell_id, 0) + 1
    returns = [r.ret_research_bps for r in res["results"]]
    return {
        "bars_armed": len(reg),
        "regime_share": (n_reg / len(reg)) if len(reg) else None,
        "firings_by_cell": firings_by_cell,
        "entries": len(res["entries"]),
        "completed": len(res["results"]),
        "mean_ret_research_bps": (sum(returns) / len(returns)) if returns else None,
    }

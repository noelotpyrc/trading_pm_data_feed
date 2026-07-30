"""Layer A tests for btcusdt_perp_signal_v2 (offline signal core + reconciliation parity)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from btcusdt_perp_signal_v2 import backtest, cells, config, features
from btcusdt_perp_signal_v2.config import Cell
from btcusdt_perp_signal_v2.decision import Decision
from btcusdt_perp_signal_v2.ranks import RankEngine, RollingRank


# ---- features: §2.1 cross-rate quirk -----------------------------------------
def test_cross_rate_one_cross_two_transitions():
    close = pd.Series([1.0, 1.0, 3.0, 3.0, 1.0])
    vwap = pd.Series([2.0, 2.0, 2.0, 2.0, 2.0])
    # single up-cross at t2; cross_dir returns to 0 at t3 -> two transitions inside [t2, t3]
    cr2 = features.cross_rate(close, vwap, 2)
    assert cr2.iloc[3] == 2
    cr_full = features.cross_rate(close, vwap, 5)
    # cumulative transitions: boundary(1) + up-cross(2) + start of down-cross(1) = 4 by t4
    assert cr_full.iloc[4] == 4


# ---- ranks: incremental == pandas rolling rank -------------------------------
@pytest.mark.parametrize("window", [10, 25])
def test_rolling_rank_matches_pandas(window):
    rng = np.random.default_rng(7)
    # mix of continuous and heavily-tied integer values (the cross-rate case)
    s = pd.Series(np.concatenate([rng.normal(size=120), rng.integers(0, 5, size=80).astype(float)]))
    want = backtest._pct_to_decile(s.rolling(window, min_periods=window // 2).rank(pct=True))
    rr = RollingRank(window)
    got = [rr.push(v) for v in s]
    for i, (g, w) in enumerate(zip(got, want)):
        w_int = None if pd.isna(w) else int(w)
        assert g == w_int, f"bar {i}: incremental={g} pandas={w_int}"


def test_rolling_rank_handles_nan_warmup():
    s = pd.Series([np.nan, np.nan, 1.0, 2.0, 3.0, 2.0])
    want = backtest._pct_to_decile(s.rolling(4, min_periods=2).rank(pct=True))
    rr = RollingRank(4)
    got = [rr.push(v) for v in s]
    for g, w in zip(got, want):
        assert g == (None if pd.isna(w) else int(w))


# ---- regime + cell firing ----------------------------------------------------
def _deciles_for(cell: Cell, extra=None):
    d = {(f, cell.window): req for f, req in cell.legs}
    d[(config.REGIME_FEATURE, config.REGIME_WINDOW)] = 9
    if extra:
        d.update(extra)
    return d


def test_cell_fires_only_when_all_legs_and_regime():
    c = config.CELLS[0]
    d = _deciles_for(c)
    assert [x.id for x in cells.firing_cells(d)] == [c.id]
    # break one leg
    f0, _ = c.legs[0]
    d[(f0, c.window)] = 1
    assert cells.firing_cells(d) == []
    # restore, kill regime
    d[(f0, c.window)] = c.legs[0][1]
    d[(config.REGIME_FEATURE, config.REGIME_WINDOW)] = 5
    assert cells.firing_cells(d) == []


def test_nan_decile_blocks_cell():
    c = config.CELLS[3]
    d = _deciles_for(c)
    d[(c.legs[0][0], c.window)] = None
    assert cells.firing_cells(d) == []


# ---- FCFS decision -----------------------------------------------------------
def _fire_deciles(cell: Cell):
    return _deciles_for(cell)


def test_fcfs_single_slot_exit_and_buffer():
    c = config.CELLS[0]
    d = Decision(buffer=2, horizon=5)
    fire = _fire_deciles(c)
    flat = {(config.REGIME_FEATURE, config.REGIME_WINDOW): 1}   # regime off -> no fire

    e0 = d.on_bar(0, 0, 100.0, fire)
    assert e0.entry is not None and e0.entry.cell_id == c.id
    assert e0.firings[0].taken is True

    # firing while in position -> blocked reason
    e1 = d.on_bar(1, 60_000, 101.0, fire)
    assert e1.entry is None
    assert e1.firings[0].taken is False and e1.firings[0].not_taken_reason == "in_position"

    for i in range(2, 5):
        d.on_bar(i, i * 60_000, 100.0 + i, flat)

    # exit at entry_bar + horizon = bar 5 (cell 1 is short -> sign flips the return)
    e5 = d.on_bar(5, 5 * 60_000, 110.0, fire)
    assert e5.result is not None
    assert e5.result.ret_research_bps == pytest.approx(c.sign * (110.0 / 100.0 - 1) * 1e4)
    # same bar: re-entry blocked by buffer (free_from = 5 + 2 = 7)
    assert e5.entry is None and e5.firings[0].not_taken_reason == "buffer"

    e6 = d.on_bar(6, 6 * 60_000, 111.0, fire)
    assert e6.entry is None and e6.firings[0].not_taken_reason == "buffer"
    e7 = d.on_bar(7, 7 * 60_000, 112.0, fire)
    assert e7.entry is not None      # free again at bar 7


def test_lowest_id_tie_break_and_lost_tie():
    # cells 2 (180d short) and 3 (90d long) share no (feature,window) key, so both can co-fire;
    # lowest id (2) is taken, the other is lost_tie, and the side disagreement is resolved by id.
    c_lo, c_hi = config.CELLS[1], config.CELLS[2]
    assert c_lo.id == 2 and c_hi.id == 3 and c_lo.side != c_hi.side
    d = Decision(buffer=0, horizon=180)
    deciles = {(config.REGIME_FEATURE, config.REGIME_WINDOW): 10}
    for c in (c_lo, c_hi):
        for f, req in c.legs:
            deciles[(f, c.window)] = req
    ev = d.on_bar(0, 0, 100.0, deciles)
    taken = [f.cell_id for f in ev.firings if f.taken]
    lost = [(f.cell_id, f.not_taken_reason) for f in ev.firings if not f.taken]
    assert taken == [c_lo.id]
    assert (c_hi.id, "lost_tie") in lost
    assert ev.entry.side == c_lo.side


# ---- reconciliation: incremental deciles == vectorized deciles ---------------
def test_incremental_vs_vectorized_deciles(monkeypatch):
    monkeypatch.setattr(config, "WBARS", {"30d": 40, "90d": 60, "180d": 80})
    pairs = config.required_series()

    rng = np.random.default_rng(3)
    n = 400
    base = 100 + np.cumsum(rng.normal(0, 0.5, n))
    df = pd.DataFrame({
        "timestamp": pd.date_range("2025-01-01", periods=n, freq="min"),
        "open": base, "high": base + rng.random(n), "low": base - rng.random(n),
        "close": base + rng.normal(0, 0.2, n), "volume": rng.random(n) * 100 + 1,
        "num_trades": rng.integers(1, 50, n),
        "taker_buy_base_volume": rng.random(n) * 50,
    })
    feat = features.compute_features(df.copy())
    vec = backtest.compute_deciles(feat, pairs)

    eng = RankEngine(pairs)
    feat_names = {f for f, _ in pairs}
    inc = {p: [] for p in pairs}
    for _, row in feat.iterrows():
        out = eng.push_bar({name: row.get(name) for name in feat_names})
        for p in pairs:
            inc[p].append(out[p])

    for p in pairs:
        want = vec[p]
        for i in range(n):
            w = None if pd.isna(want.iloc[i]) else int(want.iloc[i])
            assert inc[p][i] == w, f"{p} bar {i}: incremental={inc[p][i]} vectorized={w}"

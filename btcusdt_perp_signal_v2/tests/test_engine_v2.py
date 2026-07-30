"""Layer B tests: order-book execution math + engine/backtest decile reconciliation (§5, §7)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from btcusdt_perp_signal_v2 import backtest, book_feed, config
from btcusdt_perp_signal_v2.engine import DryRunEngine


# ---- §5.1 executable ladder --------------------------------------------------
def test_walk_book_vwap_and_partial():
    asks = [(100.0, 1.0), (101.0, 2.0), (102.0, 5.0)]
    px, filled, usd = book_feed.walk_book(asks, 150.0)     # 100 USD @100 + 50 USD @101
    assert filled is True
    assert usd == pytest.approx(150.0)
    assert px == pytest.approx(150.0 / (1.0 + 50.0 / 101.0))
    # more notional than the book holds -> partial fill, not fully filled
    _, filled2, usd2 = book_feed.walk_book(asks, 10_000.0)
    assert filled2 is False
    assert usd2 == pytest.approx(100 * 1 + 101 * 2 + 102 * 5)


def test_executable_ladder_slippage_sign_and_max_fillable():
    asks = [(100.0, 1.0), (101.0, 2.0)]
    lad = book_feed.executable_ladder(asks, mid=99.5, buy=True, notionals=[50, 100_000])
    assert lad["slippage_bps"][0] > 0                       # bought above mid
    assert lad["max_fillable_usd"] == 50                    # big rung can't fully fill
    bids = [(99.0, 5.0)]
    lad_s = book_feed.executable_ladder(bids, mid=99.5, buy=False, notionals=[100])
    assert lad_s["slippage_bps"][0] > 0                     # sold below mid -> positive (sign=-1)


def test_execution_returns_signs():
    long = book_feed.execution_returns(1, [100.0], [110.0], 100.0, 110.0, 1000.0)
    assert long["ret_exec_bps"][0] == pytest.approx(1000.0)
    assert long["cost_bps"][0] == pytest.approx(0.0)
    short = book_feed.execution_returns(-1, [100.0], [90.0], 100.0, 90.0, 1000.0)
    assert short["ret_exec_bps"][0] == pytest.approx(1000.0)   # short profits as price falls


def test_hold_stats_mae_mfe():
    samples = [{"mid": m, "spread_bps": 1.0, "depth": 10.0} for m in (100, 105, 95, 102)]
    st = book_feed.hold_stats(samples, entry_mid=100.0, side_sign=1)
    assert st["mfe_bps"] == pytest.approx(500.0)
    assert st["mae_bps"] == pytest.approx(-500.0)
    assert st["mid_high"] == 105 and st["mid_low"] == 95


# ---- §7 reconciliation: engine seed+push deciles == backtest vectorized ------
def _synth(n, seed=11):
    rng = np.random.default_rng(seed)
    base = 100 + np.cumsum(rng.normal(0, 0.4, n))
    return pd.DataFrame({
        "timestamp": pd.date_range("2025-01-01", periods=n, freq="min"),
        "open": base, "high": base + rng.random(n), "low": base - rng.random(n),
        "close": base + rng.normal(0, 0.3, n), "volume": rng.random(n) * 100 + 1,
        "num_trades": rng.integers(1, 50, n),
        "taker_buy_base_volume": rng.random(n) * 50,
    })


def _bar(row):
    ot = int(pd.Timestamp(row["timestamp"]).value // 1_000_000)
    return {"open_time_ms": ot, "close_time_ms": ot + 59_999, "open": row["open"],
            "high": row["high"], "low": row["low"], "close": row["close"],
            "volume": row["volume"], "taker_buy_base_volume": row["taker_buy_base_volume"],
            "num_trades": int(row["num_trades"])}


def test_engine_deciles_reconcile_with_backtest(tmp_path, monkeypatch):
    # windows > 1440 so parkinson_1440 is defined inside every rank window
    monkeypatch.setattr(config, "WBARS", {"30d": 1500, "90d": 1600, "180d": 1700})
    monkeypatch.setattr(config, "FEATURE_TAIL", 1600)
    n = 2100
    df = _synth(n)

    bt = backtest.replay(df.copy())
    dec = {k: v.to_numpy() for k, v in bt["deciles"].items()}
    arm = max(config.WBARS.values()) - 1
    assert bt["arming_index"] == arm

    eng = DryRunEngine(tmp_path / "v2.sqlite", ohlcv_db_path=tmp_path / "unused.sqlite", feed=None)
    eng._seed_from_frame(df.iloc[:arm].copy())

    captured = []
    orig = eng.decision.on_bar

    def spy(bi, ts, close, deciles, late_ms=0):
        captured.append(dict(deciles))
        return orig(bi, ts, close, deciles, late_ms)

    eng.decision.on_bar = spy
    for i in range(arm, n):
        eng.on_closed_bar(_bar(df.iloc[i]))

    assert len(captured) == n - arm, "engine should decide on every armed live bar"
    pairs = config.required_series()
    for j, deciles in enumerate(captured):
        row = arm + j
        for p in pairs:
            v = dec[p][row]
            want = None if (v is None or (isinstance(v, float) and np.isnan(v))) else int(v)
            assert deciles[p] == want, f"bar {row} {p}: engine={deciles[p]} backtest={want}"


def test_gap_backfill_reconciles(tmp_path, monkeypatch):
    # a <=5-bar WS gap whose missed bars are REST-refilled must still reconcile exactly (findings #1-#3)
    monkeypatch.setattr(config, "WBARS", {"30d": 1500, "90d": 1600, "180d": 1700})
    monkeypatch.setattr(config, "FEATURE_TAIL", 1600)
    n = 2100
    df = _synth(n, seed=5)
    bt = backtest.replay(df.copy())
    dec = {k: v.to_numpy() for k, v in bt["deciles"].items()}
    arm = max(config.WBARS.values()) - 1

    df_ms = [int(pd.Timestamp(t).value // 1_000_000) for t in df["timestamp"]]  # .value is always ns
    by_ms = {df_ms[i]: df.iloc[i] for i in range(n)}

    eng = DryRunEngine(tmp_path / "v2.sqlite", ohlcv_db_path=tmp_path / "unused.sqlite", feed=None)
    eng._seed_from_frame(df.iloc[:arm].copy())
    # REST backfill served from the synthetic frame instead of Binance
    monkeypatch.setattr(eng, "_rest_bars",
                        lambda s, e: [_bar(by_ms[m]) for m in range(s, e, 60_000) if m in by_ms])

    captured = []
    orig = eng.decision.on_bar
    eng.decision.on_bar = lambda bi, ts, c, d, lm=0: (captured.append(dict(d)), orig(bi, ts, c, d, lm))[1]

    skip = {arm + 50, arm + 51, arm + 52}          # 3-bar gap the engine must backfill
    for i in range(arm, n):
        if i in skip:
            continue
        eng.on_closed_bar(_bar(df.iloc[i]))

    assert len(captured) == n - arm, "backfilled bars must also be decided, in order"
    pairs = config.required_series()
    for j, deciles in enumerate(captured):
        row = arm + j
        for p in pairs:
            v = dec[p][row]
            want = None if (v is None or (isinstance(v, float) and np.isnan(v))) else int(v)
            assert deciles[p] == want, f"post-gap bar {row} {p}: engine={deciles[p]} backtest={want}"


def test_residual_gap_triggers_rewarm(tmp_path, monkeypatch):
    # if REST can't supply every missed bar, the engine must re-warm rather than continue offset (#3)
    monkeypatch.setattr(config, "WBARS", {"30d": 1500, "90d": 1600, "180d": 1700})
    monkeypatch.setattr(config, "FEATURE_TAIL", 1600)
    n = 1900
    df = _synth(n, seed=8)
    arm = max(config.WBARS.values()) - 1
    df_ms = [int(pd.Timestamp(t).value // 1_000_000) for t in df["timestamp"]]
    by_ms = {df_ms[i]: df.iloc[i] for i in range(n)}

    eng = DryRunEngine(tmp_path / "v2.sqlite", ohlcv_db_path=tmp_path / "unused.sqlite", feed=None)
    eng._seed_from_frame(df.iloc[:arm].copy())
    for i in range(arm, arm + 10):
        eng.on_closed_bar(_bar(df.iloc[i]))

    called = {"n": 0}
    monkeypatch.setattr(eng, "warm_up", lambda: called.__setitem__("n", called["n"] + 1))
    # gap of 3 bars, but REST returns only the first -> residual -> re-warm
    first_missing = df_ms[arm + 10]
    monkeypatch.setattr(eng, "_rest_bars", lambda s, e: [_bar(by_ms[first_missing])])

    eng.on_closed_bar(_bar(df.iloc[arm + 13]))    # bars arm+10,11,12 were the gap
    assert called["n"] == 1, "residual gap must trigger a re-warm"


def test_exit_settles_when_exact_bar_skipped():
    # position must settle via `>=` (flagged missed_exit) if its exact exit bar is skipped (#1)
    from btcusdt_perp_signal_v2.decision import Decision
    c = config.CELLS[0]
    d = Decision(buffer=0, horizon=5)
    fire = {(f, c.window): req for f, req in c.legs}
    fire[(config.REGIME_FEATURE, config.REGIME_WINDOW)] = 9
    flat = {(config.REGIME_FEATURE, config.REGIME_WINDOW): 1}
    assert d.on_bar(0, 0, 100.0, fire).entry is not None       # enter at bar 0, exit due at bar 5
    for i in range(1, 5):
        d.on_bar(i, i, 100.0, flat)
    ev = d.on_bar(7, 7, 108.0, flat)                            # bar 5 and 6 skipped
    assert ev.result is not None and ev.result.status == "missed_exit"
    assert ev.result.exit_bar_index == 7

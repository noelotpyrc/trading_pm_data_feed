"""
Unit tests for pm_signal_sim (offline, deterministic — no live WS).

Run: /Users/noel/projects/venvs/production/bin/python -m pytest pm_signal_sim/tests -v

Covers the per-second grid, the 3 evaluator families (onset / level-floor / cooldown), capture
staging (lookback + forward, dedup), and the Discord settlement PnL formatter. The reused
pm_shock_signal feed base is exercised live by the separate smoke.
"""
import json
import math
import statistics

import pytest

from pm_signal_sim import config, signal_db, signals_live
from pm_signal_sim.config import SignalConfig
from pm_signal_sim.signals import GridState, MultiDetector, FireEvent
from pm_signal_sim.signals_live import SigEval
from pm_signal_sim.capture import CaptureManager
from pm_signal_sim.discord_report import _fmt_window, _window_passes, sweep_and_alert
from pm_shock_signal.feeds import ActiveMarket, BookTop

E = 1_700_000_400   # 900-aligned epoch


# --------------------------------------------------------------------------- #
# GridState
# --------------------------------------------------------------------------- #

def test_grid_ffill_and_age():
    g = GridState()
    g.update(0, 0.50)          # trade
    g.update(1, None)          # no trade → ffill 0.50
    g.update(2, 0.60)          # trade
    g.update(3, None)          # ffill 0.60
    assert g.p(0) == 0.50 and g.p(1) == 0.50 and g.p(2) == 0.60 and g.p(3) == 0.60
    assert g.traded[0] and not g.traded[1] and g.traded[2] and not g.traded[3]
    assert g.age(2) == 0.0     # sec 2 had a trade
    assert g.age(3) == 1.0     # 1s since the sec-2 trade
    assert g.age(0) == 0.0
    g2 = GridState()
    assert g2.age(5) == math.inf   # no trades yet


# --------------------------------------------------------------------------- #
# Evaluators via MultiDetector (grid-driven fake feed)
# --------------------------------------------------------------------------- #

class FakePm:
    """vwap_window serves both the 1s grid (width=1) and asym windows (by width); set per test."""
    def __init__(self, epoch):
        self.epoch = epoch
        self.market = ActiveMarket(epoch, "UP", "DOWN")
        self.pdet = {}       # sec -> 1s VWAP (grid)
        self.win = {}        # width -> value (asym numer/denom for the current tick)
        self.bid, self.ask = 0.58, 0.61

    def vwap_window(self, token_id, a, b):
        width = round(b - a)
        if width == config.PDET_WIN_SEC:
            return self.pdet.get(int(round(b - self.epoch)))
        return self.win.get(width)

    def price_asof(self, token_id, now):
        return (now, 0.60)

    def book_top(self, token_id):
        return BookTop(ts=0.0, last=None, bid=self.bid, ask=self.ask)


class FakeBtc:
    def mid_now(self):
        return 64000.0


def _drive(det, pm, secs, up_only=True):
    """Run update_grid + on_tick for each sec; return Up fires."""
    fires = []
    for sec in secs:
        det.update_grid(E, sec, [("Up", "UP"), ("Down", "DOWN")])
        fires += det.on_tick(E + sec, E, sec, [("Up", "UP"), ("Down", "DOWN")])
    return [f for f in fires if (f.token == "Up" or not up_only)]


def test_trailmean_onset_and_floor():
    pm = FakePm(E)
    # flat 0.50 then a jump to 0.95 at sec 10 → ratio 0.95/mean(0.5,0.5,0.5,0.5,0.95)=1.61 ≥ 1.5
    for s in range(0, 12):
        pm.pdet[s] = 0.95 if s == 10 else 0.50
    det = MultiDetector(pm, FakeBtc(), configs=[SignalConfig("tm", "trailmean", {"w": 5})])
    fires = _drive(det, pm, range(0, 12))
    assert [f.sec for f in fires] == [10]
    assert fires[0].ratio == pytest.approx(0.95 / ((0.5 * 4 + 0.95) / 5))

    # level floor: same shape but the jump lands at 0.30 (< P_FLOOR 0.5) → no fire
    pm2 = FakePm(E)
    for s in range(0, 12):
        pm2.pdet[s] = 0.30 if s == 10 else 0.18
    det2 = MultiDetector(pm2, FakeBtc(), configs=[SignalConfig("tm", "trailmean", {"w": 5})])
    assert _drive(det2, pm2, range(0, 12)) == []


def test_consistent_onset():
    pm = FakePm(E)
    for s in range(0, 12):
        pm.pdet[s] = 0.80 if s == 10 else 0.50    # min(0.8/0.5, 0.8/0.5)=1.6 ≥ 1.5 at sec 10
    det = MultiDetector(pm, FakeBtc(),
                        configs=[SignalConfig("cs", "consistent", {"horizons": (2, 5)})])
    fires = _drive(det, pm, range(0, 12))
    assert [f.sec for f in fires] == [10]
    assert fires[0].ratio == pytest.approx(1.6)


def test_asym_onset():
    pm = FakePm(E)
    for s in range(0, 25):
        pm.pdet[s] = 0.60                     # p_entry ≥ floor throughout
    cfg = SignalConfig("as", "asym", {"w_now": 2, "gap": 5, "w_base": 10})  # lo = 15
    det = MultiDetector(pm, FakeBtc(), configs=[cfg])
    fires = []
    for sec in range(0, 22):
        if sec == 20:
            pm.win = {2: 0.80, 10: 0.50}      # ratio 1.6 ≥ 1.5
        else:
            pm.win = {2: 0.55, 10: 0.50}      # ratio 1.1 (seeds prev < k)
        det.update_grid(E, sec, [("Up", "UP"), ("Down", "DOWN")])
        fires += det.on_tick(E + sec, E, sec, [("Up", "UP"), ("Down", "DOWN")])
    ups = [f for f in fires if f.token == "Up"]
    assert [f.sec for f in ups] == [20]
    assert ups[0].ratio == pytest.approx(1.6)


def test_grid_backfill_on_skip():
    """A skipped tick (work >1s) must not leave NaN holes — update_grid backfills (REVIEW P1)."""
    pm = FakePm(E)
    for s in range(0, 6):
        pm.pdet[s] = 0.40 + 0.01 * s
    det = MultiDetector(pm, FakeBtc(), configs=[SignalConfig("tm", "trailmean", {"w": 5})])
    det.update_grid(E, 0, [("Up", "UP"), ("Down", "DOWN")])
    det.update_grid(E, 3, [("Up", "UP"), ("Down", "DOWN")])   # skips secs 1 and 2
    g = det.grid(E, "Up")
    assert g.p(1) == pytest.approx(0.41) and g.p(2) == pytest.approx(0.42)
    assert g.p(3) == pytest.approx(0.43)
    assert g.traded[1] and g.traded[2]        # backfilled as real trades, not NaN holes
    assert g.last_updated_sec == 3


def test_cooldown_blocks_refire():
    pm = FakePm(E)
    cfg = SignalConfig("tm", "trailmean", {"w": 5}, cooldown_s=20)
    det = MultiDetector(pm, FakeBtc(), configs=[cfg])
    # jump at 10, revert at 15, jump again at 16 (within 20s cooldown of the sec-10 fire)
    for s in range(0, 41):
        if s in (10, 11, 12, 13, 14) or s >= 16:
            pm.pdet[s] = 0.95
        else:
            pm.pdet[s] = 0.50
    fires = _drive(det, pm, range(0, 41))
    assert len(fires) == 1 and fires[0].sec == 10


# --------------------------------------------------------------------------- #
# capture staging
# --------------------------------------------------------------------------- #

class CapPm:
    def __init__(self, epoch, trades, book=None):
        self.market = ActiveMarket(epoch, "UP", "DOWN")
        self._tr = trades
        # book history rows: (recv, bid, bid_sz, ask, ask_sz, event_ts)
        self._book = book or []

    def trades_since(self, token_id, t0):
        return [t for t in self._tr if t[0] >= t0]

    def book_since(self, token_id, t0):
        return [r for r in self._book if r[0] > t0]

    def book_top_sized(self, token_id):
        return (0.58, 80.0, 0.61, 90.0, None)   # standing ladder top for the keepalive

    def book_top(self, token_id):
        return BookTop(ts=123.0, last=0.6, bid=0.58, ask=0.61)


class CapDepth:
    def __init__(self, frames):
        self.frames = frames

    def snapshots_since(self, t0):
        return [f for f in self.frames if f["ts"] >= t0]


def _fe(sec, config_id="trailmean_w60_k150"):
    return FireEvent(
        config_id=config_id, epoch_start=E, token="Up", token_id="UP", sec=sec,
        event_ts=float(E + sec), local_ts=float(E + sec), ratio=1.6, k=1.5, p_entry=0.62,
        entry_bid=0.60, entry_ask=0.63, btc_mid=64000.0, entry_last_age_s=0.4)


def test_capture_staging(tmp_path):
    import sqlite3
    db = tmp_path / "s.sqlite"
    signal_db.ensure_tables(db)
    # trades: two inside the lookback window, (event_ts, price, size, side, recv)
    trades = [(E + 90, 0.61, 10.0, "BUY", E + 90.1), (E + 95, 0.62, 5.0, "SELL", E + 95.1)]
    frames = [{"ts": E + 92, "mid": 64000.0, "bids": [[63999, 1]], "asks": [[64001, 2]],
               "local_ts": E + 92.1}]
    # book history (recv, bid, bid_sz, ask, ask_sz, event_ts): one pre-fire lookback sample, two
    # inside the [E+100, E+105] reaction window (kept at 500ms), and two more that thin to ~1s.
    book = [
        (E + 90.0, 0.58, 100.0, 0.61, 120.0, E + 90.0),     # pre-fire → kept (1s thin)
        (E + 100.4, 0.60, 90.0, 0.63, 110.0, E + 100.4),    # reaction → kept
        (E + 100.9, 0.60, 80.0, 0.63, 100.0, E + 100.9),    # reaction (500ms) → kept
    ]
    pm, btc, depth = CapPm(E, trades, book), FakeBtc(), CapDepth(frames)
    cap = CaptureManager(db, pm, btc, depth)

    fe = _fe(100)
    cid = cap.on_fire(fe)                       # t_back = E+100-120 = E-20 → both trades + pre-fire book
    signal_db.insert_fire(db, cid, fe)
    cap.on_tick(E + 101)                        # forward tick: no new trades/book beyond the buffer

    con = sqlite3.connect(str(db)); con.row_factory = sqlite3.Row
    n_tr = con.execute("SELECT count(*) FROM raw_pm_trades").fetchone()[0]
    n_bk = con.execute("SELECT count(*) FROM raw_pm_book").fetchone()[0]
    n_dp = con.execute("SELECT count(*) FROM raw_btc_depth").fetchone()[0]
    n_tk = con.execute("SELECT count(*) FROM raw_btc_tick").fetchone()[0]
    n_cap = con.execute("SELECT count(*) FROM captures").fetchone()[0]
    n_fire = con.execute("SELECT count(*) FROM fires").fetchone()[0]
    assert n_tr == 2          # both lookback trades, no dup on the forward tick
    assert n_bk == 3          # pre-fire + two reaction samples, cursor-deduped
    assert n_dp == 1 and n_tk >= 1 and n_cap == 1 and n_fire == 1
    # sizes are now stored (LIVE_TEST_SPEC §1.1), not NULL
    bk = con.execute("SELECT local_ts, bid, bid_sz, ask, ask_sz FROM raw_pm_book "
                     "ORDER BY local_ts").fetchall()
    assert bk[0]["ask_sz"] == pytest.approx(120.0) and bk[1]["ask_sz"] == pytest.approx(110.0)
    assert bk[0]["local_ts"] == pytest.approx(E + 90.0)
    # honest local_ts: the trade's receipt clock, not the staging time
    row = con.execute("SELECT local_ts, event_ts, price FROM raw_pm_trades ORDER BY event_ts").fetchone()
    assert row["local_ts"] == pytest.approx(E + 90.1) and row["event_ts"] == pytest.approx(E + 90)
    con.close()


def test_capture_cofire_shares_capture(tmp_path):
    db = tmp_path / "s2.sqlite"
    signal_db.ensure_tables(db)
    cap = CaptureManager(db, CapPm(E, []), FakeBtc(), CapDepth([]))
    cid1 = cap.on_fire(_fe(100, "trailmean_w60_k150"))
    cid2 = cap.on_fire(_fe(100, "asym_2_5_40_k150"))   # same (epoch, token) → same capture
    assert cid1 == cid2


def test_capture_keepalive_on_quiet_book(tmp_path):
    """A quiet book must still get a ~1s row so ask_d5 has a fresh fill near fire+5 (REVIEW §5.4)."""
    import sqlite3
    db = tmp_path / "k.sqlite"
    signal_db.ensure_tables(db)
    book = [(E + 700.0, 0.58, 80.0, 0.61, 90.0, E + 700.0)]   # one reaction row, then quiet
    cap = CaptureManager(db, CapPm(E, [], book), FakeBtc(), CapDepth([]))
    fe = _fe(700)
    cid = cap.on_fire(fe)                       # stages the fire row; last_book_kept = E+700
    signal_db.insert_fire(db, cid, fe)
    for t in (E + 701.0, E + 702.0, E + 705.0):
        cap.on_tick(t)                          # quiet book → keepalive rows ~1s apart
    con = sqlite3.connect(str(db)); con.row_factory = sqlite3.Row
    rows = con.execute("SELECT local_ts, ask, ask_sz FROM raw_pm_book ORDER BY local_ts").fetchall()
    con.close()
    lts = [r["local_ts"] for r in rows]
    assert any(abs(x - (E + 705.0)) < 1e-6 for x in lts)     # a fresh row at ~fire+5 for ask_d5
    assert all(r["ask_sz"] is not None for r in rows)        # keepalive carries sizes too


# --------------------------------------------------------------------------- #
# Discord settlement PnL formatter
# --------------------------------------------------------------------------- #

def test_fmt_window_tau_and_expiry_pnl():
    """τ60 / τ120 from the captured book + hold-to-expiry from settlement (REVIEW P1b)."""
    res = {"epoch_start": E, "token": "Up", "resolved": 1, "winner": 1, "final_price": 0.98}
    fires = [{"config_id": "trailmean_w60_k150", "sec": 120, "ratio": 1.55,
              "p_entry": 0.62, "entry_ask": 0.63, "local_ts": float(E + 120)}]
    book = [(float(E + 180), 0.66, 0.68), (float(E + 240), 0.70, 0.72)]   # samples at +60 / +120
    msg = _fmt_window(res, fires, book)
    assert "WIN" in msg and "trailmean_w60_k150" in msg
    assert "τ60: net=+0.030 gross=+0.050" in msg     # 0.66−0.63 ; mid(0.67)−0.62
    assert "τ120: net=+0.070 gross=+0.090" in msg    # 0.70−0.63 ; mid(0.71)−0.62
    assert "exp: net=+0.370 gross=+0.380" in msg     # settle 1.0 − 0.63 / − 0.62


def test_fmt_window_pin_expiry_na():
    """Pin → expiry P&L is n/a; τ exits still compute from the book where a sample exists."""
    res = {"epoch_start": E, "token": "Up", "resolved": 0, "winner": 0, "final_price": 0.55}
    fires = [{"config_id": "asym_2_5_40_k150", "sec": 100, "ratio": 1.6,
              "p_entry": 0.55, "entry_ask": 0.56, "local_ts": float(E + 100)}]
    book = [(float(E + 160), 0.58, 0.60)]            # τ60 sample only
    msg = _fmt_window(res, fires, book)
    assert "PIN" in msg
    assert "τ60: net=+0.020 gross=+0.040" in msg     # 0.58−0.56 ; mid(0.59)−0.55
    assert "τ120: n/a" in msg                         # no +120 book sample, pin → no settle fallback
    assert "exp: n/a" in msg


def test_dry_run_sweep_non_destructive(tmp_path):
    """dry-run must NOT consume windows; a real run with no webhook marks them (REVIEW P2)."""
    import os
    db = tmp_path / "d.sqlite"
    signal_db.ensure_tables(db)
    cid = signal_db.open_capture(db, E, "Up", E - 20, E + 899)
    signal_db.insert_fire(db, cid, _fe(120))
    signal_db.insert_resolution(db, E, "Up", 0.98, 1, 1, float(E + 899))

    sweep_and_alert(db, dry_run=True)                       # non-destructive
    assert signal_db.unalerted_resolved_windows(db) == [(E, "Up")]

    os.environ.pop("DISCORD_WEBHOOK_URL_PM_SIGNAL_SIM", None)
    os.environ.pop("DISCORD_WEBHOOK_URL_PM_SHOCK", None)
    sweep_and_alert(db, dry_run=False)                      # no webhook → marks (no infinite retry)
    assert signal_db.unalerted_resolved_windows(db) == []


# --------------------------------------------------------------------------- #
# LIVE_TEST_SPEC v2 — pre-registered signals, fill, tables, Discord filter
# --------------------------------------------------------------------------- #

def test_in_scope():
    assert signals_live.in_scope("trailmean_w60_k150", 720)
    assert not signals_live.in_scope("trailmean_w60_k150", 719)      # too early
    assert not signals_live.in_scope("asym_2_5_40_k150", 800)        # out-of-scope config
    assert signals_live.in_scope("asym_5_5_10_k150", 800)


def test_eval_s1_fade():
    fe = _fe(800)                                # local_ts = E+800
    fire = E + 800
    book = [
        (fire - 0.4, 0.61, 90, 0.65, 110),       # pre-fire row → NOT used as mid_fire (must be ≥ fire)
        (fire + 0.2, 0.60, 90, 0.64, 110),       # first row ≥ fire → mid_fire = 0.62
        (fire + 2.9, 0.58, 90, 0.62, 110),       # last row ≤ fire+3 → mid_last = 0.60
        (fire + 4.0, 0.50, 90, 0.54, 110),       # beyond fire+3 → ignored
    ]
    ev = signals_live.eval_s1(fe, book)
    assert ev.signal == "fade"
    assert ev.value == pytest.approx(0.60 - 0.62)
    assert ev.decision == 1                       # d_mid_3 < 0 → fade fires
    assert ev.input_ts == pytest.approx(fire + 2.9)          # newest input row
    assert ev.decided_at == pytest.approx(fire + config.S1_HORIZON_S)   # max(input, fire+3) = fire+3
    d = json.loads(ev.detail)
    assert d["mid_fire"] == pytest.approx(0.62) and d["mid_last"] == pytest.approx(0.60)
    assert d["fallback"] is False


def test_eval_s1_fire_anchor_fallback():
    """No row ≥ fire yet → fall back to the last row ≤ fire, flagged in detail."""
    fe = _fe(800); fire = E + 800
    book = [(fire - 1.0, 0.58, 9, 0.60, 9), (fire - 0.3, 0.57, 9, 0.61, 9)]   # all pre-fire
    ev = signals_live.eval_s1(fe, book)
    d = json.loads(ev.detail)
    assert d["fallback"] is True and d["mid_fire"] == pytest.approx(0.59)   # last ≤ fire


def test_eval_s1_no_fade_when_mid_rises():
    fe = _fe(800); fire = E + 800
    book = [(fire, 0.58, 9, 0.60, 9), (fire + 2.8, 0.62, 9, 0.66, 9)]   # mid up → no fade
    ev = signals_live.eval_s1(fe, book)
    assert ev.value == pytest.approx(0.64 - 0.59) and ev.decision == 0


def test_compute_fill_ask_d5():
    fe = _fe(800); fire = E + 800
    book = [
        (fire + 4.9, 0.60, 90, 0.62, 100),        # < fire+5 → skipped
        (fire + 5.2, 0.61, 80, 0.63, 110),        # first ≥ fire+5 → the fill
        (fire + 6.0, 0.62, 70, 0.64, 120),
    ]
    fill = signals_live.compute_fill(fe, book)
    assert fill[0] == pytest.approx(fire + 5.2)
    assert fill[1] == pytest.approx(0.63) and fill[2] == pytest.approx(110)


def test_eval_s2_z30_matches_formula():
    """Anchored at t3 = fire+3; ret_30 SIMPLE, rv_30 from 1s LOG returns (REVIEW §5.1)."""
    fe = _fe(800); t3 = E + 800 + config.S1_HORIZON_S; W = config.Z30_WINDOW_S
    seq = [64000.0 + 6.0 * i for i in range(W + 1)]         # t3−30 … t3
    mids = {round(float(t3 - W + i), 3): seq[i] for i in range(W + 1)}

    class Btc:
        def mid_at(self, t): return mids.get(round(float(t), 3))
        def mid_now(self): return seq[W]

    ev = signals_live.eval_s2(fe, Btc())
    log_rets = [math.log(seq[i + 1] / seq[i]) * 1e4 for i in range(W)]
    ret_30 = (seq[W] / seq[0] - 1.0) * 1e4                  # SIMPLE return
    z = ret_30 / (statistics.stdev(log_rets) * math.sqrt(W))
    assert ev.signal == "z30_gate"
    assert ev.value == pytest.approx(z)
    assert ev.decision == (1 if abs(z) > 1 else 0)
    assert ev.input_ts == pytest.approx(t3)
    assert ev.decided_at == pytest.approx(t3)               # max(t3, fire+3) = fire+3 = t3


def test_eval_s2_insufficient_btc():
    fe = _fe(800)

    class Btc:
        def mid_at(self, t): return None
        def mid_now(self): return None

    ev = signals_live.eval_s2(fe, Btc())
    assert ev.value is None and ev.decision == 0


def test_window_passes():
    assert not _window_passes({})
    assert not _window_passes({1: [{"decision": 0}]})
    assert _window_passes({1: [{"decision": 0}, {"decision": 1}]})


def test_fmt_window_signal_block():
    res = {"epoch_start": E, "token": "Up", "resolved": 1, "winner": 1, "final_price": 0.99}
    fires = [{"id": 7, "config_id": "trailmean_w60_k150", "sec": 740, "ratio": 1.6,
              "p_entry": 0.71, "entry_ask": 0.72, "local_ts": float(E + 740)}]
    book = [(float(E + 800), 0.74, 0.76)]
    sig_evals = {7: [
        {"signal": "fade", "value": -0.004, "decision": 1, "decided_at": float(E + 743)},
        {"signal": "z30_gate", "value": 1.82, "decision": 1, "decided_at": float(E + 740)}]}
    fills = {7: {"fill_ask": 0.73, "fill_ask_sz": 120.0, "margin_s": 2.0}}
    msg = _fmt_window(res, fires, book, sig_evals, fills)
    assert "fade✓ d_mid3=-0.0040" in msg
    assert "z30=+1.82✓" in msg
    assert "d5=0.730×120" in msg and "mgn+2.0s" in msg
    assert "dec@+3.0s" in msg


def test_sweep_marks_non_passing_silently(tmp_path):
    import os
    db = tmp_path / "f.sqlite"
    signal_db.ensure_tables(db)
    cid = signal_db.open_capture(db, E, "Up", E - 20, E + 899)
    signal_db.insert_fire(db, cid, _fe(740))               # fires but NO signal_evals → no pass
    signal_db.insert_resolution(db, E, "Up", 0.98, 1, 1, float(E + 899))
    os.environ.pop("DISCORD_WEBHOOK_URL_PM_SIGNAL_SIM", None)
    os.environ.pop("DISCORD_WEBHOOK_URL_PM_SHOCK", None)
    sent = sweep_and_alert(db, dry_run=False)
    assert sent == 0                                        # nothing sent...
    assert signal_db.unalerted_resolved_windows(db) == []   # ...but marked silently


def test_new_tables_and_inserts(tmp_path):
    import sqlite3
    db = tmp_path / "t.sqlite"
    signal_db.ensure_tables(db)
    signal_db.insert_epoch_strike(db, E, float(E), float(E), 64000.0)
    signal_db.insert_epoch_strike(db, E, float(E), float(E), 99999.0)   # dup epoch → ignored
    cid = signal_db.open_capture(db, E, "Up", E - 20, E + 899)
    fid = signal_db.insert_fire(db, cid, _fe(740))
    signal_db.insert_signal_eval(db, fid, SigEval("fade", -0.01, 1, float(E + 743), float(E + 743), "{}"))
    signal_db.insert_fill_log(db, fid, float(E + 745), 0.73, 120.0, 2.0)
    con = sqlite3.connect(str(db))
    assert con.execute("SELECT count(*) FROM epoch_strike").fetchone()[0] == 1
    assert con.execute("SELECT btc_mid FROM epoch_strike").fetchone()[0] == 64000.0   # first wins
    assert con.execute("SELECT count(*) FROM signal_evals").fetchone()[0] == 1
    assert con.execute("SELECT margin_s FROM fill_log").fetchone()[0] == pytest.approx(2.0)
    con.close()

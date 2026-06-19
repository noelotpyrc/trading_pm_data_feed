"""
Unit tests for pm_signal_sim (offline, deterministic — no live WS).

Run: /Users/noel/projects/venvs/production/bin/python -m pytest pm_signal_sim/tests -v

Covers the per-second grid, the 3 evaluator families (onset / level-floor / cooldown), capture
staging (lookback + forward, dedup), and the Discord settlement PnL formatter. The reused
pm_shock_signal feed base is exercised live by the separate smoke.
"""
import math

import pytest

from pm_signal_sim import config, signal_db
from pm_signal_sim.config import SignalConfig
from pm_signal_sim.signals import GridState, MultiDetector, FireEvent
from pm_signal_sim.capture import CaptureManager
from pm_signal_sim.discord_report import _fmt_window, sweep_and_alert
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
    def __init__(self, epoch, trades):
        self.market = ActiveMarket(epoch, "UP", "DOWN")
        self._tr = trades

    def trades_since(self, token_id, t0):
        return [t for t in self._tr if t[0] >= t0]

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
    pm, btc, depth = CapPm(E, trades), FakeBtc(), CapDepth(frames)
    cap = CaptureManager(db, pm, btc, depth)

    fe = _fe(100)
    cid = cap.on_fire(fe)                       # t_back = E+100-120 = E-20 → both trades staged
    signal_db.insert_fire(db, cid, fe)
    cap.on_tick(E + 101)                        # forward tick: no new trades, +1 book/tick sample

    con = sqlite3.connect(str(db)); con.row_factory = sqlite3.Row
    n_tr = con.execute("SELECT count(*) FROM raw_pm_trades").fetchone()[0]
    n_bk = con.execute("SELECT count(*) FROM raw_pm_book").fetchone()[0]
    n_dp = con.execute("SELECT count(*) FROM raw_btc_depth").fetchone()[0]
    n_tk = con.execute("SELECT count(*) FROM raw_btc_tick").fetchone()[0]
    n_cap = con.execute("SELECT count(*) FROM captures").fetchone()[0]
    n_fire = con.execute("SELECT count(*) FROM fires").fetchone()[0]
    assert n_tr == 2          # both lookback trades, no dup on the forward tick
    assert n_bk == 2          # one book sample at on_fire + one on the forward tick (distinct now)
    assert n_dp == 1 and n_tk >= 1 and n_cap == 1 and n_fire == 1
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


# --------------------------------------------------------------------------- #
# Discord settlement PnL formatter
# --------------------------------------------------------------------------- #

def test_fmt_window_settlement_pnl():
    res = {"epoch_start": E, "token": "Up", "resolved": 1, "winner": 1, "final_price": 0.98}
    fires = [
        {"config_id": "trailmean_w60_k150", "sec": 120, "ratio": 1.55, "p_entry": 0.62, "entry_ask": 0.63},
        {"config_id": "asym_2_5_40_k150", "sec": 130, "ratio": 1.71, "p_entry": 0.66, "entry_ask": 0.67},
    ]
    msg = _fmt_window(res, fires)
    assert "WIN" in msg and "Up" in msg
    assert "trailmean_w60_k150" in msg and "asym_2_5_40_k150" in msg
    # winner → settle 1.0 → net_exp = 1.0 − 0.63 = +0.370
    assert "net_exp=+0.370" in msg
    # a pin shows no settlement PnL
    res_pin = {**res, "resolved": 0, "winner": 0, "final_price": 0.55}
    assert "PIN" in _fmt_window(res_pin, fires)


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

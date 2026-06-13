"""
Unit tests for pm_shock_signal (BUILD_SPEC §10 + REVIEW.md).

Run: /Users/noel/projects/venvs/production/bin/python -m pytest pm_shock_signal/tests -v

Offline + deterministic: synthetic feeds, no live WS. Everything is anchored on
explicit event timestamps (no time.time() dependence in the logic tests). The live
WS smoke (test_live_ws_smoke) stays opt-in behind PM_SHOCK_LIVE_SMOKE.
"""
import math
import os
import statistics
import time

import pytest

from pm_shock_signal import config
from pm_shock_signal.feeds import ActiveMarket, BookTop, BtcMidFeed
from pm_shock_signal.shock_signal import (
    ShockDetector, compute_z_shock, z_from_inputs, FireEvent,
)
from pm_shock_signal.config import OperatingPoint
from pm_shock_signal.sim import SimPositionManager


# --------------------------------------------------------------------------- #
# Fakes (event-time aware)
# --------------------------------------------------------------------------- #

class FakeBtc:
    """Duck-typed BtcMidFeed: mid(t) = 1 + slope·(t − t0); constant rv."""
    def __init__(self, slope, rv, t0):
        self._slope = slope
        self._rv = rv
        self._t0 = t0

    def mid_now(self):
        return 1.0

    def mid_at(self, t):
        return 1.0 + self._slope * (t - self._t0)

    def rv_60s(self, t):
        return self._rv


class FakePm:
    """Duck-typed PmTokenFeed over an integer-second price table per token."""
    def __init__(self, epoch_start, series_by_token, ask=0.26, bid=0.0, book_ts=None):
        self.market = ActiveMarket(epoch_start, "UP", "DOWN")
        self._series = series_by_token   # {token_id: {sec:int -> price}}
        self._ask = ask
        self._bid = bid
        self._book_ts = float(epoch_start) if book_ts is None else book_ts

    def price_asof(self, token_id, ts):
        secs = self._series.get(token_id, {})
        sec = int(math.floor(ts - self.market.epoch_start))
        for s in range(sec, -1, -1):
            if s in secs:
                return (self.market.epoch_start + s, secs[s])
        return None

    def price_at(self, token_id, ts):
        a = self.price_asof(token_id, ts)
        return a[1] if a else None

    def book_top(self, token_id):
        return BookTop(ts=self._book_ts, last=None, bid=self._bid, ask=self._ask)


# --------------------------------------------------------------------------- #
# rv_60s + mid interpolation
# --------------------------------------------------------------------------- #

def test_rv_60s_matches_fixture():
    """rv_60s(t) == std of consecutive 5s mid log-returns over the trailing 60s."""
    btc = BtcMidFeed()
    base = 1_000_000.0
    mids = [100.0, 100.2, 99.9, 100.5, 101.0, 100.7, 100.3,
            100.9, 101.4, 101.1, 100.6, 101.2, 101.8]   # 13 samples, 5s apart
    for i, m in enumerate(mids):
        btc._buf.append((base - 60 + i * 5, m))          # samples sit on the grid

    expected = statistics.stdev([math.log(b / a) for a, b in zip(mids, mids[1:])])
    got = btc.rv_60s(base)
    assert got is not None
    assert got == pytest.approx(expected, abs=1e-9)


def test_rv_60s_insufficient_history():
    btc = BtcMidFeed()
    btc._buf.append((1_000_000.0, 100.0))   # one sample → <2 returns
    assert btc.rv_60s(1_000_000.0) is None


def test_mid_at_interpolation_and_gate():
    btc = BtcMidFeed()
    btc._buf.append((0.0, 100.0))
    btc._buf.append((10.0, 110.0))
    assert btc.mid_at(0.0) == pytest.approx(100.0)
    assert btc.mid_at(10.0) == pytest.approx(110.0)
    assert btc.mid_at(5.0) == pytest.approx(105.0)     # interpolated
    assert btc.mid_at(7.0) == pytest.approx(107.0)
    assert btc.mid_at(11.0) is None                    # gate: beyond latest tick
    assert btc.mid_at(-1.0) is None                    # before earliest tick


# --------------------------------------------------------------------------- #
# z_shock
# --------------------------------------------------------------------------- #

def test_z_from_inputs_formula():
    mid_t, mid_prev, rv, delta = 101.0, 100.0, 0.0008, 10
    expected = math.log(mid_t / mid_prev) / (rv * math.sqrt(delta / 5.0))
    assert z_from_inputs(mid_t, mid_prev, rv, delta, 1.0) == pytest.approx(expected, rel=1e-12)
    assert z_from_inputs(mid_t, mid_prev, rv, delta, -1.0) == pytest.approx(-expected, rel=1e-12)


def test_z_from_inputs_missing():
    assert z_from_inputs(None, 100.0, 0.001, 5, 1.0) is None
    assert z_from_inputs(101.0, None, 0.001, 5, 1.0) is None
    assert z_from_inputs(101.0, 100.0, 0.0, 5, 1.0) is None


def test_compute_z_shock_anchored():
    """compute_z_shock reads interpolated mids at t and t−Δ and rv at t."""
    t0 = 1_000_000.0
    btc = FakeBtc(slope=0.001, rv=0.0005, t0=t0)
    t, delta = t0 + 50, 10
    z = compute_z_shock(btc, t, delta, 1.0)
    expected = z_from_inputs(btc.mid_at(t), btc.mid_at(t - delta), btc.rv_60s(t), delta, 1.0)
    assert z == pytest.approx(expected, rel=1e-12)


# --------------------------------------------------------------------------- #
# onset / fire
# --------------------------------------------------------------------------- #

def _flat_then_jump(flat=0.10, jumped=0.25, jump_sec=10, end=20):
    up = {s: (flat if s < jump_sec else jumped) for s in range(0, end + 1)}
    down = {s: flat for s in range(0, end + 1)}
    return {"UP": up, "DOWN": down}


def test_onset_rising_edge_only():
    """A sustained 2.5x jump fires exactly ONE onset (rising edge), not one per second."""
    E = 1_700_000_400   # any 900-aligned epoch
    pm = FakePm(E, _flat_then_jump())
    btc = FakeBtc(slope=0.001, rv=0.0001, t0=E)   # big positive z → confirms Up
    op = OperatingPoint("t", delta=5, k=1.5, z_thr=2.0, exit_tau=120)
    det = ShockDetector(btc, pm, ops=[op])

    fires = [f for s in range(5, 21) for f in det.on_tick(E + s)]

    assert len(fires) == 1
    f = fires[0]
    assert f.token == "Up"
    assert f.sec_into_window == 10
    assert f.back_ratio == pytest.approx(2.5)
    assert f.entry_ask == pytest.approx(0.26)
    # z is reproducible offline from the stored inputs (REVIEW item 1 accept check)
    assert z_from_inputs(f.entry_mid, f.mid_prev, f.rv_60s, f.delta, 1.0) == pytest.approx(f.z_shock)
    assert f.pm_event_ts == E + 10
    assert f.mid_prev_event_ts == E + 5


def test_no_onset_below_origin_floor():
    """Origin (p_{t-Δ}) below ORIGIN_FLOOR (0.01) is ignored even on a big ratio."""
    E = 1_700_000_400
    series = {"UP": {s: (0.005 if s < 10 else 0.05) for s in range(0, 21)},
              "DOWN": {s: 0.005 for s in range(0, 21)}}
    pm = FakePm(E, series)
    btc = FakeBtc(0.001, 0.0001, E)
    op = OperatingPoint("t", delta=5, k=1.5, z_thr=2.0, exit_tau=120)
    det = ShockDetector(btc, pm, ops=[op])
    fires = [f for s in range(5, 21) for f in det.on_tick(E + s)]
    assert fires == []


def test_fire_requires_z_confirmation():
    """Same PM shock fires with z>=z_thr, not with z<z_thr."""
    E = 1_700_000_400
    op = OperatingPoint("t", delta=5, k=1.5, z_thr=2.0, exit_tau=120)

    det1 = ShockDetector(FakeBtc(0.001, 0.0001, E), FakePm(E, _flat_then_jump()), ops=[op])
    assert len([f for s in range(5, 21) for f in det1.on_tick(E + s)]) == 1

    det2 = ShockDetector(FakeBtc(0.0, 0.0001, E), FakePm(E, _flat_then_jump()), ops=[op])
    assert [f for s in range(5, 21) for f in det2.on_tick(E + s)] == []   # no BTC move → z=0


def test_cooldown_blocks_refire():
    """After firing, a second rising edge inside COOLDOWN_SEC does not re-fire."""
    E = 1_700_000_400
    up = {}
    for s in range(0, 41):
        up[s] = 0.10 if (s < 10 or s == 15) else 0.25   # jump@10, dip@15, jump@16
    series = {"UP": up, "DOWN": {s: 0.10 for s in range(0, 41)}}
    det = ShockDetector(FakeBtc(0.001, 0.0001, E), FakePm(E, series),
                        ops=[OperatingPoint("t", delta=5, k=1.5, z_thr=2.0, exit_tau=120)])
    fires = [f for s in range(5, 41) for f in det.on_tick(E + s)]
    assert len(fires) == 1   # 2nd rising edge (sec=20) is within 30s cooldown of the sec=10 fire


# --------------------------------------------------------------------------- #
# sim PnL + TTL cap + staleness
# --------------------------------------------------------------------------- #

def _fire(epoch, sec, config_id="d5_k15_z2_t120", entry_last=0.20, entry_ask=0.22, entry_bid=0.19):
    t = epoch + sec
    return FireEvent(
        config_id=config_id, epoch_start=epoch, fire_ts=t, sec_into_window=sec,
        token="Up", token_id="UP", delta=5, k=1.5, z_thr=2.0, exit_tau=120,
        back_ratio=2.0, z_shock=5.0,
        p_shock=entry_last, entry_ask=entry_ask, entry_bid=entry_bid, rv_60s=0.001,
        entry_mid=1.01, mid_prev=1.005, mid_event_ts=t, mid_prev_event_ts=t - 5,
        pm_event_ts=t, receipt_ts=t + 1.0, entry_last_age_s=0.0, strike=None,
    )


def test_sim_pnl_and_book_both_ends():
    E = 1_700_000_400
    pm = FakePm(E, {"UP": {100: 0.20, 220: 0.30}}, ask=0.22, bid=0.28, book_ts=E + 218)
    sim = SimPositionManager(pm)
    pos = sim.open_position(_fire(E, sec=100), signal_id=1)   # 100 + 120 = 220
    assert pos.exit_sec == 220 and pos.ttl_capped is False

    trades = sim.close_due(E + 220)
    assert len(trades) == 1
    t = trades[0]
    assert t.exit_last == pytest.approx(0.30)
    assert t.exit_bid == pytest.approx(0.28)
    assert t.exit_ask == pytest.approx(0.22)             # REVIEW item 2: full book both ends
    assert t.pnl_gross == pytest.approx(0.30 - 0.20)
    assert t.pnl_net == pytest.approx(0.28 - 0.22)
    assert t.roi_net == pytest.approx((0.28 - 0.22) / 0.22)
    assert t.exit_last_age_s == pytest.approx(0.0)       # fresh exit print
    assert t.exit_book_age_s == pytest.approx(2.0)       # E+220 − (E+218)
    assert sim.open_count() == 0


def test_ttl_cap_at_window_end():
    E = 1_700_000_400
    pm = FakePm(E, {"UP": {899: 0.40}}, ask=0.22, bid=0.30)
    sim = SimPositionManager(pm)
    pos = sim.open_position(_fire(E, sec=850), signal_id=2)   # 850+120=970 > 899 → capped
    assert pos.exit_sec == config.WINDOW_END_SEC
    assert pos.ttl_capped is True
    assert pos.exit_ts == E + config.WINDOW_END_SEC
    trades = sim.close_due(E + config.WINDOW_END_SEC)
    assert len(trades) == 1 and trades[0].ttl_capped is True


def test_exit_staleness_recorded():
    """A token that goes quiet before exit yields a large exit_last_age_s."""
    E = 1_700_000_400
    pm = FakePm(E, {"UP": {100: 0.20}}, ask=0.22, bid=0.21)   # no trade after sec 100
    sim = SimPositionManager(pm)
    sim.open_position(_fire(E, sec=100), signal_id=1)         # exit at sec 220
    t = sim.close_due(E + 220)[0]
    assert t.exit_last == pytest.approx(0.20)
    assert t.exit_last_age_s == pytest.approx(120.0)         # 220 − 100


def test_sim_not_due_stays_open():
    E = 1_700_000_400
    pm = FakePm(E, {"UP": {}})
    sim = SimPositionManager(pm)
    sim.open_position(_fire(E, sec=100), signal_id=1)   # exit at sec 220
    assert sim.close_due(E + 150) == []
    assert sim.open_count() == 1


# --------------------------------------------------------------------------- #
# DB schema + resolved_outcome backfill helpers
# --------------------------------------------------------------------------- #

def test_db_roundtrip_and_resolve(tmp_path):
    from pm_shock_signal import signal_db
    db = tmp_path / "t.sqlite"
    signal_db.ensure_tables(db)
    E = 1_700_000_400
    sid = signal_db.insert_signal(
        db, config_id="d5_k15_z2_t120", epoch_start=E, fire_ts="2026-06-12 00:00:00",
        sec_into_window=100, token="Up", delta=5, k=1.5, z_thr=2.0, back_ratio=2.5,
        z_shock=3.1, p_shock=0.20, entry_ask=0.22, entry_bid=0.19, rv_60s=0.0008,
        entry_mid=101.0, mid_prev=100.0, mid_event_ts=float(E + 100),
        mid_prev_event_ts=float(E + 95), pm_event_ts=float(E + 100),
        receipt_ts=float(E + 101), entry_last_age_s=1.0, strike=None,
    )
    signal_db.insert_sim_trade(
        db, signal_id=sid, config_id="d5_k15_z2_t120", entry_ts="2026-06-12 00:00:00",
        entry_last=0.20, entry_ask=0.22, exit_ts="2026-06-12 00:02:00", exit_sec=220,
        exit_last=0.30, exit_bid=0.28, exit_ask=0.22, ttl_capped=False,
        pnl_gross=0.10, pnl_net=0.06, roi_net=0.27, exit_last_age_s=0.0, exit_book_age_s=2.0,
    )
    assert signal_db.epochs_needing_resolution(db) == [E]
    assert signal_db.set_resolved_outcome(db, E, "Up") == 1
    assert signal_db.epochs_needing_resolution(db) == []


# --------------------------------------------------------------------------- #
# live smoke (opt-in)
# --------------------------------------------------------------------------- #

@pytest.mark.skipif(
    not os.environ.get("PM_SHOCK_LIVE_SMOKE"),
    reason="live, read-only — set PM_SHOCK_LIVE_SMOKE=1 to run",
)
def test_live_ws_smoke():
    """Connect both feeds for ~60s; assert mid updates and PM book/trade arrive."""
    from pm_shock_signal.feeds import PmTokenFeed
    from pm_btc15updown_data.collect_pm_btcupdown import current_epoch_ts

    btc = BtcMidFeed()
    pm = PmTokenFeed()
    btc.start()
    pm.start()
    pm.roll_market(current_epoch_ts())
    deadline = time.time() + 60
    got_mid = got_book = False
    while time.time() < deadline and not (got_mid and got_book):
        if btc.mid_now():
            got_mid = True
        if pm.market and pm.book_top(pm.market.up_token_id):
            got_book = True
        time.sleep(1)
    btc.stop()
    pm.stop()
    assert got_mid, "no BTC mid received"
    assert got_book, "no PM book received"

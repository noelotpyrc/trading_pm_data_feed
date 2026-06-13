"""
Shock detection + BTC confirmation + fire decision.

Pure logic over the two feeds — no I/O, no WS. Implements BUILD_SPEC §4 exactly;
cross-checked against
/Users/noel/projects/data_analysis/btc_depth_15updown/scripts/10_build_shocks.py
and _shock_frame.py (z_shock = s_raw / (rv_60s * sqrt(Δ/5))).

EVENT-TIME alignment (REVIEW item 1): the engine ticks on the wall clock, but each
(op, token) is evaluated at `t_anchor` = the event timestamp of the latest PM trade
seen for that token. back_ratio AND the BTC z-window are both computed over
[t_anchor − Δ, t_anchor], using BTC mids interpolated at exactly those instants and
rv_60s over the trailing 60 s ending at t_anchor — so live and backtest align on the
same (event) time axis. z is deferred (no fire) until the BTC buffer covers t_anchor.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from pm_shock_signal import config
from pm_shock_signal.config import OperatingPoint
from pm_shock_signal.feeds import BtcMidFeed, PmTokenFeed


@dataclass
class FireEvent:
    """A fired signal — everything needed to open a sim position + persist + alert."""
    config_id: str
    epoch_start: int
    fire_ts: float            # PM event time of the shock (t_anchor), epoch seconds
    sec_into_window: int
    token: str                # "Up" | "Down"
    token_id: str
    delta: int
    k: float
    z_thr: float
    exit_tau: int             # seconds held before exit (TTL-capped), from the operating point
    back_ratio: float
    z_shock: float
    p_shock: float            # last-trade at fire (entry reference)
    entry_ask: Optional[float]  # best ask at fire (realistic buy fill); None if no book
    entry_bid: Optional[float]  # best bid at fire (entry half-spread = ask − bid)
    rv_60s: float
    # z inputs + timestamps (REVIEW item 4) — make z reproducible/auditable offline.
    entry_mid: float          # mid_t  (BTC mid interpolated at t_anchor)
    mid_prev: float           # mid_{t−Δ}
    mid_event_ts: float       # = t_anchor
    mid_prev_event_ts: float  # = t_anchor − Δ
    pm_event_ts: float        # PM trade event time anchoring the fire (= t_anchor)
    receipt_ts: float         # wall clock at fire; latency = receipt_ts − pm_event_ts
    entry_last_age_s: float   # now − t_anchor: staleness of the entry last-trade
    strike: Optional[float]   # window strike (for later moneyness analysis)


def z_from_inputs(mid_t, mid_prev, rv, delta, sign) -> Optional[float]:
    """z_shock = log(mid_t/mid_{t-Δ})·sign / (rv·√(Δ/5)). None if any input invalid."""
    if not mid_t or not mid_prev or mid_t <= 0 or mid_prev <= 0 or not rv or rv <= 0:
        return None
    s_raw = math.log(mid_t / mid_prev) * sign
    return s_raw / (rv * math.sqrt(delta / config.RV_STEP_SEC))


def compute_z_shock(btc: BtcMidFeed, t: float, delta: int, btc_sign: float) -> Optional[float]:
    """z_shock anchored at event time `t` (interpolated mids + rv over [t−60, t])."""
    return z_from_inputs(btc.mid_at(t), btc.mid_at(t - delta), btc.rv_60s(t), delta, btc_sign)


def btc_sign(token: str) -> float:
    """+1 for the Up token, −1 for Down (BUILD_SPEC §4)."""
    return 1.0 if token == "Up" else -1.0


class ShockDetector:
    """Evaluates all operating points × tokens each tick; emits FireEvents."""

    def __init__(self, btc: BtcMidFeed, pm: PmTokenFeed,
                 ops: list[OperatingPoint] | None = None) -> None:
        self.btc = btc
        self.pm = pm
        self.ops = ops if ops is not None else config.OPERATING_POINTS
        self._prev_ratio: dict[tuple[str, str], float] = {}
        self._last_fire: dict[tuple[str, str], float] = {}
        self._cur_epoch: int | None = None

    def on_tick(self, now_ts: float) -> list[FireEvent]:
        """Return FireEvents that fire at `now_ts` (BUILD_SPEC §4 fire logic)."""
        market = self.pm.market
        if market is None:
            return []

        epoch_start = market.epoch_start
        sec = int(now_ts) - epoch_start
        if sec < 0 or sec > config.WINDOW_END_SEC:
            return []   # outside the active 15m window

        # On market roll, drop per-token state for tokens no longer active so the
        # state dicts stay bounded over a long-running process (new ids each window).
        if epoch_start != self._cur_epoch:
            self._cur_epoch = epoch_start
            valid = {market.up_token_id, market.down_token_id}
            self._prev_ratio = {k: v for k, v in self._prev_ratio.items() if k[1] in valid}
            self._last_fire = {k: v for k, v in self._last_fire.items() if k[1] in valid}

        tokens = (("Up", market.up_token_id), ("Down", market.down_token_id))
        fires: list[FireEvent] = []

        for op in self.ops:
            for token, token_id in tokens:
                key = (op.config_id, token_id)
                asof = self.pm.price_asof(token_id, now_ts)
                if asof is None:
                    continue
                t_anchor, p_now = asof
                # the shock and its origin must both lie inside the current window
                if t_anchor < epoch_start or (t_anchor - op.delta) < epoch_start:
                    continue
                p_prev = self.pm.price_at(token_id, t_anchor - op.delta)
                if p_prev is None or p_prev < config.ORIGIN_FLOOR:
                    continue
                back_ratio = p_now / p_prev
                prev_ratio = self._prev_ratio.get(key)
                # rising edge: crosses k upward (need a prior reading below k)
                onset = (back_ratio >= op.k
                         and prev_ratio is not None and prev_ratio < op.k)
                self._prev_ratio[key] = back_ratio
                if not onset:
                    continue

                # cooldown (wall clock)
                last_fire = self._last_fire.get(key)
                if last_fire is not None and (now_ts - last_fire) < config.COOLDOWN_SEC:
                    continue

                # BTC confirmation anchored at the PM event time
                mid_t = self.btc.mid_at(t_anchor)
                mid_prev = self.btc.mid_at(t_anchor - op.delta)
                rv = self.btc.rv_60s(t_anchor)
                z = z_from_inputs(mid_t, mid_prev, rv, op.delta, btc_sign(token))
                if z is None or z < op.z_thr:
                    continue

                top = self.pm.book_top(token_id)
                entry_ask = top.ask if top is not None else None
                entry_bid = top.bid if top is not None else None
                self._last_fire[key] = now_ts
                fires.append(FireEvent(
                    config_id=op.config_id,
                    epoch_start=epoch_start,
                    fire_ts=t_anchor,
                    sec_into_window=max(0, min(int(t_anchor) - epoch_start, config.WINDOW_END_SEC)),
                    token=token,
                    token_id=token_id,
                    delta=op.delta,
                    k=op.k,
                    z_thr=op.z_thr,
                    exit_tau=op.exit_tau,
                    back_ratio=back_ratio,
                    z_shock=z,
                    p_shock=p_now,
                    entry_ask=entry_ask,
                    entry_bid=entry_bid,
                    rv_60s=rv,
                    entry_mid=mid_t,
                    mid_prev=mid_prev,
                    mid_event_ts=t_anchor,
                    mid_prev_event_ts=t_anchor - op.delta,
                    pm_event_ts=t_anchor,
                    receipt_ts=now_ts,
                    entry_last_age_s=now_ts - t_anchor,
                    strike=market.strike,
                ))
        return fires

"""
Sim position lifecycle: open on fire → hold τ (TTL-capped) → close → PnL.

No real orders — paper only. The engine: on a FireEvent, call `open_position(...)`;
every tick, call `close_due(now_ts)` to close any positions whose exit time has
arrived; each closed position yields a SimTrade.

Entry/exit fills are SPREAD-AWARE (ask→bid) so the sim answers "does the gross EV
survive PM costs" — the key open question (BUILD_SPEC §1, §4). Full top-of-book is
recorded at both ends (entry ask+bid via the FireEvent, exit bid+ask here) to split
spread cost from adverse move; price/book ages flag stale fills (REVIEW items 2–3).
Any leg whose price is unavailable at close is recorded as None and its PnL left None.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pm_shock_signal import config
from pm_shock_signal.feeds import BtcMidFeed, PmTokenFeed
from pm_shock_signal.shock_signal import FireEvent


@dataclass
class OpenPosition:
    signal_id: int            # FK to shock_signals row
    config_id: str
    token_id: str
    epoch_start: int
    entry_ts: float
    entry_last: float         # p_shock
    entry_ask: Optional[float]  # realistic buy fill (None if no book at fire)
    exit_sec: int             # min(sec_fire + τ, WINDOW_END_SEC)
    exit_ts: float            # epoch_start + exit_sec (event-time second)
    ttl_capped: bool


@dataclass
class SimTrade:
    """A completed sim trade — persist via signal_db.insert_sim_trade + Discord exit alert."""
    signal_id: int
    config_id: str
    entry_ts: float
    entry_last: float
    entry_ask: Optional[float]
    exit_ts: float
    exit_sec: int
    exit_last: Optional[float]
    exit_bid: Optional[float]
    exit_ask: Optional[float]       # exit half-spread = ask − bid @exit (REVIEW item 2)
    ttl_capped: bool
    pnl_gross: Optional[float]      # exit_last − entry_last  (matches backtest EV)
    pnl_net: Optional[float]        # exit_bid  − entry_ask   (spread-aware)
    roi_net: Optional[float]        # pnl_net / entry_ask
    exit_last_age_s: Optional[float]  # exit_ts − last actual trade ts (REVIEW item 3)
    exit_book_age_s: Optional[float]  # close wall time − book update time (fill staleness)


class SimPositionManager:
    """Tracks open sim positions and closes them at their exit time."""

    def __init__(self, pm: PmTokenFeed) -> None:
        self.pm = pm
        self._open: list[OpenPosition] = []

    def open_position(self, fire: FireEvent, signal_id: int) -> OpenPosition:
        """Create an OpenPosition from a FireEvent (BUILD_SPEC §4 sim entry)."""
        raw_exit_sec = fire.sec_into_window + fire.exit_tau
        exit_sec = min(raw_exit_sec, config.WINDOW_END_SEC)
        ttl_capped = raw_exit_sec > config.WINDOW_END_SEC
        pos = OpenPosition(
            signal_id=signal_id,
            config_id=fire.config_id,
            token_id=fire.token_id,
            epoch_start=fire.epoch_start,
            entry_ts=fire.fire_ts,
            entry_last=fire.p_shock,
            entry_ask=fire.entry_ask,
            exit_sec=exit_sec,
            exit_ts=fire.epoch_start + exit_sec,
            ttl_capped=ttl_capped,
        )
        self._open.append(pos)
        return pos

    def close_due(self, now_ts: float) -> list[SimTrade]:
        """Close every open position with exit_ts <= now_ts; return their SimTrades."""
        due = [p for p in self._open if p.exit_ts <= now_ts]
        if not due:
            return []
        self._open = [p for p in self._open if p.exit_ts > now_ts]

        trades: list[SimTrade] = []
        for p in due:
            asof = self.pm.price_asof(p.token_id, p.exit_ts)
            exit_last = asof[1] if asof is not None else None
            exit_last_age_s = (p.exit_ts - asof[0]) if asof is not None else None
            top = self.pm.book_top(p.token_id)
            exit_bid = top.bid if top is not None else None
            exit_ask = top.ask if top is not None else None
            exit_book_age_s = (now_ts - top.ts) if top is not None else None

            pnl_gross = (exit_last - p.entry_last) if exit_last is not None else None
            pnl_net = (exit_bid - p.entry_ask
                       if (exit_bid is not None and p.entry_ask is not None) else None)
            roi_net = (pnl_net / p.entry_ask
                       if (pnl_net is not None and p.entry_ask not in (None, 0)) else None)

            trades.append(SimTrade(
                signal_id=p.signal_id,
                config_id=p.config_id,
                entry_ts=p.entry_ts,
                entry_last=p.entry_last,
                entry_ask=p.entry_ask,
                exit_ts=p.exit_ts,
                exit_sec=p.exit_sec,
                exit_last=exit_last,
                exit_bid=exit_bid,
                exit_ask=exit_ask,
                ttl_capped=p.ttl_capped,
                pnl_gross=pnl_gross,
                pnl_net=pnl_net,
                roi_net=roi_net,
                exit_last_age_s=exit_last_age_s,
                exit_book_age_s=exit_book_age_s,
            ))
        return trades

    def open_count(self) -> int:
        return len(self._open)


# --------------------------------------------------------------------------- #
# Forward price/book path sampler (REVIEW item 6)
# --------------------------------------------------------------------------- #

@dataclass
class PathSample:
    """One forward-trajectory sample for a fired signal → shock_price_path row."""
    signal_id: int
    offset_s: int             # seconds after entry (t_anchor); 0 = entry
    ts: float                 # sample time (epoch s) = t_anchor + offset_s
    pm_last: Optional[float]
    pm_bid: Optional[float]
    pm_ask: Optional[float]
    pm_last_age_s: Optional[float]
    btc_mid: Optional[float]


@dataclass
class _PathTrack:
    signal_id: int
    token_id: str
    t_anchor: float
    pending: list[int]        # offsets not yet emitted, ascending


class PricePathSampler:
    """Per-fire forward sampler. On register, schedules the config.PATH_OFFSETS_S grid
    (capped to the window end); each tick, emits a PathSample for every offset whose
    sample time has arrived. The surging token's market is read as-of the sample time
    (PM last/age via price_asof, BTC mid via mid_at) with the freshest top-of-book."""

    def __init__(self, pm: PmTokenFeed, btc: BtcMidFeed) -> None:
        self.pm = pm
        self.btc = btc
        self._tracks: list[_PathTrack] = []

    def register(self, fire: FireEvent, signal_id: int) -> None:
        window_end_ts = fire.epoch_start + config.WINDOW_END_SEC
        pending = [o for o in config.PATH_OFFSETS_S if fire.fire_ts + o <= window_end_ts]
        if pending:
            self._tracks.append(_PathTrack(signal_id, fire.token_id, fire.fire_ts, pending))

    def emit_due(self, now_ts: float) -> list[PathSample]:
        """Emit samples for every scheduled offset whose time (t_anchor+offset) <= now."""
        out: list[PathSample] = []
        for tr in self._tracks:
            still = []
            for o in tr.pending:
                sample_t = tr.t_anchor + o
                if sample_t <= now_ts:
                    out.append(self._sample(tr, o, sample_t))
                else:
                    still.append(o)
            tr.pending = still
        self._tracks = [t for t in self._tracks if t.pending]
        return out

    def _sample(self, tr: _PathTrack, offset_s: int, sample_t: float) -> PathSample:
        asof = self.pm.price_asof(tr.token_id, sample_t)
        pm_last = asof[1] if asof is not None else None
        pm_last_age_s = (sample_t - asof[0]) if asof is not None else None
        top = self.pm.book_top(tr.token_id)
        pm_bid = top.bid if top is not None else None
        pm_ask = top.ask if top is not None else None
        return PathSample(
            signal_id=tr.signal_id, offset_s=offset_s, ts=sample_t,
            pm_last=pm_last, pm_bid=pm_bid, pm_ask=pm_ask,
            pm_last_age_s=pm_last_age_s, btc_mid=self.btc.mid_at(sample_t),
        )

    def active_count(self) -> int:
        return len(self._tracks)

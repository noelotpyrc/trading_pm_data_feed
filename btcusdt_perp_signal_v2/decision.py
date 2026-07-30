"""
FCFS single-slot decision logic — SPEC_dryrun_book11.md §4 (= SPEC_book25_fcfs.md §6).

One position at a time. On each closed bar: settle an exit first, record every firing (taken or not,
with the block reason), then open a new position from the lowest-id fired cell if flat and past the
re-entry buffer. Entry/exit prices are the firing/exit bar closes (research-parity). Order-book
execution (fills, mid, hold stats) is layered on in Layer B; the returns here are `ret_research_bps`.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from btcusdt_perp_signal_v2 import cells, config
from btcusdt_perp_signal_v2.config import Cell


@dataclass
class Firing:
    bar_index: int
    ts_bar_close: int          # ms
    cell_id: int
    side: str
    window: str
    legs: list                 # [(feature, actual_decile, required_decile), ...]
    regime_decile: int
    taken: bool
    not_taken_reason: str      # "" | "in_position" | "buffer" | "lost_tie"
    close_px: float
    late_ms: int


@dataclass
class Entry:
    entry_id: int
    bar_index: int
    ts_bar_close: int
    cell_id: int
    side: str
    close_px: float
    scheduled_exit_bar: int


@dataclass
class Result:
    entry_id: int
    entry_bar_index: int
    exit_bar_index: int
    ts_exit: int
    cell_id: int
    side: str
    entry_close_px: float
    exit_close_px: float
    ret_research_bps: float
    status: str                # "ok" | "missed_exit"


@dataclass
class BarEvents:
    firings: list = field(default_factory=list)
    entry: object = None       # Entry | None
    result: object = None      # Result | None


def _sign(side: str) -> int:
    return 1 if side == "long" else -1


class Decision:
    """Stateful FCFS book. Feed closed bars in order; collect FIRING/ENTRY/RESULT events."""

    def __init__(self, buffer: int = config.BUFFER, horizon: int = config.N_HORIZON) -> None:
        self.buffer = buffer
        self.horizon = horizon
        self.state = "FLAT"
        self.free_from = -(10 ** 18)
        self._pos = None                # dict: entry_id, entry_bar, entry_close, side, cell_id
        self._next_entry_id = 1

    def on_bar(self, bar_index: int, ts_bar_close: int, close_px: float,
               deciles: dict, late_ms: int = 0) -> BarEvents:
        ev = BarEvents()

        # 1. settle a scheduled exit before anything else. `>=` (not `==`) so a position still settles
        # if its exact exit bar was skipped (e.g. a WS gap that REST could not fully refill); an
        # off-schedule settle is flagged missed_exit and excluded from execution stats (§8).
        if self.state == "IN_POSITION" and bar_index >= self._pos["entry_bar"] + self.horizon:
            p = self._pos
            scheduled = p["entry_bar"] + self.horizon
            ret = _sign(p["side"]) * (close_px / p["entry_close"] - 1.0) * 1e4
            ev.result = Result(
                entry_id=p["entry_id"], entry_bar_index=p["entry_bar"], exit_bar_index=bar_index,
                ts_exit=ts_bar_close, cell_id=p["cell_id"], side=p["side"],
                entry_close_px=p["entry_close"], exit_close_px=close_px,
                ret_research_bps=ret, status=("ok" if bar_index == scheduled else "missed_exit"))
            self.state = "FLAT"
            self._pos = None
            self.free_from = bar_index + self.buffer

        # 2. record every firing
        fired = cells.firing_cells(deciles)
        regime_dec = cells.regime_decile(deciles)
        blocked = self.state == "IN_POSITION" or bar_index < self.free_from
        taker_id = fired[0].id if (fired and not blocked) else None
        for c in fired:
            if blocked:
                reason = "in_position" if self.state == "IN_POSITION" else "buffer"
                taken = False
            else:
                taken = c.id == taker_id
                reason = "" if taken else "lost_tie"
            ev.firings.append(Firing(
                bar_index=bar_index, ts_bar_close=ts_bar_close, cell_id=c.id, side=c.side,
                window=c.window, legs=cells.leg_detail(c, deciles), regime_decile=regime_dec,
                taken=taken, not_taken_reason=reason, close_px=close_px, late_ms=late_ms))

        # 3. open from the lowest-id fired cell if eligible
        if taker_id is not None:
            c = next(x for x in fired if x.id == taker_id)
            eid = self._next_entry_id
            self._next_entry_id += 1
            self._pos = {"entry_id": eid, "entry_bar": bar_index, "entry_close": close_px,
                         "side": c.side, "cell_id": c.id}
            self.state = "IN_POSITION"
            ev.entry = Entry(entry_id=eid, bar_index=bar_index, ts_bar_close=ts_bar_close,
                             cell_id=c.id, side=c.side, close_px=close_px,
                             scheduled_exit_bar=bar_index + self.horizon)
        return ev

    def resume(self, pos: dict, next_entry_id: int) -> None:
        """Restore IN_POSITION after a restart (§8). `pos` carries entry_id/entry_bar/entry_close/
        side/cell_id; the scheduled exit is entry_bar + horizon, unchanged."""
        self._pos = dict(pos)
        self.state = "IN_POSITION"
        self._next_entry_id = next_entry_id

    def set_next_entry_id(self, n: int) -> None:
        self._next_entry_id = n

    @property
    def open_position(self):
        return self._pos

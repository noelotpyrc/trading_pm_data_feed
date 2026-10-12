"""
Recorded lines -> per-second signal rows and paper entries. Used unchanged by `live` and `replay`, so a replay of
raw/ must reproduce the live CSVs (apart from wall-clock columns).

Line kinds (one JSON object per line in raw/*.jsonl.gz; r = local receive time, c = connection id):
  {"r", "c", "sub": [{"T", "up", "down"}, ...]}   a new connection and the windows it subscribes
  {"r", "c", "m": <raw WS text>}                  a frame as received
  {"r", "c", "close": <reason>}                   the connection was closed
"""
from __future__ import annotations

import json
import math

from .add_asym import AddAsym, walk

S0, S1, WARM = 720, 895, 30          # signal seconds of a window; seconds of history needed before the first one
Q = 10.0                             # shares per paper entry
SEC_COLS = ["T", "s", "wall", "v_up", "v_down_own", "bb", "ba", "buy_up", "buy_down", "buy_down_own", "gap"]
ENTRY_COLS = ["T", "s", "token", "value", "e", "delay", "gap"]


def _f(x) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return math.nan


class Window:
    def __init__(self, T: int, up: str, down: str, r: float) -> None:
        self.T, self.up, self.down = T, up, down
        self.sig = {up: AddAsym(), down: AddAsym()}
        self.next_s = max(S0, math.floor(r - T) + WARM + 1)   # joined late -> skip seconds without 30 s of history
        self.last = {"up": -math.inf, "down": -math.inf}   # last kept entry second per token
        self.done = -math.inf                                # last emitted second


class Engine:
    def __init__(self, cut: float, lo: float, hi: float, cool: float) -> None:
        self.cut, self.lo, self.hi, self.cool = cut, lo, hi, cool
        self.windows: dict[int, Window] = {}
        self.token: dict[str, Window] = {}
        self.gaps: list[tuple[float, float]] = []          # (closed at, first frame of the next connection), local time
        self.gap_start: float | None = None
        self.conn = None
        self.seconds: list[dict] = []                      # drained by the caller
        self.entries: list[dict] = []
        self.n = {"frames": 0, "levels": 0, "books": 0, "late": 0, "bad": 0}
        self.lags: list[float] = []                        # recv - exchange time, ms, drained by the caller

    # ------------------------------------------------------------------ input
    def on_line(self, x: dict) -> None:
        r = x["r"]
        self.tick(r)
        if "sub" in x:
            self.windows = {w["T"]: self.windows.get(w["T"]) or Window(w["T"], w["up"], w["down"], r) for w in x["sub"]}
            self.token = {k: w for w in self.windows.values() for k in (w.up, w.down)}
            self.conn = x["c"]
        elif "close" in x:
            if self.gap_start is None:                     # failed reconnects keep the first close time
                self.gap_start = r
        elif "m" in x:
            if self.gap_start is not None:
                self.gaps.append((self.gap_start, r))
                self.gap_start = None
            self.frame(r, x["m"])

    def frame(self, r: float, raw: str) -> None:
        self.n["frames"] += 1
        if not raw or raw[0] not in "[{":
            return                                         # PONG
        try:
            msgs = json.loads(raw)
        except ValueError:
            self.n["bad"] += 1
            return
        for m in msgs if isinstance(msgs, list) else [msgs]:
            try:
                self._msg(r, m)
            except (KeyError, TypeError, AttributeError, ValueError):
                self.n["bad"] += 1

    def _msg(self, r: float, m: dict) -> None:
        if isinstance(m, dict):
            kind, ts = m.get("event_type"), _f(m.get("timestamp")) / 1000
            if kind == "book" and m.get("asset_id") in self.token:
                self.n["books"] += 1
                self._before(ts, r, m["asset_id"])
                lv = lambda side: {_f(z["price"]): _f(z["size"]) for z in m.get(side) or []}
                self.token[m["asset_id"]].sig[m["asset_id"]].snapshot(lv("bids"), lv("asks"))
            elif kind == "price_change":
                for c in m.get("price_changes") or []:
                    a = c.get("asset_id")
                    if a not in self.token:
                        continue
                    self.n["levels"] += 1
                    self._before(ts, r, a)
                    self.token[a].sig[a].change(ts, c.get("side") == "BUY", _f(c.get("price")), _f(c.get("size")),
                                                _f(c.get("best_bid")), _f(c.get("best_ask")))

    def _before(self, ts: float, r: float, asset: str) -> None:
        """Emit every second t < ts (state before this event), count events older than an emitted second."""
        if not math.isfinite(ts):
            return
        self.lags.append((r - ts) * 1000)
        w = self.token[asset]
        if w.T + S0 - WARM < ts <= w.T + w.done:
            self.n["late"] += 1
        for w in self.windows.values():
            while w.next_s <= S1 and w.T + w.next_s < ts:
                self._emit(w, w.next_s, r)

    def tick(self, now: float) -> None:
        """Timer: emit seconds that ended more than 1 s ago with no later event."""
        for w in self.windows.values():
            while w.next_s <= S1 and w.T + w.next_s < now - 1.0:
                self._emit(w, w.next_s, now)

    # ------------------------------------------------------------------ output
    def _emit(self, w: Window, s: int, wall: float) -> None:
        w.next_s, w.done = s + 1, s
        t = w.T + s
        u, d = w.sig[w.up], w.sig[w.down]
        v = u.value(t)
        sell_up = walk(u.bids, Q, buy=False)
        row = {"T": w.T, "s": s, "wall": wall, "v_up": v, "v_down_own": d.value(t),
               "bb": max(u.bids) if u.bids else math.nan, "ba": min(u.asks) if u.asks else math.nan,
               "buy_up": walk(u.asks, Q, buy=True), "buy_down": 1 - sell_up, "buy_down_own": walk(d.asks, Q, buy=True),
               "gap": int(any(g0 < t and g1 > t - WARM for g0, g1 in self.gaps + [(self.gap_start, math.inf)] if g0 is not None))}
        self.seconds.append(row)
        for tok, val, e in (("up", v, row["buy_up"]), ("down", -v, row["buy_down"])):
            if val <= self.cut and self.lo <= e <= self.hi and s - w.last[tok] >= self.cool:   # NaN fails every test
                w.last[tok] = s
                self.entries.append({"T": w.T, "s": s, "token": tok, "value": val, "e": e, "delay": wall - t, "gap": row["gap"]})

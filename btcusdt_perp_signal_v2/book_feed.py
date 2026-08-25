"""
Order-book feed, executable ladder, and phased poller — SPEC_dryrun_book11.md §5.

Pure functions (walk_book / executable_ladder / hold_stats / execution_returns) hold the execution
math and are unit-tested offline. BookFeed maintains the live depth20@100ms + bookTicker book;
BookPoller records the phased samples around a paper entry and produces the RESULT execution block.

No orders are placed anywhere. This module only reads the book and measures what a fill would cost.
"""
from __future__ import annotations

import json
import threading
import time
from collections import deque
from typing import Optional

from btcusdt_perp_signal_v2 import config

# websocket-client is imported lazily inside BookFeed._run so the signal/execution tests (and the
# offline backtest) run in environments without it installed.

STREAM_URL = ("wss://fstream.binance.com/stream?streams="
              "btcusdt@depth20@100ms/btcusdt@bookTicker")
STALE_S = 5.0            # a book with no update for >5s is stale (§8)


# ---- pure execution math -----------------------------------------------------
def walk_book(levels, notional_usd: float):
    """Size-weighted average fill price walking `levels` (price, base_size) from the touch out until
    `notional_usd` USD is filled. Returns (fill_px, fully_filled, filled_usd)."""
    remaining = float(notional_usd)
    cost_usd = qty = 0.0
    for price, size in levels:
        price = float(price); size = float(size)
        if price <= 0 or size <= 0:
            continue
        lvl_usd = price * size
        take_usd = min(remaining, lvl_usd)
        cost_usd += take_usd
        qty += take_usd / price
        remaining -= take_usd
        if remaining <= 1e-6:
            break
    if qty <= 0:
        return None, False, 0.0
    return cost_usd / qty, remaining <= 1e-6, cost_usd


def executable_ladder(levels, mid: float, buy: bool, notionals=None) -> dict:
    """Per-notional fill_px + slippage_bps (§5.1), plus the largest fully-fillable rung."""
    notionals = config.NOTIONALS if notionals is None else notionals
    sign = 1.0 if buy else -1.0
    fill_px, slippage_bps, max_fillable = [], [], 0.0
    for usd in notionals:
        px, filled, _ = walk_book(levels, usd)
        if px is None:
            fill_px.append(None)
            slippage_bps.append(None)
        else:
            fill_px.append(px)
            slippage_bps.append(sign * (px - mid) / mid * 1e4)
            if filled:
                max_fillable = usd
    return {"fill_px": fill_px, "slippage_bps": slippage_bps, "max_fillable_usd": max_fillable}


def execution_returns(side_sign: int, entry_fill, exit_fill, entry_mid, exit_mid,
                      ret_research_bps: float) -> dict:
    """ret_mid / ret_exec[k] / cost[k] (§6.3). Taker both ways: buy ladder in, sell ladder out."""
    ret_mid = (side_sign * (exit_mid / entry_mid - 1.0) * 1e4
               if entry_mid and exit_mid else None)
    ret_exec, cost = [], []
    for ef, xf in zip(entry_fill, exit_fill):
        if ef and xf:
            r = side_sign * (xf / ef - 1.0) * 1e4
            ret_exec.append(r)
            cost.append(ret_research_bps - r)
        else:
            ret_exec.append(None)
            cost.append(None)
    return {"ret_mid_bps": ret_mid, "ret_exec_bps": ret_exec, "cost_bps": cost}


def _pct(sorted_vals, q):
    if not sorted_vals:
        return None
    i = min(len(sorted_vals) - 1, int(q * len(sorted_vals)))
    return sorted_vals[i]


def hold_stats(samples: list, entry_mid: float, side_sign: int) -> dict:
    """§5.2 hold-window summary from the 1s samples: mid OHLC, MAE/MFE bps vs entry mid (sign-adjusted),
    mean/p95 spread, mean top-of-book depth, and stale/missed counts."""
    mids = [s["mid"] for s in samples if s.get("mid")]
    spreads = sorted(s["spread_bps"] for s in samples if s.get("spread_bps") is not None)
    depths = [s["depth"] for s in samples if s.get("depth") is not None]
    stale = sum(1 for s in samples if s.get("stale"))
    if not mids:
        return {"n_samples": len(samples), "stale_samples": stale, "mae_bps": None, "mfe_bps": None}
    exc = [side_sign * (m / entry_mid - 1.0) * 1e4 for m in mids]
    return {
        "n_samples": len(samples),
        "mid_open": mids[0], "mid_high": max(mids), "mid_low": min(mids), "mid_close": mids[-1],
        "mae_bps": min(exc), "mfe_bps": max(exc),
        "mean_spread_bps": sum(spreads) / len(spreads) if spreads else None,
        "p95_spread_bps": _pct(spreads, 0.95),
        "mean_depth": sum(depths) / len(depths) if depths else None,
        "stale_samples": stale,
    }


# ---- live feed ---------------------------------------------------------------
class BookFeed:
    """depth20@100ms (20 levels each side) + bookTicker (best bid/ask + sizes), latest-only."""

    def __init__(self, url: str = STREAM_URL) -> None:
        self.url = url
        self._lock = threading.Lock()
        self._bids: list = []          # [[price, size], ...] highest first
        self._asks: list = []          # lowest first
        self._book_ts = 0.0
        self._top = None               # (bid, bid_sz, ask, ask_sz, ts)
        self._ring: deque = deque()    # ~120s of throttled light samples (pre-entry capture, §5)
        self._ring_last = 0.0
        self._ws = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self.reconnects = 0

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        # The feed thread owns this socket and closes it in its own finally.
        # Closing it from another thread can free an fd that has already been
        # reopened elsewhere in the process (2026-08-12 DB corruption).
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    def _run(self) -> None:
        import logging

        import websocket as ws_client
        self._ws_client = ws_client
        log = logging.getLogger(__name__)
        delay = 5
        while not self._stop.is_set():
            try:
                self._ws = ws_client.create_connection(self.url, timeout=10)
                self._ws.settimeout(2)
                delay = 5
                while not self._stop.is_set():
                    try:
                        raw = self._ws.recv()
                    except ws_client.WebSocketTimeoutException:
                        continue
                    self._on_message(raw)
            except Exception as e:
                if not self._stop.is_set():
                    self.reconnects += 1
                    log.warning("book feed: %s; reconnecting in %ss", e, delay)
            finally:
                if self._ws:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
            if not self._stop.is_set():
                time.sleep(delay)
                delay = min(delay * 2, 60)

    @staticmethod
    def _levels(raw):
        out = []
        for e in raw or []:
            try:
                out.append([float(e[0]), float(e[1])])
            except (TypeError, ValueError, IndexError):
                continue
        return out

    def _on_message(self, raw: str) -> None:
        msg = json.loads(raw)
        data = msg.get("data", msg)
        stream = msg.get("stream", "")
        now = time.time()
        if "depth" in stream or "b" in data and "a" in data and "u" in data:
            bids = self._levels(data.get("b"))
            asks = self._levels(data.get("a"))
            if bids and asks:
                with self._lock:
                    self._bids, self._asks, self._book_ts = bids, asks, now
        if "bookTicker" in stream or ("b" in data and "B" in data and "a" in data and "A" in data):
            try:
                top = (float(data["b"]), float(data["B"]), float(data["a"]), float(data["A"]), now)
            except (TypeError, ValueError, KeyError):
                return
            with self._lock:
                self._top = top
        self._record_ring()

    def _record_ring(self) -> None:
        """Throttled light top-of-book sample into the trailing ~120s ring (feeds the poller's
        pre-entry window, §5, without persisting full 20-level books)."""
        now = time.time()
        if now - self._ring_last < 0.1:
            return
        snap = self.snapshot()
        if snap is None:
            return
        self._ring_last = now
        light = {k: snap[k] for k in ("ts", "mid", "spread_bps", "depth", "stale",
                                      "best_bid", "best_ask", "bid_sz", "ask_sz")}
        with self._lock:
            self._ring.append(light)
            cutoff = now - 120.0
            while self._ring and self._ring[0]["ts"] < cutoff:
                self._ring.popleft()

    def recent_since(self, t0: float) -> list:
        with self._lock:
            return [s for s in self._ring if s["ts"] >= t0]

    def snapshot(self) -> Optional[dict]:
        """Point-in-time book: best bid/ask + sizes, mid, spread bps, 20 levels, staleness flag."""
        with self._lock:
            bids, asks, book_ts = list(self._bids), list(self._asks), self._book_ts
            top = self._top
        now = time.time()
        if top is not None:
            bid, bid_sz, ask, ask_sz, top_ts = top
            last = max(top_ts, book_ts)
        elif bids and asks:
            bid, bid_sz, ask, ask_sz, last = bids[0][0], bids[0][1], asks[0][0], asks[0][1], book_ts
        else:
            return None
        mid = (bid + ask) / 2.0
        spread_bps = (ask - bid) / mid * 1e4 if mid else None
        return {"ts": now, "best_bid": bid, "best_ask": ask, "bid_sz": bid_sz, "ask_sz": ask_sz,
                "mid": mid, "spread_bps": spread_bps, "depth": (bid_sz + ask_sz) / 2.0,
                "bids": bids, "asks": asks, "stale": (now - last) > STALE_S}


# ---- phased poller -----------------------------------------------------------
class BookPoller(threading.Thread):
    """Samples the book across a paper entry's [entry-60s, exit+60s] window at the §5 cadence, then
    produces the RESULT execution block. Runs one per open position (the book holds one at a time)."""

    def __init__(self, feed: BookFeed, side_sign: int, entry_mid: float, entry_fill: list,
                 entry_ts: float, exit_ts: float, backfilled: bool = False) -> None:
        super().__init__(daemon=True)
        self.feed = feed
        self.side_sign = side_sign
        self.entry_mid = entry_mid
        self.entry_fill = entry_fill
        self.entry_ts = entry_ts
        self.exit_ts = exit_ts
        self.backfilled = backfilled
        self._stop = threading.Event()
        self.samples: list = []
        self.pre_entry: list = []
        self._exit_snap = None
        self.missed = 0

    def _cadence(self, now: float) -> float:
        if now <= self.entry_ts + 5 or now >= self.exit_ts - 60 or now <= self.entry_ts:
            return 0.1                       # pre-entry / entry / exit windows
        return 1.0                           # hold

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        try:                                     # §5 pre-entry window: last 60s from the feed ring
            self.pre_entry = self.feed.recent_since(self.entry_ts - 60)
        except Exception:
            self.pre_entry = []
        end = self.exit_ts + 60
        while not self._stop.is_set() and time.time() < end:
            snap = self.feed.snapshot()
            now = time.time()
            if snap is not None:
                self.samples.append(snap)
                if abs(now - self.exit_ts) < 2 or (now >= self.exit_ts and self._exit_snap is None):
                    self._exit_snap = snap
            else:
                self.missed += 1
            time.sleep(self._cadence(now))
        if self._exit_snap is None:
            self._exit_snap = self.feed.snapshot()

    def result_block(self, ret_research_bps: float) -> dict:
        """Assemble the §6.3 execution columns from the collected samples + exit book."""
        snap = self._exit_snap
        buy_to_close = self.side_sign < 0           # short closes by buying, long closes by selling
        if snap is not None:
            levels = snap["asks"] if buy_to_close else snap["bids"]
            ladder = executable_ladder(levels, snap["mid"], buy=buy_to_close)
            exit_mid, exit_spread = snap["mid"], snap["spread_bps"]
        else:
            ladder = {"fill_px": [None] * len(config.NOTIONALS),
                      "slippage_bps": [None] * len(config.NOTIONALS), "max_fillable_usd": 0.0}
            exit_mid = exit_spread = None
        rets = execution_returns(self.side_sign, self.entry_fill, ladder["fill_px"],
                                 self.entry_mid, exit_mid, ret_research_bps)
        # §5.2 hold window is [entry, exit] — drop the pre-entry seed and the post-exit +60s tail.
        hold = [s for s in self.samples if self.entry_ts <= s["ts"] <= self.exit_ts]
        stats = hold_stats(hold, self.entry_mid, self.side_sign)
        pre_spreads = [s["spread_bps"] for s in self.pre_entry if s.get("spread_bps") is not None]
        return {
            "exit_mid": exit_mid, "exit_spread_bps": exit_spread,
            "fill_px": ladder["fill_px"], "slippage_bps": ladder["slippage_bps"],
            "ret_mid_bps": rets["ret_mid_bps"], "ret_exec_bps": rets["ret_exec_bps"],
            "cost_bps": rets["cost_bps"], "hold_stats": stats,
            "data_quality": {"missed_samples": self.missed, "stale_at_exit": bool(snap and snap["stale"]),
                             "reconnects": self.feed.reconnects,
                             "max_fillable_usd": ladder["max_fillable_usd"],
                             "pre_entry_samples": len(self.pre_entry),
                             "pre_entry_mean_spread_bps": (sum(pre_spreads) / len(pre_spreads))
                             if pre_spreads else None,
                             "backfilled_entry": self.backfilled},
        }

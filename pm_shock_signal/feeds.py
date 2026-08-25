"""
Live data feeds (WebSocket) for the shock signal.

Two feeds, each a daemon thread with its own auto-reconnect loop (connect /
reconnect shape copied from
pm_btc15updown_data/collect_pm_btcupdown.py::TradeFeed and ::DepthFeed).

  BtcMidFeed   — Binance bookTicker → rolling (event_ts, mid); serves mid_at(t)
                 (interpolated, causal) and rv_60s(t).
  PmTokenFeed  — Polymarket market channel → per-token last-trade + best bid/ask;
                 rolls to the new 15m market each window, tracks the strike.

Clock convention (REVIEW item 1 — EVENT-TIME alignment): price/mid *series* are
stamped with the message's own exchange/server event time, not receipt time —
Binance `E`, Polymarket `last_trade_price.timestamp` (both ms). PM trades are
delivered ~1 s late (docs/polymarket_api_endpoint_inventory.md), so aligning the
BTC confirmation window to the PM shock's *event* time (as the backtest's
merge_asof did) removes a systematic ~1 s skew. The engine still ticks on the wall
clock; it anchors each evaluation on the PM trade's event time (see ShockDetector).
Top-of-book freshness (`BookTop.ts`) stays on the wall/receipt clock — it measures
delivery staleness at the moment of a fill, which is a wall-time question.

p_tok (BUILD_SPEC §12) = last_trade_price, forward-filled. Entry/exit *fills* use
best ask / best bid regardless.
"""
from __future__ import annotations

import json
import logging
import math
import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

import websocket as ws_client

from pm_shock_signal import config
from pm_btc15updown_data.collect_pm_btcupdown import (
    resolve_market,
    fetch_strike,
)

log = logging.getLogger(__name__)

# Keep enough BTC mid history for rv_60s + the largest Δ lookback, with slack.
_BTC_HISTORY_SEC = config.RV_WINDOW_SEC + max(config.DELTAS) + 60   # ~130 s
# Throttle BTC buffer writes (bookTicker can be very high frequency); 250 ms keeps
# the buffer small while staying far denser than the 5 s rv grid.
_BTC_STORE_MIN_GAP_S = 0.25
# Keep ~window of PM last-trade history per token.
_PM_SERIES_MAXLEN = 4000


@dataclass
class BookTop:
    """Top-of-book snapshot for one PM token. `ts` is the wall/receipt clock."""
    ts: float            # receipt time of the last book/trade update (for staleness)
    last: Optional[float]  # last-trade price (p_tok); None before first trade
    bid: Optional[float]   # best bid (sell-into for exit)
    ask: Optional[float]   # best ask (buy-at for entry)


class BtcMidFeed:
    """Binance bookTicker WS → rolling (event_ts, mid) buffer + realized-vol."""

    def __init__(self) -> None:
        self._buf: deque = deque()      # (event_ts, mid), pruned by time, guarded by _lock
        self._lock = threading.Lock()
        self._last_mid: Optional[float] = None
        self._last_store: float = 0.0
        self._ws = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    # -- lifecycle ----------------------------------------------------------
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
        delay = 5
        while not self._stop.is_set():
            try:
                log.info("BTC feed: connecting to %s", config.BTC_WS_URL)
                self._ws = ws_client.create_connection(config.BTC_WS_URL, timeout=10)
                self._ws.settimeout(2)
                log.info("BTC feed: connected")
                delay = 5
                while not self._stop.is_set():
                    try:
                        raw = self._ws.recv()
                    except ws_client.WebSocketTimeoutException:
                        continue
                    self._on_message(raw)
            except Exception as e:
                if not self._stop.is_set():
                    log.warning("BTC feed: %s. Reconnecting in %ss...", e, delay)
            finally:
                if self._ws:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
            if not self._stop.is_set():
                time.sleep(delay)
                delay = min(delay * 2, 60)

    def _on_message(self, raw: str) -> None:
        data = json.loads(raw)
        b = data.get("b")
        a = data.get("a")
        if b is None or a is None:
            return
        mid = (float(b) + float(a)) / 2.0
        if mid <= 0:
            return
        # Binance event time E (ms); fall back to receipt if absent.
        e = data.get("E")
        event_ts = float(e) / 1000.0 if e is not None else time.time()
        recv = time.time()
        self._last_mid = mid
        if recv - self._last_store >= _BTC_STORE_MIN_GAP_S:
            with self._lock:
                self._buf.append((event_ts, mid))
                cutoff = event_ts - _BTC_HISTORY_SEC
                while self._buf and self._buf[0][0] < cutoff:
                    self._buf.popleft()
            self._last_store = recv

    # -- reads --------------------------------------------------------------
    def mid_now(self) -> Optional[float]:
        return self._last_mid

    @staticmethod
    def _mid_at(snap: list, t: float) -> Optional[float]:
        """Mid at exactly `t`, linearly interpolated between bracketing ticks.

        Causal/gate: returns None if the buffer does not bracket `t` — i.e. no
        sample at/after `t` (we'd have to extrapolate forward) or none before it.
        At the ~1 s-lagged anchor the BTC buffer normally extends past `t`.
        """
        lo = hi = None
        for i in range(len(snap) - 1, -1, -1):
            if snap[i][0] <= t:
                lo = snap[i]
                hi = snap[i + 1] if i + 1 < len(snap) else None
                break
        if lo is None:
            return None                 # t before earliest sample
        if lo[0] == t:
            return lo[1]
        if hi is None:
            return None                 # t after latest sample → not yet covered
        span = hi[0] - lo[0]
        if span <= 0:
            return lo[1]
        frac = (t - lo[0]) / span
        return lo[1] + frac * (hi[1] - lo[1])

    def mid_at(self, t: float) -> Optional[float]:
        with self._lock:
            snap = list(self._buf)
        return self._mid_at(snap, t)

    def rv_60s(self, t: float) -> Optional[float]:
        """std of consecutive ~5 s mid log-returns over [t−60, t] (BUILD_SPEC §4).

        Resamples the buffer onto a 5 s grid ending at `t`, takes consecutive
        log-returns, returns their sample std (ddof=1) — matching
        01_build_depth_features.py. None if fewer than 2 returns are available
        (e.g. the buffer does not yet cover `t`).
        """
        with self._lock:
            snap = list(self._buf)
        if not snap:
            return None
        n_steps = config.RV_WINDOW_SEC // config.RV_STEP_SEC
        grid_mids = [
            self._mid_at(snap, t - config.RV_WINDOW_SEC + i * config.RV_STEP_SEC)
            for i in range(n_steps + 1)
        ]
        rets = [
            math.log(b / a)
            for a, b in zip(grid_mids, grid_mids[1:])
            if a and b and a > 0 and b > 0
        ]
        if len(rets) < 2:
            return None
        return statistics.stdev(rets)


@dataclass
class ActiveMarket:
    """The currently-tracked 15m market."""
    epoch_start: int
    up_token_id: str
    down_token_id: str
    strike: Optional[float] = None     # Binance BTC mid at window open (optional, for analysis)


class PmTokenFeed:
    """Polymarket market-channel WS → per-token last-trade + top-of-book, with 15m roll."""

    def __init__(self) -> None:
        self.market: Optional[ActiveMarket] = None
        self._series: dict = {}    # token_id -> deque[(event_ts, last)]
        self._top: dict = {}       # token_id -> BookTop
        self._lock = threading.Lock()
        self._token_ids: list[str] = []
        self._restart = threading.Event()
        self._ws = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    # -- lifecycle ----------------------------------------------------------
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

    def roll_market(self, epoch_start: int) -> None:
        """Resolve + (re)subscribe to the market for `epoch_start`; reset per-token series."""
        info = resolve_market(epoch_start)
        if not info:
            log.warning("PM feed: no market for epoch %s", epoch_start)
            return
        outcomes = info.get("outcomes") or ["Up", "Down"]
        token_ids = info.get("token_ids") or []
        if len(token_ids) < 2:
            log.warning("PM feed: market %s has <2 tokens", info.get("slug"))
            return
        up_idx = next((i for i, o in enumerate(outcomes) if str(o).lower() == "up"), 0)
        down_idx = 1 - up_idx if up_idx in (0, 1) else 1
        up_token_id = token_ids[up_idx]
        down_token_id = token_ids[down_idx]

        strike = None
        try:
            s = fetch_strike(epoch_start)
            strike = float(s) if s else None
        except Exception as e:
            log.warning("PM feed: strike fetch failed for %s: %s", epoch_start, e)

        with self._lock:
            self.market = ActiveMarket(epoch_start, up_token_id, down_token_id, strike)
            self._series = {up_token_id: deque(maxlen=_PM_SERIES_MAXLEN),
                            down_token_id: deque(maxlen=_PM_SERIES_MAXLEN)}
            self._top = {}
            self._token_ids = [up_token_id, down_token_id]
            self._restart.set()
        # _restart alone makes the WS loop resubscribe: its inner loop re-checks the
        # flag every recv() timeout (2s). Do NOT close the socket here — the feed
        # thread owns it (2026-08-12 DB corruption).
        log.info("PM feed: rolled to epoch %s (%s) strike=%s",
                 epoch_start, info.get("slug"), strike)

    def _run(self) -> None:
        delay = 5
        while not self._stop.is_set():
            with self._lock:
                tids = list(self._token_ids)
            if not tids:
                time.sleep(1)
                continue
            try:
                log.info("PM feed: connecting to %s (%d tokens)", config.PM_WS_URL, len(tids))
                self._ws = ws_client.create_connection(config.PM_WS_URL, timeout=10)
                self._ws.send(json.dumps({"type": "market", "assets_ids": tids}))
                self._ws.settimeout(2)
                self._restart.clear()
                log.info("PM feed: connected")
                delay = 5
                while not self._stop.is_set() and not self._restart.is_set():
                    try:
                        raw = self._ws.recv()
                    except ws_client.WebSocketTimeoutException:
                        continue
                    self._on_message(raw)
            except Exception as e:
                if not self._stop.is_set():
                    log.warning("PM feed: %s. Reconnecting in %ss...", e, delay)
            finally:
                if self._ws:
                    try:
                        self._ws.close()
                    except Exception:
                        pass
            if not self._stop.is_set() and not self._restart.is_set():
                time.sleep(delay)
                delay = min(delay * 2, 60)

    def _on_message(self, raw: str) -> None:
        if not raw:
            return
        msgs = json.loads(raw) if raw.lstrip().startswith("[") else [json.loads(raw)]
        recv = time.time()
        for msg in msgs:
            if not isinstance(msg, dict):
                continue
            etype = msg.get("event_type")
            asset_id = msg.get("asset_id")
            if asset_id is None:
                continue
            if etype == "book":
                self._apply_book(asset_id, msg, recv)
            elif etype == "price_change":
                self._apply_price_change(asset_id, msg, recv)
            elif etype == "last_trade_price":
                self._apply_last_trade(asset_id, msg, recv)

    @staticmethod
    def _f(v) -> Optional[float]:
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def _get_top(self, asset_id: str, recv: float) -> BookTop:
        top = self._top.get(asset_id)
        if top is None:
            top = BookTop(ts=recv, last=None, bid=None, ask=None)
            self._top[asset_id] = top
        return top

    def _apply_book(self, asset_id: str, msg: dict, recv: float) -> None:
        """Initial/refresh full-ladder snapshot → best bid (max) / best ask (min)."""
        bids = msg.get("bids") or msg.get("buys") or []
        asks = msg.get("asks") or msg.get("sells") or []
        bid_px = [self._f(lvl.get("price")) for lvl in bids if isinstance(lvl, dict)]
        ask_px = [self._f(lvl.get("price")) for lvl in asks if isinstance(lvl, dict)]
        bid_px = [p for p in bid_px if p is not None and p > 0]
        ask_px = [p for p in ask_px if p is not None and p > 0]
        with self._lock:
            top = self._get_top(asset_id, recv)
            top.ts = recv
            if bid_px:
                top.bid = max(bid_px)
            if ask_px:
                top.ask = min(ask_px)

    def _apply_price_change(self, asset_id: str, msg: dict, recv: float) -> None:
        """Single-level book delta — carries top-of-book best_bid/best_ask (inventory doc)."""
        bid = self._f(msg.get("best_bid"))
        ask = self._f(msg.get("best_ask"))
        if bid is None and ask is None:
            return
        with self._lock:
            top = self._get_top(asset_id, recv)
            top.ts = recv
            if bid is not None and bid > 0:
                top.bid = bid
            if ask is not None and ask > 0:
                top.ask = ask

    def _apply_last_trade(self, asset_id: str, msg: dict, recv: float) -> None:
        price = self._f(msg.get("price"))
        if price is None or price <= 0:
            return
        # PM event time (ms string); fall back to receipt if absent.
        raw_ts = msg.get("timestamp")
        ev = self._f(raw_ts)
        event_ts = ev / 1000.0 if ev is not None else recv
        with self._lock:
            series = self._series.get(asset_id)
            if series is None:
                series = deque(maxlen=_PM_SERIES_MAXLEN)
                self._series[asset_id] = series
            series.append((event_ts, price))
            top = self._get_top(asset_id, recv)
            top.ts = recv
            top.last = price

    # -- reads --------------------------------------------------------------
    def price_asof(self, token_id: str, ts: float) -> Optional[tuple[float, float]]:
        """Most recent (event_ts, last) with event_ts <= ts (forward-filled, causal)."""
        with self._lock:
            series = list(self._series.get(token_id, ()))
        for ev_ts, last in reversed(series):
            if ev_ts <= ts:
                return (ev_ts, last)
        return None

    def price_at(self, token_id: str, ts: float) -> Optional[float]:
        """Last-trade p_tok as-of `ts` (value only). None if before first trade."""
        asof = self.price_asof(token_id, ts)
        return asof[1] if asof is not None else None

    def book_top(self, token_id: str) -> Optional[BookTop]:
        """Latest top-of-book (bid/ask) for the token — used for realistic fills."""
        with self._lock:
            top = self._top.get(token_id)
            if top is None:
                return None
            return BookTop(ts=top.ts, last=top.last, bid=top.bid, ask=top.ask)

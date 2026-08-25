"""
Feeds for pm_signal_sim — SELF-OWNED. Depends ONLY on pm_shock_signal (the stable, running base
feed package); does NOT import pm_asym_signal (which is being archived/deleted).

pm_shock_signal's PmTokenFeed._series carries only (event_ts, last_price) — no size — so VWAP windows
and a raw trade buffer must be added here. This module subclasses pm_shock_signal.PmTokenFeed to add a
size-bearing trade buffer + vwap_window + trades_since, and ports BtcDepth20Feed (which pm_shock_signal
does not have). BtcMidFeed is reused unchanged (mid_now / mid_at / rv_60s — mid_at covers lookback).

pm_shock_signal is NOT modified (it runs in production on vps-madrid): we only subclass/import it.
"""
from __future__ import annotations

import json
import threading
import time
from collections import deque
from typing import Optional

import websocket as ws_client

from pm_shock_signal.feeds import BtcMidFeed                       # reused unchanged
from pm_shock_signal.feeds import PmTokenFeed as _BasePmTokenFeed  # subclassed below

from pm_signal_sim import config

__all__ = ["BtcMidFeed", "PmTokenFeed", "BtcDepth20Feed"]

_TRADE_MAXLEN = 50_000          # one 15-min window of trades per token, with headroom (reset on roll)
_DEPTH_STORE_MIN_GAP_S = 1.0    # throttle depth20 (stream pushes ~500ms) to ~1s frames


class PmTokenFeed(_BasePmTokenFeed):
    """pm_shock_signal.PmTokenFeed + a per-token (event_ts, price, size, side) trade buffer back to
    window-start, plus a size-bearing top-of-book history buffer, with the helpers the detector +
    capture + live signal evaluators need.

    Inherited: price_asof / price_at / book_top / start / stop.
    Added:    vwap_window(token_id, a, b) -> float | None   (size-weighted VWAP over (a, b])
              trades_since(token_id, t0)  -> [(ts, price, size, side, recv)]   (raw capture)
              book_since(token_id, t0)    -> [(recv, bid, bid_sz, ask, ask_sz, event_ts)]

    The base feed keeps only best bid/ask prices (no sizes, no history). LIVE_TEST_SPEC §1.1/§1.3
    need best-bid/ask SIZES on every book row and a ≥120s pre-fire book buffer, so this subclass
    maintains a local top-of-book ladder (seeded by the `book` snapshot, updated by `price_change`
    deltas) and a rolling, size-bearing book-top history throttled to ~500ms.
    """

    def __init__(self) -> None:
        super().__init__()
        # token_id -> deque[(event_ts, price, size, side, recv)]; recv = local receipt clock so the
        # raw capture can stamp an honest local_ts on lookback trades pulled from the buffer.
        self._trades: dict = {}
        self._ladder: dict = {}       # token_id -> {"bids": {price: size}, "asks": {price: size}}
        self._book_hist: dict = {}    # token_id -> deque[(recv, bid, bid_sz, ask, ask_sz, event_ts)]
        self._book_store: dict = {}   # token_id -> last book-history store recv (500ms throttle)

    def roll_market(self, epoch_start: int) -> None:
        super().roll_market(epoch_start)
        with self._lock:
            if self.market is not None:
                tids = (self.market.up_token_id, self.market.down_token_id)
                self._trades = {t: deque(maxlen=_TRADE_MAXLEN) for t in tids}
                self._ladder = {t: {"bids": {}, "asks": {}} for t in tids}
                self._book_hist = {t: deque() for t in tids}
                self._book_store = {}
            else:
                self._trades = {}
                self._ladder = {}
                self._book_hist = {}
                self._book_store = {}

    # ---- top-of-book ladder + history (sizes) --------------------------------
    @staticmethod
    def _ladder_from(levels) -> dict:
        out: dict = {}
        for lvl in levels or []:
            if not isinstance(lvl, dict):
                continue
            p = PmTokenFeed._f(lvl.get("price"))
            s = PmTokenFeed._f(lvl.get("size"))
            if p is not None and p > 0 and s is not None and s > 0:
                out[p] = s
        return out

    @staticmethod
    def _best(book: dict, side: str):
        valid = {p: s for p, s in book.items() if s > 0}
        if not valid:
            return None, None
        p = max(valid) if side == "bid" else min(valid)
        return p, valid[p]

    def _record_book_locked(self, asset_id: str, recv: float) -> None:
        """Append one throttled book-top sample (best bid/ask + sizes) to the history. Caller holds
        _lock. Prefers the local ladder; falls back to the base top's prices (sizes NULL) pre-snapshot."""
        if recv - self._book_store.get(asset_id, 0.0) < config.BOOK_STORE_MIN_GAP_S:
            return
        lad = self._ladder.get(asset_id)
        bid = bid_sz = ask = ask_sz = None
        if lad is not None:
            bid, bid_sz = self._best(lad["bids"], "bid")
            ask, ask_sz = self._best(lad["asks"], "ask")
        if bid is None and ask is None:
            top = self._top.get(asset_id)
            if top is None:
                return
            bid, ask = top.bid, top.ask
            if bid is None and ask is None:
                return
        hist = self._book_hist.setdefault(asset_id, deque())
        hist.append((recv, bid, bid_sz, ask, ask_sz, recv))
        cutoff = recv - config.BOOK_HISTORY_SEC
        while hist and hist[0][0] < cutoff:
            hist.popleft()
        self._book_store[asset_id] = recv

    def _apply_book(self, asset_id: str, msg: dict, recv: float) -> None:
        super()._apply_book(asset_id, msg, recv)   # base updates best bid/ask prices on `top`
        bids = msg.get("bids") or msg.get("buys") or []
        asks = msg.get("asks") or msg.get("sells") or []
        with self._lock:
            lad = self._ladder.setdefault(asset_id, {"bids": {}, "asks": {}})
            lad["bids"] = self._ladder_from(bids)
            lad["asks"] = self._ladder_from(asks)
            self._record_book_locked(asset_id, recv)

    def _apply_price_change(self, asset_id: str, msg: dict, recv: float) -> None:
        super()._apply_price_change(asset_id, msg, recv)   # base updates top best_bid/best_ask
        changes = msg.get("changes")
        items = changes if isinstance(changes, list) else [msg]
        with self._lock:
            lad = self._ladder.setdefault(asset_id, {"bids": {}, "asks": {}})
            for it in items:
                if not isinstance(it, dict):
                    continue
                price = self._f(it.get("price"))
                size = self._f(it.get("size"))
                side = str(it.get("side") or "").upper()
                book = lad["bids"] if side in ("BUY", "BID") else (
                    lad["asks"] if side in ("SELL", "ASK") else None)
                if price is None or size is None or book is None:
                    continue
                if size <= 0:
                    book.pop(price, None)
                else:
                    book[price] = size
            self._record_book_locked(asset_id, recv)

    def book_since(self, token_id: str, t0: float) -> list:
        """Book-top samples (recv, bid, bid_sz, ask, ask_sz, event_ts) with recv > t0 (raw capture)."""
        with self._lock:
            return [r for r in self._book_hist.get(token_id, ()) if r[0] > t0]

    def book_top_sized(self, token_id: str):
        """Standing top-of-book (bid, bid_sz, ask, ask_sz, last_update_recv) from the local ladder,
        for the quiet-book keepalive (LIVE_TEST_SPEC §5.4). None if no book seen yet."""
        with self._lock:
            lad = self._ladder.get(token_id)
            last_recv = self._book_store.get(token_id)
            bid = bid_sz = ask = ask_sz = None
            if lad is not None:
                bid, bid_sz = self._best(lad["bids"], "bid")
                ask, ask_sz = self._best(lad["asks"], "ask")
            if bid is None and ask is None:
                top = self._top.get(token_id)
                if top is None:
                    return None
                bid, ask = top.bid, top.ask
                if bid is None and ask is None:
                    return None
            return (bid, bid_sz, ask, ask_sz, last_recv)

    def _apply_last_trade(self, asset_id: str, msg: dict, recv: float) -> None:
        super()._apply_last_trade(asset_id, msg, recv)   # updates _series + top.last (unchanged)
        price = self._f(msg.get("price"))
        if price is None or price <= 0:
            return
        size = self._f(msg.get("size"))
        ev = self._f(msg.get("timestamp"))
        event_ts = ev / 1000.0 if ev is not None else recv
        with self._lock:
            buf = self._trades.get(asset_id)
            if buf is None:
                buf = deque(maxlen=_TRADE_MAXLEN)
                self._trades[asset_id] = buf
            buf.append((event_ts, price, size if size is not None else 0.0, msg.get("side"), recv))

    def vwap_window(self, token_id: str, a: float, b: float) -> Optional[float]:
        """Size-weighted mean trade price over (a, b]. None if no usable (size>0) trades."""
        with self._lock:
            buf = list(self._trades.get(token_id, ()))
        num = den = 0.0
        for ts, price, size, _side, _recv in buf:
            if a < ts <= b and size > 0:
                num += price * size
                den += size
        return (num / den) if den > 0 else None

    def trades_since(self, token_id: str, t0: float) -> list:
        """Buffered trades (event_ts, price, size, side, recv) with event_ts >= t0 (raw capture)."""
        with self._lock:
            buf = list(self._trades.get(token_id, ()))
        return [t for t in buf if t[0] >= t0]


class BtcDepth20Feed:
    """Binance depth20 partial-book WS → trailing ring buffer of ~1s frames {ts, mid, bids, asks},
    for the honest raw capture (and offline OBI/microprice). Self-contained lifecycle."""

    def __init__(self, url: str, buffer_sec: int) -> None:
        self.url = url
        self.buffer_sec = buffer_sec
        self._buf: deque = deque()
        self._lock = threading.Lock()
        self._last_store = 0.0
        self._ws = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

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
                    log.warning("BTC depth feed: %s. Reconnecting in %ss...", e, delay)
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
    def _levels(raw_levels) -> list:
        out = []
        for entry in raw_levels or []:
            try:
                out.append([float(entry[0]), float(entry[1])])
            except (TypeError, ValueError, IndexError):
                continue
        return out

    def _on_message(self, raw: str) -> None:
        data = json.loads(raw)
        bids = self._levels(data.get("b"))
        asks = self._levels(data.get("a"))
        if not bids or not asks:
            return
        best_bid = max(p for p, _ in bids)
        best_ask = min(p for p, _ in asks)
        mid = (best_bid + best_ask) / 2.0
        e = data.get("E")
        ts = float(e) / 1000.0 if e is not None else time.time()
        recv = time.time()
        if recv - self._last_store < _DEPTH_STORE_MIN_GAP_S:
            return
        self._last_store = recv
        frame = {"ts": ts, "mid": mid, "bids": bids, "asks": asks, "local_ts": recv}
        with self._lock:
            self._buf.append(frame)
            cutoff = ts - self.buffer_sec
            while self._buf and self._buf[0]["ts"] < cutoff:
                self._buf.popleft()

    def snapshots_since(self, t0: float) -> list[dict]:
        with self._lock:
            return [f for f in self._buf if f["ts"] >= t0]

    def latest(self) -> Optional[dict]:
        with self._lock:
            return self._buf[-1] if self._buf else None

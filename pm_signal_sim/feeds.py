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

__all__ = ["BtcMidFeed", "PmTokenFeed", "BtcDepth20Feed"]

_TRADE_MAXLEN = 50_000          # one 15-min window of trades per token, with headroom (reset on roll)
_DEPTH_STORE_MIN_GAP_S = 1.0    # throttle depth20 (stream pushes ~500ms) to ~1s frames


class PmTokenFeed(_BasePmTokenFeed):
    """pm_shock_signal.PmTokenFeed + a per-token (event_ts, price, size, side) trade buffer back to
    window-start, with the VWAP-window / raw-trade helpers the detector + capture need.

    Inherited: price_asof / price_at / book_top / roll_market / start / stop.
    Added:    vwap_window(token_id, a, b) -> float | None   (size-weighted VWAP over (a, b])
              trades_since(token_id, t0)  -> [(ts, price, size, side)]   (for the raw capture)
    """

    def __init__(self) -> None:
        super().__init__()
        # token_id -> deque[(event_ts, price, size, side, recv)]; recv = local receipt clock so the
        # raw capture can stamp an honest local_ts on lookback trades pulled from the buffer.
        self._trades: dict = {}

    def roll_market(self, epoch_start: int) -> None:
        super().roll_market(epoch_start)
        with self._lock:
            if self.market is not None:
                self._trades = {
                    self.market.up_token_id: deque(maxlen=_TRADE_MAXLEN),
                    self.market.down_token_id: deque(maxlen=_TRADE_MAXLEN),
                }
            else:
                self._trades = {}

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
        self._stop.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

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

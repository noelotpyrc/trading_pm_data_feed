"""Whole-window wire recording. No in-process book or signal computation."""
from __future__ import annotations

import queue
import threading

from .storage import RecordingError, append, day


class WireWriter:
    """A bounded FIFO keeps JSON parsing and disk writes off the socket reader.

    All frames and gap markers from a reader share one FIFO, across reconnects.
    Saturation is fatal: never discard a frame or quietly backpressure receipt
    timestamps. On graceful shutdown, finish queued observations before closing.
    """

    def __init__(self, consume, stop, *, max_items=10_000):
        self.consume = consume
        self.stop = stop
        self.queue = queue.Queue(maxsize=max_items)
        self.error = None
        self.thread = threading.Thread(target=self.run, name="nba-wire-writer")

    def __enter__(self):
        self.thread.start()
        return self

    def submit(self, item):
        if self.error is not None:
            raise RecordingError("Websocket writer failed") from self.error
        try:
            self.queue.put_nowait(item)
        except queue.Full as exc:
            self.stop.set()
            raise RecordingError("Websocket recording queue is full") from exc

    def run(self):
        try:
            while True:
                item = self.queue.get()
                if item is None:
                    return
                self.consume(item)
        except BaseException as exc:
            self.error = exc
            self.stop.set()

    def __exit__(self, *_exc):
        while self.thread.is_alive():
            try:
                self.queue.put(None, timeout=.1)
                break
            except queue.Full:
                continue
        self.thread.join()
        if self.error is not None:
            raise RecordingError("Websocket writer failed") from self.error


class BookCapture:
    def __init__(self, store):
        self.store = store

    def marker(self, routes, recv_ms, event, **details):
        grouped = {}
        for token, (slug, _, _) in routes.items():
            grouped.setdefault(slug, []).append(token)
        for slug, tokens in grouped.items():
            append(self.store, f"book/{slug}.jsonl", "book_gap",
                   {"recv_ms": recv_ms, "slug": slug, "kind": "gap", "event": event,
                    "tokens": sorted(tokens), **details})

    def record(self, payload, recv_ms, routes):
        messages = payload if isinstance(payload, list) else [payload]
        for frame_index, message in enumerate(messages):
            if not isinstance(message, dict):
                continue
            tokens = {str(message.get("asset_id"))}
            tokens.update(str(c.get("asset_id")) for c in message.get("price_changes", []))
            condition = message.get("market")
            slugs = {routes[t][0] for t in tokens if t in routes}
            # Lifecycle messages can identify a condition or an assets_ids list.
            tokens.update(map(str, message.get("assets_ids") or []))
            slugs.update(routes[t][0] for t in tokens if t in routes)
            slugs.update(info[0] for info in routes.values() if condition and info[2] == condition)
            for slug in sorted(slugs):
                kind = "fill" if message.get("event_type") == "last_trade_price" else "book_message"
                append(self.store, f"book/{slug}.jsonl", kind,
                       {"recv_ms": recv_ms, "slug": slug, "kind": "message", "frame_index": frame_index,
                        "message": message})  # Never trim cross-token/cross-game wire payloads.


def record_sports(store, payload, recv_ms):
    for message in payload if isinstance(payload, list) else [payload]:
        if not isinstance(message, dict):
            continue
        if str(message.get("leagueAbbreviation", "")).lower() != "nba":
            continue
        append(store, f"pm_sports/{day(recv_ms)}.jsonl", "sports_message",
               {"recv_ms": recv_ms, "message": message})

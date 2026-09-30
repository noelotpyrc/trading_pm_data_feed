"""Direct JSONL recording and shared, process-local state; no recovery journal."""
from __future__ import annotations

import fcntl
import json
import os
import threading
import time
from collections import Counter, OrderedDict
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path


def now_ms():
    return time.time_ns() // 1_000_000


def day(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d")


def dumps(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


class RecordingError(RuntimeError):
    """Fatal output failure: stop the session instead of running unrecorded."""


class Store:
    """One shared instance per session, used by all workers.

    Input is written once to its final JSONL path and delivered synchronously
    to the engine under the same lock. There is no replay queue or checkpoint.
    Raw files flush periodically; engine records flush immediately. fsync is
    reserved for graceful close. A crash can lose buffered rows or a partial
    final line; a restart must use a fresh session directory.
    """
    INPUT_KINDS = {"match", "market", "score", "poll", "gap"}

    def __init__(self, root, *, session_id=None, max_open_files=64):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        if any(self.root.rglob("*.jsonl")) or (self.root / "journal.sqlite").exists():
            raise ValueError("Recordings already exist; start a fresh session directory")
        self.session_id = session_id
        self.lock = threading.RLock()
        self.on_event = None
        self._state = {}
        self._seq = 0
        self._files = OrderedDict()
        self._max_open_files = max_open_files
        self._dirty = set()
        self._failed = None
        self._closed = False
        self._counts = Counter()
        self._health = Counter()
        self._last_backoff = None
        self.started_ms = now_ms()

    def get(self, key, default=None):
        with self.lock:
            return deepcopy(self._state.get(key, default))

    def put(self, key, value):
        with self.lock:
            self._state[key] = deepcopy(value)

    def _check(self):
        if self._failed is not None:
            raise RecordingError("Recording previously failed") from self._failed
        if self._closed:
            raise RecordingError("Recording session is closed")

    def _handle(self, path):
        if path in self._files:
            self._files.move_to_end(path)
            return self._files[path]
        # Validate when opening, not on every market update. An open handle
        # continues to refer to that same file even if a symlink later changes.
        resolved = path.resolve()
        if not resolved.is_relative_to(self.root):
            raise ValueError(f"Recording path escapes data directory: {path}")
        if len(self._files) >= self._max_open_files:
            old, handle = self._files.popitem(last=False)
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()
            self._dirty.discard(old)
        resolved.parent.mkdir(parents=True, exist_ok=True)
        handle = resolved.open("a", encoding="utf-8")
        self._files[path] = handle
        return handle

    def append(self, producer, kind, payload, path):
        with self.lock:
            self._check()
            target = self.root / path
            seq = self._seq + 1
            row = {**payload, "event_seq": seq}
            if self.session_id is not None:
                row["session_id"] = self.session_id
            encoded = dumps(row) + "\n"
            try:
                handle = self._handle(target)
                handle.write(encoded)
                self._dirty.add(target)
                if producer == "engine":
                    handle.flush()
                    self._dirty.discard(target)
            except (OSError, ValueError) as exc:
                self._failed = exc
                raise RecordingError("Cannot write live recording") from exc
            self._seq = seq
            self._counts[kind] += 1
            if kind == "fire" and payload.get("f_minute30") and payload.get("f_leader1_up"):
                self._health["selected_filter_count"] += 1
            if kind == "candidate" and payload.get("late"):
                self._health["late_candidates"] += 1
            if kind == "poll" and payload.get("http_status") == 403:
                backoff = payload.get("backoff_until_ms")
                if backoff != self._last_backoff or backoff is None:
                    self._health["http_403s"] += 1
                self._last_backoff = backoff
            if kind == "gap" and payload.get("reason") == "connect_or_reconnect":
                self._health["websocket_connections"] += 1
            if self.on_event is not None and kind in self.INPUT_KINDS:
                try:
                    self.on_event({"seq": seq, "producer": producer, "kind": kind, "payload": deepcopy(payload)})
                except Exception as exc:
                    self._failed = exc
                    raise RecordingError("Live engine delivery failed") from exc
            return seq

    publish = append

    def summary(self):
        with self.lock:
            health = {key: self._health[key] for key in
                      ("selected_filter_count", "late_candidates", "http_403s", "websocket_connections")}
            return {**self._counts, **health}

    def flush(self):
        with self.lock:
            self._check()
            try:
                for path in self._dirty:
                    self._files[path].flush()
                self._dirty.clear()
            except (OSError, ValueError) as exc:
                self._failed = exc
                raise RecordingError("Cannot flush live recording") from exc

    def close(self):
        with self.lock:
            if self._closed:
                return
            error = None
            for handle in self._files.values():
                try:
                    handle.flush()
                    os.fsync(handle.fileno())
                except OSError as exc:
                    error = error or exc
                finally:
                    handle.close()
            self._files.clear()
            self._closed = True
            if error:
                raise RecordingError("Cannot close live recording") from error


@contextmanager
def process_lock(root, role):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / f".{role}.lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"Another {role} process owns {root}") from exc
        yield

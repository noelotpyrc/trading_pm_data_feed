#!/usr/bin/env python3
"""
Live dry run — SPEC_dryrun_book11.md. Streams btcusdt@kline_1m, drives the engine on each closed bar,
and runs the order-book poller for taken entries. NO ORDERS ARE PLACED.

Usage:
    python -m btcusdt_perp_signal_v2.scripts.run_dryrun [--dry-run] [--db PATH] [--no-book]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal as signal_mod
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import websocket  # noqa: E402

from btcusdt_perp_signal_v2 import config  # noqa: E402
from btcusdt_perp_signal_v2.book_feed import BookFeed  # noqa: E402
from btcusdt_perp_signal_v2.engine import DryRunEngine  # noqa: E402

log = logging.getLogger("btcusdt_perp_signal_v2")
LOG_FILE = ROOT / "data" / "btcusdt_perp_v2.log"
_shutdown = False


def _setup_logging() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in (logging.FileHandler(LOG_FILE), logging.StreamHandler()):
        h.setFormatter(fmt)
        root.addHandler(h)


def _load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def _kline_to_bar(k: dict) -> dict:
    return {"open_time_ms": int(k["t"]), "close_time_ms": int(k["T"]),
            "open": k["o"], "high": k["h"], "low": k["l"], "close": k["c"],
            "volume": k["v"], "taker_buy_base_volume": k["V"], "num_trades": int(k["n"])}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="btcusdt_perp_signal_v2 live dry run")
    ap.add_argument("--dry-run", action="store_true", help="log the daily summary instead of posting")
    ap.add_argument("--db", type=Path, default=ROOT / config.DB_PATH)
    ap.add_argument("--no-book", action="store_true", help="skip the order-book feed (signal-only)")
    args = ap.parse_args(argv)

    os.chdir(ROOT)
    _setup_logging()
    _load_env()

    feed = None if args.no_book else BookFeed()
    if feed is not None:
        feed.start()
    engine = DryRunEngine(args.db, ROOT / config.OHLCV_DB_PATH, feed=feed, dry_run=args.dry_run)
    engine.warm_up()

    def _stop(sig, _frame):
        global _shutdown
        _shutdown = True

    signal_mod.signal(signal_mod.SIGINT, _stop)
    signal_mod.signal(signal_mod.SIGTERM, _stop)

    def _on_message(ws, message):
        try:
            k = json.loads(message).get("k", {})
            if k.get("x"):
                engine.on_closed_bar(_kline_to_bar(k))
        except Exception:
            log.exception("bar processing error")
        if _shutdown:
            ws.close()

    log.info("dry run started (dry_run=%s, book=%s)", args.dry_run, not args.no_book)
    while not _shutdown:
        try:
            ws = websocket.WebSocketApp(config.KLINE_WS_URL, on_message=_on_message)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception:
            log.exception("ws run_forever raised")
        if not _shutdown:
            time.sleep(5)

    if feed is not None:
        feed.stop()
    log.info("dry run stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

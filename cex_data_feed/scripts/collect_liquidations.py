#!/usr/bin/env python3
"""
BTCUSDT perpetual liquidation collector via Binance WebSocket.

Connects to the forceOrder stream and appends each event as a JSON line
to a daily log file. Handles reconnects and 24h session limits.

Usage:
  python -m cex_data_feed.scripts.collect_liquidations \
    --log-dir data/liquidations [--debug]

Output: one JSONL file per day, e.g. data/liquidations/liq_2026-04-10.jsonl
Each line is the raw forceOrder event with a local receive timestamp added.

Run as a long-lived process (systemd service or tmux/screen on VPS).
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import websocket

WS_URI = "wss://fstream.binance.com/ws/btcusdt@forceOrder"
RECONNECT_DELAY_S = 5
MAX_RECONNECT_DELAY_S = 60

_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    print(f"\n[{fmt_now()}] Caught {signal.Signals(sig).name}, shutting down...")
    _shutdown = True


def fmt_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def get_log_file(base: Path) -> Path:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return base / f"liq_{date_str}.jsonl"


def append_event(base: Path, event: dict) -> Path:
    fp = get_log_file(base)
    with open(fp, "a") as f:
        f.write(json.dumps(event, separators=(",", ":")) + "\n")
    return fp


def collect(log_base: Path, debug: bool = False) -> None:
    delay = RECONNECT_DELAY_S
    total_events = 0

    while not _shutdown:
        ws = None
        try:
            print(f"[{fmt_now()}] Connecting to {WS_URI}")
            ws = websocket.create_connection(WS_URI, timeout=10)
            print(f"[{fmt_now()}] Connected. Listening for BTCUSDT liquidations...")
            delay = RECONNECT_DELAY_S

            while not _shutdown:
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue

                data = json.loads(raw)
                data["_recv_ms"] = int(time.time() * 1000)

                fp = append_event(log_base, data)
                total_events += 1

                o = data.get("o", {})
                print(
                    f"[{fmt_now()}] #{total_events} "
                    f"side={o.get('S')} price={o.get('p')} "
                    f"qty={o.get('q')} filled={o.get('z')} "
                    f"status={o.get('X')} → {fp.name}"
                )

        except (
            websocket.WebSocketConnectionClosedException,
            ConnectionError,
            OSError,
        ) as e:
            print(f"[{fmt_now()}] Disconnected: {e}. Reconnecting in {delay}s...")
        except Exception as e:
            print(f"[{fmt_now()}] Unexpected error: {e}. Reconnecting in {delay}s...")
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass

        if not _shutdown:
            time.sleep(delay)
            delay = min(delay * 2, MAX_RECONNECT_DELAY_S)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="BTCUSDT liquidation collector (forceOrder WebSocket → JSONL)",
    )
    p.add_argument(
        "--log-dir", type=Path, default=Path("data/liquidations"),
        help="Directory for daily JSONL log files (default: data/liquidations)",
    )
    p.add_argument("--debug", action="store_true")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    log_base = args.log_dir
    log_base.mkdir(parents=True, exist_ok=True)
    print(f"[{fmt_now()}] Liquidation collector starting. Logs → {log_base.resolve()}")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    collect(log_base, debug=args.debug)

    print(f"[{fmt_now()}] Collector stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

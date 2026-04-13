#!/usr/bin/env python3
"""
BTCUSDT perpetual order book depth collector via Binance WebSocket.

Connects to the partial depth stream (top 20 levels) and logs a snapshot
every 5 seconds to a daily JSONL file.

Usage:
  python -m cex_data_feed.scripts.collect_btc_depth \
    --log-dir data/btc_depth [--sample-interval 5]

Output: daily JSONL files, e.g. data/btc_depth/depth_2026-04-13.jsonl
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

WS_URI = "wss://fstream.binance.com/ws/btcusdt@depth20@500ms"
DEFAULT_SAMPLE_S = 5.0

_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    print(f"\n[{fmt_now()}] Caught {signal.Signals(sig).name}, shutting down...")
    _shutdown = True


def fmt_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def get_log_file(base: Path) -> Path:
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return base / f"depth_{date_str}.jsonl"


def append_event(base: Path, event: dict) -> Path:
    fp = get_log_file(base)
    with open(fp, "a") as f:
        f.write(json.dumps(event, separators=(",", ":")) + "\n")
    return fp


def collect(log_base: Path, sample_interval: float) -> None:
    delay = 5
    total_snapshots = 0
    last_store_s: float = 0

    while not _shutdown:
        ws = None
        try:
            print(f"[{fmt_now()}] Connecting to {WS_URI}")
            ws = websocket.create_connection(WS_URI, timeout=10)
            print(f"[{fmt_now()}] Connected. Sampling every {sample_interval}s...")
            delay = 5

            while not _shutdown:
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue

                now_s = time.time()
                if now_s - last_store_s < sample_interval:
                    continue

                data = json.loads(raw)
                snapshot = {
                    "ts_ms": int(now_s * 1000),
                    "E": data.get("E"),
                    "bids": data.get("b", []),
                    "asks": data.get("a", []),
                }

                fp = append_event(log_base, snapshot)
                total_snapshots += 1
                last_store_s = now_s

                # Summary: best bid/ask
                bids = snapshot["bids"]
                asks = snapshot["asks"]
                best_bid = bids[0][0] if bids else "?"
                best_ask = asks[0][0] if asks else "?"
                print(
                    f"[{fmt_now()}] #{total_snapshots} "
                    f"bid={best_bid} ask={best_ask} "
                    f"levels={len(bids)}/{len(asks)} → {fp.name}"
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
            delay = min(delay * 2, 60)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="BTCUSDT depth collector (top 20 levels → JSONL)",
    )
    p.add_argument(
        "--log-dir", type=Path, default=Path("data/btc_depth"),
        help="Directory for daily JSONL log files (default: data/btc_depth)",
    )
    p.add_argument(
        "--sample-interval", type=float, default=DEFAULT_SAMPLE_S,
        help=f"Seconds between logged snapshots (default: {DEFAULT_SAMPLE_S})",
    )
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    log_base = args.log_dir
    log_base.mkdir(parents=True, exist_ok=True)
    print(f"[{fmt_now()}] BTC depth collector starting")
    print(f"  Logs → {log_base.resolve()}")
    print(f"  Sample interval: {args.sample_interval}s")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    collect(log_base, args.sample_interval)

    print(f"[{fmt_now()}] Collector stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

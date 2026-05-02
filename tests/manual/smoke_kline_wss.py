"""
Smoke test for Binance kline WSS — baseline read of the endpoint used by
signal_stream_v3 / signal_stream_v3_directional.

Mirrors the production connect call exactly (no settimeout), reads for a
fixed duration, prints per-message annotation + summary stats.

Run:
  /Users/noel/projects/venvs/production/bin/python tests/manual/smoke_kline_wss.py
"""
from __future__ import annotations

import argparse
import json
import statistics
import time
from datetime import datetime, timezone

import websocket as ws_client

KLINE_WS = "wss://fstream.binance.com/ws/btcusdt@kline_1m"


def stream_url(stream: str, route: str = "ws") -> str:
    """route is the path prefix before the stream name.
    Legacy: "ws" (decommissioned 2026-04-23 for /market and /private streams).
    New:    "market/ws" for kline/aggTrade/etc, "public/ws" for depth/bookTicker.
    """
    return f"wss://fstream.binance.com/{route}/{stream}"


def fmt_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def run(duration_s: int, stream: str = "btcusdt@kline_1m", route: str = "ws") -> None:
    url = stream_url(stream, route)
    print(f"Connecting to {url} ...")
    t_connect_start = time.monotonic()
    ws = ws_client.create_connection(url, timeout=10)
    print(f"Connected in {(time.monotonic() - t_connect_start) * 1000:.0f}ms")
    print(f"Reading for {duration_s}s. Columns: recv_ts | kline_t/event | x | close/price | gap_ms")
    print("-" * 80)

    gaps_ms: list[float] = []
    closed_bars = 0
    total_msgs = 0
    timeouts = 0
    last_recv_mono: float | None = None

    deadline = time.monotonic() + duration_s
    try:
        while time.monotonic() < deadline:
            try:
                raw = ws.recv()
            except ws_client.WebSocketTimeoutException:
                timeouts += 1
                print(f"{fmt_ts(int(time.time() * 1000))} | <recv timeout #{timeouts}>")
                continue
            now_mono = time.monotonic()
            now_wall = time.time()
            gap_ms = (now_mono - last_recv_mono) * 1000 if last_recv_mono else 0.0
            last_recv_mono = now_mono

            obj = json.loads(raw)
            # kline shape: {"e":"kline","k":{...}}; aggTrade: {"e":"aggTrade","p":...,"q":...}
            if "k" in obj:
                k = obj["k"]
                x_flag = k.get("x", False)
                price = k.get("c", "?")
                event_t = int(k.get("t", 0))
                event_label = "kline " + fmt_ts(event_t)
            else:
                x_flag = False
                price = obj.get("p", "?")
                event_t = int(obj.get("E", 0))
                event_label = obj.get("e", "?") + " " + fmt_ts(event_t)

            total_msgs += 1
            if total_msgs > 1:
                gaps_ms.append(gap_ms)
            if x_flag:
                closed_bars += 1

            marker = "  X CLOSED" if x_flag else ""
            print(
                f"{fmt_ts(int(now_wall * 1000))} | {event_label} | "
                f"{str(x_flag):5s} | {price:>10s} | {gap_ms:7.1f}{marker}"
            )
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        try:
            ws.close()
        except Exception:
            pass

    print("-" * 80)
    print(f"Total messages: {total_msgs}  |  recv timeouts: {timeouts}  |  closed bars: {closed_bars}")
    if gaps_ms:
        gaps_sorted = sorted(gaps_ms)
        p = lambda q: gaps_sorted[min(int(len(gaps_sorted) * q), len(gaps_sorted) - 1)]
        print(
            f"Recv gap ms — min={min(gaps_ms):.1f} "
            f"p50={statistics.median(gaps_ms):.1f} "
            f"p95={p(0.95):.1f} "
            f"p99={p(0.99):.1f} "
            f"max={max(gaps_ms):.1f}"
        )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=120)
    ap.add_argument("--stream", default="btcusdt@kline_1m",
                   help="Binance fstream name, e.g. btcusdt@kline_1m or btcusdt@aggTrade")
    ap.add_argument("--route", default="ws",
                   help="Path prefix: 'ws' (legacy), 'market/ws' (kline/aggTrade), 'public/ws' (depth)")
    args = ap.parse_args()
    run(args.seconds, args.stream, args.route)

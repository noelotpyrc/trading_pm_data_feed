"""
Phase 1: Live smoke test.

Pulls warmup data from VPS over SSH, connects to Binance WS, runs feature
computation on each candle close, logs results, and prints a summary on Ctrl+C.

Runs indefinitely by default. Use --candles N to stop after N candles.

Usage:
    python -m btcusdt_perp_signal.tests.test_live_smoke
    python -m btcusdt_perp_signal.tests.test_live_smoke --candles 10
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import websocket

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from utils.local import fetch_warmup_bars
from btcusdt_perp_signal.features import compute_features, check_signal
from btcusdt_perp_signal.signal_engine import HISTORY_BARS, BINANCE_WS_URL, FEATURE_COLS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)


def _print_summary(results: list[dict]) -> None:
    total = len(results)
    if total == 0:
        print("\nNo candles processed.")
        return

    nan_count = sum(1 for r in results if r["nan_features"])
    error_count = sum(1 for r in results if r["error"])
    signal_count = sum(1 for r in results if r["signal"])

    print(f"\n{'=' * 60}")
    print(f"  Smoke Test Summary")
    print(f"{'=' * 60}")
    print(f"  Candles processed:  {total}")
    print(f"  Time range:         {results[0]['timestamp']} → {results[-1]['timestamp']}")
    print(f"  Errors:             {error_count}")
    print(f"  Rows with NaN:      {nan_count}")
    print(f"  Signals fired:      {signal_count}")

    if signal_count > 0:
        print(f"\n  Signals:")
        for r in results:
            if r["signal"]:
                print(f"    [{r['timestamp']}] {r['signal'].upper()}")

    if error_count > 0:
        print(f"\n  Errors:")
        for r in results:
            if r["error"]:
                print(f"    [{r['timestamp']}] {r['error']}")

    if nan_count == 0 and error_count == 0:
        print(f"\n  PASS")
    else:
        print(f"\n  ISSUES FOUND — review above")


def main() -> None:
    parser = argparse.ArgumentParser(description="Live smoke test")
    parser.add_argument("--candles", type=int, default=0,
                        help="Stop after N candles (0 = run until Ctrl+C)")
    args = parser.parse_args()

    # Pull warmup data from VPS
    log.info("Fetching %d warmup bars from VPS ...", HISTORY_BARS)
    csv_text = fetch_warmup_bars(HISTORY_BARS)
    buffer = pd.read_csv(io.StringIO(csv_text))
    buffer["timestamp"] = pd.to_datetime(buffer["timestamp"])
    log.info("Buffer loaded: %d rows (%s → %s)",
             len(buffer), buffer["timestamp"].iloc[0], buffer["timestamp"].iloc[-1])

    if len(buffer) < 1440:
        log.error("Not enough history (%d rows, need >=1440)", len(buffer))
        sys.exit(1)

    candle_count = 0
    results = []

    def on_message(ws, message):
        nonlocal candle_count, buffer
        data = json.loads(message)
        kline = data.get("k", {})
        if not kline or not kline.get("x", False):
            return

        ts_ms = kline["t"]
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

        result = {
            "timestamp": ts_str,
            "signal": None,
            "buffer_len": 0,
            "nan_features": [],
            "error": None,
        }

        try:
            new_row = {
                "timestamp": pd.Timestamp(ts_str),
                "open": float(kline["o"]),
                "high": float(kline["h"]),
                "low": float(kline["l"]),
                "close": float(kline["c"]),
                "volume": float(kline["v"]),
                "num_trades": int(kline["n"]),
            }

            # Append to buffer
            fresh = pd.DataFrame([new_row])
            buffer = buffer[buffer["timestamp"] != new_row["timestamp"]]
            buffer = pd.concat([buffer, fresh], ignore_index=True)
            if len(buffer) > HISTORY_BARS:
                buffer = buffer.iloc[-HISTORY_BARS:].reset_index(drop=True)

            # Compute features
            t0 = time.monotonic()
            df = compute_features(buffer.copy())
            elapsed_ms = (time.monotonic() - t0) * 1000
            latest = df.iloc[-1]

            # Check for NaN in key features
            nan_features = [c for c in FEATURE_COLS if pd.isna(latest.get(c))]

            direction = check_signal(latest)

            result["signal"] = direction
            result["buffer_len"] = len(buffer)
            result["nan_features"] = nan_features

            feat_summary = {c: round(float(latest[c]), 4) for c in FEATURE_COLS
                           if not pd.isna(latest.get(c))}

            log.info("[%s] signal=%-5s  buffer=%d  compute=%.0fms  nan=%s  feats=%s",
                     ts_str, direction or "none", len(buffer), elapsed_ms,
                     nan_features or "none", feat_summary)

        except Exception as e:
            result["error"] = str(e)
            log.exception("[%s] Error processing candle", ts_str)

        results.append(result)
        candle_count += 1

        if args.candles > 0 and candle_count >= args.candles:
            ws.close()

    def on_open(ws):
        mode = f"for {args.candles} candles" if args.candles > 0 else "until Ctrl+C"
        log.info("Connected to %s, running %s", BINANCE_WS_URL, mode)

    def on_error(ws, error):
        log.error("WS error: %s", error)

    def on_close(ws, status, msg):
        log.warning("WS closed: status=%s msg=%s", status, msg)

    # Graceful shutdown on Ctrl+C
    ws_ref = [None]

    def _shutdown(signum, frame):
        log.info("Shutting down...")
        if ws_ref[0]:
            ws_ref[0].close()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    ws = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_message=on_message,
        on_open=on_open,
        on_error=on_error,
        on_close=on_close,
    )
    ws_ref[0] = ws
    ws.run_forever(ping_interval=30, ping_timeout=10)

    _print_summary(results)


if __name__ == "__main__":
    main()

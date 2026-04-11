"""
E2E integration test: WS candle close → feature computation → signal check → Discord alert.

Temporarily lowers all thresholds so the next candle close triggers a signal,
then verifies the full path works end-to-end. Restores original thresholds on exit.

Usage (VPS):
    .venv/bin/python -m btcusdt_perp_signal.tests.test_e2e_alert
"""
from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import websocket

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from cex_data_feed.pipeline_1m.sqlite_db import read_last_n
from btcusdt_perp_signal.features import (
    compute_features, check_signal, COMMON_FILTERS, LONG_FILTERS, SHORT_FILTERS,
)
from btcusdt_perp_signal.signal_engine import HISTORY_BARS, BINANCE_WS_URL, FEATURE_COLS
from btcusdt_perp_signal.alert import send_discord, format_signal_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

# Thresholds that will always pass — every feature satisfies these
PASS_ALL_COMMON = {k: (">=", -999.0) for k in COMMON_FILTERS}
PASS_ALL_LONG = {k: (">=", -999.0) for k in LONG_FILTERS}
# Make short impossible so we always get long
BLOCK_SHORT = {k: ("<=", -999.0) for k in SHORT_FILTERS}


def main() -> None:
    db_path = ROOT / "data" / "btcusdt_perp_1m.sqlite"
    if not db_path.exists():
        log.error("DB not found: %s", db_path)
        sys.exit(1)

    log.info("Loading %d warmup bars ...", HISTORY_BARS)
    buffer = read_last_n(db_path, HISTORY_BARS)
    log.info("Buffer: %d rows, latest=%s", len(buffer), buffer["timestamp"].iloc[-1])

    if len(buffer) < 1440:
        log.error("Not enough history")
        sys.exit(1)

    result = {"passed": False, "timestamp": None, "direction": None, "discord_sent": False}

    def on_message(ws, message):
        data = json.loads(message)
        kline = data.get("k", {})
        if not kline or not kline.get("x", False):
            return

        ts_ms = kline["t"]
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

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
        nonlocal buffer
        fresh = pd.DataFrame([new_row])
        buffer = buffer[buffer["timestamp"] != new_row["timestamp"]]
        buffer = pd.concat([buffer, fresh], ignore_index=True)
        if len(buffer) > HISTORY_BARS:
            buffer = buffer.iloc[-HISTORY_BARS:].reset_index(drop=True)

        # Compute features
        df = compute_features(buffer.copy())
        latest = df.iloc[-1]

        feat_vals = {}
        for col in FEATURE_COLS:
            val = latest.get(col)
            feat_vals[col] = round(float(val), 6) if val is not None and not np.isnan(val) else None

        # Check signal with lowered thresholds
        with patch.dict("btcusdt_perp_signal.features.COMMON_FILTERS", PASS_ALL_COMMON), \
             patch.dict("btcusdt_perp_signal.features.LONG_FILTERS", PASS_ALL_LONG), \
             patch.dict("btcusdt_perp_signal.features.SHORT_FILTERS", BLOCK_SHORT):
            direction = check_signal(latest)

        log.info("[%s] signal=%s  feats=%s", ts_str, direction, feat_vals)

        if direction is None:
            log.error("Signal should have fired with lowered thresholds — something is wrong")
            ws.close()
            return

        # Send Discord alert (marked as test)
        msg = format_signal_message(ts_str, direction, feat_vals)
        msg = f"🧪 **[E2E TEST]**\n{msg}"
        discord_ok = send_discord(msg)

        result["passed"] = direction is not None and discord_ok
        result["timestamp"] = ts_str
        result["direction"] = direction
        result["discord_sent"] = discord_ok

        log.info("Discord sent: %s", discord_ok)
        ws.close()

    def on_open(ws):
        log.info("Connected, waiting for next candle close ...")

    def on_error(ws, error):
        log.error("WS error: %s", error)

    ws = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_message=on_message,
        on_open=on_open,
        on_error=on_error,
    )
    ws.run_forever(ping_interval=30, ping_timeout=10)

    # Summary
    print(f"\n{'=' * 50}")
    print(f"  E2E Alert Test")
    print(f"{'=' * 50}")
    print(f"  Candle:       {result['timestamp']}")
    print(f"  Signal:       {result['direction']}")
    print(f"  Discord sent: {result['discord_sent']}")
    print(f"  Result:       {'PASS' if result['passed'] else 'FAIL'}")

    sys.exit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()

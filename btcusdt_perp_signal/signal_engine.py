"""
WebSocket-based signal engine for BTCUSDT perp strategy.

Connects to Binance kline_1m stream, computes features on each candle close,
and fires alerts when thresholds are met.

Uses an in-memory rolling buffer: loads history from DB once on startup
(and on reconnect), then appends each new candle from WS and trims to
HISTORY_BARS length.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import websocket

from cex_data_feed.pipeline_1m.sqlite_db import read_last_n
from btcusdt_perp_signal.features import compute_features, check_signal
from btcusdt_perp_signal.signal_db import ensure_tables, insert_feature_log, insert_signal, mark_alerted
from btcusdt_perp_signal.alert import send_telegram, format_signal_message

log = logging.getLogger(__name__)

# parkinson_1440 is the largest rolling window; 1800 gives comfortable padding
HISTORY_BARS = 1800
BINANCE_WS_URL = "wss://fstream.binance.com/ws/btcusdt@kline_1m"

# Feature columns to record with each signal
FEATURE_COLS = [
    "parkinson_ratio", "volume_ratio_30", "volume_ratio_60",
    "avg_trade_size_zscore_60", "vwap_60_cross_rate_90",
    "cum_return_5bar_norm_p30", "efficiency_ratio_360",
    "parkinson_30",
]


class SignalEngine:
    def __init__(self, db_path: Path, signals_db_path: Path | None = None):
        self.db_path = Path(db_path)
        self.signals_db_path = Path(signals_db_path) if signals_db_path else self.db_path
        ensure_tables(self.signals_db_path)
        self._ws = None
        self._buffer: pd.DataFrame | None = None  # rolling in-memory buffer

    def _load_history(self) -> None:
        """Load recent candles from DB into the in-memory buffer."""
        self._buffer = read_last_n(self.db_path, HISTORY_BARS)
        log.info("Loaded %d history bars into buffer (latest: %s)",
                 len(self._buffer),
                 self._buffer["timestamp"].iloc[-1] if len(self._buffer) else "N/A")

    def _append_candle(self, new_row: dict) -> pd.DataFrame:
        """Append a new candle to the buffer, trim to HISTORY_BARS, return full df."""
        fresh = pd.DataFrame([new_row])

        if self._buffer is not None and len(self._buffer) > 0:
            # Drop if buffer already has this timestamp (e.g. cron inserted it)
            self._buffer = self._buffer[
                self._buffer["timestamp"] != new_row["timestamp"]
            ]
            self._buffer = pd.concat([self._buffer, fresh], ignore_index=True)
        else:
            self._buffer = fresh

        # Trim oldest rows to keep buffer bounded
        if len(self._buffer) > HISTORY_BARS:
            self._buffer = self._buffer.iloc[-HISTORY_BARS:].reset_index(drop=True)

        return self._buffer.copy()

    def _process_candle_close(self, candle: dict) -> None:
        """Called when a 1m candle closes. Compute features and check signal."""
        ts_ms = candle["t"]
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

        new_row = {
            "timestamp": pd.Timestamp(ts_str),
            "open": float(candle["o"]),
            "high": float(candle["h"]),
            "low": float(candle["l"]),
            "close": float(candle["c"]),
            "volume": float(candle["v"]),
            "num_trades": int(candle["n"]),
        }

        # Append to rolling buffer (no DB read)
        df = self._append_candle(new_row)

        # Compute features
        df = compute_features(df)
        latest = df.iloc[-1]

        # Extract feature values
        feat_vals = {}
        for col in FEATURE_COLS:
            val = latest.get(col)
            feat_vals[col] = round(float(val), 6) if val is not None and not np.isnan(val) else None

        # Check signal
        direction = check_signal(latest)

        # Persist every candle to feature log
        ohlcv = {
            "open": new_row["open"],
            "high": new_row["high"],
            "low": new_row["low"],
            "close": new_row["close"],
            "volume": new_row["volume"],
        }
        insert_feature_log(self.signals_db_path, ts_str, ohlcv, feat_vals, direction)

        if direction is None:
            log.info("[%s] No signal  buffer=%d  feats=%s", ts_str, len(self._buffer), feat_vals)
            return

        log.info("[%s] *** %s SIGNAL ***  feats=%s", ts_str, direction.upper(), feat_vals)

        # Persist signal (separate table for quick lookup)
        row_id = insert_signal(self.signals_db_path, ts_str, direction, feat_vals)

        # Send Telegram alert
        msg = format_signal_message(ts_str, direction, feat_vals)
        if send_telegram(msg):
            mark_alerted(self.signals_db_path, row_id)

    def _on_message(self, ws, message: str) -> None:
        data = json.loads(message)
        kline = data.get("k", {})
        if not kline:
            return
        # Only process on candle close
        if kline.get("x", False):
            try:
                self._process_candle_close(kline)
            except Exception:
                log.exception("Error processing candle close")

    def _on_error(self, ws, error) -> None:
        log.error("WebSocket error: %s", error)

    def _on_close(self, ws, close_status, close_msg) -> None:
        log.warning("WebSocket closed: status=%s msg=%s", close_status, close_msg)

    def _on_open(self, ws) -> None:
        log.info("WebSocket connected to %s", BINANCE_WS_URL)
        # Load history from DB on each (re)connect to seed the buffer
        self._load_history()

    def run(self) -> None:
        """Run the signal engine with auto-reconnect."""
        log.info("Starting signal engine (db=%s)", self.db_path)

        while True:
            try:
                self._ws = websocket.WebSocketApp(
                    BINANCE_WS_URL,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    on_open=self._on_open,
                )
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception:
                log.exception("WebSocket run_forever raised")

            log.info("Reconnecting in 5 seconds...")
            time.sleep(5)

    def stop(self) -> None:
        if self._ws:
            self._ws.close()

"""
SQLite persistence for signal engine.

Two tables:
  - feature_log: every candle close with computed feature values and signal result
  - signals: only rows where a signal fired (subset of feature_log)
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


SIGNALS_TABLE = "signals_btcusdt_perp"
FEATURE_LOG_TABLE = "feature_log_btcusdt_perp"


def ensure_tables(db_path: Path) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {SIGNALS_TABLE} (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT NOT NULL,
                direction   TEXT NOT NULL,
                features    TEXT,
                alerted     INTEGER DEFAULT 0,
                created_at  TEXT NOT NULL
            );
        """)
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{SIGNALS_TABLE}_ts ON {SIGNALS_TABLE}(timestamp);"
        )
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {FEATURE_LOG_TABLE} (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT NOT NULL,
                open        REAL,
                high        REAL,
                low         REAL,
                close       REAL,
                volume      REAL,
                features    TEXT,
                signal      TEXT,
                created_at  TEXT NOT NULL
            );
        """)
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{FEATURE_LOG_TABLE}_ts ON {FEATURE_LOG_TABLE}(timestamp);"
        )
        con.commit()
    finally:
        con.close()


# Keep old name working
ensure_signals_table = ensure_tables


def insert_feature_log(db_path: Path, timestamp: str, ohlcv: dict,
                       features: dict, signal: str | None) -> int:
    """Insert a feature log row for every candle close. Returns the row id."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    con = sqlite3.connect(str(db_path))
    try:
        cur = con.execute(
            f"""
            INSERT INTO {FEATURE_LOG_TABLE}
              (timestamp, open, high, low, close, volume, features, signal, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (timestamp, ohlcv.get("open"), ohlcv.get("high"), ohlcv.get("low"),
             ohlcv.get("close"), ohlcv.get("volume"),
             json.dumps(features), signal, now),
        )
        con.commit()
        return cur.lastrowid
    finally:
        con.close()


def insert_signal(db_path: Path, timestamp: str, direction: str,
                  features: dict) -> int:
    """Insert a signal row. Returns the row id."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    con = sqlite3.connect(str(db_path))
    try:
        cur = con.execute(
            f"""
            INSERT INTO {SIGNALS_TABLE} (timestamp, direction, features, alerted, created_at)
            VALUES (?, ?, ?, 0, ?)
            """,
            (timestamp, direction, json.dumps(features), now),
        )
        con.commit()
        return cur.lastrowid
    finally:
        con.close()


def mark_alerted(db_path: Path, row_id: int) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            f"UPDATE {SIGNALS_TABLE} SET alerted = 1 WHERE id = ?", (row_id,)
        )
        con.commit()
    finally:
        con.close()

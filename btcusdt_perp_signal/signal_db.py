"""
SQLite persistence for signal events.

Writes fired signals to a `signals` table in the same DB as OHLCV data
(or a separate DB if preferred).
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


SIGNALS_TABLE = "signals_btcusdt_perp"


def ensure_signals_table(db_path: Path) -> None:
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
        con.commit()
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

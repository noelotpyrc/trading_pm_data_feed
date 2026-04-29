"""
SQLite persistence layer for the 1m BTC-USD Coinbase accumulator.

Slim schema (Coinbase candles only expose OHLCV — no trade count, no taker
breakdown, no quote-asset volume). Mirrors pipeline_1m/sqlite_db.py otherwise.

Table: ohlcv_btcusd_coinbase_1m
  id          INTEGER PRIMARY KEY AUTOINCREMENT
  timestamp   TEXT     (e.g. "2024-01-15 12:34:00", UTC, no tz)
  open        REAL
  high        REAL
  low         REAL
  close       REAL
  volume      REAL
  ingested_at TEXT

Multiple rows per timestamp are allowed (data corrections).
Consumers should dedup by taking the latest ingested_at per timestamp.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


TABLE = "ohlcv_btcusd_coinbase_1m"


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def ensure_table(db_path: Path) -> None:
    con = _connect(db_path)
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT NOT NULL,
                open        REAL NOT NULL,
                high        REAL NOT NULL,
                low         REAL NOT NULL,
                close       REAL NOT NULL,
                volume      REAL NOT NULL,
                ingested_at TEXT NOT NULL
            );
            """
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_ts ON {TABLE}(timestamp);"
        )
        con.commit()
    finally:
        con.close()


def _ts_to_str(ts) -> str:
    if isinstance(ts, str):
        return ts
    if isinstance(ts, pd.Timestamp):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def insert_candles(db_path: Path, df: pd.DataFrame) -> int:
    """Bulk-insert closed candles. Required cols: timestamp, open, high, low, close, volume."""
    if df.empty:
        return 0

    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for _, row in df.iterrows():
        rows.append(
            (
                _ts_to_str(row["timestamp"]),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]),
                ingested_at,
            )
        )

    con = _connect(db_path)
    try:
        con.executemany(
            f"""
            INSERT INTO {TABLE}
              (timestamp, open, high, low, close, volume, ingested_at)
            VALUES (?,?,?,?,?,?,?)
            """,
            rows,
        )
        con.commit()
        return len(rows)
    finally:
        con.close()


def read_last_n(db_path: Path, n: int) -> pd.DataFrame:
    """Return N most recent closed candles, ascending. Dedup on timestamp (latest ingested_at wins)."""
    con = _connect(db_path)
    try:
        rows = con.execute(
            f"""
            SELECT timestamp, open, high, low, close, volume
            FROM {TABLE}
            WHERE id IN (
                SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (
                        PARTITION BY timestamp ORDER BY ingested_at DESC, id DESC
                    ) AS rn
                    FROM {TABLE}
                )
                WHERE rn = 1
            )
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (n,),
        ).fetchall()
    finally:
        con.close()

    if not rows:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )

    df = pd.DataFrame(
        rows,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def find_first_gap(db_path: Path) -> Optional[pd.Timestamp]:
    """Find the first missing 1-minute candle in the DB, or None if continuous."""
    con = _connect(db_path)
    try:
        row = con.execute(
            f"""
            SELECT datetime(t.timestamp, '+1 minute') AS gap_start
            FROM (SELECT DISTINCT timestamp FROM {TABLE}) t
            WHERE NOT EXISTS (
                SELECT 1 FROM {TABLE} t2
                WHERE t2.timestamp = datetime(t.timestamp, '+1 minute')
            )
            AND t.timestamp < (SELECT MAX(timestamp) FROM {TABLE})
            ORDER BY t.timestamp ASC
            LIMIT 1
            """
        ).fetchone()
    finally:
        con.close()

    if row is None:
        return None
    return pd.Timestamp(row[0])


def coverage_stats(db_path: Path) -> Optional[Tuple[pd.Timestamp, pd.Timestamp, int]]:
    """Return (min_ts, max_ts, distinct_count) or None if empty."""
    con = _connect(db_path)
    try:
        row = con.execute(
            f"SELECT MIN(timestamp), MAX(timestamp), COUNT(DISTINCT timestamp) FROM {TABLE}"
        ).fetchone()
    finally:
        con.close()

    if row is None or row[0] is None:
        return None
    return pd.Timestamp(row[0]), pd.Timestamp(row[1]), int(row[2])

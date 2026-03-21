"""
SQLite persistence layer for the 1m BTCUSDT perp accumulator.

Uses Python stdlib sqlite3 only — no extra dependencies.
All timestamps stored as ISO-format strings (UTC, no timezone suffix).

Table: ohlcv_btcusdt_1m
  timestamp TEXT PRIMARY KEY  (e.g. "2024-01-15 12:34:00")
  open      REAL
  high      REAL
  low       REAL
  close     REAL
  volume    REAL
  quote_asset_volume      REAL
  num_trades              INTEGER
  taker_buy_base_volume   REAL
  taker_buy_quote_volume  REAL
  ingested_at TEXT
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


TABLE = "ohlcv_btcusdt_1m"

# All OHLCV columns we care about (excluding PK and ingested_at)
_DATA_COLS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_asset_volume",
    "num_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
]


def _connect(db_path: Path) -> sqlite3.Connection:
    """Open (and create) the SQLite database with WAL mode for safe concurrent reads."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def ensure_table(db_path: Path) -> None:
    """Create the OHLCV table and unique index if they do not yet exist."""
    con = _connect(db_path)
    try:
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                timestamp               TEXT PRIMARY KEY,
                open                    REAL NOT NULL,
                high                    REAL NOT NULL,
                low                     REAL NOT NULL,
                close                   REAL NOT NULL,
                volume                  REAL NOT NULL,
                quote_asset_volume      REAL,
                num_trades              INTEGER,
                taker_buy_base_volume   REAL,
                taker_buy_quote_volume  REAL,
                ingested_at             TEXT NOT NULL
            );
            """
        )
        # Belt-and-suspenders: explicit unique index (PK already implies one)
        con.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE}_ts ON {TABLE}(timestamp);"
        )
        con.commit()
    finally:
        con.close()


def _ts_to_str(ts) -> str:
    """Normalise any timestamp-like value to 'YYYY-MM-DD HH:MM:SS' string (UTC, no tz)."""
    if isinstance(ts, str):
        return ts
    if isinstance(ts, pd.Timestamp):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    # Fallback: let pandas parse it
    return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def upsert_candles(db_path: Path, df: pd.DataFrame) -> int:
    """Bulk-insert closed candles from a DataFrame; silently skips duplicates.

    DataFrame must contain at least: timestamp, open, high, low, close, volume.
    Optional columns: quote_asset_volume, num_trades, taker_buy_base_volume,
                      taker_buy_quote_volume.

    Returns the number of rows actually inserted (not skipped).
    """
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
                float(row["quote_asset_volume"]) if "quote_asset_volume" in row and pd.notna(row.get("quote_asset_volume")) else None,
                int(row["num_trades"]) if "num_trades" in row and pd.notna(row.get("num_trades")) else None,
                float(row["taker_buy_base_volume"]) if "taker_buy_base_volume" in row and pd.notna(row.get("taker_buy_base_volume")) else None,
                float(row["taker_buy_quote_volume"]) if "taker_buy_quote_volume" in row and pd.notna(row.get("taker_buy_quote_volume")) else None,
                ingested_at,
            )
        )

    con = _connect(db_path)
    try:
        before = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        con.executemany(
            f"""
            INSERT OR IGNORE INTO {TABLE}
              (timestamp, open, high, low, close, volume,
               quote_asset_volume, num_trades,
               taker_buy_base_volume, taker_buy_quote_volume,
               ingested_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
        con.commit()
        after = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        return after - before
    finally:
        con.close()


def read_last_n(db_path: Path, n: int) -> pd.DataFrame:
    """Return the N most recent closed candles, sorted ascending by timestamp."""
    con = _connect(db_path)
    try:
        rows = con.execute(
            f"""
            SELECT timestamp, open, high, low, close, volume,
                   quote_asset_volume, num_trades,
                   taker_buy_base_volume, taker_buy_quote_volume
            FROM {TABLE}
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (n,),
        ).fetchall()
    finally:
        con.close()

    if not rows:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume",
                     "quote_asset_volume", "num_trades",
                     "taker_buy_base_volume", "taker_buy_quote_volume"]
        )

    df = pd.DataFrame(
        rows,
        columns=["timestamp", "open", "high", "low", "close", "volume",
                 "quote_asset_volume", "num_trades",
                 "taker_buy_base_volume", "taker_buy_quote_volume"],
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def coverage_stats(db_path: Path) -> Optional[Tuple[pd.Timestamp, pd.Timestamp, int]]:
    """Return (min_ts, max_ts, count) or None if the table is empty."""
    con = _connect(db_path)
    try:
        row = con.execute(
            f"SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM {TABLE}"
        ).fetchone()
    finally:
        con.close()

    if row is None or row[0] is None:
        return None
    return pd.Timestamp(row[0]), pd.Timestamp(row[1]), int(row[2])

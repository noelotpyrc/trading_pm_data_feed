"""Shared OHLCV data loading utilities for the artifact pipeline.

Handles reading from SQLite (with dedup) or CSV, mapping column names
to the internal `datetime_utc` convention.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

TABLE = "ohlcv_btcusdt_1m"


def load_ohlcv_window(
    db_path: Path,
    start: pd.Timestamp,
    end: pd.Timestamp,
    table: str = TABLE,
) -> pd.DataFrame:
    """Load deduped OHLCV from SQLite for [start, end).

    Deduplicates by taking the latest ingested_at per timestamp.
    Returns DataFrame with columns: datetime_utc, open, high, low, close, volume
    sorted ascending by datetime_utc.
    """
    con = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query(
            f"""
            SELECT timestamp AS datetime_utc, open, high, low, close, volume
            FROM {table}
            WHERE id IN (
                SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (
                        PARTITION BY timestamp ORDER BY ingested_at DESC, id DESC
                    ) AS rn
                    FROM {table}
                    WHERE timestamp >= ? AND timestamp < ?
                )
                WHERE rn = 1
            )
            ORDER BY timestamp
            """,
            con,
            params=[start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")],
            parse_dates=["datetime_utc"],
        )
    finally:
        con.close()
    return df


def load_csv_window(
    path: Path,
    start: pd.Timestamp,
    end: pd.Timestamp,
    usecols: list[str] | None = None,
    chunksize: int = 250_000,
) -> pd.DataFrame:
    """Load a window from a CSV file with a datetime_utc column."""
    frames: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunksize):
        chunk["datetime_utc"] = pd.to_datetime(chunk["datetime_utc"])
        window = chunk[(chunk["datetime_utc"] >= start) & (chunk["datetime_utc"] < end)]
        if not window.empty:
            frames.append(window)
    if not frames:
        return pd.DataFrame(columns=usecols or [])
    return pd.concat(frames, ignore_index=True)


def check_data_quality(
    df: pd.DataFrame,
    expected_start: pd.Timestamp,
    expected_end: pd.Timestamp,
    label: str = "",
) -> None:
    """Log warnings about missing data in the OHLCV window.

    Checks for:
    - Missing candles vs expected count
    - Largest contiguous gap
    """
    if df.empty:
        print(f"[WARN]{' ' + label + ':' if label else ''} no data in window "
              f"{expected_start} .. {expected_end}")
        return

    expected_minutes = int((expected_end - expected_start).total_seconds() / 60)
    actual_minutes = len(df)
    missing = expected_minutes - actual_minutes

    if missing > 0:
        pct = missing / expected_minutes * 100
        print(f"[WARN]{' ' + label + ':' if label else ''} "
              f"missing {missing} of {expected_minutes} expected 1m candles ({pct:.1f}%)")

    # Find largest contiguous gap
    if len(df) >= 2:
        diffs = df["datetime_utc"].diff().dt.total_seconds() / 60
        max_gap_minutes = diffs.max()
        if max_gap_minutes > 1:
            gap_idx = diffs.idxmax()
            gap_at = df["datetime_utc"].iloc[gap_idx - 1] if gap_idx > 0 else df["datetime_utc"].iloc[0]
            print(f"[WARN]{' ' + label + ':' if label else ''} "
                  f"largest gap: {int(max_gap_minutes)} minutes at {gap_at}")

#!/usr/bin/env python3
"""
Phase 1: Import Binance historical 1m kline CSV into SQLite.

Reads the merged CSV produced by merge_binance_klines.py (which itself merges
the ZIPs downloaded by download_binance_monthly_klines.py /
download_binance_daily_klines.py).

Expected CSV columns (Binance kline format):
  open_time, open, high, low, close, volume, close_time,
  quote_asset_volume, number_of_trades,
  taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
  ignore, datetime_utc

Workflow:
  # 1. Download monthly ZIPs (reuse existing script)
  python -m cex_data_feed.scripts.download_binance_monthly_klines \\
    --symbol BTCUSDT --interval 1m --start 2024-01 --end 2024-12 \\
    --out /tmp/klines_1m

  # 2. (Optional) Download daily ZIPs for current partial month
  python -m cex_data_feed.scripts.download_binance_daily_klines \\
    --symbol BTCUSDT --interval 1m --start-date 2025-01-01 \\
    --out /tmp/klines_1m

  # 3. Merge ZIPs → single CSV (reuse existing script)
  python -m cex_data_feed.scripts.merge_binance_klines \\
    --in-dir /tmp/klines_1m --symbol BTCUSDT --interval 1m \\
    --out /tmp/BTCUSDT-1m-merged.csv

  # 4. Import to SQLite (this script)
  python -m cex_data_feed.scripts.backfill_1m_from_csv \\
    --csv /tmp/BTCUSDT-1m-merged.csv \\
    --db ~/data/btcusdt_perp_1m.sqlite \\
    [--chunk-size 50000] [--dry-run] [--debug]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Allow running as a script
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from cex_data_feed.pipeline_1m.sqlite_db import ensure_table, upsert_candles, coverage_stats


# Binance kline CSV column names (from merge_binance_klines.py)
_BINANCE_COLS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
    "datetime_utc",
]

# Chunk size for memory-efficient import of large CSVs (~43m rows for 1m/year)
DEFAULT_CHUNK_SIZE = 50_000


def _detect_has_header(csv_path: Path) -> bool:
    """Return True if the CSV has a header row."""
    with open(csv_path, "r") as f:
        first = f.readline().strip()
    return first.startswith("open_time") or any(c.isalpha() for c in first.split(",")[0])


def _read_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Normalise a raw Binance kline chunk into the format expected by upsert_candles."""
    # If the chunk came from a headerless CSV we assign column names
    if "open_time" not in chunk.columns:
        if len(chunk.columns) >= 12:
            chunk.columns = _BINANCE_COLS[: len(chunk.columns)]
        else:
            raise ValueError(
                f"Unexpected column count {len(chunk.columns)}. "
                "Expected Binance kline CSV (12 or 13 columns)."
            )

    # open_time is in milliseconds; convert to UTC datetime string
    df = pd.DataFrame()
    df["timestamp"] = pd.to_datetime(chunk["open_time"].astype(float), unit="ms", utc=True).dt.tz_convert(None)
    df["open"] = chunk["open"].astype(float)
    df["high"] = chunk["high"].astype(float)
    df["low"] = chunk["low"].astype(float)
    df["close"] = chunk["close"].astype(float)
    df["volume"] = chunk["volume"].astype(float)
    df["quote_asset_volume"] = chunk["quote_asset_volume"].astype(float)
    df["num_trades"] = chunk["number_of_trades"].astype(float).astype("Int64")
    df["taker_buy_base_volume"] = chunk["taker_buy_base_asset_volume"].astype(float)
    df["taker_buy_quote_volume"] = chunk["taker_buy_quote_asset_volume"].astype(float)
    return df


def run_backfill(
    csv_path: Path,
    db_path: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    dry_run: bool = False,
    debug: bool = False,
) -> int:
    """Import the merged CSV into SQLite. Returns 0 on success, non-zero on error."""
    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}", file=sys.stderr)
        return 1

    ensure_table(db_path)

    has_header = _detect_has_header(csv_path)
    if debug:
        print(f"[DEBUG] CSV has header: {has_header}")

    before = coverage_stats(db_path)
    if before:
        print(f"[INFO] DB before: min={before[0]}  max={before[1]}  rows={before[2]:,}")
    else:
        print("[INFO] DB is empty — bootstrapping")

    total_inserted = 0
    total_skipped = 0
    chunk_num = 0

    reader = pd.read_csv(
        csv_path,
        header=0 if has_header else None,
        chunksize=chunk_size,
        dtype=str,  # read raw; cast in _read_chunk
        on_bad_lines="skip",
    )

    for chunk in reader:
        chunk_num += 1
        try:
            df = _read_chunk(chunk)
        except Exception as e:
            print(f"[WARN] Chunk {chunk_num}: parse error — {e}", file=sys.stderr)
            continue

        if dry_run:
            if debug:
                print(f"[DRY-RUN] Would upsert chunk {chunk_num} ({len(df)} rows)")
            total_inserted += len(df)
            total_skipped += 0
            continue

        inserted = upsert_candles(db_path, df)
        skipped = len(df) - inserted
        total_inserted += inserted
        total_skipped += skipped

        if debug:
            ts_min = df["timestamp"].min()
            ts_max = df["timestamp"].max()
            print(
                f"[DEBUG] Chunk {chunk_num}: rows={len(df)}  inserted={inserted}  "
                f"skipped={skipped}  range=[{ts_min} .. {ts_max}]"
            )
        elif chunk_num % 20 == 0:
            print(f"[INFO] Processed chunk {chunk_num}  total_inserted={total_inserted:,}")

    after = coverage_stats(db_path)
    if dry_run:
        print(f"[DRY-RUN] Would insert ≈{total_inserted:,} rows")
    else:
        if after:
            print(
                f"[INFO] DB after:  min={after[0]}  max={after[1]}  rows={after[2]:,}"
            )
        print(
            f"[INFO] Done. chunks={chunk_num}  inserted={total_inserted:,}  "
            f"skipped(dup)={total_skipped:,}"
        )

    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill Binance 1m klines from merged CSV into SQLite (Phase 1)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--csv", type=Path, required=True,
                   help="Path to the merged Binance 1m kline CSV")
    p.add_argument("--db", type=Path, required=True,
                   help="Path to target SQLite file (created if absent)")
    p.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                   help=f"Rows per chunk (default: {DEFAULT_CHUNK_SIZE})")
    p.add_argument("--dry-run", action="store_true",
                   help="Parse and count rows but do not write to DB")
    p.add_argument("--debug", action="store_true",
                   help="Verbose per-chunk output")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    try:
        return run_backfill(
            csv_path=args.csv,
            db_path=args.db,
            chunk_size=args.chunk_size,
            dry_run=args.dry_run,
            debug=args.debug,
        )
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        raise


if __name__ == "__main__":
    raise SystemExit(main())

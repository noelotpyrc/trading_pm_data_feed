#!/usr/bin/env python3
"""
Unzip and merge Binance monthly/daily kline ZIPs into a single CSV.

Supports both Spot and Futures (USDT-M) klines. For Spot data from 2025+,
timestamps are automatically normalized from microseconds to milliseconds.

Assumes files are named like: {SYMBOL}-{INTERVAL}-YYYY-MM.zip (monthly)
or {SYMBOL}-{INTERVAL}-YYYY-MM-DD.zip (daily).

Usage examples:
  # Futures (default)
  python -m cex_data_feed.scripts.merge_binance_klines \
    --in-dir ./data/futures_klines --symbol BTCUSDT --interval 1h \
    --out ./data/BTCUSDT-1h-futures-merged.csv

  # Spot (with timestamp normalization)
  python -m cex_data_feed.scripts.merge_binance_klines \
    --market spot --in-dir ./data/spot_klines --symbol BTCUSDT --interval 1h \
    --out ./data/BTCUSDT-1h-spot-merged.csv

Notes:
- Binance Spot data from Jan 1, 2025 uses microseconds; this script normalizes
  them to milliseconds for consistency.
- A datetime_utc column is added for human-readable timestamps.
- Original kline CSVs generally have no header; this script adds one.
"""

from __future__ import annotations

import argparse
import glob
import io
import os
import zipfile
from datetime import datetime, timezone
from typing import Iterable, List, Optional


DEFAULT_INPUT_DIR = "/tmp/klines_1m"

# Standard Binance kline column names for CSV (we add datetime_utc at the end)
BINANCE_KLINE_HEADER = [
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
    "datetime_utc",  # Added: human-readable timestamp
]

# Timestamp boundaries for sanity checks (milliseconds)
# 2017-01-01 00:00:00 UTC = 1483228800000
# 2030-01-01 00:00:00 UTC = 1893456000000
TS_MIN_MS = 1483228800000
TS_MAX_MS = 1893456000000

# Threshold to distinguish microseconds from milliseconds
# Microseconds (16 digits) are > 10^15, milliseconds (13 digits) are < 10^14
MICROSECOND_THRESHOLD = 10**15


def normalize_timestamp(ts_str: str, market: str) -> tuple[int, bool]:
    """
    Normalize timestamp to milliseconds.

    Returns (normalized_ms, was_converted) tuple.
    For spot market, detects microseconds and converts to milliseconds.
    """
    try:
        ts = int(ts_str)
    except (ValueError, TypeError):
        # Return original if not parseable
        return int(ts_str) if ts_str.isdigit() else 0, False

    was_converted = False

    # Detect microseconds (> 10^15) and convert to milliseconds
    if ts > MICROSECOND_THRESHOLD:
        ts = ts // 1000
        was_converted = True

    # Sanity check
    if not (TS_MIN_MS <= ts <= TS_MAX_MS):
        print(f"[WARN] Timestamp {ts} outside expected range (2017-2030)")

    return ts, was_converted


def ms_to_datetime_utc(ts_ms: int) -> str:
    """Convert milliseconds timestamp to ISO format UTC datetime string."""
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError):
        return ""


def process_line(line: str, market: str) -> tuple[str, bool]:
    """
    Process a kline data line:
    - Normalize timestamps for spot market
    - Add datetime_utc column

    Returns (processed_line, had_microseconds).
    """
    parts = line.split(",")
    if len(parts) < 7:
        # Malformed line, return as-is with empty datetime
        return line + ",", False

    had_microseconds = False

    # Normalize open_time (index 0)
    open_time_ms, converted = normalize_timestamp(parts[0], market)
    parts[0] = str(open_time_ms)
    if converted:
        had_microseconds = True

    # Normalize close_time (index 6)
    close_time_ms, converted = normalize_timestamp(parts[6], market)
    parts[6] = str(close_time_ms)
    if converted:
        had_microseconds = True

    # Add datetime_utc column based on open_time
    datetime_utc = ms_to_datetime_utc(open_time_ms)
    parts.append(datetime_utc)

    return ",".join(parts), had_microseconds


def is_header_line(line: str) -> bool:
    """Check if line is a header (contains alphabetic chars in first field)."""
    s = line.strip().lower()
    if not s:
        return False
    if s.startswith("open_time,"):
        return True
    # If any alpha character exists in first field, assume header
    return any(c.isalpha() for c in s.split(",", 1)[0])


def iter_zip_csv_lines(zip_path: str) -> Iterable[str]:
    """Yield lines from the first CSV found in the ZIP."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            return
        name = names[0]
        with zf.open(name, "r") as f:
            wrapper = io.TextIOWrapper(f, encoding="utf-8-sig", newline="")
            for line in wrapper:
                if line and line.strip():
                    yield line.rstrip("\n\r")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Merge Binance kline ZIPs into one CSV (Spot or Futures)"
    )
    p.add_argument(
        "--market",
        choices=["spot", "futures"],
        default="futures",
        help="Market type: 'spot' or 'futures' (default: futures)",
    )
    p.add_argument(
        "--in-dir", default=DEFAULT_INPUT_DIR, help="Directory with ZIP files"
    )
    p.add_argument("--symbol", default="BTCUSDT", help="Symbol, e.g., BTCUSDT")
    p.add_argument("--interval", default="1h", help="Interval, e.g., 1h")
    p.add_argument(
        "--out", default=None, help="Output CSV path; default computed from symbol/interval"
    )
    p.add_argument(
        "--no-header", action="store_true", help="Do not write header to merged CSV"
    )
    p.add_argument(
        "--overwrite", action="store_true", help="Overwrite output if it exists"
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    pattern = os.path.join(args.in_dir, f"{args.symbol}-{args.interval}-*.zip")
    zip_paths = sorted(glob.glob(pattern))
    if not zip_paths:
        print(f"[ERROR] No ZIPs found matching: {pattern}")
        return 2

    out_path = (
        args.out
        if args.out
        else os.path.join(args.in_dir, f"{args.symbol}-{args.interval}-{args.market}-merged.csv")
    )

    if os.path.exists(out_path) and not args.overwrite:
        print(f"[ERROR] Output exists: {out_path}. Use --overwrite to replace.")
        return 2

    # Ensure output directory exists
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    print(f"[INFO] Market: {args.market}")
    print(f"[INFO] Found {len(zip_paths)} ZIP files to merge")

    total_rows = 0
    microsecond_rows = 0

    with open(out_path, "w", encoding="utf-8", newline="\n") as out_f:
        if not args.no_header:
            out_f.write(",".join(BINANCE_KLINE_HEADER) + "\n")

        for idx, zp in enumerate(zip_paths, start=1):
            base = os.path.basename(zp)
            print(f"[{idx}/{len(zip_paths)}] Merging {base}")

            for line in iter_zip_csv_lines(zp):
                if is_header_line(line):
                    continue

                processed_line, had_microseconds = process_line(line, args.market)
                out_f.write(processed_line + "\n")
                total_rows += 1
                if had_microseconds:
                    microsecond_rows += 1

    print(f"\n[INFO] Merge complete!")
    print(f"  - Output: {out_path}")
    print(f"  - Total rows: {total_rows:,}")
    if args.market == "spot" and microsecond_rows > 0:
        print(f"  - Rows with microseconds normalized: {microsecond_rows:,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

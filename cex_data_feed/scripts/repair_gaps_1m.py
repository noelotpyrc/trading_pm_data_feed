#!/usr/bin/env python3
"""
Repair gaps in the 1m BTCUSDT perp SQLite database.

Scans the DB for missing 1-minute candles, fetches them from Binance FAPI,
and inserts them. Run manually or on a less frequent schedule (e.g. daily).

Note: The gap scan is O(n) on the number of distinct timestamps, so avoid
running this on every cron tick — use accumulate_1m.py for that.

Usage:
  python -m cex_data_feed.scripts.repair_gaps_1m \\
    --db ~/data/btcusdt_perp_1m.sqlite \\
    [--symbol BTCUSDT] [--dry-run] [--debug]
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

# Allow running as a script
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from cex_data_feed.pipeline_1m.fetch import fetch_closed_1m_since, DEFAULT_SYMBOL
from cex_data_feed.pipeline_1m.sqlite_db import ensure_table, insert_candles, coverage_stats, find_first_gap


def run_once(
    db_path: Path,
    symbol: str = DEFAULT_SYMBOL,
    dry_run: bool = False,
    debug: bool = False,
) -> int:
    """Find gaps in the DB and fill them from Binance API. Returns exit code."""
    ensure_table(db_path)

    stats = coverage_stats(db_path)
    if stats is None:
        print("[ERROR] DB is empty — run backfill_1m first", file=sys.stderr)
        return 1

    if debug:
        print(f"[DEBUG] DB coverage: {stats[0]} .. {stats[1]}  total: {stats[2]:,}")

    now_utc = datetime.now(timezone.utc)
    now_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    now_ts = pd.Timestamp(now_utc).tz_convert(None).floor("min")

    # Check for internal gaps (missing minutes between min and max)
    gap_ts = find_first_gap(db_path)

    # Check for trailing gap (db_max is behind current time)
    db_max = stats[1]
    trailing_start = db_max + pd.Timedelta(minutes=1)
    has_trailing_gap = trailing_start < now_ts

    if gap_ts is None and not has_trailing_gap:
        print(f"[{now_str}] No gaps found — DB is up to date")
        return 0

    # Prefer internal gap; fall back to trailing gap
    fetch_from = gap_ts if gap_ts is not None else trailing_start
    gap_type = "internal" if gap_ts is not None else "trailing"
    print(f"[{now_str}] {gap_type} gap at {fetch_from}, fetching from there")

    df = fetch_closed_1m_since(start_ts=fetch_from, symbol=symbol)

    if df.empty:
        print(f"[{now_str}] No closed candles available to fill {gap_type} gap at {fetch_from}")
        return 0

    if debug:
        print(f"[DEBUG] Fetched {len(df)} closed candles: "
              f"{df['timestamp'].min()} .. {df['timestamp'].max()}")

    if dry_run:
        print(
            f"[{now_str}] [DRY-RUN] Would insert {len(df)} candles "
            f"(from {df['timestamp'].min()} to {df['timestamp'].max()})"
        )
        return 0

    inserted = insert_candles(db_path, df)

    stats = coverage_stats(db_path)
    next_gap = find_first_gap(db_path)

    print(
        f"[{now_str}] inserted={inserted}  "
        f"db_max={stats[1]}  db_total={stats[2]:,}  "
        f"next_gap={'none' if next_gap is None else next_gap}"
    )
    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Repair gaps in the 1m BTCUSDT perp SQLite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--db", type=Path, required=True,
                   help="Path to SQLite file")
    p.add_argument("--symbol", default=DEFAULT_SYMBOL,
                   help=f"Binance symbol (default: {DEFAULT_SYMBOL})")
    p.add_argument("--dry-run", action="store_true",
                   help="Detect gaps but do not write to DB")
    p.add_argument("--debug", action="store_true",
                   help="Verbose output")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    try:
        return run_once(
            db_path=args.db,
            symbol=args.symbol,
            dry_run=args.dry_run,
            debug=args.debug,
        )
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Periodic 1m BTC-USD Coinbase accumulator.

Run-and-exit script intended to be called by cron to keep the SQLite DB current
with recently closed 1m candles from Coinbase Exchange public REST.

Each invocation:
  1. Reads the DB's max timestamp.
  2. Fetches all closed 1m candles from max + 1 min to now (pages 5h windows).
  3. Inserts all rows into SQLite.
  4. Prints a one-line summary and exits.

Usage:
  python -m cex_data_feed.scripts.coinbase_accumulate_1m \\
    --db data/btcusd_coinbase_1m.sqlite \\
    [--product BTC-USD] [--dry-run] [--debug]

Suggested cron entry (every 5 minutes):
  */5 * * * * cd /path/to/trading_pm_data_feed && .venv/bin/python -m cex_data_feed.scripts.coinbase_accumulate_1m \\
    --db data/btcusd_coinbase_1m.sqlite >> data/coinbase_accumulate_1m.log 2>&1
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from cex_data_feed.pipeline_1m_coinbase.fetch import fetch_closed_1m_since, DEFAULT_PRODUCT
from cex_data_feed.pipeline_1m_coinbase.sqlite_db import ensure_table, insert_candles, coverage_stats


def run_once(
    db_path: Path,
    product_id: str = DEFAULT_PRODUCT,
    dry_run: bool = False,
    debug: bool = False,
) -> int:
    ensure_table(db_path)

    stats = coverage_stats(db_path)
    if stats is None:
        print("[ERROR] DB is empty — run coinbase_backfill_1m first", file=sys.stderr)
        return 1

    db_max = stats[1]
    start_ts = db_max + pd.Timedelta(minutes=1)

    if debug:
        print(f"[DEBUG] DB max: {db_max}  total: {stats[2]:,}  start: {start_ts}")

    df = fetch_closed_1m_since(start_ts=start_ts, product_id=product_id)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if df.empty:
        print(f"[{now_str}] No new closed candles since {db_max}")
        return 0

    if debug:
        print(f"[DEBUG] Fetched {len(df)} closed candles: "
              f"{df['timestamp'].min()} .. {df['timestamp'].max()}")

    if dry_run:
        print(f"[{now_str}] [DRY-RUN] Would insert {len(df)} candles "
              f"(newest: {df['timestamp'].max()})")
        return 0

    inserted = insert_candles(db_path, df)
    stats = coverage_stats(db_path)
    db_max = stats[1] if stats else "n/a"
    db_total = stats[2] if stats else "?"

    print(
        f"[{now_str}] fetched={len(df)}  inserted={inserted}  "
        f"db_max={db_max}  db_total={db_total:,}"
        if stats else
        f"[{now_str}] fetched={len(df)}  inserted={inserted}"
    )
    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Periodic 1m BTC-USD Coinbase accumulator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--db", type=Path, required=True,
                   help="Path to SQLite file (created if absent)")
    p.add_argument("--product", default=DEFAULT_PRODUCT,
                   help=f"Coinbase product (default: {DEFAULT_PRODUCT})")
    p.add_argument("--dry-run", action="store_true",
                   help="Fetch but do not write to DB")
    p.add_argument("--debug", action="store_true",
                   help="Verbose output")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    try:
        return run_once(
            db_path=args.db,
            product_id=args.product,
            dry_run=args.dry_run,
            debug=args.debug,
        )
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

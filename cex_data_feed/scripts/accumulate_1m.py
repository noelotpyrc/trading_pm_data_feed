#!/usr/bin/env python3
"""
Phase 2: Periodic 1m BTCUSDT perp accumulator.

Run-and-exit script intended to be called by cron or a systemd timer to keep
the SQLite DB current with recently closed 1m candles from Binance FAPI.

Each invocation:
  1. Fetches --limit recent 1m candles from Binance.
  2. Filters to fully closed candles (close_time < now UTC).
  3. Upserts new rows into SQLite (duplicates silently ignored).
  4. Prints a one-line summary and exits.

Usage:
  python -m cex_data_feed.scripts.accumulate_1m \\
    --db ~/data/btcusdt_perp_1m.sqlite \\
    --limit 10 \\
    [--symbol BTCUSDT] [--dry-run] [--debug]

Suggested cron entry (every 2 minutes):
  */2 * * * * /path/to/venv/bin/python -m cex_data_feed.scripts.accumulate_1m \\
    --db /data/btcusdt_perp_1m.sqlite >> /var/log/accumulate_1m.log 2>&1
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Allow running as a script
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from cex_data_feed.pipeline_1m.fetch import fetch_closed_1m_candles, DEFAULT_SYMBOL, DEFAULT_LIMIT
from cex_data_feed.pipeline_1m.sqlite_db import ensure_table, upsert_candles, coverage_stats


def run_once(
    db_path: Path,
    symbol: str = DEFAULT_SYMBOL,
    limit: int = DEFAULT_LIMIT,
    dry_run: bool = False,
    debug: bool = False,
) -> int:
    """Fetch closed 1m candles and upsert into SQLite. Returns exit code."""
    ensure_table(db_path)

    df = fetch_closed_1m_candles(symbol=symbol, limit=limit)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if df.empty:
        print(f"[{now_str}] No closed candles returned from API (limit={limit})")
        return 0

    if debug:
        print(f"[DEBUG] Fetched {len(df)} closed candles: "
              f"{df['timestamp'].min()} .. {df['timestamp'].max()}")

    if dry_run:
        print(
            f"[{now_str}] [DRY-RUN] Would upsert {len(df)} candles "
            f"(newest: {df['timestamp'].max()})"
        )
        return 0

    inserted = upsert_candles(db_path, df)
    skipped = len(df) - inserted

    stats = coverage_stats(db_path)
    db_max = stats[1] if stats else "n/a"
    db_total = stats[2] if stats else "?"

    print(
        f"[{now_str}] fetched={len(df)}  inserted={inserted}  "
        f"skipped(dup)={skipped}  db_max={db_max}  db_total={db_total:,}"
        if stats else
        f"[{now_str}] fetched={len(df)}  inserted={inserted}  skipped(dup)={skipped}"
    )
    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Periodic 1m BTCUSDT perp accumulator (Phase 2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--db", type=Path, required=True,
                   help="Path to SQLite file (created if absent)")
    p.add_argument("--symbol", default=DEFAULT_SYMBOL,
                   help=f"Binance symbol (default: {DEFAULT_SYMBOL})")
    p.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                   help=f"Number of recent candles to fetch per run (default: {DEFAULT_LIMIT})")
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
            symbol=args.symbol,
            limit=args.limit,
            dry_run=args.dry_run,
            debug=args.debug,
        )
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Backfill 1m BTC-USD candles from Coinbase Exchange public REST.

Coinbase has no bulk archive (unlike Binance Vision), so this walks forward
through history in 5h windows (300 candles/call, the API max), inserting each
batch into SQLite as it goes. Safe to re-run — resumes from the DB's max
timestamp by default.

Rough cost for 1 year of history: ~1,750 calls × ~0.4s sleep + request time
≈ 20-30 minutes wall clock.

Usage:
  python -m cex_data_feed.scripts.coinbase_backfill_1m \\
    --db data/btcusd_coinbase_1m.sqlite \\
    --start 2025-04-27 \\
    [--end 2026-04-27]            # default: now (floored to minute)
    [--product BTC-USD]
    [--sleep 0.4]                 # seconds between requests
    [--dry-run] [--debug]

Resume behaviour: if the DB already has data and --start is omitted (or is
older than db_max + 1min), the effective start is db_max + 1min.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from cex_data_feed.coinbase.api import fetch_candles, candles_to_dataframe
from cex_data_feed.pipeline_1m_coinbase.sqlite_db import (
    ensure_table, insert_candles, coverage_stats,
)
from cex_data_feed.pipeline_1m_coinbase.fetch import DEFAULT_PRODUCT


_GRANULARITY = 60
_WINDOW_S = 300 * _GRANULARITY  # 5h per request


def _to_iso_utc(ts: pd.Timestamp) -> str:
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.isoformat()


def run(
    db_path: Path,
    start: Optional[str],
    end: Optional[str],
    product_id: str,
    sleep_s: float,
    dry_run: bool,
    debug: bool,
) -> int:
    ensure_table(db_path)

    # Resolve end (default: now floored to minute)
    if end:
        end_ts = pd.Timestamp(end).floor("min")
    else:
        end_ts = pd.Timestamp(datetime.now(timezone.utc)).tz_convert(None).floor("min")

    # Resolve start: prefer explicit; fall back to db_max+1min; require at least one.
    stats = coverage_stats(db_path)
    if start:
        start_ts = pd.Timestamp(start).floor("min")
        if stats is not None:
            resume_ts = stats[1] + pd.Timedelta(minutes=1)
            if resume_ts > start_ts:
                print(f"[INFO] DB max is {stats[1]}; resuming from {resume_ts} "
                      f"(later than --start {start_ts})")
                start_ts = resume_ts
    else:
        if stats is None:
            print("[ERROR] DB is empty and --start was not given", file=sys.stderr)
            return 1
        start_ts = stats[1] + pd.Timedelta(minutes=1)
        print(f"[INFO] Resuming from db_max + 1min = {start_ts}")

    if start_ts >= end_ts:
        print(f"[INFO] start ({start_ts}) >= end ({end_ts}) — nothing to backfill")
        return 0

    total_minutes = int((end_ts - start_ts).total_seconds() // 60)
    total_calls = (total_minutes + 299) // 300

    print(f"Backfill plan:")
    print(f"  Product:      {product_id}")
    print(f"  Range:        {start_ts} .. {end_ts}  ({total_minutes:,} minutes)")
    print(f"  Calls:        ~{total_calls:,}  (5h windows, 300 candles each)")
    print(f"  Sleep/call:   {sleep_s}s")
    print(f"  Target DB:    {db_path}")
    if dry_run:
        print("  [DRY-RUN] no requests, no writes")
        return 0

    cur = start_ts
    call_idx = 0
    inserted_total = 0
    t0 = time.time()

    while cur < end_ts:
        win_end = min(cur + pd.Timedelta(seconds=_WINDOW_S), end_ts)
        call_idx += 1

        try:
            candles = fetch_candles(
                product_id,
                _GRANULARITY,
                start_iso=_to_iso_utc(cur),
                end_iso=_to_iso_utc(win_end),
            )
        except Exception as e:
            print(f"  [{call_idx}/{total_calls}] {cur} .. {win_end} — FAILED: {e}",
                  file=sys.stderr)
            # Back off and continue rather than aborting the whole run
            time.sleep(max(sleep_s * 5, 2.0))
            cur = win_end
            continue

        if candles:
            df = candles_to_dataframe(candles, _GRANULARITY)
            df = df.drop(columns=["_close_time"], errors="ignore")
            n = insert_candles(db_path, df)
            inserted_total += n
            if debug:
                print(f"  [{call_idx}/{total_calls}] {cur} .. {win_end}  +{n} rows")
            elif call_idx % 20 == 0:
                elapsed = time.time() - t0
                rate = call_idx / elapsed if elapsed > 0 else 0
                eta = (total_calls - call_idx) / rate if rate > 0 else 0
                print(f"  [{call_idx}/{total_calls}] {cur}  inserted_total={inserted_total:,}  "
                      f"eta={eta/60:.1f}min")
        else:
            if debug:
                print(f"  [{call_idx}/{total_calls}] {cur} .. {win_end}  (empty)")

        cur = win_end
        if cur < end_ts:
            time.sleep(sleep_s)

    elapsed = time.time() - t0
    final = coverage_stats(db_path)
    if final:
        print(f"\n✓ Backfill complete in {elapsed/60:.1f}min  "
              f"inserted={inserted_total:,}  db_total={final[2]:,}  "
              f"coverage={final[0]} .. {final[1]}")
    else:
        print(f"\n✓ Backfill complete in {elapsed/60:.1f}min  inserted={inserted_total:,}")
    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill 1m BTC-USD Coinbase candles into SQLite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--db", type=Path, required=True,
                   help="Target SQLite file path (created if absent)")
    p.add_argument("--start", default=None,
                   help="Start datetime (UTC), e.g. 2025-04-27 or '2025-04-27 00:00:00'. "
                        "If omitted, resumes from db_max + 1min.")
    p.add_argument("--end", default=None,
                   help="End datetime (UTC). Default: now floored to minute.")
    p.add_argument("--product", default=DEFAULT_PRODUCT,
                   help=f"Coinbase product (default: {DEFAULT_PRODUCT})")
    p.add_argument("--sleep", type=float, default=0.4,
                   help="Seconds to sleep between requests (default 0.4; limit is 3 req/s)")
    p.add_argument("--dry-run", action="store_true",
                   help="Show plan and exit without making requests")
    p.add_argument("--debug", action="store_true",
                   help="Per-call progress output")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    try:
        return run(
            db_path=args.db,
            start=args.start,
            end=args.end,
            product_id=args.product,
            sleep_s=args.sleep,
            dry_run=args.dry_run,
            debug=args.debug,
        )
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user — DB has all batches inserted so far. "
              "Re-run to resume.")
        return 1
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        if args.debug:
            raise
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

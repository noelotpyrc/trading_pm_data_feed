#!/usr/bin/env python3
"""
Backfill v3 daily artifacts over a date range.

Generic — works for any 1m OHLCV SQLite table by passing --db / --table.
Idempotent: skips dates whose `model.json` already exists in --out-dir.
Continues past per-day failures and prints a final summary.

Usage:
  python -m pm_btc15updown_artifact.scripts.backfill_artifacts_v3 \\
    --db data/btcusd_coinbase_1m.sqlite \\
    --table ohlcv_btcusd_coinbase_1m \\
    --out-dir data/artifacts_v3_coinbase \\
    --start 2025-05-05 \\
    --end 2026-04-28 \\
    [--force] [--debug]

Use --force to rebuild dates that already have artifacts (e.g. after a config change).
"""
from __future__ import annotations

import argparse
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from pm_btc15updown_artifact.data_loader import TABLE as DEFAULT_TABLE
from pm_btc15updown_artifact.scripts.build_daily_artifact_v3 import build_once
from pm_btc15updown_artifact.vol_signal_artifacts_v3 import VolSignalBuildConfigV3


def _date_range(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    """Inclusive list of UTC-normalized daily timestamps."""
    return list(pd.date_range(start.normalize(), end.normalize(), freq="1D"))


def run(
    db_path: Path,
    out_dir: Path,
    start: pd.Timestamp,
    end: pd.Timestamp,
    table: str,
    force: bool,
    debug: bool,
) -> int:
    config = VolSignalBuildConfigV3()
    dates = _date_range(start, end)
    if not dates:
        print(f"[ERROR] empty date range: {start} .. {end}", file=sys.stderr)
        return 1

    print(f"Backfill plan:")
    print(f"  DB:        {db_path}")
    print(f"  Table:     {table}")
    print(f"  Out dir:   {out_dir}")
    print(f"  Range:     {dates[0].date()} .. {dates[-1].date()}  ({len(dates)} days)")
    print(f"  Force:     {force}")

    skipped, built, failed = 0, 0, 0
    failures: list[tuple[str, str]] = []
    t0 = time.time()

    for i, score_date in enumerate(dates, 1):
        date_str = score_date.strftime("%Y-%m-%d")
        artifact_dir = out_dir / date_str
        model_path = artifact_dir / "model.json"

        if model_path.exists() and not force:
            skipped += 1
            if debug:
                print(f"  [{i}/{len(dates)}] {date_str}  skipped (exists)")
            continue

        try:
            build_once(db_path, out_dir, score_date, config, table=table, debug=debug)
            built += 1
        except Exception as e:
            failed += 1
            failures.append((date_str, str(e)))
            print(f"  [{i}/{len(dates)}] {date_str}  FAILED: {e}", file=sys.stderr)
            if debug:
                traceback.print_exc()
            continue

        # Lightweight progress every 25 dates
        if not debug and (i % 25 == 0 or i == len(dates)):
            elapsed = time.time() - t0
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(dates) - i) / rate if rate > 0 else 0
            print(f"  [{i}/{len(dates)}] {date_str}  built={built} skipped={skipped} "
                  f"failed={failed}  eta={eta/60:.1f}min")

    elapsed = time.time() - t0
    print(f"\n✓ Backfill done in {elapsed/60:.1f}min  "
          f"built={built}  skipped={skipped}  failed={failed}  "
          f"total={len(dates)}")

    if failures:
        print("\nFailures:", file=sys.stderr)
        for d, msg in failures:
            print(f"  {d}: {msg}", file=sys.stderr)

    return 0 if failed == 0 else 1


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill v3 daily artifacts over a date range",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--db", type=Path, required=True,
                   help="Path to 1m OHLCV SQLite database")
    p.add_argument("--out-dir", type=Path, required=True,
                   help="Output directory for artifacts")
    p.add_argument("--start", type=str, required=True,
                   help="First score date (YYYY-MM-DD, inclusive)")
    p.add_argument("--end", type=str, required=True,
                   help="Last score date (YYYY-MM-DD, inclusive)")
    p.add_argument("--table", default=None,
                   help=f"OHLCV table name (default: {DEFAULT_TABLE})")
    p.add_argument("--force", action="store_true",
                   help="Rebuild dates that already have artifacts")
    p.add_argument("--debug", action="store_true",
                   help="Per-day progress + tracebacks on failure")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    try:
        return run(
            db_path=args.db,
            out_dir=args.out_dir,
            start=pd.Timestamp(args.start),
            end=pd.Timestamp(args.end),
            table=args.table or DEFAULT_TABLE,
            force=args.force,
            debug=args.debug,
        )
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted — re-run to resume (idempotent)")
        return 1
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        if args.debug:
            raise
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

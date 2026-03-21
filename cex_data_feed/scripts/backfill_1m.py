#!/usr/bin/env python3
"""
Unified 1m BTCUSDT perp backfill: download → merge → import to SQLite in one command.

Steps run internally:
  1. Download monthly kline ZIPs from Binance Vision (futures USDT-M)
  2. Download daily kline ZIPs for the current partial month
  3. Merge all ZIPs into a single temporary CSV
  4. Import CSV into SQLite (chunked, idempotent)

Usage:
  python -m cex_data_feed.scripts.backfill_1m \\
    --db data/btcusdt_perp_1m.sqlite \\
    --start 2024-01 \\
    [--end 2025-02]        # default: last completed month
    [--symbol BTCUSDT]
    [--chunk-size 50000]
    [--keep-zips /tmp/klines_1m]   # keep ZIPs for faster re-runs; omit to auto-clean
    [--dry-run] [--debug]

The --keep-zips flag is optional but recommended: it caches the downloaded ZIPs
so a re-run skips already-downloaded files (the download scripts are idempotent).
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from cex_data_feed.scripts.download_binance_monthly_klines import (
    _month_iter,
    build_monthly_url,
    download_if_needed as monthly_download,
    url_exists as monthly_url_exists,
)
from cex_data_feed.scripts.download_binance_daily_klines import (
    _date_iter,
    build_daily_url,
    download_if_needed as daily_download,
)
from cex_data_feed.scripts.merge_binance_klines import (
    iter_zip_csv_lines,
    is_header_line,
    process_line,
    BINANCE_KLINE_HEADER,
)
from cex_data_feed.scripts.backfill_1m_from_csv import run_backfill

DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_CHUNK_SIZE = 50_000


# ── helpers ───────────────────────────────────────────────────────────────────

def _prev_month_str() -> str:
    """Return the most recently completed full month as 'YYYY-MM'."""
    today = datetime.now(timezone.utc)
    first_of_this = today.replace(day=1)
    last_month = first_of_this - timedelta(days=1)
    return last_month.strftime("%Y-%m")


def _yesterday_str() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def _first_day_of_current_month() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-01")


# ── step 1 & 2: download ──────────────────────────────────────────────────────

def _download_monthly(symbol: str, start: str, end: str, zip_dir: str, dry_run: bool, debug: bool) -> int:
    """Download monthly ZIPs. Returns number of failures."""
    months = _month_iter(start, end)
    if not months:
        print(f"[WARN] No months in range {start}..{end}")
        return 0

    print(f"\n[Step 1/4] Downloading {len(months)} monthly ZIP(s): {months[0]} .. {months[-1]}")
    failed = 0
    for i, m in enumerate(months, 1):
        url = build_monthly_url(symbol, "1m", m, "futures")
        fname = os.path.basename(url)
        exists, _ = monthly_url_exists(url)
        if not exists:
            print(f"  [{i}/{len(months)}] {fname} — not available on Binance Vision, skipping")
            continue
        if dry_run:
            print(f"  [{i}/{len(months)}] [DRY-RUN] {fname}")
            continue
        status, _ = monthly_download(url, zip_dir)
        label = "downloaded" if status == "downloaded" else ("skipped" if status == "skipped" else "FAILED")
        if debug or status not in ("skipped",):
            print(f"  [{i}/{len(months)}] {fname} — {label}")
        if status == "failed":
            failed += 1
    return failed


def _download_daily(symbol: str, start_date: str, end_date: str, zip_dir: str, dry_run: bool, debug: bool) -> int:
    """Download daily ZIPs for the current partial month. Returns number of failures."""
    days = _date_iter(start_date, end_date)
    if not days:
        print(f"  (no days in {start_date}..{end_date}, skipping daily)")
        return 0

    print(f"\n[Step 2/4] Downloading {len(days)} daily ZIP(s): {days[0]} .. {days[-1]}")
    failed = 0
    for i, d in enumerate(days, 1):
        url = build_daily_url(symbol, "1m", d, "futures")
        fname = os.path.basename(url)
        if dry_run:
            print(f"  [{i}/{len(days)}] [DRY-RUN] {fname}")
            continue
        status, _ = daily_download(url, zip_dir)
        label = "downloaded" if status == "downloaded" else ("skipped" if status == "skipped" else "FAILED")
        if debug or status not in ("skipped",):
            print(f"  [{i}/{len(days)}] {fname} — {label}")
        if status == "failed":
            failed += 1
    return failed


# ── step 3: merge ─────────────────────────────────────────────────────────────

def _merge_zips(symbol: str, zip_dir: str, out_csv: str, dry_run: bool, debug: bool) -> int:
    """Merge all ZIPs in zip_dir → single CSV. Returns row count."""
    pattern = os.path.join(zip_dir, f"{symbol}-1m-*.zip")
    zip_paths = sorted(glob.glob(pattern))
    if not zip_paths:
        print(f"[ERROR] No ZIPs found matching {pattern}", file=sys.stderr)
        return 0

    print(f"\n[Step 3/4] Merging {len(zip_paths)} ZIP(s) → {out_csv}")
    if dry_run:
        print("  [DRY-RUN] Would merge ZIPs")
        return 0

    total_rows = 0
    with open(out_csv, "w", encoding="utf-8", newline="\n") as f:
        f.write(",".join(BINANCE_KLINE_HEADER) + "\n")
        for idx, zp in enumerate(zip_paths, 1):
            base = os.path.basename(zp)
            if debug:
                print(f"  [{idx}/{len(zip_paths)}] {base}")
            for line in iter_zip_csv_lines(zp):
                if is_header_line(line):
                    continue
                processed, _ = process_line(line, "futures")
                f.write(processed + "\n")
                total_rows += 1

    print(f"  Merged {total_rows:,} rows")
    return total_rows


# ── main orchestrator ─────────────────────────────────────────────────────────

def run(
    db_path: Path,
    start: str,
    end: Optional[str],
    symbol: str,
    keep_zips: Optional[Path],
    chunk_size: int,
    dry_run: bool,
    debug: bool,
) -> int:
    # Resolve end month for monthly ZIPs (always last completed month)
    end_month = end or _prev_month_str()

    # Daily ZIPs cover the current partial month. Run the daily step when:
    #   - no explicit --end was given (intent: backfill up to now), OR
    #   - --end reaches into the current (incomplete) month
    current_month_str = datetime.now(timezone.utc).strftime("%Y-%m")
    if end is None or end_month >= current_month_str:
        daily_start_str = _first_day_of_current_month()
        daily_end_str = _yesterday_str()
        run_daily = True
    else:
        # explicit --end is a fully completed month — no daily needed
        daily_start_str = daily_end_str = ""
        run_daily = False

    print(f"Backfill plan:")
    print(f"  Symbol:        {symbol}")
    print(f"  Monthly range: {start} .. {end_month}")
    if run_daily:
        print(f"  Daily range:   {daily_start_str} .. {daily_end_str}  (current partial month)")
    else:
        print(f"  Daily range:   (skipped — {end_month} is a completed month)")
    print(f"  Target DB:     {db_path}")

    # Decide where to put ZIPs
    _tmp_dir = None
    if keep_zips:
        zip_dir = str(keep_zips)
        os.makedirs(zip_dir, exist_ok=True)
    else:
        _tmp_dir = tempfile.mkdtemp(prefix="binance_1m_zips_")
        zip_dir = _tmp_dir
        if debug:
            print(f"  ZIP temp dir:  {zip_dir}")

    try:
        # Step 1 — monthly
        failed = _download_monthly(symbol, start, end_month, zip_dir, dry_run, debug)
        if failed > 0:
            print(f"[WARN] {failed} monthly ZIP(s) failed to download; continuing anyway")

        # Step 2 — daily (only for current in-progress month)
        if run_daily and daily_start_str <= daily_end_str:
            failed_d = _download_daily(symbol, daily_start_str, daily_end_str, zip_dir, dry_run, debug)
            if failed_d > 0:
                print(f"[WARN] {failed_d} daily ZIP(s) failed; continuing anyway")
        else:
            print(f"\n[Step 2/4] Daily ZIPs skipped (completed months only)")

        # Step 3 — merge
        with tempfile.NamedTemporaryFile(
            suffix=".csv", prefix="btcusdt_1m_merged_", delete=False
        ) as tmp_csv:
            merged_csv = tmp_csv.name

        try:
            row_count = _merge_zips(symbol, zip_dir, merged_csv, dry_run, debug)
            if dry_run:
                print(f"\n[Step 4/4] [DRY-RUN] Would import merged CSV into SQLite: {db_path}")
                rc = 0
            elif row_count == 0:
                print("[ERROR] No rows merged — nothing to import", file=sys.stderr)
                rc = 1
            else:
                # Step 4 — import
                print(f"\n[Step 4/4] Importing into SQLite: {db_path}")
                rc = run_backfill(
                    csv_path=Path(merged_csv),
                    db_path=db_path,
                    chunk_size=chunk_size,
                    dry_run=False,
                    debug=debug,
                )
        finally:
            try:
                os.unlink(merged_csv)
            except OSError:
                pass
    finally:
        # Clean up temp ZIP dir if we created it
        if _tmp_dir:
            import shutil
            if debug:
                print(f"\n[INFO] Cleaning up temp ZIP dir: {_tmp_dir}")
            shutil.rmtree(_tmp_dir, ignore_errors=True)

    print("\n✓ Backfill complete")
    return rc


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Unified 1m BTCUSDT perp backfill: download + merge + import to SQLite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--db", type=Path, required=True,
                   help="Target SQLite file path (created if absent)")
    p.add_argument("--start", required=True, metavar="YYYY-MM",
                   help="First month to backfill (inclusive), e.g. 2024-01")
    p.add_argument("--end", default=None, metavar="YYYY-MM",
                   help="Last month to backfill (inclusive); default: last completed month")
    p.add_argument("--symbol", default=DEFAULT_SYMBOL,
                   help=f"Binance symbol (default: {DEFAULT_SYMBOL})")
    p.add_argument("--keep-zips", type=Path, default=None, metavar="DIR",
                   help="Directory to cache downloaded ZIPs (re-runs skip already-downloaded files). "
                        "Omit to use a temp dir that is auto-deleted after import.")
    p.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                   help=f"Rows per import chunk (default: {DEFAULT_CHUNK_SIZE})")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would happen without downloading or writing to DB")
    p.add_argument("--debug", action="store_true",
                   help="Verbose per-file output")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    try:
        return run(
            db_path=args.db,
            start=args.start,
            end=args.end,
            symbol=args.symbol,
            keep_zips=args.keep_zips,
            chunk_size=args.chunk_size,
            dry_run=args.dry_run,
            debug=args.debug,
        )
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user")
        return 1
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        if args.debug:
            raise
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

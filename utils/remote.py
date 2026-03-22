#!/usr/bin/env python3
"""Remote utilities — runs ON the VPS for data/log/artifact management.

Usage:
  .venv/bin/python -m utils.remote <command> [args]

Commands:
  db-stats                          Print DB coverage stats
  cleanup-artifacts [--keep-days N] Delete artifact dirs older than N days (default: 30)
  rotate-logs [--max-mb N]          Rotate log files larger than N MB (default: 50)
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Allow running as a script
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def db_stats(db_path: Path | None = None) -> None:
    """Print DB coverage stats."""
    from cex_data_feed.pipeline_1m.sqlite_db import coverage_stats

    db_path = db_path or DATA_DIR / "btcusdt_perp_1m.sqlite"
    stats = coverage_stats(db_path)
    if stats is None:
        print("DB is empty")
        return
    min_ts, max_ts, count = stats
    print(f"min={min_ts}  max={max_ts}  rows={count:,}")


def cleanup_artifacts(keep_days: int = 30, dry_run: bool = False) -> None:
    """Delete artifact dirs older than keep_days."""
    artifacts_dir = DATA_DIR / "artifacts"
    if not artifacts_dir.exists():
        print("No artifacts directory found")
        return

    cutoff = datetime.now(timezone.utc).date() - timedelta(days=keep_days)
    removed = 0

    for d in sorted(artifacts_dir.iterdir()):
        if not d.is_dir():
            continue
        try:
            dir_date = datetime.strptime(d.name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if dir_date < cutoff:
            if dry_run:
                print(f"[DRY-RUN] Would remove {d}")
            else:
                shutil.rmtree(d)
                print(f"Removed {d}")
            removed += 1

    print(f"{'Would remove' if dry_run else 'Removed'} {removed} artifact dirs (cutoff: {cutoff})")


def rotate_logs(max_mb: float = 50) -> None:
    """Rotate log files larger than max_mb by renaming to .old and truncating."""
    max_bytes = max_mb * 1024 * 1024

    for log_file in DATA_DIR.glob("*.log"):
        size = log_file.stat().st_size
        if size > max_bytes:
            old = log_file.with_suffix(".log.old")
            if old.exists():
                old.unlink()
            log_file.rename(old)
            log_file.touch()
            print(f"Rotated {log_file.name} ({size / 1024 / 1024:.1f} MB)")
        else:
            print(f"OK {log_file.name} ({size / 1024 / 1024:.1f} MB)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="VPS data/log/artifact management")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("db-stats", help="Print DB coverage stats")

    p_clean = sub.add_parser("cleanup-artifacts", help="Delete old artifact dirs")
    p_clean.add_argument("--keep-days", type=int, default=30)
    p_clean.add_argument("--dry-run", action="store_true")

    p_rotate = sub.add_parser("rotate-logs", help="Rotate large log files")
    p_rotate.add_argument("--max-mb", type=float, default=50)

    args = parser.parse_args(argv)

    if args.command == "db-stats":
        db_stats()
    elif args.command == "cleanup-artifacts":
        cleanup_artifacts(keep_days=args.keep_days, dry_run=args.dry_run)
    elif args.command == "rotate-logs":
        rotate_logs(max_mb=args.max_mb)
    else:
        parser.print_help()
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

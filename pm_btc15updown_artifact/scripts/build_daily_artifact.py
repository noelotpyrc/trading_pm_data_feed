#!/usr/bin/env python3
"""
Build daily volatility signal artifact from the 1m OHLCV SQLite database.

Runs on VPS once per day before the trading node starts. Produces:
  - model.json   (linear regression coefficients per MAR horizon)
  - z_pool.npy   (empirical z-score distribution)
  - metadata.json (build info, coverage stats)

Usage:
  python -m pm_btc15updown_artifact.scripts.build_daily_artifact \
    --db data/btcusdt_perp_1m.sqlite \
    --out-dir data/artifacts \
    [--date 2026-03-21] [--retry 3] [--debug]
"""
from __future__ import annotations

import argparse
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

# Allow running as a script
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from pm_btc15updown_artifact.data_loader import load_ohlcv_window, check_data_quality
from pm_btc15updown_artifact.vol_signal_artifacts import (
    VolSignalBuildConfig,
    build_daily_signal_artifact,
)


def _load_and_check(
    db_path: Path,
    score_date: pd.Timestamp,
    config: VolSignalBuildConfig,
    debug: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """Load OHLCV window needed for artifact build, with data quality checks.

    Returns (dataframe, data_quality_stats) where data_quality_stats contains
    training and z-pool window quality metrics for metadata.json.
    """
    history_start = score_date - timedelta(days=config.min_required_history_days)
    data_end = score_date + timedelta(days=1)

    if debug:
        print(f"[DEBUG] Loading OHLCV: {history_start} .. {data_end}")

    df = load_ohlcv_window(db_path, start=history_start, end=data_end)

    if df.empty:
        raise ValueError(
            f"No OHLCV data in window {history_start} .. {data_end}. "
            "Run backfill or accumulate first."
        )

    if debug:
        print(f"[DEBUG] Loaded {len(df)} rows: "
              f"{df['datetime_utc'].min()} .. {df['datetime_utc'].max()}")

    # Check training window quality
    train_start = score_date - timedelta(days=config.training_lookback_days)
    train_quality = check_data_quality(
        df[(df["datetime_utc"] >= train_start) & (df["datetime_utc"] < score_date)],
        expected_start=train_start,
        expected_end=score_date,
        label="training window",
    )

    # Check z-pool window quality
    zpool_start = score_date - timedelta(days=config.z_pool_lookback_days)
    zpool_end = score_date - timedelta(minutes=config.group_minutes)
    zpool_quality = check_data_quality(
        df[(df["datetime_utc"] >= zpool_start) & (df["datetime_utc"] < zpool_end)],
        expected_start=zpool_start,
        expected_end=zpool_end,
        label="z-pool window",
    )

    quality_stats = {
        "training_data_quality": train_quality,
        "zpool_data_quality": zpool_quality,
    }

    return df, quality_stats


def build_once(
    db_path: Path,
    out_dir: Path,
    score_date: pd.Timestamp,
    config: VolSignalBuildConfig,
    debug: bool = False,
) -> Path:
    """Build and save a single day's artifact. Returns the output directory."""
    ohlcv_df, quality_stats = _load_and_check(db_path, score_date, config, debug=debug)

    artifact = build_daily_signal_artifact(ohlcv_df, score_date, config, extra_metadata=quality_stats)

    artifact_dir = out_dir / score_date.strftime("%Y-%m-%d")
    artifact.save(artifact_dir)

    print(
        f"[OK] score_date={artifact.score_date}  "
        f"z_pool_size={artifact.z_pool.size}  "
        f"models={list(artifact.models.keys())}  "
        f"output={artifact_dir}"
    )
    return artifact_dir


def run(
    db_path: Path,
    out_dir: Path,
    score_date: pd.Timestamp,
    retries: int = 3,
    debug: bool = False,
) -> int:
    """Build with retries. Returns exit code."""
    config = VolSignalBuildConfig()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    for attempt in range(1, retries + 1):
        try:
            build_once(db_path, out_dir, score_date, config, debug=debug)
            return 0
        except Exception as e:
            print(f"[{now_str}] Attempt {attempt}/{retries} failed: {e}",
                  file=sys.stderr)
            if debug:
                traceback.print_exc()
            if attempt < retries:
                wait = 2 ** attempt
                print(f"[{now_str}] Retrying in {wait}s...", file=sys.stderr)
                time.sleep(wait)

    print(f"[{now_str}] All {retries} attempts failed", file=sys.stderr)
    return 1


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build daily volatility signal artifact from 1m OHLCV SQLite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--db", type=Path, required=True,
                   help="Path to 1m OHLCV SQLite database")
    p.add_argument("--out-dir", type=Path, required=True,
                   help="Output directory for artifacts (creates YYYY-MM-DD/ subdirs)")
    p.add_argument("--date", type=str, default=None,
                   help="Score date (YYYY-MM-DD, default: today UTC)")
    p.add_argument("--retry", type=int, default=3,
                   help="Number of retry attempts on failure (default: 3)")
    p.add_argument("--debug", action="store_true",
                   help="Verbose output")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    if args.date:
        score_date = pd.Timestamp(args.date).normalize()
    else:
        score_date = pd.Timestamp(datetime.now(timezone.utc).date())

    return run(
        db_path=args.db,
        out_dir=args.out_dir,
        score_date=score_date,
        retries=args.retry,
        debug=args.debug,
    )


if __name__ == "__main__":
    raise SystemExit(main())

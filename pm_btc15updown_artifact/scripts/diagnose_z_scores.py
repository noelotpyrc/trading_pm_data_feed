#!/usr/bin/env python3
"""
Diagnose z-score distributions from scored 1m OHLCV data.

Builds features, scores each day with a walk-forward model, computes
per-row z-scores (TTL-matched), and empirical probabilities using
the per-day z-pool (matching production setup).

Usage:
  python -m pm_btc15updown_artifact.scripts.diagnose_z_scores \
    --csv data/ohlcv_2026-02_to_2026-04-01.csv \
    --start 2026-03-01 --end 2026-03-31 \
    --out data/scored_march_2026.csv \
    [--debug]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from pm_btc15updown_artifact.vol_signal_artifacts import (
    VolSignalBuildConfig,
    prepare_feature_frame,
    score_date_range_for_history,
    attach_signal_columns,
    build_z_pool,
)


def run(
    csv_path: Path,
    start_date: str,
    end_date: str,
    out_path: Path | None = None,
    debug: bool = False,
) -> pd.DataFrame:
    config = VolSignalBuildConfig()

    # Load and prepare
    raw = pd.read_csv(csv_path)
    raw.rename(columns={"timestamp": "datetime_utc"}, inplace=True)
    if debug:
        print(f"[DEBUG] Loaded {len(raw)} rows: {raw['datetime_utc'].min()} .. {raw['datetime_utc'].max()}")

    feat = prepare_feature_frame(raw, config)
    if debug:
        print(f"[DEBUG] Feature frame: {len(feat)} rows")

    # Score each day with walk-forward model
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()

    # Need z-pool history from z_pool_lookback_days before start
    history_start = start - pd.Timedelta(days=config.z_pool_lookback_days)
    scored = score_date_range_for_history(feat, history_start, end, config)
    scored = attach_signal_columns(scored, config)
    if debug:
        print(f"[DEBUG] Scored {len(scored)} rows from {history_start.date()} to {end.date()}")

    # Compute per-row z (TTL-matched)
    scored["z"] = np.nan
    for ttl in range(1, config.group_minutes + 1):
        mask = scored["ttl"] == ttl
        fwd_col = f"target_fwd_ret_{ttl}"
        scored.loc[mask, "z"] = scored.loc[mask, fwd_col] / scored.loc[mask, "sigma_W_ttl"]

    # Compute per-row empirical prob using per-day z-pool
    scored["p_z_gt"] = np.nan  # P(Z > z) from that day's pool

    for score_date in pd.date_range(start=start, end=end, freq="D"):
        day_mask = (
            (scored["datetime_utc"] >= score_date)
            & (scored["datetime_utc"] < score_date + pd.Timedelta(days=1))
        )
        day_rows = scored.loc[day_mask]
        if day_rows.empty:
            continue

        try:
            z_pool = build_z_pool(scored, score_date=score_date, config=config)
        except ValueError as e:
            if debug:
                print(f"[DEBUG] {score_date.date()}: skipped z-pool — {e}")
            continue

        z_vals = day_rows["z"].dropna()
        if z_vals.empty:
            continue

        # P(Z > z) = 1 - CDF(z)
        positions = np.searchsorted(z_pool, scored.loc[day_mask, "z"].values)
        scored.loc[day_mask, "p_z_gt"] = 1.0 - positions / len(z_pool)

        if debug:
            print(f"[DEBUG] {score_date.date()}: z_pool_size={len(z_pool)}  "
                  f"scored_rows={len(day_rows)}  z_mean={z_vals.mean():.4f}")

    # Filter to requested date range
    result = scored[
        (scored["datetime_utc"] >= start)
        & (scored["datetime_utc"] < end + pd.Timedelta(days=1))
    ].copy()

    # Summary
    print(f"\n=== Z-Score Summary ({start.date()} to {end.date()}) ===")
    print(f"Total rows: {len(result):,}")
    print(f"\nBy TTL:")
    print(result.groupby("ttl")["z"].describe().to_string())
    print(f"\nP(Z > z) distribution:")
    print(result["p_z_gt"].describe().to_string())

    if out_path:
        result.to_csv(out_path, index=False)
        print(f"\nSaved to {out_path}")

    return result


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Diagnose z-score distributions from scored 1m OHLCV data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--csv", type=Path, required=True,
                   help="Path to OHLCV CSV (with timestamp column)")
    p.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    p.add_argument("--out", type=Path, default=None,
                   help="Output CSV path (optional)")
    p.add_argument("--debug", action="store_true")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    try:
        run(
            csv_path=args.csv,
            start_date=args.start,
            end_date=args.end,
            out_path=args.out,
            debug=args.debug,
        )
        return 0
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        raise


if __name__ == "__main__":
    raise SystemExit(main())

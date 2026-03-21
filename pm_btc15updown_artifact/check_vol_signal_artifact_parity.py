#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from live.private_strategies.vol_signal_artifacts import (
    VolSignalBuildConfig,
    attach_signal_columns,
    prepare_feature_frame,
    score_date_range_for_history,
)


DEFAULT_RAW_CSV = Path(
    "/Volumes/Extreme SSD/trading_data/cex/ohlvc/binance_btcusdt_perp_1m/BTCUSDT-1m-merged.csv"
)
DEFAULT_TRUTH_CSV = Path(
    "/Volumes/Extreme SSD/trading_data/cex/ohlvc/binance_btcusdt_perp_1m/BTCUSDT-1m-features-vol.csv"
)
DEFAULT_FEATURE_DAY = "2022-01-01"
DEFAULT_PREDICTION_DAY = "2025-01-16"
DEFAULT_TOLERANCE = 1e-10


FEATURE_COMPARE_COLS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "parkinson_10",
    "parkinson_15",
    "parkinson_30",
    "parkinson_45",
    "parkinson_60",
    "parkinson_1440",
    "parkinson_ratio_30_1440",
    "parkinson_ratio_15_1440",
    *[f"target_fwd_ret_{n}" for n in range(1, 16)],
    "target_mar_next1_close",
    "target_mar_next3_close",
    "target_mar_next5_close",
]

PREDICTION_COMPARE_COLS = [
    "pred_mar_1",
    "pred_mar_3",
    "pred_mar_5",
    "mar_blend",
    "strike_K",
    "ttl",
    "sigma_W_ttl",
]


@dataclass(frozen=True)
class CompareResult:
    rows_compared: int
    mismatched_columns: list[str]
    max_abs_diffs: dict[str, float]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parity check for private vol-signal artifact builder.")
    parser.add_argument("--raw-csv", type=Path, default=DEFAULT_RAW_CSV)
    parser.add_argument("--sqlite-db", type=Path, default=None)
    parser.add_argument("--sqlite-table", default="ohlcv_btcusdt_1m")
    parser.add_argument("--truth-csv", type=Path, default=DEFAULT_TRUTH_CSV)
    parser.add_argument("--feature-day", default=DEFAULT_FEATURE_DAY)
    parser.add_argument("--prediction-day", default=DEFAULT_PREDICTION_DAY)
    parser.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE)
    return parser.parse_args()


def load_window(
    path: Path,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    usecols: list[str] | None = None,
    chunksize: int = 250_000,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunksize):
        chunk["datetime_utc"] = pd.to_datetime(chunk["datetime_utc"])
        window = chunk[(chunk["datetime_utc"] >= start) & (chunk["datetime_utc"] < end)]
        if not window.empty:
            frames.append(window)
    if not frames:
        return pd.DataFrame(columns=usecols or [])
    return pd.concat(frames, ignore_index=True)


def load_sqlite_window(
    db_path: Path,
    table_name: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    con = sqlite3.connect(str(db_path))
    try:
        return pd.read_sql_query(
            (
                f"SELECT timestamp as datetime_utc, open, high, low, close, volume "
                f"FROM {table_name} WHERE timestamp >= ? AND timestamp < ? ORDER BY timestamp"
            ),
            con,
            params=[start.isoformat(sep=" "), end.isoformat(sep=" ")],
            parse_dates=["datetime_utc"],
        )
    finally:
        con.close()


def load_raw_window(
    *,
    raw_csv: Path | None,
    sqlite_db: Path | None,
    sqlite_table: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    if sqlite_db is not None:
        return load_sqlite_window(sqlite_db, sqlite_table, start, end)
    if raw_csv is None:
        raise ValueError("either raw_csv or sqlite_db must be provided")
    return load_window(
        raw_csv,
        start=start,
        end=end,
        usecols=["datetime_utc", "open", "high", "low", "close", "volume"],
    )


def compare_frames(
    left: pd.DataFrame,
    right: pd.DataFrame,
    cols: list[str],
    *,
    tolerance: float,
) -> CompareResult:
    merged = left.merge(right, on="datetime_utc", suffixes=("_new", "_truth"))
    mismatched: list[str] = []
    max_abs_diffs: dict[str, float] = {}

    for col in cols:
        a = pd.to_numeric(merged[f"{col}_new"], errors="coerce")
        b = pd.to_numeric(merged[f"{col}_truth"], errors="coerce")
        both_nan = a.isna() & b.isna()
        comparable = ~both_nan
        if comparable.sum() == 0:
            max_abs_diffs[col] = 0.0
            continue
        diff = (a[comparable] - b[comparable]).abs()
        max_abs = float(diff.max()) if len(diff) else 0.0
        has_nan_mismatch = bool(((a[comparable].isna()) ^ (b[comparable].isna())).any())
        if max_abs > tolerance or has_nan_mismatch:
            mismatched.append(col)
        max_abs_diffs[col] = max_abs

    return CompareResult(
        rows_compared=len(merged),
        mismatched_columns=mismatched,
        max_abs_diffs=max_abs_diffs,
    )


def run_feature_day_check(
    raw_csv: Path | None,
    sqlite_db: Path | None,
    sqlite_table: str,
    truth_csv: Path,
    feature_day: pd.Timestamp,
    *,
    config: VolSignalBuildConfig,
    tolerance: float,
) -> CompareResult:
    raw_start = feature_day - pd.Timedelta(days=1)
    raw_end = feature_day + pd.Timedelta(days=1, minutes=16)
    raw = load_raw_window(
        raw_csv=raw_csv,
        sqlite_db=sqlite_db,
        sqlite_table=sqlite_table,
        start=raw_start,
        end=raw_end,
    )
    truth = load_window(
        truth_csv,
        start=feature_day,
        end=feature_day + pd.Timedelta(days=1),
        usecols=["datetime_utc", *FEATURE_COMPARE_COLS],
    )
    new_feat = prepare_feature_frame(raw, config)
    new_feat = new_feat[
        (new_feat["datetime_utc"] >= feature_day)
        & (new_feat["datetime_utc"] < feature_day + pd.Timedelta(days=1))
    ]
    return compare_frames(
        new_feat[["datetime_utc", *FEATURE_COMPARE_COLS]],
        truth[["datetime_utc", *FEATURE_COMPARE_COLS]],
        FEATURE_COMPARE_COLS,
        tolerance=tolerance,
    )


def run_prediction_day_check(
    raw_csv: Path | None,
    sqlite_db: Path | None,
    sqlite_table: str,
    truth_csv: Path,
    prediction_day: pd.Timestamp,
    *,
    config: VolSignalBuildConfig,
    tolerance: float,
) -> CompareResult:
    raw_start = prediction_day - pd.Timedelta(days=config.min_required_history_days)
    raw_end = prediction_day + pd.Timedelta(days=1)
    raw = load_raw_window(
        raw_csv=raw_csv,
        sqlite_db=sqlite_db,
        sqlite_table=sqlite_table,
        start=raw_start,
        end=raw_end,
    )
    truth = load_window(
        truth_csv,
        start=prediction_day,
        end=prediction_day + pd.Timedelta(days=1),
        usecols=["datetime_utc", *PREDICTION_COMPARE_COLS],
    )

    feature_df = prepare_feature_frame(raw, config)
    context_start_day = prediction_day - pd.Timedelta(days=1)
    scored = score_date_range_for_history(feature_df, context_start_day, prediction_day, config)
    scored = attach_signal_columns(scored, config)
    scored = scored[
        (scored["datetime_utc"] >= prediction_day)
        & (scored["datetime_utc"] < prediction_day + pd.Timedelta(days=1))
    ]
    return compare_frames(
        scored[["datetime_utc", *PREDICTION_COMPARE_COLS]],
        truth[["datetime_utc", *PREDICTION_COMPARE_COLS]],
        PREDICTION_COMPARE_COLS,
        tolerance=tolerance,
    )


def print_result(label: str, result: CompareResult) -> None:
    print(f"[{label}] rows_compared={result.rows_compared}")
    for col, max_abs in result.max_abs_diffs.items():
        status = "MISMATCH" if col in result.mismatched_columns else "ok"
        print(f"  {col}: max_abs_diff={max_abs:.12g} status={status}")


def main() -> int:
    args = parse_args()
    config = VolSignalBuildConfig()
    feature_day = pd.Timestamp(args.feature_day).normalize()
    prediction_day = pd.Timestamp(args.prediction_day).normalize()

    feature_result = run_feature_day_check(
        args.raw_csv if args.sqlite_db is None else None,
        args.sqlite_db,
        args.sqlite_table,
        args.truth_csv,
        feature_day,
        config=config,
        tolerance=args.tolerance,
    )
    print_result(f"feature_day {feature_day.date()}", feature_result)

    prediction_result = run_prediction_day_check(
        args.raw_csv if args.sqlite_db is None else None,
        args.sqlite_db,
        args.sqlite_table,
        args.truth_csv,
        prediction_day,
        config=config,
        tolerance=args.tolerance,
    )
    print_result(f"prediction_day {prediction_day.date()}", prediction_result)

    failures = feature_result.mismatched_columns + prediction_result.mismatched_columns
    if failures:
        print(f"\nFAILED: mismatched columns detected: {sorted(set(failures))}")
        return 1

    print("\nPASSED: parity check matched truth within tolerance")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

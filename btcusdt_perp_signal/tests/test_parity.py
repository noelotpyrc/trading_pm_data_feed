"""
Phase 2: Parity test — online (buffer replay) vs offline (batch) feature computation.

Takes a chunk of historical data from DB. Computes features two ways:
  1. Offline: compute_features on the entire DataFrame at once (truth).
  2. Online:  seed buffer with first 1800 rows, then append one row at a time
              and recompute features (simulates live engine).

Compares feature values on the overlapping rows. They should match exactly
since both use the same pandas rolling operations on the same data.

Usage:
    python -m btcusdt_perp_signal.tests.test_parity [--replay-bars N] [--db PATH]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from cex_data_feed.pipeline_1m.sqlite_db import read_last_n
from btcusdt_perp_signal.features import compute_features, check_signal
from btcusdt_perp_signal.signal_engine import HISTORY_BARS, FEATURE_COLS


def run_parity(db_path: Path, replay_bars: int, atol: float = 1e-10) -> bool:
    total_bars = HISTORY_BARS + replay_bars
    print(f"Loading {total_bars} bars from {db_path} ...")
    raw = read_last_n(db_path, total_bars)
    print(f"Got {len(raw)} bars ({raw['timestamp'].iloc[0]} → {raw['timestamp'].iloc[-1]})")

    if len(raw) < total_bars:
        print(f"WARNING: requested {total_bars} but only got {len(raw)}, "
              f"adjusting replay_bars to {len(raw) - HISTORY_BARS}")
        replay_bars = len(raw) - HISTORY_BARS
        if replay_bars <= 0:
            print("Not enough data for parity test.")
            return False

    # --- Offline (truth): batch compute on full data ---
    offline_df = compute_features(raw.copy())

    # --- Online (replay): buffer + append one at a time ---
    seed = raw.iloc[:HISTORY_BARS].copy()
    replay_rows = raw.iloc[HISTORY_BARS:]

    mismatches = 0
    signal_mismatches = 0
    total_checked = 0

    for i, (_, row) in enumerate(replay_rows.iterrows()):
        # Append to buffer
        new_row = row[["timestamp", "open", "high", "low", "close", "volume", "num_trades"]].to_dict()
        fresh = pd.DataFrame([new_row])
        seed = seed[seed["timestamp"] != new_row["timestamp"]]
        seed = pd.concat([seed, fresh], ignore_index=True)
        if len(seed) > HISTORY_BARS:
            seed = seed.iloc[-HISTORY_BARS:].reset_index(drop=True)

        # Compute features on buffer
        online_df = compute_features(seed.copy())
        online_latest = online_df.iloc[-1]

        # Get matching offline row
        offline_idx = HISTORY_BARS + i
        offline_latest = offline_df.iloc[offline_idx]

        # Compare
        ts = str(new_row["timestamp"])
        row_ok = True
        for col in FEATURE_COLS:
            online_val = online_latest.get(col)
            offline_val = offline_latest.get(col)

            both_nan = (pd.isna(online_val) and pd.isna(offline_val))
            if both_nan:
                continue

            if pd.isna(online_val) != pd.isna(offline_val):
                print(f"  MISMATCH [{ts}] {col}: online={online_val} offline={offline_val} (NaN vs value)")
                row_ok = False
                continue

            if not np.isclose(online_val, offline_val, atol=atol):
                print(f"  MISMATCH [{ts}] {col}: online={online_val:.10f} offline={offline_val:.10f}")
                row_ok = False

        # Compare signal direction
        online_signal = check_signal(online_latest)
        offline_signal = check_signal(offline_latest)
        if online_signal != offline_signal:
            print(f"  SIGNAL MISMATCH [{ts}]: online={online_signal} offline={offline_signal}")
            signal_mismatches += 1
            row_ok = False

        if not row_ok:
            mismatches += 1
        total_checked += 1

    # Summary
    print(f"\n=== Parity Test Results ===")
    print(f"  Bars replayed:      {total_checked}")
    print(f"  Feature mismatches: {mismatches}")
    print(f"  Signal mismatches:  {signal_mismatches}")

    if mismatches == 0 and signal_mismatches == 0:
        print("  PASS — online replay matches offline batch exactly.")
        return True
    else:
        print("  FAIL — see mismatches above.")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Parity test: online vs offline features")
    parser.add_argument("--replay-bars", type=int, default=100,
                        help="Number of bars to replay after warmup (default: 100)")
    parser.add_argument("--db", type=str,
                        default=str(ROOT / "data" / "btcusdt_perp_1m.sqlite"),
                        help="Path to OHLCV SQLite DB")
    args = parser.parse_args()

    ok = run_parity(Path(args.db), args.replay_bars)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

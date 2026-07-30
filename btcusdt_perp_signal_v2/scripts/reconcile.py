#!/usr/bin/env python3
"""
Offline reconciliation run — SPEC_dryrun_book11.md §7.

Replays the book over stored OHLCV bars and reports the pipeline-defect checks: regime share
(~21% expected), firing counts per cell, entries/day, and mean ret_research_bps. Optionally writes
the FIRING/ENTRY/RESULT records to the dry-run DB. This is the reference the live dry run reconciles
against; over a short local window the absolute numbers differ from §7 (different sample), but the
regime share and decile balance are period-robust sanity checks.

Usage:
    python -m btcusdt_perp_signal_v2.scripts.reconcile [--start YYYY-MM-DD] [--end YYYY-MM-DD]
                                                       [--ohlcv-db PATH] [--write] [--buffer N]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from btcusdt_perp_signal_v2 import backtest, config, records_db


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="btcusdt_perp_signal_v2 offline reconciliation")
    ap.add_argument("--ohlcv-db", type=Path, default=ROOT / config.OHLCV_DB_PATH)
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--buffer", type=int, default=config.BUFFER)
    ap.add_argument("--write", action="store_true", help="also write records to the dry-run DB")
    ap.add_argument("--out", type=Path, default=ROOT / config.DB_PATH)
    args = ap.parse_args(argv)

    df = backtest.load_bars(args.ohlcv_db, args.start, args.end)
    print(f"loaded {len(df):,} bars  {df['timestamp'].iloc[0]} -> {df['timestamp'].iloc[-1]}")
    if len(df) <= max(config.WBARS.values()):
        print(f"WARNING: {len(df):,} bars <= 180d window ({max(config.WBARS.values()):,}); "
              "no bar is armed. Load more history.")
        return 1

    res = backtest.replay(df, buffer=args.buffer)
    st = backtest.sanity_stats(res)

    armed_days = st["bars_armed"] / 1440.0
    print(f"\narmed bars: {st['bars_armed']:,}  (~{armed_days:.1f} days)")
    rs = st["regime_share"]
    print(f"regime share: {rs:.4f}  (expected ~0.21)" if rs is not None else "regime share: n/a")
    print(f"entries: {st['entries']}  (~{st['entries']/armed_days:.2f}/day; §7 expects ~0.97/day)")
    print(f"completed holds: {st['completed']}")
    if st["mean_ret_research_bps"] is not None:
        print(f"mean ret_research_bps: {st['mean_ret_research_bps']:+.2f}  "
              f"(§7 gross ~+27.7, net ~+21.7)")
    # time in position
    tip = st["completed"] * config.N_HORIZON / st["bars_armed"] if st["bars_armed"] else 0.0
    print(f"time in position: {tip*100:.2f}%  (§7 expects ~12.09%)")

    print("\nfirings by cell (all, taken+blocked):")
    for c in config.CELLS:
        n = st["firings_by_cell"].get(c.id, 0)
        print(f"  cell {c.id:>2} {c.side:>5} {c.window:>4}: {n:>6}  (~{n/armed_days:.2f}/day)")

    if args.write:
        records_db.ensure_tables(args.out)
        for f in res["firings"]:
            records_db.insert_firing(args.out, f)
        for e in res["entries"]:
            records_db.insert_entry(args.out, e)
        for r in res["results"]:
            records_db.insert_result(args.out, r)
        print(f"\nwrote {len(res['firings'])} firings / {len(res['entries'])} entries / "
              f"{len(res['results'])} results to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

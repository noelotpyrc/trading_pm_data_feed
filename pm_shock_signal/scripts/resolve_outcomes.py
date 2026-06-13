"""
Backfill `resolved_outcome` on shock_sim_trades (REVIEW item 5 — "a later pass").

For each window that has fired sim trades still missing a resolution and whose 15m
window has ended, look up which side won (Up/Down) from Gamma's resolved market
(`outcomePrices`) and write it. Self-contained — uses Gamma only (no Binance FAPI,
so it is unaffected by the FAPI geo-block that nulls `strike` in some locations).

Usage:
    /Users/noel/projects/venvs/production/bin/python -m pm_shock_signal.scripts.resolve_outcomes
    # one window only:
    ... -m pm_shock_signal.scripts.resolve_outcomes --epoch 1781305200
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pm_shock_signal import signal_db
from pm_btc15updown_data.collect_pm_btcupdown import GAMMA_BASE, EPOCH_S

DEFAULT_DB = ROOT / "data" / "pm_shock_signal.sqlite"


def fetch_outcome(epoch_start: int) -> str | None:
    """Resolved winner ('Up'/'Down') for the window, or None if not yet resolved."""
    slug = f"btc-updown-15m-{epoch_start}"
    url = f"{GAMMA_BASE}/events/keyset?slug={slug}"
    req = urllib.request.Request(url, headers={"User-Agent": "pm-shock-resolve/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"[resolve] gamma error for {slug}: {e}")
        return None

    events = data.get("events", []) if isinstance(data, dict) else []
    if not events or not events[0].get("markets"):
        return None
    m = events[0]["markets"][0]
    if not m.get("closed"):
        return None   # not resolved yet

    def _parse(v):
        return json.loads(v) if isinstance(v, str) else (v or [])

    outcomes = _parse(m.get("outcomes", "[]"))
    prices = [float(p) for p in _parse(m.get("outcomePrices", "[]"))]
    if not outcomes or len(prices) != len(outcomes):
        return None
    win_idx = max(range(len(prices)), key=lambda i: prices[i])
    if prices[win_idx] < 0.99:
        return None   # not decisively resolved
    label = str(outcomes[win_idx]).lower()
    return "Up" if label == "up" else "Down"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Backfill resolved_outcome on shock_sim_trades")
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--epoch", type=int, default=None, help="resolve only this epoch_start")
    args = p.parse_args(argv)

    if args.epoch is not None:
        epochs = [args.epoch]
    else:
        epochs = signal_db.epochs_needing_resolution(args.db)

    now = int(time.time())
    updated = skipped = 0
    for ep in epochs:
        if ep + EPOCH_S > now:
            skipped += 1
            continue   # window not closed yet
        outcome = fetch_outcome(ep)
        if outcome is None:
            print(f"[resolve] {ep}: not resolved yet — skipping")
            skipped += 1
            continue
        n = signal_db.set_resolved_outcome(args.db, ep, outcome)
        updated += n
        print(f"[resolve] {ep}: {outcome} → {n} trades")

    print(f"[resolve] done: {updated} trades updated, {skipped} windows skipped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

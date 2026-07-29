#!/usr/bin/env python3
"""
Engine loop for pm_signal_sim (BUILD_SPEC §4).

~1s wall-tick engine (same pattern as pm_shock_signal). Each tick: resolve the active 15-min window;
update the per-second grid; run MultiDetector; on a fire open/extend the capture + write the fire row;
stage forward raw records. At each window roll, resolve the closing window (final/winner/pin from the
detector grid — robust to tick drift), flush its captures, and sweep Discord.

Runs in its OWN process / tmux / DB. Does NOT touch pm_shock_signal or pm_asym_signal.

Usage: python -m pm_signal_sim.scripts.run_signal_sim [--dry-run] [--db PATH]
"""
from __future__ import annotations

import argparse
import logging
import os
import signal as signal_mod
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pm_signal_sim import config, signal_db, signals_live
from pm_signal_sim.config import SignalConfig
from pm_signal_sim.capture import CaptureManager
from pm_signal_sim.discord_report import sweep_and_alert
from pm_signal_sim.feeds import BtcMidFeed, PmTokenFeed, BtcDepth20Feed
from pm_signal_sim.signals import MultiDetector
from btcusdt_perp_signal.alert import _load_env  # type: ignore  # loads .env (webhooks)

log = logging.getLogger("pm_signal_sim")
LOG_FILE = ROOT / "data" / "pm_signal_sim.log"
_BTC_DEPTH_BUFFER_SEC = config.L_BACK_SEC + 60   # ≥ lookback so the fire's depth slice is buffered

_shutdown = False


def _stop(sig, _frame):
    global _shutdown
    log.info("Caught %s, shutting down...", signal_mod.Signals(sig).name)
    _shutdown = True


def _setup_logging() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in (logging.FileHandler(LOG_FILE), logging.StreamHandler()):
        h.setFormatter(fmt)
        root.addHandler(h)


def _resolve_window(db: Path, detector: MultiDetector, epoch: int, fired_tokens, now: float) -> None:
    """Persist resolution for the closing epoch's fired tokens (report_20 rule, RESOLVE_MIN)."""
    finals = {tok: detector.final_pdet(epoch, tok) for tok in ("Up", "Down")}
    finals = {k: v for k, v in finals.items() if v is not None}
    if not finals or not fired_tokens:
        return
    mx = max(finals.values())
    resolved = 1 if mx >= config.RESOLVE_MIN else 0
    win = max(finals, key=finals.get)
    for tok in fired_tokens:
        fin = finals.get(tok)
        if fin is None:
            continue
        signal_db.insert_resolution(db, epoch, tok, fin, int(tok == win), resolved, now)
        log.info("RESOLVE %s %s final=%.3f winner=%s resolved=%d",
                 epoch, tok, fin, tok == win, resolved)


def _drain_pending(db: Path, btc, pending: list, latency: dict, now: float) -> None:
    """Evaluate any in-scope fire's S1/S2 (both due fire+3) and fill (due fire+5) whose horizon has
    passed. `decided_at = max(newest input local_ts, fire+3)` (stamped by the evaluator) — the honest
    time the value is knowable. S1/fill read the same book rows offline reads; S2 reads BTC mid."""
    remaining = []
    for p in pending:
        fe = p["fe"]
        if not p["s2_done"] and now >= p["s2_due"]:
            ev = signals_live.eval_s2(fe, btc)
            ev.eval_wall_ts = now
            signal_db.insert_signal_eval(db, p["fire_id"], ev)
            if ev.decided_at is not None:
                p["decided_ats"].append(ev.decided_at)
                latency.setdefault("z30_gate", []).append(ev.decided_at - fe.local_ts)
            p["s2_done"] = True
        if not p["s1_done"] and now >= p["s1_due"]:
            ev = signals_live.eval_s1(fe, signal_db.fetch_book_full(db, p["cid"]))
            ev.eval_wall_ts = now
            signal_db.insert_signal_eval(db, p["fire_id"], ev)
            if ev.decided_at is not None:
                p["decided_ats"].append(ev.decided_at)
                latency.setdefault("fade", []).append(
                    ev.decided_at - (fe.local_ts + config.S1_HORIZON_S))
            p["s1_done"] = True
        if not p["fill_done"] and now >= p["fill_due"]:
            fill = signals_live.compute_fill(fe, signal_db.fetch_book_full(db, p["cid"]))
            if fill is not None:
                fill_ts, fill_ask, fill_ask_sz = fill
                dats = [d for d in p["decided_ats"] if d is not None]
                margin = (fill_ts - max(dats)) if dats else None
                signal_db.insert_fill_log(db, p["fire_id"], fill_ts, fill_ask, fill_ask_sz, margin)
            p["fill_done"] = True
        if not (p["s1_done"] and p["s2_done"] and p["fill_done"]):
            remaining.append(p)
    pending[:] = remaining


def _log_heartbeat(latency: dict) -> None:
    """Per-signal decision-latency histogram (LIVE_TEST_SPEC §3): fade = decided_at−(fire+3),
    z30_gate = decided_at−fire. Then reset the buckets."""
    parts = []
    for sig in ("z30_gate", "fade"):
        xs = sorted(latency.get(sig, []))
        if xs:
            p50 = xs[len(xs) // 2]
            p90 = xs[min(len(xs) - 1, int(len(xs) * 0.9))]
            parts.append(f"{sig}: n={len(xs)} p50={p50:.2f} p90={p90:.2f} max={xs[-1]:.2f}")
        latency[sig] = []
    log.info("HEARTBEAT latency %s", " · ".join(parts) if parts else "(no in-scope fires this period)")


def _relaxed_configs() -> list[SignalConfig]:
    """Dev-only near-trivial thresholds (k≈1.01, no level floor) to force fires for the §8 E2E /
    capture check. NOT for real data."""
    return [SignalConfig(c.config_id, c.kind, c.params, k_collect=1.01, p_floor=0.0,
                         cooldown_s=c.cooldown_s) for c in config.CONFIGS]


def run(db: Path, dry_run: bool, relax: bool = False) -> int:
    signal_db.ensure_tables(db)
    log.info("DB ready at %s", db)

    btc = BtcMidFeed()
    pm = PmTokenFeed()
    btc_depth = BtcDepth20Feed(config.BTC_DEPTH_WS_URL, _BTC_DEPTH_BUFFER_SEC)
    btc.start()
    pm.start()
    btc_depth.start()

    detector = MultiDetector(pm, btc, configs=_relaxed_configs() if relax else None)
    if relax:
        log.warning("RELAXED dev thresholds active (k=1.01, no p-floor) — E2E/capture test only")
    capture = CaptureManager(db, pm, btc, btc_depth)

    signal_mod.signal(signal_mod.SIGINT, _stop)
    signal_mod.signal(signal_mod.SIGTERM, _stop)
    sweep_and_alert(db, dry_run=dry_run)   # catch up any unalerted windows from a prior run
    log.info("Engine started (dry_run=%s, configs=%s)", dry_run, [c.config_id for c in config.CONFIGS])

    last_epoch = None
    pending: list = []          # in-scope fires awaiting S1 (fire+3) / fill (fire+5) evaluation
    latency: dict = {"z30_gate": [], "fade": []}
    strike_epochs: set = set()  # epochs with an epoch_strike row written
    last_heartbeat = time.time()
    while not _shutdown:
        loop_start = time.time()
        try:
            now = time.time()
            epoch = (int(now) // config.WINDOW_SEC) * config.WINDOW_SEC

            if epoch != last_epoch:
                if last_epoch is not None:
                    fired = [tok for (e, tok) in list(capture._cap) if e == last_epoch]
                    _resolve_window(db, detector, last_epoch, fired, now)
                    capture.on_resolution(last_epoch)
                    _drain_pending(db, btc, pending, latency, now)   # settle stragglers before dropping
                    pending[:] = [p for p in pending if p["fe"].epoch_start != last_epoch]
                    sweep_and_alert(db, dry_run=dry_run)
                pm.roll_market(epoch)
                last_epoch = epoch

            market = pm.market
            if market is None:
                continue
            sec = int(now) - epoch
            tokens = [("Up", market.up_token_id), ("Down", market.down_token_id)]

            # one epoch_strike row per epoch the engine is up, at epoch start (once BTC mid warms).
            if epoch not in strike_epochs:
                mid = btc.mid_now()
                if mid is not None:
                    signal_db.insert_epoch_strike(db, epoch, now, now, mid)
                    strike_epochs.add(epoch)

            if 0 <= sec < config.WINDOW_SEC:
                detector.update_grid(epoch, sec, tokens)
                for fe in detector.on_tick(now, epoch, sec, tokens):
                    cid = capture.on_fire(fe)
                    fire_id = signal_db.insert_fire(db, cid, fe)
                    log.info("FIRE [%s] %s sec=%d ratio=%.3f p=%.3f ask=%s",
                             fe.config_id, fe.token, fe.sec, fe.ratio, fe.p_entry, fe.entry_ask)
                    if signals_live.in_scope(fe.config_id, fe.sec):
                        # S1 + S2 both decide at fire+3 (evaluated from the pending queue); fill+5.
                        horizon_due = fe.local_ts + config.S1_HORIZON_S + config.EVAL_GUARD_S
                        pending.append({
                            "fire_id": fire_id, "fe": fe, "cid": cid,
                            "s1_due": horizon_due, "s2_due": horizon_due,
                            "fill_due": fe.local_ts + config.FILL_DELAY_S + config.EVAL_GUARD_S,
                            "s1_done": False, "s2_done": False, "fill_done": False,
                            "decided_ats": []})
                capture.on_tick(now)
                _drain_pending(db, btc, pending, latency, now)

            if now - last_heartbeat >= config.HEARTBEAT_SEC:
                _log_heartbeat(latency)
                last_heartbeat = now
        except Exception:
            log.exception("engine tick error")

        elapsed = time.time() - loop_start
        if not _shutdown and elapsed < config.LOOP_INTERVAL_S:
            time.sleep(config.LOOP_INTERVAL_S - elapsed)

    btc.stop()
    pm.stop()
    btc_depth.stop()
    log.info("Engine stopped. Open captures: %d", len(capture._cap))
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="PM multi-def signal collector + sim")
    ap.add_argument("--dry-run", action="store_true",
                    help="log Discord alerts instead of posting; still writes DB + raw slices")
    ap.add_argument("--db", type=Path, default=None, help="override DB path")
    ap.add_argument("--relax", action="store_true",
                    help="dev: near-trivial thresholds to force fires (E2E/capture test, not real data)")
    args = ap.parse_args(argv)

    os.chdir(ROOT)   # relative paths (config.DB_PATH) resolve under the repo root
    _setup_logging()
    _load_env()
    db = args.db if args.db is not None else (ROOT / config.DB_PATH)
    return run(db, args.dry_run, relax=args.relax)


if __name__ == "__main__":
    raise SystemExit(main())

"""
Entry point for the PM shock signal engine.

Wires feeds → detector → sim → db + alert in one ~1s loop. Mirrors the shape of
btcusdt_perp_signal/signal_engine.py and pm_btc15updown_data/signal_stream_v3_directional.py
(env load, logging, graceful shutdown). See BUILD_SPEC §9, §11.

Usage:
    # local dev/test (NEVER auto-deploy to VPS — repo CLAUDE.md)
    /Users/noel/projects/venvs/production/bin/python -m pm_shock_signal.scripts.run_shock_signal --dry-run
    # VPS (only after explicit user confirmation)
    .venv/bin/python -m pm_shock_signal.scripts.run_shock_signal

Env (.env):
    DISCORD_WEBHOOK_URL_PM_SHOCK  — Discord channel (falls back to DISCORD_WEBHOOK_URL)
    PM_SHOCK_DB_PATH              — signals/sim SQLite (default: data/pm_shock_signal.sqlite)
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Project root on path (like the sibling runner)
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pm_shock_signal import alert, config, signal_db
from pm_shock_signal.config import OperatingPoint
from pm_shock_signal.feeds import BtcMidFeed, PmTokenFeed
from pm_shock_signal.shock_signal import ShockDetector
from pm_shock_signal.sim import SimPositionManager
from pm_btc15updown_data.collect_pm_btcupdown import current_epoch_ts
from btcusdt_perp_signal.alert import _load_env

log = logging.getLogger("pm_shock_signal")

LOOP_INTERVAL_S = 1.0
DEFAULT_DB = ROOT / "data" / "pm_shock_signal.sqlite"
LOG_FILE = ROOT / "data" / "pm_shock_signal.log"

# Dev-only near-trivial thresholds so the engine fires within a short window — for
# the BUILD_SPEC §10 E2E check (write a real signal+trade pair). NOT for real data.
RELAXED_OPS = [OperatingPoint("relax_dev", delta=5, k=1.01, z_thr=0.0, exit_tau=30)]

_shutdown = False


def _handle_signal(sig, _frame):
    global _shutdown
    log.info("Caught %s, shutting down...", signal.Signals(sig).name)
    _shutdown = True


def _ts_str(epoch_sec: float) -> str:
    return datetime.fromtimestamp(epoch_sec, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _setup_logging() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)


def _resolve_db(arg_db: Path | None) -> Path:
    if arg_db is not None:
        return arg_db
    env = os.environ.get("PM_SHOCK_DB_PATH")
    return Path(env) if env else DEFAULT_DB


def run(db_path: Path, dry_run: bool, relax: bool = False) -> int:
    signal_db.ensure_tables(db_path)
    log.info("DB ready at %s", db_path)

    btc = BtcMidFeed()
    pm = PmTokenFeed()
    btc.start()
    pm.start()

    # Resolve the first market so the detector has tokens immediately.
    pm.roll_market(current_epoch_ts())
    last_epoch = pm.market.epoch_start if pm.market else None

    ops = RELAXED_OPS if relax else None
    detector = ShockDetector(btc, pm, ops=ops)
    sim = SimPositionManager(pm)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    if relax:
        log.warning("RELAXED dev thresholds active (k=1.01, z_thr=0) — E2E test only, not real data")
    log.info("Engine started (dry_run=%s, configs=%s)",
             dry_run, [op.config_id for op in detector.ops])

    while not _shutdown:
        loop_start = time.time()
        try:
            epoch = current_epoch_ts()
            if epoch != last_epoch:
                pm.roll_market(epoch)
                last_epoch = epoch

            now = time.time()

            for fire in detector.on_tick(now):
                sid = signal_db.insert_signal(
                    db_path,
                    config_id=fire.config_id, epoch_start=fire.epoch_start,
                    fire_ts=_ts_str(fire.fire_ts), sec_into_window=fire.sec_into_window,
                    token=fire.token, delta=fire.delta, k=fire.k, z_thr=fire.z_thr,
                    back_ratio=fire.back_ratio, z_shock=fire.z_shock, p_shock=fire.p_shock,
                    entry_ask=fire.entry_ask, entry_bid=fire.entry_bid, rv_60s=fire.rv_60s,
                    entry_mid=fire.entry_mid, mid_prev=fire.mid_prev,
                    mid_event_ts=fire.mid_event_ts, mid_prev_event_ts=fire.mid_prev_event_ts,
                    pm_event_ts=fire.pm_event_ts, receipt_ts=fire.receipt_ts,
                    entry_last_age_s=fire.entry_last_age_s, strike=fire.strike,
                )
                log.info("FIRE [%s] %s sec=%d back_ratio=%.3f z=%.2f p_shock=%.4f ask=%s "
                         "lat=%.2fs",
                         fire.config_id, fire.token, fire.sec_into_window,
                         fire.back_ratio, fire.z_shock, fire.p_shock, fire.entry_ask,
                         fire.receipt_ts - fire.pm_event_ts)
                if alert.send_entry(fire, dry_run=dry_run):
                    signal_db.mark_signal_alerted(db_path, sid)
                sim.open_position(fire, sid)

            for trade in sim.close_due(now):
                tid = signal_db.insert_sim_trade(
                    db_path,
                    signal_id=trade.signal_id, config_id=trade.config_id,
                    entry_ts=_ts_str(trade.entry_ts), entry_last=trade.entry_last,
                    entry_ask=trade.entry_ask, exit_ts=_ts_str(trade.exit_ts),
                    exit_sec=trade.exit_sec, exit_last=trade.exit_last,
                    exit_bid=trade.exit_bid, exit_ask=trade.exit_ask,
                    ttl_capped=trade.ttl_capped, pnl_gross=trade.pnl_gross,
                    pnl_net=trade.pnl_net, roi_net=trade.roi_net,
                    exit_last_age_s=trade.exit_last_age_s,
                    exit_book_age_s=trade.exit_book_age_s,
                )
                log.info("EXIT [%s] exit_last=%s exit_bid=%s pnl_gross=%s pnl_net=%s",
                         trade.config_id, trade.exit_last, trade.exit_bid,
                         trade.pnl_gross, trade.pnl_net)
                if alert.send_exit(trade, dry_run=dry_run):
                    signal_db.mark_trade_exit_alerted(db_path, tid)
        except Exception:
            log.exception("Error in engine loop tick")

        elapsed = time.time() - loop_start
        if not _shutdown:
            time.sleep(max(0.0, LOOP_INTERVAL_S - elapsed))

    btc.stop()
    pm.stop()
    log.info("Engine stopped. Open positions left unclosed: %d", sim.open_count())
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="PM shock signal engine (sim + Discord)")
    parser.add_argument("--dry-run", action="store_true",
                        help="log alerts instead of posting to Discord; still writes DB")
    parser.add_argument("--db", type=Path, default=None,
                        help="override PM_SHOCK_DB_PATH")
    parser.add_argument("--relax", action="store_true",
                        help="dev: near-trivial thresholds to force a fire (E2E test, not real data)")
    args = parser.parse_args(argv)

    _setup_logging()
    _load_env()
    db_path = _resolve_db(args.db)
    return run(db_path, args.dry_run, relax=args.relax)


if __name__ == "__main__":
    raise SystemExit(main())

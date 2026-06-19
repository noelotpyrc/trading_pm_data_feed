"""
Batched, merged-per-window Discord reporter (BUILD_SPEC §6, Q2).

NOT real-time: a sweep runs at/after each window resolution, finds resolved-but-unalerted firing
windows, and sends ONE merged message per (epoch, token) — all configs that fired + the outcome —
then marks it alerted. Built from the DB → complete & deduped across restarts; retried (a failed send
leaves window_alerted=0 → resent next sweep) → 100% delivery.

Transport reuses pm_shock_signal.alert.send_discord (urllib; send_discord(message, env_key=...)).
Webhook env key DISCORD_WEBHOOK_URL_PM_SIGNAL_SIM, fallback DISCORD_WEBHOOK_URL_PM_SHOCK.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from pm_signal_sim import config, signal_db
from pm_shock_signal.alert import send_discord, _load_env  # type: ignore  # reuse transport

log = logging.getLogger(__name__)
_FALLBACK_KEY = "DISCORD_WEBHOOK_URL_PM_SHOCK"


def resolve_key() -> str:
    _load_env()
    return config.DISCORD_ENV_KEY if os.environ.get(config.DISCORD_ENV_KEY) else _FALLBACK_KEY


def _webhook_configured() -> bool:
    _load_env()
    return bool(os.environ.get(config.DISCORD_ENV_KEY) or os.environ.get(_FALLBACK_KEY))


def _pnl_at_tau(fire, tau, book_path, settle):
    """(net, gross) for exiting `fire` at +tau seconds. net = exit_bid − entry_ask (sell into the bid);
    gross = exit_mid − p_entry. Exit price = first book sample at/after fire.local_ts+tau. Falls back to
    settlement when the τ exit would land past window end (rides to resolution) or no book sample exists
    — matching the offline resolution-fill. (None, None) if no usable price."""
    entry_ask = fire["entry_ask"]
    p = fire["p_entry"]
    if (fire["sec"] + tau) < config.WINDOW_END_SEC:
        exit_local = fire["local_ts"] + tau
        for lts, bid, ask in book_path:
            if lts >= exit_local:
                mid = (bid + ask) / 2 if (bid is not None and ask is not None) else None
                net = (bid - entry_ask) if (bid is not None and entry_ask is not None) else None
                gross = (mid - p) if (mid is not None and p is not None) else None
                return net, gross
    return _pnl_settle(fire, settle)   # too late to reach τ, or no book sample → ride to resolution


def _pnl_settle(fire, settle):
    """(net, gross) held to expiry: settle (1/0) − entry_ask / − p_entry. (None, None) on a pin."""
    if settle is None:
        return None, None
    net = (settle - fire["entry_ask"]) if fire["entry_ask"] is not None else None
    gross = (settle - fire["p_entry"]) if fire["p_entry"] is not None else None
    return net, gross


def _seg(label, net, gross) -> str:
    if net is None and gross is None:
        return f"{label}: n/a"
    parts = []
    if net is not None:
        parts.append(f"net={net:+.3f}")
    if gross is not None:
        parts.append(f"gross={gross:+.3f}")
    return f"{label}: " + " ".join(parts)


def _fmt_window(res, fires, book_path) -> str:
    """One merged message: configs that fired + per-config entry refs + outcome, with P&L at each
    τ in config.DISCORD_EXIT_TAUS AND hold-to-expiry (net + gross) so the scalp-exit vs held-to-settle
    gap is visible (REVIEW P1b). τ exits use the captured raw_pm_book; expiry uses 1/0 settlement."""
    epoch = res["epoch_start"]
    token = res["token"]
    ts = datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    if not res["resolved"]:
        outcome, settle = "PIN", None
    elif res["winner"]:
        outcome, settle = "WIN ✅", 1.0
    else:
        outcome, settle = "LOSS ❌", 0.0

    final = res["final_price"]
    lines = [
        f"🎯 **15updown {ts} UTC** · token **{token}** · {outcome}"
        + (f" · final `{final:.3f}`" if final is not None else ""),
    ]
    for f in sorted(fires, key=lambda r: r["sec"]):
        head = f"  `{f['config_id']}` sec={f['sec']} ratio={f['ratio']:.3f}"
        if f["p_entry"] is not None:
            head += f" p={f['p_entry']:.3f}"
        if f["entry_ask"] is not None:
            head += f" ask={f['entry_ask']:.3f}"
        lines.append(head)
        segs = [_seg(f"τ{tau}", *_pnl_at_tau(f, tau, book_path, settle))
                for tau in config.DISCORD_EXIT_TAUS]
        segs.append(_seg("exp", *_pnl_settle(f, settle)))
        lines.append("    " + " · ".join(segs))
    return "\n".join(lines)


def sweep_and_alert(db_path: Path, dry_run: bool = False) -> int:
    """Send one merged message per resolved-unalerted firing window. Returns count sent."""
    configured = _webhook_configured()
    sent = 0
    for epoch, token in signal_db.unalerted_resolved_windows(db_path):
        res, fires = signal_db.fetch_window_for_alert(db_path, epoch, token)
        if res is None or not fires:
            continue
        book_path = signal_db.fetch_book_path(db_path, fires[0]["capture_id"])  # shared per (epoch,token)
        msg = _fmt_window(res, fires, book_path)
        if dry_run:
            # non-destructive: log only, do NOT mark — the window stays unalerted for a real run later
            log.info("[dry-run] %s", msg.replace("\n", " | "))
            continue
        if not configured:
            # real run, no webhook → mark anyway (logged) so we don't retry forever
            log.warning("no PM_SIGNAL_SIM webhook configured; marking alerted: %s",
                        msg.replace("\n", " | "))
            signal_db.mark_window_alerted(db_path, epoch, token)
            continue
        if send_discord(msg, env_key=resolve_key()):
            signal_db.mark_window_alerted(db_path, epoch, token)
            sent += 1
    return sent

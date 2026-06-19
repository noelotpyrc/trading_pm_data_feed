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


def _fmt_window(res, fires) -> str:
    """One merged message: configs that fired + per-config entry refs + outcome + settlement PnL.

    15updown tokens settle to 1.0 (win) / 0.0 (loss), so hold-to-expiry gross/net are exact from the
    resolution + each fire's p_entry/entry_ask. τ=60 and other-τ fills are recomputed offline from the
    raw_pm_* slice (not in the message).
    """
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
        pe = f["p_entry"]
        ask = f["entry_ask"]
        seg = f"  `{f['config_id']}` sec={f['sec']} ratio={f['ratio']:.3f}"
        if pe is not None:
            seg += f" p={pe:.3f}"
        if ask is not None:
            seg += f" ask={ask:.3f}"
        if settle is not None:
            g = (settle - pe) if pe is not None else None
            n = (settle - ask) if ask is not None else None
            if g is not None:
                seg += f" → gross_exp={g:+.3f}"
            if n is not None:
                seg += f" net_exp={n:+.3f}"
        lines.append(seg)
    return "\n".join(lines)


def sweep_and_alert(db_path: Path, dry_run: bool = False) -> int:
    """Send one merged message per resolved-unalerted firing window. Returns count sent."""
    configured = _webhook_configured()
    sent = 0
    for epoch, token in signal_db.unalerted_resolved_windows(db_path):
        res, fires = signal_db.fetch_window_for_alert(db_path, epoch, token)
        if res is None or not fires:
            continue
        msg = _fmt_window(res, fires)
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

"""
Discord formatting for shock entry/exit alerts.

Transport is reused: `from btcusdt_perp_signal.alert import send_discord`.
This module only builds the message strings and picks the channel env key
(DISCORD_WEBHOOK_URL_PM_SHOCK, falling back to DISCORD_WEBHOOK_URL). See BUILD_SPEC §7.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from pm_shock_signal import config
from pm_shock_signal.shock_signal import FireEvent
from pm_shock_signal.sim import SimTrade

from btcusdt_perp_signal.alert import send_discord, _load_env

log = logging.getLogger(__name__)


def _resolve_key() -> str:
    """Primary PM-shock channel if configured, else the shared default."""
    _load_env()
    if os.environ.get(config.DISCORD_ENV_KEY):
        return config.DISCORD_ENV_KEY
    return "DISCORD_WEBHOOK_URL"


def _f(x, nd: int = 4) -> str:
    return f"{x:.{nd}f}" if isinstance(x, (int, float)) else "?"


def _hhmm(epoch_start: int) -> str:
    return datetime.fromtimestamp(epoch_start, tz=timezone.utc).strftime("%H:%M UTC")


def format_entry(fire: FireEvent) -> str:
    """🟢 PM SHOCK FIRED [config_id] — short (BUILD_SPEC §7)."""
    tau = fire.exit_tau
    return (
        f"\U0001f7e2 **PM SHOCK FIRED** [{fire.config_id}]\n"
        f"{_hhmm(fire.epoch_start)} window · sec={fire.sec_into_window} · {fire.token}\n"
        f"Δ{fire.delta}/k{fire.k} back_ratio=`{_f(fire.back_ratio, 3)}` "
        f"z_shock=`{_f(fire.z_shock, 2)}`\n"
        f"p_shock=`{_f(fire.p_shock)}` entry_ask=`{_f(fire.entry_ask)}` → exit τ={tau}s"
    )


def format_exit(trade: SimTrade) -> str:
    """⬜ SHOCK EXIT [config_id] — short (BUILD_SPEC §7)."""
    hold_s = int(round(trade.exit_ts - trade.entry_ts))
    ttl = " [TTL-capped]" if trade.ttl_capped else ""
    roi = f"{trade.roi_net:+.2%}" if trade.roi_net is not None else "?"
    return (
        f"⬜ **SHOCK EXIT** [{trade.config_id}]{ttl}\n"
        f"exit last=`{_f(trade.exit_last)}` bid=`{_f(trade.exit_bid)}` · hold {hold_s}s\n"
        f"pnl_gross=`{_f(trade.pnl_gross)}` pnl_net=`{_f(trade.pnl_net)}` roi_net=`{roi}`"
    )


def send_entry(fire: FireEvent, dry_run: bool = False) -> bool:
    """Post the entry alert. Return True on success (or on dry-run log)."""
    msg = format_entry(fire)
    if dry_run:
        log.info("[dry-run] %s", msg.replace("\n", " | "))
        return True
    return send_discord(msg, env_key=_resolve_key())


def send_exit(trade: SimTrade, dry_run: bool = False) -> bool:
    """Post the exit alert (with realized sim PnL). Return True on success."""
    msg = format_exit(trade)
    if dry_run:
        log.info("[dry-run] %s", msg.replace("\n", " | "))
        return True
    return send_discord(msg, env_key=_resolve_key())

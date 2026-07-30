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


def _window_passes(sig_evals: dict) -> bool:
    """True iff ≥1 in-scope fire passed S1 or S2 (LIVE_TEST_SPEC §2 filter)."""
    return any(e["decision"] for evs in sig_evals.values() for e in evs)


def _sig_block(f, evals, fill) -> str:
    """Per-fire signal line (LIVE_TEST_SPEC §2 content): which signal(s) passed, their values,
    decided_at (as +Δs from the fire), and the realized ask_d5 fill + tradability margin."""
    by = {e["signal"]: e for e in evals}
    segs = []
    fade = by.get("fade")
    if fade is not None:
        v = fade["value"]
        segs.append(("fade✓ " if fade["decision"] else "fade✗ ")
                    + (f"d_mid3={v:+.4f}" if v is not None else "d_mid3=n/a"))
    z = by.get("z30_gate")
    if z is not None:
        v = z["value"]
        mark = "✓" if z["decision"] else "✗"
        segs.append((f"z30={v:+.2f}{mark}" if v is not None else f"z30=n/a{mark}"))
    if fill is not None:
        seg = "d5=" + (f"{fill['fill_ask']:.3f}" if fill["fill_ask"] is not None else "n/a")
        if fill["fill_ask_sz"] is not None:
            seg += f"×{fill['fill_ask_sz']:.0f}"
        if fill["margin_s"] is not None:
            seg += f" mgn{fill['margin_s']:+.1f}s"
        segs.append(seg)
    dats = [e["decided_at"] for e in evals if e["decided_at"] is not None]
    if dats:
        segs.append(f"dec@+{max(dats) - f['local_ts']:.1f}s")
    return "    ▸ " + " · ".join(segs)


def _fmt_window(res, fires, book_path, sig_evals=None, fills=None) -> str:
    """One merged message: configs that fired + per-config entry refs + outcome, with P&L at each
    τ in config.DISCORD_EXIT_TAUS AND hold-to-expiry (net + gross) so the scalp-exit vs held-to-settle
    gap is visible (REVIEW P1b). τ exits use the captured raw_pm_book; expiry uses 1/0 settlement.
    In-scope fires also get a signal block (LIVE_TEST_SPEC §2): S1/S2 pass, values, decided_at, fill."""
    sig_evals = sig_evals or {}
    fills = fills or {}
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
    # asym_2_5_40_k150 is out of scope (no S1/S2 applied) — keep it firing/recording but off the report.
    reportable = [f for f in fires if f["config_id"] in config.SIGNAL_SCOPE_CONFIGS]
    for f in sorted(reportable, key=lambda r: r["sec"]):
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
        evals = sig_evals.get(f["id"]) if sig_evals else None
        if evals:                                   # in-scope fire → signal block
            lines.append(_sig_block(f, evals, fills.get(f["id"])))
    return "\n".join(lines)


def sweep_and_alert(db_path: Path, dry_run: bool = False) -> int:
    """Send one merged message per resolved-unalerted firing window that has ≥1 in-scope fire passing
    S1 or S2 (LIVE_TEST_SPEC §2). Windows with fires but no passing signal are marked silently.
    Returns count sent."""
    configured = _webhook_configured()
    sent = 0
    for epoch, token in signal_db.unalerted_resolved_windows(db_path):
        res, fires = signal_db.fetch_window_for_alert(db_path, epoch, token)
        if res is None or not fires:
            continue
        book_path = signal_db.fetch_book_path(db_path, fires[0]["capture_id"])  # shared per (epoch,token)
        sig_evals = signal_db.fetch_signal_evals_for_window(db_path, epoch, token)
        fills = signal_db.fetch_fill_log_for_window(db_path, epoch, token)
        passes = _window_passes(sig_evals)
        msg = _fmt_window(res, fires, book_path, sig_evals, fills)
        if dry_run:
            # non-destructive: log only, do NOT mark — the window stays unalerted for a real run later
            log.info("[dry-run]%s %s", "" if passes else " (filtered: no S1/S2 pass)",
                     msg.replace("\n", " | "))
            continue
        if not passes:
            # fires but no passing S1/S2 (or no in-scope fire) → mark silently, send nothing
            signal_db.mark_window_alerted(db_path, epoch, token)
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

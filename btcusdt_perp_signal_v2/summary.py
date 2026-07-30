"""
Daily summary (§6.4): entries, firings by cell (taken + blocked), time in position, mean returns and
execution cost per notional rung, deadline misses. Posted once per UTC day to the DISCORD_WEBHOOK_URL
webhook.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from btcusdt_perp_signal_v2 import config


def _day_bounds_ms(day: str):
    import datetime as _dt
    d = _dt.datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=_dt.timezone.utc)
    start = int(d.timestamp() * 1000)
    return start, start + 1440 * 60_000


def _mean_by_rung(rows) -> list:
    n = len(config.NOTIONALS)
    sums = [0.0] * n
    counts = [0] * n
    for r in rows:
        if not r:
            continue
        vals = json.loads(r)
        for i in range(min(n, len(vals))):
            if vals[i] is not None:
                sums[i] += vals[i]
                counts[i] += 1
    return [(sums[i] / counts[i]) if counts[i] else None for i in range(n)]


def build_daily_summary(db_path: Path, day: str) -> dict:
    start, end = _day_bounds_ms(day)
    start_bar, end_bar = start // 60_000, end // 60_000
    con = sqlite3.connect(str(db_path))
    try:
        entries = con.execute(
            "SELECT COUNT(*) FROM entries_v2 WHERE ts_bar_close >= ? AND ts_bar_close < ?",
            (start, end)).fetchone()[0]
        fire_rows = con.execute(
            """SELECT cell_id, SUM(taken), COUNT(*) FROM firings_v2
               WHERE ts_bar_close >= ? AND ts_bar_close < ? GROUP BY cell_id""",
            (start, end)).fetchall()
        deadline_misses = con.execute(
            "SELECT COUNT(*) FROM firings_v2 WHERE ts_bar_close >= ? AND ts_bar_close < ? AND late_ms > ?",
            (start, end, int(config.DEADLINE_S * 1000))).fetchone()[0]
        res = con.execute(
            """SELECT ret_research_bps, ret_exec_bps, cost_bps, entry_bar_index, exit_bar_index
               FROM results_v2 WHERE status = 'ok' AND ts_exit >= ? AND ts_exit < ?""",
            (start, end)).fetchall()
    finally:
        con.close()

    firings_by_cell = {str(cid): {"taken": int(tk or 0), "total": int(tot)}
                       for cid, tk, tot in fire_rows}
    rr = [r[0] for r in res if r[0] is not None]
    in_pos = sum(max(0, min(r[4], end_bar) - max(r[3], start_bar)) for r in res)
    return {
        "entries": entries,
        "firings_by_cell": firings_by_cell,
        "time_in_position_pct": round(in_pos / 1440.0 * 100, 2),
        "mean_ret_research_bps": (sum(rr) / len(rr)) if rr else None,
        "mean_ret_exec_bps": _mean_by_rung([r[1] for r in res]),
        "mean_cost_bps": _mean_by_rung([r[2] for r in res]),
        "deadline_misses": deadline_misses,
    }


def format_summary(day: str, s: dict) -> str:
    lines = [f"**btcusdt_perp_v2 dry run — {day}**",
             f"entries {s['entries']} · time-in-pos {s['time_in_position_pct']}% · "
             f"deadline-misses {s['deadline_misses']}"]
    if s["mean_ret_research_bps"] is not None:
        lines.append(f"mean ret_research {s['mean_ret_research_bps']:+.1f} bps")
    cost = s.get("mean_cost_bps") or []
    rungs = " · ".join(f"${n//1000}k:{c:+.1f}" for n, c in zip(config.NOTIONALS, cost)
                       if c is not None)
    if rungs:
        lines.append(f"mean cost_bps by size — {rungs}")
    fired = {k: v["total"] for k, v in s["firings_by_cell"].items() if v["total"]}
    if fired:
        lines.append("firings: " + ", ".join(f"c{k}×{v}" for k, v in sorted(fired.items(),
                                                                             key=lambda x: int(x[0]))))
    return "\n".join(lines)


def send(msg: str) -> bool:
    try:
        from btcusdt_perp_signal.alert import send_discord
        return send_discord(msg, env_key=config.DISCORD_ENV_KEY)
    except Exception:
        import logging
        logging.getLogger("btcusdt_perp_signal_v2").exception("discord send failed")
        return False

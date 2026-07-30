"""
Append-only record store — SPEC_dryrun_book11.md §6. One table per record type, never overwritten.

Layer A writes the signal/research-parity columns (FIRING, ENTRY, RESULT.ret_research_bps, DAILY).
The order-book columns (mid/fills/slippage/hold stats) are filled by Layer B; they default NULL here.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA journal_mode=WAL;")
    return con


def ensure_tables(db_path: Path) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = _connect(db_path)
    try:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS firings_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bar_index INTEGER, ts_bar_close INTEGER, cell_id INTEGER, side TEXT, window TEXT,
            legs TEXT, regime_decile INTEGER, taken INTEGER, not_taken_reason TEXT,
            close_px REAL, late_ms INTEGER, created_at TEXT);

        CREATE TABLE IF NOT EXISTS entries_v2 (
            entry_id INTEGER PRIMARY KEY,
            bar_index INTEGER, ts_bar_close INTEGER, cell_id INTEGER, side TEXT, close_px REAL,
            mid REAL, best_bid REAL, best_ask REAL, spread_bps REAL,
            fill_px TEXT, slippage_bps TEXT, book_snapshot TEXT,
            scheduled_exit_bar INTEGER, created_at TEXT);

        CREATE TABLE IF NOT EXISTS results_v2 (
            entry_id INTEGER PRIMARY KEY,
            entry_bar_index INTEGER, exit_bar_index INTEGER, ts_exit INTEGER,
            cell_id INTEGER, side TEXT, entry_close_px REAL, exit_close_px REAL,
            exit_mid REAL, exit_spread_bps REAL, fill_px TEXT, slippage_bps TEXT,
            ret_research_bps REAL, ret_mid_bps REAL, ret_exec_bps TEXT, cost_bps TEXT,
            hold_stats TEXT, data_quality TEXT, status TEXT, created_at TEXT);

        CREATE TABLE IF NOT EXISTS daily_summary_v2 (
            day TEXT PRIMARY KEY,
            entries INTEGER, firings_by_cell TEXT, time_in_position_pct REAL,
            mean_ret_research_bps REAL, mean_ret_exec_bps TEXT, mean_cost_bps TEXT,
            deadline_misses INTEGER, created_at TEXT);

        CREATE TABLE IF NOT EXISTS pipeline_state_v2 (k TEXT PRIMARY KEY, v TEXT);

        CREATE INDEX IF NOT EXISTS idx_firings_v2_bar ON firings_v2(bar_index);
        CREATE INDEX IF NOT EXISTS idx_results_v2_exit ON results_v2(exit_bar_index);
        """)
        con.commit()
    finally:
        con.close()


def insert_firing(db_path: Path, f) -> None:
    con = _connect(db_path)
    try:
        con.execute(
            """INSERT INTO firings_v2 (bar_index, ts_bar_close, cell_id, side, window, legs,
               regime_decile, taken, not_taken_reason, close_px, late_ms, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (f.bar_index, f.ts_bar_close, f.cell_id, f.side, f.window, json.dumps(f.legs),
             f.regime_decile, int(f.taken), f.not_taken_reason, f.close_px, f.late_ms, _now()))
        con.commit()
    finally:
        con.close()


def insert_entry(db_path: Path, e, book: dict | None = None) -> None:
    b = book or {}
    con = _connect(db_path)
    try:
        con.execute(
            """INSERT OR REPLACE INTO entries_v2 (entry_id, bar_index, ts_bar_close, cell_id, side,
               close_px, mid, best_bid, best_ask, spread_bps, fill_px, slippage_bps, book_snapshot,
               scheduled_exit_bar, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (e.entry_id, e.bar_index, e.ts_bar_close, e.cell_id, e.side, e.close_px,
             b.get("mid"), b.get("best_bid"), b.get("best_ask"), b.get("spread_bps"),
             json.dumps(b["fill_px"]) if "fill_px" in b else None,
             json.dumps(b["slippage_bps"]) if "slippage_bps" in b else None,
             json.dumps(b["book_snapshot"]) if "book_snapshot" in b else None,
             e.scheduled_exit_bar, _now()))
        con.commit()
    finally:
        con.close()


def insert_result(db_path: Path, r, book: dict | None = None) -> None:
    b = book or {}
    con = _connect(db_path)
    try:
        con.execute(
            """INSERT OR REPLACE INTO results_v2 (entry_id, entry_bar_index, exit_bar_index, ts_exit,
               cell_id, side, entry_close_px, exit_close_px, exit_mid, exit_spread_bps, fill_px,
               slippage_bps, ret_research_bps, ret_mid_bps, ret_exec_bps, cost_bps, hold_stats,
               data_quality, status, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (r.entry_id, r.entry_bar_index, r.exit_bar_index, r.ts_exit, r.cell_id, r.side,
             r.entry_close_px, r.exit_close_px, b.get("exit_mid"), b.get("exit_spread_bps"),
             json.dumps(b["fill_px"]) if "fill_px" in b else None,
             json.dumps(b["slippage_bps"]) if "slippage_bps" in b else None,
             r.ret_research_bps, b.get("ret_mid_bps"),
             json.dumps(b["ret_exec_bps"]) if "ret_exec_bps" in b else None,
             json.dumps(b["cost_bps"]) if "cost_bps" in b else None,
             json.dumps(b["hold_stats"]) if "hold_stats" in b else None,
             json.dumps(b["data_quality"]) if "data_quality" in b else None,
             r.status, _now()))
        con.commit()
    finally:
        con.close()


def get_entry_book(db_path: Path, entry_id: int) -> dict | None:
    """Entry-instant book block (mid + fill ladder) for computing execution returns at exit."""
    con = _connect(db_path)
    try:
        row = con.execute("SELECT mid, fill_px FROM entries_v2 WHERE entry_id = ?",
                          (entry_id,)).fetchone()
    finally:
        con.close()
    if row is None:
        return None
    return {"mid": row[0], "fill_px": json.loads(row[1]) if row[1] else None}


def update_result_book(db_path: Path, entry_id: int, book: dict) -> None:
    """Fill the RESULT order-book columns once the poller finishes (~exit + 60s)."""
    con = _connect(db_path)
    try:
        con.execute(
            """UPDATE results_v2 SET exit_mid=?, exit_spread_bps=?, fill_px=?, slippage_bps=?,
               ret_mid_bps=?, ret_exec_bps=?, cost_bps=?, hold_stats=?, data_quality=? WHERE entry_id=?""",
            (book.get("exit_mid"), book.get("exit_spread_bps"),
             json.dumps(book["fill_px"]) if book.get("fill_px") is not None else None,
             json.dumps(book["slippage_bps"]) if book.get("slippage_bps") is not None else None,
             book.get("ret_mid_bps"),
             json.dumps(book["ret_exec_bps"]) if book.get("ret_exec_bps") is not None else None,
             json.dumps(book["cost_bps"]) if book.get("cost_bps") is not None else None,
             json.dumps(book["hold_stats"]) if book.get("hold_stats") is not None else None,
             json.dumps(book["data_quality"]) if book.get("data_quality") is not None else None,
             entry_id))
        con.commit()
    finally:
        con.close()


def set_state(db_path: Path, key: str, value) -> None:
    con = _connect(db_path)
    try:
        con.execute("INSERT OR REPLACE INTO pipeline_state_v2 (k, v) VALUES (?, ?)",
                    (key, json.dumps(value) if value is not None else None))
        con.commit()
    finally:
        con.close()


def get_state(db_path: Path, key: str):
    con = _connect(db_path)
    try:
        row = con.execute("SELECT v FROM pipeline_state_v2 WHERE k = ?", (key,)).fetchone()
    finally:
        con.close()
    return json.loads(row[0]) if row and row[0] is not None else None


def next_entry_id(db_path: Path) -> int:
    con = _connect(db_path)
    try:
        row = con.execute("SELECT MAX(entry_id) FROM entries_v2").fetchone()
    finally:
        con.close()
    return (row[0] or 0) + 1


def insert_daily_summary(db_path: Path, day: str, s: dict) -> None:
    con = _connect(db_path)
    try:
        con.execute(
            """INSERT OR REPLACE INTO daily_summary_v2 (day, entries, firings_by_cell,
               time_in_position_pct, mean_ret_research_bps, mean_ret_exec_bps, mean_cost_bps,
               deadline_misses, created_at) VALUES (?,?,?,?,?,?,?,?,?)""",
            (day, s.get("entries"), json.dumps(s.get("firings_by_cell", {})),
             s.get("time_in_position_pct"), s.get("mean_ret_research_bps"),
             json.dumps(s.get("mean_ret_exec_bps")) if s.get("mean_ret_exec_bps") is not None else None,
             json.dumps(s.get("mean_cost_bps")) if s.get("mean_cost_bps") is not None else None,
             s.get("deadline_misses", 0), _now()))
        con.commit()
    finally:
        con.close()

"""
SQLite persistence for pm_signal_sim. Separate DB (data/pm_signal_sim.sqlite); does NOT touch
pm_shock_signal / pm_asym_signal tables. One connection per call (open/commit/close).

Schema (BUILD_SPEC §5, LIVE_TEST_SPEC §1/§3):
  captures        one per (epoch, token) with >=1 fire; raw slice span [t_back, t_fwd]
  fires           one per config-fire; references capture_id (co-fires share a capture)
  raw_pm_trades   FK capture_id   (per epoch,token)
  raw_pm_book     FK capture_id   (per epoch,token), top-of-book + sizes (500ms in reaction window)
  raw_btc_depth   FK epoch_start  (shared per epoch — BTC is not per-token)
  raw_btc_tick    FK epoch_start
  resolution      per (epoch, token): final/winner/pin + window_alerted
  epoch_strike    one per epoch the engine is up (strike btc_mid at epoch start)
  signal_evals    one per (in-scope fire × pre-registered signal): value/decision/decided_at
  fill_log        one per in-scope fire: realized ask_d5 fill + tradability margin

Every raw record carries BOTH local_ts (receipt, one clock across sources) and event_ts (source).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def ensure_tables(db_path: Path) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS captures (
            id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_start INTEGER NOT NULL, token TEXT NOT NULL,
            t_back REAL NOT NULL, t_fwd REAL NOT NULL, status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL, UNIQUE(epoch_start, token));

        CREATE TABLE IF NOT EXISTS fires (
            id INTEGER PRIMARY KEY AUTOINCREMENT, capture_id INTEGER NOT NULL,
            config_id TEXT NOT NULL, epoch_start INTEGER NOT NULL, token TEXT NOT NULL,
            token_id TEXT NOT NULL, sec INTEGER NOT NULL, event_ts REAL, local_ts REAL,
            ratio REAL, k REAL, p_entry REAL, entry_bid REAL, entry_ask REAL, btc_mid REAL,
            entry_last_age_s REAL, created_at TEXT NOT NULL,
            FOREIGN KEY (capture_id) REFERENCES captures(id));

        CREATE TABLE IF NOT EXISTS raw_pm_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, capture_id INTEGER NOT NULL, epoch_start INTEGER,
            token TEXT, local_ts REAL, event_ts REAL, price REAL, size REAL, side TEXT,
            FOREIGN KEY (capture_id) REFERENCES captures(id));

        CREATE TABLE IF NOT EXISTS raw_pm_book (
            id INTEGER PRIMARY KEY AUTOINCREMENT, capture_id INTEGER NOT NULL, epoch_start INTEGER,
            token TEXT, local_ts REAL, event_ts REAL, bid REAL, bid_sz REAL, ask REAL, ask_sz REAL,
            FOREIGN KEY (capture_id) REFERENCES captures(id));

        CREATE TABLE IF NOT EXISTS raw_btc_depth (
            id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_start INTEGER, local_ts REAL, event_ts REAL,
            depth_json TEXT);

        CREATE TABLE IF NOT EXISTS raw_btc_tick (
            id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_start INTEGER, local_ts REAL, event_ts REAL,
            mid REAL, bid REAL, ask REAL);

        CREATE TABLE IF NOT EXISTS resolution (
            id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_start INTEGER NOT NULL, token TEXT NOT NULL,
            final_price REAL, winner INTEGER, resolved INTEGER, window_alerted INTEGER DEFAULT 0,
            local_ts REAL, created_at TEXT NOT NULL, UNIQUE(epoch_start, token));

        CREATE TABLE IF NOT EXISTS epoch_strike (
            id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_start INTEGER NOT NULL UNIQUE,
            local_ts REAL, event_ts REAL, btc_mid REAL, created_at TEXT NOT NULL);

        CREATE TABLE IF NOT EXISTS signal_evals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, fire_id INTEGER NOT NULL, signal TEXT NOT NULL,
            value REAL, decision INTEGER, decided_at REAL, input_ts REAL, eval_wall_ts REAL,
            detail TEXT, created_at TEXT NOT NULL, UNIQUE(fire_id, signal),
            FOREIGN KEY (fire_id) REFERENCES fires(id));

        CREATE TABLE IF NOT EXISTS fill_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, fire_id INTEGER NOT NULL UNIQUE,
            fill_ts REAL, fill_ask REAL, fill_ask_sz REAL, margin_s REAL, created_at TEXT NOT NULL,
            FOREIGN KEY (fire_id) REFERENCES fires(id));

        CREATE INDEX IF NOT EXISTS idx_fires_cap ON fires(capture_id);
        CREATE INDEX IF NOT EXISTS idx_fires_epoch ON fires(epoch_start, token);
        CREATE INDEX IF NOT EXISTS idx_trades_cap ON raw_pm_trades(capture_id);
        CREATE INDEX IF NOT EXISTS idx_book_cap ON raw_pm_book(capture_id);
        CREATE INDEX IF NOT EXISTS idx_btcdepth_epoch ON raw_btc_depth(epoch_start);
        CREATE INDEX IF NOT EXISTS idx_btctick_epoch ON raw_btc_tick(epoch_start);
        CREATE INDEX IF NOT EXISTS idx_sigeval_fire ON signal_evals(fire_id);
        CREATE INDEX IF NOT EXISTS idx_filllog_fire ON fill_log(fire_id);
        """)
        con.commit()
    finally:
        con.close()


def open_capture(db_path: Path, epoch_start: int, token: str, t_back: float, t_fwd: float) -> int:
    """Get-or-create the (epoch, token) capture; widen [t_back, t_fwd] to the union of fire spans."""
    con = sqlite3.connect(str(db_path))
    try:
        row = con.execute("SELECT id, t_back, t_fwd FROM captures WHERE epoch_start=? AND token=?",
                          (epoch_start, token)).fetchone()
        if row:
            cid, tb, tf = row
            con.execute("UPDATE captures SET t_back=?, t_fwd=? WHERE id=?",
                       (min(tb, t_back), max(tf, t_fwd), cid))
            con.commit()
            return cid
        cur = con.execute(
            "INSERT INTO captures (epoch_start, token, t_back, t_fwd, status, created_at) "
            "VALUES (?, ?, ?, ?, 'open', ?)", (epoch_start, token, t_back, t_fwd, _now()))
        con.commit()
        return cur.lastrowid
    finally:
        con.close()


def insert_fire(db_path: Path, capture_id: int, fe) -> int:
    con = sqlite3.connect(str(db_path))
    try:
        cur = con.execute(
            "INSERT INTO fires (capture_id, config_id, epoch_start, token, token_id, sec, event_ts, "
            "local_ts, ratio, k, p_entry, entry_bid, entry_ask, btc_mid, entry_last_age_s, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (capture_id, fe.config_id, fe.epoch_start, fe.token, fe.token_id, fe.sec, fe.event_ts,
             fe.local_ts, fe.ratio, fe.k, fe.p_entry, fe.entry_bid, fe.entry_ask, fe.btc_mid,
             fe.entry_last_age_s, _now()))
        con.commit()
        return cur.lastrowid
    finally:
        con.close()


def _bulk(db_path: Path, sql: str, rows) -> int:
    rows = list(rows)
    if not rows:
        return 0
    con = sqlite3.connect(str(db_path))
    try:
        con.executemany(sql, rows)
        con.commit()
        return len(rows)
    finally:
        con.close()


def insert_raw_pm_trades(db_path, capture_id, rows):  # rows: (epoch, token, local_ts, event_ts, price, size, side)
    return _bulk(db_path, "INSERT INTO raw_pm_trades (capture_id, epoch_start, token, local_ts, "
                 "event_ts, price, size, side) VALUES (?,?,?,?,?,?,?,?)",
                 [(capture_id, *r) for r in rows])


def insert_raw_pm_book(db_path, capture_id, rows):    # rows: (epoch, token, local_ts, event_ts, bid, bid_sz, ask, ask_sz)
    return _bulk(db_path, "INSERT INTO raw_pm_book (capture_id, epoch_start, token, local_ts, "
                 "event_ts, bid, bid_sz, ask, ask_sz) VALUES (?,?,?,?,?,?,?,?,?)",
                 [(capture_id, *r) for r in rows])


def insert_raw_btc_depth(db_path, rows):              # rows: (epoch, local_ts, event_ts, depth_json)
    return _bulk(db_path, "INSERT INTO raw_btc_depth (epoch_start, local_ts, event_ts, depth_json) "
                 "VALUES (?,?,?,?)", rows)


def insert_raw_btc_tick(db_path, rows):               # rows: (epoch, local_ts, event_ts, mid, bid, ask)
    return _bulk(db_path, "INSERT INTO raw_btc_tick (epoch_start, local_ts, event_ts, mid, bid, ask) "
                 "VALUES (?,?,?,?,?,?)", rows)


def insert_epoch_strike(db_path: Path, epoch_start, local_ts, event_ts, btc_mid):
    """One row per epoch the engine is up (LIVE_TEST_SPEC §1.4); first-writer-wins per epoch."""
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("INSERT OR IGNORE INTO epoch_strike (epoch_start, local_ts, event_ts, btc_mid, "
                    "created_at) VALUES (?,?,?,?,?)", (epoch_start, local_ts, event_ts, btc_mid, _now()))
        con.commit()
    finally:
        con.close()


def insert_signal_eval(db_path: Path, fire_id, ev):
    """ev: a signals_live.SigEval (signal/value/decision/decided_at/input_ts/eval_wall_ts/detail)."""
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("INSERT OR IGNORE INTO signal_evals (fire_id, signal, value, decision, decided_at, "
                    "input_ts, eval_wall_ts, detail, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (fire_id, ev.signal, ev.value, int(ev.decision), ev.decided_at, ev.input_ts,
                     ev.eval_wall_ts, ev.detail, _now()))
        con.commit()
    finally:
        con.close()


def insert_fill_log(db_path: Path, fire_id, fill_ts, fill_ask, fill_ask_sz, margin_s):
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("INSERT OR IGNORE INTO fill_log (fire_id, fill_ts, fill_ask, fill_ask_sz, "
                    "margin_s, created_at) VALUES (?,?,?,?,?,?)",
                    (fire_id, fill_ts, fill_ask, fill_ask_sz, margin_s, _now()))
        con.commit()
    finally:
        con.close()


def insert_resolution(db_path: Path, epoch_start, token, final_price, winner, resolved, local_ts):
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("INSERT OR IGNORE INTO resolution (epoch_start, token, final_price, winner, "
                    "resolved, window_alerted, local_ts, created_at) VALUES (?,?,?,?,?,0,?,?)",
                    (epoch_start, token, final_price, winner, resolved, local_ts, _now()))
        con.commit()
    finally:
        con.close()


def close_capture(db_path: Path, capture_id: int) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("UPDATE captures SET status='closed' WHERE id=?", (capture_id,))
        con.commit()
    finally:
        con.close()


def unalerted_resolved_windows(db_path: Path):
    """(epoch_start, token) that have a resolution, >=1 fire, and window_alerted=0 — for the Discord sweep."""
    con = sqlite3.connect(str(db_path))
    try:
        return con.execute("""
            SELECT r.epoch_start, r.token FROM resolution r
            WHERE r.window_alerted = 0
              AND EXISTS (SELECT 1 FROM fires f WHERE f.epoch_start=r.epoch_start AND f.token=r.token)
            ORDER BY r.epoch_start""").fetchall()
    finally:
        con.close()


def fetch_window_for_alert(db_path: Path, epoch_start: int, token: str):
    """Return (resolution_row, [fire_rows]) for building one merged Discord message."""
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        res = con.execute("SELECT * FROM resolution WHERE epoch_start=? AND token=?",
                          (epoch_start, token)).fetchone()
        fires = con.execute("SELECT * FROM fires WHERE epoch_start=? AND token=? ORDER BY sec",
                           (epoch_start, token)).fetchall()
        return res, fires
    finally:
        con.close()


def fetch_book_path(db_path: Path, capture_id: int):
    """Post-fire book path for a capture: [(local_ts, bid, ask), ...] ordered by local_ts.
    Used by the Discord report to find the exit bid/ask at each τ (all fires in an
    (epoch, token) share one capture, so fetch once)."""
    con = sqlite3.connect(str(db_path))
    try:
        return con.execute(
            "SELECT local_ts, bid, ask FROM raw_pm_book WHERE capture_id=? ORDER BY local_ts",
            (capture_id,)).fetchall()
    finally:
        con.close()


def fetch_book_full(db_path: Path, capture_id: int):
    """Full top-of-book path for a capture: [(local_ts, bid, bid_sz, ask, ask_sz), ...] by local_ts.
    Used by the live S1 (`d_mid_3`) + fill (`ask_d5`) evaluators — parity with the offline rebuild,
    which reads the same raw_pm_book rows."""
    con = sqlite3.connect(str(db_path))
    try:
        return con.execute(
            "SELECT local_ts, bid, bid_sz, ask, ask_sz FROM raw_pm_book WHERE capture_id=? "
            "ORDER BY local_ts", (capture_id,)).fetchall()
    finally:
        con.close()


def fetch_signal_evals_for_window(db_path: Path, epoch_start: int, token: str):
    """{fire_id: [eval_row, ...]} for the window's fires (for the Discord filter + signal block)."""
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            "SELECT se.* FROM signal_evals se JOIN fires f ON f.id = se.fire_id "
            "WHERE f.epoch_start=? AND f.token=?", (epoch_start, token)).fetchall()
        out: dict = {}
        for r in rows:
            out.setdefault(r["fire_id"], []).append(r)
        return out
    finally:
        con.close()


def fetch_fill_log_for_window(db_path: Path, epoch_start: int, token: str):
    """{fire_id: fill_row} for the window's fires."""
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            "SELECT fl.* FROM fill_log fl JOIN fires f ON f.id = fl.fire_id "
            "WHERE f.epoch_start=? AND f.token=?", (epoch_start, token)).fetchall()
        return {r["fire_id"]: r for r in rows}
    finally:
        con.close()


def mark_window_alerted(db_path: Path, epoch_start: int, token: str) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("UPDATE resolution SET window_alerted=1 WHERE epoch_start=? AND token=?",
                    (epoch_start, token))
        con.commit()
    finally:
        con.close()

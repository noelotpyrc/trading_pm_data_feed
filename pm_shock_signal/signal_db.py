"""
SQLite persistence for shock signals + sim trades.

Schema follows BUILD_SPEC §6, extended per REVIEW.md (full top-of-book at both ends,
z inputs + event timestamps, price/book staleness ages, strike, resolved_outcome).
Mirrors btcusdt_perp_signal/signal_db.py: one sqlite3 connection per call
(open/commit/close), simple and process-safe enough for this single-writer engine.

Timestamps: `fire_ts`/`entry_ts`/`exit_ts` are human-readable UTC strings; the
`*_event_ts`, `pm_event_ts`, `receipt_ts` columns are REAL epoch seconds (so
latency = receipt_ts − pm_event_ts and offline z-recompute are trivial).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SIGNALS_TABLE = "shock_signals"
SIM_TRADES_TABLE = "shock_sim_trades"
PRICE_PATH_TABLE = "shock_price_path"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def ensure_tables(db_path: Path) -> None:
    """Create shock_signals + shock_sim_trades if absent (+ indices)."""
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {SIGNALS_TABLE} (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                config_id         TEXT NOT NULL,
                epoch_start       INTEGER NOT NULL,
                fire_ts           TEXT NOT NULL,
                sec_into_window   INTEGER NOT NULL,
                token             TEXT NOT NULL,
                delta             INTEGER NOT NULL,
                k                 REAL NOT NULL,
                z_thr             REAL NOT NULL,
                back_ratio        REAL NOT NULL,
                z_shock           REAL NOT NULL,
                p_shock           REAL NOT NULL,
                entry_ask         REAL,
                entry_bid         REAL,
                rv_60s            REAL,
                entry_mid         REAL,
                mid_prev          REAL,
                mid_event_ts      REAL,
                mid_prev_event_ts REAL,
                pm_event_ts       REAL,
                receipt_ts        REAL,
                entry_last_age_s  REAL,
                strike            REAL,
                alerted           INTEGER DEFAULT 0,
                created_at        TEXT NOT NULL
            );
        """)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {SIM_TRADES_TABLE} (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id         INTEGER NOT NULL,
                config_id         TEXT NOT NULL,
                entry_ts          TEXT NOT NULL,
                entry_last        REAL,
                entry_ask         REAL,
                exit_ts           TEXT NOT NULL,
                exit_sec          INTEGER NOT NULL,
                exit_last         REAL,
                exit_bid          REAL,
                exit_ask          REAL,
                ttl_capped        INTEGER DEFAULT 0,
                pnl_gross         REAL,
                pnl_net           REAL,
                roi_net           REAL,
                exit_last_age_s   REAL,
                exit_book_age_s   REAL,
                resolved_outcome  TEXT,
                exit_alerted      INTEGER DEFAULT 0,
                created_at        TEXT NOT NULL,
                FOREIGN KEY (signal_id) REFERENCES {SIGNALS_TABLE}(id)
            );
        """)
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{SIGNALS_TABLE}_epoch_ts "
            f"ON {SIGNALS_TABLE}(epoch_start, fire_ts);"
        )
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {PRICE_PATH_TABLE} (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id     INTEGER NOT NULL,
                offset_s      INTEGER NOT NULL,
                ts            REAL,
                pm_last       REAL,
                pm_bid        REAL,
                pm_ask        REAL,
                pm_last_age_s REAL,
                btc_mid       REAL,
                FOREIGN KEY (signal_id) REFERENCES {SIGNALS_TABLE}(id)
            );
        """)
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{SIM_TRADES_TABLE}_signal "
            f"ON {SIM_TRADES_TABLE}(signal_id);"
        )
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{PRICE_PATH_TABLE}_sig "
            f"ON {PRICE_PATH_TABLE}(signal_id, offset_s);"
        )
        con.commit()
    finally:
        con.close()


def insert_signal(db_path: Path, *, config_id: str, epoch_start: int, fire_ts: str,
                  sec_into_window: int, token: str, delta: int, k: float, z_thr: float,
                  back_ratio: float, z_shock: float, p_shock: float,
                  entry_ask: float | None, entry_bid: float | None, rv_60s: float,
                  entry_mid: float | None, mid_prev: float | None,
                  mid_event_ts: float | None, mid_prev_event_ts: float | None,
                  pm_event_ts: float | None, receipt_ts: float | None,
                  entry_last_age_s: float | None, strike: float | None) -> int:
    """Insert a fired-signal row (alerted=0). Return the new row id."""
    con = sqlite3.connect(str(db_path))
    try:
        cur = con.execute(
            f"""
            INSERT INTO {SIGNALS_TABLE}
                (config_id, epoch_start, fire_ts, sec_into_window, token, delta, k, z_thr,
                 back_ratio, z_shock, p_shock, entry_ask, entry_bid, rv_60s, entry_mid,
                 mid_prev, mid_event_ts, mid_prev_event_ts, pm_event_ts, receipt_ts,
                 entry_last_age_s, strike, alerted, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (config_id, epoch_start, fire_ts, sec_into_window, token, delta, k, z_thr,
             back_ratio, z_shock, p_shock, entry_ask, entry_bid, rv_60s, entry_mid,
             mid_prev, mid_event_ts, mid_prev_event_ts, pm_event_ts, receipt_ts,
             entry_last_age_s, strike, _now()),
        )
        con.commit()
        return cur.lastrowid
    finally:
        con.close()


def mark_signal_alerted(db_path: Path, signal_id: int) -> None:
    """Set shock_signals.alerted = 1."""
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(f"UPDATE {SIGNALS_TABLE} SET alerted = 1 WHERE id = ?", (signal_id,))
        con.commit()
    finally:
        con.close()


def insert_sim_trade(db_path: Path, *, signal_id: int, config_id: str, entry_ts: str,
                     entry_last: float | None, entry_ask: float | None, exit_ts: str,
                     exit_sec: int, exit_last: float | None, exit_bid: float | None,
                     exit_ask: float | None, ttl_capped: bool,
                     pnl_gross: float | None, pnl_net: float | None, roi_net: float | None,
                     exit_last_age_s: float | None, exit_book_age_s: float | None) -> int:
    """Insert a completed sim trade (entry + exit + PnL). Return the new row id."""
    con = sqlite3.connect(str(db_path))
    try:
        cur = con.execute(
            f"""
            INSERT INTO {SIM_TRADES_TABLE}
                (signal_id, config_id, entry_ts, entry_last, entry_ask, exit_ts, exit_sec,
                 exit_last, exit_bid, exit_ask, ttl_capped, pnl_gross, pnl_net, roi_net,
                 exit_last_age_s, exit_book_age_s, resolved_outcome, exit_alerted, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, ?)
            """,
            (signal_id, config_id, entry_ts, entry_last, entry_ask, exit_ts, exit_sec,
             exit_last, exit_bid, exit_ask, int(ttl_capped), pnl_gross, pnl_net, roi_net,
             exit_last_age_s, exit_book_age_s, _now()),
        )
        con.commit()
        return cur.lastrowid
    finally:
        con.close()


def mark_trade_exit_alerted(db_path: Path, trade_id: int) -> None:
    """Set shock_sim_trades.exit_alerted = 1."""
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(f"UPDATE {SIM_TRADES_TABLE} SET exit_alerted = 1 WHERE id = ?", (trade_id,))
        con.commit()
    finally:
        con.close()


def insert_path_samples(db_path: Path, samples) -> int:
    """Bulk-insert forward price-path rows (REVIEW item 6). `samples` is an iterable of
    objects with attrs: signal_id, offset_s, ts, pm_last, pm_bid, pm_ask, pm_last_age_s,
    btc_mid. Returns the number inserted."""
    rows = [(s.signal_id, s.offset_s, s.ts, s.pm_last, s.pm_bid, s.pm_ask,
             s.pm_last_age_s, s.btc_mid) for s in samples]
    if not rows:
        return 0
    con = sqlite3.connect(str(db_path))
    try:
        con.executemany(
            f"""INSERT INTO {PRICE_PATH_TABLE}
                (signal_id, offset_s, ts, pm_last, pm_bid, pm_ask, pm_last_age_s, btc_mid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        con.commit()
        return len(rows)
    finally:
        con.close()


# --- resolved_outcome backfill (REVIEW item 5 — populated by scripts/resolve_outcomes.py) ---

def epochs_needing_resolution(db_path: Path) -> list[int]:
    """Distinct epoch_starts that have sim trades still missing resolved_outcome."""
    con = sqlite3.connect(str(db_path))
    try:
        rows = con.execute(
            f"""
            SELECT DISTINCT s.epoch_start
            FROM {SIM_TRADES_TABLE} t JOIN {SIGNALS_TABLE} s ON t.signal_id = s.id
            WHERE t.resolved_outcome IS NULL
            ORDER BY s.epoch_start
            """
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


def set_resolved_outcome(db_path: Path, epoch_start: int, outcome: str) -> int:
    """Fill resolved_outcome for all (still-NULL) sim trades of `epoch_start`. Return count."""
    con = sqlite3.connect(str(db_path))
    try:
        cur = con.execute(
            f"""
            UPDATE {SIM_TRADES_TABLE} SET resolved_outcome = ?
            WHERE resolved_outcome IS NULL AND signal_id IN
                (SELECT id FROM {SIGNALS_TABLE} WHERE epoch_start = ?)
            """,
            (outcome, epoch_start),
        )
        con.commit()
        return cur.rowcount
    finally:
        con.close()

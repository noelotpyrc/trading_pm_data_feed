#!/usr/bin/env python3
"""
Offline replay of the pm_signal_sim engine from recorded feeds.

Built for the 2026-08-12 13:00 → 2026-08-24 19:43 UTC gap, when the live DB was corrupted and the
engine recorded nothing. Re-runs the SAME detector (signals.MultiDetector), the SAME S1/S2 evaluators
and fill rule (signals_live) and the SAME resolution rule (run_signal_sim._resolve_window) over:

  * PM trades   — data-api /trades per epoch slug (pm_btc15updown_data.fetch_pm_trades output):
                  <trades-dir>/btc-updown-15m-<epoch>.jsonl  {timestamp, asset, outcome, side, price, size}
  * PM book     — pm_btcupdown collector (~1.1 s):   <feeds-dir>/pm_btcupdown/pm_btcupdown_YYYY-MM-DD.jsonl
  * BTC book    — btc_depth collector (~1 s, top-20): <feeds-dir>/btc_depth/depth_YYYY-MM-DD.jsonl

and writes the live schema (captures, fires, epoch_strike, signal_evals, fill_log, resolution) to a
fresh sqlite. Raw slice tables are not rebuilt (the source files are the raw record).

Known differences from the live run (measured on the Aug 6–12 overlap by report_06 in
pm_shock_live_v2):
  * trade timestamps are whole seconds and sit ~1.5 s after the live ws event_ts → `--ts-shift`
    (seconds added to API timestamps) is calibrated on the overlap;
  * the engine tick phase is fixed at `--tick-phase` into each second (live: uniform in [0, 1));
  * book rows are ~1.1 s apart (live: 0.5 s throttle) → S1 `mid_last` and `ask_d5` can be read up
    to ~1 s later than live;
  * BTC mid comes from the depth20 top (live: bookTicker).

Usage:
  python -m pm_signal_sim.scripts.replay_feeds --start 2026-08-06 --end 2026-08-24 --db OUT.sqlite
         [--start-ts EPOCH] [--stop-ts EPOCH] [--ts-shift -1.5] [--tick-phase 0.5]
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import time
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pm_signal_sim import config, signal_db, signals_live          # noqa: E402
from pm_signal_sim.signals import MultiDetector                     # noqa: E402

log = logging.getLogger("replay")

SSD = Path("/Volumes/Extreme SSD/vps_madrid_backup")
DEFAULT_TRADES_DIR = SSD / "pm_btc15updown_trades"
DEFAULT_FEEDS_DIR = SSD / "feeds_2026-08-24"
SLUG_PREFIX = "btc-updown-15m-"
WIN = config.WINDOW_SEC


@dataclass
class BookTop:
    ts: float
    last: Optional[float]
    bid: Optional[float]
    ask: Optional[float]


def _f(x) -> Optional[float]:
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------- offline feeds
class OfflinePm:
    """Stand-in for feeds.PmTokenFeed: vwap_window / price_asof / book_top, plus book_full for the
    S1 + fill evaluators. One epoch loaded at a time; `now` gates the book_top / price_asof reads."""

    def __init__(self, ts_shift: float) -> None:
        self.ts_shift = ts_shift
        self.now = 0.0
        self._tr: dict = {}     # token_id -> (ts[], price[], cum_pv[], cum_v[])
        self._bk: dict = {}     # token_id -> (ts[], rows[(ts, bid, bid_sz, ask, ask_sz)], last[])

    def load_epoch(self, trades_path: Path, book_rows: dict) -> int:
        self._tr, self._bk = {}, {}
        n = 0
        by_tok: dict = {}
        with trades_path.open() as f:
            for line in f:
                r = json.loads(line)
                p, s = _f(r.get("price")), _f(r.get("size"))
                ts = _f(r.get("timestamp"))
                if p is None or s is None or s <= 0 or ts is None:
                    continue
                by_tok.setdefault(str(r["asset"]), []).append((ts + self.ts_shift, p, s))
                n += 1
        for tid, rows in by_tok.items():
            rows.sort()
            ts, pr, cpv, cv = [], [], [0.0], [0.0]
            for t, p, s in rows:
                ts.append(t); pr.append(p)
                cpv.append(cpv[-1] + p * s); cv.append(cv[-1] + s)
            self._tr[tid] = (ts, pr, cpv, cv)
        for tid, rows in book_rows.items():
            rows.sort()
            self._bk[tid] = ([r[0] for r in rows], [r[:5] for r in rows], [r[5] for r in rows])
        return n

    def vwap_window(self, token_id: str, a: float, b: float) -> Optional[float]:
        tr = self._tr.get(token_id)
        if tr is None:
            return None
        ts, _pr, cpv, cv = tr
        i, j = bisect_right(ts, a), bisect_right(ts, b)
        v = cv[j] - cv[i]
        return (cpv[j] - cpv[i]) / v if v > 0 else None

    def price_asof(self, token_id: str, ts: float):
        tr = self._tr.get(token_id)
        if tr is None:
            return None
        i = bisect_right(tr[0], ts) - 1
        return (tr[0][i], tr[1][i]) if i >= 0 else None

    def book_top(self, token_id: str) -> Optional[BookTop]:
        bk = self._bk.get(token_id)
        if bk is None:
            return None
        i = bisect_right(bk[0], self.now) - 1
        if i < 0:
            return None
        ts, bid, _bsz, ask, _asz = bk[1][i]
        return BookTop(ts=ts, last=bk[2][i], bid=bid, ask=ask)

    def book_full(self, token_id: str) -> list:
        bk = self._bk.get(token_id)
        return bk[1] if bk else []


class OfflineBtc:
    """Stand-in for pm_shock_signal.feeds.BtcMidFeed (mid_now / mid_at) on the depth20 top mid.
    Same causal gate as live: mid_at(t) interpolates between bracketing samples ≤ now, else None."""

    def __init__(self) -> None:
        self.now = 0.0
        self.ts: list = []
        self.mid: list = []

    def extend(self, rows: list) -> None:
        rows.sort()
        if self.ts and rows and rows[0][0] < self.ts[-1]:
            allr = sorted(zip(self.ts, self.mid)) + rows
            self.ts, self.mid = [r[0] for r in allr], [r[1] for r in allr]
        else:
            self.ts.extend(r[0] for r in rows); self.mid.extend(r[1] for r in rows)

    def _last_idx(self, t: float) -> int:
        return bisect_right(self.ts, t) - 1

    def mid_now(self) -> Optional[float]:
        i = self._last_idx(self.now)
        return self.mid[i] if i >= 0 else None

    def mid_at(self, t: float) -> Optional[float]:
        i = self._last_idx(t)
        if i < 0:
            return None
        if self.ts[i] == t:
            return self.mid[i]
        if i + 1 >= len(self.ts) or self.ts[i + 1] > self.now:
            return None                       # not yet bracketed at `now`
        span = self.ts[i + 1] - self.ts[i]
        if span <= 0:
            return self.mid[i]
        return self.mid[i] + (t - self.ts[i]) / span * (self.mid[i + 1] - self.mid[i])


# ---------------------------------------------------------------- file loaders
def load_book_day(path: Path) -> tuple[dict, dict]:
    """→ (epoch -> {token_id: [(ts, bid, bid_sz, ask, ask_sz, last)]}, epoch -> {"Up": tid, "Down": tid})"""
    rows: dict = {}
    toks: dict = {}
    if not path.exists():
        return rows, toks
    with path.open() as f:
        for line in f:
            r = json.loads(line)
            slug = r.get("slug") or ""
            if not slug.startswith(SLUG_PREFIX):
                continue
            ep = int(slug.rsplit("-", 1)[1])
            ts = r["ts_ms"] / 1000.0
            outs = r.get("outcomes") or ["Up", "Down"]
            ep_rows = rows.setdefault(ep, {})
            for out, t in zip(outs, r.get("tokens") or []):
                tid = str(t.get("token_id"))
                toks.setdefault(ep, {}).setdefault(out, tid)
                ep_rows.setdefault(tid, []).append(
                    (ts, _f(t.get("bid")), _f(t.get("bid_size")), _f(t.get("ask")),
                     _f(t.get("ask_size")), _f(t.get("last_trade_price"))))
    return rows, toks


def load_depth_day(path: Path) -> list:
    out = []
    if not path.exists():
        return out
    with path.open() as f:
        for line in f:
            r = json.loads(line)
            try:
                bid, ask = float(r["bids"][0][0]), float(r["asks"][0][0])
            except (KeyError, IndexError, TypeError, ValueError):
                continue
            out.append((r["ts_ms"] / 1000.0, (bid + ask) / 2.0))
    return out


def tokens_from_trades(trades_path: Path) -> dict:
    toks: dict = {}
    with trades_path.open() as f:
        for line in f:
            r = json.loads(line)
            toks.setdefault(str(r.get("outcome")), str(r.get("asset")))
            if len(toks) >= 2:
                break
    return toks


# ---------------------------------------------------------------- engine replay
def _resolve_window(db: Path, detector: MultiDetector, epoch: int, fired_tokens, now: float) -> int:
    """Verbatim rule from run_signal_sim._resolve_window."""
    finals = {tok: detector.final_pdet(epoch, tok) for tok in ("Up", "Down")}
    finals = {k: v for k, v in finals.items() if v is not None}
    if not finals or not fired_tokens:
        return 0
    mx = max(finals.values())
    resolved = 1 if mx >= config.RESOLVE_MIN else 0
    win = max(finals, key=finals.get)
    n = 0
    for tok in fired_tokens:
        fin = finals.get(tok)
        if fin is None:
            continue
        signal_db.insert_resolution(db, epoch, tok, fin, int(tok == win), resolved, now)
        n += 1
    return n


def _drain_pending(db: Path, pm: OfflinePm, btc: OfflineBtc, pending: list, now: float) -> None:
    """Same horizons/order as run_signal_sim._drain_pending; book rows come from the epoch file."""
    remaining = []
    for p in pending:
        fe = p["fe"]
        if not p["s2_done"] and now >= p["s2_due"]:
            ev = signals_live.eval_s2(fe, btc)
            ev.eval_wall_ts = now
            signal_db.insert_signal_eval(db, p["fire_id"], ev)
            if ev.decided_at is not None:
                p["decided_ats"].append(ev.decided_at)
            p["s2_done"] = True
        if not p["s1_done"] and now >= p["s1_due"]:
            ev = signals_live.eval_s1(fe, pm.book_full(fe.token_id))
            ev.eval_wall_ts = now
            signal_db.insert_signal_eval(db, p["fire_id"], ev)
            if ev.decided_at is not None:
                p["decided_ats"].append(ev.decided_at)
            p["s1_done"] = True
        if not p["fill_done"] and now >= p["fill_due"]:
            fill = signals_live.compute_fill(fe, pm.book_full(fe.token_id))
            if fill is not None:
                fill_ts, fill_ask, fill_ask_sz = fill
                dats = [d for d in p["decided_ats"] if d is not None]
                margin = (fill_ts - max(dats)) if dats else None
                signal_db.insert_fill_log(db, p["fire_id"], fill_ts, fill_ask, fill_ask_sz, margin)
            p["fill_done"] = True
        if not (p["s1_done"] and p["s2_done"] and p["fill_done"]):
            remaining.append(p)
    pending[:] = remaining


def replay_epoch(db: Path, epoch: int, pm: OfflinePm, btc: OfflineBtc, detector: MultiDetector,
                 tokens: list, tick_phase: float) -> dict:
    stats = {"fires": 0, "in_scope": 0, "resolved_rows": 0}
    pending: list = []
    fired_tokens: set = set()
    strike_done = False
    for sec in range(WIN):
        now = epoch + sec + tick_phase
        pm.now = btc.now = now
        if not strike_done:
            mid = btc.mid_now()
            if mid is not None:
                signal_db.insert_epoch_strike(db, epoch, now, now, mid)
                strike_done = True
        detector.update_grid(epoch, sec, tokens)
        for fe in detector.on_tick(now, epoch, sec, tokens):
            t_back = fe.event_ts - config.L_BACK_SEC
            t_fwd = fe.epoch_start + config.WINDOW_END_SEC
            cid = signal_db.open_capture(db, fe.epoch_start, fe.token, t_back, t_fwd)
            fire_id = signal_db.insert_fire(db, cid, fe)
            fired_tokens.add(fe.token)
            stats["fires"] += 1
            if signals_live.in_scope(fe.config_id, fe.sec):
                stats["in_scope"] += 1
                due = fe.local_ts + config.S1_HORIZON_S + config.EVAL_GUARD_S
                pending.append({"fire_id": fire_id, "fe": fe, "s1_due": due, "s2_due": due,
                                "fill_due": fe.local_ts + config.FILL_DELAY_S + config.EVAL_GUARD_S,
                                "s1_done": False, "s2_done": False, "fill_done": False,
                                "decided_ats": []})
        _drain_pending(db, pm, btc, pending, now)
    # window roll (live: resolve, then settle stragglers with the roll-time `now`, then drop)
    now = epoch + WIN + tick_phase
    pm.now = btc.now = now
    stats["resolved_rows"] = _resolve_window(db, detector, epoch, sorted(fired_tokens), now)
    _drain_pending(db, pm, btc, pending, now)
    return stats


def _write_meta(db: Path, meta: dict) -> None:
    import sqlite3
    con = sqlite3.connect(str(db))
    con.execute("CREATE TABLE IF NOT EXISTS replay_meta (key TEXT PRIMARY KEY, value TEXT)")
    con.executemany("INSERT OR REPLACE INTO replay_meta VALUES (?,?)",
                    [(k, json.dumps(v) if not isinstance(v, str) else v) for k, v in meta.items()])
    con.commit(); con.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=date.fromisoformat, required=True, help="UTC date, inclusive")
    ap.add_argument("--end", type=date.fromisoformat, required=True, help="UTC date, inclusive")
    ap.add_argument("--db", type=Path, required=True, help="output sqlite (created; must not exist)")
    ap.add_argument("--trades-dir", type=Path, default=DEFAULT_TRADES_DIR)
    ap.add_argument("--feeds-dir", type=Path, default=DEFAULT_FEEDS_DIR)
    ap.add_argument("--start-ts", type=int, default=None, help="first epoch_start to replay")
    ap.add_argument("--stop-ts", type=int, default=None, help="last epoch_start to replay (inclusive)")
    ap.add_argument("--ts-shift", type=float, default=-1.5,
                    help="seconds added to API trade timestamps (calibrated on the live overlap)")
    ap.add_argument("--tick-phase", type=float, default=0.5, help="engine tick offset into each second")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if args.db.exists():
        raise SystemExit(f"refusing to overwrite existing {args.db}")
    signal_db.ensure_tables(args.db)
    _write_meta(args.db, {"start": str(args.start), "end": str(args.end), "start_ts": args.start_ts,
                          "stop_ts": args.stop_ts, "ts_shift": args.ts_shift, "tick_phase": args.tick_phase,
                          "trades_dir": str(args.trades_dir), "feeds_dir": str(args.feeds_dir),
                          "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds")})

    pm = OfflinePm(args.ts_shift)
    btc = OfflineBtc()
    detector = MultiDetector(pm, btc)
    tot = {"epochs": 0, "skipped_no_trades": 0, "skipped_no_tokens": 0, "fires": 0, "in_scope": 0,
           "resolved_rows": 0, "trades": 0}
    t0 = time.perf_counter()
    d = args.start
    prev_depth_loaded = None
    while d <= args.end:
        ds = d.isoformat()
        # BTC depth: load the previous day too (30 s lookback across midnight) on first use
        for dd in (d - timedelta(days=1), d):
            if prev_depth_loaded is None or dd > prev_depth_loaded:
                rows = load_depth_day(args.feeds_dir / "btc_depth" / f"depth_{dd.isoformat()}.jsonl")
                btc.extend(rows)
                prev_depth_loaded = dd
                log.info("depth %s: %d rows", dd.isoformat(), len(rows))
        book_rows, book_toks = load_book_day(args.feeds_dir / "pm_btcupdown" / f"pm_btcupdown_{ds}.jsonl")
        log.info("book %s: %d epochs", ds, len(book_rows))
        midnight = int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())
        for i in range(86400 // WIN):
            epoch = midnight + i * WIN
            if args.start_ts is not None and epoch < args.start_ts:
                continue
            if args.stop_ts is not None and epoch > args.stop_ts:
                continue
            tp = args.trades_dir / f"{SLUG_PREFIX}{epoch}.jsonl"
            if not tp.exists():
                tot["skipped_no_trades"] += 1
                continue
            toks = dict(book_toks.get(epoch, {}))
            if len(toks) < 2:
                toks.update({k: v for k, v in tokens_from_trades(tp).items() if k not in toks})
            if not ({"Up", "Down"} <= set(toks)):
                tot["skipped_no_tokens"] += 1
                continue
            tot["trades"] += pm.load_epoch(tp, book_rows.get(epoch, {}))
            tokens = [("Up", toks["Up"]), ("Down", toks["Down"])]
            st = replay_epoch(args.db, epoch, pm, btc, detector, tokens, args.tick_phase)
            tot["epochs"] += 1
            for k in ("fires", "in_scope", "resolved_rows"):
                tot[k] += st[k]
        log.info("%s done: %s  (%.0fs)", ds, tot, time.perf_counter() - t0)
        d += timedelta(days=1)
    _write_meta(args.db, {"totals": tot, "elapsed_s": round(time.perf_counter() - t0, 1)})
    log.info("finished: %s", tot)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

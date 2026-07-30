"""
Live dry-run engine — SPEC_dryrun_book11.md §2-§6, §8.

Warm-up seeds the incremental rank engine from >=180d of stored bars (REST-filled to now), then each
closed 1m bar: recompute features on the tail, push the incremental deciles, run the FCFS decision,
and record FIRING/ENTRY/RESULT. On a taken entry it captures the entry book and starts a BookPoller
that measures the exit fill and hold stats. No orders are placed.

The per-bar path (`on_closed_bar`) is fed by the kline WS in `scripts/run_dryrun.py` but takes plain
dicts, so it is exercised offline in tests. Deciles here reconcile exactly with backtest.replay (§7).
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from btcusdt_perp_signal_v2 import backtest, book_feed, config, features, records_db
from btcusdt_perp_signal_v2.book_feed import BookPoller, executable_ladder
from btcusdt_perp_signal_v2.decision import Decision
from btcusdt_perp_signal_v2.ranks import RankEngine

log = logging.getLogger("btcusdt_perp_signal_v2")

_FEATURE_NAMES = sorted({feat for feat, _win in config.required_series()})


def _minute_index(open_time_ms: int) -> int:
    return open_time_ms // 60_000


class DryRunEngine:
    def __init__(self, db_path: Path, ohlcv_db_path: Path, feed=None, dry_run: bool = False) -> None:
        self.db_path = Path(db_path)
        self.ohlcv_db_path = Path(ohlcv_db_path)
        self.feed = feed
        self.dry_run = dry_run
        self.rank_engine = RankEngine()
        self.decision = Decision()
        self.buffer: pd.DataFrame | None = None
        self.armed = False
        self.last_bar_index: int | None = None
        self._poller: BookPoller | None = None
        self._current_day: str | None = None
        self._clock_drift_s = 0.0          # local - Binance server time (§2)
        self._last_drift_check = 0.0
        self._last_rewarm = 0.0
        records_db.ensure_tables(self.db_path)

    # ---- warm-up -------------------------------------------------------------
    def warm_up(self) -> None:
        need = max(config.WBARS.values()) + config.FEATURE_TAIL
        df = backtest.load_bars(self.ohlcv_db_path).tail(need + 5000).reset_index(drop=True)
        df = self._rest_fill_to_now(df)
        if len(df) < max(config.WBARS.values()):
            log.warning("warm-up has %d bars < 180d window (%d); engine will not arm until filled",
                        len(df), max(config.WBARS.values()))
        self._seed_from_frame(df)
        self._check_drift()
        # surface the runtime datetime unit — the pandas 2.x[ns]/3.0[us] difference silently broke ms
        # conversions before every cast was made unit-explicit; log it so a future change is loud.
        log.info("warm-up: %d bars, armed=%s, last_bar=%s (pandas %s, ts dtype %s)",
                 len(df), self.armed, self.last_bar_index, pd.__version__,
                 df["timestamp"].dtype if len(df) else "n/a")
        self._reconcile_restart()

    def _check_drift(self) -> None:
        """Binance server-time check (§2): warn on >1s local drift; late_ms uses the adjusted clock."""
        try:
            import json as _json
            from urllib.request import urlopen
            with urlopen("https://fapi.binance.com/fapi/v1/time", timeout=10) as r:
                server_ms = _json.loads(r.read())["serverTime"]
            self._clock_drift_s = time.time() - server_ms / 1000.0
            self._last_drift_check = time.time()
            if abs(self._clock_drift_s) > 1.0:
                log.warning("clock drift %.2fs vs Binance server time (>1s)", self._clock_drift_s)
        except Exception as e:
            log.warning("server-time check failed: %s", e)

    def _seed_from_frame(self, df: pd.DataFrame) -> None:
        """Compute features, seed the rank structures from the trailing windows, set the tail buffer
        and arming state. Shared by warm_up() and (bypassing DB/REST) the reconciliation tests."""
        feat = features.compute_features(df.copy())
        self.rank_engine.seed_from_history(feat)
        self.buffer = df.tail(config.FEATURE_TAIL).reset_index(drop=True)
        self.last_bar_index = _minute_index(int(df["timestamp"].iloc[-1].value // 1_000_000))
        self.armed = self.rank_engine.all_windows_full and len(df) >= max(config.WBARS.values())
        self.decision.set_next_entry_id(records_db.next_entry_id(self.db_path))
        if self.armed and records_db.get_state(self.db_path, "armed_at") is None:
            records_db.set_state(self.db_path, "armed_at",
                                 datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

    def _rest_fill_to_now(self, df: pd.DataFrame) -> pd.DataFrame:
        """Append any bars missing between the last stored bar and now via REST klines (§2/§8)."""
        try:
            from cex_data_feed.binance.api import fetch_klines, klines_to_dataframe
        except Exception as e:  # offline / import failure -> use stored bars as-is
            log.warning("REST fill unavailable (%s); using stored bars", e)
            return df
        if df.empty:
            return df
        last_ms = int(df["timestamp"].iloc[-1].value // 1_000_000)
        start = last_ms + 60_000
        now_ms = int(time.time() * 1000)
        added = []
        while start < now_ms - 60_000:
            kl = fetch_klines(config.SYMBOL, "1m", limit=1500, start_time_ms=start)
            if not kl:
                break
            fdf = klines_to_dataframe(kl)
            # closed bars only; cast to ms explicitly (pandas 3.0 datetime64 defaults to [us], so a
            # raw astype(int64)//1e6 would collapse the filter to a no-op and admit the forming bar).
            close_ms = fdf["_close_time"].astype("datetime64[ms]").astype("int64")
            fdf = fdf[close_ms < now_ms]
            if fdf.empty:
                break
            added.append(fdf.drop(columns=["_close_time"], errors="ignore"))
            start = int(fdf["timestamp"].iloc[-1].value // 1_000_000) + 60_000
            if len(fdf) < 1500:
                break
        if not added:
            return df
        out = pd.concat([df] + added, ignore_index=True)
        out = out.drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
        log.info("REST-filled %d bars to now", sum(len(a) for a in added))
        return out

    def _reconcile_restart(self) -> None:
        pos = records_db.get_state(self.db_path, "open_position")
        if not pos:
            return
        if pos["scheduled_exit_bar"] <= (self.last_bar_index or 0):
            # exit bar passed while the process was down -> missed_exit, excluded from stats (§8)
            from btcusdt_perp_signal_v2.decision import Result
            r = Result(entry_id=pos["entry_id"], entry_bar_index=pos["entry_bar"],
                       exit_bar_index=pos["scheduled_exit_bar"], ts_exit=0, cell_id=pos["cell_id"],
                       side=pos["side"], entry_close_px=pos["entry_close"], exit_close_px=None,
                       ret_research_bps=None, status="missed_exit")
            records_db.insert_result(self.db_path, r)
            records_db.set_state(self.db_path, "open_position", None)
            log.warning("restart: entry %s missed its exit bar; wrote missed_exit", pos["entry_id"])
        else:
            self.decision.resume(pos, records_db.next_entry_id(self.db_path))
            log.info("restart: resumed open position entry %s (exit bar %s)",
                     pos["entry_id"], pos["scheduled_exit_bar"])

    # ---- per closed bar ------------------------------------------------------
    def on_closed_bar(self, bar: dict) -> None:
        """bar: {open_time_ms, close_time_ms, open, high, low, close, volume,
        taker_buy_base_volume, num_trades}."""
        bi = _minute_index(bar["open_time_ms"])
        if self.last_bar_index is not None and bi <= self.last_bar_index:
            return  # duplicate / already processed
        if self.last_bar_index is not None and bi > self.last_bar_index + 1:
            missing = bi - self.last_bar_index - 1
            # refill the missed bars and run each through the normal path so the rank engine and the
            # decision state machine see every bar in order (else deciles offset + exits never settle).
            # A too-long gap, or one REST cannot fully refill, would reintroduce that desync — so
            # disarm and re-warm from contiguous history instead of continuing offset.
            if missing > config.MAX_CONSEC_MISSING or not self._backfill_missing(bi):
                if time.time() - self._last_rewarm < config.REWARM_COOLDOWN_S:
                    log.warning("gap at bar %s but re-warmed <%ds ago; dropping bar",
                                bi, config.REWARM_COOLDOWN_S)
                    return
                log.warning("gap not fully refilled (missing=%d at bar %s, filled to %s): re-warming",
                            missing, bi, self.last_bar_index)
                records_db.set_state(self.db_path, "last_gap",
                                     {"at_bar": bi, "missing": missing, "filled_to": self.last_bar_index})
                self.armed = False
                self.warm_up()
                self._last_rewarm = time.time()
                return
        self._process_bar(bar)

    def _process_bar(self, bar: dict, backfilled: bool = False) -> None:
        """Append one bar, recompute tail features, push deciles, arm, and run the decision."""
        bi = _minute_index(bar["open_time_ms"])
        self.buffer = pd.concat([self.buffer, self._bar_row(bar)], ignore_index=True)
        if len(self.buffer) > config.FEATURE_TAIL:
            self.buffer = self.buffer.iloc[-config.FEATURE_TAIL:].reset_index(drop=True)
        feat = features.compute_features(self.buffer.copy())
        last = feat.iloc[-1]
        deciles = self.rank_engine.push_bar({n: last.get(n) for n in _FEATURE_NAMES})
        self.last_bar_index = bi

        if not self.armed:
            if not self.rank_engine.all_windows_full:
                return
            # this bar completes every window -> arm and decide on it (matches backtest's first
            # armed bar, whose 180d window is exactly full).
            self.armed = True
            records_db.set_state(self.db_path, "armed_at",
                                 datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
            log.info("engine armed at bar %s", bi)

        if self.feed is not None and time.time() - self._last_drift_check >= 3600:
            self._check_drift()
        late_ms = max(0, int((time.time() - self._clock_drift_s) * 1000 - bar["close_time_ms"]))
        ev = self.decision.on_bar(bi, bar["close_time_ms"], float(bar["close"]), deciles, late_ms)
        self._record(ev, backfilled)
        self._maybe_daily_summary(bar["close_time_ms"])

    @staticmethod
    def _bar_row(bar: dict) -> pd.DataFrame:
        return pd.DataFrame([{
            "timestamp": pd.Timestamp(bar["open_time_ms"], unit="ms"),
            "open": float(bar["open"]), "high": float(bar["high"]), "low": float(bar["low"]),
            "close": float(bar["close"]), "volume": float(bar["volume"]),
            "num_trades": int(bar.get("num_trades") or 0),
            "taker_buy_base_volume": float(bar["taker_buy_base_volume"]),
        }])

    def _backfill_missing(self, bi: int) -> bool:
        """Fetch bars for the missing minute indices [last+1 .. bi-1] via REST. Returns True only if
        REST supplies the whole contiguous run (validated BEFORE pushing, so a short or hole-y reply
        pushes nothing and the caller re-warms — no partial desync)."""
        first = self.last_bar_index + 1
        want = list(range(first, bi))
        bars = self._rest_bars(first * 60_000, bi * 60_000)
        got = [_minute_index(b["open_time_ms"]) for b in bars]
        log.warning("gap: backfill %d/%d bars for indices [%d..%d]", len(got), len(want), first, bi - 1)
        if got != want:                       # short, out-of-order, or a middle/start hole
            return False
        for b in bars:
            self._process_bar(b, backfilled=True)
        return True

    def _rest_bars(self, start_ms: int, end_ms: int) -> list:
        """Normalized closed-bar dicts with open_time in [start_ms, end_ms) from REST klines."""
        try:
            from cex_data_feed.binance.api import fetch_klines
        except Exception as e:
            log.warning("REST backfill unavailable (%s)", e)
            return []
        out, cur = [], start_ms
        while cur < end_ms:
            kl = fetch_klines(config.SYMBOL, "1m", limit=1500, start_time_ms=cur)
            if not kl:
                break
            for k in kl:
                if int(k.open_time_ms) >= end_ms:
                    break
                out.append({
                    "open_time_ms": int(k.open_time_ms), "close_time_ms": int(k.close_time_ms),
                    "open": float(k.open), "high": float(k.high), "low": float(k.low),
                    "close": float(k.close), "volume": float(k.volume),
                    "taker_buy_base_volume": float(k.taker_buy_base_volume),
                    "num_trades": int(k.num_trades)})
            cur = int(kl[-1].open_time_ms) + 60_000
            if len(kl) < 1500:
                break
        return out

    # ---- recording -----------------------------------------------------------
    def _record(self, ev, backfilled: bool = False) -> None:
        for f in ev.firings:
            records_db.insert_firing(self.db_path, f)
        if ev.result is not None:
            self._handle_exit(ev.result)
        if ev.entry is not None:
            self._handle_entry(ev.entry, backfilled)

    def _handle_entry(self, entry, backfilled: bool = False) -> None:
        snap = self.feed.snapshot() if self.feed is not None else None
        book = {}
        entry_fill = None
        if snap is not None:
            buy = entry.side == "long"
            levels = snap["asks"] if buy else snap["bids"]
            ladder = executable_ladder(levels, snap["mid"], buy)
            entry_fill = ladder["fill_px"]
            book = {"mid": snap["mid"], "best_bid": snap["best_bid"], "best_ask": snap["best_ask"],
                    "spread_bps": snap["spread_bps"], "fill_px": entry_fill,
                    "slippage_bps": ladder["slippage_bps"],
                    "book_snapshot": {"bids": snap["bids"], "asks": snap["asks"]}}
        records_db.insert_entry(self.db_path, entry, book)
        records_db.set_state(self.db_path, "open_position", {
            "entry_id": entry.entry_id, "entry_bar": entry.bar_index, "entry_close": entry.close_px,
            "side": entry.side, "cell_id": entry.cell_id,
            "scheduled_exit_bar": entry.scheduled_exit_bar})
        if self.feed is not None and snap is not None:
            now = time.time()
            self._poller = BookPoller(self.feed, 1 if entry.side == "long" else -1, snap["mid"],
                                      entry_fill, now, now + config.N_HORIZON * 60, backfilled=backfilled)
            self._poller.start()
        if backfilled:
            log.warning("ENTRY id=%s taken on a backfilled bar; book + exit window offset from schedule",
                        entry.entry_id)
        log.info("ENTRY id=%s cell=%s side=%s bar=%s close=%.2f",
                 entry.entry_id, entry.cell_id, entry.side, entry.bar_index, entry.close_px)

    def _handle_exit(self, result) -> None:
        records_db.insert_result(self.db_path, result)
        records_db.set_state(self.db_path, "open_position", None)
        poller, self._poller = self._poller, None
        if poller is not None:
            threading.Thread(target=self._finalize_poller, daemon=True,
                             args=(result.entry_id, poller, result.ret_research_bps)).start()
        else:
            # no poller: restart-resumed position (entry book lost) or --no-book run. Flag it so the
            # empty execution block is a deliberate exclusion, not a silent NULL.
            reason = "no_feed" if self.feed is None else "resumed_position_no_book"
            records_db.update_result_book(self.db_path, result.entry_id,
                                          {"data_quality": {"reason": reason}})
        log.info("EXIT id=%s bar=%s ret_research=%.2f bps",
                 result.entry_id, result.exit_bar_index, result.ret_research_bps)

    def _finalize_poller(self, entry_id: int, poller: BookPoller, ret_research: float) -> None:
        poller.join(timeout=config.N_HORIZON * 60 + 120)
        try:
            block = poller.result_block(ret_research)
            records_db.update_result_book(self.db_path, entry_id, block)
            log.info("RESULT book id=%s cost_bps=%s", entry_id, block.get("cost_bps"))
        except Exception:
            log.exception("poller finalize failed for entry %s", entry_id)

    # ---- daily summary (§6.4) ------------------------------------------------
    def _maybe_daily_summary(self, close_ms: int) -> None:
        day = datetime.fromtimestamp(close_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        if self._current_day is None:
            self._current_day = day
            return
        if day != self._current_day:
            try:
                self._post_daily_summary(self._current_day)
            except Exception:
                log.exception("daily summary failed for %s", self._current_day)
            self._current_day = day

    def _post_daily_summary(self, day: str) -> None:
        from btcusdt_perp_signal_v2.summary import build_daily_summary, format_summary, send
        s = build_daily_summary(self.db_path, day)
        records_db.insert_daily_summary(self.db_path, day, s)
        if not self.dry_run:
            send(format_summary(day, s))
        else:
            log.info("DAILY %s %s", day, s)

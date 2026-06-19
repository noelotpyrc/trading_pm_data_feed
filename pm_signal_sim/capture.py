"""
Raw multi-source slice capture (BUILD_SPEC §3).

On the first fire of an `(epoch, token)` we open a capture spanning [t − L_BACK, window_end] and stage
the lookback slice; each tick we stage new forward records; at window resolution we flush the tail and
close. Co-firing configs share one capture (open_capture is get-or-create) so raw streams aren't
duplicated. Binance slices key by epoch (shared across both tokens) — staged once per epoch per tick.

Every staged record carries (local_ts, event_ts):
  - PM trades  : lookback + forward from pm.trades_since (carries the original receipt `recv`).
  - PM book TOP: forward sampling of pm.book_top each tick (best bid/ask; sizes not tracked by the
                 base feed → NULL). local_ts = now (sample), event_ts = book update receipt.
  - BTC depth20: lookback + forward from btc_depth.snapshots_since (frame carries its receipt local_ts).
  - BTC tick   : forward sampling of btc.mid_now each tick (mid only; bookTicker bid/ask not retained
                 by the reused BtcMidFeed → NULL). Lookback BTC mid is reconstructable offline from
                 raw_btc_depth + the production mid feed.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from pm_signal_sim import config, signal_db


class CaptureManager:
    """Owns open captures for the active epoch; stages raw records; flushes at resolution."""

    def __init__(self, db_path: Path, pm, btc, btc_depth) -> None:
        self.db = db_path
        self.pm, self.btc, self.btc_depth = pm, btc, btc_depth
        self._cap: dict[tuple[int, str], int] = {}          # (epoch, token) -> capture_id
        self._tokid: dict[tuple[int, str], str] = {}        # (epoch, token) -> token_id
        self._pulled_pm: dict[tuple[int, str], float] = {}  # last trade event_ts staged
        self._pulled_btc: dict[int, float] = {}             # last depth frame ts staged (per epoch)
        self._book_now: dict[tuple[int, str], float] = {}   # last book-sample now (dedup co-fires)
        self._tick_now: dict[int, float] = {}               # last btc-tick-sample now (per epoch)

    # ---- lifecycle ----
    def on_fire(self, fe) -> int:
        """Ensure/extend the (epoch, token) capture; stage the lookback slice. Return capture_id."""
        key = (fe.epoch_start, fe.token)
        t_back = fe.event_ts - config.L_BACK_SEC
        window_end_ts = fe.epoch_start + config.WINDOW_END_SEC
        t_fwd = window_end_ts if config.L_FWD_TO_WINDOW_END else fe.event_ts + config.L_FWD_SEC
        cid = signal_db.open_capture(self.db, fe.epoch_start, fe.token, t_back, t_fwd)
        self._cap[key] = cid
        self._tokid[key] = fe.token_id
        self._pulled_pm.setdefault(key, t_back)
        self._pulled_btc.setdefault(fe.epoch_start, t_back)
        self._stage_pm(fe.epoch_start, fe.token, cid, fe.local_ts)
        self._stage_btc(fe.epoch_start, fe.local_ts)
        return cid

    def on_tick(self, now_ts: float) -> None:
        """Stage forward records for all open captures up to now."""
        epochs = set()
        for (epoch, token), cid in list(self._cap.items()):
            self._stage_pm(epoch, token, cid, now_ts)
            epochs.add(epoch)
        for epoch in epochs:
            self._stage_btc(epoch, now_ts)

    def on_resolution(self, epoch: int) -> None:
        """Final flush of all captures for `epoch` through window end, then close + free state.

        Uses the real wall clock for the closing book/tick sample's local_ts: book_top()/mid_now()
        return the book at the actual flush time (~window end+), so stamping it with a fabricated
        epoch+899 would break the one-clock invariant. Forward trades/depth are event_ts-cursor pulled
        and carry their own receipt local_ts, so they're unaffected."""
        now = time.time()
        for (e, token), cid in list(self._cap.items()):
            if e != epoch:
                continue
            self._stage_pm(e, token, cid, now)
            self._stage_btc(e, now)
            signal_db.close_capture(self.db, cid)
            del self._cap[(e, token)]
            self._tokid.pop((e, token), None)
            self._pulled_pm.pop((e, token), None)
            self._book_now.pop((e, token), None)
        self._pulled_btc.pop(epoch, None)
        self._tick_now.pop(epoch, None)

    # ---- staging ----
    def _stage_pm(self, epoch: int, token: str, cid: int, now_ts: float) -> None:
        key = (epoch, token)
        token_id = self._tokid.get(key)
        if token_id is None:
            return
        # trades: lookback + forward, deduped by the cursor (strictly after last staged event_ts)
        cursor = self._pulled_pm.get(key, now_ts - config.L_BACK_SEC)
        new = [tr for tr in self.pm.trades_since(token_id, cursor) if tr[0] > cursor]
        if new:
            signal_db.insert_raw_pm_trades(self.db, cid, [
                (epoch, token, recv, ev, price, size, side)
                for (ev, price, size, side, recv) in new
            ])
            self._pulled_pm[key] = max(tr[0] for tr in new)
        # book TOP: one forward sample per distinct now (dedup co-fires within the same tick)
        if self._book_now.get(key) != now_ts:
            top = self.pm.book_top(token_id)
            if top is not None:
                signal_db.insert_raw_pm_book(self.db, cid, [
                    (epoch, token, now_ts, top.ts, top.bid, None, top.ask, None)
                ])
            self._book_now[key] = now_ts

    def _stage_btc(self, epoch: int, now_ts: float) -> None:
        cursor = self._pulled_btc.get(epoch, now_ts - config.L_BACK_SEC)
        frames = [f for f in self.btc_depth.snapshots_since(cursor) if f["ts"] > cursor]
        if frames:
            signal_db.insert_raw_btc_depth(self.db, [
                (epoch, f.get("local_ts"), f["ts"],
                 json.dumps({"mid": f["mid"], "bids": f["bids"], "asks": f["asks"]},
                            separators=(",", ":")))
                for f in frames
            ])
            self._pulled_btc[epoch] = max(f["ts"] for f in frames)
        # BTC mid tick: one forward sample per distinct now (shared per epoch)
        if self._tick_now.get(epoch) != now_ts:
            mid = self.btc.mid_now()
            if mid is not None:
                signal_db.insert_raw_btc_tick(self.db, [(epoch, now_ts, now_ts, mid, None, None)])
            self._tick_now[epoch] = now_ts

"""
pm_signal_sim signal core — per-second grid + the four candidate evaluators + de-cluster detector.

Mirrors the offline builds (btc_depth_15updown/scripts/22_build_shockdef_onsets.py, 23*): the engine
reconstructs offline's `token_state` incrementally on the integer-second grid, and the evaluators are
the SAME formulas (asym raw windows; trailmean/consistent on ffilled pdet with the L_Δ=Δ freshness
gate). De-cluster carries `prev` across invalid seconds. Level filter (p≥P_FLOOR) gates EMISSION only,
not the rising-edge detection (matches offline: onsets found at all levels, then price-band filtered).

See BUILD_SPEC §2. Firing anchor `t` = latest trade event_ts (price_asof); the grid second `sec` is
the integer second-into-window.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from pm_signal_sim import config
from pm_signal_sim.config import SignalConfig

WIN = config.WINDOW_SEC
FLOOR = config.ORIGIN_FLOOR
NAN = float("nan")


@dataclass
class FireEvent:
    config_id: str
    epoch_start: int
    token: str
    token_id: str
    sec: int
    event_ts: float          # anchor trade event ts (t)
    local_ts: float          # receipt wall clock (now)
    ratio: float
    k: float
    p_entry: float           # pdet(sec)
    entry_bid: Optional[float]
    entry_ask: Optional[float]
    btc_mid: Optional[float]
    entry_last_age_s: float   # now − event_ts


class GridState:
    """Per-(epoch, token) incremental token_state: ffilled pdet + traded + last-trade-age."""

    def __init__(self) -> None:
        self.pdet = [NAN] * WIN
        self.traded = [False] * WIN
        self._last_trade_sec = -1   # most recent sec with a real trade
        self.last_updated_sec = -1  # highest sec written (for tick-skip backfill)

    def update(self, sec: int, vwap_1s: Optional[float]) -> None:
        """Set pdet[sec]/traded[sec] for the integer second `sec`. vwap_1s = VWAP of trades in
        (sec−1, sec] (None if no trade) → traded flag + ffill of pdet."""
        if not (0 <= sec < WIN):
            return
        if vwap_1s is not None and math.isfinite(vwap_1s):
            self.pdet[sec] = vwap_1s
            self.traded[sec] = True
            self._last_trade_sec = sec
        else:
            self.traded[sec] = False
            self.pdet[sec] = self.pdet[sec - 1] if sec > 0 else NAN   # ffill last known price

    def age(self, sec: int) -> float:
        """Seconds since the last real trade as of `sec` (inf before the first trade)."""
        if sec < 0 or self._last_trade_sec < 0:
            return math.inf
        # last_trade_sec is the global latest; for a baseline point we scan back from sec.
        s = min(sec, WIN - 1)
        while s >= 0 and not self.traded[s]:
            s -= 1
        return math.inf if s < 0 else float(sec - s)

    def p(self, sec: int) -> float:
        return self.pdet[sec] if 0 <= sec < WIN else NAN


# ---- evaluators: return ratio (float) or None (invalid → carry prev, no fire) ----

def _eval_asym(cfg: SignalConfig, grid: GridState, sec: int, pm, token_id: str, epoch: int):
    """V(t−w_now, t] / V(t−gap−w_base, t−gap] via integer-second-aligned vwap_window. Inherently
    gated (None if a window is empty)."""
    p = cfg.params
    lo = p["gap"] + p["w_base"]
    if sec < lo:
        return None
    b_now = epoch + sec
    numer = pm.vwap_window(token_id, b_now - p["w_now"], b_now)
    denom = pm.vwap_window(token_id, b_now - p["gap"] - p["w_base"], b_now - p["gap"])
    if numer is None or denom is None or denom < FLOOR:
        return None
    return numer / denom


def _eval_trailmean(cfg: SignalConfig, grid: GridState, sec: int, pm, token_id: str, epoch: int):
    """pdet(sec) / mean(pdet[sec−W+1 … sec]); freshness gate age[sec−W] ≤ W."""
    W = cfg.params["w"]
    if sec < W:
        return None
    if grid.age(sec - W) > W:                       # baseline-freshness gate (L=W)
        return None
    vals = grid.pdet[sec - W + 1: sec + 1]
    if any(not math.isfinite(v) for v in vals):
        return None
    base = sum(vals) / len(vals)
    pnow = grid.p(sec)
    if base < FLOOR or not math.isfinite(pnow):
        return None
    return pnow / base


def _eval_consistent(cfg: SignalConfig, grid: GridState, sec: int, pm, token_id: str, epoch: int):
    """min over Δ∈H of pdet(sec)/pdet(sec−Δ); require ALL Δ defined AND age[sec−Δ] ≤ Δ."""
    H = cfg.params["horizons"]
    pnow = grid.p(sec)
    if not math.isfinite(pnow):
        return None
    if grid.p(sec - max(H)) < FLOOR:                # floor_ref = longest-horizon baseline
        return None
    ratios = []
    for d in H:
        if sec - d < 0:
            return None
        if grid.age(sec - d) > d:                    # per-horizon freshness gate (L_Δ=Δ)
            return None
        base = grid.p(sec - d)
        if not math.isfinite(base) or base <= 0:
            return None
        ratios.append(pnow / base)
    return min(ratios)


_EVAL = {"asym": _eval_asym, "trailmean": _eval_trailmean, "consistent": _eval_consistent}


class MultiDetector:
    """Evaluates all CONFIGS × tokens each tick; de-clustered rising-edge onsets with cooldown.

    Owns the per-(epoch, token) GridState. The engine calls update_grid() before on_tick() each tick.
    """

    def __init__(self, pm, btc, configs=None) -> None:
        self.pm = pm
        self.btc = btc
        self.cfgs = list(configs if configs is not None else config.CONFIGS)
        self._grids: dict[tuple[int, str], GridState] = {}
        self._prev: dict[tuple[str, str], float] = {}     # (config_id, token) -> prev ratio
        self._last_fire: dict[tuple[str, str], float] = {}  # (config_id, token) -> now_ts
        self._epoch: Optional[int] = None

    def _reset_epoch(self, epoch: int) -> None:
        self._epoch = epoch
        self._grids = {}
        self._prev = {}
        self._last_fire = {}

    def grid(self, epoch: int, token: str) -> GridState:
        return self._grids.setdefault((epoch, token), GridState())

    def final_pdet(self, epoch: int, token: str) -> Optional[float]:
        """Last finite ffilled pdet for (epoch, token) — the window's terminal price for resolution.
        Read at roll BEFORE update_grid() resets to the new epoch."""
        g = self._grids.get((epoch, token))
        if g is None:
            return None
        for v in reversed(g.pdet):
            if math.isfinite(v):
                return v
        return None

    def update_grid(self, epoch: int, sec: int, tokens) -> None:
        """Update each token's per-second grid up to integer second `sec`. `tokens` = [(token, token_id)].

        Backfills any seconds skipped since the last update (a >1s tick — plausible right after a fire's
        DB writes — or a restart mid-window) so trailmean/consistent windows never go blind on a NaN
        hole. `vwap_window` can compute any past 1s window exactly, so backfill is cheap and faithful.
        """
        if epoch != self._epoch:
            self._reset_epoch(epoch)
        if not (0 <= sec < WIN):
            return
        for token, token_id in tokens:
            g = self.grid(epoch, token)
            for k in range(max(0, g.last_updated_sec + 1), sec + 1):
                bk = epoch + k
                g.update(k, self.pm.vwap_window(token_id, bk - config.PDET_WIN_SEC, bk))
            g.last_updated_sec = sec

    def on_tick(self, now_ts: float, epoch: int, sec: int, tokens) -> list[FireEvent]:
        """Return FireEvents for this tick. `tokens` = [(token, token_id)] for the active window."""
        out: list[FireEvent] = []
        if not (0 <= sec < WIN):
            return out
        for token, token_id in tokens:
            grid = self.grid(epoch, token)
            pnow = grid.p(sec)
            for cfg in self.cfgs:
                ratio = _EVAL[cfg.kind](cfg, grid, sec, self.pm, token_id, epoch)
                key = (cfg.config_id, token)
                if ratio is None:
                    continue                                  # invalid → carry prev, no fire
                prev = self._prev.get(key)
                onset = ratio >= cfg.k_collect and prev is not None and prev < cfg.k_collect
                self._prev[key] = ratio                        # update prev on every VALID eval
                if not onset:
                    continue
                # --- emission filters (do NOT affect de-cluster) ---
                if not (math.isfinite(pnow) and pnow >= cfg.p_floor):   # level floor p≥0.5
                    continue
                lf = self._last_fire.get(key)
                if lf is not None and (now_ts - lf) < cfg.cooldown_s:
                    continue
                self._last_fire[key] = now_ts
                asof = self.pm.price_asof(token_id, now_ts)
                t = asof[0] if asof else now_ts
                top = self.pm.book_top(token_id)
                out.append(FireEvent(
                    config_id=cfg.config_id, epoch_start=epoch, token=token, token_id=token_id,
                    sec=sec, event_ts=t, local_ts=now_ts, ratio=ratio, k=cfg.k_collect, p_entry=pnow,
                    entry_bid=(top.bid if top else None), entry_ask=(top.ask if top else None),
                    btc_mid=self.btc.mid_now(), entry_last_age_s=now_ts - t))
        return out

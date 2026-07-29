"""
Live evaluators for the two pre-registered signals (LIVE_TEST_SPEC §0) + the base-trade fill.

These are additive: base fire detection / capture / resolution are unchanged. For every IN-SCOPE
fire (`sec >= SIGNAL_SCOPE_MIN_SEC`, config in `SIGNAL_SCOPE_CONFIGS`) the engine evaluates, all
anchored at t3 = fire+3s (both signals decide together, ≥ 2s before the fill):

  S1 `fade`     — d_mid_3 = mid(last book ≤ fire+3) − mid(first book ≥ fire), < 0.
  S2 `z30_gate` — z30 = ret_30 / (rv_30·√30) over [t3−30, t3], |z30| > 1  (ret_30 SIMPLE, rv LOG).
  fill          — at fire+5s: first book ask with local_ts ≥ fire+5 (`ask_d5`), held to expiry.

S1 and the fill read the SAME stored book rows the offline rebuild reads (raw_pm_book via
signal_db.fetch_book_full) → exact parity; S2 mirrors `z30_f3` in the frozen offline reference
(pm_shock_live_v2/_btc_features.py). `decided_at = max(newest input local_ts, fire+3)` — the final
value is only knowable at the horizon, so the honest decision time is the horizon (REVIEW §5.3);
`input_ts` keeps the raw newest-input local_ts (the gap between the two is diagnostic).
"""
from __future__ import annotations

import json
import math
import statistics
from dataclasses import dataclass
from typing import Optional

from pm_signal_sim import config


@dataclass
class SigEval:
    signal: str
    value: Optional[float]
    decision: int
    decided_at: Optional[float]   # honest decision time: max(newest input local_ts, fire+3)
    input_ts: Optional[float]     # local_ts of the newest data used
    detail: str                   # JSON
    eval_wall_ts: Optional[float] = None   # wall clock at evaluation (engine jitter; set by caller)


def in_scope(config_id: str, sec: int) -> bool:
    return sec >= config.SIGNAL_SCOPE_MIN_SEC and config_id in config.SIGNAL_SCOPE_CONFIGS


def _mid(bid, ask) -> Optional[float]:
    return (bid + ask) / 2.0 if (bid is not None and ask is not None) else None


def _decided_at(fe, input_ts) -> Optional[float]:
    """max(newest input local_ts, fire+3) — the value is only knowable at the horizon (REVIEW §5.3)."""
    if input_ts is None:
        return None
    return max(input_ts, fe.local_ts + config.S1_HORIZON_S)


def eval_s1(fe, book_full) -> SigEval:
    """S1 `fade`. book_full = [(local_ts, bid, bid_sz, ask, ask_sz), ...] ordered by local_ts.
    mid_fire = mid of the FIRST row ≥ fire (matches offline _pm_features), falling back to the last
    row ≤ fire only if none exists yet. mid_last = mid of the last row ≤ fire+3."""
    fire = fe.local_ts
    target = fire + config.S1_HORIZON_S
    mid_fire = None
    for lts, bid, _bsz, ask, _asz in book_full:          # first row ≥ fire
        if lts >= fire:
            m = _mid(bid, ask)
            if m is not None:
                mid_fire = m
                break
    fell_back = False
    if mid_fire is None:                                  # fallback: last row ≤ fire
        for lts, bid, _bsz, ask, _asz in book_full:
            if lts <= fire:
                m = _mid(bid, ask)
                if m is not None:
                    mid_fire = m
        fell_back = mid_fire is not None
    mid_last = ts_last = None
    for lts, bid, _bsz, ask, _asz in book_full:           # last row ≤ fire+3
        if lts <= target:
            m = _mid(bid, ask)
            if m is not None:
                mid_last, ts_last = m, lts
    value = (mid_last - mid_fire) if (mid_fire is not None and mid_last is not None) else None
    decision = 1 if (value is not None and value < 0) else 0
    detail = json.dumps({"mid_fire": mid_fire, "mid_last": mid_last, "row_ts": ts_last,
                         "fallback": fell_back}, separators=(",", ":"))
    return SigEval("fade", value, decision, _decided_at(fe, ts_last), ts_last, detail)


def eval_s2(fe, btc) -> SigEval:
    """S2 `z30_gate`, anchored at t3 = fire+3 (REVIEW §5.1). BTC mid on a 1s grid over [t3−30, t3]:
    ret_30 = SIMPLE return (mid(t3)/mid(t3−30) − 1)·1e4; rv_30 = std of the 1s LOG returns (bps)."""
    t3 = fe.local_ts + config.S1_HORIZON_S
    W = config.Z30_WINDOW_S
    mids = [btc.mid_at(t3 - W + i) for i in range(W + 1)]          # t3−30 … t3  (W+1 points)
    if mids[W] is None:
        mids[W] = btc.mid_now()
    if any(m is None or m <= 0 for m in mids):
        detail = json.dumps({"reason": "insufficient_btc", "n_obs": 0}, separators=(",", ":"))
        return SigEval("z30_gate", None, 0, _decided_at(fe, t3), t3, detail)
    log_rets = [math.log(mids[i + 1] / mids[i]) * 1e4 for i in range(W)]   # 1s log returns, bps
    ret_30 = (mids[W] / mids[0] - 1.0) * 1e4                               # simple return, bps
    rv_30 = statistics.stdev(log_rets) if len(log_rets) >= 2 else 0.0
    z30 = ret_30 / (rv_30 * math.sqrt(W)) if rv_30 > 0 else None
    decision = 1 if (z30 is not None and abs(z30) > 1) else 0
    detail = json.dumps({"ret_30": ret_30, "rv_30": rv_30, "n_obs": len(log_rets)},
                        separators=(",", ":"))
    return SigEval("z30_gate", z30, decision, _decided_at(fe, t3), t3, detail)


def compute_fill(fe, book_full):
    """First book ask with local_ts ≥ fire+5 (`ask_d5`). Returns (fill_ts, fill_ask, fill_ask_sz)
    or None. book_full ordered by local_ts."""
    target = fe.local_ts + config.FILL_DELAY_S
    for lts, _bid, _bsz, ask, ask_sz in book_full:
        if lts >= target and ask is not None:
            return (lts, ask, ask_sz)
    return None

"""
Incremental trailing rank -> decile — SPEC_book25_fcfs.md §3, SPEC_dryrun_book11.md §3.1.

One order-statistic structure per (feature, window): a sorted list kept in step with a time-ordered
deque of the trailing window. push() inserts the new bar, evicts the departing bar, and returns the
averaged-tie percentile rank of the current value — exactly reproducing
`series.rolling(W, min_periods=W//2).rank(pct=True)`, decile = clip(floor(pct·10),0,9)+1.

This is the §3.1-mandated exact path (no decile-cut-point cache): the cross-rate features are integer
counts with heavy ties, where a threshold cache disagrees with the averaged-tie rank on ~20% of bars.
At one push/minute the O(W) list insert is negligible.
"""
from __future__ import annotations

import bisect
import math
from collections import deque
from typing import Optional

from btcusdt_perp_signal_v2 import config


def _is_nan(x) -> bool:
    return x is None or (isinstance(x, float) and math.isnan(x))


class RollingRank:
    """Trailing-window averaged-tie percentile rank of the most recent value."""

    def __init__(self, window: int, min_periods: Optional[int] = None) -> None:
        self.window = window
        self.min_periods = window // 2 if min_periods is None else min_periods
        self._vals: deque = deque()     # last `window` values in time order (may hold NaN)
        self._sorted: list = []         # the non-NaN values currently in the window, sorted

    @property
    def full(self) -> bool:
        return len(self._vals) >= self.window

    def seed(self, values) -> None:
        """One-shot warm-up: adopt the trailing window (time-ordered, last `window` kept) as current
        state, so the next push() ranks against exactly the same window pandas would. O(W log W)."""
        vals = [None if _is_nan(v) else float(v) for v in list(values)[-self.window:]]
        self._vals = deque(vals)
        self._sorted = sorted(v for v in vals if v is not None)

    def push(self, value) -> Optional[int]:
        """Append `value`, evict the bar leaving the window, return the current decile (1..10) or
        None when the value is NaN or fewer than min_periods valid obs are in the window."""
        nan = _is_nan(value)
        v = None if nan else float(value)
        self._vals.append(v)
        if len(self._vals) > self.window:
            old = self._vals.popleft()
            if old is not None:
                idx = bisect.bisect_left(self._sorted, old)
                del self._sorted[idx]
        if v is not None:
            bisect.insort(self._sorted, v)

        if v is None:
            return None
        n = len(self._sorted)
        if n < self.min_periods:
            return None
        lt = bisect.bisect_left(self._sorted, v)
        le = bisect.bisect_right(self._sorted, v)
        eq = le - lt
        avg_rank = lt + (eq + 1) / 2.0
        pct = avg_rank / n
        return min(9, int(pct * 10)) + 1


class RankEngine:
    """One RollingRank per (feature, window) pair the book needs; pushes a bar of feature values."""

    def __init__(self, pairs=None) -> None:
        pairs = config.required_series() if pairs is None else pairs
        self._ranks = {p: RollingRank(config.WBARS[p[1]]) for p in pairs}

    def push_bar(self, feature_values: dict) -> dict:
        """feature_values: {feature_name: value}. Returns {(feature, window): decile|None}."""
        out = {}
        for (feat, win), rr in self._ranks.items():
            out[(feat, win)] = rr.push(feature_values.get(feat))
        return out

    def seed_from_history(self, feature_frame) -> None:
        """Seed every series from a warm-up features DataFrame (its last WBARS rows per series)."""
        for (feat, _win), rr in self._ranks.items():
            rr.seed(feature_frame[feat].to_numpy())

    @property
    def all_windows_full(self) -> bool:
        return all(rr.full for rr in self._ranks.values())

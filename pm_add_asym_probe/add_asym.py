"""
add_asym_30 computed one book message at a time (copied unchanged from data_analysis/pm_lob_15m/scripts/live_signal.py),
plus the exact 10-share walk price.

  snapshot(bids, asks)                       full book ({price: size}); resets the book, counts nothing
  change(ts, is_bid, price, size, bb, ba)    one changed level with its NEW resting size; bb / ba = the best bid / ask
                                             carried in the message (NaN = missing -> not near), or None for this book's best
  value(t)                                   add_asym_30 at time t, using changes with t - 30 < ts <= t
d = new - old size at that price; a change counts when d > 0 and |price - best on its side| <= 0.02;
add_asym_30 = (bid adds - ask adds) / (bid adds + ask adds).
"""
from __future__ import annotations

import math
from collections import deque

NEAR, LOOK = 0.02, 30.0


class AddAsym:
    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.adds: deque = deque()                  # (ts, is_bid, shares added)
        self.add_b = self.add_a = 0.0

    def snapshot(self, bids: dict, asks: dict) -> None:
        self.bids, self.asks = dict(bids), dict(asks)

    def change(self, ts: float, is_bid: bool, price: float, size: float, bb: float | None = None, ba: float | None = None) -> None:
        book = self.bids if is_bid else self.asks
        old = book.get(price, 0.0)
        if size <= 0:
            book.pop(price, None)
        else:
            book[price] = size
        d = size - old
        if d <= 0:
            return
        ref = bb if is_bid else ba
        if ref is None:
            ref = (max(self.bids) if self.bids else math.nan) if is_bid else (min(self.asks) if self.asks else math.nan)
        if not (abs(price - ref) <= NEAR + 1e-9):     # NaN ref -> not near
            return
        self.adds.append((ts, is_bid, d))
        if is_bid:
            self.add_b += d
        else:
            self.add_a += d

    def value(self, t: float) -> float:
        while self.adds and self.adds[0][0] <= t - LOOK:
            _, b, d = self.adds.popleft()
            if b:
                self.add_b -= d
            else:
                self.add_a -= d
        if not self.adds:
            self.add_b = self.add_a = 0.0
            return math.nan
        s = self.add_b + self.add_a
        return (self.add_b - self.add_a) / s if s > 0 else math.nan


def walk(book: dict, q: float, buy: bool) -> float:
    """Average price of the first q shares, walking the asks up (buy) or the bids down (sell); NaN if fewer than q shares."""
    got = cost = 0.0
    for p in sorted(book, reverse=not buy):
        take = min(book[p], q - got)
        got += take
        cost += take * p
        if got >= q - 1e-9:
            return cost / q
    return math.nan

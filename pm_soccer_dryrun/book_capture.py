"""Post-fire depth recording; only current ladders live in memory outside windows."""
from collections import Counter
from decimal import Decimal
import math

from .storage import day


def window_ms(minutes):
    if not math.isfinite(minutes * 60_000) or minutes * 60_000 < 1:
        raise ValueError("book-window-minutes must be finite and at least 1/60000")
    return int(minutes * 60_000)


def level(raw):
    price, size = Decimal(str(raw["price"])), Decimal(str(raw["size"]))
    if not price.is_finite() or not 0 <= price <= 1 or not size.is_finite() or size < 0:
        raise ValueError("Invalid depth price/size")
    return price, str(raw["size"]), size == 0


class BookCapture:
    """Call under Store.lock, in delivery order (including within a WS frame).

    Book snapshots seed complete ladders, absolute-size changes update them.
    Gaps invalidate those ladders; deltas alone cannot restore completeness.
    Derived opening snapshots explicitly retain the last source observation
    time, which is not a claim that the upstream book was fresh at fire time.
    """

    def __init__(self, store, minutes=5):
        self.store = store
        self.duration_ms = window_ms(minutes)
        self.routing = {}
        self.books = {}
        self.ends = {}
        self.counts = Counter()
        self.clock_second = None

    def set_routing(self, routing):
        self.books = {token: book for token, book in self.books.items()
                      if token in routing and self.routing.get(token) == routing[token]}
        slugs = {info[0] for info in routing.values()}
        self.ends = {slug: end for slug, end in self.ends.items() if slug in slugs}
        self.routing = routing

    def gap(self):
        self.books.clear()
        self.counts["cache_invalidations"] += 1

    def _marker(self, slug, ms, payload):
        self.store.append("collector", "book_window", {"slug": slug, "recorded_ms": ms, **payload},
                          f"book_windows/{slug}/{day(ms)}.jsonl")

    def fire(self, row):
        if row.get("kind") != "fire" or row.get("f_minute30") is not True or row.get("f_leader1_up") is not True:
            return
        self._open(row, row["fire_recv_ms"], {"fire_recv_ms": row["fire_recv_ms"]})

    def fallback_candidate(self, row, evidence):
        if row.get("kind") != "candidate" or not evidence:
            return
        self.counts["fotmob_candidate_windows"] += 1
        self._open(row, row["emitted_ms"], evidence)

    def _open(self, row, ms, evidence):
        slug = row["slug"]
        previous = self.ends.get(slug, 0)
        end = max(previous, ms + self.duration_ms)
        self.ends[slug] = end
        active = previous > ms
        self.counts["window_extensions" if active else "windows_opened"] += 1
        tokens = {token: info for token, info in self.routing.items() if info[0] == slug}
        self._marker(slug, ms, {"event": "extend" if active else "open", "candidate_id": row["candidate_id"],
                               **evidence, "end_ms": end, "duration_ms": self.duration_ms,
                               "tokens": sorted(tokens), "missing_tokens": sorted(set(tokens) - self.books.keys())})
        if active:
            return
        for token, (_, role, outcome) in tokens.items():
            book = self.books.get(token)
            if book is None:
                continue
            self._marker(slug, ms, {"event": "opening_snapshot", "source": "local_book_state",
                                   "candidate_id": row["candidate_id"], "asset_id": token,
                                   "book_role": role, "outcome": outcome,
                                   "book_asof_recv_ms": book["recv_ms"],
                                   "exchange_timestamp": book["exchange_timestamp"],
                                   "seed_recv_ms": book["seed_recv_ms"],
                                   "seed_metadata": book["seed_metadata"], "tick_size": book["tick_size"],
                                   **{side: [{"price": str(price), "size": size} for price, size in
                                             sorted(book[side].items(), reverse=side == "bids")]
                                      for side in ("bids", "asks")}})

    def _update(self, row):
        msg, ms = row["message"], row["recv_ms"]
        if msg["event_type"] == "book":
            token = str(msg["asset_id"])
            affected = [token]
        else:
            affected = [str(change["asset_id"]) for change in msg["price_changes"]]
        try:
            if msg["event_type"] == "book":
                book = {"seed_recv_ms": ms, "recv_ms": ms, "exchange_timestamp": msg.get("timestamp"),
                        "seed_metadata": {k: v for k, v in msg.items() if k not in ("bids", "asks")},
                        "tick_size": msg.get("tick_size")}
                for side in ("bids", "asks"):
                    book[side] = {}
                    for raw in msg[side]:
                        price, size, deleted = level(raw)
                        if not deleted:
                            book[side][price] = size
                self.books[token] = book
            else:
                for change, token in zip(msg["price_changes"], affected):
                    book = self.books.get(token)
                    if book is None:
                        continue
                    side = {"BUY": "bids", "SELL": "asks"}[change["side"]]
                    price, size, deleted = level(change)
                    if deleted:
                        book[side].pop(price, None)
                    else:
                        book[side][price] = size
                    book["recv_ms"], book["exchange_timestamp"] = ms, msg.get("timestamp")
        except (KeyError, ValueError, TypeError, ArithmeticError) as exc:
            for token in affected:
                self.books.pop(token, None)
            self.counts["invalid_depth_rows"] += 1
            self._marker(row["slug"], ms, {"event": "invalid_depth", "tokens": affected, "error": str(exc)})

    def record(self, row):
        kind, ms = row["message"].get("event_type"), row["recv_ms"]
        depth = kind in ("book", "price_change")
        if depth:
            self.counts["depth_received"] += 1
            self._update(row)
            # Causal delivery order defines opening, not the WS frame timestamp:
            # a later message in the confirming frame may share an older recv_ms.
            if ms >= self.ends.get(row["slug"], 0):
                self.counts["depth_discarded"] += 1
                if self.clock_second != ms // 1000:
                    self.clock_second = ms // 1000
                    # Preserve second-close causality/replay without saving depth.
                    clock = {"slug": row["slug"], "recv_ms": ms, "source": "local_depth_clock",
                             "message": {"event_type": "capture_clock"}}
                    self.store.append("collector", "market", clock, f"pm_soccer/{row['slug']}/{day(ms)}.jsonl")
                return
            self.counts["depth_recorded"] += 1
        else:
            self.counts["fills_recorded" if kind == "last_trade_price" else "metadata_recorded"] += 1
            if kind == "tick_size_change":
                book = self.books.get(str(row["message"].get("asset_id")))
                if book is not None:
                    book["tick_size"] = row["message"].get("new_tick_size")
        self.store.append("collector", "market", row, f"pm_soccer/{row['slug']}/{day(ms)}.jsonl")

    def summary(self):
        return {"duration_ms": self.duration_ms, **self.counts, "cached_tokens": len(self.books)}

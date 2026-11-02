"""Offline ladder inspection for consumers and recording verification.

No live service imports this module. The statistical report belongs in pm_lob_nba.
"""
from decimal import Decimal

from pm_soccer_dryrun.book_capture import level


class Ladders:
    def __init__(self, tokens):
        self.tokens = set(map(str, tokens))
        self.books = {}

    def apply(self, row):
        if row.get("kind") == "gap":
            for token in row["tokens"]:
                self.books.pop(token, None)
            return
        msg = row["message"]
        kind = msg.get("event_type")
        if kind == "book":
            token = str(msg.get("asset_id"))
            if token not in self.tokens:
                return
            self.books.pop(token, None)
            book = {"recv_ms": row["recv_ms"], "tick_size": msg.get("tick_size")}
            for side in ("bids", "asks"):
                book[side] = {}
                for raw in msg[side]:
                    price, size, deleted = level(raw)
                    if not deleted:
                        book[side][price] = Decimal(size)
            self.books[token] = book
        elif kind == "price_change":
            affected = {str(c.get("asset_id")) for c in msg["price_changes"]} & self.tokens
            try:
                changes = [(str(c["asset_id"]), {"BUY": "bids", "SELL": "asks"}[c["side"]], level(c))
                           for c in msg["price_changes"] if str(c.get("asset_id")) in self.tokens]
                for token, side, (price, size, deleted) in changes:
                    if token not in self.books:
                        continue
                    if deleted:
                        self.books[token][side].pop(price, None)
                    else:
                        self.books[token][side][price] = Decimal(size)
                    self.books[token]["recv_ms"] = row["recv_ms"]
            except (KeyError, ValueError, TypeError, ArithmeticError):
                for token in affected:
                    self.books.pop(token, None)
                raise
        elif kind == "tick_size_change":
            token = str(msg.get("asset_id"))
            if token in self.books:
                self.books[token]["tick_size"] = msg.get("new_tick_size")

    def ask(self, token):
        book = self.books.get(str(token))
        if book is None or not book["asks"]:
            return None
        price = min(book["asks"])
        return {"price": price, "shares": book["asks"][price],
                "shares_within_cent": sum(size for p, size in book["asks"].items() if p <= price + Decimal("0.01"))}

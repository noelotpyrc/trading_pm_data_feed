"""Depth retention boundaries, reconstruction and causal delivery; no network."""
import json

import pytest

from pm_soccer_dryrun.book_capture import BookCapture, window_ms
from pm_soccer_dryrun.sources import market_rows
from pm_soccer_dryrun.storage import Store
from tests.test_soccer_dryrun import fill, primed


ROUTES = {"homey": ("home-away", "home", "Yes"), "homen": ("home-away", "home", "No"),
          "awayy": ("home-away", "away", "Yes"), "othery": ("other", "home", "Yes")}


def fire(ms=1000, **changes):
    return {"kind": "fire", "slug": "home-away", "candidate_id": "candidate-1", "fire_recv_ms": ms,
            "f_minute30": True, "f_leader1_up": True, **changes}


def book(token="homey", price=".5"):
    return {"event_type": "book", "asset_id": token, "timestamp": "10",
            "bids": [{"price": price, "size": "9"}], "asks": [{"price": ".8", "size": "2"}]}


def change(token="homey", price=".50", size="4", side="BUY"):
    return {"event_type": "price_change", "timestamp": "20", "price_changes": [
        {"asset_id": token, "price": price, "size": size, "side": side}]}


@pytest.fixture
def capture(tmp_path):
    store = Store(tmp_path)
    capture = BookCapture(store, 1 / 60)  # One second, to make boundary tests readable.
    capture.set_routing(ROUTES)
    yield capture
    store.close()


def receive(capture, msg, ms):
    with capture.store.lock:
        for row in market_rows(msg, ms, ROUTES):
            capture.record(row)


def rows(capture, directory):
    capture.store.flush()
    return sorted((json.loads(line) for path in (capture.store.root / directory).rglob("*.jsonl")
                   for line in path.read_text().splitlines()), key=lambda row: row["event_seq"])


def depth(capture):
    return [r for r in rows(capture, "pm_soccer") if r["message"]["event_type"] in ("book", "price_change")]


@pytest.mark.parametrize("changes", [{"kind": "candidate"}, {"f_minute30": False},
                                      {"f_leader1_up": False}, {"f_leader1_up": None}])
def test_only_filter_passing_fires_open_windows(capture, changes):
    capture.fire(fire(**changes))
    receive(capture, book(), 1000)
    assert depth(capture) == []
    assert rows(capture, "book_windows") == []


def test_window_bounds_extension_match_scope_and_always_fills(capture):
    receive(capture, book(), 999)
    capture.fire(fire())
    receive(capture, [book(), book("homen"), book("awayy"), book("othery")], 1000)
    capture.fire(fire(1500, candidate_id="candidate-2"))
    capture.fire(fire(2300, f_leader1_up=False))  # Does not extend.
    receive(capture, change(), 2499)
    receive(capture, book(), 2500)
    receive(capture, fill("home", ".5", 0, 0)["payload"]["message"], 2501)
    receive(capture, fill("home", ".5", 0, 0, outcome="No")["payload"]["message"], 2502)
    assert [(r["recv_ms"], r["slug"]) for r in depth(capture)] == [(1000, "home-away")] * 3 + [(2499, "home-away")]
    markers = rows(capture, "book_windows")
    assert [r["event"] for r in markers] == ["open", "opening_snapshot", "extend"]
    assert markers[-1]["end_ms"] == 2500
    assert capture.counts["fills_recorded"] == 2
    capture.fire(fire(3000))
    assert rows(capture, "book_windows")[-1]["book_asof_recv_ms"] == 1000  # Away book.
    assert capture.counts["windows_opened"] == 2


def test_opening_snapshot_applies_absolute_updates_and_deletes(capture):
    receive(capture, [book(), book("homen", ".2")], 100)
    receive(capture, change(size="4"), 200)
    receive(capture, change(price=".8", size="0", side="SELL"), 300)
    receive(capture, change(token="homen", price=".20", size="7"), 400)
    capture.fire(fire())
    snapshots = [r for r in rows(capture, "book_windows") if r["event"] == "opening_snapshot"]
    assert snapshots[0]["bids"] == [{"price": "0.5", "size": "4"}]
    assert snapshots[0]["asks"] == []
    assert snapshots[0]["recorded_ms"] == 1000
    assert snapshots[0]["book_asof_recv_ms"] == 300 and snapshots[0]["seed_recv_ms"] == 100
    assert snapshots[0]["exchange_timestamp"] == "20"
    assert snapshots[1]["bids"] == [{"price": "0.2", "size": "7"}]
    assert rows(capture, "book_windows")[0]["missing_tokens"] == ["awayy"]
    assert depth(capture) == []


def test_gaps_and_resubscriptions_do_not_reuse_stale_baselines(capture):
    receive(capture, book(), 100)
    capture.gap()
    receive(capture, change(), 200)
    capture.fire(fire())
    assert rows(capture, "book_windows")[0]["missing_tokens"] == ["awayy", "homen", "homey"]
    assert not capture.books
    receive(capture, book(), 1001)  # Fresh wire snapshot inside active window.
    assert len(depth(capture)) == 1
    capture.set_routing({})
    capture.set_routing(ROUTES)
    assert not capture.books and not capture.ends


def test_opening_retains_seed_metadata_and_current_tick_size(capture):
    receive(capture, {**book(), "tick_size": "0.01", "hash": "seed-hash"}, 100)
    receive(capture, {"event_type": "tick_size_change", "asset_id": "homey",
                      "old_tick_size": "0.01", "new_tick_size": "0.001"}, 200)
    capture.fire(fire())
    snapshot = rows(capture, "book_windows")[-1]
    assert snapshot["tick_size"] == "0.001"
    assert snapshot["seed_metadata"]["tick_size"] == "0.01"
    assert snapshot["seed_metadata"]["hash"] == "seed-hash"
    assert snapshot["book_asof_recv_ms"] == 100
    assert capture.counts["metadata_recorded"] == 1


def test_invalid_depth_invalidates_cache_without_losing_wire_data(capture):
    receive(capture, book(), 100)
    capture.fire(fire())
    receive(capture, change(price="NaN"), 1001)
    assert "homey" not in capture.books
    assert len(depth(capture)) == 1
    assert rows(capture, "book_windows")[-1]["event"] == "invalid_depth"
    receive(capture, change(), 1100)
    assert "homey" not in capture.books


def test_later_message_in_confirming_frame_is_saved_after_opening(capture):
    receive(capture, book(), 100)
    def deliver(event):
        if event["payload"]["message"]["event_type"] == "last_trade_price":
            capture.fire(fire(1005))  # Evaluation later than frame receipt.
    capture.store.on_event = deliver
    receive(capture, [fill("home", ".56", 0, 0)["payload"]["message"], change(size="6")], 1000)
    snapshot = [r for r in rows(capture, "book_windows") if r["event"] == "opening_snapshot"][0]
    assert snapshot["bids"][0]["size"] == "9"  # No future update in opening state.
    assert len(depth(capture)) == 1 and depth(capture)[0]["recv_ms"] == 1000
    assert list(capture.books["homey"]["bids"].values()) == ["6"]


def test_suppressed_depth_still_expires_window_and_is_replayable(capture):
    engine = primed()
    engine.handle(fill("home", ".56", 1100, 100), 1100)
    outputs = []
    capture.store.on_event = lambda e: outputs.extend(engine.handle(e, e["payload"]["recv_ms"]))
    receive(capture, book(), 2101)
    receive(capture, change(), 2102)
    assert [r["kind"] for r in outputs] == ["candidate_result"]
    recorded = rows(capture, "pm_soccer")
    assert len(recorded) == 1 and recorded[0]["message"]["event_type"] == "capture_clock"
    replay = primed()
    replay.handle(fill("home", ".56", 1100, 100), 1100)
    assert replay.handle({"kind": "market", "seq": recorded[0]["event_seq"], "payload": recorded[0]}, 2101) == outputs


@pytest.mark.parametrize("minutes", [0, -1, float("nan"), float("inf"), 1e308, 0.00000001])
def test_invalid_window_duration(minutes):
    with pytest.raises(ValueError, match="book-window-minutes"):
        window_ms(minutes)


def tops(capture):
    return [r for r in rows(capture, "top")]


def test_top_rows_track_best_levels_outside_windows(capture):
    receive(capture, book(), 100)
    receive(capture, {**change(price=".55", size="3"), "price_changes": [
        {"asset_id": "homey", "price": ".55", "size": "3", "side": "BUY", "best_bid": ".55", "best_ask": ".8"},
        {"asset_id": "homey", "price": ".8", "size": "5", "side": "SELL", "best_bid": ".55", "best_ask": ".8"}]}, 200)
    receive(capture, change(price=".40", size="1"), 300)  # Behind the best: no row.
    receive(capture, change(price=".8", size="0", side="SELL"), 400)  # Ask side emptied.
    t = tops(capture)
    assert [(r["recv_ms"], r["best_bid"], r["best_bid_size"], r["best_ask"], r["best_ask_size"]) for r in t] == [
        (100, "0.5", "9", "0.8", "2"), (200, "0.55", "3", "0.8", "5"), (400, "0.55", "3", None, None)]
    assert t[1]["wire_best_bid"] == ".55" and "wire_best_bid" not in t[0]
    assert t[0]["book_role"] == "home" and t[0]["outcome"] == "Yes" and t[0]["seed_recv_ms"] == 100
    assert depth(capture) == []  # Depth itself is still discarded outside windows.


def test_top_rows_stop_at_gaps_and_invalid_depth_until_snapshot(capture):
    receive(capture, [book(), book("homen", ".2")], 100)
    capture.gap()
    receive(capture, change(size="4"), 200)  # No ladder, no row.
    receive(capture, book(), 300)
    receive(capture, change(price="NaN"), 400)
    t = tops(capture)
    assert [r["event"] for r in t] == ["top", "top", "invalid", "top", "invalid"]
    assert t[2]["reason"] == "gap" and t[2]["tokens"] == ["homen", "homey"]
    assert t[4]["reason"] == "invalid_depth" and t[4]["tokens"] == ["homey"]

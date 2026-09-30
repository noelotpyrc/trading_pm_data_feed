"""Causality, rule parity, direct recording, and source contracts; no network."""
import json
from copy import deepcopy
from decimal import Decimal

import pytest

from pm_soccer_dryrun.engine import Engine, implied_direction, minute_of, reference
from pm_soccer_dryrun.service import engine_batch, save_score, save_live_scores, subscription_matches, update_match, save_gamma_event
from pm_soccer_dryrun.lifecycle import collection_end, in_discovery_window, HOUR_MS
from pm_soccer_dryrun.sources import (GammaClient, Resolver, market_rows, match_from_event,
                                     score_row, settlement_of, token_index, SofaClient, BackoffError)
from pm_soccer_dryrun.storage import RecordingError, Store, process_lock


MATCH = {"slug": "home-away", "kickoff_ms": 0, "sofa_event_id": 42, "home": "Home FC", "away": "Away FC",
         "books": {role: {"id": role, "tokens": {"Yes": role + "y", "No": role + "n"}}
                   for role in ("home", "draw", "away")}}


def event(kind, payload, seq=1):
    return {"seq": seq, "kind": kind, "payload": payload}


def fill(role, price, ms, seq, outcome="Yes", **extra):
    return event("market", {"slug": MATCH["slug"], "book_role": role, "outcome": outcome, "recv_ms": ms,
                           "message": {"event_type": "last_trade_price", "asset_id": role + outcome[0].lower(),
                                       "price": str(price), "timestamp": str(ms - 30), **extra}}, seq)


def primed():
    engine = Engine("test")
    engine.handle(event("match", deepcopy(MATCH)), 0)
    engine.handle(event("poll", {"poll_ms": 0, "state": "ok", "http_status": 200}), 0)
    score = {"id": 42, "home_score": 1, "away_score": 0, "poll_ms": 0, "changeTimestamp": 0,
             "status_type": "inprogress", "status": {"description": "2nd half"}, "period": 2,
             "currentPeriodStartTimestamp": 0, "stale": 0}
    engine.handle(event("score", score), 0)
    for role in MATCH["books"]:
        for i in range(5):
            assert engine.handle(fill(role, ".5", 100 + i, 10 + i), 100 + i) == []
    return engine


def test_reference_excludes_current_and_falls_back_to_last_five():
    assert reference([".5"] * 4) is None
    assert reference([".1", ".2", ".3", ".4", ".5", ".6", ".7"]) == (Decimal(".5"), Decimal(".3"))
    engine = primed()
    rows = engine.handle(fill("home", ".55", 1100, 100), 1100)
    assert rows[0]["ref"] == .5
    assert rows[0]["noise"] == .01
    assert rows[0]["kind"] == "candidate"


def test_later_confirmation_has_later_fire_time_and_original_score():
    engine = primed()
    initial = engine.handle(fill("home", ".56", 1100, 100), 1103)
    assert len(initial) == 1 and initial[0]["fire"] is None
    engine.handle(event("score", {"id": 42, "home_score": 2, "away_score": 0, "poll_ms": 1400,
                                 "changeTimestamp": 1, "stale": 0}), 1400)
    rows = engine.handle(fill("away", ".48", 1800, 101), 1805)
    fire = rows[0]
    assert fire["kind"] == "fire"
    assert fire["candidate_id"] == initial[0]["candidate_id"]
    assert fire["candidate_recv_ms"] == 1100
    assert fire["confirmation_recv_ms"] == 1800
    assert fire["fire_recv_ms"] == 1805
    assert fire["home_score"] == 1 and fire["f_leader1_up"]
    assert engine.advance(2000, 2000) == []


def test_prior_confirmation_fires_immediately_with_strict_noise_threshold():
    engine = primed()
    assert engine.handle(fill("away", ".49", 1001, 99), 1001) == []  # exactly noise, not confirmation
    rows = engine.handle(fill("home", ".56", 1100, 100), 1100)
    assert [r["kind"] for r in rows] == ["candidate"]
    assert engine.handle(fill("away", ".48", 1200, 101), 1200)[0]["kind"] == "fire"
    rows = engine.handle(fill("home", ".57", 1300, 102), 1300)
    assert [r["kind"] for r in rows] == ["candidate", "fire"]  # no live lockout


def test_second_boundary_does_not_confirm_previous_second():
    engine = primed()
    engine.handle(fill("home", ".56", 1999, 100), 1999)
    result = engine.handle(fill("away", ".48", 2000, 101), 2000)
    assert len(result) == 1
    assert result[0]["kind"] == "candidate_result" and result[0]["cross_book"] is False


def test_no_token_normalization_and_identical_messages_are_not_deduped():
    engine = primed()
    rows = engine.handle(fill("home", ".44", 1100, 100, "No", transaction_hash="same"), 1100)
    assert rows[0]["p"] == .56 and rows[0]["d"] == 1
    rows2 = engine.handle(fill("home", ".44", 1100, 101, "No", transaction_hash="same"), 1100)
    assert rows2[0]["candidate_id"] != rows[0]["candidate_id"]
    assert len(engine.state["books"]["home-away:home"]) == 7


def test_gate_failure_still_records_candidate_and_confirmation():
    engine = primed()
    engine.state["scores"]["42"][-1]["home_score"] = 2
    first = engine.handle(fill("home", ".56", 1100, 100), 1100)
    assert first[0]["gate"] is False
    result = engine.handle(fill("away", ".48", 1200, 101), 1200)
    assert result[0]["kind"] == "candidate_result"
    assert result[0]["cross_book"] is True and result[0]["fire"] is False


def test_no_score_unknown_checks_and_no_clock_do_not_drop_candidate():
    engine = primed()
    engine.state["scores"] = {}
    rows = engine.handle(fill("home", ".56", 1100, 100), 1100)
    assert rows[0]["no_score"] and rows[0]["gate"] is None
    assert rows[0]["f_minute30"] is None
    result = engine.handle(fill("away", ".48", 1200, 101), 1200)
    assert result[0]["cross_book"] and result[0]["fire"] is None


def test_stale_score_ignored_and_backoff_does_not_suppress():
    engine = primed()
    engine.handle(event("score", {"id": 42, "home_score": 3, "away_score": 0, "poll_ms": 1000, "stale": 1}), 1000)
    engine.handle(event("poll", {"poll_ms": 1000, "state": "backoff", "http_status": 403}), 1000)
    rows = engine.handle(fill("home", ".56", 1100, 100), 1100)
    assert rows[0]["home_score"] == 1 and rows[0]["score_stale"]
    assert rows[0]["poller_state"] == "backoff" and rows[0]["gate"] is True
    assert engine.handle(fill("away", ".48", 1200, 101), 1200)[0]["fire"]


def test_score_from_after_fill_is_never_attached():
    engine = primed()
    engine.handle(event("score", {"id": 42, "home_score": 2, "away_score": 0, "poll_ms": 1150, "stale": 0}), 1150)
    row = engine.handle(fill("home", ".56", 1100, 100), 1160)[0]
    assert row["home_score"] == 1


def test_late_processing_logged_without_retrospective_fire():
    engine = primed()
    engine.advance(2000, 2000)
    rows = engine.handle(fill("home", ".56", 1100, 100), 2100)
    assert [r["kind"] for r in rows] == ["candidate", "candidate_result"]
    assert all(r["late"] and r["fire_recv_ms"] is None for r in rows)


def test_processing_delay_does_not_backdate_or_suppress_known_fire():
    engine = primed()
    engine.handle(fill("away", ".48", 1100, 99), 2100)
    rows = engine.handle(fill("home", ".56", 1200, 100), 2200)
    assert rows[-1]["kind"] == "fire"
    assert rows[-1]["late"] and rows[-1]["fire_recv_ms"] == 2200


def test_gap_is_flagged_without_suppression():
    engine = primed()
    engine.handle(event("gap", {"recv_ms": 1000}), 1000)
    assert engine.handle(fill("home", ".56", 1100, 100), 1100)[0]["after_gap"]
    assert engine.handle(fill("away", ".48", 1200, 101), 1200)[0]["fire"]


@pytest.mark.parametrize("role,other,d,lead,expected", [
    ("home", "away", 1, None, -1), ("home", "draw", 1, 0, -1),
    ("home", "draw", 1, -1, 1), ("home", "draw", -1, 1, 1),
    ("draw", "home", 1, 1, -1), ("draw", "away", 1, 1, 1),
    ("draw", "home", -1, 1, 1), ("draw", "away", -1, 1, -1),
    ("draw", "home", -1, 0, 0), ("draw", "home", -1, None, None),
])
def test_cross_book_reference_directions(role, other, d, lead, expected):
    assert implied_direction(role, other, d, lead) == expected


def test_late_leader_hypothesis_and_draw_gate():
    snap = {"home_score": 1, "away_score": 0, "feed_minute": 75}
    assert Engine.checks("home", -1, Decimal(".85"), snap)["hypothesis"] is False
    assert Engine.checks("home", -1, Decimal(".84"), snap)["hypothesis"] is True
    snap["away_score"] = 1
    assert Engine.checks("draw", 1, Decimal(".5"), snap)["gate"] is False
    assert Engine.checks("draw", -1, Decimal(".5"), snap)["gate"] is True


def test_clock_and_swapped_mapping():
    score = {"period": 2, "currentPeriodStartTimestamp": 3000, "status_type": "inprogress"}
    assert minute_of(score, 3_600_000) == 55
    assert minute_of({**score, "period": None}, 3_600_000) is None
    assert minute_of({**score, "status_type": "finished"}, 3_600_000) is None
    engine = primed()
    match = {**MATCH, "sofa_swapped": True}
    assert engine.score_snapshot(match, 1000)["away_score"] == 1


def test_prekickoff_fills_not_in_reference():
    engine = Engine("test")
    engine.handle(event("match", {**MATCH, "kickoff_ms": 1000}), 0)
    for i in range(10):
        assert engine.handle(fill("home", ".5", 100 + i, i), 100 + i) == []
    assert not engine.state["books"]


def test_book_and_trade_transport_parsing():
    index = token_index({MATCH["slug"]: MATCH})
    trade = {"event_type": "last_trade_price", "asset_id": "homen", "price": ".4", "transaction_hash": "abc"}
    changes = {"event_type": "price_change", "price_changes": [
        {"asset_id": "homey", "price": ".6"}, {"asset_id": "homen", "price": ".4"}]}
    book = {"event_type": "book", "asset_id": "homey", "bids": [{"price": ".5", "size": "9"}]}
    rows = list(market_rows([trade, trade, changes, book], 1000, index))
    assert len(rows) == 4
    assert rows[0]["message"] == rows[1]["message"]
    assert rows[0]["outcome"] == "No"
    assert len(rows[2]["message"]["price_changes"]) == 2
    assert rows[3]["message"]["bids"][0]["size"] == "9"


def gamma_event():
    markets = []
    for role, q in [("home", "Will Home FC win on 2026-09-16?"), ("away", "Will Away FC win on 2026-09-16?"),
                    ("draw", "Will Home FC vs. Away FC end in a draw?")]:
        markets.append({"id": role, "conditionId": role, "question": q, "sportsMarketType": "moneyline",
                        "gameStartTime": "2026-09-16T18:00:00Z", "outcomes": '["No","Yes"]',
                        "clobTokenIds": json.dumps([role + "n", role + "y"]), "feeSchedule": {"rate": .05}})
    return {"id": 9, "slug": "home-away", "title": "Home FC vs. Away FC", "markets": markets}


def test_discovery_complete_trio_and_outcome_order():
    raw = gamma_event()
    match = match_from_event(raw, 1000)
    assert match["books"]["home"]["tokens"]["Yes"] == "homey"
    assert match["books"]["away"]["fee_fields"]["feeSchedule"]["rate"] == .05
    raw["markets"].pop()
    assert match_from_event(raw, 1000) is None


def test_discovery_paginates_and_dedups_tags(monkeypatch):
    monkeypatch.setattr("pm_soccer_dryrun.sources.TAGS", ["soccer", "epl"])
    monkeypatch.setattr("pm_soccer_dryrun.sources.time.sleep", lambda _: None)
    client = GammaClient()
    calls = []

    def get(path, **params):
        calls.append((path, params))
        return {"events": [{"id": 2}], "next_cursor": None} if params.get("after_cursor") else {"events": [{"id": 1}], "next_cursor": "page2"}

    monkeypatch.setattr(client, "get", get)
    assert [e["id"] for e in client.discover()] == [1, 2]
    assert len(calls) == 4 and calls[1][1]["after_cursor"] == "page2"


def test_resolver_uses_upcoming_events_and_checks_opponent(monkeypatch):
    monkeypatch.setattr("pm_soccer_dryrun.sources.time.sleep", lambda _: None)
    class Client:
        def get(self, path, **params):
            if path == "/search/all":
                return {"results": [{"type": "team", "entity": {"id": 1, "name": "Home FC", "sport": {"name": "Football"}}}]}
            if "/next/" in path:
                return {"events": [{"id": 42, "startTimestamp": 1000, "homeTeam": {"id": 2, "name": "Away FC"},
                                    "awayTeam": {"id": 1, "name": "Home FC"}}]}
            return {"events": []}

    resolver = Resolver(Client(), {"Home FC": {"id": 1, "name": "Home FC"},
                                   "Away FC": {"id": 2, "name": "Away FC"}})
    result = resolver.resolve({**MATCH, "kickoff_ms": 1_000_000})
    assert result["sofa_event_id"] == 42 and result["sofa_swapped"]
    assert resolver.resolve({**MATCH, "kickoff_ms": 1_000_000, "away": "Unrelated"})["sofa_event_id"] is None


def test_score_stale_copies_written_and_not_replaced(tmp_path):
    store = Store(tmp_path)
    ev = {"id": 42, "homeScore": {"current": 1}, "awayScore": {"current": 0},
          "status": {"type": "inprogress", "description": "2nd half"},
          "time": {"currentPeriodStartTimestamp": 100}, "changes": {"changeTimestamp": 200}}
    save_score(store, ev, 210_000, "scores")
    old = deepcopy(ev)
    old["changes"]["changeTimestamp"] = 100
    old["homeScore"]["current"] = 0
    row = save_score(store, old, 215_000, "scores")
    assert row["stale"] == 1
    assert store.get("score:42")["home_score"] == 1
    store.flush()
    assert len((tmp_path / "score/scores/1970-01-01.jsonl").read_text().splitlines()) == 2
    assert score_row(ev, 210_000)["period"] == 2
    store.close()


def test_live_scores_keep_full_tracked_payload_and_every_observation(tmp_path):
    store = Store(tmp_path)
    store.put("matches", {MATCH["slug"]: deepcopy(MATCH),
                          "future": {**MATCH, "sofa_event_id": 43, "kickoff_ms": 9_000_000},
                          "expired": {**MATCH, "sofa_event_id": 44, "collection_expired_ms": 100},
                          "unmapped": {**MATCH, "sofa_event_id": None}})
    raw = {"id": 42, "homeScore": {"current": 1}, "awayScore": {"current": 0},
           "homeTeam": {"name": "Home", "unfamiliarField": [1, 2]},
           "varInProgress": {"homeTeam": True}, "changes": {"changeTimestamp": 1}}
    events = [raw, {"id": 43}, {"id": 44}, {"id": 999, "unrelated": True}]
    for received in (1000, 6000):
        save_live_scores(store, events, received, received - 50)
    # A tracked match missing from a successful poll is not given an invented observation.
    save_live_scores(store, [{"id": 999}], 11000, 10950)
    assert store.get("score:999") is None
    assert store.get("score:43") is None
    assert store.get("score:44") is None
    assert store.get("score:42")["poll_ms"] == 6000
    store.close()
    rows = [json.loads(line) for p in (tmp_path / "score").rglob("*.jsonl") for line in p.read_text().splitlines()]
    assert len(rows) == 2
    assert [r["poll_ms"] for r in rows] == [1000, 6000]
    assert all(r["raw_event"] == raw for r in rows)
    polls = [json.loads(line) for p in (tmp_path / "polls").glob("*.jsonl") for line in p.read_text().splitlines()]
    assert [r["tracked_event_count"] for r in polls] == [1, 1, 0]
    assert polls[0]["event_count"] == 4 and polls[0]["tracked_match_count"] == 2
    assert polls[0]["mapped_event_count"] == 1


def test_new_mapping_waits_for_a_recorded_tracked_observation(tmp_path, monkeypatch):
    monkeypatch.setattr("pm_soccer_dryrun.service.now_ms", lambda: 1500)
    store = Store(tmp_path)
    store.put("matches", {MATCH["slug"]: {**MATCH, "sofa_event_id": None}})
    raw = {"id": 42, "homeScore": {"current": 1}, "awayScore": {"current": 0}}
    save_live_scores(store, [raw], 1000, 900)
    mapped = update_match(store, MATCH["slug"], {"sofa_event_id": 42}, "resolver")
    assert mapped["score_at_mapping"] is None and "score_observed_ms" not in mapped
    save_live_scores(store, [raw], 6000, 5900)
    assert store.get("matches")[MATCH["slug"]]["score_observed_ms"] == 6000
    store.close()


@pytest.mark.parametrize("scenario", ["queued_pong", "busy_no_pong", "silent", "closed"])
def test_collector_reads_backlog_before_idle_timeout(tmp_path, monkeypatch, scenario):
    import threading
    import websocket
    from pm_soccer_dryrun import service
    clock = [1000]
    stop = threading.Event()
    market = json.dumps({"event_type": "last_trade_price", "asset_id": "homey", "price": ".5", "timestamp": "1000"})
    actions = {
        "queued_pong": [(0, market, False), (0, "PONG", True)],
        "busy_no_pong": [(31, market, False), (31, market, True)],
        "silent": [(31, None, True)],
        "closed": [(1, "", True)],
    }[scenario]
    class Socket:
        def __init__(self):
            self.sent, self.closed = [], False
        def settimeout(self, timeout):
            assert timeout == .2
        def send(self, msg):
            self.sent.append(msg)
        def recv(self):
            delta, frame, finish = actions.pop(0)
            clock[0] += delta * 1000
            if finish:
                stop.set()
            if frame is None:
                raise websocket.WebSocketTimeoutException()
            return frame
        def close(self):
            self.closed = True
    socket = Socket()
    monkeypatch.setattr(websocket, "create_connection", lambda *a, **k: socket)
    monkeypatch.setattr(service, "maintenance", lambda *a: None)
    monkeypatch.setattr(service, "now_ms", lambda: clock[0])
    monkeypatch.setattr(service.time, "monotonic", lambda: clock[0] / 1000)
    store = Store(tmp_path)
    store.put("matches", {MATCH["slug"]: deepcopy(MATCH)})
    if scenario == "queued_pong":
        append = store.append
        def stalled_append(producer, kind, payload, path):
            seq = append(producer, kind, payload, path)
            if kind == "market":
                clock[0] += 31_000  # local processing stalls with a PONG queued
            return seq
        monkeypatch.setattr(store, "append", stalled_append)
    service.collect(store, stop)
    store.close()
    ops = [json.loads(line) for p in (tmp_path / "ops/collector").glob("*.jsonl") for line in p.read_text().splitlines()]
    disconnects = [r for r in ops if r.get("reason") == "disconnect"]
    assert len(disconnects) == (1 if scenario in ("silent", "closed") else 0)
    if scenario == "silent":
        assert "No websocket frame or PONG" in disconnects[0]["error"]
    if scenario == "busy_no_pong":
        health = next(r for r in ops if r.get("event") == "websocket_health")
        assert health["pong_overdue"] and health["frames"] == 2
    assert "PING" in socket.sent and socket.closed


def test_recording_path_validation_survives_handle_eviction(tmp_path):
    store = Store(tmp_path / "session", max_open_files=1)
    store.append("x", "ops", {}, "first.jsonl")
    store.append("x", "ops", {}, "second.jsonl")
    (store.root / "first.jsonl").unlink()
    (store.root / "first.jsonl").symlink_to(tmp_path / "outside.jsonl")
    with pytest.raises(RecordingError):
        store.append("x", "ops", {}, "first.jsonl")
    assert not (tmp_path / "outside.jsonl").exists()
    store.close()


def test_lifecycle_uses_later_of_close_and_fulltime_plus_30m():
    match = {**MATCH, "full_time_ms": 1000, "all_closed_ms": 2_000_000}
    assert subscription_matches({"m": match}, 1_900_000)
    assert not subscription_matches({"m": match}, 2_000_000)
    assert subscription_matches({"m": {**match, "all_closed_ms": None}}, 9_000_000)


def test_only_final_resolution_accepted():
    raw = {"closed": True, "outcomes": '["Yes","No"]', "outcomePrices": '["1","0"]'}
    assert settlement_of(raw) is None
    assert settlement_of({**raw, "umaResolutionStatus": "resolved"})["y"] == 1
    assert settlement_of({**raw, "umaResolutionStatus": "resolved", "outcomePrices": '["0.5","0.5"]'})["status"] == "non_binary_resolution"


def test_outputs_written_immediately_without_checkpoint_or_database(tmp_path):
    store, engine = Store(tmp_path), primed()
    rows = engine_batch(store, engine, [fill("home", ".56", 1100, 100)], 1100)
    path = tmp_path / "fires/test/1970-01-01.jsonl"
    assert json.loads(path.read_text())["kind"] == "candidate"
    assert store.get("engine") is None
    assert not list(tmp_path.glob("*.sqlite*"))
    engine_batch(store, engine, [fill("away", ".48", 1200, 101)], 1200)
    assert [json.loads(line)["kind"] for line in path.read_text().splitlines()] == ["candidate", "fire"]
    store.close()
    with pytest.raises(ValueError, match="fresh session"):
        Store(tmp_path)
    fresh = Store(tmp_path / "new-session")
    assert fresh.get("engine") is None and fresh.get("matches") is None
    assert Engine("test").state["pending"] == {}
    fresh.close()


def test_session_ids_disambiguate_sequence_numbers_after_restart(tmp_path):
    ids = []
    for session in ("first", "second"):
        store, engine = Store(tmp_path / session, session_id=session), primed()
        engine.session_id = session
        rows = engine_batch(store, engine, [fill("home", ".56", 1100, 100)], 1100)
        row = json.loads((store.root / "fires/test/1970-01-01.jsonl").read_text())
        ids.append(row["candidate_id"])
        assert row["session_id"] == session and row["event_seq"] == 1
        store.close()
    assert ids == ["test:first:100", "test:second:100"]


def test_mapping_survives_metadata_refresh(tmp_path):
    store = Store(tmp_path)
    update_match(store, MATCH["slug"], deepcopy(MATCH), "metadata")
    update_match(store, MATCH["slug"], {"mapping_status": "matched", "sofa_swapped": True}, "resolver")
    update_match(store, MATCH["slug"], {"title": "Refreshed"}, "metadata")
    assert store.get("matches")[MATCH["slug"]]["sofa_swapped"]
    store.close()


def test_duplicate_process_role_rejected(tmp_path):
    with process_lock(tmp_path, "engine"):
        with pytest.raises(RuntimeError):
            with process_lock(tmp_path, "engine"):
                pass


def test_live_dispatch_records_inputs_once_and_preserves_signal_joins(tmp_path):
    store = Store(tmp_path, session_id="session")
    engine = Engine("test", session_id="session")
    clock = [0]
    store.on_event = lambda item: engine_batch(store, engine, [item], clock[0])
    items = [event("match", deepcopy(MATCH)), event("poll", {"poll_ms": 0, "state": "ok"}),
             event("score", {"id": 42, "home_score": 1, "away_score": 0, "poll_ms": 0, "stale": 0,
                             "period": 2, "status_type": "inprogress", "currentPeriodStartTimestamp": 0})]
    for role in MATCH["books"]:
        items.extend(fill(role, ".5", 1000 + i, 0) for i in range(5))
    items.extend([fill("home", ".56", 1100, 0), fill("away", ".48", 1800, 0)])
    for item in items:
        clock[0] = item["payload"].get("recv_ms", 0)
        store.publish("fixture", item["kind"], item["payload"], "fixture.jsonl")
    store.flush()
    source = {r["event_seq"]: r for r in map(json.loads, (tmp_path / "fixture.jsonl").read_text().splitlines())}
    rows = list(map(json.loads, (tmp_path / "fires/test/1970-01-01.jsonl").read_text().splitlines()))
    assert [r["kind"] for r in rows] == ["candidate", "fire"]
    fire = rows[-1]
    assert source[fire["source_event_seq"]]["message"]["price"] == ".56"
    assert source[fire["confirming_event_seq"]]["message"]["price"] == ".48"
    assert fire["fire_recv_ms"] == 1800
    assert len(source) == len(items)
    assert not list(tmp_path.glob("*.sqlite*"))
    store.close()


def test_malformed_fill_does_not_poison_live_state(tmp_path):
    store = Store(tmp_path)
    engine = primed()
    rows = engine_batch(store, engine, [fill("home", "NaN", 1100, 100)], 1100)
    assert rows[0]["kind"] == "input_error"
    assert engine.state["books"]["home-away:home"] == ["0.5"] * 5
    rows = engine_batch(store, engine, [fill("home", ".56", 1200, 101)], 1200)
    assert rows[0]["kind"] == "candidate" and rows[0]["after_gap"]
    store.close()


def test_malformed_next_second_does_not_lose_pending_result(tmp_path):
    store, engine = Store(tmp_path), primed()
    _ = engine_batch(store, engine, [fill("home", ".56", 1100, 100)], 1100)
    rows = engine_batch(store, engine, [fill("home", "bad", 2100, 101)], 2100)
    assert rows[0]["kind"] == "input_error"
    rows = engine_batch(store, engine, [], 2200)
    assert rows[0]["kind"] == "candidate_result" and rows[0]["reason"] == "second_closed"
    store.close()


def test_missing_change_timestamp_does_not_reset_high_watermark(tmp_path):
    store = Store(tmp_path)
    ev = {"id": 42, "changes": {"changeTimestamp": 200}}
    save_score(store, ev, 210000, "scores")
    save_score(store, {"id": 42}, 215000, "scores")
    assert save_score(store, {"id": 42, "changes": {"changeTimestamp": 100}}, 220000, "scores")["stale"]
    store.close()


def test_final_score_replaces_market_close_lifecycle_fallback(tmp_path):
    store = Store(tmp_path)
    update_match(store, MATCH["slug"], {**MATCH, "full_time_ms": 1000,
                                      "full_time_source": "market_close_fallback"}, "metadata")
    save_score(store, {"id": 42, "status": {"type": "finished"}, "homeScore": {"current": 2},
                      "awayScore": {"current": 1}}, 2000, "scores")
    final = store.get("matches")[MATCH["slug"]]
    assert final["final_score"] == [2, 1] and final["full_time_ms"] == 2000
    store.close()


def test_sofa_backoff_blocks_additional_requests(monkeypatch):
    from types import SimpleNamespace
    calls = []
    response = SimpleNamespace(status_code=429, headers={}, json=lambda: {})
    session = SimpleNamespace(get=lambda *args, **kwargs: calls.append((args, kwargs)) or response)
    client = SofaClient(session_factory=lambda **kw: session, clock=lambda: 1000)
    with pytest.raises(BackoffError):
        client.get("/sport/football/events/live")
    with pytest.raises(BackoffError):
        client.get("/team/7/events/next/0")
    assert len(calls) == 1 and client.until_ms == 181000
    assert calls[0][1]["impersonate"] == "chrome"


@pytest.mark.parametrize("offset,expected", [
    (-4 * HOUR_MS - 1, False), (-4 * HOUR_MS, True), (0, True),
    (7 * 24 * HOUR_MS, True), (7 * 24 * HOUR_MS + 1, False),
])
def test_discovery_kickoff_boundaries(offset, expected):
    now = 100 * 24 * HOUR_MS
    assert in_discovery_window(now + offset, now) is expected


def test_discovery_does_not_admit_old_or_far_future_matches(tmp_path, monkeypatch):
    from pm_soccer_dryrun.sources import timestamp_ms
    raw = gamma_event()
    kickoff = timestamp_ms(raw["markets"][0]["gameStartTime"])
    store = Store(tmp_path)
    monkeypatch.setattr("pm_soccer_dryrun.service.now_ms", lambda: kickoff + 4 * HOUR_MS + 1)
    save_gamma_event(store, raw)
    assert store.get("matches", {}) == {}
    monkeypatch.setattr("pm_soccer_dryrun.service.now_ms", lambda: kickoff - 7 * 24 * HOUR_MS - 1)
    save_gamma_event(store, raw)
    assert store.get("matches", {}) == {}
    monkeypatch.setattr("pm_soccer_dryrun.service.now_ms", lambda: kickoff - 7 * 24 * HOUR_MS)
    save_gamma_event(store, raw)
    assert "home-away" in store.get("matches")
    # Once tracked, lifecycle refreshes must still attach late market closure.
    monkeypatch.setattr("pm_soccer_dryrun.service.now_ms", lambda: kickoff + 5 * HOUR_MS)
    for market in raw["markets"]:
        market["closed"] = True
    save_gamma_event(store, raw, discovery=False)
    assert store.get("matches")["home-away"]["all_closed_ms"] == kickoff + 5 * HOUR_MS
    store.close()


def test_no_feed_timeout_applies_even_when_mapping_succeeded():
    assert subscription_matches({"m": MATCH}, 4 * HOUR_MS - 1)
    assert subscription_matches({"m": MATCH}, 4 * HOUR_MS) == {}
    assert collection_end(MATCH) == (4 * HOUR_MS, "no_feed_or_close")
    covered = {**MATCH, "score_observed_ms": 0}
    assert subscription_matches({"m": covered}, 4 * HOUR_MS)
    assert collection_end(covered) == (None, None)


def test_market_close_evidence_disarms_no_feed_timeout():
    match = deepcopy(MATCH)
    match["books"]["home"]["closed"] = True
    assert subscription_matches({"m": match}, 4 * HOUR_MS)
    # Persisted expirations are terminal even if a later global poll sees a score.
    expired = {**MATCH, "collection_expired_ms": 4 * HOUR_MS, "score_observed_ms": 5 * HOUR_MS}
    assert not subscription_matches({"m": expired}, 5 * HOUR_MS)


def test_accepted_score_marks_actual_feed_coverage(tmp_path):
    store = Store(tmp_path)
    update_match(store, MATCH["slug"], deepcopy(MATCH), "metadata")
    save_score(store, {"id": 42}, 1000, "scores")
    assert store.get("matches")[MATCH["slug"]].get("score_observed_ms") is None
    save_score(store, {"id": 42, "homeScore": {"current": 0}, "awayScore": {"current": 0}}, 2000, "scores")
    assert store.get("matches")[MATCH["slug"]]["score_observed_ms"] == 2000
    store.close()


def test_timeout_evicts_engine_book_windows():
    engine = primed()
    engine.state["matches"]["home-away"].pop("score_observed_ms")
    engine.state["scores"] = {}
    engine.advance(4 * HOUR_MS + 60_001, 4 * HOUR_MS + 60_001)
    assert engine.state["matches"] == {} and engine.state["books"] == {}


def test_depth_only_traffic_closes_pending_second():
    engine = primed()
    engine.handle(fill("home", ".56", 1100, 100), 1100)
    rows = engine.handle(event("market", {"recv_ms": 2000,
                                          "message": {"event_type": "price_change", "price_changes": []}}, 101), 2001)
    assert len(rows) == 1 and rows[0]["kind"] == "candidate_result"
    assert rows[0]["cross_book"] is False and rows[0]["emitted_ms"] == 2001


def test_recording_failure_is_fatal_not_a_silent_drop(tmp_path, monkeypatch):
    store = Store(tmp_path)
    def fail(_):
        raise OSError("disk full")
    monkeypatch.setattr(store, "_handle", fail)
    with pytest.raises(RecordingError):
        store.publish("collector", "market", {"recv_ms": 1}, "tape.jsonl")
    with pytest.raises(RecordingError, match="previously failed"):
        store.publish("collector", "market", {"recv_ms": 2}, "tape.jsonl")
    store.close()


def test_threaded_publish_orders_callbacks_and_records(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    store = Store(tmp_path)
    seen = []
    store.on_event = lambda item: seen.append(item["seq"])
    def publish(i):
        store.publish("collector", "market", {"recv_ms": i}, "tape.jsonl")
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(publish, range(200)))
    store.close()
    rows = [json.loads(line) for line in (tmp_path / "tape.jsonl").read_text().splitlines()]
    assert seen == [r["event_seq"] for r in rows] == list(range(1, 201))
    assert len({r["recv_ms"] for r in rows}) == 200


def test_no_raw_discovery_saved_but_compact_catalog_remains(tmp_path, monkeypatch):
    from pm_soccer_dryrun.sources import timestamp_ms
    raw = gamma_event()
    raw["description"] = "UNNEEDED_PAYLOAD" * 100
    monkeypatch.setattr("pm_soccer_dryrun.service.now_ms", lambda: timestamp_ms(raw["markets"][0]["gameStartTime"]))
    store = Store(tmp_path)
    save_gamma_event(store, raw)
    store.close()
    assert not (tmp_path / "discovery").exists()
    paths = list(tmp_path.rglob("*.jsonl"))
    assert len(paths) == 1 and "matches" in paths[0].parts
    record = json.loads(paths[0].read_text())
    assert record["books"]["home"]["fee_fields"]["feeSchedule"]["rate"] == .05
    assert record["books"]["home"]["tokens"]["Yes"] == "homey"
    assert "UNNEEDED_PAYLOAD" not in paths[0].read_text()


def test_discovery_uses_only_specified_leagues(monkeypatch):
    queried = []
    client = GammaClient()
    def get(path, **params):
        queried.append(params["tag_slug"])
        return {"events": [], "next_cursor": None}
    monkeypatch.setattr(client, "get", get)
    assert list(client.discover()) == []
    assert set(queried) == {"epl", "la-liga", "sea", "bundesliga", "ligue-1", "ucl", "uel",
                            "mex", "brazil-serie-a", "mls", "fifa-friendly"}


def test_unchanged_discovery_refresh_does_not_duplicate_catalog(tmp_path, monkeypatch):
    from pm_soccer_dryrun.sources import timestamp_ms
    raw = gamma_event()
    kickoff = timestamp_ms(raw["markets"][0]["gameStartTime"])
    clock = [kickoff]
    monkeypatch.setattr("pm_soccer_dryrun.service.now_ms", lambda: clock[0])
    store = Store(tmp_path)
    save_gamma_event(store, raw)
    clock[0] += 60_000
    save_gamma_event(store, raw)
    raw["markets"][0]["feeSchedule"]["rate"] = .06
    save_gamma_event(store, raw)
    store.close()
    rows = [json.loads(l) for p in (tmp_path / "matches").rglob("*.jsonl") for l in p.read_text().splitlines()]
    assert len(rows) == 2
    assert [r["books"]["home"]["fee_fields"]["feeSchedule"]["rate"] for r in rows] == [.05, .06]


def test_run_live_delivers_signals_and_flushes_all_workers(tmp_path, monkeypatch):
    import threading
    from pm_soccer_dryrun import service
    monkeypatch.setattr(service, "now_ms", lambda: 1800)
    def collect(store, stop, capture):
        capture.set_routing(token_index({MATCH["slug"]: MATCH}))
        with store.lock:
            store.publish("metadata", "match", deepcopy(MATCH), "matches.jsonl")
            store.publish("scores", "poll", {"poll_ms": 0, "state": "ok"}, "polls.jsonl")
            store.publish("scores", "score", {"id": 42, "poll_ms": 0, "home_score": 1, "away_score": 0,
                                               "period": 2, "status_type": "inprogress", "currentPeriodStartTimestamp": 0}, "score.jsonl")
            for role in MATCH["books"]:
                for i in range(5):
                    store.publish("collector", "market", fill(role, ".5", 1000 + i, 0)["payload"], "tape.jsonl")
            for role, price, ms in [("home", ".56", 1100), ("away", ".48", 1800)]:
                store.publish("collector", "market", fill(role, price, ms, 0)["payload"], "tape.jsonl")
    monkeypatch.setattr(service, "collect", collect)
    monkeypatch.setattr(service, "scores", lambda store, stop, alias_path: stop.wait(2))
    store = Store(tmp_path, session_id="test-session")
    service.run_live(store, threading.Event(), "test")
    rows = list(map(json.loads, (tmp_path / "fires/test/1970-01-01.jsonl").read_text().splitlines()))
    assert [r["kind"] for r in rows] == ["candidate", "fire"]
    assert rows[-1]["candidate_id"].startswith("test:test-session:")
    assert store.on_event is None
    assert len((tmp_path / "tape.jsonl").read_text().splitlines()) == 17
    windows = [json.loads(line) for p in (tmp_path / "book_windows").rglob("*.jsonl") for line in p.read_text().splitlines()]
    assert windows[0]["event"] == "open" and windows[0]["end_ms"] == 301800
    assert len(windows[0]["missing_tokens"]) == 6
    store.close()


def test_main_restart_uses_fresh_session_without_replaying(tmp_path, monkeypatch):
    from pm_soccer_dryrun import service
    sessions = []
    monkeypatch.setattr(service.signal, "signal", lambda *_: None)
    def run(store, stop, run_id, alias_path, book_window_minutes):
        assert book_window_minutes == 5
        assert store.get("engine") is None and store.get("matches") is None
        store.put("engine", {"not": "persisted"})
        store.publish("test", "raw", {"value": 1}, "tape.jsonl")
        sessions.append(store.root)
    monkeypatch.setattr(service, "run_live", run)
    args = ["live", "--data-dir", str(tmp_path), "--run-id", "test"]
    service.main(args)
    service.main(args)
    assert len(set(sessions)) == 2
    assert all(len((p / "tape.jsonl").read_text().splitlines()) == 1 for p in sessions)
    assert not list(tmp_path.rglob("*.sqlite*"))
    with pytest.raises(ValueError, match="new data directory"):
        service.main(args + ["--book-window-minutes", "2"])


def test_main_rejects_old_run_without_touching_it(tmp_path, monkeypatch):
    from pm_soccer_dryrun import service
    monkeypatch.setattr(service.signal, "signal", lambda *_: None)
    old_manifest = '{"run_id":"test","code_sha256":"old"}\n'
    (tmp_path / "run.json").write_text(old_manifest)
    (tmp_path / "journal.sqlite").write_bytes(b'old-recording')
    with pytest.raises(ValueError, match="new data directory"):
        service.main(["live", "--data-dir", str(tmp_path), "--run-id", "test"])
    assert (tmp_path / "journal.sqlite").read_bytes() == b'old-recording'
    assert (tmp_path / "run.json").read_text() == old_manifest
    assert not (tmp_path / "sessions").exists()


def test_live_worker_failure_stops_other_worker_and_is_reported(tmp_path, monkeypatch):
    import threading
    from pm_soccer_dryrun import service
    exited = threading.Event()
    def fail(store, stop, capture):
        raise RuntimeError("collector failed")
    def other(store, stop, alias_path):
        assert stop.wait(2)
        exited.set()
    monkeypatch.setattr(service, "collect", fail)
    monkeypatch.setattr(service, "scores", other)
    store = Store(tmp_path)
    with pytest.raises(RuntimeError, match="Live recording worker failed"):
        service.run_live(store, threading.Event(), "test")
    assert exited.is_set()
    store.close()

"""NBA recording invariants using local fixtures and fake transports only."""
import json
import threading
from decimal import Decimal
from http.client import BadStatusLine, IncompleteRead

import pytest

from pm_nba_dryrun.backup import backup_run
from pm_nba_dryrun.book_capture import BookCapture, WireWriter, record_sports
from pm_nba_dryrun.espn_live_log import ESPNLog
from pm_nba_dryrun.replay import Ladders
from pm_nba_dryrun.service import Games, MINUTE, Service
from pm_nba_dryrun.sources import discover, espn_date, game_from_event, map_event, resolved, timestamp_ms
from pm_nba_dryrun.storage import RecordingError, Store, open_session, validate_rules

KICK = timestamp_ms("2026-10-26T00:20:00Z")
SLUG = "nba-min-no-2026-10-25"


def example_rules():
    # Synthetic definitions: actual strategy configs remain private per .gitignore.
    return {"schema_version": 1,
            "cells": [{"clock_bin": "Q1", "price_bins": [1, 1]} for _ in range(4)],
            "observations": {"side": "BUY"}, "clock_source": "ESPN plays", "clock_bins": ["Q1"],
            "clock_boundaries": "fixture", "price_bin_width": .05, "price_bins": "fixture",
            "entry": {"delay_ms": 1000, "price": "ask", "fee": "fixture", "hold": "settlement"},
            "size": "per share", "gap_policy": "exclude"}


def market(**changes):
    return {"id": "123", "slug": SLUG, "conditionId": "condition", "clobTokenIds": '["a","b"]',
            "outcomes": '["Vikings","Saints"]', "gameStartTime": "2026-10-26T00:20:00Z", "closed": False,
            "feesEnabled": True, "feeSchedule": {"rate": .05}, **changes}


def game(ms=KICK - 60 * MINUTE, **changes):
    return game_from_event({"id": "e1", "slug": SLUG, "markets": [market(**changes)]}, ms)


def board_event(event_id="401", **changes):
    return {"id": event_id, "status": {"type": {"state": "in"}}, "competitions": [{"competitors": [
        {"homeAway": "home", "team": {"name": "Saints"}},
        {"homeAway": "away", "team": {"name": "Vikings"}}]}], **changes}


def mapped():
    return {**game(), "espn_event": "401", "espn_date": "20261025", "team0_home": False}


def play(pid="1", **changes):
    return {"id": pid, "sequenceNumber": "1", "wallclock": "2026-10-26T00:30:00Z", "period": {"number": 1},
            "clock": {"displayValue": "14:25"}, "type": {"text": "Rush"}, "scoringPlay": False,
            "awayScore": 0, "homeScore": 0, "text": "rush", "modified": "m1", **changes}


def summary(ps, state="in", previous=None):
    return {"header": {"competitions": [{"competitors": [
        {"homeAway": "away", "score": "0"}, {"homeAway": "home", "score": "6"}],
        "status": {"displayClock": "14:25", "period": 1, "type": {"state": state}}}]},
        "plays": (previous or []) + ps}


def book(token="a", **changes):
    return {"event_type": "book", "asset_id": token, "market": "condition", "timestamp": "111",
            "bids": [{"price": ".4", "size": "10"}],
            "asks": [{"price": ".5", "size": "7"}, {"price": ".51", "size": "3"}], **changes}


def delta(token="a", **changes):
    return {"event_type": "price_change", "price_changes": [
        {"asset_id": token, "side": "SELL", "price": ".50", "size": "4", **changes}]}


def rows(store, directory):
    store.flush()
    return sorted([json.loads(line) for path in (store.root / directory).rglob("*.jsonl")
                   for line in path.read_text().splitlines()], key=lambda r: r["event_seq"])


@pytest.fixture
def store(tmp_path):
    output = Store(tmp_path / "session")
    yield output
    output.close()


@pytest.fixture
def rules_file(tmp_path):
    rules = example_rules()
    path = tmp_path / "rules.json"
    path.write_text(json.dumps(rules))
    return path


def test_freeze_rules_new_sessions_and_changed_run_rejection(tmp_path, rules_file):
    root = tmp_path / "data"
    first, meta = open_session(root, "run1", rules_file)
    first.append("nba", "test", {"recv_ms": 1}, "book/test.jsonl")
    first.close()
    second, other = open_session(root, "run1", rules_file)
    try:
        assert first.root != second.root
        assert meta["rules_sha256"] == other["rules_sha256"]
        assert second.summary() == {}
        assert not list(second.root.rglob("*.jsonl"))
    finally:
        second.close()
    rules = json.loads(rules_file.read_text())
    rules["entry"]["delay_ms"] = 2000
    rules_file.write_text(json.dumps(rules))
    with pytest.raises(ValueError, match="new run-id"):
        open_session(root, "run1", rules_file)
    (root / "run1" / "rules.json").write_bytes(rules_file.read_bytes())
    with pytest.raises(ValueError, match="run.json"):
        open_session(root, "run1", rules_file)


def test_analysis_choices_and_incomplete_rules_fail_before_creating_output(tmp_path):
    example = tmp_path / "rules.json"
    example.write_text(json.dumps({**example_rules(), "entries_per_game": "every_fill"}))
    with pytest.raises(ValueError, match="analysis rules"):
        open_session(tmp_path / "data", "run", example)
    assert not (tmp_path / "data").exists()
    with pytest.raises(ValueError, match="analysis rules"):
        validate_rules({**example_rules(), "decision_rule": None})
    rules = example_rules()
    del rules["entry"]
    with pytest.raises(ValueError, match="entry"):
        validate_rules(rules)


def test_cli_check_never_creates_recording_output(tmp_path, rules_file, capsys):
    from pm_nba_dryrun.__main__ import main
    output = tmp_path / "absent"
    main(["--check", "--rules", str(rules_file), "--data-dir", str(output)])
    assert "rules valid" in capsys.readouterr().out
    assert not output.exists()


@pytest.mark.parametrize("run_id", ["../x", "/tmp/x", ".", "..", "a/b"])
def test_run_path_is_confined(tmp_path, rules_file, run_id):
    with pytest.raises(ValueError, match="run-id"):
        open_session(tmp_path / "data", run_id, rules_file)


def test_moneyline_selection_and_us_date():
    g = game()
    assert g["clobTokenIds"] == ["a", "b"]
    assert g["fee_fields"]["feeSchedule"] == {"rate": .05}
    assert espn_date(g) == "20261025"
    assert game_from_event({"slug": SLUG + "-spread", "markets": [market()]}, 1) is None
    assert game_from_event({"id": "e", "slug": SLUG, "markets": [market(), market()]}, 1) is None
    assert game(clobTokenIds='["a","a"]') is None
    assert game(outcomes='["Vikings"]') is None


def test_discovery_weekly_paging_and_dedup():
    class Fake:
        calls = []
        def get(self, base, path, **params):
            self.calls.append((path, params))
            if path == "/sports":
                return [{"sport": "nba", "primaryTagId": 7}], 1
            if params["offset"] == 0:
                return [{"id": str(i), "slug": SLUG, "markets": [market()]} for i in range(100)], 22
            return [], 23
    fake = Fake()
    games = list(discover(fake, KICK))
    assert len(games) == 1 and games[0]["recv_ms"] == 22
    events = [params for path, params in fake.calls if path == "/events"]
    assert [p["offset"] for p in events] == [0, 100] * 3
    assert all(p["tag_id"] == 7 and p["limit"] == 100 for p in events)


def test_mapping_exact_team_names_alias_and_ambiguity():
    assert map_event(game(), [board_event()]) == {"espn_event": "401", "team0_home": False, "mapping_status": "matched"}
    assert map_event(game(), [board_event(), board_event("402")])["mapping_status"] == "ambiguous"
    assert map_event(game(), [])["mapping_status"] == "missing"
    event = board_event()
    event["competitions"][0]["competitors"][1]["team"]["name"] = "Suns"
    assert map_event(game(outcomes='["PHO","Saints"]'), [event])["mapping_status"] == "matched"


def test_game_changes_and_window_both_close_and_final_tail(store):
    registry = Games(store)
    registry.update(SLUG, game(), KICK - 60 * MINUTE)
    registry.update(SLUG, game(), KICK - 59 * MINUTE)
    assert len(rows(store, "games")) == 1
    assert registry.routes(KICK - 30 * MINUTE - 1) == {}
    assert set(registry.routes(KICK - 30 * MINUTE)) == {"a", "b"}
    last_play = KICK + 180 * MINUTE
    registry.espn(SLUG, "post", last_play, last_play)
    assert registry.active(last_play + 31 * MINUTE)  # Close still unknown.
    close = last_play + 40 * MINUTE
    registry.update(SLUG, {"closed": True}, close)
    assert registry.active(close - 1)
    assert registry.active(close) == {}
    assert len(rows(store, "games")) == 2


def test_close_before_final_play_tail_and_silence_is_not_game_end(store):
    registry = Games(store)
    registry.update(SLUG, game(), 1)
    registry.update(SLUG, {"closed": True}, KICK + 100 * MINUTE)
    assert registry.active(KICK + 300 * MINUTE)  # No final ESPN summary.
    registry.espn(SLUG, "post", KICK + 200 * MINUTE, KICK + 200 * MINUTE)
    assert registry.active(KICK + 230 * MINUTE - 1)
    assert not registry.active(KICK + 230 * MINUTE)


def test_espn_new_changed_gone_reappearing_preserve_first_sighting(store):
    log = ESPNLog(store)
    g = mapped()
    assert log.record(g, summary([play()]), 100) == ("in", 100)
    log.record(g, summary([play()]), 200)
    log.record(g, summary([play(homeScore=6, text="TOUCHDOWN")]), 300)
    log.record(g, summary([]), 400)
    assert log.record(g, summary([play(homeScore=7)]), 500) == ("in", None)
    out = rows(store, "espn")
    ps = [r for r in out if "id" in r]
    assert [r["kind"] for r in ps] == ["new", "changed", "gone", "changed"]
    assert [r["first_recv_ms"] for r in ps] == [100] * 4
    assert ps[1]["home"] == 6 and ps[1]["raw_play"]["homeScore"] == 6
    assert [r["recv_ms"] for r in ps] == [100, 300, 400, 500]
    assert len([r for r in out if "raw_header" in r]) == 1


def test_espn_modified_is_a_change_and_missing_plays_is_an_error(store):
    log = ESPNLog(store)
    log.record(mapped(), summary([play()]), 1)
    log.record(mapped(), summary([play(modified="m2")]), 2)
    ps = [r for r in rows(store, "espn") if "id" in r]
    assert [r["kind"] for r in ps] == ["new", "changed"]
    missing = summary([])
    del missing["plays"]
    with pytest.raises(ValueError, match="plays"):
        log.record(mapped(), missing, 3)
    assert len([r for r in rows(store, "espn") if "kind" in r]) == 2


def test_raw_depth_both_tokens_duplicates_batch_and_cross_game_payload(store):
    capture = BookCapture(store)
    routes = {"a": (SLUG, 0, "condition"), "b": (SLUG, 1, "condition"), "other": ("other", 0, "c2")}
    fill = {"event_type": "last_trade_price", "asset_id": "b", "price": ".3", "side": "SELL", "size": "8"}
    cross = delta()
    cross["price_changes"].append({"asset_id": "other", "price": ".2", "size": "3", "side": "BUY"})
    capture.record([book(), book("b"), fill, fill, cross], 100, routes)
    out = rows(store, "book")
    assert len(out) == 6
    assert sum(r["message"] == fill for r in out) == 2
    assert all(r["message"] == cross for r in out[-2:])
    assert {r["frame_index"] for r in out} == set(range(5))
    assert {r["recv_ms"] for r in out} == {100}


def test_ladder_reconstruction_absolute_size_delete_and_reconnect(store):
    capture = BookCapture(store)
    routes = {"a": (SLUG, 0, "condition"), "b": (SLUG, 1, "condition")}
    capture.marker(routes, 0, "connect_or_reconnect")
    capture.record([book(), book("b"), delta()], 1, routes)
    capture.record(delta(size="0"), 2, routes)
    capture.marker(routes, 3, "disconnect")
    capture.marker(routes, 4, "connect_or_reconnect")
    capture.record(delta(), 5, routes)
    capture.record(book("b"), 6, routes)
    replay = Ladders(["a", "b"])
    out = rows(store, "book")
    for row in out:
        replay.apply(row)
        if row["recv_ms"] == 1:
            if row.get("message", {}).get("event_type") == "price_change":
                assert replay.ask("a") == {"price": Decimal(".5"), "shares": Decimal(4), "shares_within_cent": Decimal(7)}
        if row["recv_ms"] == 2:
            assert replay.ask("a")["price"] == Decimal(".51")
        if 3 <= row["recv_ms"] <= 5:
            assert replay.ask("a") is None and replay.ask("b") is None
    assert replay.ask("a") is None
    assert replay.ask("b")["shares_within_cent"] == 10


def test_sports_filter_preserves_nba_wire_messages(store):
    message = {"leagueAbbreviation": "NBA", "gameId": 10, "score": "6-0", "period": "1Q", "elapsed": "12:30"}
    record_sports(store, [{**message, "leagueAbbreviation": "NFL"}, message, message], KICK)
    out = rows(store, "pm_sports")
    assert len(out) == 2 and all(r["message"] == message and r["recv_ms"] == KICK for r in out)


def test_settlement_first_six_after_kickoff_hourly_retry_until_resolved(store):
    class Fake:
        replies = [market(closed=True, outcomePrices='["0.5","0.5"]', umaResolutionStatus="proposed"),
                   market(closed=True, outcomePrices='["0.5","0.5"]', umaResolutionStatus="resolved")]
        def get(self, *args, **kwargs):
            return self.replies.pop(0), KICK
    fake = Fake()
    service = Service(store, http=fake)
    service.games.update(SLUG, game(), 1)
    due = timestamp_ms("2026-10-26T06:00:00Z")
    retries, finished = {}, set()
    service.settlement_pass(due - 1, retries, finished)
    assert len(fake.replies) == 2
    service.settlement_pass(due, retries, finished)
    assert not finished
    service.settlement_pass(due + 60 * MINUTE - 1, retries, finished)
    assert len(fake.replies) == 1
    service.settlement_pass(due + 60 * MINUTE, retries, finished)
    assert finished == {SLUG}
    service.settlement_pass(due + 2 * 60 * MINUTE, retries, finished)
    assert [r["resolved"] for r in rows(store, "settlement")] == [False, True]
    assert not resolved(market(closed=True, outcomePrices='["1","0"]'))


class FastStop(threading.Event):
    def wait(self, timeout=None):
        return self.is_set()


class Socket:
    def __init__(self, messages, sent):
        self.messages, self.sent = iter(messages), sent
    def settimeout(self, timeout):
        pass
    def send(self, message):
        self.sent.append(message)
    def recv(self):
        value = next(self.messages)
        if callable(value):
            value = value()
        if isinstance(value, Exception):
            raise value
        return value
    def close(self):
        pass


def test_socket_reconnect_resubscribes_and_logs_gap_before_snapshot(store):
    sent = []
    service = Service(store, clock=lambda: KICK)
    service.stop = FastStop()
    service.games.update(SLUG, game(), 1)
    def final():
        service.stop.set()
        return json.dumps(book("b"))
    sockets = iter([Socket([json.dumps(book()), ConnectionError("lost")], sent), Socket([final], sent)])
    service.socket_factory = lambda *a, **k: next(sockets)
    service.websocket_worker()
    assert [json.loads(s) for s in sent] == [{"type": "market", "assets_ids": ["a", "b"]}] * 2
    out = rows(store, "book")
    assert [r.get("event", "message") for r in out] == ["connect_or_reconnect", "message", "disconnect",
                                                               "connect_or_reconnect", "message", "disconnect"]


def test_sports_socket_answers_ping_without_subscription(store):
    sent = []
    service = Service(store, clock=lambda: KICK)
    service.stop = FastStop()
    def final():
        service.stop.set()
        return json.dumps({"leagueAbbreviation": "NBA", "gameId": 1})
    service.socket_factory = lambda *a, **k: Socket(["ping", final], sent)
    service.websocket_worker(True)
    assert sent == ["pong"]
    assert len(rows(store, "pm_sports")) == 1


def test_recording_failure_stops_supervised_workers(store):
    service = Service(store)
    def fail():
        raise RecordingError("disk full")
    service.guarded(fail)
    assert service.stop.is_set()
    assert isinstance(service.failures.get_nowait(), RecordingError)


def test_partial_scoreboard_preserves_verified_mapping_and_live_polls(store):
    service = Service(store, clock=lambda: KICK)
    service.games.update(SLUG, game(), 1)
    service.games.update(SLUG, {"espn_event": "401", "espn_date": "20261025", "team0_home": False,
                                "mapping_status": "matched"}, 2)
    service.games.runtime[SLUG]["board_state"] = "in"
    class Fake:
        def get(self, *args, **kwargs):
            service.stop.set()
            return {"events": []}, KICK
    service.http = Fake()
    service.scoreboard_worker()
    assert service.games.all()[SLUG]["espn_event"] == "401"
    assert service.games.runtime[SLUG]["board_state"] == "in"


def test_subscription_changes_add_only_new_game_tokens(store, monkeypatch):
    from pm_nba_dryrun import service as module
    tick = [0]
    monkeypatch.setattr(module.time, "monotonic", lambda: tick[0])
    service = Service(store, clock=lambda: KICK)
    service.stop = FastStop()
    service.games.update(SLUG, game(), 1)
    sent = []
    other = game()
    other.update(slug="nba-a-b-2026-10-25", clobTokenIds=["c", "d"], conditionId="c2")
    def add_game():
        service.games.update(other["slug"], other, KICK)
        tick[0] = 1.1
        return json.dumps(book())
    def final():
        service.stop.set()
        return json.dumps(book("c", market="c2"))
    service.socket_factory = lambda *a, **k: Socket([add_game, final], sent)
    service.websocket_worker()
    assert [json.loads(s) for s in sent] == [
        {"type": "market", "assets_ids": ["a", "b"]}, {"operation": "subscribe", "assets_ids": ["c", "d"]}]
    out = rows(store, "book")
    assert any(r.get("event") == "subscribe" and r["tokens"] == ["c", "d"] for r in out)


def test_ntp_offset_diagnostics_macos_and_unsynchronized_linux(monkeypatch):
    from types import SimpleNamespace
    from pm_nba_dryrun import service as module
    monkeypatch.setattr(module.shutil, "which", lambda name: name if name == "sntp" else None)
    monkeypatch.setattr(module.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        returncode=0, stdout="2026-10-25 21:00:00.000 (+0000) +0.012000 +/- 0.001 time.apple.com\n", stderr=""))
    assert module.ntp_sample()["offset_ms"] == -12
    monkeypatch.setattr(module.shutil, "which", lambda name: name if name == "chronyc" else None)
    monkeypatch.setattr(module.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        returncode=0, stdout="System time : 0.003 seconds fast of NTP time\nLeap status : Not synchronised", stderr=""))
    result = module.ntp_sample()
    assert result["offset_ms"] is None and result["error"]


def test_fatal_worker_error_propagates_from_process_and_closes_store(store):
    service = Service(store)
    def idle():
        service.stop.wait(1)
    def fail():
        raise RecordingError("disk full")
    for name in ("discovery_worker", "scoreboard_worker", "summary_worker", "settlement_worker", "health_worker"):
        setattr(service, name, idle)
    service.discovery_worker = fail
    service.websocket_worker = lambda *args: idle()
    with pytest.raises(RuntimeError, match="worker failure"):
        service.run()
    assert all(not thread.is_alive() for thread in service.threads)
    with pytest.raises(RecordingError, match="closed"):
        store.flush()


def test_backup_copies_flushed_prefixes_without_partial_jsonl(tmp_path, rules_file):
    store, _ = open_session(tmp_path / "data", "run1", rules_file)
    destination = tmp_path / "ssd"
    destination.mkdir()
    try:
        store.append("nba", "test", {"recv_ms": 1}, "book/test.jsonl")
        manifest = backup_run(store, destination, threading.Event())
        for relative, size in manifest["files"].items():
            assert (destination / "run1" / relative).stat().st_size == size
        copied = next((destination / "run1").rglob("test.jsonl"))
        assert json.loads(copied.read_text())["recv_ms"] == 1
        store.append("nba", "test", {"recv_ms": 2}, "book/test.jsonl")
        assert len(copied.read_text().splitlines()) == 1
        backup_run(store, destination, threading.Event())
        assert len(copied.read_text().splitlines()) == 2
        with pytest.raises(ValueError, match="overlap"):
            backup_run(store, store.root, threading.Event())
    finally:
        store.close()


class OnePassStop(threading.Event):
    """End a periodic worker after one pass, including its pending HTTP work."""
    def wait(self, timeout=None):
        self.set()
        return True


@pytest.mark.parametrize("error", [IncompleteRead(b"partial"), BadStatusLine("bad status")])
def test_http_protocol_errors_are_logged_and_next_poll_recovers(store, error):
    class Fake:
        calls = 0
        def get(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise error
            return {"ok": True}, KICK
    service = Service(store, http=Fake(), clock=lambda: KICK)
    assert service.poll("espn_summary", "", "/summary") is None
    assert not service.stop.is_set()
    assert service.poll("espn_summary", "", "/summary") == ({"ok": True}, KICK)
    health = rows(store, "health")
    assert [r["kind"] for r in health] == ["poll_error", "poll"]


def test_discovery_protocol_error_is_recoverable(store):
    class Fake:
        def get(self, *args, **kwargs):
            raise IncompleteRead(b"partial")
    service = Service(store, http=Fake(), clock=lambda: KICK)
    service.stop = OnePassStop()
    service.guarded(service.discovery_worker)
    assert service.failures.empty()
    assert rows(store, "health")[0]["source"] == "gamma_discovery"


def test_poll_keeps_recording_failures_fatal(store):
    class Fake:
        def get(self, *args, **kwargs):
            raise RecordingError("disk full")
    with pytest.raises(RecordingError, match="disk full"):
        Service(store, http=Fake()).poll("espn_summary", "", "/summary")


def test_malformed_scoreboard_events_do_not_block_healthy_mapping(store):
    malformed = board_event("broken")
    del malformed["competitions"][0]["competitors"][0]["homeAway"]
    no_id = board_event()
    del no_id["id"]
    class Fake:
        def get(self, *args, **kwargs):
            return {"events": [malformed, no_id, board_event()]}, KICK
    service = Service(store, http=Fake(), clock=lambda: KICK)
    service.stop = OnePassStop()
    service.games.update(SLUG, game(), 1)
    service.guarded(service.scoreboard_worker)
    assert service.failures.empty()
    assert service.games.all()[SLUG]["espn_event"] == "401"
    errors = [r for r in rows(store, "health") if r["kind"] == "poll_error"]
    assert [r["event_index"] for r in errors] == [0, 1]
    raw = rows(store, "espn")[0]["message"]
    assert raw["events"][0] == malformed


def test_per_game_mapping_error_does_not_stop_other_games(store, monkeypatch):
    from pm_nba_dryrun import service as module
    original = module.map_event
    def mapping(g, events):
        if g["slug"] == SLUG:
            raise KeyError("malformed mapping data")
        return original(g, events)
    monkeypatch.setattr(module, "map_event", mapping)
    class Fake:
        def get(self, *args, **kwargs):
            return {"events": [board_event()]}, KICK
    service = Service(store, http=Fake(), clock=lambda: KICK)
    service.stop = OnePassStop()
    service.games.update(SLUG, game(), 1)
    second = {**game(), "slug": "nba-other-game-2026-10-25"}
    service.games.update(second["slug"], second, 1)
    service.guarded(service.scoreboard_worker)
    assert service.failures.empty()
    assert service.games.all()[second["slug"]]["espn_event"] == "401"
    errors = [r for r in rows(store, "health") if r["kind"] == "poll_error"]
    assert errors[0]["slug"] == SLUG


@pytest.mark.parametrize("board_state", [None, "pre", "in"])
@pytest.mark.parametrize("before_seconds,expected_calls", [(301, 0), (300, 1), (0, 1)])
def test_summary_starts_five_minutes_before_kickoff_independent_of_scoreboard(store, board_state, before_seconds, expected_calls):
    ms = KICK - before_seconds * 1000
    class Fake:
        calls = 0
        def get(self, *args, **kwargs):
            self.calls += 1
            return summary([play()]), ms
    fake = Fake()
    service = Service(store, http=fake, clock=lambda: ms)
    service.stop = OnePassStop()
    service.games.update(SLUG, game(), 1)
    service.games.update(SLUG, {k: v for k, v in mapped().items() if k not in {"recv_ms"}}, 2)
    service.games.runtime[SLUG]["board_state"] = board_state
    service.summary_worker()
    assert fake.calls == expected_calls
    new = [r for r in rows(store, "espn") if r.get("kind") == "new"]
    assert [r["first_recv_ms"] for r in new] == [ms] * expected_calls


@pytest.mark.parametrize("state", ["pending", "missing", "ambiguous", "matched"])
def test_game_timeout_is_exact_and_logged_once(store, state):
    registry = Games(store)
    registry.update(SLUG, game(), 1)
    registry.update(SLUG, {"mapping_status": state}, 2)
    cutoff = KICK + 8 * 60 * MINUTE
    assert registry.active(cutoff - 1)
    assert registry.active(cutoff) == {}
    assert registry.active(cutoff + MINUTE) == {}
    timeout, = rows(store, "health")
    assert timeout["kind"] == "game_timeout"
    assert timeout["reason"] == "kickoff_plus_8h"
    assert timeout["mapping_status"] == state
    assert timeout["cutoff_ms"] == cutoff


def test_expired_unmapped_game_is_not_polled_by_scoreboard_or_summary(store):
    class Fake:
        def get(self, *args, **kwargs):
            pytest.fail("Expired game must not cause an HTTP request")
    service = Service(store, http=Fake(), clock=lambda: KICK + 8 * 60 * MINUTE)
    service.games.update(SLUG, game(), 1)
    service.stop = OnePassStop()
    service.scoreboard_worker()
    service.stop = OnePassStop()
    service.summary_worker()
    assert len(rows(store, "health")) == 1


def test_closed_latches_across_conbaicting_gamma_sources(store):
    registry = Games(store)
    registry.update(SLUG, game(), 1)
    close = KICK + 180 * MINUTE
    registry.update(SLUG, {"closed": True, "closedTime": "first close"}, close)
    registry.update(SLUG, game(ms=close + 1, closed=False), close + 1)
    registry.update(SLUG, {"closed": True, "closedTime": "later close"}, close + 2)
    assert registry.all()[SLUG]["closed"] is True
    assert registry.all()[SLUG]["closedTime"] == "first close"
    assert registry.runtime[SLUG]["close_seen_ms"] == close
    assert all(r["closed"] for r in rows(store, "games")[1:])


def test_nba_summary_only_reports_actual_nba_counts(store):
    store.append("nba", "websocket_connect", {"recv_ms": 1}, "health/test.jsonl")
    store.append("nba", "book_gap", {"recv_ms": 2}, "book/test.jsonl")
    assert store.summary() == {"websocket_connect": 1, "book_gap": 1}


@pytest.mark.parametrize("state", ["pre", "in"])
@pytest.mark.parametrize("missing", [None, []])
def test_opening_summary_without_plays_records_header_without_error(store, state, missing):
    log = ESPNLog(store)
    payload = summary([], state=state)
    payload["plays"] = missing
    assert log.record(mapped(), payload, 1) == (state, None)
    assert len(rows(store, "espn")) == 1
    assert log.record(mapped(), summary([play()]), 2) == ("in", 2)
    new = [r for r in rows(store, "espn") if r.get("kind") == "new"]
    assert new[0]["first_recv_ms"] == 2


def test_missing_plays_after_all_plays_are_removed_is_still_an_error(store):
    log = ESPNLog(store)
    log.record(mapped(), summary([play()]), 1)
    log.record(mapped(), summary([]), 2)
    payload = summary([])
    del payload["plays"]
    with pytest.raises(ValueError, match="plays"):
        log.record(mapped(), payload, 3)


@pytest.mark.parametrize("state,period", [("in", 2), ("post", 4)])
def test_late_or_final_summary_without_plays_is_an_error(store, state, period):
    payload = summary([], state=state)
    del payload["plays"]
    payload["header"]["competitions"][0]["status"]["period"] = period
    with pytest.raises(ValueError, match="plays"):
        ESPNLog(store).record(mapped(), payload, 1)


def test_socket_reads_and_stamps_next_frame_while_writer_is_blocked(store, monkeypatch):
    from pm_nba_dryrun import service as module
    monkeypatch.setattr(module.time, "monotonic", lambda: 0)
    now = [KICK]
    service = Service(store, clock=lambda: now[0])
    service.games.update(SLUG, game(), 1)
    writing, release, received = threading.Event(), threading.Event(), threading.Event()
    original_record = service.book.record
    def slow_record(payload, ms, routes):
        if ms == KICK + 100:
            writing.set()
            if not release.wait(3):
                raise AssertionError("Test did not release writer")
        original_record(payload, ms, routes)
    service.book.record = slow_record
    original_routes = service.games.routes
    route_calls = []
    def routes(ms):
        route_calls.append(ms)
        return original_routes(ms)
    service.games.routes = routes
    def first():
        now[0] = KICK + 100
        return json.dumps(book())
    def second():
        assert writing.wait(2)
        now[0] = KICK + 200
        received.set()
        service.stop.set()
        return json.dumps(delta())
    service.socket_factory = lambda *a, **k: Socket([first, second], [])
    failures = []
    def run():
        try:
            service.websocket_worker()
        except BaseException as exc:
            failures.append(exc)
    reader = threading.Thread(target=run)
    reader.start()
    try:
        assert received.wait(2), "Disk writing blocked socket receipt"
    finally:
        release.set()
        reader.join(3)
    assert not reader.is_alive()
    assert not failures
    out = [r for r in rows(store, "book") if r["kind"] == "message"]
    assert [r["recv_ms"] for r in out] == [KICK + 100, KICK + 200]
    assert len(route_calls) == 1


def test_wire_queue_saturation_is_fatal_without_dropping_accepted_items():
    stop = threading.Event()
    entered, release = threading.Event(), threading.Event()
    consumed = []
    def consume(item):
        if item == 1:
            entered.set()
            assert release.wait(2)
        consumed.append(item)
    with WireWriter(consume, stop, max_items=1) as writer:
        try:
            writer.submit(1)
            assert entered.wait(2)
            writer.submit(2)
            with pytest.raises(RecordingError, match="queue is full"):
                writer.submit(3)
            assert stop.is_set()
        finally:
            release.set()
    assert consumed == [1, 2]


def test_async_recording_failure_propagates_and_stops_reader():
    stop = threading.Event()
    def fail(item):
        raise RecordingError("disk full")
    with pytest.raises(RecordingError, match="writer failed") as failure:
        with WireWriter(fail, stop) as writer:
            writer.submit("frame")
            assert stop.wait(2)
    assert isinstance(failure.value.__cause__, RecordingError)


def test_protocol_pings_keep_socket_alive_without_data(store):
    import websocket
    from types import SimpleNamespace
    from pm_nba_dryrun.service import read_frame
    class Framed:
        frames = [(websocket.ABNF.OPCODE_PING, b""), (websocket.ABNF.OPCODE_TEXT, b'{"a":1}'), (websocket.ABNF.OPCODE_CLOSE, b"")]
        def recv_data_frame(self, control):
            op, data = self.frames.pop(0)
            return op, SimpleNamespace(data=data)
    sock = Framed()
    assert read_frame(sock, websocket) == (None, True)
    assert read_frame(sock, websocket) == (b'{"a":1}', False)
    assert read_frame(sock, websocket) == ("", False)

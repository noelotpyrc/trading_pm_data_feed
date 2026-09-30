"""Outage-only depth capture, without changing the live engine's score input."""
from copy import deepcopy
import json
import threading

import pytest

from pm_soccer_dryrun.book_capture import BookCapture
from pm_soccer_dryrun.fotmob_capture import FotmobCapture, minute_hint
from pm_soccer_dryrun.storage import Store
from tests.test_soccer_dryrun import primed, fill
from tests.test_soccer_book_capture import ROUTES, receive, book, change, rows, depth


ALIASES = {'Home FC': {'id': 10}, 'Away FC': {'id': 20}}


def observation(**changes):
    return {'provider': 'fotmob', 'primary_session': 'primary', 'event_seq': 1,
            'slug': 'home-away', 'fotmob_match_id': 42, 'home_score': 1, 'away_score': 0,
            'status_type': 'inprogress', 'clock_raw': {'short': "35'"},
            'mapping': {'status': 'matched', 'fotmob_match_id': 42,
                        'team_ids': {'home': 10, 'away': 20}, 'kickoff_ms': 0},
            'source': {'source_request_ms': 850, 'source_received_ms': 880, 'cache': {}},
            'vps_received_ms': 900, 'vps_applied_ms': 910,
            'raw_payload_path': 'payloads/example.json.gz', **changes}


@pytest.fixture
def setup(tmp_path):
    store = Store(tmp_path/'primary', session_id='primary')
    engine = primed()
    engine.state['scores'].clear()
    gate = FotmobCapture(tmp_path/'shadow', store, engine, ALIASES)
    gate.session = 'shadow-session'
    gate.ingest(observation(), 950)
    capture = BookCapture(store, 1/60)
    capture.set_routing(ROUTES)
    yield gate, capture, engine
    store.close()


def candidate(engine, **changes):
    row = engine.handle(fill('home', '.56', 1100, 100), 1105)[0]
    return {**row, **changes}


def test_candidate_opens_without_fire_and_does_not_change_engine(setup):
    gate, capture, engine = setup
    original = deepcopy(engine)
    receive(capture, book(), 800)
    row = candidate(engine)
    before = deepcopy(row)
    evidence = gate.decision(row)
    capture.fallback_candidate(row, evidence)
    assert row == before
    assert row == candidate(original)
    assert row['gate'] is None and row['fire'] is None
    assert engine.handle(fill('away', '.48', 1200, 101), 1200) == original.handle(fill('away', '.48', 1200, 101), 1200)
    receive(capture, change(), 1100)  # Same WS frame, later delivery order.
    receive(capture, change(), 2105)  # Expired at precisely emitted time + N.
    assert len(depth(capture)) == 1
    marker = rows(capture, 'book_windows')[0]
    assert marker['capture_reason'] == 'fotmob_fallback_candidate'
    assert marker['passing_fire'] is False and 'fire_recv_ms' not in marker
    assert marker['fotmob_snapshot']['raw_payload_path'] == 'payloads/example.json.gz'
    assert marker['recorded_ms'] == 1105 and marker['candidate_recv_ms'] == 1100
    assert capture.store.summary()['selected_filter_count'] == 0


@pytest.mark.parametrize('changes', [
    {'poller_state': 'ok', 'no_score': False, 'score_stale': False, 'score_age_s': 1},
    {'book_role': 'draw'}, {'d': -1}, {'kind': 'fire'},
])
def test_healthy_sofa_and_wrong_candidates_do_not_open(setup, changes):
    gate, capture, engine = setup
    row = candidate(engine, **changes)
    assert gate.decision(row) is None
    capture.fallback_candidate(row, gate.decision(row))
    assert not capture.ends


@pytest.mark.parametrize('changes', [
    {'poller_state': 'backoff'}, {'poller_state': 'error'}, {'poller_state': 'unmapped'},
    {'poller_state': 'ok', 'no_score': False, 'score_age_s': 31},
    {'poller_state': 'ok', 'no_score': False, 'score_age_s': 1, 'score_stale': True},
])
def test_outage_reasons_allow_recording(setup, changes):
    gate, _, engine = setup
    assert gate.decision(candidate(engine, **changes)) is not None


@pytest.mark.parametrize('changes', [
    {'home_score': 0}, {'home_score': 2}, {'home_score': None}, {'home_score': True},
    {'status_type': 'finished'}, {'status_type': 'notstarted'}, {'status_type': 'postponed'},
    {'clock_raw': {'short': "29'"}},
    {'source': {'source_request_ms': -40_000, 'cache': {}}},
    {'source': {'source_request_ms': 850, 'cache': {'Age': '31'}}},
    {'source': {'source_request_ms': 850, 'cache': {'Age': 'NaN'}}},
    {'source': {'source_request_ms': 1200, 'cache': {}}},
    {'vps_received_ms': 1200}, {'fotmob_match_id': 999},
    {'mapping': {'status': 'missing'}},
])
def test_invalid_fotmob_latest_observation_never_revives_previous_score(setup, changes):
    gate, _, engine = setup
    gate.ingest(observation(event_seq=2, **changes), 1000)
    assert gate.decision(candidate(engine)) is None


@pytest.mark.parametrize('clock,minute', [({'short': "30'"}, 30), ({'short': "45+2'"}, 45),
                                       ({'short': 'HT'}, None), ({}, None)])
def test_minute_hint_or_unknown_retains_evidence(setup, clock, minute):
    gate, _, engine = setup
    gate.ingest(observation(event_seq=2, clock_raw=clock), 1000)
    evidence = gate.decision(candidate(engine))
    assert evidence['minute_hint'] == minute
    assert evidence['minute_unknown'] is (minute is None)


def test_causal_import_no_future_snapshot_and_expiry(setup):
    gate, _, engine = setup
    # Newer score after candidate receipt may not replace the earlier snapshot.
    gate.ingest(observation(event_seq=2, home_score=0, vps_applied_ms=1110), 1120)
    row = candidate(engine)
    assert gate.decision(row)['fotmob_snapshot']['event_seq'] == 1
    assert gate.decision({**row, 'candidate_recv_ms': 1121}) is None
    assert gate.decision({**row, 'candidate_recv_ms': 40_000}) is None
    gate.history.clear()
    gate.ingest(observation(), 1101)  # File existed earlier but imported too late.
    assert gate.decision(row) is None


def test_primary_session_identity_and_orientation(setup):
    gate, _, engine = setup
    gate.history.clear()
    gate.ingest(observation(primary_session='old-primary'), 950)
    assert gate.decision(candidate(engine)) is None
    gate.ingest(observation(home_score=0, away_score=1), 950)
    row = {'kind': 'candidate', 'book_role': 'away', 'd': 1, 'slug': 'home-away',
           'candidate_recv_ms': 1100, 'emitted_ms': 1105, 'poller_state': 'unmapped'}
    assert gate.decision(row) is not None
    engine.state['matches']['home-away']['home'] = 'Different Team'
    assert gate.decision(row) is None


def test_tail_partial_rows_restarts_and_no_source_writes(setup, monkeypatch):
    gate, _, engine = setup
    monkeypatch.setattr('pm_soccer_dryrun.fotmob_capture.now_ms', lambda: 1000)
    directory = gate.root/'sessions'/'s1'/'observations'
    directory.mkdir(parents=True)
    path = directory/'2026-09-30.jsonl'
    encoded = json.dumps(observation())
    path.write_text(encoded)
    gate.refresh()
    assert not gate.history
    path.write_text(encoded+'\n')
    gate.refresh()
    assert gate.decision(candidate(engine))
    assert path.read_text() == encoded+'\n'
    (gate.root/'sessions'/'s2').mkdir()
    gate.refresh()
    assert not gate.history


def test_missing_shadow_does_not_stop_primary(setup):
    gate, capture, _ = setup
    class Stop:
        stopped = False
        def is_set(self): return self.stopped
        def wait(self, seconds): self.stopped = True
    gate.run(Stop())
    assert not gate.history
    assert rows(capture, 'fotmob_capture_health')[0]['state'] == 'FileNotFoundError'
    receive(capture, book(), 1000)  # Primary recording still works.


def test_service_rejects_overlapping_input_output(tmp_path):
    from pm_soccer_dryrun.service import main
    with pytest.raises(SystemExit):
        main(['live', '--data-dir', str(tmp_path), '--fotmob-shadow-dir', str(tmp_path/'shadow')])


def test_live_service_background_reader_opens_window_without_score_injection(setup, monkeypatch):
    from pm_soccer_dryrun import service
    gate, capture, engine = setup
    directory = gate.root/'sessions'/'s1'/'observations'
    directory.mkdir(parents=True)
    (directory/'2026-09-30.jsonl').write_text(json.dumps(observation())+'\n')
    loaded = threading.Event()
    original_ingest = FotmobCapture.ingest
    def ingest(self, row, imported):
        original_ingest(self, row, imported)
        loaded.set()
    monkeypatch.setattr(FotmobCapture, 'ingest', ingest)
    monkeypatch.setattr('pm_soccer_dryrun.fotmob_capture.aliases_from', lambda: ALIASES)
    monkeypatch.setattr('pm_soccer_dryrun.fotmob_capture.now_ms', lambda: 1000)
    monkeypatch.setattr(service, 'now_ms', lambda: 1105)
    monkeypatch.setattr(service, 'Engine', lambda *args, **kwargs: engine)
    monkeypatch.setattr(service, 'BookCapture', lambda *args, **kwargs: capture)
    monkeypatch.setattr(service, 'daily_summary', lambda *args: None)
    def collect(store, stop, target):
        assert loaded.wait(2)
        receive(target, book(), 1000)
        receive(target, fill('home', '.56', 1100, 100)['payload']['message'], 1100)
        receive(target, change(), 1101)
        # Avoid waiting for a wall-clock second in service shutdown.
        engine.state['pending'].clear()
        stop.set()
    monkeypatch.setattr(service, 'collect', collect)
    monkeypatch.setattr(service, 'scores', lambda store, stop, aliases: stop.wait(3))
    service.run_live(capture.store, threading.Event(), 'test', fotmob_shadow_dir=gate.root)
    assert not engine.state['scores']
    assert len(depth(capture)) == 1
    assert rows(capture, 'book_windows')[0]['capture_reason'] == 'fotmob_fallback_candidate'
    assert capture.store.summary().get('fire', 0) == 0

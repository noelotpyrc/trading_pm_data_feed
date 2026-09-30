"""Verified identity matching and per-match failure isolation; no network."""
import csv
import json
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from pm_soccer_dryrun import service, sources
from pm_soccer_dryrun.sources import BackoffError, Resolver, SofaClient, SofaHTTPError, team_whitelist_from
from pm_soccer_dryrun.storage import RecordingError, Store

TEAMS = {"CA Mineiro": {"id": 1977, "name": "Atlético Mineiro"},
         "Chapecoense": {"id": 21845, "name": "Chapecoense"},
         "Mirassol": {"id": 21982, "name": "Mirassol"},
         "Remo": {"id": 2012, "name": "Remo"}}
MATCH = {"slug": "mineiro-chapecoense", "home": "CA Mineiro", "away": "Chapecoense", "kickoff_ms": 10_000_000}


def fixture(event_id=42, home=1977, away=21845, start=10000):
    # Names deliberately do not help: matching must depend on both IDs.
    return {"id": event_id, "homeTeam": {"id": home, "name": "Same name"},
            "awayTeam": {"id": away, "name": "Same name"}, "startTimestamp": start}


def whitelist(path):
    with path.open('w', newline='') as out:
        writer = csv.writer(out)
        writer.writerow(['pm_name', 'query', 'sofascore_team_id'])
        writer.writerows((name, t['name'], t['id']) for name, t in TEAMS.items())
    return path


class Client:
    def __init__(self, responses):
        self.responses, self.calls = responses, []

    def get(self, path, **params):
        self.calls.append(path)
        assert path != '/search/all', 'Global search must never run'
        response = self.responses[path]
        if isinstance(response, Exception):
            raise response
        return response() if callable(response) else response


@pytest.fixture(autouse=True)
def no_request_sleep(monkeypatch):
    monkeypatch.setattr(sources.time, 'sleep', lambda _: None)


def test_bundled_ids_match_audit_and_flagged_names_are_excluded():
    teams = team_whitelist_from()
    audit = list(csv.DictReader(Path(sources.__file__).with_name('soccer_team_aliases_audit.csv').open()))
    assert len(teams) == 865
    assert len(audit) == 890
    for row in audit:
        if row['included_in_aliases'] == 'true':
            assert teams[row['pm_name']] == {'id': int(row['sofascore_team_id']), 'name': row['query']}
        else:
            assert row['pm_name'] not in teams
    assert teams['CA Mineiro']['id'] == 1977


@pytest.mark.parametrize('content', [
    'pm_name,query\nA,Alpha\n',
    'pm_name,query,sofascore_team_id\nA,Alpha,0\n',
    'pm_name,query,sofascore_team_id\nA,Alpha,1.0\n',
    'pm_name,query,sofascore_team_id\nA,Alpha,\n',
    'pm_name,query,sofascore_team_id\nA,,1\n',
    'pm_name,query,sofascore_team_id\nA,Alpha,1\n A ,Other,2\n',
    'pm_name,query,sofascore_team_id\n',
])
def test_invalid_or_legacy_whitelist_fails_closed(tmp_path, content):
    path = tmp_path / 'teams.csv'
    path.write_text(content)
    with pytest.raises(ValueError):
        team_whitelist_from(path)


@pytest.mark.parametrize('side,name', [('home', 'Víkingur'), ('away', 'Unknown club')])
def test_unverified_team_on_either_side_makes_no_requests(side, name):
    client = Client({})
    result = Resolver(client, TEAMS).resolve({**MATCH, side: name})
    assert result['mapping_status'] == 'team_not_whitelisted'
    assert result['unverified_teams'] == [name]
    assert result['sofa_event_id'] is None and client.calls == []


def test_verified_ids_reject_popular_wrong_club_and_similarly_named_opponent():
    client = Client({'/team/1977/events/next/0': {'events': [
        fixture(40, home=1101347), fixture(41, away=999), fixture(42)]},
        '/team/1977/events/last/0': {'events': []}})
    result = Resolver(client, TEAMS).resolve(MATCH)
    assert result['sofa_event_id'] == 42 and not result['sofa_swapped']
    assert result['mapping_team_ids'] == {'home': 1977, 'away': 21845}
    assert client.calls == ['/team/1977/events/next/0', '/team/1977/events/last/0']


def test_next_404_still_uses_last_and_preserves_swapped_orientation():
    path = '/team/1977/events/next/0'
    client = Client({path: SofaHTTPError(404, path),
                     '/team/1977/events/last/0': {'events': [fixture(home=21845, away=1977)]}})
    result = Resolver(client, TEAMS).resolve(MATCH)
    assert result['sofa_event_id'] == 42 and result['sofa_swapped']
    assert result['mapping_lookup_404s'] == [path]


@pytest.mark.parametrize('status', [500, 429])
def test_non_404_errors_are_not_treated_as_empty_fixtures(status):
    path = '/team/1977/events/next/0'
    error = BackoffError(status, 99999999) if status == 429 else SofaHTTPError(status, path)
    client = Client({path: error})
    with pytest.raises(type(error)):
        Resolver(client, TEAMS).resolve(MATCH)
    assert client.calls == [path]


@pytest.mark.parametrize('events,status', [
    ([fixture(start=10000 + 10801)], 'missing'),
    ([fixture(42), fixture(43, start=10001)], 'ambiguous'),
])
def test_kickoff_window_and_multiple_fixture_rejection(events, status):
    client = Client({'/team/1977/events/next/0': {'events': events},
                     '/team/1977/events/last/0': {'events': []}})
    result = Resolver(client, TEAMS).resolve(MATCH)
    assert result['mapping_status'] == status and result['sofa_event_id'] is None


def test_two_aliases_for_same_team_cannot_form_a_fixture():
    teams = {**TEAMS, 'Mineiro duplicate': TEAMS['CA Mineiro']}
    client = Client({})
    assert Resolver(client, teams).resolve({**MATCH, 'away': 'Mineiro duplicate'})['mapping_status'] == 'team_identity_conflict'
    assert client.calls == []


def test_http_error_keeps_status_and_endpoint(monkeypatch):
    client = SofaClient(session_factory=lambda **kw: SimpleNamespace(
        get=lambda *a, **k: SimpleNamespace(status_code=404, headers={})))
    with pytest.raises(SofaHTTPError) as error:
        client.get('/team/1977/events/next/0')
    assert error.value.status == 404 and error.value.path == '/team/1977/events/next/0'
    assert client.until_ms == 0  # 404 is not the shared 403/429 backoff.


class StopRounds:
    """Advance a fake clock only at the end of a worker pass."""
    def __init__(self, clock, rounds=1):
        self.clock, self.rounds, self.done = clock, rounds, False
        self.waits = []

    def is_set(self):
        return self.done

    def set(self):
        self.done = True

    def wait(self, seconds):
        self.waits.append(seconds)
        if seconds == 1:
            self.rounds -= 1
            self.clock[0] += 30000
            if self.rounds == 0:
                self.done = True
        elif seconds in (5, 30):
            self.done = True
        return self.done


@pytest.mark.parametrize('failure', ['404', '500', 'timeout', 'empty'])
def test_failed_mapping_does_not_block_next_match_and_retries_after_minute(tmp_path, monkeypatch, failure):
    clock = [9_000_000]  # Both matches within collection window, before kickoff.
    monkeypatch.setattr(service, 'now_ms', lambda: clock[0])
    path = '/team/1977/events/next/0'
    errors = {'404': SofaHTTPError(404, path), '500': SofaHTTPError(500, path),
              'timeout': TimeoutError('timed out'), 'empty': {'events': []}}
    attempts = [0]
    def recovering():
        attempts[0] += 1
        if attempts[0] == 1:
            if isinstance(errors[failure], Exception):
                raise errors[failure]
            return errors[failure]
        return {'events': [fixture()]}
    last = '/team/1977/events/last/0'
    client = Client({path: recovering, last: SofaHTTPError(404, last) if failure == '404' else {'events': []},
                     '/team/21982/events/next/0': {'events': [fixture(43, home=21982, away=2012)]},
                     '/team/21982/events/last/0': {'events': []}})
    store = Store(tmp_path / 'run')
    store.put('matches', {MATCH['slug']: MATCH,
                          'mirassol-remo': {**MATCH, 'slug': 'mirassol-remo', 'home': 'Mirassol', 'away': 'Remo'}})
    stop = StopRounds(clock, rounds=3)
    service.resolve_matches(store, stop, client, whitelist(tmp_path / 'teams.csv'))
    matches = store.get('matches')
    assert matches[MATCH['slug']]['sofa_event_id'] == 42
    assert matches['mirassol-remo']['sofa_event_id'] == 43
    assert attempts[0] == 2  # No retry at +30 s; retry at +60 s succeeds.
    assert client.calls.index('/team/21982/events/next/0') < len(client.calls) - 1 - client.calls[::-1].index(path)
    assert 30 not in stop.waits  # No whole-loop error sleep.
    if failure in ('500', 'timeout'):
        logs = [json.loads(line) for p in (store.root / 'ops/resolver').glob('*.jsonl') for line in p.read_text().splitlines()]
        assert len(logs) == 1 and logs[0]['slug'] == MATCH['slug'] and logs[0]['phase'] == 'mapping'
        assert matches[MATCH['slug']]['mapping_error'] is None
    store.close()


def test_detail_404_keeps_mapping_and_does_not_block_other_scores(tmp_path, monkeypatch):
    clock = [10_000_001]
    monkeypatch.setattr(service, 'now_ms', lambda: clock[0])
    good = {**fixture(43, home=21982, away=2012), 'status': {'type': 'inprogress'},
            'homeScore': {'current': 1}, 'awayScore': {'current': 0}}
    client = Client({'/event/42': SofaHTTPError(404, '/event/42'), '/event/43': {'event': good}})
    store = Store(tmp_path / 'run')
    store.put('matches', {MATCH['slug']: {**MATCH, 'sofa_event_id': 42, 'mapping_status': 'matched'},
                          'mirassol-remo': {**MATCH, 'slug': 'mirassol-remo', 'home': 'Mirassol', 'away': 'Remo',
                                           'sofa_event_id': 43, 'mapping_status': 'matched'}})
    service.resolve_matches(store, StopRounds(clock, rounds=2), client, whitelist(tmp_path / 'teams.csv'))
    assert client.calls == ['/event/42', '/event/43']  # Failure not retried at +30 s.
    assert store.get('matches')[MATCH['slug']]['sofa_event_id'] == 42
    assert store.get('score:43')['home_score'] == 1
    store.close()


@pytest.mark.parametrize('error', [BackoffError(429, 99999999), RecordingError('disk failed')])
def test_shared_backoff_and_recording_failure_are_not_swallowed(tmp_path, monkeypatch, error):
    clock = [9_000_000]
    monkeypatch.setattr(service, 'now_ms', lambda: clock[0])
    client = Client({'/team/1977/events/next/0': error})
    store = Store(tmp_path / 'run')
    store.put('matches', {MATCH['slug']: MATCH,
                          'other': {**MATCH, 'slug': 'other', 'home': 'Mirassol', 'away': 'Remo'}})
    stop = StopRounds(clock)
    if isinstance(error, RecordingError):
        with pytest.raises(RecordingError):
            service.resolve_matches(store, stop, client, whitelist(tmp_path / 'teams.csv'))
        assert stop.is_set()
    else:
        service.resolve_matches(store, stop, client, whitelist(tmp_path / 'teams.csv'))
        assert 5 in stop.waits
    assert client.calls == ['/team/1977/events/next/0']
    store.close()


def test_run_records_whitelist_snapshot_and_rejects_changed_ids(tmp_path, monkeypatch):
    monkeypatch.setattr(service.signal, 'signal', lambda *_: None)
    config = whitelist(tmp_path / 'teams.csv')
    snapshots = []
    def run(store, stop, run_id, alias_path, book_window_minutes):
        snapshots.append(alias_path)
        assert alias_path == store.root / 'team_whitelist.csv'
        assert team_whitelist_from(alias_path) == TEAMS
    monkeypatch.setattr(service, 'run_live', run)
    args = ['live', '--data-dir', str(tmp_path / 'run'), '--aliases', str(config)]
    service.main(args)
    manifest = json.loads((tmp_path / 'run/run.json').read_text())
    assert len(manifest['team_whitelist_sha256']) == 64
    config.write_text(config.read_text().replace('1977', '1101347'))
    with pytest.raises(ValueError, match='new data directory'):
        service.main(args)
    assert len(snapshots) == 1
    assert team_whitelist_from(snapshots[0])['CA Mineiro']['id'] == 1977

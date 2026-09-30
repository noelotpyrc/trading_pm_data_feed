"""Read-only FotMob adapter and strict provider-specific identity matching."""
from __future__ import annotations

import csv
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

BASE = 'https://www.fotmob.com/api/data'
LEAGUES = {'epl': 47, 'lal': 87, 'sea': 55, 'bun': 54, 'fl1': 53,
           'ucl': 42, 'uel': 73, 'mex': 230, 'bra': 268, 'mls': 130, 'fif': 114}
HOUR = 3_600_000


def utc_ms(value):
    """Never assign a timezone to FotMob's ambiguous display-clock strings."""
    if not isinstance(value, str): return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return int(parsed.timestamp() * 1000) if parsed.tzinfo is not None else None
    except ValueError:
        return None


def aliases_from(path=None):
    path = Path(path) if path else Path(__file__).with_name('fotmob_team_aliases.csv')
    out = {}
    with path.open() as f:
        reader = csv.DictReader(f)
        if not {'pm_name', 'fotmob_team_id', 'fotmob_name', 'evidence_url'}.issubset(reader.fieldnames or []):
            raise ValueError('FotMob aliases require PM name, FotMob ID/name and evidence URL')
        for r in reader:
            name = r['pm_name'].strip()
            if not name or name in out or not r['fotmob_team_id'].isdigit() or int(r['fotmob_team_id']) <= 0:
                raise ValueError(f'Invalid/duplicate FotMob alias at line {reader.line_num}')
            if not r['fotmob_name'] or not r['evidence_url'].startswith('https://www.fotmob.com/'):
                raise ValueError('Missing FotMob identity evidence')
            out[name] = {'id': int(r['fotmob_team_id']), 'name': r['fotmob_name']}
    if not out: raise ValueError('Empty FotMob aliases')
    return out


def alias_digest(aliases):
    return hashlib.sha256(json.dumps(aliases, sort_keys=True).encode()).hexdigest()


class FotmobError(RuntimeError):
    def __init__(self, message, status=None, until_ms=0):
        super().__init__(message)
        self.status, self.until_ms = status, until_ms


class FotmobClient:
    """One paced, independently backed-off HTTP session; no SofaScore state."""
    def __init__(self, session=None, clock=None, sleep=None, min_spacing=.5):
        from curl_cffi import requests, CurlOpt
        self.session = session or requests.Session(curl_options={CurlOpt.IPRESOLVE: 1})
        self.clock = clock or (lambda: time.time_ns() // 1_000_000)
        self.sleep = sleep or time.sleep
        self.until_ms, self.next_ms, self.status = 0, 0, None
        self.spacing_ms = int(min_spacing * 1000)

    def get(self, route, **params):
        if route not in ('matches', 'matchDetails'):
            raise ValueError('Unapproved FotMob route')
        if self.clock() < self.until_ms:
            raise FotmobError('FotMob cooldown', self.status, self.until_ms)
        self.sleep(max(0, self.next_ms-self.clock()) / 1000)
        started = self.clock()
        self.next_ms = started + self.spacing_ms
        try:
            r = self.session.get(BASE+'/'+route, params=params, impersonate='chrome', timeout=8,
                                 headers={'Referer': 'https://www.fotmob.com/', 'Accept': 'application/json'})
        except Exception as exc:
            raise FotmobError(type(exc).__name__) from exc
        received = self.clock()
        if r.status_code in (403, 429):
            from .sources import SofaClient
            self.status = r.status_code
            self.until_ms = received + SofaClient.retry_after_ms(r.headers.get('Retry-After'), received)
            raise FotmobError(f'FotMob HTTP {r.status_code}', r.status_code, self.until_ms)
        if r.status_code != 200:
            raise FotmobError(f'FotMob HTTP {r.status_code}', r.status_code)
        if len(r.content) > 8 * 1024 * 1024:
            raise FotmobError('FotMob response exceeds 8 MiB')
        try:
            payload = r.json()
            if not isinstance(payload, dict): raise ValueError('Expected object')
            if route == 'matches' and not isinstance(payload.get('leagues'), list):
                raise ValueError('Missing leagues')
            if route == 'matchDetails':
                if str(payload.get('general', {}).get('matchId')) != str(params['matchId']):
                    raise ValueError('Match ID mismatch')
                if not isinstance(payload.get('header', {}).get('teams'), list):
                    raise ValueError('Missing teams')
        except (ValueError, TypeError, AttributeError) as exc:
            raise FotmobError(f'Invalid FotMob JSON: {exc}') from exc
        return payload, {'source_request_ms': started, 'source_received_ms': received,
                         'http_status': 200, 'http_elapsed_ms': received-started,
                         'cache': {k: r.headers[k] for k in ('Date', 'Age', 'Cache-Control') if k in r.headers}}


def fixtures(payload):
    out = []
    for league in payload['leagues']:
        for match in league.get('matches', []):
            # The outer league supplies the parent ID for grouped friendlies.
            out.append({'match': match, 'league': {k: league.get(k) for k in
                        ('id', 'primaryId', 'parentLeagueId', 'name', 'ccode')}})
    return out


def resolve(match, candidates, aliases):
    names = (match['home'], match['away'])
    missing = [n for n in names if n not in aliases]
    if missing: return {'status': 'team_not_whitelisted', 'unverified_teams': missing}
    home, away = (aliases[n]['id'] for n in names)
    if home == away: return {'status': 'team_identity_conflict'}
    league_id = LEAGUES.get(match['slug'].split('-')[0])
    if league_id is None: return {'status': 'competition_unverified'}
    found = {}
    for item in candidates:
        m, league = item['match'], item['league']
        if league_id not in (league.get('id'), league.get('primaryId'), league.get('parentLeagueId')):
            continue
        pair = (m.get('home', {}).get('id'), m.get('away', {}).get('id'))
        if pair not in ((home, away), (away, home)): continue
        kickoff = utc_ms(m.get('status', {}).get('utcTime'))
        if kickoff is None or abs(kickoff-match['kickoff_ms']) > 3*HOUR: continue
        if not isinstance(m.get('id'), int): continue
        found[m['id']] = (abs(kickoff-match['kickoff_ms']), kickoff, pair != (home, away), item)
    ordered = sorted(found.items(), key=lambda p: (p[1][0], p[0]))
    if not ordered: return {'status': 'missing'}
    if len(ordered) > 1 and ordered[1][1][0]-ordered[0][1][0] <= 30*60_000:
        return {'status': 'ambiguous', 'candidate_ids': [i for i, _ in ordered]}
    event_id, (_, kickoff, swapped, item) = ordered[0]
    return {'status': 'matched', 'fotmob_match_id': event_id, 'swapped': swapped,
            'team_ids': {'home': home, 'away': away}, 'kickoff_ms': kickoff,
            'kickoff_delta_ms': kickoff-match['kickoff_ms'], 'fixture': item}


def dates_for(matches):
    """Only dates intersecting each tracked kickoff's allowed mapping window."""
    return sorted({datetime.fromtimestamp((m['kickoff_ms']+offset)/1000, timezone.utc).strftime('%Y%m%d')
                   for m in matches for offset in (-3*HOUR, 0, 3*HOUR)})


def normalize(payload, kind, swapped=False):
    if kind == 'match_list':
        m = payload['match']; teams = [m.get('home', {}), m.get('away', {})]
        status = m.get('status', {}); match_id = m.get('id'); pending = None
    else:
        teams = payload.get('header', {}).get('teams', [])
        status = payload.get('header', {}).get('status', {})
        match_id = payload.get('general', {}).get('matchId'); pending = payload.get('hasPendingVAR')
    if len(teams) != 2: raise ValueError('Expected two FotMob teams')
    scores = [t.get('score') for t in teams]
    scores = [v if isinstance(v, int) and not isinstance(v, bool) and v >= 0 else None for v in scores]
    if swapped: scores.reverse()
    live = status.get('liveTime') or {}
    # Keep provider clocks verbatim. In particular, do not turn display times
    # without timezone into UTC or manufacture a SofaScore minute/period start.
    state = ('cancelled' if status.get('cancelled') else 'finished' if status.get('finished')
             else 'inprogress' if status.get('started') else 'notstarted')
    if 'postpon' in str(status.get('reason', '')).lower(): state = 'postponed'
    return {'provider': 'fotmob', 'fotmob_match_id': int(match_id),
            'home_score': scores[0], 'away_score': scores[1], 'status_type': state,
            'status_raw': status, 'clock_raw': live, 'has_pending_var': pending,
            'swapped': swapped, 'clock_eligible_for_engine': False}

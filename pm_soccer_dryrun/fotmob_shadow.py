"""Independent FotMob shadow receiver/worker. Never publishes into the engine.

Receiver reads primary JSONL files; worker receives a compact scope on its own
loopback SSH forward. All output belongs to a separate shadow session.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import math
import os
import signal
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .fotmob import (FotmobClient, FotmobError, aliases_from, alias_digest,
                     fixtures, resolve, dates_for, normalize, utc_ms)
from .lifecycle import collection_end
from .relay import Wire, MAX_FRAME
from .storage import Store, RecordingError, day, dumps, now_ms, process_lock

VERSION = 1
DISCOVERY_RETRY_MS = 60_000
LOG = logging.getLogger(__name__)


class PrimaryCatalog:
    """Incremental, read-only tail; never advances past an incomplete JSONL row."""
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.positions, self.matches, self.scores = {}, {}, {}
        self.session = None

    def refresh(self, at_ms):
        sessions = sorted(p for p in (self.root/'sessions').glob('*') if p.is_dir())
        if not sessions: return []
        current = sessions[-1]
        if self.session != current.name:
            self.session = current.name
            self.positions.clear(); self.matches.clear(); self.scores.clear()
        for family, target, key in [('matches', self.matches, 'slug'), ('score', self.scores, 'id')]:
            for p in sorted((current/family).glob('*/*.jsonl')):
                stat = p.stat()
                inode, offset = self.positions.get(p, (stat.st_ino, 0))
                if inode != stat.st_ino or stat.st_size < offset: offset = 0
                with p.open('rb') as f:
                    f.seek(offset)
                    while True:
                        line = f.readline()
                        if not line or not line.endswith(b'\n'): break
                        r = json.loads(line)
                        k = r.get(key)
                        if k is not None and r.get('event_seq', 0) > target.get(k, {}).get('event_seq', -1):
                            # Retain compact state, never global raw score payloads.
                            fields = ('slug', 'home', 'away', 'kickoff_ms', 'sofa_event_id', 'sofa_swapped',
                                      'full_time_ms', 'all_closed_ms', 'score_observed_ms', 'collection_expired_ms',
                                      'books', 'event_seq') if family == 'matches' else (
                                      'id', 'home_score', 'away_score', 'status_type', 'poll_ms', 'stale', 'event_seq')
                            target[k] = {n: r[n] for n in fields if n in r}
                        offset = f.tell()
                self.positions[p] = (stat.st_ino, offset)
        # A live process normally writes poll health every five seconds, even
        # while SofaScore is unavailable. Do not keep collecting obsolete scope
        # indefinitely if the primary recorder itself stops.
        activity = [p.stat().st_mtime*1000 for family in ('matches', 'polls')
                    for p in (current/family).rglob('*.jsonl')]
        if not activity or at_ms-max(activity)>30_000: return []
        out = []
        for m in self.matches.values():
            end, _ = collection_end(m)
            if at_ms < m['kickoff_ms'] - 1_800_000 or (end is not None and at_ms >= end): continue
            out.append({k: m[k] for k in ('slug', 'home', 'away', 'kickoff_ms')})
        return sorted(out, key=lambda m: m['slug'])

    def score_for(self, slug):
        m = self.matches.get(slug, {})
        r = self.scores.get(m.get('sofa_event_id'))
        if not r: return None
        score = [r.get('home_score'), r.get('away_score')]
        if m.get('sofa_swapped'): score.reverse()
        return {'provider': 'sofascore', 'score': score, 'poll_ms': r.get('poll_ms'),
                'status_type': r.get('status_type'), 'stale': r.get('stale', False)}


def detail_snapshot(payload):
    """Keep complete live event/VAR evidence; omit offline-retrievable stats."""
    raw = {k: payload[k] for k in ('general', 'header', 'ongoing', 'hasPendingVAR') if k in payload}
    content = payload.get('content') or {}
    raw['content'] = {k: content[k] for k in ('matchFacts', 'liveticker') if k in content}
    raw['omitted_content_keys'] = sorted(set(content)-set(raw['content']))
    return raw


def detail_fixture(payload):
    """Build identity evidence from the current detail response, not an old list."""
    general, header = payload['general'], payload['header']
    teams, status_raw = header['teams'], header['status']
    if not isinstance(general, dict) or not isinstance(status_raw, dict) or len(teams) != 2:
        raise ValueError('Expected two FotMob teams')
    kickoff = utc_ms(general.get('matchTimeUTCDate'))
    if kickoff is None or utc_ms(status_raw.get('utcTime')) != kickoff:
        raise ValueError('Missing/conflicting FotMob kickoff')
    for role, team in zip(('homeTeam', 'awayTeam'), teams):
        general_team = general.get(role)
        if not isinstance(team, dict) or not isinstance(general_team, dict) or general_team.get('id') != team.get('id'):
            raise ValueError('Conflicting FotMob team identity')
    return {'match': {'id': int(general['matchId']), 'home': teams[0], 'away': teams[1],
                      'status': status_raw},
            'league': {'id': general.get('leagueId'),
                       'parentLeagueId': general.get('parentLeagueId')}}


def verified_detail(match, mapping, payload, aliases):
    verified = resolve(match, [detail_fixture(payload)], aliases)
    if verified.get('status') != 'matched' or any(verified.get(k) != mapping.get(k)
            for k in ('fotmob_match_id', 'swapped', 'team_ids')):
        raise ValueError('FotMob detail identity mismatch')
    return verified


class ShadowSink:
    def __init__(self, store):
        self.store = store
        self.mappings = {}
        self._loaded_seeds = None

    def mapping_seeds(self, scope, aliases):
        """Reuse only IDs/evidence from recordings; never replay their old scores."""
        if scope and self._loaded_seeds is None:
            latest = {}
            # Read once, outside the primary process. Old observations are only
            # identity hints; a fresh detail request is still mandatory.
            for session in sorted(self.store.root.parent.glob('*')):
                for path in sorted((session/'observations').glob('*.jsonl')):
                    with path.open() as f:
                        for line in f:
                            if not line.endswith('\n'): continue
                            try:
                                row = json.loads(line)
                                if row.get('mapping', {}).get('status') == 'matched':
                                    latest[row['slug']] = (session, row)
                            except (ValueError, KeyError, TypeError):
                                continue
            self._loaded_seeds = latest
        allowed = {m['slug'] for m in scope}
        self.mappings = {k:v for k,v in self.mappings.items() if k in allowed}
        out = {}
        for match in scope:
            slug = match['slug']
            mapping = self.mappings.get(slug)
            if mapping is None and slug in (self._loaded_seeds or {}):
                session, row = self._loaded_seeds[slug]
                try:
                    identity = row['mapping']
                    if identity['kickoff_ms']-identity['kickoff_delta_ms'] != match['kickoff_ms']:
                        continue
                    relative = row['raw_payload_path']
                    payload_path = (session/relative).resolve()
                    if not payload_path.is_relative_to(session.resolve()):
                        raise ValueError('FotMob payload path escapes session')
                    raw = gzip.decompress(payload_path.read_bytes())
                    if hashlib.sha256(raw).hexdigest() != row['payload_sha256']:
                        raise ValueError('FotMob payload digest mismatch')
                    payload = json.loads(raw)
                    fixture = payload if row['payload_kind'] == 'match_list' else detail_fixture(payload)
                    mapping = resolve(match, [fixture], aliases)
                except (OSError, EOFError, ValueError, KeyError, TypeError):
                    mapping = None
            if mapping and mapping.get('status') == 'matched':
                if mapping.get('kickoff_ms', 0)-mapping.get('kickoff_delta_ms', 0) != match['kickoff_ms']:
                    continue
                verified = resolve(match, [mapping['fixture']], aliases)
                if verified.get('status') == 'matched':
                    out[slug] = verified
                    self.mappings[slug] = verified
        return out

    def record(self, message, received_ms, scope, catalog):
        slug = message.get('slug')
        scoped = {m['slug']: m for m in scope}
        if slug not in scoped: raise ValueError('Out-of-scope FotMob match')
        mapping = message['mapping']
        event_id = mapping.get('fotmob_match_id')
        applied = now_ms()
        if message['kind'] == 'mapping':
            if mapping.get('status') == 'identity_mismatch':
                self.mappings.pop(slug, None)
                if self._loaded_seeds is not None:
                    self._loaded_seeds.pop(slug, None)
            self.store.append('fotmob', 'mapping', {'slug': slug, 'provider': 'fotmob',
                'mapping': mapping, 'received_ms': received_ms, 'applied_ms': applied},
                f'mappings/{day(applied)}.jsonl')
            return
        if mapping.get('status') != 'matched' or not isinstance(event_id, int):
            raise ValueError('Observation requires a verified mapping')
        payload, payload_kind = message['payload'], message['payload_kind']
        if payload_kind not in ('match_list', 'match_details'): raise ValueError('Unknown FotMob payload kind')
        score = normalize(payload, payload_kind, mapping['swapped'])
        if score['fotmob_match_id'] != event_id: raise ValueError('Mapped match ID mismatch')
        if payload_kind == 'match_list':
            teams = [payload['match']['home'], payload['match']['away']]
        else:
            teams = payload['header']['teams']
        expected = [mapping['team_ids']['home'], mapping['team_ids']['away']]
        if mapping['swapped']: expected.reverse()
        if [t.get('id') for t in teams] != expected: raise ValueError('Mapped team IDs mismatch')
        raw = dumps(payload).encode()
        digest = hashlib.sha256(raw).hexdigest()
        relative = f'payloads/{digest[:2]}/{digest}.json.gz'
        dest = self.store.root/relative
        if not dest.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix('.tmp')
            try:
                with tmp.open('wb') as f:
                    f.write(gzip.compress(raw, mtime=0)); f.flush(); os.fsync(f.fileno())
                os.replace(tmp, dest)
            except OSError as exc:
                raise RecordingError('Cannot retain FotMob payload') from exc
        source = message['timing']
        row = {**score, 'slug': slug, 'payload_kind': payload_kind, 'payload_sha256': digest,
               'raw_payload_path': relative, 'source': source, 'mapping': {k:v for k,v in mapping.items() if k!='fixture'},
               'worker_sent_ms': message['sent_ms'], 'vps_received_ms': received_ms,
               'vps_applied_ms': applied, 'clock_corrected': False,
               'transport_age_ms_unadjusted': received_ms-message['sent_ms'],
               'worker_session': message['session'], 'worker_seq': message['seq'],
               'primary_session': catalog.session}
        self.store.append('fotmob', 'shadow_observation', row, f'observations/{day(applied)}.jsonl')
        self.mappings[slug] = mapping
        primary = catalog.score_for(slug)
        if primary:
            comparable = not primary['stale'] and None not in primary['score'] + [score['home_score'], score['away_score']]
            self.store.append('fotmob', 'comparison', {'slug': slug, 'observed_ms': applied,
                'sofascore': primary, 'fotmob': {k: score[k] for k in ('home_score','away_score','status_type')},
                'fotmob_source': source, 'score_agrees': primary['score']==[score['home_score'],score['away_score']] if comparable else None,
                'primary_snapshot_age_ms': applied-primary['poll_ms'] if primary.get('poll_ms') else None,
                'note': 'Comparison only; no engine input or correction'}, f'comparisons/{day(applied)}.jsonl')


def status(store, event, **fields):
    ms = now_ms()
    store.append('fotmob', 'health', {'observed_ms': ms, 'event': event, **fields}, f'health/{day(ms)}.jsonl')
    store.flush()


def send_shadow(wire, kind, **fields):
    if len(dumps(fields).encode()) > MAX_FRAME-1024:
        wire.send('health', report={'state':'oversize','slug':fields.get('slug'), 'kind':kind})
        return False
    wire.send(kind, **fields)
    return True


def receive_connection(conn, catalog, sink, aliases, stop):
    wire = Wire(conn)
    last_in = time.monotonic()
    try:
        hello, _ = wire.receive()
        if hello.get('kind')!='fotmob_hello' or hello.get('version')!=VERSION or hello.get('aliases')!=alias_digest(aliases):
            raise ValueError('FotMob protocol/alias mismatch')
        scope, last_scope = [], 0
        while not stop.is_set():
            if time.monotonic()-last_scope >= 5:
                scope = catalog.refresh(now_ms())
                wire.send('scope', matches=scope, primary_session=catalog.session,
                          mapping_seeds=sink.mapping_seeds(scope, aliases))
                last_scope = time.monotonic()
            try: message, received = wire.receive()
            except socket.timeout:
                if time.monotonic()-last_in > 20: raise TimeoutError('FotMob worker heartbeat missing')
                continue
            last_in = time.monotonic()
            kind = message.get('kind')
            if kind in ('observation','mapping'):
                # Recheck scope at arrival; a collection may have just ended.
                scope = catalog.refresh(received)
                if message.get('slug') not in {m['slug'] for m in scope}:
                    status(sink.store, 'out_of_scope_discard', slug=message.get('slug'))
                    continue
                if kind == 'observation':
                    m = next(m for m in scope if m['slug']==message['slug'])
                    verified = resolve(m, [message['mapping']['fixture']], aliases)
                    if verified.get('status')!='matched' or any(verified.get(k)!=message['mapping'].get(k)
                         for k in ('fotmob_match_id','swapped','team_ids')):
                        raise ValueError('Receiver rejected worker mapping')
                    if message['payload_kind'] == 'match_details':
                        verified_detail(m, verified, message['payload'], aliases)
                sink.record(message, received, scope, catalog)
            elif kind == 'health': status(sink.store, 'worker', report=message.get('report'))
            elif kind != 'heartbeat': raise ValueError('Unexpected FotMob message')
            sink.store.flush()
    finally:
        wire.close()


class CollectionState:
    """Verified IDs and retry deadlines in memory, scoped to primary identity."""
    def __init__(self):
        self.primary_session = None
        self.entries = {}

    def prepare(self, matches, aliases, primary_session, seeds):
        if primary_session != self.primary_session:
            self.entries.clear()
            self.primary_session = primary_session
        allowed = {m['slug'] for m in matches}
        self.entries = {k:v for k,v in self.entries.items() if k in allowed}
        for match in matches:
            slug = match['slug']
            identity = tuple(match[k] for k in ('home', 'away', 'kickoff_ms'))
            if self.entries.get(slug, {}).get('identity') == identity:
                continue
            entry = {'identity': identity, 'mapping': None, 'lookup_after_ms': 0,
                     'detail_after_ms': 0, 'last_mapping': None}
            seed = seeds.get(slug)
            if seed:
                verified = resolve(match, [seed['fixture']], aliases)
                if verified.get('status') == 'matched':
                    entry['mapping'] = verified
            self.entries[slug] = entry


def collect_cycle(client, matches, aliases, send, stop, state=None,
                  primary_session=None, seeds=None):
    state = state if state is not None else CollectionState()
    state.prepare(matches, aliases, primary_session, seeds or {})
    if not matches:
        send('health', report={'state':'idle', 'tracked_matches':0})
        return
    due, candidates, observations = [], [], {}
    discovery_attempts, detail_attempts = 0, 0
    discovery_error = None
    for match in matches:
        entry = state.entries[match['slug']]
        if entry['mapping'] is not None or now_ms() < entry['lookup_after_ms']:
            continue
        eligibility = resolve(match, [], aliases)
        if eligibility['status'] != 'missing':
            # Unverified team/competition cannot be fixed by another HTTP poll.
            if entry['last_mapping'] != eligibility:
                send('mapping', slug=match['slug'], mapping=eligibility)
                entry['last_mapping'] = eligibility
            entry['lookup_after_ms'] = float('inf')
        else:
            entry['lookup_after_ms'] = now_ms() + DISCOVERY_RETRY_MS
            due.append(match)
    try:
        # Request each needed date once, for unresolved matches only. A fully
        # mapped batch performs no fixture-list requests at all.
        for date in dates_for(due):
            if stop.is_set(): return
            discovery_attempts += 1
            p, timing = client.get('matches', date=date, timezone='UTC', ccode3='USA')
            for item in fixtures(p):
                candidates.append(item)
                observations[item['match']['id']] = (item, timing)
    except FotmobError as exc:
        discovery_error = exc
        send('health', report={'state':'discovery_error', 'error':str(exc), 'status':exc.status,
                              'backoff_until_ms':exc.until_ms})
    for match in due:
        entry = state.entries[match['slug']]
        if discovery_error:
            entry['lookup_after_ms'] = max(entry['lookup_after_ms'], discovery_error.until_ms)
            continue
        mapping = resolve(match, candidates, aliases)
        send('mapping', slug=match['slug'], mapping={k:v for k,v in mapping.items() if k!='fixture'})
        entry['last_mapping'] = mapping
        if mapping['status'] == 'matched':
            entry['mapping'] = mapping
            item, timing = observations[mapping['fotmob_match_id']]
            send('observation', slug=match['slug'], mapping=mapping, payload_kind='match_list', payload=item, timing=timing)
    matched, detailed, detail_errors = 0, 0, 0
    for match in matches:
        if stop.is_set(): return
        entry = state.entries[match['slug']]
        mapping = entry['mapping']
        if mapping is None: continue
        matched += 1
        if now_ms() < entry['detail_after_ms']:
            detail_errors += 1
            continue
        try:
            detail_attempts += 1
            p, timing = client.get('matchDetails', matchId=str(mapping['fotmob_match_id']))
            try:
                mapping = verified_detail(match, mapping, p, aliases)
            except (ValueError, KeyError, TypeError) as exc:
                entry.update(mapping=None, lookup_after_ms=now_ms()+DISCOVERY_RETRY_MS)
                matched -= 1
                send('mapping', slug=match['slug'], mapping={'status':'identity_mismatch',
                     'fotmob_match_id':mapping['fotmob_match_id'], 'error':str(exc)})
                raise FotmobError(f'Invalid FotMob detail identity: {exc}') from exc
            entry['mapping'] = mapping
            retained = send('observation', slug=match['slug'], mapping=mapping, payload_kind='match_details',
                            payload=detail_snapshot(p), timing=timing)
            if retained is False: detail_errors += 1
            else: detailed += 1
        except FotmobError as exc:
            detail_errors += 1
            entry['detail_after_ms'] = max(exc.until_ms, now_ms()+DISCOVERY_RETRY_MS if exc.status==404 else 0)
            send('health', report={'state':'detail_error','slug':match['slug'],'error':str(exc),
                                  'status':exc.status,'backoff_until_ms':exc.until_ms})
            # Continue other matches after a per-match failure. The client
            # itself enforces provider-wide cooldowns without extra HTTP calls.
    cycle_state = 'detail_error' if detail_errors else 'mapping_gap' if matched < len(matches) else 'ok'
    send('health', report={'state':cycle_state,'tracked_matches':len(matches),
                          'mapped_matches':matched,'detail_observations':detailed,
                          'discovery_matches':len(due), 'discovery_attempts':discovery_attempts,
                          'detail_attempts':detail_attempts})


def work_connection(sock, client, aliases, stop, interval=10, state=None):
    state = state if state is not None else CollectionState()
    wire = Wire(sock); ended = threading.Event(); lock = threading.Lock()
    scope, last_scope, configuration = [], [0.], {'primary_session':None, 'mapping_seeds':{}}
    wire.send('fotmob_hello', version=VERSION, aliases=alias_digest(aliases))
    def send(kind, **fields):
        if stop.is_set() or ended.is_set(): raise ConnectionError('Shadow connection closed')
        # Wire enforces a hard frame limit. Log a missing observation instead
        # of dropping content silently or poisoning the unrelated live pipeline.
        return send_shadow(wire, kind, **fields)
    def collect():
        while not stop.is_set() and not ended.is_set():
            started = time.monotonic()
            with lock:
                current = list(scope)
                config = dict(configuration)
                fresh = last_scope[0] and started-last_scope[0] < 20
            if not fresh:
                ended.wait(.5)
                continue
            try:
                collect_cycle(client, current, aliases, send, ended, state,
                              config['primary_session'], config['mapping_seeds'])
            except FotmobError as exc:
                try:
                    send('health', report={'state':'error','error':str(exc),'status':exc.status,'backoff_until_ms':exc.until_ms})
                except ConnectionError: ended.set()
            except Exception as exc:
                LOG.exception('Shadow collection failed')
                try: send('health', report={'state':'error','error':str(exc)[:300]})
                except Exception: ended.set()
            ended.wait(max(.1, interval-(time.monotonic()-started)))
    thread=threading.Thread(target=collect, name='fotmob-http'); thread.start()
    last_in=time.monotonic(); last_beat=0
    try:
        while not stop.is_set() and not ended.is_set():
            if time.monotonic()-last_beat >= 2:
                send('heartbeat'); last_beat=time.monotonic()
            try: msg, _ = wire.receive()
            except socket.timeout:
                if time.monotonic()-last_in > 20: raise TimeoutError('FotMob receiver scope missing')
                continue
            last_in=time.monotonic()
            if msg.get('kind')!='scope': raise ValueError('Unexpected FotMob receiver message')
            with lock:
                scope[:] = msg['matches']
                last_scope[0] = time.monotonic()
                configuration.update(primary_session=msg.get('primary_session'),
                                     mapping_seeds=msg.get('mapping_seeds', {}))
    finally:
        ended.set(); thread.join(timeout=12); wire.close()
        if thread.is_alive(): raise RuntimeError('FotMob HTTP thread failed to stop')


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest='mode', required=True)
    r=sub.add_parser('receiver'); r.add_argument('--primary-dir',type=Path,required=True)
    r.add_argument('--data-dir',type=Path,required=True)
    w=sub.add_parser('worker'); w.add_argument('--interval',type=float,default=10)
    for mode in (r,w):
        mode.add_argument('--port',type=int,default=18766); mode.add_argument('--aliases',type=Path)
        mode.add_argument('--duration',type=float,help='Optional bounded test duration in seconds')
    args=p.parse_args(argv)
    if not 1024<=args.port<=65535 or args.port==18765: p.error('Use a separate unprivileged shadow port, not 18765')
    if args.mode=='worker' and (not math.isfinite(args.interval) or args.interval<10): p.error('FotMob interval must be finite and at least ten seconds')
    if args.duration is not None and (not math.isfinite(args.duration) or args.duration<=0): p.error('Duration must be finite and positive')
    aliases=aliases_from(args.aliases); stop=threading.Event()
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    for sig in (signal.SIGTERM,signal.SIGINT): signal.signal(sig,lambda *_:stop.set())
    timer=None
    if args.duration:
        timer=threading.Timer(args.duration,stop.set); timer.daemon=True; timer.start()
    if args.mode=='worker':
        client=FotmobClient()
        state=CollectionState()
        while not stop.is_set():
            try:
                with socket.create_connection(('127.0.0.1',args.port),timeout=5) as sock:
                    work_connection(sock,client,aliases,stop,args.interval,state)
            except Exception:
                if not stop.is_set(): LOG.exception('Shadow connection failed; retry in five seconds')
            stop.wait(5)
        return
    primary,out=args.primary_dir.resolve(),args.data_dir.resolve()
    if out==primary or out.is_relative_to(primary) or primary.is_relative_to(out):
        p.error('Shadow output and primary recordings must be disjoint directories')
    if not (primary/'run.json').is_file(): p.error('Primary directory must contain run.json')
    with process_lock(out,'fotmob-shadow'):
        session=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:12]
        store=Store(out/'sessions'/session,session_id=session)
        (store.root/'aliases.json').write_text(dumps(aliases)+'\n')
        alias_path=args.aliases or Path(__file__).with_name('fotmob_team_aliases.csv')
        (store.root/'aliases.csv').write_bytes(alias_path.read_bytes())
        code_hash=hashlib.sha256()
        for name in ('fotmob.py','fotmob_shadow.py','relay.py','storage.py','lifecycle.py','sources.py'):
            code_hash.update(Path(__file__).with_name(name).read_bytes())
        (store.root/'session.json').write_text(dumps({'provider':'fotmob','mode':'shadow','version':VERSION,
            'primary_dir':str(primary),'aliases_sha256':alias_digest(aliases),'started_ms':now_ms(),
            'code_sha256':code_hash.hexdigest(),'receiver_host':socket.gethostname(),
            'detail_retention':'general/header/ongoing/hasPendingVAR + full matchFacts/liveticker'})+'\n')
        catalog=PrimaryCatalog(primary); sink=ShadowSink(store)
        try:
            with socket.socket() as listener:
                listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                listener.bind(('127.0.0.1',args.port)); listener.listen(1); listener.settimeout(1)
                status(store,'listening',port=args.port)
                while not stop.is_set():
                    try: conn,_=listener.accept()
                    except socket.timeout: continue
                    try: receive_connection(conn,catalog,sink,aliases,stop)
                    except RecordingError: raise
                    except Exception as exc:
                        status(store,'disconnected',error=str(exc)[:300])
        finally: store.close()


if __name__=='__main__': main()

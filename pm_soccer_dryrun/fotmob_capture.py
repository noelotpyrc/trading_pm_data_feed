"""Optional recording-only FotMob gate; never supplies scores to the engine."""
from collections import deque
import json
import logging
from pathlib import Path
import re

from .fotmob import aliases_from
from .storage import day, now_ms

LOG = logging.getLogger(__name__)
POLICY = 'fotmob-fallback-candidate-1'
MAX_AGE_MS = 30_000


def minute_hint(clock):
    """Read an explicit regulation display minute; never extrapolate a clock.

    Added time remains relative to its period (45+2 is not second-half 47).
    Unknown text, HT and ambiguous timestamps remain unknown.
    """
    short = clock.get('short') if isinstance(clock, dict) else None
    if not isinstance(short, str):
        return None
    match = re.fullmatch(r"(\d{1,3})(?:\+\d{1,2})?['′]?", short.strip())
    return int(match[1]) if match and int(match[1]) <= 120 else None


def sofa_failure(candidate):
    if candidate.get('poller_state') != 'ok':
        return 'sofascore_' + str(candidate.get('poller_state', 'unknown'))
    if candidate.get('no_score') or candidate.get('score_stale'):
        return 'sofascore_missing_or_stale_score'
    age = candidate.get('score_age_s')
    if age is None or not 0 <= age <= MAX_AGE_MS / 1000:
        return 'sofascore_observation_expired'
    return None


class FotmobCapture:
    """Background JSONL tail with a small cache guarded by the primary Store lock.

    The capture path reads memory only. An observation must have been imported
    before candidate receipt, not just before delayed candidate processing.
    """
    def __init__(self, root, store, engine, aliases=None):
        self.root, self.store, self.engine = Path(root).resolve(), store, engine
        self.aliases = aliases if aliases is not None else aliases_from()
        self.session = None
        self.positions, self.history = {}, {}
        self.health = None

    def ingest(self, row, imported_ms):
        if not isinstance(row, dict):
            raise ValueError('FotMob observation must be an object')
        if row.get('primary_session') != self.store.session_id or row.get('provider') != 'fotmob':
            return
        slug = row.get('slug')
        if slug not in self.engine.state['matches']:
            return
        applied = row.get('vps_applied_ms')
        if not isinstance(applied, int) or not 0 <= imported_ms-applied <= MAX_AGE_MS:
            return
        # Retain latest invalid/finished rows too: never revive an older valid
        # score after a correction or FT. Bound histories even under bad input.
        history = self.history.setdefault(slug, deque(maxlen=64))
        if history and row.get('event_seq', 0) <= history[-1]['event_seq']:
            return
        history.append({**row, 'capture_imported_ms': imported_ms})

    def refresh(self):
        sessions = sorted(p for p in (self.root/'sessions').glob('*') if p.is_dir())
        if not sessions:
            raise FileNotFoundError('FotMob shadow session not available')
        current = sessions[-1]
        with self.store.lock:
            if self.session != current.name:
                self.session = current.name
                self.positions.clear()
                self.history.clear()
        for path in sorted((current/'observations').glob('*.jsonl')):
            stat = path.stat()
            inode, offset = self.positions.get(path, (stat.st_ino, 0))
            if inode != stat.st_ino or stat.st_size < offset:
                offset = 0
                with self.store.lock:
                    self.history.clear()
            batch = []
            with path.open('rb') as f:
                f.seek(offset)
                # Bound each iteration; catch up gradually without blocking fills.
                for _ in range(256):
                    line = f.readline(1_048_577)
                    if len(line) > 1_048_576:
                        raise ValueError('Oversized FotMob observation row')
                    if not line or not line.endswith(b'\n'):
                        break
                    batch.append(json.loads(line))
                    offset = f.tell()
            with self.store.lock:
                imported = now_ms()
                for row in batch:
                    self.ingest(row, imported)
            self.positions[path] = (stat.st_ino, offset)
        with self.store.lock:
            cutoff = now_ms()-2*MAX_AGE_MS
            for slug in list(self.history):
                history = self.history[slug]
                while history and history[0]['capture_imported_ms'] < cutoff:
                    history.popleft()
                if not history or slug not in self.engine.state['matches']:
                    del self.history[slug]

    def run(self, stop):
        while not stop.is_set():
            state = 'ok'
            try:
                self.refresh()
            except (OSError, ValueError, KeyError, TypeError) as exc:
                state = type(exc).__name__
                with self.store.lock:
                    self.history.clear()
                if state != self.health:
                    LOG.warning('FotMob capture input unavailable: %s', exc)
            if state != self.health:
                ms = now_ms()
                self.store.append('collector', 'fotmob_capture_health',
                    {'recorded_ms': ms, 'state': state, 'policy': POLICY},
                    f'fotmob_capture_health/{day(ms)}.jsonl')
                self.health = state
            stop.wait(.25)

    def decision(self, candidate):
        """Called under Store.lock; return evidence for capture, never a fire."""
        if candidate.get('kind') != 'candidate' or candidate.get('book_role') not in ('home', 'away') or candidate.get('d') != 1:
            return None
        reason = sofa_failure(candidate)
        if reason is None:
            return None
        at = candidate['candidate_recv_ms']
        eligible = [r for r in self.history.get(candidate['slug'], ()) if r['capture_imported_ms'] <= at]
        if not eligible:
            return None
        row = eligible[-1]
        try:
            # VPS import and receiver times are comparable. Genie source times
            # are checked conservatively and retained without clock correction.
            for stamp in ('vps_received_ms', 'vps_applied_ms', 'capture_imported_ms'):
                if not 0 <= at-row[stamp] <= MAX_AGE_MS:
                    return None
            source = row['source']
            request_age = at-source['source_request_ms']
            cache_age = float(source.get('cache', {}).get('Age', 0))*1000
            if not 0 <= cache_age or not 0 <= request_age or not request_age+cache_age <= MAX_AGE_MS:
                return None
            mapping = row['mapping']
            match = self.engine.state['matches'][candidate['slug']]
            expected = {role: self.aliases[match[role]]['id'] for role in ('home', 'away')}
            if (mapping['status'] != 'matched' or mapping['team_ids'] != expected
                    or mapping['fotmob_match_id'] != row['fotmob_match_id']
                    or abs(mapping['kickoff_ms']-match['kickoff_ms']) > 3*3_600_000
                    or row['status_type'] != 'inprogress'):
                return None
            h, a = row['home_score'], row['away_score']
            if any(type(s) is not int or s < 0 for s in (h, a)):
                return None
            lead = h-a if candidate['book_role'] == 'home' else a-h
            minute = minute_hint(row.get('clock_raw'))
            if lead != 1 or (minute is not None and minute < 30):
                return None
        except (KeyError, TypeError, ValueError, OverflowError):
            return None
        return {'capture_reason': 'fotmob_fallback_candidate', 'capture_policy': POLICY,
                'sofascore_failure': reason, 'candidate_recv_ms': at,
                'candidate_emitted_ms': candidate['emitted_ms'], 'passing_fire': False,
                'minute_hint': minute, 'minute_unknown': minute is None,
                'fotmob_snapshot': row, 'fotmob_shadow_session': self.session,
                'fotmob_shadow_dir': str(self.root)}

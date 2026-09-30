"""Sofascore transport over Tailscale or an SSH tunnel. No orders or replay."""
from __future__ import annotations
import argparse
import hashlib
import ipaddress
import json
import logging
import queue
import re
import resource
import signal
import socket
import struct
import sys
import threading
import time
import uuid
from pathlib import Path

from .sources import BackoffError, SofaClient, SofaHTTPError, team_whitelist_from
from .storage import RecordingError, day, dumps, now_ms

LOG = logging.getLogger(__name__)
VERSION = 1
MAX_FRAME = 2 * 1024 * 1024
TAILNET = ipaddress.ip_network('100.64.0.0/10')


def validate_addresses(bind, peer, port):
    loopback = bind == peer == '127.0.0.1'
    if not loopback and (not peer or any(ipaddress.ip_address(x) not in TAILNET for x in (bind, peer))):
        raise ValueError('Relay bind and peer must both be Tailscale IPv4 addresses or both 127.0.0.1 for SSH')
    if not 1024 <= port <= 65535:
        raise ValueError('Relay port must be between 1024 and 65535')


def whitelist_digest(path=None):
    return hashlib.sha256(json.dumps(team_whitelist_from(path), sort_keys=True, ensure_ascii=False).encode()).hexdigest()


class Wire:
    def __init__(self, sock):
        self.sock = sock
        sock.settimeout(1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buffer = bytearray()
        self.lock = threading.RLock()
        self.session = uuid.uuid4().hex
        self.sequence = 0
        self.remote_session = None
        self.remote_sequence = 0

    def send(self, kind, **body):
        with self.lock:
            self.sequence += 1
            message = {'kind': kind, 'session': self.session, 'seq': self.sequence,
                       **body, 'sent_ms': now_ms()}
            raw = dumps(message).encode()
            if len(raw) > MAX_FRAME:
                raise ValueError('Relay frame exceeds limit')
            self.sock.sendall(struct.pack('!I', len(raw)) + raw)
            return self.sequence

    def receive(self):
        while True:
            if len(self.buffer) >= 4:
                length = struct.unpack('!I', self.buffer[:4])[0]
                if not 0 < length <= MAX_FRAME:
                    raise ValueError('Invalid relay frame size')
                if len(self.buffer) >= length + 4:
                    received = now_ms()
                    raw = bytes(self.buffer[4:length + 4]); del self.buffer[:length + 4]
                    msg = json.loads(raw)
                    if self.remote_session is None:
                        self.remote_session = msg['session']
                    if msg['session'] != self.remote_session or msg['seq'] != self.remote_sequence + 1:
                        raise ValueError('Relay session/sequence gap or duplicate')
                    self.remote_sequence = msg['seq']
                    return msg, received
            part = self.sock.recv(65536)
            if not part:
                raise ConnectionError('Relay connection closed')
            self.buffer.extend(part)

    def close(self):
        try: self.sock.shutdown(socket.SHUT_RDWR)
        except OSError: pass
        self.sock.close()


def timing(message, received, applied=None):
    return {'worker_session': message['session'], 'worker_seq': message['seq'],
            'source_request_ms': message.get('request_ms'), 'source_received_ms': message.get('fetched_ms'),
            'http_elapsed_ms': message.get('http_elapsed_ms'), 'worker_sent_ms': message['sent_ms'],
            'vps_received_ms': received, 'vps_applied_ms': applied,
            'transport_age_ms_unadjusted': received - message['sent_ms'],
            'source_age_ms_unadjusted': received - message['fetched_ms'] if message.get('fetched_ms') is not None else None,
            'clock_corrected': False, **({'source_http': message['source_http']} if message.get('source_http') else {})}


class RelayPayload(dict):
    pass


class ProxyClient:
    """Only fixture/detail RPC uses request/response; the live feed is pushed."""
    def __init__(self, configure=None):
        self.lock = threading.Lock()
        self.wire = None
        self.pending = {}
        self.last_timing = None
        self.configure = configure

    def attach(self, wire):
        with self.lock: self.wire = wire

    def detach(self):
        with self.lock:
            self.wire = None
            for result in self.pending.values():
                if result.empty(): result.put_nowait(ConnectionError('Score relay disconnected'))

    def response(self, message, received):
        with self.lock:
            result = self.pending.get(message.get('request_id'))
            if result is not None and result.empty(): result.put_nowait((message, received))

    def get(self, path, **params):
        request_id = uuid.uuid4().hex
        result = queue.Queue(maxsize=1)
        with self.lock:
            wire = self.wire
            if wire is None: raise ConnectionError('Score relay unavailable')
            self.pending[request_id] = result
        try:
            # Mapping can be followed immediately by a detail request. Deliver the
            # current scope before that request, without an older config interleaving.
            with wire.lock:
                if self.configure is not None: self.configure(wire)
                wire.send('request', request_id=request_id, path=path, params=params)
            try: response = result.get(timeout=22)
            except queue.Empty: raise TimeoutError('Score relay request timed out') from None
            if isinstance(response, Exception): raise response
            msg, received = response
            self.last_timing = timing(msg, received)
            if msg.get('backoff_ms') is not None:
                raise BackoffError(msg['status'], now_ms() + msg['backoff_ms'])
            if msg.get('status') != 200:
                if msg.get('status'): raise SofaHTTPError(msg['status'], path)
                raise RuntimeError(msg.get('error', 'Score relay request failed'))
            out = RelayPayload(msg['payload']); out.relay_timing = self.last_timing
            return out
        finally:
            with self.lock: self.pending.pop(request_id, None)


def apply_push(store, message, received):
    from .service import save_score, subscription_matches
    with store.lock:
        applied = now_ms()
        matches = subscription_matches(store.get('matches', {}), applied)
        ids = {m['sofa_event_id'] for m in matches.values() if m.get('sofa_event_id') is not None}
        metadata = timing(message, received, applied)
        if message['kind'] == 'poll':
            row = {k: message.get(k) for k in ('state', 'http_status', 'event_count', 'tracked_event_count', 'error')}
            row.update(poll_ms=applied, request_ms=None, tracked_match_count=len(matches),
                       mapped_event_count=len(ids), relay=metadata)
            if message.get('backoff_ms') is not None:
                # Preserve one local deadline per source backoff episode, so
                # transport jitter does not turn every retry into a new 403.
                key = [message['session'], message.get('backoff_until_ms')]
                previous = store.get('relay_backoff', {})
                if previous.get('key') != key:
                    previous = {'key': key, 'until_ms': applied + message['backoff_ms']}
                    store.put('relay_backoff', previous)
                row['backoff_until_ms'] = previous['until_ms']
                metadata['source_backoff_until_ms'] = message.get('backoff_until_ms')
            store.publish('scores', 'poll', row, f'polls/{day(applied)}.jsonl')
        elif message['kind'] == 'score':
            event = message['event']
            if event.get('id') not in ids:
                return False  # Scope may have changed while the request was in flight.
            save_score(store, event, applied, 'scores', matches=matches, relay=metadata)
        else:
            raise ValueError('Unexpected score push')
    return True


def relay_scores(store, stop, alias_path, config):
    from .service import log_event, resolve_matches, subscription_matches
    validate_addresses(config['bind'], config['peer'], config['port'])
    expected = whitelist_digest(alias_path)
    def configure(wire):
        with wire.lock:
            active = subscription_matches(store.get('matches', {}), now_ms())
            ids = sorted({m['sofa_event_id'] for m in active.values() if m.get('sofa_event_id') is not None})
            wire.send('config', event_ids=ids, tracked_match_count=len(active))
    proxy = ProxyClient(configure)
    resolver = None
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((config['bind'], config['port'])); listener.listen(2); listener.settimeout(1)
        log_event(store, 'relay', 'listening', **config)
        try:
            while not stop.is_set():
                try: conn, peer = listener.accept()
                except socket.timeout: continue
                if peer[0] != config['peer']:
                    conn.close(); LOG.warning('Rejected unexpected relay peer'); continue
                wire = Wire(conn)
                try:
                    msg, _ = wire.receive()
                    if msg['kind'] != 'hello' or msg.get('version') != VERSION or msg.get('whitelist_sha256') != expected:
                        raise ValueError('Score worker protocol/whitelist mismatch')
                    proxy.attach(wire)
                    log_event(store, 'relay', 'connected', worker_session=msg['session'], worker=msg.get('worker'), peer=peer[0])
                    if resolver is None:
                        resolver = threading.Thread(target=resolve_matches, args=(store, stop, proxy, alias_path), name='soccer-relay-resolver')
                        resolver.start()
                    last_config = last_health = 0.; last_in = time.monotonic(); last_poll = time.monotonic(); poll_timeout = False
                    while not stop.is_set():
                        tick = time.monotonic()
                        if tick - last_config >= 1:
                            configure(wire)
                            last_config = tick
                        if tick - last_poll > 12 and not poll_timeout:
                            ms = now_ms()
                            store.publish('scores', 'poll', {'poll_ms': ms, 'state': 'relay_source_timeout', 'http_status': None}, f'polls/{day(ms)}.jsonl')
                            poll_timeout = True
                        try: msg, received = wire.receive()
                        except socket.timeout:
                            if time.monotonic() - last_in > 10: raise TimeoutError('Score worker heartbeat timeout')
                            continue
                        last_in = time.monotonic()
                        if msg['kind'] == 'response': proxy.response(msg, received)
                        elif msg['kind'] in ('poll', 'score'):
                            apply_push(store, msg, received)
                            if msg['kind'] == 'poll': last_poll = time.monotonic(); poll_timeout = False
                            wire.send('ack', worker_seq=msg['seq'], worker_sent_ms=msg['sent_ms'], received_ms=received, applied_ms=now_ms())
                        elif msg['kind'] == 'heartbeat':
                            wire.send('ack', worker_seq=msg['seq'], worker_sent_ms=msg['sent_ms'], received_ms=received, applied_ms=now_ms())
                            if time.monotonic() - last_health >= 60:
                                log_event(store, 'relay', 'health', worker_session=msg['session'], **msg.get('stats', {}))
                                last_health = time.monotonic()
                        else: raise ValueError('Unexpected worker message')
                        store.flush()
                except RecordingError:
                    stop.set(); raise
                except Exception as exc:
                    if not stop.is_set():
                        ms = now_ms()
                        store.publish('scores', 'poll', {'poll_ms': ms, 'state': 'relay_disconnected', 'http_status': None, 'error': str(exc)}, f'polls/{day(ms)}.jsonl')
                        log_event(store, 'relay', 'disconnected', error=str(exc))
                finally:
                    proxy.detach(); wire.close()
        finally:
            proxy.detach()
            if resolver is not None: resolver.join(timeout=25)
            if resolver is not None and resolver.is_alive(): raise RuntimeError('Relay resolver failed to stop')
            store.flush()


def request_result(client, path, params):
    started = now_ms(); mono = time.monotonic()
    try:
        payload = client.get(path, **params)
        result = {'status': 200, 'payload': payload, 'request_ms': started, 'fetched_ms': now_ms(),
                  'http_elapsed_ms': (time.monotonic() - mono) * 1000}
    except BackoffError as exc:
        result = {'status': exc.status, 'backoff_ms': max(0, exc.until_ms - now_ms()),
                  'backoff_until_ms': exc.until_ms, 'request_ms': started, 'error': str(exc)}
    except Exception as exc:
        result = {'status': getattr(exc, 'status', None), 'request_ms': started, 'error': str(exc)}
    if getattr(client, 'last_http', None): result['source_http'] = client.last_http
    return result


def authorized_request(path, event_ids, team_ids):
    fixture = re.fullmatch(r'/team/(\d+)/events/(?:next|last)/0', path)
    detail = re.fullmatch(r'/event/(\d+)', path)
    return bool((fixture and int(fixture[1]) in team_ids) or (detail and int(detail[1]) in event_ids))


def compact_fixtures(payload):
    if not isinstance(payload.get('events'), list): return payload
    # Resolver requires identities and kickoff only; full live/detail events stay intact.
    return {'events': [{k: e[k] for k in ('id', 'startTimestamp', 'homeTeam', 'awayTeam') if k in e}
                       for e in payload['events']]}


def worker_connection(sock, client, stop, alias_path, worker_name):
    wire = Wire(sock); ended = threading.Event(); ready = threading.Event(); lock = threading.Lock()
    state = {'event_ids': [], 'tracked_match_count': 0}; jobs = queue.Queue(maxsize=4)
    team_ids = {t['id'] for t in team_whitelist_from(alias_path).values()}
    stats = {'polls': 0, 'observations': 0, 'last_http_status': None, 'ack_rtt_ms': None}
    wire.send('hello', version=VERSION, whitelist_sha256=whitelist_digest(alias_path), worker=worker_name)
    def send(kind, **body):
        if ended.is_set(): raise ConnectionError('Relay session ended')
        return wire.send(kind, **body)
    def polls():
        while not ended.is_set() and not stop.is_set():
            if not ready.wait(.5): continue
            tick = time.monotonic()
            result = request_result(client, '/sport/football/events/live', {})
            if ended.is_set() or stop.is_set(): return
            payload = result.pop('payload', {})
            events = payload.get('events')
            if result['status'] == 200 and not isinstance(events, list):
                result.update(status=None, error='Sofascore live response has no events array')
            with lock: ids = set(state['event_ids'])
            tracked = [e for e in events if e.get('id') in ids] if result['status'] == 200 else []
            status = result.pop('status')
            send('poll', state='ok' if status == 200 else ('backoff' if 'backoff_ms' in result else 'error'),
                 http_status=status, event_count=len(events) if isinstance(events, list) else None,
                 tracked_event_count=len(tracked), **result)
            for event in tracked: send('score', event=event, **result)
            stats.update(polls=stats['polls'] + 1, observations=stats['observations'] + len(tracked), last_http_status=status)
            ended.wait(max(0, 5 - (time.monotonic() - tick)))
    def rpc():
        while not ended.is_set() and not stop.is_set():
            try: job = jobs.get(timeout=.5)
            except queue.Empty: continue
            with lock: ids = set(state['event_ids'])
            path = job['path']
            if not authorized_request(path, ids, team_ids) or job.get('params'):
                result = {'status': None, 'error': 'Unapproved Sofascore request'}
            else:
                result = request_result(client, path, {})
                if result['status'] == 200 and path.startswith('/team/'):
                    result['payload'] = compact_fixtures(result['payload'])
            if not ended.is_set(): send('response', request_id=job['request_id'], **result)
    def guarded(target):
        try: target()
        except Exception:
            if not ended.is_set(): LOG.exception('Score worker thread failed')
            ended.set()
    threads = [threading.Thread(target=guarded, args=(f,), name=n) for f, n in ((polls, 'sofa-live'), (rpc, 'sofa-rpc'))]
    for t in threads: t.start()
    last_in = time.monotonic(); last_heartbeat = last_log = 0.
    try:
        while not stop.is_set() and not ended.is_set():
            tick = time.monotonic()
            if tick - last_heartbeat >= 2:
                usage = resource.getrusage(resource.RUSAGE_SELF)
                send('heartbeat', stats={**stats, 'cpu_s': usage.ru_utime + usage.ru_stime,
                                         'peak_rss_bytes': usage.ru_maxrss * (1 if sys.platform == 'darwin' else 1024)})
                last_heartbeat = tick
            try: msg, received = wire.receive()
            except socket.timeout:
                if time.monotonic() - last_in > 10: raise TimeoutError('VPS relay heartbeat timeout')
                continue
            last_in = time.monotonic()
            if msg['kind'] == 'config':
                with lock: state.update(event_ids=msg['event_ids'], tracked_match_count=msg['tracked_match_count'])
                ready.set()
            elif msg['kind'] == 'request': jobs.put_nowait(msg)
            elif msg['kind'] == 'ack': stats['ack_rtt_ms'] = received - msg['worker_sent_ms']
            else: raise ValueError('Unexpected VPS relay message')
            if tick - last_log >= 60:
                LOG.info('relay_health %s', dumps(stats)); last_log = tick
    finally:
        ended.set(); wire.close()
        for t in threads: t.join(timeout=17)
        if any(t.is_alive() for t in threads): raise RuntimeError('Score worker failed to stop')


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--vps', required=True); p.add_argument('--port', type=int, default=18765)
    p.add_argument('--aliases', type=Path); p.add_argument('--name', default='genie')
    args = p.parse_args(argv)
    if args.vps != '127.0.0.1' and ipaddress.ip_address(args.vps) not in TAILNET:
        p.error('--vps must be a Tailscale IPv4 address or 127.0.0.1 for an SSH tunnel')
    if not 1024 <= args.port <= 65535: p.error('--port must be between 1024 and 65535')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    team_whitelist_from(args.aliases)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM): signal.signal(sig, lambda *_: stop.set())
    client = SofaClient()
    while not stop.is_set():
        try:
            with socket.create_connection((args.vps, args.port), timeout=10) as sock:
                LOG.info('Connected to VPS score receiver')
                worker_connection(sock, client, stop, args.aliases, args.name)
        except Exception:
            if not stop.is_set(): LOG.exception('Score relay disconnected; retrying')
        stop.wait(5)

if __name__ == '__main__': main()

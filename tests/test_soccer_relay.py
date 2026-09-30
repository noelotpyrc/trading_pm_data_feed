"""Relay transport, scoped delivery and cross-host score causality; no internet."""
import json
import socket
import struct
import threading
import time
from contextlib import contextmanager

import pytest

from pm_soccer_dryrun import relay
from pm_soccer_dryrun.engine import Engine
from pm_soccer_dryrun.storage import Store


@contextmanager
def connection():
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0)); listener.listen()
        left = socket.create_connection(listener.getsockname())
        right, _ = listener.accept()
        try: yield left, right
        finally: left.close(); right.close()


def test_fragment_survives_timeout_and_rejects_duplicate():
    with connection() as (sender, sock):
        wire = relay.Wire(sock); sock.settimeout(.02)
        msg = {'kind': 'poll', 'session': 'one', 'seq': 1, 'sent_ms': 100}
        raw = json.dumps(msg).encode(); frame = struct.pack('!I', len(raw)) + raw
        sender.sendall(frame[:9])
        with pytest.raises(socket.timeout): wire.receive()
        sender.sendall(frame[9:])
        assert wire.receive()[0] == msg
        sender.sendall(frame)
        with pytest.raises(ValueError, match='duplicate'): wire.receive()


def test_oversize_rejected_before_body_read():
    with connection() as (sender, sock):
        wire = relay.Wire(sock)
        sender.sendall(struct.pack('!I', relay.MAX_FRAME + 1))
        with pytest.raises(ValueError, match='size'): wire.receive()


def test_scoped_score_uses_vps_application_time(tmp_path, monkeypatch):
    store = Store(tmp_path)
    match = {'slug': 'test', 'sofa_event_id': 42, 'kickoff_ms': 0, 'books': {}}
    store.put('matches', {'test': match})
    engine = Engine('relay-test'); store.on_event = lambda e: engine.handle(e, 5000)
    engine.state['matches']['test'] = match.copy()
    monkeypatch.setattr(relay, 'now_ms', lambda: 5000)
    raw = {'id': 42, 'homeScore': {'current': 1}, 'awayScore': {'current': 0},
           'status': {'type': 'inprogress', 'description': '1st half'},
           'extra_field': {'keep': 'everything'}}
    msg = {'kind': 'score', 'session': 'worker', 'seq': 2, 'sent_ms': 1100,
           'request_ms': 900, 'fetched_ms': 1000, 'event': raw,
           'source_http': {'hostname': 'www.sofascore.com', 'address_family': 'IPv4'}}
    assert relay.apply_push(store, msg, 4900)
    assert engine.score_snapshot(match, 4999)['no_score']
    known = engine.score_snapshot(match, 5000)
    assert known['home_score'] == 1
    assert known['score_relay']['source_received_ms'] == 1000
    assert known['score_relay']['vps_received_ms'] == 4900
    assert known['score_relay']['vps_applied_ms'] == 5000
    assert known['score_relay']['source_http']['hostname'] == 'www.sofascore.com'
    assert store.get('score:42')['raw_event'] == raw
    assert not relay.apply_push(store, {**msg, 'event': {**raw, 'id': 99}}, 4900)
    assert store.get('score:99') is None
    store.close()


def test_proxy_disconnect_wakes_pending_request():
    with connection() as (sock, peer):
        proxy = relay.ProxyClient(); proxy.attach(relay.Wire(sock)); remote = relay.Wire(peer)
        errors = []
        def request():
            try: proxy.get('/event/42')
            except Exception as exc: errors.append(exc)
        t = threading.Thread(target=request); t.start()
        assert remote.receive()[0]['kind'] == 'request'
        proxy.detach(); t.join(1)
        assert not t.is_alive()
        assert isinstance(errors[0], ConnectionError)


def test_worker_push_and_rpc_scope_update(monkeypatch):
    """A newly mapped detail is authorized immediately; global payload stays local."""
    monkeypatch.setattr(relay, 'team_whitelist_from', lambda _: {'Team': {'id': 7}})
    monkeypatch.setattr(relay, 'whitelist_digest', lambda _: 'same')
    fetched = []
    class Client:
        def get(self, path):
            fetched.append(path)
            if path.endswith('/live'):
                return {'events': [{'id': 42, 'extra': 'full'}, {'id': 99, 'secret': 'untracked'}]}
            return {'event': {'id': 43, 'extra': 'detail'}}
    stop = threading.Event(); failures = []
    with connection() as (sock, peer):
        remote = relay.Wire(peer)
        def run():
            try: relay.worker_connection(sock, Client(), stop, None, 'test')
            except Exception as exc: failures.append(exc)
        t = threading.Thread(target=run); t.start()
        try:
            assert remote.receive()[0]['kind'] == 'hello'
            remote.send('config', event_ids=[42], tracked_match_count=1)
            messages = []; deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                msg, _ = remote.receive(); messages.append(msg)
                if msg['kind'] == 'score': break
            scores = [m['event'] for m in messages if m['kind'] == 'score']
            assert scores == [{'id': 42, 'extra': 'full'}]
            assert next(m for m in messages if m['kind'] == 'poll')['event_count'] == 2
            def config(wire): wire.send('config', event_ids=[42, 43], tracked_match_count=2)
            proxy = relay.ProxyClient(config); proxy.attach(remote)
            result = []; rpc = threading.Thread(target=lambda: result.append(proxy.get('/event/43')))
            rpc.start()
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                msg, received = remote.receive()
                if msg['kind'] == 'response':
                    proxy.response(msg, received); break
            rpc.join(1)
            assert not rpc.is_alive()
            assert result[0]['event'] == {'id': 43, 'extra': 'detail'}
            assert '/event/43' in fetched
        finally:
            stop.set(); t.join(3)
        assert not t.is_alive()
        assert failures == []


def test_request_whitelist_and_listener_restrictions():
    assert relay.authorized_request('/team/7/events/next/0', {42}, {7})
    assert relay.authorized_request('/event/42', {42}, {7})
    for path in ('/event/99', '/team/8/events/next/0', '/search/all', 'https://other/event/42'):
        assert not relay.authorized_request(path, {42}, {7})
    relay.validate_addresses('100.86.137.98', '100.80.55.8', 18765)
    relay.validate_addresses('127.0.0.1', '127.0.0.1', 18765)
    with pytest.raises(ValueError): relay.validate_addresses('127.0.0.1', '100.80.55.8', 18765)
    with pytest.raises(ValueError): relay.validate_addresses('100.86.137.98', '127.0.0.1', 18765)
    with pytest.raises(ValueError): relay.validate_addresses('0.0.0.0', '100.80.55.8', 18765)


def test_relay_backoff_is_one_episode_despite_transport_jitter(tmp_path, monkeypatch):
    store = Store(tmp_path)
    applied = [1000]
    monkeypatch.setattr(relay, 'now_ms', lambda: applied[0])
    msg = {'kind': 'poll', 'session': 'worker', 'seq': 1, 'sent_ms': 900,
           'state': 'backoff', 'http_status': 403, 'backoff_ms': 180000,
           'backoff_until_ms': 180900}
    relay.apply_push(store, msg, 1000)
    applied[0] = 6002
    relay.apply_push(store, {**msg, 'seq': 2, 'sent_ms': 5900, 'backoff_ms': 175000}, 6000)
    assert store.summary()['http_403s'] == 1
    store.close()

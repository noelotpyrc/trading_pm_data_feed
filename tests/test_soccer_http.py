"""Endpoint failover without network calls or changes to signal semantics."""
import threading
from types import SimpleNamespace

import pytest
from curl_cffi import CurlOpt

from pm_soccer_dryrun.sources import SofaClient, BackoffError, SofaHTTPError, SOFA, SOFA_FALLBACK
from pm_soccer_dryrun import relay

LIVE = '/sport/football/events/live'


def response(status=200, payload=None, headers=None):
    return SimpleNamespace(status_code=status, headers=headers or {},
                           json=lambda: {'events': []} if payload is None else payload)


def client_for(actions, clock=None, mono=None):
    calls, options = [], []
    def factory(**kwargs):
        options.append(kwargs)
        def get(url, **kw):
            calls.append((url, kw))
            action = actions.pop(0)
            if isinstance(action, Exception): raise action
            if callable(action): return action()
            return action
        return SimpleNamespace(get=get)
    c = SofaClient(session_factory=factory, clock=clock, monotonic=mono)
    return c, calls, options


def test_primary_ipv4_and_provenance():
    c, calls, options = client_for([response(headers={'Age': '1'})])
    assert c.get(LIVE) == {'events': []}
    assert calls[0][0] == SOFA + LIVE
    assert all(o['curl_options'][CurlOpt.IPRESOLVE] == 1 for o in options)
    assert c.last_http['hostname'] == 'www.sofascore.com'
    assert c.last_http['cache'] == {'Age': '1'}


def test_403_fallback_sticky_then_primary_recovers():
    now = [1000]
    c, calls, _ = client_for([response(403, {'error': {'reason': 'challenge'}}),
                             response(), response(), response()], clock=lambda: now[0])
    c.get(LIVE)
    assert c.last_http['attempts'][0]['reason'] == 'challenge'
    assert c.last_http['hostname'] == 'api.sofascore.com'
    c.get(LIVE)
    assert [u for u, _ in calls] == [SOFA+LIVE, SOFA_FALLBACK+LIVE, SOFA_FALLBACK+LIVE]
    now[0] = 181001
    c.get(LIVE)
    assert calls[-1][0] == SOFA+LIVE


def test_both_blocked_do_not_retry_before_cooldown():
    c, calls, _ = client_for([response(403), response(403)], clock=lambda: 1000)
    for _ in range(3):
        with pytest.raises(BackoffError) as e: c.get(LIVE)
        assert e.value.until_ms == 181000
    assert len(calls) == 2


@pytest.mark.parametrize('header,expected', [('600', 600000), ('Wed, 30 Sep 2026 14:10:00 GMT', 600000), ('bad', 180000)])
def test_429_is_provider_wide_and_respects_retry_after(header, expected):
    now = 1790776800000  # 14:00 UTC
    c, calls, _ = client_for([response(429, headers={'Retry-After': header})], clock=lambda: now)
    for _ in range(2):
        with pytest.raises(BackoffError) as e: c.get(LIVE)
        assert e.value.until_ms == now + expected
    assert len(calls) == 1


@pytest.mark.parametrize('failure', [response(503), TimeoutError('timeout'), response(payload={'bad': []}),
                                   response(payload={'events': [None]}), response(payload={'events': [{'id': None}]})])
def test_transient_and_invalid_payload_fall_back(failure):
    c, calls, _ = client_for([failure, response()])
    assert c.get(LIVE) == {'events': []}
    assert len(calls) == 2


def test_404_is_not_retried_on_other_host():
    c, calls, _ = client_for([response(404)])
    with pytest.raises(SofaHTTPError) as e: c.get('/team/7/events/next/0')
    assert e.value.status == 404
    assert len(calls) == 1


def test_detail_id_must_match_and_fallback_can_supply_detail():
    c, calls, _ = client_for([response(payload={'event': {'id': 43}}),
                             response(payload={'event': {'id': 42}})])
    assert c.get('/event/42')['event']['id'] == 42
    assert len(calls) == 2


def test_second_attempt_uses_remaining_budget():
    times = iter([0, 0, 7])
    c, calls, _ = client_for([TimeoutError(), response()], mono=lambda: next(times))
    c.get(LIVE)
    assert [kw['timeout'] for _, kw in calls] == [5, 3]


def test_exhausted_budget_never_starts_second_attempt():
    times = iter([0, 11])
    c, calls, _ = client_for([], mono=lambda: next(times))
    with pytest.raises(TimeoutError): c.get(LIVE)
    assert not calls


def test_concurrent_requests_share_cooldown_and_do_not_duplicate_probes():
    entered, release = threading.Event(), threading.Event()
    def blocked():
        entered.set()
        assert release.wait(2)
        return response(403)
    c, calls, _ = client_for([blocked, response(403)], clock=lambda: 1000)
    errors = []
    def run():
        try: c.get(LIVE)
        except Exception as exc: errors.append(exc)
    a = threading.Thread(target=run); b = threading.Thread(target=run)
    a.start(); assert entered.wait(1); b.start(); release.set()
    a.join(2); b.join(2)
    assert not a.is_alive() and not b.is_alive()
    assert len(calls) == 2 and len(errors) == 2
    assert all(isinstance(e, BackoffError) for e in errors)


def test_route_survives_relay_result_and_timing():
    c, _, _ = client_for([response()])
    result = relay.request_result(c, LIVE, {})
    metadata = relay.timing({**result, 'session': 'one', 'seq': 1, 'sent_ms': 5}, 6, 7)
    assert metadata['source_http']['hostname'] == 'www.sofascore.com'
    assert metadata['vps_applied_ms'] == 7

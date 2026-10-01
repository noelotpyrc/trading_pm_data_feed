import gzip
import json
import socket
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from pm_soccer_dryrun.fotmob import (FotmobClient, FotmobError, aliases_from, dates_for,
                                    fixtures, normalize, resolve, utc_ms)
from pm_soccer_dryrun.fotmob_shadow import (PrimaryCatalog, ShadowSink, collect_cycle,
                                          detail_snapshot, receive_connection, work_connection, main, send_shadow,
                                          CollectionState)
from pm_soccer_dryrun.storage import Store

ALIASES = {'Home': {'id': 10, 'name': 'Home'}, 'Away': {'id': 20, 'name': 'Away'}}
KICK = 1790773200000
MATCH = {'slug': 'fif-home-away-2026-09-30', 'home': 'Home', 'away': 'Away', 'kickoff_ms': KICK}


def fixture(id=42, home=10, away=20, utc='2026-09-30T13:00:00Z'):
    return {'match': {'id': id, 'home': {'id':home,'name':'Home','score':0},
                     'away': {'id':away,'name':'Away','score':1},
                     'status': {'utcTime':utc,'started':True,'finished':False,'liveTime':{'short':'HT'}}},
            'league': {'id':914609,'primaryId':114}}


def detail():
    return {'general':{'matchId':'42','coverageLevel':'lower','leagueId':914609,'parentLeagueId':114,
                      'homeTeam':{'id':10},'awayTeam':{'id':20},'matchTimeUTCDate':'2026-09-30T13:00:00Z'},
            'header':{'teams':[{'id':10,'score':0},{'id':20,'score':1}],
                      'status':{'utcTime':'2026-09-30T13:00:00Z','started':True,'liveTime':{'short':'HT'}}},
            'hasPendingVAR':True,'content':{'matchFacts':{'events':{'events':[
                {'type':'Goal','minute':32},{'type':'VAR','unknown_provider_field':'keep'}]}},
                'liveticker':{'messages':['review']},'stats':{'offline':'omit'}}}


def test_mapping_exact_swapped_ambiguous_and_competition():
    assert resolve(MATCH,[fixture()],ALIASES)['fotmob_match_id']==42
    assert resolve(MATCH,[fixture(home=20,away=10)],ALIASES)['swapped']
    assert resolve(MATCH,[fixture(),fixture(id=43)],ALIASES)['status']=='ambiguous'
    assert resolve(MATCH,[fixture(home=999)],ALIASES)['status']=='missing'
    wrong=fixture();wrong['league']={'id':47}
    assert resolve(MATCH,[wrong],ALIASES)['status']=='missing'
    assert resolve(MATCH,[fixture()],{})['status']=='team_not_whitelisted'


def test_dates_and_timezone_fail_closed():
    assert dates_for([{**MATCH,'kickoff_ms':utc_ms('2026-10-01T00:15:00Z')}])==['20260930','20261001']
    assert utc_ms('30.09.2026 15:00:34') is None
    assert utc_ms('2026-09-30T13:00:00') is None
    assert resolve(MATCH,[fixture(utc='2026-10-01T13:00:00Z')],ALIASES)['status']=='missing'


def test_raw_incidents_var_and_clock_preserved_without_invented_minutes():
    raw=detail_snapshot(detail());r=normalize(raw,'match_details',True)
    assert (r['home_score'],r['away_score'])==(1,0)
    assert r['has_pending_var'] is True and r['clock_raw']=={'short':'HT'}
    assert not r['clock_eligible_for_engine']
    assert raw['content']['matchFacts']['events']['events'][1]['unknown_provider_field']=='keep'
    assert raw['omitted_content_keys']==['stats'] and 'stats' not in raw['content']


@pytest.mark.parametrize('status,expected', [({'finished':True},'finished'),({'cancelled':True},'cancelled'),
                                            ({'reason':{'short':'Postponed'}},'postponed'),({},'notstarted')])
def test_status_and_missing_score(status,expected):
    p=detail();p['header']['status']=status;p['header']['teams'][0]['score']=None
    r=normalize(p,'match_details')
    assert r['home_score'] is None and r['status_type']==expected


def primary(tmp_path):
    root=tmp_path/'primary';folder=root/'sessions'/'s1'/'matches'/'metadata';folder.mkdir(parents=True)
    (root/'run.json').write_text('{}\n')
    row={**MATCH,'sofa_event_id':77,'score_observed_ms':KICK,'books':{},'event_seq':1}
    path=folder/'2026-09-30.jsonl';path.write_text(json.dumps(row)+'\n')
    return root,path,row


def test_catalog_read_only_partial_lines_and_new_session(tmp_path):
    root,p,row=primary(tmp_path);catalog=PrimaryCatalog(root)
    before=p.read_bytes()
    assert catalog.refresh(KICK)[0]['slug']==MATCH['slug']
    with p.open('a') as f:f.write(json.dumps({**row,'home':'Changed','event_seq':2}))
    assert catalog.refresh(KICK)[0]['home']=='Home'
    with p.open('a') as f:f.write('\n')
    assert catalog.refresh(KICK)[0]['home']=='Changed'
    assert p.read_bytes().startswith(before)
    (root/'sessions'/'s2').mkdir()
    assert catalog.refresh(KICK)==[]


def test_stale_primary_scope_is_not_kept_alive_by_sidecar(tmp_path):
    import os
    root,p,row=primary(tmp_path);catalog=PrimaryCatalog(root)
    os.utime(p,(KICK/1000-60,KICK/1000-60))
    assert catalog.refresh(KICK)==[]
    poll=root/'sessions'/'s1'/'polls'/'2026-09-30.jsonl';poll.parent.mkdir()
    poll.write_text('{}\n');os.utime(poll,(KICK/1000,KICK/1000))
    assert catalog.refresh(KICK)[0]['slug']==MATCH['slug']


def test_oversize_is_an_explicit_health_record():
    sent=[];wire=SimpleNamespace(send=lambda *a,**kw:sent.append((a,kw)))
    send_shadow(wire,'observation',slug='test',payload='x'*(2*1024*1024))
    assert len(sent)==1 and sent[0][0]==('health',)
    assert sent[0][1]['report']['state']=='oversize'


def test_payload_dedup_observation_times_and_score_correction(tmp_path,monkeypatch):
    root,_,_=primary(tmp_path);catalog=PrimaryCatalog(root);scope=catalog.refresh(KICK)
    catalog.scores[77]={'home_score':0,'away_score':1,'poll_ms':KICK,'status_type':'inprogress'}
    store=Store(tmp_path/'shadow');sink=ShadowSink(store)
    mapping=resolve(MATCH,[fixture()],ALIASES)
    message={'kind':'observation','slug':MATCH['slug'],'mapping':mapping,'payload':detail_snapshot(detail()),
             'payload_kind':'match_details','timing':{'source_received_ms':KICK-100},
             'session':'worker','seq':1,'sent_ms':KICK-50}
    clock=[KICK];monkeypatch.setattr('pm_soccer_dryrun.fotmob_shadow.now_ms',lambda:clock[0])
    sink.record(message,KICK,scope,catalog);clock[0]+=10;sink.record(message,KICK+10,scope,catalog)
    assert len(list(store.root.glob('payloads/*/*.gz')))==1
    message['payload']['header']['teams'][1]['score']=0
    clock[0]+=10;sink.record(message,KICK+20,scope,catalog);store.close()
    rows=[json.loads(x) for p in store.root.glob('observations/*.jsonl') for x in p.read_text().splitlines()]
    assert len(rows)==3 and rows[0]['vps_applied_ms']<rows[1]['vps_applied_ms']
    assert rows[-1]['away_score']==0  # Retain VAR reversals, never monotonic-score filtering.
    assert len(list(store.root.glob('payloads/*/*.gz')))==2
    assert json.loads(gzip.decompress((store.root/rows[0]['raw_payload_path']).read_bytes()))['hasPendingVAR']
    assert store.on_event is None and not (store.root/'fires').exists()


def test_out_of_scope_and_wrong_identity_rejected(tmp_path):
    root,_,_=primary(tmp_path);cat=PrimaryCatalog(root);scope=cat.refresh(KICK);store=Store(tmp_path/'shadow')
    sink=ShadowSink(store);m={'kind':'observation','slug':'other','mapping':{}}
    with pytest.raises(ValueError,match='scope'):sink.record(m,KICK,scope,cat)
    m.update(slug=MATCH['slug'],mapping=resolve(MATCH,[fixture()],ALIASES),payload=detail(),payload_kind='match_details')
    m['payload']['header']['teams'][0]['id']=99
    with pytest.raises(ValueError,match='team IDs'):sink.record(m,KICK,scope,cat)
    store.close()


def test_fotmob_backoff_is_independent_and_no_network_while_idle():
    calls=[]
    session=SimpleNamespace(get=lambda *a,**k:calls.append(1) or SimpleNamespace(status_code=429,headers={'Retry-After':'600'}))
    c=FotmobClient(session=session,clock=lambda:1000,sleep=lambda _:None)
    for _ in range(2):
        with pytest.raises(FotmobError) as e:c.get('matches',date='20260930')
        assert e.value.until_ms==601000
    assert len(calls)==1
    sent=[];collect_cycle(c,[],ALIASES,lambda *a,**kw:sent.append((a,kw)),threading.Event())
    assert len(calls)==1 and sent[0][1]['report']['state']=='idle'


def test_cli_refuses_primary_output_and_production_port(tmp_path):
    root,_,_=primary(tmp_path)
    with pytest.raises(SystemExit):main(['receiver','--primary-dir',str(root),'--data-dir',str(root/'shadow')])
    with pytest.raises(SystemExit):main(['worker','--port','18765'])


def test_bundled_aliases_have_real_provider_ids():
    a=aliases_from()
    assert a['Seychelles']['id']==121274 and a['Sri Lanka']['id']==5895
    assert a['Botswana']['id']==5887


def test_separate_relay_integration_leaves_primary_bytes_unchanged(tmp_path,monkeypatch):
    root,p,row=primary(tmp_path);at=int(time.time()*1000)
    row['kickoff_ms']=at;p.write_text(json.dumps(row)+'\n');before=p.read_bytes()
    catalog=PrimaryCatalog(root);store=Store(tmp_path/'shadow');sink=ShadowSink(store)
    stop=threading.Event();errors=[]
    f=fixture();f['match']['status']['utcTime']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
    class Client:
        def get(self,route,**kw):
            payload={'leagues':[{'id':914609,'primaryId':114,'matches':[f['match']]}]} if route=='matches' else detail()
            if route=='matchDetails':
                payload['general']['matchTimeUTCDate']=f['match']['status']['utcTime']
                payload['header']['status']['utcTime']=f['match']['status']['utcTime']
            return payload,{'source_request_ms':at,'source_received_ms':at+1}
    def runner(fn,*a):
        try:fn(*a)
        except Exception as e:errors.append(e)
    with socket.socket() as listener:
        listener.bind(('127.0.0.1',0));listener.listen()
        left=socket.create_connection(listener.getsockname());right,_=listener.accept()
        receiver=threading.Thread(target=runner,args=(receive_connection,right,catalog,sink,ALIASES,stop))
        worker=threading.Thread(target=runner,args=(work_connection,left,Client(),ALIASES,stop,.05))
        receiver.start();worker.start()
        try:
            deadline=time.monotonic()+3
            while time.monotonic()<deadline and not list(store.root.glob('observations/*.jsonl')):time.sleep(.02)
        finally:
            stop.set();receiver.join(3);worker.join(3)
    store.close()
    assert not receiver.is_alive() and not worker.is_alive()
    assert list(store.root.glob('observations/*.jsonl'))
    assert p.read_bytes()==before and not (root/'fires').exists()
    assert all(isinstance(e,ConnectionError) for e in errors)


class FixtureClient:
    def __init__(self):
        self.calls=[]
        self.listing=[fixture()]
        self.payload=detail()
        self.failure={}

    def get(self,route,**params):
        self.calls.append((route,params))
        if route in self.failure: raise self.failure[route]
        payload=({'leagues':[{'id':914609,'primaryId':114,
                             'matches':[i['match'] for i in self.listing]}]}
                 if route=='matches' else self.payload)
        return payload,{'source_request_ms':KICK,'source_received_ms':KICK+1}


def cycle(client,state,matches=None,seeds=None,session='primary'):
    sent=[]
    collect_cycle(client,[MATCH] if matches is None else matches,ALIASES,
                  lambda kind,**fields:sent.append((kind,fields)),threading.Event(),state,session,seeds)
    return sent


def test_verified_mapping_survives_listing_disappearance_without_redundant_lookup(monkeypatch):
    monkeypatch.setattr('pm_soccer_dryrun.fotmob_shadow.now_ms',lambda:KICK)
    client,state=FixtureClient(),CollectionState()
    first=cycle(client,state)
    client.listing=[]
    second=cycle(client,state);third=cycle(client,state)
    assert [r for r,_ in client.calls]==['matches','matchDetails','matchDetails','matchDetails']
    assert sum(k=='mapping' for k,_ in first+second+third)==1
    assert [v['payload_kind'] for k,v in second if k=='observation']==['match_details']
    assert second[-1][1]['report']['state']=='ok'
    assert second[-1][1]['report']['discovery_matches']==0


def test_unresolved_discovery_retries_once_per_minute(monkeypatch):
    clock=[KICK];monkeypatch.setattr('pm_soccer_dryrun.fotmob_shadow.now_ms',lambda:clock[0])
    client,state=FixtureClient(),CollectionState();client.listing=[]
    cycle(client,state)
    clock[0]+=10_000;cycle(client,state)
    clock[0]+=49_999;cycle(client,state)
    assert len(client.calls)==1
    clock[0]+=1;client.listing=[fixture()];cycle(client,state)
    assert [r for r,_ in client.calls]==['matches','matches','matchDetails']
    cycle(client,state)
    assert [r for r,_ in client.calls][-1]=='matchDetails'
    assert sum(r=='matches' for r,_ in client.calls)==2


def test_unknown_aliases_do_not_generate_useless_discovery_requests():
    client,state=FixtureClient(),CollectionState()
    sent=cycle(client,state,matches=[{**MATCH,'home':'Unknown'}])
    cycle(client,state,matches=[{**MATCH,'home':'Unknown'}])
    assert not client.calls
    assert sent[0][1]['mapping']['status']=='team_not_whitelisted'


def test_new_match_scope_session_and_identity_changes_control_discovery(monkeypatch):
    monkeypatch.setattr('pm_soccer_dryrun.fotmob_shadow.now_ms',lambda:KICK)
    client,state=FixtureClient(),CollectionState()
    cycle(client,state)
    cycle(client,state,matches=[MATCH,{**MATCH,'slug':'fif-home-away-second'}])
    assert sum(r=='matches' for r,_ in client.calls)==2
    cycle(client,state,session='new-primary')
    assert sum(r=='matches' for r,_ in client.calls)==3
    cycle(client,state,matches=[{**MATCH,'kickoff_ms':KICK+3600000}],session='new-primary')
    assert sum(r=='matches' for r,_ in client.calls)==4
    before=len(client.calls);cycle(client,state,matches=[])
    assert not state.entries and len(client.calls)==before


def test_discovery_failure_does_not_interrupt_already_mapped_details(monkeypatch):
    monkeypatch.setattr('pm_soccer_dryrun.fotmob_shadow.now_ms',lambda:KICK)
    client,state=FixtureClient(),CollectionState()
    cycle(client,state)
    client.failure['matches']=FotmobError('Timeout')
    sent=cycle(client,state,matches=[MATCH,{**MATCH,'slug':'fif-home-away-new'}])
    assert client.calls[-1][0]=='matchDetails'
    assert any(k=='observation' and v['slug']==MATCH['slug'] for k,v in sent)
    assert any(k=='health' and v['report']['state']=='discovery_error' for k,v in sent)


def test_detail_404_retains_mapping_and_paces_retry(monkeypatch):
    clock=[KICK];monkeypatch.setattr('pm_soccer_dryrun.fotmob_shadow.now_ms',lambda:clock[0])
    client,state=FixtureClient(),CollectionState()
    cycle(client,state)
    client.failure['matchDetails']=FotmobError('HTTP 404',404)
    cycle(client,state)
    clock[0]+=10_000;cycle(client,state)
    assert len(client.calls)==3 and state.entries[MATCH['slug']]['mapping'] is not None
    clock[0]+=50_000;del client.failure['matchDetails'];cycle(client,state)
    assert len(client.calls)==4 and client.calls[-1][0]=='matchDetails'
    assert sum(r=='matches' for r,_ in client.calls)==1


@pytest.mark.parametrize('change',['teams','competition','kickoff','id'])
def test_changed_detail_identity_invalidates_mapping_without_accepting_score(monkeypatch,change):
    monkeypatch.setattr('pm_soccer_dryrun.fotmob_shadow.now_ms',lambda:KICK)
    client,state=FixtureClient(),CollectionState();cycle(client,state)
    if change=='teams':client.payload['header']['teams'][0]['id']=999
    elif change=='competition':client.payload['general'].update(leagueId=47,parentLeagueId=47)
    elif change=='kickoff':
        client.payload['general']['matchTimeUTCDate']='2026-10-01T13:00:00Z'
        client.payload['header']['status']['utcTime']='2026-10-01T13:00:00Z'
    else:client.payload['general']['matchId']='999'
    sent=cycle(client,state)
    assert not any(k=='observation' for k,_ in sent)
    assert state.entries[MATCH['slug']]['mapping'] is None
    assert any(k=='mapping' and v['mapping']['status']=='identity_mismatch' for k,v in sent)
    cycle(client,state)
    assert sum(r=='matches' for r,_ in client.calls)==1  # Retry is paced too.


def test_restart_seed_from_existing_recording_requires_fresh_detail(tmp_path,monkeypatch):
    monkeypatch.setattr('pm_soccer_dryrun.fotmob_shadow.now_ms',lambda:KICK)
    root,_,_=primary(tmp_path);catalog=PrimaryCatalog(root);scope=catalog.refresh(KICK)
    old=Store(tmp_path/'shadow/sessions/s1');sink=ShadowSink(old)
    mapping=resolve(MATCH,[fixture()],ALIASES)
    message={'kind':'observation','slug':MATCH['slug'],'mapping':mapping,'payload':detail(),
             'payload_kind':'match_details','timing':{},'session':'old-worker','seq':1,'sent_ms':KICK}
    sink.record(message,KICK,scope,catalog);old.close()
    new=Store(tmp_path/'shadow/sessions/s2');sink=ShadowSink(new)
    seeds=sink.mapping_seeds(scope,ALIASES)
    assert seeds[MATCH['slug']]['fotmob_match_id']==42
    client=FixtureClient();client.listing=[];client.payload['header']['teams'][1]['score']=2
    sent=cycle(client,CollectionState(),seeds=seeds)
    assert [r for r,_ in client.calls]==['matchDetails']
    observation=next(v for k,v in sent if k=='observation')
    assert observation['payload']['header']['teams'][1]['score']==2
    assert not list(new.root.rglob('observations/*.jsonl'))  # Seeds did not replay old scores.
    assert sink.mapping_seeds([{**MATCH,'kickoff_ms':KICK+3600000}],ALIASES)=={}
    new.close()

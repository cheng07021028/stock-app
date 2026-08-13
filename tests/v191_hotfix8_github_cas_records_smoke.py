# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from types import ModuleType, SimpleNamespace
import json, sys, threading, time

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
fake_st=ModuleType('streamlit'); fake_st.secrets={}; fake_st.session_state={}
fake_st.cache_data=SimpleNamespace(clear=lambda:None); fake_st.cache_resource=SimpleNamespace(clear=lambda:None)
sys.modules.setdefault('streamlit',fake_st)

import godpick_persistence_service as gps

# 1) Generic GitHub writer: first PUT 409, next CAS re-fetches new SHA and succeeds.
orig_cfg=gps.github_config; orig_get=gps.requests.get; orig_put=gps.requests.put; orig_read=gps.read_github_json
try:
    gps.github_config=lambda:{'token':'t','repo':'o/r','branch':'runtime-data'}
    gps._github_url=lambda path:f'https://api.github.test/{path}'
    gps._github_headers=lambda token:{}
    gets=[]; puts=[]; remote={'sha':'a'*40,'payload':None}
    class Resp:
        def __init__(self,status,data=None,text=''):
            self.status_code=status; self._data=data or {}; self.text=text; self.content=b'x' if data is not None else b''
        def json(self): return self._data
    def fake_get(url,**kwargs):
        gets.append(remote['sha'])
        return Resp(200,{'sha':remote['sha']})
    def fake_put(url,headers=None,json=None,timeout=None):
        puts.append(json.get('sha'))
        if len(puts)==1:
            remote['sha']='b'*40
            return Resp(409,text=f'{{"message":"is at {remote["sha"]} but expected {json.get("sha")}"}}')
        assert json.get('sha')=='b'*40,(puts,json)
        remote['payload']={'x':1}; remote['sha']='c'*40
        return Resp(200,{'content':{'sha':remote['sha']}})
    gps.requests.get=fake_get; gps.requests.put=fake_put
    gps.read_github_json=lambda path,default:(remote['payload'],'mock')
    ok,msg=gps.write_github_json('x.json',{'x':1},'test')
    assert ok,(msg,gets,puts)
    assert puts==['a'*40,'b'*40],puts
    assert 'CAS衝突自動重試 1 次' in msg,msg
finally:
    gps.github_config=orig_cfg; gps.requests.get=orig_get; gps.requests.put=orig_put; gps.read_github_json=orig_read
print('PASS H8 GitHub 409 re-fetches latest SHA and CAS retries')

# 2) Same-process writers to the same path must never overlap PUTs.
orig_cfg=gps.github_config; orig_get=gps.requests.get; orig_put=gps.requests.put; orig_read=gps.read_github_json
try:
    gps.github_config=lambda:{'token':'t','repo':'o/r','branch':'runtime-data'}
    gps._github_url=lambda path:f'https://api.github.test/{path}'
    gps._github_headers=lambda token:{}
    active={'n':0,'max':0}; guard=threading.Lock(); payload_by_thread={}
    class Resp:
        status_code=200; text=''; content=b'x'
        def json(self): return {'sha':'d'*40}
    gps.requests.get=lambda *a,**k:Resp()
    def fake_put(*a,**k):
        with guard:
            active['n']+=1; active['max']=max(active['max'],active['n'])
        time.sleep(0.08)
        with guard: active['n']-=1
        return Resp()
    gps.requests.put=fake_put
    # verification payload follows current thread's input
    gps.read_github_json=lambda path,default:({'v':threading.current_thread().name},'mock')
    results=[]
    def one(name):
        threading.current_thread().name=name
        results.append(gps.write_github_json('same.json',{'v':name},'lock-test')[0])
    t1=threading.Thread(target=one,args=('A',)); t2=threading.Thread(target=one,args=('B',))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert all(results),results
    assert active['max']==1,active
finally:
    gps.github_config=orig_cfg; gps.requests.get=orig_get; gps.requests.put=orig_put; gps.read_github_json=orig_read
print('PASS H8 same-path writes are serialized in-process')

# 3) Records gateway skips a stale local snapshot when remote state revision is newer.
orig_read=gps.read_github_json; orig_write=gps.write_github_json; orig_secret=gps._secret
try:
    state={'payload_hash':'localhash','revision':100,'count':2}
    gps.read_github_json=lambda path,default:({'payload_hash':'remotehash','revision':200,'count':3},'mock') if path==gps.RECORDS_STATE_FILE else (default,'mock')
    calls=[]
    gps.write_github_json=lambda *a,**k:(calls.append(a) or (True,'should-not-write'))
    gps._secret=lambda key,default='': default
    ok,msg=gps._sync_records_github_snapshot_v191_h8([{'record_id':'1'},{'record_id':'2'}],state,'stale-test')
    assert ok,msg
    assert not calls,calls
    assert '較新權威取代' in msg,msg
finally:
    gps.read_github_json=orig_read; gps.write_github_json=orig_write; gps._secret=orig_secret
print('PASS H8 stale records snapshot yields to newer remote authority')

# 4) Same-hash remote state prevents unnecessary 29MB re-upload.
orig_read=gps.read_github_json; orig_write=gps.write_github_json
try:
    state={'payload_hash':'samehash','revision':300,'count':1927}
    gps.read_github_json=lambda path,default:({'payload_hash':'samehash','revision':300,'count':1927},'mock') if path==gps.RECORDS_STATE_FILE else (default,'mock')
    calls=[]; gps.write_github_json=lambda *a,**k:(calls.append(a) or (True,'bad'))
    ok,msg=gps._sync_records_github_snapshot_v191_h8([{'record_id':'1'}],state,'same-test')
    assert ok and not calls,(msg,calls)
    assert '同Hash' in msg,msg
finally:
    gps.read_github_json=orig_read; gps.write_github_json=orig_write
print('PASS H8 same-hash remote state skips large records upload')

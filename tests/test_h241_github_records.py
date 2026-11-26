# -*- coding: utf-8 -*-
"""GitHub-only authority contract; offline real H127 serialization and CAS simulator."""
import copy, json, hashlib, base64, sys, types
import pytest
if 'streamlit' not in sys.modules:
    sys.modules['streamlit']=types.SimpleNamespace(secrets={})
import godpick_persistence_service as p
import godpick_h241_github_records as h
import godpick_h127_sharded_records as sh


def blob(obj):
    raw=json.dumps(obj,ensure_ascii=False,separators=(',',':')).encode()
    return hashlib.sha1(f'blob {len(raw)}\0'.encode()+raw).hexdigest(),raw

class Resp:
    def __init__(self,status,data=None):self.status_code=status;self._data=data;self.content=b'{}';self.text='mock'
    def json(self):return self._data

@pytest.fixture
def env(tmp_path,monkeypatch):
    monkeypatch.setattr(p,'BASE_DIR',tmp_path)
    monkeypatch.setattr(p,'github_config',lambda:{'token':'test','branch':'runtime-data','owner':'owner','repo':'repo'})
    monkeypatch.setattr(p,'firebase_configured',lambda:True)
    monkeypatch.setattr(p,'_init_firebase_app',lambda:pytest.fail('Firestore recommendation access'))
    monkeypatch.setattr(p,'_github_url',lambda path:path)
    rows=[{'record_id':f'id{i}','股票代號':str(1000+i),'推薦日期':'2026-11-25' if i>=1796 else '2026-07-08','推薦模式':'研究','更新時間':'原始','損益幅%':i/100,'私有欄位':f'保留{i}'} for i in range(2623)]
    head=p._new_state('fixture',rows,count=len(rows),latest_recommendation_date='2026-11-25');store={}; calls=[]; faults={}
    def get(path,default):
        calls.append(('read',path));return copy.deepcopy(store.get(path,default)), 'ok' if path in store else '404 not found'
    def seed(path,body):store[path]=copy.deepcopy(body);return True,'ok'
    ext,msg=sh.write_snapshot(rows,head,get_json=get,put_json=seed,json_safe=p._json_safe_value,record_hash_func=p._json_hash);assert ext,msg
    head.update(ext);store[p.RECORDS_STATE_FILE]=copy.deepcopy(head)
    def httpget(path,**kw):
        calls.append(('GET',path));
        if faults.get('read') or path not in store:return Resp(403 if faults.get('read') else 404)
        sha,raw=blob(store[path]);return Resp(200,{'sha':sha,'content':base64.b64encode(raw).decode()})
    def httpput(path,**kw):
        calls.append(('PUT',path));body=kw['json']
        if faults.get('write'):return Resp(403)
        obj=json.loads(base64.b64decode(body['content']))
        if path==p.RECORDS_STATE_FILE:
            if faults.get('race'):
                store[path]['revision']+=1;faults['race']=False
            if body.get('sha')!=blob(store[path])[0]:return Resp(409)
        elif path in store:return Resp(422)
        store[path]=obj
        if path==p.RECORDS_STATE_FILE and faults.get('ambiguous'):raise TimeoutError('lost response')
        return Resp(200)
    monkeypatch.setattr(p,'read_github_json',get);monkeypatch.setattr(p.requests,'get',httpget);monkeypatch.setattr(p.requests,'put',httpput)
    assert p.write_local_json_atomic(p.RECORDS_FILE,rows[:1796])[0]
    got,_,ok=p.ensure_records_local_authority_current();assert ok and got==rows
    calls.clear()
    return rows,head,store,calls,faults


def remove(rows,ids):
    # Simulate a projected table with only identity; other values MUST remain.
    view=[{'record_id':r['record_id']} for r in rows if r['record_id'] not in ids]
    return p.save_records_mutation_fast(view,expected_authority_signature=p.records_authority_signature(),deleted_ids=ids,previous_count=len(rows),reason='刪除勾選',require_remote_confirm=True)


def test_delete_ten_reboot_preserves_other_2613_exactly(env):
    rows,head,store,calls,_=env;ids={r['record_id'] for r in rows[-10:]};result=remove(rows,ids)
    assert result.permanent_ok and result.github_ok and result.local_ok and not result.firestore_ok
    remaining=[r for r in rows if r['record_id'] not in ids];assert len(remaining)==2613
    new=store[p.RECORDS_STATE_FILE];assert new['count']==2613 and new['parent_payload_hash']==head['payload_hash'] and len(new['h241_deleted_id_hashes'])==10
    assert p.read_local_json(p.RECORDS_FILE,[])[0]==remaining
    # Simulate reboot restoring repository JSON (old 1796) and missing local state.
    p.write_local_json_atomic(p.RECORDS_FILE,rows[:1796]);(p.BASE_DIR/p.RECORDS_STATE_FILE).unlink()
    got,_,ok=p.ensure_records_local_authority_current();assert ok and got==remaining
    assert sum(1 for op,path in calls if op=='PUT' and path==p.RECORDS_STATE_FILE)==1


def test_old_page10_upsert_does_not_resurrect_even_new_id(env):
    rows,head,store,calls,_=env;r=rows[-1];assert remove(rows,{r['record_id']}).permanent_ok
    rep,stats=p.upsert_records_authority_fast([r,dict(r,record_id='different')],reason='Page10 stale')
    assert rep.permanent_ok and stats['deleted_skipped']==2 and store[p.RECORDS_STATE_FILE]['count']==2622


def test_delete_rejects_unselected_omission(env):
    rows,head,store,calls,_=env;rep=p.save_records_mutation_fast(rows[:-11],deleted_ids=[rows[-1]['record_id']],previous_count=len(rows))
    assert not rep.permanent_ok and store[p.RECORDS_STATE_FILE]==head and not any(x[0]=='PUT' for x in calls)


def test_concurrent_head_cas_does_not_rebase(env):
    rows,head,store,calls,faults=env;faults['race']=True;rep=remove(rows,{rows[-1]['record_id']})
    assert not rep.permanent_ok and store[p.RECORDS_STATE_FILE]['payload_hash']==head['payload_hash']
    assert p.read_local_json(p.RECORDS_FILE,[])[0]==rows
    assert sum(1 for op,path in calls if op=='PUT' and path==p.RECORDS_STATE_FILE)==1


def test_network_failure_no_local_delete(env):
    rows,head,store,calls,faults=env;faults['write']=True
    assert not remove(rows,{rows[-1]['record_id']}).permanent_ok
    assert store[p.RECORDS_STATE_FILE]==head and p.read_local_json(p.RECORDS_FILE,[])[0]==rows


def test_ambiguous_http_response_readback_confirms(env):
    rows,_,store,_,faults=env;faults['ambiguous']=True
    assert remove(rows,{rows[-1]['record_id']}).permanent_ok and store[p.RECORDS_STATE_FILE]['count']==2622


def test_edit_only_patch_preserves_omitted_fields(env):
    rows,_,store,_,_=env;r=rows[-1];patch={'record_id':r['record_id'],'備註':'修改'}
    rep=p.save_records_mutation_fast(rows,upsert_rows=[patch],previous_count=len(rows),expected_authority_signature=p.records_authority_signature())
    assert rep.permanent_ok
    new=p.read_local_json(p.RECORDS_FILE,[])[0];assert new[:-1]==rows[:-1] and new[-1]==dict(r,備註='修改')


def test_full_sync_no_signature_cannot_change_existing(env):
    rows,_,_,calls,_=env;rep=p.save_records_sync_fast([*rows[:-1],dict(rows[-1],備註='old')]);assert not rep.permanent_ok and not any(x[0]=='PUT' for x in calls)


def test_stale_signature_rejected(env):
    rows,head,store,calls,_=env;rep=p.save_records_mutation_fast(rows[:-1],deleted_ids=[rows[-1]['record_id']],expected_authority_signature='stale');assert not rep.permanent_ok and store[p.RECORDS_STATE_FILE]==head


def test_firestore_legacy_entrypoints_never_access(env):
    rows,head,store,calls,_=env
    assert p._read_records_firestore_summary()[0]=={}
    assert p._read_records_firestore_full()[0]==[]
    assert not p.write_records_firestore(rows,head)[0]
    assert not p.write_records_firestore_mutation(rows,head)[0]
    assert not p._h176_repair_firestore_gap_for_verified_delete(rows,expected_local_hash=head['payload_hash'])[0]
    assert not p.h182_readonly_recommendation_replica_audit()[0]['valid']
    assert not p.write_github_json(p.RECORDS_STATE_FILE,head)[0]
    assert not p.schedule_records_github_sync(rows,head)[0]
    assert p.firebase_configured() # other modules still enabled


def test_delete_all_is_explicit_and_reboot_safe(env):
    rows,head,store,calls,_=env;rep=remove(rows,{r['record_id'] for r in rows});assert rep.permanent_ok
    assert store[p.RECORDS_STATE_FILE]['count']==0
    p.write_local_json_atomic(p.RECORDS_FILE,rows[:1796]);(p.BASE_DIR/p.RECORDS_STATE_FILE).unlink()
    got,_,ok=p.ensure_records_local_authority_current();assert got==[] and ok


def test_older_head_cannot_overwrite_verified_local(env):
    rows,head,store,calls,_=env;assert remove(rows,{rows[-1]['record_id']}).permanent_ok
    store[p.RECORDS_STATE_FILE]=head
    got,_,ok=p.ensure_records_local_authority_current();assert not ok and len(got)==2622 and p.records_authority_verification_h240()['status']=='BLOCKED'


def test_upsert_new_after_delete_carries_tombstones(env):
    rows,head,store,calls,_=env;assert remove(rows,{rows[-1]['record_id']}).permanent_ok
    new={'record_id':'new','股票代號':'1234','推薦日期':'2026-11-26','推薦模式':'研究'}
    rep,stats=p.upsert_records_authority_fast([new]);assert rep.permanent_ok and stats['added']==1 and store[p.RECORDS_STATE_FILE]['count']==2623
    assert h._id_hash(rows[-1]['record_id']) in store[p.RECORDS_STATE_FILE]['h241_deleted_id_hashes']

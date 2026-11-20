# -*- coding: utf-8 -*-
from godpick_h182_record_diff_audit import audit_replicas,succinct_reason
from godpick_h176_delete_reconcile import plan_missing_record_repair


def rows(n):
    return [dict(record_id=f'id{i:04}', 股票代號='1477', 推薦日期='2026-11-19',推薦模式='研究',推薦分數=i) for i in range(n)]


def test_2617_v_2560_local_added_columns_explains_old_block():
    base=rows(2617)
    for r in base:r['資料完整性狀態']='待補證'
    old=rows(2560)
    audit=audit_replicas(base,old,[r['record_id'] for r in old])
    assert audit['missing_remote_count']==57
    assert audit['classifications']=={'LOCAL_EXTRA_FIELDS_ONLY':2560}
    assert audit['changed_fields_top']==[]
    assert audit['destructive_action_authorized'] is False
    plan=plan_missing_record_repair(base,old,remote_doc_ids=[r['record_id'] for r in old])
    assert not plan['allowed']
    assert '僅本機新增欄位2560筆' in plan['reason']


def test_true_content_conflict_detected_by_field_name_not_repaired():
    local=rows(2);remote=rows(2);remote[0]['推薦分數']=-999
    z=plan_missing_record_repair(local,remote,remote_doc_ids=['id0000','id0001'])
    assert not z['allowed']
    assert z['audit']['classifications']['VALUE_CONFLICT']==1
    assert '推薦分數(1)' in z['reason']
    assert remote[0]['推薦分數']==-999


def test_transport_timestamp_never_trigger_conflict():
    local=rows(2);remote=[dict(r,updated_at='server Timestamp') for r in local]
    z=plan_missing_record_repair(local,remote,remote_doc_ids=['id0000','id0001'])
    assert z['allowed'] and z['missing_ids']==[]


def test_legacy_extra_field_never_silently_dropped():
    local=rows(1);remote=rows(1);remote[0]['管理者註記']='保留'
    audit=audit_replicas(local,remote,['id0000'])
    assert audit['classifications']=={'REMOTE_EXTRA_FIELDS_ONLY':1}
    assert audit['remote_added_fields_top'][0][0]=='管理者註記'
    assert not plan_missing_record_repair(local,remote,remote_doc_ids=['id0000'])['allowed']


def test_strict_selected_id_agrees_only_for_original_identity():
    local=rows(2);remote=rows(2);remote[0]['推薦分數']=-999
    a=audit_replicas(local,remote,['id0000','id0001'],['id0000'])
    assert a['selected_identity_verified']
    assert not a['destructive_action_authorized']
    remote[0]['推薦日期']='2026-11-18'
    b=audit_replicas(local,remote,['id0000','id0001'],['id0000'])
    assert not b['selected_identity_verified']
    assert '推薦日期' in b['selected_identity_issues']['id0000']


def test_missing_recommendation_date_blocks_selected_verification():
    local=rows(1);remote=rows(1);local[0]['推薦日期']='';remote[0]['推薦日期']=''
    z=audit_replicas(local,remote,['id0000'],['id0000'])
    assert not z['selected_identity_verified']


def test_remote_extra_id_and_duplicate_record_id_never_authorized():
    local=rows(2);remote=rows(3)
    z=audit_replicas(local,remote,[r['record_id'] for r in remote])
    assert z['extra_remote_count']==1 and not z['destructive_action_authorized']
    assert not audit_replicas(local,[remote[0],remote[0]],['id0000','id0000'])['valid']


def test_excludes_values_to_avoid_sensitive_leakage():
    local=rows(1);remote=rows(1);remote[0]['管理者私有備註']='password X123'
    z=audit_replicas(local,remote,['id0000'])
    s=str(z)
    assert 'password X123' not in s
    assert '管理者私有備註' in s


def test_no_mutation_even_when_selected():
    local=rows(2);remote=rows(2);x=[dict(r) for r in local];y=[dict(r) for r in remote]
    z=audit_replicas(local,remote,['id0000','id0001'],['id0000'])
    assert local==x and remote==y and not z['destructive_action_authorized']


def test_missing_selected_id_diagnostic_does_not_assume_deleted():
    z=audit_replicas(rows(2),rows(1),['id0000'],['id0001'])
    assert not z['selected_identity_verified']
    assert 'Firestore無對應文件' in z['selected_identity_issues']['id0001']


def test_live_manual_audit_reads_but_never_writes(monkeypatch):
    import godpick_persistence_service as p
    local=rows(3)
    remote=[dict(x) for x in local[:2]]
    for x in local:x['資料完整性狀態']='待核實'
    class Snapshot:
        def __init__(self,rid,body):self.id=rid;self._body=body;self.exists=True
        def to_dict(self):return dict(self._body)
    class Ref:
        def get(self):return Snapshot('godpick_records_summary',{'count':2,'payload_hash':'older','revision':10})
    class Collection:
        def __init__(self,name):self.name=name
        def stream(self):
            assert self.name=='godpick_records'
            return [Snapshot(r['record_id'],r) for r in remote]
        def document(self,name):
            assert self.name=='system'
            return Ref()
    class Database:
        def collection(self,name):return Collection(name)
    monkeypatch.setattr(p,'firebase_configured',lambda:True)
    monkeypatch.setattr(p,'_init_firebase_app',lambda:None)
    monkeypatch.setattr(p,'firestore',type('Firestore',(),{'client':staticmethod(lambda:Database())}))
    monkeypatch.setattr(p,'read_local_json',lambda *args:(local,'local',None))
    monkeypatch.setattr(p,'github_config',lambda:{'token':'fake'})
    monkeypatch.setattr(p,'read_github_json',lambda *args:({'count':3,'payload_hash':p._json_hash(local)},'state'))
    monkeypatch.setattr(p._H163_FS_GUARD,'quota_reason',lambda:'')
    monkeypatch.setattr(p._H163_FS_GUARD,'full_read_blocked',lambda *args:'')
    monkeypatch.setattr(p._H163_FS_GUARD,'note_full_success',lambda:None)
    report,msg=p.h182_readonly_recommendation_replica_audit(selected_ids=['id0000'])
    assert report['valid'] and report['remote_count']==2
    assert report['missing_remote_count']==1
    assert report['classifications']['LOCAL_EXTRA_FIELDS_ONLY']==2
    assert report['github_state_matches_local_metadata']
    assert not report['full_github_snapshot_verified']
    assert not report['destructive_action_authorized']
    assert '沒有變更任何紀錄' in msg

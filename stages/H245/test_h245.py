import copy
from godpick_h245_list_authority import *

ROW={'record_id':'r1','股票代號':'2330','市場別':'上市','推薦日期':'2026-07-09'}

def state(data,rev):return {'revision':rev,'payload_hash':digest(data)}

def source(latest, lrev=20, rrev=10, actual=None):
    actual=[] if actual is None else actual
    data={LIST:actual,state_name(LIST):state(actual,lrev),state_name(LATEST):state(latest,rrev)}
    def read(k,default):return copy.deepcopy(data.get(k,default)),'mock'
    return data,read

def test_verified_empty_is_no_work():
    latest={'recommendations':[],'recommendation_count':0};_,read=source(latest)
    assert audit_empty_list([],latest,read)['code']=='NO_CURRENT_LIST'

def test_old_57_snapshot_is_quarantined():
    latest={'recommendations':[{'股票代號':str(1000+i)} for i in range(57)]};_,read=source(latest)
    a=audit_empty_list([],latest,read);assert a['code']=='QUARANTINED' and a['snapshot_rows']==57

def test_remote_nonempty_cannot_certify_empty():
    latest={'recommendations':[]};_,read=source(latest,actual=[ROW])
    assert audit_empty_list([],latest,read)['code']=='UNVERIFIED'

def test_unavailable_not_empty():
    latest={'recommendations':[]};assert audit_empty_list([],latest,lambda k,d:(d,'timeout'))['code']=='UNVERIFIED'

def test_hash_mismatch():
    latest={'recommendations':[]};d,read=source(latest);d[state_name(LATEST)]['payload_hash']='x'
    assert audit_empty_list([],latest,read)['code']=='UNVERIFIED'

def test_race():
    latest={'recommendations':[]};d,read=source(latest);calls={}
    def race(k,default):
        calls[k]=calls.get(k,0)+1
        if k==state_name(LIST) and calls[k]==2:d[k]['revision']+=1
        return read(k,default)
    assert audit_empty_list([],latest,race)['code']=='UNVERIFIED'

def test_declared_count_mismatch():
    latest={'recommendations':[],'recommendation_count':57};_,read=source(latest)
    assert audit_empty_list([],latest,read)['code']=='QUARANTINED'

def test_newer_snapshot_missing_id():
    latest={'recommendations':[{'股票代號':'2330'}]};_,read=source(latest,1,2)
    assert audit_empty_list([],latest,read)['code']=='QUARANTINED'

def test_newer_verified_snapshot_still_review_not_write():
    latest={'recommendations':[ROW]};_,read=source(latest,1,2)
    assert audit_empty_list([],latest,read,lambda:[ROW])['code']=='RECOVERY_REVIEW'

def test_deleted_id_not_revived():
    latest={'recommendations':[ROW]};_,read=source(latest,1,2)
    assert audit_empty_list([],latest,read,lambda:[])['code']=='QUARANTINED'

def test_same_code_wrong_id_not_evidence():
    r={**ROW,'record_id':'wrong','推薦日期':''}
    assert plan_dates([r],[ROW])[0]['status']=='NO_AUTHORITY_MATCH'

def test_exact_id_proposal_preserves_source():
    r={**ROW,'推薦日期':''};before=copy.deepcopy(r)
    a=plan_dates([r],[ROW])[0]
    assert a['status']=='DATE_EVIDENCE_AVAILABLE' and a['proposed_date']==ROW['推薦日期'] and r==before

def test_duplicate_id_both_directions():
    r={**ROW,'推薦日期':''}
    assert plan_dates([r],[ROW,ROW])[0]['status']=='DUPLICATE_ID'
    assert all(a['status']=='DUPLICATE_ID' for a in plan_dates([r,r],[ROW]))

def test_no_identity_no_guess():
    r={**ROW,'record_id':'','推薦日期':''}
    assert plan_dates([r],[ROW])[0]['status']=='MISSING_IDENTITY'

def test_market_conflict():
    assert plan_dates([{**ROW,'市場別':'上櫃','推薦日期':''}],[ROW])[0]['status']=='IDENTITY_CONFLICT'

def test_date_conflict():
    assert plan_dates([{**ROW,'推薦日期':'2026-07-10'}],[ROW])[0]['status']=='DATE_CONFLICT'

def test_clock_only_not_overwritten():
    assert plan_dates([{**ROW,'推薦日期':'10:06'}],[ROW])[0]['status']=='INVALID_EXPLICIT_DATE'

def test_unknown_snapshot_shape_not_success():
    latest={'data':[]};_,read=source(latest)
    assert audit_empty_list([],latest,read)['code']=='UNVERIFIED'

def test_unverified_history():
    latest={'recommendations':[ROW]};_,read=source(latest,1,2)
    assert audit_empty_list([],latest,read,lambda:None)['code']=='UNVERIFIED'


def test_overlay_rejects_missing_anchors():
    import pytest
    from apply_h245 import transform
    with pytest.raises(ValueError):transform('pass\n')


def test_overlay_rejects_duplicate_anchor():
    import pytest
    from apply_h245 import transform,PATCHES
    with pytest.raises(ValueError):transform(PATCHES[0][0]+'\n'+PATCHES[0][0])


def test_overlay_guard_behavior_from_generated_source():
    import ast
    from apply_h245 import PATCHES
    old,new=PATCHES[1]
    # Exact generated function with the original tail replaced by a stub.
    source=new+'\n    return None\n'
    ast.parse(source)
    def report(ok,message,**kw):return {'ok':ok,'message':message,**kw}
    class Frame:empty=True
    ns={'_report':report};exec(source,ns)
    for code in ('NO_CURRENT_LIST','QUARANTINED','UNVERIFIED','RECOVERY_REVIEW'):
        r=ns['_h243_identity_guard'](Frame(),[PREFIX+code+'｜test'],0)
        assert r['warning']==(code!='NO_CURRENT_LIST') and not r['retry_pending']
        assert r['details']['outcome']==code

from datetime import datetime,timedelta
from pathlib import Path
import io,json,subprocess,sys
import pytest
import godpick_h237_pipeline_acceptance as h
from godpick_h190_market_archive import _write,_seal,_read
from test_h222_financial_history import NOW,M,monthly,Response
from test_h236_cloud_financial import Store,roots
from test_h235_official_batch import collect,Q, NOW as BATCH_NOW
import godpick_h236_cloud_financial as cloud
import godpick_h222_financial_history as hist
from test_h194_performance import setup_market,result,NOW as SIGNAL_NOW


def entry(r,name):return next(x for x in r['checks'] if x['項目']==name)
def row(state='VERIFIED',n=42):return {'來源':'revenue','市場':'TWSE','期間':'202610','狀態':state,'有效筆數':n,'必要範圍':True,'原因':''}
def claim(n=42,state='VERIFIED'):return {'kind':'revenue','market':'TWSE','period':'202610','rows':n,'status':state}

@pytest.mark.parametrize('state,n,claimed,expected', [('VERIFIED',42,[claim()],'MATCH'),('MISSING',None,[claim()],'CLAIM_NOT_REVERIFIED'),('VERIFIED',42,[claim(41)],'COUNT_MISMATCH'),('VERIFIED',42,[],'READER_VERIFIED_ONLY'),('INVALID',None,[claim(state='REJECTED')],'PENDING'),('VERIFIED',42,[claim(),claim()],'DUPLICATE_CLAIM')])
def test_reconciliation(state,n,claimed,expected):
    assert h.reconcile_periods([row(state,n)],{'scope':claimed})[0]['對帳']==expected

@pytest.mark.parametrize('state',['MISSING','INVALID','INVALID_TIME','NOT_YET_OBSERVED','SEALED_CLAIM'])
def test_health_visibility(tmp_path,state):
    p=tmp_path/'health.json'
    if state=='INVALID':p.write_text('{}')
    elif state!='MISSING':_write(p,_seal({'asof': 'bad' if state=='INVALID_TIME' else (NOW+timedelta(days=state=='NOT_YET_OBSERVED')).isoformat()}))
    assert h.health(tmp_path,'health.json','asof',NOW)[1]==state

def test_raw_partition_not_health_claim(tmp_path,monkeypatch):
    monkeypatch.setattr(hist,'LIMITS',{'TWSE':1,'TPEX':1})
    hist.collect(tmp_path,now=NOW,budget=1,http=lambda method,url,**kw:Response(url,monthly()))
    r=h.run(tmp_path,NOW);v=entry(r,'歷史彙總財報原始重驗');assert v['有效']==v['收集器宣稱']==1 and v['對帳差異']==0
    p=hist.partition(tmp_path,'revenue','TWSE',M);p.unlink()
    v=entry(h.run(tmp_path,NOW),'歷史彙總財報原始重驗');assert v['有效']==0 and v['收集器宣稱']==1 and v['對帳差異']==1

def test_no_network_or_authority_default(tmp_path,monkeypatch):
    monkeypatch.setattr(cloud,'GCS',lambda:pytest.fail('must not connect'))
    r=h.run(tmp_path,NOW);assert r['status']=='EVIDENCE_PENDING' and r['weight_multiplier']==1 and not r['trade_authority_changed']
    assert len(r['periods'])==64 and len(r['local_batch'])==8 and not r['first']
    assert entry(r,'H236雲端持久儲存')['狀態']=='NOT_REQUESTED'
    assert not list(tmp_path.rglob('*'))

def test_stage_failure_preserves_report(tmp_path,monkeypatch):
    def error(*a,**k):raise ValueError('SECRET URL example')
    monkeypatch.setattr(hist,'inspect',error)
    r=h.run(tmp_path,NOW);assert entry(r,'具名高成長財報初步複核')['狀態']=='ERROR';assert all(x['狀態']=='NOT_RECONCILED' for x in r['local_batch'])
    assert len(r['periods'])==64 and 'SECRET' not in json.dumps(r) and r['errors'][0]['錯誤類型']=='ValueError'

def test_sealed_ledger_comparison_time(tmp_path):
    p=h.perf;m=setup_market(tmp_path);body,_=p.freeze(result(),m,p.models({}, {},SIGNAL_NOW),SIGNAL_NOW)
    p.immutable(tmp_path/'data/h194/signals/2026-11-23.json.gz',body)
    v=h.ledger_check(tmp_path,SIGNAL_NOW,{'asof':(SIGNAL_NOW-timedelta(seconds=1)).isoformat(),'signals':0,'mature':0,'pending':0});assert v['封存訊號']==1 and v['健康封存對帳']=='MATCH_AT_HEALTH_TIME'
    v=h.ledger_check(tmp_path,SIGNAL_NOW,{'asof':SIGNAL_NOW.isoformat(),'signals':0,'mature':0,'pending':0});assert v['健康封存對帳']=='CLAIM_LEDGER_MISMATCH' and v['首批研究日']=='2026-11-23'
    assert h.ledger_check(tmp_path,SIGNAL_NOW-timedelta(seconds=1))['首批研究']==0

def test_cloud_metadata_not_zip_download(tmp_path,monkeypatch):
    a,b=roots(tmp_path,monkeypatch);collect(a);s=Store();cloud.publish(a,Q,s,BATCH_NOW);s.reads.clear()
    x=h.cloud_audit(b,BATCH_NOW,'2026Q3',s)[0];assert x['雲端狀態']=='CLOUD_MANIFEST_PRESENT' and x['本機原始重驗']=='MISSING' and x['同雜湊及首次時間'] is None
    assert s.reads==[cloud.manifest_key(Q)]
    x=h.cloud_audit(a,BATCH_NOW,'2026Q3',s)[0];assert x['本機原始重驗']=='VERIFIED' and x['同雜湊及首次時間'] is True
    assert all(k.endswith('manifest.json') for k in s.reads)

def test_invalid_cloud_plan(tmp_path):
    with pytest.raises(ValueError):h.cloud_audit(tmp_path,NOW,'2026Q3,2026Q3',Store())

def test_malformed_claim_is_not_success(tmp_path):
    _write(tmp_path/'data/h222/health.json',_seal({'asof':NOW.isoformat(),'scope':None}))
    r=h.run(tmp_path,NOW);assert entry(r,'歷史彙總財報原始重驗')['健康驗證']=='INVALID_SCHEMA'

def test_full_result_time_mismatch(tmp_path):
    with pytest.raises(ValueError,match='observation time'):h.run(tmp_path,NOW,base={'asof':(NOW-timedelta(days=1)).isoformat()},acceptance={'asof':NOW.isoformat()})

def test_excel_and_sealed_compact(tmp_path):
    from openpyxl import load_workbook
    from tools.h237_acceptance import write
    r=h.run(tmp_path,NOW);out=tmp_path/'out';write(r,out);w=load_workbook(out/'H237_PipelineAcceptance.xlsx');assert len(w.sheetnames)==10
    compact=_read(out/'health.json');assert compact['status']=='EVIDENCE_PENDING' and 'candidates' not in compact and compact['financial_candidate_count']==0

def test_workflow_fallback_is_read_only_and_no_new_schedule():
    import yaml
    root=Path(__file__).resolve().parents[1]
    for file in ('h222_financial_history.yml','h224_growth_resonance.yml'):
        doc=yaml.safe_load((root/'.github/workflows'/file).read_text());steps=next(iter(doc['jobs'].values()))['steps'];step=next(x for x in steps if 'H237' in x.get('name',''));assert 'always()' in step['if'] and '--report-only' in step['run'] and '--json-only' in step['run'] and '--cloud-check' not in step['run']

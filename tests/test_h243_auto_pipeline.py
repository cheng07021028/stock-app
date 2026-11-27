# -*- coding: utf-8 -*-
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import base64,copy,json
import pandas as pd
import pytest
import godpick_h243_taifex_web as w
import godpick_h158_taifex_openapi as api
import godpick_auto_scheduler as s
import godpick_auto_update_tasks as t
import godpick_h243_tracking_evidence as ev
from tools import h243_wake_receipt as wake
from godpick_headless_page_loader import load_page_namespace
TZ=ZoneInfo('Asia/Taipei'); NOW=datetime(2026,11,26,22,39,tzinfo=TZ); DAY='2026-11-26'
PCR='''<h2>臺指選擇權(TXO)Put/Call比</h2><table><tr><th>日期</th><th>賣權成交量</th><th>買權成交量</th><th>買賣權成交量比率%</th><th>賣權未平倉量</th><th>買權未平倉量</th><th>買賣權未平倉量比率%</th></tr><tr><td>2026/11/26</td><td>151782</td><td>176676</td><td>85.91</td><td>73367</td><td>139680</td><td>52.53</td></tr></table>'''
FUT='''<p>日期：2026/11/26</p><table><thead><tr><th rowspan="3">商品名稱</th><th rowspan="3">身份別</th><th colspan="6">未平倉餘額</th></tr><tr><th colspan="2">多方</th><th colspan="2">空方</th><th colspan="2">多空淨額</th></tr><tr><th>口數</th><th>契約金額</th><th>口數</th><th>契約金額</th><th>口數</th><th>契約金額</th></tr></thead><tbody><tr><td>臺股期貨</td><td>外資</td><td>9083</td><td>1</td><td>115358</td><td>2</td><td>-106275</td><td>-1</td></tr></tbody></table>'''
def test_pcr_official_numbers():
    assert w.parse_pcr_html(PCR,DAY)=={'TAIFEX_PCR資料日期':DAY,'PCR成交量比%':85.91,'PCR未平倉量比%':52.53}
def test_tx_official_numbers():
    assert w.parse_futures_html(FUT,DAY)=={'TAIFEX期貨資料日期':DAY,'外資臺指期未平倉淨口數':-106275}
@pytest.mark.parametrize('html,parser',[(PCR.replace('85.91','1.01'),w.parse_pcr_html),(PCR.replace('2026/11/26','2026/11/25'),w.parse_pcr_html),(FUT.replace('臺股期貨','小型臺指期貨'),w.parse_futures_html),(FUT.replace('-106275','-106274'),w.parse_futures_html),(FUT.replace('2026/11/26','2026/11/25'),w.parse_futures_html)])
def test_wrong_evidence_rejected(html,parser):assert not parser(html,DAY)
class Resp:
    status_code=200
    def __init__(self,h):self.text=h;self.content=h.encode()
    def raise_for_status(self):pass

def test_fallback_replaces_stale_groups():
    data={'TAIFEX_PCR資料日期':'2026-11-25','PCR成交量比%':84.13,'PCR未平倉量比%':39.84,'TAIFEX期貨資料日期':'2026-11-25','外資臺指期未平倉淨口數':-105709,'來源':'TAIFEX_OPENAPI_VERIFIED'}
    got,msg=w.fallback(data,get=lambda url,**kw:Resp(PCR if url==w.PCR_URL else FUT),now=NOW)
    assert api.complete(got,NOW) and got['來源']=='TAIFEX_OFFICIAL_DUAL_VERIFIED' and got['外資臺指期未平倉淨口數']==-106275
    assert data['TAIFEX_PCR資料日期']=='2026-11-25'

def test_partial_fallback_not_certified():
    got,_=w.fallback({},get=lambda url,**kw:Resp(PCR if url==w.PCR_URL else ''),now=NOW)
    assert not api.complete(got,NOW)

def test_fresh_api_no_extra_request():
    data={**w.parse_pcr_html(PCR,DAY),**w.parse_futures_html(FUT,DAY),'來源':'TAIFEX_OPENAPI_VERIFIED'}
    got,_=w.fallback(data,get=lambda *a,**kw:pytest.fail('unneeded request'),now=NOW)
    assert got==data

def test_warning_completion_reopened_after_cooldown():
    slot=NOW.replace(hour=20,minute=15);key=s._run_key('super_ai_context',slot)
    status={'completed_run_keys':[key],'jobs':{'super_ai_context':{'last_status':'WARNING','last_slot':'2026-11-26 22:35','last_run_at':'2026-11-26 22:35:00'}}}
    assert not s._already_done(status,key)
    assert not s._h243_due_allowed(status,'super_ai_context',slot,NOW)
    assert s._h243_due_allowed(status,'super_ai_context',slot,NOW+timedelta(minutes=20))
    status['jobs']['super_ai_context']['last_status']='SUCCESS'
    assert s._already_done(status,key)

def test_nonactionable_warning_not_retryable():
    slot=NOW.replace(hour=21,minute=25);key=s._run_key('recommend_list_hits',slot)
    status={'completed_run_keys':[key],'jobs':{'recommend_list_hits':{'last_status':'WARNING','last_slot':'2026-11-26 21:25','retry_pending':False}}}
    assert s._already_done(status,key)
    status['jobs']['recommend_list_hits']['retry_pending']=True
    assert not s._already_done(status,key)

def test_identity_date_missing_no_fabrication():
    rows=pd.DataFrame([{'record_id':None,'股票代號':'2330','市場別':'上市','推薦日期':pd.NA,'更新時間':DAY}])
    got,n=ev.repair(rows,[{'record_id':'x','股票代號':'2330','市場別':'上市','推薦日期':'2026-07-09'}])
    assert n==0 and not ev.eligible(got.iloc[0]) and not ev.row_date(got.iloc[0])

def test_exact_unique_id_date_repair():
    a={'record_id':'r1','股票代號':'2330','市場別':'上市','推薦日期':''}; b={**a,'推薦日期':'2026-07-09'}
    got,n=ev.repair(pd.DataFrame([a]),[b]);assert n==1 and ev.eligible(got.iloc[0])
    assert ev.repair(pd.DataFrame([a]),[b,b])[1]==0
    assert ev.repair(pd.DataFrame([a]),[{**b,'市場別':'上櫃'}])[1]==0

def test_57_rows_guard_preserves_data():
    df=pd.DataFrame([{'股票代號':str(i+1000),'推薦日期':'','record_id':None,'市場別':'上市'} for i in range(57)])
    original=df.copy(deep=True);result=t._h243_identity_guard(df,[],0)
    assert result['ok'] and result['warning'] and result['details']['quarantined']==57
    pd.testing.assert_frame_equal(df,original)

def test_page10_invalid_cannot_sync():
    ns=load_page_namespace('pages/10_推薦清單.py',base_dir=Path(__file__).resolve().parents[1]);ns['upsert_records_authority_fast']=lambda *a,**kw:pytest.fail('invalid row written');ns['save_named_json_permanent']=lambda *a,**kw:pytest.fail('invalid row saved')
    ok,msg=ns['_sync_records'](pd.DataFrame([{'股票代號':'2330','推薦日期':'','record_id':None}]))
    assert not ok and any('隔離' in m for m in msg)

def test_receipt_cas_no_retry():
    env={'GITHUB_RUN_ID':'20','GITHUB_RUN_ATTEMPT':'1','GITHUB_REPOSITORY':'a/b','GITHUB_TOKEN':'test'};new=wake.make('FINISH',env);old={**new,'run_id':'19'};calls=[]
    def http(method,url,body=None):
        calls.append((method,body));return (200,{'sha':'parent','content':base64.b64encode(json.dumps(old).encode()).decode()}) if method=='GET' else (409,{})
    assert not wake.publish(new,env,request=http)[0] and len(calls)==2 and calls[1][1]['sha']=='parent'
    assert not wake.can_publish(new,{**new,'phase':'START'})
    assert not wake.can_publish(new,{**new,'run_id':'19'})

def test_official_web_passes_existing_readiness_gate():
    from godpick_h166_operational_readiness import context_evidence
    tai={**w.parse_pcr_html(PCR,DAY),**w.parse_futures_html(FUT,DAY),'來源':'TAIFEX_OFFICIAL_DUAL_VERIFIED','H243來源':{'TAIFEX_PCR資料日期':'OFFICIAL_HTML','TAIFEX期貨資料日期':'OFFICIAL_HTML'}}
    ctx={'market_margin':{'TWSE融資券資料日期':DAY},'taifex':tai,'margin_by_stock':{str(i):{} for i in range(100)},'source_coverage':{'TPEx融資券檔數':30}}
    assert context_evidence(ctx,NOW)['verified']
    ctx['taifex']['TAIFEX期貨資料日期']='2026-11-25';assert not context_evidence(ctx,NOW)['verified']

def test_failed_control_plane_restore_rejected(tmp_path,monkeypatch):
    from tools import restore_runtime_data_v191 as r
    import subprocess
    monkeypatch.setattr(r,'ROOT',tmp_path)
    monkeypatch.setattr(r,'RUNTIME_ARTIFACTS',['data/config/godpick_auto_scheduler_settings.json','godpick_auto_scheduler_status.json','godpick_auto_scheduler_history.json','godpick_records.json'])
    replies={'data/config/godpick_auto_scheduler_settings.json':b'{"enabled":true,"jobs":{}}','godpick_auto_scheduler_status.json':b'{}','godpick_auto_scheduler_history.json':b'{"records":[]}','godpick_records.json':b'[]'}
    (tmp_path/'godpick_records.json').write_text('[{"record_id":"keep"}]')
    def git(args,**kw):return subprocess.CompletedProcess(args,0,stdout=replies[args[-1].split(':',1)[1]])
    monkeypatch.setattr(r.subprocess,'run',git)
    with pytest.raises(SystemExit,match='control authority restore failed'):r.main()
    assert json.loads((tmp_path/'godpick_records.json').read_text())==[{'record_id':'keep'}]

def test_hit_worker_quarantines_no_id_even_with_date():
    ns=load_page_namespace('pages/10_推薦清單.py',base_dir=Path(__file__).resolve().parents[1]);ns['_calc_night_hit_for_row_v101']=lambda *a,**kw:pytest.fail('unidentified row provider request')
    df=pd.DataFrame([{'股票代號':'2330','市場別':'上市','推薦日期':'2026-07-09','record_id':None}]);out,meta=ns['_update_night_hit_tracking_v101'](df,show_progress=False,time_budget_seconds=20,require_identity=True)
    assert meta['missing_identity']==1 and meta['processed']==0 and meta['success']==0 and len(out)==1

def test_core_warning_gate_never_accepts_stale_data():
    cfg=copy.deepcopy(s.DEFAULT_SETTINGS['jobs']['godpick_recommendation']);status={'jobs':{x:{'last_status':'SUCCESS','last_success_at':DAY+' 21:00:00'} for x in cfg['dependencies']}}
    status['jobs']['super_ai_context']['last_status']='WARNING'
    assert not s._dependency_check(cfg,status,NOW)[0]
    status['jobs']['super_ai_context']['last_status']='SUCCESS'
    assert s._dependency_check(cfg,status,NOW)[0]

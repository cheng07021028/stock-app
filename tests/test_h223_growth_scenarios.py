from datetime import datetime,timedelta
from pathlib import Path
import hashlib,io
import pytest
import godpick_h223_growth_scenarios as h
import godpick_h222_financial_history as hist
import godpick_h192_financial_growth as fin
NOW=datetime.fromisoformat('2026-11-26T19:00:00+08:00');M,Q=fin.expected_periods(NOW)

def rev(end=M):
    return {p:{'revenue_twd':(100+(p-end+17)*5)*1000,'available_at':NOW.isoformat(),'source_url':'test'} for p in range(end-17,end+1)}

def test_forecast_formula():
    rr,reason=h.scenarios(rev(),NOW,M);assert reason=='SCENARIO_AVAILABLE' and len(rr)==3
    r=rr[0];assert r['目標年月']=='202611' and r['保守推估元']<=r['基準推估元']<=r['樂觀推估元'] and len(r['18月證據雜湊'])==64
    assert r['去年同期實際元']==rev()[M-11]['revenue_twd']

@pytest.mark.parametrize('change,expected',[(lambda d:d.pop(M-5),'ACTUAL_18_MONTHS_PENDING'),(lambda d:d[M].update(previous_month_twd=1),'COMPARATIVE_CONFLICT'),(lambda d:d[M].update(available_at=(NOW+timedelta(seconds=1)).isoformat()),'OBSERVATION_TIME_PENDING'),(lambda d:d[M].update(revenue_twd=0),'ACTUAL_18_MONTHS_PENDING')])
def test_invalid_evidence(change,expected):
    d=rev();change(d);assert h.scenarios(d,NOW,M)==([],expected)

def test_stale_and_no_backfill():
    assert h.scenarios(rev(M-1),NOW,M)[1]=='STALE_OR_MISSING'
    r,_=h.scenarios(rev(M-1),NOW,M-1);assert all(x['目標年月']>'202610' for x in r)

def test_rank_never_promotes_forecast_only():
    p={'市場':'TWSE','股票代號':'2330','股票名稱':'測試','產業':'半導體','連3月成長':False,'近13月新高':False}
    r,_=h.scenarios(rev(),NOW,M);assert not h.candidates([p],[{**p,**x} for x in r],{})

def profile(code='2330'):
    return {'市場':'TWSE','股票代號':code,'股票名稱':'測試','產業':'半導體','營收年月':'202610','營收年增%':20,'連3月成長':True,'近13月新高':True,'連兩季三率年增':True,'獲利突破':True,'TTM EPS重建':8,'獲利率為正':True,'EPS季度':'2026Q3','三率季度':'2026Q3','官方PE':12,'PE同交易日':True,'研究池':'低PE高TTM EPS'}

def test_rank_confirmed_first_and_eps_caveat():
    a,b=profile('1001'),profile('1002');r=h.candidates([a,b],[],{('TWSE','1002'):{'H198階段':'初期','籌碼分組':'已核'}})
    assert [x['股票代號'] for x in r]==['1002','1001'] and r[0]['分市場研究順位']==1 and '未獨立核實' in r[0]['EPS可比性']
    a['EPS季度']='2026Q2';assert not h.candidates([a],[],{})

def body(ts=NOW):
    rows,_=h.scenarios(rev(),NOW,M);return {'version':h.VERSION,'created_at':ts.isoformat(),'day':ts.date().isoformat(),'forecasts':[{'市場':'TWSE','股票代號':'2330',**x} for x in rows],'ranked':[],'trade_authority_changed':False}

def test_snapshot_read_and_immutable(tmp_path):
    p=tmp_path/h.REL/'2026-11-26.json.gz';hist._persist(p,body());assert len(h.read_history(tmp_path,NOW))==1
    assert not h.read_history(tmp_path,NOW-timedelta(seconds=1))
    with pytest.raises(FileExistsError):hist._persist(p,body())

def test_outcome_pending_and_dedupe():
    v=body();summary,rows=h.outcomes([v,v],{},NOW);assert len(rows)==3 and summary[0]['有效實際']==0 and summary[0]['平均模型誤差率%'] is None

def test_outcome_real_and_baseline():
    v=body();r=v['forecasts'][0];t=datetime.fromisoformat('2026-12-10T10:00:00+08:00');a={'revenue_twd':r['基準推估元'],'available_at':t.isoformat()};summary,rows=h.outcomes([v],{('revenue','TWSE','2330'):{M+1:a}},t)
    assert rows[0]['狀態']=='VERIFIED_ACTUAL' and rows[0]['模型絕對誤差率%']==0 and summary[0]['平均模型誤差率%']==0

def test_outcome_reject_premature_actual():
    v=body();summary,rows=h.outcomes([v],{('revenue','TWSE','2330'):{M+1:{'revenue_twd':100,'available_at':NOW.isoformat()}}},NOW+timedelta(days=15));assert rows[0]['狀態']=='AWAITING_OFFICIAL_ACTUAL' and summary[0]['平均模型誤差率%'] is None

def test_audit_missing_scope(tmp_path):
    rows=h.audit(tmp_path,NOW);assert len(rows)==64 and all(x['狀態']=='MISSING' for x in rows)

def test_audit_reparse_mismatch(tmp_path,monkeypatch):
    from test_h222_financial_history import monthly
    monkeypatch.setattr(hist,'LIMITS',{'TWSE':1,'TPEX':1});html=monthly(M);rr=hist.parse('revenue','TWSE',M,html,NOW);rr[0]['revenue_twd']+=1
    hist._persist(hist.partition(tmp_path,'revenue','TWSE',M),{'version':hist.VERSION,'kind':'revenue','market':'TWSE','period':M,'url':hist.source_url('revenue','TWSE',M),'observed_at':NOW.isoformat(),'html':html,'raw_sha256':hashlib.sha256(html.encode()).hexdigest(),'rows':rr})
    assert h.audit(tmp_path,NOW)[0]['狀態']=='INVALID'

def test_diagnostic_no_data_rows():
    from test_h222_financial_history import monthly
    d=hist.response_diagnostic(monthly(M));assert d['table_count']==1 and d['headers'] and '1200' not in str(d['headers'])

def test_excel():
    from openpyxl import load_workbook
    r={'day':'','status':'EVIDENCE_PENDING','capture':'READ_ONLY','counts':{},'scope':[],'ranked':[],'forecast':[],'audit':[],'feedback':[],'outcomes':[],'errors':[],'conflicts':{}}
    w=load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==8

def test_zero_actual_distinct_from_missing():
    v=body();t=NOW+timedelta(days=15);summary,rows=h.outcomes([v],{('revenue','TWSE','2330'):{M+1:{'revenue_twd':0,'available_at':t.isoformat()}}},t)
    assert rows[0]['狀態']=='ACTUAL_ZERO_PERCENT_ERROR_UNDEFINED' and rows[0]['實際營收元']==0 and summary[0]['零營收誤差不可計']==1 and summary[0]['平均模型誤差率%'] is None

from datetime import datetime
from pathlib import Path
import io,json
import pandas as pd
import pytest
import godpick_h193_joint_decision as h
D='2026-11-23'; NOW=datetime(2026,11,24,14,0,tzinfo=h.archive.TZ)

def inputs(tp='技術突破',fp='營收成長',**kw):
    t={'asof':D,'calendar_fresh':True,'status':'PARTIAL_RESEARCH','archive_errors':[],'diagnostic':[{'市場':'TWSE','股票代號':'1101','流動性通過':True,'資料日期':D,'研究池':tp}]}
    f={'asof':NOW.isoformat(),'market_day':D,'profiles':[{'市場':'TWSE','股票代號':'1101','研究池':fp,'營收年增%':20,'營收月增%':12}],'evidence':[]}
    return t,f

def test_dual():
    r=h.combine(*inputs());assert r['counts'][h.TIERS[0]]==1;assert not r['trade_authority_changed'];assert r['audit'][0]['交易狀態']=='僅研究／未授權'
@pytest.mark.parametrize('pool',['單月營收觀察','月增待年增補證','H191財技共振'])
def test_no_double_count(pool):
    r=h.combine(*inputs(fp=pool));assert r['counts'][h.TIERS[0]]==0;assert r['counts'][h.TIERS[1]]==1

def test_missing_history_does_not_erase_financial():
    t,f=inputs();t['status']='DATA_PENDING';r=h.combine(t,f);assert r['counts'][h.TIERS[2]]==1 and not r['same_session']

def test_market_date_mismatch():
    t,f=inputs();f['market_day']='2026-11-20';r=h.combine(t,f);assert r['counts'][h.TIERS[0]]==0

def test_partial_market_blocks_mainstream():
    r=h.combine(*inputs(tp='主流強勢'));assert r['counts'][h.TIERS[2]]==1

def test_stale_diagnostic():
    t,f=inputs();t['diagnostic'][0]['資料日期']='2026-11-20';r=h.combine(t,f);assert r['counts'][h.TIERS[2]]==1

def test_duplicate_identity_not_promoted():
    t,f=inputs();t['diagnostic']*=2;r=h.combine(t,f);assert r['counts'][h.TIERS[0]]==0 and r['duplicate_identities']

def test_no_join_by_code_only():
    t,f=inputs();f['profiles'][0]['市場']='TPEX';r=h.combine(t,f);assert r['counts'][h.TIERS[0]]==0 and len(r['audit'])==2

def test_full_membership_without_top_lists():
    t,f=inputs();t['pools']={};f['pools']={};r=h.combine(t,f);assert r['counts'][h.TIERS[0]]==1

def test_etf_not_common_stock():
    t,f=inputs();t['diagnostic'][0]['股票代號']='0050';f['profiles'][0]['股票代號']='0050';assert not h.combine(t,f)['audit']

def test_unknown_missing_not_zero(tmp_path):
    t,f=inputs(tp='',fp='月增待年增補證');r=h.combine(t,f);o=h.detail(r,f,'TWSE','1101',root=tmp_path,now=NOW)
    assert set(o['roles'])==set(h.lab.ROLES)
    for i in ('04','05','06','10'):assert not o['roles'][i]['metrics']
    assert o['roles']['01']['status']=='待補證'
    assert not o['trade_authority_changed']
    assert '勝率%' not in o['roles']['04']['metrics']
    assert h.excel(r,o).startswith(b'PK')

def test_formula_escaped():
    t,f=inputs();f['profiles'][0]['股票名稱']='=1+1';r=h.combine(t,f)
    import openpyxl
    w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));assert all(c.data_type!='f' for sheet in w for row in sheet for c in row)

def test_event_observation_future(monkeypatch,tmp_path):
    monkeypatch.setattr(h.sources,'load_source',lambda *a,**k:{'observed_at':'2026-11-24T15:00:00+08:00','rows':[{'code':'1101'}]})
    assert not h._events(tmp_path,'1101','TWSE',NOW)

def test_empty_root(tmp_path):
    r,f=h.run(tmp_path,now=NOW);assert r['status']=='NO_MATCH_OR_EVIDENCE_PENDING';assert not any(r['counts'].values())

def test_single_research_pending():
    t,f=inputs(tp='',fp='月增待年增補證');assert h.combine(t,f)['counts'][h.TIERS[3]]==1

def test_pe_not_current():
    t,f=inputs(tp='',fp='低PE高TTM EPS');f['market_day']='2026-11-20';assert not any(h.combine(t,f)['counts'].values())

def test_actual_upstream_evaluate_contracts():
    from test_h191_opportunity_engine import fixture
    from test_h192_financial_growth import full,NOW as finance_now
    t=h.tech.evaluate(**fixture(),top=1);t['calendar_fresh']=True
    m,d=full()
    for rs in d.values():
        for x in rs:
            if x['code']=='1101':x['code']='1000'
    m[0]['code']='1000'
    f=h.fin.evaluate(master=m,data=d,now=finance_now,market_day=D,opportunities=t,top=1)
    r=h.combine(t,f);assert any(x['股票代號']=='1000' for x in r['pools'][h.TIERS[0]])
    assert r['counts'][h.TIERS[1]]>1

def test_technical_visible_rank():
    t,f=inputs(fp='');a=t['diagnostic'][0];t['diagnostic'].append({**a,'股票代號':'1102'})
    t['pools']={'技術突破':[{**a,'股票代號':'1102','研究排名':1}]}
    r=h.combine(t,f);assert [x['股票代號'] for x in r['pools'][h.TIERS[1]]]==['1102','1101']

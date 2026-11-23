# -*- coding: utf-8 -*-
from __future__ import annotations
import ast
import io
import json
import math
from pathlib import Path
import sys
import numpy as np
import pandas as pd
import pytest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h184_research_lab import ROLES,run_role,research_manifest,export_workbook,normalize_frame


def prices(n=150,code='2330',with_source=True):
    dat=pd.bdate_range('2026-01-02',periods=n)
    close=100+np.arange(n)*.2+np.sin(np.arange(n)/5)*3
    return pd.DataFrame(dict(date=dat.strftime('%Y-%m-%d'),open=close-0.2,high=close+2,
                             low=close-2,close=close,volume=np.arange(n)*100+100000,
                             code=code,source='測試行情' if with_source else ''))

@pytest.fixture
def docs():
    return {'ohlcv':prices(),
            'events':pd.DataFrame([{'code':'2330','event_date':'2026-11-24','event_type':'法說會','title':'測試事件',
                                   'source_url':'https://example.org/events','fact':'日期由測試資料提供',
                                   'hypothesis':'營收可能改變'}]),
            'portfolio':pd.DataFrame([{'code':'2330','weight_pct':30,'sector':'半導體','currency':'TWD','region':'TW'},
                                      {'code':'2317','weight_pct':40,'sector':'電子','currency':'TWD','region':'TW'}]),
            'trades':pd.DataFrame([{'code':'2330','entry_price':100,'exit_price':110,'quantity':10,
                                   'cost':10,'rule_followed':'否'}]),
            'financial':pd.DataFrame([{'code':'2330','fiscal_period':'2026Q3','disclosed_at':'2026-11-14',
                                      'revenue':1000,'gross_margin_pct':55,'operating_margin_pct':33,
                                      'eps_ttm':25,'eps_ttm_verified':True,'unit':'million TWD'},
                                     {'code':'2317','fiscal_period':'2026Q3','disclosed_at':'2026-11-14',
                                      'revenue':2000,'gross_margin_pct':11,'operating_margin_pct':5,
                                      'eps_ttm':10,'eps_ttm_verified':False,'unit':'million TWD'}]),
            'quotes':pd.DataFrame([{'code':'2330','date':'2026-11-19','close':1000,'source':'測試'},
                                   {'code':'2317','date':'2026-11-19','close':200,'source':'測試'}]),
            'evidence':pd.DataFrame([{'code':'2330','metric':'EPS','value':25,'unit':'元/股',
                                      'period':'2026Q3','asof_date':'2026-11-15',
                                      'source_url':'https://example.com/a','quote':'示例','claim':'EPS高'}])}

DCF={'revenue':1000,'growth':.08,'margin':.15,'tax':.20,'capex':.05,
     'working_capital':.02,'wacc':.10,'terminal_growth':.02,
     'shares':100,'debt':200,'cash':100,'acknowledge_assumptions':True}

def test_12_roles_declared():assert len(ROLES)==12

def test_all_12_roles_execute(docs):
    for k in ROLES:
        x=run_role(k,docs,{**DCF,'code':'2330','cash_pct':30,'thesis':'營收成長與產能提高可能讓公司未來獲利持續增長，需要核對訂單。','asof':'2026-11-20'})
        assert x.role==k
        assert x.status!='' and isinstance(x.gaps,list)
        assert isinstance(x.tabular(),dict)

def test_ideas_have_five_scenarios_no_trade_authority(docs):
    r=run_role('01',docs,{'code':'2330'})
    assert len(r.tables['五種研究情境'])==5
    assert r.tables['五種研究情境']['證據等級'].str.contains('非買進').all()

def test_technical_has_60_day_and_week(docs):
    o=run_role('02',docs,{'code':'2330'})
    assert o.metrics['MA60']>0 and o.metrics['週線10週均']>0

def test_no_multistock_mixing(docs):
    combined=pd.concat([prices(90,'2330'),prices(90,'2317')],ignore_index=True)
    o=run_role('01',{'ohlcv':combined})
    assert o.status=='待補證'
    assert any('多檔行情' in x for x in o.gaps)

def test_broken_prices_fail_closed(docs):
    f=docs['ohlcv'].copy();f['volume']=-1
    o=run_role('02',{'ohlcv':f},{'code':'2330'})
    assert o.status=='待補證'

def test_news_claim_not_certified(docs):
    o=run_role('03',docs)
    assert '待' in o.status and '待' in o.tables['新聞事件查核']['證據狀態'].iloc[0]

def test_backtest_next_open_and_costs(docs):
    o=run_role('04',docs,{'fee_bps':14.25,'tax_bps':30,'slippage_bps':5,'code':'2330'})
    assert o.metrics['交易成本_bps買']==19.25
    assert o.metrics['交易成本_bps賣']==49.25
    assert o.metrics['回測迄日']

def test_backtest_no_fake_sample_metrics():
    o=run_role('04',{'ohlcv':prices(61)},{'code':'2330'})
    assert o.status=='待補證'

def test_portfolio_20pct_stress(docs):
    o=run_role('05',docs,{'cash_pct':30})
    assert o.metrics['市場全持股跌20%假設組合損益%']==-14

def test_journal_flags_noncompliance(docs):
    o=run_role('06',docs)
    assert o.metrics['明確標記違規筆數']==1
    assert len(o.tables['三條可檢查規則'])==3

def test_financial_no_future_filing(docs):
    o=run_role('07',{'financial':docs['financial']},{'code':'2330','asof':'2026-11-10'})
    assert '提供之財報' not in o.tables or len(o.tables['提供之財報'])==0

def test_contrarian_hypothesis_not_claim(docs):
    o=run_role('08',docs,{'thesis':'我認為獲利逐季增加，股價應該會成長，需要檢查客戶訂單。'})
    assert (o.tables['反方研究檢核']['狀態']=='待核實').all()

def test_peer_does_not_infer_eps_from_pe(docs):
    o=run_role('09',docs,{'asof':'2026-11-20'})
    d=o.tables['同期間估值與營運比較']
    assert d.loc[d['股票代號']=='2330','PE'].iloc[0]==40
    assert pd.isna(d.loc[d['股票代號']=='2317','PE'].iloc[0])

def test_dcf_requires_acknowledge():
    a=dict(DCF);a.pop('acknowledge_assumptions')
    assert run_role('10',{},a).status=='待補證'
    assert run_role('10',{},DCF).status=='情境試算（非預測）'

def test_dcf_invalid_discount_rate_rejected():
    a=dict(DCF);a['wacc']=.01;a['terminal_growth']=.02
    assert run_role('10',{},a).status=='待修正假設'

def test_calendar_missing_dates_not_fabricated(docs):
    o=run_role('11',docs)
    assert o.metrics['事件數']==1

def test_audit_not_verified_without_fetch(docs):
    o=run_role('12',docs)
    assert o.metrics['已核實']==0 and o.metrics['待核實']==1

def test_audit_duplicate_conflicting_values_flagged(docs):
    f=docs['evidence'].copy();f.loc[1]=f.iloc[0];f.loc[1,'value']=28
    o=run_role('12',{'evidence':f})
    assert o.metrics['相互矛盾']==2

def test_export_manifest_has_hash_and_12_tabs(docs):
    out={'01':run_role('01',docs,{'code':'2330'}),'12':run_role('12',docs)}
    manifest=research_manifest(docs,{},out)
    assert manifest['authority']=='RESEARCH_ONLY_H109_H126_UNCHANGED'
    assert len(manifest['data']['ohlcv']['sha256'])==64
    from openpyxl import load_workbook
    book=load_workbook(io.BytesIO(export_workbook(out,manifest)),read_only=True)
    assert len(book.sheetnames)==14
    assert '00_研究導航' in book.sheetnames
    assert book['00_研究導航']['F2'].value=='H109／H126 未改變'

def test_role_readonly_contract():
    code=(ROOT/'godpick_h184_research_lab.py').read_text('utf-8')
    page=(ROOT/'pages/28_十二研究員決策研究中心.py').read_text('utf-8')
    assert not any(s in code for s in ('godpick_records.json','delete_record','firebase_admin','github_write'))
    assert 'require_login()' in page
    ast.parse(code);ast.parse(page)

@pytest.mark.parametrize('role',list(ROLES))
def test_missing_inputs_returns_explicit_gap(role):
    result=run_role(role,{},{});
    assert result.gaps or result.tables or result.metrics
    assert result.status!='已核實'

# -*- coding: utf-8 -*-
"""H149: evidence reliability, opponent challenge, zero fabrication, Excel closure."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pandas as pd
from godpick_h149_trade_intelligence import (build_six_pillar_review,
    append_six_pillar_to_sheet, _journal_diagnostics, _valid_close_levels)

DAY='2026-10-30'

def raw(code='2352', **extra):
    d={'股票代號':code,'股票名稱':'示範股','類別':'電子',
       'H114K線證據日':DAY,'H114K線有效根數':80,
       'H114K線資料狀態':'VALID_OHLCV',
       'H113當日成交額百萬':255,'H113二十日均成交額百萬':180,
       'H114當日成交量張':7200,'H114K線量比':1.6,
       'H114當日收盤價':100,'H114有效支撐價':95,'H114有效壓力價':120,
       'H109決策權威':'ALLOW','H123法人實證狀態':'PENDING_EVIDENCE',
       'H124大戶實證狀態':'PENDING_EVIDENCE',
       'H108族群生命週期':'IGNITION'}
    d.update(extra)
    return d

def lead(code='2352', tier='A｜同日量價研究已核'):
    return {'股票代號':code,'股票名稱':'示範股','產業':'電子','研究層級':tier,
           '突破／續攻判定':'突破前20日高點研究','來源證據':'H114同日內部K線標記',
           'H109風控':'ALLOW'}

def review(rows=None, leads=None, **kw):
    return build_six_pillar_review(diagnosis_frame=pd.DataFrame(rows if rows is not None else [raw()]),
        research_frame=pd.DataFrame(leads if leads is not None else [lead()]),
        authority_frame=pd.DataFrame([{'股票代號':'2330','H126正式資格':'禁止新倉'}]),
        research_day=DAY,h145_bridge={'same_run_proven':True,'candidate_complete':True},**kw)

def test_real_same_day_levels_only_from_same_stock():
    d,s,m=review()
    assert len(d)==1 and d.iloc[0]['參考風險報酬比']==4.0
    assert d.iloc[0]['研究現價']==100
    assert '非買進' in d.iloc[0]['交易資格'] or '未授權' in d.iloc[0]['交易資格']
    assert m['formal_h126_modified'] is False and m['formal_h126_count']==1

def test_stale_and_future_dates_do_not_offer_price():
    for day in ('2026-10-29','2026-10-31',''):
        d,_,m=review(rows=[raw(H114K線證據日=day)])
        assert pd.isna(d.iloc[0]['研究現價']) and pd.isna(d.iloc[0]['參考風險報酬比'])
        assert m['same_day_quote']==0

def test_synthetic_target_prohibited_without_actual_support_resistance():
    d,_,m=review(rows=[raw(H114有效壓力價=None)])
    assert d.iloc[0]['研究現價']==100 and pd.isna(d.iloc[0]['參考風險報酬比'])
    assert '不產生目標' in d.iloc[0]['價位計算狀態']
    assert m['calculated_rr']==0

def test_conflicting_levels_rejected():
    d,_,_=review(rows=[raw(H114有效支撐價=102)])
    assert pd.isna(d.iloc[0]['支撐價']) and pd.isna(d.iloc[0]['參考風險報酬比'])

def test_volume_and_duplicate_code_not_used_to_fill_five():
    d,_,meta=review(rows=[raw(),raw()],leads=[lead()])
    assert d.empty and '2352' in meta['conflict_codes']
    d,_,meta=review(rows=[raw(H114K線量比=None)])
    assert d.empty and meta['reviewable']==0

def test_news_must_have_date_and_source_no_price_causality():
    d,_,_=review(rows=[raw(消息標題='利多=',消息日期=DAY,消息來源網址='')])
    assert '未有可查證' in d.iloc[0]['消息證據']
    d,_,_=review(rows=[raw(消息標題='董事會決議',消息日期=DAY,消息來源網址='https://example.org/item')])
    assert '事件待核' in d.iloc[0]['消息證據']


def test_h109_veto_and_h124_missing_never_promoted():
    d,s,m=review(rows=[raw(H109決策權威='FADE_VETO')])
    assert 'FADE_VETO' in d.iloc[0]['反證／失效條件']
    assert '不能稱鎖碼' in d.iloc[0]['反證／失效條件']
    assert len(s)==9


def test_partial_h145_scope_is_not_market_top_five():
    d,s,m=build_six_pillar_review(diagnosis_frame=pd.DataFrame([raw()]),research_frame=pd.DataFrame([lead()]),
        authority_frame=pd.DataFrame(),research_day=DAY,h145_bridge={})
    assert not m['scope_proven']
    assert '母體' in m['warning']
    assert '母體' in d.iloc[0]['反證／失效條件']


def test_closed_trade_counts_only_actual_fills_and_no_fabricated_winrate():
    state,_=_journal_diagnostics(pd.DataFrame([{'成交狀態':'研究推薦','進場日期':DAY,'出場日期':DAY,'已實現報酬率%':100}]))
    assert '0筆' in state
    trades=pd.DataFrame([{'成交狀態':'已平倉','進場日期':'2026-10-01',
             '出場日期':'2026-10-30','已實現報酬率%':x} for x in [3,4,-2,-1,2]])
    state,warning=_journal_diagnostics(trades)
    assert '60.0%' in state and '樣本描述' in warning


def test_incomplete_holdings_never_claims_hedging_stress_results():
    _,summary,_=review(holdings=pd.DataFrame([{'股票代號':'2352','配置比例%':100}]))
    r=summary.set_index('研究面向').loc['5｜組合風險']
    assert '未' in r['主要盲點／反證'] or '禁止' in r['主要盲點／反證']


def test_excel_append_is_non_trading_and_escapes_formula_injection():
    from openpyxl import Workbook
    d,s,_=review(rows=[raw(股票名稱='=cmd')],leads=[dict(lead(),股票名稱='=cmd')])
    wb=Workbook();wb.active.title='00_主管重點觀察榜'
    wb.create_sheet('00_H126主管決策')
    wb.worksheets[1]['A1']='不可修改的H126權威'
    wb.active['A1']='原主榜';wb.active.auto_filter.ref='A1:P2'
    before=wb.active.auto_filter.ref
    audit=append_six_pillar_to_sheet(wb.active,d,s)
    assert audit['rows']==1
    assert wb.sheetnames==['00_主管重點觀察榜','00_H126主管決策']
    assert wb.worksheets[1]['A1'].value=='不可修改的H126權威'
    assert wb.active.auto_filter.ref==before
    values=[str(c.value) for row in wb.active for c in row if c.value is not None]
    assert "'=cmd" in values


def test_page07_has_all_pipeline_hooks_and_cache_version():
    code=(Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py').read_text('utf-8')
    assert 'build_six_pillar_review' in code
    assert 'append_six_pillar_to_sheet' in code
    assert 'H1482|H1483|H149' in code
    assert 'StockPick_H149_TradeResearch_' in code


def test_h129_replay_evidence_kept_separate_from_real_trades():
    feedback=pd.DataFrame([{'項目':'H129原T+1檢討筆數','數值':80},
        {'項目':'H129經H103驗證可用筆數','數值':7},
        {'項目':'H129有效Alpha平均pp','數值':1.3}])
    _,summary,_=review(feedback_health=feedback)
    fact=summary.set_index('研究面向').loc['4｜策略回測','本輪事實']
    assert '7/80' in fact and '非真實交易績效' in fact


def test_research_rank_prioritizes_evidence_and_distinct_sectors():
    rows=[raw(str(code), 類別=sector,H124大戶實證狀態=holder) for code,sector,holder in [
          (2352,'電子','VERIFIED_RISING'),(2353,'電子','PENDING_EVIDENCE'),
          (2354,'電子','PENDING_EVIDENCE'),(2603,'航運','VERIFIED_RISING')]]
    leads=[lead(str(x['股票代號'])) for x in rows]
    d,_,m=review(rows=rows,leads=leads,limit=3)
    assert m['different_sectors']==2
    assert d.iloc[0]['股票代號'] in ('2352','2603')
    assert all('研究證據排序' in v for v in d['研究優先序'])

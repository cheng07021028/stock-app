# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
from godpick_h1535_research_recovery import (
    build_verification_watchlist,merge_nontrading_watchlist,
    restore_verified_research_pool,
)

DAY='2026-11-09'

def _stock(code='2618',risk='FADE_VETO',*,amount=1365.2,close=35.,ma20=34.,ratio=1.68):
    return {'股票代號':code,'股票名稱':'測試_'+code,'類別':'航運','市場別':'上市',
        'H114K線證據日':DAY,'H114K線資料狀態':'VALID_OHLCV','H114K線有效根數':162,
        'H114當日收盤價':close,'H114前日收盤價':34.5,'H114前次交易日':'2026-11-06',
        'H114MA20':ma20,'H114MA60':32.,'H114突破前蓄勢分':63,
        'H114K線量比':ratio,'H114當日成交量張':31097.,
        'H114當前5日漲跌幅%':2.1,'H114當日漲跌幅%':1.449,
        'H113當日成交額百萬':amount,'H113二十日均成交額百萬':872.5,
        'H109決策權威':risk,'H1532布林收斂':'否',
        'H1532OBV量價背離':'未確認'}

def test_same_run_etf_does_not_discard_whole_2000_pool():
    limited=pd.DataFrame([_stock(), {'股票代號':'0056','股票名稱':'ETF'}])
    full=pd.DataFrame([_stock(),_stock('2330'),{'股票代號':'0056','股票名稱':'ETF'}])
    res,meta=restore_verified_research_pool(limited,full,
        current_signature='scan20261109',full_signature='scan20261109')
    assert meta['status']=='VERIFIED_SAME_RUN_RESEARCH_SUPERSET'
    assert set(res['股票代號'])=={'2618','2330'}
    assert meta['full_excluded_nonstocks']==1

def test_missing_h114_in_full_only_recovers_same_code_and_not_extra():
    limited=pd.DataFrame([_stock()]);full=pd.DataFrame([
        {'股票代號':'2618','股票名稱':'本輪完整快取'},
        {'股票代號':'2330','股票名稱':'本輪新股但無行情'}])
    res,meta=restore_verified_research_pool(limited,full,
        current_signature='a',full_signature='a')
    assert meta['status']=='VERIFIED_SAME_RUN_RESEARCH_SUPERSET'
    assert res.set_index('股票代號').loc['2618','H114K線證據日']==DAY
    assert pd.isna(res.set_index('股票代號').loc['2330','H114K線證據日'])
    watch,summary,info=build_verification_watchlist(res,research_day=DAY)
    assert watch['股票代號'].tolist()==['2618']
    assert info['total_watch']==1

def test_conflict_and_different_scan_both_block():
    limited=pd.DataFrame([_stock()])
    different=pd.DataFrame([_stock(),_stock('2330')])
    assert restore_verified_research_pool(limited,different,
        current_signature='today',full_signature='yesterday')[1]['status'].startswith('BLOCKED_RUN')
    different.loc[0,'H114當日收盤價']=50
    restored,meta=restore_verified_research_pool(limited,different,
        current_signature='today',full_signature='today')
    assert meta['status']=='BLOCKED_OVERLAP_H114_CONFLICT'
    assert len(restored)==1

def test_duplicate_stock_outside_overlap_is_quarantined_not_rejected_wholesale():
    limited=pd.DataFrame([_stock()])
    full=pd.DataFrame([_stock(),_stock('2330'),_stock('2330'),_stock('1795')])
    restored,meta=restore_verified_research_pool(limited,full,
        current_signature='a',full_signature='a')
    assert meta['status']=='VERIFIED_SAME_RUN_RESEARCH_SUPERSET'
    assert set(restored['股票代號'])=={'2618','1795'}
    assert meta['full_duplicate_codes']==1
    overlap=pd.DataFrame([_stock(),_stock(),_stock('2330')])
    assert restore_verified_research_pool(limited,overlap,
        current_signature='a',full_signature='a')[1]['status']=='BLOCKED_OVERLAP_DUPLICATE_STOCK_ID'

def test_zero_buy_all_fade_veto_still_has_visible_nontrade_audit_priority():
    # The requested regression: no H153 confirmed signals, all 119 H109 FADE.
    raw=[_stock('2618'),_stock('1795',amount=352.8),
         _stock('8042',amount=2732.4,ratio=1.02)]
    watch,summary,meta=build_verification_watchlist(pd.DataFrame(raw),research_day=DAY)
    assert len(watch)==3
    assert watch['研究排名'].isna().all()
    assert '禁止買進' in watch.iloc[0]['交易資格']
    assert watch['研究分類'].str.contains('隔離反證').all()
    assert set(watch['H109風控'])=={'FADE_VETO'}
    assert all('H126' in str(v) for v in watch['交易資格'])
    assert watch['核查優先序'].notna().all()
    research=merge_nontrading_watchlist(pd.DataFrame(),watch)
    assert len(research)==3
    assert research['股票代號'].nunique()==3
    assert meta['formal_trade_modified'] is False

def test_no_fake_recommendations_when_missing_dated_volume_and_tech():
    a=_stock();a['H114K線證據日']='2026-11-06'
    b=_stock('2330');b['H113當日成交額百萬']=10
    c=_stock('2382');c['H114MA20']=None;c['H114MA60']=None
    c['H114突破前蓄勢分']=40
    d=_stock('1216');d['H114當日收盤價']=None
    watch,summary,meta=build_verification_watchlist(pd.DataFrame([a,b,c,d]),research_day=DAY)
    assert watch.empty
    assert meta['shown_watch']==0

def test_watch_does_not_overwrite_original_research_or_fabricate_rank():
    original=pd.DataFrame([{'股票代號':'2618','研究排名':1,'排名等級':'A｜正式研究','研究證據分':70}])
    watch,_,_=build_verification_watchlist(pd.DataFrame([_stock(),_stock('1795')]),research_day=DAY)
    merged=merge_nontrading_watchlist(original,watch)
    assert len(merged)==2
    assert merged.loc[merged['股票代號']=='2618','研究排名'].iloc[0]==1
    assert pd.isna(merged.loc[merged['股票代號']=='1795','研究排名'].iloc[0])

def test_rank_ties_not_forced_into_distinct_fake_investment_ranks():
    watch,_,_=build_verification_watchlist(pd.DataFrame([_stock('2618'),_stock('1795')]),research_day=DAY)
    assert watch['核查優先序'].tolist()==[1,1]
    assert watch['研究排名'].isna().all()

# -*- coding: utf-8 -*-
"""Execute the REAL utils.get_history_data AST, with deterministic offline providers."""
from __future__ import annotations
import ast
import sys
import time
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h186_evidence_integrity import volume_quality
from godpick_h145_core_bridge import restore_export_pool
from godpick_h186_evidence_integrity import verified_same_run_export_frame


def frame(n=150,zero=0):
    ds=pd.bdate_range('2026-04-27',periods=n)
    df=pd.DataFrame({'日期':ds,'開盤價':[99.]*n,'最高價':[101.]*n,
                     '最低價':[98.]*n,'收盤價':[100.]*n,
                     '成交股數':[1000000.]*n})
    if zero:df.loc[df.index[-zero:],'成交股數']=0
    return df


def get_runtime(cache=None,fast=None,official=None):
    source=(ROOT/'utils.py').read_text('utf-8')
    node=next(n for n in ast.parse(source).body
              if isinstance(n,ast.FunctionDef) and n.name=='get_history_data')
    # No source rewriting or monkeypatching actual business logic.
    data=[node]
    module=ast.fix_missing_locations(ast.Module(body=data,type_ignores=[]))
    events=[];saves=[]
    cache=frame(150) if cache is None else cache
    fast=frame(150) if fast is None else fast
    official=frame(150) if official is None else official
    env={
        'pd':pd,'date':date,'time':time,'V180_DISABLE_DUPLICATE_HISTORY_RETRY':True,
        'HISTORY_DEBUG_ON_FAIL':False,
        '_diag_add_event':lambda *a,**k:events.append((a,k)),
        '_load_history_disk_cache':lambda *a,**k:cache,
        '_save_history_disk_cache':lambda *a,**k:saves.append((a,k)),
        '_fetch_yahoo_history_fast':lambda *a,**k:fast,
        '_history_frame_business_lag':lambda *a,**k:0,
        '_history_frame_is_fresh_enough':lambda *a,**k:True,
        '_history_market_candidates':lambda *a,**k:['上市'],
        '_fetch_twse_history_month':lambda *a,**k:(official,'official'),
        '_fetch_tpex_history_month':lambda *a,**k:(official,'official'),
        '_normalize_history_df':lambda f,*a,**k:f,
        '_save_debug_scan_summary':lambda *a,**k:None,
        '_h186_bad':False,
    }
    exec(compile(module,str(ROOT/'utils.py'),'exec'),env)
    return env['get_history_data'],events,saves


def source_names(events):
    return [x[0][1] for x in events]


def test_clean_cache_no_network_and_no_write():
    get,events,saves=get_runtime(cache=frame(162))
    a=get('2330',start_date=date(2026,3,1),end_date=date(2026,11,23))
    assert len(a)==162 and 'DiskCache' in source_names(events)
    assert 'H186CacheRevalidate' not in source_names(events) and not saves


def test_invalid_cache_repaired_by_complete_yahoo():
    good=frame(162);bad=frame(162,zero=12)
    get,events,saves=get_runtime(cache=bad,fast=good)
    a=get('2330',start_date=date(2026,3,1),end_date=date(2026,11,23))
    assert volume_quality(a)['status']=='VALID_60_DATED_POSITIVE_VOLUME'
    assert 'H186CacheRevalidate' in source_names(events)
    assert 'YahooFastTotal' in source_names(events)
    assert len(saves)==1


def test_invalid_yahoo_repairs_from_existing_official_flow():
    empty=pd.DataFrame();bad=frame(162,zero=12);good=frame(162)
    get,events,saves=get_runtime(cache=bad,fast=bad,official=good)
    a=get('2330',start_date=date(2026,3,1),end_date=date(2026,11,23))
    assert volume_quality(a)['status']=='VALID_60_DATED_POSITIVE_VOLUME'
    assert 'H186YahooVolumeRetry' in source_names(events)
    assert len(saves)==1


def test_remote_fail_preserves_price_only_but_not_certified():
    bad=frame(162,zero=12)
    get,events,saves=get_runtime(cache=bad,fast=bad,official=pd.DataFrame())
    a=get('2330',start_date=date(2026,3,1),end_date=date(2026,11,23))
    assert len(a)==162 and volume_quality(a)['status']=='INCOMPLETE_VOLUME'
    assert 'H186PriceOnlyUnverified' in source_names(events)
    assert not saves


def test_scope_recovery_proceeds_to_real_h145_only_with_signatures():
    full=pd.DataFrame({'股票代號':['2330','2385'],
        'H114K線證據日':['2026-11-23']*2,'H114K線資料狀態':['INVALID_OHLCV_VOLUME']*2})
    limited=full.iloc[:1].copy()
    selected,diag=verified_same_run_export_frame(limited,full,expected_count=2,
        current_signature='signed',full_signature='signed',current_run_id='run',scan_run_id='run')
    restored,audit=restore_export_pool(selected,pd.DataFrame(),pd.DataFrame(),
        scan_report={'candidate_diagnosis_count':2,'final_candidate_count':0},
        context_run_id='run',scan_run_id='run',decision_signature='signed',active_signature='signed')
    assert diag['status']=='RECOVERED_FOR_H145_RECHECK'
    assert len(restored)==2 and audit['candidate_complete']
    assert audit['expected_diagnosis']==2
    assert audit['status']=='SUPPLIED_FULL_SCAN_MATCHED'


def test_partial_scope_never_claims_complete_h145():
    full=pd.DataFrame({'股票代號':['2330','2385'],
        'H114K線證據日':['2026-11-23']*2,'H114K線資料狀態':['VALID_OHLCV']*2})
    limited=full.iloc[:1].copy()
    selected,diag=verified_same_run_export_frame(limited,full,expected_count=2,
        current_signature='signed',full_signature='DIFFERENT',current_run_id='run',scan_run_id='run')
    _,audit=restore_export_pool(selected,pd.DataFrame(),pd.DataFrame(),
        scan_report={'candidate_diagnosis_count':2,'final_candidate_count':0},
        context_run_id='run',scan_run_id='run',decision_signature='signed',active_signature='signed')
    assert not audit['candidate_complete'] and audit['status'].startswith('BLOCKED_')

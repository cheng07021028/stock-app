# -*- coding: utf-8 -*-
from __future__ import annotations
import sys
from pathlib import Path
from datetime import date
import pandas as pd
import pytest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h186_evidence_integrity import volume_quality,verified_same_run_export_frame
from godpick_h114_parallel_alpha import analyze_history


def history(n=162, source='yahoo_chart_TW', lots=3000):
    days=pd.bdate_range('2026-03-01',periods=n)
    return pd.DataFrame({'日期':days,'開盤價':[99+i*.1 for i in range(n)],
        '最高價':[101+i*.1 for i in range(n)], '最低價':[98+i*.1 for i in range(n)],
        '收盤價':[100+i*.1 for i in range(n)],'成交股數':[lots*1000]*n,
        '資料源':[source]*n})


def test_60_valid_shares_and_h114():
    h=history()
    q=volume_quality(h)
    assert q['status']=='VALID_60_DATED_POSITIVE_VOLUME' and q['positive']==60
    a=analyze_history(h,asof=str(h['日期'].iloc[-1].date()))
    assert a['H114K線資料狀態']=='VALID_OHLCV'
    assert a['H186成交量單位']=='股' and a['H186量價資料源']=='yahoo_chart_TW'


def test_missing_recent_shares_stays_unverified():
    h=history();h.loc[h.index[-8:],'成交股數']=float('nan')
    q=volume_quality(h)
    assert q['missing']==8 and q['status']=='INCOMPLETE_VOLUME'
    a=analyze_history(h,asof=str(h['日期'].iloc[-1].date()))
    assert a['H114K線資料狀態']=='INVALID_OHLCV_VOLUME'
    assert a['H175最近60根成交量缺失數']==8
    assert '缺值8' in a['H186近60根成交量缺口']
    assert a['H114當日成交量張'] is None


def test_nonpositive_kept_not_estimated():
    h=history();h.loc[h.index[-1],'成交股數']=0
    q=volume_quality(h)
    assert q['nonpositive']==1 and q['status']=='INCOMPLETE_VOLUME'
    a=analyze_history(h,asof=str(h['日期'].iloc[-1].date()))
    assert a['H114K線資料狀態']=='INVALID_OHLCV_VOLUME'


def test_ambiguous_volume_unit_cannot_be_reinterpreted():
    h=history().rename(columns={'成交股數':'成交量'})
    assert volume_quality(h)['status']=='MISSING_VOLUME_UNIT'
    a=analyze_history(h,asof=str(h['日期'].iloc[-1].date()))
    assert a['H114K線資料狀態']=='VALID_OHLC_ONLY_VOLUME_UNIT_UNKNOWN'
    assert a['H114當日成交量張'] is None


def test_lots_explicit_unit():
    h=history().drop(columns=['成交股數']);h['成交量(張)']=2000.
    assert volume_quality(h)['unit']=='LOTS'
    a=analyze_history(h,asof=str(h['日期'].iloc[-1].date()))
    assert a['H186成交量單位']=='張'


def test_bad_ohlc_does_not_poison_cache_green():
    h=history();h.loc[h.index[-3],'最高價']=5
    assert volume_quality(h)['status']=='INVALID_OHLC'
    assert volume_quality(h)['bad_ohlc']==1


def test_history_only_30_bars_not_60():
    h=history(30)
    assert volume_quality(h)['status']=='SHORT_HISTORY'


def test_future_rows_never_make_h114_current():
    h=history();end=str(h['日期'].iloc[-2].date())
    a=analyze_history(h,asof=end)
    assert a['H114K線證據日']==end
    assert a['H186量價資料源']=='yahoo_chart_TW'


def scanned(*codes):
    return pd.DataFrame({'股票代號':list(codes),
        'H114K線資料狀態':['INVALID_OHLCV_VOLUME']*len(codes),
        'H114K線證據日':['2026-11-23']*len(codes)})


def scope(a,b,**kwargs):
    pars={'expected_count':len(b), 'current_signature':'signature-11-23',
          'full_signature':'signature-11-23', 'current_run_id':'run-11-23',
          'scan_run_id':'run-11-23'}
    pars.update(kwargs)
    return verified_same_run_export_frame(a,b,**pars)


def test_same_run_full_2260_recovers_from_limited_120():
    codes=[f'{1000+i}' for i in range(2260)]
    full=scanned(*codes);limited=full.iloc[:120].copy()
    result,meta=scope(limited,full)
    assert len(result)==2260 and meta['status']=='RECOVERED_FOR_H145_RECHECK'
    assert not meta['authority_changed']


@pytest.mark.parametrize('field,value', [('current_signature','other'),('full_signature','other'),
                                          ('current_run_id','other'),('scan_run_id','other'),
                                          ('expected_count',2261)])
def test_cross_run_rejected(field,value):
    a=scanned('2330');b=scanned('2330','2385')
    result,meta=scope(a,b,**{field:value})
    assert len(result)==1 and meta['status']=='NO_SAME_RUN_FULL_DIAGNOSIS'


def test_extra_unknown_stock_in_limited_rejected():
    a=scanned('9999');b=scanned('2330','2385')
    result,meta=scope(a,b)
    assert meta['status']=='FULL_DIAGNOSIS_CODE_CONFLICT' and len(result)==1


def test_duplicate_full_stopped():
    a=scanned('2330');b=scanned('2330','2330')
    _,m=scope(a,b)
    assert m['status']=='FULL_DIAGNOSIS_CODE_CONFLICT'


def test_full_missing_dated_evidence_stopped():
    a=scanned('2330');b=scanned('2330','2385').drop(columns=['H114K線證據日'])
    _,m=scope(a,b)
    assert m['status']=='FULL_DIAGNOSIS_NO_DATED_EVIDENCE'


def test_source_code_has_repair_hook_not_permission_overwrite():
    utils=(ROOT/'utils.py').read_text('utf-8')
    page=(ROOT/'pages'/'7_股神推薦.py').read_text('utf-8')
    assert 'H186CacheRevalidate' in utils
    assert 'H186YahooVolumeRetry' in utils
    assert 'H186PriceOnlyUnverified' in utils
    assert 'h186_full_export_scope' in page
    # The existing H126 document remains untouched; H186 is evidence-only.
    assert 'H126能否買進' not in (ROOT/'godpick_h186_evidence_integrity.py').read_text('utf-8')


def test_0050_etf_does_not_invalidate_same_run_diagnosis():
    limited=scanned('2330')
    full=scanned('2330','0050','2385')
    result,meta=scope(limited,full)
    assert len(result)==3
    assert meta['zero_prefixed_codes_not_formal_stocks']==1
    assert not meta['authority_changed']

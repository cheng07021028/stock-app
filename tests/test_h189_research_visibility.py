# -*- coding: utf-8 -*-
"""H189: distinct PIT research visibility and turnover proxy (NO trade authority)."""
import pandas as pd
from godpick_h114_parallel_alpha import analyze_history
from godpick_h153_growth_setup_rank import build_growth_setup_ranking
from godpick_h175_evidence_rescue import build_h175_audit

DAY='2026-11-23'

def bars(valid=True, *, volume=4_000_000):
    dates=pd.bdate_range(end=DAY,periods=65)
    prices=[100+i*.01 for i in range(65)]
    prices[-1]=101.0
    rows=[]
    for i,(date,close) in enumerate(zip(dates,prices)):
        rows.append({'日期':date.date().isoformat(),'開盤價':close-.3,
                     '最高價':close+(.1 if i==64 else .65),'最低價':close-.7,
                     '收盤價':close,
                     '成交股數':volume if valid else None,'資料源':'test_dated_declared_shares'})
    return pd.DataFrame(rows)

def candidate():
    row=analyze_history(bars(),asof=DAY)
    row.update({'股票代號':'2330','股票名稱':'驗證用股票','類別':'電子',
                'H109決策權威':'RESEARCH_ELIGIBLE'})
    return row

def test_h189_proxy_real_ohlcv_and_explicit_nonofficial():
    s=candidate()
    assert s['H114K線資料狀態']=='VALID_OHLCV'
    assert s['H189研究估算當日成交額百萬']>300
    assert s['H189研究估算20日均成交額百萬']>300
    assert 'H113/H126' in s['H189研究估算成交額依據']
    assert 'H113當日成交額百萬' not in s

def test_h189_no_volume_no_proxy_and_no_false_rank():
    x=analyze_history(bars(False),asof=DAY)
    assert x['H114K線資料狀態'] != 'VALID_OHLCV'
    assert x['H189研究估算當日成交額百萬'] is None
    x.update({'股票代號':'2330','H109決策權威':'RESEARCH_ELIGIBLE'})
    rank,_,_=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([x]),
        research_frame=pd.DataFrame(), research_day=DAY)
    assert rank.empty

def test_h189_full_pool_can_research_without_limited_h113_columns():
    s=candidate()
    assert 'H113當日成交額百萬' not in s
    rank,_,meta=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([s]),
        research_frame=pd.DataFrame(),research_day=DAY)
    assert len(rank)==1
    assert '研究代理成交額' in rank.iloc[0]['量能驗證']
    assert 'H113/H126不得因此通過' in rank.iloc[0]['反證與待補']
    assert meta['formal_h126_modified'] is False

def test_h189_explicit_h113_zero_is_not_ignored_by_proxy():
    s=candidate();s['H113當日成交額百萬']=0.;s['H113二十日均成交額百萬']=0.
    rank,_,_=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([s]),
        research_frame=pd.DataFrame(),research_day=DAY)
    assert rank.empty

def test_h189_h145_formal_block_does_not_hide_matched_h85_waiting():
    diag=pd.DataFrame([{'股票代號':'2330','H114K線資料狀態':'INVALID_OHLCV_VOLUME',
                        'H114K線有效根數':162,'H114K線證據日':DAY,
                        'H109決策權威':'FADE_VETO'}])
    waiting=pd.DataFrame([{'股票代號':'2330','股票名稱':'台積電'},
                          {'股票代號':'0050','股票名稱':'元大台灣50'}])
    rep=build_h175_audit(diagnosis=diag,tables={'waiting':waiting},
        h154_audit={'run_identity':'MATCHED','run_id':'same-run','h145_status':'BLOCKED_PARTIAL_EXPORT_NO_TRUSTED_RECOVERY',
                   'h85_waiting_codes':['2330','0050'],'h85_emerging_codes':[]},
        study_day=DAY)
    assert rep['same_run'] is True
    assert rep['formal_h145_blocked'] is True
    assert [x['股票代號'] for x in rep['radar']]==['2330','0050']
    assert all('禁止' in x['資格'] for x in rep['radar'])
    assert 'ETF' in rep['radar'][1]['先補的資料／禁止誤判']
    assert 'FADE_VETO' in rep['radar'][0]['H109原風控']
    assert rep['h126_trade_authority_changed'] is False

def test_h189_unmatched_run_keeps_research_hidden():
    report=build_h175_audit(diagnosis=pd.DataFrame(),
        tables={'waiting':pd.DataFrame([{'股票代號':'2330','股票名稱':'台積電'}])},
        h154_audit={'run_identity':'UNMATCHED','run_id':'stale','h145_status':'BLOCKED_PARTIAL_EXPORT_NO_TRUSTED_RECOVERY'},
        study_day=DAY)
    assert report['radar']==[]
    assert report['same_run'] is False

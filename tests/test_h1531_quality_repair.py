# -*- coding: utf-8 -*-
"""H153.1 regression: UNKNOWN is not VETO and raw official PE/EPS ≠ peer discount."""
from datetime import date
from pathlib import Path
import pandas as pd
from godpick_h1531_quality_repair import classify_h109,liquidity_reasons,source_coverage
from godpick_h153_growth_setup_rank import build_growth_setup_ranking,attach_rank_to_research,build_ranked_five_plans
from godpick_h115_pit_evidence import append_valuations,build_h114_evidence
from test_h153_growth_technical_ranking import diagnostic, DAY


def test_h109_tristate_and_conflict_never_h126_authorize():
    assert classify_h109('UNKNOWN')=='PENDING_EVIDENCE'
    assert classify_h109(None)=='PENDING_EVIDENCE'
    assert classify_h109('UNEXPECTED_NEW_STATE')=='PENDING_EVIDENCE'
    assert classify_h109('RESEARCH_ELIGIBLE')=='RESEARCH_ALLOWED'
    assert classify_h109('FADE_VETO')=='EXPLICIT_VETO'
    assert classify_h109('SHOCK_QUARANTINE')=='EXPLICIT_VETO'
    assert classify_h109('COOLDOWN_QUARANTINE')=='EXPLICIT_VETO'
    assert classify_h109('FAST_SHOCK_RECHECK')=='PENDING_EVIDENCE'
    assert classify_h109('EVIDENCE_CONFLICT')=='EVIDENCE_CONFLICT'


def test_h109_unknown_ranked_b_not_veto_c():
    f,s,m=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([
        diagnostic('2330',H109決策權威='UNKNOWN'),
        diagnostic('2317',H109決策權威='FADE_VETO')]),research_frame=pd.DataFrame(),research_day=DAY)
    assert f['股票代號'].tolist()==['2330','2317']
    assert f.iloc[0]['排名等級'].startswith('B')
    assert f.iloc[0]['H109證據分類']=='PENDING_EVIDENCE'
    assert '不是已否決' in f.iloc[0]['反證與待補']
    assert f.iloc[1]['排名等級'].startswith('C')
    assert m['formal_h126_modified'] is False


def test_veto_only_does_not_fill_top_five():
    frame=pd.DataFrame([diagnostic('2330',H109決策權威='FADE_VETO')])
    ranked,_,_=build_growth_setup_ranking(diagnosis_frame=frame,research_frame=pd.DataFrame(),research_day=DAY)
    top=build_ranked_five_plans(ranked=ranked,diagnosis_frame=frame,previous_plans=pd.DataFrame(),research_day=DAY)
    assert ranked.shape[0]==1 and ranked.iloc[0]['排名等級'].startswith('C')
    assert top.empty


def test_peer_insufficiency_keeps_verified_raw_pe_eps(tmp_path:Path):
    records=[dict(code='2330',asof_date='2026-11-02',market='上市',sector='半導體',
                  pe=10,eps_ttm=9.2,source='TWSE',available_at='2026-11-03T18:00:00+08:00',
                  source_url='https://www.twse.com.tw/zh/page/trading/exchange/BWIBBU.html',
                  availability_basis='PUBLISHED_VERIFIED')]
    append_valuations(tmp_path/'data'/'h115',records)
    evidence=build_h114_evidence(tmp_path,'2026-11-03T23:00:00+08:00')
    e=evidence['2330']
    assert e['H114本益比實值']==10 and e['H114近四季EPS實值']==9.2
    assert e['H115同業估值狀態']=='PEER_SAMPLE_INSUFFICIENT'
    assert 'H114同業本益比中位數_官方' not in e
    research,_,_=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([diagnostic()]),
       research_frame=pd.DataFrame(),research_day=DAY,verified_fundamentals=evidence)
    assert research.iloc[0]['本益比']==10
    assert research.iloc[0]['近四季EPS']==9.2
    assert pd.isna(research.iloc[0]['相對同業PE折價%'])
    assert '同業PE樣本不足' in research.iloc[0]['反證與待補']
    assert not research.iloc[0]['研究主題'].startswith('高成長低估')


def test_future_raw_valuation_not_shown_in_historical_rank():
    facts={'2330':{'H114本益比實值':8,'H114近四季EPS實值':10,
      'H114估值資料來源':'OFFICIAL_VERIFIED','H114估值資料日期':'2026-11-12'}}
    ranked,_,_=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([diagnostic()]),
      research_frame=pd.DataFrame(),research_day=DAY,verified_fundamentals=facts)
    assert ranked.loc[0,'本益比'] is None and ranked.loc[0,'近四季EPS'] is None


def test_independent_liquidity_reasons_multiple_missing_not_called_low():
    reasons=liquidity_reasons(amount=180,average=90,lots=None,ratio=.42)
    assert '20日均成交額百萬未達研究門檻' in reasons
    assert '成交量張缺來源' in reasons
    assert '量比未達研究門檻' in reasons
    assert '當日成交額百萬未達研究門檻' not in reasons


def test_coverage_is_truthful_not_read_ok():
    facts={'2330':{'H114月營收歷史':[{'year_month':f'2025{i:02d}'} for i in range(1,13)]},
           '2317':{'H114本益比實值':12,'H114近四季EPS實值':5}}
    cov=source_coverage(facts)
    assert cov['有至少12月已公告營收個股']==1
    assert cov['有官方原始PE_EPS個股']==1
    assert cov.get('有至少24月已公告營收個股',0)==0


def test_research_old_columns_are_aligned_with_same_day_diagnosis():
    diag=pd.DataFrame([diagnostic('2330')])
    rank,_,_=build_growth_setup_ranking(diagnosis_frame=diag,research_frame=pd.DataFrame(),research_day=DAY)
    old=pd.DataFrame([{'股票代號':'2330','H114行情日期':'2026-10-01',
       '當日成交額百萬':99,'成交量張':99,'量比':.22,'研究層級':'舊觀察'}])
    new=attach_rank_to_research(old,rank)
    assert new.iloc[0]['H114行情日期']==DAY
    assert new.iloc[0]['當日成交額百萬']==360
    assert new.iloc[0]['成交量張']==900
    assert new.iloc[0]['量比'] is not None and new.iloc[0]['量比']>1
    assert '原值' in new.iloc[0]['H153.1原值差異']
    assert '衝突' in new.iloc[0]['H153.1欄位對齊']

# -*- coding: utf-8 -*-
"""H148.1 adversarial tests: eliminate false top12 without any new trade authority."""
import pandas as pd

from godpick_h144_focus_board import build_focus_board
from godpick_h148_strength_provenance import build_strength_trace
from godpick_h1481_focus_gate import curate_focus_board, EXTRA_COLS

DAY = '2026-10-29'


def row(code, **extra):
    r = dict(股票代號=code,股票名稱='強勢測試'+code,類別='AI高效能運算',
        H110最終研究優先分=52,H110研究資訊截止日=DAY,
        H114K線證據日=DAY,H114K線資料狀態='VALID_OHLCV',H114K線有效根數=80,
        H113當日成交額百萬=310,H113二十日均成交額百萬=210,
        H113當日成交量張=2000,H113流動性合格='是',H114K線量比=1.75,
        H105發動階段='BH1',H108族群生命週期='IGNITION',
        H108族群前次資料日='2026-10-28',H108族群資金流變化=5,
        H108族群廣度變化=9,H108內部樣本可信度=80,
        H109決策權威='RESEARCH_ELIGIBLE',
        H123法人實證狀態='PENDING_EVIDENCE',H124大戶實證狀態='PENDING_EVIDENCE',
    )
    r['H114距20日高點%']=-1.5
    r['收盤距MA20%']=5
    r['收盤距MA60%']=8
    r.update(extra)
    return r


def run(rows, *, date=DAY, same_run=True):
    audit=pd.DataFrame(rows)
    auth=pd.DataFrame([{'股票代號':r['股票代號'],'H126能否買進':'否'} for r in rows])
    state={'status':'SCOPE_RECONCILED_FOR_DISPLAY',
       'scan_count':max(10,len(rows)),'diagnosis_count':len(rows),
       'reported_final':len(rows),'export_count':len(rows)}
    legacy, _=build_focus_board({'audit':audit,'h126_authority':auth},state,all_rows=True)
    trace,_,meta=build_strength_trace(candidate_frame=audit,audit_frame=audit,result_frame=audit,
        authority_frame=auth,scan_report={'total_count':max(10,len(rows)),
          'candidate_diagnosis_count':len(rows),'final_candidate_count':len(rows)},
        h145_bridge={'candidate_complete':same_run,'same_run_proven':same_run},
        research_day=date)
    main,hold,quality=curate_focus_board(legacy,audit,trace,research_day=date,trace_meta=meta)
    return main,hold,quality,trace,legacy


def test_real_breakout_visible_despite_same_h110_scores_and_all_research_only():
    main,hold,meta,trace,old=run([row('3035'),row('2344',H109決策權威='FADE_VETO')])
    assert len(old)==2 and set(old['H110研究分'])=={52}
    assert list(main['股票代號'])==['3035']
    assert list(hold['股票代號'])==['2344']
    assert all(main['買進資格']=='禁止依本榜買進')
    assert main.iloc[0]['觀察順位']=='同層並列｜非買進排名'
    assert '籌碼待補' in main.iloc[0]['H1481研究分層']
    assert meta['eligible']==1 and meta['quarantine']==1


def test_legacy_twelve_fade_zero_truly_strong_no_code_order_fake_ranking():
    rows=[row(str(1000+i),H109決策權威='FADE_VETO', H114K線證據日=None,
         H123法人實證狀態='PENDING_EVIDENCE',H124大戶實證狀態='PENDING_EVIDENCE') for i in range(12)]
    main,blocked,info,_,_=run(rows)
    assert main.empty and len(blocked)==12
    assert info['eligible']==0 and info['quarantine']==12
    assert '本輪0檔' in info['warning']
    assert blocked['H1481入榜／隔離依據'].str.contains('H109').all()


def test_old_or_missing_kline_never_passes_even_with_strong_score():
    main,blocked,_,_,_=run([row('3035',H110最終研究優先分=99,H114K線證據日='2026-10-28')])
    assert main.empty and 'K線' in blocked.iloc[0]['H1481入榜／隔離依據']


def test_insufficient_60_bars_is_quarantined():
    main,blocked,_,_,_=run([row('3035',H114K線有效根數=59)])
    assert main.empty and '不足60' in blocked.iloc[0]['H1481入榜／隔離依據']


def test_unverified_volume_and_liquidity_are_quarantined():
    main,blocked,_,_,_=run([row('3035',H113當日成交量張=300,H113流動性合格='否')])
    assert main.empty and '500張' in blocked.iloc[0]['H1481入榜／隔離依據']


def test_no_sector_breadth_or_prior_day_prevents_main_board():
    main,blocked,_,_,_=run([row('3035',H108族群前次資料日=None)])
    assert main.empty and 'H108' in blocked.iloc[0]['H1481入榜／隔離依據']


def test_overheated_tech_not_promoted_even_with_breakout():
    main,blocked,_,_,_=run([row('3035',**{'收盤距MA20%':16})])
    assert main.empty and '均線' in blocked.iloc[0]['H1481入榜／隔離依據']


def test_unknown_research_day_never_fabricates_current_market():
    main,blocked,info,_,_=run([row('3035')],date=None)
    assert main.empty and info['research_day']=='未核實'


def test_conflicting_duplicate_quarantined_not_cherry_picked():
    first=row('3035')
    second=row('3035',H110最終研究優先分=92)
    main,blocked,info,_,_=run([first,second])
    assert main.empty and len(blocked)==1
    assert '重複' in blocked.iloc[0]['H1481入榜／隔離依據']


def test_all_audit_rows_examined_before_top12_selection():
    rows=[row(str(2000+i),H109決策權威='FADE_VETO') for i in range(20)]
    rows += [row('3035',H110最終研究優先分=48)]
    main,blocked,info,_,_=run(rows)
    assert list(main['股票代號'])==['3035']
    assert info['full_audit']==21 and info['quarantine']==20


def test_missing_trace_or_conflicting_trace_is_fail_closed():
    raw=pd.DataFrame([row('3035')])
    board,_=build_focus_board({'audit':raw}, {},all_rows=True)
    main,blocked,info=curate_focus_board(board,raw,pd.DataFrame(),research_day=DAY)
    assert main.empty and len(blocked)==1
    assert 'H148' in blocked.iloc[0]['H1481入榜／隔離依據']


def test_second_H126_authority_never_modified():
    assert EXTRA_COLS == ('H1481研究分層','H1481有效量價','H1481主流與籌碼','H1481入榜／隔離依據')
    a=pd.DataFrame([{'股票代號':'3035','H126能否買進':'否'}]);original=a.copy(deep=True)
    main,hold,info,_,_=run([row('3035')])
    pd.testing.assert_frame_equal(a,original)
    assert '禁止依本榜買進' in main.iloc[0]['買進資格']

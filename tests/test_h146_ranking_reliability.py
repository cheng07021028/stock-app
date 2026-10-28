# -*- coding: utf-8 -*-
"""H146 behavioral regressions: ties, contradictory denominators, authority.

Synthetic rows; no real investments, network, token or durable writes.
"""
import ast
import io
import json
from pathlib import Path
import pandas as pd
from godpick_h144_focus_board import build_focus_board

ROOT=Path(__file__).resolve().parents[1]


def audit_row(c,score=52,**kw):
    r={'股票代號':str(c),'股票名稱':'測試股'+str(c),'類別':'測試產業',
       'H110最終研究優先分':score,'H110證據狀態':'PARTIAL｜來源/資料不足',
       'H110研究資訊截止日':'2026-10-28',
       'H109決策權威':'FADE_VETO','H123法人實證狀態':'PENDING_EVIDENCE',
       'H124大戶實證狀態':'PENDING_EVIDENCE','H105發動階段':'BH0｜WATCH'}
    r.update(kw);return r


def stock_rows(rows,authority=None,health=None,state=None,limit=12):
    tabs={'audit':pd.DataFrame(rows),
          'h126_authority':pd.DataFrame(authority or []),
          'h126_health':pd.DataFrame(health or [])}
    return build_focus_board(tabs,state or {},limit=limit)


def test_oct28_twelve_identical_scores_are_ties_not_fake_top12():
    ids=['1326','1717','2115','2317','2337','2344','2376','2395','3037','3044','3680','3702']
    r,m=stock_rows([audit_row(c) for c in ids],state={
        'status':'EXPORT_SCOPE_INCOMPLETE','scope_incomplete':True,
        'scan_count':0,'diagnosis_count':0,'reported_final':8,'export_count':1667})
    assert len(r)==12
    assert set(r['觀察順位'])=={'並列／無法區分'}
    assert set(r['H110研究分'])=={52}
    assert m['all_scores_tied'] and m['score_distinct']==1
    assert not m['ranking_usable'] and m['h126_buyable']==0
    assert any('掃描數為0' in s for s in m['scope_issues'])
    assert any('診斷數為0' in s for s in m['scope_issues'])
    assert (r['買進資格']=='禁止依本榜買進').all()
    assert (r['H126正式資格']=='未列入H126合格決策').all()


def test_realistic_diverse_score_competition_ranking_preserves_ties():
    r,m=stock_rows([audit_row('2317',72),audit_row('2344',72),audit_row('3037',64)],
        authority=[{'股票代號':c,'H126能否買進':'否'} for c in ('2317','2344','3037')],
        state={'status':'SCOPE_RECONCILED_FOR_DISPLAY','scan_count':3,'diagnosis_count':3,
               'reported_final':3,'export_count':3})
    assert list(r['觀察順位'])==['並列第1','並列第1',3]
    assert m['score_distinct']==2 and not m['all_scores_tied']
    assert m['ranking_usable'] and m['h126_buyable']==0


def test_no_scores_never_generate_fake_ranks():
    r,m=stock_rows([audit_row('2317',None),audit_row('2344','nan'),audit_row('3037',120)])
    assert (r['觀察順位']=='無分／不排序').all()
    assert m['valid_score_rows']==0 and m['score_distinct']==0
    assert not m['ranking_usable']


def test_conflicting_duplicate_scores_quarantined_not_picked_max():
    r,m=stock_rows([audit_row('2344',52),audit_row('2344',92)])
    assert len(r)==1 and pd.isna(r.loc[0,'H110研究分'])
    assert r.loc[0,'觀察順位']=='資料衝突／不排序'
    assert m['conflict_codes']==['2344']


def test_missing_then_valid_duplicate_retains_verifiable_score():
    r,m=stock_rows([audit_row('2344',None),audit_row('2344',49)])
    assert r.loc[0,'H110研究分']==49


def test_conflicting_risk_never_turned_into_allow():
    r,_=stock_rows([audit_row('2344',49,H109決策權威='ALLOW'),
                    audit_row('2344',49,H109決策權威='FADE_VETO')])
    assert '衝突' in r.loc[0,'H109風控']
    assert r.loc[0,'買進資格']=='禁止依本榜買進'


def test_h102_legacy_not_used_to_fill_missing_h110():
    r,m=build_focus_board({'audit':pd.DataFrame([audit_row('2344',None)]),
        'legacy_priority_overview':pd.DataFrame([{'股票代號':'2344','H102動態優先分':99}])}, {})
    assert pd.isna(r.loc[0,'H110研究分'])
    assert not m['ranking_usable']


def test_missing_formal_authority_cannot_be_replaced_by_audit_rows():
    r,m=stock_rows([audit_row('2317',80)], authority=[{'股票名稱':'無合格推薦','H126能否買進':'禁止買進'}])
    assert m['authority_count']==0 and m['h126_buyable']==0
    assert '未提供可識別合格個股' in m['warning']


def test_contradictory_authority_summary_is_flagged():
    r,m=stock_rows([audit_row('2317',80)],
       authority=[{'股票代號':'2317','H126能否買進':'否'}],
       health=[{'項目':'H126授權進場檔數','數值':3}],
       state={'status':'SCOPE_RECONCILED_FOR_DISPLAY', 'scan_count':1,
              'diagnosis_count':1,'reported_final':1,'export_count':1})
    assert m['h126_buyable']==0 and not m['ranking_usable']
    assert any('H126摘要數' in a for a in m['scope_issues'])


def test_invalid_code_rejected_including_etf_like_codes():
    r,_=stock_rows([audit_row('ABCD',98),audit_row('0050',96),audit_row('2344',52)])
    assert list(r['股票代號'])==['2344']


def test_limit_does_not_choose_tie_breaks_as_real_priority():
    r,m=stock_rows([audit_row(str(1000+i),52) for i in range(15)],limit=12)
    assert len(r)==12
    assert set(r['觀察順位'])=={'並列／無法區分'}
    assert not m['ranking_usable']


def test_single_candidate_rank_still_local_only():
    r,m=stock_rows([audit_row('2344',49.75)],state={
       'scope_incomplete':True,'status':'EXPORT_SCOPE_INCOMPLETE',
       'scan_count':1718,'diagnosis_count':1665,'reported_final':8,'export_count':1})
    assert r.loc[0,'觀察順位']==1
    assert not m['ranking_usable'] and '不能' in m['warning']


def test_page_writer_is_exactly_two_sheets_and_quality_header_wired():
    source=(ROOT/'pages/7_股神推薦.py').read_text('utf-8')
    assert source.count('("00_主管重點觀察榜", h144_focus,')==1
    assert source.count('("00_H126主管決策", tables')==1
    assert "'h146_focus_quality'" in source
    assert "_h146_tie = h144_focus_meta['all_scores_tied']" in source
    assert 'ws.insert_rows(1, amount=6)' in source
    assert "ws.freeze_panes = 'D8'" in source
    assert 'StockPick_H146_QualityFocus_' in source
    assert 'sig_raw = f"H146|H86|' in source


def test_no_network_or_mutation_in_pure_board():
    source=(ROOT/'godpick_h144_focus_board.py').read_text('utf-8')
    assert 'requests.' not in source and 'http.' not in source
    assert 'st.session_state' not in source


def test_contradictory_formal_authority_rows_do_not_count_as_buyable():
    r,m=stock_rows([audit_row('2344',75)],
       authority=[{'股票代號':'2344','H126能否買進':'否','H126權威層別':'隔離'},
                  {'股票代號':'2344','H126能否買進':'是','H126權威層別':'A'}],
       state={'status':'SCOPE_RECONCILED_FOR_DISPLAY','scan_count':1,
              'diagnosis_count':1,'reported_final':1,'export_count':1})
    assert m['h126_buyable']==0 and not m['ranking_usable']
    assert any('H126同代號決策衝突' in s for s in m['scope_issues'])
    assert '矛盾' in r.loc[0,'H126正式資格']


def test_conflicting_duplicate_risk_does_not_keep_rank():
    r,m=stock_rows([audit_row('2344',75,H109決策權威='ALLOW'),
                    audit_row('2344',75,H109決策權威='FADE_VETO'),
                    audit_row('2317',61)],
       authority=[{'股票代號':'2344','H126能否買進':'否'},
                  {'股票代號':'2317','H126能否買進':'否'}],
       state={'status':'SCOPE_RECONCILED_FOR_DISPLAY','scan_count':2,
              'diagnosis_count':2,'reported_final':2,'export_count':2})
    assert r.loc[r['股票代號']=='2344','觀察順位'].iloc[0]=='資料衝突／不排序'
    assert pd.isna(r.loc[r['股票代號']=='2344','H110研究分'].iloc[0])
    assert r.loc[r['股票代號']=='2317','觀察順位'].iloc[0]==1


def test_missing_funnel_counts_do_not_prove_full_market_rank():
    r,m=stock_rows([audit_row('2317',72),audit_row('2344',50)],
       authority=[{'股票代號':'2317','H126能否買進':'否'}],
       state={'status':'SCOPE_RECONCILED_FOR_DISPLAY'})
    assert any('掃描漏斗沒有可核實筆數' in x for x in m['scope_issues'])
    assert not m['ranking_usable']

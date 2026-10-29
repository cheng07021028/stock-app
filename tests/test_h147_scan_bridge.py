# -*- coding: utf-8 -*-
"""Reproduce H146 false zero by replaying the real governance schema."""
from pathlib import Path
from godpick_h147_scan_bridge import normalize_scan_report
from godpick_h145_core_bridge import restore_export_pool
from godpick_h144_scope_challenge import audit_h144
import pandas as pd


def test_real_chinese_scan_quality_counts_map_exactly():
    r,diag=normalize_scan_report({'預計掃描數':1718,'完整候選診斷數':1665,
       '最終作戰候選數':8,'通過推薦前置篩選數':1665,'正式推薦可用':False})
    assert (r['total_count'],r['candidate_diagnosis_count'],r['final_candidate_count'])==(1718,1665,8)
    assert diag['status']=='SCHEMA_RECONCILED'
    assert r['正式推薦可用'] is False
    assert not diag['run_id_proven']


def test_h144_unbroken_funnel_after_schema_fix():
    r,_=normalize_scan_report({'預計掃描數':1718,'完整候選診斷數':1665,'最終作戰候選數':8})
    source=pd.DataFrame([{'股票代號':str(i)} for i in range(1000,1100)])
    stat,*_=audit_h144(scan_report=r,candidate_frame=source,result_frame=pd.DataFrame(),
                        sector_frame=pd.DataFrame(), tables={}, research_day='2026-10-28')
    assert stat['scan_count']==1718
    assert stat['diagnosis_count']==1665
    assert stat['reported_final']==8
    assert stat['scope_incomplete'] # 100/1665 still genuinely incomplete


def test_h145_exact_same_run_still_required_even_with_chinese_count_bridge():
    r,_=normalize_scan_report({'預計掃描數':3,'完整候選診斷數':3,'最終作戰候選數':1})
    full=pd.DataFrame([{'股票代號':c} for c in ('2317','2344','3037')])
    one=full.iloc[:1]
    recovered,meta=restore_export_pool(one,full,one,scan_report=r,
                context_run_id='run1',scan_run_id='run1',decision_signature='abc',active_signature='abc')
    assert len(recovered)==3 and meta['candidate_complete'] and meta['recovered']
    unsafe,blocked=restore_export_pool(one,full,one,scan_report=r,
                context_run_id='run1',scan_run_id='run2',decision_signature='abc',active_signature='abc')
    assert len(unsafe)==1 and not blocked['candidate_complete']


def test_schema_conflict_fails_closed_and_reports_both_values():
    r,diag=normalize_scan_report({'預計掃描數':1718,'total_count':19,'完整候選診斷數':1665})
    assert 'total_count' not in r
    assert any('CONFLICT_total_count' in x for x in diag['issues'])
    assert diag['status']=='SCAN_SCHEMA_NEEDS_REVIEW'


def test_empty_inputs_do_not_mint_a_market_denominator():
    r,diag=normalize_scan_report({'完整候選診斷數':1665})
    assert 'total_count' not in r
    assert diag['status']=='SCAN_SCHEMA_NEEDS_REVIEW'


def test_export_uses_bridge_before_h145_scanning_without_modifying_qualifications():
    page=(Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py').read_text('utf-8')
    assert page.index('_h147_normalize_scan(scan_report)') < page.index('candidate_for_core, h145_bridge')
    assert 'tables = _h126_attach_authority(tables)' in page

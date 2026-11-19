# -*- coding: utf-8 -*-
"""H174: real zero A/B research must export; corrupt nonzero ranks still fail closed."""
from __future__ import annotations
import io
import pandas as pd
import pytest
from openpyxl import load_workbook
from test_h1537_manager_visible import make_workbook
# Extract the unchanged real H84 writer ONCE, rather than reparsing the
# 18,000-line page for every fixture (several seconds per test).
import ast
import json
from functools import lru_cache
from pathlib import Path

@lru_cache(maxsize=1)
def _actual_page_writer():
    page=Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py'
    tree=ast.parse(page.read_text('utf-8'))
    fns=[f for f in tree.body if isinstance(f,ast.FunctionDef) and
         f.name in ('_excel_safe_value','_excel_safe_df','_write_df_to_ws_fast_h84')]
    ns={'pd':pd,'json':json}
    exec(compile(ast.fix_missing_locations(ast.Module(body=fns,type_ignores=[])),str(page),'exec'),ns,ns)
    return ns['_write_df_to_ws_fast_h84']

def _writer(wb,name,frame,fallback):
    return _actual_page_writer()(wb,name,frame,fallback)
from godpick_h151_workbook_sections import add_research_section_sheets, CORE_TABS
from godpick_h1537_manager_visible import render_manager_visible,validate_exported_cross_tab
from godpick_h154_export_parity import (reconcile_run,append_audit_summary,
                                        append_audit_and_overview,verify_final_xlsx)
from godpick_h157_trader_audit import render_trader_audit


def _audit(ranked, watch, research):
    return reconcile_run(tables={'h126_authority':pd.DataFrame([
        {'H126能否買進':'禁止買進'}]), 'waiting':pd.DataFrame()},
        ranked=ranked,watch=watch,research=research,plans=pd.DataFrame(),
        six_summary=pd.DataFrame(),h145_status='BLOCKED_FINAL_RESULT_COUNT_GAP',
        run_id='h174-run',scan_run_id='h174-run')


def _build(n=0,w=0):
    wb,ranked,watch,research=make_workbook(n=n,w=w)
    audit=_audit(ranked,watch,research)
    shown=render_manager_visible(wb,ranked=ranked,watch=watch,research=research,
                                  h126_buyable=0,research_day='2026-11-19',
                                  h145_status='BLOCKED_FINAL_RESULT_COUNT_GAP')
    append_audit_and_overview(wb,audit)
    summary=append_audit_summary(pd.DataFrame(columns=['檢查項目','結果','說明']),audit)
    count=add_research_section_sheets(wb,writer=_writer,research=research,
                                      research_summary=summary)
    assert validate_exported_cross_tab(wb,shown)
    return wb,audit,shown,count


def test_h174_original_exception_zero_research_is_now_exportable():
    wb,audit,shown,count=_build(0,0)
    ws=wb['01_具名強勢研究']
    assert ws['A1'].value=='狀態'
    assert str(ws['A2'].value).startswith('本輪沒有')
    assert {'研究排名','股票代號','研究證據分','排名等級'}.issubset(
        {cell.value for cell in ws[1]})
    assert count['01_具名強勢研究']==0
    assert verify_final_xlsx(wb,audit)
    assert wb['00_H126主管決策']['G2'].value=='禁止買進'
    assert audit['h126_buyable']==0 and not audit['ranked_codes']
    assert len(wb.sheetnames)==12 and wb.sheetnames[:2]==list(CORE_TABS)


def test_h174_empty_export_preserves_all_14_tabs_after_h157():
    wb,audit,_,_=_build(0,0)
    render_trader_audit(wb,research_day='2026-11-19',h126_buyable=0,
                        h145_status='BLOCKED_FINAL_RESULT_COUNT_GAP',run_id='h174-run')
    assert len(wb.sheetnames)==14
    assert wb.sheetnames[-2:]==['11_交易員重點導航','12_逐股分類與反證']
    out=io.BytesIO();wb.save(out)
    saved=load_workbook(io.BytesIO(out.getvalue()))
    assert saved.sheetnames==wb.sheetnames
    assert verify_final_xlsx(saved,audit)
    assert saved['00_H126主管決策']['G2'].value=='禁止買進'
    assert saved['01_具名強勢研究']['A2'].value.startswith('本輪沒有')


def test_h174_watch_only_not_counted_as_ranked_trade():
    wb,audit,shown,count=_build(0,2)
    assert shown['a_b_rows']==0 and shown['w_rows']==2
    assert audit['ranked_codes']==[]
    assert count['01_具名強勢研究']==2
    assert verify_final_xlsx(wb,audit)
    assert wb['00_H126主管決策']['G2'].value=='禁止買進'


def test_h174_nonempty_rank_still_crosschecks_exactly():
    wb,audit,shown,count=_build(3,1)
    assert shown['a_b_rows']==3 and shown['w_rows']==1
    assert count['01_具名強勢研究']==4
    assert verify_final_xlsx(wb,audit)
    assert wb['00_H126主管決策']['G2'].value=='禁止買進'


@pytest.mark.parametrize('col,bad',[
    ('研究排名',None),('研究排名','not-a-rank'),
    ('研究證據分',None),('股票代號',''),
])
def test_h174_missing_required_nonempty_evidence_is_blocked(col,bad):
    wb,audit,_,_=_build(3,0)
    ws=wb['01_具名強勢研究']
    headers=[cell.value for cell in ws[1]]
    ws.cell(2,headers.index(col)+1).value=bad
    with pytest.raises(ValueError,match='第2列 A/B|主管00與01'):
        verify_final_xlsx(wb,audit)


def test_h174_missing_header_in_nonempty_list_is_not_silently_accepted():
    wb,audit,_,_=_build(2,0)
    ws=wb['01_具名強勢研究']
    headers=[cell.value for cell in ws[1]]
    ws.cell(1,headers.index('研究排名')+1,'錯誤欄位')
    with pytest.raises(ValueError,match='H154研究表缺少：研究排名'):
        verify_final_xlsx(wb,audit)


def test_h174_legacy_empty_marker_allowed_only_with_zero_ranked():
    wb,audit,_,_=_build(0,0)
    ws=wb['01_具名強勢研究']
    ws.delete_cols(2, ws.max_column-1)
    assert verify_final_xlsx(wb,audit)
    ws['A2']='其他錯誤訊息'
    with pytest.raises(ValueError,match='H154研究表缺少：研究排名'):
        verify_final_xlsx(wb,audit)


def test_h174_no_fictitious_stock_in_placeholder_row():
    wb,audit,_,_=_build(0,0)
    ws=wb['01_具名強勢研究']
    header=[c.value for c in ws[1]]
    for field in ('股票代號','研究排名','研究證據分','排名等級'):
        assert ws.cell(2,header.index(field)+1).value in (None, "")
    assert audit['ranked']==0 and audit['h126_buyable']==0

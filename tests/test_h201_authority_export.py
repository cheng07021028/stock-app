import io
import pandas as pd
import pytest
from openpyxl import load_workbook
from godpick_h126_authority_report import build_authority
from godpick_h1537_manager_visible import render_manager_visible, validate_manager_content, validate_zero_authority, validate_exported_cross_tab
from godpick_h154_export_parity import reconcile_run, append_audit_summary, verify_final_xlsx
from godpick_h151_workbook_sections import add_research_section_sheets
from test_h126_authority import stock, tab
from test_h1537_manager_visible import make_workbook
from test_h151_excel_split import _writer

@pytest.mark.parametrize('kind',['empty','pending','certified','mixed'])
def test_actual_producer_writer_roundtrip(kind):
    rows=[]
    if kind in ('pending','mixed'): rows.append(stock('2330',H113入榜資格='WATCH_ONLY'))
    if kind in ('certified','mixed'): rows.append(stock('2317'))
    frame=build_authority(tab(research=rows))['h126_authority']
    wb,ranked,watch,research=make_workbook(2,1)
    del wb['00_H126主管決策']
    _writer(wb,'00_H126主管決策',frame,'空')
    au=wb['00_H126主管決策']; original=list(au.values)
    report=render_manager_visible(wb,ranked=ranked,watch=watch,research=research,h126_buyable=0)
    assert validate_manager_content(wb,report,research=research)
    plans=ranked[['股票代號','研究排名']]
    audit=reconcile_run(tables={'h126_authority':frame},ranked=ranked,watch=watch,research=research,plans=plans,six_summary=[],h145_status='BLOCKED_PARTIAL_EXPORT_NO_TRUSTED_RECOVERY')
    summary=append_audit_summary(None,audit)
    add_research_section_sheets(wb,writer=_writer,research=research,research_summary=summary,opportunities=plans)
    assert validate_exported_cross_tab(wb,report)
    assert verify_final_xlsx(wb,audit)
    buf=io.BytesIO();wb.save(buf);again=load_workbook(io.BytesIO(buf.getvalue()))
    assert verify_final_xlsx(again,audit)
    assert list(again['00_H126主管決策'].values)[:len(original)] == [tuple(None if v=='' else v for v in r) for r in original]
    assert audit['h126_buyable']==0

@pytest.mark.parametrize('bad',['可以買進','允許買進','開放交易','禁止買進但可以買進',None,'未知','=1'])
def test_late_contradiction_not_hidden(bad):
    wb,_,_,_=make_workbook(0,0)
    au=wb['00_H126主管決策']
    au.append([1,'2330','測試','','','','禁止買進','PENDING_EVIDENCE'])
    au.append([2,'2317','測試','','','',bad,'PENDING_EVIDENCE'])
    with pytest.raises(ValueError,match='第4列'):
        validate_zero_authority(wb)

def test_note_only_is_not_evidence():
    wb,_,_,_=make_workbook(0,0)
    au=wb['00_H126主管決策'];au.delete_rows(2)
    render_manager_visible(wb,h126_buyable=0)
    with pytest.raises(ValueError,match='沒有權威資料列'):
        validate_zero_authority(wb)

def test_header_based_not_g2():
    wb,_,_,_=make_workbook(0,0)
    au=wb['00_H126主管決策'];au.insert_cols(3)
    assert validate_zero_authority(wb)
    au['H1']='錯誤欄位'
    with pytest.raises(ValueError,match='權威欄位'):
        validate_zero_authority(wb)

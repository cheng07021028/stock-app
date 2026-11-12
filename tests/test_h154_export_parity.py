# -*- coding: utf-8 -*-
"""H154: no false equivalence between system H85 and H153 Excel research."""
from datetime import date
import io
import pytest
import pandas as pd
from openpyxl import load_workbook
from test_h1537_manager_visible import make_workbook
from test_h151_excel_split import _writer
from godpick_h151_workbook_sections import add_research_section_sheets
from godpick_h1537_manager_visible import render_manager_visible,validate_exported_cross_tab
from godpick_h1539_mainstream_freshness import audit_mainstream_repetition
from godpick_h154_export_parity import (reconcile_run,append_audit_summary,
    append_audit_and_overview,tag_six_pillar_stage,verify_final_xlsx)


def test_current_run_pool_concepts_never_mixed():
    tables={'h126_authority':pd.DataFrame([{'H126能否買進':'禁止買進','H126認證資格':'NO_QUALIFIED'}]),
            'waiting':pd.DataFrame([{'股票代號':'4566','股票名稱':'時碩工業'},{'股票代號':'6949','股票名稱':'沛爾生醫-創'},{'股票代號':'6240','股票名稱':'松崗'}]),
            'emerging_watch':pd.DataFrame([{'股票代號':'7867'}]),'research':pd.DataFrame()}
    ranks=pd.DataFrame([{'排名等級':'B｜待補證','股票代號':'2254'}])
    a=reconcile_run(tables=tables,ranked=ranks,watch=pd.DataFrame([{'股票代號':'2033'}]),
        research=ranks,plans=pd.DataFrame([{'股票代號':'2254'}]),six_summary=pd.DataFrame(),
        h145_status='BLOCKED_FINAL_RESULT_COUNT_GAP',run_id='runX',scan_run_id='runX')
    assert a['h126_buyable']==0 and a['h85_waiting']==3 and a['ranked']==1
    assert a['h85_emerging']==1 and a['watch']==1
    assert a['status']=='FORMAL_BLOCKED_H145' and a['run_identity']=='MATCHED'
    assert a['h85_waiting_codes']==['4566','6949','6240']
    assert a['h85_waiting_named'][0]==('4566','時碩工業')
    b=reconcile_run(tables=tables,ranked=ranks,watch=[],research=ranks,plans=[],six_summary=[],
        h145_status='',run_id='A',scan_run_id='B')
    assert b['status']=='RUN_CONFLICT'
    b['status']='RUN_CONFLICT'


def test_repeated_higher_raw_score_has_visible_lower_adjusted_score():
    r=pd.DataFrame([
        {'研究排名':1,'排名等級':'B｜研究證據待補','研究證據分':37,'股票代號':'2618','股票名稱':'長榮航','反證與待補':'H126禁止買進'},
        {'研究排名':2,'排名等級':'B｜研究證據待補','研究證據分':31,'股票代號':'2002','股票名稱':'中鋼','反證與待補':'H126禁止買進'}])
    diag=pd.DataFrame([{'股票代號':code,'H114K線證據日':'2026-11-12',
        'H114K線資料狀態':'VALID_OHLCV','類別':'航運', 'H109決策權威':'UNKNOWN'} for code in ('2618','2002')])
    hist=[{'date':'2026-11-09','rows':[{'股票代號':'2618'}]},
          {'date':'2026-11-10','rows':[{'股票代號':'2618'}]}]
    result,_,_=audit_mainstream_repetition(r,diag,research_day='2026-11-12',snapshots=hist)
    assert list(result['股票代號'])==['2002','2618']
    late=result[result['股票代號']=='2618'].iloc[0]
    assert late['研究證據分']==37 and late['H154研究排序分']==30
    assert '調整-7分' in late['H154順位調整說明']
    assert late['研究排名']==2


def test_actual_excel_manager_01_03_h126_and_h85_lanes_roundtrip():
    wb,ranked,watch,research=make_workbook(n=3,w=2,trade=0)
    ranked['H154研究排序分']=[60,59,58]
    ranked['H154順位調整說明']='原分不調整'
    research=pd.concat([ranked,watch],ignore_index=True)
    plans=pd.DataFrame([{'股票代號':c,'研究排名':i} for i,c in enumerate(ranked['股票代號'],1)])
    report=render_manager_visible(wb,ranked=ranked,watch=watch,research=research,
                                  h126_buyable=0,research_day='2026-11-12')
    a=reconcile_run(tables={'waiting':pd.DataFrame([{'股票代號':'4566'}]),
          'h126_authority':pd.DataFrame([{'H126能否買進':'禁止買進'}])},
          ranked=ranked,watch=watch,research=research,plans=plans,
          six_summary=pd.DataFrame([{'研究面向':'交易機會','本輪事實':'13檔'}]),
          h145_status='BLOCKED_FINAL_RESULT_COUNT_GAP',run_id='run1',scan_run_id='run1')
    assert append_audit_and_overview(wb,a)
    assert '等待/雷達 1 檔' in wb['00_主管重點觀察榜']['B4'].value
    audit=append_audit_summary(pd.DataFrame(columns=['檢查項目','結果','說明']),a)
    assert 'H154系統H85等待具名清單' in audit['檢查項目'].tolist()
    six=tag_six_pillar_stage(pd.DataFrame([{'研究面向':'交易機會','本輪事實':'13檔'}]))
    add_research_section_sheets(wb,writer=_writer,research=research,research_summary=audit,
                                opportunities=plans,six_pillar_summary=six)
    assert validate_exported_cross_tab(wb,report)
    assert verify_final_xlsx(wb,a)
    b=io.BytesIO();wb.save(b);again=load_workbook(io.BytesIO(b.getvalue()))
    assert verify_final_xlsx(again,a)
    assert 'H154階段口徑說明' in [again['04_六維研究摘要'].cell(i,1).value for i in range(2,again['04_六維研究摘要'].max_row+1)]
    assert len(again.sheetnames)==12
    assert again['00_H126主管決策']['G2'].value=='禁止買進'
    assert '排序60' in str(again['00_主管重點觀察榜']['E9'].value)
    ph=[c.value for c in again['03_五檔研究計畫'][1]]
    again['03_五檔研究計畫'].cell(2,ph.index('股票代號')+1).value='9999'
    with pytest.raises(ValueError,match='五檔計畫'):
        verify_final_xlsx(again,a)

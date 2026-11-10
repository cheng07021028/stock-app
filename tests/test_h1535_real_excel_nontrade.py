# -*- coding: utf-8 -*-
"""End-to-end export smoke: zero official recommendations but a useful audited list."""
import io
import pandas as pd
from openpyxl import Workbook,load_workbook
from godpick_h151_workbook_sections import CORE_TABS,TABS_COLOR,add_research_section_sheets
from godpick_h1512_excel_integrity import normalize_frozen_sheet_views
from godpick_h1535_research_recovery import build_verification_watchlist,merge_nontrading_watchlist
from godpick_h153_growth_setup_rank import build_ranked_five_plans
from test_h151_excel_split import _writer
from test_h1535_zero_recommendation_recovery import _stock,DAY


def test_zero_formal_still_produces_named_readonly_research_in_12_tabs():
    src=pd.DataFrame([_stock('2618'),_stock('1795'),_stock('2382')])
    watch,stats,meta=build_verification_watchlist(src,research_day=DAY)
    assert len(watch)==3
    research=merge_nontrading_watchlist(pd.DataFrame(),watch)
    assert len(research)==3
    plans=build_ranked_five_plans(ranked=pd.DataFrame(),diagnosis_frame=src,
          previous_plans=pd.DataFrame(),research_day=DAY)
    assert plans.empty  # H153.1 C-veto safety contract remains in force
    wb=Workbook();wb.active.title=CORE_TABS[0];authority=wb.create_sheet(CORE_TABS[1])
    wb[CORE_TABS[0]]['A1']='主管非交易觀察';wb[CORE_TABS[0]].freeze_panes='D8'
    authority['A1']='股票代號';authority['B1']='H126能否買進'
    authority['A2']='未授權';authority['B2']='禁止買進'
    add_research_section_sheets(wb,writer=_writer,research=research,
             research_summary=stats,opportunities=plans)
    normalize_frozen_sheet_views(wb)
    b=io.BytesIO();wb.save(b)
    out=load_workbook(io.BytesIO(b.getvalue()))
    assert len(out.sheetnames)==12
    assert out.sheetnames[:2]==list(CORE_TABS)
    assert out[CORE_TABS[1]]['B2'].value=='禁止買進'
    tab=out['01_具名強勢研究']
    heads=[c.value for c in tab[1]]
    assert '核查優先序' in heads and '交易資格' in heads
    assert tab.max_row==4
    index=heads.index('股票代號')+1
    code_set={str(tab.cell(i,index).value) for i in range(2,5)}
    assert code_set=={'2618','1795','2382'}
    trade_col=heads.index('交易資格')+1
    assert all('禁止買進' in str(tab.cell(i,trade_col).value) for i in range(2,5))
    assert out['03_五檔研究計畫']['A2'].value.startswith('本輪沒有')
    assert any('H153.5補證優先觀察候選總數' in str(c.value)
           for row in out['02_研究召回統計'] for c in row)
    for name in out.sheetnames:
        assert out[name].sheet_properties.tabColor.rgb[-6:]==TABS_COLOR[name]

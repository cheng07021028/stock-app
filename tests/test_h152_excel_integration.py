# -*- coding: utf-8 -*-
"""Actual H151.2 12-sheet Excel writer with H152 frames and no H126 mutation."""
from __future__ import annotations
import io
from zipfile import ZipFile
import pandas as pd
from openpyxl import Workbook,load_workbook
from godpick_h151_workbook_sections import add_research_section_sheets,CORE_TABS,TABS_COLOR
from godpick_h1512_excel_integrity import normalize_frozen_sheet_views
from godpick_h152_resilience_forecast import build_resilience_research
from test_h151_excel_split import _writer

DAY='2026-11-02'

def test_manager_export_retains_all_12_tabs_colored_and_h126_under_anti_fall_data():
    raw={'股票代號':'3105','股票名稱':'研究樣本','類別':'半導體','市場別':'上櫃',
         'H114K線證據日':DAY,'H114前次交易日':'2026-10-30',
         'H114K線有效根數':65,'H114K線資料狀態':'VALID_OHLCV',
         'H114前日收盤價':100,'H114當日收盤價':100,
         'H114當日漲跌幅%':0,'H114當日成交量張':4000,
         'H114K線量比':1.25,'H113當日成交額百萬':500,
         'H113二十日均成交額百萬':180,'H109決策權威':'FADE_VETO'}
    signals,ext,extra,meta=build_resilience_research(
        diagnosis_frame=pd.DataFrame([raw]),research_frame=pd.DataFrame(),
        research_day=DAY,market_snapshot={'data_date':DAY,'twse_change_pct':-2,
        'otc_change_pct':-3,'source':'same-day benchmark'})
    assert len(signals)==1 and len(ext)==1
    wb=Workbook();wb.active.title=CORE_TABS[0]
    formal=wb.create_sheet(CORE_TABS[1]);formal['A1']='H126授權';formal['A2']='禁止買進'
    wb[CORE_TABS[0]]['A1']='主管原有總覽'
    wb[CORE_TABS[0]].freeze_panes='D8'
    add_research_section_sheets(wb,writer=_writer,
        research=ext, research_summary=extra,
        opportunities=pd.DataFrame([{'股票代號':'3105','H152隔日情境':'中性觀察',
                                       '買進授權':'禁止買進'}]))
    normalize_frozen_sheet_views(wb)
    buf=io.BytesIO();wb.save(buf);data=buf.getvalue()
    with ZipFile(io.BytesIO(data)) as z:
        assert z.testzip() is None
        assert len([n for n in z.namelist() if n.startswith('xl/worksheets/sheet') and n.endswith('.xml')])==12
    restored=load_workbook(io.BytesIO(data))
    assert len(restored.sheetnames)==12
    assert restored.sheetnames[:2]==list(CORE_TABS)
    assert restored[CORE_TABS[1]]['A2'].value=='禁止買進'
    research=restored['01_具名強勢研究']
    header=[c.value for c in research[1]]
    assert '相對強度pp' in header and '隔日方向情境' in header
    assert research.cell(2,header.index('股票代號')+1).value=='3105'
    assert 'H109' in research.cell(2,header.index('隔日方向情境')+1).value
    assert 'H152抗跌候選總數' in [c.value for row in restored['02_研究召回統計'].iter_rows() for c in row]
    assert restored['03_五檔研究計畫']['C2'].value=='禁止買進'
    for name in restored.sheetnames:
        assert restored[name].sheet_properties.tabColor.rgb[-6:]==TABS_COLOR[name]
    assert research.freeze_panes=='A2'

# -*- coding: utf-8 -*-
"""H153.1 real XLSX: 12 sheets, nonfabricated research vs H126 trade gate."""
import io
from pathlib import Path
import pandas as pd
from openpyxl import Workbook,load_workbook
from godpick_h151_workbook_sections import add_research_section_sheets,CORE_TABS,TABS_COLOR
from godpick_h1512_excel_integrity import normalize_frozen_sheet_views
from godpick_h153_growth_setup_rank import build_growth_setup_ranking,attach_rank_to_research,build_ranked_five_plans
from tools.h1531_verify_deploy import verify
from test_h151_excel_split import _writer
from test_h153_growth_technical_ranking import diagnostic,DAY


def test_cumulative_deploy_preflight():
    assert not verify(Path(__file__).resolve().parents[1])


def test_report_presents_unknown_as_pending_not_veto_in_colored_twelve_sheets():
    d=pd.DataFrame([diagnostic('2330',H109決策權威='UNKNOWN'),
                    diagnostic('2317',H109決策權威='FADE_VETO')])
    ranked,summary,meta=build_growth_setup_ranking(diagnosis_frame=d,research_frame=pd.DataFrame(),research_day=DAY)
    research=attach_rank_to_research(pd.DataFrame(),ranked)
    plans=build_ranked_five_plans(ranked=ranked,diagnosis_frame=d,previous_plans=pd.DataFrame(),research_day=DAY)
    assert len(research)==2 and len(plans)==1
    assert plans.loc[0,'股票代號']=='2330'
    assert '2317' not in plans['股票代號'].tolist()
    wb=Workbook();wb.active.title=CORE_TABS[0];h126=wb.create_sheet(CORE_TABS[1]);
    wb[CORE_TABS[0]]['A1']='研究總覽';wb[CORE_TABS[0]].freeze_panes='D8'
    h126['A1']='股票代號';h126['B1']='正式資格';h126['A2']='2330';h126['B2']='禁止買進'
    add_research_section_sheets(wb,writer=_writer,research=research,
         opportunities=plans,research_summary=summary)
    normalize_frozen_sheet_views(wb)
    buf=io.BytesIO();wb.save(buf)
    actual=load_workbook(io.BytesIO(buf.getvalue()))
    assert len(actual.sheetnames)==12 and actual.sheetnames[:2]==list(CORE_TABS)
    assert actual[CORE_TABS[1]]['B2'].value=='禁止買進'
    rs=actual['01_具名強勢研究']
    headers=[x.value for x in rs[1]]
    assert 'H109證據分類' in headers and 'H153.1欄位對齊' in headers
    gate=actual['03_五檔研究計畫']
    values=list(gate.values)
    assert any('2330' in row for row in values)
    assert not any('2317' in row for row in values)
    for n in actual.sheetnames:
        assert actual[n].sheet_properties.tabColor.rgb[-6:]==TABS_COLOR[n]

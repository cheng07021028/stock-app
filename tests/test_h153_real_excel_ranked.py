# -*- coding: utf-8 -*-
"""H153 manager production Excel contracts; 12-sheet, color, H126 immutable."""
import io
from pathlib import Path
import pandas as pd
from openpyxl import Workbook, load_workbook
from godpick_h151_workbook_sections import add_research_section_sheets,CORE_TABS,TABS_COLOR
from godpick_h1512_excel_integrity import normalize_frozen_sheet_views
from godpick_h153_growth_setup_rank import (build_growth_setup_ranking, attach_rank_to_research,
                                              build_ranked_five_plans)
from test_h151_excel_split import _writer
from test_h153_growth_technical_ranking import diagnostic, DAY


def test_real_excel_has_ranked_nominations_without_change_to_h126():
    diag=pd.DataFrame([diagnostic('2454',H114量縮墊高='是'),
                       diagnostic('2330'),diagnostic('2317')])
    before=pd.DataFrame([{'股票代號':'2330','研究層級':'P1｜整理'},
                         {'股票代號':'2454','研究層級':'B｜線索'},
                         {'股票代號':'2317','研究層級':'A｜已核'}])
    ranked,summary,meta=build_growth_setup_ranking(diagnosis_frame=diag,research_frame=before,research_day=DAY)
    research=attach_rank_to_research(before,ranked)
    plans=build_ranked_five_plans(ranked=ranked,diagnosis_frame=diag,previous_plans=pd.DataFrame(),research_day=DAY)
    assert research['研究排名'].tolist()==[1,2,2]
    assert plans['研究排名'].tolist()==[1,2,2]
    wb=Workbook(); wb.active.title=CORE_TABS[0];h126=wb.create_sheet(CORE_TABS[1])
    wb[CORE_TABS[0]]['A1']='主管總覽';wb[CORE_TABS[0]].freeze_panes='D8'
    h126['A1']='股票代號';h126['B1']='資格';h126['A2']='2454';h126['B2']='禁止買進'
    add_research_section_sheets(wb,writer=_writer,research=research,opportunities=plans,
                                research_summary=summary)
    normalize_frozen_sheet_views(wb)
    buf=io.BytesIO();wb.save(buf)
    restored=load_workbook(io.BytesIO(buf.getvalue()))
    assert len(restored.sheetnames)==12
    assert restored.sheetnames[:2]==list(CORE_TABS)
    assert restored['00_H126主管決策']['B2'].value=='禁止買進'
    research_sheet=restored['01_具名強勢研究']
    headers=[x.value for x in research_sheet[1]]
    assert headers[:5]==['研究排名','排名等級','研究證據分','股票代號','股票名稱']
    assert [research_sheet.cell(i,1).value for i in (2,3,4)]==[1,2,2]
    assert [research_sheet.cell(i,4).value for i in (2,3,4)]==['2454','2317','2330']
    plan_sheet=restored['03_五檔研究計畫']
    assert plan_sheet['A2'].value==1
    assert len([v for v in plan_sheet.iter_rows(min_row=2,values_only=True) if v[0] is not None])==3
    for name in restored.sheetnames:
        assert restored[name].sheet_properties.tabColor.rgb[-6:]==TABS_COLOR[name]


def test_page_wires_asof_ledger_and_rank_to_both_tabs():
    page=(Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py').read_text('utf-8')
    assert '_h153_pit_evidence(_h152_root, _h148_research_day)' in page
    assert '_h153_attach_research(h152_named_research, h153_ranked)' in page
    assert '_h153_five_plans(ranked=h153_ranked' in page
    assert "tables['h126_authority']" in page
    assert "_h1512_normalize_views(wb)" in page

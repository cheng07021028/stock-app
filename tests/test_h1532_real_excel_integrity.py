# -*- coding: utf-8 -*-
"""H153.2 actual Excel write/reload: stable 12 sheets, distinct priority tabs,
and explicit, non-tradable indicator research evidence."""
import io
import pandas as pd
from openpyxl import Workbook,load_workbook
from godpick_h151_workbook_sections import CORE_TABS,TABS_COLOR,add_research_section_sheets
from godpick_h1512_excel_integrity import normalize_frozen_sheet_views
from godpick_h114_parallel_alpha import analyze_history
from godpick_h153_growth_setup_rank import build_growth_setup_ranking,attach_rank_to_research,build_ranked_five_plans
from test_h1532_prebreakout_scope import bars,DAY
from test_h151_excel_split import _writer

def test_h1532_real_excel_has_indicators_and_no_authority_mutation():
    row=analyze_history(bars(),asof=DAY)
    row.update({'股票代號':'2330','股票名稱':'測試觀察股','類別':'電子','H109決策權威':'UNKNOWN',
                'H113當日成交額百萬':320,'H113二十日均成交額百萬':290})
    ranked,summary,meta=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([row]),
                 research_frame=pd.DataFrame(),research_day=DAY)
    assert meta['formal_h126_modified'] is False
    # If any research row is ranked, the anti-fall downgrade is still a research flag.
    research=attach_rank_to_research(pd.DataFrame(),ranked)
    plans=build_ranked_five_plans(ranked=ranked,diagnosis_frame=pd.DataFrame([row]),
                                  previous_plans=pd.DataFrame(),research_day=DAY)
    wb=Workbook();wb.active.title=CORE_TABS[0];gate=wb.create_sheet(CORE_TABS[1]);
    wb[CORE_TABS[0]]['A1']='主管主榜';wb[CORE_TABS[0]].freeze_panes='D8'
    gate['A1']='股票代號';gate['B1']='H126交易資格';gate['A2']='2330';gate['B2']='禁止買進'
    add_research_section_sheets(wb,writer=_writer,research=research,
                                opportunities=plans,research_summary=summary)
    normalize_frozen_sheet_views(wb)
    b=io.BytesIO();wb.save(b);read=load_workbook(io.BytesIO(b.getvalue()))
    assert len(read.sheetnames)==12
    assert read.sheetnames[:2]==list(CORE_TABS)
    assert read[CORE_TABS[1]]['B2'].value=='禁止買進'
    for name in read.sheetnames:
        assert read[name].sheet_properties.tabColor.rgb[-6:]==TABS_COLOR[name]
    headers=[c.value for c in read['01_具名強勢研究'][1]]
    assert 'H1532布林收斂' in headers
    assert 'H1532OBV量價背離' in headers
    assert 'H1532ADX' in headers
    assert 'H1532DMI方向' in headers
    assert any(x.value=='H153.2獨立布林收斂已核' for col in read['02_研究召回統計'].columns for x in col)

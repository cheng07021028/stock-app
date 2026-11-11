# -*- coding: utf-8 -*-
"""H153.6 real regression: unique W backfill, truthful board, H126 unchanged."""
from __future__ import annotations
import io
import pandas as pd
from openpyxl import Workbook, load_workbook
from godpick_h1535_research_recovery import build_verification_watchlist, merge_nontrading_watchlist
from godpick_h1536_manager_clarity import clarify_manager_board
from godpick_h151_workbook_sections import CORE_TABS, TABS_COLOR, add_research_section_sheets
from godpick_h1512_excel_integrity import normalize_frozen_sheet_views
from test_h1535_zero_recommendation_recovery import _stock, DAY
from test_h151_excel_split import _writer


def test_backfill_after_excluding_already_ranked_not_before_cap():
    rows=[_stock('2618'),_stock('1795'),_stock('2330'),_stock('8042'),_stock('2382')]
    watch,stats,meta=build_verification_watchlist(pd.DataFrame(rows),research_day=DAY,limit=3,
                                                existing_codes=['2618','1795'])
    assert len(watch)==3
    assert not set(watch['股票代號']).intersection({'2618','1795'})
    assert meta['total_watch']==5 and meta['shown_watch']==3
    assert meta['existing_research_overlap']==2
    assert watch['核查優先序'].iloc[0]==1
    assert stats.loc[stats['檢查項目']=='H153.6核查榜與既有研究重疊','結果'].iloc[0]==2
    assert watch['研究排名'].isna().all()
    assert all('禁止買進' in x for x in watch['交易資格'])


def test_all_eligible_cannot_manufacture_w_rank_or_buy():
    src=pd.DataFrame([_stock('2618'),_stock('1795')])
    w,_,m=build_verification_watchlist(src,research_day=DAY,existing_codes=['2618','1795'])
    assert w.empty and m['total_watch']==2 and m['shown_watch']==0
    assert merge_nontrading_watchlist(pd.DataFrame(),w).empty


def test_manager_summary_and_real_12_tab_excel_roundtrip():
    ranked=pd.DataFrame([{'研究排名':1,'排名等級':'B｜研究證據待補（不得交易）',
                         '股票代號':'2618','研究證據分':44},
                        {'研究排名':2,'排名等級':'C｜明確否決',
                         '股票代號':'1216','研究證據分':35}])
    watch,stats,meta=build_verification_watchlist(
        pd.DataFrame([_stock('2618'),_stock('1795'),_stock('2330')]),
        research_day=DAY,limit=3,existing_codes=['2618'])
    research=merge_nontrading_watchlist(pd.DataFrame([{'股票代號':'2618','研究排名':1,
        '排名等級':'B｜研究證據待補（不得交易）'}]),watch)
    wb=Workbook(); wb.active.title=CORE_TABS[0];wb.create_sheet(CORE_TABS[1]);
    ws=wb[CORE_TABS[0]]
    for r,k,v in [(1,'舊主榜','舊版內容'),(4,'分數品質','完整原始稽核'),(6,'完整說明','舊文字'*260)]:
        ws.cell(r,1,k);ws.cell(r,2,v)
    ws.freeze_panes='D8'
    wb[CORE_TABS[1]]['A1']='H126能否買進'
    wb[CORE_TABS[1]]['A2']='禁止買進'
    archive=clarify_manager_board(wb,ranked=ranked,watch=watch,
        research=research,diagnosis=pd.DataFrame([_stock('2618')]),
        h126_buyable=0,h145_bridge={'status':'BLOCKED_FINAL_RESULT_COUNT_GAP','expected_final':2},
        research_day=DAY,h115_ledger_rows={'revenue':1088,'valuation':0,'quarterly_margins':0})
    assert ws['A1'].value.startswith('H153.6')
    assert '正式買進 0' in ws['B1'].value
    assert '研究A/B 1' in ws['B4'].value
    assert 'H145：BLOCKED_FINAL_RESULT_COUNT_GAP' in ws['B6'].value
    assert len(ws['B6'].value)<450  # no invisible multi-KB summary
    assert any('舊文字' in str(x) for x in archive['結果'])
    add_research_section_sheets(wb,writer=_writer,research=research,
         research_summary=pd.concat([stats,archive],ignore_index=True))
    normalize_frozen_sheet_views(wb)
    bio=io.BytesIO();wb.save(bio);out=load_workbook(io.BytesIO(bio.getvalue()))
    assert len(out.sheetnames)==12 and out.sheetnames[:2]==list(CORE_TABS)
    assert out[CORE_TABS[1]]['A2'].value=='禁止買進'
    assert out[CORE_TABS[0]].freeze_panes=='D8'
    assert out['01_具名強勢研究'].max_row==4
    assert out['01_具名強勢研究']['A2'].value is not None
    assert any('H153.6 01具名表不重複股票' in str(c.value)
          for row in out['02_研究召回統計'] for c in row)
    for name in out.sheetnames:
        assert out[name].sheet_properties.tabColor.rgb[-6:]==TABS_COLOR[name]


def test_source_never_modifies_h126_or_trade_engine():
    from pathlib import Path
    root=Path(__file__).resolve().parents[1]
    s=(root/'godpick_h1536_manager_clarity.py').read_text('utf-8')
    assert 'H126' in s and '核查' in s
    assert 'requests.' not in s and 'st.session_state' not in s
    p=(root/'pages/7_股神推薦.py').read_text('utf-8')
    assert "existing_codes=_h1536_shown_codes" in p
    assert '_h1536_clarity(' in p
    assert "tables['h126_authority']" in p

# -*- coding: utf-8 -*-
"""User-reported nonempty research but empty 00 manager page is a failure."""
import io
import pytest
import pandas as pd
from openpyxl import Workbook, load_workbook
from godpick_h1537_manager_visible import (
    render_manager_visible,validate_manager_content,validate_exported_cross_tab)
from godpick_h151_workbook_sections import CORE_TABS,TABS_COLOR,add_research_section_sheets
from godpick_h1512_excel_integrity import normalize_frozen_sheet_views
from test_h151_excel_split import _writer


def make_workbook(n=24,w=12,trade=0,with_legacy=True):
    wb=Workbook();s=wb.active;s.title=CORE_TABS[0]
    au=wb.create_sheet(CORE_TABS[1])
    au.append(['H126順位','股票代號','股票名稱','市場別','類別','H126權威層別','H126能否買進','H126認證資格'])
    if trade==0: au.append(['','','未通過官方資格','', '', '無合格推薦','禁止買進','NO_QUALIFIED'])
    else:au.append([1,'2330','台積電','','','正式合格','可以買進','QUALIFIED'])
    for row in [('H153.6｜主管摘要','摘要A/B與W'),('掃描','同輪'),('母體','H145阻止'),('研究','有具名'),('正式','禁止買進'),('需核對','B6長篇文字')]:s.append(row)
    s.append(['狀態','舊 H148.1 欄'])
    if with_legacy:s.append(['原始舊版列表','舊版依據應保留'])
    s.freeze_panes='D8';s.auto_filter.ref=f'A7:O{s.max_row}'
    normal=pd.DataFrame([{'研究排名':i+1,'排名等級':'B｜研究證據待補（不得交易）',
        '研究證據分':60-i,'股票代號':str(1000+i),'股票名稱':f'模擬股{i+1}',
        '研究主題':'量縮／箱型研究','H1534三率三升狀態':'待補官方單季財報',
        'H109證據分類':'PENDING_EVIDENCE','反證與待補':'官方獲利與法人待補證'}
        for i in range(n)])
    watch=pd.DataFrame([{'核查優先序':i+1,'股票代號':str(3000+i),
        '股票名稱':f'核查股{i+1}', '研究分類':'明確否決只可核查',
        '技術觀察證據':'同日MA20改善', 'H109風控':'FADE_VETO',
        '反證與待補':'H109禁止買進'} for i in range(w)])
    research=pd.concat([normal,watch],ignore_index=True)
    return wb,normal,watch,research


def test_24_research_12_watch_h126_zero_actual_cells_roundtrip():
    wb,normal,watch,research=make_workbook()
    s=wb[CORE_TABS[0]]
    old_legacy=s['A8'].value; old_h126=[c.value for c in wb[CORE_TABS[1]][2]]
    result=render_manager_visible(wb,ranked=normal,watch=watch,research=research,
        h126_buyable=0,research_day='2026-11-11',h145_status='BLOCKED_FINAL_RESULT_COUNT_GAP')
    assert result['a_b_rows']==24 and result['w_rows']==12
    assert s['B9'].value=='1000' and s['C9'].value=='模擬股1'
    assert s['A7'].value.startswith('H153.7')
    assert s.cell(result['legacy_header_row']+1,1).value==old_legacy
    assert s.auto_filter.ref.startswith('A'+str(result['legacy_header_row']))
    assert [wb[CORE_TABS[1]].cell(2,c).value for c in range(1,len(old_h126)+1)]==old_h126
    assert '0 檔｜禁止買進' in wb[CORE_TABS[1]]['B4'].value
    assert validate_manager_content(wb,result,research=research)
    add_research_section_sheets(wb,writer=_writer,research=research)
    assert validate_exported_cross_tab(wb,result)
    normalize_frozen_sheet_views(wb)
    f=io.BytesIO();wb.save(f)
    final=load_workbook(io.BytesIO(f.getvalue()))
    assert len(final.sheetnames)==12
    assert len(final[CORE_TABS[0]].tables)==2
    assert final[CORE_TABS[0]].freeze_panes=='D8'
    assert final[CORE_TABS[1]]['G2'].value=='禁止買進'
    assert final[CORE_TABS[0]]['B9'].value=='1000'
    assert validate_exported_cross_tab(final,result)
    for name in final.sheetnames:
        assert final[name].sheet_properties.tabColor.rgb[-6:]==TABS_COLOR[name]


def test_tied_rank_retained_unknown_and_h126_zero():
    wb,normal,watch,research=make_workbook(3,2)
    normal.loc[:2,'研究排名']=[1,1,3]
    out=render_manager_visible(wb,ranked=normal,watch=watch,research=research,
                               h126_buyable=0)
    assert wb[CORE_TABS[0]]['A9'].value=='1'
    assert wb[CORE_TABS[0]]['A10'].value=='1'
    assert wb[CORE_TABS[0]]['A11'].value=='3'
    assert wb[CORE_TABS[0]]['J9'].value.startswith('禁止買進')
    assert out['w_rows']==2


def test_missing_visible_table_is_explicit_failure():
    wb,normal,watch,research=make_workbook(3,0)
    result=render_manager_visible(wb,ranked=normal,watch=watch,research=research,h126_buyable=0)
    wb[CORE_TABS[0]].tables.clear()
    with pytest.raises(ValueError,match='沒有可篩選的股票明細表'):
        validate_manager_content(wb,result,research=research)


def test_cross_tab_mismatch_fails_instead_of_green_checkmark():
    wb,normal,watch,research=make_workbook(3,1)
    report=render_manager_visible(wb,ranked=normal,watch=watch,research=research,h126_buyable=0)
    add_research_section_sheets(wb,writer=_writer,research=research.iloc[:-1])
    with pytest.raises(ValueError,match='00/01股票代號不一致'):
        validate_exported_cross_tab(wb,report)


def test_empty_research_and_watch_shows_honest_missing_data_not_fake_stock():
    wb,normal,watch,research=make_workbook(0,0)
    out=render_manager_visible(wb,ranked=normal,watch=watch,research=research,h126_buyable=0)
    assert out['a_b_rows']==out['w_rows']==0
    assert wb[CORE_TABS[0]]['A7'].value=='狀態'  # empty legacy filter kept
    assert not wb[CORE_TABS[0]].tables
    assert validate_manager_content(wb,out,research=research)


def test_declared_H126_qualified_row_is_not_rewritten():
    wb,normal,watch,research=make_workbook(2,0,trade=1)
    preserved=[c.value for c in wb[CORE_TABS[1]][2]]
    report=render_manager_visible(wb,ranked=normal,watch=watch,research=research,h126_buyable=1)
    assert report['h126_authorized']==1
    assert [c.value for c in wb[CORE_TABS[1]][2]]==preserved
    assert wb[CORE_TABS[0]]['J9'].value.startswith('研究非交易')

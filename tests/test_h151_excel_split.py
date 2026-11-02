# -*- coding: utf-8 -*-
"""H151 real Excel category split: data preservation, freeze and authority."""
import io
from pathlib import Path
import importlib.util
import pandas as pd
import pytest
from openpyxl import Workbook, load_workbook
from godpick_h151_workbook_sections import (
    CATEGORY_TABS, CORE_TABS, TABS_COLOR, PRIMARY_TAB_COLORS, SECONDARY_TAB_COLOR, add_research_section_sheets,
)

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / 'pages' / '7_股神推薦.py'


def _writer(wb, name, frame, fallback):
    # Uses the actual page writer instead of duplicating the Excel layout.
    import ast
    import json
    tree = ast.parse(PAGE.read_text('utf-8'))
    funcs = [x for x in tree.body if isinstance(x, ast.FunctionDef) and
             x.name in ('_excel_safe_value', '_excel_safe_df', '_write_df_to_ws_fast_h84')]
    ns = {'pd': pd, 'json': json}
    exec(compile(ast.fix_missing_locations(ast.Module(body=funcs, type_ignores=[])),str(PAGE),'exec'),ns,ns)
    return ns['_write_df_to_ws_fast_h84'](wb, name, frame, fallback)


def _book():
    wb=Workbook()
    wb.active.title=CORE_TABS[0]
    wb.create_sheet(CORE_TABS[1])
    wb[CORE_TABS[0]]['A1']='主管現有主榜'
    wb[CORE_TABS[1]]['A1']='H126權威'
    return wb


def test_each_category_is_independent_with_own_freeze_and_filter():
    frames={key:pd.DataFrame([{'股票代號':'2352','風險判讀':f'驗證 {name}',
                                 'H126授權':'禁止買進'},
                                {'股票代號':'3105','風險判讀':'缺來源','H126授權':'禁止買進'}])
            for name,key,_ in CATEGORY_TABS}
    wb=_book()
    before=[wb.worksheets[0]['A1'].value, wb.worksheets[1]['A1'].value]
    sizes=add_research_section_sheets(wb,writer=_writer,**frames)
    assert list(wb.sheetnames)==list(CORE_TABS)+[x[0] for x in CATEGORY_TABS]
    assert len(set(wb.sheetnames))==12
    assert [wb.worksheets[0]['A1'].value, wb.worksheets[1]['A1'].value]==before
    for name,key,_ in CATEGORY_TABS:
        ws=wb[name]
        assert ws['A1'].value=='股票代號'
        assert ws['A2'].value=='2352' and ws['A3'].value=='3105'
        assert ws.freeze_panes=='A2'
        assert ws.auto_filter.ref=='A1:C3'
        assert sizes[name]==2
    buf=io.BytesIO();wb.save(buf)
    book=load_workbook(io.BytesIO(buf.getvalue()))
    assert book.sheetnames==wb.sheetnames
    assert book['00_H126主管決策']['A1'].value=='H126權威'


def test_empty_frame_has_owned_placeholder_and_never_crossfills():
    wb=_book()
    sizes=add_research_section_sheets(wb,writer=_writer)
    assert all(v==0 for v in sizes.values())
    assert wb['01_具名強勢研究']['A2'].value.startswith('本輪沒有')
    assert wb['10_逐股排除明細']['A2'].value.startswith('本輪沒有')
    assert wb['00_主管重點觀察榜'].max_row==1


def test_formula_like_input_is_not_executable_in_any_sheet():
    wb=_book()
    add_research_section_sheets(
        wb, writer=_writer,
        research=pd.DataFrame([{'股票代號':'1234', '來源':'=HYPERLINK("http://bad","x")',
                                '備註':'+2+3'}])
    )
    ws=wb['01_具名強勢研究']
    assert ws['B2'].data_type=='s'
    assert ws['C2'].data_type=='s'
    assert str(ws['B2'].value).startswith("'=")
    assert str(ws['C2'].value).startswith("'+")


def test_page_source_exports_split_after_core_not_vertical_append():
    s=PAGE.read_text('utf-8')
    assert 'from godpick_h151_workbook_sections import add_research_section_sheets' in s
    assert '_h151_section_sheets(' in s
    assert 'ws.insert_rows(1, amount=6)' in s
    assert 'ws.freeze_panes = \'D8\'' in s
    assert '00_H126主管決策' in s
    # legacy append helpers still exist as backward compatible functions in
    # their modules; they are no longer called by the H85 manager exporter.
    assert '_h1483_append(ws' not in s
    assert '_h149_append(ws' not in s
    assert '_h1482_append(ws' not in s


def test_no_trading_authority_modification_in_section_adapter():
    src=(ROOT/'godpick_h151_workbook_sections.py').read_text('utf-8')
    for forbidden in ('requests.', 'streamlit', 'subprocess', 'st.session_state', 'GITHUB_TOKEN'):
        assert forbidden not in src


def test_h1511_four_priority_tab_colors_roundtrip_without_value_changes():
    wb = _book()
    original_values = [wb[name]['A1'].value for name in CORE_TABS]
    add_research_section_sheets(wb, writer=_writer,
        research=pd.DataFrame([{'股票代號':'2330','研究資格':'僅研究'}]),
        opportunities=pd.DataFrame([{'股票代號':'2330','H126授權':'禁止買進'}]))
    assert len(PRIMARY_TAB_COLORS) == 4
    assert len(set(PRIMARY_TAB_COLORS.values())) == 4
    assert [wb[name]['A1'].value for name in CORE_TABS] == original_values
    for name in wb.sheetnames:
        color = wb[name].sheet_properties.tabColor
        assert color is not None
        assert color.rgb[-6:].upper() == TABS_COLOR[name]
        if name not in PRIMARY_TAB_COLORS:
            assert color.rgb[-6:].upper() == SECONDARY_TAB_COLOR
    data = io.BytesIO()
    wb.save(data)
    restored = load_workbook(io.BytesIO(data.getvalue()))
    assert restored.sheetnames == list(CORE_TABS) + [n for n, _, _ in CATEGORY_TABS]
    for name, expected in PRIMARY_TAB_COLORS.items():
        assert restored[name].sheet_properties.tabColor.rgb[-6:].upper() == expected
    assert restored['00_H126主管決策']['A1'].value == 'H126權威'
    assert restored['01_具名強勢研究']['A2'].value == '2330'
    assert restored['03_五檔研究計畫']['B2'].value == '禁止買進'
    assert restored['00_主管重點觀察榜'].freeze_panes is None  # simplified test book
    assert restored['01_具名強勢研究'].freeze_panes == 'A2'

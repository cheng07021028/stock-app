# -*- coding: utf-8 -*-
"""Regression for Excel repair popup after H151/H151.1 freeze-view reassignment."""
from __future__ import annotations

import io
import xml.etree.ElementTree as ET
from zipfile import ZipFile

from openpyxl import Workbook, load_workbook
import pandas as pd

from godpick_h1512_excel_integrity import normalize_frozen_sheet_views
from godpick_h151_workbook_sections import (
    CATEGORY_TABS, CORE_TABS, add_research_section_sheets, TABS_COLOR,
)
from test_h151_excel_split import _writer

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def _real_shape_book() -> Workbook:
    wb = Workbook()
    wb.remove(wb.active)
    first = _writer(wb, CORE_TABS[0], pd.DataFrame([{
        "觀察順位": "研究", "股票代號": "2352", "股票名稱": "測試股",
        "量價證據": "量能待驗證"}]), "無強勢股")
    _writer(wb, CORE_TABS[1], pd.DataFrame([{
        "股票代號": "2352", "H126能否買進": "禁止買進", "證據": "待補證"}]), "禁止買進")
    # Exact production sequence: the writer uses D2, the six summary lines
    # shift the first sheet's header and it is then reset to D8.
    first.insert_rows(1, amount=6)
    first.freeze_panes = "D8"
    first.auto_filter.ref = f"A7:D{first.max_row}"
    frames = {key: pd.DataFrame([{
        "股票代號": "2352", "研究理由": "有量但非正式交易", "資料日": "2026-11-02"
    }]) for _, key, _ in CATEGORY_TABS}
    add_research_section_sheets(wb, writer=_writer, **frames)
    return wb


def _xml_sheet_views(payload: bytes) -> list[tuple[str | None, tuple[str | None, ...]]]:
    views = []
    with ZipFile(io.BytesIO(payload)) as archive:
        assert archive.testzip() is None
        for idx in range(1, 13):
            xml = archive.read(f"xl/worksheets/sheet{idx}.xml")
            root = ET.fromstring(xml)
            sv = root.find(f"{NS}sheetViews/{NS}sheetView")
            assert sv is not None
            pane = sv.find(f"{NS}pane")
            selections = tuple(e.get("pane") for e in sv.findall(f"{NS}selection"))
            views.append((pane.get("topLeftCell") if pane is not None else None, selections))
    return views


def test_exact_export_writer_had_duplicate_selection_metadata():
    wb = _real_shape_book()
    # This is why Python round-tripping alone did not catch the Excel popup.
    assert len(wb.worksheets[0].sheet_view.selection) > 3
    assert len(wb.worksheets[2].sheet_view.selection) > 1


def test_h1512_repairs_ooxml_while_preserving_all_twelve_tabs_and_h126():
    wb = _real_shape_book()
    before = [(ws.title, ws.max_row, ws.max_column, ws.auto_filter.ref,
               ws.sheet_properties.tabColor.rgb[-6:]) for ws in wb]
    authority_before = wb[CORE_TABS[1]]["B2"].value
    change = normalize_frozen_sheet_views(wb)
    assert change[CORE_TABS[0]]["previous_selections"] > 3
    assert change[CORE_TABS[0]]["normalised_selections"] == 3
    assert change[CATEGORY_TABS[0][0]]["previous_selections"] > 1
    assert change[CATEGORY_TABS[0][0]]["normalised_selections"] == 1
    assert normalize_frozen_sheet_views(wb)[CORE_TABS[0]]["normalised_selections"] == 3
    now = [(ws.title, ws.max_row, ws.max_column, ws.auto_filter.ref,
            ws.sheet_properties.tabColor.rgb[-6:]) for ws in wb]
    assert now == before
    assert wb[CORE_TABS[1]]["B2"].value == authority_before
    stream = io.BytesIO()
    wb.save(stream)
    payload = stream.getvalue()
    views = _xml_sheet_views(payload)
    assert len(views) == 12
    assert views[0] == ("D8", ("topRight", "bottomLeft", "bottomRight"))
    assert views[1] == ("D2", ("topRight", "bottomLeft", "bottomRight"))
    for coordinate, selections in views[2:]:
        assert (coordinate, selections) == ("A2", ("bottomLeft",))
    reopened = load_workbook(io.BytesIO(payload))
    assert reopened.sheetnames == list(CORE_TABS) + [name for name, _, _ in CATEGORY_TABS]
    assert reopened[CORE_TABS[1]]["B2"].value == authority_before
    assert reopened[CORE_TABS[0]].auto_filter.ref == "A7:D8"
    for ws in reopened:
        assert ws.sheet_properties.tabColor.rgb[-6:] == TABS_COLOR[ws.title]
    reopened.close()


def test_exporter_calls_final_view_repair_before_saving():
    from pathlib import Path
    source = (Path(__file__).resolve().parents[1] / "pages" / "7_股神推薦.py").read_text("utf-8")
    block = source[source.index('    sheets = [\n        ("00_主管重點觀察榜", h144_focus,'):
                   source.index('def _build_selected_excel_bytes_h85')]
    assert '_h1512_normalize_views(wb)\n    wb.save(output)' in block

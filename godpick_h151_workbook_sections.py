# -*- coding: utf-8 -*-
"""V191-H151: One workbook, one category per worksheet, no data loss.

This is purely an Excel layout adapter. Inputs are the already-computed H147-
H149 evidence frames; no ranking, scanning, I/O, H109 or H126 mutation.
"""
from __future__ import annotations

from typing import Any, Callable
import pandas as pd

VERSION = "V191-H151.1-MANAGER-PRIMARY-TABS"

# Keep core manager observation and authority tabs first and unchanged.
CORE_TABS = ("00_主管重點觀察榜", "00_H126主管決策")
# Excel worksheet titles must be <= 31 chars and unique.
CATEGORY_TABS = (
    ("01_具名強勢研究", "research", "本輪沒有可核對量價的具名研究股票；不能拿舊分數湊推薦。"),
    ("02_研究召回統計", "research_summary", "本輪沒有研究召回統計；檢查診斷股票池來源。"),
    ("03_五檔研究計畫", "opportunities", "本輪沒有具證據的五檔交易研究機會；不是正式交易授權。"),
    ("04_六維研究摘要", "six_pillar_summary", "六維研究資料待補；不推測新聞、績效或持倉。"),
    ("05_五關量價審查", "five_gate", "本輪沒有 H147 五關量價證據。"),
    ("06_強勢股漏網追蹤", "trace", "本輪沒有可核對的 H148 漏網追蹤資料。"),
    ("07_族群證據分析", "sectors", "本輪沒有可核對的跨期強勢族群證據。"),
    ("08_未入榜候選隔離", "quarantine", "本輪沒有 H148.1 隔離資料；不代表所有股票均已通過。"),
    ("09_資料來源品質", "source_summary", "本輪沒有 H148.2 資料來源稽核摘要。"),
    ("10_逐股排除明細", "source_detail", "本輪沒有逐股排除原因；不代表全市場均可交易。"),
)

# V191-H151.1: Four daily-priority worksheets get distinct, vivid tab colors.
# Colors are visual navigation hints ONLY. They never imply H126 buy permission.
# Keep the existing worksheet order, freeze panes, filters and exact data.
PRIMARY_TAB_COLORS = {
    "00_主管重點觀察榜": "F59E0B",  # amber  - daily summary
    "00_H126主管決策": "7C3AED",    # purple - formal trading authority
    "01_具名強勢研究": "16A34A",   # green  - first research priority
    "03_五檔研究計畫": "2563EB",   # blue   - evidence-backed plan
}
SECONDARY_TAB_COLOR = "94A3B8"   # muted slate for other eight sheets
TABS_COLOR = {
    name: PRIMARY_TAB_COLORS.get(name, SECONDARY_TAB_COLOR)
    for name in CORE_TABS + tuple(tab[0] for tab in CATEGORY_TABS)
}


def color_manager_tabs(wb: Any) -> None:
    """Highlight the four priority tabs without touching values or trading decisions."""
    for name in CORE_TABS:
        wb[name].sheet_properties.tabColor = TABS_COLOR[name]


def _safe_cell(v: Any) -> Any:
    """Protect text from Excel formula injection in research/news/free text."""
    if isinstance(v, str) and v.lstrip().startswith(("=", "+", "-", "@")):
        return "'" + v
    return v


def _safe_frame(frame: Any) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        return pd.DataFrame()
    # Preserve rows and column order, without changing any decision values.
    work = frame.copy(deep=True)
    if not work.empty:
        for column in work.select_dtypes(include=["object", "string"]).columns:
            work[column] = work[column].map(_safe_cell)
    return work


def add_research_section_sheets(wb: Any, *, writer: Callable, **frames: Any) -> dict:
    """Export each previously appended research/audit table separately.

    writer is the existing application's fast dataframe-to-worksheet routine.
    Crucially: no re-computation, network lookup, old recommendation recovery,
    deletion of rows or synthetic formal buy lists. H126 remains worksheet #2.
    """
    assert tuple(wb.sheetnames[:2]) == CORE_TABS, "H126 authority must remain worksheet #2"
    color_manager_tabs(wb)
    summary = {}
    for name, key, fallback in CATEGORY_TABS:
        frame = _safe_frame(frames.get(key))
        ws = writer(wb, name, frame, fallback)
        ws.freeze_panes = "A2"  # each category freezes only its own header row
        ws.auto_filter.ref = ws.dimensions
        ws.sheet_properties.tabColor = TABS_COLOR[name]
        ws.sheet_view.showGridLines = False
        summary[name] = len(frame)
    assert len(wb.sheetnames) == 2 + len(CATEGORY_TABS), "Incomplete manager export section set"
    return summary


__all__ = ["VERSION", "CORE_TABS", "CATEGORY_TABS", "add_research_section_sheets", "PRIMARY_TAB_COLORS", "SECONDARY_TAB_COLOR", "TABS_COLOR", "color_manager_tabs"]

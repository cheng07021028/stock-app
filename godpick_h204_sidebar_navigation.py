# -*- coding: utf-8 -*-
"""H204 sidebar navigation with explicit module numbers.

Purpose
-------
Streamlit's default multipage sidebar strips leading numeric prefixes from
filenames, so pages like ``29_主流趨勢與潛力黑馬.py`` appear without their
module number. This helper rebuilds the sidebar navigation explicitly through
``st.navigation`` so the left sidebar keeps stable module numbers.

Design principles
-----------------
- Preserve current page scripts; do not rewrite page logic.
- Prefer readable UTF-8 page filenames when duplicates exist.
- Fall back to the original ``streamlit_app.py`` behavior if navigation setup
  cannot be completed.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Callable, Dict, List, Optional

try:
    import streamlit as st
except Exception:  # pragma: no cover - test environment fallback
    class _DummyStreamlit:
        def Page(self, *args, **kwargs):
            return (args, kwargs)

        def navigation(self, *args, **kwargs):
            raise RuntimeError('streamlit not available')

    st = _DummyStreamlit()

PAGE_TITLE_MAP: Dict[int, str] = {
    0: "00｜大盤走勢",
    1: "01｜儀表板",
    2: "02｜行情查詢",
    3: "03｜歷史K線分析",
    4: "04｜自選股中心",
    5: "05｜排行榜",
    6: "06｜多股比較",
    7: "07｜股神推薦",
    8: "08｜股神推薦紀錄",
    9: "09｜股票主檔更新",
    10: "10｜推薦清單",
    11: "11｜資料診斷",
    12: "12｜股神管理中心",
    14: "14｜股神權重校正",
    15: "15｜帳號管理",
    16: "16｜官方因子快取中心",
    17: "17｜系統健康檢查",
    18: "18｜股神AI專業研究中心",
    19: "19｜Adaptive GodPick Learning Core",
    20: "20｜自動資料新鮮度中心",
    21: "21｜選股與交易執行學習中心",
    22: "22｜股神七維進化中心",
    23: "23｜研究選股智慧中心",
    24: "24｜六引擎交易研究",
    25: "25｜主管智慧決策戰情",
    26: "26｜訊號生命週期與策略審查",
    27: "27｜AI自動資料中樞",
    28: "28｜十二研究員決策研究中心",
    29: "29｜主流趨勢與潛力黑馬",
    30: "30｜財報成長與合理估值",
    31: "31｜十二研究員聯合決策",
    32: "32｜隔日預測與AI績效學習",
    33: "33｜超級AI股神決策總指揮中心",
    98: "98｜股神營運健康戰情",
}

HOME_TITLE = "首頁"


@dataclass(frozen=True)
class PageEntry:
    page_no: int
    path: Path
    title: str


def _pages_dir() -> Path:
    return Path(__file__).resolve().parent / "pages"


def _page_no_from_name(filename: str) -> Optional[int]:
    match = re.match(r"^(\d+)_.*\.py$", filename)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _is_encoded_name(filename: str) -> bool:
    return "#U" in filename


def _display_title(page_no: int, path: Path) -> str:
    title = PAGE_TITLE_MAP.get(page_no)
    if title:
        return title
    raw = path.stem
    raw = re.sub(r"^\d+_", "", raw)
    raw = raw.replace("_", " ").strip()
    return f"{page_no:02d}｜{raw or '未命名模組'}"


def discover_page_entries() -> List[PageEntry]:
    """Discover page scripts and keep one preferred file per module number."""
    pages_dir = _pages_dir()
    candidates: Dict[int, Path] = {}
    for path in pages_dir.glob("*.py"):
        page_no = _page_no_from_name(path.name)
        if page_no is None:
            continue
        current = candidates.get(page_no)
        if current is None:
            candidates[page_no] = path
            continue
        # Prefer human-readable UTF-8 filename over encoded '#U' variants.
        current_encoded = _is_encoded_name(current.name)
        new_encoded = _is_encoded_name(path.name)
        if current_encoded and not new_encoded:
            candidates[page_no] = path
        elif current_encoded == new_encoded and path.name < current.name:
            candidates[page_no] = path

    entries = [
        PageEntry(page_no=no, path=path, title=_display_title(no, path))
        for no, path in sorted(candidates.items(), key=lambda item: item[0])
    ]
    return entries


def build_streamlit_pages(home_callable: Callable[[], None]):
    pages = [st.Page(home_callable, title=HOME_TITLE, icon="🏠")]
    for entry in discover_page_entries():
        pages.append(st.Page(str(entry.path), title=entry.title))
    return pages


def run_numbered_navigation(home_callable: Callable[[], None]) -> bool:
    """Run explicit sidebar navigation. Returns True when handled."""
    try:
        pages = build_streamlit_pages(home_callable)
        navigator = st.navigation(pages, position="sidebar")
        navigator.run()
        return True
    except Exception:
        return False

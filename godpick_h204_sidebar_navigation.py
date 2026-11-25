# -*- coding: utf-8 -*-
"""H207 grouped sidebar navigation, retaining H204 module numbers.

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
- Retain the original entrypoint fallback only if the navigation API is unavailable.
- Do not swallow errors from the selected page.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import logging
from typing import Callable, Dict, List, Optional

try:
    import streamlit as st
except ImportError:  # permits pure catalog tests without Streamlit installed
    st = None

LOG = logging.getLogger(__name__)
NAVIGATION_VERSION = "V191-H207"

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

# Group order is deliberate: everyday use first, system maintenance last.
# Existing page numbers, titles, script paths and URL inference are retained.
GROUPS = (
    ("首頁／行情研究", (0, 1, 2, 3, 4, 5, 6)),
    ("推薦／決策戰情", (7, 8, 10, 12, 25, 33)),
    ("AI研究／績效學習", (14, 18, 19, 21, 22, 23, 24, 26, 28, 29, 30, 31, 32)),
    ("資料／系統維運", (9, 11, 15, 16, 17, 20, 27, 98)),
)
GROUP_BY_NUMBER = {number: label for label, numbers in GROUPS for number in numbers}
# Future numbered modules remain accessible even before a group is defined.
FUTURE_GROUP = "其他／新增模組"


# Shared layout, independent of menu item order or number of pages.  Keep
# existing theme colors and native active/hover states, including dark themes.
SIDEBAR_STYLE = """<style>
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a,
section[data-testid="stSidebar"] [data-testid="stSidebarNavLink"] {
    padding-left: .65rem !important;
    padding-right: .65rem !important;
    min-height: 36px !important;
    height: auto !important;
    white-space: normal !important;
    align-items: center !important;
    overflow: visible !important;
    min-width: 0 !important;
}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a span,
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a p,
section[data-testid="stSidebar"] [data-testid="stSidebarNavLink"] span,
section[data-testid="stSidebar"] [data-testid="stSidebarNavLink"] p,
section[data-testid="stSidebar"] [data-testid="stSidebarNavLinkText"] {
    white-space: normal !important;
    overflow: visible !important;
    text-overflow: clip !important;
    overflow-wrap: anywhere !important;
    min-width: 0 !important;
    max-width: 100% !important;
    line-height: 1.4 !important;
}
/* Defensive reset of legacy generated text; never introduce an extra number. */
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a::before,
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a span::before,
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] a p::before,
section[data-testid="stSidebar"] [data-testid="stSidebarNavLink"]::before,
section[data-testid="stSidebar"] [data-testid="stSidebarNavLink"] span::before,
section[data-testid="stSidebar"] [data-testid="stSidebarNavLink"] p::before {
    content: none !important;
    display: none !important;
}
</style>"""


def inject_sidebar_layout() -> None:
    # CSS-only st.html does not create a visible main-area element.  No page
    # content, login, session state, or URL rewriting is performed here.
    html = getattr(st, "html", None)
    if callable(html):
        html(SIDEBAR_STYLE)
    else:
        markdown = getattr(st, "markdown", None)
        if callable(markdown):
            markdown(SIDEBAR_STYLE, unsafe_allow_html=True)



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


def _home_page(home_callable: Callable[[], None]):
    # Keep the root URL and existing home callable, as in H204.
    return st.Page(home_callable, title=HOME_TITLE, icon="🏠", default=True)


def _script_page(entry: PageEntry):
    # The same source path as H204: do not override Streamlit's inferred URL.
    return st.Page(str(entry.path), title=entry.title)


def group_page_entries(entries: List[PageEntry]) -> Dict[str, List[PageEntry]]:
    """Pure grouping. Preserve every discovered numbered module exactly once."""
    grouped = {label: [] for label, _ in GROUPS}
    for entry in sorted(entries, key=lambda item: (item.page_no, item.path.name)):
        label = GROUP_BY_NUMBER.get(entry.page_no, FUTURE_GROUP)
        grouped.setdefault(label, []).append(entry)
    return grouped


def build_streamlit_pages(home_callable: Callable[[], None]):
    """H204-compatible flat catalog, retained for callers and diagnostics."""
    return [_home_page(home_callable)] + [_script_page(entry) for entry in discover_page_entries()]


def build_grouped_pages(home_callable: Callable[[], None]):
    """Build Streamlit's native grouped menu without renaming scripts."""
    grouped = group_page_entries(discover_page_entries())
    first_group = GROUPS[0][0]
    pages = {first_group: [_home_page(home_callable)]}
    for label, entries in grouped.items():
        if entries:
            pages.setdefault(label, []).extend(_script_page(entry) for entry in entries)
    return pages


def run_numbered_navigation(home_callable: Callable[[], None]) -> bool:
    """Register the navigation once, and execute only the selected page.

    Legacy fallback is safe only before navigation is registered, and only if
    the API does not exist. Runtime failures must remain visible; executing
    the home page after a partially executed module can duplicate side effects.
    """
    if not callable(getattr(st, "Page", None)) or not callable(getattr(st, "navigation", None)):
        LOG.warning("%s: Streamlit navigation API unavailable; using legacy entrypoint", NAVIGATION_VERSION)
        return False
    pages = build_grouped_pages(home_callable)
    navigator = st.navigation(pages, position="sidebar", expanded=True)
    inject_sidebar_layout()
    navigator.run()
    return True

# -*- coding: utf-8 -*-
"""H151.1 dependency and four priority Excel tab-color preflight (no network)."""
from __future__ import annotations
import argparse
import sys
from pathlib import Path


def verify(root: Path) -> tuple[bool, list[str]]:
    root = Path(root).resolve()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from tools.h151_verify_deploy import verify as old_verify
    ok, notes = old_verify(root)
    problems = [] if ok else list(notes)
    from godpick_h151_workbook_sections import (
        CORE_TABS, CATEGORY_TABS, PRIMARY_TAB_COLORS, SECONDARY_TAB_COLOR,
        TABS_COLOR, add_research_section_sheets,
    )
    expected_four = {'00_主管重點觀察榜', '00_H126主管決策',
                     '01_具名強勢研究', '03_五檔研究計畫'}
    all_tabs = list(CORE_TABS) + [name for name, _, _ in CATEGORY_TABS]
    if set(PRIMARY_TAB_COLORS) != expected_four:
        problems.append('INVALID_PRIMARY_TAB_SET')
    if len(all_tabs) != 12 or set(TABS_COLOR) != set(all_tabs):
        problems.append('INVALID_TAB_COLOR_COVERAGE')
    if len(set(PRIMARY_TAB_COLORS.values())) != 4:
        problems.append('PRIMARY_TABS_NOT_DISTINCT')
    if any(TABS_COLOR[name] != SECONDARY_TAB_COLOR for name in all_tabs
           if name not in expected_four):
        problems.append('SECONDARY_TABS_NOT_MUTED')
    # Ensure the single export adapter applies colors rather than requiring
    # a new spreadsheet layout or modifying the trade authority workflow.
    if 'color_manager_tabs(wb)' not in (root / 'godpick_h151_workbook_sections.py').read_text('utf-8'):
        problems.append('MISSING_CORE_TAB_COLOR_ASSIGNMENT')
    if not callable(add_research_section_sheets):
        problems.append('INVALID_EXPORT_ADAPTER')
    return not problems, problems or ['PASS H151.1: H151 dependencies, 4 highlighted priority tabs, 8 muted tabs']


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    args = ap.parse_args()
    ok, notes = verify(args.root)
    for note in notes:
        print(note)
    sys.exit(0 if ok else 1)

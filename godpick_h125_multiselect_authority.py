# -*- coding: utf-8 -*-
"""H125: category selection roundtrip, preserving all user-selected values.

Authority is H111 GitHub scan-settings document. The current market/master list is
an optional UI choice catalogue, never a source allowed to erase saved choices.
"""
from __future__ import annotations
from typing import Any


def preserve_selected_categories(saved: Any, available: Any = None) -> tuple[list[str], list[str]]:
    """Return (selected, expanded_options), stable order and no deleted sectors.

    The special ['全部'] stands for the legacy all-sectors UI default. Unknown
    saved sector labels remain selectable even when market master is delayed.
    """
    opts: list[str] = []
    for option in (available if isinstance(available, (list, tuple)) else ["全部"]):
        if isinstance(option, str) and option.strip() and option not in opts:
            opts.append(option)
    if "全部" not in opts:
        opts.insert(0, "全部")
    if not isinstance(saved, (list, tuple)):
        raw = ["全部"]
    else:
        raw = saved
    selection: list[str] = []
    for item in raw:
        if isinstance(item, str) and item.strip() and item not in selection:
            selection.append(item)
    if not selection:
        selection = ["全部"]
    for item in selection:
        if item not in opts:
            opts.append(item)
    return selection, opts


def category_roundtrip_state(raw_settings: Any, options: Any) -> dict[str, Any]:
    """Small pure helper for regression: restore and preserve original selection."""
    data = raw_settings if isinstance(raw_settings, dict) else {}
    selected, expanded = preserve_selected_categories(data.get("selected_categories", ["全部"]), options)
    return {"selected_categories": selected, "category_options": expanded}

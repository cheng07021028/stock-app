# -*- coding: utf-8 -*-
"""Phase 8.3 shared-column overlay for liquidity and scan-quality evidence."""
from _phase83_core import godpick_column_schema_core as _core

_EXTRA_DISPLAY = [
    "流動性資料狀態", "流動性資料來源", "掃描品質等級", "推薦適用範圍", "倉位折減係數",
    "有效K線資料率%", "流動性資料覆蓋率%", "官方因子覆蓋率%",
]
for _col in reversed(_EXTRA_DISPLAY):
    if _col not in _core.UNIFIED_RECOMMEND_DISPLAY_COLUMNS:
        try:
            _pos = _core.UNIFIED_RECOMMEND_DISPLAY_COLUMNS.index("正式推薦分區")
        except ValueError:
            _pos = len(_core.UNIFIED_RECOMMEND_DISPLAY_COLUMNS)
        _core.UNIFIED_RECOMMEND_DISPLAY_COLUMNS.insert(_pos, _col)
_core.ALIASES.update({
    "流動性資料狀態": ["流動性狀態", "成交資料狀態"],
    "流動性資料來源": ["流動性來源", "成交額資料來源"],
    "有效K線資料率%": ["歷史資料成功率%"],
})
_core.NUMERIC_LIKE_COLUMNS.update({
    "倉位折減係數", "掃描覆蓋率%", "有效K線資料率%", "歷史資料成功率%",
    "流動性資料覆蓋率%", "官方因子覆蓋率%",
})
for _name in dir(_core):
    if not _name.startswith("__"):
        globals()[_name] = getattr(_core, _name)

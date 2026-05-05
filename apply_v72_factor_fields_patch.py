# -*- coding: utf-8 -*-
from __future__ import annotations

"""
v72 欄位完整寫入補丁：
- 將 godpick_factor_schema.enrich_dataframe 接到常見的推薦紀錄寫入前
- 盡量用保守插入，不刪原本功能
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent

IMPORT_LINE = "from godpick_factor_schema import enrich_dataframe, ensure_factor_columns, V72_FACTOR_FIELDS\n"

def patch_import(path: Path):
    text = path.read_text(encoding="utf-8", errors="replace")
    if "godpick_factor_schema import" in text:
        return text
    lines = text.splitlines(keepends=True)
    insert_at = 0
    # 放在 future import 後
    for i, line in enumerate(lines):
        if line.startswith("from __future__ import"):
            insert_at = i + 1
    lines.insert(insert_at, IMPORT_LINE)
    return "".join(lines)

def patch_common_dataframe_writes(text: str) -> str:
    """
    在常見變數名稱 df / result_df / rec_df / save_df 顯示或寫入前補欄。
    不保證所有自訂流程都命中，但對目前專案常見命名有效。
    """
    # 防重複
    if "# >>> V72_FACTOR_ENRICH" in text:
        return text

    marker = 
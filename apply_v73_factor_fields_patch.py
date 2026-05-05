# -*- coding: utf-8 -*-
from __future__ import annotations

"""
v73：修正 v72 factor fields patch 的 SyntaxError。
用途：
- 只補 godpick_factor_schema 匯入與安全輔助函式
- 不再用不穩定的多行 marker 字串寫法
- 不破壞原始頁面邏輯
"""

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent

TARGET_FILES = [
    "pages/7_股神推薦.py",
    "pages/8_股神推薦紀錄.py",
    "pages/10_推薦清單.py",
    "pages/14_股神權重校正.py",
    "godpick_record_service.py",
]

IMPORT_LINE = "from godpick_factor_schema import enrich_dataframe, ensure_factor_columns, V72_FACTOR_FIELDS\n"

HELPER_BLOCK = 
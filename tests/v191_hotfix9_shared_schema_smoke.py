# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd

from godpick_column_schema import (
    EXPORT_ALWAYS_KEEP_COLUMNS,
    GODPICK_RECORD_TRACE_COLUMNS,
    NUMERIC_LIKE_COLUMNS,
    UNIFIED_RECOMMEND_DISPLAY_COLUMNS,
    normalize_godpick_dataframe,
    prune_empty_recommendation_columns,
    standardize_records_for_storage,
)


def main() -> None:
    row = {
        "推薦日期": "2026-08-12",
        "推薦時間": "14:42:10",
        "推薦批次日期": "2026-08-12",
        "推薦批次時間": "14:42:10",
        "推薦執行ID": "gprun_h9_20260812_144210",
        "推薦執行來源": "07_股神推薦",
        "推薦觸發方式": "manual",
        "推薦執行版本": "V191-H9",
        "原始推薦日期": "2026-08-11",
        "股票代號": "5347",
        "股票名稱": "世界",
        "理論進場參考價": 156.025,
        "執行基準價": 158.365375,
        "執行價來源": "回測承接1.5%可成交區上緣",
        "執行價可成交驗證": True,
    }
    df = pd.DataFrame([row])
    normalized = normalize_godpick_dataframe(df, add_missing=True, clean_none=True)
    stored = standardize_records_for_storage(df, keep_extras=True)
    pruned = prune_empty_recommendation_columns(stored)

    for col, value in row.items():
        assert col in normalized.columns, f"normalize dropped H9 field: {col}"
        assert col in stored.columns, f"storage dropped H9 field: {col}"
        assert col in pruned.columns, f"export/prune dropped H9 field: {col}"
        if col == "執行價可成交驗證":
            assert bool(stored.iloc[0][col]) is True
        elif isinstance(value, float):
            assert abs(float(stored.iloc[0][col]) - value) < 1e-9
        else:
            assert str(stored.iloc[0][col]) == str(value)

    for col in ["推薦批次日期", "推薦執行ID", "原始推薦日期", "理論進場參考價", "執行價來源", "執行價可成交驗證"]:
        assert col in UNIFIED_RECOMMEND_DISPLAY_COLUMNS, col
        assert col in EXPORT_ALWAYS_KEEP_COLUMNS, col
    for col in ["推薦批次日期", "推薦執行ID", "原始推薦日期", "理論進場參考價", "執行價來源", "執行價可成交驗證"]:
        assert col in GODPICK_RECORD_TRACE_COLUMNS, col
    assert "理論進場參考價" in NUMERIC_LIKE_COLUMNS
    assert "執行價可成交驗證" not in NUMERIC_LIKE_COLUMNS

    print("PASS H9 shared schema | run provenance + tradable-fill audit fields survive normalize/storage/export")


if __name__ == "__main__":
    main()

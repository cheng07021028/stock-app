# -*- coding: utf-8 -*-
"""
godpick_column_manager.py
v30：股神欄位管理共用工具

用途：
- 提供 7_股神推薦、8_股神推薦紀錄、10_推薦清單、12_股神管理中心共用欄位管理基礎函式。
- 設定儲存在 godpick_management_ui_config.json。
- 以安全顯示為主，不直接刪除原始資料。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
import json

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "godpick_management_ui_config.json"

EMPTY_VALUES = {"", "None", "none", "nan", "NaN", "null", "NULL", "<NA>"}


def _is_empty_value(v: Any) -> bool:
    if v is None:
        return True
    try:
        if pd.isna(v):
            return True
    except Exception:
        pass
    return str(v).strip() in EMPTY_VALUES


def safe_text(v: Any, blank: str = "") -> str:
    """把 None / NaN / list / dict 安全轉為畫面可顯示文字。"""
    if _is_empty_value(v):
        return blank
    if isinstance(v, (dict, list, tuple, set)):
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)
    return str(v)


def clean_display_df(df: pd.DataFrame, hide_empty_columns: bool = True) -> pd.DataFrame:
    """清理畫面用 DataFrame：去重欄、清 None、可選擇隱藏整欄空白欄。"""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out = out.loc[:, ~out.columns.duplicated()].copy()
    for c in out.columns:
        if out[c].dtype == "object":
            out[c] = out[c].map(lambda x: "" if _is_empty_value(x) else x)
        else:
            out[c] = out[c].where(~out[c].isna(), "")
    if hide_empty_columns:
        keep = []
        for c in out.columns:
            s = out[c].map(lambda x: "" if _is_empty_value(x) else str(x).strip())
            if s.ne("").any():
                keep.append(c)
        out = out[keep] if keep else out.iloc[:, :0]
    return out


def load_column_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    try:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_column_config(config: Dict[str, Any]) -> bool:
    try:
        CONFIG_PATH.write_text(json.dumps(config or {}, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


def unique_existing_columns(cols: Iterable[str], df: Optional[pd.DataFrame] = None) -> List[str]:
    seen, out = set(), []
    available = set(df.columns) if isinstance(df, pd.DataFrame) else None
    for c in cols or []:
        if not c or c in seen:
            continue
        if available is not None and c not in available:
            continue
        seen.add(c)
        out.append(c)
    return out


def get_table_columns(table_key: str, default_cols: Iterable[str], df: Optional[pd.DataFrame] = None) -> List[str]:
    """讀取某表格已儲存欄位；沒有設定時回傳 default_cols。"""
    config = load_column_config()
    table_cfg = config.get(table_key, {}) if isinstance(config.get(table_key, {}), dict) else {}
    saved = table_cfg.get("columns") or table_cfg.get("display_columns") or []
    cols = unique_existing_columns(saved, df) if saved else unique_existing_columns(default_cols, df)
    if not cols and isinstance(df, pd.DataFrame):
        cols = list(df.columns)
    return cols


def set_table_columns(table_key: str, columns: Iterable[str], template: str = "custom") -> bool:
    config = load_column_config()
    config[table_key] = {
        "columns": unique_existing_columns(columns),
        "template": template,
    }
    return save_column_config(config)


def apply_columns(df: pd.DataFrame, table_key: str, default_cols: Iterable[str], hide_empty_columns: bool = False) -> pd.DataFrame:
    """依已儲存欄位順序輸出畫面表格。"""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    clean = clean_display_df(df, hide_empty_columns=False)
    cols = get_table_columns(table_key, default_cols, clean)
    out = clean[cols].copy() if cols else clean.copy()
    if hide_empty_columns:
        out = clean_display_df(out, hide_empty_columns=True)
    return out


def column_templates(all_cols: Iterable[str]) -> Dict[str, List[str]]:
    """提供統一快速模板；不存在的欄位由呼叫端再過濾。"""
    all_cols = list(all_cols or [])
    templates = {
        "核心推薦欄位": [
            "勾選", "匯入自選", "刪除", "推薦日期", "推薦時間", "股票代號", "股票名稱", "市場別", "類別", "產業",
            "推薦模式", "推薦等級", "推薦分數", "股神決策分數", "買點分級", "最新價", "推薦價格",
        ],
        "操作與倉位欄位": [
            "v21操作優先順序", "追蹤分級", "今日操作建議", "建議動作", "股神建議動作", "股神信心", "進場時機",
            "股神進場區間", "等待條件", "建議倉位%", "動態建議倉位%", "第一筆進場%", "分批策略",
        ],
        "風控停利停損欄位": [
            "高風險狀態", "品質分級", "品質建議", "風險說明", "停損價", "停損參考", "停利目標", "賣出目標1", "賣出目標2",
            "最大回撤%", "風險報酬比", "R/R", "等待條件",
        ],
        "族群大盤欄位": [
            "類別", "產業", "族群資金說明", "族群策略建議", "族群資金流分數", "族群資金流說明", "族群輪動狀態",
            "大盤情境分析", "大盤情境調權說明", "大盤策略建議", "大盤風控", "大盤交易時段",
        ],
        "績效追蹤欄位": [
            "推薦後1日%", "推薦後1日勝率", "推薦後3日%", "推薦後3日勝率", "推薦後5日%", "推薦後5日勝率",
            "推薦後10日%", "推薦後10日勝率", "推薦後20日%", "推薦後20日勝率", "最大漲幅%", "最大回撤%", "目前績效%",
        ],
        "全部欄位": all_cols,
    }
    return templates

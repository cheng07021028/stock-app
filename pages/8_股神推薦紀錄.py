







from __future__ import annotations

import streamlit as st

from godpick_factor_schema import enrich_dataframe, ensure_factor_columns, V72_FACTOR_FIELDS

# >>> APP_AUTH_GUARD_V84
try:
    from app_auth import require_login
    require_login()
except Exception as _auth_e:
    st.error(f"登入系統載入失敗：{_auth_e}")
    st.stop()


try:
    from godpick_persistence_service import (
        append_export_history,
        load_export_history,
        load_export_sync_settings,
        load_module_sync_state,
        load_named_json_permanent,
        load_records_permanent,
        load_records_github_sync_status,
        load_watchlist_permanent,
        save_export_sync_settings,
        save_module_sync_state,
        save_named_json_permanent,
        save_records_permanent,
        save_records_sync_fast,
        save_records_mutation_fast,
        save_watchlist_permanent,
        write_export_file,
        project_path,
        read_local_json,
        ensure_records_local_authority_current,
        records_authority_signature,
        records_authority_status,
        upsert_records_authority_fast,
    )
except Exception:
    append_export_history = None
    load_export_history = None
    load_export_sync_settings = None
    load_module_sync_state = None
    load_named_json_permanent = None
    load_records_permanent = None
    load_records_github_sync_status = None
    load_watchlist_permanent = None
    save_export_sync_settings = None
    save_module_sync_state = None
    save_named_json_permanent = None
    save_records_permanent = None
    save_records_sync_fast = None
    save_records_mutation_fast = None
    save_watchlist_permanent = None
    write_export_file = None
    project_path = None
    read_local_json = None
    ensure_records_local_authority_current = None
    records_authority_signature = None
    records_authority_status = None
    upsert_records_authority_fast = None

try:
    from stock_master_service import upsert_stock_master_rows
except Exception:
    upsert_stock_master_rows = None
# <<< APP_AUTH_GUARD_V84

# pages/8_股神推薦紀錄.py
# -*- coding: utf-8 -*-

from datetime import datetime, date, timedelta
from typing import Any
from zoneinfo import ZoneInfo
import json
import base64
import io
import hashlib
import copy
import time
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed


import pandas as pd
import requests
from godpick_perf_fast_update_v77 import update_recommendation_perf_fast_v77
from godpick_history_sources import fetch_multi_source_history
try:
    from godpick_official_latest_price_service import (
        SERVICE_VERSION as OFFICIAL_LATEST_PRICE_SERVICE_VERSION,
        fetch_latest_official_market_snapshot,
    )
except Exception:
    OFFICIAL_LATEST_PRICE_SERVICE_VERSION = "official_latest_snapshot_unavailable"
    fetch_latest_official_market_snapshot = None
try:
    from godpick_calibration_sample_service import sync_existing_calibration_samples
except Exception:
    sync_existing_calibration_samples = None
try:
    from godpick_learning_system import (
        LEARNING_SYSTEM_VERSION,
        MODEL_VERSION as GODPICK_AI_MODEL_VERSION,
        LEARNING_COLUMNS as GODPICK_LEARNING_COLUMNS,
        load_learning_state,
        refresh_learning_state_from_records,
    )
except Exception:
    LEARNING_SYSTEM_VERSION = "learning_system_unavailable"
    GODPICK_AI_MODEL_VERSION = "learning_model_unavailable"
    GODPICK_LEARNING_COLUMNS = []
    load_learning_state = None
    refresh_learning_state_from_records = None
try:
    from official_factor_service import FACTOR_COLUMNS as OFFICIAL_FACTOR_SERVICE_COLUMNS, load_factor_frame as _load_official_factor_frame
except Exception:
    OFFICIAL_FACTOR_SERVICE_COLUMNS = []
    _load_official_factor_frame = None
try:
    from godpick_t1_trade_truth import (
        refresh_t1_trade_truth as _v188_refresh_t1_truth,
        load_t1_truth_summary as _v188_load_t1_truth_summary,
        load_t1_truth_rows as _v188_load_t1_truth_rows,
    )
except Exception:
    _v188_refresh_t1_truth = None
    _v188_load_t1_truth_summary = None
    _v188_load_t1_truth_rows = None

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except Exception:
    firebase_admin = None
    credentials = None
    firestore = None


try:
    from godpick_column_schema import (
        UNIFIED_RECOMMEND_DISPLAY_COLUMNS,
        UNIFIED_MANAGEMENT_COLUMNS as SHARED_UNIFIED_MANAGEMENT_COLUMNS,
        normalize_godpick_dataframe,
        effective_display_dataframe,
        unified_display_columns,
        filter_effective_columns,
        V136_EFFECTIVE_REQUIRED_COLUMNS,
        dedupe_keep_order as shared_dedupe_keep_order,
    )
except Exception:
    UNIFIED_RECOMMEND_DISPLAY_COLUMNS = []
    SHARED_UNIFIED_MANAGEMENT_COLUMNS = []
    normalize_godpick_dataframe = None
    effective_display_dataframe = None
    unified_display_columns = None
    filter_effective_columns = None
    V136_EFFECTIVE_REQUIRED_COLUMNS = []
    shared_dedupe_keep_order = None

from utils import (
    format_number,
    get_history_data,
    get_realtime_stock_info,
    inject_pro_theme,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
    get_normalized_watchlist,
)

try:
    from utils import get_realtime_stock_info_batch as _rt_batch_fetch
except Exception:
    _rt_batch_fetch = None

try:
    from utils import _get_realtime_yahoo_history_fallback as _rt_yahoo_fallback
except Exception:
    _rt_yahoo_fallback = None

PAGE_TITLE = "股神推薦紀錄"
PFX = "godpick_record_"
GOD_DECISION_V10_LINK_VERSION = "record_v10_entry_decision_v1_20260428"
BACKTEST_V12_VERSION = "record_v110_official_factor_sync_20260513"
PRELAUNCH_789_VERSION = "record_prelaunch_789_delete_fix_v1_20260425"
DELETE_FIX_VERSION = "record_delete_form_atomic_v162_20260720"
RECORD_SPEED_FIX_VERSION = "record_v175_reboot_remote_authority_restore_20260727"
LATEST_PRICE_PNL_FIX_VERSION = "record_v179_official_market_snapshot_price_fix_v1_20260809"
RECORD_AUTHORITY_COMPAT_VERSION = "record_v181_authority_restore_nameerror_compat_20260809"
RECORD_INTEGRITY_FIX_VERSION = "record_v178_record_identity_perf_separation_v1_20260807"
NORMALIZED_RECORD_CACHE_FILE_V165 = "data/godpick_records_normalized_v165.pkl"
NORMALIZED_RECORD_CACHE_VERSION_V165 = "v178_integrity_20260807"
RECORD_FIX_VERSION = "record_prelaunch_grade_read_v2_verified_20260425"
MARKET_TREND_V38_LINK_VERSION = "record_market_trend_v76_practical_entry_fields_20260430"

GODPICK_RECORD_COLUMNS = [
    "record_id", "股票代號", "股票名稱", "市場別", "類別", "推薦模式", "推薦型態", "機會型態", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "機會股分數", "機會股說明", "進場時機", "進場時機分數", "建議動作", "等待條件", "近端支撐", "主要支撐", "近端壓力", "突破確認價", "停損參考", "操作區間", "風險報酬比_決策", "追高風險分數_決策", "追高風險等級", "是否建議追價", "風險扣分原因", "決策說明", "推薦等級", "推薦總分", "上漲機率估計%", "上漲機率等級", "上漲機率信心", "買點狀態", "進場型態", "高分禁買旗標", "高分禁買原因", "實戰買點分數", "實戰操作建議", "上漲機率說明", "上漲機率因子明細",
    "買點狀態",
    "進場型態",
    "高分禁買旗標",
    "高分禁買原因",
    "實戰買點分數",
    "支撐距離%",
    "壓力空間%",
    "近5日漲幅%",
    "長上影風險",
    "實戰操作建議",
    "V76買點防呆版本",
  "上漲機率估計%", "上漲機率等級", "上漲機率信心", "上漲機率說明", "上漲機率因子明細", 
    "大盤橋接分數", "大盤橋接狀態", "大盤橋接加權", "大盤橋接風控", "大盤橋接策略", "大盤橋接更新時間", "大盤交易時段", "大盤交易時段可用", "大盤資料品質", "大盤影響加減分", "大盤影響說明", "大盤資料診斷摘要",
    "股神決策模式",
    "股神進場建議",
    "推薦分層",
    "建議部位%",
    "建議倉位%",
    "建議投入等級",
    "分批策略",
    "第一筆進場%",
    "第二筆加碼條件",
    "停利策略",
    "停損策略",
    "最大風險%",
    "資金風險說明",
    "單檔風險等級",
    "族群集中警示",
    "組合配置建議", "大盤策略模式", "大盤多空分數", "推薦積極度係數", "適合推薦型態", "大盤策略建議", "大盤風控建議", "市場策略調整說明", "動態建議倉位%",

    "風險報酬比",
    "追價風險分",
    "停損距離%",
    "目標報酬%",
    "不建議買進原因",
    "最佳操作劇本",
    "隔日操作建議",
    "失效價位",
    "轉弱條件",
    "大盤情境調權說明",
    "大盤情境分桶",
    "買點分級", "風險說明", "股神推論邏輯", "權重設定", "推薦分桶", "起漲等級", "信心等級",
    "技術結構分數", "起漲前兆分數", "飆股起漲分數", "起漲摘要", "飆股起漲分數", "起漲摘要", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明",  "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", "同類股領先幅度", "是否領先同類股",
    "推薦標籤", "推薦理由摘要", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2", "推薦日期", "推薦時間",
    "建立時間", "更新時間", "目前狀態", "是否已實際買進", "實際買進價", "實際賣出價", "實際報酬%", "最新價",
    "最新價資料日期", "最新價資料時間", "最新價來源", "最新價更新狀態", "推薦基準價來源", "損益計算基準", "損益計算狀態",
    "最新更新時間", "損益金額", "損益幅%", "是否達停損", "是否達目標1", "是否達目標2", "持有天數",
    "模式績效標籤", "股神決策分數", "股神建議動作", "股神信心", "股神進場區間", "股神推論", "備註",
    "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否曾達標_回測", "達標確認狀態", "回測事件摘要", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "進場觸發狀態", "進場觸發日期", "進場評估路徑", "是否納入可執行績效", "執行基準價", "觸發訊號品質分", "觸發後收盤績效%", "觸發當日收盤績效%", "觸發當日最高報酬%", "觸發當日最大回撤%", "觸發當日收盤保留率%", "觸發收盤確認層級", "隔日候選漲跌%", "隔日執行命中結果", "隔日績效檢討標籤", "未觸發漏選標記", "候選與交易分流說明", "績效更新版本", "可執行交易1日%", "可執行交易3日%", "可執行交易5日%", "可執行交易10日%", "可執行交易20日%", "可執行交易最大漲幅%", "可執行交易最大回撤%", "除權息調整旗標", "績效計算口徑", "3日績效%", "5日績效%", "10日績效%", "20日績效%",
]

GODPICK_RECORD_COLUMNS = list(dict.fromkeys(list(GODPICK_RECORD_COLUMNS) + list(GODPICK_LEARNING_COLUMNS)))

STATUS_OPTIONS = ["觀察", "持有", "已買進", "已賣出", "停損", "達標", "取消", "封存"]

DEFAULT_STANDARD_COLS = [
    "record_id", "股票代號", "股票名稱", "市場別", "類別", "推薦模式", "推薦型態", "機會型態", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "機會股分數", "機會股說明", "進場時機", "進場時機分數", "建議動作", "等待條件", "近端支撐", "主要支撐", "近端壓力", "突破確認價", "停損參考", "操作區間", "風險報酬比_決策", "追高風險分數_決策", "追高風險等級", "是否建議追價", "風險扣分原因", "決策說明", "推薦等級", "推薦總分", "上漲機率估計%", "上漲機率等級", "上漲機率信心", "上漲機率說明", "上漲機率因子明細", 
    "大盤橋接分數", "大盤橋接狀態", "大盤橋接加權", "大盤橋接風控", "大盤橋接策略", "大盤橋接更新時間", "大盤交易時段", "大盤交易時段可用", "大盤資料品質", "大盤影響加減分", "大盤影響說明", "大盤資料診斷摘要",
    "股神決策模式",
    "股神進場建議",
    "推薦分層",
    "建議部位%",
    "風險報酬比",
    "追價風險分",
    "停損距離%",
    "目標報酬%",
    "不建議買進原因",
    "最佳操作劇本",
    "隔日操作建議",
    "失效價位",
    "轉弱條件",
    "大盤情境調權說明",
    "大盤情境分桶",
    "買點分級", "風險說明", "股神推論邏輯",
    "股神決策分數", "股神建議動作", "股神信心", "股神進場區間",
    "進場時機", "進場時機分數", "建議動作", "等待條件", "操作區間", "近端支撐", "近端壓力", "突破確認價", "停損參考", "追高風險等級", "是否建議追價", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "最新價", "損益幅%", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否曾達標_回測", "達標確認狀態", "回測事件摘要", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "進場觸發狀態", "進場觸發日期", "進場評估路徑", "是否納入可執行績效", "執行基準價", "觸發訊號品質分", "觸發後收盤績效%", "觸發當日收盤績效%", "觸發當日最高報酬%", "觸發當日最大回撤%", "觸發當日收盤保留率%", "觸發收盤確認層級", "隔日候選漲跌%", "隔日執行命中結果", "隔日績效檢討標籤", "未觸發漏選標記", "候選與交易分流說明", "績效更新版本", "可執行交易1日%", "可執行交易3日%", "可執行交易5日%", "可執行交易10日%", "可執行交易20日%", "可執行交易最大漲幅%", "可執行交易最大回撤%", "除權息調整旗標", "績效計算口徑", "3日績效%", "5日績效%", "10日績效%", "20日績效%",
    "目前狀態", "是否已實際買進", "實際買進價", "實際賣出價", "實際報酬%", "推薦日期", "推薦時間", "模式績效標籤", "備註"
]

DEFAULT_ADVANCED_COLS = [
    "record_id", "股票代號", "股票名稱", "市場別", "類別", "推薦模式", "推薦型態", "機會型態", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "機會股分數", "機會股說明", "進場時機", "進場時機分數", "建議動作", "等待條件", "近端支撐", "主要支撐", "近端壓力", "突破確認價", "停損參考", "操作區間", "風險報酬比_決策", "追高風險分數_決策", "追高風險等級", "是否建議追價", "風險扣分原因", "決策說明", "推薦等級", "推薦總分", "上漲機率估計%", "上漲機率等級", "上漲機率信心", "上漲機率說明", "上漲機率因子明細", 
    "大盤橋接分數", "大盤橋接狀態", "大盤橋接加權", "大盤橋接風控", "大盤橋接策略", "大盤橋接更新時間", "大盤交易時段", "大盤交易時段可用", "大盤資料品質", "大盤影響加減分", "大盤影響說明", "大盤資料診斷摘要",
    "股神決策模式",
    "股神進場建議",
    "推薦分層",
    "建議部位%",
    "風險報酬比",
    "追價風險分",
    "停損距離%",
    "目標報酬%",
    "不建議買進原因",
    "最佳操作劇本",
    "買點分級", "風險說明", "股神推論邏輯", "權重設定", "推薦分桶", "起漲等級", "信心等級",
    "技術結構分數", "起漲前兆分數", "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明",  "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", "股神決策分數", "股神建議動作",
    "股神信心", "股神進場區間", "進場時機", "進場時機分數", "建議動作", "等待條件", "操作區間", "近端支撐", "近端壓力", "突破確認價", "停損參考", "追高風險等級", "是否建議追價", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2",
    "最新價", "損益幅%", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否曾達標_回測", "達標確認狀態", "回測事件摘要", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "進場觸發狀態", "進場觸發日期", "進場評估路徑", "是否納入可執行績效", "執行基準價", "觸發訊號品質分", "觸發後收盤績效%", "觸發當日收盤績效%", "觸發當日最高報酬%", "觸發當日最大回撤%", "觸發當日收盤保留率%", "觸發收盤確認層級", "隔日候選漲跌%", "隔日執行命中結果", "隔日績效檢討標籤", "未觸發漏選標記", "候選與交易分流說明", "績效更新版本", "可執行交易1日%", "可執行交易3日%", "可執行交易5日%", "可執行交易10日%", "可執行交易20日%", "可執行交易最大漲幅%", "可執行交易最大回撤%", "除權息調整旗標", "績效計算口徑", "3日績效%", "5日績效%", "10日績效%", "20日績效%", "目前狀態", "是否已實際買進",
    "實際買進價", "實際賣出價", "實際報酬%", "是否達停損", "是否達目標1", "是否達目標2", "持有天數",
    "推薦日期", "推薦時間", "模式績效標籤", "股神推論", "機會股說明", "推薦理由摘要", "備註"
]

FAST_VISIBLE_LIMIT = 500
UI_CONFIG_DEFAULT = {
    "fast_mode": True,
    "visible_limit": FAST_VISIBLE_LIMIT,
    "profiles": {
        "標準": DEFAULT_STANDARD_COLS.copy(),
        "進階": DEFAULT_ADVANCED_COLS.copy(),
    },
    "updated_at": "",
}


# v74 欄位統一：覆蓋標準/進階欄位，讓 8_股神推薦紀錄與 7_股神推薦完整推薦表一致。
try:
    if UNIFIED_RECOMMEND_DISPLAY_COLUMNS:
        GODPICK_RECORD_COLUMNS = shared_dedupe_keep_order((GODPICK_RECORD_COLUMNS or []) + list(UNIFIED_RECOMMEND_DISPLAY_COLUMNS)) if shared_dedupe_keep_order else list(dict.fromkeys((GODPICK_RECORD_COLUMNS or []) + list(UNIFIED_RECOMMEND_DISPLAY_COLUMNS)))
        DEFAULT_STANDARD_COLS = [c for c in UNIFIED_RECOMMEND_DISPLAY_COLUMNS if c in GODPICK_RECORD_COLUMNS]
        DEFAULT_ADVANCED_COLS = [c for c in GODPICK_RECORD_COLUMNS if c not in []]
        UI_CONFIG_DEFAULT["profiles"]["標準"] = DEFAULT_STANDARD_COLS.copy()
        UI_CONFIG_DEFAULT["profiles"]["進階"] = DEFAULT_ADVANCED_COLS.copy()
except Exception:
    pass



# >>> V72_FACTOR_ENRICH_HELPER
def _v72_enrich_recommendation_df_safe(df):
    try:
        return enrich_dataframe(df)
    except Exception:
        try:
            return ensure_factor_columns(df)
        except Exception:
            return df
# <<< V72_FACTOR_ENRICH_HELPER

def _dedupe_keep_order(seq):
    out = []
    seen = set()
    for x in seq:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

GODPICK_RECORD_COLUMNS = _dedupe_keep_order(GODPICK_RECORD_COLUMNS)
DEFAULT_STANDARD_COLS = _dedupe_keep_order(DEFAULT_STANDARD_COLS)
DEFAULT_ADVANCED_COLS = _dedupe_keep_order(DEFAULT_ADVANCED_COLS)

# V176：最新價與損益必須可稽核。即使共用欄位設定沒有這些新欄，
# 第 8 頁仍會保存並在標準/進階設定中提供顯示。
V176_PRICE_AUDIT_COLUMNS = [
    "推薦價格", "推薦日價格", "最新價", "最新價資料日期", "最新價資料時間",
    "最新價來源", "最新價更新狀態", "推薦基準價來源",
    "損益金額", "損益幅%", "損益計算基準", "損益計算狀態",
]
for _v176_col in V176_PRICE_AUDIT_COLUMNS:
    if _v176_col not in GODPICK_RECORD_COLUMNS:
        GODPICK_RECORD_COLUMNS.append(_v176_col)
    if _v176_col not in DEFAULT_STANDARD_COLS:
        DEFAULT_STANDARD_COLS.append(_v176_col)
    if _v176_col not in DEFAULT_ADVANCED_COLS:
        DEFAULT_ADVANCED_COLS.append(_v176_col)
V178_INTEGRITY_COLUMNS = [
    "原始record_id", "record_id修復狀態", "業務事件重複狀態", "資料完整性狀態",
    "系統追蹤每股損益", "系統追蹤報酬%", "實際未實現報酬%", "實際已實現報酬%",
    "績效最新收盤價", "績效行情日期", "績效行情來源",
]
for _v178_col in V178_INTEGRITY_COLUMNS:
    if _v178_col not in GODPICK_RECORD_COLUMNS:
        GODPICK_RECORD_COLUMNS.append(_v178_col)
    if _v178_col not in DEFAULT_ADVANCED_COLS:
        DEFAULT_ADVANCED_COLS.append(_v178_col)
for _v178_col in ["資料完整性狀態", "系統追蹤每股損益", "系統追蹤報酬%", "實際未實現報酬%", "實際已實現報酬%"]:
    if _v178_col not in DEFAULT_STANDARD_COLS:
        DEFAULT_STANDARD_COLS.append(_v178_col)
UI_CONFIG_DEFAULT["profiles"]["標準"] = DEFAULT_STANDARD_COLS.copy()
UI_CONFIG_DEFAULT["profiles"]["進階"] = DEFAULT_ADVANCED_COLS.copy()


def _k(key: str) -> str:
    return f"{PFX}{key}"


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _safe_float(v: Any, default=None):
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return default



def _derive_prelaunch_grade_from_score(score: Any) -> str:
    """依起漲前兆分數補齊舊紀錄的起漲等級。"""
    s = _safe_float(score, 0) or 0
    if s >= 88:
        return "S｜強烈起漲"
    if s >= 78:
        return "A｜起漲優先"
    if s >= 68:
        return "B｜轉強確認"
    if s >= 55:
        return "C｜初步轉強"
    return "D｜尚未起漲"


# 相容保險：處理任何舊版呼叫名稱
def derive_prelaunch_grade_from_score(score: Any) -> str:
    return _derive_prelaunch_grade_from_score(score)



def _normalize_code(v: Any) -> str:
    s = _safe_str(v)
    if not s:
        return ""
    if s.isdigit():
        return s
    digits = "".join(ch for ch in s if ch.isdigit())
    if 4 <= len(digits) <= 6:
        return digits
    return s


def _normalize_bool(v: Any) -> bool:
    return _safe_str(v).lower() in {"true", "1", "yes", "y", "是"}


def _normalize_category(v: Any) -> str:
    return _safe_str(v).replace("　", " ").strip()


_TW_TZ = ZoneInfo("Asia/Taipei")


def _tw_now() -> datetime:
    return datetime.now(_TW_TZ)


def _tw_today() -> date:
    return _tw_now().date()


def _tw_now_naive() -> datetime:
    return _tw_now().replace(tzinfo=None)


def _now_text() -> str:
    return _tw_now().strftime("%Y-%m-%d %H:%M:%S")


def _now_date_text() -> str:
    return _tw_now().strftime("%Y-%m-%d")


def _now_time_text() -> str:
    return _tw_now().strftime("%H:%M:%S")


def _create_record_id(code: str, rec_date: str, rec_time: str, mode: str) -> str:
    raw = f"{_safe_str(code)}|{_safe_str(rec_date)}|{_safe_str(rec_time)}|{_safe_str(mode)}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _set_status(msg: str, level: str = "info"):
    st.session_state[_k("status_msg")] = msg
    st.session_state[_k("status_type")] = level


def _add_action_result(action: str, ok: bool, message: str, detail: str = ""):
    """v104：五個主操作按鈕都要留下明確結果，成功/失敗都顯示。"""
    try:
        rows = st.session_state.get(_k("action_results"), [])
        if not isinstance(rows, list):
            rows = []
        rows.insert(0, {
            "時間": _now_text(),
            "操作": _safe_str(action),
            "結果": "成功" if ok else "失敗",
            "訊息": _safe_str(message),
            "明細": _safe_str(detail),
        })
        st.session_state[_k("action_results")] = rows[:10]
    except Exception:
        pass


def _render_action_results():
    """v104：固定顯示最近操作結果，避免按鈕失敗時使用者不知道原因。"""
    rows = st.session_state.get(_k("action_results"), [])
    if not rows:
        st.info("v104：主操作結果會顯示在這裡：重新載入、更新最新價、儲存同步、清除快取、更新推薦後績效。")
        return
    latest = rows[0]
    msg = f"{latest.get('操作', '')}｜{latest.get('結果', '')}｜{latest.get('訊息', '')}"
    if latest.get("結果") == "成功":
        st.success(msg)
    else:
        st.error(msg)
    with st.expander("最近主操作結果明細（成功 / 失敗都保留）", expanded=False):
        try:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        except Exception:
            st.write(rows)


def _github_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("GODPICK_RECORDS_GITHUB_PATH", "godpick_records.json")) or "godpick_records.json",
    }


def _watchlist_github_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("WATCHLIST_GITHUB_PATH", "watchlist.json")) or "watchlist.json",
    }


def _ui_config_github_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("GODPICK_UI_CONFIG_GITHUB_PATH", "godpick_record_ui_config.json")) or "godpick_record_ui_config.json",
    }


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


def _firebase_config() -> dict[str, str]:
    return {
        "project_id": _safe_str(st.secrets.get("FIREBASE_PROJECT_ID", "")),
        "client_email": _safe_str(st.secrets.get("FIREBASE_CLIENT_EMAIL", "")),
        "private_key": _safe_str(st.secrets.get("FIREBASE_PRIVATE_KEY", "")),
    }


def _clean_private_key(raw_key: str) -> str:
    private_key = _safe_str(raw_key).replace("\\n", "\n").strip()
    if private_key.startswith("\ufeff"):
        private_key = private_key.lstrip("\ufeff")
    return private_key


def _init_firebase_app():
    if firebase_admin is None or credentials is None or firestore is None:
        raise RuntimeError("firebase-admin 未安裝或無法載入；已略過 Firestore，同步改用本機/GitHub。")
    try:
        return firebase_admin.get_app()
    except ValueError:
        pass

    cfg = _firebase_config()
    project_id = _safe_str(cfg["project_id"]).strip()
    client_email = _safe_str(cfg["client_email"]).strip()
    private_key = _clean_private_key(cfg["private_key"])

    if not project_id:
        raise ValueError("缺少 FIREBASE_PROJECT_ID")
    if not client_email:
        raise ValueError("缺少 FIREBASE_CLIENT_EMAIL")
    if not private_key:
        raise ValueError("缺少 FIREBASE_PRIVATE_KEY")
    if "BEGIN PRIVATE KEY" not in private_key or "END PRIVATE KEY" not in private_key:
        raise ValueError("FIREBASE_PRIVATE_KEY 不是有效 PEM 格式")

    cred_dict = {
        "type": "service_account",
        "project_id": project_id,
        "private_key": private_key,
        "client_email": client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    cred = credentials.Certificate(cred_dict)
    return firebase_admin.initialize_app(cred, {"projectId": project_id})



def _derive_prelaunch_summary_from_row(row: pd.Series) -> str:
    s = _safe_float(row.get("飆股起漲分數"), row.get("起漲前兆分數")) or 0
    text = _safe_str(row.get("起漲摘要"))
    if text:
        return text
    parts = []
    if s >= 90:
        parts.append("接近漲停")
    elif s >= 78:
        parts.append("強漲")
    elif s >= 68:
        parts.append("明顯上漲")
    elif s >= 55:
        parts.append("小漲轉強")
    if _safe_float(row.get("爆發力分數"), 0) and _safe_float(row.get("爆發力分數"), 0) >= 70:
        parts.append("量能放大")
    if _safe_float(row.get("型態突破分數"), 0) and _safe_float(row.get("型態突破分數"), 0) >= 70:
        parts.append("突破結構")
    return "、".join(parts) if parts else "未見明顯起漲訊號"



def _dedupe_keep_order_v73(cols: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for c in cols or []:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out


V73_MESSAGE_TEXT_FIELDS = [
    "推薦型態", "機會型態", "機會股說明", "進場時機", "建議動作", "等待條件",
    "追高風險等級", "是否建議追價", "風險扣分原因", "決策說明",
    "上漲機率等級", "上漲機率信心", "買點狀態", "進場型態", "高分禁買旗標", "高分禁買原因",
    "實戰操作建議", "上漲機率說明", "上漲機率因子明細",
    "大盤橋接狀態", "大盤橋接加權", "大盤橋接風控", "大盤橋接策略", "大盤橋接更新時間",
    "大盤交易時段", "大盤交易時段可用", "大盤資料品質", "大盤影響說明", "大盤資料診斷摘要",
    "股神決策模式", "股神進場建議", "推薦分層", "建議投入等級", "分批策略",
    "第二筆加碼條件", "停利策略", "停損策略", "資金風險說明", "單檔風險等級",
    "族群集中警示", "組合配置建議", "大盤策略模式", "適合推薦型態", "大盤策略建議",
    "大盤風控建議", "市場策略調整說明", "不建議買進原因", "最佳操作劇本", "隔日操作建議",
    "失效價位", "轉弱條件", "大盤情境調權說明", "大盤情境分桶",
    "買點分級", "風險說明", "股神推論邏輯", "權重設定", "推薦分桶", "起漲等級", "信心等級",
    "起漲摘要", "強勢族群等級", "族群輪動狀態", "族群策略建議", "族群資金流說明",
    "是否領先同類股", "推薦標籤", "推薦理由摘要", "K線驗證標記", "推薦日支撐壓力摘要",
    "K線查詢參數", "K線檢視提示", "建立時間", "更新時間", "目前狀態", "最新更新時間",
    "最新價資料日期", "最新價資料時間", "最新價來源", "最新價更新狀態", "推薦基準價來源", "損益計算基準", "損益計算狀態",
    "模式績效標籤", "股神建議動作", "股神信心", "股神進場區間", "股神推論", "績效資料型態", "績效資料來源", "備註",
    "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間",
]


V73_NUMERIC_FIELDS = [
    "推薦總分", "上漲機率估計%", "大盤橋接分數", "大盤可參考分數", "大盤加權分", "大盤影響加減分",
    "技術結構分數", "起漲前兆分數", "飆股起漲分數", "交易可行分數", "類股熱度分數",
    "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "進場時機分數",
    "近端支撐", "主要支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2",
    "風險報酬比_決策", "追高風險分數_決策", "建議部位%", "建議倉位%", "第一筆進場%",
    "最大風險%", "大盤多空分數", "推薦積極度係數", "動態建議倉位%", "風險報酬比", "追價風險分",
    "停損距離%", "目標報酬%", "族群資金流分數", "同族群強勢比例", "同族群推薦密度",
    "同族群平均量能分", "同類股領先幅度", "推薦價格", "推薦日價格", "實際買進價", "實際賣出價",
    "實際報酬%", "最新價", "損益金額", "損益幅%", "持有天數", "股神決策分數",
    "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%",
    "即時追蹤報酬%", "3日績效%", "5日績效%", "10日績效%", "20日績效%",
]




# >>> V98_NIGHT_BATTLE_RECORD_SYNC
# V98：同步 07/10 夜間隔日股神欄位到 8_股神推薦紀錄。
# 採安全補欄，不改舊主流程，避免舊 GitHub/JSON 紀錄因混合型資料造成頁面錯誤。
V98_NIGHT_NUMERIC_FIELDS = [
    "夜間股神總分", "隔日實戰排序分", "隔日進場分數", "波段潛力分數",
    "技術趨勢分數", "量價動能分數", "法人籌碼分數", "大戶鎖碼分數",
    "基本面成長分數", "營收成長分數", "EPS成長分數", "估值風險分數",
    "PER本益比", "估算EPS", "預估進場點", "回測承接價",
    "突破確認價_隔日", "停損價_隔日", "第一壓力價",
]
V98_NIGHT_TEXT_FIELDS = [
    "進場型態_隔日", "隔日建議動作", "夜間股神建議", "隔日作戰策略",
    "資料完整度", "觀察週期", "進場條件說明", "不追高條件", "夜間風險提醒",
    "法人籌碼摘要", "基本面摘要", "估值風險摘要",
]
V98_NIGHT_DISPLAY_COLS = [
    "推薦日期", "推薦時間", "股票代號", "股票名稱", "類別", "推薦總分",
    "夜間股神總分", "隔日實戰排序分", "隔日進場分數", "波段潛力分數",
    "進場型態_隔日", "隔日建議動作", "預估進場點", "回測承接價",
    "突破確認價_隔日", "停損價_隔日", "第一壓力價", "夜間股神建議", "隔日作戰策略", "資料完整度",
]
for _v98_c in V98_NIGHT_NUMERIC_FIELDS:
    if _v98_c not in V73_NUMERIC_FIELDS:
        V73_NUMERIC_FIELDS.append(_v98_c)
    if _v98_c not in GODPICK_RECORD_COLUMNS:
        GODPICK_RECORD_COLUMNS.append(_v98_c)
for _v98_c in V98_NIGHT_TEXT_FIELDS:
    if _v98_c not in V73_MESSAGE_TEXT_FIELDS:
        V73_MESSAGE_TEXT_FIELDS.append(_v98_c)
    if _v98_c not in GODPICK_RECORD_COLUMNS:
        GODPICK_RECORD_COLUMNS.append(_v98_c)
for _v98_c in V98_NIGHT_DISPLAY_COLS:
    if _v98_c not in DEFAULT_STANDARD_COLS:
        DEFAULT_STANDARD_COLS.append(_v98_c)
    if _v98_c not in DEFAULT_ADVANCED_COLS:
        DEFAULT_ADVANCED_COLS.append(_v98_c)


def _v98_scalar_text(v: Any) -> str:
    """把舊紀錄的 list/dict/Series/array 轉成穩定文字，避免 pandas 指派或顯示爆錯。"""
    try:
        if isinstance(v, pd.Series):
            vals = [_v98_scalar_text(x) for x in v.tolist()]
            vals = [x for x in vals if x]
            return " / ".join(vals[:5])
    except Exception:
        pass
    if isinstance(v, dict):
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)
    if isinstance(v, (list, tuple, set)):
        vals = [_v98_scalar_text(x) for x in list(v)]
        vals = [x for x in vals if x]
        return " / ".join(vals[:5])
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    s = str(v).strip()
    if s.lower() in {"none", "nan", "nat", "null", "<na>"}:
        return ""
    return s


def _v98_scalar_float(v: Any, default=None):
    if isinstance(v, pd.Series):
        for item in v.tolist():
            got = _v98_scalar_float(item, None)
            if got is not None:
                return got
        return default
    if isinstance(v, dict):
        return default
    if isinstance(v, (list, tuple, set)):
        for item in list(v):
            got = _v98_scalar_float(item, None)
            if got is not None:
                return got
        return default
    s = _v98_scalar_text(v).replace("%", "").replace(",", "")
    if not s:
        return default
    try:
        return float(s)
    except Exception:
        return default


def _v98_first_text(row: pd.Series, cols: list[str], default: str = "") -> str:
    for c in cols:
        if c not in row.index:
            continue
        s = _v98_scalar_text(row.get(c))
        if s:
            return s
    return default


def _v98_first_num(row: pd.Series, cols: list[str], default=None):
    for c in cols:
        if c not in row.index:
            continue
        n = _v98_scalar_float(row.get(c), None)
        if n is not None:
            return n
    return default


def _v98_derive_entry_type(row: pd.Series) -> str:
    existing = _v98_first_text(row, ["進場型態_隔日", "進場型態", "推薦型態", "買點分級", "起漲等級"])
    if existing:
        return existing
    entry = _v98_first_num(row, ["隔日進場分數", "進場時機分數", "實戰買點分數", "推薦總分"], 0) or 0
    score = _v98_first_num(row, ["夜間股神總分", "隔日實戰排序分", "推薦總分"], 0) or 0
    support = _v98_first_num(row, ["回測承接價", "近端支撐", "主要支撐"], None)
    breakout = _v98_first_num(row, ["突破確認價_隔日", "突破確認價", "近端壓力"], None)
    if entry >= 82 and breakout is not None:
        return "隔日突破型"
    if support is not None and entry >= 68:
        return "回測承接型"
    if score >= 80:
        return "剛起漲型"
    return "夜間觀察型"


def _v98_derive_action(row: pd.Series) -> str:
    existing = _v98_first_text(row, ["隔日建議動作", "股神建議動作", "建議動作", "實戰操作建議", "隔日操作建議"])
    if existing:
        return existing
    entry = _v98_first_num(row, ["隔日進場分數", "進場時機分數", "實戰買點分數", "推薦總分"], 0) or 0
    typ = _v98_derive_entry_type(row)
    if entry >= 82:
        return "隔日高度關注，符合條件可小量分批"
    if "突破" in typ:
        return "等突破確認"
    if "回測" in typ:
        return "等拉回承接"
    if entry >= 65:
        return "觀察確認後再進場"
    return "先觀察，不追高"


def _v98_backfill_night_battle_record_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame):
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)
    if df.empty:
        out = df.copy()
        for c in V98_NIGHT_NUMERIC_FIELDS + V98_NIGHT_TEXT_FIELDS:
            if c not in out.columns:
                out[c] = pd.NA if c in V98_NIGHT_NUMERIC_FIELDS else ""
        return out

    out = df.loc[:, ~pd.Index(df.columns).duplicated()].copy()
    for c in V98_NIGHT_NUMERIC_FIELDS:
        if c not in out.columns:
            out[c] = pd.NA
    for c in V98_NIGHT_TEXT_FIELDS:
        if c not in out.columns:
            out[c] = ""

    rows = list(out.iterrows())
    numeric_aliases = {
        "夜間股神總分": ["夜間股神總分", "隔日實戰排序分", "股神決策分數", "推薦總分", "推薦分數"],
        "隔日實戰排序分": ["隔日實戰排序分", "夜間股神總分", "股神決策分數", "推薦總分", "推薦分數"],
        "隔日進場分數": ["隔日進場分數", "進場時機分數", "實戰買點分數", "交易可行分數", "推薦總分"],
        "波段潛力分數": ["波段潛力分數", "推薦總分", "技術結構分數", "起漲前兆分數"],
        "預估進場點": ["預估進場點", "推薦價格", "推薦日價格", "建議價位"],
        "回測承接價": ["回測承接價", "近端支撐", "主要支撐", "推薦買點_拉回"],
        "突破確認價_隔日": ["突破確認價_隔日", "突破確認價", "推薦買點_突破", "近端壓力"],
        "停損價_隔日": ["停損價_隔日", "停損價", "停損參考"],
        "第一壓力價": ["第一壓力價", "近端壓力", "賣出目標1"],
    }
    text_aliases = {
        "進場型態_隔日": ["進場型態_隔日", "進場型態", "推薦型態", "買點分級", "起漲等級"],
        "隔日建議動作": ["隔日建議動作", "股神建議動作", "建議動作", "實戰操作建議", "隔日操作建議"],
        "夜間股神建議": ["夜間股神建議", "股神推論邏輯", "股神推論", "推薦理由摘要", "決策說明"],
        "隔日作戰策略": ["隔日作戰策略", "隔日操作建議", "最佳操作劇本", "股神進場建議", "操作區間"],
        "資料完整度": ["資料完整度", "大盤資料品質", "績效資料來源"],
        "觀察週期": ["觀察週期", "進場時機", "等待條件"],
        "進場條件說明": ["進場條件說明", "等待條件", "K線檢視提示"],
        "不追高條件": ["不追高條件", "不建議買進原因", "風險扣分原因"],
        "夜間風險提醒": ["夜間風險提醒", "風險說明", "大盤風控建議", "轉弱條件"],
        "法人籌碼摘要": ["法人籌碼摘要", "族群資金流說明"],
        "基本面摘要": ["基本面摘要", "推薦理由摘要"],
        "估值風險摘要": ["估值風險摘要", "風險說明", "不建議買進原因"],
    }

    for target, aliases in numeric_aliases.items():
        vals = []
        for _, row in rows:
            current = _v98_scalar_float(row.get(target), None)
            vals.append(current if current is not None else _v98_first_num(row, aliases, pd.NA))
        out[target] = vals
    for target, aliases in text_aliases.items():
        vals = []
        for _, row in rows:
            current = _v98_scalar_text(row.get(target))
            vals.append(current or _v98_first_text(row, aliases, ""))
        out[target] = vals

    # 進一步推導主欄位，讓舊紀錄也能直接進行夜間追蹤。
    out["進場型態_隔日"] = [_v98_scalar_text(v) or _v98_derive_entry_type(row) for (_, row), v in zip(rows, out["進場型態_隔日"].tolist())]
    out["隔日建議動作"] = [_v98_scalar_text(v) or _v98_derive_action(row) for (_, row), v in zip(rows, out["隔日建議動作"].tolist())]
    out["資料完整度"] = [_v98_scalar_text(v) or "舊紀錄補欄" for v in out["資料完整度"].tolist()]
    out["夜間股神建議"] = [_v98_scalar_text(v) or "已保留原推薦紀錄，夜間欄位由 V98 相容層補齊。" for v in out["夜間股神建議"].tolist()]
    out["隔日作戰策略"] = [_v98_scalar_text(v) or _v98_derive_action(row) for (_, row), v in zip(rows, out["隔日作戰策略"].tolist())]

    for c in V98_NIGHT_NUMERIC_FIELDS:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    for c in V98_NIGHT_TEXT_FIELDS:
        out[c] = out[c].map(_v98_scalar_text)
    return out
# <<< V98_NIGHT_BATTLE_RECORD_SYNC

# >>> V110_OFFICIAL_FACTOR_RECORD_SYNC
# V110：8_股神推薦紀錄保存 16_官方因子快取中心欄位；只讀快取，不連官方網站。
OFFICIAL_FACTOR_COLUMNS_V110 = list(OFFICIAL_FACTOR_SERVICE_COLUMNS or [
    "官方資料日期", "外資近1日買賣超", "外資近3日買賣超", "外資近5日買賣超",
    "投信近1日買賣超", "投信近3日買賣超", "投信近5日買賣超",
    "自營商近1日買賣超", "自營商近3日買賣超", "自營商近5日買賣超",
    "三大法人近1日合計", "三大法人近3日合計", "三大法人近5日合計",
    "法人連買天數", "法人籌碼官方分數", "當月營收", "月營收MoM%", "月營收YoY%",
    "累計營收YoY%", "營收年月", "營收成長官方分數", "PER本益比", "PBR股價淨值比",
    "股利殖利率%", "估算EPS", "官方估值風險分數", "官方基本面成長分數",
    "官方因子總分", "官方資料完整度", "官方因子資料狀態", "官方因子更新時間", "官方因子資料源",
])
OFFICIAL_FACTOR_COLUMNS_V110 = [c for c in OFFICIAL_FACTOR_COLUMNS_V110 if c not in {"股票代號", "股票名稱", "市場別", "正式產業別"}]
OFFICIAL_FACTOR_NUMERIC_FIELDS_V110 = [
    "外資近1日買賣超", "外資近3日買賣超", "外資近5日買賣超",
    "投信近1日買賣超", "投信近3日買賣超", "投信近5日買賣超",
    "自營商近1日買賣超", "自營商近3日買賣超", "自營商近5日買賣超",
    "三大法人近1日合計", "三大法人近3日合計", "三大法人近5日合計",
    "法人連買天數", "法人籌碼官方分數", "當月營收", "月營收MoM%", "月營收YoY%",
    "累計營收YoY%", "營收成長官方分數", "PER本益比", "PBR股價淨值比",
    "股利殖利率%", "估算EPS", "官方估值風險分數", "官方基本面成長分數",
    "官方因子總分", "官方資料完整度",
]
for _v110_c in OFFICIAL_FACTOR_COLUMNS_V110:
    if _v110_c not in GODPICK_RECORD_COLUMNS:
        GODPICK_RECORD_COLUMNS.append(_v110_c)
    if _v110_c in OFFICIAL_FACTOR_NUMERIC_FIELDS_V110 and _v110_c not in V73_NUMERIC_FIELDS:
        V73_NUMERIC_FIELDS.append(_v110_c)
# <<< V110_OFFICIAL_FACTOR_RECORD_SYNC


# >>> V120_REAL_QUALITY_RECORD_SYNC
# V120：同步 07/V118 實戰品質防呆欄位到 8_股神推薦紀錄，並提供品質分層準確率分析。
V120_QUALITY_NUMERIC_FIELDS = [
    "股神輸出排序", "候補排序分", "主推薦排序分", "實戰主推薦分", "實戰品質分", "實戰降分",
    "最新成交量", "5日均量", "20日均量", "均量比", "收盤距MA20%", "收盤距MA60%",
    "量能啟動分", "均線轉強分", "動能翻多分", "突破準備分", "支撐防守分",
]
V120_QUALITY_TEXT_FIELDS = [
    "股神推薦層級", "候補等級", "是否主要顯示", "主表篩選", "股神輸出排序", "候補排序分",
    "股神實戰建議", "限制原因", "族群名稱", "資金流熱門族群", "族群熱度排名",
    "族群資金流分數", "族群流動性分數", "族群樣本數", "族群判斷依據", "大盤趨勢模式",
    "成交額百萬", "20日均成交額百萬", "流動性等級", "實戰版本",
    "原始推薦總分", "實戰調整推薦分", "主推薦排序分", "實戰主推薦分", "主推薦不合格原因",
    "實戰品質分", "量能狀態", "趨勢狀態", "實戰降分", "實戰品質提醒",
]
V120_QUALITY_COLUMNS = V120_QUALITY_NUMERIC_FIELDS + V120_QUALITY_TEXT_FIELDS
V120_QUALITY_DISPLAY_COLS = [
    "推薦日期", "推薦時間", "股票代號", "股票名稱", "類別", "產業",
    "股神推薦層級", "候補等級", "是否主要顯示", "主表篩選", "股神輸出排序", "候補排序分",
    "股神實戰建議", "限制原因", "族群名稱", "資金流熱門族群", "族群熱度排名", "族群資金流分數", "族群流動性分數",
    "成交額百萬", "20日均成交額百萬", "流動性等級", "大盤趨勢模式",
    "推薦總分", "原始推薦總分", "實戰調整推薦分", "夜間股神總分", "隔日進場分數",
    "實戰品質分", "量能狀態", "趨勢狀態", "實戰降分", "實戰品質提醒",
    "最新成交量", "5日均量", "20日均量", "均量比", "收盤距MA20%", "收盤距MA60%",
    "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "命中結果", "達標確認狀態",
]
for _v120_c in V120_QUALITY_COLUMNS:
    if _v120_c not in GODPICK_RECORD_COLUMNS:
        GODPICK_RECORD_COLUMNS.append(_v120_c)
    if _v120_c in V120_QUALITY_NUMERIC_FIELDS and _v120_c not in V73_NUMERIC_FIELDS:
        V73_NUMERIC_FIELDS.append(_v120_c)

def _v120_blank(v: Any) -> bool:
    try:
        return _is_empty_display_value(v)
    except Exception:
        if v is None:
            return True
        s = str(v).strip()
        return s == "" or s.lower() in {"nan", "none", "null", "<na>"}

def _v120_num(row: Any, names: list[str], default: Any = pd.NA) -> Any:
    try:
        for c in names:
            val = row.get(c, pd.NA)
            if not _v120_blank(val):
                n = pd.to_numeric(pd.Series([val]), errors="coerce").iloc[0]
                if pd.notna(n):
                    return float(n)
    except Exception:
        pass
    return default

def _v120_text(row: Any, names: list[str], default: str = "") -> str:
    try:
        for c in names:
            val = row.get(c, "")
            if not _v120_blank(val):
                return str(val).strip()
    except Exception:
        pass
    return default

def _v120_derive_volume_state(row: Any) -> str:
    if _v120_text(row, ["量能狀態"]):
        return _v120_text(row, ["量能狀態"])
    ratio = _v120_num(row, ["均量比", "量比", "成交量比"], None)
    vol20 = _v120_num(row, ["20日均量", "20日平均量", "月均量"], None)
    vol_score = _v120_num(row, ["量能啟動分", "交易可行分數"], None)
    if ratio is not None and ratio >= 1.2:
        return "量能轉強"
    if vol_score is not None and vol_score >= 70:
        return "量能可用"
    if vol20 is not None and vol20 < 300:
        return "低量警示"
    if ratio is not None and ratio < 0.8:
        return "量能不足"
    return "待觀察"

def _v120_derive_trend_state(row: Any) -> str:
    if _v120_text(row, ["趨勢狀態"]):
        return _v120_text(row, ["趨勢狀態"])
    ma20 = _v120_num(row, ["收盤距MA20%"], None)
    ma60 = _v120_num(row, ["收盤距MA60%"], None)
    tech = _v120_num(row, ["技術結構分數", "均線轉強分", "動能翻多分"], None)
    if tech is not None and tech >= 70 and (ma20 is None or ma20 >= 0):
        return "趨勢轉強"
    if ma20 is not None and ma20 < 0 and (ma60 is not None and ma60 < 0):
        return "均線偏弱"
    if tech is not None and tech < 50:
        return "趨勢不足"
    return "待確認"

def _v120_quality_bucket(v: Any) -> str:
    n = _v98_scalar_float(v, None)
    if n is None or pd.isna(n):
        return "未分層"
    if n >= 80:
        return "A 高品質>=80"
    if n >= 70:
        return "B 可操作70-79"
    if n >= 60:
        return "C 觀察60-69"
    return "D 低品質<60"

def _v120_backfill_quality_record_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)
    out = df.copy()
    if out.empty:
        for c in V120_QUALITY_COLUMNS:
            if c not in out.columns:
                out[c] = pd.NA if c in V120_QUALITY_NUMERIC_FIELDS else ""
        return out
    for c in V120_QUALITY_COLUMNS:
        if c not in out.columns:
            out[c] = pd.NA if c in V120_QUALITY_NUMERIC_FIELDS else ""
    rows = list(out.iterrows())
    num_aliases = {
        "實戰品質分": ["實戰品質分", "交易可行分數", "技術結構分數", "推薦總分"],
        "實戰降分": ["實戰降分", "品質降分", "風險扣分", "追高風險分數_決策"],
        "最新成交量": ["最新成交量", "成交量", "今日成交量"],
        "5日均量": ["5日均量", "5日平均量", "週均量"],
        "20日均量": ["20日均量", "20日平均量", "月均量"],
        "均量比": ["均量比", "量比", "成交量比"],
        "收盤距MA20%": ["收盤距MA20%", "距月線%", "MA20乖離%"],
        "收盤距MA60%": ["收盤距MA60%", "距季線%", "MA60乖離%"],
        "量能啟動分": ["量能啟動分", "交易可行分數", "爆發力分數"],
        "均線轉強分": ["均線轉強分", "技術結構分數"],
        "動能翻多分": ["動能翻多分", "起漲前兆分數", "飆股起漲分數"],
        "突破準備分": ["突破準備分", "型態突破分數", "突破分數"],
        "支撐防守分": ["支撐防守分", "支撐回測分數", "拉回承接分數"],
    }
    for target, aliases in num_aliases.items():
        vals = []
        for _, row in rows:
            cur = _v120_num(row, [target], None)
            vals.append(cur if cur is not None else _v120_num(row, aliases, pd.NA))
        out[target] = pd.to_numeric(pd.Series(vals, index=out.index), errors="coerce")
    # 若沒有實戰品質分，用技術 / 量能 / 起漲分保守估算，只作歷史分析，不回推 07 分數。
    if "實戰品質分" in out.columns:
        q = pd.to_numeric(out["實戰品質分"], errors="coerce")
        need = q.isna()
        if need.any():
            parts = []
            for c in ["量能啟動分", "均線轉強分", "動能翻多分", "交易可行分數", "技術結構分數"]:
                if c in out.columns:
                    parts.append(pd.to_numeric(out[c], errors="coerce"))
            if parts:
                est = pd.concat(parts, axis=1).mean(axis=1, skipna=True)
                out.loc[need, "實戰品質分"] = est.loc[need]
    for c in ["量能狀態", "趨勢狀態", "實戰品質提醒"]:
        out[c] = out[c].map(lambda v: "" if _v120_blank(v) else str(v).strip())
    out["量能狀態"] = [v or _v120_derive_volume_state(row) for (_, row), v in zip(rows, out["量能狀態"].tolist())]
    out["趨勢狀態"] = [v or _v120_derive_trend_state(row) for (_, row), v in zip(rows, out["趨勢狀態"].tolist())]
    alerts = []
    for _, row in out.iterrows():
        existing = _v120_text(row, ["實戰品質提醒"])
        if existing:
            alerts.append(existing)
            continue
        q = _v120_num(row, ["實戰品質分"], None)
        vol_state = _v120_text(row, ["量能狀態"])
        trend_state = _v120_text(row, ["趨勢狀態"])
        notes = []
        if q is not None and q < 60:
            notes.append("品質分偏低")
        if any(k in vol_state for k in ["低量", "不足"]):
            notes.append("量能不足")
        if any(k in trend_state for k in ["偏弱", "不足"]):
            notes.append("趨勢未確認")
        alerts.append("、".join(notes) if notes else "實戰品質可追蹤")
    out["實戰品質提醒"] = alerts
    return out

def _v120_primary_return(df: pd.DataFrame, prefer: str = "10日") -> pd.Series:
    try:
        return _v102_primary_return(df, prefer)
    except Exception:
        if df is None or df.empty:
            return pd.Series(dtype="float64")
        return pd.to_numeric(df.get("損益幅%", pd.Series(index=df.index, dtype="float64")), errors="coerce")

def _v120_quality_summary_table(df: pd.DataFrame, prefer: str = "10日") -> pd.DataFrame:
    if df is None or df.empty or "實戰品質分" not in df.columns:
        return pd.DataFrame()
    x = _v120_backfill_quality_record_columns(df.copy())
    x["_quality_bucket"] = x["實戰品質分"].map(_v120_quality_bucket)
    x["_ret"] = _v120_primary_return(x, prefer)
    x["_hit"] = x["_ret"] > 0
    rows = []
    for key, g in x.groupby("_quality_bucket", dropna=False):
        ret = pd.to_numeric(g["_ret"], errors="coerce").dropna()
        rows.append({
            "實戰品質級距": key,
            "樣本數": int(len(g)),
            "有效績效樣本": int(ret.count()),
            "平均績效%": None if ret.empty else round(float(ret.mean()), 2),
            "勝率%": None if ret.empty else round(float((ret > 0).mean() * 100), 1),
            "平均品質分": round(float(pd.to_numeric(g["實戰品質分"], errors="coerce").mean()), 2) if pd.to_numeric(g["實戰品質分"], errors="coerce").notna().any() else None,
        })
    return pd.DataFrame(rows).sort_values("實戰品質級距")

def _render_v120_quality_accuracy_panel(df: pd.DataFrame) -> None:
    render_pro_section("V120 實戰品質準確率分析", "分析 V118 量能 / 趨勢 / 實戰降分是否真的改善後續績效，供 14 權重校正參考。")
    if df is None or df.empty:
        st.info("目前沒有推薦紀錄可分析實戰品質。")
        return
    x = _v120_backfill_quality_record_columns(df.copy())
    ret_prefer = st.selectbox("主要績效週期", ["3日", "5日", "10日", "20日", "隔日"], index=2, key=_k("v120_quality_period"))
    qscore = pd.to_numeric(x.get("實戰品質分"), errors="coerce")
    ret = _v120_primary_return(x, ret_prefer)
    high = x[qscore >= 70]
    low = x[qscore < 60]
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("有品質欄位筆數", int(qscore.notna().sum()))
    with c2:
        st.metric("平均品質分", "-" if qscore.dropna().empty else f"{qscore.mean():.1f}")
    with c3:
        st.metric("品質>=70", int((qscore >= 70).sum()))
    with c4:
        st.metric("品質<60", int((qscore < 60).sum()))
    st.dataframe(_safe_display_df(_v120_quality_summary_table(x, ret_prefer)), use_container_width=True, hide_index=True)

    f1, f2, f3 = st.columns([1,1,1.4])
    with f1:
        vol_opts = [v for v in sorted(x.get("量能狀態", pd.Series(dtype=str)).fillna("").astype(str).unique().tolist()) if v]
        vol_sel = st.selectbox("量能狀態", ["全部"] + vol_opts, key=_k("v120_vol_filter"))
    with f2:
        trend_opts = [v for v in sorted(x.get("趨勢狀態", pd.Series(dtype=str)).fillna("").astype(str).unique().tolist()) if v]
        trend_sel = st.selectbox("趨勢狀態", ["全部"] + trend_opts, key=_k("v120_trend_filter"))
    with f3:
        quality_mode = st.selectbox("品質篩選", ["全部", "品質>=70", "品質<60", "有實戰降分", "量能/趨勢警示"], key=_k("v120_quality_filter"))
    view = x.copy()
    if vol_sel != "全部" and "量能狀態" in view.columns:
        view = view[view["量能狀態"].astype(str) == vol_sel]
    if trend_sel != "全部" and "趨勢狀態" in view.columns:
        view = view[view["趨勢狀態"].astype(str) == trend_sel]
    if quality_mode == "品質>=70":
        view = view[pd.to_numeric(view.get("實戰品質分"), errors="coerce") >= 70]
    elif quality_mode == "品質<60":
        view = view[pd.to_numeric(view.get("實戰品質分"), errors="coerce") < 60]
    elif quality_mode == "有實戰降分":
        view = view[pd.to_numeric(view.get("實戰降分"), errors="coerce").fillna(0) > 0]
    elif quality_mode == "量能/趨勢警示":
        txt = (view.get("量能狀態", pd.Series("", index=view.index)).astype(str) + " " + view.get("趨勢狀態", pd.Series("", index=view.index)).astype(str) + " " + view.get("實戰品質提醒", pd.Series("", index=view.index)).astype(str))
        view = view[txt.str.contains("低量|不足|偏弱|警示|未確認", na=False)]
    if "實戰品質分" in view.columns:
        view = view.sort_values(["實戰品質分", "推薦日期"], ascending=[False, False], na_position="last")
    cols = [c for c in V120_QUALITY_DISPLAY_COLS if c in view.columns]
    st.dataframe(_safe_display_df(view[cols].head(500)), use_container_width=True, hide_index=True)
    if not low.empty and not high.empty:
        high_ret = _v120_primary_return(high, ret_prefer).dropna()
        low_ret = _v120_primary_return(low, ret_prefer).dropna()
        if not high_ret.empty and not low_ret.empty:
            st.caption(f"品質>=70 平均{ret_prefer}績效 {high_ret.mean():.2f}%；品質<60 平均{ret_prefer}績效 {low_ret.mean():.2f}%。若低品質仍勝率偏高，後續可調低防呆降分。")
# <<< V120_REAL_QUALITY_RECORD_SYNC


# >>> V102_NIGHT_ACCURACY_ANALYSIS
# V102：夜間隔日股神準確率分析。只讀既有推薦紀錄欄位，不自動抓資料，避免拖慢頁面。
V102_ACCURACY_GROUP_COLS = [
    "進場型態_隔日", "隔日建議動作", "資料完整度", "類別", "推薦模式", "推薦等級"
]
V102_ACCURACY_RETURN_COLS = [
    "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%",
    "1日績效%", "3日績效%", "5日績效%", "10日績效%", "20日績效%",
    "隔日最高漲幅%", "3日最高漲幅%", "5日最高漲幅%", "10日最高漲幅%",
]


def _v102_num_series(df: pd.DataFrame, col: str) -> pd.Series:
    if df is None or col not in df.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def _v102_bool_series(df: pd.DataFrame, col: str) -> pd.Series:
    if df is None or col not in df.columns:
        return pd.Series(False, index=df.index if isinstance(df, pd.DataFrame) else None)
    def _b(v):
        s = _v98_scalar_text(v).lower()
        if s in {"true", "1", "yes", "y", "是", "命中", "已命中", "觸發", "達標", "✅", "成功"}:
            return True
        try:
            return bool(_normalize_bool(v))
        except Exception:
            return False
    return df[col].map(_b).fillna(False)


def _v102_primary_return(df: pd.DataFrame, prefer: str = "10日") -> pd.Series:
    """依使用者選擇的週期挑主要績效；缺值時依序用相近欄位補。"""
    if df is None or df.empty:
        return pd.Series(dtype="float64")
    candidates = {
        "隔日": ["推薦後1日%", "1日績效%", "隔日最高漲幅%", "損益幅%"],
        "3日": ["推薦後3日%", "3日績效%", "3日最高漲幅%", "推薦後1日%", "損益幅%"],
        "5日": ["推薦後5日%", "5日績效%", "5日最高漲幅%", "推薦後3日%", "損益幅%"],
        "10日": ["推薦後10日%", "10日績效%", "10日最高漲幅%", "推薦後5日%", "損益幅%"],
        "20日": ["推薦後20日%", "20日績效%", "推薦後10日%", "損益幅%"],
    }.get(prefer, ["推薦後10日%", "10日績效%", "損益幅%"])
    out = pd.Series(float("nan"), index=df.index, dtype="float64")
    for c in candidates:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            out = out.where(out.notna(), s)
    return pd.to_numeric(out, errors="coerce")


def _v102_bucket_score(v: Any) -> str:
    n = _v98_scalar_float(v, None)
    if n is None:
        return "未分級"
    if n >= 90:
        return "90以上"
    if n >= 85:
        return "85-89"
    if n >= 80:
        return "80-84"
    if n >= 75:
        return "75-79"
    if n >= 70:
        return "70-74"
    return "70以下"


def _v102_make_group_accuracy(df: pd.DataFrame, group_col: str, prefer: str = "10日") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    x = _v98_backfill_night_battle_record_columns(df.copy())
    if group_col not in x.columns:
        return pd.DataFrame()
    ret = _v102_primary_return(x, prefer)
    valid = ret.notna()
    entry_hit = _v102_bool_series(x, "進場點命中")
    breakout_hit = _v102_bool_series(x, "突破價命中")
    stop_hit = _v102_bool_series(x, "停損價觸發")
    pressure_hit = _v102_bool_series(x, "第一壓力命中")
    work = x.copy()
    work["_v102_ret"] = ret
    work["_v102_valid"] = valid
    work["_v102_entry_hit"] = entry_hit
    work["_v102_breakout_hit"] = breakout_hit
    work["_v102_stop_hit"] = stop_hit
    work["_v102_pressure_hit"] = pressure_hit
    rows = []
    for key, g in work.groupby(group_col, dropna=False):
        label = _v98_scalar_text(key) or "未分類"
        r = pd.to_numeric(g["_v102_ret"], errors="coerce")
        rv = r.dropna()
        total = int(len(g))
        n = int(len(rv))
        win = float((rv > 0).mean() * 100) if n else 0.0
        avg = float(rv.mean()) if n else 0.0
        med = float(rv.median()) if n else 0.0
        best = float(rv.max()) if n else 0.0
        worst = float(rv.min()) if n else 0.0
        rows.append({
            group_col: label,
            "總筆數": total,
            "有效績效樣本": n,
            f"{prefer}勝率%": round(win, 2),
            f"平均{prefer}績效%": round(avg, 2),
            f"中位數{prefer}績效%": round(med, 2),
            "最佳績效%": round(best, 2),
            "最差績效%": round(worst, 2),
            "進場點命中率%": round(float(g["_v102_entry_hit"].mean() * 100), 2) if "_v102_entry_hit" in g else 0.0,
            "突破價命中率%": round(float(g["_v102_breakout_hit"].mean() * 100), 2) if "_v102_breakout_hit" in g else 0.0,
            "第一壓力命中率%": round(float(g["_v102_pressure_hit"].mean() * 100), 2) if "_v102_pressure_hit" in g else 0.0,
            "停損觸發率%": round(float(g["_v102_stop_hit"].mean() * 100), 2) if "_v102_stop_hit" in g else 0.0,
            "校正分數": round(avg + win * 0.18 - (float(g["_v102_stop_hit"].mean() * 100) if "_v102_stop_hit" in g else 0.0) * 0.12 + min(n, 50) * 0.05, 2),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["校正分數", "有效績效樣本", f"{prefer}勝率%"], ascending=[False, False, False])


def _v102_build_accuracy_pack(df: pd.DataFrame, prefer: str = "10日") -> dict[str, pd.DataFrame]:
    if df is None or df.empty:
        return {"summary": pd.DataFrame(), "type": pd.DataFrame(), "action": pd.DataFrame(), "score": pd.DataFrame(), "review": pd.DataFrame()}
    x = _v98_backfill_night_battle_record_columns(df.copy())
    ret = _v102_primary_return(x, prefer)
    x["_v102_ret"] = ret
    x["夜間分數級距"] = x.get("夜間股神總分", pd.Series(index=x.index, dtype=object)).map(_v102_bucket_score)
    x["隔日分數級距"] = x.get("隔日進場分數", pd.Series(index=x.index, dtype=object)).map(_v102_bucket_score)
    valid_ret = ret.dropna()
    entry_hit = _v102_bool_series(x, "進場點命中")
    breakout_hit = _v102_bool_series(x, "突破價命中")
    stop_hit = _v102_bool_series(x, "停損價觸發")
    pressure_hit = _v102_bool_series(x, "第一壓力命中")
    summary = pd.DataFrame([{
        "總紀錄筆數": int(len(x)),
        "有效績效樣本": int(len(valid_ret)),
        f"{prefer}勝率%": round(float((valid_ret > 0).mean() * 100), 2) if len(valid_ret) else 0.0,
        f"平均{prefer}績效%": round(float(valid_ret.mean()), 2) if len(valid_ret) else 0.0,
        "進場點命中率%": round(float(entry_hit.mean() * 100), 2) if len(x) else 0.0,
        "突破價命中率%": round(float(breakout_hit.mean() * 100), 2) if len(x) else 0.0,
        "第一壓力命中率%": round(float(pressure_hit.mean() * 100), 2) if len(x) else 0.0,
        "停損觸發率%": round(float(stop_hit.mean() * 100), 2) if len(x) else 0.0,
    }])
    # 弱勢檢討清單：高夜間/隔日分但績效弱或停損觸發，用來調權。
    review = x.copy()
    night_score = pd.to_numeric(review.get("夜間股神總分"), errors="coerce") if "夜間股神總分" in review.columns else pd.Series(index=review.index, dtype="float64")
    entry_score = pd.to_numeric(review.get("隔日進場分數"), errors="coerce") if "隔日進場分數" in review.columns else pd.Series(index=review.index, dtype="float64")
    review_mask = ((night_score >= 80) | (entry_score >= 80)) & ((ret <= 0) | stop_hit)
    review = review.loc[review_mask].copy()
    if not review.empty:
        review["主要績效%"] = ret.loc[review.index]
        review["檢討原因"] = [
            "停損觸發" if bool(stop_hit.loc[i]) else "高分但績效未轉正" for i in review.index
        ]
        keep = [c for c in [
            "推薦日期", "股票代號", "股票名稱", "類別", "夜間股神總分", "隔日進場分數", "波段潛力分數",
            "進場型態_隔日", "隔日建議動作", "主要績效%", "檢討原因", "夜間股神建議", "隔日作戰策略"
        ] if c in review.columns]
        review = review[keep].sort_values(["主要績效%", "夜間股神總分"], ascending=[True, False], na_position="last").head(100)
    return {
        "summary": summary,
        "type": _v102_make_group_accuracy(x, "進場型態_隔日", prefer),
        "action": _v102_make_group_accuracy(x, "隔日建議動作", prefer),
        "night_score": _v102_make_group_accuracy(x, "夜間分數級距", prefer),
        "entry_score": _v102_make_group_accuracy(x, "隔日分數級距", prefer),
        "sector": _v102_make_group_accuracy(x, "類別", prefer),
        "review": review,
    }


def _render_v102_night_accuracy_panel(df: pd.DataFrame):
    render_pro_section("V102 夜間隔日股神準確率分析", "統計夜間分數、隔日進場型態、建議動作與後續績效的命中率，作為 14 權重校正依據。")
    if df is None or df.empty:
        st.info("目前沒有推薦紀錄可分析。")
        return
    c1, c2, c3 = st.columns([1.1, 1.1, 2.8])
    with c1:
        prefer = st.selectbox("主要績效週期", ["隔日", "3日", "5日", "10日", "20日"], index=3, key=_k("v102_accuracy_prefer"))
    with c2:
        min_samples = st.number_input("分組最少樣本", min_value=1, max_value=100, value=3, step=1, key=_k("v102_accuracy_min_samples"))
    with c3:
        st.caption("此頁只分析既有紀錄，不自動抓 K 線；請先在 8 頁更新推薦後績效，或在 10 頁更新隔日命中追蹤。")
    pack = _v102_build_accuracy_pack(df, prefer)
    summary = pack.get("summary", pd.DataFrame())
    if not summary.empty:
        s = summary.iloc[0]
        render_pro_kpi_row([
            {"label": "有效樣本", "value": int(s.get("有效績效樣本", 0)), "delta": f"總筆數 {int(s.get('總紀錄筆數', 0))}", "delta_class": "pro-kpi-delta-flat"},
            {"label": f"{prefer}勝率", "value": f"{_safe_float(s.get(f'{prefer}勝率%'), 0):.2f}%", "delta": "績效>0", "delta_class": "pro-kpi-delta-flat"},
            {"label": f"平均{prefer}績效", "value": f"{_safe_float(s.get(f'平均{prefer}績效%'), 0):.2f}%", "delta": "主評估週期", "delta_class": "pro-kpi-delta-flat"},
            {"label": "進場點命中", "value": f"{_safe_float(s.get('進場點命中率%'), 0):.2f}%", "delta": "10頁追蹤欄位", "delta_class": "pro-kpi-delta-flat"},
            {"label": "突破價命中", "value": f"{_safe_float(s.get('突破價命中率%'), 0):.2f}%", "delta": "10頁追蹤欄位", "delta_class": "pro-kpi-delta-flat"},
            {"label": "停損觸發", "value": f"{_safe_float(s.get('停損觸發率%'), 0):.2f}%", "delta": "越低越好", "delta_class": "pro-kpi-delta-flat"},
        ])
    def _filter_min(t: pd.DataFrame) -> pd.DataFrame:
        if t is None or t.empty or "有效績效樣本" not in t.columns:
            return t
        return t[pd.to_numeric(t["有效績效樣本"], errors="coerce").fillna(0) >= int(min_samples)].copy()
    sub = st.tabs(["進場型態", "隔日建議", "夜間分數級距", "隔日分數級距", "類別", "弱勢檢討"])
    with sub[0]:
        st.dataframe(_safe_display_df(_filter_min(pack.get("type", pd.DataFrame()))), use_container_width=True, hide_index=True)
    with sub[1]:
        st.dataframe(_safe_display_df(_filter_min(pack.get("action", pd.DataFrame()))), use_container_width=True, hide_index=True)
    with sub[2]:
        st.dataframe(_safe_display_df(_filter_min(pack.get("night_score", pd.DataFrame()))), use_container_width=True, hide_index=True)
    with sub[3]:
        st.dataframe(_safe_display_df(_filter_min(pack.get("entry_score", pd.DataFrame()))), use_container_width=True, hide_index=True)
    with sub[4]:
        st.dataframe(_safe_display_df(_filter_min(pack.get("sector", pd.DataFrame()))), use_container_width=True, hide_index=True)
    with sub[5]:
        review = pack.get("review", pd.DataFrame())
        if review is None or review.empty:
            st.success("目前沒有明顯高分低績效的弱勢檢討樣本。")
        else:
            st.warning("下列資料是高分但後續績效弱或停損觸發的樣本，建議後續提供給 14 權重校正參考。")
            st.dataframe(_safe_display_df(review), use_container_width=True, hide_index=True)
# <<< V102_NIGHT_ACCURACY_ANALYSIS


def _restore_text_fields_from_raw_v73(x: pd.DataFrame, raw: pd.DataFrame) -> pd.DataFrame:
    """v73：修正舊版把文字訊息欄誤轉成數值後，畫面大量空白 / None 的問題。"""
    if x is None or x.empty or raw is None or raw.empty:
        return x
    out = x.copy()
    for c in V73_MESSAGE_TEXT_FIELDS:
        if c not in out.columns:
            out[c] = ""
        # 先把目前欄位清成可顯示文字，避免 None/nan 露出。
        try:
            out[c] = out[c].map(lambda v: "" if _is_empty_display_value(v) else str(v))
        except Exception:
            out[c] = out[c].fillna("").astype(str)
        if c in raw.columns:
            raw_s = raw[c].map(lambda v: "" if _is_empty_display_value(v) else str(v))
            empty_mask = out[c].map(_is_empty_display_value)
            try:
                out.loc[empty_mask, c] = raw_s.loc[empty_mask]
            except Exception:
                # index 不一致時用位置回補
                vals = raw_s.tolist()
                for i, idx in enumerate(out.index):
                    if i < len(vals) and _is_empty_display_value(out.at[idx, c]):
                        out.at[idx, c] = vals[i]
    return out


def _apply_display_backfill_v73(x: pd.DataFrame) -> pd.DataFrame:
    """v73：補齊畫面常用說明欄位，避免資料存在於替代欄位但主表顯示空白。"""
    if x is None or x.empty:
        return x
    out = x.copy()

    def _fill_text(target: str, sources: list[str], default: str = "") -> None:
        if target not in out.columns:
            out[target] = ""
        cur = out[target].map(lambda v: "" if _is_empty_display_value(v) else str(v))
        for s in sources:
            if s not in out.columns:
                continue
            src = out[s].map(lambda v: "" if _is_empty_display_value(v) else str(v))
            mask = cur.map(_is_empty_display_value) & ~src.map(_is_empty_display_value)
            cur.loc[mask] = src.loc[mask]
        if default:
            cur = cur.map(lambda v: default if _is_empty_display_value(v) else v)
        out[target] = cur

    def _fill_num(target: str, sources: list[str]) -> None:
        if target not in out.columns:
            out[target] = pd.NA
        cur = pd.to_numeric(out[target], errors="coerce")
        for s in sources:
            if s not in out.columns:
                continue
            src = pd.to_numeric(out[s], errors="coerce")
            cur = cur.fillna(src)
        out[target] = cur

    _fill_text("推薦型態", ["進場型態", "買點狀態", "推薦分桶", "起漲等級"])
    _fill_text("機會型態", ["機會股說明", "起漲摘要", "推薦理由摘要", "股神推論邏輯"])
    _fill_text("股神建議動作", ["建議動作", "股神進場建議", "實戰操作建議"])
    _fill_text("股神信心", ["上漲機率信心", "信心等級"])
    _fill_text("股神進場區間", ["操作區間", "股神場區間", "股神進場建議"])
    _fill_text("股神推論", ["股神推論邏輯", "推薦理由摘要", "決策說明", "起漲摘要"])
    _fill_text("買點分級", ["買點狀態", "進場型態", "起漲等級"])
    _fill_text("風險說明", ["風險扣分原因", "不建議買進原因", "大盤風控建議", "轉弱條件"])
    _fill_text("大盤情境調權說明", ["市場策略調整說明", "大盤影響說明", "大盤資料診斷摘要", "大盤策略建議"])
    _fill_text("大盤情境分桶", ["大盤策略模式", "大盤橋接狀態", "大盤橋接風控"])
    _fill_text("大盤橋接狀態", ["大盤策略模式", "大盤情境分桶"])
    _fill_text("大盤資料品質", ["大盤交易時段可用", "大盤資料診斷摘要"])
    _fill_text("族群策略建議", ["族群資金流說明", "類別"])
    _fill_text("強勢族群等級", ["類別"])
    _fill_text("K線驗證標記", ["K線檢視提示", "買點狀態"])
    _fill_text("目前狀態", ["狀態"], "觀察")

    # V176：推薦基準價是不可變資料，禁止再由會持續變動的「最新價」反向補值。
    # 舊版此處會把缺少推薦價的紀錄補成最新價，導致「最新價－最新價＝0」。
    _fill_num("推薦價格", ["推薦日價格"])
    _fill_num("推薦日價格", ["推薦價格"])
    _fill_num("股神決策分數", ["推薦總分", "實戰買點分數", "交易可行分數"])
    _fill_num("3日績效%", ["推薦後3日%"])
    _fill_num("5日績效%", ["推薦後5日%"])
    _fill_num("10日績效%", ["推薦後10日%"])
    _fill_num("20日績效%", ["推薦後20日%"])
    return out


def _v113_record_age_days(df: pd.DataFrame) -> pd.Series:
    """V113：估算推薦紀錄已經過幾個日曆天，用來判斷績效欄位是缺資料還是尚未成熟。"""
    if df is None or df.empty:
        return pd.Series([], dtype="float64")
    date_col = None
    for c in ["推薦日期", "推薦時間", "建立時間", "紀錄時間", "匯入時間"]:
        if c in df.columns:
            date_col = c
            break
    if not date_col:
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="float64")
    dt = pd.to_datetime(df[date_col], errors="coerce")
    today = pd.Timestamp.today().normalize()
    return (today - dt.dt.normalize()).dt.days


def _data_completeness_report_v73(df: pd.DataFrame) -> pd.DataFrame:
    """
    V113：資料完整度診斷升級。
    目的不是把所有空欄都視為錯誤，而是分辨：
    1. 主檔必備欄位真的缺漏。
    2. 文字說明/族群/K線屬於可補強欄位。
    3. 3/5/10/20日績效欄位常因推薦日期還沒滿足天數，所以應顯示「待追蹤」而非錯誤。
    """
    columns = ["欄位", "欄位類型", "有資料筆數", "應有筆數", "總筆數", "完整率%", "狀態", "判讀", "建議動作"]
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)
    total = len(df)
    ages = _v113_record_age_days(df)

    check_items = [
        ("推薦日期", "主檔必備", None), ("股票代號", "主檔必備", None), ("股票名稱", "主檔必備", None),
        ("類別", "主檔必備", None), ("推薦模式", "主檔必備", None), ("推薦等級", "主檔必備", None), ("推薦總分", "主檔必備", None),
        ("推薦型態", "策略說明", None), ("機會型態", "策略說明", None), ("股神建議動作", "策略說明", None),
        ("股神信心", "策略說明", None), ("股神進場區間", "策略說明", None), ("股神推論", "策略說明", None),
        ("買點分級", "策略說明", None), ("風險說明", "策略說明", None),
        ("大盤情境調權說明", "大盤橋接", None), ("大盤情境分桶", "大盤橋接", None), ("大盤橋接狀態", "大盤橋接", None), ("大盤資料品質", "大盤橋接", None),
        ("強勢族群等級", "族群輔助", None), ("族群策略建議", "族群輔助", None),
        ("K線驗證標記", "K線驗證", None),
        ("推薦價格", "價格追蹤", None), ("最新價", "價格追蹤", None),
        ("3日績效%", "績效成熟", 3), ("5日績效%", "績效成熟", 5), ("10日績效%", "績效成熟", 10), ("20日績效%", "績效成熟", 20),
        ("目前狀態", "狀態追蹤", None),
    ]
    rows = []
    for c, kind, mature_days in check_items:
        if mature_days is not None:
            eligible_mask = ages >= mature_days
            eligible = int(eligible_mask.fillna(False).sum()) if len(ages) else 0
            if c in df.columns and eligible > 0:
                filled = int((~df.loc[eligible_mask, c].map(_is_empty_display_value)).sum())
            else:
                filled = 0
            denom = eligible
            rate = round(filled / denom * 100, 1) if denom else 0
            pending = total - eligible
            if eligible == 0:
                status = "待追蹤"
                note = f"目前沒有滿 {mature_days} 日的樣本，這不是錯誤。"
                action = "等推薦紀錄累積到期，或到 10/8 更新推薦後績效。"
            elif rate >= 70:
                status = "OK"
                note = f"已滿 {mature_days} 日樣本大多有績效資料。"
                action = "OK"
            elif rate >= 30:
                status = "部分資料"
                note = f"滿 {mature_days} 日樣本已有部分績效；尚未滿期樣本 {pending} 筆。"
                action = "可在 8 頁執行更新推薦後績效，或先累積更多交易日。"
            else:
                status = "待更新"
                note = f"已有 {eligible} 筆滿 {mature_days} 日樣本，但績效欄位尚未更新。"
                action = "建議執行 8 頁更新推薦後績效，或確認歷史K線資料來源。"
            rows.append({"欄位": c, "欄位類型": kind, "有資料筆數": filled, "應有筆數": denom, "總筆數": total, "完整率%": rate, "狀態": status, "判讀": note, "建議動作": action})
            continue

        if c not in df.columns:
            filled = 0
        else:
            filled = int((~df[c].map(_is_empty_display_value)).sum())
        rate = round(filled / total * 100, 1) if total else 0
        if kind == "主檔必備":
            if rate >= 95:
                status = "OK"
            elif rate >= 70:
                status = "需補強"
            else:
                status = "異常"
            note = "主檔欄位會影響列表辨識與後續回測。"
            action = "若異常，請回 07 重新匯入或用 17 系統健康檢查修復缺欄。"
        elif kind in {"策略說明", "大盤橋接", "族群輔助", "K線驗證"}:
            if rate >= 80:
                status = "OK"
            elif rate >= 30:
                status = "部分資料"
            else:
                status = "可補強"
            note = "屬於說明/輔助欄位，缺少時通常不影響推薦紀錄本體。"
            action = "可重新從 07 匯入新版推薦，或保留為舊紀錄。"
            if kind == "K線驗證":
                note = "K線驗證需有對應的K線分析/回測更新流程；舊紀錄沒有是正常情況之一。"
                action = "需要時再執行 K線/推薦後績效更新，不建議視為錯誤。"
        elif kind == "價格追蹤":
            if rate >= 80:
                status = "OK"
            elif rate >= 30:
                status = "部分資料"
            else:
                status = "待更新"
            note = "價格欄位需由 07 匯入或後續行情更新補入。"
            action = "可執行即時行情/績效更新；若是舊資料可先不處理。"
        else:
            status = "OK" if rate >= 80 else ("部分資料" if rate >= 30 else "可補強")
            note = "一般追蹤欄位。"
            action = "視需求補齊。"
        rows.append({"欄位": c, "欄位類型": kind, "有資料筆數": filled, "應有筆數": total, "總筆數": total, "完整率%": rate, "狀態": status, "判讀": note, "建議動作": action})
    return pd.DataFrame(rows, columns=columns)


def _v110_is_blank(v: Any) -> bool:
    try:
        if v is None or pd.isna(v):
            return True
    except Exception:
        pass
    if isinstance(v, str):
        return v.strip() in {"", "None", "nan", "NaN", "<NA>"}
    return False


def _v110_cell_safe(v: Any) -> Any:
    if _v110_is_blank(v):
        return None
    if isinstance(v, (list, tuple, set)):
        return "、".join(_safe_str(x) for x in v if _safe_str(x))
    if isinstance(v, dict):
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)
    return v


def _load_official_factor_map_v110() -> dict[str, dict[str, Any]]:
    if _load_official_factor_frame is None:
        return {}
    try:
        fdf = _load_official_factor_frame()
    except Exception:
        return {}
    if fdf is None or fdf.empty or "股票代號" not in fdf.columns:
        return {}
    fdf = fdf.copy()
    fdf["股票代號"] = fdf["股票代號"].map(_normalize_code)
    keep_cols = [c for c in OFFICIAL_FACTOR_COLUMNS_V110 if c in fdf.columns]
    out: dict[str, dict[str, Any]] = {}
    for _, r in fdf.drop_duplicates("股票代號", keep="last").iterrows():
        code = _normalize_code(r.get("股票代號"))
        if code:
            out[code] = {c: _v110_cell_safe(r.get(c)) for c in keep_cols}
    return out


def _apply_official_factor_backfill_v110(df: pd.DataFrame) -> pd.DataFrame:
    """V110：把官方因子快取安全補進推薦紀錄；只補空欄，不覆蓋歷史紀錄原值。"""
    if df is None or df.empty:
        return df
    x = df.copy()
    x = x.loc[:, ~pd.Index(x.columns).duplicated()].copy()
    for c in OFFICIAL_FACTOR_COLUMNS_V110:
        if c not in x.columns:
            x[c] = None
    fmap = _load_official_factor_map_v110()
    if not fmap:
        if "官方因子資料狀態" in x.columns:
            mask = x["官方因子資料狀態"].map(_v110_is_blank)
            x.loc[mask, "官方因子資料狀態"] = "未讀到官方快取"
        return x
    if "股票代號" not in x.columns:
        return x
    for idx, row in x.iterrows():
        code = _normalize_code(row.get("股票代號"))
        rec = fmap.get(code)
        if not rec:
            continue
        for c, v in rec.items():
            if c not in x.columns:
                x[c] = None
            try:
                if _v110_is_blank(x.at[idx, c]):
                    x.at[idx, c] = v
            except Exception:
                pass
    for c in OFFICIAL_FACTOR_NUMERIC_FIELDS_V110:
        if c in x.columns:
            x[c] = pd.to_numeric(x[c], errors="coerce")
    return x


def _render_v110_official_factor_record_panel(df: pd.DataFrame) -> None:
    render_pro_section("官方因子紀錄追蹤｜法人 / 營收 / EPS / PER")
    if df is None or df.empty:
        st.info("目前沒有推薦紀錄可分析官方因子。")
        return
    x = _apply_official_factor_backfill_v110(df.copy())
    if x.empty:
        st.info("目前沒有官方因子資料。")
        return
    comp = pd.to_numeric(x.get("官方資料完整度"), errors="coerce") if "官方資料完整度" in x.columns else pd.Series([], dtype="float64")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("紀錄筆數", len(x))
    with c2:
        st.metric("完整度>=60", int((comp >= 60).sum()) if not comp.empty else 0)
    with c3:
        avg = pd.to_numeric(x.get("官方因子總分"), errors="coerce").dropna().mean() if "官方因子總分" in x.columns else None
        st.metric("平均官方分", format_number(avg, 1) if avg == avg else "—")
    with c4:
        pos = int((pd.to_numeric(x.get("三大法人近5日合計"), errors="coerce") > 0).sum()) if "三大法人近5日合計" in x.columns else 0
        st.metric("法人5日買超", pos)
    with c5:
        yoy_pos = int((pd.to_numeric(x.get("月營收YoY%"), errors="coerce") > 0).sum()) if "月營收YoY%" in x.columns else 0
        st.metric("營收YoY正成長", yoy_pos)
    show_cols = [
        "推薦日期", "股票代號", "股票名稱", "推薦總分", "夜間股神總分", "官方因子總分", "官方資料完整度", "官方因子資料狀態",
        "外資近5日買賣超", "投信近5日買賣超", "三大法人近5日合計", "法人連買天數",
        "月營收YoY%", "月營收MoM%", "累計營收YoY%", "PER本益比", "PBR股價淨值比", "估算EPS", "官方因子更新時間",
    ]
    show_cols = [c for c in show_cols if c in x.columns]
    if show_cols:
        try:
            x = x.sort_values(["官方資料完整度", "官方因子總分"], ascending=[False, False], na_position="last")
        except Exception:
            pass
        st.dataframe(_safe_display_df(x[show_cols].head(500)), use_container_width=True, hide_index=True)
    st.caption("V110：此區只讀 official_factors_cache.json，不會在 8 頁即時抓官方網站。")

def _add_missing_columns_bulk_v156(df: pd.DataFrame, cols: list[str], default: Any = None) -> pd.DataFrame:
    """V156：一次補齊缺欄，避免 df[c] 逐欄插入造成 DataFrame fragmentation 與頁面變慢。"""
    if df is None:
        df = pd.DataFrame()
    missing = [c for c in _dedupe_keep_order_v73(cols or []) if c not in df.columns]
    if not missing:
        return df
    fill = pd.DataFrame({c: [default] * len(df) for c in missing}, index=df.index)
    return pd.concat([df, fill], axis=1)


def _record_identity_seed_v178(row: dict[str, Any], original_id: str = "") -> str:
    fields = [
        original_id,
        _normalize_code(row.get("股票代號")),
        _safe_str(row.get("推薦日期")),
        _safe_str(row.get("推薦時間")),
        _safe_str(row.get("推薦模式")),
        _safe_str(row.get("建立時間")),
        _safe_str(row.get("推薦價格") or row.get("推薦日價格")),
        _safe_str(row.get("推薦理由摘要") or row.get("備註")),
    ]
    return "|".join(fields)


def _repair_record_ids_v178(df: pd.DataFrame) -> pd.DataFrame:
    """V178: deterministically repair missing/duplicate record_id without deleting records."""
    if df is None or df.empty:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    x = df.copy()
    for col, default in [("record_id", ""), ("原始record_id", ""), ("record_id修復狀態", ""), ("業務事件重複狀態", ""), ("資料完整性狀態", "")]:
        if col not in x.columns:
            x[col] = default
    ids = x["record_id"].fillna("").astype(str).str.strip()
    dup_mask = ids.ne("") & ids.duplicated(keep=False)
    miss_mask = ids.eq("")
    repair_indexes = list(x.index[dup_mask | miss_mask])
    used = set(ids[~(dup_mask | miss_mask) & ids.ne("")].tolist())
    for idx in repair_indexes:
        row = dict(x.loc[idx])
        old_id = _safe_str(row.get("record_id"))
        seed = _record_identity_seed_v178(row, old_id)
        base = hashlib.md5(seed.encode("utf-8")).hexdigest()
        new_id = base
        ordinal = 1
        while new_id in used:
            ordinal += 1
            new_id = hashlib.md5(f"{seed}|{ordinal}".encode("utf-8")).hexdigest()
        used.add(new_id)
        if old_id:
            x.at[idx, "原始record_id"] = _safe_str(x.at[idx, "原始record_id"]) or old_id
            x.at[idx, "record_id修復狀態"] = "V178重建｜原ID重複"
        else:
            x.at[idx, "record_id修復狀態"] = "V178建立｜原ID缺失"
        x.at[idx, "record_id"] = new_id
    # 同股票＋推薦日＋推薦模式是目前權威 upsert 的業務事件鍵。舊資料若同鍵重複，
    # 原始列保留供稽核，但統計分析只計一筆，避免勝率／樣本數被重複放大。
    _biz_keys_v178 = []
    for idx in x.index:
        _code = _normalize_code(x.at[idx, "股票代號"] if "股票代號" in x.columns else "")
        _date = _safe_str(x.at[idx, "推薦日期"] if "推薦日期" in x.columns else "")[:10]
        _mode = _safe_str(x.at[idx, "推薦模式"] if "推薦模式" in x.columns else "")
        _biz_keys_v178.append(f"{_code}|{_date}|{_mode}" if _code and _date else f"RID|{_safe_str(x.at[idx, 'record_id'])}")
    _biz_series_v178 = pd.Series(_biz_keys_v178, index=x.index, dtype="object")
    _biz_dup_mask_v178 = _biz_series_v178.duplicated(keep=False) & _biz_series_v178.str.contains(r"\|20\d{2}-", regex=True)
    x["業務事件重複狀態"] = ""
    x.loc[_biz_dup_mask_v178, "業務事件重複狀態"] = "歷史重複｜統計僅計一次"

    # integrity flags are warnings, not destructive fixes.
    for idx in x.index:
        issues = []
        if not _normalize_code(x.at[idx, "股票代號"] if "股票代號" in x.columns else ""):
            issues.append("缺股票代號")
        if not _safe_str(x.at[idx, "推薦日期"] if "推薦日期" in x.columns else ""):
            issues.append("缺推薦日期")
        if _safe_str(x.at[idx, "業務事件重複狀態"] if "業務事件重複狀態" in x.columns else ""):
            issues.append("同日同模式重複事件")
        if _safe_float(x.at[idx, "推薦價格"] if "推薦價格" in x.columns else None) in [None, 0] and _safe_float(x.at[idx, "推薦日價格"] if "推薦日價格" in x.columns else None) in [None, 0]:
            issues.append("缺推薦基準價")
        qd = _safe_str(x.at[idx, "最新價資料日期"] if "最新價資料日期" in x.columns else "")
        if _safe_float(x.at[idx, "最新價"] if "最新價" in x.columns else None) not in [None, 0] and not qd:
            issues.append("最新價日期未驗證")
        x.at[idx, "資料完整性狀態"] = "正常" if not issues else "｜".join(issues)
    return x


def _record_integrity_summary_v178(df: pd.DataFrame) -> dict[str, Any]:
    if df is None or df.empty:
        return {"rows": 0, "duplicate_record_ids": 0, "missing_rec_date": 0, "unverified_latest_date": 0, "missing_basis": 0}
    x = df.copy()
    ids = x.get("record_id", pd.Series([""] * len(x), index=x.index)).fillna("").astype(str).str.strip()
    latest = pd.to_numeric(x.get("最新價", pd.Series([None] * len(x), index=x.index)), errors="coerce")
    qd = x.get("最新價資料日期", pd.Series([""] * len(x), index=x.index)).fillna("").astype(str).str.strip()
    rp = pd.to_numeric(x.get("推薦價格", pd.Series([None] * len(x), index=x.index)), errors="coerce")
    rdp = pd.to_numeric(x.get("推薦日價格", pd.Series([None] * len(x), index=x.index)), errors="coerce")
    recd = x.get("推薦日期", pd.Series([""] * len(x), index=x.index)).fillna("").astype(str).str.strip()
    _codes_v178 = x.get("股票代號", pd.Series([""] * len(x), index=x.index)).fillna("").astype(str).map(_normalize_code)
    _modes_v178 = x.get("推薦模式", pd.Series([""] * len(x), index=x.index)).fillna("").astype(str).str.strip()
    _biz_v178 = pd.Series([f"{c}|{d[:10]}|{m}" if c and d else f"RID|{rid}" for c,d,m,rid in zip(_codes_v178,recd,_modes_v178,ids)], index=x.index)
    _biz_valid_v178 = _codes_v178.ne("") & recd.ne("")
    _biz_counts_v178 = _biz_v178[_biz_valid_v178].value_counts()
    return {
        "rows": int(len(x)),
        "duplicate_record_ids": int((ids.ne("") & ids.duplicated(keep=False)).sum()),
        "duplicate_business_groups": int((_biz_counts_v178 > 1).sum()),
        "duplicate_business_rows": int(_biz_counts_v178[_biz_counts_v178 > 1].sum()),
        "missing_rec_date": int(recd.eq("").sum()),
        "unverified_latest_date": int((latest.gt(0) & qd.eq("")).sum()),
        "missing_basis": int((~rp.gt(0) & ~rdp.gt(0)).sum()),
        "repaired_ids": int(x.get("record_id修復狀態", pd.Series([""] * len(x), index=x.index)).fillna("").astype(str).str.len().gt(0).sum()),
    }


_NORMALIZED_RECORD_ATTR_V157 = "godpick_record_columns_normalized_version"
_NORMALIZED_RECORD_VERSION_V157 = "v178_integrity_normalize"
_NORMALIZED_RECORD_REQUIRED_V157 = ("record_id", "股票代號", "股票名稱", "推薦日期", "目前狀態")


def _mark_normalized_records_v157(df: pd.DataFrame) -> pd.DataFrame:
    """V157：標記已完成重欄位正規化，避免同一份資料在每次 rerun 重跑完整 backfill。"""
    try:
        if isinstance(df, pd.DataFrame):
            df.attrs[_NORMALIZED_RECORD_ATTR_V157] = _NORMALIZED_RECORD_VERSION_V157
    except Exception:
        pass
    return df


def _is_normalized_records_v157(df: pd.DataFrame) -> bool:
    """V157：快速判斷 session_state 內資料是否已正規化。

    8_股神推薦紀錄原本在 _get_state_df() / live_df 建立 / 儲存 / 分析前反覆呼叫
    _ensure_godpick_record_columns()。該函式會補官方因子、統一 schema、夜間欄位、品質欄位與
    Phase 6.1 分區；506 筆、數百欄時，每次 Streamlit rerun 都重做會明顯拖慢。
    """
    try:
        if not isinstance(df, pd.DataFrame) or df.empty:
            return False
        if df.attrs.get(_NORMALIZED_RECORD_ATTR_V157) != _NORMALIZED_RECORD_VERSION_V157:
            return False
        if pd.Index(df.columns).duplicated().any():
            return False
        return all(c in df.columns for c in _NORMALIZED_RECORD_REQUIRED_V157)
    except Exception:
        return False


def _copy_records_frame_v157(df: pd.DataFrame) -> pd.DataFrame:
    """V157：回傳淺拷貝供頁面讀取，避免不必要的深拷貝與完整欄位重算。"""
    try:
        out = df.copy(deep=False)
        if isinstance(df, pd.DataFrame):
            out.attrs.update(getattr(df, "attrs", {}) or {})
        return out
    except Exception:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()


def _ensure_godpick_record_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return _mark_normalized_records_v157(pd.DataFrame(columns=GODPICK_RECORD_COLUMNS))

    # V157：同一份 session_state 資料已正規化時直接回傳淺拷貝。
    # 這是 8_股神推薦紀錄運算變慢的主因之一：舊版每次 rerun 都會重跑 schema / 官方因子 / 夜間 / 品質 / Phase6.1 backfill。
    if _is_normalized_records_v157(df):
        return _copy_records_frame_v157(df)

    x = df.copy()
    raw_src_v73 = df.copy()
    if "record_id" not in x.columns and "rec_id" in x.columns:
        x["record_id"] = x["rec_id"]

    # V156：避免逐欄 x[c] = None 造成 pandas fragmentation，進頁/刪除後重繪會慢。
    x = _add_missing_columns_bulk_v156(x, GODPICK_RECORD_COLUMNS, None)

    # V110：補入官方因子快取欄位，只補空值，不連外、不覆蓋歷史紀錄原值。
    try:
        x = _apply_official_factor_backfill_v110(x)
    except Exception:
        pass

    numeric_cols = [
        "推薦總分", "上漲機率估計%", "大盤橋接分數", "大盤可參考分數", "大盤加權分", "大盤影響加減分", "族群資金流分數", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "技術結構分數", "起漲前兆分數", "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "進場時機分數", "近端支撐", "主要支撐", "近端壓力", "突破確認價", "停損參考", "風險報酬比_決策", "追高風險分數_決策", "飆股起漲分數", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", 
        "同類股領先幅度", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2",
        "實際買進價", "實際賣出價", "實際報酬%", "最新價", "損益金額", "損益幅%",
        "持有天數", "股神決策分數", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否曾達標_回測", "達標確認狀態", "回測事件摘要", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "進場觸發狀態", "進場觸發日期", "進場評估路徑", "是否納入可執行績效", "執行基準價", "觸發訊號品質分", "觸發後收盤績效%", "觸發當日收盤績效%", "觸發當日最高報酬%", "觸發當日最大回撤%", "觸發當日收盤保留率%", "觸發收盤確認層級", "隔日候選漲跌%", "隔日執行命中結果", "隔日績效檢討標籤", "未觸發漏選標記", "候選與交易分流說明", "績效更新版本", "可執行交易1日%", "可執行交易3日%", "可執行交易5日%", "可執行交易10日%", "可執行交易20日%", "可執行交易最大漲幅%", "可執行交易最大回撤%", "除權息調整旗標", "績效計算口徑", "3日績效%", "5日績效%", "10日績效%", "20日績效%",
    ]
    # v46 修正：舊紀錄或 Firestore 回補資料可能沒有部分數值欄。
    # 先補欄再轉型，避免 x[c] 觸發 KeyError 造成整頁無法開啟。
    # v73：舊版 numeric_cols 曾誤放多個文字訊息欄，會把「族群策略建議 / K線驗證 / 大盤說明」轉成 NaN。
    # 這裡改用白名單數值欄，文字欄後續再由 raw_src_v73 回補。
    numeric_cols = _dedupe_keep_order_v73(V73_NUMERIC_FIELDS)
    x = _add_missing_columns_bulk_v156(x, numeric_cols, None)
    for c in numeric_cols:
        x[c] = pd.to_numeric(x[c], errors="coerce")

    bool_cols = ["是否領先同類股", "是否已實際買進", "是否達停損", "是否達目標1", "是否達目標2"]
    x = _add_missing_columns_bulk_v156(x, bool_cols, False)
    for c in bool_cols:
        x[c] = x[c].fillna(False).map(_normalize_bool)

    text_cols = [
        "股票代號", "股票名稱", "市場別", "類別", "推薦模式", "推薦等級", "上漲機率估計%", "上漲機率等級", "上漲機率信心", "推薦標籤", "推薦理由摘要", "大盤橋接狀態", "大盤橋接加權", "大盤橋接風控", "大盤橋接策略", "大盤橋接更新時間", "大盤交易時段", "大盤交易時段可用", "大盤資料品質", "大盤影響說明", "大盤資料診斷摘要",
        "推薦分桶", "起漲等級", "信心等級", "推薦日期", "推薦時間", "建立時間", "更新時間", "最新更新時間", "模式績效標籤", "股神建議動作", "股神信心", "股神進場區間", "股神推論", "績效資料型態", "績效資料來源", "備註",
    ]
    # v73：把所有常用說明欄納入文字欄處理，並從原始 df 回補被舊版誤轉掉的訊息。
    text_cols_all = _dedupe_keep_order_v73(text_cols + V73_MESSAGE_TEXT_FIELDS)
    x = _add_missing_columns_bulk_v156(x, text_cols_all, "")
    for c in text_cols_all:
        x[c] = x[c].map(lambda v: "" if _is_empty_display_value(v) else str(v))
    x = _restore_text_fields_from_raw_v73(x, raw_src_v73)
    x = _apply_display_backfill_v73(x)

    if "目前狀態" not in x.columns:
        x["目前狀態"] = "觀察"
    if "股票代號" not in x.columns:
        x["股票代號"] = ""
    if "類別" not in x.columns:
        x["類別"] = ""

    x["股票代號"] = x["股票代號"].map(_normalize_code)
    x["類別"] = x["類別"].map(_normalize_category)
    x["目前狀態"] = x["目前狀態"].fillna("觀察").astype(str).replace("", "觀察")


    # 舊紀錄沒有起漲等級時，依起漲前兆分數自動補齊，避免 7頁/8頁/10頁欄位不一致。
    if "起漲等級" in x.columns:
        empty_grade = x["起漲等級"].fillna("").astype(str).str.strip() == ""
        if empty_grade.any():
            x.loc[empty_grade, "起漲等級"] = x.loc[empty_grade, "起漲前兆分數"].apply(_derive_prelaunch_grade_from_score)

    # 7/8/9 起漲欄位串聯補齊：舊資料沒有新欄位時自動用起漲前兆分數補。
    if "飆股起漲分數" in x.columns:
        x["飆股起漲分數"] = pd.to_numeric(x["飆股起漲分數"], errors="coerce")
        if "起漲前兆分數" in x.columns:
            x["飆股起漲分數"] = x["飆股起漲分數"].fillna(pd.to_numeric(x["起漲前兆分數"], errors="coerce"))
    if "起漲等級" in x.columns:
        empty_grade = x["起漲等級"].fillna("").astype(str).str.strip() == ""
        if empty_grade.any():
            x.loc[empty_grade, "起漲等級"] = x.loc[empty_grade, "飆股起漲分數"].apply(_derive_prelaunch_grade_from_score)
    if "起漲摘要" in x.columns:
        empty_summary = x["起漲摘要"].fillna("").astype(str).str.strip() == ""
        if empty_summary.any():
            x.loc[empty_summary, "起漲摘要"] = x.loc[empty_summary].apply(_derive_prelaunch_summary_from_row, axis=1)

    need_id = x["record_id"].isna() | (x["record_id"].astype(str).str.strip() == "")
    if need_id.any():
        for idx in x[need_id].index:
            rec_date = _safe_str(x.at[idx, "推薦日期"]) or _now_date_text()
            rec_time = _safe_str(x.at[idx, "推薦時間"]) or _now_time_text()
            x.at[idx, "record_id"] = _create_record_id(
                _safe_str(x.at[idx, "股票代號"]),
                rec_date,
                rec_time,
                _safe_str(x.at[idx, "推薦模式"]),
            )

    # v74 欄位統一：共用 schema 回補不同模組欄位別名，避免 7/8/10/12 顯示不一致。
    try:
        if normalize_godpick_dataframe is not None:
            x = normalize_godpick_dataframe(x, add_missing=True)
    except Exception:
        pass

    # V165：正式推薦引擎會新增一百多個欄位，但目前 8 頁紀錄 schema / 顯示欄位
    # 並不保留其中任何欄位；舊版仍在每次首次載入 1,796 筆紀錄時完整重算，
    # 實測單此步約 17 秒，最後又全部被 ordered_cols 丟棄。
    # 僅當未來紀錄 schema 明確要求正式推薦欄位時才執行，功能需求一旦加入會自動恢復。
    formal_record_fields_v165 = {
        "正式推薦分區", "是否正式推薦", "操作許可", "股神推薦優先分",
        "隔日觸發品質分", "隔日觸發品質判定", "紅燈觸發管制", "紅燈反轉首觸禁買", "大盤與K線對齊狀態",
    }
    requested_record_fields_v165 = set(UNIFIED_RECOMMEND_DISPLAY_COLUMNS or []) | set(GODPICK_RECORD_COLUMNS or [])
    if formal_record_fields_v165 & requested_record_fields_v165:
        try:
            from godpick_formal_recommendation_engine import apply_formal_recommendation_engine
            x = apply_formal_recommendation_engine(x)
        except Exception:
            pass

    # V98：補齊 07/10 夜間隔日股神欄位，讓歷史紀錄也能追蹤進場點/突破/停損/壓力。
    try:
        x = _v98_backfill_night_battle_record_columns(x)
    except Exception:
        pass

    # V120：補齊 V118 實戰品質欄位，讓 8 頁能保存並分析量能 / 趨勢 / 防呆降分。
    try:
        x = _v120_backfill_quality_record_columns(x)
    except Exception:
        pass

    # >>> PHASE61_RECORD_SYNC
    # 只用現有欄位補 Phase 6.1 同步分區，不重跑推薦、不連網、不寫 JSON。
    try:
        from godpick_signal_hub import add_phase61_signal_columns
        x = add_phase61_signal_columns(x)
    except Exception:
        pass
    # <<< PHASE61_RECORD_SYNC

    # V178：任何缺失／重複 record_id 在進入編輯、刪除、Firestore 前先確定性修復；不刪資料。
    try:
        x = _repair_record_ids_v178(x)
    except Exception:
        pass

    # v73：GODPICK_RECORD_COLUMNS 內歷史整合後有重複欄名，回傳前統一去重，避免 data_editor / arrow 顯示異常。
    ordered_cols = _dedupe_keep_order_v73([c for c in (UNIFIED_RECOMMEND_DISPLAY_COLUMNS or GODPICK_RECORD_COLUMNS) if c in x.columns] + [c for c in GODPICK_RECORD_COLUMNS if c in x.columns])
    if callable(filter_effective_columns):
        ordered_cols = filter_effective_columns(ordered_cols)
    x = x.loc[:, ~pd.Index(x.columns).duplicated()].copy()
    out_v157 = x[ordered_cols].copy()
    return _mark_normalized_records_v157(out_v157)


def _append_records_dedup_by_business_key(base_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    base_df = _ensure_godpick_record_columns(base_df)
    new_df = _ensure_godpick_record_columns(new_df)
    if new_df.empty:
        return base_df.copy()

    merged = pd.concat([base_df, new_df], ignore_index=True)
    merged["_biz_key"] = (
        merged["股票代號"].fillna("").astype(str) + "|"
        + merged["推薦日期"].fillna("").astype(str) + "|"
        + merged["推薦時間"].fillna("").astype(str) + "|"
        + merged["推薦模式"].fillna("").astype(str)
    )
    merged["_upd"] = pd.to_datetime(merged["更新時間"], errors="coerce")
    merged = merged.sort_values(["_biz_key", "_upd"], ascending=[True, False], na_position="last")
    merged = merged.drop_duplicates(subset=["_biz_key"], keep="first")
    return _ensure_godpick_record_columns(merged.drop(columns=["_biz_key", "_upd"], errors="ignore"))


def _delete_records_by_ids(df: pd.DataFrame, record_ids: list[str]) -> pd.DataFrame:
    df = _ensure_godpick_record_columns(df)
    ids = {_safe_str(x) for x in (record_ids or []) if _safe_str(x)}
    if df.empty or not ids:
        return df.copy()
    out = df[~df["record_id"].astype(str).isin(ids)].copy()
    out["更新時間"] = _now_text()
    return _ensure_godpick_record_columns(out)


def _clear_filtered_records(df: pd.DataFrame, filtered_df: pd.DataFrame) -> pd.DataFrame:
    df = _ensure_godpick_record_columns(df)
    filtered_df = _ensure_godpick_record_columns(filtered_df)
    if df.empty or filtered_df.empty:
        return df.copy()

    ids = {_safe_str(x) for x in filtered_df["record_id"].astype(str).tolist() if _safe_str(x)}
    if ids:
        out = df[~df["record_id"].astype(str).isin(ids)].copy()
    else:
        drop_keys = {
            f"{_safe_str(r.get('股票代號'))}|{_safe_str(r.get('推薦日期'))}|{_safe_str(r.get('推薦時間'))}|{_safe_str(r.get('推薦模式'))}"
            for _, r in filtered_df.iterrows()
        }
        keep_mask = []
        for _, r in df.iterrows():
            key = f"{_safe_str(r.get('股票代號'))}|{_safe_str(r.get('推薦日期'))}|{_safe_str(r.get('推薦時間'))}|{_safe_str(r.get('推薦模式'))}"
            keep_mask.append(key not in drop_keys)
        out = df[pd.Series(keep_mask, index=df.index)].copy()

    out["更新時間"] = _now_text()
    return _ensure_godpick_record_columns(out)


def _read_records_from_github() -> tuple[pd.DataFrame, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), "未設定 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), ""
        if resp.status_code != 200:
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), f"GitHub 讀取失敗：{resp.status_code} / {resp.text[:300]}"
        data = resp.json()
        content = data.get("content", "")
        if not content:
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), ""
        decoded = base64.b64decode(content).decode("utf-8")
        payload = json.loads(decoded)
        if isinstance(payload, list):
            return _ensure_godpick_record_columns(pd.DataFrame(payload)), ""
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), ""
    except Exception as e:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), f"GitHub 讀取例外：{e}"


LOCAL_RECORD_SOURCE_FILES = ["godpick_records.json"]


def _records_local_signature() -> str:
    """V174：使用奈秒mtime＋state hash/revision判斷權威檔是否更新。"""
    if callable(records_authority_signature):
        try:
            return _safe_str(records_authority_signature())
        except Exception:
            pass
    parts: list[str] = []
    for fn in [*LOCAL_RECORD_SOURCE_FILES, "godpick_records_sync_state.json"]:
        try:
            path = project_path(fn) if callable(project_path) else fn
            stt = os.stat(path)
            parts.append(f"{path}:{int(getattr(stt, 'st_mtime_ns', int(stt.st_mtime * 1_000_000_000)))}:{stt.st_size}")
        except Exception:
            parts.append(f"{fn}:missing")
    return "|".join(parts)



def _normalized_record_cache_path_v165() -> str:
    try:
        return project_path(NORMALIZED_RECORD_CACHE_FILE_V165) if callable(project_path) else NORMALIZED_RECORD_CACHE_FILE_V165
    except Exception:
        return NORMALIZED_RECORD_CACHE_FILE_V165


def _load_normalized_record_cache_v165(source_signature: str) -> tuple[pd.DataFrame, str]:
    """Load the trusted local runtime cache when the canonical JSON is unchanged.

    JSON remains the authority.  The pickle only stores the already-normalized
    DataFrame so new Streamlit sessions do not repeat hundreds of column
    conversions/backfills.  File name includes the schema version, and the
    canonical JSON mtime/size signature must match exactly.
    """
    path = _normalized_record_cache_path_v165()
    try:
        if not source_signature or not os.path.exists(path):
            return pd.DataFrame(), "正規化快取不存在"
        payload = pd.read_pickle(path)
        if not isinstance(payload, dict):
            return pd.DataFrame(), "正規化快取格式不符"
        if _safe_str(payload.get("version")) != NORMALIZED_RECORD_CACHE_VERSION_V165:
            return pd.DataFrame(), "正規化快取版本不同"
        if _safe_str(payload.get("source_signature")) != _safe_str(source_signature):
            return pd.DataFrame(), "正規化快取已過期"
        cached_df = payload.get("dataframe")
        if not isinstance(cached_df, pd.DataFrame) or cached_df.empty:
            return pd.DataFrame(), "正規化快取無資料"
        cached_df = _mark_normalized_records_v157(cached_df)
        if not _is_normalized_records_v157(cached_df):
            return pd.DataFrame(), "正規化快取驗證失敗"
        return _copy_records_frame_v157(cached_df), f"正規化快取命中：{len(cached_df)} 筆"
    except Exception as exc:
        return pd.DataFrame(), f"正規化快取讀取失敗：{exc}"


def _save_normalized_record_cache_v165(df: pd.DataFrame, source_signature: str) -> tuple[bool, str]:
    """Atomically refresh the disposable local normalized cache."""
    if not isinstance(df, pd.DataFrame) or df.empty or not source_signature:
        return False, "正規化快取未寫入：資料或來源簽章為空"
    path = _normalized_record_cache_path_v165()
    tmp = f"{path}.tmp"
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        cache_df = _mark_normalized_records_v157(_copy_records_frame_v157(df))
        pd.to_pickle(
            {
                "version": NORMALIZED_RECORD_CACHE_VERSION_V165,
                "source_signature": _safe_str(source_signature),
                "saved_at": _now_text(),
                "dataframe": cache_df,
            },
            tmp,
        )
        os.replace(tmp, path)
        return True, f"正規化快取已更新：{len(cache_df)} 筆"
    except Exception as exc:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        return False, f"正規化快取寫入失敗：{exc}"


def _remove_normalized_record_cache_v165() -> tuple[bool, str]:
    path = _normalized_record_cache_path_v165()
    try:
        if os.path.exists(path):
            os.remove(path)
            return True, "正規化快取檔"
        return True, "正規化快取檔（原本不存在）"
    except Exception as exc:
        return False, f"正規化快取檔清除失敗:{exc}"


def _read_records_from_local_files() -> tuple[pd.DataFrame, str]:
    source_signature = _records_local_signature()
    cached_df, cached_msg = _load_normalized_record_cache_v165(source_signature)
    if not cached_df.empty:
        return cached_df, cached_msg

    rows: list[dict[str, Any]] = []
    messages: list[str] = [cached_msg]
    for fn in LOCAL_RECORD_SOURCE_FILES:
        try:
            if callable(read_local_json):
                payload, msg, _ = read_local_json(fn, [])
                messages.append(msg)
            else:
                path = project_path(fn) if callable(project_path) else fn
                if not os.path.exists(path):
                    messages.append(f"{path}: 不存在")
                    continue
                with open(path, "r", encoding="utf-8-sig") as f:
                    payload = json.load(f)
                messages.append(f"{path}: 已讀取")
            if isinstance(payload, list):
                rows.extend([dict(x) for x in payload if isinstance(x, dict)])
            else:
                messages.append(f"{fn}: 格式不是 list")
        except Exception as e:
            messages.append(f"{fn}: 讀取失敗 {e}")
    if not rows:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), "；".join(messages)
    normalized_df = _ensure_godpick_record_columns(pd.DataFrame(rows))
    cache_ok, cache_msg = _save_normalized_record_cache_v165(normalized_df, source_signature)
    messages.append(cache_msg)
    return normalized_df, "；".join(messages)



def _write_records_to_local_file(df: pd.DataFrame) -> tuple[bool, str]:
    """V150：儲存同步時也寫回本機 JSON，讓頁面下次進入可直接讀最新資料。"""
    try:
        path = _github_config().get("path", "godpick_records.json") or "godpick_records.json"
        clean_df = _ensure_godpick_record_columns(df)
        ok, msg = _safe_json_write_local(path, clean_df.to_dict(orient="records"))
        if ok:
            _save_normalized_record_cache_v165(clean_df, _records_local_signature())
        return ok, msg.replace("UI 設定", "推薦紀錄")
    except Exception as e:
        return False, f"本機推薦紀錄寫入失敗：{e}"


def _get_records_sha() -> tuple[str, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return "", "缺少 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 200:
            return _safe_str(resp.json().get("sha")), ""
        if resp.status_code == 404:
            return "", ""
        return "", f"讀取 SHA 失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return "", f"讀取 SHA 例外：{e}"


def _write_records_to_github(df: pd.DataFrame) -> tuple[bool, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN"
    sha, err = _get_records_sha()
    if err:
        return False, err

    content_text = json.dumps(_ensure_godpick_record_columns(df).to_dict(orient="records"), ensure_ascii=False, indent=2)
    encoded = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")
    body: dict[str, Any] = {
        "message": f"update godpick records at {_now_text()}",
        "content": encoded,
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha

    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已回寫 GitHub：{cfg['path']}"
        return False, f"GitHub 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"GitHub 寫入例外：{e}"


def _read_records_from_firestore() -> tuple[pd.DataFrame, str]:
    try:
        _init_firebase_app()
        db = firestore.client()
        docs = list(db.collection("godpick_records").stream())
        rows = []
        for doc in docs:
            data = doc.to_dict() or {}
            data.setdefault("record_id", doc.id)
            rows.append(data)
        return _ensure_godpick_record_columns(pd.DataFrame(rows)), ""
    except Exception as e:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), f"Firestore 讀取失敗：{e}"


def _write_records_to_firestore(df: pd.DataFrame) -> tuple[bool, str]:
    try:
        _init_firebase_app()
        db = firestore.client()
        batch = db.batch()
        now = firestore.SERVER_TIMESTAMP
        records_ref = db.collection("godpick_records")
        summary_ref = db.collection("system").document("godpick_records_summary")

        clean_df = _ensure_godpick_record_columns(df)
        batch.set(summary_ref, {"count": len(clean_df), "updated_at": now, "source": "streamlit_record_page"}, merge=True)

        existing_docs = list(records_ref.stream())
        existing_ids = {doc.id for doc in existing_docs}
        new_ids = set()
        for row in clean_df.to_dict(orient="records"):
            rec_id = _safe_str(row.get("record_id"))
            if not rec_id:
                continue
            new_ids.add(rec_id)
            payload = dict(row)
            payload["updated_at"] = now
            batch.set(records_ref.document(rec_id), payload, merge=True)
        for old_id in existing_ids - new_ids:
            batch.delete(records_ref.document(old_id))
        batch.commit()
        return True, "已同步寫入 Firestore"
    except Exception as e:
        return False, f"Firestore 寫入失敗：{e}"


def _save_records_dual(df: pd.DataFrame) -> bool:
    clean_df = _ensure_godpick_record_columns(df)
    sync_func = save_records_sync_fast if callable(save_records_sync_fast) else save_records_permanent
    if not callable(sync_func):
        _set_status("永久紀錄服務未載入，為避免假成功，本次不寫入。", "error")
        return False
    started = time.perf_counter()
    report = sync_func(clean_df, reason="page8 explicit save sync", expected_authority_signature=_safe_str(st.session_state.get(_k("records_source_sig"), ""))) if sync_func is save_records_sync_fast else sync_func(clean_df)
    elapsed = time.perf_counter() - started
    try:
        current_sig_v165 = _records_local_signature()
        if bool(getattr(report, "local_ok", False)):
            st.session_state[_k("records_source_sig")] = current_sig_v165
            _save_normalized_record_cache_v165(clean_df, current_sig_v165)
        elif "權威檔已被" in _safe_str(getattr(report, "local_message", "")):
            fresh_df_v174 = _load_records(force_remote=False)
            _save_state_df(fresh_df_v174)
            st.session_state[_k("records_source_sig")] = current_sig_v165
    except Exception:
        pass
    details = report.messages()
    st.session_state[_k("last_sync_detail")] = details
    st.session_state[_k("last_sync_report")] = report.to_dict()
    st.session_state[_k("last_sync_failed")] = not bool(report.permanent_ok)

    source_state = (
        f"本機{'成功' if report.local_ok else '失敗'}、"
        f"GitHub{'成功' if report.github_ok else ('背景同步中' if getattr(report, 'github_pending', False) else '失敗')}、"
        f"Firestore{'成功' if report.firestore_ok else '失敗'}"
    )
    if report.permanent_ok:
        if report.github_ok and report.firestore_ok:
            msg = f"推薦紀錄已完成三層永久保存（{source_state}），前台耗時 {elapsed:.2f} 秒。"
        elif report.github_ok or report.firestore_ok or getattr(report, "github_pending", False):
            msg = f"推薦紀錄已完成本機保存，遠端已驗證或排入背景同步（{source_state}），前台耗時 {elapsed:.2f} 秒。"
        else:
            msg = f"推薦紀錄已保存至專案固定路徑（{source_state}），前台耗時 {elapsed:.2f} 秒。"
        _set_status(msg, "success" if (report.github_ok or report.firestore_ok or getattr(report, "github_pending", False)) else "warning")
        return True

    if report.local_ok:
        msg = f"推薦紀錄本機寫入成功，但遠端永久備份失敗（{source_state}）；請展開同步明細。"
    else:
        msg = f"推薦紀錄本機與遠端均未完成（{source_state}）；請展開同步明細。"
    _set_status(msg, "error")
    return False



def _apply_persistence_report(report: Any, *, action_name: str, github_deferred_ok: bool = False) -> bool:
    """Store a persistence report in the shared status panel without blocking UI logic."""
    if report is None:
        _set_status(f"{action_name}失敗：沒有取得儲存結果。", "error")
        return False
    try:
        details = report.messages()
        report_dict = report.to_dict()
        permanent_ok = bool(report.permanent_ok)
        local_ok = bool(report.local_ok)
        github_ok = bool(report.github_ok)
        github_pending = bool(getattr(report, "github_pending", False))
        firestore_ok = bool(report.firestore_ok)
    except Exception as exc:
        _set_status(f"{action_name}失敗：儲存結果格式異常｜{exc}", "error")
        return False

    st.session_state[_k("last_sync_detail")] = details
    st.session_state[_k("last_sync_report")] = report_dict
    st.session_state[_k("last_sync_failed")] = not permanent_ok
    try:
        if local_ok:
            st.session_state[_k("records_source_sig")] = _records_local_signature()
        elif "權威檔已被" in _safe_str(getattr(report, "local_message", "")):
            fresh_df_v174 = _load_records(force_remote=False)
            _save_state_df(fresh_df_v174)
            st.session_state[_k("records_source_sig")] = _records_local_signature()
    except Exception:
        pass

    if permanent_ok:
        if github_pending:
            remote_text = "Firestore已驗證＋GitHub背景同步中" if firestore_ok else "GitHub背景同步中"
            _set_status(
                f"{action_name}已完成本機保存｜{remote_text}；不再阻塞刪除/編輯按鈕。",
                "success" if firestore_ok else "warning",
            )
        elif github_deferred_ok and firestore_ok and not github_ok:
            _set_status(
                f"{action_name}已完成本機＋Firestore永久保存；大型 GitHub 備份待同步。",
                "success",
            )
        else:
            _set_status(
                f"{action_name}已保存｜本機{'✓' if local_ok else '✗'}／GitHub{'✓' if github_ok else '✗'}／Firestore{'✓' if firestore_ok else '✗'}。",
                "success" if (github_ok or firestore_ok) else "warning",
            )
        return True

    _set_status(
        f"{action_name}未完成永久保存｜本機{'✓' if local_ok else '✗'}／GitHub{'✓' if github_ok else '✗'}／Firestore{'✓' if firestore_ok else '✗'}；請查看同步明細。",
        "error",
    )
    return False


def _save_records_mutation_fast_ui(
    df: pd.DataFrame,
    *,
    action_name: str,
    deleted_ids: list[str] | None = None,
    upsert_rows: list[dict[str, Any]] | None = None,
    previous_count: int | None = None,
) -> bool:
    """Fast durable path for table mutations.

    Delete/edit/add actions no longer upload the entire 20+ MB GitHub JSON in
    the button callback.  They atomically save locally and perform a verified
    Firestore incremental mutation; the explicit ``儲存同步`` button remains
    the full GitHub backup path.
    """
    clean_df = _ensure_godpick_record_columns(df)
    if callable(save_records_mutation_fast):
        report = save_records_mutation_fast(
            clean_df,
            expected_authority_signature=_safe_str(st.session_state.get(_k("records_source_sig"), "")),
            deleted_ids=deleted_ids or [],
            upsert_rows=upsert_rows or [],
            previous_count=previous_count,
            reason=action_name,
        )
        _apply_persistence_report(report, action_name=action_name, github_deferred_ok=True)
        # V162：表格 mutation 以本機原子寫入是否成功決定畫面是否切換。
        # 遠端 Firestore/GitHub 失敗或排隊中會在同步明細顯示，但不再讓刪除按鈕無限等待、也不讓已刪資料留在畫面。
        local_ok = bool(getattr(report, "local_ok", False))
        if local_ok:
            try:
                _save_normalized_record_cache_v165(clean_df, _records_local_signature())
            except Exception:
                pass
        st.session_state[_k("last_mutation_local_ok")] = local_ok
        st.session_state[_k("last_mutation_permanent_ok")] = bool(getattr(report, "permanent_ok", False))
        return local_ok
    # Compatibility fallback for deployments that did not yet load the new
    # persistence helper.  This can be slower but must never pretend success.
    return _save_records_dual(clean_df)



def _normalize_watchlist_payload(data: dict[str, list[dict[str, str]]]) -> dict[str, list[dict[str, str]]]:
    payload: dict[str, list[dict[str, str]]] = {}
    for group_name, items in data.items():
        g = _safe_str(group_name)
        if not g:
            continue
        seen = set()
        normalized_items = []
        for item in items:
            if not isinstance(item, dict):
                continue
            code = _normalize_code(item.get("code"))
            name = _safe_str(item.get("name")) or code
            market = _safe_str(item.get("market")) or "上市"
            category = _safe_str(item.get("category"))
            if not code:
                continue
            key = (g, code)
            if key in seen:
                continue
            seen.add(key)
            row = {"code": code, "name": name, "market": market}
            if category:
                row["category"] = category
            normalized_items.append(row)
        payload[g] = sorted(normalized_items, key=lambda x: (_normalize_code(x.get("code")), _safe_str(x.get("name"))))
    return payload


def _read_watchlist_from_github() -> tuple[dict[str, list[dict[str, str]]], str]:
    cfg = _watchlist_github_config()
    token = cfg["token"]
    if not token:
        return {}, "未設定 GITHUB_TOKEN，無法讀取 watchlist.json"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return {}, ""
        if resp.status_code != 200:
            return {}, f"watchlist GitHub 讀取失敗：{resp.status_code} / {resp.text[:300]}"
        data = resp.json()
        content = data.get("content", "")
        if not content:
            return {}, ""
        decoded = base64.b64decode(content).decode("utf-8")
        payload = json.loads(decoded)
        if not isinstance(payload, dict):
            return {}, "watchlist.json 格式錯誤，根層必須是 dict"
        return _normalize_watchlist_payload(payload), ""
    except Exception as e:
        return {}, f"watchlist GitHub 讀取例外：{e}"


def _load_watchlist_payload() -> dict[str, list[dict[str, str]]]:
    if callable(load_watchlist_permanent):
        try:
            payload, details = load_watchlist_permanent()
            st.session_state[_k("watchlist_import_detail")] = "｜".join(details)
            st.session_state["watchlist_data"] = copy.deepcopy(payload)
            return _normalize_watchlist_payload(payload)
        except Exception as exc:
            st.session_state[_k("watchlist_import_detail")] = f"永久自選股載入失敗：{exc}"
    try:
        payload = get_normalized_watchlist()
    except Exception:
        payload = {}
    return _normalize_watchlist_payload(payload)



def _get_watchlist_sha() -> tuple[str, str]:
    cfg = _watchlist_github_config()
    token = cfg["token"]
    if not token:
        return "", "缺少 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 200:
            return _safe_str(resp.json().get("sha")), ""
        if resp.status_code == 404:
            return "", ""
        return "", f"讀取 watchlist SHA 失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return "", f"讀取 watchlist SHA 例外：{e}"


def _write_watchlist_to_github(payload: dict[str, list[dict[str, str]]]) -> tuple[bool, str]:
    cfg = _watchlist_github_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN，無法回寫 watchlist.json"

    sha, err = _get_watchlist_sha()
    if err:
        return False, err

    content_text = json.dumps(_normalize_watchlist_payload(payload), ensure_ascii=False, indent=2)
    encoded = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")
    body: dict[str, Any] = {
        "message": f"update watchlist from godpick record page at {_now_text()}",
        "content": encoded,
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha

    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已回寫自選股：{cfg['path']}"
        return False, f"watchlist GitHub 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"watchlist GitHub 寫入例外：{e}"


def _export_records_to_watchlist(records_df: pd.DataFrame, selected_ids: list[str], target_group: str) -> tuple[bool, str]:
    records_df = _ensure_godpick_record_columns(records_df)
    ids = {_safe_str(x) for x in (selected_ids or []) if _safe_str(x)}
    if records_df.empty:
        return False, "目前沒有推薦紀錄可匯出"
    if not ids:
        return False, "請先勾選要匯入自選股中心的股票"
    chosen = records_df[records_df["record_id"].astype(str).isin(ids)].copy()
    if chosen.empty:
        return False, "找不到要匯入的推薦紀錄"
    payload = _load_watchlist_payload()
    target_group = _safe_str(target_group) or "股神推薦"
    payload.setdefault(target_group, [])
    existing_codes = {_normalize_code(x.get("code")) for x in payload.get(target_group, []) if isinstance(x, dict)}
    add_count = 0
    skip_count = 0
    for _, row in chosen.iterrows():
        code = _normalize_code(row.get("股票代號"))
        name = _safe_str(row.get("股票名稱")) or code
        market = _safe_str(row.get("市場別")) or "上市"
        category = _safe_str(row.get("類別")) or _safe_str(row.get("產業"))
        if not code or code in existing_codes:
            skip_count += 1
            continue
        item = {"code": code, "name": name, "market": market}
        if category:
            item["category"] = category
        payload[target_group].append(item)
        existing_codes.add(code)
        add_count += 1
    payload = _normalize_watchlist_payload(payload)
    if not callable(save_watchlist_permanent):
        return False, "永久自選股服務未載入"
    report = save_watchlist_permanent(payload)
    st.session_state[_k("watchlist_import_sync_detail")] = report.messages()
    if report.permanent_ok:
        try:
            get_normalized_watchlist.clear()
        except Exception:
            pass
        st.session_state["watchlist_data"] = copy.deepcopy(payload)
        st.session_state["watchlist_version"] = int(st.session_state.get("watchlist_version", 0) or 0) + 1
        st.session_state["watchlist_last_saved_at"] = report.updated_at or _now_text()
        st.session_state[_k("watchlist_import_detail")] = f"目標群組：{target_group}｜新增 {add_count} 檔｜略過 {skip_count} 檔"
        return True, f"自選股永久保存完成｜匯入 {add_count} 檔，略過 {skip_count} 檔"
    return False, "自選股只寫入部分來源，未通過永久保存條件｜" + "；".join(report.messages())



def _safe_json_read_local(path_name: str, default):
    try:
        with open(path_name, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _safe_json_write_local(path_name: str, payload) -> tuple[bool, str]:
    try:
        with open(path_name, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        return True, f"已寫入本機 UI 設定：{path_name}"
    except Exception as e:
        return False, f"本機 UI 設定寫入失敗：{e}"


def _config_ts(payload: dict[str, Any]):
    raw = _safe_str(payload.get("updated_at")) if isinstance(payload, dict) else ""
    try:
        return datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.min


def _normalize_ui_config(payload: dict[str, Any] | None) -> dict[str, Any]:
    base = copy.deepcopy(UI_CONFIG_DEFAULT)
    if not isinstance(payload, dict):
        payload = {}

    base["fast_mode"] = bool(payload.get("fast_mode", base["fast_mode"]))
    base["visible_limit"] = int(_safe_float(payload.get("visible_limit"), base["visible_limit"]) or base["visible_limit"])
    base["visible_limit"] = max(100, min(base["visible_limit"], 5000))
    base["updated_at"] = _safe_str(payload.get("updated_at"))

    profiles = payload.get("profiles", {})
    if not isinstance(profiles, dict):
        profiles = {}

    for mode, defaults in UI_CONFIG_DEFAULT["profiles"].items():
        raw_cols = profiles.get(mode, defaults)
        if not isinstance(raw_cols, list):
            raw_cols = defaults
        clean_cols = []
        seen = set()
        for c in raw_cols:
            cc = _safe_str(c)
            if cc and cc not in seen:
                seen.add(cc)
                clean_cols.append(cc)
        remain = [c for c in defaults if c not in seen]
        base["profiles"][mode] = clean_cols + remain

    return base


def _read_ui_config_from_github() -> tuple[dict[str, Any], str]:
    cfg = _ui_config_github_config()
    token = cfg["token"]
    if not token:
        return copy.deepcopy(UI_CONFIG_DEFAULT), "未設定 GITHUB_TOKEN，無法讀取 UI 設定"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return copy.deepcopy(UI_CONFIG_DEFAULT), ""
        if resp.status_code != 200:
            return copy.deepcopy(UI_CONFIG_DEFAULT), f"UI 設定 GitHub 讀取失敗：{resp.status_code} / {resp.text[:300]}"
        data = resp.json()
        content = data.get("content", "")
        if not content:
            return copy.deepcopy(UI_CONFIG_DEFAULT), ""
        decoded = base64.b64decode(content).decode("utf-8")
        payload = json.loads(decoded)
        return _normalize_ui_config(payload), ""
    except Exception as e:
        return copy.deepcopy(UI_CONFIG_DEFAULT), f"UI 設定 GitHub 讀取例外：{e}"


def _get_ui_config_sha(cfg_override: dict[str, str] | None = None) -> tuple[str, str]:
    cfg = dict(cfg_override) if isinstance(cfg_override, dict) else _ui_config_github_config()
    token = cfg["token"]
    if not token:
        return "", "缺少 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 200:
            return _safe_str(resp.json().get("sha")), ""
        if resp.status_code == 404:
            return "", ""
        return "", f"讀取 UI 設定 SHA 失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return "", f"讀取 UI 設定 SHA 例外：{e}"


def _write_ui_config_to_github(payload: dict[str, Any], cfg_override: dict[str, str] | None = None) -> tuple[bool, str]:
    cfg = dict(cfg_override) if isinstance(cfg_override, dict) else _ui_config_github_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN，無法回寫 UI 設定"

    sha, err = _get_ui_config_sha(cfg)
    if err:
        return False, err

    clean_payload = _normalize_ui_config(payload)
    clean_payload["updated_at"] = _now_text()
    content_text = json.dumps(clean_payload, ensure_ascii=False, indent=2)
    encoded = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")
    body: dict[str, Any] = {
        "message": f"update godpick ui config at {_now_text()}",
        "content": encoded,
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha

    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已回寫 UI 設定：{cfg['path']}"
        return False, f"UI 設定 GitHub 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"UI 設定 GitHub 寫入例外：{e}"


@st.cache_resource(show_spinner=False)
def _page08_ui_sync_executor():
    return ThreadPoolExecutor(max_workers=1, thread_name_prefix="godpick-page08-ui")


def _queue_ui_config_github_sync(payload: dict[str, Any]) -> tuple[bool, str]:
    cfg = _ui_config_github_config()
    if not _safe_str(cfg.get("token")):
        return False, "未設定 GITHUB_TOKEN，UI 設定已保存在本機"
    try:
        _page08_ui_sync_executor().submit(_write_ui_config_to_github, copy.deepcopy(payload), dict(cfg))
        return True, "GitHub UI 設定背景同步已排程"
    except Exception as exc:
        return False, f"GitHub UI 設定背景同步排程失敗：{exc}"


def _load_ui_config_once():
    if st.session_state.get(_k("ui_config_loaded"), False):
        return

    # V164：本機存在時立即使用，不在進頁或任意按鈕 rerun 等待 GitHub GET。
    local_raw = _safe_json_read_local(_ui_config_github_config()["path"], {})
    local_payload = _normalize_ui_config(local_raw) if isinstance(local_raw, dict) and local_raw else {}
    if local_payload:
        payload = local_payload
        detail = "V164 本機優先：已立即讀取 UI 設定，未等待 GitHub。"
    else:
        github_payload, err = _read_ui_config_from_github()
        payload = _normalize_ui_config(github_payload)
        detail = err or "本機尚無設定，已從 GitHub 初始化 UI 設定"

    payload = _normalize_ui_config(payload)
    st.session_state[_k("ui_config_loaded")] = True
    st.session_state[_k("ui_config_detail")] = detail
    st.session_state[_k("ui_config")] = copy.deepcopy(payload)
    st.session_state[_k("fast_mode")] = bool(payload.get("fast_mode", True))
    st.session_state[_k("visible_limit")] = int(payload.get("visible_limit", FAST_VISIBLE_LIMIT))
    st.session_state[_k("ui_last_auto_sig")] = f"{bool(payload.get('fast_mode', True))}|{int(payload.get('visible_limit', FAST_VISIBLE_LIMIT))}"
    for mode in ["標準", "進階"]:
        st.session_state[_get_profile_key(mode)] = payload.get("profiles", {}).get(mode, _get_default_col_profile(mode)).copy()


def _persist_ui_config() -> tuple[bool, str]:
    payload = {
        "fast_mode": bool(st.session_state.get(_k("fast_mode"), True)),
        "visible_limit": int(st.session_state.get(_k("visible_limit"), FAST_VISIBLE_LIMIT)),
        "profiles": {
            "標準": st.session_state.get(_get_profile_key("標準"), DEFAULT_STANDARD_COLS.copy()),
            "進階": st.session_state.get(_get_profile_key("進階"), DEFAULT_ADVANCED_COLS.copy()),
        },
        "updated_at": _now_text(),
    }
    payload = _normalize_ui_config(payload)
    st.session_state[_k("ui_config")] = copy.deepcopy(payload)
    local_ok, local_msg = _safe_json_write_local(_ui_config_github_config()["path"], payload)
    github_ok, github_msg = _queue_ui_config_github_sync(payload)
    msg = f"{local_msg}｜{github_msg}"
    st.session_state[_k("ui_save_detail")] = msg
    st.session_state[_k("ui_last_saved_at")] = _now_text()
    return (local_ok or github_ok), msg


@st.cache_data(ttl=120, show_spinner=False)
def _get_latest_close(stock_no: str, stock_name: str, market_type: str) -> tuple[float | None, str, str]:
    stock_no = _normalize_code(stock_no)
    stock_name = _safe_str(stock_name)
    market_type = _safe_str(market_type) or "上市"

    tried = []
    if market_type:
        tried.append(market_type)
    for mk in ["上市", "上櫃", "興櫃"]:
        if mk not in tried:
            tried.append(mk)

    for mk in tried:
        try:
            info = get_realtime_stock_info(stock_no, stock_name, mk, refresh_token=str(int(_tw_now().timestamp() * 1000)))
            # V178：即使是舊的輔助函式，也只能接受實際成交或有日期的日線收盤；
            # 不得再讓 bid/ask/mid/pz/昨收繞過最新價品質規則。
            price, used_market, src, _qdate, _qtime = _quote_price_from_info(info)
            if price is not None and price > 0:
                return float(price), used_market or mk, src or "verified_quote"
        except Exception:
            pass

    today = _tw_today()
    start_date = today - timedelta(days=60)
    for mk in tried + [""]:
        try:
            try:
                df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_date=start_date, end_date=today)
            except TypeError:
                try:
                    df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_dt=start_date, end_dt=today)
                except Exception:
                    df = get_history_data(code=stock_no, start_date=start_date, end_date=today)
            if isinstance(df, pd.DataFrame) and not df.empty:
                temp = df.copy()
                if "日期" not in temp.columns:
                    for c in temp.columns:
                        if str(c).lower() in {"date", "日期"}:
                            temp = temp.rename(columns={c: "日期"})
                            break
                for c in temp.columns:
                    if str(c).lower() == "close":
                        temp = temp.rename(columns={c: "收盤價"})
                if "收盤價" not in temp.columns:
                    continue
                temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
                temp["收盤價"] = pd.to_numeric(temp["收盤價"], errors="coerce")
                temp = temp.dropna(subset=["日期", "收盤價"]).sort_values("日期")
                if not temp.empty:
                    return float(temp.iloc[-1]["收盤價"]), _safe_str(mk or market_type or "未知"), "history_close"
        except Exception:
            pass
    return None, _safe_str(market_type or "未知"), ""


@st.cache_data(ttl=3600, show_spinner=False)
def _get_forward_return(stock_no: str, stock_name: str, market_type: str, rec_date_text: str, days_after: int) -> float | None:
    rec_date = pd.to_datetime(rec_date_text, errors="coerce")
    if pd.isna(rec_date):
        return None

    start_date = rec_date.date() - timedelta(days=5)
    end_date = rec_date.date() + timedelta(days=max(days_after * 4, 40))
    tried = []
    primary = _safe_str(market_type)
    if primary:
        tried.append(primary)
    for mk in ["上市", "上櫃", "興櫃", ""]:
        if mk not in tried:
            tried.append(mk)

    for mk in tried:
        try:
            try:
                df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_date=start_date, end_date=end_date)
            except TypeError:
                try:
                    df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_dt=start_date, end_dt=end_date)
                except Exception:
                    df = get_history_data(code=stock_no, start_date=start_date, end_date=end_date)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            temp = df.copy()
            if "日期" not in temp.columns:
                for c in temp.columns:
                    if str(c).lower() in {"date", "日期"}:
                        temp = temp.rename(columns={c: "日期"})
                        break
            for c in temp.columns:
                if str(c).lower() == "close":
                    temp = temp.rename(columns={c: "收盤價"})
            if "日期" not in temp.columns or "收盤價" not in temp.columns:
                continue

            temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
            temp["收盤價"] = pd.to_numeric(temp["收盤價"], errors="coerce")
            temp = temp.dropna(subset=["日期", "收盤價"]).sort_values("日期").reset_index(drop=True)
            if temp.empty:
                continue

            base_candidates = temp[temp["日期"].dt.date >= rec_date.date()].reset_index(drop=True)
            if base_candidates.empty:
                continue
            if len(base_candidates) <= days_after:
                return None

            base_px = float(base_candidates.iloc[0]["收盤價"])
            target_px = float(base_candidates.iloc[days_after]["收盤價"])
            if base_px == 0:
                return None
            return (target_px - base_px) / base_px * 100
        except Exception:
            pass
    return None



@st.cache_data(ttl=3600, show_spinner=False)
def _get_forward_metrics(
    stock_no: str,
    stock_name: str,
    market_type: str,
    rec_date_text: str,
    stop_price: float | None,
    target_price: float | None,
) -> dict[str, Any]:
    """一次抓歷史資料，計算推薦後 1/3/5/10/20 日、最大漲幅、最大回撤與命中結果。"""
    rec_date = pd.to_datetime(rec_date_text, errors="coerce")
    if pd.isna(rec_date):
        return {}

    stock_no = _normalize_code(stock_no)
    stock_name = _safe_str(stock_name)
    primary = _safe_str(market_type)
    start_date = rec_date.date() - timedelta(days=5)
    end_date = rec_date.date() + timedelta(days=90)

    tried = []
    if primary:
        tried.append(primary)
    for mk in ["上市", "上櫃", "興櫃", ""]:
        if mk not in tried:
            tried.append(mk)

    for mk in tried:
        try:
            try:
                df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_date=start_date, end_date=end_date)
            except TypeError:
                try:
                    df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_dt=start_date, end_dt=end_date)
                except Exception:
                    df = get_history_data(code=stock_no, start_date=start_date, end_date=end_date)
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            temp = df.copy()
            rename_map = {}
            for c in temp.columns:
                low = str(c).lower()
                if low in {"date", "日期"}:
                    rename_map[c] = "日期"
                elif low in {"close", "收盤價"}:
                    rename_map[c] = "收盤價"
                elif low in {"high", "最高價"}:
                    rename_map[c] = "最高價"
                elif low in {"low", "最低價"}:
                    rename_map[c] = "最低價"
            if rename_map:
                temp = temp.rename(columns=rename_map)
            if "日期" not in temp.columns or "收盤價" not in temp.columns:
                continue
            temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
            for c in ["收盤價", "最高價", "最低價"]:
                if c in temp.columns:
                    temp[c] = pd.to_numeric(temp[c], errors="coerce")
            temp = temp.dropna(subset=["日期", "收盤價"]).sort_values("日期").reset_index(drop=True)
            if temp.empty:
                continue

            window = temp[temp["日期"].dt.date >= rec_date.date()].reset_index(drop=True)
            if window.empty:
                continue
            base_px = _safe_float(window.iloc[0].get("收盤價"))
            if base_px in [None, 0]:
                continue

            result: dict[str, Any] = {}
            for d in [1, 3, 5, 10, 20]:
                key_new = f"推薦後{d}日%"
                if len(window) > d:
                    target_px = _safe_float(window.iloc[d].get("收盤價"))
                    result[key_new] = None if target_px in [None, 0] else round((target_px - base_px) / base_px * 100, 2)
                else:
                    result[key_new] = None

            use_window = window.head(min(len(window), 21)).copy()
            high_col = "最高價" if "最高價" in use_window.columns else "收盤價"
            low_col = "最低價" if "最低價" in use_window.columns else "收盤價"
            max_high = _safe_float(use_window[high_col].max())
            min_low = _safe_float(use_window[low_col].min())
            max_gain = None if max_high in [None, 0] else round((max_high - base_px) / base_px * 100, 2)
            max_drawdown = None if min_low in [None, 0] else round((min_low - base_px) / base_px * 100, 2)
            result["推薦後最大漲幅%"] = max_gain
            result["推薦後最大回撤%"] = max_drawdown

            tgt = _safe_float(target_price)
            stop = _safe_float(stop_price)
            target_hit = False
            stop_hit = False
            if tgt not in [None, 0] and max_high is not None:
                target_hit = max_high >= tgt
            elif max_gain is not None:
                target_hit = max_gain >= 8
            if stop not in [None, 0] and min_low is not None:
                stop_hit = min_low <= stop
            elif max_drawdown is not None:
                stop_hit = max_drawdown <= -6

            result["是否達標_回測"] = bool(target_hit)
            result["是否停損_回測"] = bool(stop_hit)
            ret20 = result.get("推薦後20日%")
            ret10 = result.get("推薦後10日%")
            ret5 = result.get("推薦後5日%")
            benchmark = ret20 if ret20 is not None else (ret10 if ret10 is not None else ret5)
            if target_hit and not stop_hit:
                hit_result = "達標"
            elif stop_hit and not target_hit:
                hit_result = "停損"
            elif benchmark is not None and benchmark >= 5:
                hit_result = "有效"
            elif benchmark is not None and benchmark <= -5:
                hit_result = "偏弱"
            else:
                hit_result = "觀察中"
            result["命中結果"] = hit_result
            if hit_result == "達標":
                comment = "推薦後已達標，型態有效，可納入權重正向校正"
            elif hit_result == "停損":
                comment = "推薦後觸及停損，需檢討追高、支撐或大盤風險"
            elif hit_result == "有效":
                comment = "推薦後報酬為正，持續觀察是否擴大漲幅"
            elif hit_result == "偏弱":
                comment = "推薦後轉弱，需檢討等待條件與停損設定"
            else:
                comment = "尚未形成明確績效，持續追蹤"
            result["績效評語"] = comment
            result["追蹤更新時間"] = _now_text()
            return result
        except Exception:
            pass
    return {}


# =========================================================
# V71：推薦後績效更新快取防卡核心
# 核心原則：
# 1. 同股票本批只抓一次 K 線。
# 2. 同股票跨 rerun 優先使用本機 / session 快取。
# 3. 單批限制「股票數」而不是只限制筆數，避免 150 筆分散成 80 檔時卡死。
# 4. 歷史資料失敗會短時間黑名單，避免一直重試同一檔。
# 5. 推薦日太近、交易日不足者直接標記等待，不硬抓。
# =========================================================

PERF_HISTORY_CACHE_FILE = "godpick_perf_history_cache.json"
PERF_FAIL_RETRY_HOURS = 0.02
PERF_CACHE_MAX_STOCKS = 180


def _normalize_history_df_for_perf(df: pd.DataFrame) -> pd.DataFrame:
    """統一歷史K線欄位名稱，供推薦後績效批次計算使用。"""
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    temp = df.copy()
    rename_map = {}
    for c in temp.columns:
        low = str(c).strip().lower()
        if low in {"date", "日期", "datetime", "time"}:
            rename_map[c] = "日期"
        elif low in {"close", "收盤價", "收盤", "收盤價(元)"}:
            rename_map[c] = "收盤價"
        elif low in {"high", "最高價", "最高"}:
            rename_map[c] = "最高價"
        elif low in {"low", "最低價", "最低"}:
            rename_map[c] = "最低價"
        elif low in {"open", "開盤價", "開盤"}:
            rename_map[c] = "開盤價"
        elif low in {"volume", "成交量", "vol"}:
            rename_map[c] = "成交量"
    if rename_map:
        temp = temp.rename(columns=rename_map)
    if "日期" not in temp.columns or "收盤價" not in temp.columns:
        return pd.DataFrame()
    temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
    for c in ["開盤價", "收盤價", "最高價", "最低價", "成交量"]:
        if c in temp.columns:
            temp[c] = pd.to_numeric(temp[c], errors="coerce")
    keep_cols = [c for c in ["日期", "開盤價", "最高價", "最低價", "收盤價", "成交量"] if c in temp.columns]
    temp = temp[keep_cols].dropna(subset=["日期", "收盤價"]).sort_values("日期").reset_index(drop=True)
    return temp




def _fetch_yahoo_history_direct_v72(stock_no: str, market_type: str, start_date_value: date, end_date_value: date) -> pd.DataFrame:
    """V72：推薦後績效專用 Yahoo 直接備援。
    不依賴 utils.get_history_data，避免該函式或官方來源暫時失效時導致整批 ONLINE_FAIL。
    """
    code = _normalize_code(stock_no)
    if not code:
        return pd.DataFrame()
    mk = _safe_str(market_type)
    suffix_candidates = []
    if mk in ["上櫃", "興櫃", "OTC", "TPEX"]:
        suffix_candidates = ["TWO", "TW"]
    else:
        suffix_candidates = ["TW", "TWO"]
    try:
        p1 = int(pd.Timestamp(start_date_value).timestamp())
        # Yahoo period2 是 exclusive，往後加一天，避免最後一日漏掉。
        p2 = int((pd.Timestamp(end_date_value) + pd.Timedelta(days=1)).timestamp())
    except Exception:
        return pd.DataFrame()

    headers = {"User-Agent": "Mozilla/5.0"}
    for suffix in suffix_candidates:
        symbol = f"{code}.{suffix}"
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {
            "period1": p1,
            "period2": p2,
            "interval": "1d",
            "events": "history",
            "includeAdjustedClose": "true",
        }
        try:
            r = requests.get(url, params=params, headers=headers, timeout=8)
            if r.status_code != 200:
                continue
            js = r.json()
            result = (((js or {}).get("chart") or {}).get("result") or [])
            if not result:
                continue
            item = result[0]
            ts = item.get("timestamp") or []
            quote = (((item.get("indicators") or {}).get("quote") or [{}])[0])
            if not ts or not isinstance(quote, dict):
                continue
            rows = []
            opens = quote.get("open") or []
            highs = quote.get("high") or []
            lows = quote.get("low") or []
            closes = quote.get("close") or []
            vols = quote.get("volume") or []
            for i, t in enumerate(ts):
                close = closes[i] if i < len(closes) else None
                if close is None:
                    continue
                rows.append({
                    "日期": pd.to_datetime(int(t), unit="s").tz_localize("UTC").tz_convert("Asia/Taipei").tz_localize(None).date(),
                    "開盤價": opens[i] if i < len(opens) else None,
                    "最高價": highs[i] if i < len(highs) else None,
                    "最低價": lows[i] if i < len(lows) else None,
                    "收盤價": close,
                    "成交量": vols[i] if i < len(vols) else None,
                })
            df = _normalize_history_df_for_perf(pd.DataFrame(rows))
            if not df.empty:
                return df
        except Exception:
            continue
    return pd.DataFrame()


def _perf_cache_load() -> dict[str, Any]:
    """讀取 V71 歷史K線快取；session 優先，本機 JSON 備援。"""
    key = _k("v71_perf_history_cache")
    if isinstance(st.session_state.get(key), dict):
        return st.session_state[key]
    payload = _safe_json_read_local(PERF_HISTORY_CACHE_FILE, {})
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("version", "v71")
    payload.setdefault("history", {})
    payload.setdefault("fail", {})
    payload.setdefault("updated_at", _now_text())
    st.session_state[key] = payload
    return payload


def _perf_cache_save(payload: dict[str, Any]) -> None:
    """寫回 V71 歷史K線快取。失敗不影響主流程。"""
    if not isinstance(payload, dict):
        return
    payload.setdefault("version", "v71")
    payload.setdefault("history", {})
    payload.setdefault("fail", {})
    payload["updated_at"] = _now_text()
    # 控制快取大小，避免 JSON 變太大拖慢 Streamlit Cloud。
    hist = payload.get("history", {})
    if isinstance(hist, dict) and len(hist) > PERF_CACHE_MAX_STOCKS:
        items = sorted(hist.items(), key=lambda kv: _safe_str(kv[1].get("updated_at")) if isinstance(kv[1], dict) else "")[-PERF_CACHE_MAX_STOCKS:]
        payload["history"] = dict(items)
    st.session_state[_k("v71_perf_history_cache")] = payload
    try:
        _safe_json_write_local(PERF_HISTORY_CACHE_FILE, payload)
    except Exception:
        pass


def _history_df_to_cache_payload(df: pd.DataFrame, used_market: str, msg: str) -> dict[str, Any]:
    temp = _normalize_history_df_for_perf(df)
    if temp.empty:
        return {}
    out = temp.copy()
    out["日期"] = out["日期"].dt.strftime("%Y-%m-%d")
    return {
        "used_market": _safe_str(used_market),
        "msg": _safe_str(msg or "OK"),
        "start": _safe_str(out["日期"].min()),
        "end": _safe_str(out["日期"].max()),
        "updated_at": _now_text(),
        "rows": out.to_dict(orient="records"),
    }


def _history_df_from_cache_payload(payload: dict[str, Any], need_start: date, need_end: date) -> tuple[pd.DataFrame, str, str, bool]:
    if not isinstance(payload, dict):
        return pd.DataFrame(), "", "無快取", False
    rows = payload.get("rows", [])
    if not rows:
        return pd.DataFrame(), "", "無快取資料", False
    c_start = pd.to_datetime(payload.get("start"), errors="coerce")
    c_end = pd.to_datetime(payload.get("end"), errors="coerce")
    if pd.isna(c_start) or pd.isna(c_end):
        return pd.DataFrame(), "", "快取日期異常", False
    # 快取涵蓋需求區間才直接使用。
    if c_start.date() > need_start or c_end.date() < need_end:
        return pd.DataFrame(), "", "快取區間不足", False
    temp = _normalize_history_df_for_perf(pd.DataFrame(rows))
    if temp.empty:
        return pd.DataFrame(), "", "快取內容異常", False
    return temp, _safe_str(payload.get("used_market")), _safe_str(payload.get("msg") or "CACHE"), True


def _perf_cache_key(code: str, market: str) -> str:
    return f"{_normalize_code(code)}|{_safe_str(market) or '未知'}"


def _fail_cache_is_blocked(cache: dict[str, Any], key: str) -> tuple[bool, str]:
    fail_map = cache.get("fail", {}) if isinstance(cache, dict) else {}
    info = fail_map.get(key) if isinstance(fail_map, dict) else None
    if not isinstance(info, dict):
        return False, ""
    ts = pd.to_datetime(_safe_str(info.get("time")), errors="coerce")
    if pd.isna(ts):
        return False, ""
    try:
        age_hr = (_tw_now_naive() - ts.to_pydatetime().replace(tzinfo=None)).total_seconds() / 3600
        if age_hr < PERF_FAIL_RETRY_HOURS:
            return True, _safe_str(info.get("reason") or "近期抓取失敗，暫停重試")
    except Exception:
        pass
    return False, ""


@st.cache_data(ttl=3600, show_spinner=False)
def _get_perf_history_bundle(
    stock_no: str,
    stock_name: str,
    market_type: str,
    start_date_text: str,
    end_date_text: str,
) -> tuple[pd.DataFrame, str, str]:
    """V71：單股票歷史K線抓取函式；仍保留 Streamlit cache。"""
    stock_no = _normalize_code(stock_no)
    stock_name = _safe_str(stock_name)
    primary = _safe_str(market_type)
    start_date = pd.to_datetime(start_date_text, errors="coerce")
    end_date = pd.to_datetime(end_date_text, errors="coerce")
    if pd.isna(start_date) or pd.isna(end_date) or not stock_no:
        return pd.DataFrame(), _safe_str(primary or "未知"), "日期或股票代號異常"

    tried = []
    if primary:
        tried.append(primary)
    for mk in ["上市", "上櫃", "興櫃", ""]:
        if mk not in tried:
            tried.append(mk)

    last_err = ""
    for mk in tried:
        try:
            try:
                df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_date=start_date.date(), end_date=end_date.date())
            except TypeError:
                try:
                    df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_dt=start_date.date(), end_dt=end_date.date())
                except Exception:
                    df = get_history_data(code=stock_no, start_date=start_date.date(), end_date=end_date.date())
            temp = _normalize_history_df_for_perf(df)
            if not temp.empty:
                return temp, _safe_str(mk or primary or "未知"), "OK(utils)"
        except Exception as e:
            last_err = str(e)[:120]
            continue

    # V72：utils / 官方來源都失敗時，改走 Yahoo chart 直接備援。
    # 這可大幅降低 ONLINE_FAIL，讓 14 權重校正能取得有效績效樣本。
    try:
        for mk in tried:
            direct_df = _fetch_yahoo_history_direct_v72(stock_no, mk or primary, start_date.date(), end_date.date())
            if isinstance(direct_df, pd.DataFrame) and not direct_df.empty:
                return direct_df, _safe_str(mk or primary or "未知"), "OK(YahooDirectV72)"
    except Exception as e:
        last_err = (last_err + " | YahooDirect: " + str(e)[:120])[:220]

    # V72：第二層備援，加入 Stooq CSV + TWSE/TPEx 官方日行情逐日補抓。
    try:
        alt_df, alt_msg = fetch_multi_source_history(stock_no, stock_name, primary, start_date.date(), end_date.date())
        alt_df = _normalize_history_df_for_perf(alt_df)
        if isinstance(alt_df, pd.DataFrame) and not alt_df.empty:
            return alt_df, _safe_str(primary or "未知"), f"OK(MultiSource:{alt_msg})"
        last_err = (last_err + " | MultiSource: " + str(alt_msg)[:120])[:260]
    except Exception as e:
        last_err = (last_err + " | MultiSource: " + str(e)[:120])[:260]

    return pd.DataFrame(), _safe_str(primary or "未知"), last_err or "無歷史資料 / YahooDirect / MultiSource 皆無資料"


def _get_perf_history_bundle_v71(
    stock_no: str,
    stock_name: str,
    market_type: str,
    start_date_value: date,
    end_date_value: date,
) -> tuple[pd.DataFrame, str, str, str]:
    """V71：先查本機/session快取，必要時才抓線上歷史K線。回傳 df, market, msg, source。"""
    code = _normalize_code(stock_no)
    market = _safe_str(market_type)
    cache = _perf_cache_load()
    key = _perf_cache_key(code, market)

    blocked, reason = _fail_cache_is_blocked(cache, key)
    if blocked:
        return pd.DataFrame(), market, f"失敗快取保護：{reason}", "FAIL_CACHE"

    hist_map = cache.get("history", {}) if isinstance(cache.get("history"), dict) else {}
    cached_df, cached_market, cached_msg, ok = _history_df_from_cache_payload(hist_map.get(key, {}), start_date_value, end_date_value)
    if ok:
        return cached_df, cached_market or market, cached_msg or "CACHE", "LOCAL_CACHE"

    hist_df, used_market, hist_msg = _get_perf_history_bundle(code, stock_name, market, str(start_date_value), str(end_date_value))
    if isinstance(hist_df, pd.DataFrame) and not hist_df.empty:
        hist_map[key] = _history_df_to_cache_payload(hist_df, used_market, hist_msg)
        cache["history"] = hist_map
        # 成功後清掉同股票失敗快取。
        fail_map = cache.get("fail", {}) if isinstance(cache.get("fail"), dict) else {}
        fail_map.pop(key, None)
        cache["fail"] = fail_map
        _perf_cache_save(cache)
        return hist_df, used_market, hist_msg, "ONLINE"

    fail_map = cache.get("fail", {}) if isinstance(cache.get("fail"), dict) else {}
    fail_map[key] = {"time": _now_text(), "reason": _safe_str(hist_msg or "無歷史資料")[:160]}
    cache["fail"] = fail_map
    _perf_cache_save(cache)
    return pd.DataFrame(), used_market or market, hist_msg or "無歷史資料", "ONLINE_FAIL"


def _calc_proxy_perf_metrics_v71(payload: dict[str, Any], reason: str = "") -> dict[str, Any]:
    """V178：歷史K線不可用時只提供「即時追蹤報酬」，禁止冒充推薦後1日績效。"""
    if not isinstance(payload, dict):
        return {}
    rec_px = _safe_float(payload.get("推薦價格")) or _safe_float(payload.get("推薦日價格")) or _safe_float(payload.get("建議價位"))
    latest = _safe_float(payload.get("最新價")) or _safe_float(payload.get("最新價格"))
    if rec_px in [None, 0] or latest in [None, 0]:
        return {}
    ret = round((latest - rec_px) / rec_px * 100, 2)
    return {
        "即時追蹤報酬%": ret,
        "績效資料型態": "即時代理｜非N日回測",
        "績效資料來源": "推薦價_vs_已保存最新價",
        "績效評語": f"歷史K線暫不可用；僅顯示即時追蹤報酬，未寫入推薦後1/3/5/10/20日績效。原因：{_safe_str(reason)[:80]}",
        "追蹤更新時間": _now_text(),
    }



def _calc_forward_metrics_from_history(
    hist_df: pd.DataFrame,
    rec_date_text: str,
    stop_price: float | None,
    target_price: float | None,
    recommended_price: float | None = None,
    latest_price: float | None = None,
) -> dict[str, Any]:
    """V115：用已抓好的單股K線計算推薦後績效。

    修正重點：
    1. N日績效仍以「推薦價格 / 推薦日價格」作為基準。
    2. 推薦後績效從推薦日之後的交易日開始算，不拿推薦當天高低價判斷達標/停損。
    3. 把「曾經碰到目標價」與「仍可視為達標成功」拆開：
       - 是否曾達標_回測：日K最高價曾碰到目標價，代表有成交機會。
       - 是否達標_回測：需曾碰到目標價，且最新價/期末價仍站上或接近目標，才列為成功。
       這可避免只是一度沖高碰價，後面跌回來，卻被統計成單純達標。
    4. 命中結果新增「曾達標回落 / 達標後回落」，讓準確率分析不再虛高。
    """
    rec_date = pd.to_datetime(rec_date_text, errors="coerce")
    if pd.isna(rec_date) or not isinstance(hist_df, pd.DataFrame) or hist_df.empty:
        return {}

    temp = _normalize_history_df_for_perf(hist_df)
    if temp.empty:
        return {}

    forward = temp[temp["日期"].dt.date > rec_date.date()].reset_index(drop=True)
    if forward.empty:
        return {}

    base_px = _safe_float(recommended_price)
    if base_px in [None, 0]:
        base_candidates = temp[temp["日期"].dt.date >= rec_date.date()].reset_index(drop=True)
        if base_candidates.empty:
            return {}
        base_px = _safe_float(base_candidates.iloc[0].get("收盤價"))
    if base_px in [None, 0]:
        return {}

    result: dict[str, Any] = {}
    for d in [1, 3, 5, 10, 20]:
        key_new = f"推薦後{d}日%"
        if len(forward) >= d:
            target_px = _safe_float(forward.iloc[d - 1].get("收盤價"))
            result[key_new] = None if target_px in [None, 0] else round((target_px - base_px) / base_px * 100, 2)
        else:
            result[key_new] = None

    use_window = forward.head(min(len(forward), 20)).copy()
    high_col = "最高價" if "最高價" in use_window.columns else "收盤價"
    low_col = "最低價" if "最低價" in use_window.columns else "收盤價"
    close_col = "收盤價" if "收盤價" in use_window.columns else high_col

    max_high = _safe_float(use_window[high_col].max())
    min_low = _safe_float(use_window[low_col].min())
    ending_close = _safe_float(use_window.iloc[-1].get(close_col)) if not use_window.empty else None
    max_gain = None if max_high in [None, 0] else round((max_high - base_px) / base_px * 100, 2)
    max_drawdown = None if min_low in [None, 0] else round((min_low - base_px) / base_px * 100, 2)
    result["推薦後最大漲幅%"] = max_gain
    result["推薦後最大回撤%"] = max_drawdown

    tgt = _safe_float(target_price)
    stop = _safe_float(stop_price)
    latest = _safe_float(latest_price)

    target_date = None
    stop_date = None
    if tgt not in [None, 0] and high_col in use_window.columns:
        hit_rows = use_window[pd.to_numeric(use_window[high_col], errors="coerce") >= float(tgt)]
        if not hit_rows.empty:
            target_date = hit_rows.iloc[0].get("日期")
    elif max_gain is not None and max_gain >= 8:
        hit_rows = use_window[pd.to_numeric(use_window[high_col], errors="coerce") >= base_px * 1.08]
        if not hit_rows.empty:
            target_date = hit_rows.iloc[0].get("日期")

    if stop not in [None, 0] and low_col in use_window.columns:
        hit_rows = use_window[pd.to_numeric(use_window[low_col], errors="coerce") <= float(stop)]
        if not hit_rows.empty:
            stop_date = hit_rows.iloc[0].get("日期")
    elif max_drawdown is not None and max_drawdown <= -6:
        hit_rows = use_window[pd.to_numeric(use_window[low_col], errors="coerce") <= base_px * 0.94]
        if not hit_rows.empty:
            stop_date = hit_rows.iloc[0].get("日期")

    latest_stop_hit = bool(stop not in [None, 0] and latest not in [None, 0] and latest <= stop)
    target_touched = target_date is not None
    stop_hit = stop_date is not None or latest_stop_hit

    ret20 = result.get("推薦後20日%")
    ret10 = result.get("推薦後10日%")
    ret5 = result.get("推薦後5日%")
    ret3 = result.get("推薦後3日%")
    benchmark = ret20 if ret20 is not None else (ret10 if ret10 is not None else (ret5 if ret5 is not None else ret3))

    target_return_pct = None
    if tgt not in [None, 0] and base_px not in [None, 0]:
        target_return_pct = (float(tgt) - float(base_px)) / float(base_px) * 100

    still_above_target = False
    if target_touched and tgt not in [None, 0]:
        if latest not in [None, 0]:
            still_above_target = latest >= float(tgt) * 0.995
        elif ending_close not in [None, 0]:
            still_above_target = ending_close >= float(tgt) * 0.995
        elif benchmark is not None and target_return_pct is not None:
            still_above_target = benchmark >= target_return_pct * 0.8

    if target_touched and tgt in [None, 0] and benchmark is not None:
        still_above_target = benchmark >= 5

    same_day = False
    if target_date is not None and stop_date is not None:
        try:
            same_day = pd.to_datetime(target_date).date() == pd.to_datetime(stop_date).date()
        except Exception:
            same_day = False

    target_confirmed = bool(target_touched and still_above_target and not same_day and not (stop_date is not None and pd.to_datetime(stop_date) <= pd.to_datetime(target_date)))

    result["是否曾達標_回測"] = bool(target_touched)
    result["是否達標_回測"] = bool(target_confirmed)
    result["是否停損_回測"] = bool(stop_hit)
    if target_touched and target_confirmed:
        result["達標確認狀態"] = "確認達標"
    elif target_touched:
        result["達標確認狀態"] = "曾觸及但回落"
    elif stop_hit:
        result["達標確認狀態"] = "未達標且觸發停損"
    else:
        result["達標確認狀態"] = "未達標"

    target_date_text = ""
    stop_date_text = ""
    try:
        target_date_text = pd.to_datetime(target_date).strftime("%Y-%m-%d") if target_date is not None else ""
    except Exception:
        target_date_text = str(target_date or "")
    try:
        stop_date_text = pd.to_datetime(stop_date).strftime("%Y-%m-%d") if stop_date is not None else ""
    except Exception:
        stop_date_text = str(stop_date or "")
    result["回測事件摘要"] = (
        f"曾觸目標:{'是' if target_touched else '否'}"
        + (f"({target_date_text})" if target_date_text else "")
        + f"｜確認達標:{'是' if target_confirmed else '否'}"
        + f"｜停損:{'是' if stop_hit else '否'}"
        + (f"({stop_date_text})" if stop_date_text else "")
    )

    if same_day:
        hit_result = "同日觸及"
        comment = "V115：達標與停損同日觸及，日K無法判斷先後，需人工檢視盤中走勢；不列入單純達標。"
    elif stop_date is not None and (target_date is None or pd.to_datetime(stop_date) < pd.to_datetime(target_date)):
        hit_result = "停損"
        comment = "V115：推薦後先觸及停損，需檢討追高、支撐或大盤風險。"
    elif target_touched and stop_hit:
        hit_result = "達標後回落"
        comment = "V115：推薦後曾碰到目標價，但後續回落或最新價跌破停損，不再列為單純達標。"
    elif target_confirmed:
        hit_result = "達標"
        comment = "V115：推薦後觸及目標且目前/期末仍站上目標附近，型態有效。"
    elif target_touched:
        hit_result = "曾達標回落"
        comment = "V115：推薦後曾碰到目標價，但目前/期末已跌回目標價下方，準確率不列入單純成功。"
    elif latest_stop_hit or stop_date is not None:
        hit_result = "停損"
        comment = "V115：最新價或推薦後低點已觸及停損，需檢討風控。"
    elif benchmark is not None and benchmark >= 5:
        hit_result = "有效"
        comment = "V115：推薦後報酬為正，持續觀察是否擴大漲幅。"
    elif benchmark is not None and benchmark <= -5:
        hit_result = "偏弱"
        comment = "V115：推薦後轉弱，需檢討等待條件與停損設定。"
    else:
        hit_result = "觀察中"
        comment = "V115：尚未形成明確績效，持續追蹤。"

    result["命中結果"] = hit_result
    result["績效評語"] = comment
    result["追蹤更新時間"] = _now_text()
    return result

def _clip(v: float | None, low: float, high: float, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        v = float(v)
    except Exception:
        return default
    return max(low, min(high, v))


def _fmt_pct(v: float | None) -> str:
    return "-" if v is None else f"{v:.2f}%"


def _build_entry_zone(rec_price: float | None, stop_price: float | None) -> str:
    if rec_price in [None, 0]:
        return "-"
    low = rec_price * 0.97
    high = rec_price * 1.03
    if stop_price not in [None, 0]:
        low = max(low, stop_price * 1.03)
    return f"{low:.2f} ~ {high:.2f}"


def _god_mode_decision(row: dict[str, Any]) -> dict[str, Any]:
    rec_price = _safe_float(row.get("推薦價格"))
    latest = _safe_float(row.get("最新價"))
    stop_price = _safe_float(row.get("停損價"))
    target1 = _safe_float(row.get("賣出目標1"))
    target2 = _safe_float(row.get("賣出目標2"))
    bought = _normalize_bool(row.get("是否已實際買進"))
    status = _safe_str(row.get("目前狀態")) or "觀察"

    rec_total = _clip(_safe_float(row.get("推薦總分"), 0), 0, 100, 0)
    tech = _clip(_safe_float(row.get("技術結構分數"), rec_total), 0, 100, rec_total)
    pre_move = _clip(_safe_float(row.get("起漲前兆分數"), rec_total), 0, 100, rec_total)
    trade = _clip(_safe_float(row.get("交易可行分數"), rec_total), 0, 100, rec_total)
    sector = _clip(_safe_float(row.get("類股熱度分數"), rec_total), 0, 100, rec_total)

    perf3 = _safe_float(row.get("3日績效%"))
    perf5 = _safe_float(row.get("5日績效%"))
    perf10 = _safe_float(row.get("10日績效%"))
    perf20 = _safe_float(row.get("20日績效%"))
    pnl_pct = _safe_float(row.get("損益幅%"))

    setup_score = rec_total * 0.28 + tech * 0.24 + pre_move * 0.20 + trade * 0.14 + sector * 0.14

    perf_score = 0.0
    for p, w in [(perf3, 0.15), (perf5, 0.20), (perf10, 0.25), (perf20, 0.40)]:
        if p is not None:
            perf_score += _clip(50 + p * 4, 0, 100, 50) * w
        else:
            perf_score += 50 * w

    mode_label = _safe_str(row.get("模式績效標籤"))
    mode_bonus = {"強勢模式": 8, "穩健模式": 4, "一般模式": 0, "觀察中": -2, "偏弱模式": -8, "弱": -10, "樣本不足": 0}.get(mode_label, 0)

    price_bonus = 0.0
    reasons = []
    if latest not in [None, 0] and rec_price not in [None, 0]:
        drift = (latest - rec_price) / rec_price * 100
        if -3 <= drift <= 3:
            price_bonus += 8
            reasons.append(f"股價接近推薦價({_fmt_pct(drift)})")
        elif 3 < drift <= 8:
            price_bonus += 2
            reasons.append(f"股價小幅高於推薦價({_fmt_pct(drift)})")
        elif drift > 15:
            price_bonus -= 10
            reasons.append(f"股價偏離推薦價過大({_fmt_pct(drift)})")
        elif drift < -8:
            price_bonus -= 6
            reasons.append(f"股價明顯跌破推薦價({_fmt_pct(drift)})")

    risk_penalty = 0.0
    if latest not in [None, 0] and stop_price not in [None, 0]:
        risk_gap = (latest - stop_price) / latest * 100
        if risk_gap <= 0:
            risk_penalty -= 25
            reasons.append("已跌破停損價")
        elif risk_gap < 2.5:
            risk_penalty -= 14
            reasons.append("距停損過近")
        elif risk_gap < 5:
            risk_penalty -= 6
            reasons.append("停損空間偏小")

    target_bonus = 0.0
    if latest not in [None, 0] and target1 not in [None, 0]:
        if latest >= target1:
            target_bonus -= 4
            reasons.append("已接近/到達目標1")
    if latest not in [None, 0] and target2 not in [None, 0] and latest >= target2:
        target_bonus -= 10
        reasons.append("已到達目標2")

    total_score = _clip(setup_score * 0.55 + perf_score * 0.25 + 50 * 0.20 + mode_bonus + price_bonus + risk_penalty + target_bonus, 0, 100, 0)

    if status in {"已賣出", "取消", "封存"}:
        action = "不追蹤"
    elif latest not in [None, 0] and stop_price not in [None, 0] and latest <= stop_price:
        action = "立即出場"
    elif latest not in [None, 0] and target2 not in [None, 0] and latest >= target2:
        action = "分批停利"
    elif bought or status in {"持有", "已買進"}:
        if total_score >= 78:
            action = "續抱"
        elif total_score >= 63:
            action = "續抱觀察"
        elif total_score >= 50:
            action = "減碼觀察"
        else:
            action = "轉弱出場"
    else:
        if total_score >= 80:
            action = "可進場"
        elif total_score >= 68:
            action = "拉回可布局"
        elif total_score >= 56:
            action = "觀察等待"
        else:
            action = "暫不進場"

    if total_score >= 85:
        confidence = "高"
    elif total_score >= 70:
        confidence = "中高"
    elif total_score >= 58:
        confidence = "中"
    else:
        confidence = "保守"

    if tech >= 75:
        reasons.append("技術結構分數強")
    if pre_move >= 75:
        reasons.append("起漲前兆明顯")
    if trade >= 70:
        reasons.append("交易可行性佳")
    if sector >= 70:
        reasons.append("類股熱度有支撐")
    if perf20 is not None and perf20 > 0:
        reasons.append(f"20日績效為正({_fmt_pct(perf20)})")
    elif perf20 is not None and perf20 < 0:
        reasons.append(f"20日績效轉弱({_fmt_pct(perf20)})")
    if pnl_pct is not None and bought:
        reasons.append(f"目前持倉損益{_fmt_pct(pnl_pct)}")

    # 去重保留前 5 項
    cleaned = []
    for r in reasons:
        if r and r not in cleaned:
            cleaned.append(r)
    reason_text = "；".join(cleaned[:5]) if cleaned else "依分數、價格位置、停損距離與歷史績效綜合判斷"

    return {
        "股神決策分數": round(total_score, 2),
        "股神建議動作": action,
        "股神信心": confidence,
        "股神進場區間": _build_entry_zone(rec_price, stop_price),
        "股神推論": reason_text,
    }


def _resolve_recommendation_basis_v176(src: dict[str, Any]) -> tuple[float | None, str]:
    """Resolve an immutable recommendation basis without ever using latest price.

    The mutable ``最新價`` must never become the historical recommendation price.
    Only fields that describe the original recommendation/entry snapshot are valid.
    """
    for field in [
        "推薦價格", "推薦日價格", "原始推薦價格", "推薦當下價格", "推薦基準價",
    ]:
        value = _safe_float(src.get(field))
        if value is not None and value > 0:
            return float(value), field
    return None, ""


def _recalc_row(row: pd.Series | dict[str, Any]) -> dict[str, Any]:
    """V178：分離「系統推薦追蹤」與「實際交易」損益，避免一欄混用兩種成本基準。"""
    src = dict(row)
    rec_price, rec_price_source = _resolve_recommendation_basis_v176(src)
    buy_price = _safe_float(src.get("實際買進價"))
    sell_price = _safe_float(src.get("實際賣出價"))
    latest_price = _safe_float(src.get("最新價"))
    stop_price = _safe_float(src.get("停損價"))
    target1 = _safe_float(src.get("賣出目標1"))
    target2 = _safe_float(src.get("賣出目標2"))
    status = _safe_str(src.get("目前狀態")) or "觀察"

    if rec_price not in [None, 0]:
        if _safe_float(src.get("推薦價格")) in [None, 0]: src["推薦價格"] = rec_price
        if _safe_float(src.get("推薦日價格")) in [None, 0]: src["推薦日價格"] = rec_price

    system_amt = system_pct = None
    if rec_price not in [None, 0] and latest_price not in [None, 0]:
        system_amt = float(latest_price) - float(rec_price)
        system_pct = system_amt / float(rec_price) * 100

    unrealized_pct = None
    if buy_price not in [None, 0] and sell_price in [None, 0] and latest_price not in [None, 0]:
        unrealized_pct = (float(latest_price) - float(buy_price)) / float(buy_price) * 100
    realized_pct = None
    if buy_price not in [None, 0] and sell_price not in [None, 0]:
        realized_pct = (float(sell_price) - float(buy_price)) / float(buy_price) * 100

    buy_flag = _normalize_bool(src.get("是否已實際買進")) or buy_price not in [None, 0] or status in {"已買進", "持有"}
    hit_stop = _normalize_bool(src.get("是否達停損"))
    hit_t1 = _normalize_bool(src.get("是否達目標1"))
    hit_t2 = _normalize_bool(src.get("是否達目標2"))
    if latest_price is not None:
        if stop_price is not None and latest_price <= stop_price: hit_stop = True
        if target1 is not None and latest_price >= target1: hit_t1 = True
        if target2 is not None and latest_price >= target2: hit_t2 = True

    rec_date = pd.to_datetime(_safe_str(src.get("推薦日期")), errors="coerce")
    tracking_days = _safe_float(src.get("持有天數"))
    if pd.notna(rec_date):
        # 沒有買賣日期欄位，這裡只能代表「推薦追蹤日數」，不能冒充實際持有日數。
        if sell_price not in [None, 0] and tracking_days not in [None, 0]:
            tracking_days = tracking_days
        else:
            tracking_days = max((_tw_today() - rec_date.date()).days, 0)

    perf_label = _safe_str(src.get("模式績效標籤"))
    score_for_label = realized_pct if realized_pct is not None else system_pct
    if not perf_label and score_for_label is not None:
        perf_label = "強" if score_for_label >= 12 else "中" if score_for_label >= 3 else "觀察中" if score_for_label > -3 else "弱"
    if status == "停損": hit_stop = True
    if status == "達標": hit_t1 = True

    quote_status = _safe_str(src.get("最新價更新狀態"))
    if rec_price in [None, 0]: calc_status = "缺少不可變推薦基準價，未計算系統追蹤損益"
    elif latest_price in [None, 0]: calc_status = "缺少最新價，未計算系統追蹤損益"
    elif quote_status.startswith(("保留舊價", "等待新交易日", "行情失敗", "舊資料未驗證")): calc_status = f"沿用已保存價格｜{quote_status}"
    elif "日期未驗證" in quote_status: calc_status = "行情日期未驗證，不視為正式更新"
    elif system_amt == 0 and _safe_str(src.get("最新價資料日期")): calc_status = "已驗證行情，價格恰與推薦基準相同"
    else: calc_status = "系統追蹤損益已計算"

    src["是否已實際買進"] = buy_flag
    src["推薦基準價來源"] = rec_price_source or "缺少推薦基準價"
    src["損益計算基準"] = f"{rec_price_source or '缺少推薦基準價'} → 最新價"
    src["損益計算狀態"] = calc_status
    src["系統追蹤每股損益"] = system_amt
    src["系統追蹤報酬%"] = system_pct
    # 舊欄位保留相容性，但固定代表推薦基準→最新價，不再被實際買賣價改變定義。
    src["損益金額"] = system_amt
    src["損益幅%"] = system_pct
    src["損益%"] = system_pct
    src["實際未實現報酬%"] = unrealized_pct
    src["實際已實現報酬%"] = realized_pct
    src["實際報酬%"] = realized_pct
    src["是否達停損"] = hit_stop
    src["是否達目標1"] = hit_t1
    src["是否達目標2"] = hit_t2
    src["持有天數"] = tracking_days
    src["模式績效標籤"] = perf_label
    src.update(_god_mode_decision(src))
    src["更新時間"] = _now_text()
    return src



def _normalize_quote_date_v176(value: Any) -> str:
    """Normalize quote date to YYYY-MM-DD; empty string means unverifiable."""
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        try:
            n = float(value)
            if n > 10_000_000_000:
                n /= 1000.0
            if n > 1_000_000_000:
                return datetime.fromtimestamp(n, _TW_TZ).strftime("%Y-%m-%d")
        except Exception:
            pass
    text = _safe_str(value)
    if not text:
        return ""
    m = re.search(r"(?<!\d)(20\d{2})[-/]?(\d{2})[-/]?(\d{2})(?!\d)", text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
        except Exception:
            pass
    dt = pd.to_datetime(text, errors="coerce")
    if pd.notna(dt):
        try:
            return dt.date().isoformat()
        except Exception:
            return ""
    return ""


def _normalize_quote_time_v176(value: Any) -> str:
    text = _safe_str(value)
    if not text:
        return ""
    m = re.search(r"(?<!\d)(\d{1,2}):(\d{2})(?::(\d{2}))?", text)
    if m:
        hh, mm, ss = int(m.group(1)), int(m.group(2)), int(m.group(3) or 0)
        if 0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59:
            return f"{hh:02d}:{mm:02d}:{ss:02d}"
    return ""


def _quote_price_from_info(info: Any) -> tuple[float | None, str, str, str, str]:
    """V178：推薦紀錄只接受「實際成交」或有交易日的日線收盤；bid/ask/mid/match/昨收皆不可當最新價。"""
    if not isinstance(info, dict):
        return None, "", "NO_INFO", "", ""
    market = _safe_str(info.get("market") or info.get("市場別"))
    src = _safe_str(info.get("price_source") or info.get("來源") or info.get("message") or "realtime")
    raw = info.get("raw") if isinstance(info.get("raw"), dict) else {}
    quote_date = _normalize_quote_date_v176(info.get("quote_date") or info.get("date") or info.get("交易日期") or raw.get("d") or info.get("update_time"))
    quote_time = _normalize_quote_time_v176(info.get("quote_time") or raw.get("t") or info.get("update_time"))
    src_key = src.strip().lower().replace("-", "_").replace(" ", "_")
    unsafe = {"prev_close", "previous_close", "previousclose", "yesterday_close", "昨收", "昨收回退", "mid", "bid", "ask", "match", "pz"}
    if src_key in unsafe or any(k in src_key for k in ["prev_close", "reference_only", "_mid", "_bid", "_ask", "_match"]):
        return None, market, f"INDICATIVE_ONLY:{src or 'reference'}", quote_date, quote_time
    price = _safe_float(info.get("price") or info.get("現價") or info.get("最新價") or info.get("close") or info.get("收盤價"))
    if price is None or price <= 0:
        return None, market, src or "NO_PRICE", quote_date, quote_time
    # utils realtime source: only 'trade' is a final transaction price. Daily fallbacks must carry a date.
    if src_key == "trade":
        if not quote_date:
            return None, market, "UNVERIFIED_TRADE_DATE", "", quote_time
        return float(price), market, src, quote_date, quote_time
    daily_markers = ("daily", "history", "close", "yahoo", "finmind", "stooq", "twse_openapi", "tpex_openapi")
    if any(k in src_key for k in daily_markers):
        if not quote_date:
            return None, market, f"UNVERIFIED_DAILY_DATE:{src}", "", quote_time
        return float(price), market, src, quote_date, quote_time
    return None, market, f"UNVERIFIED_QUOTE_TYPE:{src}", quote_date, quote_time


def _market_candidates(market_type: Any) -> list[str]:
    mk0 = _safe_str(market_type) or "上市"
    out = []
    for mk in [mk0, "上市", "上櫃", "興櫃"]:
        if mk and mk not in out:
            out.append(mk)
    return out


def _fast_latest_quote(stock_no: str, stock_name: str, market_type: str) -> tuple[float | None, str, str, str, str]:
    """Single-stock safety fallback; reference-only previous close is rejected."""
    stock_no = _normalize_code(stock_no)
    stock_name = _safe_str(stock_name)
    if not stock_no:
        return None, _safe_str(market_type), "NO_CODE", "", ""

    token = f"record_latest_v178_{_tw_now():%Y%m%d%H%M%S}"
    last_src = "ONLINE_FAIL"
    last_date = ""
    last_time = ""
    for mk in _market_candidates(market_type):
        try:
            info = get_realtime_stock_info(stock_no, stock_name, mk, refresh_token=token + mk)
            price, used_market, src, qdate, qtime = _quote_price_from_info(info)
            last_src, last_date, last_time = src or last_src, qdate or last_date, qtime or last_time
            if price is not None and price > 0:
                return price, used_market or mk, src or "realtime", qdate, qtime
        except Exception as e:
            last_src = f"REALTIME_EXCEPTION:{str(e)[:60]}"
            continue

    if _rt_yahoo_fallback is not None:
        for mk in _market_candidates(market_type):
            try:
                info = _rt_yahoo_fallback(stock_no, stock_name, mk, refresh_day=_tw_today().isoformat())
                price, used_market, src, qdate, qtime = _quote_price_from_info(info)
                last_src, last_date, last_time = src or last_src, qdate or last_date, qtime or last_time
                if price is not None and price > 0:
                    return price, used_market or mk, src or "yahoo_daily_fallback", qdate, qtime
            except Exception as e:
                last_src = f"YAHOO_EXCEPTION:{str(e)[:60]}"
                continue
    return None, _safe_str(market_type), last_src or "ONLINE_FAIL", last_date, last_time


def _quote_request_json(url: str, params: dict[str, Any] | None = None, timeout: float = 4.0) -> Any:
    """V176：輕量 HTTP JSON 讀取，避免最新價來源失敗時整頁卡死。"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
    }
    resp = requests.get(url, params=params or {}, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _quote_from_twse_mis(stock_no: str, market_type: str) -> tuple[float | None, str, str, str, str]:
    """TWSE MIS actual trade/match quote. Previous close is never accepted as latest."""
    code = _normalize_code(stock_no)
    if not code:
        return None, _safe_str(market_type), "TWSE_MIS_NO_CODE", "", ""
    mk = _safe_str(market_type)
    prefixes = ["otc", "tse"] if mk in {"上櫃", "興櫃"} else ["tse", "otc"]
    last_src = "TWSE_MIS_NO_DATA"
    last_date = ""
    last_time = ""
    for pref in prefixes:
        try:
            url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
            data = _quote_request_json(url, {"ex_ch": f"{pref}_{code}.tw", "json": "1", "delay": "0", "_": str(int(time.time() * 1000))}, timeout=3.5)
            arr = data.get("msgArray") if isinstance(data, dict) else None
            if not arr:
                last_src = f"TWSE_MIS_EMPTY:{pref}"
                continue
            row = arr[0] if isinstance(arr, list) and arr else {}
            last_date = _normalize_quote_date_v176(row.get("d"))
            last_time = _normalize_quote_time_v176(row.get("t"))
            # V178：z 才是已成交價；pz / bid / ask / 模擬撮合都只供參考，不寫入績效最新價。
            z_price = _safe_float(str(row.get("z") or "").replace(",", "").replace("-", "").strip())
            if z_price is not None and z_price > 0 and last_date:
                used_market = "上櫃" if pref == "otc" else "上市"
                return float(z_price), used_market, f"TWSE_MIS_{pref}_TRADE", last_date, last_time
            last_src = f"TWSE_MIS_NO_ACTUAL_TRADE:{pref}"
        except Exception as e:
            last_src = f"TWSE_MIS_EXCEPTION:{str(e)[:60]}"
            continue
    return None, _safe_str(market_type), last_src, last_date, last_time


def _yahoo_symbol_candidates(stock_no: str, market_type: str) -> list[str]:
    code = _normalize_code(stock_no)
    if not code:
        return []
    mk = _safe_str(market_type)
    suffixes = ["TWO", "TW"] if mk in {"上櫃", "興櫃"} else ["TW", "TWO"]
    out = []
    for suffix in suffixes:
        sym = f"{code}.{suffix}"
        if sym not in out:
            out.append(sym)
    return out


def _quote_from_yahoo_chart(stock_no: str, market_type: str) -> tuple[float | None, str, str, str, str]:
    """Yahoo chart daily close with its actual trading date; previousClose is rejected."""
    last_src = "YAHOO_CHART_NO_DATA"
    last_date = ""
    for symbol in _yahoo_symbol_candidates(stock_no, market_type):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            data = _quote_request_json(url, {"range": "14d", "interval": "1d", "events": "history"}, timeout=4.0)
            result = (((data or {}).get("chart") or {}).get("result") or [])
            if not result:
                last_src = f"YAHOO_CHART_EMPTY:{symbol}"
                continue
            r0 = result[0]
            timestamps = r0.get("timestamp") or []
            quote = (((r0.get("indicators") or {}).get("quote") or [{}])[0]) or {}
            closes = quote.get("close") or []
            chosen_price = None
            chosen_ts = None
            for i in range(min(len(timestamps), len(closes)) - 1, -1, -1):
                px = _safe_float(closes[i])
                if px is not None and px > 0:
                    chosen_price = float(px)
                    chosen_ts = timestamps[i]
                    break
            if chosen_price is None:
                meta = r0.get("meta") or {}
                chosen_price = _safe_float(meta.get("regularMarketPrice"))
                chosen_ts = meta.get("regularMarketTime")
            last_date = _normalize_quote_date_v176(chosen_ts)
            if chosen_price is not None and chosen_price > 0:
                used_market = "上櫃" if symbol.endswith(".TWO") else "上市"
                return float(chosen_price), used_market, f"YAHOO_CHART:{symbol}", last_date, ""
            last_src = f"YAHOO_CHART_NO_PRICE:{symbol}"
        except Exception as e:
            last_src = f"YAHOO_CHART_EXCEPTION:{str(e)[:60]}"
            continue
    return None, _safe_str(market_type), last_src, last_date, ""


def _quote_from_finmind_daily(stock_no: str, market_type: str) -> tuple[float | None, str, str, str, str]:
    code = _normalize_code(stock_no)
    if not code:
        return None, _safe_str(market_type), "FINMIND_NO_CODE", "", ""
    try:
        start_date = (_tw_today() - timedelta(days=21)).isoformat()
        url = "https://api.finmindtrade.com/api/v4/data"
        data = _quote_request_json(url, {"dataset": "TaiwanStockPrice", "data_id": code, "start_date": start_date}, timeout=5.0)
        rows = data.get("data") if isinstance(data, dict) else None
        if not rows:
            return None, _safe_str(market_type), "FINMIND_EMPTY", "", ""
        valid = []
        for row in rows:
            px = _safe_float((row or {}).get("close"))
            qdate = _normalize_quote_date_v176((row or {}).get("date"))
            if px is not None and px > 0:
                valid.append((qdate, float(px)))
        if valid:
            valid.sort(key=lambda x: x[0] or "")
            qdate, price = valid[-1]
            return price, _safe_str(market_type), "FINMIND_DAILY_CLOSE", qdate, ""
        return None, _safe_str(market_type), "FINMIND_NO_PRICE", "", ""
    except Exception as e:
        return None, _safe_str(market_type), f"FINMIND_EXCEPTION:{str(e)[:60]}", "", ""


def _quote_from_stooq_daily(stock_no: str, market_type: str) -> tuple[float | None, str, str, str, str]:
    code = _normalize_code(stock_no)
    if not code:
        return None, _safe_str(market_type), "STOOQ_NO_CODE", "", ""
    candidates = [f"{code}.tw", f"{code}.two"]
    last_src = "STOOQ_NO_DATA"
    headers = {"User-Agent": "Mozilla/5.0"}
    for symbol in candidates:
        try:
            url = "https://stooq.com/q/l/"
            resp = requests.get(url, params={"s": symbol, "f": "sd2t2ohlcv", "h": "", "e": "csv"}, headers=headers, timeout=4.0)
            resp.raise_for_status()
            lines = [ln.strip() for ln in resp.text.splitlines() if ln.strip()]
            if len(lines) < 2:
                last_src = f"STOOQ_EMPTY:{symbol}"
                continue
            parts = lines[-1].split(",")
            if len(parts) < 7:
                last_src = f"STOOQ_BAD_CSV:{symbol}"
                continue
            price = _safe_float(parts[6])
            qdate = _normalize_quote_date_v176(parts[1])
            qtime = _normalize_quote_time_v176(parts[2])
            if price is not None and price > 0:
                used_market = "上櫃" if symbol.endswith(".two") else _safe_str(market_type)
                return float(price), used_market, f"STOOQ_DAILY:{symbol}", qdate, qtime
            last_src = f"STOOQ_NO_PRICE:{symbol}"
        except Exception as e:
            last_src = f"STOOQ_EXCEPTION:{str(e)[:60]}"
            continue
    return None, _safe_str(market_type), last_src, "", ""


def _history_row_date_v176(hist: pd.DataFrame, idx: Any) -> str:
    for col in ["日期", "交易日期", "date", "Date", "datetime", "Datetime"]:
        if col in hist.columns:
            try:
                return _normalize_quote_date_v176(hist.loc[idx, col])
            except Exception:
                pass
    return _normalize_quote_date_v176(idx)


def _quote_from_local_history(stock_no: str, stock_name: str, market_type: str) -> tuple[float | None, str, str, str, str]:
    code = _normalize_code(stock_no)
    if not code:
        return None, _safe_str(market_type), "LOCAL_HISTORY_NO_CODE", "", ""
    try:
        end_dt = _tw_today()
        start_dt = end_dt - timedelta(days=21)
        hist = get_history_data(code, _safe_str(stock_name), _safe_str(market_type), start_dt, end_dt)
        if isinstance(hist, pd.DataFrame) and not hist.empty:
            for col in ["收盤價", "close", "Close", "收盤"]:
                if col in hist.columns:
                    vals = pd.to_numeric(hist[col], errors="coerce")
                    valid = vals[(vals.notna()) & (vals > 0)]
                    if not valid.empty:
                        idx = valid.index[-1]
                        return float(valid.iloc[-1]), _safe_str(market_type), "LOCAL_HISTORY_CLOSE", _history_row_date_v176(hist, idx), ""
        return None, _safe_str(market_type), "LOCAL_HISTORY_EMPTY", "", ""
    except Exception as e:
        return None, _safe_str(market_type), f"LOCAL_HISTORY_EXCEPTION:{str(e)[:60]}", "", ""


def _alternative_latest_quote(stock_no: str, stock_name: str, market_type: str) -> tuple[float | None, str, str, str, str]:
    """Fallback chain with quote-date metadata."""
    chain = [
        lambda: _quote_from_twse_mis(stock_no, market_type),
        lambda: _quote_from_yahoo_chart(stock_no, market_type),
        lambda: _quote_from_finmind_daily(stock_no, market_type),
        lambda: _quote_from_stooq_daily(stock_no, market_type),
        lambda: _quote_from_local_history(stock_no, stock_name, market_type),
    ]
    last = (None, _safe_str(market_type), "ALT_ALL_FAIL", "", "")
    for fn in chain:
        try:
            result = fn()
            last = result
            price, used_market, src, qdate, qtime = result
            if price is not None and price > 0:
                return float(price), used_market or _safe_str(market_type), src or "ALT_SOURCE", qdate, qtime
        except Exception as e:
            last = (None, _safe_str(market_type), f"ALT_EXCEPTION:{str(e)[:60]}", "", "")
    return last


@st.cache_data(ttl=90, show_spinner=False)
def _official_latest_market_snapshot_v179(as_of_text: str) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """V179: one full-market TWSE/TPEx daily snapshot, cached briefly.

    This is the key weekend/holiday fallback: MIS has no actual `z` trade when the
    market is closed, so page 8 must use the latest completed official exchange
    daily close rather than silently retaining recommendation-day prices.
    """
    if not callable(fetch_latest_official_market_snapshot):
        return {}, {"version": OFFICIAL_LATEST_PRICE_SERVICE_VERSION, "error": "service unavailable"}
    try:
        rows, diag = fetch_latest_official_market_snapshot(as_of=as_of_text, lookback_days=10, timeout=6.0)
        return rows if isinstance(rows, dict) else {}, diag if isinstance(diag, dict) else {}
    except Exception as exc:
        return {}, {
            "version": OFFICIAL_LATEST_PRICE_SERVICE_VERSION,
            "error": f"{type(exc).__name__}:{str(exc)[:160]}",
            "as_of": as_of_text,
        }


def _batch_latest_quotes(target_payloads: list[dict[str, Any]]) -> dict[str, tuple[float | None, str, str, str, str]]:
    """Batch latest quote with source/date audit metadata.

    Only actual trade or dated daily close is accepted; indicative match/bid/ask/previous close are rejected
    so the alternative chain can obtain a real newer trading-day close.
    """
    result: dict[str, tuple[float | None, str, str, str, str]] = {}
    if not target_payloads:
        return result

    code_meta: dict[str, dict[str, str]] = {}
    for payload in target_payloads:
        code = _normalize_code(payload.get("股票代號"))
        if not code:
            continue
        code_meta[code] = {
            "code": code,
            "name": _safe_str(payload.get("股票名稱")),
            "market": _safe_str(payload.get("市場別")) or "上市",
        }

    unresolved = set(code_meta.keys())
    token = f"record_latest_v178_batch_{_tw_now():%Y%m%d%H%M%S}"

    def _run_batch(items: list[dict[str, str]], source_tag: str) -> None:
        nonlocal result, unresolved
        if not items or _rt_batch_fetch is None:
            return
        try:
            batch_map = _rt_batch_fetch(items, refresh_token=token + source_tag)
        except Exception as e:
            for item in items:
                code = _normalize_code(item.get("code"))
                if code and code not in result:
                    result[code] = (None, _safe_str(item.get("market")), f"BATCH_EXCEPTION:{str(e)[:80]}", "", "")
            return
        if not isinstance(batch_map, dict):
            return
        for item in items:
            code = _normalize_code(item.get("code"))
            if not code or code not in unresolved:
                continue
            info = batch_map.get(code) or batch_map.get(str(code))
            price, used_market, src, qdate, qtime = _quote_price_from_info(info)
            if price is not None and price > 0:
                result[code] = (price, used_market or _safe_str(item.get("market")), src or f"batch_{source_tag}", qdate, qtime)
                unresolved.discard(code)
            elif src:
                result[code] = (None, used_market or _safe_str(item.get("market")), src, qdate, qtime)

    _run_batch(list(code_meta.values()), "_origin")

    # V179：市場休市／週末時 MIS 通常沒有實際成交 z。此時先用交易所「整個市場」
    # 最新已完成交易日收盤快照補價，只需少數請求，不再讓每檔逐一依賴 Yahoo/FinMind。
    official_diag: dict[str, Any] = {}
    official_matched = 0
    if unresolved:
        _official_fn = globals().get("_official_latest_market_snapshot_v179")
        if callable(_official_fn):
            try:
                official_map, official_diag = _official_fn(_tw_today().isoformat())
                if isinstance(official_map, dict):
                    for code in list(unresolved):
                        q = official_map.get(code) or {}
                        price = _safe_float(q.get("price")) if isinstance(q, dict) else None
                        qdate = _normalize_quote_date_v176(q.get("date")) if isinstance(q, dict) else ""
                        if price is None or price <= 0 or not qdate:
                            continue
                        qmarket = _safe_str(q.get("market")) or code_meta.get(code, {}).get("market", "上市")
                        qsrc = _safe_str(q.get("source")) or "OFFICIAL_DAILY_CLOSE"
                        qtime = _normalize_quote_time_v176(q.get("time"))
                        result[code] = (float(price), qmarket, qsrc, qdate, qtime)
                        unresolved.discard(code)
                        official_matched += 1
            except Exception as exc:
                official_diag = {"error": f"{type(exc).__name__}:{str(exc)[:120]}"}
    try:
        st.session_state[_k("v179_official_snapshot_diag")] = {**(official_diag or {}), "matched_records": official_matched}
    except Exception:
        pass

    for market in ["上市", "上櫃", "興櫃"]:
        if not unresolved:
            break
        items = [{"code": code, "name": code_meta.get(code, {}).get("name", ""), "market": market} for code in list(unresolved)]
        _run_batch(items, f"_{market}")

    allow_alt = bool(st.session_state.get(_k("enable_alt_price_sources"), True))
    alt_chunk_size = max(10, min(300, int(st.session_state.get(_k("alt_price_source_limit"), 60) or 60)))
    alt_workers = max(1, min(10, int(st.session_state.get(_k("alt_price_workers"), 6) or 6)))
    if allow_alt and unresolved:
        def _alt_job(code: str):
            meta = code_meta.get(code, {})
            latest, used_market, src, qdate, qtime = _alternative_latest_quote(code, meta.get("name", ""), meta.get("market", "上市"))
            return code, latest, used_market, src, qdate, qtime

        # V178：alt_chunk_size 只控制每批，不是總量上限；完整處理所有 unresolved。
        pending = list(unresolved)
        for pos in range(0, len(pending), alt_chunk_size):
            alt_codes = [c for c in pending[pos:pos + alt_chunk_size] if c in unresolved]
            if not alt_codes:
                continue
            with ThreadPoolExecutor(max_workers=min(alt_workers, max(1, len(alt_codes)))) as executor:
                futures = [executor.submit(_alt_job, code) for code in alt_codes]
                for future in as_completed(futures):
                    try:
                        code, latest, used_market, src, qdate, qtime = future.result(timeout=12)
                    except Exception:
                        continue
                    meta = code_meta.get(code, {})
                    if latest is not None and latest > 0 and qdate:
                        # 市場別以既有主檔為優先，避免批次 API meta 對應錯誤把上櫃寫成上市。
                        result[code] = (latest, used_market or meta.get("market", "上市"), src or "alt_source", qdate, qtime)
                        unresolved.discard(code)
                    else:
                        result[code] = (None, meta.get("market", "上市"), src or "ALT_FAIL", qdate, qtime)

    allow_slow = bool(st.session_state.get(_k("enable_slow_price_fallback"), False))
    slow_limit = int(st.session_state.get(_k("slow_price_fallback_limit"), 20) or 20)
    if allow_slow and unresolved:
        for code in list(unresolved)[:max(0, slow_limit)]:
            meta = code_meta.get(code, {})
            latest, used_market, src, qdate, qtime = _fast_latest_quote(code, meta.get("name", ""), meta.get("market", "上市"))
            if latest is not None and latest > 0:
                result[code] = (latest, used_market or meta.get("market", "上市"), src or "single_slow_fallback", qdate, qtime)
                unresolved.discard(code)
            else:
                result[code] = (None, meta.get("market", "上市"), src or "ONLINE_FAIL", qdate, qtime)

    for code in list(unresolved):
        meta = code_meta.get(code, {})
        old = result.get(code)
        if old is None or old[0] is None:
            prior_src = old[2] if old else ""
            prior_date = old[3] if old else ""
            prior_time = old[4] if old else ""
            result[code] = (None, meta.get("market", "上市"), prior_src or "BATCH_FAIL_KEEP_OLD", prior_date, prior_time)
    return result


def _refresh_latest_prices(df: pd.DataFrame, only_active: bool = False) -> pd.DataFrame:
    """V176 verified latest-price update and immediate P/L recalculation.

    - Processes the complete target set; batch size is not a total limit.
    - Previous close is not counted as a successful latest-price update.
    - Quote source/date/status is stored per row.
    - A quote that is not newer than a previous-day recommendation is reported
      as waiting for a new trading day instead of silently showing false success.
    - Recommendation price is immutable and is never backfilled from latest price.
    """
    if df is None or df.empty:
        out = _ensure_godpick_record_columns(pd.DataFrame())
        out.attrs["latest_refresh_summary"] = {
            "target": 0, "success": 0, "fail": 0, "skipped": 0, "limited": 0, "batches": 0,
            "preserved_old_price": 0, "waiting_new_trade": 0, "stale_quote": 0,
            "unverified_date": 0, "pnl_calculated": 0, "missing_basis": 0, "unchanged_price": 0,
        }
        return out

    active_status = {"觀察", "已買進", "持有", "追蹤", "未出場", "強烈關注", "新推薦", "雷達觀察", ""}
    rows = [dict(row) for _, row in df.iterrows()]

    batch_size = int(st.session_state.get(_k("latest_price_batch_size"), st.session_state.get(_k("perf_update_batch_size"), 80)) or 80)
    batch_size = max(10, min(batch_size, 500))

    target_indexes = []
    skipped = 0
    for i, payload in enumerate(rows):
        status = _safe_str(payload.get("目前狀態")) or "觀察"
        if only_active and status not in active_status:
            skipped += 1
            rows[i] = _recalc_row(payload)
            continue
        if not _normalize_code(payload.get("股票代號")):
            skipped += 1
            payload["最新價更新狀態"] = "行情失敗｜缺少股票代號"
            rows[i] = _recalc_row(payload)
            continue
        target_indexes.append(i)

    success = 0
    fail = 0
    preserved_old_price = 0
    waiting_new_trade = 0
    stale_quote = 0
    unverified_date = 0
    pnl_calculated = 0
    missing_basis = 0
    unchanged_price = 0
    source_counts: dict[str, int] = {}
    batches = 0
    issue_samples: list[dict[str, str]] = []
    today_text = _tw_today().isoformat()
    try:
        st.session_state[_k("v179_official_snapshot_diag")] = {}
    except Exception:
        pass

    for start_i in range(0, len(target_indexes), batch_size):
        batch_indexes = target_indexes[start_i:start_i + batch_size]
        if not batch_indexes:
            continue
        batches += 1
        quote_map = _batch_latest_quotes([rows[i] for i in batch_indexes])

        for i in batch_indexes:
            payload = rows[i]
            code = _normalize_code(payload.get("股票代號"))
            latest, used_market, price_src, quote_date, quote_time = quote_map.get(
                code,
                (None, _safe_str(payload.get("市場別")), "ONLINE_FAIL", "", ""),
            )
            old_latest = _safe_float(payload.get("最新價"))
            old_quote_date = _normalize_quote_date_v176(payload.get("最新價資料日期"))
            rec_date = _normalize_quote_date_v176(payload.get("推薦日期"))
            source_key = _safe_str(price_src) or "UNKNOWN"
            source_counts[source_key] = source_counts.get(source_key, 0) + 1

            accept_quote = latest is not None and latest > 0 and bool(quote_date)
            status_text = ""
            if latest is not None and latest > 0 and not quote_date:
                unverified_date += 1
                status_text = f"保留舊價｜行情日期未驗證：{source_key}"
            if accept_quote and quote_date and old_quote_date and quote_date < old_quote_date:
                accept_quote = False
                stale_quote += 1
                status_text = f"保留舊價｜來源行情日期 {quote_date} 早於已保存日期 {old_quote_date}"
            elif accept_quote and quote_date and rec_date and rec_date < today_text and quote_date <= rec_date:
                accept_quote = False
                waiting_new_trade += 1
                status_text = f"等待新交易日｜來源日期 {quote_date} 未晚於推薦日 {rec_date}"
            elif accept_quote and quote_date and rec_date and quote_date < rec_date:
                accept_quote = False
                stale_quote += 1
                status_text = f"保留舊價｜來源日期 {quote_date} 早於推薦日 {rec_date}"

            if accept_quote:
                payload["最新價"] = float(latest)
                payload["市場別"] = used_market or _safe_str(payload.get("市場別"))
                payload["最新價資料日期"] = quote_date
                payload["最新價資料時間"] = quote_time
                payload["最新價來源"] = source_key
                status_text = "已更新｜行情日期已驗證"
                payload["最新價更新狀態"] = status_text
                payload["最新更新時間"] = _now_text()
                payload["追蹤更新時間"] = _now_text()
                success += 1
                if old_latest is not None and abs(float(latest) - old_latest) < 1e-12:
                    unchanged_price += 1
            else:
                fail += 1
                payload["最新價來源"] = source_key
                if quote_date:
                    payload["最新價資料日期"] = quote_date
                if quote_time:
                    payload["最新價資料時間"] = quote_time
                if old_latest is not None and old_latest > 0:
                    payload["最新價"] = old_latest
                    preserved_old_price += 1
                    if not status_text:
                        status_text = f"保留舊價｜行情失敗：{source_key}"
                else:
                    if not status_text:
                        status_text = f"行情失敗｜{source_key}"
                payload["最新價更新狀態"] = status_text
                if len(issue_samples) < 30:
                    issue_samples.append({
                        "股票代號": code,
                        "股票名稱": _safe_str(payload.get("股票名稱")),
                        "狀態": status_text,
                        "來源": source_key,
                        "行情日期": quote_date,
                        "推薦日期": rec_date,
                    })

            rows[i] = _recalc_row(payload)
            if _safe_float(rows[i].get("損益幅%")) is not None:
                pnl_calculated += 1
            if _safe_str(rows[i].get("推薦基準價來源")) == "缺少推薦基準價":
                missing_basis += 1

    processed = set(target_indexes)
    for i, payload in enumerate(rows):
        if i not in processed:
            rows[i] = _recalc_row(payload)

    out = _ensure_godpick_record_columns(pd.DataFrame(rows))
    _official_diag_v179 = dict(st.session_state.get(_k("v179_official_snapshot_diag"), {}) or {})
    _official_diag_v179["matched_records"] = int(sum(
        count for src, count in source_counts.items()
        if _safe_str(src).startswith("TWSE_OFFICIAL_DAILY_CLOSE") or _safe_str(src).startswith("TPEX_OFFICIAL_DAILY_CLOSE")
    ))
    out.attrs["latest_refresh_summary"] = {
        "version": LATEST_PRICE_PNL_FIX_VERSION,
        "target": len(target_indexes),
        "success": success,
        "fail": fail,
        "skipped": skipped,
        "limited": 0,
        "batch_size": batch_size,
        "batches": batches,
        "preserved_old_price": preserved_old_price,
        "waiting_new_trade": waiting_new_trade,
        "stale_quote": stale_quote,
        "unverified_date": unverified_date,
        "pnl_calculated": pnl_calculated,
        "missing_basis": missing_basis,
        "unchanged_price": unchanged_price,
        "source_counts": source_counts,
        "issue_samples": issue_samples,
        "fast_mode": not bool(st.session_state.get(_k("enable_slow_price_fallback"), False)),
        "alt_sources_enabled": bool(st.session_state.get(_k("enable_alt_price_sources"), True)),
        "alt_source_limit": int(st.session_state.get(_k("alt_price_source_limit"), 60) or 60),
        "alt_workers": int(st.session_state.get(_k("alt_price_workers"), 6) or 6),
        "slow_fallback_enabled": bool(st.session_state.get(_k("enable_slow_price_fallback"), False)),
        "official_snapshot": _official_diag_v179,
    }
    return out

def _row_needs_perf_update(payload: dict[str, Any]) -> bool:
    """V51：判斷是否真的需要抓歷史資料，避免每次全表重跑。"""
    if not payload:
        return False
    code = _normalize_code(payload.get("股票代號"))
    if not code:
        return False
    rec_date = pd.to_datetime(_safe_str(payload.get("推薦日期")), errors="coerce")
    if pd.isna(rec_date):
        return False
    # 推薦日太近時，1/3/5/10/20 日資料本來就尚未完整，不重複卡住等待。
    age_days = (_tw_today() - rec_date.date()).days
    if age_days < 1:
        return False
    # V115：舊版 V115 會把「盤中一度碰到目標價」直接列為達標。
    # 若資料仍是 V115 或達標判斷疑似虛高，必須強制重算，不受 12 小時快取保護限制。
    perf_comment = _safe_str(payload.get("績效評語"))
    hit_result_now = _safe_str(payload.get("命中結果"))
    target_px_now = (
        _safe_float(payload.get("賣出目標1"))
        or _safe_float(payload.get("第一壓力價"))
        or _safe_float(payload.get("突破確認價_隔日"))
        or _safe_float(payload.get("近端壓力"))
    )
    latest_px_now = _safe_float(payload.get("最新價"))
    old_target_success = _normalize_bool(payload.get("是否達標_回測"))
    if "V115" in perf_comment:
        return True
    if "是否曾達標_回測" not in payload:
        return True
    if old_target_success and target_px_now not in [None, 0] and latest_px_now not in [None, 0] and latest_px_now < target_px_now * 0.995:
        return True
    if hit_result_now == "達標" and target_px_now not in [None, 0] and latest_px_now not in [None, 0] and latest_px_now < target_px_now * 0.995:
        return True

    # 已有 20 日績效且最近 12 小時更新過，就不重複抓。
    has_any = any(_safe_float(payload.get(c)) is not None for c in ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%"])
    last = pd.to_datetime(_safe_str(payload.get("追蹤更新時間")), errors="coerce")
    if has_any and not pd.isna(last):
        try:
            if (_tw_now_naive() - last.to_pydatetime().replace(tzinfo=None)).total_seconds() < 12 * 3600:
                return False
        except Exception:
            pass
    # 優先更新缺資料、或 20 日資料尚未形成的紀錄。
    if not has_any:
        return True
    if age_days >= 20 and _safe_float(payload.get("推薦後20日%")) is None:
        return True
    if age_days >= 10 and _safe_float(payload.get("推薦後10日%")) is None:
        return True
    if age_days >= 5 and _safe_float(payload.get("推薦後5日%")) is None:
        return True
    if age_days >= 3 and _safe_float(payload.get("推薦後3日%")) is None:
        return True
    return False


def _backfill_perf_columns(
    df: pd.DataFrame,
    max_rows: int = 80,
    show_progress: bool = True,
    only_active: bool = True,
    max_seconds: int = 60,
    max_stocks: int = 10,
) -> pd.DataFrame:
    """V71：績效更新快取防卡版。限制單批股票數，降低 Streamlit Cloud 逾時風險。"""
    if df is None or df.empty:
        return _ensure_godpick_record_columns(pd.DataFrame())

    work = _ensure_godpick_record_columns(df.copy()).reset_index(drop=True)
    rows = [dict(r) for _, r in work.iterrows()]
    active_status = {"觀察", "已買進", "持有", "追蹤", "未出場", "等待"}

    candidates: list[int] = []
    wait_count = 0
    for i, payload in enumerate(rows):
        status = _safe_str(payload.get("目前狀態")) or "觀察"
        if only_active and status not in active_status:
            continue
        rec_date = pd.to_datetime(_safe_str(payload.get("推薦日期")), errors="coerce")
        if pd.isna(rec_date):
            continue
        age_days = (_tw_today() - rec_date.date()).days
        if age_days < 1:
            # 太新的推薦先標記等待，不浪費線上抓取。
            payload["績效評語"] = "推薦日期太近，尚無足夠交易日，等待下一批更新。"
            payload["追蹤更新時間"] = _now_text()
            rows[i] = payload
            wait_count += 1
            continue
        if _row_needs_perf_update(payload):
            candidates.append(i)

    max_rows = int(max(10, min(max_rows or 80, 500)))
    max_seconds = int(max(25, min(max_seconds or 60, 150)))
    max_stocks = int(max(3, min(max_stocks or 10, 30)))
    row_targets = candidates[:max_rows]
    target_set = set(row_targets)

    if not row_targets:
        st.session_state[_k("v51_perf_update_summary")] = {
            "待更新總數": len(candidates), "本次更新上限": max_rows, "本次處理": 0,
            "成功": 0, "略過或失敗": 0, "剩餘": 0, "時間防呆觸發": False,
            "時間防呆略過": 0, "單批秒數上限": max_seconds, "更新時間": _now_text(),
            "加速模式": "V71 防卡＋Yahoo/TWSE/TPEX＋即時代理版", "本批抓取股票數": 0, "快取命中估計": 0,
            "等待交易日": wait_count, "單批股票上限": max_stocks,
        }
        return _ensure_godpick_record_columns(pd.DataFrame(rows))

    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for i in row_targets:
        payload = rows[i]
        code = _normalize_code(payload.get("股票代號"))
        name = _safe_str(payload.get("股票名稱"))
        market = _safe_str(payload.get("市場別"))
        rec_date = pd.to_datetime(_safe_str(payload.get("推薦日期")), errors="coerce")
        if not code or pd.isna(rec_date):
            continue
        key = (code, name, market)
        item = groups.setdefault(key, {"indices": [], "min_date": rec_date.date(), "max_date": rec_date.date()})
        item["indices"].append(i)
        item["min_date"] = min(item["min_date"], rec_date.date())
        item["max_date"] = max(item["max_date"], rec_date.date())

    # 優先處理同股筆數較多者，單次抓一檔可補較多紀錄。
    group_items = sorted(groups.items(), key=lambda kv: len(kv[1].get("indices", [])), reverse=True)
    selected_groups = group_items[:max_stocks]
    selected_indices = {idx for _, grp in selected_groups for idx in grp.get("indices", [])}
    total = len(selected_indices)

    prog = st.progress(0, text="V72：準備快取防卡＋Yahoo直接備援更新推薦後績效...") if show_progress and total else None
    status_box = st.empty() if show_progress and total else None
    started_ts = time.time()
    stopped_by_time_guard = False
    time_guard_skip_count = 0
    done = 0
    ok_count = 0
    fail_count = 0
    stock_fetch_count = 0
    cache_hit_count = 0
    fail_cache_skip_count = 0
    online_fail_count = 0

    for g_pos, ((code, name, market), info) in enumerate(selected_groups):
        if time.time() - started_ts > max_seconds:
            stopped_by_time_guard = True
            remaining = [idx for _, grp in selected_groups[g_pos:] for idx in grp.get("indices", [])]
            time_guard_skip_count += len([idx for idx in remaining if idx in target_set])
            break

        start_date = info["min_date"] - timedelta(days=5)
        max_end = min(_tw_today(), info["max_date"] + timedelta(days=95))
        hist_df, used_market, hist_msg, source = _get_perf_history_bundle_v71(code, name, market, start_date, max_end)
        if source == "LOCAL_CACHE":
            cache_hit_count += 1
        elif source == "FAIL_CACHE":
            fail_cache_skip_count += 1
        elif source == "ONLINE_FAIL":
            online_fail_count += 1
            stock_fetch_count += 1
        else:
            stock_fetch_count += 1

        for i in info.get("indices", []):
            if i not in selected_indices:
                continue
            payload = rows[i]
            if time.time() - started_ts > max_seconds:
                stopped_by_time_guard = True
                time_guard_skip_count += 1
                continue

            rec_date_text = _safe_str(payload.get("推薦日期"))
            stop_price = _safe_float(payload.get("停損參考")) or _safe_float(payload.get("停損價"))
            target_price = (
                _safe_float(payload.get("賣出目標1"))
                or _safe_float(payload.get("第一壓力價"))
                or _safe_float(payload.get("突破確認價_隔日"))
                or _safe_float(payload.get("近端壓力"))
            )
            metrics = _calc_forward_metrics_from_history(
                hist_df,
                rec_date_text,
                stop_price,
                target_price,
                _safe_float(payload.get("推薦價格")) or _safe_float(payload.get("推薦日價格")) or _safe_float(payload.get("建議價位")),
                _safe_float(payload.get("最新價")),
            )

            if metrics:
                ok_count += 1
                payload["市場別"] = used_market or payload.get("市場別")
                payload["績效資料型態"] = payload.get("績效資料型態") or "歷史K線"
                payload["績效資料來源"] = source
                for k, v in metrics.items():
                    payload[k] = v
                for d in [1, 3, 5, 10, 20]:
                    old_key = f"{d}日績效%"
                    new_key = f"推薦後{d}日%"
                    if _safe_float(payload.get(old_key)) is None and _safe_float(payload.get(new_key)) is not None:
                        payload[old_key] = payload.get(new_key)
            else:
                proxy_metrics = _calc_proxy_perf_metrics_v71(payload, hist_msg)
                if proxy_metrics:
                    ok_count += 1
                    for k, v in proxy_metrics.items():
                        payload[k] = v
                    if _safe_float(payload.get("1日績效%")) is None and _safe_float(payload.get("推薦後1日%")) is not None:
                        payload["1日績效%"] = payload.get("推薦後1日%")
                else:
                    fail_count += 1
                    payload["績效評語"] = f"V71 本批略過：{hist_msg}"
                    payload["績效資料型態"] = "抓取失敗"
                    payload["績效資料來源"] = source
                    payload["追蹤更新時間"] = _now_text()

            rows[i] = _recalc_row(payload)
            done += 1
            if prog is not None and (done == total or done % 5 == 0):
                prog.progress(
                    min(1.0, done / max(total, 1)),
                    text=f"V71：防卡更新 {done}/{total}｜成功 {ok_count}｜略過/失敗 {fail_count}｜目前 {code} {name}｜{source}",
                )
            if status_box is not None and (done == total or done % 20 == 0):
                status_box.caption(
                    f"本批股票上限 {max_stocks} 檔；筆數上限 {max_rows}；時間防呆 {max_seconds} 秒；"
                    f"線上抓取 {stock_fetch_count} 檔；快取命中 {cache_hit_count} 檔；失敗保護略過 {fail_cache_skip_count} 檔；"
                    f"剩餘待更新約 {max(0, len(candidates)-done)} 筆。"
                )

    remaining_count = max(0, len(candidates) - done)
    # 未被本批股票數涵蓋者，也算保留到下一批，不視為失敗。
    if len(row_targets) > len(selected_indices):
        remaining_count = max(remaining_count, len(candidates) - len(selected_indices))

    st.session_state[_k("v51_perf_update_summary")] = {
        "待更新總數": len(candidates), "本次更新上限": max_rows, "本次處理": done,
        "成功": ok_count, "略過或失敗": fail_count, "剩餘": remaining_count,
        "時間防呆觸發": bool(stopped_by_time_guard), "時間防呆略過": int(time_guard_skip_count),
        "單批秒數上限": max_seconds, "更新時間": _now_text(), "加速模式": "V71 防卡＋Yahoo/TWSE/TPEX＋即時代理版",
        "本批抓取股票數": stock_fetch_count, "快取命中估計": cache_hit_count,
        "單批股票上限": max_stocks, "失敗快取略過": fail_cache_skip_count,
        "線上失敗股票數": online_fail_count, "等待交易日": wait_count,
    }
    return _ensure_godpick_record_columns(pd.DataFrame(rows))


def _load_records(force_remote: bool = False) -> pd.DataFrame:
    """以專案固定路徑秒開；啟動時已先比對遠端權威，重新載入僅作人工驗證。"""
    if not force_remote:
        local_df, local_err = _read_records_from_local_files()
        if not local_df.empty:
            st.session_state[_k("load_detail")] = [f"本機固定路徑：{local_err}", "遠端：啟動階段已比對摘要；重新載入僅用於人工完整驗證"]
            return _ensure_godpick_record_columns(local_df)
    if callable(load_records_permanent):
        try:
            rows, details = load_records_permanent()
            st.session_state[_k("load_detail")] = details
            return _ensure_godpick_record_columns(pd.DataFrame(rows))
        except Exception as exc:
            # V181：即使雲端部署發生 persistence 檔案版本混用（例如舊 caller 找不到
            # restore_records_snapshot），第8頁也必須先以本機權威檔繼續可用，不讓整頁崩潰。
            st.session_state[_k("load_detail")] = [
                f"永久來源讀取失敗：{exc}",
                f"V181相容層：已改用本機固定權威檔；版本={RECORD_AUTHORITY_COMPAT_VERSION}",
            ]
    local_df, local_err = _read_records_from_local_files()
    if local_err:
        current = list(st.session_state.get(_k("load_detail"), []) or [])
        current.append(f"本機 fallback：{local_err}")
        st.session_state[_k("load_detail")] = current
    return _ensure_godpick_record_columns(local_df)


def _save_state_df(df: pd.DataFrame):
    # V157：只在資料進入 session_state 時正規化一次，後續讀取用淺拷貝。
    normalized = _ensure_godpick_record_columns(df)
    normalized = _mark_normalized_records_v157(normalized)
    st.session_state[_k("records_df")] = normalized
    st.session_state[_k("records_saved_at")] = _now_text()
    _invalidate_analysis_cache()


def _get_state_df() -> pd.DataFrame:
    df = st.session_state.get(_k("records_df"))
    if isinstance(df, pd.DataFrame):
        if _is_normalized_records_v157(df):
            return _copy_records_frame_v157(df)
        normalized = _ensure_godpick_record_columns(df)
        st.session_state[_k("records_df")] = normalized
        return _copy_records_frame_v157(normalized)
    return _mark_normalized_records_v157(pd.DataFrame(columns=GODPICK_RECORD_COLUMNS))



def _reconcile_latest_snapshot_into_authority_v174(authority_df: pd.DataFrame) -> tuple[bool, list[str]]:
    """Repair the newest day from the saved page-7 snapshot when authority lags.

    This is a safety net, not the primary write path.  It only imports formal,
    A- and R1 research rows from ``godpick_latest_recommendations.json`` when
    that snapshot date is newer than the canonical record file.
    """
    details: list[str] = []
    if not callable(upsert_records_authority_fast) or not callable(read_local_json):
        return False, details
    try:
        latest_path = project_path("godpick_latest_recommendations.json") if callable(project_path) else "godpick_latest_recommendations.json"
        stt = os.stat(latest_path)
        snapshot_sig = f"{int(getattr(stt, 'st_mtime_ns', int(stt.st_mtime * 1_000_000_000)))}:{stt.st_size}"
    except Exception:
        return False, details

    authority_latest = ""
    if isinstance(authority_df, pd.DataFrame) and not authority_df.empty and "推薦日期" in authority_df.columns:
        dates = pd.to_datetime(authority_df["推薦日期"], errors="coerce").dropna()
        if not dates.empty:
            authority_latest = dates.max().strftime("%Y-%m-%d")
    check_key = f"{snapshot_sig}|{authority_latest}"
    if _safe_str(st.session_state.get(_k("latest_snapshot_reconcile_sig_v174"), "")) == check_key:
        return False, details
    st.session_state[_k("latest_snapshot_reconcile_sig_v174")] = check_key

    payload, msg, _ = read_local_json("godpick_latest_recommendations.json", {})
    details.append(f"最新推薦快照：{msg}")
    if not isinstance(payload, dict):
        return False, details
    saved_at = _safe_str(payload.get("saved_at"))
    snapshot_date = saved_at[:10] if len(saved_at) >= 10 else ""
    rows = payload.get("recommendations", [])
    if not snapshot_date or not isinstance(rows, list) or snapshot_date <= authority_latest:
        return False, details

    saved_time = saved_at[11:19] if len(saved_at) >= 19 else _tw_now().strftime("%H:%M:%S")
    actionable: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        bucket = _safe_str(raw.get("正式推薦分區"))
        radar = _safe_str(raw.get("盤中雷達優先級"))
        is_actionable = bucket in {"正式下週主推薦", "A-｜準主推薦小量試單"} or (bucket == "盤中雷達追蹤" and radar.startswith("R1"))
        if not is_actionable:
            continue
        row = dict(raw)
        row["推薦日期"] = snapshot_date
        row["推薦時間"] = _safe_str(row.get("推薦時間")) or saved_time
        row["推薦模式"] = _safe_str(row.get("推薦模式")) or "股神推薦"
        row["紀錄來源"] = "08_股神推薦紀錄｜自動修復07最新快照"
        row["自動記錄"] = "是"
        if bucket == "正式下週主推薦":
            row["紀錄層級"] = "正式主推薦"
        elif bucket == "A-｜準主推薦小量試單":
            row["紀錄層級"] = "A-準主推薦"
        else:
            row["紀錄層級"] = radar or "R1核心雷達"
        row["建立時間"] = _safe_str(row.get("建立時間")) or saved_at or _now_text()
        row["更新時間"] = _now_text()
        actionable.append(row)
    if not actionable:
        details.append(f"快照日期 {snapshot_date} 較新，但沒有正式/A-/R1可修復紀錄。")
        return False, details

    report, stats = upsert_records_authority_fast(actionable, reason="08 自動修復較新07推薦快照")
    details.extend(report.messages())
    if report.permanent_ok and int(stats.get("changed", 0) or 0) > 0:
        details.append(f"已自動補入權威檔：{snapshot_date}｜新增 {stats.get('added', 0)}｜更新 {stats.get('updated', 0)}。")
        return True, details
    if not report.permanent_ok:
        details.append("較新推薦快照存在，但自動修復權威檔失敗。")
    return False, details


def _sync_authority_before_actions(force_remote_summary: bool = False) -> tuple[pd.DataFrame, list[str], bool]:
    """Before any button callback, make page 8 use the newest authority revision."""
    details: list[str] = []
    restored = False
    now_ts = time.time()
    last_remote_check = float(st.session_state.get(_k("authority_remote_check_ts_v174"), 0.0) or 0.0)
    remote_due = bool(force_remote_summary or last_remote_check <= 0 or now_ts - last_remote_check >= 60.0)

    if remote_due and callable(ensure_records_local_authority_current):
        try:
            rows, remote_details, remote_restored = ensure_records_local_authority_current()
            details.extend([_safe_str(x) for x in (remote_details or []) if _safe_str(x)])
            st.session_state[_k("authority_remote_check_ts_v174")] = now_ts
            if remote_restored:
                restored_df = _ensure_godpick_record_columns(pd.DataFrame(rows))
                _save_state_df(restored_df)
                _remove_normalized_record_cache_v165()
                restored = True
        except Exception as exc:
            details.append(f"權威遠端摘要檢查失敗：{exc}")
            st.session_state[_k("authority_remote_check_ts_v174")] = now_ts

    current_sig = _records_local_signature()
    last_sig = _safe_str(st.session_state.get(_k("records_source_sig"), ""))
    current_df = _get_state_df()
    if current_df.empty or not last_sig or (current_sig and current_sig != last_sig):
        current_df = _load_records(force_remote=False)
        _save_state_df(current_df)
        st.session_state[_k("records_source_sig")] = current_sig
        restored = True
        details.append(f"第8頁已在按鈕執行前自動載入最新權威紀錄：{len(current_df)}筆。")

    reconciled, reconcile_details = _reconcile_latest_snapshot_into_authority_v174(current_df)
    details.extend(reconcile_details)
    if reconciled:
        _remove_normalized_record_cache_v165()
        current_sig = _records_local_signature()
        current_df = _load_records(force_remote=False)
        _save_state_df(current_df)
        st.session_state[_k("records_source_sig")] = current_sig
        restored = True

    return current_df, details, restored




def _unique_existing_cols(df: pd.DataFrame, cols: list[str]) -> list[str]:
    """Return existing columns only once, preserving order.

    Streamlit/pyarrow will raise ValueError when the dataframe passed to
    st.dataframe contains duplicate column names. Some legacy column lists in
    this page intentionally include the same field more than once after v60~v68
    feature merges, so every display slice must be de-duplicated before render.
    """
    out: list[str] = []
    seen: set[str] = set()
    if df is None or not isinstance(df, pd.DataFrame):
        return out
    for c in cols:
        if c in df.columns and c not in seen:
            out.append(c)
            seen.add(c)
    return out


def _is_empty_display_value(v: Any) -> bool:
    """v72：判斷畫面用空值，避免 None / nan / NaT 被直接顯示。"""
    if v is None:
        return True
    try:
        if pd.isna(v):
            return True
    except Exception:
        pass
    text = str(v).strip()
    return text == "" or text.lower() in {"none", "nan", "nat", "null", "<na>"}


def _clean_none_for_display(df: pd.DataFrame, *, drop_empty_cols: bool = False, keep_cols: list[str] | None = None) -> pd.DataFrame:
    """v72：清理表格畫面的 None，必要時隱藏整欄都沒有資料的欄位。

    注意：這只處理畫面顯示用 dataframe，不會刪除原始推薦紀錄資料。
    """
    if df is None or not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    out = df.loc[:, ~pd.Index(df.columns).duplicated()].copy()
    keep_set = set(keep_cols or [])

    # 先把字串型 None 清掉；object 欄位的 None 也清掉。
    for c in list(out.columns):
        try:
            if out[c].dtype == "object":
                out[c] = out[c].map(lambda x: "" if _is_empty_display_value(x) else x)
        except Exception:
            pass

    if drop_empty_cols and not out.empty:
        drop_cols: list[str] = []
        for c in list(out.columns):
            if c in keep_set:
                continue
            try:
                if out[c].map(_is_empty_display_value).all():
                    drop_cols.append(c)
            except Exception:
                pass
        if drop_cols:
            out = out.drop(columns=drop_cols, errors="ignore")
    return out




def _is_blank_value(v: Any) -> bool:
    """判斷空白值，避免分群表遇到 None/NaN/空字串時 NameError 或顯示異常。"""
    try:
        if v is None:
            return True
        if pd.isna(v):
            return True
    except Exception:
        pass
    try:
        text = str(v).strip()
    except Exception:
        return True
    return text == "" or text.lower() in {"none", "nan", "null", "nat", "—", "-"}


def _safe_display_df(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicated dataframe columns before Streamlit Arrow conversion and clean None text."""
    return _clean_none_for_display(df, drop_empty_cols=True)


def _format_df(df: pd.DataFrame) -> pd.DataFrame:
    show = df.copy()
    pct_cols = ["實際報酬%", "損益幅%", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "觸發後收盤績效%", "觸發當日收盤績效%", "觸發當日最高報酬%", "觸發當日最大回撤%", "觸發當日收盤保留率%", "觸發收盤確認層級", "隔日候選漲跌%", "可執行交易1日%", "可執行交易3日%", "可執行交易5日%", "可執行交易10日%", "可執行交易20日%", "可執行交易最大漲幅%", "可執行交易最大回撤%", "3日績效%", "5日績效%", "10日績效%", "20日績效%"]
    num_cols = [
        "推薦總分", "上漲機率估計%", "族群資金流分數", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "技術結構分數", "起漲前兆分數", "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明",  "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", "同類股領先幅度",
        "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2", "實際買進價", "實際賣出價", "最新價", "損益金額", "持有天數", "執行基準價", "觸發訊號品質分", "還原價格調整係數",
    ]
    for c in pct_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: f"{x:,.2f}%" if pd.notna(x) else "")
    for c in num_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: format_number(x, 2) if pd.notna(x) else "")
    for c in ["是否已實際買進", "是否達停損", "是否達目標1", "是否達目標2", "是否達標_回測", "是否停損_回測"]:
        if c in show.columns:
            show[c] = show[c].map(lambda v: "是" if _normalize_bool(v) else "否")
    return show


def _df_signature(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "empty"
    base_cols = [c for c in ["record_id", "更新時間", "最新更新時間", "最新價", "損益幅%", "實際報酬%", "20日績效%", "推薦後20日%", "推薦後最大漲幅%", "命中結果", "追蹤更新時間"] if c in df.columns]
    if not base_cols:
        base_cols = list(df.columns[:8])
    try:
        sig_src = df[base_cols].fillna("").astype(str)
        return hashlib.md5(sig_src.to_csv(index=False).encode("utf-8")).hexdigest()
    except Exception:
        return hashlib.md5(str(df.shape).encode("utf-8")).hexdigest()


def _get_default_col_profile(mode: str) -> list[str]:
    return DEFAULT_ADVANCED_COLS.copy() if mode == "進階" else DEFAULT_STANDARD_COLS.copy()


def _get_profile_key(mode: str) -> str:
    return _k(f"col_profile_{mode}")


def _dedupe_cols(cols: list[str], available_cols: list[str]) -> list[str]:
    final = []
    seen = set()
    for c in cols or []:
        c = _safe_str(c)
        if c in available_cols and c not in seen:
            final.append(c)
            seen.add(c)
    return final


def _get_saved_col_profile(mode: str, available_cols: list[str]) -> list[str]:
    """
    重要修正：
    只要使用者已經按「套用設定並永久記錄」，就完全依照已套用順序顯示。
    不再把 DEFAULT_STANDARD_COLS / DEFAULT_ADVANCED_COLS 插回中間，
    避免「推薦日期」等欄位下次又跑回原始位置。
    """
    profile_key = _get_profile_key(mode)
    has_saved = profile_key in st.session_state and isinstance(st.session_state.get(profile_key), list) and len(st.session_state.get(profile_key)) > 0

    if has_saved:
        saved = _dedupe_cols(st.session_state.get(profile_key), available_cols)
        extra = [c for c in available_cols if c not in saved]
        return saved + extra

    default_cols = [c for c in _get_default_col_profile(mode) if c in available_cols]
    extra = [c for c in available_cols if c not in default_cols]
    return default_cols + extra


def _save_col_profile(mode: str, cols: list[str]):
    # 保存完整順序；後續只補新欄位到最後，不會重套預設順序。
    available = list(dict.fromkeys(cols or []))
    st.session_state[_get_profile_key(mode)] = available.copy()
    st.session_state[_k("last_col_profile_save")] = _now_text()
    ok, msg = _persist_ui_config()
    if ok:
        _set_status(f"欄位順序已保存：{mode}", "success")
    else:
        _set_status(f"欄位順序已更新本機狀態，但 GitHub 保存失敗：{msg}", "warning")


def _reset_col_profile(mode: str, available_cols: list[str]):
    default_cols = [c for c in _get_default_col_profile(mode) if c in available_cols]
    extra = [c for c in available_cols if c not in default_cols]
    _save_col_profile(mode, default_cols + extra)


def _stage_col_profile(mode: str, cols: list[str], available_cols: list[str]):
    st.session_state[_k(f"staged_col_profile_{mode}")] = _dedupe_cols(cols, available_cols) + [
        c for c in available_cols if c not in _dedupe_cols(cols, available_cols)
    ]


def _get_stage_col_profile(mode: str, applied_cols: list[str], available_cols: list[str]) -> list[str]:
    staged = st.session_state.get(_k(f"staged_col_profile_{mode}"))
    if isinstance(staged, list) and staged:
        staged = _dedupe_cols(staged, available_cols)
        return staged + [c for c in available_cols if c not in staged]
    return applied_cols.copy()


def _restore_original_col_profile_to_stage(mode: str, available_cols: list[str]):
    default_cols = [c for c in _get_default_col_profile(mode) if c in available_cols]
    extra = [c for c in available_cols if c not in default_cols]
    st.session_state[_k(f"staged_col_profile_{mode}")] = default_cols + extra


def _move_col(cols: list[str], col_name: str, direction: str) -> list[str]:
    x = cols.copy()
    if col_name not in x:
        return x
    idx = x.index(col_name)
    if direction == "up" and idx > 0:
        x[idx], x[idx - 1] = x[idx - 1], x[idx]
    elif direction == "down" and idx < len(x) - 1:
        x[idx], x[idx + 1] = x[idx + 1], x[idx]
    elif direction == "top" and idx > 0:
        x.insert(0, x.pop(idx))
    elif direction == "bottom" and idx < len(x) - 1:
        x.append(x.pop(idx))
    return x


def _build_filtered_view_df(
    df: pd.DataFrame,
    keyword: str,
    mode_filter: str,
    category_filter: str,
    status_filter: str,
    bought_filter: str,
    sort_by: str,
    sort_asc: bool,
) -> pd.DataFrame:
    view_df = df.copy(deep=False)

    if keyword:
        mask = (
            view_df["股票代號"].astype(str).str.contains(keyword, case=False, na=False)
            | view_df["股票名稱"].astype(str).str.contains(keyword, case=False, na=False)
            | view_df["推薦理由摘要"].astype(str).str.contains(keyword, case=False, na=False)
        )
        view_df = view_df.loc[mask]

    if mode_filter != "全部":
        view_df = view_df.loc[view_df["推薦模式"].astype(str) == mode_filter]

    if category_filter != "全部":
        view_df = view_df.loc[view_df["類別"].astype(str) == category_filter]

    if status_filter != "全部":
        view_df = view_df.loc[view_df["目前狀態"].astype(str) == status_filter]

    if bought_filter != "全部":
        target_bool = bought_filter == "是"
        view_df = view_df.loc[view_df["是否已實際買進"].fillna(False).map(_normalize_bool) == target_bool]

    if sort_by in view_df.columns:
        view_df = view_df.sort_values(sort_by, ascending=sort_asc, na_position="last")

    return view_df.reset_index(drop=True)


def _analysis_effective_records_v178(df: pd.DataFrame) -> pd.DataFrame:
    """One statistical sample per authority business event; raw rows remain visible/auditable."""
    x = _ensure_godpick_record_columns(df.copy())
    if x.empty:
        return x
    codes = x.get("股票代號", pd.Series([""] * len(x), index=x.index)).fillna("").astype(str).map(_normalize_code)
    dates = x.get("推薦日期", pd.Series([""] * len(x), index=x.index)).fillna("").astype(str).str[:10]
    modes = x.get("推薦模式", pd.Series([""] * len(x), index=x.index)).fillna("").astype(str).str.strip()
    rids = x.get("record_id", pd.Series([""] * len(x), index=x.index)).fillna("").astype(str)
    x["_v178_business_key"] = [f"{c}|{d}|{m}" if c and d else f"RID|{rid}" for c,d,m,rid in zip(codes,dates,modes,rids)]
    # 最新一筆代表該日該模式的最終狀態；不修改原始紀錄，只在統計視圖去重。
    rec_time = x.get("推薦時間", pd.Series([""] * len(x), index=x.index)).fillna("").astype(str)
    upd_time = x.get("更新時間", pd.Series([""] * len(x), index=x.index)).fillna("").astype(str)
    x["_v178_sort"] = dates + " " + rec_time + "|" + upd_time
    x = x.sort_values("_v178_sort", kind="stable").drop_duplicates("_v178_business_key", keep="last")
    return x.drop(columns=["_v178_business_key", "_v178_sort"], errors="ignore").reset_index(drop=True)


def _get_analysis_cache(df: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], dict[str, Any], float | None, float | None]:
    sig = _df_signature(df)
    cache_key = _k("analysis_cache")
    cache = st.session_state.get(cache_key, {})

    if cache.get("sig") == sig:
        return cache["ana_tables"], cache["summary"], cache["avg_20"], cache["avg_real"]

    effective_df = _analysis_effective_records_v178(df)
    ana_tables = _build_analysis_tables(effective_df)
    summary = _build_summary(effective_df)
    avg_20 = pd.to_numeric(effective_df.get("推薦後20日%", effective_df.get("20日績效%")), errors="coerce").dropna().mean() if not effective_df.empty else None
    avg_real = pd.to_numeric(effective_df.loc[effective_df["是否已實際買進"] == True, "實際報酬%"], errors="coerce").dropna().mean() if not effective_df.empty else None

    st.session_state[cache_key] = {
        "sig": sig,
        "ana_tables": ana_tables,
        "summary": summary,
        "avg_20": avg_20,
        "avg_real": avg_real,
    }
    return ana_tables, summary, avg_20, avg_real


def _invalidate_analysis_cache():
    st.session_state.pop(_k("analysis_cache"), None)


def _get_editor_df(view_df: pd.DataFrame, use_cols: list[str], fast_mode: bool, visible_limit: int) -> tuple[pd.DataFrame, int, bool]:
    safe_cols = []
    seen = set()

    # record_id 是刪除 / 編輯 / 同步的必要識別欄。
    # 即使使用者欄位設定把它移除，也要保留在 editor_df 裡，畫面再用 column_config 隱藏。
    if "record_id" in view_df.columns:
        safe_cols.append("record_id")
        seen.add("record_id")

    for c in use_cols or []:
        if c in view_df.columns and c not in seen and c not in ["匯入自選", "刪除"]:
            safe_cols.append(c)
            seen.add(c)

    # V164：先截列、後選欄；舊版先複製 1,796 x 130~150 欄後才 head(500)，
    # 每次按鈕 rerun 都製造大型暫存 DataFrame。
    total_rows = len(view_df)
    truncated = bool(fast_mode and total_rows > visible_limit)
    row_source = view_df.head(visible_limit) if truncated else view_df
    src = row_source.loc[:, safe_cols].copy()
    # Streamlit data_editor 不允許重複欄位名稱；這裡再保險清除一次。
    src = src.loc[:, ~src.columns.duplicated()].copy()

    # v73：主表畫面不要直接露出 None；修正文字訊息欄被誤轉數值，並自動回補常用說明。
    # 只影響畫面 editor_df，不會刪除 live_df / JSON 內的原始欄位。
    must_keep_cols = [
        "record_id", "股票代號", "股票名稱", "推薦日期", "推薦時間", "推薦模式", "推薦等級",
        "推薦總分", "狀態", "是否已實際買進", "實際買進價", "實際賣出價", "最新價",
    ]
    src = _clean_none_for_display(src, drop_empty_cols=True, keep_cols=must_keep_cols)

    if "匯入自選" not in src.columns:
        src.insert(0, "匯入自選", False)
    if "刪除" not in src.columns:
        src.insert(1, "刪除", False)
    return src, total_rows, truncated


def _record_selection_key(checkbox_col: str) -> str:
    """Stable selection storage independent of data_editor key/nonces."""
    return _k(f"record_selection_{checkbox_col}_ids")


def _selection_ids(checkbox_col: str) -> list[str]:
    values = st.session_state.get(_record_selection_key(checkbox_col), []) or []
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        rid = _safe_str(value)
        if rid and rid not in seen:
            seen.add(rid)
            out.append(rid)
    return out


def _set_selection_ids(checkbox_col: str, values: list[str] | None, visible_ids: list[str] | None = None) -> list[str]:
    visible = {_safe_str(x) for x in (visible_ids or []) if _safe_str(x)} if visible_ids is not None else None
    out: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        rid = _safe_str(value)
        if not rid or rid in seen:
            continue
        if visible is not None and rid not in visible:
            continue
        seen.add(rid)
        out.append(rid)
    st.session_state[_record_selection_key(checkbox_col)] = out
    return out


def _apply_sticky_editor_checkboxes(editor_key: str, edited_df: pd.DataFrame, id_col: str = "record_id", checkbox_cols: list[str] | None = None) -> pd.DataFrame:
    """Apply stable checkbox selections by record_id before rendering.

    Previous versions keyed selections by the changing data_editor nonce and
    parsed ``edited_rows`` by row index.  Sorting/filtering/reruns could map the
    click to another row or make it disappear.  Selection is now stored only by
    the immutable record_id and survives a normal rerun.
    """
    if checkbox_cols is None:
        checkbox_cols = ["匯入自選", "刪除"]
    if edited_df is None or edited_df.empty or id_col not in edited_df.columns:
        return edited_df
    out = edited_df.copy()
    visible_ids = [_safe_str(x) for x in out[id_col].astype(str).tolist() if _safe_str(x)]
    visible_set = set(visible_ids)
    for col in checkbox_cols:
        if col not in out.columns:
            continue
        # Migrate legacy per-editor sticky state once, then use the stable key.
        legacy_key = _k(f"sticky_{editor_key}_{col}_ids")
        merged = _selection_ids(col) + [
            _safe_str(x) for x in st.session_state.get(legacy_key, []) or [] if _safe_str(x)
        ]
        selected = _set_selection_ids(col, merged, visible_ids=visible_ids)
        selected_set = set(selected)
        out[col] = out[id_col].astype(str).map(lambda x: _safe_str(x) in selected_set)
        st.session_state.pop(legacy_key, None)
    return out


def _sync_editor_selections_from_returned(
    edited_df: pd.DataFrame,
    id_col: str = "record_id",
    checkbox_cols: list[str] | None = None,
) -> None:
    """Persist the data_editor returned checkbox values by record_id.

    No row-index callback is used.  This remains correct after client-side sort,
    filter, column rearrangement and Streamlit reruns.
    """
    if checkbox_cols is None:
        checkbox_cols = ["匯入自選", "刪除"]
    if edited_df is None or edited_df.empty or id_col not in edited_df.columns:
        for col in checkbox_cols:
            _set_selection_ids(col, [])
        return
    visible_ids = [_safe_str(x) for x in edited_df[id_col].astype(str).tolist() if _safe_str(x)]
    for col in checkbox_cols:
        if col not in edited_df.columns:
            continue
        try:
            mask = edited_df[col].fillna(False).map(_normalize_bool)
            checked = [_safe_str(x) for x in edited_df.loc[mask, id_col].astype(str).tolist() if _safe_str(x)]
        except Exception:
            checked = []
        _set_selection_ids(col, checked, visible_ids=visible_ids)


def _record_editor_nonce_key(show_cols_mode: str) -> str:
    return _k(f"record_editor_nonce_{show_cols_mode}")


def _record_editor_key_for_mode(show_cols_mode: str) -> str:
    nonce_key = _record_editor_nonce_key(show_cols_mode)
    try:
        nonce = int(st.session_state.get(nonce_key, 0) or 0)
    except Exception:
        nonce = 0
    return _k(f"record_editor_{show_cols_mode}_{nonce}")


def _reset_record_editor_for_bulk_delete(
    show_cols_mode: str,
    delete_ids: list[str] | None = None,
    import_ids: list[str] | None = None,
) -> str:
    """Recreate the editor and seed stable selections for the next render."""
    _set_selection_ids("刪除", delete_ids or [])
    _set_selection_ids("匯入自選", import_ids or [])
    nonce_key = _record_editor_nonce_key(show_cols_mode)
    try:
        next_nonce = int(st.session_state.get(nonce_key, 0) or 0) + 1
    except Exception:
        next_nonce = 1
    st.session_state[nonce_key] = next_nonce
    return _k(f"record_editor_{show_cols_mode}_{next_nonce}")


def _collect_editor_selected_ids(
    editor_key: str,
    edited_df: pd.DataFrame,
    id_col: str = "record_id",
    checkbox_col: str = "刪除",
) -> list[str]:
    """Collect selected IDs from returned data and stable record-id state."""
    ids: list[str] = []
    seen: set[str] = set()

    def _add(value: Any) -> None:
        rid = _safe_str(value)
        if rid and rid not in seen:
            seen.add(rid)
            ids.append(rid)

    for rid in _selection_ids(checkbox_col):
        _add(rid)
    try:
        if edited_df is not None and not edited_df.empty and id_col in edited_df.columns and checkbox_col in edited_df.columns:
            mask = edited_df[checkbox_col].fillna(False).map(_normalize_bool)
            for rid in edited_df.loc[mask, id_col].astype(str).tolist():
                _add(rid)
    except Exception:
        pass
    return ids


def _remove_ids_from_list(values: list[str] | None, remove_ids: set[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        rid = _safe_str(value)
        if not rid or rid in remove_ids or rid in seen:
            continue
        seen.add(rid)
        out.append(rid)
    return out


def _build_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df is None or df.empty:
        return {"count": 0, "buy_count": 0, "sold_count": 0, "avg_ret": 0, "win_rate": 0}
    ret_series = pd.to_numeric(df["實際報酬%"], errors="coerce")
    pnl_series = pd.to_numeric(df["損益幅%"], errors="coerce")
    used_ret = ret_series.fillna(pnl_series)
    valid = used_ret.dropna()
    buy_count = int(df["是否已實際買進"].fillna(False).map(_normalize_bool).sum())
    sold_count = int(df["目前狀態"].isin(["已賣出", "停損", "達標"]).sum())
    win_rate = float((valid > 0).mean() * 100) if not valid.empty else 0.0
    avg_ret = float(valid.mean()) if not valid.empty else 0.0
    return {"count": int(len(df)), "buy_count": buy_count, "sold_count": sold_count, "avg_ret": avg_ret, "win_rate": win_rate}


def _win_rate(series) -> float:
    s = pd.to_numeric(pd.Series(series), errors="coerce").dropna()
    return float((s > 0).mean() * 100) if len(s) else 0.0


def _build_analysis_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    local_df = _analysis_effective_records_v178(df.copy())
    if local_df.empty:
        return {
            "mode": pd.DataFrame(columns=["推薦模式", "筆數", "平均系統報酬", "系統勝率", "平均3日績效", "平均5日績效", "平均10日績效", "平均20日績效", "3日勝率", "5日勝率", "10日勝率", "20日勝率", "達目標1比率", "停損率", "平均推薦總分"]),
            "category": pd.DataFrame(columns=["類別", "筆數", "平均系統報酬", "平均3日績效", "平均5日績效", "平均10日績效", "平均20日績效", "3日勝率", "5日勝率", "10日勝率", "20日勝率", "系統勝率", "達目標1比率", "停損率"]),
            "grade": pd.DataFrame(columns=["推薦等級", "筆數", "平均系統報酬", "系統勝率", "達目標1比率", "停損率"]),
            "trade_mode": pd.DataFrame(columns=["推薦模式", "筆數", "平均實際報酬", "實際勝率"]),
            "best_mode": pd.DataFrame(),
            "best_category": pd.DataFrame(),
        }

    x = local_df.copy()
    x["系統報酬基準"] = pd.to_numeric(x["損益幅%"], errors="coerce")
    x["實際交易基準"] = pd.to_numeric(x["實際報酬%"], errors="coerce")

    mode_df = x.groupby("推薦模式", dropna=False).agg(
        筆數=("record_id", "count"),
        平均系統報酬=("系統報酬基準", "mean"),
        系統勝率=("系統報酬基準", _win_rate),
        平均3日績效=("3日績效%", "mean"),
        平均5日績效=("5日績效%", "mean"),
        平均10日績效=("10日績效%", "mean"),
        平均20日績效=("20日績效%", "mean"),
        **{
            "3日勝率": ("3日績效%", _win_rate),
            "5日勝率": ("5日績效%", _win_rate),
            "10日勝率": ("10日績效%", _win_rate),
            "20日勝率": ("20日績效%", _win_rate),
        },
        達目標1比率=("是否達目標1", lambda s: float(pd.Series(s).fillna(False).map(_normalize_bool).mean() * 100) if len(pd.Series(s)) else 0.0),
        停損率=("是否達停損", lambda s: float(pd.Series(s).fillna(False).map(_normalize_bool).mean() * 100) if len(pd.Series(s)) else 0.0),
        平均推薦總分=("推薦總分", "mean"),
    ).reset_index()

    category_df = x.groupby("類別", dropna=False).agg(
        筆數=("record_id", "count"),
        平均系統報酬=("系統報酬基準", "mean"),
        平均3日績效=("3日績效%", "mean"),
        平均5日績效=("5日績效%", "mean"),
        平均10日績效=("10日績效%", "mean"),
        平均20日績效=("20日績效%", "mean"),
        **{
            "3日勝率": ("3日績效%", _win_rate),
            "5日勝率": ("5日績效%", _win_rate),
            "10日勝率": ("10日績效%", _win_rate),
            "20日勝率": ("20日績效%", _win_rate),
        },
        系統勝率=("系統報酬基準", _win_rate),
        達目標1比率=("是否達目標1", lambda s: float(pd.Series(s).fillna(False).map(_normalize_bool).mean() * 100) if len(pd.Series(s)) else 0.0),
        停損率=("是否達停損", lambda s: float(pd.Series(s).fillna(False).map(_normalize_bool).mean() * 100) if len(pd.Series(s)) else 0.0),
    ).reset_index()

    grade_df = x.groupby("推薦等級", dropna=False).agg(
        筆數=("record_id", "count"),
        平均系統報酬=("系統報酬基準", "mean"),
        系統勝率=("系統報酬基準", _win_rate),
        達目標1比率=("是否達目標1", lambda s: float(pd.Series(s).fillna(False).map(_normalize_bool).mean() * 100) if len(pd.Series(s)) else 0.0),
        停損率=("是否達停損", lambda s: float(pd.Series(s).fillna(False).map(_normalize_bool).mean() * 100) if len(pd.Series(s)) else 0.0),
    ).reset_index()

    trade_df = x[x["是否已實際買進"].fillna(False).map(_normalize_bool)].copy()
    if trade_df.empty:
        trade_mode_df = pd.DataFrame(columns=["推薦模式", "筆數", "平均實際報酬", "實際勝率"])
    else:
        trade_mode_df = trade_df.groupby("推薦模式", dropna=False).agg(
            筆數=("record_id", "count"),
            平均實際報酬=("實際交易基準", "mean"),
            實際勝率=("實際交易基準", _win_rate),
        ).reset_index()

    best_mode_df = mode_df.copy()
    if not best_mode_df.empty:
        best_mode_df["綜合模式分數"] = (
            best_mode_df["平均20日績效"].fillna(0) * 0.50
            + best_mode_df["20日勝率"].fillna(0) * 0.35
            + best_mode_df["平均推薦總分"].fillna(0) * 0.15
        )
        best_mode_df = best_mode_df.sort_values(["綜合模式分數", "平均20日績效", "20日勝率"], ascending=[False, False, False]).reset_index(drop=True)

    best_category_df = category_df.copy()
    if not best_category_df.empty:
        best_category_df["綜合類別分數"] = (
            best_category_df["平均20日績效"].fillna(0) * 0.55
            + best_category_df["20日勝率"].fillna(0) * 0.35
            + best_category_df["系統勝率"].fillna(0) * 0.10
        )
        best_category_df = best_category_df.sort_values(["綜合類別分數", "平均20日績效", "20日勝率"], ascending=[False, False, False]).reset_index(drop=True)

    return {"mode": mode_df, "category": category_df, "grade": grade_df, "trade_mode": trade_mode_df, "best_mode": best_mode_df, "best_category": best_category_df}


def _build_mode_performance_label(row: pd.Series | dict[str, Any], mode_stats_df: pd.DataFrame) -> str:
    src = dict(row)
    mode = _safe_str(src.get("推薦模式"))
    if mode_stats_df is None or mode_stats_df.empty or not mode:
        return _safe_str(src.get("模式績效標籤"))
    hit = mode_stats_df[mode_stats_df["推薦模式"].astype(str) == mode]
    if hit.empty:
        return _safe_str(src.get("模式績效標籤"))
    r = hit.iloc[0]
    avg_20 = _safe_float(r.get("平均20日績效"))
    win20 = _safe_float(r.get("20日勝率"))
    sample_n = int(_safe_float(r.get("筆數"), 0) or 0)
    if sample_n < 3:
        return "樣本不足"
    if avg_20 is not None and win20 is not None:
        if avg_20 >= 8 and win20 >= 65:
            return "強勢模式"
        if avg_20 >= 3 and win20 >= 55:
            return "穩健模式"
        if avg_20 < 0 and win20 < 45:
            return "偏弱模式"
        return "一般模式"
    return _safe_str(src.get("模式績效標籤"))


def _apply_mode_labels(df: pd.DataFrame) -> pd.DataFrame:
    x = _ensure_godpick_record_columns(df.copy())
    ana = _build_analysis_tables(x)
    x["模式績效標籤"] = x.apply(lambda r: _build_mode_performance_label(r, ana["mode"]), axis=1)
    return _ensure_godpick_record_columns(x)


def _v15_perf_series(df: pd.DataFrame) -> pd.Series:
    """V178：逐列選擇已成熟的最長可用績效，避免只因20日欄少量有值就丟掉其餘樣本。"""
    out = pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
    for col in ["推薦後20日%", "20日績效%", "推薦後10日%", "10日績效%", "推薦後5日%", "5日績效%", "推薦後3日%", "3日績效%", "推薦後1日%", "系統追蹤報酬%", "損益幅%"]:
        if col not in df.columns:
            continue
        s2 = pd.to_numeric(df[col], errors="coerce")
        fill_mask = out.isna() & s2.notna()
        out.loc[fill_mask] = s2.loc[fill_mask]
    return out


def _build_v15_auto_tune_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """V15：用推薦後績效反推哪些模式、買點、型態應提高或降低權重。"""
    x = _ensure_godpick_record_columns(df.copy())
    if x.empty:
        empty = pd.DataFrame(columns=["項目", "樣本數", "平均績效%", "勝率%", "平均最大回撤%", "校正分數", "建議"])
        return {"mode": empty.copy(), "type": empty.copy(), "entry": empty.copy(), "risk": empty.copy(), "sector": empty.copy(), "summary": empty.copy()}

    perf = _v15_perf_series(x)
    x["__v15_perf"] = perf
    x["__v15_win"] = perf > 0
    if "推薦後最大回撤%" in x.columns:
        x["__v15_dd"] = pd.to_numeric(x["推薦後最大回撤%"], errors="coerce")
    else:
        x["__v15_dd"] = pd.Series([float("nan")] * len(x), index=x.index, dtype="float64")

    def one_table(group_col: str, label: str) -> pd.DataFrame:
        if group_col not in x.columns:
            return pd.DataFrame(columns=[label, "樣本數", "平均績效%", "勝率%", "平均最大回撤%", "校正分數", "建議", "校正原因"])
        rows = []
        work = x.copy()
        work[group_col] = work[group_col].fillna("").astype(str).replace("", "未分類")
        for key, g in work.groupby(group_col, dropna=False):
            v = pd.to_numeric(g["__v15_perf"], errors="coerce").dropna()
            n = int(len(v))
            if n <= 0:
                continue
            avg = float(v.mean())
            win = float((v > 0).mean() * 100)
            dd = pd.to_numeric(g["__v15_dd"], errors="coerce").dropna()
            avg_dd = float(dd.mean()) if not dd.empty else float("nan")
            dd_penalty = min(abs(avg_dd), 12) if avg_dd == avg_dd else 3.0
            sample_bonus = min(n / 20 * 8, 8)
            tune_score = max(0.0, min(100.0, 50 + avg * 3.0 + (win - 50) * 0.45 - dd_penalty * 1.2 + sample_bonus))
            if n < 3:
                suggestion = "樣本不足，暫不調權"
                reason = "樣本少於3筆，先累積紀錄，避免過度擬合。"
            elif tune_score >= 68 and avg > 0 and win >= 55:
                suggestion = "建議提高權重"
                reason = "平均績效與勝率同時偏強，可提高此類訊號排序權重。"
            elif tune_score <= 42 or (avg < 0 and win < 50):
                suggestion = "建議降低權重"
                reason = "回測績效偏弱或勝率不足，建議降低排序權重並檢查追高風險。"
            else:
                suggestion = "建議維持觀察"
                reason = "績效尚可但優勢不明顯，先維持現有權重。"
            rows.append({
                label: key,
                "樣本數": n,
                "平均績效%": round(avg, 2),
                "勝率%": round(win, 2),
                "平均最大回撤%": None if avg_dd != avg_dd else round(avg_dd, 2),
                "校正分數": round(tune_score, 2),
                "建議": suggestion,
                "校正原因": reason,
            })
        out = pd.DataFrame(rows)
        if out.empty:
            return pd.DataFrame(columns=[label, "樣本數", "平均績效%", "勝率%", "平均最大回撤%", "校正分數", "建議", "校正原因"])
        return out.sort_values(["校正分數", "樣本數"], ascending=[False, False]).reset_index(drop=True)

    mode_df = one_table("推薦模式", "推薦模式")
    type_df = one_table("推薦型態", "推薦型態")
    entry_df = one_table("進場時機", "進場時機")
    risk_df = one_table("追高風險等級", "追高風險等級")
    sector_df = one_table("類別", "類別")

    summary_rows = []
    for name, table, key_col in [
        ("推薦模式", mode_df, "推薦模式"),
        ("推薦型態", type_df, "推薦型態"),
        ("進場時機", entry_df, "進場時機"),
        ("追高風險", risk_df, "追高風險等級"),
        ("類別", sector_df, "類別"),
    ]:
        if not table.empty:
            top = table.iloc[0]
            weak = table.sort_values(["校正分數", "樣本數"], ascending=[True, False]).iloc[0]
            summary_rows.append({
                "校正面向": name,
                "最強項目": _safe_str(top.get(key_col)),
                "最強校正分數": _safe_float(top.get("校正分數"), 0),
                "最強建議": _safe_str(top.get("建議")),
                "偏弱項目": _safe_str(weak.get(key_col)),
                "偏弱校正分數": _safe_float(weak.get("校正分數"), 0),
                "偏弱建議": _safe_str(weak.get("建議")),
            })
    summary_df = pd.DataFrame(summary_rows)
    return {"mode": mode_df, "type": type_df, "entry": entry_df, "risk": risk_df, "sector": sector_df, "summary": summary_df}


def _render_v15_auto_tune_panel(df: pd.DataFrame):
    """V15：顯示自動權重校正建議；只提供決策參考，不直接改權重，避免誤傷推薦邏輯。"""
    render_pro_section("V15 權重回饋校正建議", "根據推薦後績效、勝率與最大回撤，判斷哪些推薦模式/型態應提高或降低權重；此區不自動改設定，避免過度擬合。")
    tables = _build_v15_auto_tune_tables(df)
    if tables["summary"].empty:
        st.info("目前回測樣本不足。請先在本頁按『更新推薦後績效』，並累積更多推薦紀錄。")
        return
    st.dataframe(_safe_display_df(tables["summary"]), use_container_width=True, hide_index=True)
    st.caption("判讀：校正分數越高，代表該模式/型態在目前紀錄中平均績效、勝率、回撤表現越好。樣本少於3筆不建議調權。")
    sub = st.tabs(["推薦模式", "推薦型態", "進場時機", "追高風險", "類別"])
    with sub[0]:
        st.dataframe(_safe_display_df(tables["mode"]), use_container_width=True, hide_index=True)
    with sub[1]:
        st.dataframe(_safe_display_df(tables["type"]), use_container_width=True, hide_index=True)
    with sub[2]:
        st.dataframe(_safe_display_df(tables["entry"]), use_container_width=True, hide_index=True)
    with sub[3]:
        st.dataframe(_safe_display_df(tables["risk"]), use_container_width=True, hide_index=True)
    with sub[4]:
        st.dataframe(_safe_display_df(tables["sector"]), use_container_width=True, hide_index=True)


def _build_export_bytes(df: pd.DataFrame, tables: dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        _ensure_godpick_record_columns(df).to_excel(writer, sheet_name="推薦紀錄", index=False)
        tables["mode"].to_excel(writer, sheet_name="模式分析", index=False)
        tables["category"].to_excel(writer, sheet_name="類別分析", index=False)
        tables["grade"].to_excel(writer, sheet_name="等級分析", index=False)
        tables["trade_mode"].to_excel(writer, sheet_name="實際交易分析", index=False)
        if not tables["best_mode"].empty:
            tables["best_mode"].to_excel(writer, sheet_name="最強模式", index=False)
        if not tables["best_category"].empty:
            tables["best_category"].to_excel(writer, sheet_name="最強類別", index=False)
        try:
            for ws in writer.book.worksheets:
                ws.freeze_panes = "A2"
                for col_cells in ws.columns:
                    max_len = 0
                    col_letter = col_cells[0].column_letter
                    for cell in col_cells:
                        cell_val = "" if cell.value is None else str(cell.value)
                        max_len = max(max_len, len(cell_val))
                    ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 42)
        except Exception:
            pass
    output.seek(0)
    return output.getvalue()


def _persist_export_action(excel_bytes: bytes, filename: str, record_count: int, source: str) -> dict[str, Any]:
    """Persist export metadata on every browser/server export action.

    The browser download itself is not a durable audit record.  This helper also
    writes a server-side copy when possible and always appends a remote-capable
    history row, including failed folder writes, so the user can see what happened
    after an APP reboot.
    """
    settings = dict(st.session_state.get(_k("export_sync_settings"), {}) or {})
    folder = _safe_str(settings.get("export_folder")) or "exports/godpick"
    file_ok = False
    file_msg = "永久匯出服務未載入"
    path = ""
    if callable(write_export_file):
        file_ok, file_msg, path = write_export_file(folder, filename, excel_bytes)

    event = {
        "file_name": filename,
        "path": path,
        "configured_folder": folder,
        "record_count": int(record_count),
        "source": source,
        "file_write_ok": bool(file_ok),
        "file_write_message": file_msg,
        "download_requested": True,
    }
    history_ok = False
    history_messages: list[str] = []
    if callable(append_export_history):
        try:
            report = append_export_history(event)
            history_ok = bool(report.permanent_ok)
            history_messages = report.messages()
        except Exception as exc:
            history_messages = [f"匯出紀錄寫入例外：{exc}"]
    else:
        history_messages = ["匯出紀錄永久服務未載入"]

    result = {
        "ok": bool(history_ok),
        "file_ok": bool(file_ok),
        "history_ok": bool(history_ok),
        "file_message": file_msg,
        "history_messages": history_messages,
        "path": path,
        "file_name": filename,
        "source": source,
    }
    st.session_state[_k("last_export_action")] = result
    st.session_state[_k("export_history_detail")] = history_messages
    return result


def _record_browser_export(excel_bytes: bytes, filename: str, record_count: int) -> None:
    result = _persist_export_action(excel_bytes, filename, record_count, "08瀏覽器下載")
    if result.get("history_ok"):
        level = "success" if result.get("file_ok") else "warning"
        msg = "Excel 下載已建立永久匯出紀錄。"
        if result.get("file_ok"):
            msg += f" 伺服器副本：{result.get('path')}"
        else:
            msg += f" 伺服器副本未完成：{result.get('file_message')}"
        _set_status(msg, level)
    else:
        _set_status("Excel 可下載，但匯出永久紀錄寫入失敗；請查看匯出明細。", "error")


# ============================================================
# V50：推薦後績效追蹤總控
# ============================================================
def _render_v50_performance_tracker(df: pd.DataFrame, title: str = "V68 推薦後績效追蹤總控") -> None:
    """V68：只用真正有績效數值的樣本計算 KPI；避免空白績效被顯示成 0%。"""
    if df is None or df.empty:
        st.info("V68：目前沒有資料可做推薦後績效追蹤。")
        return

    x = df.copy()
    x = x.loc[:, ~x.columns.duplicated()].copy()
    perf_base_cols = ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%"]
    perf_cols = [c for c in perf_base_cols if c in x.columns]
    if not perf_cols:
        st.info("V68：目前尚未產生推薦後績效欄位，請先按『更新推薦後績效』。")
        return

    def _num_col(col: str) -> pd.Series:
        if col not in x.columns:
            return pd.Series([float('nan')] * len(x), index=x.index)
        s = pd.to_numeric(x[col], errors="coerce")
        return s

    def _valid_mask_for(col: str) -> pd.Series:
        # 只要該週期有數值就算該週期有效樣本；空白不再被當 0。
        return _num_col(col).notna()

    any_perf_mask = pd.Series(False, index=x.index)
    for c in perf_cols:
        any_perf_mask = any_perf_mask | _valid_mask_for(c)

    def _avg(col: str):
        s = _num_col(col).dropna()
        return None if s.empty else float(s.mean())

    def _wr(col: str):
        s = _num_col(col).dropna()
        return None if s.empty else float((s > 0).mean() * 100)

    def _fmt_pct(v, digits=1):
        return "—" if v is None or pd.isna(v) else f"{v:.{digits}f}%"

    def _bool_rate(col: str) -> float | None:
        if col not in x.columns or len(x) == 0:
            return None
        base = x.loc[any_perf_mask, col] if any_perf_mask.any() else pd.Series([], dtype=object)
        if base.empty:
            return None
        def _b(v):
            if isinstance(v, bool):
                return v
            return str(v).strip().lower() in {"true", "1", "yes", "y", "是", "達標", "停損"}
        return float(base.map(_b).mean() * 100)

    with st.expander(title, expanded=True):
        kpi_payload = []
        for col in perf_base_cols:
            if col in x.columns:
                wr = _wr(col)
                avg = _avg(col)
                n = int(_valid_mask_for(col).sum())
                kpi_payload.append({
                    "label": f"{col.replace('%','')} 勝率",
                    "value": _fmt_pct(wr),
                    "delta": f"有效樣本 {n}｜平均 {_fmt_pct(avg, 2)}",
                    "delta_class": "pro-kpi-delta-flat",
                })
        if 'render_pro_kpi_row' in globals() and callable(globals().get('render_pro_kpi_row')):
            try:
                render_pro_kpi_row(kpi_payload[:6])
            except Exception:
                cols = st.columns(max(1, min(len(kpi_payload), 5)))
                for c, item in zip(cols, kpi_payload):
                    c.metric(item["label"], item["value"], item["delta"])
        else:
            cols = st.columns(max(1, min(len(kpi_payload), 5)))
            for c, item in zip(cols, kpi_payload):
                c.metric(item["label"], item["value"], item["delta"])

        valid_n = int(any_perf_mask.sum())
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("追蹤總筆數", int(len(x)))
        k2.metric("有效績效樣本", valid_n)
        target_rate = _bool_rate('是否達標_回測')
        if target_rate is None:
            target_rate = _bool_rate('是否達目標1')
        stop_rate = _bool_rate('是否停損_回測')
        if stop_rate is None:
            stop_rate = _bool_rate('是否達停損')
        k3.metric("達標率", _fmt_pct(target_rate))
        dd = _avg('推薦後最大回撤%') if '推薦後最大回撤%' in x.columns else None
        k4.metric("平均最大回撤", _fmt_pct(dd, 2))

        if valid_n == 0:
            st.warning("目前沒有真正可用的推薦後績效數值。請先更新推薦後績效；若曾出現 ONLINE_FAIL，請清除績效快取後重跑。")
        else:
            st.caption("V68：此區只用有實際績效數值的樣本計算，不再把空白績效偽裝成 0%。")

        def _group_table(group_col: str) -> pd.DataFrame:
            if group_col not in x.columns:
                return pd.DataFrame()
            rows = []
            for key, g in x.groupby(group_col, dropna=False):
                row = {group_col: "未分類" if _is_blank_value(key) else key, "總筆數": len(g)}
                g_any = pd.Series(False, index=g.index)
                for col in perf_cols:
                    g_any = g_any | pd.to_numeric(g[col], errors="coerce").notna()
                row["有效績效樣本"] = int(g_any.sum())
                for col in perf_base_cols:
                    if col in g.columns:
                        s = pd.to_numeric(g[col], errors="coerce").dropna()
                        row[f"平均{col}"] = round(float(s.mean()), 2) if not s.empty else ""
                        row[f"{col.replace('%','')}勝率"] = round(float((s > 0).mean() * 100), 1) if not s.empty else ""
                if "推薦後最大漲幅%" in g.columns:
                    s1 = pd.to_numeric(g["推薦後最大漲幅%"], errors="coerce").dropna()
                    row["平均最大漲幅%"] = round(float(s1.mean()), 2) if not s1.empty else ""
                if "推薦後最大回撤%" in g.columns:
                    s2 = pd.to_numeric(g["推薦後最大回撤%"], errors="coerce").dropna()
                    row["平均最大回撤%"] = round(float(s2.mean()), 2) if not s2.empty else ""
                rows.append(row)
            out = pd.DataFrame(rows)
            sort_col = "有效績效樣本" if "有效績效樣本" in out.columns else None
            if sort_col:
                out = out.sort_values(sort_col, ascending=False, na_position="last")
            return out

        tabs_v50 = st.tabs(["依推薦模式", "依推薦等級", "依類別", "依大盤風控", "弱勢檢討清單"])
        with tabs_v50[0]:
            try:
                _safe_dataframe(_group_table("推薦模式"), keep_cols=["推薦模式", "總筆數", "有效績效樣本"], use_container_width=True, hide_index=True)
            except Exception:
                st.dataframe(_group_table("推薦模式"), use_container_width=True, hide_index=True)
        with tabs_v50[1]:
            try:
                _safe_dataframe(_group_table("推薦等級"), keep_cols=["推薦等級", "總筆數", "有效績效樣本"], use_container_width=True, hide_index=True)
            except Exception:
                st.dataframe(_group_table("推薦等級"), use_container_width=True, hide_index=True)
        with tabs_v50[2]:
            try:
                _safe_dataframe(_group_table("類別"), keep_cols=["類別", "總筆數", "有效績效樣本"], use_container_width=True, hide_index=True)
            except Exception:
                st.dataframe(_group_table("類別"), use_container_width=True, hide_index=True)
        with tabs_v50[3]:
            mcol = "大盤橋接風控" if "大盤橋接風控" in x.columns else ("大盤橋接狀態" if "大盤橋接狀態" in x.columns else "大盤趨勢")
            if mcol in x.columns:
                try:
                    _safe_dataframe(_group_table(mcol), keep_cols=[mcol, "總筆數", "有效績效樣本"], use_container_width=True, hide_index=True)
                except Exception:
                    st.dataframe(_group_table(mcol), use_container_width=True, hide_index=True)
            else:
                st.info("尚無大盤風控欄位可分群。")
        with tabs_v50[4]:
            weak_col = "推薦後10日%" if "推薦後10日%" in x.columns else ("推薦後5日%" if "推薦後5日%" in x.columns else None)
            if weak_col and _valid_mask_for(weak_col).any():
                weak = x.copy()
                weak[weak_col] = pd.to_numeric(weak[weak_col], errors="coerce")
                weak = weak.dropna(subset=[weak_col]).sort_values(weak_col, ascending=True).head(30)
                candidate_cols = ["股票代號", "股票名稱", "類別", "推薦模式", "推薦等級", "推薦總分", weak_col, "推薦後最大回撤%", "命中結果", "績效評語", "推薦日期", "推薦理由摘要", "風險說明"]
                cols = [c for c in candidate_cols if c in weak.columns]
                try:
                    st.dataframe(_safe_display_df(weak[cols]), use_container_width=True, hide_index=True)
                except Exception:
                    st.dataframe(weak[cols], use_container_width=True, hide_index=True)
            else:
                st.info("尚無 5日/10日有效績效可列弱勢檢討清單。")



def _extract_recommendation_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(x) for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ["recommendations", "data", "rows", "items", "records", "股票清單"]:
            value = payload.get(key)
            if isinstance(value, list):
                return [dict(x) for x in value if isinstance(x, dict)]
    return []


def _latest_recommendation_rows(records_df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[str]]:
    details: list[str] = []
    rows: list[dict[str, Any]] = []
    if callable(load_named_json_permanent):
        try:
            payload, d1 = load_named_json_permanent("godpick_recommend_list.json", [])
            rows = _extract_recommendation_rows(payload)
            details.extend(d1)
        except Exception as exc:
            details.append(f"推薦清單讀取失敗：{exc}")
        if not rows:
            try:
                payload, d2 = load_named_json_permanent("godpick_latest_recommendations.json", {})
                rows = _extract_recommendation_rows(payload)
                details.extend(d2)
            except Exception as exc:
                details.append(f"最新推薦讀取失敗：{exc}")
    if not rows and isinstance(records_df, pd.DataFrame) and not records_df.empty:
        work = records_df.copy()
        date_s = pd.to_datetime(work.get("推薦日期"), errors="coerce")
        if date_s.notna().any():
            latest_date = date_s.max().date()
            work = work[date_s.dt.date == latest_date]
        rows = work.to_dict(orient="records")
        details.append(f"以推薦紀錄最新日期備援：{len(rows)} 筆")

    # 推薦清單可能混有數個日期；只同步其中最新的一輪，避免舊股票重新灌回 05/09/10。
    parsed_dates = []
    for row in rows:
        date_text = _safe_str(row.get("推薦日期"))
        if not date_text:
            date_text = _safe_str(row.get("建立時間"))[:10] or _safe_str(row.get("更新時間"))[:10]
        parsed_dates.append(pd.to_datetime(date_text, errors="coerce"))
    valid_dates = [x for x in parsed_dates if not pd.isna(x)]
    if valid_dates:
        max_date = max(x.date() for x in valid_dates)
        filtered = []
        for row, dt in zip(rows, parsed_dates):
            if not pd.isna(dt) and dt.date() == max_date:
                filtered.append(row)
        if filtered:
            rows = filtered
            details.append(f"只同步最新推薦日期 {max_date}：{len(rows)} 筆")

    out = []
    seen = set()
    for raw in rows:
        row = dict(raw)
        code = _normalize_code(_safe_str(row.get("股票代號")) or _safe_str(row.get("code")))
        if not code or code in seen:
            continue
        seen.add(code)
        row["股票代號"] = code
        if not _safe_str(row.get("股票名稱")):
            row["股票名稱"] = _safe_str(row.get("name")) or code
        if not _safe_str(row.get("市場別")):
            row["市場別"] = _safe_str(row.get("market")) or "上市"
        out.append(row)
    return out, details



def _run_one_click_sync_05_09_10(records_df: pd.DataFrame, settings: dict[str, Any], excel_bytes: bytes | None = None) -> dict[str, Any]:
    """08 真正執行 05 自選股、09 主檔、10 推薦清單與紀錄永久同步。"""
    started = _now_text()
    result: dict[str, Any] = {"started_at": started, "modules": {}, "overall_ok": False}
    rows, source_details = _latest_recommendation_rows(records_df)
    result["latest_rows"] = len(rows)
    result["source_details"] = source_details
    target_group = _safe_str(settings.get("target_group")) or "股神推薦"

    if callable(save_records_permanent):
        rec_report = save_records_permanent(records_df)
        result["modules"]["08推薦紀錄"] = {"ok": rec_report.permanent_ok, "details": rec_report.messages(), "count": len(records_df)}
    else:
        result["modules"]["08推薦紀錄"] = {"ok": False, "details": ["永久紀錄服務未載入"]}

    # 05 群組即使本輪沒有股票也要永久建立；有股票時只增不重複。
    if callable(load_watchlist_permanent) and callable(save_watchlist_permanent):
        watchlist, load_detail = load_watchlist_permanent()
        watchlist.setdefault(target_group, [])
        existing = {_normalize_code(x.get("code")) for x in watchlist[target_group] if isinstance(x, dict)}
        added = 0
        for row in rows:
            code = _normalize_code(row.get("股票代號"))
            if not code or code in existing:
                continue
            item = {"code": code, "name": _safe_str(row.get("股票名稱")) or code, "market": _safe_str(row.get("市場別")) or "上市"}
            category = _safe_str(row.get("類別")) or _safe_str(row.get("產業"))
            if category:
                item["category"] = category
            watchlist[target_group].append(item)
            existing.add(code)
            added += 1
        w_report = save_watchlist_permanent(watchlist)
        st.session_state["watchlist_data"] = copy.deepcopy(watchlist)
        st.session_state["watchlist_version"] = int(st.session_state.get("watchlist_version", 0) or 0) + 1
        st.session_state["watchlist_last_saved_at"] = w_report.updated_at or _now_text()
        try:
            get_normalized_watchlist.clear()
        except Exception:
            pass
        result["modules"]["05排行榜/自選股群組"] = {"ok": w_report.permanent_ok, "details": load_detail + w_report.messages(), "group": target_group, "added": added, "count": len(watchlist.get(target_group, []))}
    else:
        result["modules"]["05排行榜/自選股群組"] = {"ok": False, "details": ["永久自選股服務未載入"], "group": target_group}

    if rows and callable(upsert_stock_master_rows):
        master_result = upsert_stock_master_rows(rows)
        result["modules"]["09股票主檔"] = {
            "ok": bool(master_result.get("permanent_ok")),
            "details": list(master_result.get("details", []) or []) + [master_result.get("message", "")],
            "added": master_result.get("added", 0),
            "updated": master_result.get("updated", 0),
            "count": master_result.get("row_count", 0),
        }
    elif not rows:
        result["modules"]["09股票主檔"] = {"ok": True, "details": ["本輪推薦清單為空，09 無需新增或補值"], "added": 0, "updated": 0}
    else:
        result["modules"]["09股票主檔"] = {"ok": False, "details": ["主檔同步服務未載入"]}

    if callable(save_named_json_permanent):
        # 清單為空也要保存空清單，避免 10 頁繼續顯示上一輪資料。
        list_report = save_named_json_permanent("godpick_recommend_list.json", rows)
        old_latest, _ = load_named_json_permanent("godpick_latest_recommendations.json", {})
        latest_payload = dict(old_latest) if isinstance(old_latest, dict) else {}
        latest_payload["saved_at"] = _now_text()
        latest_payload["recommendations"] = rows
        latest_report = save_named_json_permanent("godpick_latest_recommendations.json", latest_payload)
        result["modules"]["10推薦清單"] = {"ok": bool(list_report.permanent_ok and latest_report.permanent_ok), "details": list_report.messages() + latest_report.messages(), "count": len(rows)}
    else:
        result["modules"]["10推薦清單"] = {"ok": False, "details": ["永久清單服務未載入"]}

    export_required = bool(settings.get("auto_export_excel", True))
    if export_required and excel_bytes and callable(write_export_file):
        filename = f"股神推薦紀錄_一鍵同步_{_tw_now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        ok, msg, path = write_export_file(_safe_str(settings.get("export_folder")), filename, excel_bytes)
        history_ok = True
        history_details = []
        if ok and callable(append_export_history):
            h_report = append_export_history({"file_name": filename, "path": path, "target_group": target_group, "record_count": len(records_df), "recommendation_count": len(rows), "source": "08一鍵同步05+09+10"})
            history_ok = h_report.permanent_ok
            history_details = h_report.messages()
        result["modules"]["匯出資料夾/紀錄"] = {"ok": bool(ok and history_ok), "details": [msg] + history_details, "path": path}
    elif export_required:
        result["modules"]["匯出資料夾/紀錄"] = {"ok": False, "details": ["已啟用自動匯出，但 Excel 內容或匯出服務不可用"]}
    else:
        result["modules"]["匯出資料夾/紀錄"] = {"ok": True, "details": ["設定為不自動匯出 Excel"]}

    required = ["08推薦紀錄", "05排行榜/自選股群組", "09股票主檔", "10推薦清單", "匯出資料夾/紀錄"]
    result["overall_ok"] = all(bool(result["modules"].get(k, {}).get("ok")) for k in required)
    result["finished_at"] = _now_text()
    if callable(save_module_sync_state):
        audit_report = save_module_sync_state(result)
        result["audit"] = {"ok": audit_report.permanent_ok, "details": audit_report.messages()}
        result["overall_ok"] = bool(result["overall_ok"] and audit_report.permanent_ok)
    return result


def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")

    # v40：啟用欄位管理極速模式；不再全頁攔截所有表格
    try:
        from godpick_column_manager import install_auto_column_manager
        install_auto_column_manager("8_股神推薦紀錄")
    except Exception:
        pass
    inject_pro_theme()

    if _k("status_msg") not in st.session_state:
        st.session_state[_k("status_msg")] = ""
        st.session_state[_k("status_type")] = "info"
    if _k("watchlist_target_group") not in st.session_state:
        st.session_state[_k("watchlist_target_group")] = "股神推薦"
    if _k("show_column_manager") not in st.session_state:
        st.session_state[_k("show_column_manager")] = False
    if _k("selected_col_to_move") not in st.session_state:
        st.session_state[_k("selected_col_to_move")] = ""
    if _k("ui_auto_save_flag") not in st.session_state:
        st.session_state[_k("ui_auto_save_flag")] = False
    if _k("export_sync_settings") not in st.session_state:
        if callable(load_export_sync_settings):
            try:
                _settings, _settings_detail = load_export_sync_settings()
                st.session_state[_k("export_sync_settings")] = _settings
                st.session_state[_k("export_settings_detail")] = _settings_detail
            except Exception as _settings_e:
                st.session_state[_k("export_sync_settings")] = {"export_folder": "exports/godpick", "target_group": "股神推薦", "auto_export_excel": True, "sync_latest_only": True}
                st.session_state[_k("export_settings_detail")] = [f"匯出設定載入失敗：{_settings_e}"]
        else:
            st.session_state[_k("export_sync_settings")] = {"export_folder": "exports/godpick", "target_group": "股神推薦", "auto_export_excel": True, "sync_latest_only": True}
    st.session_state[_k("watchlist_target_group")] = _safe_str(st.session_state[_k("export_sync_settings")].get("target_group")) or st.session_state.get(_k("watchlist_target_group"), "股神推薦")

    _load_ui_config_once()

    render_pro_hero(
        title="股神推薦紀錄",
        subtitle="追蹤 7_股神推薦 推薦股票，支援 GitHub + Firestore 雙寫、每日更新、實際交易分析、績效統計、Excel 匯出，並可匯入 4_自選股中心。",
    )
    st.caption(f"目前8頁修正版：{RECORD_FIX_VERSION}")
    st.caption(f"刪除修正版：{DELETE_FIX_VERSION}｜V162 表單批次送出＋本機先行刪除＋遠端不阻塞")
    st.caption(f"運算加速修正版：{RECORD_SPEED_FIX_VERSION}")
    st.caption(f"最新價／損益修正版：{LATEST_PRICE_PNL_FIX_VERSION}｜V179 交易所整體盤後快照＋實際成交雙軌｜保存行情日期／來源／計算狀態")
    st.caption(f"7/8/9 起漲欄位版：{PRELAUNCH_789_VERSION}")
    st.caption(f"股神決策V10進場決策版：{GOD_DECISION_V10_LINK_VERSION}")
    st.caption(f"推薦績效追蹤V12回測校正版：{BACKTEST_V12_VERSION} ｜ V149 單頁籤運算加速版 ｜ V157 狀態正規化快取")
    st.caption(f"每日學習型AI：{LEARNING_SYSTEM_VERSION}｜Champion {GODPICK_AI_MODEL_VERSION}｜績效更新後自動重建經驗校準")

    status_msg = _safe_str(st.session_state.get(_k("status_msg"), ""))
    status_type = _safe_str(st.session_state.get(_k("status_type"), "info"))
    if status_msg:
        if status_type == "success":
            st.success(status_msg)
        elif status_type == "warning":
            st.warning(status_msg)
        elif status_type == "error":
            st.error(status_msg)
        else:
            st.info(status_msg)

    # V174：任何按鈕執行前，先同步唯一權威檔。舊畫面不得回寫覆蓋新紀錄。
    _authority_df_v174, _authority_details_v174, _authority_restored_v174 = _sync_authority_before_actions()
    if _authority_restored_v174:
        st.success(f"Reboot 後已自動還原最新遠端權威推薦紀錄，共 {len(_authority_df_v174)} 筆；不需再按重新帶入。")
    if _authority_details_v174:
        st.session_state[_k("authority_auto_sync_details_v174")] = _authority_details_v174

    try:
        _authority_latest_date_v174 = ""
        if isinstance(_authority_df_v174, pd.DataFrame) and not _authority_df_v174.empty and "推薦日期" in _authority_df_v174.columns:
            _dates_v174 = pd.to_datetime(_authority_df_v174["推薦日期"], errors="coerce").dropna()
            if not _dates_v174.empty:
                _authority_latest_date_v174 = _dates_v174.max().strftime("%Y-%m-%d")
        _authority_path_v174 = project_path("godpick_records.json") if callable(project_path) else "godpick_records.json"
        st.caption(f"唯一權威檔：{_authority_path_v174}｜{len(_authority_df_v174)} 筆｜最新推薦日期：{_authority_latest_date_v174 or '未取得'}")
    except Exception:
        pass

    if callable(load_learning_state):
        try:
            _learning_state_v105 = load_learning_state() or {}
            _learning_profile_v105 = _learning_state_v105.get("experience_profile", {}) if isinstance(_learning_state_v105, dict) else {}
            st.caption(
                f"AI經驗庫：可驗證樣本 {int(_learning_profile_v105.get('eligible_samples', 0) or 0)}｜"
                f"最後決策日 {_safe_str(_learning_state_v105.get('last_run_date')) or '尚未建立'}｜"
                f"模型 {_safe_str(_learning_state_v105.get('model_version')) or GODPICK_AI_MODEL_VERSION}"
            )
        except Exception:
            pass

    # V188：績效頁直接呈現「選股Alpha」與「真正觸發交易」兩套真相。
    if callable(_v188_load_t1_truth_summary):
        try:
            _v188_truth = _v188_load_t1_truth_summary() or {}
        except Exception:
            _v188_truth = {}
        with st.expander("V188｜T+1實戰真相（Selection / Entry / Risk Alpha）", expanded=False):
            render_pro_kpi_row([
                {"label":"T+1成熟樣本","value":int(_v188_truth.get("matured_t1_samples") or 0),"delta":"選股真相"},
                {"label":"真正觸發","value":int(_v188_truth.get("executable_samples") or 0),"delta":f"觸發率 {float(_v188_truth.get('trigger_rate_pct') or 0):.1f}%"},
                {"label":"可執行勝率","value":f"{float(_v188_truth.get('executable_win_rate_pct') or 0):.1f}%" if _v188_truth.get("executable_win_rate_pct") is not None else "待累積","delta":"未觸發不計勝負"},
                {"label":"Selection Alpha","value":f"{float(_v188_truth.get('avg_selection_alpha_pct') or 0):+.2f}%" if _v188_truth.get("avg_selection_alpha_pct") is not None else "待累積","delta":"相對大盤"},
                {"label":"Brier","value":f"{float(_v188_truth.get('brier_score')):.4f}" if _v188_truth.get("brier_score") is not None else "待累積","delta":"機率校準"},
            ])
            st.caption("候選股上漲但未觸發，不得算成交易獲利；假突破雖不計交易勝負，仍會進入訊號品質與風控校準。")
            if callable(_v188_refresh_t1_truth) and st.button("更新 V188 T+1實戰真相", key=_k("v188_truth_refresh_page8"), use_container_width=True):
                with st.spinner("回放成熟推薦並更新MFE/MAE、Alpha與機率校準..."):
                    try:
                        _v188_res = _v188_refresh_t1_truth(max_records=240, max_workers=8, persist=True)
                        _v188_done = f"V188真相更新完成：本輪 {_v188_res.get('processed_this_run',0)} 筆｜成熟 {_v188_res.get('matured_t1_samples',0)}｜真正觸發 {_v188_res.get('executable_samples',0)}。"
                        if bool(_v188_res.get("persistence_ok", True)):
                            st.success(_v188_done + "｜永久化已確認。")
                        else:
                            st.warning(_v188_done + "｜永久化未確認；請至第17頁重試後再Reboot。")
                            for _pm in (_v188_res.get("persistence_messages") or [])[:4]:
                                st.caption(str(_pm))
                    except Exception as _v188_e:
                        st.error(f"V188真相更新失敗：{_v188_e}")

    top_cols = st.columns([1.1, 1.1, 1.1, 1.1, 1.2, 1.4, 2.0])
    with top_cols[0]:
        if st.button("🔄 重新載入", use_container_width=True):
            try:
                df = _load_records(force_remote=True)
                _save_state_df(df)
                _load_ui_config_once()
                msg = f"推薦紀錄已重新載入，共 {len(df) if df is not None else 0} 筆。"
                _set_status(msg, "success")
                _add_action_result("重新載入", True, msg)
            except Exception as e:
                msg = f"重新載入失敗：{e}"
                _set_status(msg, "error")
                _add_action_result("重新載入", False, msg)
            st.rerun()
    with top_cols[1]:
        if st.button("📈 更新最新價", use_container_width=True):
            try:
                df = _get_state_df()
                before_sig = _df_signature(df)
                with st.spinner("V178：驗證實際成交／正式日線日期，拒絕昨收、bid/ask、中間價與模擬撮合，並分離系統／實際損益..."):
                    df = _refresh_latest_prices(df, only_active=bool(st.session_state.get(_k("only_active_update"), True)))
                after_sig = _df_signature(df)
                if before_sig != after_sig:
                    df = _apply_mode_labels(df)
                _save_state_df(df)
                summary = df.attrs.get("latest_refresh_summary", {}) if hasattr(df, "attrs") else {}
                # V179：更新最新價不再只停在 session_state。只要有成功行情，立即原子保存本機權威檔，
                # 並沿用既有快速永久化服務排入 Firestore / GitHub，避免 rerun / reboot 又回到推薦日價格。
                auto_save_ok = False
                auto_save_note = "未自動保存（本次沒有成功新行情）"
                if int(summary.get("success", 0) or 0) > 0:
                    try:
                        auto_save_ok = bool(_save_records_dual(df))
                        auto_save_note = "已自動保存權威紀錄" if auto_save_ok else "自動保存未完成，畫面資料仍保留於本次 session"
                    except Exception as _auto_save_exc:
                        auto_save_note = f"自動保存失敗：{_auto_save_exc}"
                official_diag = summary.get("official_snapshot", {}) if isinstance(summary.get("official_snapshot"), dict) else {}
                official_note = (
                    f"官方盤後快照 TWSE {official_diag.get('twse_date') or '未取得'} / "
                    f"TPEx {official_diag.get('tpex_date') or '未取得'}，命中 {official_diag.get('matched_records', 0)} 筆。"
                )
                msg = (
                    f"V179 最新價／損益更新完成：符合條件 {summary.get('target', 0)} 筆，"
                    f"分 {summary.get('batches', 0)} 批；行情成功 {summary.get('success', 0)} 筆，"
                    f"失敗或沒有新交易日 {summary.get('fail', 0)} 筆，保留舊價 {summary.get('preserved_old_price', 0)} 筆；"
                    f"等待新交易日 {summary.get('waiting_new_trade', 0)} 筆，日期未驗證 {summary.get('unverified_date', 0)} 筆；"
                    f"已算出損益 {summary.get('pnl_calculated', 0)} 筆，缺少推薦基準價 {summary.get('missing_basis', 0)} 筆。"
                    f"{official_note}{auto_save_note}。"
                )
                ok = int(summary.get('success', 0) or 0) > 0 or int(summary.get('target', 0) or 0) == 0
                issue_lines = []
                for item in (summary.get('issue_samples') or [])[:15]:
                    issue_lines.append(
                        f"{item.get('股票代號', '')} {item.get('股票名稱', '')}｜{item.get('狀態', '')}｜來源 {item.get('來源', '')}"
                    )
                detail = str(summary)
                if official_diag.get("messages"):
                    detail += "\n官方盤後快照：\n" + "\n".join(str(x) for x in official_diag.get("messages")[-8:])
                if issue_lines:
                    detail += "\n問題樣本：\n" + "\n".join(issue_lines)
                _set_status(msg, "success" if ok and int(summary.get('fail', 0) or 0) == 0 else "warning")
                _add_action_result("更新最新價", ok, msg, detail)
            except Exception as e:
                msg = f"更新最新價失敗：{e}"
                _set_status(msg, "error")
                _add_action_result("更新最新價", False, msg)
            st.rerun()
    with top_cols[2]:
        if st.button("💾 儲存同步", use_container_width=True):
            try:
                latest_df = _get_state_df()
                latest_df = _apply_mode_labels(latest_df)
                _save_state_df(latest_df)
                ok = _save_records_dual(latest_df)
                report = st.session_state.get(_k("last_sync_report"), {}) or {}
                source_summary = (
                    f"本機{'✓' if report.get('local_ok') else '✗'} / "
                    f"GitHub{'✓' if report.get('github_ok') else ('背景中' if report.get('github_pending') else '✗')} / "
                    f"Firestore{'✓' if report.get('firestore_ok') else '✗'}"
                )
                msg = (
                    f"儲存同步{'成功' if ok else '未完成'}：目前資料 "
                    f"{len(latest_df) if latest_df is not None else 0} 筆｜{source_summary}。"
                )
                _set_status(msg, "success" if ok else "error")
                _add_action_result("儲存同步", bool(ok), msg, "\n".join(st.session_state.get(_k("last_sync_detail"), [])))
            except Exception as e:
                msg = f"儲存同步失敗：{e}"
                _set_status(msg, "error")
                _add_action_result("儲存同步", False, msg)
            st.rerun()
    with top_cols[3]:
        if st.button("🧹 清除快取", use_container_width=True):
            try:
                cleared = []
                try:
                    _get_latest_close.clear(); cleared.append("最新收盤價")
                except Exception as e:
                    cleared.append(f"最新收盤價清除失敗:{e}")
                try:
                    _get_forward_return.clear(); cleared.append("推薦後報酬")
                except Exception as e:
                    cleared.append(f"推薦後報酬清除失敗:{e}")
                try:
                    _get_forward_metrics.clear(); cleared.append("推薦後績效指標")
                except Exception as e:
                    cleared.append(f"推薦後績效指標清除失敗:{e}")
                try:
                    _get_perf_history_bundle.clear(); cleared.append("歷史績效包")
                except Exception as e:
                    cleared.append(f"歷史績效包清除失敗:{e}")
                try:
                    st.session_state.pop(_k("v71_perf_history_cache"), None)
                    _safe_json_write_local(PERF_HISTORY_CACHE_FILE, {"version": "v71", "history": {}, "fail": {}, "updated_at": _now_text()})
                    cleared.append("本機績效快取檔")
                except Exception as e:
                    cleared.append(f"本機績效快取檔清除失敗:{e}")
                cache_ok_v165, cache_msg_v165 = _remove_normalized_record_cache_v165()
                cleared.append(cache_msg_v165)
                _invalidate_analysis_cache()
                msg = "快取已清除：" + "、".join(cleared)
                _set_status(msg, "success")
                _add_action_result("清除快取", True, msg)
            except Exception as e:
                msg = f"清除快取失敗：{e}"
                _set_status(msg, "error")
                _add_action_result("清除快取", False, msg)
            st.rerun()
    with top_cols[4]:
        batch_n = st.number_input("增量更新最多掃描筆數", min_value=20, max_value=2000, value=300, step=20, key=_k("perf_update_batch_size"))
        perf_seconds = st.number_input("單批秒數上限", min_value=30, max_value=150, value=60, step=15, key=_k("perf_update_seconds"))
        max_stock_n = st.number_input("績效每批股票數", min_value=3, max_value=80, value=30, step=1, key=_k("perf_update_stock_limit"))
        perf_force_full = st.toggle("完整重算全部紀錄（較慢）", value=False, key=_k("perf_force_full_v164"))
        st.caption("V164：預設只更新最近範圍內缺資料／過期資料；需要稽核全檔時再開啟完整重算。完整功能保留，但日常按鈕不再每次掃過全部檔案。")
        if st.button("🧮 更新推薦後績效", use_container_width=True):
            try:
                with st.spinner("V104：快速防卡更新推薦後績效中，只更新缺資料 / 過期資料..."):
                    summary = update_recommendation_perf_fast_v77(
                        json_files=["godpick_records.json", "godpick_calibration_samples.json", "godpick_recommend_list.json", "godpick_latest_recommendations.json"],
                        max_records=0 if bool(perf_force_full) else int(batch_n),
                        batch_limit=int(max_stock_n),
                        max_workers=12,
                        stale_minutes=60,
                        process_all=bool(perf_force_full),
                    )
                    calibration_sync_msg = ""
                    if callable(sync_existing_calibration_samples):
                        try:
                            _, calibration_sync_msgs = sync_existing_calibration_samples()
                            calibration_sync_msg = " 校正樣本同步：" + "；".join(str(x) for x in calibration_sync_msgs)
                        except Exception as _cal_sync_e:
                            calibration_sync_msg = f" 校正樣本遠端同步失敗：{_cal_sync_e}"
                    reload_msg = ""
                    perf_persist_msg = ""
                    try:
                        refreshed = _load_records()
                        if refreshed is not None and not refreshed.empty:
                            refreshed = _apply_mode_labels(refreshed)
                            _save_state_df(refreshed)
                            reload_msg = f"重新載入 {len(refreshed)} 筆。"
                            # V178：績效更新器會先原子回寫 JSON；此處立即補齊 authority state /
                            # manifest / Firestore / GitHub 背景同步，避免重啟時被舊權威狀態覆蓋。
                            if callable(save_records_sync_fast):
                                try:
                                    _perf_report = save_records_sync_fast(refreshed, reason="Page8 V178 performance update")
                                    perf_persist_msg = (
                                        f" 績效永久化：本機{'✓' if getattr(_perf_report, 'local_ok', False) else '✗'} / "
                                        f"Firestore{'✓' if getattr(_perf_report, 'firestore_ok', False) else '✗'} / "
                                        f"GitHub{'✓' if getattr(_perf_report, 'github_ok', False) else ('背景中' if getattr(_perf_report, 'github_pending', False) else '✗')}。"
                                    )
                                except Exception as _perf_persist_e:
                                    perf_persist_msg = f" 績效永久化失敗：{_perf_persist_e}"
                    except Exception as _v77_reload_e:
                        reload_msg = f"已更新 JSON，但重新載入畫面資料失敗：{_v77_reload_e}"
                    learning_refresh_msg = ""
                    if callable(refresh_learning_state_from_records):
                        try:
                            _learning_state, _learning_msgs = refresh_learning_state_from_records(persist_remote=True)
                            learning_refresh_msg = " AI經驗校準：" + "；".join(str(x) for x in (_learning_msgs or []))
                        except Exception as _learning_e:
                            learning_refresh_msg = f" AI經驗校準失敗：{_learning_e}"

                msg = (
                    f"V164 已完成{'完整' if bool(perf_force_full) else '增量'}績效更新：候選 {summary.get('candidates', 0)} 筆，"
                    f"成功 {summary.get('success', 0)} 筆，失敗 {summary.get('fail', 0)} 筆；"
                    f"更新檔案：{', '.join(summary.get('updated_files', [])) or '無'}。{reload_msg}{perf_persist_msg}{calibration_sync_msg}{learning_refresh_msg}"
                )
                detail = "；".join(summary.get("messages", [])) if summary.get("messages") else str(summary)
                ok = int(summary.get('success', 0) or 0) > 0 or int(summary.get('fail', 0) or 0) == 0
                _set_status(msg, "success" if ok else "warning")
                _add_action_result("更新推薦後績效", ok, msg, detail)
            except Exception as e:
                msg = f"更新推薦後績效失敗：{e}"
                _set_status(msg, "error")
                _add_action_result("更新推薦後績效", False, msg)
            st.rerun()
    with top_cols[5]:
        st.toggle("只更新未出場", value=True, key=_k("only_active_update"))
        st.number_input("最新價每批筆數（會跑完整份）", min_value=20, max_value=500, value=120, step=10, key=_k("latest_price_batch_size"))
        st.caption("V179：交易中優先實際成交價；休市／週末優先抓 TWSE＋TPEx 最新已完成交易日『整體盤後收盤快照』。仍拒絕昨收、bid/ask、中間價與模擬撮合。所有缺口完整跑完，不設總量上限。")
        st.toggle("啟用替代來源補價（建議開啟）", value=True, key=_k("enable_alt_price_sources"), help="批次來源抓不到時，先用 TWSE/TPEx 官方盤後整體快照，再依序使用 TWSE MIS、Yahoo chart、FinMind、Stooq、本地歷史收盤價補缺口。")
        st.number_input("替代來源每批補價數（會跑完整缺口）", min_value=10, max_value=300, value=60, step=10, key=_k("alt_price_source_limit"))
        st.number_input("替代來源並行數", min_value=1, max_value=10, value=6, step=1, key=_k("alt_price_workers"))
        st.toggle("慢速備援補缺口（utils/Yahoo/歷史逐檔，較慢）", value=False, key=_k("enable_slow_price_fallback"), help="替代來源仍抓不到時才開。預設關閉，避免 Streamlit Cloud 卡很久。")
        st.number_input("慢速備援最多補幾檔 / 每批", min_value=0, max_value=100, value=20, step=5, key=_k("slow_price_fallback_limit"))
    with top_cols[6]:
        st.caption(
            f"GitHub紀錄：{'✅' if _safe_str(_github_config().get('token')) else '❌'} ｜ "
            f"Firestore：{'✅' if _safe_str(_firebase_config().get('project_id')) else '❌'} ｜ "
            f"自選股：{'✅' if _safe_str(_watchlist_github_config().get('token')) else '❌'} ｜ "
            f"UI設定：{'✅' if _safe_str(_ui_config_github_config().get('token')) else '❌'}"
        )

    _render_action_results()

    _authority_auto_details_v174 = st.session_state.get(_k("authority_auto_sync_details_v174"), [])
    if _authority_auto_details_v174:
        with st.expander("權威推薦紀錄自動同步明細", expanded=False):
            for _line_v174 in _authority_auto_details_v174:
                st.write(f"- {_line_v174}")

    df = _get_state_df()
    current_record_sig = _records_local_signature()
    last_record_sig = _safe_str(st.session_state.get(_k("records_source_sig"), ""))
    if df.empty or (current_record_sig and current_record_sig != last_record_sig):
        df = _load_records(force_remote=False)
        _save_state_df(df)
        st.session_state[_k("records_source_sig")] = current_record_sig

    try:
        _integrity_v178 = _record_integrity_summary_v178(df)
        if _integrity_v178.get("duplicate_record_ids", 0) or _integrity_v178.get("duplicate_business_groups", 0) or _integrity_v178.get("missing_rec_date", 0) or _integrity_v178.get("unverified_latest_date", 0) or _integrity_v178.get("missing_basis", 0):
            st.warning(
                "V178 資料完整性檢查："
                f"共 {_integrity_v178.get('rows', 0)} 筆｜"
                f"重複 record_id {_integrity_v178.get('duplicate_record_ids', 0)} 筆｜"
                f"同日同模式重複群組 {_integrity_v178.get('duplicate_business_groups', 0)} 組（原始 {_integrity_v178.get('duplicate_business_rows', 0)} 筆，統計僅計一次）｜"
                f"已自動重建 ID {_integrity_v178.get('repaired_ids', 0)} 筆｜"
                f"缺推薦日期 {_integrity_v178.get('missing_rec_date', 0)} 筆｜"
                f"最新價日期未驗證 {_integrity_v178.get('unverified_latest_date', 0)} 筆｜"
                f"缺推薦基準價 {_integrity_v178.get('missing_basis', 0)} 筆。"
                "舊價格日期未驗證者不視為本次更新成功；V179 成功更新後會自動保存權威紀錄，『儲存同步』仍保留供人工完整驗證。"
            )
        else:
            st.caption(f"V178 資料完整性：{_integrity_v178.get('rows', 0)} 筆｜record_id 唯一｜推薦基準／最新價日期稽核正常")
    except Exception:
        pass

    # V179：不依賴使用者自訂欄位設定，固定提供最近行情稽核表，讓「推薦價＝最新價」
    # 到底是價格真的沒變、尚未取得新交易日，或更新失敗一眼可辨。
    try:
        _audit_cols_v179 = [
            "推薦日期", "股票代號", "股票名稱", "市場別", "推薦價格", "最新價",
            "最新價資料日期", "最新價來源", "最新價更新狀態",
            "系統追蹤每股損益", "系統追蹤報酬%", "損益計算狀態",
        ]
        _audit_cols_v179 = [c for c in _audit_cols_v179 if c in df.columns]
        if _audit_cols_v179:
            with st.expander("V179｜最新價來源與損益稽核（建議更新後先看這裡）", expanded=False):
                _audit_v179 = df[_audit_cols_v179].copy()
                if "推薦日期" in _audit_v179.columns:
                    _audit_v179["_sort_date"] = pd.to_datetime(_audit_v179["推薦日期"], errors="coerce")
                    _audit_v179 = _audit_v179.sort_values("_sort_date", ascending=False, na_position="last").drop(columns=["_sort_date"])
                st.dataframe(_safe_display_df(_audit_v179.head(300)), use_container_width=True, hide_index=True)
                _od = dict(st.session_state.get(_k("v179_official_snapshot_diag"), {}) or {})
                st.caption(
                    f"官方盤後快照：TWSE {_od.get('twse_date') or '未取得'}（{_od.get('twse_count', 0)} 檔）｜"
                    f"TPEx {_od.get('tpex_date') or '未取得'}（{_od.get('tpex_count', 0)} 檔）｜"
                    f"本輪命中 {_od.get('matched_records', 0)} 筆。"
                )
    except Exception:
        pass

    load_detail = st.session_state.get(_k("load_detail"), [])
    if load_detail:
        with st.expander("讀取來源明細", expanded=False):
            for line in load_detail:
                st.write(f"- {line}")

    sync_detail = st.session_state.get(_k("last_sync_detail"), [])
    if sync_detail:
        sync_failed = bool(st.session_state.get(_k("last_sync_failed"), False))
        with st.expander("同步明細｜本機 / GitHub / Firestore", expanded=sync_failed):
            for line in sync_detail:
                st.write(f"- {line}")
            st.caption("刪除／編輯採本機＋Firestore增量保存；『儲存同步』也已改為內容比對＋差異同步，未變更時不再重寫 1,800 多筆，GitHub 大型備份在背景合併上傳。")

    if callable(load_records_github_sync_status):
        try:
            bg_status, _bg_detail = load_records_github_sync_status()
            if isinstance(bg_status, dict) and bg_status:
                bg_state = _safe_str(bg_status.get("status"))
                bg_count = int(bg_status.get("count") or 0)
                if bg_state == "running":
                    st.info(f"GitHub 大型推薦紀錄正在背景同步：{bg_count} 筆。可繼續操作，不需停留等待。")
                elif bg_state == "failed":
                    st.error(f"GitHub 背景備份失敗：{_safe_str(bg_status.get('message'))}；請按上方『儲存同步』重試。")
                elif bg_state == "success":
                    st.caption(f"GitHub 背景備份已完成：{bg_count} 筆｜{_safe_str(bg_status.get('finished_at'))}")
        except Exception:
            pass

    ui_detail = _safe_str(st.session_state.get(_k("ui_config_detail"), ""))
    ui_save_detail = _safe_str(st.session_state.get(_k("ui_save_detail"), ""))
    if ui_detail or ui_save_detail:
        with st.expander("UI 設定明細", expanded=False):
            if ui_detail:
                st.write(f"- 載入：{ui_detail}")
            if ui_save_detail:
                st.write(f"- 保存：{ui_save_detail}")

    # V157：_get_state_df() 已確保 session_state 資料完成正規化，不再每次 rerun 重跑完整 _ensure。
    live_df = _get_state_df().copy(deep=False)

    # V149：首頁只計算 KPI 必要摘要；大型分群分析表改到對應頁籤/面板才運算。
    # 舊版每次按任何按鈕都會先建立所有 analysis tables，造成「重新載入 / 更新最新價 / 儲存同步」後畫面等待很久。
    summary = _build_summary(live_df)
    avg_20 = pd.to_numeric(live_df.get("推薦後20日%", live_df.get("20日績效%")), errors="coerce").dropna().mean() if not live_df.empty else None
    avg_real = pd.to_numeric(live_df.loc[live_df["是否已實際買進"] == True, "實際報酬%"], errors="coerce").dropna().mean() if (not live_df.empty and "是否已實際買進" in live_df.columns and "實際報酬%" in live_df.columns) else None
    ana_tables = None

    def _get_ana_tables_v149() -> dict[str, pd.DataFrame]:
        nonlocal ana_tables
        if ana_tables is None:
            ana_tables, _, _, _ = _get_analysis_cache(live_df)
        return ana_tables

    st.caption("V165 加速：紀錄 JSON 正規化結果以來源簽章建立本機可拋棄快取；新工作階段約可直接載入，不再重算被丟棄的正式推薦欄位。診斷 / 夜間追蹤 / 官方因子 / 品質分析仍採需要時才運算。")

    render_pro_kpi_row([
        {"label": "總筆數", "value": summary["count"], "delta": "推薦紀錄", "delta_class": "pro-kpi-delta-flat"},
        {"label": "持有中", "value": int((live_df["目前狀態"] == "持有").sum()) if not live_df.empty else 0, "delta": "狀態追蹤", "delta_class": "pro-kpi-delta-flat"},
        {"label": "平均系統報酬%", "value": f"{summary['avg_ret']:.2f}%", "delta": f"勝率 {summary['win_rate']:.1f}%", "delta_class": "pro-kpi-delta-flat"},
        {"label": "平均20日績效%", "value": "-" if pd.isna(avg_20) else f"{avg_20:.2f}%", "delta": "-" if pd.isna(avg_real) else f"平均實際 {avg_real:.2f}%", "delta_class": "pro-kpi-delta-flat"},
    ])

    with st.expander("v113 資料完整度檢查 / 欄位訊息診斷", expanded=False):
        if st.toggle("啟動本區運算 / 顯示", value=False, key=_k("lazy_v113_diag")):
            st.caption("V113 已把診斷分成主檔必備、策略說明、K線驗證、價格追蹤、績效成熟。3/5/10/20日績效若推薦時間尚未滿期，會顯示待追蹤，不再誤判成系統錯誤。")
            diag_df = _data_completeness_report_v73(live_df)
            st.dataframe(_safe_display_df(diag_df), use_container_width=True, hide_index=True)
            if not diag_df.empty:
                hard_df = diag_df[diag_df["狀態"].isin(["異常", "需補強", "待更新"])].copy()
                wait_df = diag_df[diag_df["狀態"].isin(["待追蹤", "可補強", "部分資料"])].copy()
                if not hard_df.empty:
                    st.warning("需要優先處理的欄位：" + "、".join(hard_df["欄位"].astype(str).head(12).tolist()))
                if not wait_df.empty:
                    st.info("可觀察或等待資料成熟的欄位：" + "、".join(wait_df["欄位"].astype(str).head(12).tolist()))
            st.success("判讀重點：K線驗證與 3/5/10/20日績效多半需要另外更新或等待交易日成熟；不是 07、8、10、14 串接壞掉。")


        else:
            st.caption("為加速，預設不運算資料完整度診斷；需要檢查欄位時再開啟。")
    with st.expander("🌙 V98 夜間隔日股神紀錄追蹤", expanded=False):
        if st.toggle("啟動本區運算 / 顯示", value=False, key=_k("lazy_v98_night")):
            st.caption("同步 07 股神推薦與 10 推薦清單的夜間隔日欄位；舊紀錄會自動補欄，不影響原始推薦紀錄。")
            night_df = _v98_backfill_night_battle_record_columns(live_df.copy()) if not live_df.empty else pd.DataFrame(columns=V98_NIGHT_DISPLAY_COLS)
            if night_df.empty:
                st.info("目前沒有推薦紀錄。")
            else:
                n1, n2, n3, n4 = st.columns(4)
                night_score = pd.to_numeric(night_df.get("夜間股神總分"), errors="coerce")
                entry_score = pd.to_numeric(night_df.get("隔日進場分數"), errors="coerce")
                swing_score = pd.to_numeric(night_df.get("波段潛力分數"), errors="coerce")
                with n1:
                    st.metric("夜間紀錄筆數", len(night_df))
                with n2:
                    st.metric("平均夜間分", "-" if night_score.dropna().empty else f"{night_score.mean():.2f}")
                with n3:
                    st.metric("隔日高關注", int((entry_score >= 80).sum()))
                with n4:
                    st.metric("波段潛力>=80", int((swing_score >= 80).sum()))

                f1, f2, f3 = st.columns([1.2, 1.2, 1.6])
                with f1:
                    type_opts = [x for x in sorted(night_df.get("進場型態_隔日", pd.Series(dtype=str)).fillna("").astype(str).unique().tolist()) if x]
                    type_sel = st.selectbox("進場型態", ["全部"] + type_opts, key=_k("v98_night_type_filter"))
                with f2:
                    action_opts = [x for x in sorted(night_df.get("隔日建議動作", pd.Series(dtype=str)).fillna("").astype(str).unique().tolist()) if x]
                    action_sel = st.selectbox("隔日建議", ["全部"] + action_opts, key=_k("v98_night_action_filter"))
                with f3:
                    night_kw = st.text_input("搜尋代號 / 名稱 / 策略", value="", key=_k("v98_night_kw"))

                view_night = night_df.copy()
                if type_sel != "全部" and "進場型態_隔日" in view_night.columns:
                    view_night = view_night[view_night["進場型態_隔日"].astype(str) == type_sel]
                if action_sel != "全部" and "隔日建議動作" in view_night.columns:
                    view_night = view_night[view_night["隔日建議動作"].astype(str) == action_sel]
                if night_kw:
                    kw = str(night_kw).strip().lower()
                    mask = pd.Series(False, index=view_night.index)
                    for c in ["股票代號", "股票名稱", "夜間股神建議", "隔日作戰策略"]:
                        if c in view_night.columns:
                            mask = mask | view_night[c].astype(str).str.lower().str.contains(kw, na=False)
                    view_night = view_night[mask]
                show_night_cols = [c for c in V98_NIGHT_DISPLAY_COLS if c in view_night.columns]
                if "隔日實戰排序分" in view_night.columns:
                    view_night = view_night.sort_values("隔日實戰排序分", ascending=False, na_position="last")
                st.dataframe(_safe_display_df(view_night[show_night_cols].head(500)), use_container_width=True, hide_index=True)
                st.caption(f"顯示 {min(len(view_night), 500)} / {len(night_df)} 筆；如要永久保存欄位，請使用上方『儲存同步』。")

        else:
            st.caption("為加速，夜間隔日股神紀錄追蹤改為手動啟動，不影響資料保存。")
    with st.expander("🎯 V102 夜間隔日股神準確率分析", expanded=False):
        if st.toggle("啟動本區運算 / 顯示", value=False, key=_k("lazy_v102_accuracy")):
            _render_v102_night_accuracy_panel(live_df.copy())

        else:
            st.caption("為加速，夜間隔日準確率分析改為手動啟動。")
    with st.expander("🏛️ V110 官方因子紀錄追蹤", expanded=False):
        if st.toggle("啟動本區運算 / 顯示", value=False, key=_k("lazy_v110_official")):
            _render_v110_official_factor_record_panel(live_df.copy())

        else:
            st.caption("為加速，官方因子紀錄追蹤改為手動啟動。")
    with st.expander("🧪 統一欄位｜實戰品質紀錄 / 準確率分析", expanded=False):
        if st.toggle("啟動本區運算 / 顯示", value=False, key=_k("lazy_v120_quality")):
            _render_v120_quality_accuracy_panel(live_df.copy())

        else:
            st.caption("為加速，實戰品質紀錄 / 準確率分析改為手動啟動。")
    tab_options = ["📋 總表管理", "🧠 股神決策", "➕ 手動新增", "📊 系統績效分析", "💹 實際交易分析", "📤 Excel 匯出", "⚙️ 同步檢查"]
    active_tab = st.radio(
        "功能區｜V149 單頁籤運算",
        tab_options,
        index=tab_options.index(st.session_state.get(_k("active_tab"), "📋 總表管理")) if st.session_state.get(_k("active_tab"), "📋 總表管理") in tab_options else 0,
        horizontal=True,
        key=_k("active_tab"),
    )
    st.caption("V149：只運算目前選到的功能區，避免 st.tabs 一次執行全部頁面造成按鈕等待過久。")

    if active_tab == "📋 總表管理":
        render_pro_section("推薦紀錄總表", "先篩選再編輯，減少 data_editor 負擔。支援欄位順序永久保存、重新整理不還原。")
        if st.session_state.get(_k("last_delete_msg")):
            st.success(st.session_state.pop(_k("last_delete_msg")))

        opt_top = st.columns([1.2, 1.2, 1.2, 2.8])
        with opt_top[0]:
            fast_mode = st.toggle("快速模式", value=bool(st.session_state.get(_k("fast_mode"), True)), key=_k("fast_mode"))
        with opt_top[1]:
            visible_limit = st.number_input("顯示筆數上限", min_value=100, max_value=5000, step=100, key=_k("visible_limit"))
        with opt_top[2]:
            if st.button("🧩 欄位管理", use_container_width=True):
                st.session_state[_k("show_column_manager")] = not st.session_state.get(_k("show_column_manager"), False)
                st.rerun()
        with opt_top[3]:
            st.caption("快速模式開啟時，大表只先渲染前 N 筆；設定先存本機並在背景同步 GitHub，不阻塞目前操作。")

        auto_sig = f"{bool(fast_mode)}|{int(visible_limit)}"
        last_auto_sig = _safe_str(st.session_state.get(_k("ui_last_auto_sig")))
        if auto_sig != last_auto_sig:
            st.session_state[_k("ui_last_auto_sig")] = auto_sig
            ok, msg = _persist_ui_config()
            if ok:
                st.session_state[_k("ui_save_detail")] = msg
            else:
                st.session_state[_k("ui_save_detail")] = msg

        filter_cols = st.columns([1.1, 1.1, 1.1, 1.1, 1.1, 1.0, 1.0, 1.0])
        with filter_cols[0]:
            keyword = st.text_input("搜尋代號 / 名稱 / 理由", value="", key=_k("kw"))
        with filter_cols[1]:
            mode_filter = st.selectbox("推薦模式", ["全部"] + sorted([x for x in live_df["推薦模式"].dropna().astype(str).unique().tolist() if x]), index=0, key=_k("mode_filter"))
        with filter_cols[2]:
            category_filter = st.selectbox("類別", ["全部"] + sorted([x for x in live_df["類別"].dropna().astype(str).unique().tolist() if x]), index=0, key=_k("cat_filter"))
        with filter_cols[3]:
            status_filter = st.selectbox("狀態", ["全部"] + STATUS_OPTIONS, index=0, key=_k("status_filter"))
        with filter_cols[4]:
            bought_filter = st.selectbox("是否已買進", ["全部", "是", "否"], index=0, key=_k("buy_filter"))
        with filter_cols[5]:
            sort_by = st.selectbox("排序", ["推薦日期", "推薦總分", "20日績效%", "損益幅%", "實際報酬%", "持有天數"], index=0, key=_k("sort_by"))
        with filter_cols[6]:
            sort_asc = st.toggle("升冪", value=False, key=_k("sort_asc"))
        with filter_cols[7]:
            show_cols_mode = st.selectbox("顯示模式", ["標準", "進階"], index=0, key=_k("show_cols_mode"))

        view_df = _build_filtered_view_df(
            live_df,
            keyword=keyword,
            mode_filter=mode_filter,
            category_filter=category_filter,
            status_filter=status_filter,
            bought_filter=bought_filter,
            sort_by=sort_by,
            sort_asc=sort_asc,
        )

        # v48：推薦紀錄總表欄位管理改用與 12_股神管理中心相同的表單式管理。
        available_cols = [c for c in view_df.columns if c not in ["匯入自選", "刪除"]]
        default_profile_cols = _get_saved_col_profile(show_cols_mode, available_cols)
        try:
            from godpick_column_manager import render_column_manager
            use_cols = render_column_manager(
                f"page08_godpick_record_total_{show_cols_mode}",
                "推薦紀錄總表",
                view_df.loc[:, available_cols].head(1).copy() if available_cols else view_df.head(1).copy(),
                default_profile_cols or available_cols,
            )
        except Exception:
            use_cols = default_profile_cols or available_cols
        use_cols = [c for c in use_cols if c in available_cols] or available_cols

        editor_df, total_rows, truncated = _get_editor_df(
            view_df=view_df,
            use_cols=use_cols,
            fast_mode=bool(st.session_state.get(_k("fast_mode"), True)),
            visible_limit=int(st.session_state.get(_k("visible_limit"), FAST_VISIBLE_LIMIT)),
        )

        if truncated:
            st.warning(f"快速模式啟用中：目前符合條件 {total_rows} 筆，只先顯示前 {len(editor_df)} 筆以加速操作。要編輯全部可關閉快速模式。")
        else:
            st.caption(f"目前顯示 {len(view_df)} / {len(live_df)} 筆")

        editor_key = _record_editor_key_for_mode(show_cols_mode)
        editor_df = _apply_sticky_editor_checkboxes(editor_key, editor_df, "record_id", ["匯入自選", "刪除"])

        # V162：將勾選與欄位編輯放入 form。
        # 使用者點 checkbox / 修改儲存格時不會觸發整頁 rerun；只有按下表單按鈕才執行。
        editor_id_map_key = _k(f"{editor_key}_record_id_map")
        if "record_id" in editor_df.columns:
            st.session_state[editor_id_map_key] = [_safe_str(x) for x in editor_df["record_id"].astype(str).tolist()]
        else:
            st.session_state[editor_id_map_key] = []

        record_column_config = {
            "匯入自選": st.column_config.CheckboxColumn("匯入自選"),
            "刪除": st.column_config.CheckboxColumn("刪除"),
            "record_id": None,
            "股票代號": st.column_config.TextColumn("股票代號", disabled=True),
            "股票名稱": st.column_config.TextColumn("股票名稱", disabled=True),
            "推薦模式": st.column_config.TextColumn("推薦模式", disabled=True),
            "推薦等級": st.column_config.TextColumn("推薦等級", disabled=True),
            "推薦總分": st.column_config.NumberColumn("推薦總分", format="%.2f", disabled=True),
            "股神決策模式": st.column_config.TextColumn("股神決策模式", disabled=True),
            "股神進場建議": st.column_config.TextColumn("股神進場建議", disabled=True),
            "推薦分層": st.column_config.TextColumn("推薦分層", disabled=True),
            "建議部位%": st.column_config.NumberColumn("建議部位%", format="%.1f", disabled=True),
            "風險報酬比": st.column_config.NumberColumn("風險報酬比", format="%.2f", disabled=True),
            "追價風險分": st.column_config.NumberColumn("追價風險分", format="%.2f", disabled=True),
            "停損距離%": st.column_config.NumberColumn("停損距離%", format="%.2f", disabled=True),
            "目標報酬%": st.column_config.NumberColumn("目標報酬%", format="%.2f", disabled=True),
            "不建議買進原因": st.column_config.TextColumn("不建議買進原因", disabled=True),
            "最佳操作劇本": st.column_config.TextColumn("最佳操作劇本", disabled=True),
            "隔日操作建議": st.column_config.TextColumn("隔日操作建議", disabled=True),
            "轉弱條件": st.column_config.TextColumn("轉弱條件", disabled=True),
            "股神決策分數": st.column_config.NumberColumn("股神決策分數", format="%.2f", disabled=True),
            "股神建議動作": st.column_config.TextColumn("股神建議動作", disabled=True),
            "股神信心": st.column_config.TextColumn("股神信心", disabled=True),
            "股神進場區間": st.column_config.TextColumn("股神進場區間", disabled=True),
            "技術結構分數": st.column_config.NumberColumn("技術結構分數", format="%.2f", disabled=True),
            "起漲前兆分數": st.column_config.NumberColumn("起漲前兆分數", format="%.2f", disabled=True),
            "飆股起漲分數": st.column_config.NumberColumn("飆股起漲分數", format="%.2f", disabled=True),
            "起漲等級": st.column_config.TextColumn("起漲等級", disabled=True),
            "起漲摘要": st.column_config.TextColumn("起漲摘要", disabled=True),
            "交易可行分數": st.column_config.NumberColumn("交易可行分數", format="%.2f", disabled=True),
            "類股熱度分數": st.column_config.NumberColumn("類股熱度分數", format="%.2f", disabled=True),
            "強勢族群等級": st.column_config.TextColumn("強勢族群等級", disabled=True),
            "族群資金流分數": st.column_config.NumberColumn("族群資金流分數", format="%.2f", disabled=True),
            "族群輪動狀態": st.column_config.TextColumn("族群輪動狀態", disabled=True),
            "同族群強勢比例": st.column_config.NumberColumn("同族群強勢比例", format="%.2f", disabled=True),
            "同族群推薦密度": st.column_config.NumberColumn("同族群推薦密度", format="%.2f", disabled=True),
            "同族群平均量能分": st.column_config.NumberColumn("同族群平均量能分", format="%.2f", disabled=True),
            "族群策略建議": st.column_config.TextColumn("族群策略建議", disabled=True),
            "族群資金流說明": st.column_config.TextColumn("族群資金流說明", width="large", disabled=True),
            "推薦價格": st.column_config.NumberColumn("推薦價格", format="%.2f", disabled=True),
            "推薦日價格": st.column_config.NumberColumn("推薦日價格", format="%.2f", disabled=True),
            "最新價": st.column_config.NumberColumn("最新價", format="%.2f", disabled=True),
            "最新價資料日期": st.column_config.TextColumn("行情日期", disabled=True),
            "最新價資料時間": st.column_config.TextColumn("行情時間", disabled=True),
            "最新價來源": st.column_config.TextColumn("行情來源", disabled=True),
            "最新價更新狀態": st.column_config.TextColumn("最新價更新狀態", width="large", disabled=True),
            "推薦基準價來源": st.column_config.TextColumn("推薦基準價來源", disabled=True),
            "損益金額": st.column_config.NumberColumn("每股損益", format="%.2f", disabled=True, help="未輸入股數時，此欄是每股價差，不是整筆交易金額。"),
            "損益幅%": st.column_config.NumberColumn("損益幅%", format="%.2f", disabled=True),
            "損益計算基準": st.column_config.TextColumn("損益計算基準", disabled=True),
            "損益計算狀態": st.column_config.TextColumn("損益計算狀態", width="large", disabled=True),
            "3日績效%": st.column_config.NumberColumn("3日績效%", format="%.2f", disabled=True),
            "5日績效%": st.column_config.NumberColumn("5日績效%", format="%.2f", disabled=True),
            "10日績效%": st.column_config.NumberColumn("10日績效%", format="%.2f", disabled=True),
            "20日績效%": st.column_config.NumberColumn("20日績效%", format="%.2f", disabled=True),
            "目前狀態": st.column_config.SelectboxColumn("目前狀態", options=STATUS_OPTIONS),
            "是否已實際買進": st.column_config.CheckboxColumn("是否已實際買進"),
            "實際買進價": st.column_config.NumberColumn("實際買進價", format="%.2f"),
            "實際賣出價": st.column_config.NumberColumn("實際賣出價", format="%.2f"),
            "實際報酬%": st.column_config.NumberColumn("實際報酬%", format="%.2f", disabled=True),
            "是否達停損": st.column_config.CheckboxColumn("是否達停損"),
            "是否達目標1": st.column_config.CheckboxColumn("是否達目標1"),
            "是否達目標2": st.column_config.CheckboxColumn("是否達目標2"),
            "持有天數": st.column_config.NumberColumn("持有天數", format="%d", disabled=True),
            "推薦日期": st.column_config.TextColumn("推薦日期", disabled=True),
            "推薦時間": st.column_config.TextColumn("推薦時間", disabled=True),
            "模式績效標籤": st.column_config.TextColumn("模式績效標籤", disabled=True),
            "股神推論": st.column_config.TextColumn("股神推論", width="large", disabled=True),
            "推薦理由摘要": st.column_config.TextColumn("推薦理由摘要", width="large", disabled=True),
            "備註": st.column_config.TextColumn("備註", width="large"),
        }

        st.info("V162 快速批次模式：勾選與編輯時不會立即重跑；完成選取後再按下方按鈕才執行。")
        form_key = _k(f"record_batch_form_{show_cols_mode}")
        with st.form(form_key, clear_on_submit=False, border=True):
            edited_df = st.data_editor(
                editor_df,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key=editor_key,
                column_config=record_column_config,
            )

            _import_n = len(_selection_ids("匯入自選"))
            _delete_n = len(_selection_ids("刪除"))
            st.caption(
                f"上次已送出的勾選：匯入自選 {_import_n} 筆｜刪除 {_delete_n} 筆。"
                "本次表格勾選尚未按按鈕前，不會啟動任何計算。"
            )

            action_cols = st.columns([1.6, 1.2, 1.2, 1.15, 1.15, 1.2, 1.2, 2.6])
            with action_cols[0]:
                target_group = st.text_input(
                    "匯入自選群組",
                    value=st.session_state.get(_k("watchlist_target_group"), "股神推薦"),
                    key=_k("watchlist_target_group"),
                )
            with action_cols[1]:
                import_clicked = st.form_submit_button("📥 匯入勾選到4_自選股", use_container_width=True)
            with action_cols[2]:
                apply_clicked = st.form_submit_button("✅ 套用編輯", use_container_width=True)
            with action_cols[3]:
                select_all_clicked = st.form_submit_button(
                    "☑️ 刪除全選",
                    use_container_width=True,
                    help="全選目前畫面顯示的紀錄為待刪除；不會立即刪除。",
                )
            with action_cols[4]:
                cancel_delete_clicked = st.form_submit_button(
                    "↩️ 刪除取消",
                    use_container_width=True,
                    help="取消所有刪除勾選，不影響匯入自選勾選。",
                )
            with action_cols[5]:
                delete_clicked = st.form_submit_button("🗑️ 刪除勾選", use_container_width=True, type="primary")
            with action_cols[6]:
                clear_filter_clicked = st.form_submit_button("🧼 清空目前篩選", use_container_width=True)
            with action_cols[7]:
                st.caption("勾選/編輯不會重跑；按按鈕後才送出。刪除先本機生效，遠端同步狀態會分開顯示。")

        any_form_action = any([
            import_clicked,
            apply_clicked,
            select_all_clicked,
            cancel_delete_clicked,
            delete_clicked,
            clear_filter_clicked,
        ])
        if any_form_action:
            # 只在使用者按下表單按鈕時才把 checkbox 寫入穩定 record_id 狀態。
            _sync_editor_selections_from_returned(edited_df, "record_id", ["匯入自選", "刪除"])

        def _current_import_ids_from_editor() -> list[str]:
            return _collect_editor_selected_ids(editor_key, edited_df, "record_id", "匯入自選")

        if import_clicked:
            selected_ids = _collect_editor_selected_ids(editor_key, edited_df, "record_id", "匯入自選")
            ok, msg = _export_records_to_watchlist(live_df, selected_ids, target_group)
            if ok:
                _set_selection_ids("匯入自選", [])
                _reset_record_editor_for_bulk_delete(
                    show_cols_mode,
                    delete_ids=_selection_ids("刪除"),
                    import_ids=[],
                )
            _set_status(msg, "success" if ok else "warning")
            st.rerun()

        if apply_clicked:
            master = live_df.copy()
            edit_map = {str(r["record_id"]): dict(r) for _, r in edited_df.iterrows()}
            for idx in master.index:
                rec_id = _safe_str(master.at[idx, "record_id"])
                if rec_id not in edit_map:
                    continue
                src = edit_map[rec_id]
                for c in [c for c in master.columns if c in src]:
                    if c in ["record_id", "股票代號", "股票名稱", "推薦模式", "推薦等級", "推薦總分", "上漲機率估計%", "上漲機率等級", "上漲機率信心", "上漲機率說明", "上漲機率因子明細", "族群資金流分數", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "技術結構分數", "起漲前兆分數", "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群輪動狀態", "族群策略建議", "族群資金流說明", "股神決策分數", "股神建議動作", "股神信心", "股神進場區間", "股神推論", "最新價", "損益幅%", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否曾達標_回測", "達標確認狀態", "回測事件摘要", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "進場觸發狀態", "進場觸發日期", "進場評估路徑", "是否納入可執行績效", "執行基準價", "觸發訊號品質分", "觸發後收盤績效%", "觸發當日收盤績效%", "觸發當日最高報酬%", "觸發當日最大回撤%", "觸發當日收盤保留率%", "觸發收盤確認層級", "隔日候選漲跌%", "隔日執行命中結果", "隔日績效檢討標籤", "未觸發漏選標記", "候選與交易分流說明", "績效更新版本", "可執行交易1日%", "可執行交易3日%", "可執行交易5日%", "可執行交易10日%", "可執行交易20日%", "可執行交易最大漲幅%", "可執行交易最大回撤%", "除權息調整旗標", "績效計算口徑", "3日績效%", "5日績效%", "10日績效%", "20日績效%", "推薦日期", "推薦時間", "推薦理由摘要", "匯入自選", "刪除"]:
                        continue
                    master.at[idx, c] = src.get(c)
                recalc = _recalc_row(master.loc[idx].to_dict())
                for k2, v2 in recalc.items():
                    if k2 in master.columns:
                        master.at[idx, k2] = v2
            master = _apply_mode_labels(master)
            changed_ids = [_safe_str(x) for x in edited_df.get("record_id", pd.Series(dtype=str)).astype(str).tolist() if _safe_str(x)]
            changed_rows = master[master["record_id"].astype(str).isin(set(changed_ids))].to_dict(orient="records")
            local_ok = _save_records_mutation_fast_ui(
                master,
                action_name="套用編輯",
                upsert_rows=changed_rows,
                previous_count=len(live_df),
            )
            if local_ok:
                _save_state_df(master)
                st.session_state[_k("last_delete_msg")] = f"已套用並保存 {len(changed_rows)} 筆編輯；遠端狀態請看同步明細。"
            st.rerun()

        if select_all_clicked:
            if "record_id" not in edited_df.columns or edited_df.empty:
                st.warning("目前畫面沒有可全選的紀錄。")
            else:
                visible_delete_ids = [_safe_str(x) for x in edited_df["record_id"].astype(str).tolist() if _safe_str(x)]
                _reset_record_editor_for_bulk_delete(
                    show_cols_mode,
                    delete_ids=visible_delete_ids,
                    import_ids=_current_import_ids_from_editor(),
                )
                st.session_state[_k("last_delete_msg")] = f"已勾選目前顯示 {len(visible_delete_ids)} 筆為待刪除；確認後再按『刪除勾選』。"
                st.rerun()

        if cancel_delete_clicked:
            _reset_record_editor_for_bulk_delete(
                show_cols_mode,
                delete_ids=[],
                import_ids=_current_import_ids_from_editor(),
            )
            st.session_state[_k("last_delete_msg")] = "已取消所有刪除勾選。"
            st.rerun()

        if delete_clicked:
            delete_ids = _collect_editor_selected_ids(editor_key, edited_df, "record_id", "刪除")
            if not delete_ids:
                st.session_state[_k("last_delete_msg")] = "請先勾選要刪除的紀錄，再按『刪除勾選』。"
                _set_status(st.session_state[_k("last_delete_msg")], "warning")
                st.rerun()

            before_n = len(live_df)
            new_df = _delete_records_by_ids(live_df, delete_ids)
            after_n = len(new_df)
            deleted_n = max(before_n - after_n, 0)
            remove_set = {_safe_str(x) for x in delete_ids if _safe_str(x)}
            keep_import_ids = _remove_ids_from_list(_current_import_ids_from_editor(), remove_set)

            if deleted_n <= 0:
                st.session_state[_k("last_delete_msg")] = "沒有刪到資料：勾選的 record_id 與目前紀錄不一致，請重新載入後再試。"
                _set_selection_ids("刪除", [])
                _set_status(st.session_state[_k("last_delete_msg")], "error")
                st.rerun()

            # 先完成本機原子保存並立即切換畫面；Firestore / GitHub 各自回報，不再因遠端落差阻塞刪除。
            local_ok = _save_records_mutation_fast_ui(
                new_df,
                action_name=f"刪除 {deleted_n} 筆紀錄",
                deleted_ids=delete_ids,
                previous_count=before_n,
            )
            if local_ok:
                _save_state_df(new_df)
                _reset_record_editor_for_bulk_delete(
                    show_cols_mode,
                    delete_ids=[],
                    import_ids=keep_import_ids,
                )
                st.session_state[_k("last_delete_msg")] = (
                    f"已刪除 {deleted_n} 筆並完成本機原子保存；"
                    "Firestore / GitHub 同步結果請查看『同步明細』，不會再卡住畫面。"
                )
            else:
                st.session_state[_k("last_delete_msg")] = "本機保存失敗，為避免畫面與檔案不一致，本次未切換資料。"
            st.rerun()

        if clear_filter_clicked:
            source_df = view_df if not truncated else view_df.head(int(st.session_state.get(_k("visible_limit"), FAST_VISIBLE_LIMIT)))
            if source_df.empty:
                st.session_state[_k("last_delete_msg")] = "目前篩選結果沒有資料可清空。"
                _set_status(st.session_state[_k("last_delete_msg")], "warning")
                st.rerun()
            new_df = _clear_filtered_records(live_df, source_df)
            before_n = len(live_df)
            after_n = len(new_df)
            deleted_n = max(before_n - after_n, 0)
            delete_ids = [_safe_str(x) for x in source_df["record_id"].astype(str).tolist() if _safe_str(x)] if "record_id" in source_df.columns else []
            local_ok = _save_records_mutation_fast_ui(
                new_df,
                action_name=f"清空篩選 {deleted_n} 筆",
                deleted_ids=delete_ids,
                previous_count=before_n,
            )
            if local_ok:
                _save_state_df(new_df)
                _reset_record_editor_for_bulk_delete(show_cols_mode, delete_ids=[], import_ids=[])
                st.session_state[_k("last_delete_msg")] = f"已清空 {deleted_n} 筆並完成本機保存；遠端狀態請看同步明細。"
            st.rerun()

    if active_tab == "🧠 股神決策":
        render_pro_section("股神模式進出場決策", "將 7_股神推薦 的分數欄位，結合最新價、停損距離、歷史績效與模式標籤，轉成可操作建議。")
        god_df = live_df.copy()
        if god_df.empty:
            st.info("目前沒有推薦紀錄可分析。")
        else:
            topk = st.columns(4)
            decision_counts = god_df["股神建議動作"].fillna("未判定").value_counts()
            with topk[0]:
                render_pro_info_card("可進場 / 布局", [("筆數", int(decision_counts.get("可進場", 0) + decision_counts.get("拉回可布局", 0)), "股神模式")], chips=["進場"])
            with topk[1]:
                render_pro_info_card("續抱 / 續抱觀察", [("筆數", int(decision_counts.get("續抱", 0) + decision_counts.get("續抱觀察", 0)), "股神模式")], chips=["持有"])
            with topk[2]:
                render_pro_info_card("減碼 / 出場", [("筆數", int(decision_counts.get("減碼觀察", 0) + decision_counts.get("轉弱出場", 0) + decision_counts.get("立即出場", 0) + decision_counts.get("分批停利", 0)), "股神模式")], chips=["風險"])
            with topk[3]:
                avg_god = pd.to_numeric(god_df["股神決策分數"], errors="coerce").dropna().mean()
                render_pro_info_card("平均決策分數", [("分數", "-" if pd.isna(avg_god) else f"{avg_god:.2f}", "0~100")], chips=["綜合"])

            decision_filter_cols = st.columns([1.2, 1.2, 1.2])
            with decision_filter_cols[0]:
                act_filter = st.selectbox("股神建議動作", ["全部"] + sorted([x for x in god_df["股神建議動作"].dropna().astype(str).unique().tolist() if x]), key=_k("god_action_filter"))
            with decision_filter_cols[1]:
                conf_filter = st.selectbox("股神信心", ["全部"] + sorted([x for x in god_df["股神信心"].dropna().astype(str).unique().tolist() if x]), key=_k("god_conf_filter"))
            with decision_filter_cols[2]:
                min_score = st.slider("最低決策分數", 0, 100, 60, 1, key=_k("god_min_score"))

            show_god = god_df.copy()
            if act_filter != "全部":
                show_god = show_god[show_god["股神建議動作"].astype(str) == act_filter].copy()
            if conf_filter != "全部":
                show_god = show_god[show_god["股神信心"].astype(str) == conf_filter].copy()
            show_god = show_god[pd.to_numeric(show_god["股神決策分數"], errors="coerce").fillna(0) >= min_score].copy()
            show_god = show_god.sort_values(["股神決策分數", "推薦總分", "20日績效%"], ascending=[False, False, False], na_position="last")

            st.dataframe(
                _safe_display_df(show_god[_unique_existing_cols(show_god, [
                    "股票代號", "股票名稱", "類別", "推薦模式", "推薦總分", "上漲機率估計%", "上漲機率等級", "買點分級", "風險說明", "股神推論邏輯",
                    "股神決策分數", "股神建議動作", "股神信心", "股神進場區間", "進場時機", "進場時機分數", "建議動作", "等待條件", "操作區間", "近端支撐", "近端壓力", "突破確認價", "停損參考", "追高風險等級", "是否建議追價", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "最新價", "停損價", "賣出目標1", "賣出目標2", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否曾達標_回測", "達標確認狀態", "回測事件摘要", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "進場觸發狀態", "進場觸發日期", "進場評估路徑", "是否納入可執行績效", "執行基準價", "觸發訊號品質分", "觸發後收盤績效%", "觸發當日收盤績效%", "觸發當日最高報酬%", "觸發當日最大回撤%", "觸發當日收盤保留率%", "觸發收盤確認層級", "隔日候選漲跌%", "隔日執行命中結果", "隔日績效檢討標籤", "未觸發漏選標記", "候選與交易分流說明", "績效更新版本", "可執行交易1日%", "可執行交易3日%", "可執行交易5日%", "可執行交易10日%", "可執行交易20日%", "可執行交易最大漲幅%", "可執行交易最大回撤%", "除權息調整旗標", "績效計算口徑", "3日績效%", "5日績效%", "10日績效%", "20日績效%", "模式績效標籤", "股神推論"
                ])]),
                use_container_width=True,
                hide_index=True,
            )

    if active_tab == "➕ 手動新增":
        render_pro_section("手動新增推薦紀錄")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            manual_code = st.text_input("股票代號", value="", key=_k("manual_code"))
        with c2:
            manual_name = st.text_input("股票名稱", value="", key=_k("manual_name"))
        with c3:
            manual_market = st.selectbox("市場別", ["上市", "上櫃", "興櫃"], index=0, key=_k("manual_market"))
        with c4:
            manual_category = st.text_input("類別", value="", key=_k("manual_category"))
        c5, c6, c7, c8 = st.columns(4)
        with c5:
            manual_mode = st.text_input("推薦模式", value="手動新增", key=_k("manual_mode"))
        with c6:
            manual_grade = st.selectbox("推薦等級", ["", "S", "A", "B", "C", "股神級", "強烈關注", "優先觀察", "可列追蹤", "觀察"], index=1, key=_k("manual_grade"))
        with c7:
            manual_total = st.number_input("推薦總分", min_value=0.0, max_value=1000.0, value=85.0, step=0.1, key=_k("manual_total"))
        with c8:
            manual_price = st.number_input("推薦價格", min_value=0.0, value=0.0, step=0.01, key=_k("manual_price"))
        c9, c10, c11, c12 = st.columns(4)
        with c9:
            manual_stop = st.number_input("停損價", min_value=0.0, value=0.0, step=0.01, key=_k("manual_stop"))
        with c10:
            manual_t1 = st.number_input("賣出目標1", min_value=0.0, value=0.0, step=0.01, key=_k("manual_t1"))
        with c11:
            manual_t2 = st.number_input("賣出目標2", min_value=0.0, value=0.0, step=0.01, key=_k("manual_t2"))
        with c12:
            manual_status = st.selectbox("目前狀態", STATUS_OPTIONS, index=0, key=_k("manual_status"))
        manual_reason = st.text_area("推薦理由摘要", value="", height=90, key=_k("manual_reason"))
        manual_tag = st.text_input("推薦標籤", value="", key=_k("manual_tag"))
        if st.button("➕ 新增並同步", use_container_width=True, type="primary"):
            if not _normalize_code(manual_code):
                st.warning("請輸入股票代號")
            else:
                rec_date = _now_date_text()
                rec_time = _now_time_text()
                row = {
                    "record_id": _create_record_id(_normalize_code(manual_code), rec_date, rec_time, manual_mode),
                    "股票代號": _normalize_code(manual_code),
                    "股票名稱": _safe_str(manual_name) or _normalize_code(manual_code),
                    "市場別": manual_market,
                    "類別": manual_category,
                    "推薦模式": manual_mode,
                    "推薦等級": manual_grade,
                    "推薦總分": manual_total,
                    "推薦價格": manual_price if manual_price > 0 else None,
                    "停損價": manual_stop if manual_stop > 0 else None,
                    "賣出目標1": manual_t1 if manual_t1 > 0 else None,
                    "賣出目標2": manual_t2 if manual_t2 > 0 else None,
                    "推薦日期": rec_date,
                    "推薦時間": rec_time,
                    "建立時間": _now_text(),
                    "更新時間": _now_text(),
                    "目前狀態": manual_status,
                    "推薦標籤": manual_tag,
                    "推薦理由摘要": manual_reason,
                }
                new_df = _append_records_dedup_by_business_key(_get_state_df(), pd.DataFrame([row]))
                new_df = _backfill_perf_columns(new_df)
                new_df = _apply_mode_labels(new_df)
                added_rows = new_df[new_df["record_id"].astype(str) == _safe_str(row.get("record_id"))].to_dict(orient="records")
                with st.spinner("正在新增並永久保存..."):
                    ok = _save_records_mutation_fast_ui(
                        new_df,
                        action_name="手動新增推薦紀錄",
                        upsert_rows=added_rows,
                        previous_count=len(_get_state_df()),
                    )
                if ok:
                    _save_state_df(new_df)
                    st.success("已加入並永久保存成功")
                    st.rerun()

    if active_tab == "📊 系統績效分析":
        render_pro_section("系統推薦績效分析", "以推薦價格對照最新價與推薦後 1/3/5/10/20 日、最大漲幅、最大回撤做回測校正")
        valid_sys = pd.to_numeric(live_df["損益幅%"], errors="coerce").dropna()
        win_rate_sys = float((valid_sys > 0).mean() * 100) if not valid_sys.empty else 0.0
        avg_sys_ret = float(valid_sys.mean()) if not valid_sys.empty else 0.0
        valid_20 = pd.to_numeric(live_df.get("推薦後20日%", live_df.get("20日績效%")), errors="coerce").dropna()
        avg_20_v = float(valid_20.mean()) if not valid_20.empty else 0.0
        win_20 = float((valid_20 > 0).mean() * 100) if not valid_20.empty else 0.0
        target_rate = float(live_df["是否達目標1"].fillna(False).map(_normalize_bool).mean() * 100) if len(live_df) else 0.0
        stop_rate = float(live_df["是否達停損"].fillna(False).map(_normalize_bool).mean() * 100) if len(live_df) else 0.0

        render_pro_kpi_row([
            {"label": "系統樣本數", "value": format_number(len(live_df)), "delta": "", "delta_class": "pro-kpi-delta-flat"},
            {"label": "系統勝率", "value": f"{win_rate_sys:.2f}%", "delta": "", "delta_class": "pro-kpi-delta-flat"},
            {"label": "平均系統報酬%", "value": f"{avg_sys_ret:.2f}%", "delta": "", "delta_class": "pro-kpi-delta-flat"},
            {"label": "20日勝率", "value": f"{win_20:.2f}%", "delta": "", "delta_class": "pro-kpi-delta-flat"},
            {"label": "平均20日績效%", "value": f"{avg_20_v:.2f}%", "delta": "", "delta_class": "pro-kpi-delta-flat"},
            {"label": "達目標1比率", "value": f"{target_rate:.2f}%", "delta": f"停損率 {stop_rate:.2f}%", "delta_class": "pro-kpi-delta-flat"},
        ])

        _render_v50_performance_tracker(live_df, "V50 推薦後績效追蹤總控｜8_股神推薦紀錄")
        best_cols = st.columns(2)
        with best_cols[0]:
            ana_tables_v149 = _get_ana_tables_v149()
            if not ana_tables_v149["best_mode"].empty:
                top_mode = ana_tables_v149["best_mode"].iloc[0]
                st.info(f"最強模式：{_safe_str(top_mode.get('推薦模式'))} ｜ 平均20日績效 {(_safe_float(top_mode.get('平均20日績效'), 0) or 0):.2f}% ｜ 20日勝率 {(_safe_float(top_mode.get('20日勝率'), 0) or 0):.2f}%")
            else:
                st.info("最強模式：暫無資料")
        with best_cols[1]:
            if not ana_tables_v149["best_category"].empty:
                top_cat = ana_tables_v149["best_category"].iloc[0]
                st.info(f"最強類別：{_safe_str(top_cat.get('類別'))} ｜ 平均20日績效 {(_safe_float(top_cat.get('平均20日績效'), 0) or 0):.2f}% ｜ 20日勝率 {(_safe_float(top_cat.get('20日勝率'), 0) or 0):.2f}%")
            else:
                st.info("最強類別：暫無資料")
        sub_tabs = st.tabs(["模式分析", "類別分析", "等級分析", "明細表"])
        with sub_tabs[0]:
            st.dataframe(_safe_display_df(ana_tables_v149["mode"]), use_container_width=True, hide_index=True)
        with sub_tabs[1]:
            st.dataframe(_safe_display_df(ana_tables_v149["category"]), use_container_width=True, hide_index=True)
        with sub_tabs[2]:
            st.dataframe(_safe_display_df(ana_tables_v149["grade"]), use_container_width=True, hide_index=True)
        with sub_tabs[3]:
            detail_cols = [c for c in [
                "股票代號", "股票名稱", "類別", "推薦模式", "推薦等級", "模式績效標籤",
                "進場時機", "進場時機分數", "建議動作", "等待條件", "操作區間", "近端支撐", "近端壓力", "突破確認價", "停損參考", "追高風險等級", "是否建議追價", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "最新價", "損益金額", "損益幅%", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "是否曾達標_回測", "達標確認狀態", "回測事件摘要", "是否達標_回測", "是否停損_回測", "命中結果", "績效評語", "追蹤更新時間", "進場觸發狀態", "進場觸發日期", "進場評估路徑", "是否納入可執行績效", "執行基準價", "觸發訊號品質分", "觸發後收盤績效%", "觸發當日收盤績效%", "觸發當日最高報酬%", "觸發當日最大回撤%", "觸發當日收盤保留率%", "觸發收盤確認層級", "隔日候選漲跌%", "隔日執行命中結果", "隔日績效檢討標籤", "未觸發漏選標記", "候選與交易分流說明", "績效更新版本", "可執行交易1日%", "可執行交易3日%", "可執行交易5日%", "可執行交易10日%", "可執行交易20日%", "可執行交易最大漲幅%", "可執行交易最大回撤%", "除權息調整旗標", "績效計算口徑", "3日績效%", "5日績效%", "10日績效%", "20日績效%",
                "是否達停損", "是否達目標1", "是否達目標2", "推薦日期", "持有天數", "推薦理由摘要"
            ] if c in live_df.columns]
            st.dataframe(_safe_display_df(_format_df(live_df[_unique_existing_cols(live_df, detail_cols)])), use_container_width=True, hide_index=True)

        st.divider()
        _render_v15_auto_tune_panel(live_df)

    if active_tab == "💹 實際交易分析":
        render_pro_section("實際交易分析", "只統計有實際買進資料的紀錄")
        trade_df = live_df[live_df["是否已實際買進"].fillna(False).map(_normalize_bool)].copy()
        if trade_df.empty:
            st.info("目前沒有實際交易資料。")
        else:
            valid_real = pd.to_numeric(trade_df["實際報酬%"], errors="coerce").dropna()
            real_win = float((valid_real > 0).mean() * 100) if not valid_real.empty else 0.0
            real_avg = float(valid_real.mean()) if not valid_real.empty else 0.0
            render_pro_kpi_row([
                {"label": "實際交易筆數", "value": len(trade_df), "delta": "", "delta_class": "pro-kpi-delta-flat"},
                {"label": "實際勝率", "value": f"{real_win:.2f}%", "delta": "", "delta_class": "pro-kpi-delta-flat"},
                {"label": "平均實際報酬%", "value": f"{real_avg:.2f}%", "delta": "", "delta_class": "pro-kpi-delta-flat"},
            ])
            st.dataframe(_safe_display_df(trade_df[_unique_existing_cols(trade_df, ["股票代號", "股票名稱", "推薦模式", "推薦價格", "實際買進價", "實際賣出價", "實際報酬%", "目前狀態", "備註"])]), use_container_width=True, hide_index=True)
            ana_tables_v149 = _get_ana_tables_v149()
            st.dataframe(_safe_display_df(ana_tables_v149["trade_mode"]), use_container_width=True, hide_index=True)

    if active_tab == "📤 Excel 匯出":
        render_pro_section("Excel 匯出")
        ana_tables_v149 = _get_ana_tables_v149()
        excel_bytes = _build_export_bytes(live_df, ana_tables_v149)
        _download_filename = f"股神推薦紀錄_{_tw_now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        st.download_button(
            "📥 下載 Excel（下載時同步建立永久匯出紀錄）",
            data=excel_bytes,
            file_name=_download_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            on_click=_record_browser_export,
            args=(excel_bytes, _download_filename, len(live_df)),
        )
        _export_settings = st.session_state.get(_k("export_sync_settings"), {})
        st.caption(f"永久匯出資料夾：{_safe_str(_export_settings.get('export_folder')) or 'exports/godpick'}")
        if st.button("💾 匯出到設定資料夾並永久記錄", use_container_width=True, key=_k("export_to_saved_folder")):
            _fn = f"股神推薦紀錄_{_tw_now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            _result = _persist_export_action(excel_bytes, _fn, len(live_df), "08手動永久匯出")
            if _result.get("history_ok"):
                _level = "success" if _result.get("file_ok") else "warning"
                _set_status(
                    f"匯出紀錄已永久保存｜{_result.get('file_message')}",
                    _level,
                )
            else:
                _set_status("匯出檔案或永久紀錄未完整保存，請查看匯出明細。", "error")
            st.rerun()

    if active_tab == "⚙️ 同步檢查":
        render_pro_info_card(
            "同步 / 欄位完整性",
            [
                ("主要來源", "godpick_records.json + Firestore", "雙寫"),
                ("自選股匯入", "可將勾選紀錄匯入 watchlist.json", "已整合"),
                ("匯入位置", _watchlist_github_config()["path"], "沿用4_自選股中心"),
                ("UI 設定", _ui_config_github_config()["path"], "永久記錄"),
                ("刪除 / 清空", "支援", "總表管理內"),
                ("批次更新", "支援表格編輯 / 刪除 / 清空 / 更新", "已保留"),
                ("推薦後績效", "1/3/5/10/20 日績效% + 最大漲幅/回撤 + 命中結果", "V12已整合"),
                ("模式績效標籤", "依模式歷史表現自動標記", "已整合"),
                ("V15權重回饋", "依推薦後績效、勝率、回撤提出調權建議", "只建議不自動改"),
                ("最強模式 / 類別", "依20日績效 + 勝率綜合排序", "已整合"),
                ("Excel 匯出", "推薦紀錄 / 分析表 / 最強榜", "已整合"),
            ],
            chips=["完整版", "不可缺功能", "雙寫同步", "匯入自選股", "推薦後績效", "回測校正", "最強模式", "最強類別", "權重回饋V15", "UI永久記錄"],
        )

        _sync_settings = dict(st.session_state.get(_k("export_sync_settings"), {}))
        st.markdown("### 永久資料夾／群組設定")
        with st.form(_k("export_sync_settings_form")):
            _folder = st.text_input("推薦紀錄永久匯出資料夾", value=_safe_str(_sync_settings.get("export_folder")) or "exports/godpick", help="可填絕對路徑；相對路徑固定以專案根目錄為基準。")
            _group = st.text_input("05 自選股同步目標群組", value=_safe_str(_sync_settings.get("target_group")) or "股神推薦")
            _auto_export = st.checkbox("一鍵同步時自動匯出 Excel 並保存匯出紀錄", value=bool(_sync_settings.get("auto_export_excel", True)))
            _save_setting = st.form_submit_button("💾 永久保存資料夾與群組設定", use_container_width=True)
        if _save_setting:
            if callable(save_export_sync_settings):
                _new_settings = {"export_folder": _folder, "target_group": _group, "auto_export_excel": _auto_export, "sync_latest_only": True}
                _sr = save_export_sync_settings(_new_settings)
                st.session_state[_k("export_settings_save_detail")] = _sr.messages()
                if _sr.permanent_ok:
                    st.session_state[_k("export_sync_settings")] = _new_settings
                    st.session_state[_k("watchlist_target_group")] = _group
                    _set_status("資料夾與群組設定已永久保存並回讀驗證。", "success" if (_sr.github_ok or _sr.firestore_ok) else "warning")
                else:
                    _set_status("設定只寫入部分來源，未通過永久保存條件。", "error")
                st.rerun()
            else:
                st.error("永久設定服務未載入")

        _settings_detail = st.session_state.get(_k("export_settings_detail"), [])
        _settings_save_detail = st.session_state.get(_k("export_settings_save_detail"), [])
        if _settings_detail or _settings_save_detail:
            with st.expander("資料夾／群組設定來源明細", expanded=False):
                for _line in list(_settings_detail or []) + list(_settings_save_detail or []):
                    st.write(f"- {_line}")

        st.markdown("### 08 一鍵同步 05 + 09 + 10")
        st.caption("此按鈕會真正逐項執行：08推薦紀錄永久保存 → 05目標群組 → 09股票主檔 → 10推薦清單／最新快照 → 設定資料夾Excel與匯出紀錄。任一項失敗都不會顯示全部成功。")
        if st.button("🔁 一鍵同步 05 + 09 + 10（永久驗證）", type="primary", use_container_width=True, key=_k("one_click_sync_05_09_10")):
            with st.spinner("正在逐項同步並回讀驗證 05、08、09、10..."):
                _excel = _build_export_bytes(live_df, _get_ana_tables_v149()) if bool(_sync_settings.get("auto_export_excel", True)) else None
                _sync_result = _run_one_click_sync_05_09_10(live_df, _sync_settings, _excel)
                st.session_state[_k("last_one_click_sync")] = _sync_result
                _ok = bool(_sync_result.get("overall_ok"))
                _set_status("一鍵同步 05+09+10 全部完成並驗證。" if _ok else "一鍵同步有項目失敗；請查看逐項明細。", "success" if _ok else "error")
            st.rerun()

        _last_sync = st.session_state.get(_k("last_one_click_sync"))
        if not isinstance(_last_sync, dict) and callable(load_module_sync_state):
            try:
                _last_sync, _audit_detail = load_module_sync_state()
                st.session_state[_k("module_sync_load_detail")] = _audit_detail
            except Exception:
                _last_sync = {}
        if isinstance(_last_sync, dict) and _last_sync:
            st.write(f"最近一鍵同步：{_last_sync.get('finished_at') or _last_sync.get('updated_at') or '—'}｜整體：{'成功' if _last_sync.get('overall_ok') else '有失敗'}")
            for _module, _info in (_last_sync.get("modules") or {}).items():
                _icon = "✅" if _info.get("ok") else "❌"
                st.write(f"{_icon} {_module}｜{_info.get('count', '')} {_info.get('group', '')}")
                with st.expander(f"{_module} 明細", expanded=not bool(_info.get("ok"))):
                    for _line in _info.get("details", []) or []:
                        if _line:
                            st.write(f"- {_line}")

        st.markdown("### 推薦匯出永久紀錄")
        if callable(load_export_history):
            try:
                _export_history, _export_history_detail = load_export_history()
                if isinstance(_export_history, list) and _export_history:
                    _hist_df = pd.DataFrame(_export_history[:50])
                    _hist_cols = [c for c in ["created_at", "file_name", "path", "configured_folder", "file_write_ok", "target_group", "record_count", "recommendation_count", "source"] if c in _hist_df.columns]
                    st.dataframe(_hist_df[_hist_cols], use_container_width=True, hide_index=True)
                else:
                    st.info("尚未建立永久匯出紀錄。請使用『匯出到設定資料夾並永久記錄』或一鍵同步。")
                with st.expander("匯出紀錄來源明細", expanded=False):
                    for _line in _export_history_detail:
                        st.write(f"- {_line}")
            except Exception as _hist_e:
                st.warning(f"匯出永久紀錄讀取失敗：{_hist_e}")


# =========================================================
# v71：隔夜國際盤欄位相容補強
# 由 07 股神推薦 v69/v71 寫入，8/10 只負責保存與顯示，避免舊資料缺欄位 KeyError。
# =========================================================
OVERNIGHT_V71_COLUMNS = [
    "隔夜風控分數", "隔夜風險等級", "隔夜偏向", "隔夜解讀", "隔夜資料品質", "台指夜盤資料來源", "台指夜盤備援說明",
    "台指夜盤漲跌", "NASDAQ漲跌%", "S&P500漲跌%", "道瓊漲跌%", "費半漲跌%",
    "Nasdaq期貨偏向", "S&P期貨偏向", "匯率風險等級",
]
try:
    for _c in OVERNIGHT_V71_COLUMNS:
        if _c not in GODPICK_RECORD_COLUMNS:
            GODPICK_RECORD_COLUMNS.append(_c)
        if "DEFAULT_STANDARD_COLS" in globals() and _c not in DEFAULT_STANDARD_COLS:
            DEFAULT_STANDARD_COLS.append(_c)
        if "DEFAULT_ADVANCED_COLS" in globals() and _c not in DEFAULT_ADVANCED_COLS:
            DEFAULT_ADVANCED_COLS.append(_c)
except Exception:
    pass


if __name__ == "__main__":
    main()



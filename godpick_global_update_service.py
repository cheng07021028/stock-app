# -*- coding: utf-8 -*-
from __future__ import annotations

"""股神全模組資料檢查與既有健康按鈕一鍵更新服務 v171

設計原則：
- 只補缺檔、補缺欄、更新必要快取；不刪除既有資料，不用假資料覆蓋。
- 依照股神推薦需要的資料順序更新：核心檔案 -> 主檔 -> 大盤 -> 官方因子 -> 自選股同步 -> 推薦紀錄/清單績效 -> 健康檢查。
- 所有步驟獨立回報成功/失敗；單一步驟失敗不會中斷後續安全步驟。
"""

import importlib
import json
import os
import time
import hashlib
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

BASE_DIR = Path(__file__).resolve().parent
GLOBAL_UPDATE_STATUS_FILE = BASE_DIR / "godpick_global_update_status.json"
GLOBAL_UPDATE_SETTINGS_FILE = BASE_DIR / "data" / "config" / "godpick_global_update_settings.json"
GLOBAL_REFRESH_TOKEN_FILE = BASE_DIR / "godpick_global_refresh_token.json"
RECOMMENDATION_READINESS_FILE = BASE_DIR / "godpick_recommendation_readiness.json"
PERFORMANCE_PROFILE_FILE = BASE_DIR / "godpick_performance_profile.json"
GLOBAL_UPDATE_LOCK_FILE = BASE_DIR / ".godpick_global_update.lock"
GLOBAL_UPDATE_VERSION = "v174_bounded_sources_20260805"

CORE_REQUIRED_JSON_DEFAULTS: dict[str, Any] = {
    "market_snapshot.json": {},
    "macro_mode_bridge.json": {},
    "macro_trend_records.json": [],
    "godpick_latest_recommendations.json": {"saved_at": "", "weights": {}, "recommendations": [], "category_strength": [], "hot_pick": []},
    "godpick_records.json": [],
    "godpick_recommend_list.json": [],
    "godpick_user_settings.json": {},
    "godpick_record_ui_config.json": {},
    "godpick_management_ui_config.json": {},
    "godpick_ui_font_settings.json": {},
    "watchlist.json": {},
    "stock_master_cache.json": [],
    "stock_category_overrides.json": {},
    "official_factors_cache.json": {"version": "empty", "updated_at": "", "record_count": 0, "records": [], "diagnostics": [], "meta": {}},
    "official_factors_update_log.json": [],
}

PERSISTENT_SETTING_FILES: dict[str, Any] = {
    "last_query_state.json": {"quick_group": "", "quick_stock_code": "", "home_start": "", "home_end": "", "updated_at": ""},
    "dashboard_table_settings.json": {"version": GLOBAL_UPDATE_VERSION, "updated_at": "", "profiles": {}},
    "hk_chart_settings.json": {"version": GLOBAL_UPDATE_VERSION, "updated_at": "", "settings": {}},
    "godpick_user_settings.json": {},
    "godpick_record_ui_config.json": {},
    "godpick_management_ui_config.json": {},
    "godpick_ui_font_settings.json": {},
    str(GLOBAL_UPDATE_SETTINGS_FILE.relative_to(BASE_DIR)): {},
}

SOURCE_REFRESH_TTL_MINUTES: dict[str, int] = {
    "stock_master_cache.json": 1440,
    "market_snapshot.json": 10,
    "macro_mode_bridge.json": 10,
    "official_factors_cache.json": 720,
}

MODULE_UPDATE_PLAN: list[dict[str, Any]] = [
    {
        "模組": "0_大盤走勢",
        "需要資料": "market_snapshot.json、macro_mode_bridge.json、macro_trend_records.json",
        "全域按鈕會更新": "會：大盤快照、股神橋接、大盤歷史紀錄",
        "仍需手動": "特殊事件、期貨/外盤手動修正、盤後人工判斷仍建議在本頁確認",
        "永久設定": "大盤快取/橋接 JSON；部分人工事件快取由本頁保存",
    },
    {
        "模組": "1_儀表板",
        "需要資料": "推薦結果、推薦紀錄、自選股、大盤快照",
        "全域按鈕會更新": "會：更新儀表板依賴資料來源；儀表板本身重新整理即可帶入",
        "仍需手動": "表格版面偏好若有調整需按本頁套用並永久保存",
        "永久設定": "dashboard_table_settings.json",
    },
    {
        "模組": "2_行情查詢",
        "需要資料": "即時行情、股票主檔、自選股查詢狀態",
        "全域按鈕會更新": "部分：更新股票主檔；即時行情仍依查詢股票即時抓取",
        "仍需手動": "單股即時查詢與查詢條件",
        "永久設定": "last_query_state.json",
    },
    {
        "模組": "3_歷史K線分析",
        "需要資料": "歷史K線快取、股票主檔、自選股、查詢區間、圖表設定",
        "全域按鈕會更新": "部分：更新股票主檔與自選股同步；個股歷史K線依選股/區間抓取",
        "仍需手動": "指定股票、區間、圖表參數",
        "永久設定": "last_query_state.json、hk_chart_settings.json",
    },
    {
        "模組": "4_自選股中心",
        "需要資料": "watchlist.json、watchlist_runtime_snapshot.json、watchlist_normalized.json",
        "全域按鈕會更新": "會：重新正規化自選股並產生 runtime 同步檔",
        "仍需手動": "新增/刪除/改名群組仍在本頁操作",
        "永久設定": "watchlist.json；新增刪除應立即寫回",
    },
    {
        "模組": "5_排行榜",
        "需要資料": "股票主檔、歷史資料、自選股",
        "全域按鈕會更新": "部分：更新股票主檔與自選股；排行榜計算仍依頁面條件執行",
        "仍需手動": "排行榜篩選條件與日期範圍",
        "永久設定": "需再次確認：目前未看到獨立排行榜設定檔",
    },
    {
        "模組": "6_多股比較",
        "需要資料": "自選股、歷史資料、查詢狀態",
        "全域按鈕會更新": "部分：更新自選股同步；多股比較仍依選取群組即時/歷史抓取",
        "仍需手動": "比較群組、日期範圍",
        "永久設定": "last_query_state.json",
    },
    {
        "模組": "7_股神推薦",
        "需要資料": "stock_master_cache、market_snapshot、macro_bridge、official_factors、watchlist、godpick_records、godpick_user_settings",
        "全域按鈕會更新": "會：更新推薦前置資料；不自動重跑推薦，避免覆蓋使用者本次掃描條件",
        "仍需手動": "開始推薦/重新推薦、掃描模式、勾選匯入紀錄/自選股",
        "永久設定": "godpick_user_settings.json",
    },
    {
        "模組": "8_股神推薦紀錄",
        "需要資料": "godpick_records.json、即時價、推薦後績效、UI欄位設定",
        "全域按鈕會更新": "會：更新最新價/績效欄位、補缺欄",
        "仍需手動": "手動標記已買進/已賣出/停損/達標、人工備註",
        "永久設定": "godpick_records.json、godpick_record_ui_config.json",
    },
    {
        "模組": "9_股票主檔更新",
        "需要資料": "stock_master_cache.json、stock_category_overrides.json",
        "全域按鈕會更新": "會：呼叫 refresh_stock_master 更新主檔",
        "仍需手動": "個別分類覆蓋或人工分類修正",
        "永久設定": "stock_master_cache.json、stock_category_overrides.json",
    },
    {
        "模組": "10_推薦清單",
        "需要資料": "godpick_recommend_list.json、即時價、推薦後績效",
        "全域按鈕會更新": "會：更新清單績效欄位、補缺欄",
        "仍需手動": "日期篩選、批次刪除、人工清單管理",
        "永久設定": "godpick_recommend_list.json",
    },
    {
        "模組": "11_資料診斷",
        "需要資料": "全系統 JSON、快取、資料源診斷",
        "全域按鈕會更新": "會：更新後可用此頁再次檢查",
        "仍需手動": "診斷與修復按鈕",
        "永久設定": "診斷產物 JSON；不屬交易參數",
    },
    {
        "模組": "12_股神管理中心",
        "需要資料": "推薦紀錄、推薦清單、管理中心UI設定",
        "全域按鈕會更新": "會：更新依賴資料；管理中心重整後帶入",
        "仍需手動": "管理表格版面/分析視角",
        "永久設定": "godpick_management_ui_config.json",
    },
    {
        "模組": "14_股神權重校正",
        "需要資料": "godpick_records.json、績效欄位、godpick_user_settings.json",
        "全域按鈕會更新": "會：先更新紀錄績效，讓權重校正有最新績效依據",
        "仍需手動": "產生建議權重、套用建議權重",
        "永久設定": "godpick_user_settings.json",
    },
    {
        "模組": "15_帳號管理",
        "需要資料": "auth_config.json / Firebase 設定",
        "全域按鈕會更新": "不更新：帳號權限不可由行情更新批次自動改動",
        "仍需手動": "新增/刪除帳號、改密碼、權限調整",
        "永久設定": "auth_config.json / Firebase",
    },
    {
        "模組": "16_官方因子快取中心",
        "需要資料": "official_factors_cache.json、official_factors_update_log.json",
        "全域按鈕會更新": "會：更新法人、營收、估值官方因子快取",
        "仍需手動": "GitHub 拉取/推送狀態檢查、排程設定",
        "永久設定": "data/config/official_factor_schedule_settings.json",
    },
    {
        "模組": "17_系統健康檢查",
        "需要資料": "全模組狀態",
        "全域按鈕會更新": "會：本頁新增總更新按鈕與全模組資料檢查",
        "仍需手動": "編譯煙霧測試、人工確認異常項目",
        "永久設定": "data/config/godpick_global_update_settings.json",
    },
]

MODULE_REFRESH_STRATEGY: dict[str, tuple[str, str, str]] = {
    "0_大盤走勢": ("一鍵更新直接重建大盤快照/橋接；開頁讀本機快取", "10分鐘新鮮度略過、網路失敗保留舊有效快照", "舊大盤不得硬封鎖新K線；推薦頁另做新鮮度閘門"),
    "1_儀表板": ("清除全域 st.cache_data，開頁依最新推薦/紀錄/大盤重算", "5秒表格快取只在資料未變時使用", "不把舊推薦包裝成即時結果"),
    "2_行情查詢": ("股票主檔刷新；單股即時價仍在查詢時抓", "查詢與歷史資料依股票/日期快取", "即時來源失敗不以0價覆蓋"),
    "3_歷史K線分析": ("主檔與自選股刷新；選定股票開頁重取/命中快取", "歷史K線、分析Bundle依參數快取", "保留多來源與資料新鮮度標記"),
    "4_自選股中心": ("直接重建 normalized/runtime 檔", "本機永久檔先讀，避免每次遠端同步", "不刪群組、不用主檔空值覆蓋既有名稱"),
    "5_排行榜": ("清除排行榜運算快取，開頁依最新主檔與K線重算", "不在一鍵更新時全市場預算排行榜，避免長時間阻塞", "排行榜結果依當前日期與資料源重新計算"),
    "6_多股比較": ("清除比較快取，開頁依目前選取群組重算", "只計算使用者選取股票，不預抓全市場", "比較區間與群組不被一鍵更新改寫"),
    "7_股神推薦": ("更新主檔/大盤/官方因子/績效回饋；舊掃描標示需重跑", "績效回饋預先快取、推薦後處理依檔案簽章重算", "前置資料不足時禁止宣稱正式推薦"),
    "8_股神推薦紀錄": ("更新JSON後移除舊正規化pkl；頁面依來源簽章自動重載", "增量績效、批次報價、公平輪詢各資料檔", "候選績效與可執行交易績效分流"),
    "9_股票主檔更新": ("一鍵更新每日智慧刷新；本頁仍可強制手動重建", "主檔新鮮時略過Yahoo全量補值；本機先保存", "主檔少於100筆視為失敗，不覆蓋有效快取"),
    "10_推薦清單": ("來源JSON簽章改變後自動重載session表格", "不再依賴手動重新讀取；績效批次共用", "latest dict容器不再被績效更新覆蓋成list"),
    "11_資料診斷": ("一鍵更新最後自動重跑全模組資料檢查", "診斷只讀摘要，不重做行情", "異常/注意分開，不把尚未成熟績效誤報為錯誤"),
    "12_股神管理中心": ("清除表格快取，開頁讀最新紀錄/清單", "欄位設定與資料計算分離", "不自動改管理設定或人工狀態"),
    "14_股神權重校正": ("一鍵先更新績效並重建回饋摘要；開頁直接分析", "20MB紀錄解析結果寫入簽章快取", "只產生校正依據，不自動套用權重避免過擬合"),
    "15_帳號管理": ("不納入行情一鍵更新", "避免行情更新觸碰權限資料", "帳密/權限只接受本頁人工操作"),
    "16_官方因子快取中心": ("一鍵更新法人/營收/估值快取", "12小時內新鮮資料略過；GitHub改背景備份", "新抓資料完整度大幅下降時保留舊有效快取"),
    "17_系統健康檢查": ("現有按鈕為唯一一鍵入口，顯示逐步進度與就緒度", "防重複點擊、失敗隔離、只更新需更新資料", "更新後需7頁重掃時明確警告，不偽造新推薦"),
}

FILE_TO_MODULES: list[tuple[str, str, str]] = [
    ("market_snapshot.json", "0/7/1/17", "大盤快照與股神大盤分數"),
    ("macro_mode_bridge.json", "0/7/17", "大盤橋接檔"),
    ("macro_trend_records.json", "0/7/17", "大盤歷史紀錄"),
    ("stock_master_cache.json", "2/3/4/5/6/7/9/16", "股票主檔與分類"),
    ("stock_category_overrides.json", "7/9", "人工分類覆蓋"),
    ("official_factors_cache.json", "7/8/10/14/16", "法人/營收/估值官方因子"),
    ("godpick_latest_recommendations.json", "1/7/12", "最新股神推薦結果"),
    ("godpick_records.json", "1/7/8/12/14", "股神推薦紀錄與績效"),
    ("godpick_recommend_list.json", "7/10/12", "推薦清單與績效"),
    ("godpick_user_settings.json", "7/14", "股神權重、掃描、欄位設定"),
    ("godpick_record_ui_config.json", "8", "推薦紀錄欄位設定"),
    ("godpick_management_ui_config.json", "12", "管理中心欄位設定"),
    ("godpick_ui_font_settings.json", "全頁", "字體比例設定"),
    ("watchlist.json", "3/4/6/7/8", "自選股永久資料"),
    ("auth_config.json", "15/全頁", "帳號與權限設定"),
]


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def json_safe(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, (str, int, bool)):
        return obj
    if isinstance(obj, float):
        if obj != obj or obj in (float("inf"), float("-inf")):
            return None
        return obj
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [json_safe(v) for v in obj]
    try:
        import pandas as pd  # type: ignore
        if isinstance(obj, pd.DataFrame):
            return [json_safe(x) for x in obj.to_dict(orient="records")]
        if isinstance(obj, pd.Series):
            return json_safe(obj.to_dict())
    except Exception:
        pass
    try:
        if hasattr(obj, "item"):
            return json_safe(obj.item())
    except Exception:
        pass
    return str(obj)


def read_json(path: Path) -> tuple[bool, Any, str]:
    try:
        if not path.exists():
            return False, None, "檔案不存在"
        text = path.read_text(encoding="utf-8-sig")
        if not text.strip():
            return False, None, "檔案空白"
        return True, json.loads(text), ""
    except Exception as exc:
        return False, None, f"{type(exc).__name__}: {exc}"


def write_json(path: Path, data: Any) -> tuple[bool, str]:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp_v140")
        tmp.write_text(json.dumps(json_safe(data), ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
        return True, "OK"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def shape_text(data: Any) -> str:
    if isinstance(data, list):
        return f"list / {len(data)} 筆"
    if isinstance(data, dict):
        for key in ["records", "data", "items", "recommendations"]:
            if isinstance(data.get(key), list):
                return f"dict.{key} / {len(data.get(key) or [])} 筆"
        return f"dict / {len(data)} keys"
    if data is None:
        return "None"
    return type(data).__name__


def iter_records(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ["recommendations", "records", "data", "items"]:
            val = data.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    return []


def parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("T", " ").replace("Z", "")
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d"]:
        try:
            return datetime.strptime(s[:19], fmt)
        except Exception:
            pass
    return None


def parse_content_date(value: Any) -> datetime | None:
    """解析資料內容日期；支援官方常見 YYYYMMDD，避免被當成 Unix 奈秒。"""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    if len(text) == 8 and text.isdigit():
        try:
            return datetime.strptime(text, "%Y%m%d")
        except Exception:
            return None
    return parse_dt(text)


def latest_record_content_date(data: Any, fields: list[str]) -> datetime | None:
    rows = iter_records(data)
    best: datetime | None = None
    for row in rows:
        for field in fields:
            parsed = parse_content_date(row.get(field))
            if parsed is not None and (best is None or parsed > best):
                best = parsed
    return best


def top_level_content_date(data: Any, fields: list[str]) -> datetime | None:
    if not isinstance(data, dict):
        return None
    for field in fields:
        parsed = parse_content_date(data.get(field))
        if parsed is not None:
            return parsed
    return None


def find_updated_at(data: Any) -> str:
    """取得容器中真正最新的內容時間，不用上傳/複製後的檔案時間冒充資料日期。"""
    direct_keys = ["updated_at", "saved_at", "last_update", "time", "date", "data_date", "performance_updated_at"]
    record_keys = [
        "updated_at", "saved_at", "最新更新時間", "資料更新時間", "官方因子更新時間",
        "績效更新時間", "追蹤更新時間", "推薦日期", "建立時間", "更新時間",
    ]
    if isinstance(data, dict):
        for key in direct_keys:
            if data.get(key):
                return str(data.get(key))
        data = iter_records(data)
    if isinstance(data, list) and data:
        best_dt: datetime | None = None
        best_text = ""
        for item in data:
            if not isinstance(item, dict):
                continue
            for key in record_keys:
                value = item.get(key)
                if not value:
                    continue
                parsed = parse_dt(value)
                if parsed is not None and (best_dt is None or parsed > best_dt):
                    best_dt = parsed
                    best_text = str(value)
        return best_text
    return ""


def is_blank_value(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return v.strip() in {"", "-", "—", "None", "nan", "NaN"}
    return False


def blank_column_summary(records: list[dict[str, Any]], max_cols: int = 8) -> str:
    if not records:
        return ""
    keys: list[str] = []
    for r in records[:50]:
        for k in r.keys():
            if k not in keys:
                keys.append(k)
    if not keys:
        return ""
    sample = records[: min(len(records), 100)]
    cols = []
    for k in keys:
        blank = sum(1 for r in sample if is_blank_value(r.get(k)))
        if blank == len(sample):
            cols.append(k)
    if not cols:
        return "無整欄空白"
    return "整欄空白：" + "、".join(cols[:max_cols]) + ("..." if len(cols) > max_cols else "")


def file_status_row(base: Path, file_name: str, modules: str, meaning: str) -> dict[str, Any]:
    path = base / file_name
    ok, data, err = read_json(path)
    records = iter_records(data) if ok else []
    count = len(records) if records else (len(data) if isinstance(data, (list, dict)) else 0)
    updated = find_updated_at(data) if ok else ""
    blank_info = blank_column_summary(records)
    status = "OK"
    reason = ""
    if not ok:
        status = "異常"
        reason = err
    elif count == 0 and file_name not in {"stock_category_overrides.json", "godpick_user_settings.json", "godpick_record_ui_config.json", "godpick_management_ui_config.json", "godpick_ui_font_settings.json"}:
        status = "注意"
        reason = "資料筆數為 0；不以假資料補齊，需由對應更新步驟或推薦流程產生。"
    elif blank_info and blank_info != "無整欄空白":
        status = "注意"
        reason = blank_info
    else:
        reason = "資料存在"
    if updated:
        updated_dt = parse_dt(updated)
    else:
        updated_dt = None
    age_minutes = max(0.0, (datetime.now() - updated_dt).total_seconds() / 60.0) if updated_dt is not None else _path_age_minutes(path)
    ttl = SOURCE_REFRESH_TTL_MINUTES.get(file_name)
    freshness = "不適用"
    if age_minutes is not None and ttl is not None:
        freshness = "新鮮" if age_minutes <= ttl else "過期"
        if freshness == "過期" and status == "OK":
            status = "注意"
            reason = f"資料已超過建議更新週期 {ttl} 分鐘；" + reason
    return {
        "檔案": file_name,
        "使用模組": modules,
        "用途": meaning,
        "狀態": status,
        "型態/筆數": shape_text(data) if ok else "",
        "最後更新": updated,
        "大小KB": round(path.stat().st_size / 1024, 2) if path.exists() else 0,
        "資料年齡分鐘": round(age_minutes, 1) if age_minutes is not None else None,
        "建議更新週期分鐘": ttl,
        "新鮮度": freshness,
        "原因/空白檢查": reason,
        "建議更新方式": suggested_update_for_file(file_name),
    }


def suggested_update_for_file(file_name: str) -> str:
    mapping = {
        "market_snapshot.json": "全域更新按鈕 > 大盤快照；或 0_大盤走勢手動更新",
        "macro_mode_bridge.json": "全域更新按鈕 > 大盤橋接；或 0_大盤走勢寫入橋接",
        "macro_trend_records.json": "全域更新按鈕 > 大盤歷史；或 0_大盤走勢寫入紀錄",
        "stock_master_cache.json": "全域更新按鈕 > 股票主檔；或 9_股票主檔更新",
        "official_factors_cache.json": "全域更新按鈕 > 官方因子；或 16_官方因子快取中心",
        "godpick_latest_recommendations.json": "7_股神推薦完成掃描後自動寫入；全域按鈕只檢查不偽造推薦",
        "godpick_records.json": "7/8 新增紀錄；全域按鈕更新最新價/績效與補欄",
        "godpick_recommend_list.json": "7/10 新增清單；全域按鈕更新績效與補欄",
        "watchlist.json": "4_自選股中心手動新增刪除；全域按鈕正規化同步檔",
    }
    return mapping.get(file_name, "保留本檔案既有流程；全域檢查會補缺檔/補缺欄")


def check_all_module_data_status(base_dir: Path | None = None) -> dict[str, Any]:
    base = Path(base_dir or BASE_DIR)
    file_rows = [file_status_row(base, f, m, desc) for f, m, desc in FILE_TO_MODULES]
    module_rows = []
    for item in MODULE_UPDATE_PLAN:
        row = dict(item)
        strategy = MODULE_REFRESH_STRATEGY.get(str(row.get("模組")), ("依原頁面流程", "依頁面快取", "保留既有風控"))
        row["更新後表格刷新方式"] = strategy[0]
        row["更新加速策略"] = strategy[1]
        row["精準度保護"] = strategy[2]
        module_rows.append(row)
    setting_rows = []
    for name, default in PERSISTENT_SETTING_FILES.items():
        path = base / name
        ok, data, err = read_json(path)
        setting_rows.append({
            "設定檔": name,
            "狀態": "OK" if ok else "缺少/需建立",
            "型態": shape_text(data) if ok else "",
            "說明": "已具備永久保存檔" if ok else err,
        })
    abnormal = sum(1 for r in file_rows if r.get("狀態") == "異常")
    warn = sum(1 for r in file_rows if r.get("狀態") == "注意")
    return {
        "summary": {
            "檢查時間": now_text(),
            "資料檔案數": len(file_rows),
            "異常": abnormal,
            "注意": warn,
            "正常": len(file_rows) - abnormal - warn,
        },
        "file_rows": file_rows,
        "module_rows": module_rows,
        "setting_rows": setting_rows,
    }


def ensure_required_json_files(base_dir: Path | None = None) -> list[dict[str, Any]]:
    base = Path(base_dir or BASE_DIR)
    rows = []
    for name, default in CORE_REQUIRED_JSON_DEFAULTS.items():
        path = base / name
        if path.exists() and path.stat().st_size > 0:
            rows.append({"檔案": name, "動作": "略過", "結果": "已存在"})
            continue
        ok, msg = write_json(path, default)
        rows.append({"檔案": name, "動作": "建立預設空檔", "結果": "OK" if ok else msg})
    return rows


def ensure_persistent_setting_files(base_dir: Path | None = None) -> list[dict[str, Any]]:
    base = Path(base_dir or BASE_DIR)
    rows = []
    for name, default in PERSISTENT_SETTING_FILES.items():
        path = base / name
        if path.exists() and path.stat().st_size > 0:
            rows.append({"設定檔": name, "動作": "略過", "結果": "已存在"})
            continue
        payload = dict(default) if isinstance(default, dict) else default
        if isinstance(payload, dict):
            payload.setdefault("version", GLOBAL_UPDATE_VERSION)
            payload.setdefault("updated_at", now_text())
        ok, msg = write_json(path, payload)
        rows.append({"設定檔": name, "動作": "建立", "結果": "OK" if ok else msg})
    return rows


def normalize_watchlist_payload(data: Any) -> dict[str, list[dict[str, str]]]:
    out: dict[str, list[dict[str, str]]] = {}
    if not isinstance(data, dict):
        return out
    for group, items in data.items():
        group_name = str(group).strip()
        if not group_name:
            continue
        normalized = []
        seen = set()
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    code = str(item.get("code") or item.get("股票代號") or item.get("代號") or "").strip()
                    name = str(item.get("name") or item.get("股票名稱") or item.get("名稱") or code).strip()
                    market = str(item.get("market") or item.get("市場別") or "上市").strip() or "上市"
                else:
                    code = str(item).strip()
                    name = code
                    market = "上市"
                if not code:
                    continue
                key = (code, market)
                if key in seen:
                    continue
                seen.add(key)
                normalized.append({"code": code, "name": name or code, "market": market})
        out[group_name] = normalized
    return out


def sync_watchlist_runtime_files(base_dir: Path | None = None) -> dict[str, Any]:
    base = Path(base_dir or BASE_DIR)
    ok, data, err = read_json(base / "watchlist.json")
    if not ok:
        return {"ok": False, "message": err, "rows": []}
    normalized = normalize_watchlist_payload(data)
    payload = {
        "version": GLOBAL_UPDATE_VERSION,
        "updated_at": now_text(),
        "group_count": len(normalized),
        "stock_count": sum(len(v) for v in normalized.values()),
        "watchlist": normalized,
    }
    rows = []
    for name, content in [
        ("watchlist_runtime_snapshot.json", payload),
        ("watchlist_normalized.json", normalized),
    ]:
        w_ok, msg = write_json(base / name, content)
        rows.append({"檔案": name, "結果": "OK" if w_ok else "失敗", "說明": msg})
    return {"ok": all(r.get("結果") == "OK" for r in rows), "message": f"自選股同步完成：{payload['group_count']} 群 / {payload['stock_count']} 檔", "rows": rows}


@contextmanager
def pushd(path: Path):
    old = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old)


def run_step(name: str, func: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    start = time.time()
    try:
        result = func() or {}
        ok = bool(result.get("ok", True))
        msg = str(result.get("message", "完成"))
        detail = result
        mode = "智慧略過" if bool(result.get("skipped")) else "已執行"
    except Exception as exc:
        ok = False
        msg = f"{type(exc).__name__}: {exc}"
        detail = {"error": msg}
        mode = "失敗"
    return {
        "步驟": name,
        "狀態": "OK" if ok else "失敗",
        "執行模式": mode,
        "耗時秒": round(time.time() - start, 2),
        "說明": msg,
        "明細": detail,
    }


def step_core_files(base: Path) -> dict[str, Any]:
    rows = ensure_required_json_files(base)
    setting_rows = ensure_persistent_setting_files(base)
    return {"ok": True, "message": f"核心檔案檢查 {len(rows)} 項、設定檔檢查 {len(setting_rows)} 項", "rows": rows, "setting_rows": setting_rows}


def step_repair_schema(base: Path) -> dict[str, Any]:
    """只在真的缺檔/缺欄時修復，避免每次一鍵更新都備份 20MB 大檔。"""
    try:
        svc = importlib.import_module("godpick_system_health_service")
        health = svc.run_health_check(base)
        needs = [
            row for row in (health.get("rows", []) if isinstance(health, dict) else [])
            if row.get("群組") in {"核心檔案", "欄位串接"} and row.get("狀態") != "OK"
        ]
        if not needs:
            return {"ok": True, "message": "核心檔案與推薦欄位完整，略過備份/補欄", "skipped": True}
        repair = svc.full_safe_repair(base)
        schema_rows = repair.get("schema_rows", []) if isinstance(repair, dict) else []
        # 系統健康備份每個來源檔只保留最近 3 份，避免長期操作越來越慢。
        removed = 0
        backup_dir = Path(getattr(svc, "BACKUP_DIR", base / "backups" / "system_health"))
        if backup_dir.exists():
            groups: dict[str, list[Path]] = {}
            for item in backup_dir.glob("*.bak"):
                source = item.name.split(".", 1)[0]
                groups.setdefault(source, []).append(item)
            for items in groups.values():
                items.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                for old_file in items[3:]:
                    try:
                        old_file.unlink()
                        removed += 1
                    except Exception:
                        pass
        return {"ok": True, "message": f"安全修復完成，欄位檢查 {len(schema_rows)} 項；清理舊備份 {removed} 份", "repair": repair, "skipped": False}
    except Exception as exc:
        return {"ok": False, "message": f"安全修復失敗：{type(exc).__name__}: {exc}"}


def _path_age_minutes(path: Path) -> float | None:
    try:
        return max(0.0, (time.time() - path.stat().st_mtime) / 60.0)
    except Exception:
        return None


def _data_age_minutes(base: Path, file_name: str) -> tuple[float | None, str]:
    path = base / file_name
    ok, data, _ = read_json(path)
    if ok:
        updated_text = find_updated_at(data)
        updated_dt = parse_dt(updated_text)
        if updated_dt is not None:
            return max(0.0, (datetime.now() - updated_dt).total_seconds() / 60.0), f"內容時間 {updated_text}"
    age = _path_age_minutes(path)
    return age, "檔案修改時間"


def _fresh_files(base: Path, names: list[str], ttl_minutes: int) -> tuple[bool, str]:
    ages = []
    for name in names:
        age, source = _data_age_minutes(base, name)
        if age is None:
            return False, f"{name} 不存在"
        ages.append((name, age, source))
    fresh = all(age <= max(0, ttl_minutes) for _, age, _ in ages)
    desc = "、".join(f"{name} {age:.1f}分鐘（{source}）" for name, age, source in ages)
    return fresh, desc


def step_stock_master(base: Path, *, force: bool = False) -> dict[str, Any]:
    fresh, desc = _fresh_files(base, ["stock_master_cache.json"], SOURCE_REFRESH_TTL_MINUTES["stock_master_cache.json"])
    if fresh and not force:
        ok, data, _ = read_json(base / "stock_master_cache.json")
        count = len(data) if ok and isinstance(data, list) else 0
        return {"ok": count >= 100, "message": f"股票主檔仍新鮮，略過外部重抓：{count} 筆｜{desc}", "skipped": True, "row_count": count}
    with pushd(base):
        svc = importlib.import_module("stock_master_service")
        try:
            df, logs = svc.refresh_stock_master(sync_github=False)
        except TypeError:
            df, logs = svc.refresh_stock_master()
    row_count = int(len(df)) if df is not None else 0
    ok = row_count >= 100
    return {"ok": ok, "message": f"股票主檔更新完成：{row_count} 筆" if ok else f"股票主檔更新異常：{row_count} 筆", "logs": logs[-20:] if isinstance(logs, list) else logs, "row_count": row_count, "skipped": False}


def step_macro(base: Path, *, force: bool = False, max_runtime_seconds: int = 35) -> dict[str, Any]:
    fresh, desc = _fresh_files(base, ["market_snapshot.json", "macro_mode_bridge.json"], SOURCE_REFRESH_TTL_MINUTES["market_snapshot.json"])
    if fresh and not force:
        return {"ok": True, "message": f"大盤快照仍新鮮，略過網路抓取｜{desc}", "skipped": True}
    with pushd(base):
        svc = importlib.import_module("macro_startup_service")
        if hasattr(svc, "_run_fast_update"):
            result = svc._run_fast_update(sync_github=False, max_runtime_seconds=int(max_runtime_seconds))
        else:
            result = svc.ensure_macro_startup_update(ttl_seconds=0)
    ok = bool(result.get("ok")) if isinstance(result, dict) else False
    msg = str(result.get("message") or result.get("short_message") or "大盤更新完成") if isinstance(result, dict) else "大盤更新完成"
    return {"ok": ok, "message": msg, "result": result, "skipped": False}



def _latest_expected_trade_date(base: Path) -> datetime | None:
    """Return the newest content date already known by K-line/market data.

    File mtime is deliberately ignored: Streamlit reboot/redeploy recreates files and
    makes stale content look new.  Recommendation freshness must compare business
    content dates instead.
    """
    candidates: list[datetime] = []
    for file_name, fields in [
        ("godpick_latest_recommendations.json", ["本輪市場最新交易日", "K線最後交易日", "行情資料日期", "價格資料日期"]),
        ("market_snapshot.json", ["data_date", "market_date", "twse_data_date", "otc_data_date", "資料日期"]),
    ]:
        ok, data, _ = read_json(base / file_name)
        if not ok:
            continue
        dtv = latest_record_content_date(data, fields) if file_name.startswith("godpick_") else top_level_content_date(data, fields)
        if dtv is not None:
            candidates.append(dtv)
    return max(candidates) if candidates else None


def _official_factor_content_summary(base: Path) -> dict[str, Any]:
    ok, data, err = read_json(base / "official_factors_cache.json")
    rows = iter_records(data) if ok else []
    content_date = latest_record_content_date(data, [
        "官方資料日期", "官方因子資料日期", "三大法人資料日期", "法人資料日期", "FinMind資料日期"
    ]) if ok else None
    complete = 0
    foreign_nonzero = 0
    date_rows = 0
    for row in rows:
        try:
            if float(row.get("官方資料完整度") or 0) >= 60:
                complete += 1
        except Exception:
            pass
        if str(row.get("官方資料日期") or row.get("官方因子資料日期") or "").strip():
            date_rows += 1
        try:
            if abs(float(row.get("外資近1日買賣超") or 0)) > 0:
                foreign_nonzero += 1
        except Exception:
            pass
    return {
        "ok": ok, "error": err, "count": len(rows), "complete": complete,
        "content_date": content_date, "date_rows": date_rows,
        "foreign_nonzero": foreign_nonzero,
    }


def _official_is_content_fresh(base: Path) -> tuple[bool, str, dict[str, Any]]:
    summary = _official_factor_content_summary(base)
    expected = _latest_expected_trade_date(base)
    actual = summary.get("content_date")
    aligned = bool(expected is None or (actual is not None and actual.date() >= expected.date()))
    enough = summary.get("count", 0) >= 1000 and summary.get("complete", 0) >= 100
    foreign_ok = summary.get("foreign_nonzero", 0) >= 20
    fresh = bool(summary.get("ok") and aligned and enough and foreign_ok)
    detail = (
        f"筆數 {summary.get('count', 0)} / 完整>=60 {summary.get('complete', 0)} / "
        f"內容日期 {actual.date() if actual else '未驗證'} / 目標 {expected.date() if expected else '未驗證'} / "
        f"外資非0 {summary.get('foreign_nonzero', 0)}"
    )
    summary["expected_date"] = expected
    summary["aligned"] = aligned
    summary["enough"] = enough
    summary["foreign_ok"] = foreign_ok
    return fresh, detail, summary

def step_official_factors(
    base: Path, push_github: bool = False, *, force: bool = False,
    max_runtime_seconds: int = 75, max_requests: int = 48, quick_mode: bool = True,
) -> dict[str, Any]:
    # 智慧略過只有在「檔案時間新 + 內容日期已追上最新K線 + 完整度足夠 +
    # 外資欄位不是整批0」時才成立。只看 mtime 會讓舊內容在 Reboot 後被誤判新鮮。
    file_fresh, file_desc = _fresh_files(base, ["official_factors_cache.json"], SOURCE_REFRESH_TTL_MINUTES["official_factors_cache.json"])
    content_fresh, content_desc, before = _official_is_content_fresh(base)
    if file_fresh and content_fresh and not force:
        return {
            "ok": True,
            "message": f"官方因子內容已對齊，安全智慧略過｜{content_desc}｜{file_desc}",
            "skipped": True,
            "content_validation": before,
        }
    auto_force_reason = []
    if not file_fresh:
        auto_force_reason.append("檔案超過更新週期")
    if not before.get("aligned"):
        auto_force_reason.append("內容日期落後最新K線")
    if not before.get("enough"):
        auto_force_reason.append("有效完整度不足")
    if not before.get("foreign_ok"):
        auto_force_reason.append("外資欄位疑似整批為0")
    with pushd(base):
        svc = importlib.import_module("godpick_system_health_service")
        manual_cfg = dict(svc.load_schedule_settings() or {})
        manual_cfg["enabled"] = True
        manual_cfg["include_institutional"] = True
        manual_cfg["include_revenue"] = True
        manual_cfg["include_valuation"] = True
        manual_cfg["quick_mode"] = bool(quick_mode)
        manual_cfg["max_runtime_seconds"] = int(max_runtime_seconds)
        manual_cfg["max_requests"] = int(max_requests)
        manual_cfg["finmind_bulk_only"] = True
        manual_cfg["finmind_max_stocks"] = min(30, int(manual_cfg.get("finmind_max_stocks", 30) or 30))
        result = svc.run_official_factor_update_once(manual_cfg, push_github=push_github)
    after_fresh, after_desc, after = _official_is_content_fresh(base)
    raw_ok = bool(result.get("ok"))
    # 更新函式回報成功但內容仍舊，不能再顯示 OK。
    ok = bool(raw_ok and after_fresh)
    msg = str(result.get("message", "官方因子更新完成"))
    timed_out = bool(((result.get("meta") or {}) if isinstance(result, dict) else {}).get("timed_out"))
    if ok and timed_out:
        msg = f"官方因子已達時間/請求上限並安全停止；目前有效快取仍通過驗證：{after_desc}"
    elif ok:
        msg = f"官方因子強制更新並完成內容驗證：{after_desc}"
    else:
        msg = (
            f"官方因子更新後仍未就緒：{after_desc}｜觸發原因："
            f"{ '、'.join(auto_force_reason) or '手動強制更新' }｜原始結果：{msg}"
        )
    return {
        "ok": ok, "message": msg, "result": result, "skipped": False,
        "auto_force_reason": auto_force_reason,
        "content_validation_before": before,
        "content_validation_after": after,
    }


def step_watchlist(base: Path) -> dict[str, Any]:
    return sync_watchlist_runtime_files(base)


def step_perf(base: Path, *, process_all: bool = False, max_records: int = 300, batch_limit: int = 80, stale_minutes: int = 30) -> dict[str, Any]:
    with pushd(base):
        svc = importlib.import_module("godpick_perf_fast_update_v77")
        result = svc.update_recommendation_perf_fast_v77(
            json_files=[
                "godpick_latest_recommendations.json",
                "godpick_recommend_list.json",
                "godpick_records.json",
                "godpick_calibration_samples.json",
            ],
            max_records=int(max_records),
            max_total_records=0 if process_all else int(max_records),
            batch_limit=int(batch_limit),
            max_workers=12,
            stale_minutes=int(stale_minutes),
            process_all=bool(process_all),
        )
    ok = isinstance(result, dict) and int(result.get("fail", 0) or 0) == 0
    msg = (
        f"最新推薦/清單/紀錄績效更新：候選 {result.get('candidates', 0)} / "
        f"成功 {result.get('success', 0)} / 失敗 {result.get('fail', 0)} / "
        f"更新檔 {len(result.get('updated_files', []))}"
    )
    return {"ok": ok or int(result.get("success", 0) or 0) > 0 or int(result.get("candidates", 0) or 0) == 0, "message": msg, "result": result}


def step_feedback_profile(base: Path) -> dict[str, Any]:
    """重建績效回饋快取，讓 7/14 不必每次重新解析 20MB 紀錄。"""
    with pushd(base):
        svc = importlib.import_module("godpick_performance_feedback")
        if hasattr(svc, "refresh_godpick_performance_profile"):
            profile, cache_result = svc.refresh_godpick_performance_profile(
                "godpick_records.json", "godpick_performance_profile.json"
            )
        else:
            profile = svc.load_godpick_performance_profile("godpick_records.json")
            cache_result = (True, "已重建績效回饋摘要")
    quality = profile.get("data_quality", {}) if isinstance(profile, dict) else {}
    baseline = profile.get("baseline", {}) if isinstance(profile, dict) else {}
    sample = int(quality.get("trusted_records", baseline.get("sample", 0)) or 0)
    win_rate = float(baseline.get("win_rate", 0) or 0) * 100.0
    cache_ok = bool(cache_result[0]) if isinstance(cache_result, (tuple, list)) and cache_result else True
    available = bool(profile.get("available")) if isinstance(profile, dict) else False
    # Operation success and model readiness are different states.  A valid cache
    # rebuild with too few trusted samples is a WARNING, not an execution failure.
    return {
        "ok": bool(isinstance(profile, dict) and cache_ok),
        "available": available,
        "warning": bool(isinstance(profile, dict) and cache_ok and not available),
        "message": f"績效回饋摘要完成：可信樣本 {sample} / 勝率 {win_rate:.1f}%｜{cache_result[1]}",
        "profile_summary": {"可信樣本": sample, "勝率%": round(win_rate, 2), "資料品質": quality},
    }


def step_learning_profile(base: Path) -> dict[str, Any]:
    """依最新推薦績效紀錄重建每日學習型AI經驗校準。"""
    with pushd(base):
        svc = importlib.import_module("godpick_learning_system")
        state, messages = svc.refresh_learning_state_from_records(base_dir=base, persist_remote=True)
    profile = state.get("experience_profile", {}) if isinstance(state, dict) else {}
    samples = int(profile.get("eligible_samples", 0) or 0)
    msg_list=[str(x) for x in (messages or [])]
    joined="；".join(msg_list)
    warning_tokens=("待同步","pending","僅本機","永久保存例外","遠端未確認","Hash確認")
    warning=bool(isinstance(state, dict) and state and any(t.lower() in joined.lower() for t in warning_tokens))
    return {
        "ok": bool(isinstance(state, dict) and state),
        "warning": warning,
        "message": f"每日學習型AI經驗校準完成：可驗證樣本 {samples}｜" + joined,
        "learning_summary": {
            "可驗證樣本": samples,
            "模型版本": state.get("model_version", "") if isinstance(state, dict) else "",
            "最後決策日": state.get("last_run_date", "") if isinstance(state, dict) else "",
        },
    }


def _latest_data_time(base: Path, file_name: str) -> datetime | None:
    ok, data, _ = read_json(base / file_name)
    if ok:
        parsed = parse_dt(find_updated_at(data))
        if parsed is not None:
            return parsed
    try:
        return datetime.fromtimestamp((base / file_name).stat().st_mtime)
    except Exception:
        return None


def step_recommendation_readiness(base: Path) -> dict[str, Any]:
    """建立推薦前置資料新鮮度閘門，不偽造或自動覆蓋 7 頁掃描結果。"""
    checks: list[dict[str, Any]] = []

    def add(name: str, ok: bool, score: int, detail: str) -> None:
        checks.append({"項目": name, "狀態": "OK" if ok else "需更新", "得分": score if ok else 0, "滿分": score, "說明": detail})

    master_ok, master_data, master_err = read_json(base / "stock_master_cache.json")
    master_count = len(master_data) if master_ok and isinstance(master_data, list) else 0
    master_age, _ = _data_age_minutes(base, "stock_master_cache.json")
    add("股票主檔", master_count >= 1000 and (master_age is not None and master_age <= 10080), 15, f"{master_count}筆 / 檔案年齡 {master_age:.0f} 分鐘" if master_age is not None else master_err)

    latest_ok, latest_data, latest_err = read_json(base / "godpick_latest_recommendations.json")
    latest_rows = iter_records(latest_data) if latest_ok else []
    kline_date = latest_record_content_date(latest_data, [
        "本輪市場最新交易日", "K線最後交易日", "行情資料日期", "價格資料日期"
    ]) if latest_ok else None

    market_ok, market_data, market_err = read_json(base / "market_snapshot.json")
    macro_age, _ = _data_age_minutes(base, "market_snapshot.json")
    market_date = top_level_content_date(market_data, [
        "data_date", "market_date", "twse_data_date", "otc_data_date"
    ]) if market_ok else None
    market_aligned = bool(kline_date is None or (market_date is not None and market_date.date() >= kline_date.date()))
    macro_ok = bool(market_ok and macro_age is not None and macro_age <= 1440 and market_aligned)
    add(
        "大盤快照", macro_ok, 20,
        f"內容日期 {market_date.date() if market_date else '未驗證'} / 推薦K線 {kline_date.date() if kline_date else '尚無'} / 檔案年齡 {macro_age:.0f} 分鐘"
        if macro_age is not None else market_err or "不存在",
    )

    official_ok, official_data, official_err = read_json(base / "official_factors_cache.json")
    official_rows = iter_records(official_data) if official_ok else []
    official_age, _ = _data_age_minutes(base, "official_factors_cache.json")
    official_date = latest_record_content_date(official_data, [
        "官方資料日期", "官方因子資料日期", "三大法人資料日期", "法人資料日期"
    ]) if official_ok else None
    official_aligned = bool(kline_date is None or (official_date is not None and official_date.date() >= kline_date.date()))
    official_ready = bool(
        len(official_rows) >= 1000 and official_age is not None and official_age <= 4320 and official_aligned
    )
    add(
        "官方因子", official_ready, 20,
        f"{len(official_rows)}筆 / 內容日期 {official_date.date() if official_date else '未驗證'} / 推薦K線 {kline_date.date() if kline_date else '尚無'} / 檔案年齡 {official_age:.0f} 分鐘"
        if official_age is not None else official_err,
    )

    records_ok, records_data, records_err = read_json(base / "godpick_records.json")
    records = iter_records(records_data) if records_ok else []
    perf_ready = 0
    for row in records[-500:]:
        if any(not is_blank_value(row.get(c)) for c in ["推薦後1日%", "可執行交易1日%", "隔日執行命中結果"]):
            perf_ready += 1
    add("推薦績效樣本", len(records) >= 50 and perf_ready >= 20, 20, f"紀錄 {len(records)}筆 / 近500筆有效績效 {perf_ready}筆" if records_ok else records_err)

    profile_ok, profile_data, profile_err = read_json(base / "godpick_performance_profile.json")
    profile = profile_data.get("profile", {}) if isinstance(profile_data, dict) else {}
    trusted = int((profile.get("data_quality") or {}).get("trusted_records", 0) or 0) if isinstance(profile, dict) else 0
    add("績效回饋模型", profile_ok and bool(profile.get("available")) and trusted >= 10, 15, f"可信樣本 {trusted}" if profile_ok else profile_err)

    rec_time = parse_dt(find_updated_at(latest_data)) if latest_ok else None
    dep_times = [x for x in [
        _latest_data_time(base, "market_snapshot.json"),
        _latest_data_time(base, "official_factors_cache.json"),
        _latest_data_time(base, "godpick_records.json"),
    ] if x is not None]
    dependency_time = max(dep_times) if dep_times else None
    scan_current = bool(rec_time and dependency_time and rec_time >= dependency_time)
    add("最新推薦掃描", bool(latest_rows) and scan_current, 10, f"推薦 {len(latest_rows)}筆 / 保存 {rec_time} / 依賴最新 {dependency_time}" if latest_ok else latest_err)

    total = sum(int(x["得分"]) for x in checks)
    full = sum(int(x["滿分"]) for x in checks)
    if total >= 85 and scan_current:
        status = "READY｜資料與推薦結果皆可用"
        action = "可直接檢視推薦；下單仍須依觸發價與守價。"
    elif total >= 70:
        status = "RESCAN｜前置資料已足夠，需到 7 頁重新推薦"
        action = "一鍵更新完成後，請在 7_股神推薦按重新推薦，讓新大盤與官方因子重算排名。"
    else:
        status = "BLOCK｜前置資料不足，禁止宣稱精準正式推薦"
        action = "先處理需更新項目；系統只能顯示研究雷達。"
    payload = {
        "version": GLOBAL_UPDATE_VERSION,
        "updated_at": now_text(),
        "score": total,
        "full_score": full,
        "status": status,
        "recommended_action": action,
        "content_dates": {
            "推薦K線日期": kline_date.strftime("%Y-%m-%d") if kline_date else "",
            "大盤內容日期": market_date.strftime("%Y-%m-%d") if market_date else "",
            "官方因子內容日期": official_date.strftime("%Y-%m-%d") if official_date else "",
        },
        "checks": checks,
    }
    write_json(base / RECOMMENDATION_READINESS_FILE.name, payload)
    return {"ok": total >= 70, "message": f"推薦就緒度 {total}/{full}｜{status}", "readiness": payload}


def step_invalidate_runtime_caches(base: Path, changed_files: list[str] | None = None) -> dict[str, Any]:
    removed: list[str] = []
    for rel in [
        "data/godpick_records_normalized_v165.pkl",
        "data/godpick_records_normalized_v165.pkl.tmp",
    ]:
        path = base / rel
        try:
            if path.exists():
                path.unlink()
                removed.append(rel)
        except Exception:
            pass
    try:
        cache_mod = importlib.import_module("godpick_runtime_cache")
        cache_mod.clear_runtime_cache()
    except Exception:
        pass
    token = {
        "version": GLOBAL_UPDATE_VERSION,
        "updated_at": now_text(),
        "token": hashlib.sha256(f"{time.time_ns()}|{changed_files}".encode("utf-8")).hexdigest()[:24],
        "changed_files": sorted(set(changed_files or [])),
        "instruction": "各頁 st.cache_data 已由 17 頁清除；頁面開啟後依最新 JSON 重算表格。",
    }
    write_json(base / GLOBAL_REFRESH_TOKEN_FILE.name, token)
    return {"ok": True, "message": f"已刷新全模組運算快取；移除 {len(removed)} 個舊衍生快取", "removed": removed, "token": token}


def _background_github_sync(base: Path, include_stock_master: bool, include_official: bool) -> None:
    def worker() -> None:
        with pushd(base):
            if include_stock_master:
                try:
                    svc = importlib.import_module("stock_master_service")
                    df = svc.load_stock_master()
                    if df is not None and not df.empty and hasattr(svc, "_save_master_cache_to_repo"):
                        svc._save_master_cache_to_repo(df, sync_github=True)
                except Exception:
                    pass
            if include_official:
                try:
                    svc = importlib.import_module("official_factor_service")
                    svc.push_cache_to_github()
                except Exception:
                    pass
    threading.Thread(target=worker, name="godpick-global-github-sync", daemon=True).start()


def step_background_backup(base: Path, *, stock_master: bool, official: bool, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {"ok": True, "message": "GitHub 背景備份未啟用", "skipped": True}
    if not stock_master and not official:
        return {"ok": True, "message": "本次股票主檔與官方因子均未重抓，不需重複 GitHub 備份", "skipped": True}
    _background_github_sync(base, stock_master, official)
    targets = "、".join(x for x, enabled_flag in [("股票主檔", stock_master), ("官方因子", official)] if enabled_flag)
    return {"ok": True, "message": f"本機更新已完成；{targets} GitHub 備份已排入背景，不阻塞按鈕", "queued": True, "targets": targets}



def step_godpick_dependency_audit(base: Path) -> dict[str, Any]:
    """Audit every persistent input used by 7_股神推薦 and 8_股神推薦紀錄."""
    rows: list[dict[str, Any]] = []
    def add(name: str, ok: bool, detail: str, critical: bool = True) -> None:
        rows.append({"資料": name, "狀態": "OK" if ok else ("異常" if critical else "注意"), "說明": detail, "關鍵": critical})

    ok, master, err = read_json(base / "stock_master_cache.json")
    master_count = len(master) if ok and isinstance(master, list) else 0
    add("股票主檔", master_count >= 1000, f"{master_count} 筆" if ok else err)

    ok, market, err = read_json(base / "market_snapshot.json")
    mdate = top_level_content_date(market, ["data_date", "market_date", "twse_data_date", "otc_data_date", "資料日期"]) if ok else None
    add("大盤快照/櫃買", bool(ok and mdate), f"內容日期 {mdate.date() if mdate else '未驗證'}" if ok else err)

    _, off_desc, off = _official_is_content_fresh(base)
    add("官方因子", bool(off.get("aligned") and off.get("enough") and off.get("foreign_ok")), off_desc)

    ok, latest, err = read_json(base / "godpick_latest_recommendations.json")
    latest_rows = iter_records(latest) if ok else []
    latest_date = latest_record_content_date(latest, ["推薦日期", "本輪市場最新交易日", "K線最後交易日"]) if ok else None
    add("最新推薦結果", bool(ok), f"{len(latest_rows)} 筆 / 日期 {latest_date.date() if latest_date else '尚未重新推薦'}" if ok else err, critical=False)

    ok, records, err = read_json(base / "godpick_records.json")
    rec_rows = iter_records(records) if ok else []
    perf_rows = sum(1 for r in rec_rows[-500:] if any(not is_blank_value(r.get(c)) for c in ["推薦後1日%", "可執行交易1日%", "隔日執行命中結果"]))
    add("股神推薦紀錄/績效", bool(ok and len(rec_rows) > 0), f"{len(rec_rows)} 筆 / 近500筆已有績效 {perf_rows}")

    ok, rec_list, err = read_json(base / "godpick_recommend_list.json")
    list_rows = iter_records(rec_list) if ok else []
    add("推薦清單", bool(ok), f"{len(list_rows)} 筆（0筆可代表目前尚未加入清單）" if ok else err, critical=False)

    ok, profile, err = read_json(base / "godpick_performance_profile.json")
    profile_data = profile.get("profile", {}) if isinstance(profile, dict) else {}
    trusted = int((profile_data.get("data_quality") or {}).get("trusted_records", 0) or 0) if isinstance(profile_data, dict) else 0
    add("績效回饋模型", bool(ok and trusted >= 10), f"可信樣本 {trusted}" if ok else err, critical=False)

    ok, learning_state, err = read_json(base / "godpick_learning_state.json")
    learning_profile = learning_state.get("experience_profile", {}) if isinstance(learning_state, dict) else {}
    learning_samples = int(learning_profile.get("eligible_samples", 0) or 0) if isinstance(learning_profile, dict) else 0
    learning_date = str(learning_state.get("last_run_date", "")) if isinstance(learning_state, dict) else ""
    add("每日學習型AI", bool(ok and learning_samples >= 10), f"可驗證樣本 {learning_samples} / 最後決策日 {learning_date or '尚未完成推薦'}" if ok else err, critical=False)

    critical_failed = [r for r in rows if r["關鍵"] and r["狀態"] != "OK"]
    return {
        "ok": not critical_failed,
        "message": f"7/8頁資料鏈檢查：關鍵異常 {len(critical_failed)} / 共 {len(rows)} 項",
        "audit_rows": rows,
    }

def step_health_snapshot(base: Path) -> dict[str, Any]:
    status = check_all_module_data_status(base)
    return {"ok": int(status.get("summary", {}).get("異常", 0)) == 0, "message": f"全模組資料檢查完成：異常 {status['summary']['異常']} / 注意 {status['summary']['注意']}", "status": status}


def load_global_update_settings(base_dir: Path | None = None) -> dict[str, Any]:
    base = Path(base_dir or BASE_DIR)
    ok, data, _ = read_json(base / GLOBAL_UPDATE_SETTINGS_FILE.relative_to(BASE_DIR))
    defaults = {
        "version": GLOBAL_UPDATE_VERSION,
        "updated_at": "",
        "update_stock_master": True,
        "update_macro": True,
        "update_official_factors": True,
        "update_watchlist_runtime": True,
        "update_performance": True,
        "repair_schema": True,
        "process_all_performance": False,
        "max_records": 300,
        "batch_limit": 80,
        "stale_minutes": 30,
        "push_github": True,
        "force_source_refresh": False,
        "rebuild_feedback_profile": True,
        "invalidate_runtime_caches": True,
        "macro_max_runtime_seconds": 35,
        "official_max_runtime_seconds": 75,
        "official_max_requests": 48,
        "official_quick_mode": True,
    }
    if isinstance(data, dict):
        defaults.update(data)
    return defaults


def save_global_update_settings(settings: dict[str, Any], base_dir: Path | None = None) -> tuple[bool, str]:
    base = Path(base_dir or BASE_DIR)
    payload = load_global_update_settings(base)
    payload.update(settings or {})
    payload["updated_at"] = now_text()
    return write_json(base / GLOBAL_UPDATE_SETTINGS_FILE.relative_to(BASE_DIR), payload)


def run_global_update(
    base_dir: Path | None = None,
    settings: dict[str, Any] | None = None,
    progress_callback: Callable[[dict[str, Any], list[dict[str, Any]]], None] | None = None,
) -> dict[str, Any]:
    base = Path(base_dir or BASE_DIR)
    cfg = load_global_update_settings(base)
    if settings:
        cfg.update(settings)

    # 防止連點造成兩個全域更新同時覆寫相同 JSON。
    try:
        lock_age = time.time() - GLOBAL_UPDATE_LOCK_FILE.stat().st_mtime if GLOBAL_UPDATE_LOCK_FILE.exists() else None
        if lock_age is not None and lock_age < 15 * 60:
            return {
                "version": GLOBAL_UPDATE_VERSION,
                "updated_at": now_text(),
                "settings": cfg,
                "steps": [],
                "ok": False,
                "message": "已有一鍵更新正在執行；請勿重複點擊。逾時保護最長約數分鐘，鎖定會在15分鐘內自動失效。",
            }
        if GLOBAL_UPDATE_LOCK_FILE.exists() and lock_age is not None and lock_age >= 15 * 60:
            GLOBAL_UPDATE_LOCK_FILE.unlink(missing_ok=True)
        GLOBAL_UPDATE_LOCK_FILE.write_text(json.dumps({"started_at": now_text(), "pid": os.getpid(), "version": GLOBAL_UPDATE_VERSION}, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

    steps: list[dict[str, Any]] = []
    changed_files: list[str] = []

    def add(name: str, fn: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        if callable(progress_callback):
            try:
                running = {"步驟": name, "狀態": "執行中", "執行模式": "有限時執行", "耗時秒": 0, "說明": "已開始，逾時會自動停止並保留舊有效資料"}
                progress_callback(running, list(steps) + [running])
            except Exception:
                pass
        row = run_step(name, fn)
        steps.append(row)
        detail = row.get("明細") if isinstance(row.get("明細"), dict) else {}
        result = detail.get("result") if isinstance(detail, dict) else {}
        if isinstance(result, dict):
            changed_files.extend([str(x) for x in result.get("updated_files", [])])
        if callable(progress_callback):
            try:
                progress_callback(row, list(steps))
            except Exception:
                pass
        return row

    force = bool(cfg.get("force_source_refresh", False))
    stock_master_changed = False
    official_changed = False
    try:
        add("1. 核心檔案與永久設定檢查", lambda: step_core_files(base))
        if cfg.get("repair_schema", True):
            add("2. 安全補欄/補缺檔", lambda: step_repair_schema(base))
        if cfg.get("update_stock_master", True):
            _row = add("3. 股票主檔智慧更新", lambda: step_stock_master(base, force=force))
            _detail = _row.get("明細", {}) if isinstance(_row.get("明細"), dict) else {}
            stock_master_changed = _row.get("狀態") == "OK" and not bool(_detail.get("skipped"))
            if stock_master_changed:
                changed_files.append("stock_master_cache.json")
        if cfg.get("update_macro", True):
            _row = add("4. 大盤快照與股神橋接智慧更新", lambda: step_macro(
                base, force=force, max_runtime_seconds=int(cfg.get("macro_max_runtime_seconds", 35) or 35)
            ))
            _detail = _row.get("明細", {}) if isinstance(_row.get("明細"), dict) else {}
            if _row.get("狀態") == "OK" and not bool(_detail.get("skipped")):
                changed_files.extend(["market_snapshot.json", "macro_mode_bridge.json", "macro_trend_records.json"])
        if cfg.get("update_official_factors", True):
            _row = add("5. 官方因子快取智慧更新", lambda: step_official_factors(
                base, push_github=False, force=force,
                max_runtime_seconds=int(cfg.get("official_max_runtime_seconds", 75) or 75),
                max_requests=int(cfg.get("official_max_requests", 48) or 48),
                quick_mode=bool(cfg.get("official_quick_mode", True)),
            ))
            _detail = _row.get("明細", {}) if isinstance(_row.get("明細"), dict) else {}
            official_changed = _row.get("狀態") == "OK" and not bool(_detail.get("skipped"))
            if official_changed:
                changed_files.extend(["official_factors_cache.json", "official_factors_update_log.json"])
        if cfg.get("update_watchlist_runtime", True):
            _row = add("6. 自選股 runtime 同步", lambda: step_watchlist(base))
            if _row.get("狀態") == "OK":
                changed_files.extend(["watchlist_runtime_snapshot.json", "watchlist_normalized.json"])
        if cfg.get("update_performance", True):
            add("7. 最新推薦/推薦清單/推薦紀錄價格與績效更新", lambda: step_perf(
                base,
                process_all=bool(cfg.get("process_all_performance", False)),
                max_records=int(cfg.get("max_records", 300) or 300),
                batch_limit=int(cfg.get("batch_limit", 80) or 80),
                stale_minutes=int(cfg.get("stale_minutes", 30) or 30),
            ))
        if cfg.get("rebuild_feedback_profile", True):
            _row = add("8. 重建績效回饋與精準度摘要", lambda: step_feedback_profile(base))
            if _row.get("狀態") == "OK":
                changed_files.append("godpick_performance_profile.json")
        _learning_row = add("9. 重建每日學習型AI經驗校準", lambda: step_learning_profile(base))
        if _learning_row.get("狀態") == "OK":
            changed_files.append("godpick_learning_state.json")
        add("10. 7/8頁股神資料鏈完整性檢查", lambda: step_godpick_dependency_audit(base))
        add("11. 股神推薦前置資料就緒度檢查", lambda: step_recommendation_readiness(base))
        changed_files.append("godpick_recommendation_readiness.json")
        if cfg.get("invalidate_runtime_caches", True):
            add("12. 清除舊表格運算快取並發布刷新版本", lambda: step_invalidate_runtime_caches(base, changed_files))
        add("13. GitHub 非阻塞背景備份", lambda: step_background_backup(
            base,
            stock_master=stock_master_changed,
            official=official_changed,
            enabled=bool(cfg.get("push_github", True)),
        ))
        add("14. 全模組資料狀態複檢", lambda: step_health_snapshot(base))

        payload = {
            "version": GLOBAL_UPDATE_VERSION,
            "updated_at": now_text(),
            "settings": cfg,
            "steps": steps,
            "changed_files": sorted(set(changed_files)),
            "ok": all(s.get("狀態") == "OK" for s in steps if not str(s.get("步驟", "")).startswith("13.")),
        }
        readiness_ok, readiness, _ = read_json(base / RECOMMENDATION_READINESS_FILE.name)
        if readiness_ok:
            payload["recommendation_readiness"] = readiness
        write_json(base / GLOBAL_UPDATE_STATUS_FILE.name, payload)
        save_global_update_settings(cfg, base)
        return payload
    finally:
        try:
            GLOBAL_UPDATE_LOCK_FILE.unlink(missing_ok=True)
        except Exception:
            pass

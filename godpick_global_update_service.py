# -*- coding: utf-8 -*-
from __future__ import annotations

"""股神全模組資料檢查與一鍵更新服務 v140

設計原則：
- 只補缺檔、補缺欄、更新必要快取；不刪除既有資料，不用假資料覆蓋。
- 依照股神推薦需要的資料順序更新：核心檔案 -> 主檔 -> 大盤 -> 官方因子 -> 自選股同步 -> 推薦紀錄/清單績效 -> 健康檢查。
- 所有步驟獨立回報成功/失敗；單一步驟失敗不會中斷後續安全步驟。
"""

import importlib
import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

BASE_DIR = Path(__file__).resolve().parent
GLOBAL_UPDATE_STATUS_FILE = BASE_DIR / "godpick_global_update_status.json"
GLOBAL_UPDATE_SETTINGS_FILE = BASE_DIR / "data" / "config" / "godpick_global_update_settings.json"

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
    "dashboard_table_settings.json": {"version": "v140", "updated_at": "", "profiles": {}},
    "hk_chart_settings.json": {"version": "v140", "updated_at": "", "settings": {}},
    "godpick_user_settings.json": {},
    "godpick_record_ui_config.json": {},
    "godpick_management_ui_config.json": {},
    "godpick_ui_font_settings.json": {},
    str(GLOBAL_UPDATE_SETTINGS_FILE.relative_to(BASE_DIR)): {},
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


def find_updated_at(data: Any) -> str:
    if isinstance(data, dict):
        for key in ["updated_at", "saved_at", "last_update", "time", "date", "data_date"]:
            if data.get(key):
                return str(data.get(key))
        records = iter_records(data)
        if records:
            return find_updated_at(records[0])
    if isinstance(data, list) and data:
        for item in data[:5]:
            if isinstance(item, dict):
                for key in ["updated_at", "saved_at", "最新更新時間", "資料更新時間", "官方因子更新時間", "推薦日期", "建立時間"]:
                    if item.get(key):
                        return str(item.get(key))
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
    return {
        "檔案": file_name,
        "使用模組": modules,
        "用途": meaning,
        "狀態": status,
        "型態/筆數": shape_text(data) if ok else "",
        "最後更新": updated,
        "大小KB": round(path.stat().st_size / 1024, 2) if path.exists() else 0,
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
        module_rows.append(dict(item))
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
            payload.setdefault("version", "v140")
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
        "version": "v140",
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
    except Exception as exc:
        ok = False
        msg = f"{type(exc).__name__}: {exc}"
        detail = {"error": msg}
    return {
        "步驟": name,
        "狀態": "OK" if ok else "失敗",
        "耗時秒": round(time.time() - start, 2),
        "說明": msg,
        "明細": detail,
    }


def step_core_files(base: Path) -> dict[str, Any]:
    rows = ensure_required_json_files(base)
    setting_rows = ensure_persistent_setting_files(base)
    return {"ok": True, "message": f"核心檔案檢查 {len(rows)} 項、設定檔檢查 {len(setting_rows)} 項", "rows": rows, "setting_rows": setting_rows}


def step_repair_schema(base: Path) -> dict[str, Any]:
    try:
        svc = importlib.import_module("godpick_system_health_service")
        repair = svc.full_safe_repair(base)
        schema_rows = repair.get("schema_rows", []) if isinstance(repair, dict) else []
        return {"ok": True, "message": f"安全修復完成，欄位檢查 {len(schema_rows)} 項", "repair": repair}
    except Exception as exc:
        return {"ok": False, "message": f"安全修復失敗：{type(exc).__name__}: {exc}"}


def step_stock_master(base: Path) -> dict[str, Any]:
    with pushd(base):
        svc = importlib.import_module("stock_master_service")
        df, logs = svc.refresh_stock_master()
    row_count = int(len(df)) if df is not None else 0
    ok = row_count >= 100
    return {"ok": ok, "message": f"股票主檔更新完成：{row_count} 筆" if ok else f"股票主檔更新異常：{row_count} 筆", "logs": logs[-20:] if isinstance(logs, list) else logs, "row_count": row_count}


def step_macro(base: Path) -> dict[str, Any]:
    with pushd(base):
        svc = importlib.import_module("macro_startup_service")
        if hasattr(svc, "_run_fast_update"):
            result = svc._run_fast_update(sync_github=True)
        else:
            result = svc.ensure_macro_startup_update(ttl_seconds=0)
    ok = bool(result.get("ok")) if isinstance(result, dict) else False
    msg = str(result.get("message") or result.get("short_message") or "大盤更新完成") if isinstance(result, dict) else "大盤更新完成"
    return {"ok": ok, "message": msg, "result": result}


def step_official_factors(base: Path, push_github: bool = True) -> dict[str, Any]:
    with pushd(base):
        svc = importlib.import_module("godpick_system_health_service")
        result = svc.run_official_factor_update_once(svc.load_schedule_settings(), push_github=push_github)
    return {"ok": bool(result.get("ok")), "message": str(result.get("message", "官方因子更新完成")), "result": result}


def step_watchlist(base: Path) -> dict[str, Any]:
    return sync_watchlist_runtime_files(base)


def step_perf(base: Path, *, process_all: bool = False, max_records: int = 300, batch_limit: int = 80, stale_minutes: int = 30) -> dict[str, Any]:
    with pushd(base):
        svc = importlib.import_module("godpick_perf_fast_update_v77")
        result = svc.update_recommendation_perf_fast_v77(
            json_files=["godpick_records.json", "godpick_recommend_list.json"],
            max_records=int(max_records),
            batch_limit=int(batch_limit),
            max_workers=12,
            stale_minutes=int(stale_minutes),
            process_all=bool(process_all),
        )
    ok = isinstance(result, dict) and int(result.get("fail", 0) or 0) == 0
    msg = f"績效更新：候選 {result.get('candidates', 0)} / 成功 {result.get('success', 0)} / 失敗 {result.get('fail', 0)} / 更新檔 {len(result.get('updated_files', []))}"
    return {"ok": ok or int(result.get("success", 0) or 0) > 0 or int(result.get("candidates", 0) or 0) == 0, "message": msg, "result": result}


def step_health_snapshot(base: Path) -> dict[str, Any]:
    status = check_all_module_data_status(base)
    return {"ok": int(status.get("summary", {}).get("異常", 0)) == 0, "message": f"全模組資料檢查完成：異常 {status['summary']['異常']} / 注意 {status['summary']['注意']}", "status": status}


def load_global_update_settings(base_dir: Path | None = None) -> dict[str, Any]:
    base = Path(base_dir or BASE_DIR)
    ok, data, _ = read_json(base / GLOBAL_UPDATE_SETTINGS_FILE.relative_to(BASE_DIR))
    defaults = {
        "version": "v140",
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


def run_global_update(base_dir: Path | None = None, settings: dict[str, Any] | None = None) -> dict[str, Any]:
    base = Path(base_dir or BASE_DIR)
    cfg = load_global_update_settings(base)
    if settings:
        cfg.update(settings)
    steps: list[dict[str, Any]] = []

    steps.append(run_step("1. 核心檔案與永久設定檢查", lambda: step_core_files(base)))
    if cfg.get("repair_schema", True):
        steps.append(run_step("2. 安全補欄/補缺檔", lambda: step_repair_schema(base)))
    if cfg.get("update_stock_master", True):
        steps.append(run_step("3. 股票主檔更新", lambda: step_stock_master(base)))
    if cfg.get("update_macro", True):
        steps.append(run_step("4. 大盤快照與股神橋接更新", lambda: step_macro(base)))
    if cfg.get("update_official_factors", True):
        steps.append(run_step("5. 官方因子快取更新", lambda: step_official_factors(base, push_github=bool(cfg.get("push_github", True)))))
    if cfg.get("update_watchlist_runtime", True):
        steps.append(run_step("6. 自選股 runtime 同步", lambda: step_watchlist(base)))
    if cfg.get("update_performance", True):
        steps.append(run_step("7. 推薦紀錄/推薦清單最新價與績效更新", lambda: step_perf(
            base,
            process_all=bool(cfg.get("process_all_performance", False)),
            max_records=int(cfg.get("max_records", 300) or 300),
            batch_limit=int(cfg.get("batch_limit", 80) or 80),
            stale_minutes=int(cfg.get("stale_minutes", 30) or 30),
        )))
    steps.append(run_step("8. 全模組資料狀態複檢", lambda: step_health_snapshot(base)))

    payload = {
        "version": "v140",
        "updated_at": now_text(),
        "settings": cfg,
        "steps": steps,
        "ok": all(s.get("狀態") == "OK" for s in steps if not str(s.get("步驟", "")).startswith("8.")),
    }
    write_json(base / GLOBAL_UPDATE_STATUS_FILE.name, payload)
    save_global_update_settings(cfg, base)
    return payload

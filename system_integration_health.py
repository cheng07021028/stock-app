# -*- coding: utf-8 -*-
from __future__ import annotations

"""
系統串聯健康檢查服務 v41

用途：
- 檢查 0_大盤趨勢 -> 7_股神推薦 -> 8_股神推薦紀錄 -> 10_推薦清單 -> 首頁/儀表板 的 JSON 串聯。
- 不主動抓網路資料，不拖慢頁面。
- JSON 缺檔時可由診斷頁手動建立預設空檔。
"""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

CORE_JSON_DEFAULTS: Dict[str, Any] = {
    "market_snapshot.json": {},
    "macro_mode_bridge.json": {},
    "macro_trend_records.json": [],
    "godpick_latest_recommendations.json": [],
    "godpick_records.json": [],
    "godpick_recommend_list.json": [],
    "godpick_user_settings.json": {},
    "godpick_record_ui_config.json": {},
    "watchlist.json": {},
    "stock_master_cache.json": {},
}

MARKET_REQUIRED_KEYS = [
    "market_score",
    "market_trend",
    "market_risk_level",
    "risk_gate",
    "position_hint",
    "recommendation_adjustment",
    "volume_status",
    "data_quality",
    "freshness",
    "market_session",
    "market_session_label",
    "market_session_usable",
    "godpick_market_effect",
    "data_diagnostics",
]

GODPICK_RESULT_MARKET_KEYS = [
    "大盤橋接分數",
    "大盤橋接狀態",
    "大盤橋接風控",
    "大盤交易時段",
    "大盤交易時段可用",
    "大盤資料品質",
    "大盤影響加減分",
    "大盤影響說明",
    "大盤資料診斷摘要",
]

PAGE_REQUIRED = {
    "0 大盤趨勢": ["0_#U5927#U76e4#U8d70#U52e2.py", "01_market_trend.py"],
    "1 儀表板": ["1_#U5100#U8868#U677f.py"],
    "7 股神推薦": ["7_#U80a1#U795e#U63a8#U85a6.py"],
    "8 股神推薦紀錄": ["8_#U80a1#U795e#U63a8#U85a6#U7d00#U9304.py"],
    "10 推薦清單": ["10_#U63a8#U85a6#U6e05#U55ae.py"],
    "11 資料診斷": ["11_#U8cc7#U6599#U8a3a#U65b7.py"],
}


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_read_json(path: Path) -> Tuple[bool, Any, str]:
    try:
        if not path.exists():
            return False, None, "檔案不存在"
        text = path.read_text(encoding="utf-8-sig")
        if not text.strip():
            return False, None, "檔案空白"
        return True, json.loads(text), ""
    except Exception as e:
        return False, None, f"{type(e).__name__}: {e}"


def safe_write_json(path: Path, data: Any) -> Tuple[bool, str]:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + f".tmp_{int(time.time())}")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
        return True, "OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def json_shape(data: Any) -> str:
    if isinstance(data, list):
        return f"list / {len(data)} 筆"
    if isinstance(data, dict):
        return f"dict / {len(data)} keys"
    if data is None:
        return "None"
    return type(data).__name__


def get_first_record(data: Any) -> Dict[str, Any]:
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    if isinstance(data, dict):
        for key in ["data", "records", "items", "recommendations"]:
            val = data.get(key)
            if isinstance(val, list) and val and isinstance(val[0], dict):
                return val[0]
        return data
    return {}


def normalize_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).replace(",", "").replace("%", "").strip()
    if s in {"", "-", "—", "None", "nan", "NaN"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def validate_file_matrix(base_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for name, default in CORE_JSON_DEFAULTS.items():
        path = base_dir / name
        ok, data, err = safe_read_json(path)
        rows.append({
            "檔案": name,
            "必要性": "必要" if name in {"watchlist.json", "stock_master_cache.json"} else "串聯建議",
            "狀態": "OK" if ok else "缺少/錯誤",
            "型態": json_shape(data) if ok else "",
            "大小KB": round(path.stat().st_size / 1024, 2) if path.exists() and path.is_file() else "",
            "錯誤": err,
            "路徑": str(path),
        })
    return rows


def ensure_missing_json_files(base_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for name, default in CORE_JSON_DEFAULTS.items():
        path = base_dir / name
        if path.exists():
            rows.append({"檔案": name, "動作": "略過", "結果": "已存在"})
            continue
        ok, msg = safe_write_json(path, default)
        rows.append({"檔案": name, "動作": "建立預設空檔", "結果": "OK" if ok else msg})
    return rows


def validate_market_snapshot(base_dir: Path) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    path = base_dir / "market_snapshot.json"
    ok, data, err = safe_read_json(path)
    rows: List[Dict[str, Any]] = []
    if not ok:
        return ([{
            "檢查項目": "market_snapshot.json",
            "狀態": "失敗",
            "說明": err,
            "建議": "先到 0_大盤趨勢 按立即寫入 market_snapshot。",
        }], {})

    if not isinstance(data, dict):
        return ([{
            "檢查項目": "market_snapshot.json 型態",
            "狀態": "失敗",
            "說明": json_shape(data),
            "建議": "market_snapshot.json 應為 dict。",
        }], {})

    missing = [k for k in MARKET_REQUIRED_KEYS if k not in data]
    rows.append({
        "檢查項目": "v36/v37 大盤必要欄位",
        "狀態": "OK" if not missing else "缺欄位",
        "說明": "全部存在" if not missing else "缺：" + "、".join(missing),
        "建議": "OK" if not missing else "請用 0_大盤趨勢 v36 重新寫入 market_snapshot.json。",
    })

    score = normalize_number(data.get("market_score"))
    score_ok = score is not None and 0 <= score <= 100
    rows.append({
        "檢查項目": "market_score 範圍",
        "狀態": "OK" if score_ok else "異常",
        "說明": str(data.get("market_score")),
        "建議": "OK" if score_ok else "大盤分數應為 0~100。",
    })

    session_ok = data.get("market_session") not in {None, ""}
    rows.append({
        "檢查項目": "交易時段欄位",
        "狀態": "OK" if session_ok else "缺少",
        "說明": str(data.get("market_session_label") or data.get("market_session")),
        "建議": "OK" if session_ok else "請確認 0_大盤趨勢 v36 欄位有寫入。",
    })

    diag = data.get("data_diagnostics")
    rows.append({
        "檢查項目": "資料來源診斷",
        "狀態": "OK" if isinstance(diag, (list, dict)) else "缺少/格式異常",
        "說明": json_shape(diag),
        "建議": "OK" if isinstance(diag, (list, dict)) else "應由 0_大盤趨勢寫入 data_diagnostics。",
    })

    return rows, data


def validate_bridge_files(base_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for name in ["market_snapshot.json", "macro_mode_bridge.json", "macro_trend_records.json"]:
        ok, data, err = safe_read_json(base_dir / name)
        rows.append({
            "檔案": name,
            "狀態": "OK" if ok else "缺少/錯誤",
            "型態": json_shape(data) if ok else "",
            "錯誤": err,
            "用途": {
                "market_snapshot.json": "7_股神推薦優先讀取的大盤快照",
                "macro_mode_bridge.json": "舊版大盤橋接備援",
                "macro_trend_records.json": "大盤風控歷史圖資料",
            }.get(name, ""),
        })
    return rows


def validate_recommendation_market_fields(base_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    targets = [
        ("godpick_latest_recommendations.json", "7_股神推薦最新結果"),
        ("godpick_records.json", "8_股神推薦紀錄"),
        ("godpick_recommend_list.json", "10_推薦清單"),
    ]
    for file_name, purpose in targets:
        ok, data, err = safe_read_json(base_dir / file_name)
        if not ok:
            rows.append({
                "檔案": file_name,
                "用途": purpose,
                "狀態": "缺少/錯誤",
                "筆數": "",
                "大盤欄位狀態": "無法檢查",
                "缺少欄位": err,
                "建議": "若尚未產生紀錄可忽略；若已有推薦，請確認寫入功能。",
            })
            continue
        first = get_first_record(data)
        count = len(data) if isinstance(data, list) else (len(data.get("data", [])) if isinstance(data, dict) and isinstance(data.get("data"), list) else "dict")
        if not first:
            rows.append({
                "檔案": file_name,
                "用途": purpose,
                "狀態": "OK",
                "筆數": count,
                "大盤欄位狀態": "尚無樣本",
                "缺少欄位": "",
                "建議": "產生推薦後再檢查大盤欄位是否有帶入。",
            })
            continue
        missing = [k for k in GODPICK_RESULT_MARKET_KEYS if k not in first]
        rows.append({
            "檔案": file_name,
            "用途": purpose,
            "狀態": "OK",
            "筆數": count,
            "大盤欄位狀態": "OK" if not missing else "缺欄位",
            "缺少欄位": "、".join(missing),
            "建議": "OK" if not missing else "請確認 7_股神推薦 v37/v40 寫入欄位與 8/10 v38/v40 顯示欄位一致。",
        })
    return rows


def validate_pages(base_dir: Path) -> List[Dict[str, Any]]:
    pages_dir = base_dir / "pages"
    rows: List[Dict[str, Any]] = []
    for module_name, candidates in PAGE_REQUIRED.items():
        found = [name for name in candidates if (pages_dir / name).exists()]
        rows.append({
            "模組": module_name,
            "狀態": "OK" if found else "缺少",
            "符合檔案": "、".join(found),
            "候選檔名": "、".join(candidates),
            "建議": "OK" if found else "請確認 pages 內檔名是否被改掉。",
        })
    return rows


def build_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(rows)
    bad_words = {"失敗", "缺少", "異常", "缺欄位", "缺少/錯誤"}
    bad = 0
    warn = 0
    for row in rows:
        status = str(row.get("狀態", "")) + str(row.get("大盤欄位狀態", ""))
        if any(w in status for w in bad_words):
            bad += 1
        elif "尚無樣本" in status or "無法檢查" in status:
            warn += 1
    ok = total - bad - warn
    if bad == 0 and warn == 0:
        level = "OK"
    elif bad == 0:
        level = "注意"
    else:
        level = "需修正"
    return {"總項目": total, "正常": ok, "注意": warn, "異常": bad, "整體狀態": level, "檢查時間": now_text()}


def run_full_integration_check(base_dir: Path) -> Dict[str, Any]:
    base_dir = Path(base_dir)
    file_rows = validate_file_matrix(base_dir)
    market_rows, market_snapshot = validate_market_snapshot(base_dir)
    bridge_rows = validate_bridge_files(base_dir)
    rec_rows = validate_recommendation_market_fields(base_dir)
    page_rows = validate_pages(base_dir)

    all_rows: List[Dict[str, Any]] = []
    for group_name, rows in [
        ("核心 JSON", file_rows),
        ("大盤快照", market_rows),
        ("橋接檔", bridge_rows),
        ("推薦/紀錄/清單", rec_rows),
        ("頁面檔案", page_rows),
    ]:
        for row in rows:
            merged = {"群組": group_name}
            merged.update(row)
            all_rows.append(merged)

    summary = build_summary(all_rows)
    return {
        "summary": summary,
        "file_rows": file_rows,
        "market_rows": market_rows,
        "bridge_rows": bridge_rows,
        "recommendation_rows": rec_rows,
        "page_rows": page_rows,
        "all_rows": all_rows,
        "market_snapshot": market_snapshot,
    }

# -*- coding: utf-8 -*-
from __future__ import annotations

"""
系統串聯健康檢查服務 v43

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
    "0 大盤趨勢": [
        "0_#U5927#U76e4#U8d70#U52e2.py", "0_大盤走勢.py", "0_大盤趨勢.py",
        "01_大盤趨勢.py", "01_market_trend.py", "market_trend_01.py",
    ],
    "1 儀表板": [
        "1_#U5100#U8868#U677f.py", "1_儀表板.py", "01_儀表板.py", "dashboard_1.py",
    ],
    "7 股神推薦": [
        "7_#U80a1#U795e#U63a8#U85a6.py", "7_股神推薦.py", "07_股神推薦.py",
    ],
    "8 股神推薦紀錄": [
        "8_#U80a1#U795e#U63a8#U85a6#U7d00#U9304.py", "8_股神推薦紀錄.py", "08_股神推薦紀錄.py",
    ],
    "10 推薦清單": [
        "10_#U63a8#U85a6#U6e05#U55ae.py", "10_推薦清單.py", "10_recommend_list.py",
    ],
    "11 資料診斷": [
        "11_#U8cc7#U6599#U8a3a#U65b7.py", "11_資料診斷.py", "11_data_diagnostics.py",
    ],
}

PAGE_FUZZY_RULES = {
    "0 大盤趨勢": ["0_", "01_"],
    "1 儀表板": ["1_", "01_"],
    "7 股神推薦": ["7_", "07_"],
    "8 股神推薦紀錄": ["8_", "08_"],
    "10 推薦清單": ["10_"],
    "11 資料診斷": ["11_"],
}

PAGE_KEYWORDS = {
    "0 大盤趨勢": ["大盤", "走勢", "趨勢", "market", "trend", "#u5927#u76e4", "#u8d70#u52e2"],
    "1 儀表板": ["儀表", "dashboard", "#u5100#u8868#u677f"],
    "7 股神推薦": ["股神", "推薦", "godpick", "#u80a1#u795e#u63a8#u85a6"],
    "8 股神推薦紀錄": ["股神", "推薦", "紀錄", "record", "history", "#u7d00#u9304"],
    "10 推薦清單": ["推薦", "清單", "recommend", "list", "#u63a8#u85a6#u6e05#u55ae"],
    "11 資料診斷": ["資料", "診斷", "diagnostics", "health", "#u8cc7#u6599#u8a3a#u65b7"],
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


def _page_name_norm(name: str) -> str:
    return str(name).strip().lower().replace(" ", "")


def _page_matches(module_name: str, file_name: str, candidates: List[str]) -> bool:
    norm = _page_name_norm(file_name)
    candidate_norms = {_page_name_norm(x) for x in candidates}
    if norm in candidate_norms:
        return True

    # Streamlit / GitHub 有時會保留中文，有時會變成 #Uxxxx。
    # 這裡改用「頁碼前綴 + 關鍵字」做寬鬆判斷，避免誤報缺少。
    prefixes = PAGE_FUZZY_RULES.get(module_name, [])
    keywords = PAGE_KEYWORDS.get(module_name, [])
    prefix_ok = any(norm.startswith(_page_name_norm(p)) for p in prefixes)
    keyword_ok = any(_page_name_norm(k) in norm for k in keywords)

    # 針對 7 / 8：都含股神推薦，8 必須再含紀錄；7 則排除紀錄。
    if module_name == "7 股神推薦" and prefix_ok:
        return keyword_ok and ("紀錄" not in file_name and "#u7d00#u9304" not in norm and "record" not in norm and "history" not in norm)
    if module_name == "8 股神推薦紀錄" and prefix_ok:
        return keyword_ok and ("紀錄" in file_name or "#u7d00#u9304" in norm or "record" in norm or "history" in norm)

    return prefix_ok and keyword_ok


def validate_pages(base_dir: Path) -> List[Dict[str, Any]]:
    pages_dir = base_dir / "pages"
    rows: List[Dict[str, Any]] = []

    if not pages_dir.exists():
        for module_name, candidates in PAGE_REQUIRED.items():
            rows.append({
                "模組": module_name,
                "狀態": "缺少",
                "符合檔案": "",
                "候選檔名": "、".join(candidates),
                "建議": f"找不到 pages 資料夾：{pages_dir}",
            })
        return rows

    actual_files = sorted([p.name for p in pages_dir.glob("*.py")])

    for module_name, candidates in PAGE_REQUIRED.items():
        found = [name for name in actual_files if _page_matches(module_name, name, candidates)]
        rows.append({
            "模組": module_name,
            "狀態": "OK" if found else "缺少",
            "符合檔案": "、".join(found),
            "候選檔名": "、".join(candidates),
            "建議": "OK" if found else "請確認 pages 內檔名；v43 已支援中文檔名、#U 編碼檔名、英文備用檔名。",
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


# ============================================================
# v42：推薦結果大盤欄位自動補齊 / 舊資料相容修復
# ============================================================

EXTRA_GODPICK_MARKET_KEYS = [
    "大盤橋接加權",
    "大盤橋接策略",
    "大盤橋接更新時間",
]


def _compact_text(value: Any, max_len: int = 220) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        s = value
    else:
        try:
            s = json.dumps(value, ensure_ascii=False)
        except Exception:
            s = str(value)
    s = s.replace("\n", " ").strip()
    return s[:max_len] + ("..." if len(s) > max_len else "")


def _extract_market_effect(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    effect = snapshot.get("godpick_market_effect")
    if isinstance(effect, dict):
        return effect
    return {}


def _summarize_diagnostics(snapshot: Dict[str, Any]) -> str:
    diag = snapshot.get("data_diagnostics")
    if isinstance(diag, list):
        parts = []
        for item in diag[:8]:
            if isinstance(item, dict):
                name = item.get("項目") or item.get("name") or item.get("資料項目") or item.get("source") or "資料源"
                status = item.get("狀態") or item.get("status") or item.get("success") or ""
                freshness = item.get("鮮度") or item.get("freshness") or ""
                parts.append(f"{name}:{status}{('/' + str(freshness)) if freshness else ''}")
            else:
                parts.append(str(item))
        return "；".join(parts)
    if isinstance(diag, dict):
        parts = []
        for k, v in list(diag.items())[:8]:
            if isinstance(v, dict):
                status = v.get("狀態") or v.get("status") or v.get("ok") or ""
                parts.append(f"{k}:{status}")
            else:
                parts.append(f"{k}:{v}")
        return "；".join(parts)
    return _compact_text(diag)


def build_market_field_defaults_from_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """把 0_大盤趨勢 market_snapshot 轉成 7/8/10 每筆紀錄可保存的大盤欄位。"""
    effect = _extract_market_effect(snapshot)
    session_label = snapshot.get("market_session_label") or snapshot.get("market_session") or ""
    score_delta = (
        effect.get("score_delta")
        or effect.get("recommend_score_delta")
        or effect.get("大盤影響加減分")
        or snapshot.get("recommendation_adjustment")
        or ""
    )
    effect_text = (
        effect.get("effect_text")
        or effect.get("summary")
        or effect.get("description")
        or snapshot.get("trend_comment")
        or snapshot.get("market_bias")
        or ""
    )
    bridge_weight = (
        effect.get("weight")
        or effect.get("market_weight")
        or effect.get("大盤橋接加權")
        or ""
    )
    strategy = (
        effect.get("strategy")
        or effect.get("suggestion")
        or snapshot.get("position_hint")
        or snapshot.get("risk_gate")
        or ""
    )
    return {
        "大盤橋接分數": snapshot.get("market_score", ""),
        "大盤橋接狀態": snapshot.get("market_trend", ""),
        "大盤橋接風控": snapshot.get("risk_gate", snapshot.get("market_risk_level", "")),
        "大盤橋接加權": bridge_weight,
        "大盤橋接策略": strategy,
        "大盤橋接更新時間": snapshot.get("updated_at", ""),
        "大盤交易時段": session_label,
        "大盤交易時段可用": snapshot.get("market_session_usable", ""),
        "大盤資料品質": snapshot.get("data_quality", ""),
        "大盤影響加減分": score_delta,
        "大盤影響說明": effect_text,
        "大盤資料診斷摘要": _summarize_diagnostics(snapshot),
    }


def _iter_records_mutable(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ["data", "records", "items", "recommendations"]:
            val = data.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    return []


def repair_recommendation_market_fields(base_dir: Path, overwrite_blank: bool = True) -> Dict[str, Any]:
    """
    v42：補齊舊推薦資料的大盤欄位。
    - 不刪除任何推薦紀錄。
    - 不覆蓋已有非空值。
    - 只對缺少欄位或空白欄位補入 market_snapshot 推導值。
    """
    base_dir = Path(base_dir)
    ok, snapshot, err = safe_read_json(base_dir / "market_snapshot.json")
    if not ok or not isinstance(snapshot, dict):
        return {
            "ok": False,
            "message": f"market_snapshot.json 無法讀取：{err}",
            "rows": [],
        }

    defaults = build_market_field_defaults_from_snapshot(snapshot)
    target_keys = list(dict.fromkeys(GODPICK_RESULT_MARKET_KEYS + EXTRA_GODPICK_MARKET_KEYS + list(defaults.keys())))
    files = [
        "godpick_latest_recommendations.json",
        "godpick_records.json",
        "godpick_recommend_list.json",
    ]
    rows: List[Dict[str, Any]] = []
    total_fixed_records = 0
    total_added_fields = 0

    for file_name in files:
        path = base_dir / file_name
        file_ok, data, file_err = safe_read_json(path)
        if not file_ok:
            rows.append({"檔案": file_name, "結果": "略過", "原因": file_err, "修復筆數": 0, "補欄位數": 0})
            continue

        records = _iter_records_mutable(data)
        fixed_records = 0
        added_fields = 0
        for rec in records:
            changed = False
            for key in target_keys:
                default_value = defaults.get(key, "")
                if key not in rec:
                    rec[key] = default_value
                    added_fields += 1
                    changed = True
                elif overwrite_blank and str(rec.get(key, "")).strip() in {"", "—", "None", "nan"}:
                    rec[key] = default_value
                    added_fields += 1
                    changed = True
            if changed:
                fixed_records += 1

        if fixed_records > 0:
            write_ok, msg = safe_write_json(path, data)
            rows.append({
                "檔案": file_name,
                "結果": "OK" if write_ok else "寫入失敗",
                "原因": msg,
                "修復筆數": fixed_records,
                "補欄位數": added_fields,
            })
            if write_ok:
                total_fixed_records += fixed_records
                total_added_fields += added_fields
        else:
            rows.append({"檔案": file_name, "結果": "無需修復", "原因": "欄位已完整或尚無資料", "修復筆數": 0, "補欄位數": 0})

    return {
        "ok": True,
        "message": f"完成：修復 {total_fixed_records} 筆，補入 {total_added_fields} 個欄位。",
        "rows": rows,
        "defaults": defaults,
    }


def validate_recommendation_market_fields_v42(base_dir: Path) -> List[Dict[str, Any]]:
    """v42：檢查 7/8/10 推薦資料大盤欄位，並提示可用一鍵補欄位修復。"""
    rows = validate_recommendation_market_fields(base_dir)
    for row in rows:
        if row.get("大盤欄位狀態") == "缺欄位":
            row["建議"] = "可按 v42 一鍵補齊舊推薦資料大盤欄位；之後新推薦請用 7_股神推薦 v40+。"
    return rows


# 覆蓋原本 run_full_integration_check，讓 v42 檢查結果使用新建議文字。
def run_full_integration_check(base_dir: Path) -> Dict[str, Any]:
    base_dir = Path(base_dir)
    file_rows = validate_file_matrix(base_dir)
    market_rows, market_snapshot = validate_market_snapshot(base_dir)
    bridge_rows = validate_bridge_files(base_dir)
    rec_rows = validate_recommendation_market_fields_v42(base_dir)
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
        "v42_repair_available": True,
    }

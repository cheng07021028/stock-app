# -*- coding: utf-8 -*-
"""V112 系統健康檢查與一鍵修復服務

設計目的：
- 不重寫 07/10/8/14，只檢查串接檔、欄位、官方因子快取與排程設定。
- 修復採保守策略：先備份，再補缺檔/缺欄，不刪資料、不覆蓋既有非空值。
- 官方因子自動更新採 GitHub Actions / 手動更新雙路徑；Streamlit 頁面不做背景常駐任務。
"""
from __future__ import annotations

import base64
import json
import math
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

try:
    import requests
except Exception:  # GitHub 永久保存不是必要功能；未安裝 requests 時仍保留本機保存
    requests = None  # type: ignore

BASE_DIR = Path(__file__).resolve().parent
BACKUP_DIR = BASE_DIR / "backups" / "system_health"
SCHEDULE_SETTINGS_PATH = BASE_DIR / "data" / "config" / "official_factor_schedule_settings.json"

CORE_JSON_DEFAULTS: dict[str, Any] = {
    "market_snapshot.json": {},
    "macro_mode_bridge.json": {},
    "macro_trend_records.json": [],
    "godpick_latest_recommendations.json": [],
    "godpick_records.json": [],
    "godpick_recommend_list.json": [],
    "godpick_user_settings.json": {},
    "godpick_record_ui_config.json": {},
    "godpick_management_ui_config.json": {},
    "watchlist.json": {},
    "stock_master_cache.json": {},
    "official_factors_cache.json": {"version": "empty", "updated_at": "", "records": [], "diagnostics": []},
}

REQUIRED_PAGES: dict[str, list[str]] = {
    "07 股神推薦": ["7_股神推薦.py", "7_#U80a1#U795e#U63a8#U85a6.py"],
    "08 股神推薦紀錄": ["8_股神推薦紀錄.py", "8_#U80a1#U795e#U63a8#U85a6#U7d00#U9304.py"],
    "10 推薦清單": ["10_推薦清單.py", "10_#U63a8#U85a6#U6e05#U55ae.py"],
    "14 股神權重校正": ["14_股神權重校正.py", "14_#U80a1#U795e#U6b0a#U91cd#U6821#U6b63.py"],
    "16 官方因子快取中心": ["16_官方因子快取中心.py"],
    "17 系統健康檢查": ["17_系統健康檢查.py"],
}

NIGHT_FIELDS = [
    "夜間股神總分", "隔日實戰排序分", "隔日進場分數", "波段潛力分數",
    "進場型態_隔日", "隔日建議動作", "預估進場點", "回測承接價",
    "突破確認價_隔日", "停損價_隔日", "第一壓力價", "夜間股神建議",
    "隔日作戰策略", "資料完整度",
]

HIT_TRACK_FIELDS = [
    "作戰追蹤狀態", "進場點命中", "進場點命中日期", "突破價命中", "突破價命中日期",
    "停損價觸發", "停損價觸發日期", "第一壓力命中", "第一壓力命中日期",
    "隔日最高漲幅%", "3日最高漲幅%", "5日最高漲幅%", "10日最高漲幅%",
    "隔日最低回撤%", "3日最低回撤%", "5日最低回撤%", "10日最低回撤%",
    "作戰命中摘要", "作戰追蹤資料源", "作戰追蹤更新時間",
]

OFFICIAL_FACTOR_FIELDS = [
    "外資近5日買賣超", "投信近5日買賣超", "三大法人近5日合計", "法人連買天數",
    "法人籌碼官方分數", "月營收YoY%", "月營收MoM%", "累計營收YoY%",
    "營收成長官方分數", "PER本益比", "PBR股價淨值比", "股利殖利率%", "估算EPS",
    "官方估值風險分數", "官方基本面成長分數", "官方因子總分", "官方資料完整度",
    "官方因子資料狀態", "官方因子更新時間",
]


V139_DYNAMIC_FIELDS = [
    "股神推薦層級", "候補等級", "是否主要顯示", "主表篩選", "股神輸出排序",
    "候補排序分", "股神實戰建議", "限制原因", "主表篩選說明",
    "族群名稱", "資金流熱門族群", "族群熱度排名", "族群資金流分數",
    "族群流動性分數", "族群樣本數", "族群判斷依據", "大盤趨勢模式",
    "成交額百萬", "20日均成交額百萬", "流動性等級", "實戰版本",
    "V139顯示分區", "V139主升起漲候選", "V139動態熱門族群版",
]

SCHEMA_TARGETS: dict[str, list[str]] = {
    "godpick_latest_recommendations.json": NIGHT_FIELDS + OFFICIAL_FACTOR_FIELDS + V139_DYNAMIC_FIELDS,
    "godpick_recommend_list.json": NIGHT_FIELDS + HIT_TRACK_FIELDS + OFFICIAL_FACTOR_FIELDS + V139_DYNAMIC_FIELDS,
    "godpick_records.json": NIGHT_FIELDS + HIT_TRACK_FIELDS + OFFICIAL_FACTOR_FIELDS + V139_DYNAMIC_FIELDS,
}

DEFAULT_SCHEDULE_SETTINGS: dict[str, Any] = {
    "enabled": True,
    "timezone": "Asia/Taipei",
    "times": ["23:00"],
    "weekdays_only": True,
    "market_filter": "全部",
    "limit": 0,
    "include_institutional": True,
    "include_revenue": True,
    "include_valuation": True,
    "preserve_old_cache": True,
    "auto_commit_github_actions": True,
    "quick_mode": True,
    "max_runtime_seconds": 75,
    "max_requests": 48,
    "finmind_bulk_only": True,
    "finmind_max_stocks": 30,
    "last_saved_at": "",
    "note": "GitHub Actions workflow 使用 UTC 15:00，約等於台灣 23:00。",
}


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def json_safe(obj: Any) -> Any:
    if obj is None:
        return None
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    if isinstance(obj, pd.DataFrame):
        return [json_safe(r) for r in obj.to_dict(orient="records")]
    if isinstance(obj, pd.Series):
        return {str(k): json_safe(v) for k, v in obj.to_dict().items()}
    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [json_safe(v) for v in obj]
    if hasattr(obj, "item"):
        try:
            return json_safe(obj.item())
        except Exception:
            pass
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, (str, int, bool)):
        return obj
    return safe_str(obj)


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
        tmp = path.with_suffix(path.suffix + ".tmp_v112")
        tmp.write_text(json.dumps(json_safe(data), ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
        return True, "OK"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def backup_file(path: Path) -> str:
    if not path.exists():
        return ""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = BACKUP_DIR / f"{path.name}.{stamp}.bak"
    shutil.copy2(path, dst)
    return str(dst)


def backup_many(paths: Iterable[Path]) -> list[dict[str, Any]]:
    rows = []
    for p in paths:
        if p.exists():
            try:
                dst = backup_file(p)
                rows.append({"檔案": p.name, "備份": dst, "狀態": "OK"})
            except Exception as exc:
                rows.append({"檔案": p.name, "備份": "", "狀態": f"失敗：{exc}"})
    return rows


def _shape(data: Any) -> str:
    if isinstance(data, list):
        return f"list / {len(data)} 筆"
    if isinstance(data, dict):
        if isinstance(data.get("records"), list):
            return f"dict.records / {len(data.get('records', []))} 筆"
        return f"dict / {len(data)} keys"
    if data is None:
        return "None"
    return type(data).__name__


def _iter_records(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ["records", "data", "items", "recommendations"]:
            val = data.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
    return []


def run_health_check(base_dir: Path | None = None) -> dict[str, Any]:
    base = Path(base_dir or BASE_DIR)
    rows: list[dict[str, Any]] = []

    for name, default in CORE_JSON_DEFAULTS.items():
        path = base / name
        ok, data, err = read_json(path)
        size_kb = round(path.stat().st_size / 1024, 2) if path.exists() else 0
        rows.append({
            "群組": "核心檔案",
            "項目": name,
            "狀態": "OK" if ok else "異常",
            "說明": _shape(data) if ok else err,
            "建議": "OK" if ok else "可使用一鍵修復建立預設空檔；若為重要資料，請先確認 GitHub 是否有舊版。",
            "大小KB": size_kb,
        })

    pages_dir = base / "pages"
    actual = [p.name for p in pages_dir.glob("*.py")] if pages_dir.exists() else []
    for label, candidates in REQUIRED_PAGES.items():
        found = [a for a in actual if a in candidates or any(a.startswith(c.split("_")[0] + "_") and c.split("_")[0] == a.split("_")[0] for c in candidates)]
        rows.append({
            "群組": "頁面檔案",
            "項目": label,
            "狀態": "OK" if found else "異常",
            "說明": "、".join(found) if found else "找不到頁面檔",
            "建議": "OK" if found else "請確認 pages 目錄是否漏上傳。",
        })

    # Official cache quality
    ok, cache, err = read_json(base / "official_factors_cache.json")
    records = _iter_records(cache) if ok else []
    complete_count = 0
    if records:
        for r in records:
            try:
                if float(str(r.get("官方資料完整度", 0)).replace("%", "") or 0) >= 60:
                    complete_count += 1
            except Exception:
                pass
    rows.append({
        "群組": "官方因子",
        "項目": "official_factors_cache.json 完整度",
        "狀態": "OK" if complete_count > 0 else "注意",
        "說明": f"總筆數 {len(records)}，完整度>=60：{complete_count}" if ok else err,
        "建議": "OK" if complete_count > 0 else "請到 16_官方因子快取中心更新，或檢查 GitHub Actions 排程。",
    })

    # Required schema in JSON records
    for file_name, fields in SCHEMA_TARGETS.items():
        ok, data, err = read_json(base / file_name)
        records = _iter_records(data) if ok else []
        if not ok:
            status, desc = "注意", err
            missing = []
        elif not records:
            status, desc = "注意", "尚無資料可檢查"
            missing = []
        else:
            sample = records[0]
            missing = [f for f in fields if f not in sample]
            status = "OK" if not missing else "注意"
            desc = "欄位完整" if not missing else "缺欄位：" + "、".join(missing[:12]) + ("..." if len(missing) > 12 else "")
        rows.append({
            "群組": "欄位串接",
            "項目": file_name,
            "狀態": status,
            "說明": desc,
            "建議": "OK" if status == "OK" else "可使用一鍵修復補欄；不會刪除既有資料。",
        })

    # Schedule
    settings = load_schedule_settings()
    workflow = base / ".github" / "workflows" / "update_official_factors_v112.yml"
    rows.append({
        "群組": "自動排程",
        "項目": "官方因子排程設定",
        "狀態": "OK" if workflow.exists() and settings.get("enabled") else "注意",
        "說明": f"enabled={settings.get('enabled')} / times={settings.get('times')} / workflow={'存在' if workflow.exists() else '不存在'}",
        "建議": "OK" if workflow.exists() else "請上傳 V112 修改檔案中的 GitHub Actions workflow。",
    })

    bad = sum(1 for r in rows if str(r.get("狀態")) == "異常")
    warn = sum(1 for r in rows if str(r.get("狀態")) == "注意")
    ok_count = len(rows) - bad - warn
    level = "OK" if bad == 0 and warn == 0 else ("注意" if bad == 0 else "需修正")
    return {
        "summary": {"總項目": len(rows), "正常": ok_count, "注意": warn, "異常": bad, "整體狀態": level, "檢查時間": now_text()},
        "rows": rows,
    }


def ensure_core_json_files(base_dir: Path | None = None) -> list[dict[str, Any]]:
    base = Path(base_dir or BASE_DIR)
    out = []
    for name, default in CORE_JSON_DEFAULTS.items():
        path = base / name
        if path.exists():
            out.append({"檔案": name, "動作": "略過", "結果": "已存在"})
            continue
        ok, msg = write_json(path, default)
        out.append({"檔案": name, "動作": "建立預設空檔", "結果": msg if ok else f"失敗：{msg}"})
    return out


def repair_json_schema_fields(base_dir: Path | None = None) -> list[dict[str, Any]]:
    base = Path(base_dir or BASE_DIR)
    rows = []
    for file_name, fields in SCHEMA_TARGETS.items():
        path = base / file_name
        ok, data, err = read_json(path)
        if not ok:
            rows.append({"檔案": file_name, "結果": "略過", "說明": err, "修復筆數": 0, "補欄位數": 0})
            continue
        backup_file(path)
        records = _iter_records(data)
        changed_records = 0
        added = 0
        for rec in records:
            changed = False
            for f in fields:
                if f not in rec:
                    rec[f] = ""
                    added += 1
                    changed = True
            if changed:
                changed_records += 1
        if changed_records:
            w_ok, msg = write_json(path, data)
            rows.append({"檔案": file_name, "結果": "OK" if w_ok else "寫入失敗", "說明": msg, "修復筆數": changed_records, "補欄位數": added})
        else:
            rows.append({"檔案": file_name, "結果": "無需修復", "說明": "欄位已完整或尚無資料", "修復筆數": 0, "補欄位數": 0})
    return rows


def full_safe_repair(base_dir: Path | None = None) -> dict[str, Any]:
    base = Path(base_dir or BASE_DIR)
    backup_rows = backup_many([base / n for n in CORE_JSON_DEFAULTS.keys() if (base / n).exists()])
    core_rows = ensure_core_json_files(base)
    schema_rows = repair_json_schema_fields(base)
    return {"ok": True, "time": now_text(), "backup_rows": backup_rows, "core_rows": core_rows, "schema_rows": schema_rows}



# =========================================================
# 官方因子排程設定：本機 + GitHub 永久保存
# =========================================================
def _safe_secret(name: str, default: str = "") -> str:
    """讀取 Streamlit secrets；在 GitHub Actions / 本機腳本環境沒有 Streamlit 時安全略過。"""
    try:
        import streamlit as st  # type: ignore
        return str(st.secrets.get(name, default) or default).strip()
    except Exception:
        return str(os.getenv(name, default) or default).strip()


def _github_schedule_config() -> dict[str, str]:
    return {
        "token": _safe_secret("GITHUB_TOKEN", ""),
        "owner": _safe_secret("GITHUB_REPO_OWNER", "cheng07021028") or "cheng07021028",
        "repo": _safe_secret("GITHUB_REPO_NAME", "stock-app") or "stock-app",
        "branch": _safe_secret("GITHUB_REPO_BRANCH", "main") or "main",
        "path": _safe_secret(
            "OFFICIAL_FACTOR_SCHEDULE_SETTINGS_GITHUB_PATH",
            "data/config/official_factor_schedule_settings.json",
        ) or "data/config/official_factor_schedule_settings.json",
    }


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_contents_url(owner: str, repo: str, path_name: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path_name}"


def _read_schedule_settings_from_github() -> tuple[dict[str, Any] | None, str, str]:
    """回傳：(payload, sha, message)。未設定 token 時不視為錯誤。"""
    cfg = _github_schedule_config()
    token = cfg.get("token", "")
    if not token or requests is None:
        return None, "", "未設定 GITHUB_TOKEN，排程設定只會保存到目前執行環境。"
    try:
        url = _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"])
        r = requests.get(url, headers=_github_headers(token), params={"ref": cfg["branch"]}, timeout=12)
        if r.status_code == 404:
            return None, "", f"GitHub 尚未建立設定檔：{cfg['path']}"
        if r.status_code != 200:
            return None, "", f"GitHub 讀取排程設定失敗 HTTP {r.status_code}: {r.text[:180]}"
        data = r.json()
        content = base64.b64decode(data.get("content", "")).decode("utf-8")
        payload = json.loads(content)
        if not isinstance(payload, dict):
            return None, data.get("sha", ""), "GitHub 排程設定格式不是 JSON object。"
        return payload, data.get("sha", ""), f"GitHub 已讀取排程設定：{cfg['path']}"
    except Exception as exc:
        return None, "", f"GitHub 讀取排程設定例外：{type(exc).__name__}: {exc}"


def _write_schedule_settings_to_github(payload: dict[str, Any]) -> tuple[bool, str]:
    cfg = _github_schedule_config()
    token = cfg.get("token", "")
    if not token or requests is None:
        return False, "未設定 GITHUB_TOKEN，已完成本機保存；若部署在 Streamlit Cloud，重新部署後可能還原。"
    try:
        url = _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"])
        headers = _github_headers(token)
        sha = ""
        get_resp = requests.get(url, headers=headers, params={"ref": cfg["branch"]}, timeout=12)
        if get_resp.status_code == 200:
            sha = str(get_resp.json().get("sha", "") or "")
        elif get_resp.status_code not in (404,):
            return False, f"GitHub 取得設定檔 SHA 失敗 HTTP {get_resp.status_code}: {get_resp.text[:180]}"
        body: dict[str, Any] = {
            "message": f"Update official factor schedule settings {payload.get('last_saved_at', '')}",
            "content": base64.b64encode(json.dumps(json_safe(payload), ensure_ascii=False, indent=2).encode("utf-8")).decode("ascii"),
            "branch": cfg["branch"],
        }
        if sha:
            body["sha"] = sha
        put_resp = requests.put(url, headers=headers, json=body, timeout=20)
        if put_resp.status_code in (200, 201):
            return True, f"GitHub 已永久保存排程設定：{cfg['path']}"
        return False, f"GitHub 寫入排程設定失敗 HTTP {put_resp.status_code}: {put_resp.text[:220]}"
    except Exception as exc:
        return False, f"GitHub 寫入排程設定例外：{type(exc).__name__}: {exc}"


def _merge_schedule_settings(data: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(DEFAULT_SCHEDULE_SETTINGS)
    if isinstance(data, dict):
        merged.update(data)
    # 防呆：times 必須是 list[str]，避免 selectbox 讀取失敗
    times = merged.get("times")
    if isinstance(times, str):
        merged["times"] = [times]
    elif not isinstance(times, list) or not times:
        merged["times"] = list(DEFAULT_SCHEDULE_SETTINGS.get("times", ["23:00"]))
    return merged


def _ts_value(payload: dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("last_saved_at") or payload.get("updated_at") or "")


def load_schedule_settings() -> dict[str, Any]:
    """載入官方因子排程設定。

    優先使用 GitHub 內較新的設定，解決 Streamlit Cloud 重新部署後本機檔案還原的問題。
    如果沒有 GITHUB_TOKEN，則退回讀取 repo / 本機檔案。
    """
    local_data: dict[str, Any] | None = None
    if SCHEDULE_SETTINGS_PATH.exists():
        _ok, data, _msg = read_json(SCHEDULE_SETTINGS_PATH)
        if isinstance(data, dict):
            local_data = data
    gh_data, _sha, _gh_msg = _read_schedule_settings_from_github()
    if isinstance(gh_data, dict) and (_ts_value(gh_data) >= _ts_value(local_data)):
        merged = _merge_schedule_settings(gh_data)
        # 同步回本機，讓同一次工作階段後續讀取不必依賴 GitHub
        write_json(SCHEDULE_SETTINGS_PATH, merged)
        return merged
    return _merge_schedule_settings(local_data)


def save_schedule_settings(settings: dict[str, Any]) -> tuple[bool, str]:
    """保存官方因子排程設定。

    先寫本機，再嘗試寫 GitHub。回傳訊息會明確告知是否真正永久保存。
    """
    payload = _merge_schedule_settings(settings or {})
    payload["last_saved_at"] = now_text()
    payload["updated_at"] = payload["last_saved_at"]
    local_ok, local_msg = write_json(SCHEDULE_SETTINGS_PATH, payload)
    gh_ok, gh_msg = _write_schedule_settings_to_github(payload)
    if local_ok and gh_ok:
        return True, f"本機保存成功；{gh_msg}"
    if local_ok and not gh_ok:
        return True, f"本機保存成功；{gh_msg}"
    return False, f"本機保存失敗：{local_msg}；{gh_msg}"


def run_official_factor_update_once(settings: dict[str, Any] | None = None, push_github: bool = False) -> dict[str, Any]:
    """手動/排程共用更新函式；只在被呼叫時才抓官方資料。"""
    cfg = dict(DEFAULT_SCHEDULE_SETTINGS)
    cfg.update(settings or load_schedule_settings())
    if not cfg.get("enabled", True):
        return {"ok": False, "message": "官方因子自動更新設定為停用，已略過。", "meta": {}}
    try:
        from official_factor_service import build_official_factor_cache, push_cache_to_github
        df, meta = build_official_factor_cache(
            limit=int(cfg.get("limit") or 0) or None,
            market_filter=safe_str(cfg.get("market_filter") or "全部") or "全部",
            include_institutional=bool(cfg.get("include_institutional", True)),
            include_revenue=bool(cfg.get("include_revenue", True)),
            include_valuation=bool(cfg.get("include_valuation", True)),
            save=True,
            enable_finmind_fallback=bool(cfg.get("enable_finmind_fallback", True)),
            finmind_max_stocks=int(cfg.get("finmind_max_stocks", 30) or 30),
            quick_mode=bool(cfg.get("quick_mode", False)),
            max_runtime_seconds=int(cfg.get("max_runtime_seconds", 75) or 75),
            max_requests=int(cfg.get("max_requests", 48) or 48),
            finmind_bulk_only=bool(cfg.get("finmind_bulk_only", False)),
        )
        gh_msg = ""
        gh_ok = None
        if push_github:
            gh_ok, gh_msg = push_cache_to_github()
        bounded_note = ""
        if meta.get("timed_out"):
            bounded_note = f"；已達 {meta.get('max_runtime_seconds', 75)} 秒上限，保留目前成果/舊有效快取"
        return {
            "ok": True,
            "message": f"完成官方因子更新：{len(df)} 筆，完整度>=60：{meta.get('complete_count', 0)}{bounded_note}",
            "meta": meta, "github_ok": gh_ok, "github_msg": gh_msg,
        }
    except Exception as exc:
        return {"ok": False, "message": f"官方因子更新失敗：{type(exc).__name__}: {exc}", "meta": {}}


def run_compile_smoke_test(base_dir: Path | None = None) -> dict[str, Any]:
    base = Path(base_dir or BASE_DIR)
    targets = [
        "official_factor_service.py",
        "godpick_system_health_service.py",
        "pages/7_股神推薦.py",
        "pages/8_股神推薦紀錄.py",
        "pages/10_推薦清單.py",
        "pages/14_股神權重校正.py",
        "pages/16_官方因子快取中心.py",
        "pages/17_系統健康檢查.py",
        "tools/update_official_factors_scheduled.py",
    ]
    existing = [str(base / t) for t in targets if (base / t).exists()]
    try:
        proc = subprocess.run([sys.executable, "-m", "py_compile", *existing], cwd=str(base), capture_output=True, text=True, timeout=60)
        return {"ok": proc.returncode == 0, "stdout": proc.stdout, "stderr": proc.stderr, "checked": existing}
    except Exception as exc:
        return {"ok": False, "stdout": "", "stderr": str(exc), "checked": existing}

# -*- coding: utf-8 -*-
"""股神推薦校正研究樣本服務。

正式推薦紀錄維持嚴格；本服務另外保存近門檻與市場漏選強勢樣本，
避免模型只看到入選股票而產生選擇偏誤。
"""
from __future__ import annotations

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import threading
from pathlib import Path
from typing import Any
import base64
import hashlib
import json
import math
import os

import pandas as pd
import requests

try:
    import streamlit as st
except Exception:  # pragma: no cover
    st = None

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except Exception:  # pragma: no cover
    firebase_admin = None
    credentials = None
    firestore = None

CALIBRATION_SAMPLE_VERSION = "godpick_calibration_samples_v1_20260715"
DEFAULT_CALIBRATION_PATH = "godpick_calibration_samples.json"
_CALIBRATION_SYNC_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="godpick-calibration-remote")
_CALIBRATION_SYNC_LOCK = threading.Lock()

CALIBRATION_TRACE_COLUMNS = [
    "校正樣本鍵", "校正樣本類型", "校正樣本用途", "校正樣本權重",
    "是否納入正式推薦績效", "是否納入權重校正", "個股資料品質",
    "樣本可信度", "當日淘汰關卡", "當日淘汰原因", "原始推薦分區",
    "原始盤中雷達優先級", "校正樣本建立版本",
]


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _safe_float(v: Any, default: float | None = None) -> float | None:
    if v is None:
        return default
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    if isinstance(v, str):
        v = v.replace(",", "").replace("%", "").strip()
        if v.lower() in {"", "none", "nan", "null", "--", "-", "<na>"}:
            return default
    try:
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def _boolish(v: Any) -> bool:
    return _safe_str(v).lower() in {"1", "true", "yes", "y", "是", "啟用", "納入"}


def _normalize_code(v: Any) -> str:
    text = _safe_str(v)
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if 4 <= len(digits) <= 6:
        return digits[:4]
    return text


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _now_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _now_time() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _first_text(row: pd.Series | dict[str, Any], names: list[str]) -> str:
    for name in names:
        value = _safe_str(row.get(name))
        if value:
            return value
    return ""


def _first_num(row: pd.Series | dict[str, Any], names: list[str], default: float = 0.0) -> float:
    for name in names:
        value = _safe_float(row.get(name), None)
        if value is not None:
            return float(value)
    return float(default)


def _hard_exclusion_text(row: pd.Series | dict[str, Any]) -> str:
    text = "｜".join(
        _safe_str(row.get(c))
        for c in [
            "正式推薦排除原因", "進場阻擋原因", "硬否決原因", "真禁買原因",
            "高分禁買原因", "不建議買進原因", "實戰排除原因", "限制原因",
            "正式推薦資格", "K線資料新鮮度", "流動性資料狀態",
        ]
        if _safe_str(row.get(c))
    )
    return text


def assess_individual_sample_quality(row: pd.Series | dict[str, Any]) -> tuple[bool, str, str]:
    """只判斷個股資料能否成為追蹤樣本，不等同可買或正式推薦。"""
    code = _normalize_code(row.get("股票代號"))
    market = _first_text(row, ["市場別", "market"])
    price = _first_num(row, ["最新價", "推薦價格", "推薦日價格"], 0.0)
    turnover = max(
        _first_num(row, ["成交額百萬"], 0.0),
        _first_num(row, ["20日均成交額百萬"], 0.0),
    )
    kline_stale = _first_num(row, ["K線落後交易日"], 0.0)
    kline_state = _first_text(row, ["K線資料新鮮度", "K線驗證標記", "掃描狀態"])
    last_day = _first_text(row, ["K線最後交易日", "本輪市場最新交易日"])
    reason_text = _hard_exclusion_text(row)

    reasons: list[str] = []
    if not code or not code[:4].isdigit():
        reasons.append("股票代號無效")
    if "興櫃" in market:
        reasons.append("興櫃不納入校正主樣本")
    if price <= 0:
        reasons.append("價格無效")
    if turnover <= 0:
        reasons.append("成交額無效")
    if kline_stale > 1:
        reasons.append(f"K線落後{kline_stale:.0f}個交易日")
    stale_words = ["過期", "待更新", "無資料", "失敗", "錯誤", "ONLINE_FAIL"]
    if any(w in kline_state for w in stale_words):
        reasons.append(f"K線狀態不合格:{kline_state}")
    if any(w in reason_text for w in ["分析錯誤", "無K線", "K線無資料", "資料過期"]):
        reasons.append("分析或K線資料失敗")

    eligible = not reasons
    confidence = "高" if eligible and turnover >= 150 and bool(last_day) else ("中" if eligible else "不合格")
    return eligible, "；".join(reasons) if reasons else "個股K線、價格與成交資料可追蹤", confidence


def _is_actionable_row(row: pd.Series | dict[str, Any]) -> bool:
    bucket = _safe_str(row.get("正式推薦分區"))
    radar = _safe_str(row.get("盤中雷達優先級"))
    return bucket in {"正式下週主推薦", "A-｜準主推薦小量試單"} or (
        bucket == "盤中雷達追蹤" and radar.startswith("R1")
    )


def _missed_strong_score(row: pd.Series | dict[str, Any]) -> float:
    gain = _first_num(row, ["今日漲幅%", "當日漲幅%", "漲跌幅%"], 0.0)
    volume_ratio = _first_num(row, ["當日量比", "量比", "5日20日量比"], 0.0)
    close_loc = _first_num(row, ["當日收盤位置%"], 50.0)
    turnover = max(_first_num(row, ["成交額百萬"], 0.0), _first_num(row, ["20日均成交額百萬"], 0.0))
    momentum = _first_num(row, ["強勢動能分", "盤後動能救援分", "爆發雷達分", "飆股攻擊分"], 0.0)
    breakout = _first_num(row, ["突破20日高點%"], 0.0)
    return gain * 5.0 + min(volume_ratio, 5.0) * 5.0 + close_loc * 0.12 + min(turnover, 2000.0) * 0.01 + momentum * 0.35 + max(breakout, 0.0) * 1.2


def _is_missed_strong(row: pd.Series | dict[str, Any]) -> bool:
    if _is_actionable_row(row):
        return False
    gain = _first_num(row, ["今日漲幅%", "當日漲幅%", "漲跌幅%"], 0.0)
    volume_ratio = _first_num(row, ["當日量比", "量比", "5日20日量比"], 0.0)
    close_loc = _first_num(row, ["當日收盤位置%"], 50.0)
    turnover = max(_first_num(row, ["成交額百萬"], 0.0), _first_num(row, ["20日均成交額百萬"], 0.0))
    momentum = _first_num(row, ["強勢動能分", "盤後動能救援分", "爆發雷達分", "飆股攻擊分"], 0.0)
    return bool(
        turnover >= 100
        and gain >= 4.5
        and (volume_ratio >= 1.10 or gain >= 9.0 or momentum >= 72)
        and (close_loc >= 62 or gain >= 9.0)
    )


def _near_threshold_score(row: pd.Series | dict[str, Any], route: str) -> float:
    total = _first_num(row, ["正式推薦排序分", "推薦總分", "股神決策分數", "股神實戰總分"], 0.0)
    entry = _first_num(row, ["進場可執行分", "Entry進場買點分", "進場買點分", "買進分數"], 0.0)
    risk = _first_num(row, ["Risk風控安全分", "風控安全分", "交易可行分數"], 0.0)
    momentum = _first_num(row, ["強勢動能分", "盤後動能救援分", "爆發雷達分", "飆股攻擊分"], 0.0)
    rr = _first_num(row, ["保守風險報酬比", "實戰風險報酬比", "風險報酬比", "風險報酬比_決策"], 0.0)
    turnover = max(_first_num(row, ["成交額百萬"], 0.0), _first_num(row, ["20日均成交額百萬"], 0.0))
    gain = _first_num(row, ["今日漲幅%", "近5日漲幅%"], 0.0)
    if route == "entry":
        return entry * 0.45 + risk * 0.25 + min(rr, 3.0) * 8.0 + total * 0.18 + min(turnover, 1000.0) * 0.006
    if route == "momentum":
        return momentum * 0.48 + total * 0.22 + min(turnover, 1000.0) * 0.008 + max(gain, 0.0) * 1.6
    return total * 0.48 + entry * 0.18 + risk * 0.18 + momentum * 0.10 + min(turnover, 1000.0) * 0.006


def _pick_near_threshold(work: pd.DataFrame, exclude_codes: set[str], max_rows: int) -> pd.DataFrame:
    if work.empty:
        return pd.DataFrame()
    base = work.copy()
    base["_code"] = base["股票代號"].map(_normalize_code)
    base = base[~base["_code"].isin(exclude_codes)].copy()
    if base.empty:
        return pd.DataFrame()

    allowed_buckets = {"不可直接買觀察", "高風險雷達觀察", "盤中雷達追蹤", ""}
    if "正式推薦分區" in base.columns:
        base = base[base["正式推薦分區"].fillna("").astype(str).isin(allowed_buckets)].copy()
    selected: list[pd.DataFrame] = []
    per_route = max(4, int(max_rows / 3))
    for route in ["entry", "momentum", "overall"]:
        part = base.copy()
        part["_route_score"] = part.apply(lambda r, _route=route: _near_threshold_score(r, _route), axis=1)
        part = part.sort_values("_route_score", ascending=False).head(per_route)
        part["_sample_route"] = route
        selected.append(part)
    out = pd.concat(selected, ignore_index=True) if selected else pd.DataFrame()
    if out.empty:
        return out
    out = out.sort_values("_route_score", ascending=False).drop_duplicates("_code", keep="first").head(max_rows)
    return out


def _sample_reason(row: pd.Series | dict[str, Any]) -> tuple[str, str]:
    checkpoint = _first_text(row, ["進場可執行判定", "正式推薦資格", "正式推薦分區", "盤中雷達分層", "實戰過濾狀態"])
    reason = _first_text(row, ["進場阻擋原因", "正式推薦排除原因", "主推薦降級原因", "限制原因", "不建議買進原因", "等待突破原因"])
    return checkpoint or "近門檻未入選", reason or "本輪未進入正式/A-/R1，但個股資料完整，保留作門檻校正對照"


def _build_sample_record(row: pd.Series | dict[str, Any], sample_type: str, confidence: str) -> dict[str, Any]:
    raw = dict(row)
    code = _normalize_code(raw.get("股票代號"))
    rec_date = _safe_str(raw.get("推薦日期")) or _now_date()
    rec_time = _safe_str(raw.get("推薦時間")) or _now_time()
    price = _first_num(raw, ["最新價", "推薦價格", "推薦日價格"], 0.0)
    checkpoint, reason = _sample_reason(raw)
    if sample_type.startswith("D"):
        purpose = "計算強勢股召回率、漏選率與誤殺關卡；不直接餵入獲利權重"
        weight = 0.0
        include_weight = "否"
    else:
        purpose = "校正Entry/Risk/RR/動能等門檻是否過嚴，作近門檻對照"
        weight = 0.45
        include_weight = "是"
    key = f"{rec_date}|{code}|{sample_type}"
    record_id = hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]
    raw.update({
        "record_id": record_id,
        "校正樣本鍵": key,
        "股票代號": code,
        "推薦日期": rec_date,
        "推薦時間": rec_time,
        "建立時間": _safe_str(raw.get("建立時間")) or _now_text(),
        "更新時間": _now_text(),
        "最新更新時間": _now_text(),
        "原始推薦分區": _safe_str(raw.get("正式推薦分區")),
        "原始盤中雷達優先級": _safe_str(raw.get("盤中雷達優先級")),
        "推薦模式": "股神校正研究",
        "紀錄來源": "07_股神推薦｜校正研究樣本自動保存",
        "自動記錄": "是",
        "紀錄層級": sample_type,
        "校正樣本類型": sample_type,
        "校正樣本用途": purpose,
        "校正樣本權重": weight,
        "是否納入正式推薦績效": "否",
        "是否納入權重校正": include_weight,
        "個股資料品質": "可追蹤",
        "樣本可信度": confidence,
        "當日淘汰關卡": checkpoint,
        "當日淘汰原因": reason,
        "校正樣本建立版本": CALIBRATION_SAMPLE_VERSION,
        "目前狀態": "研究追蹤",
        "推薦價格": price,
        "推薦日價格": price,
        "最新價": price,
        "是否已實際買進": False,
        "是否達停損": False,
        "是否達目標1": False,
        "是否達目標2": False,
        "損益金額": None,
        "損益幅%": None,
        "K線驗證標記": _safe_str(raw.get("K線驗證標記")) or "已建立校正追蹤資料",
    })
    return raw


def build_calibration_samples(candidate_df: pd.DataFrame, *, max_near: int = 24, max_missed: int = 20) -> pd.DataFrame:
    if candidate_df is None or not isinstance(candidate_df, pd.DataFrame) or candidate_df.empty or "股票代號" not in candidate_df.columns:
        return pd.DataFrame()
    work = candidate_df.loc[:, ~candidate_df.columns.duplicated()].copy()
    work["股票代號"] = work["股票代號"].map(_normalize_code)
    work = work[work["股票代號"] != ""].drop_duplicates("股票代號", keep="first").copy()

    quality_rows: list[pd.Series] = []
    confidences: dict[str, str] = {}
    for _, row in work.iterrows():
        eligible, _, confidence = assess_individual_sample_quality(row)
        if eligible:
            quality_rows.append(row)
            confidences[_normalize_code(row.get("股票代號"))] = confidence
    if not quality_rows:
        return pd.DataFrame()
    quality = pd.DataFrame(quality_rows)
    quality = quality[~quality.apply(_is_actionable_row, axis=1)].copy()
    if quality.empty:
        return pd.DataFrame()

    missed = quality[quality.apply(_is_missed_strong, axis=1)].copy()
    if not missed.empty:
        missed["_missed_score"] = missed.apply(_missed_strong_score, axis=1)
        missed = missed.sort_values("_missed_score", ascending=False).head(max_missed)
    missed_codes = set(missed["股票代號"].map(_normalize_code).tolist()) if not missed.empty else set()
    near = _pick_near_threshold(quality, missed_codes, max_near)

    records: list[dict[str, Any]] = []
    for _, row in missed.iterrows():
        code = _normalize_code(row.get("股票代號"))
        records.append(_build_sample_record(row, "D｜市場漏選強勢", confidences.get(code, "中")))
    for _, row in near.iterrows():
        code = _normalize_code(row.get("股票代號"))
        records.append(_build_sample_record(row, "C｜近門檻對照", confidences.get(code, "中")))
    return pd.DataFrame(records)


def _read_local(path: str | Path = DEFAULT_CALIBRATION_PATH) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    try:
        payload = json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ["records", "items", "rows", "data"]:
            if isinstance(payload.get(key), list):
                return [x for x in payload[key] if isinstance(x, dict)]
    return []


def _json_safe(v: Any) -> Any:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, pd.Timestamp):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            pass
    if isinstance(v, (dict, list, tuple)):
        return v
    return v


def _atomic_write_local(records: list[dict[str, Any]], path: str | Path = DEFAULT_CALIBRATION_PATH) -> tuple[bool, str]:
    p = Path(path)
    try:
        clean = [{k: _json_safe(v) for k, v in row.items() if not str(k).startswith("_")} for row in records]
        text = json.dumps(clean, ensure_ascii=False, indent=2, allow_nan=False)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(p)
        return True, f"本機已保存 {len(clean)} 筆校正樣本"
    except Exception as exc:
        return False, f"本機校正樣本保存失敗：{exc}"


def _secret(name: str, default: str = "") -> str:
    value = os.environ.get(name, default)
    if st is not None:
        try:
            value = st.secrets.get(name, value)
        except Exception:
            pass
    return _safe_str(value)


def _github_config() -> dict[str, str]:
    return {
        "token": _secret("GITHUB_TOKEN"),
        "owner": _secret("GITHUB_OWNER"),
        "repo": _secret("GITHUB_REPO"),
        "branch": _secret("GITHUB_BRANCH", "main") or "main",
        "path": _secret("GODPICK_CALIBRATION_GITHUB_PATH", DEFAULT_CALIBRATION_PATH) or DEFAULT_CALIBRATION_PATH,
    }


def _github_url(cfg: dict[str, str]) -> str:
    return f"https://api.github.com/repos/{cfg['owner']}/{cfg['repo']}/contents/{cfg['path']}"


def _read_github() -> tuple[list[dict[str, Any]], str]:
    cfg = _github_config()
    if not all([cfg["token"], cfg["owner"], cfg["repo"]]):
        return [], "GitHub校正樣本未設定"
    headers = {"Authorization": f"Bearer {cfg['token']}", "Accept": "application/vnd.github+json"}
    try:
        resp = requests.get(_github_url(cfg), headers=headers, params={"ref": cfg["branch"]}, timeout=20)
        if resp.status_code == 404:
            return [], "GitHub尚未建立校正樣本檔"
        if resp.status_code != 200:
            return [], f"GitHub校正樣本讀取失敗：{resp.status_code}"
        content = _safe_str(resp.json().get("content")).replace("\n", "")
        if not content:
            return [], "GitHub校正樣本檔為空"
        payload = json.loads(base64.b64decode(content).decode("utf-8-sig"))
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)], "GitHub校正樣本已讀取"
        return [], "GitHub校正樣本格式不是list"
    except Exception as exc:
        return [], f"GitHub校正樣本讀取例外：{exc}"


def _sync_github(records: list[dict[str, Any]]) -> tuple[bool, str]:
    cfg = _github_config()
    if not all([cfg["token"], cfg["owner"], cfg["repo"]]):
        return False, "GitHub校正樣本未設定，已保留本機"
    headers = {"Authorization": f"Bearer {cfg['token']}", "Accept": "application/vnd.github+json"}
    sha = ""
    try:
        get_resp = requests.get(_github_url(cfg), headers=headers, params={"ref": cfg["branch"]}, timeout=20)
        if get_resp.status_code == 200:
            sha = _safe_str(get_resp.json().get("sha"))
        elif get_resp.status_code != 404:
            return False, f"GitHub校正樣本讀取失敗：{get_resp.status_code}"
        clean = [{k: _json_safe(v) for k, v in row.items() if not str(k).startswith("_")} for row in records]
        content = json.dumps(clean, ensure_ascii=False, indent=2, allow_nan=False)
        body: dict[str, Any] = {
            "message": f"update godpick calibration samples at {_now_text()}",
            "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
            "branch": cfg["branch"],
        }
        if sha:
            body["sha"] = sha
        put_resp = requests.put(_github_url(cfg), headers=headers, json=body, timeout=30)
        if put_resp.status_code in {200, 201}:
            return True, f"GitHub已同步 {cfg['path']}"
        return False, f"GitHub校正樣本寫入失敗：{put_resp.status_code}"
    except Exception as exc:
        return False, f"GitHub校正樣本同步例外：{exc}"


def _init_firebase() -> bool:
    if firebase_admin is None or firestore is None:
        return False
    try:
        if not firebase_admin._apps:
            raw = _secret("FIREBASE_SERVICE_ACCOUNT_JSON")
            if not raw:
                return False
            info = json.loads(raw)
            firebase_admin.initialize_app(credentials.Certificate(info))
        return True
    except Exception:
        return False


def _sync_firestore(records: list[dict[str, Any]]) -> tuple[bool, str]:
    if not _init_firebase():
        return False, "Firestore校正樣本未設定，已保留本機"
    try:
        db = firestore.client()
        collection = db.collection("godpick_calibration_samples")
        batch = db.batch()
        for row in records:
            rec_id = _safe_str(row.get("record_id"))
            if not rec_id:
                continue
            payload = {k: _json_safe(v) for k, v in row.items() if not str(k).startswith("_")}
            payload["updated_at"] = firestore.SERVER_TIMESTAMP
            batch.set(collection.document(rec_id), payload, merge=True)
        summary = db.collection("system").document("godpick_calibration_samples_summary")
        batch.set(summary, {"count": len(records), "updated_at": firestore.SERVER_TIMESTAMP, "version": CALIBRATION_SAMPLE_VERSION}, merge=True)
        batch.commit()
        return True, "Firestore已同步校正樣本"
    except Exception as exc:
        return False, f"Firestore校正樣本同步失敗：{exc}"


def _merge_records(old: list[dict[str, Any]], new: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in old + new:
        key = _safe_str(row.get("校正樣本鍵")) or _safe_str(row.get("record_id"))
        if not key:
            continue
        previous = merged.get(key, {})
        combined = dict(previous)
        combined.update({k: v for k, v in row.items() if v not in [None, ""] or k not in combined})
        merged[key] = combined
    return sorted(merged.values(), key=lambda r: (_safe_str(r.get("推薦日期")), _safe_str(r.get("股票代號")), _safe_str(r.get("校正樣本類型"))))


def _background_calibration_sync() -> None:
    try:
        remote_records, _ = _read_github()
        with _CALIBRATION_SYNC_LOCK:
            latest_local = _read_local(DEFAULT_CALIBRATION_PATH)
            merged = _merge_records(remote_records, latest_local)
            _atomic_write_local(merged, DEFAULT_CALIBRATION_PATH)
        _sync_github(merged)
        _sync_firestore(merged)
    except Exception:
        pass


def save_calibration_samples(
    candidate_df: pd.DataFrame, *, max_near: int = 24, max_missed: int = 20,
    background_remote: bool = False,
) -> tuple[int, list[str], dict[str, int]]:
    samples = build_calibration_samples(candidate_df, max_near=max_near, max_missed=max_missed)
    if samples.empty:
        return 0, ["本輪沒有符合個股資料品質的近門檻或市場漏選強勢樣本。"], {"near": 0, "missed": 0}
    new_records = samples.to_dict(orient="records")

    # V180：先以本機權威檔立即合併並原子保存，UI 不等待 GitHub/Firestore。
    local_records = _read_local(DEFAULT_CALIBRATION_PATH)
    before_keys = {_safe_str(r.get("校正樣本鍵")) for r in local_records}
    merged_local = _merge_records(local_records, new_records)
    added = sum(1 for r in new_records if _safe_str(r.get("校正樣本鍵")) not in before_keys)
    ok_local, msg_local = _atomic_write_local(merged_local, DEFAULT_CALIBRATION_PATH)
    messages = [msg_local]
    if ok_local and background_remote:
        _CALIBRATION_SYNC_EXECUTOR.submit(_background_calibration_sync)
        messages.append("V180：校正樣本 GitHub/Firestore 合併同步已排入背景，不阻塞推薦完成。")
    elif ok_local:
        remote_records, remote_read_msg = _read_github()
        merged = _merge_records(remote_records, merged_local)
        _atomic_write_local(merged, DEFAULT_CALIBRATION_PATH)
        _, gh_msg = _sync_github(merged)
        _, fs_msg = _sync_firestore(merged)
        messages.extend([remote_read_msg, gh_msg, fs_msg])

    type_series = samples.get("校正樣本類型", pd.Series([], dtype="object")).astype(str)
    summary = {
        "near": int(type_series.str.startswith("C").sum()),
        "missed": int(type_series.str.startswith("D").sum()),
        "total": int(len(samples)),
    }
    return added, messages, summary


def sync_existing_calibration_samples() -> tuple[bool, list[str]]:
    local_records = _read_local(DEFAULT_CALIBRATION_PATH)
    remote_records, remote_read_msg = _read_github()
    records = _merge_records(local_records, remote_records)
    if not records:
        return True, [remote_read_msg, "校正研究樣本檔尚無資料，不需同步。"]
    ok_local, local_msg = _atomic_write_local(records, DEFAULT_CALIBRATION_PATH)
    gh_ok, gh_msg = _sync_github(records)
    fs_ok, fs_msg = _sync_firestore(records)
    return bool(ok_local), [remote_read_msg, local_msg, gh_msg, fs_msg]


__all__ = [
    "CALIBRATION_SAMPLE_VERSION",
    "DEFAULT_CALIBRATION_PATH",
    "CALIBRATION_TRACE_COLUMNS",
    "assess_individual_sample_quality",
    "build_calibration_samples",
    "save_calibration_samples",
    "sync_existing_calibration_samples",
]

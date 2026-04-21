# pages/8_股神推薦紀錄.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Any
import json
import base64
import io
import hashlib

import pandas as pd
import requests
import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore

from utils import (
    format_number,
    get_history_data,
    inject_pro_theme,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
)

PAGE_TITLE = "股神推薦紀錄"
PFX = "godpick_record_"

GODPICK_RECORD_COLUMNS = [
    "record_id",
    "股票代號",
    "股票名稱",
    "市場別",
    "類別",
    "推薦模式",
    "推薦等級",
    "推薦總分",
    "技術結構分數",
    "起漲前兆分數",
    "交易可行分數",
    "類股熱度分數",
    "同類股領先幅度",
    "是否領先同類股",
    "推薦標籤",
    "推薦理由摘要",
    "推薦價格",
    "停損價",
    "賣出目標1",
    "賣出目標2",
    "推薦日期",
    "推薦時間",
    "建立時間",
    "更新時間",
    "目前狀態",
    "是否已實際買進",
    "實際買進價",
    "實際賣出價",
    "實際報酬%",
    "最新價",
    "最新更新時間",
    "損益金額",
    "損益幅%",
    "是否達停損",
    "是否達目標1",
    "是否達目標2",
    "持有天數",
    "模式績效標籤",
    "備註",
    "3日績效%",
    "5日績效%",
    "10日績效%",
    "20日績效%",
]

STATUS_OPTIONS = ["觀察", "已買進", "已賣出", "停損", "達標", "取消", "封存"]
PERF_LABEL_OPTIONS = ["", "強", "中", "弱", "觀察中", "待驗證"]


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


def _score_clip(v: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, v))


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _now_date_text() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _now_time_text() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _create_record_id(code: str, rec_date: str, rec_time: str, mode: str) -> str:
    raw = f"{_safe_str(code)}|{_safe_str(rec_date)}|{_safe_str(rec_time)}|{_safe_str(mode)}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _set_status(msg: str, level: str = "info"):
    st.session_state[_k("status_msg")] = msg
    st.session_state[_k("status_type")] = level


def _github_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("GODPICK_RECORDS_GITHUB_PATH", "godpick_records.json")) or "godpick_records.json",
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
    private_key = _safe_str(raw_key)
    private_key = private_key.replace("\\n", "\n").strip()
    if private_key.startswith("\ufeff"):
        private_key = private_key.lstrip("\ufeff")
    return private_key


def _init_firebase_app():
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


def _ensure_godpick_record_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)

    x = df.copy()
    if "record_id" not in x.columns and "rec_id" in x.columns:
        x["record_id"] = x["rec_id"]

    for c in GODPICK_RECORD_COLUMNS:
        if c not in x.columns:
            x[c] = None

    numeric_cols = [
        "推薦總分", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數",
        "同類股領先幅度", "推薦價格", "停損價", "賣出目標1", "賣出目標2",
        "實際買進價", "實際賣出價", "實際報酬%", "最新價", "損益金額", "損益幅%",
        "持有天數", "3日績效%", "5日績效%", "10日績效%", "20日績效%",
    ]
    for c in numeric_cols:
        x[c] = pd.to_numeric(x[c], errors="coerce")

    bool_cols = ["是否領先同類股", "是否已實際買進", "是否達停損", "是否達目標1", "是否達目標2"]
    for c in bool_cols:
        x[c] = x[c].fillna(False).map(_normalize_bool)

    text_cols = [
        "股票代號", "股票名稱", "市場別", "類別", "推薦模式", "推薦等級", "推薦標籤", "推薦理由摘要",
        "推薦日期", "推薦時間", "建立時間", "更新時間", "最新更新時間", "模式績效標籤", "備註",
    ]
    for c in text_cols:
        x[c] = x[c].fillna("").astype(str)

    x["股票代號"] = x["股票代號"].map(_normalize_code)
    x["類別"] = x["類別"].map(_normalize_category)
    x["目前狀態"] = x["目前狀態"].fillna("觀察").astype(str).replace("", "觀察")

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

    return x[GODPICK_RECORD_COLUMNS].copy()


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
    ok1, msg1 = _write_records_to_github(clean_df)
    ok2, msg2 = _write_records_to_firestore(clean_df)
    st.session_state[_k("last_sync_detail")] = [
        f"GitHub: {'成功' if ok1 else '失敗'} | {msg1}",
        f"Firestore: {'成功' if ok2 else '失敗'} | {msg2}",
    ]
    if ok1 and ok2:
        _set_status("推薦紀錄 GitHub + Firestore 同步成功", "success")
        return True
    if ok1 or ok2:
        _set_status("推薦紀錄部分同步成功", "warning")
        return True
    _set_status("推薦紀錄同步失敗", "error")
    return False


@st.cache_data(ttl=300, show_spinner=False)
def _get_latest_close(stock_no: str, stock_name: str, market_type: str) -> tuple[float | None, str]:
    today = date.today()
    start_date = today - timedelta(days=60)
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
                    return float(temp.iloc[-1]["收盤價"]), _safe_str(mk or market_type or "未知")
        except Exception:
            pass
    return None, _safe_str(market_type or "未知")


@st.cache_data(ttl=3600, show_spinner=False)
def _get_forward_return(stock_no: str, stock_name: str, market_type: str, rec_date_text: str, days_after: int) -> float | None:
    rec_date = pd.to_datetime(rec_date_text, errors="coerce")
    if pd.isna(rec_date):
        return None
    start_date = rec_date.date() - timedelta(days=5)
    end_date = rec_date.date() + timedelta(days=max(days_after * 4, 30))
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

            base_candidates = temp[temp["日期"].dt.date >= rec_date.date()]
            if base_candidates.empty:
                continue
            base_px = float(base_candidates.iloc[0]["收盤價"])
            if base_px == 0:
                return None
            target_idx = min(days_after, len(base_candidates) - 1)
            target_px = float(base_candidates.iloc[target_idx]["收盤價"])
            return (target_px - base_px) / base_px * 100
        except Exception:
            pass
    return None


def _recalc_row(row: pd.Series | dict[str, Any]) -> dict[str, Any]:
    src = dict(row)
    rec_price = _safe_float(src.get("推薦價格"))
    buy_price = _safe_float(src.get("實際買進價"))
    sell_price = _safe_float(src.get("實際賣出價"))
    latest_price = _safe_float(src.get("最新價"))
    stop_price = _safe_float(src.get("停損價"))
    target1 = _safe_float(src.get("賣出目標1"))
    target2 = _safe_float(src.get("賣出目標2"))
    status = _safe_str(src.get("目前狀態")) or "觀察"

    effective_cost = buy_price if buy_price not in [None, 0] else rec_price
    mark_price = sell_price if sell_price not in [None, 0] else latest_price

    pnl_amt = None
    pnl_pct = None
    if effective_cost not in [None, 0] and mark_price is not None:
        pnl_amt = mark_price - effective_cost
        pnl_pct = (pnl_amt / effective_cost) * 100

    actual_ret = None
    if buy_price not in [None, 0] and sell_price not in [None, 0]:
        actual_ret = (sell_price - buy_price) / buy_price * 100

    buy_flag = src.get("是否已實際買進")
    buy_flag = _normalize_bool(buy_flag) or buy_price not in [None, 0] or status == "已買進"

    hit_stop = _normalize_bool(src.get("是否達停損"))
    hit_t1 = _normalize_bool(src.get("是否達目標1"))
    hit_t2 = _normalize_bool(src.get("是否達目標2"))
    if latest_price is not None:
        if stop_price is not None and latest_price <= stop_price:
            hit_stop = True
        if target1 is not None and latest_price >= target1:
            hit_t1 = True
        if target2 is not None and latest_price >= target2:
            hit_t2 = True

    rec_date = pd.to_datetime(_safe_str(src.get("推薦日期")), errors="coerce")
    holding_days = _safe_float(src.get("持有天數"))
    if pd.notna(rec_date):
        holding_days = max((date.today() - rec_date.date()).days, 0)

    perf_label = _safe_str(src.get("模式績效標籤"))
    score_for_label = actual_ret if actual_ret is not None else pnl_pct
    if not perf_label and score_for_label is not None:
        if score_for_label >= 12:
            perf_label = "強"
        elif score_for_label >= 3:
            perf_label = "中"
        elif score_for_label > -3:
            perf_label = "觀察中"
        else:
            perf_label = "弱"

    if status == "停損":
        hit_stop = True
    if status == "達標":
        hit_t1 = True

    src["是否已實際買進"] = buy_flag
    src["損益金額"] = pnl_amt
    src["損益幅%"] = pnl_pct
    src["實際報酬%"] = actual_ret
    src["是否達停損"] = hit_stop
    src["是否達目標1"] = hit_t1
    src["是否達目標2"] = hit_t2
    src["持有天數"] = holding_days
    src["模式績效標籤"] = perf_label
    src["更新時間"] = _now_text()
    return src


def _refresh_latest_prices(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return _ensure_godpick_record_columns(pd.DataFrame())
    rows = []
    for _, row in df.iterrows():
        stock_no = _normalize_code(row.get("股票代號"))
        stock_name = _safe_str(row.get("股票名稱"))
        market = _safe_str(row.get("市場別"))
        latest, used_market = _get_latest_close(stock_no, stock_name, market)
        payload = dict(row)
        if latest is not None:
            payload["最新價"] = latest
            payload["市場別"] = used_market or market
            payload["最新更新時間"] = _now_text()
        payload = _recalc_row(payload)
        rows.append(payload)
    return _ensure_godpick_record_columns(pd.DataFrame(rows))


def _backfill_perf_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return _ensure_godpick_record_columns(pd.DataFrame())
    rows = []
    for _, row in df.iterrows():
        payload = dict(row)
        code = _normalize_code(payload.get("股票代號"))
        name = _safe_str(payload.get("股票名稱"))
        market = _safe_str(payload.get("市場別"))
        rec_date = _safe_str(payload.get("推薦日期"))
        for d in [3, 5, 10, 20]:
            key = f"{d}日績效%"
            val = _safe_float(payload.get(key))
            if val is None:
                payload[key] = _get_forward_return(code, name, market, rec_date, d)
        payload = _recalc_row(payload)
        rows.append(payload)
    return _ensure_godpick_record_columns(pd.DataFrame(rows))


def _load_records() -> pd.DataFrame:
    gh_df, gh_err = _read_records_from_github()
    fs_df, fs_err = _read_records_from_firestore()

    base_df = pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)
    if not gh_df.empty:
        base_df = gh_df.copy()
    if not fs_df.empty:
        base_df = _append_records_dedup_by_business_key(base_df, fs_df)

    st.session_state[_k("load_detail")] = [
        f"GitHub: {'OK' if not gh_err else gh_err}",
        f"Firestore: {'OK' if not fs_err else fs_err}",
    ]
    return _ensure_godpick_record_columns(base_df)


def _save_state_df(df: pd.DataFrame):
    st.session_state[_k("records_df")] = _ensure_godpick_record_columns(df)
    st.session_state[_k("records_saved_at")] = _now_text()


def _get_state_df() -> pd.DataFrame:
    df = st.session_state.get(_k("records_df"))
    if isinstance(df, pd.DataFrame):
        return _ensure_godpick_record_columns(df)
    return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)


def _format_df(df: pd.DataFrame) -> pd.DataFrame:
    show = df.copy()
    pct_cols = ["實際報酬%", "損益幅%", "3日績效%", "5日績效%", "10日績效%", "20日績效%"]
    num_cols = [
        "推薦總分", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數", "同類股領先幅度",
        "推薦價格", "停損價", "賣出目標1", "賣出目標2", "實際買進價", "實際賣出價", "最新價", "損益金額", "持有天數",
    ]
    for c in pct_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: f"{x:,.2f}%" if pd.notna(x) else "")
    for c in num_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: format_number(x, 2) if pd.notna(x) else "")
    for c in ["是否已實際買進", "是否達停損", "是否達目標1", "是否達目標2"]:
        if c in show.columns:
            show[c] = show[c].map(lambda v: "是" if _normalize_bool(v) else "否")
    return show


def _build_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df is None or df.empty:
        return {
            "count": 0,
            "buy_count": 0,
            "sold_count": 0,
            "avg_ret": 0,
            "win_rate": 0,
        }
    ret_series = pd.to_numeric(df["實際報酬%"], errors="coerce")
    pnl_series = pd.to_numeric(df["損益幅%"], errors="coerce")
    used_ret = ret_series.fillna(pnl_series)
    valid = used_ret.dropna()
    buy_count = int(df["是否已實際買進"].fillna(False).map(_normalize_bool).sum())
    sold_count = int(df["目前狀態"].isin(["已賣出", "停損", "達標"]).sum())
    win_rate = float((valid > 0).mean() * 100) if not valid.empty else 0.0
    avg_ret = float(valid.mean()) if not valid.empty else 0.0
    return {
        "count": int(len(df)),
        "buy_count": buy_count,
        "sold_count": sold_count,
        "avg_ret": avg_ret,
        "win_rate": win_rate,
    }


def _build_mode_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    x = df.copy()
    x["報酬基準"] = pd.to_numeric(x["實際報酬%"], errors="coerce").fillna(pd.to_numeric(x["損益幅%"], errors="coerce"))
    grp = x.groupby("推薦模式", dropna=False).agg(
        筆數=("record_id", "count"),
        已買進筆數=("是否已實際買進", lambda s: int(pd.Series(s).fillna(False).map(_normalize_bool).sum())),
        平均報酬=("報酬基準", "mean"),
        勝率=("報酬基準", lambda s: (pd.Series(s).dropna() > 0).mean() * 100 if len(pd.Series(s).dropna()) else 0),
    ).reset_index()
    grp = grp.sort_values(["勝率", "平均報酬", "筆數"], ascending=[False, False, False]).reset_index(drop=True)
    return grp


def _build_category_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    x = df.copy()
    x["報酬基準"] = pd.to_numeric(x["實際報酬%"], errors="coerce").fillna(pd.to_numeric(x["損益幅%"], errors="coerce"))
    grp = x.groupby("類別", dropna=False).agg(
        筆數=("record_id", "count"),
        平均報酬=("報酬基準", "mean"),
        勝率=("報酬基準", lambda s: (pd.Series(s).dropna() > 0).mean() * 100 if len(pd.Series(s).dropna()) else 0),
    ).reset_index()
    grp = grp.sort_values(["勝率", "平均報酬", "筆數"], ascending=[False, False, False]).reset_index(drop=True)
    return grp


def _win_rate(series) -> float:
    s = pd.to_numeric(pd.Series(series), errors="coerce").dropna()
    return float((s > 0).mean() * 100) if len(s) else 0.0


def _build_analysis_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    local_df = _ensure_godpick_record_columns(df.copy())
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

    return {
        "mode": mode_df,
        "category": category_df,
        "grade": grade_df,
        "trade_mode": trade_mode_df,
        "best_mode": best_mode_df,
        "best_category": best_category_df,
    }


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


def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    if _k("status_msg") not in st.session_state:
        st.session_state[_k("status_msg")] = ""
        st.session_state[_k("status_type")] = "info"

    render_pro_hero(
        title="股神推薦紀錄",
        subtitle="追蹤 7_股神推薦 推薦股票，支援 GitHub + Firestore 雙寫、每日更新、實際交易分析、績效統計、Excel 匯出。",
    )

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

    top_cols = st.columns([1.1, 1.1, 1.1, 1.1, 1.2, 1.4, 2.0])
    with top_cols[0]:
        if st.button("🔄 重新載入", use_container_width=True):
            df = _load_records()
            _save_state_df(df)
            _set_status("推薦紀錄已重新載入", "success")
            st.rerun()
    with top_cols[1]:
        if st.button("📈 更新最新價", use_container_width=True):
            df = _get_state_df()
            df = _refresh_latest_prices(df)
            df = _apply_mode_labels(df)
            _save_state_df(df)
            st.success("已更新最新價，尚未同步")
    with top_cols[2]:
        if st.button("💾 儲存同步", use_container_width=True):
            latest_df = _apply_mode_labels(_get_state_df())
            _save_state_df(latest_df)
            ok = _save_records_dual(latest_df)
            if ok:
                st.rerun()
    with top_cols[3]:
        if st.button("🧹 清除快取", use_container_width=True):
            try:
                _get_latest_close.clear()
                _get_forward_return.clear()
            except Exception:
                pass
            st.success("快取已清除")
    with top_cols[4]:
        if st.button("🧮 更新前推績效", use_container_width=True):
            updated = _backfill_perf_columns(_get_state_df())
            updated = _apply_mode_labels(updated)
            _save_state_df(updated)
            st.success("已更新 3/5/10/20 日績效與模式績效標籤，尚未同步")
    with top_cols[5]:
        st.toggle("只更新未出場", value=True, key=_k("only_active_update"))
    with top_cols[6]:
        st.caption(f"GitHub：{'✅' if _safe_str(_github_config().get('token')) else '❌'} ｜ Firestore：{'✅' if _safe_str(_firebase_config().get('project_id')) else '❌'}")

    df = _get_state_df()
    if df.empty:
        df = _load_records()
        _save_state_df(df)

    load_detail = st.session_state.get(_k("load_detail"), [])
    if load_detail:
        with st.expander("讀取來源明細", expanded=False):
            for line in load_detail:
                st.write(f"- {line}")

    sync_detail = st.session_state.get(_k("last_sync_detail"), [])
    if sync_detail:
        with st.expander("同步明細", expanded=False):
            for line in sync_detail:
                st.write(f"- {line}")

    live_df = _ensure_godpick_record_columns(_get_state_df().copy())
    ana_tables = _build_analysis_tables(live_df)
    summary = _build_summary(live_df)
    avg_20 = pd.to_numeric(live_df["20日績效%"], errors="coerce").dropna().mean() if not live_df.empty else None
    avg_real = pd.to_numeric(live_df.loc[live_df["是否已實際買進"] == True, "實際報酬%"], errors="coerce").dropna().mean() if not live_df.empty else None

    render_pro_kpi_row([
        {"label": "總筆數", "value": summary["count"], "delta": "推薦紀錄", "delta_class": "pro-kpi-delta-flat"},
        {"label": "持有中", "value": int((live_df["目前狀態"] == "持有").sum()) if not live_df.empty else 0, "delta": "狀態追蹤", "delta_class": "pro-kpi-delta-flat"},
        {"label": "平均系統報酬%", "value": f"{summary['avg_ret']:.2f}%", "delta": f"勝率 {summary['win_rate']:.1f}%", "delta_class": "pro-kpi-delta-flat"},
        {"label": "平均20日績效%", "value": "-" if pd.isna(avg_20) else f"{avg_20:.2f}%", "delta": "-" if pd.isna(avg_real) else f"平均實際 {avg_real:.2f}%", "delta_class": "pro-kpi-delta-flat"},
    ])

    tabs = st.tabs(["📋 總表管理", "➕ 手動新增", "📊 系統績效分析", "💹 實際交易分析", "📤 Excel 匯出", "⚙️ 同步檢查"])

    with tabs[0]:
        render_pro_section("推薦紀錄總表", "先篩選再編輯，減少 data_editor 負擔。")
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

        view_df = live_df.copy()
        if keyword:
            mask = (
                view_df["股票代號"].astype(str).str.contains(keyword, case=False, na=False)
                | view_df["股票名稱"].astype(str).str.contains(keyword, case=False, na=False)
                | view_df["推薦理由摘要"].astype(str).str.contains(keyword, case=False, na=False)
            )
            view_df = view_df[mask].copy()
        if mode_filter != "全部":
            view_df = view_df[view_df["推薦模式"].astype(str) == mode_filter].copy()
        if category_filter != "全部":
            view_df = view_df[view_df["類別"].astype(str) == category_filter].copy()
        if status_filter != "全部":
            view_df = view_df[view_df["目前狀態"].astype(str) == status_filter].copy()
        if bought_filter != "全部":
            target_bool = bought_filter == "是"
            view_df = view_df[view_df["是否已實際買進"].fillna(False).map(_normalize_bool) == target_bool].copy()
        if sort_by in view_df.columns:
            view_df = view_df.sort_values(sort_by, ascending=sort_asc, na_position="last").reset_index(drop=True)

        st.caption(f"目前顯示 {len(view_df)} / {len(live_df)} 筆")

        standard_cols = [
            "record_id", "股票代號", "股票名稱", "市場別", "類別",
            "推薦模式", "推薦等級", "推薦總分",
            "推薦價格", "最新價", "損益幅%",
            "3日績效%", "5日績效%", "10日績效%", "20日績效%",
            "目前狀態", "是否已實際買進",
            "實際買進價", "實際賣出價", "實際報酬%",
            "推薦日期", "推薦時間", "模式績效標籤", "備註"
        ]
        advanced_cols = [
            "record_id", "股票代號", "股票名稱", "市場別", "類別",
            "推薦模式", "推薦等級", "推薦總分", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數",
            "推薦價格", "停損價", "賣出目標1", "賣出目標2",
            "最新價", "損益幅%", "3日績效%", "5日績效%", "10日績效%", "20日績效%",
            "目前狀態", "是否已實際買進", "實際買進價", "實際賣出價", "實際報酬%",
            "是否達停損", "是否達目標1", "是否達目標2",
            "持有天數", "推薦日期", "推薦時間", "模式績效標籤", "推薦理由摘要", "備註"
        ]
        use_cols = advanced_cols if show_cols_mode == "進階" else standard_cols
        editor_df = view_df[[c for c in use_cols if c in view_df.columns]].copy()
        editor_df.insert(0, "刪除", False)

        edited_df = st.data_editor(
            editor_df,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            key=_k("record_editor"),
            column_config={
                "刪除": st.column_config.CheckboxColumn("刪除"),
                "record_id": st.column_config.TextColumn("record_id", disabled=True),
                "股票代號": st.column_config.TextColumn("股票代號", disabled=True),
                "股票名稱": st.column_config.TextColumn("股票名稱", disabled=True),
                "推薦模式": st.column_config.TextColumn("推薦模式", disabled=True),
                "推薦等級": st.column_config.TextColumn("推薦等級", disabled=True),
                "推薦總分": st.column_config.NumberColumn("推薦總分", format="%.2f", disabled=True),
                "技術結構分數": st.column_config.NumberColumn("技術結構分數", format="%.2f", disabled=True),
                "起漲前兆分數": st.column_config.NumberColumn("起漲前兆分數", format="%.2f", disabled=True),
                "交易可行分數": st.column_config.NumberColumn("交易可行分數", format="%.2f", disabled=True),
                "類股熱度分數": st.column_config.NumberColumn("類股熱度分數", format="%.2f", disabled=True),
                "最新價": st.column_config.NumberColumn("最新價", format="%.2f", disabled=True),
                "損益幅%": st.column_config.NumberColumn("損益幅%", format="%.2f", disabled=True),
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
                "推薦理由摘要": st.column_config.TextColumn("推薦理由摘要", width="large", disabled=True),
                "備註": st.column_config.TextColumn("備註", width="large"),
            },
        )

        action_cols = st.columns([1.2, 1.2, 1.2, 1.2, 3.2])
        with action_cols[0]:
            if st.button("✅ 套用編輯", use_container_width=True):
                master = live_df.copy()
                edit_map = {str(r["record_id"]): dict(r) for _, r in edited_df.iterrows()}
                for idx in master.index:
                    rec_id = _safe_str(master.at[idx, "record_id"])
                    if rec_id not in edit_map:
                        continue
                    src = edit_map[rec_id]
                    for c in [c for c in master.columns if c in src]:
                        if c in ["record_id", "股票代號", "股票名稱", "推薦模式", "推薦等級", "推薦總分", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數", "最新價", "損益幅%", "3日績效%", "5日績效%", "10日績效%", "20日績效%", "推薦日期", "推薦時間", "推薦理由摘要"]:
                            continue
                        master.at[idx, c] = src.get(c)
                    recalc = _recalc_row(master.loc[idx].to_dict())
                    for k2, v2 in recalc.items():
                        if k2 in master.columns:
                            master.at[idx, k2] = v2
                master = _apply_mode_labels(master)
                _save_state_df(master)
                st.success("已套用，尚未同步")
        with action_cols[1]:
            if st.button("🗑️ 刪除勾選", use_container_width=True):
                delete_ids = edited_df.loc[edited_df["刪除"] == True, "record_id"].astype(str).tolist()
                if not delete_ids:
                    st.warning("請先勾選要刪除的紀錄。")
                else:
                    new_df = _delete_records_by_ids(live_df, delete_ids)
                    _save_state_df(new_df)
                    st.success(f"已刪除 {len(delete_ids)} 筆，尚未同步")
        with action_cols[2]:
            if st.button("🧼 清空目前篩選", use_container_width=True):
                if view_df.empty:
                    st.warning("目前篩選結果沒有資料可清空。")
                else:
                    new_df = _clear_filtered_records(live_df, view_df)
                    _save_state_df(new_df)
                    st.success(f"已清空 {len(view_df)} 筆，尚未同步")
        with action_cols[3]:
            if st.button("🧮 更新績效", use_container_width=True):
                updated = _backfill_perf_columns(_get_state_df())
                updated = _apply_mode_labels(updated)
                _save_state_df(updated)
                st.success("已更新 3/5/10/20 日績效，尚未同步")
        with action_cols[4]:
            st.caption("流程：篩選 → 編輯 → 套用 / 刪除 / 清空 → 更新價格 / 更新績效 → 儲存同步")

    with tabs[1]:
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
                _save_state_df(new_df)
                ok = _save_records_dual(new_df)
                if ok:
                    st.success("已加入並同步成功")
                    st.rerun()

    with tabs[2]:
        render_pro_section("系統推薦績效分析", "以推薦價格對照最新價與前推 3/5/10/20 日績效")
        valid_sys = pd.to_numeric(live_df["損益幅%"], errors="coerce").dropna()
        win_rate_sys = float((valid_sys > 0).mean() * 100) if not valid_sys.empty else 0.0
        avg_sys_ret = float(valid_sys.mean()) if not valid_sys.empty else 0.0
        valid_20 = pd.to_numeric(live_df["20日績效%"], errors="coerce").dropna()
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
        best_cols = st.columns(2)
        with best_cols[0]:
            if not ana_tables["best_mode"].empty:
                top_mode = ana_tables["best_mode"].iloc[0]
                st.info(f"最強模式：{_safe_str(top_mode.get('推薦模式'))} ｜ 平均20日績效 {(_safe_float(top_mode.get('平均20日績效'), 0) or 0):.2f}% ｜ 20日勝率 {(_safe_float(top_mode.get('20日勝率'), 0) or 0):.2f}%")
            else:
                st.info("最強模式：暫無資料")
        with best_cols[1]:
            if not ana_tables["best_category"].empty:
                top_cat = ana_tables["best_category"].iloc[0]
                st.info(f"最強類別：{_safe_str(top_cat.get('類別'))} ｜ 平均20日績效 {(_safe_float(top_cat.get('平均20日績效'), 0) or 0):.2f}% ｜ 20日勝率 {(_safe_float(top_cat.get('20日勝率'), 0) or 0):.2f}%")
            else:
                st.info("最強類別：暫無資料")
        sub_tabs = st.tabs(["模式分析", "類別分析", "等級分析", "明細表"])
        with sub_tabs[0]:
            st.dataframe(ana_tables["mode"], use_container_width=True, hide_index=True)
        with sub_tabs[1]:
            st.dataframe(ana_tables["category"], use_container_width=True, hide_index=True)
        with sub_tabs[2]:
            st.dataframe(ana_tables["grade"], use_container_width=True, hide_index=True)
        with sub_tabs[3]:
            detail_cols = [c for c in [
                "股票代號", "股票名稱", "類別", "推薦模式", "推薦等級", "模式績效標籤",
                "推薦價格", "最新價", "損益金額", "損益幅%",
                "3日績效%", "5日績效%", "10日績效%", "20日績效%",
                "是否達停損", "是否達目標1", "是否達目標2",
                "推薦日期", "持有天數", "推薦理由摘要"
            ] if c in live_df.columns]
            st.dataframe(_format_df(live_df[detail_cols]), use_container_width=True, hide_index=True)

    with tabs[3]:
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
            st.dataframe(trade_df[[c for c in ["股票代號", "股票名稱", "推薦模式", "推薦價格", "實際買進價", "實際賣出價", "實際報酬%", "目前狀態", "備註"] if c in trade_df.columns]], use_container_width=True, hide_index=True)
            st.dataframe(ana_tables["trade_mode"], use_container_width=True, hide_index=True)

    with tabs[4]:
        render_pro_section("Excel 匯出")
        excel_bytes = _build_export_bytes(live_df, ana_tables)
        st.download_button(
            "📥 下載 Excel（推薦紀錄 / 模式分析 / 類別分析 / 等級分析 / 實際交易分析 / 最強模式 / 最強類別）",
            data=excel_bytes,
            file_name=f"股神推薦紀錄_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    with tabs[5]:
        render_pro_info_card(
            "同步 / 欄位完整性",
            [
                ("主要來源", "godpick_records.json + Firestore", "雙寫"),
                ("刪除 / 清空", "支援", "總表管理內"),
                ("批次更新", "支援表格編輯 / 刪除 / 清空 / 更新", "已保留"),
                ("前推績效", "3/5/10/20 日績效%", "已整合"),
                ("模式績效標籤", "依模式歷史表現自動標記", "已整合"),
                ("最強模式 / 類別", "依20日績效 + 勝率綜合排序", "已整合"),
                ("Excel 匯出", "推薦紀錄 / 分析表 / 最強榜", "已整合"),
            ],
            chips=["完整版", "不可缺功能", "雙寫同步", "前推績效", "最強模式", "最強類別"],
        )


if __name__ == "__main__":
    main()

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


def _build_export_bytes(df: pd.DataFrame, mode_df: pd.DataFrame, cat_df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        _ensure_godpick_record_columns(df).to_excel(writer, sheet_name="推薦紀錄", index=False)
        mode_df.to_excel(writer, sheet_name="模式統計", index=False)
        cat_df.to_excel(writer, sheet_name="類別統計", index=False)
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
        subtitle="承接 7_股神推薦 寫入結果，支援狀態維護、最新價更新、3/5/10/20日績效回填、模式/類別勝率統計。",
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

    toolbar1, toolbar2, toolbar3, toolbar4 = st.columns(4)
    with toolbar1:
        if st.button("重新載入紀錄", use_container_width=True, type="primary"):
            df = _load_records()
            _save_state_df(df)
            _set_status("推薦紀錄已重新載入", "success")
            st.rerun()
    with toolbar2:
        if st.button("更新最新價 / 損益", use_container_width=True):
            df = _get_state_df()
            df = _refresh_latest_prices(df)
            _save_state_df(df)
            ok = _save_records_dual(df)
            if ok:
                st.rerun()
    with toolbar3:
        if st.button("回填 3/5/10/20 日績效", use_container_width=True):
            df = _get_state_df()
            df = _backfill_perf_columns(df)
            _save_state_df(df)
            ok = _save_records_dual(df)
            if ok:
                st.rerun()
    with toolbar4:
        if st.button("清除快取", use_container_width=True):
            try:
                _get_latest_close.clear()
                _get_forward_return.clear()
            except Exception:
                pass
            st.success("快取已清除")

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

    summary = _build_summary(df)
    render_pro_kpi_row([
        {"label": "推薦紀錄數", "value": summary["count"], "delta": "總筆數", "delta_class": "pro-kpi-delta-flat"},
        {"label": "已實際買進", "value": summary["buy_count"], "delta": "執行數", "delta_class": "pro-kpi-delta-flat"},
        {"label": "已結案", "value": summary["sold_count"], "delta": "賣出/停損/達標", "delta_class": "pro-kpi-delta-flat"},
        {"label": "平均報酬", "value": f"{summary['avg_ret']:.2f}%", "delta": f"勝率 {summary['win_rate']:.1f}%", "delta_class": "pro-kpi-delta-flat"},
    ])

    mode_options = ["全部"] + sorted([x for x in df["推薦模式"].dropna().astype(str).unique().tolist() if x])
    category_options = ["全部"] + sorted([x for x in df["類別"].dropna().astype(str).unique().tolist() if x])
    status_options = ["全部"] + STATUS_OPTIONS

    render_pro_section("篩選 / 查詢")
    f1, f2, f3, f4 = st.columns([2, 2, 2, 3])
    with f1:
        mode_filter = st.selectbox("推薦模式", mode_options, index=0)
    with f2:
        category_filter = st.selectbox("類別", category_options, index=0)
    with f3:
        status_filter = st.selectbox("目前狀態", status_options, index=0)
    with f4:
        keyword = st.text_input("關鍵字（代號 / 名稱 / 標籤 / 理由）", value="")

    show_df = df.copy()
    if mode_filter != "全部":
        show_df = show_df[show_df["推薦模式"].astype(str) == mode_filter].copy()
    if category_filter != "全部":
        show_df = show_df[show_df["類別"].astype(str) == category_filter].copy()
    if status_filter != "全部":
        show_df = show_df[show_df["目前狀態"].astype(str) == status_filter].copy()
    if _safe_str(keyword):
        kw = _safe_str(keyword)
        mask = (
            show_df["股票代號"].astype(str).str.contains(kw, case=False, na=False)
            | show_df["股票名稱"].astype(str).str.contains(kw, case=False, na=False)
            | show_df["推薦標籤"].astype(str).str.contains(kw, case=False, na=False)
            | show_df["推薦理由摘要"].astype(str).str.contains(kw, case=False, na=False)
        )
        show_df = show_df[mask].copy()

    render_pro_section("批次狀態更新")
    select_options = show_df["record_id"].astype(str).tolist()
    label_map = {str(r["record_id"]): f"{r['股票代號']} {r['股票名稱']}｜{r['目前狀態']}｜{_safe_str(r['推薦模式'])}" for _, r in show_df.iterrows()}

    b1, b2, b3, b4 = st.columns([4, 2, 2, 2])
    with b1:
        selected_ids = st.multiselect(
            "勾選要更新的紀錄",
            options=select_options,
            format_func=lambda x: label_map.get(str(x), str(x)),
            key=_k("selected_ids"),
        )
    with b2:
        new_status = st.selectbox("更新狀態", STATUS_OPTIONS, index=0, key=_k("batch_status"))
    with b3:
        perf_label = st.selectbox("模式績效標籤", PERF_LABEL_OPTIONS, index=0, key=_k("batch_perf_label"))
    with b4:
        mark_buy = st.selectbox("是否已實際買進", ["不變", "是", "否"], index=0, key=_k("batch_buy_flag"))

    u1, u2, u3 = st.columns([2, 2, 4])
    with u1:
        buy_price_input = st.number_input("實際買進價（0=不變）", min_value=0.0, value=0.0, step=0.01, key=_k("batch_buy_price"))
    with u2:
        sell_price_input = st.number_input("實際賣出價（0=不變）", min_value=0.0, value=0.0, step=0.01, key=_k("batch_sell_price"))
    with u3:
        note_input = st.text_input("備註（批次追加）", value="", key=_k("batch_note"))

    if st.button("套用批次更新", use_container_width=True):
        if not selected_ids:
            st.warning("請先勾選要更新的紀錄。")
        else:
            work = df.copy()
            selected_set = set(selected_ids)
            for idx in work.index:
                rec_id = _safe_str(work.at[idx, "record_id"])
                if rec_id not in selected_set:
                    continue
                work.at[idx, "目前狀態"] = new_status
                if perf_label:
                    work.at[idx, "模式績效標籤"] = perf_label
                if mark_buy == "是":
                    work.at[idx, "是否已實際買進"] = True
                elif mark_buy == "否":
                    work.at[idx, "是否已實際買進"] = False
                if buy_price_input > 0:
                    work.at[idx, "實際買進價"] = buy_price_input
                if sell_price_input > 0:
                    work.at[idx, "實際賣出價"] = sell_price_input
                if _safe_str(note_input):
                    old_note = _safe_str(work.at[idx, "備註"])
                    work.at[idx, "備註"] = f"{old_note}｜{note_input}" if old_note else note_input
                recalc = _recalc_row(work.loc[idx].to_dict())
                for k2, v2 in recalc.items():
                    if k2 in work.columns:
                        work.at[idx, k2] = v2
            _save_state_df(work)
            ok = _save_records_dual(work)
            if ok:
                st.rerun()

    render_pro_section("推薦紀錄總表")
    editable_df = show_df[[
        "record_id", "股票代號", "股票名稱", "市場別", "類別", "推薦模式", "推薦等級", "推薦總分",
        "推薦價格", "停損價", "賣出目標1", "賣出目標2", "目前狀態", "是否已實際買進",
        "實際買進價", "實際賣出價", "實際報酬%", "最新價", "損益金額", "損益幅%",
        "是否達停損", "是否達目標1", "是否達目標2", "持有天數", "模式績效標籤", "備註",
        "3日績效%", "5日績效%", "10日績效%", "20日績效%", "推薦日期", "推薦時間",
        "推薦標籤", "推薦理由摘要",
    ]].copy()

    edited = st.data_editor(
        editable_df,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        key=_k("record_editor"),
        column_config={
            "record_id": st.column_config.TextColumn("record_id", disabled=True, width="medium"),
            "股票代號": st.column_config.TextColumn("股票代號", disabled=True),
            "股票名稱": st.column_config.TextColumn("股票名稱", disabled=True),
            "推薦模式": st.column_config.TextColumn("推薦模式", disabled=True),
            "推薦日期": st.column_config.TextColumn("推薦日期", disabled=True),
            "推薦時間": st.column_config.TextColumn("推薦時間", disabled=True),
            "目前狀態": st.column_config.SelectboxColumn("目前狀態", options=STATUS_OPTIONS),
            "是否已實際買進": st.column_config.CheckboxColumn("是否已實際買進"),
            "是否達停損": st.column_config.CheckboxColumn("是否達停損"),
            "是否達目標1": st.column_config.CheckboxColumn("是否達目標1"),
            "是否達目標2": st.column_config.CheckboxColumn("是否達目標2"),
            "模式績效標籤": st.column_config.SelectboxColumn("模式績效標籤", options=PERF_LABEL_OPTIONS),
            "推薦理由摘要": st.column_config.TextColumn("推薦理由摘要", width="large", disabled=True),
            "備註": st.column_config.TextColumn("備註", width="large"),
        },
    )

    s1, s2 = st.columns([2, 2])
    with s1:
        if st.button("儲存表格修改", use_container_width=True, type="primary"):
            work = df.copy()
            edit_map = {str(r["record_id"]): dict(r) for _, r in edited.iterrows()}
            for idx in work.index:
                rec_id = _safe_str(work.at[idx, "record_id"])
                if rec_id not in edit_map:
                    continue
                src = edit_map[rec_id]
                for c in [
                    "市場別", "類別", "目前狀態", "是否已實際買進", "實際買進價", "實際賣出價",
                    "是否達停損", "是否達目標1", "是否達目標2", "模式績效標籤", "備註"
                ]:
                    work.at[idx, c] = src.get(c)
                recalc = _recalc_row(work.loc[idx].to_dict())
                for k2, v2 in recalc.items():
                    if k2 in work.columns:
                        work.at[idx, k2] = v2
            _save_state_df(work)
            ok = _save_records_dual(work)
            if ok:
                st.rerun()
    with s2:
        mode_df = _build_mode_stats(show_df)
        cat_df = _build_category_stats(show_df)
        excel_bytes = _build_export_bytes(show_df, mode_df, cat_df)
        st.download_button(
            label="匯出推薦紀錄 Excel",
            data=excel_bytes,
            file_name=f"股神推薦紀錄_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    tabs = st.tabs(["完整紀錄", "模式統計", "類別統計", "欄位對齊說明"])

    with tabs[0]:
        st.dataframe(_format_df(show_df), use_container_width=True, hide_index=True)

    with tabs[1]:
        mode_df = _build_mode_stats(show_df)
        if mode_df.empty:
            st.info("目前沒有可統計資料。")
        else:
            mode_show = mode_df.copy()
            for c in ["平均報酬", "勝率"]:
                mode_show[c] = mode_show[c].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")
            st.dataframe(mode_show, use_container_width=True, hide_index=True)

    with tabs[2]:
        cat_df = _build_category_stats(show_df)
        if cat_df.empty:
            st.info("目前沒有可統計資料。")
        else:
            cat_show = cat_df.copy()
            for c in ["平均報酬", "勝率"]:
                cat_show[c] = cat_show[c].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "")
            st.dataframe(cat_show, use_container_width=True, hide_index=True)

    with tabs[3]:
        render_pro_info_card(
            "7 / 8 欄位對齊",
            [
                ("主鍵", "record_id", "已對齊"),
                ("推薦時間資訊", "推薦日期 / 推薦時間", "已對齊"),
                ("價格欄位", "推薦價格 / 停損價 / 賣出目標1 / 賣出目標2", "已對齊"),
                ("評分欄位", "技術結構分數 / 起漲前兆分數 / 交易可行分數 / 類股熱度分數", "已對齊"),
                ("相對強弱", "同類股領先幅度 / 是否領先同類股", "已對齊"),
                ("描述欄位", "推薦標籤 / 推薦理由摘要", "已對齊"),
                ("績效預留", "3/5/10/20 日績效%", "已補上"),
                ("手動維護", "已買進 / 已賣出 / 停損 / 達標 / 備註 / 績效標籤", "可直接編輯"),
            ],
            chips=["完整可貼上", "欄位對齊", "可編輯", "雙寫同步", "績效回填"],
        )


if __name__ == "__main__":
    main()

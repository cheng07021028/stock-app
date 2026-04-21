# pages/8_股神推薦紀錄.py
# -*- coding: utf-8 -*-

import io
import json
import math
import uuid
import base64
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="股神推薦紀錄", page_icon="📒", layout="wide")


# =========================================================
# utils 相容
# =========================================================
def _fallback_inject_pro_theme():
    return None

def _fallback_render_pro_hero(title: str, subtitle: str = "", key: str = None):
    st.title(title)
    if subtitle:
        st.caption(subtitle)

def _fallback_render_pro_section(title: str, subtitle: str = "", key: str = None):
    st.subheader(title)
    if subtitle:
        st.caption(subtitle)

def _fallback_render_pro_kpi_row(items, key: str = None):
    if not items:
        return
    cols = st.columns(len(items))
    for c, item in zip(cols, items):
        with c:
            st.metric(item.get("label", ""), item.get("value", ""), item.get("delta", None))

def _fallback_format_number(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return "-"
        if isinstance(x, (int, np.integer)):
            return f"{int(x):,}"
        if isinstance(x, (float, np.floating)):
            return f"{float(x):,.2f}"
        return str(x)
    except Exception:
        return str(x)

def _fallback_get_history_data(code: str, start_date=None, end_date=None):
    return pd.DataFrame()

def _fallback_get_all_code_name_map():
    return {}

try:
    from utils import (
        inject_pro_theme,
        render_pro_hero,
        render_pro_section,
        render_pro_kpi_row,
        format_number,
        get_history_data,
        get_all_code_name_map,
    )
except Exception:
    inject_pro_theme = _fallback_inject_pro_theme
    render_pro_hero = _fallback_render_pro_hero
    render_pro_section = _fallback_render_pro_section
    render_pro_kpi_row = _fallback_render_pro_kpi_row
    format_number = _fallback_format_number
    get_history_data = _fallback_get_history_data
    get_all_code_name_map = _fallback_get_all_code_name_map


def safe_render_pro_hero(title: str, subtitle: str = "", key: str = None):
    try:
        render_pro_hero(title, subtitle, key=key)
    except TypeError:
        try:
            render_pro_hero(title, subtitle)
        except Exception:
            _fallback_render_pro_hero(title, subtitle, key)
    except Exception:
        _fallback_render_pro_hero(title, subtitle, key)

def safe_render_pro_section(title: str, subtitle: str = "", key: str = None):
    try:
        render_pro_section(title, subtitle, key=key)
    except TypeError:
        try:
            render_pro_section(title, subtitle)
        except Exception:
            _fallback_render_pro_section(title, subtitle, key)
    except Exception:
        _fallback_render_pro_section(title, subtitle, key)

def safe_render_pro_kpi_row(items, key: str = None):
    try:
        render_pro_kpi_row(items, key=key)
    except TypeError:
        try:
            render_pro_kpi_row(items)
        except Exception:
            _fallback_render_pro_kpi_row(items, key)
    except Exception:
        _fallback_render_pro_kpi_row(items, key)


inject_pro_theme()
safe_render_pro_hero(
    "📒 股神推薦紀錄",
    "追蹤 7_股神推薦 推薦股票，支援 GitHub + Firestore 雙寫、每日更新、實際交易分析、Excel 匯出。",
    key="godpick_record_hero",
)


# =========================================================
# 常數
# =========================================================
DEFAULT_GITHUB_PATH = "godpick_records.json"
DEFAULT_FIRESTORE_COLLECTION = "godpick_records"

CORE_COLUMNS = [
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
]

NUMERIC_COLUMNS = [
    "推薦總分",
    "技術結構分數",
    "起漲前兆分數",
    "交易可行分數",
    "類股熱度分數",
    "同類股領先幅度",
    "推薦價格",
    "停損價",
    "賣出目標1",
    "賣出目標2",
    "實際買進價",
    "實際賣出價",
    "實際報酬%",
    "最新價",
    "損益金額",
    "損益幅%",
    "持有天數",
]

BOOL_COLUMNS = [
    "是否領先同類股",
    "是否已實際買進",
    "是否達停損",
    "是否達目標1",
    "是否達目標2",
]

STATUS_OPTIONS = ["觀察", "持有", "已出場", "停損", "達標"]

DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H:%M:%S"
DT_FMT = "%Y-%m-%d %H:%M:%S"


# =========================================================
# secrets
# =========================================================
def _safe_secret_get(key: str, default=None):
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default

GITHUB_TOKEN = _safe_secret_get("GITHUB_TOKEN", "")
GITHUB_REPO_OWNER = _safe_secret_get("GITHUB_REPO_OWNER", "")
GITHUB_REPO_NAME = _safe_secret_get("GITHUB_REPO_NAME", "")
GITHUB_REPO_BRANCH = _safe_secret_get("GITHUB_REPO_BRANCH", "main")
GODPICK_RECORDS_GITHUB_PATH = _safe_secret_get("GODPICK_RECORDS_GITHUB_PATH", DEFAULT_GITHUB_PATH)
FIRESTORE_COLLECTION = _safe_secret_get("GODPICK_RECORDS_FIRESTORE_COLLECTION", DEFAULT_FIRESTORE_COLLECTION)


# =========================================================
# 小工具
# =========================================================
def now_ts() -> str:
    return datetime.now().strftime(DT_FMT)

def now_date() -> str:
    return datetime.now().strftime(DATE_FMT)

def now_time() -> str:
    return datetime.now().strftime(TIME_FMT)

def safe_str(v, default=""):
    try:
        if v is None:
            return default
        if isinstance(v, float) and np.isnan(v):
            return default
        return str(v).strip()
    except Exception:
        return default

def to_float(v):
    try:
        if v is None or v == "":
            return np.nan
        return float(v)
    except Exception:
        return np.nan

def to_bool(v):
    if isinstance(v, bool):
        return v
    if pd.isna(v):
        return False
    s = str(v).strip().lower()
    return s in {"true", "1", "yes", "y", "是"}

def create_record_id(code: str, rec_date: str, rec_time: str, mode: str) -> str:
    raw = f"{safe_str(code)}|{safe_str(rec_date)}|{safe_str(rec_time)}|{safe_str(mode)}|{uuid.uuid4().hex[:8]}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

def ensure_core_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        df = pd.DataFrame(columns=CORE_COLUMNS)

    for col in CORE_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    for c in NUMERIC_COLUMNS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in BOOL_COLUMNS:
        df[c] = df[c].fillna(False).map(to_bool)

    if "目前狀態" in df.columns:
        df["目前狀態"] = df["目前狀態"].fillna("觀察").replace("", "觀察")

    need_id_mask = df["record_id"].isna() | (df["record_id"].astype(str).str.strip() == "")
    if need_id_mask.any():
        for idx in df[need_id_mask].index:
            df.at[idx, "record_id"] = create_record_id(
                safe_str(df.at[idx, "股票代號"]),
                safe_str(df.at[idx, "推薦日期"]) or now_date(),
                safe_str(df.at[idx, "推薦時間"]) or now_time(),
                safe_str(df.at[idx, "推薦模式"]),
            )

    return df[CORE_COLUMNS].copy()

def records_to_jsonable(df: pd.DataFrame) -> List[Dict[str, Any]]:
    return ensure_core_columns(df.copy()).replace({np.nan: None}).to_dict(orient="records")

def normalize_new_record(d: Dict[str, Any]) -> Dict[str, Any]:
    x = {c: None for c in CORE_COLUMNS}
    x.update(d or {})

    x["股票代號"] = safe_str(x.get("股票代號"))
    x["股票名稱"] = safe_str(x.get("股票名稱"))
    x["市場別"] = safe_str(x.get("市場別"))
    x["類別"] = safe_str(x.get("類別"))
    x["推薦模式"] = safe_str(x.get("推薦模式"))
    x["推薦等級"] = safe_str(x.get("推薦等級"))
    x["推薦標籤"] = safe_str(x.get("推薦標籤"))
    x["推薦理由摘要"] = safe_str(x.get("推薦理由摘要"))
    x["推薦日期"] = safe_str(x.get("推薦日期")) or now_date()
    x["推薦時間"] = safe_str(x.get("推薦時間")) or now_time()
    x["建立時間"] = safe_str(x.get("建立時間")) or now_ts()
    x["更新時間"] = now_ts()
    x["目前狀態"] = safe_str(x.get("目前狀態")) or "觀察"

    for c in BOOL_COLUMNS:
        x[c] = to_bool(x.get(c))

    for c in NUMERIC_COLUMNS:
        x[c] = to_float(x.get(c))

    if not safe_str(x.get("record_id")):
        x["record_id"] = create_record_id(x["股票代號"], x["推薦日期"], x["推薦時間"], x["推薦模式"])

    return x

def safe_sort_df(df_in: pd.DataFrame, by_candidates, ascending=False):
    df_out = df_in.copy()
    if df_out is None or df_out.empty:
        return df_out

    valid_cols = [c for c in by_candidates if c in df_out.columns]
    if not valid_cols:
        return df_out

    asc_list = ascending
    if isinstance(ascending, list):
        asc_list = ascending[:len(valid_cols)]
        if len(asc_list) < len(valid_cols):
            asc_list = asc_list + [ascending[-1]] * (len(valid_cols) - len(asc_list))

    return df_out.sort_values(valid_cols, ascending=asc_list, na_position="last")


# =========================================================
# GitHub
# =========================================================
def github_enabled() -> bool:
    return all([GITHUB_TOKEN, GITHUB_REPO_OWNER, GITHUB_REPO_NAME, GODPICK_RECORDS_GITHUB_PATH])

def github_api_headers():
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }

def read_github_json(path: str) -> List[Dict[str, Any]]:
    if not github_enabled():
        return []

    url = f"https://api.github.com/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/contents/{path}"
    r = requests.get(url, headers=github_api_headers(), params={"ref": GITHUB_REPO_BRANCH}, timeout=30)
    if r.status_code == 404:
        return []
    r.raise_for_status()

    obj = r.json()
    content_b64 = obj.get("content", "")
    if not content_b64:
        return []

    decoded = base64.b64decode(content_b64).decode("utf-8")
    try:
        data = json.loads(decoded)
    except Exception:
        data = []

    if isinstance(data, dict) and "records" in data:
        data = data["records"]
    if not isinstance(data, list):
        data = []
    return data

def write_github_json(path: str, records: List[Dict[str, Any]], commit_message: str = "") -> Dict[str, Any]:
    if not github_enabled():
        return {"ok": False, "msg": "GitHub 未設定完整"}

    url = f"https://api.github.com/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/contents/{path}"
    r = requests.get(url, headers=github_api_headers(), params={"ref": GITHUB_REPO_BRANCH}, timeout=30)

    sha = None
    if r.status_code == 200:
        sha = r.json().get("sha")
    elif r.status_code != 404:
        r.raise_for_status()

    raw = json.dumps(records, ensure_ascii=False, indent=2)
    b64 = base64.b64encode(raw.encode("utf-8")).decode("utf-8")

    payload = {
        "message": commit_message or f"update {path} @ {now_ts()}",
        "content": b64,
        "branch": GITHUB_REPO_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    put_r = requests.put(url, headers=github_api_headers(), json=payload, timeout=30)
    if put_r.status_code not in (200, 201):
        return {"ok": False, "msg": f"GitHub 寫入失敗：{put_r.status_code}"}
    return {"ok": True, "msg": "GitHub 寫入成功"}


# =========================================================
# Firestore
# =========================================================
@st.cache_resource(show_spinner=False)
def get_firestore_client():
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore

        if not firebase_admin._apps:
            if "FIREBASE_SERVICE_ACCOUNT" in st.secrets:
                cred_info = dict(st.secrets["FIREBASE_SERVICE_ACCOUNT"])
                cred = credentials.Certificate(cred_info)
                firebase_admin.initialize_app(cred)
            elif "FIREBASE_SERVICE_ACCOUNT_JSON" in st.secrets:
                raw = st.secrets["FIREBASE_SERVICE_ACCOUNT_JSON"]
                cred_info = json.loads(raw) if isinstance(raw, str) else dict(raw)
                cred = credentials.Certificate(cred_info)
                firebase_admin.initialize_app(cred)
            else:
                return None
        return firestore.client()
    except Exception:
        return None

def firestore_enabled() -> bool:
    return get_firestore_client() is not None

def read_firestore_records(collection_name: str) -> List[Dict[str, Any]]:
    db = get_firestore_client()
    if db is None:
        return []
    try:
        rows = []
        for doc in db.collection(collection_name).stream():
            item = doc.to_dict() or {}
            item["record_id"] = item.get("record_id") or doc.id
            rows.append(item)
        return rows
    except Exception:
        return []

def write_firestore_records(collection_name: str, records: List[Dict[str, Any]]) -> Dict[str, Any]:
    db = get_firestore_client()
    if db is None:
        return {"ok": False, "msg": "Firestore 未設定完整"}

    try:
        batch = db.batch()
        col_ref = db.collection(collection_name)

        existing_docs = list(col_ref.stream())
        existing_ids = {d.id for d in existing_docs}
        new_ids = set()

        for rec in records:
            rid = safe_str(rec.get("record_id"))
            if not rid:
                rid = create_record_id(
                    rec.get("股票代號", ""),
                    rec.get("推薦日期", now_date()),
                    rec.get("推薦時間", now_time()),
                    rec.get("推薦模式", "")
                )
                rec["record_id"] = rid
            new_ids.add(rid)
            batch.set(col_ref.document(rid), rec)

        for rid in existing_ids - new_ids:
            batch.delete(col_ref.document(rid))

        batch.commit()
        return {"ok": True, "msg": "Firestore 寫入成功"}
    except Exception as e:
        return {"ok": False, "msg": f"Firestore 寫入失敗：{e}"}


# =========================================================
# 載入 / 儲存
# =========================================================
def merge_sources_by_latest(github_rows: List[Dict[str, Any]], firestore_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for r in github_rows or []:
        x = dict(r)
        x["_source"] = "github"
        rows.append(x)
    for r in firestore_rows or []:
        x = dict(r)
        x["_source"] = "firestore"
        rows.append(x)

    if not rows:
        return ensure_core_columns(pd.DataFrame())

    df = ensure_core_columns(pd.DataFrame(rows))
    temp = df.copy()
    temp["_upd"] = pd.to_datetime(temp["更新時間"], errors="coerce")
    temp = temp.sort_values(["record_id", "_upd"], ascending=[True, False], na_position="last")
    temp = temp.drop_duplicates(subset=["record_id"], keep="first")
    return ensure_core_columns(temp.drop(columns=["_upd"], errors="ignore"))

@st.cache_data(show_spinner=False, ttl=60)
def load_all_records_cached(cache_buster: int = 0) -> pd.DataFrame:
    gh_rows = read_github_json(GODPICK_RECORDS_GITHUB_PATH) if github_enabled() else []
    fs_rows = read_firestore_records(FIRESTORE_COLLECTION) if firestore_enabled() else []
    return merge_sources_by_latest(gh_rows, fs_rows)

def load_records(force_refresh: bool = False) -> pd.DataFrame:
    if force_refresh:
        st.session_state["godpick_records_cache_buster"] = st.session_state.get("godpick_records_cache_buster", 0) + 1
    return ensure_core_columns(load_all_records_cached(st.session_state.get("godpick_records_cache_buster", 0)))

def save_records(df: pd.DataFrame, commit_message: str = "") -> Dict[str, Any]:
    df2 = ensure_core_columns(df.copy())
    df2["更新時間"] = now_ts()
    records = records_to_jsonable(df2)

    gh_result = {"ok": False, "msg": "GitHub 略過"}
    fs_result = {"ok": False, "msg": "Firestore 略過"}

    if github_enabled():
        gh_result = write_github_json(GODPICK_RECORDS_GITHUB_PATH, records, commit_message or f"update godpick records @ {now_ts()}")
    if firestore_enabled():
        fs_result = write_firestore_records(FIRESTORE_COLLECTION, records)

    st.session_state["godpick_records_df"] = df2
    st.session_state["godpick_records_cache_buster"] = st.session_state.get("godpick_records_cache_buster", 0) + 1
    load_all_records_cached.clear()

    return {"github": gh_result, "firestore": fs_result, "ok": gh_result.get("ok", False) or fs_result.get("ok", False)}


# =========================================================
# 7 頁串接
# =========================================================
def get_incoming_from_page7() -> pd.DataFrame:
    for key in ["godpick_selected_to_records", "godpick_records_pending_import", "godpick_rec_selected_df"]:
        obj = st.session_state.get(key)
        if isinstance(obj, pd.DataFrame) and not obj.empty:
            return obj.copy()
        if isinstance(obj, list) and obj:
            return pd.DataFrame(obj)
    return pd.DataFrame()

def convert_page7_df_to_records(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return ensure_core_columns(pd.DataFrame())

    mapping = {
        "股票代號": "股票代號",
        "股票名稱": "股票名稱",
        "市場別": "市場別",
        "類別": "類別",
        "推薦模式": "推薦模式",
        "推薦等級": "推薦等級",
        "推薦總分": "推薦總分",
        "技術結構分數": "技術結構分數",
        "起漲前兆分數": "起漲前兆分數",
        "交易可行分數": "交易可行分數",
        "類股熱度分數": "類股熱度分數",
        "同類股領先幅度": "同類股領先幅度",
        "是否領先同類股": "是否領先同類股",
        "推薦標籤": "推薦標籤",
        "推薦理由摘要": "推薦理由摘要",
        "推薦價格": "推薦價格",
        "停損價": "停損價",
        "賣出目標1": "賣出目標1",
        "賣出目標2": "賣出目標2",
    }

    x = pd.DataFrame()
    for src, dst in mapping.items():
        x[dst] = df[src] if src in df.columns else np.nan

    x["推薦日期"] = now_date()
    x["推薦時間"] = now_time()
    x["建立時間"] = now_ts()
    x["更新時間"] = now_ts()
    x["目前狀態"] = "觀察"
    x["是否已實際買進"] = False
    x["實際買進價"] = np.nan
    x["實際賣出價"] = np.nan
    x["實際報酬%"] = np.nan
    x["最新價"] = np.nan
    x["最新更新時間"] = ""
    x["損益金額"] = np.nan
    x["損益幅%"] = np.nan
    x["是否達停損"] = False
    x["是否達目標1"] = False
    x["是否達目標2"] = False
    x["持有天數"] = np.nan
    x["模式績效標籤"] = ""
    x["備註"] = ""

    x = ensure_core_columns(x)
    x["record_id"] = [
        create_record_id(
            safe_str(r["股票代號"]),
            safe_str(r["推薦日期"]),
            safe_str(r["推薦時間"]),
            safe_str(r["推薦模式"]),
        )
        for _, r in x.iterrows()
    ]
    return ensure_core_columns(x)

def append_records_dedup(base_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if new_df is None or new_df.empty:
        return ensure_core_columns(base_df.copy())

    merged = pd.concat([ensure_core_columns(base_df.copy()), ensure_core_columns(new_df.copy())], ignore_index=True)
    merged["_dedup"] = (
        merged["record_id"].fillna("").astype(str) + "|"
        + merged["股票代號"].fillna("").astype(str) + "|"
        + merged["推薦日期"].fillna("").astype(str) + "|"
        + merged["推薦時間"].fillna("").astype(str) + "|"
        + merged["推薦模式"].fillna("").astype(str)
    )
    merged["_upd"] = pd.to_datetime(merged["更新時間"], errors="coerce")
    merged = merged.sort_values(["_dedup", "_upd"], ascending=[True, False], na_position="last")
    merged = merged.drop_duplicates(subset=["_dedup"], keep="first")
    return ensure_core_columns(merged.drop(columns=["_dedup", "_upd"], errors="ignore"))


# =========================================================
# 最新價 / 績效
# =========================================================
@st.cache_data(show_spinner=False, ttl=600)
def _fetch_latest_close_cached(code: str, start_date: str, end_date: str) -> Dict[str, Any]:
    try:
        hist = get_history_data(code, start_date=start_date, end_date=end_date)
        if hist is None or hist.empty:
            return {"ok": False, "price": np.nan}

        df = hist.copy()
        price_col = None
        for c in ["Close", "close", "收盤價", "收盤", "Adj Close", "adj close"]:
            if c in df.columns:
                price_col = c
                break
        if price_col is None:
            for c in df.columns:
                cs = str(c).lower()
                if "close" in cs or "收盤" in str(c):
                    price_col = c
                    break
        if price_col is None:
            return {"ok": False, "price": np.nan}

        df = df.dropna(subset=[price_col])
        if df.empty:
            return {"ok": False, "price": np.nan}

        last_price = to_float(df.iloc[-1][price_col])
        return {"ok": True, "price": last_price}
    except Exception:
        return {"ok": False, "price": np.nan}

def calc_system_performance(row: pd.Series) -> pd.Series:
    rec_price = to_float(row.get("推薦價格"))
    last_price = to_float(row.get("最新價"))
    stop_price = to_float(row.get("停損價"))
    tgt1 = to_float(row.get("賣出目標1"))
    tgt2 = to_float(row.get("賣出目標2"))

    if pd.notna(rec_price) and rec_price > 0 and pd.notna(last_price):
        row["損益金額"] = last_price - rec_price
        row["損益幅%"] = (last_price / rec_price - 1) * 100

    if pd.notna(last_price):
        if pd.notna(stop_price) and not to_bool(row.get("是否達停損")):
            row["是否達停損"] = last_price <= stop_price
        if pd.notna(tgt1) and not to_bool(row.get("是否達目標1")):
            row["是否達目標1"] = last_price >= tgt1
        if pd.notna(tgt2) and not to_bool(row.get("是否達目標2")):
            row["是否達目標2"] = last_price >= tgt2

    try:
        rd = pd.to_datetime(safe_str(row.get("推薦日期")), errors="coerce")
        if pd.notna(rd):
            row["持有天數"] = (pd.Timestamp.now().normalize() - rd.normalize()).days
    except Exception:
        pass

    curr_status = safe_str(row.get("目前狀態"))
    if curr_status != "已出場":
        if to_bool(row.get("是否達停損")):
            row["目前狀態"] = "停損"
        elif to_bool(row.get("是否達目標1")) or to_bool(row.get("是否達目標2")):
            if curr_status != "持有":
                row["目前狀態"] = "達標"
    return row

def calc_actual_trade_performance(row: pd.Series) -> pd.Series:
    buy_price = to_float(row.get("實際買進價"))
    sell_price = to_float(row.get("實際賣出價"))
    last_price = to_float(row.get("最新價"))
    is_bought = to_bool(row.get("是否已實際買進"))

    if is_bought and pd.notna(buy_price) and buy_price > 0:
        if pd.notna(sell_price):
            row["實際報酬%"] = (sell_price / buy_price - 1) * 100
            if safe_str(row.get("目前狀態")) not in {"停損", "達標"}:
                row["目前狀態"] = "已出場"
        elif pd.notna(last_price):
            row["實際報酬%"] = (last_price / buy_price - 1) * 100
            if safe_str(row.get("目前狀態")) == "觀察":
                row["目前狀態"] = "持有"
    return row

def update_latest_prices(df: pd.DataFrame, only_active: bool = True) -> pd.DataFrame:
    df2 = ensure_core_columns(df.copy())
    if df2.empty:
        return df2

    if only_active:
        target_idx = df2[~df2["目前狀態"].isin(["已出場"])].index.tolist()
    else:
        target_idx = df2.index.tolist()

    if not target_idx:
        return df2

    codes = sorted({safe_str(df2.at[i, "股票代號"]) for i in target_idx if safe_str(df2.at[i, "股票代號"])})
    start_date = (datetime.now() - timedelta(days=120)).strftime(DATE_FMT)
    end_date = now_date()

    price_map = {}
    progress = st.progress(0, text="更新最新價格中...")
    total = max(len(codes), 1)

    for n, code in enumerate(codes, start=1):
        price_map[code] = _fetch_latest_close_cached(code, start_date, end_date)
        progress.progress(min(n / total, 1.0), text=f"更新最新價格中... {code}")
    progress.empty()

    for idx in target_idx:
        code = safe_str(df2.at[idx, "股票代號"])
        info = price_map.get(code, {})
        if info.get("ok"):
            df2.at[idx, "最新價"] = to_float(info.get("price"))
            df2.at[idx, "最新更新時間"] = now_ts()

    df2 = df2.apply(calc_system_performance, axis=1)
    df2 = df2.apply(calc_actual_trade_performance, axis=1)
    df2["更新時間"] = now_ts()
    return ensure_core_columns(df2)


# =========================================================
# 初始化
# =========================================================
if "godpick_records_cache_buster" not in st.session_state:
    st.session_state["godpick_records_cache_buster"] = 0

if "godpick_records_df" not in st.session_state:
    st.session_state["godpick_records_df"] = load_records(False)

def refresh_from_source(force=True):
    st.session_state["godpick_records_df"] = load_records(force)


# =========================================================
# 快取分析表
# =========================================================
df = ensure_core_columns(st.session_state["godpick_records_df"].copy())

@st.cache_data(show_spinner=False, ttl=60)
def build_analysis_tables(df_json: str):
    local_df = ensure_core_columns(pd.DataFrame(json.loads(df_json)))

    empty_mode = pd.DataFrame(columns=["推薦模式", "筆數", "平均系統報酬", "系統勝率", "達目標1比率", "停損率", "平均推薦總分"])
    empty_category = pd.DataFrame(columns=["類別", "筆數", "平均系統報酬", "系統勝率", "達目標1比率", "停損率"])
    empty_grade = pd.DataFrame(columns=["推薦等級", "筆數", "平均系統報酬", "系統勝率", "達目標1比率", "停損率"])
    empty_trade_mode = pd.DataFrame(columns=["推薦模式", "筆數", "平均實際報酬", "實際勝率"])

    if local_df.empty:
        return {
            "mode": empty_mode,
            "category": empty_category,
            "grade": empty_grade,
            "trade_mode": empty_trade_mode,
        }

    mode_df = local_df.groupby("推薦模式", dropna=False).agg(
        筆數=("record_id", "count"),
        平均系統報酬=("損益幅%", "mean"),
        系統勝率=("損益幅%", lambda s: (s.dropna() > 0).mean() * 100 if len(s.dropna()) else np.nan),
        達目標1比率=("是否達目標1", lambda s: s.mean() * 100 if len(s) else np.nan),
        停損率=("是否達停損", lambda s: s.mean() * 100 if len(s) else np.nan),
        平均推薦總分=("推薦總分", "mean"),
    ).reset_index()

    category_df = local_df.groupby("類別", dropna=False).agg(
        筆數=("record_id", "count"),
        平均系統報酬=("損益幅%", "mean"),
        系統勝率=("損益幅%", lambda s: (s.dropna() > 0).mean() * 100 if len(s.dropna()) else np.nan),
        達目標1比率=("是否達目標1", lambda s: s.mean() * 100 if len(s) else np.nan),
        停損率=("是否達停損", lambda s: s.mean() * 100 if len(s) else np.nan),
    ).reset_index()

    grade_df = local_df.groupby("推薦等級", dropna=False).agg(
        筆數=("record_id", "count"),
        平均系統報酬=("損益幅%", "mean"),
        系統勝率=("損益幅%", lambda s: (s.dropna() > 0).mean() * 100 if len(s.dropna()) else np.nan),
        達目標1比率=("是否達目標1", lambda s: s.mean() * 100 if len(s) else np.nan),
        停損率=("是否達停損", lambda s: s.mean() * 100 if len(s) else np.nan),
    ).reset_index()

    trade_df = local_df[local_df["是否已實際買進"] == True].copy()
    if trade_df.empty:
        trade_mode_df = empty_trade_mode
    else:
        trade_mode_df = trade_df.groupby("推薦模式", dropna=False).agg(
            筆數=("record_id", "count"),
            平均實際報酬=("實際報酬%", "mean"),
            實際勝率=("實際報酬%", lambda s: (s.dropna() > 0).mean() * 100 if len(s.dropna()) else np.nan),
        ).reset_index()

    return {
        "mode": mode_df,
        "category": category_df,
        "grade": grade_df,
        "trade_mode": trade_mode_df,
    }

df_json_key = json.dumps(records_to_jsonable(df), ensure_ascii=False, default=str)
ana_tables = build_analysis_tables(df_json_key)


# =========================================================
# 上方工具列
# =========================================================
top_cols = st.columns([1.1, 1.1, 1.1, 1.1, 1.3, 2.3])

with top_cols[0]:
    if st.button("🔄 重新載入", use_container_width=True):
        refresh_from_source(True)
        st.success("已重新載入雲端資料")
        st.rerun()

with top_cols[1]:
    if st.button("📥 匯入 7 頁", use_container_width=True):
        incoming = get_incoming_from_page7()
        if incoming.empty:
            st.warning("目前沒有可匯入的 7 頁資料")
        else:
            new_df = convert_page7_df_to_records(incoming)
            st.session_state["godpick_records_df"] = append_records_dedup(st.session_state["godpick_records_df"], new_df)
            st.success(f"已匯入 {len(new_df)} 筆，尚未同步")

with top_cols[2]:
    if st.button("💾 儲存同步", use_container_width=True):
        result = save_records(st.session_state["godpick_records_df"], f"update godpick records @ {now_ts()}")
        if result["ok"]:
            st.success(f"同步完成｜GitHub：{result['github'].get('msg','')}｜Firestore：{result['firestore'].get('msg','')}")
        else:
            st.error(f"同步失敗｜GitHub：{result['github'].get('msg','')}｜Firestore：{result['firestore'].get('msg','')}")

with top_cols[3]:
    if st.button("📈 更新最新價", use_container_width=True):
        st.session_state["godpick_records_df"] = update_latest_prices(st.session_state["godpick_records_df"], only_active=True)
        st.success("已更新最新價，尚未同步")

with top_cols[4]:
    only_active_update = st.toggle("只更新未出場", value=True)

with top_cols[5]:
    st.caption(
        f"GitHub：{'✅' if github_enabled() else '❌'} ｜ "
        f"Firestore：{'✅' if firestore_enabled() else '❌'} ｜ "
        f"筆數：{len(df)}"
    )


# =========================================================
# KPI
# =========================================================
def build_kpis(df0: pd.DataFrame):
    if df0.empty:
        return [
            {"label": "總筆數", "value": "0"},
            {"label": "持有中", "value": "0"},
            {"label": "已達標", "value": "0"},
            {"label": "停損", "value": "0"},
            {"label": "平均系統報酬%", "value": "-"},
            {"label": "平均實際報酬%", "value": "-"},
        ]

    avg_sys = df0["損益幅%"].dropna().mean()
    avg_real = df0.loc[df0["是否已實際買進"] == True, "實際報酬%"].dropna().mean()

    return [
        {"label": "總筆數", "value": format_number(len(df0))},
        {"label": "持有中", "value": format_number(int((df0["目前狀態"] == "持有").sum()))},
        {"label": "已達標", "value": format_number(int((df0["目前狀態"] == "達標").sum()))},
        {"label": "停損", "value": format_number(int((df0["目前狀態"] == "停損").sum()))},
        {"label": "平均系統報酬%", "value": "-" if pd.isna(avg_sys) else f"{avg_sys:.2f}%"},
        {"label": "平均實際報酬%", "value": "-" if pd.isna(avg_real) else f"{avg_real:.2f}%"},
    ]

safe_render_pro_kpi_row(build_kpis(df), key="godpick_record_kpis")


# =========================================================
# Tabs
# =========================================================
tabs = st.tabs([
    "📋 總表管理",
    "➕ 手動新增",
    "📊 績效分析",
    "💹 實際交易分析",
    "📤 Excel 匯出",
    "⚙️ 同步檢查",
])


# =========================================================
# Tab 1
# =========================================================
with tabs[0]:
    safe_render_pro_section("推薦紀錄總表", "不刪功能，加速版：先篩選再編輯，減少 data_editor 負擔。")

    filter_cols = st.columns([1.2, 1.2, 1.2, 1.2, 1.3, 1.1, 1.2])

    with filter_cols[0]:
        keyword = st.text_input("搜尋代號 / 名稱 / 理由", value="")
    with filter_cols[1]:
        mode_filter = st.multiselect("推薦模式", sorted([x for x in df["推薦模式"].dropna().unique() if str(x).strip()]), default=[])
    with filter_cols[2]:
        category_filter = st.multiselect("類別", sorted([x for x in df["類別"].dropna().unique() if str(x).strip()]), default=[])
    with filter_cols[3]:
        status_filter = st.multiselect("目前狀態", STATUS_OPTIONS, default=[])
    with filter_cols[4]:
        bought_filter = st.selectbox("是否已買進", ["全部", "是", "否"], index=0)
    with filter_cols[5]:
        sort_by = st.selectbox("排序", ["推薦日期", "推薦總分", "損益幅%", "實際報酬%", "起漲前兆分數", "交易可行分數", "類股熱度分數", "持有天數"], index=0)
    with filter_cols[6]:
        sort_asc = st.toggle("升冪", value=False)

    view_df = df.copy()

    if keyword.strip():
        kw = keyword.strip().lower()
        mask = (
            view_df["股票代號"].fillna("").astype(str).str.lower().str.contains(kw, na=False)
            | view_df["股票名稱"].fillna("").astype(str).str.lower().str.contains(kw, na=False)
            | view_df["推薦理由摘要"].fillna("").astype(str).str.lower().str.contains(kw, na=False)
        )
        view_df = view_df[mask]

    if mode_filter:
        view_df = view_df[view_df["推薦模式"].isin(mode_filter)]
    if category_filter:
        view_df = view_df[view_df["類別"].isin(category_filter)]
    if status_filter:
        view_df = view_df[view_df["目前狀態"].isin(status_filter)]
    if bought_filter != "全部":
        want = bought_filter == "是"
        view_df = view_df[view_df["是否已實際買進"] == want]

    if sort_by in view_df.columns:
        view_df = view_df.sort_values(sort_by, ascending=sort_asc, na_position="last").reset_index(drop=True)

    st.caption(f"目前顯示 {len(view_df)} / {len(df)} 筆")

    editor_df = view_df[[
        "record_id", "股票代號", "股票名稱", "市場別", "類別",
        "推薦模式", "推薦等級", "推薦總分", "起漲前兆分數", "交易可行分數", "類股熱度分數",
        "推薦價格", "停損價", "賣出目標1", "賣出目標2",
        "最新價", "損益幅%", "目前狀態", "是否已實際買進",
        "實際買進價", "實際賣出價", "實際報酬%",
        "是否達停損", "是否達目標1", "是否達目標2",
        "持有天數", "推薦日期", "推薦時間", "推薦理由摘要", "備註"
    ]].copy()
    editor_df.insert(0, "刪除", False)

    edited_df = st.data_editor(
        editor_df,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        key="godpick_records_editor",
        column_config={
            "刪除": st.column_config.CheckboxColumn("刪除"),
            "record_id": st.column_config.TextColumn("record_id", disabled=True),
            "股票代號": st.column_config.TextColumn("股票代號", disabled=True),
            "股票名稱": st.column_config.TextColumn("股票名稱", disabled=True),
            "市場別": st.column_config.TextColumn("市場別", disabled=True),
            "類別": st.column_config.TextColumn("類別", disabled=True),
            "推薦模式": st.column_config.TextColumn("推薦模式", disabled=True),
            "推薦等級": st.column_config.TextColumn("推薦等級", disabled=True),
            "推薦總分": st.column_config.NumberColumn("推薦總分", format="%.2f", disabled=True),
            "起漲前兆分數": st.column_config.NumberColumn("起漲前兆分數", format="%.2f", disabled=True),
            "交易可行分數": st.column_config.NumberColumn("交易可行分數", format="%.2f", disabled=True),
            "類股熱度分數": st.column_config.NumberColumn("類股熱度分數", format="%.2f", disabled=True),
            "推薦價格": st.column_config.NumberColumn("推薦價格", format="%.2f", disabled=True),
            "停損價": st.column_config.NumberColumn("停損價", format="%.2f", disabled=True),
            "賣出目標1": st.column_config.NumberColumn("賣出目標1", format="%.2f", disabled=True),
            "賣出目標2": st.column_config.NumberColumn("賣出目標2", format="%.2f", disabled=True),
            "最新價": st.column_config.NumberColumn("最新價", format="%.2f", disabled=True),
            "損益幅%": st.column_config.NumberColumn("損益幅%", format="%.2f", disabled=True),
            "目前狀態": st.column_config.SelectboxColumn("目前狀態", options=STATUS_OPTIONS),
            "是否已實際買進": st.column_config.CheckboxColumn("是否已實際買進"),
            "實際買進價": st.column_config.NumberColumn("實際買進價", format="%.2f"),
            "實際賣出價": st.column_config.NumberColumn("實際賣出價", format="%.2f"),
            "實際報酬%": st.column_config.NumberColumn("實際報酬%", format="%.2f"),
            "是否達停損": st.column_config.CheckboxColumn("是否達停損"),
            "是否達目標1": st.column_config.CheckboxColumn("是否達目標1"),
            "是否達目標2": st.column_config.CheckboxColumn("是否達目標2"),
            "持有天數": st.column_config.NumberColumn("持有天數", format="%d", disabled=True),
            "推薦日期": st.column_config.TextColumn("推薦日期", disabled=True),
            "推薦時間": st.column_config.TextColumn("推薦時間", disabled=True),
            "推薦理由摘要": st.column_config.TextColumn("推薦理由摘要", disabled=True, width="large"),
            "備註": st.column_config.TextColumn("備註", width="large"),
        },
    )

    action_cols = st.columns([1.2, 1.2, 1.2, 3.4])

    with action_cols[0]:
        if st.button("✅ 套用編輯", use_container_width=True):
            master = st.session_state["godpick_records_df"].copy()
            edit_map = edited_df.set_index("record_id").to_dict(orient="index")

            for idx in master.index:
                rid = master.at[idx, "record_id"]
                if rid in edit_map:
                    item = edit_map[rid]
                    master.at[idx, "目前狀態"] = item.get("目前狀態", master.at[idx, "目前狀態"])
                    master.at[idx, "是否已實際買進"] = to_bool(item.get("是否已實際買進", master.at[idx, "是否已實際買進"]))
                    master.at[idx, "實際買進價"] = to_float(item.get("實際買進價", master.at[idx, "實際買進價"]))
                    master.at[idx, "實際賣出價"] = to_float(item.get("實際賣出價", master.at[idx, "實際賣出價"]))
                    master.at[idx, "實際報酬%"] = to_float(item.get("實際報酬%", master.at[idx, "實際報酬%"]))
                    master.at[idx, "是否達停損"] = to_bool(item.get("是否達停損", master.at[idx, "是否達停損"]))
                    master.at[idx, "是否達目標1"] = to_bool(item.get("是否達目標1", master.at[idx, "是否達目標1"]))
                    master.at[idx, "是否達目標2"] = to_bool(item.get("是否達目標2", master.at[idx, "是否達目標2"]))
                    master.at[idx, "備註"] = safe_str(item.get("備註", master.at[idx, "備註"]))
                    master.at[idx, "更新時間"] = now_ts()

            master = master.apply(calc_actual_trade_performance, axis=1)
            master = master.apply(calc_system_performance, axis=1)
            st.session_state["godpick_records_df"] = ensure_core_columns(master)
            st.success("已套用，尚未同步")

    with action_cols[1]:
        if st.button("🗑️ 刪除勾選", use_container_width=True):
            delete_ids = edited_df.loc[edited_df["刪除"] == True, "record_id"].tolist()
            if not delete_ids:
                st.warning("請先勾選要刪除的資料")
            else:
                master = st.session_state["godpick_records_df"].copy()
                master = master[~master["record_id"].isin(delete_ids)].copy()
                st.session_state["godpick_records_df"] = ensure_core_columns(master)
                st.success(f"已刪除 {len(delete_ids)} 筆，尚未同步")

    with action_cols[2]:
        if st.button("📈 更新價格", use_container_width=True):
            st.session_state["godpick_records_df"] = update_latest_prices(st.session_state["godpick_records_df"], only_active=only_active_update)
            st.success("已更新價格，尚未同步")

    with action_cols[3]:
        st.caption("流程：篩選 → 編輯 → 套用 → 更新價格 → 儲存同步")


# =========================================================
# Tab 2
# =========================================================
with tabs[1]:
    safe_render_pro_section("手動新增推薦紀錄", "保留全部欄位，直接補登")

    code_name_map = {}
    try:
        code_name_map = get_all_code_name_map() or {}
    except Exception:
        code_name_map = {}

    c1 = st.columns(5)
    with c1[0]:
        manual_code = st.text_input("股票代號")
    with c1[1]:
        manual_name = st.text_input("股票名稱", value=code_name_map.get(manual_code, "") if manual_code else "")
    with c1[2]:
        manual_market = st.selectbox("市場別", ["", "上市", "上櫃", "興櫃"], index=0)
    with c1[3]:
        manual_category = st.text_input("類別")
    with c1[4]:
        manual_mode = st.selectbox("推薦模式", ["", "飆股模式", "波段模式", "領頭羊模式", "綜合模式"], index=4)

    c2 = st.columns(6)
    with c2[0]:
        manual_grade = st.selectbox("推薦等級", ["", "S", "A", "B", "C"], index=1)
    with c2[1]:
        manual_total = st.number_input("推薦總分", min_value=0.0, max_value=1000.0, value=85.0, step=0.1)
    with c2[2]:
        manual_struct = st.number_input("技術結構分數", min_value=0.0, max_value=1000.0, value=0.0, step=0.1)
    with c2[3]:
        manual_signal = st.number_input("起漲前兆分數", min_value=0.0, max_value=1000.0, value=0.0, step=0.1)
    with c2[4]:
        manual_trade = st.number_input("交易可行分數", min_value=0.0, max_value=1000.0, value=0.0, step=0.1)
    with c2[5]:
        manual_heat = st.number_input("類股熱度分數", min_value=0.0, max_value=1000.0, value=0.0, step=0.1)

    c3 = st.columns(5)
    with c3[0]:
        manual_lead_gap = st.number_input("同類股領先幅度", value=0.0, step=0.1)
    with c3[1]:
        manual_lead_flag = st.checkbox("是否領先同類股", value=False)
    with c3[2]:
        manual_rec_price = st.number_input("推薦價格", min_value=0.0, value=0.0, step=0.01)
    with c3[3]:
        manual_stop = st.number_input("停損價", min_value=0.0, value=0.0, step=0.01)
    with c3[4]:
        manual_tgt1 = st.number_input("賣出目標1", min_value=0.0, value=0.0, step=0.01)

    c4 = st.columns(5)
    with c4[0]:
        manual_tgt2 = st.number_input("賣出目標2", min_value=0.0, value=0.0, step=0.01)
    with c4[1]:
        manual_date = st.date_input("推薦日期", value=datetime.now().date())
    with c4[2]:
        manual_time = st.text_input("推薦時間", value=now_time())
    with c4[3]:
        manual_status = st.selectbox("目前狀態", STATUS_OPTIONS, index=0)
    with c4[4]:
        manual_bought = st.checkbox("是否已實際買進", value=False)

    c5 = st.columns(3)
    with c5[0]:
        manual_buy_price = st.number_input("實際買進價", min_value=0.0, value=0.0, step=0.01)
    with c5[1]:
        manual_sell_price = st.number_input("實際賣出價", min_value=0.0, value=0.0, step=0.01)
    with c5[2]:
        manual_tags = st.text_input("推薦標籤")

    manual_reason = st.text_area("推薦理由摘要", height=100)
    manual_note = st.text_area("備註", height=80)

    def build_manual_record():
        rec = normalize_new_record({
            "股票代號": manual_code.strip(),
            "股票名稱": manual_name.strip(),
            "市場別": manual_market,
            "類別": manual_category.strip(),
            "推薦模式": manual_mode,
            "推薦等級": manual_grade,
            "推薦總分": manual_total,
            "技術結構分數": manual_struct,
            "起漲前兆分數": manual_signal,
            "交易可行分數": manual_trade,
            "類股熱度分數": manual_heat,
            "同類股領先幅度": manual_lead_gap,
            "是否領先同類股": manual_lead_flag,
            "推薦標籤": manual_tags.strip(),
            "推薦理由摘要": manual_reason.strip(),
            "推薦價格": manual_rec_price if manual_rec_price > 0 else np.nan,
            "停損價": manual_stop if manual_stop > 0 else np.nan,
            "賣出目標1": manual_tgt1 if manual_tgt1 > 0 else np.nan,
            "賣出目標2": manual_tgt2 if manual_tgt2 > 0 else np.nan,
            "推薦日期": manual_date.strftime(DATE_FMT),
            "推薦時間": manual_time.strip() or now_time(),
            "目前狀態": manual_status,
            "是否已實際買進": manual_bought,
            "實際買進價": manual_buy_price if manual_buy_price > 0 else np.nan,
            "實際賣出價": manual_sell_price if manual_sell_price > 0 else np.nan,
            "備註": manual_note.strip(),
        })
        add_df = ensure_core_columns(pd.DataFrame([rec]))
        add_df = add_df.apply(calc_actual_trade_performance, axis=1)
        add_df = add_df.apply(calc_system_performance, axis=1)
        return add_df

    action_cols = st.columns([1.2, 1.2, 4.0])

    with action_cols[0]:
        if st.button("➕ 加入紀錄", use_container_width=True):
            if not manual_code.strip():
                st.warning("請輸入股票代號")
            else:
                st.session_state["godpick_records_df"] = append_records_dedup(st.session_state["godpick_records_df"], build_manual_record())
                st.success("已加入，尚未同步")

    with action_cols[1]:
        if st.button("➕ 加入並同步", use_container_width=True):
            if not manual_code.strip():
                st.warning("請輸入股票代號")
            else:
                st.session_state["godpick_records_df"] = append_records_dedup(st.session_state["godpick_records_df"], build_manual_record())
                result = save_records(st.session_state["godpick_records_df"], f"add one godpick record @ {now_ts()}")
                if result["ok"]:
                    st.success("已加入並同步成功")
                else:
                    st.error(f"同步失敗｜GitHub：{result['github'].get('msg','')}｜Firestore：{result['firestore'].get('msg','')}")


# =========================================================
# Tab 3
# =========================================================
with tabs[2]:
    safe_render_pro_section("系統推薦績效分析", "以推薦價格對照最新價")

    valid_sys = df["損益幅%"].dropna()
    avg_sys_ret = valid_sys.mean() if not valid_sys.empty else np.nan
    win_rate_sys = (valid_sys > 0).mean() * 100 if not valid_sys.empty else np.nan
    target_rate = df["是否達目標1"].mean() * 100 if len(df) > 0 else np.nan
    stop_rate = df["是否達停損"].mean() * 100 if len(df) > 0 else np.nan

    safe_render_pro_kpi_row([
        {"label": "系統樣本數", "value": format_number(len(df))},
        {"label": "系統勝率", "value": "-" if pd.isna(win_rate_sys) else f"{win_rate_sys:.2f}%"},
        {"label": "平均系統報酬%", "value": "-" if pd.isna(avg_sys_ret) else f"{avg_sys_ret:.2f}%"},
        {"label": "達目標1比率", "value": "-" if pd.isna(target_rate) else f"{target_rate:.2f}%"},
        {"label": "停損率", "value": "-" if pd.isna(stop_rate) else f"{stop_rate:.2f}%"},
    ])

    sub_tabs = st.tabs(["模式分析", "類別分析", "等級分析", "明細表"])

    with sub_tabs[0]:
        st.dataframe(
            safe_sort_df(ana_tables["mode"], ["平均系統報酬", "系統勝率", "勝率"], ascending=[False, False, False]),
            use_container_width=True,
            hide_index=True,
        )
    with sub_tabs[1]:
        st.dataframe(
            safe_sort_df(ana_tables["category"], ["平均系統報酬", "系統勝率", "勝率"], ascending=[False, False, False]),
            use_container_width=True,
            hide_index=True,
        )
    with sub_tabs[2]:
        st.dataframe(
            safe_sort_df(ana_tables["grade"], ["平均系統報酬", "系統勝率", "勝率"], ascending=[False, False, False]),
            use_container_width=True,
            hide_index=True,
        )
    with sub_tabs[3]:
        st.dataframe(
            df[[
                "股票代號", "股票名稱", "類別", "推薦模式", "推薦等級",
                "推薦價格", "最新價", "損益金額", "損益幅%", "是否達停損", "是否達目標1", "是否達目標2",
                "推薦日期", "持有天數", "推薦理由摘要"
            ]],
            use_container_width=True,
            hide_index=True,
        )


# =========================================================
# Tab 4
# =========================================================
with tabs[3]:
    safe_render_pro_section("實際交易分析", "只統計已實際買進樣本")

    trade_df = df[df["是否已實際買進"] == True].copy()

    if trade_df.empty:
        st.info("目前沒有已實際買進樣本")
    else:
        actual_ret = trade_df["實際報酬%"].dropna()
        win_rate = (actual_ret > 0).mean() * 100 if not actual_ret.empty else np.nan
        avg_ret = actual_ret.mean() if not actual_ret.empty else np.nan
        exit_cnt = int(trade_df["實際賣出價"].notna().sum())
        holding_cnt = int(((trade_df["實際賣出價"].isna()) & (trade_df["是否已實際買進"] == True)).sum())
        stop_cnt = int((trade_df["目前狀態"] == "停損").sum())

        safe_render_pro_kpi_row([
            {"label": "實際買進筆數", "value": format_number(len(trade_df))},
            {"label": "已出場筆數", "value": format_number(exit_cnt)},
            {"label": "持有中筆數", "value": format_number(holding_cnt)},
            {"label": "實際勝率", "value": "-" if pd.isna(win_rate) else f"{win_rate:.2f}%"},
            {"label": "平均實際報酬%", "value": "-" if pd.isna(avg_ret) else f"{avg_ret:.2f}%"},
            {"label": "停損筆數", "value": format_number(stop_cnt)},
        ])

        sub_tabs = st.tabs(["模式績效", "已買進明細"])

        with sub_tabs[0]:
            st.dataframe(
                safe_sort_df(ana_tables["trade_mode"], ["平均實際報酬", "實際勝率"], ascending=[False, False]),
                use_container_width=True,
                hide_index=True,
            )

        with sub_tabs[1]:
            st.dataframe(
                trade_df[[
                    "股票代號", "股票名稱", "類別", "推薦模式", "推薦等級",
                    "實際買進價", "實際賣出價", "最新價", "實際報酬%", "目前狀態",
                    "推薦日期", "持有天數", "備註"
                ]],
                use_container_width=True,
                hide_index=True,
            )


# =========================================================
# Tab 5
# =========================================================
# =========================================================
# Tab 5
# =========================================================
with tabs[4]:
    safe_render_pro_section("Excel 匯出", "直接用目前資料匯出，不重算")

    @st.cache_data(show_spinner=False, ttl=60)
    def build_excel_bytes(df_json: str) -> bytes:
        from openpyxl.utils import get_column_letter

        local_df = ensure_core_columns(pd.DataFrame(json.loads(df_json)))
        tables = build_analysis_tables(df_json)

        output = io.BytesIO()

        # 改用 openpyxl，避免 xlsxwriter 未安裝錯誤
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            local_df.to_excel(writer, sheet_name="推薦紀錄", index=False)
            tables["mode"].to_excel(writer, sheet_name="模式分析", index=False)
            tables["category"].to_excel(writer, sheet_name="類別分析", index=False)
            tables["grade"].to_excel(writer, sheet_name="等級分析", index=False)
            tables["trade_mode"].to_excel(writer, sheet_name="實際交易分析", index=False)

            for sheet_name, sheet_df in {
                "推薦紀錄": local_df,
                "模式分析": tables["mode"],
                "類別分析": tables["category"],
                "等級分析": tables["grade"],
                "實際交易分析": tables["trade_mode"],
            }.items():
                ws = writer.book[sheet_name]

                for col_idx, col_name in enumerate(sheet_df.columns, start=1):
                    values = [str(col_name)]
                    values.extend(sheet_df[col_name].fillna("").astype(str).head(300).tolist())
                    max_len = min(max(len(v) for v in values) + 2, 36)
                    ws.column_dimensions[get_column_letter(col_idx)].width = max(12, max_len)

        output.seek(0)
        return output.getvalue()

    excel_bytes = build_excel_bytes(df_json_key)

    st.download_button(
        "📥 下載 Excel（推薦紀錄 / 模式分析 / 類別分析 / 等級分析 / 實際交易分析）",
        data=excel_bytes,
        file_name=f"股神推薦紀錄_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

# =========================================================
# Tab 6
# =========================================================
with tabs[5]:
    safe_render_pro_section("同步檢查", "檢查 secrets / 欄位 / session_state 串接")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**必要 secrets 檢查**")
        st.dataframe(pd.DataFrame([
            {"項目": "GITHUB_TOKEN", "是否存在": bool(GITHUB_TOKEN)},
            {"項目": "GITHUB_REPO_OWNER", "是否存在": bool(GITHUB_REPO_OWNER)},
            {"項目": "GITHUB_REPO_NAME", "是否存在": bool(GITHUB_REPO_NAME)},
            {"項目": "GITHUB_REPO_BRANCH", "是否存在": bool(GITHUB_REPO_BRANCH)},
            {"項目": "GODPICK_RECORDS_GITHUB_PATH", "是否存在": bool(GODPICK_RECORDS_GITHUB_PATH)},
            {"項目": "Firestore client", "是否存在": firestore_enabled()},
            {"項目": "Firestore collection", "是否存在": bool(FIRESTORE_COLLECTION)},
        ]), use_container_width=True, hide_index=True)

    with col2:
        st.markdown("**欄位完整性檢查**")
        st.dataframe(pd.DataFrame({
            "欄位": CORE_COLUMNS,
            "是否存在": [c in df.columns for c in CORE_COLUMNS],
        }), use_container_width=True, hide_index=True)

    st.markdown("**目前 godpick 相關 session_state**")
    ss_keys = [k for k in st.session_state.keys() if "godpick" in str(k).lower()]
    st.code("\n".join(sorted(ss_keys)) if ss_keys else "(無)")

    st.markdown("**7 頁建議至少帶入欄位**")
    st.code("\n".join([
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
    ]))

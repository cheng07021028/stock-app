# pages/7_股神推薦.py
# -*- coding: utf-8 -*-

import io
import json
import math
import time
import base64
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="股神推薦", page_icon="🚀", layout="wide")

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

def _fallback_get_all_code_name_map():
    return {}

def _fallback_get_history_data(code: str, start_date=None, end_date=None):
    return pd.DataFrame()

try:
    from utils import (
        inject_pro_theme,
        render_pro_hero,
        render_pro_section,
        render_pro_kpi_row,
        format_number,
        get_all_code_name_map,
        get_history_data,
        get_normalized_watchlist,
    )
except Exception:
    inject_pro_theme = _fallback_inject_pro_theme
    render_pro_hero = _fallback_render_pro_hero
    render_pro_section = _fallback_render_pro_section
    render_pro_kpi_row = _fallback_render_pro_kpi_row
    format_number = _fallback_format_number
    get_all_code_name_map = _fallback_get_all_code_name_map
    get_history_data = _fallback_get_history_data

    def get_normalized_watchlist():
        return {}

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
    "🚀 股神推薦",
    "股神版推薦引擎｜推薦結果保留、不切頁消失、可直接寫入 8_股神推薦紀錄 / 加入自選股 / 匯出 Excel。",
    key="godpick_hero",
)

# =========================================================
# 常數 / secrets
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

WATCHLIST_GITHUB_PATH = _safe_secret_get("WATCHLIST_GITHUB_PATH", "watchlist.json")
GODPICK_RECORDS_GITHUB_PATH = _safe_secret_get("GODPICK_RECORDS_GITHUB_PATH", "godpick_records.json")
WATCHLIST_FIRESTORE_COLLECTION = _safe_secret_get("WATCHLIST_FIRESTORE_COLLECTION", "watchlist")
GODPICK_RECORDS_FIRESTORE_COLLECTION = _safe_secret_get("GODPICK_RECORDS_FIRESTORE_COLLECTION", "godpick_records")

RECOMMEND_MODES = ["飆股模式", "波段模式", "領頭羊模式", "綜合模式"]
RISK_FILTERS = ["寬鬆", "標準", "嚴格"]
SCAN_SIZES = {
    "200": 200,
    "500": 500,
    "1000": 1000,
    "1500": 1500,
    "2000": 2000,
    "全部": 999999,
}

# 7 -> 8 必要欄位對齊
RECORD_COLUMNS = [
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
    "目前狀態",
    "是否已實際買進",
    "實際買進價",
    "實際賣出價",
    "實際報酬%",
    "備註",
    "是否達停損",
    "是否達目標1",
    "是否達目標2",
]

# =========================================================
# 小工具
# =========================================================
def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def now_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def now_time() -> str:
    return datetime.now().strftime("%H:%M:%S")

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

def clamp(v, low=0.0, high=100.0):
    try:
        return max(low, min(high, float(v)))
    except Exception:
        return low

def safe_bool_str(v, default="否"):
    s = safe_str(v, default)
    if s in {"True", "true", "1", "Y", "Yes", "yes"}:
        return "是"
    if s in {"False", "false", "0", "N", "No", "no"}:
        return "否"
    if s in {"是", "否"}:
        return s
    return default

def github_enabled() -> bool:
    return all([GITHUB_TOKEN, GITHUB_REPO_OWNER, GITHUB_REPO_NAME])

def github_headers():
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }

def build_record_key_from_row(row: Dict[str, Any]) -> str:
    return (
        safe_str(row.get("股票代號"))
        + "|"
        + safe_str(row.get("推薦日期"))
        + "|"
        + safe_str(row.get("推薦時間"))
        + "|"
        + safe_str(row.get("推薦模式"))
    )

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

# =========================================================
# GitHub / Firestore 通用 JSON 讀寫
# =========================================================
def read_github_json(path: str):
    if not github_enabled():
        return None
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/contents/{path}"
        r = requests.get(url, headers=github_headers(), params={"ref": GITHUB_REPO_BRANCH}, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        obj = r.json()
        content_b64 = obj.get("content", "")
        if not content_b64:
            return None
        decoded = base64.b64decode(content_b64).decode("utf-8")
        return json.loads(decoded)
    except Exception:
        return None

def write_github_json(path: str, data: Any, commit_message: str):
    if not github_enabled():
        return {"ok": False, "msg": "GitHub 未設定完整"}

    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/contents/{path}"
        r = requests.get(url, headers=github_headers(), params={"ref": GITHUB_REPO_BRANCH}, timeout=30)

        sha = None
        if r.status_code == 200:
            sha = r.json().get("sha")
        elif r.status_code != 404:
            r.raise_for_status()

        raw = json.dumps(data, ensure_ascii=False, indent=2)
        b64 = base64.b64encode(raw.encode("utf-8")).decode("utf-8")

        payload = {
            "message": commit_message,
            "content": b64,
            "branch": GITHUB_REPO_BRANCH,
        }
        if sha:
            payload["sha"] = sha

        put_r = requests.put(url, headers=github_headers(), json=payload, timeout=30)
        if put_r.status_code not in (200, 201):
            msg = ""
            try:
                msg = put_r.json().get("message", "")
            except Exception:
                msg = put_r.text[:200]
            return {"ok": False, "msg": f"GitHub 寫入失敗：{put_r.status_code} {msg}"}
        return {"ok": True, "msg": "GitHub 寫入成功"}
    except Exception as e:
        return {"ok": False, "msg": f"GitHub 寫入失敗：{e}"}

def read_firestore_collection_as_list(collection_name: str) -> List[Dict[str, Any]]:
    db = get_firestore_client()
    if db is None:
        return []
    try:
        rows = []
        for doc in db.collection(collection_name).stream():
            item = doc.to_dict() or {}
            rows.append(item)
        return rows
    except Exception:
        return []

def overwrite_firestore_collection(collection_name: str, rows: List[Dict[str, Any]], key_field: str):
    db = get_firestore_client()
    if db is None:
        return {"ok": False, "msg": "Firestore 未設定完整"}
    try:
        batch = db.batch()
        col_ref = db.collection(collection_name)

        existing_docs = list(col_ref.stream())
        existing_ids = {d.id for d in existing_docs}
        new_ids = set()

        for row in rows:
            doc_id = safe_str(row.get(key_field))
            if not doc_id:
                continue
            new_ids.add(doc_id)
            batch.set(col_ref.document(doc_id), row)

        for doc_id in existing_ids - new_ids:
            batch.delete(col_ref.document(doc_id))

        batch.commit()
        return {"ok": True, "msg": "Firestore 寫入成功"}
    except Exception as e:
        return {"ok": False, "msg": f"Firestore 寫入失敗：{e}"}

# =========================================================
# 自選股讀寫
# =========================================================
def normalize_watchlist_obj(obj: Any) -> Dict[str, List[str]]:
    if not isinstance(obj, dict):
        return {}
    out = {}
    for k, v in obj.items():
        group = safe_str(k)
        if not group:
            continue
        if isinstance(v, list):
            codes = []
            for item in v:
                if isinstance(item, dict):
                    code = safe_str(item.get("code"))
                else:
                    code = safe_str(item)
                if code:
                    codes.append(code)
            out[group] = sorted(list(dict.fromkeys(codes)))
    return out

def load_watchlist_all() -> Dict[str, List[str]]:
    try:
        wl = get_normalized_watchlist()
        if isinstance(wl, dict) and wl:
            norm = normalize_watchlist_obj(wl)
            if norm:
                return norm
    except Exception:
        pass

    gh = read_github_json(WATCHLIST_GITHUB_PATH) if github_enabled() else None
    if gh:
        norm = normalize_watchlist_obj(gh)
        if norm:
            return norm

    fs_rows = read_firestore_collection_as_list(WATCHLIST_FIRESTORE_COLLECTION) if firestore_enabled() else []
    if fs_rows:
        obj = {}
        for row in fs_rows:
            g = safe_str(row.get("group"))
            c = safe_str(row.get("code"))
            if g and c:
                obj.setdefault(g, []).append(c)
        return normalize_watchlist_obj(obj)

    return {}

def save_watchlist_all(watchlist_obj: Dict[str, List[str]]) -> Dict[str, Any]:
    data = normalize_watchlist_obj(watchlist_obj)
    gh_result = {"ok": False, "msg": "GitHub 略過"}
    fs_result = {"ok": False, "msg": "Firestore 略過"}

    if github_enabled():
        gh_result = write_github_json(
            WATCHLIST_GITHUB_PATH,
            data,
            commit_message=f"update watchlist @ {now_ts()}",
        )

    if firestore_enabled():
        rows = []
        for group_name, codes in data.items():
            for code in codes:
                rows.append({
                    "doc_id": f"{group_name}__{code}",
                    "group": group_name,
                    "code": code,
                    "updated_at": now_ts(),
                })
        fs_result = overwrite_firestore_collection(
            WATCHLIST_FIRESTORE_COLLECTION,
            rows,
            key_field="doc_id",
        )

    return {
        "ok": gh_result.get("ok", False) or fs_result.get("ok", False),
        "github": gh_result,
        "firestore": fs_result
    }

# =========================================================
# 推薦紀錄雙寫
# =========================================================
def ensure_record_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=RECORD_COLUMNS)

    x = df.copy()
    for c in RECORD_COLUMNS:
        if c not in x.columns:
            x[c] = np.nan

    x["是否領先同類股"] = x["是否領先同類股"].apply(
        lambda v: True if str(v).strip() in {"True", "true", "1", "是"} else False
    )
    x["是否已實際買進"] = x["是否已實際買進"].apply(lambda v: safe_bool_str(v, "否"))
    x["是否達停損"] = x["是否達停損"].apply(lambda v: safe_bool_str(v, "否"))
    x["是否達目標1"] = x["是否達目標1"].apply(lambda v: safe_bool_str(v, "否"))
    x["是否達目標2"] = x["是否達目標2"].apply(lambda v: safe_bool_str(v, "否"))

    for c in [
        "推薦總分", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數",
        "同類股領先幅度", "推薦價格", "停損價", "賣出目標1", "賣出目標2",
        "實際買進價", "實際賣出價", "實際報酬%"
    ]:
        x[c] = pd.to_numeric(x[c], errors="coerce")

    x["推薦日期"] = x["推薦日期"].fillna("").astype(str)
    x["推薦時間"] = x["推薦時間"].fillna("").astype(str)
    x["目前狀態"] = x["目前狀態"].replace("", np.nan).fillna("觀察").astype(str)
    x["備註"] = x["備註"].fillna("").astype(str)

    return x[RECORD_COLUMNS].copy()

def prepare_records_from_selection(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=RECORD_COLUMNS)

    x = df.copy()

    if "推薦日期" not in x.columns:
        x["推薦日期"] = now_date()
    else:
        x["推薦日期"] = x["推薦日期"].replace("", np.nan).fillna(now_date())

    if "推薦時間" not in x.columns:
        x["推薦時間"] = now_time()
    else:
        x["推薦時間"] = x["推薦時間"].replace("", np.nan).fillna(now_time())

    if "目前狀態" not in x.columns:
        x["目前狀態"] = "觀察"
    else:
        x["目前狀態"] = x["目前狀態"].replace("", np.nan).fillna("觀察")

    if "是否已實際買進" not in x.columns:
        x["是否已實際買進"] = "否"
    else:
        x["是否已實際買進"] = x["是否已實際買進"].replace("", np.nan).fillna("否")

    for c, default_val in {
        "實際買進價": np.nan,
        "實際賣出價": np.nan,
        "實際報酬%": np.nan,
        "備註": "",
        "是否達停損": "否",
        "是否達目標1": "否",
        "是否達目標2": "否",
    }.items():
        if c not in x.columns:
            x[c] = default_val
        else:
            if pd.isna(default_val):
                x[c] = pd.to_numeric(x[c], errors="coerce")
            else:
                x[c] = x[c].replace("", np.nan).fillna(default_val)

    return ensure_record_columns(x)

def dedupe_records(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=RECORD_COLUMNS)
    x = ensure_record_columns(df.copy())
    x["_key"] = x.apply(lambda r: build_record_key_from_row(r.to_dict()), axis=1)
    x = x.drop_duplicates("_key", keep="last").drop(columns=["_key"], errors="ignore")
    return ensure_record_columns(x)

def load_godpick_records() -> pd.DataFrame:
    rows = []

    if github_enabled():
        gh = read_github_json(GODPICK_RECORDS_GITHUB_PATH)
        if isinstance(gh, list):
            rows.extend(gh)

    if firestore_enabled():
        fs_rows = read_firestore_collection_as_list(GODPICK_RECORDS_FIRESTORE_COLLECTION)
        if fs_rows:
            rows.extend(fs_rows)

    if not rows:
        return ensure_record_columns(pd.DataFrame())

    df = pd.DataFrame(rows)
    return dedupe_records(df)

def save_godpick_records(df: pd.DataFrame) -> Dict[str, Any]:
    df2 = dedupe_records(df.copy())
    rows = df2.replace({np.nan: None}).to_dict(orient="records")

    gh_result = {"ok": False, "msg": "GitHub 略過"}
    fs_result = {"ok": False, "msg": "Firestore 略過"}

    if github_enabled():
        gh_result = write_github_json(
            GODPICK_RECORDS_GITHUB_PATH,
            rows,
            commit_message=f"update godpick_records @ {now_ts()}",
        )

    if firestore_enabled():
        write_rows = []
        for row in rows:
            doc_id = (
                safe_str(row.get("股票代號"))
                + "__"
                + safe_str(row.get("推薦日期"))
                + "__"
                + safe_str(row.get("推薦時間"))
                + "__"
                + safe_str(row.get("推薦模式"))
            )
            row2 = dict(row)
            row2["doc_id"] = doc_id
            write_rows.append(row2)

        fs_result = overwrite_firestore_collection(
            GODPICK_RECORDS_FIRESTORE_COLLECTION,
            write_rows,
            key_field="doc_id",
        )

    return {
        "ok": gh_result.get("ok", False) or fs_result.get("ok", False),
        "github": gh_result,
        "firestore": fs_result
    }

# =========================================================
# 股票池 / 歷史資料
# =========================================================
@st.cache_data(show_spinner=False, ttl=21600)
def load_code_name_map_cached() -> Dict[str, str]:
    try:
        mp = get_all_code_name_map() or {}
        return {safe_str(k): safe_str(v) for k, v in mp.items() if safe_str(k)}
    except Exception:
        return {}

@st.cache_data(show_spinner=False, ttl=21600)
def build_universe_df() -> pd.DataFrame:
    code_map = load_code_name_map_cached()
    rows = []
    for code, name in code_map.items():
        market = "上市"
        if code.startswith("7") or code.startswith("8") or code.startswith("9"):
            market = "興櫃"
        elif len(code) == 4 and code[0] in {"3", "4", "5", "6"}:
            market = "上櫃"

        category = "其他"
        if any(k in name for k in ["半導體", "晶圓", "封測", "IC"]):
            category = "半導體"
        elif any(k in name for k in ["電子", "電機", "電腦", "光電", "網通"]):
            category = "電子"
        elif any(k in name for k in ["金融", "金控", "銀行", "證券", "保險"]):
            category = "金融"
        elif any(k in name for k in ["航運", "航空", "貨櫃"]):
            category = "航運"
        elif any(k in name for k in ["生技", "醫療", "藥"]):
            category = "生技"
        elif any(k in name for k in ["鋼", "塑", "化", "水泥", "紡織"]):
            category = "傳產"

        rows.append({
            "股票代號": code,
            "股票名稱": name,
            "市場別": market,
            "類別": category,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["股票代號", "股票名稱", "市場別", "類別"])
    return df

@st.cache_data(show_spinner=False, ttl=7200)
def get_history_cached(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    try:
        df = get_history_data(code, start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            return pd.DataFrame()

        x = df.copy()
        if isinstance(x.index, pd.DatetimeIndex):
            x = x.reset_index().rename(columns={x.index.name or "index": "Date"})
        if "Date" not in x.columns:
            possible = [c for c in x.columns if str(c).lower() in {"date", "日期"}]
            if possible:
                x = x.rename(columns={possible[0]: "Date"})

        col_map = {}
        for c in x.columns:
            cs = str(c).lower()
            if cs == "close" or "收盤" in str(c):
                col_map[c] = "Close"
            elif cs == "open" or "開盤" in str(c):
                col_map[c] = "Open"
            elif cs == "high" or "最高" in str(c):
                col_map[c] = "High"
            elif cs == "low" or "最低" in str(c):
                col_map[c] = "Low"
            elif cs == "volume" or "成交量" in str(c):
                col_map[c] = "Volume"
        x = x.rename(columns=col_map)

        keep = [c for c in ["Date", "Open", "High", "Low", "Close", "Volume"] if c in x.columns]
        x = x[keep].copy()
        if "Date" in x.columns:
            x["Date"] = pd.to_datetime(x["Date"], errors="coerce")
            x = x.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)

        for c in ["Open", "High", "Low", "Close", "Volume"]:
            if c in x.columns:
                x[c] = pd.to_numeric(x[c], errors="coerce")

        return x.dropna(subset=["Close"]).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()

# =========================================================
# 技術計算
# =========================================================
def calc_rsi(series: pd.Series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calc_macd(close: pd.Series):
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    hist = macd - signal
    return macd, signal, hist

def calc_kd(df: pd.DataFrame, n=9):
    low_n = df["Low"].rolling(n).min()
    high_n = df["High"].rolling(n).max()
    rsv = (df["Close"] - low_n) / (high_n - low_n).replace(0, np.nan) * 100
    k = rsv.ewm(com=2).mean()
    d = k.ewm(com=2).mean()
    return k, d

def calc_support_resistance(close: pd.Series):
    recent = close.dropna().tail(60)
    if recent.empty:
        return np.nan, np.nan
    support = recent.quantile(0.2)
    resistance = recent.quantile(0.8)
    return support, resistance

def compute_single_stock_features(code: str, name: str, market: str, category: str, start_date: str, end_date: str) -> Dict[str, Any]:
    hist = get_history_cached(code, start_date, end_date)
    if hist is None or hist.empty or len(hist) < 60:
        return {}

    if not all(c in hist.columns for c in ["Close", "Volume"]):
        return {}

    df = hist.copy()
    close = df["Close"]
    vol = df["Volume"] if "Volume" in df.columns else pd.Series(dtype=float)

    df["MA5"] = close.rolling(5).mean()
    df["MA10"] = close.rolling(10).mean()
    df["MA20"] = close.rolling(20).mean()
    df["MA60"] = close.rolling(60).mean()
    df["VOL20"] = vol.rolling(20).mean() if not vol.empty else np.nan
    df["RSI14"] = calc_rsi(close, 14)
    macd, macd_sig, macd_hist = calc_macd(close)
    df["MACD"] = macd
    df["MACD_SIG"] = macd_sig
    df["MACD_HIST"] = macd_hist

    if all(c in df.columns for c in ["Low", "High"]):
        k, d = calc_kd(df)
        df["K"] = k
        df["D"] = d
    else:
        df["K"] = np.nan
        df["D"] = np.nan

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last

    px = to_float(last.get("Close"))
    px20 = to_float(df["Close"].iloc[-20]) if len(df) >= 20 else np.nan
    px60 = to_float(df["Close"].iloc[-60]) if len(df) >= 60 else np.nan

    ret_5 = (px / to_float(df["Close"].iloc[-6]) - 1) * 100 if len(df) >= 6 and pd.notna(px) else np.nan
    ret_20 = (px / px20 - 1) * 100 if pd.notna(px20) and px20 > 0 else np.nan
    ret_60 = (px / px60 - 1) * 100 if pd.notna(px60) and px60 > 0 else np.nan

    ma5 = to_float(last.get("MA5"))
    ma10 = to_float(last.get("MA10"))
    ma20 = to_float(last.get("MA20"))
    ma60 = to_float(last.get("MA60"))
    rsi14 = to_float(last.get("RSI14"))
    vol_now = to_float(last.get("Volume"))
    vol20 = to_float(last.get("VOL20"))
    macd_hist_now = to_float(last.get("MACD_HIST"))
    macd_hist_prev = to_float(prev.get("MACD_HIST"))
    k_now = to_float(last.get("K"))
    d_now = to_float(last.get("D"))

    support, resistance = calc_support_resistance(close)

    near_breakout = 0
    if pd.notna(resistance) and pd.notna(px) and resistance > 0:
        dist = (resistance - px) / resistance * 100
        if -1.5 <= dist <= 3.0:
            near_breakout = clamp(100 - abs(dist) * 20, 0, 100)

    ma_stack_score = 0
    if pd.notna(px) and pd.notna(ma5) and pd.notna(ma10) and pd.notna(ma20):
        if px > ma5 > ma10 > ma20:
            ma_stack_score = 100
        elif px > ma10 and ma10 > ma20:
            ma_stack_score = 80
        elif px > ma20:
            ma_stack_score = 60
        else:
            ma_stack_score = 25

    volume_boost_score = 0
    if pd.notna(vol_now) and pd.notna(vol20) and vol20 > 0:
        ratio = vol_now / vol20
        volume_boost_score = clamp((ratio - 0.8) * 80, 0, 100)

    momentum_turn_score = 0
    if pd.notna(macd_hist_now):
        if pd.notna(macd_hist_prev) and macd_hist_now > macd_hist_prev and macd_hist_now > 0:
            momentum_turn_score = 100
        elif pd.notna(macd_hist_prev) and macd_hist_now > macd_hist_prev:
            momentum_turn_score = 75
        else:
            momentum_turn_score = 35

    support_defense_score = 0
    if pd.notna(px) and pd.notna(support) and support > 0:
        gap = (px - support) / support * 100
        if 0 <= gap <= 8:
            support_defense_score = clamp(100 - gap * 8, 0, 100)
        elif gap < 0:
            support_defense_score = 10
        else:
            support_defense_score = 45

    breakout_buy_score = near_breakout
    pullback_buy_score = support_defense_score
    chase_risk = 0
    if pd.notna(ret_20):
        chase_risk = clamp((ret_20 - 8) * 8, 0, 100)

    rr_score = 0
    if pd.notna(px) and pd.notna(support) and pd.notna(resistance) and px > support:
        reward = max(resistance - px, 0)
        risk = max(px - support, 0.01)
        rr = reward / risk
        rr_score = clamp(rr * 35, 0, 100)

    technical_structure = round(np.nanmean([
        ma_stack_score,
        60 if (pd.notna(px) and pd.notna(ma60) and px > ma60) else 25,
        70 if (pd.notna(rsi14) and 45 <= rsi14 <= 75) else 35,
    ]), 2)

    pre_breakout = round(np.nanmean([
        ma_stack_score,
        volume_boost_score,
        near_breakout,
        momentum_turn_score,
        support_defense_score,
    ]), 2)

    tradability = round(np.nanmean([
        pullback_buy_score,
        breakout_buy_score,
        rr_score,
        100 - chase_risk,
    ]), 2)

    latest = {
        "股票代號": code,
        "股票名稱": name,
        "市場別": market,
        "類別": category,
        "收盤價": px,
        "5日漲幅%": round(ret_5, 2) if pd.notna(ret_5) else np.nan,
        "20日漲幅%": round(ret_20, 2) if pd.notna(ret_20) else np.nan,
        "60日漲幅%": round(ret_60, 2) if pd.notna(ret_60) else np.nan,
        "量比估計": round(vol_now / vol20, 2) if pd.notna(vol_now) and pd.notna(vol20) and vol20 > 0 else np.nan,
        "技術結構分數": technical_structure,
        "起漲前兆分數": pre_breakout,
        "交易可行分數": tradability,
        "_ma_stack": ma_stack_score,
        "_volume_boost": volume_boost_score,
        "_near_breakout": near_breakout,
        "_momentum_turn": momentum_turn_score,
        "_support_defense": support_defense_score,
        "_pullback_buy": pullback_buy_score,
        "_breakout_buy": breakout_buy_score,
        "_chase_risk": chase_risk,
        "_rr_score": rr_score,
        "_support": round(support, 2) if pd.notna(support) else np.nan,
        "_resistance": round(resistance, 2) if pd.notna(resistance) else np.nan,
    }
    return latest

# =========================================================
# 模式 / 篩選
# =========================================================
def apply_risk_filter(row: Dict[str, Any], risk_filter: str) -> Tuple[bool, str]:
    ret20 = to_float(row.get("20日漲幅%"))
    vol_ratio = to_float(row.get("量比估計"))
    pre = to_float(row.get("起漲前兆分數"))
    tech = to_float(row.get("技術結構分數"))
    px = to_float(row.get("收盤價"))

    if pd.isna(px) or px <= 0:
        return False, "價格異常"

    if risk_filter == "寬鬆":
        if pd.notna(ret20) and ret20 > 25:
            return False, "近20日漲幅過大"
        if pd.notna(vol_ratio) and vol_ratio < 0.5:
            return False, "量能不足"
        return True, ""

    if risk_filter == "標準":
        if pd.notna(ret20) and ret20 > 20:
            return False, "近20日漲幅過大"
        if pd.notna(vol_ratio) and vol_ratio < 0.7:
            return False, "量能不足"
        if pd.notna(tech) and tech < 45:
            return False, "中期結構偏弱"
        return True, ""

    if risk_filter == "嚴格":
        if pd.notna(ret20) and ret20 > 15:
            return False, "近20日漲幅過大"
        if pd.notna(vol_ratio) and vol_ratio < 0.9:
            return False, "量能不足"
        if pd.notna(tech) and tech < 55:
            return False, "中期結構偏弱"
        if pd.notna(pre) and pre < 55:
            return False, "起漲前兆不足"
        return True, ""

    return True, ""

def score_by_mode(row: Dict[str, Any], mode: str) -> float:
    tech = to_float(row.get("技術結構分數"))
    pre = to_float(row.get("起漲前兆分數"))
    trade = to_float(row.get("交易可行分數"))
    heat = to_float(row.get("類股熱度分數"))
    leader = to_float(row.get("同類股領先幅度"))

    if mode == "飆股模式":
        return round((pre * 0.45) + (tech * 0.20) + (trade * 0.15) + (heat * 0.10) + (leader * 0.10), 2)
    if mode == "波段模式":
        return round((tech * 0.35) + (trade * 0.30) + (pre * 0.20) + (heat * 0.10) + (leader * 0.05), 2)
    if mode == "領頭羊模式":
        return round((heat * 0.30) + (leader * 0.25) + (pre * 0.20) + (tech * 0.15) + (trade * 0.10), 2)
    return round(np.nanmean([tech, pre, trade, heat, leader]), 2)

def build_recommendation_fields(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    x = df.copy()

    cat_grp = (
        x.groupby("類別", dropna=False)["20日漲幅%"]
        .mean()
        .reset_index()
        .rename(columns={"20日漲幅%": "_cat_avg_ret20"})
    )
    x = x.merge(cat_grp, on="類別", how="left")
    x["類股熱度分數"] = x["_cat_avg_ret20"].apply(lambda v: clamp((to_float(v) + 5) * 6, 0, 100))

    x["同類股領先幅度"] = (
        pd.to_numeric(x["20日漲幅%"], errors="coerce")
        - pd.to_numeric(x["_cat_avg_ret20"], errors="coerce")
    ).round(2)
    x["是否領先同類股"] = x["同類股領先幅度"] > 0
    x["_leader_score"] = x["同類股領先幅度"].apply(lambda v: clamp(to_float(v) * 8 + 50, 0, 100))

    x["推薦總分"] = x.apply(
        lambda r: score_by_mode({
            "技術結構分數": r["技術結構分數"],
            "起漲前兆分數": r["起漲前兆分數"],
            "交易可行分數": r["交易可行分數"],
            "類股熱度分數": r["類股熱度分數"],
            "同類股領先幅度": r["_leader_score"],
        }, mode),
        axis=1
    )

    def grade_fn(v):
        v = to_float(v)
        if v >= 85:
            return "S"
        if v >= 75:
            return "A"
        if v >= 65:
            return "B"
        return "C"

    x["推薦等級"] = x["推薦總分"].apply(grade_fn)
    x["推薦模式"] = mode

    x["推薦價格"] = pd.to_numeric(x["收盤價"], errors="coerce").round(2)
    x["停損價"] = (pd.to_numeric(x["收盤價"], errors="coerce") * 0.94).round(2)
    x["賣出目標1"] = (pd.to_numeric(x["收盤價"], errors="coerce") * 1.08).round(2)
    x["賣出目標2"] = (pd.to_numeric(x["收盤價"], errors="coerce") * 1.15).round(2)

    def tags_fn(r):
        tags = []
        if to_float(r["起漲前兆分數"]) >= 75:
            tags.append("起漲前兆強")
        if to_float(r["交易可行分數"]) >= 75:
            tags.append("交易可行")
        if to_float(r["類股熱度分數"]) >= 75:
            tags.append("類股熱")
        if bool(r["是否領先同類股"]):
            tags.append("同類股領先")
        ret20 = to_float(r["20日漲幅%"])
        if pd.notna(ret20) and 0 <= ret20 <= 12:
            tags.append("未過熱")
        return " / ".join(tags[:4])

    def reason_fn(r):
        reasons = []
        if to_float(r["_ma_stack"]) >= 80:
            reasons.append("均線結構轉強")
        if to_float(r["_volume_boost"]) >= 60:
            reasons.append("量能啟動")
        if to_float(r["_near_breakout"]) >= 60:
            reasons.append("接近突破位")
        if to_float(r["_momentum_turn"]) >= 70:
            reasons.append("動能翻多")
        if to_float(r["_support_defense"]) >= 60:
            reasons.append("支撐防守佳")
        if bool(r["是否領先同類股"]):
            reasons.append("同類股相對領先")
        return "、".join(reasons[:6])

    x["推薦標籤"] = x.apply(tags_fn, axis=1)
    x["推薦理由摘要"] = x.apply(reason_fn, axis=1)

    return x

# =========================================================
# 掃描主程式
# =========================================================
@st.cache_data(show_spinner=False, ttl=1800)
def scan_recommendations_cached(
    mode: str,
    risk_filter: str,
    market_filter: str,
    scan_limit: int,
    lookback_days: int,
    min_price: float,
    max_price: float,
):
    universe = build_universe_df()
    if universe.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if market_filter != "全部":
        universe = universe[universe["市場別"] == market_filter].copy()

    if universe.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if scan_limit < len(universe):
        universe = universe.head(scan_limit).copy()

    end_date = now_date()
    start_date = (datetime.now() - timedelta(days=int(lookback_days))).strftime("%Y-%m-%d")

    rows = []
    for row in universe.itertuples(index=False):
        rec = compute_single_stock_features(
            code=row.股票代號,
            name=row.股票名稱,
            market=row.市場別,
            category=row.類別,
            start_date=start_date,
            end_date=end_date,
        )
        if not rec:
            continue

        px = to_float(rec.get("收盤價"))
        if pd.notna(px):
            if px < min_price or px > max_price:
                continue

        ok, _reason = apply_risk_filter(rec, risk_filter)
        if not ok:
            continue

        rows.append(rec)

    base_df = pd.DataFrame(rows)
    if base_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    rec_df = build_recommendation_fields(base_df, mode)

    rec_df = rec_df.sort_values(
        ["推薦總分", "起漲前兆分數", "交易可行分數"],
        ascending=[False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    category_strength_df = rec_df.groupby("類別", dropna=False).agg(
        股票數=("股票代號", "count"),
        類股平均總分=("推薦總分", "mean"),
        類股平均起漲前兆=("起漲前兆分數", "mean"),
        類股平均交易可行=("交易可行分數", "mean"),
        類股平均20日漲幅=("20日漲幅%", "mean"),
    ).reset_index().sort_values(
        ["類股平均總分", "類股平均20日漲幅"],
        ascending=[False, False],
        na_position="last"
    )

    factor_rank_df = rec_df[[
        "股票代號", "股票名稱", "類別", "技術結構分數", "起漲前兆分數",
        "交易可行分數", "類股熱度分數", "推薦總分"
    ]].copy().sort_values("推薦總分", ascending=False, na_position="last")

    return rec_df, category_strength_df, factor_rank_df

# =========================================================
# Session State
# =========================================================
if "godpick_rec_df" not in st.session_state:
    st.session_state["godpick_rec_df"] = pd.DataFrame()

if "godpick_category_strength_df" not in st.session_state:
    st.session_state["godpick_category_strength_df"] = pd.DataFrame()

if "godpick_factor_rank_df" not in st.session_state:
    st.session_state["godpick_factor_rank_df"] = pd.DataFrame()

if "godpick_scan_meta" not in st.session_state:
    st.session_state["godpick_scan_meta"] = {}

if "godpick_selected_to_records" not in st.session_state:
    st.session_state["godpick_selected_to_records"] = pd.DataFrame(columns=RECORD_COLUMNS)

if "godpick_records_pending_import" not in st.session_state:
    st.session_state["godpick_records_pending_import"] = pd.DataFrame(columns=RECORD_COLUMNS)

# =========================================================
# 控制區
# =========================================================
safe_render_pro_section("掃描設定", "先修錯，再加速；掃描結果會保留在 session_state，不切頁消失。")

cfg_cols = st.columns([1.2, 1.1, 1.0, 1.0, 1.0, 1.0, 1.2])

with cfg_cols[0]:
    mode = st.selectbox("推薦模式", RECOMMEND_MODES, index=3)
with cfg_cols[1]:
    risk_filter = st.selectbox("風險過濾", RISK_FILTERS, index=1)
with cfg_cols[2]:
    market_filter = st.selectbox("市場別", ["全部", "上市", "上櫃", "興櫃"], index=0)
with cfg_cols[3]:
    scan_size_label = st.selectbox("掃描筆數", list(SCAN_SIZES.keys()), index=2)
with cfg_cols[4]:
    lookback_days = st.number_input("回看天數", min_value=90, max_value=500, value=180, step=10)
with cfg_cols[5]:
    min_price = st.number_input("最低價", min_value=0.0, value=5.0, step=1.0)
with cfg_cols[6]:
    max_price = st.number_input("最高價", min_value=0.0, value=300.0, step=5.0)

btn_cols = st.columns([1.2, 1.2, 1.2, 1.2, 2.4])

with btn_cols[0]:
    run_scan = st.button("🚀 開始推薦", use_container_width=True)

with btn_cols[1]:
    if st.button("♻️ 保留結果重整頁面", use_container_width=True):
        st.rerun()

with btn_cols[2]:
    clear_cache_btn = st.button("🧹 清除掃描快取", use_container_width=True)
    if clear_cache_btn:
        scan_recommendations_cached.clear()
        get_history_cached.clear()
        st.success("已清除掃描快取")

with btn_cols[3]:
    clear_result_btn = st.button("🗑️ 清空目前結果", use_container_width=True)
    if clear_result_btn:
        st.session_state["godpick_rec_df"] = pd.DataFrame()
        st.session_state["godpick_category_strength_df"] = pd.DataFrame()
        st.session_state["godpick_factor_rank_df"] = pd.DataFrame()
        st.session_state["godpick_selected_to_records"] = pd.DataFrame(columns=RECORD_COLUMNS)
        st.session_state["godpick_records_pending_import"] = pd.DataFrame(columns=RECORD_COLUMNS)
        st.success("已清空結果")
        st.rerun()

with btn_cols[4]:
    st.caption(
        f"GitHub：{'✅' if github_enabled() else '❌'} ｜ "
        f"Firestore：{'✅' if firestore_enabled() else '❌'}"
    )

if run_scan:
    with st.spinner("掃描中，請稍候..."):
        rec_df, category_strength_df, factor_rank_df = scan_recommendations_cached(
            mode=mode,
            risk_filter=risk_filter,
            market_filter=market_filter,
            scan_limit=SCAN_SIZES[scan_size_label],
            lookback_days=int(lookback_days),
            min_price=float(min_price),
            max_price=float(max_price),
        )
        st.session_state["godpick_rec_df"] = rec_df
        st.session_state["godpick_category_strength_df"] = category_strength_df
        st.session_state["godpick_factor_rank_df"] = factor_rank_df
        st.session_state["godpick_scan_meta"] = {
            "推薦模式": mode,
            "風險過濾": risk_filter,
            "市場別": market_filter,
            "掃描筆數": scan_size_label,
            "回看天數": int(lookback_days),
            "更新時間": now_ts(),
        }
        st.success(f"掃描完成：{len(rec_df)} 筆推薦結果")

# =========================================================
# 主結果
# =========================================================
rec_df = st.session_state["godpick_rec_df"].copy()
category_strength_df = st.session_state["godpick_category_strength_df"].copy()
factor_rank_df = st.session_state["godpick_factor_rank_df"].copy()
scan_meta = st.session_state["godpick_scan_meta"]

if rec_df.empty:
    st.info("目前尚無推薦結果，請先按「開始推薦」。")
    st.stop()

top_n = min(10, len(rec_df))
avg_score = rec_df["推薦總分"].dropna().mean()
avg_pre = rec_df["起漲前兆分數"].dropna().mean()
avg_trade = rec_df["交易可行分數"].dropna().mean()
avg_heat = rec_df["類股熱度分數"].dropna().mean()

safe_render_pro_kpi_row([
    {"label": "推薦筆數", "value": format_number(len(rec_df))},
    {"label": "平均推薦總分", "value": "-" if pd.isna(avg_score) else f"{avg_score:.2f}"},
    {"label": "平均起漲前兆", "value": "-" if pd.isna(avg_pre) else f"{avg_pre:.2f}"},
    {"label": "平均交易可行", "value": "-" if pd.isna(avg_trade) else f"{avg_trade:.2f}"},
    {"label": "平均類股熱度", "value": "-" if pd.isna(avg_heat) else f"{avg_heat:.2f}"},
    {"label": "更新時間", "value": scan_meta.get("更新時間", "-")},
], key="godpick_kpi")

# =========================================================
# Tabs
# =========================================================
tabs = st.tabs([
    "🏆 推薦總表",
    "🔥 類股強度榜",
    "🧠 自動因子榜",
    "✅ 勾選動作",
    "📤 Excel 匯出",
])

# =========================================================
# Tab 1 推薦總表
# =========================================================
with tabs[0]:
    safe_render_pro_section("推薦總表", "勾選後可直接寫入 8 頁或加入自選股。")

    view_df = rec_df.copy()
    if "勾選" not in view_df.columns:
        view_df.insert(0, "勾選", False)

    show_cols = [
        "勾選",
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
        "推薦價格",
        "停損價",
        "賣出目標1",
        "賣出目標2",
        "推薦標籤",
        "推薦理由摘要",
        "5日漲幅%",
        "20日漲幅%",
        "60日漲幅%",
        "量比估計",
    ]
    view_df = view_df[[c for c in show_cols if c in view_df.columns]].copy()

    edited = st.data_editor(
        view_df,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        key="godpick_result_editor",
        column_config={
            "勾選": st.column_config.CheckboxColumn("勾選"),
            "推薦總分": st.column_config.NumberColumn("推薦總分", format="%.2f"),
            "技術結構分數": st.column_config.NumberColumn("技術結構分數", format="%.2f"),
            "起漲前兆分數": st.column_config.NumberColumn("起漲前兆分數", format="%.2f"),
            "交易可行分數": st.column_config.NumberColumn("交易可行分數", format="%.2f"),
            "類股熱度分數": st.column_config.NumberColumn("類股熱度分數", format="%.2f"),
            "同類股領先幅度": st.column_config.NumberColumn("同類股領先幅度", format="%.2f"),
            "推薦價格": st.column_config.NumberColumn("推薦價格", format="%.2f"),
            "停損價": st.column_config.NumberColumn("停損價", format="%.2f"),
            "賣出目標1": st.column_config.NumberColumn("賣出目標1", format="%.2f"),
            "賣出目標2": st.column_config.NumberColumn("賣出目標2", format="%.2f"),
            "推薦理由摘要": st.column_config.TextColumn("推薦理由摘要", width="large"),
        }
    )

    selected_df = edited[edited["勾選"] == True].copy()
    if "勾選" in selected_df.columns:
        selected_df = selected_df.drop(columns=["勾選"], errors="ignore")

    st.session_state["godpick_selected_to_records"] = prepare_records_from_selection(selected_df.copy())
    st.caption(f"已勾選 {len(selected_df)} 筆")

# =========================================================
# Tab 2 類股強度榜
# =========================================================
with tabs[1]:
    safe_render_pro_section("類股強度榜", "直接用目前掃描結果，不重算。")
    if category_strength_df.empty:
        st.info("目前沒有類股強度資料")
    else:
        st.dataframe(
            category_strength_df.sort_values(
                ["類股平均總分", "類股平均20日漲幅"],
                ascending=[False, False],
                na_position="last"
            ),
            use_container_width=True,
            hide_index=True,
        )

# =========================================================
# Tab 3 自動因子榜
# =========================================================
with tabs[2]:
    safe_render_pro_section("自動因子榜", "推薦結果衍生的因子排序。")
    if factor_rank_df.empty:
        st.info("目前沒有因子榜資料")
    else:
        st.dataframe(
            factor_rank_df.sort_values("推薦總分", ascending=False, na_position="last"),
            use_container_width=True,
            hide_index=True,
        )

# =========================================================
# Tab 4 勾選動作
# =========================================================
with tabs[3]:
    safe_render_pro_section("勾選動作", "可直接寫入 8 頁、雲端同步、加入自選股。")

    action_cols = st.columns([1.2, 1.2, 1.4, 2.2])

    with action_cols[0]:
        target_group = st.text_input("加入自選股群組", value="股神推薦")

    with action_cols[1]:
        st.metric("已勾選筆數", len(st.session_state["godpick_selected_to_records"]))

    with action_cols[2]:
        direct_sync_records = st.toggle("寫入 8 頁時同步雲端", value=True)

    with action_cols[3]:
        st.caption("建議流程：勾選 → 寫入 8 頁 / 加入自選股 → 到 8 頁追蹤績效")

    btn_cols2 = st.columns([1.3, 1.3, 1.3, 2.1])

    with btn_cols2[0]:
        if st.button("📥 寫入 8 頁", use_container_width=True):
            selected = prepare_records_from_selection(st.session_state["godpick_selected_to_records"].copy())
            if selected.empty:
                st.warning("請先勾選推薦股票")
            else:
                st.session_state["godpick_records_pending_import"] = selected.copy()

                if direct_sync_records:
                    old_records = load_godpick_records()
                    merged = pd.concat([old_records, selected], ignore_index=True) if not old_records.empty else selected.copy()
                    merged = dedupe_records(merged)
                    result = save_godpick_records(merged)
                    if result["ok"]:
                        st.success("已寫入 8 頁待匯入資料，且已同步 GitHub / Firestore")
                    else:
                        st.warning("已寫入 8 頁待匯入資料，但雲端同步失敗")
                else:
                    st.success("已寫入 8 頁待匯入資料，切到 8 頁按『匯入 7 頁推薦結果』即可")

    with btn_cols2[1]:
        if st.button("⭐ 加入自選股", use_container_width=True):
            selected = prepare_records_from_selection(st.session_state["godpick_selected_to_records"].copy())
            if selected.empty:
                st.warning("請先勾選推薦股票")
            else:
                group_name = target_group.strip() or "股神推薦"
                watchlist = load_watchlist_all()
                watchlist.setdefault(group_name, [])
                for code in selected["股票代號"].fillna("").astype(str).tolist():
                    if code and code not in watchlist[group_name]:
                        watchlist[group_name].append(code)

                watchlist[group_name] = sorted(list(dict.fromkeys(watchlist[group_name])))
                result = save_watchlist_all(watchlist)
                if result["ok"]:
                    st.success(f"已加入自選股群組：{group_name}")
                else:
                    st.warning("已更新本地結構，但雲端同步失敗")

    with btn_cols2[2]:
        if st.button("🧾 寫入 8 頁且清空勾選", use_container_width=True):
            selected = prepare_records_from_selection(st.session_state["godpick_selected_to_records"].copy())
            if selected.empty:
                st.warning("請先勾選推薦股票")
            else:
                st.session_state["godpick_records_pending_import"] = selected.copy()
                st.session_state["godpick_selected_to_records"] = pd.DataFrame(columns=RECORD_COLUMNS)
                st.success("已寫入 8 頁待匯入資料，並清空本頁暫存勾選")

    with btn_cols2[3]:
        st.code(
            "8 頁接收 key：\n"
            "st.session_state['godpick_selected_to_records']\n"
            "st.session_state['godpick_records_pending_import']"
        )

    pending_preview = st.session_state["godpick_records_pending_import"].copy()
    if not pending_preview.empty:
        st.markdown("##### 待匯入 8 頁預覽")
        st.dataframe(
            pending_preview[
                [c for c in [
                    "股票代號", "股票名稱", "推薦模式", "推薦等級", "推薦總分",
                    "推薦價格", "停損價", "賣出目標1", "賣出目標2",
                    "推薦日期", "推薦時間", "目前狀態", "是否已實際買進"
                ] if c in pending_preview.columns]
            ],
            use_container_width=True,
            hide_index=True,
        )

# =========================================================
# Tab 5 Excel 匯出
# =========================================================
with tabs[4]:
    safe_render_pro_section("Excel 匯出", "直接匯出目前掃描結果，不重算。")

    @st.cache_data(show_spinner=False, ttl=60)
    def build_excel_bytes(rec_json: str, cat_json: str, factor_json: str) -> bytes:
        from openpyxl.utils import get_column_letter

        rec_local = pd.DataFrame(json.loads(rec_json))
        cat_local = pd.DataFrame(json.loads(cat_json))
        factor_local = pd.DataFrame(json.loads(factor_json))

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            rec_local.to_excel(writer, sheet_name="完整推薦表", index=False)
            cat_local.to_excel(writer, sheet_name="類股強度榜", index=False)
            factor_local.to_excel(writer, sheet_name="自動因子榜", index=False)

            lead_cols = [c for c in ["股票代號", "股票名稱", "類別", "同類股領先幅度", "推薦總分"] if c in rec_local.columns]
            lead_df = rec_local[lead_cols].sort_values(
                ["同類股領先幅度", "推薦總分"],
                ascending=[False, False],
                na_position="last"
            )
            lead_df.to_excel(writer, sheet_name="同類股領先榜", index=False)

            for sheet_name, sheet_df in {
                "完整推薦表": rec_local,
                "類股強度榜": cat_local,
                "自動因子榜": factor_local,
                "同類股領先榜": lead_df,
            }.items():
                ws = writer.book[sheet_name]
                for col_idx, col_name in enumerate(sheet_df.columns, start=1):
                    values = [str(col_name)]
                    values.extend(sheet_df[col_name].fillna("").astype(str).head(300).tolist())
                    max_len = min(max(len(v) for v in values) + 2, 36)
                    ws.column_dimensions[get_column_letter(col_idx)].width = max(12, max_len)

        output.seek(0)
        return output.getvalue()

    rec_export = rec_df.replace({np.nan: None}).to_json(orient="records", force_ascii=False)
    cat_export = category_strength_df.replace({np.nan: None}).to_json(orient="records", force_ascii=False)
    factor_export = factor_rank_df.replace({np.nan: None}).to_json(orient="records", force_ascii=False)
    excel_bytes = build_excel_bytes(rec_export, cat_export, factor_export)

    st.download_button(
        "📥 下載 Excel（完整推薦表 / 類股強度榜 / 同類股領先榜 / 自動因子榜）",
        data=excel_bytes,
        file_name=f"股神推薦_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

# =========================================================
# 下方摘要
# =========================================================
with st.expander("查看本次掃描摘要", expanded=False):
    st.json(scan_meta)

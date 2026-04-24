# -*- coding: utf-8 -*-
"""
stock_master_service.py｜股票主檔更新 / 快取 / 搜尋恢復版
版本：2026-04-23 串聯修正版重建
功能：
- load_stock_master()
- refresh_stock_master()
- search_stock_master()
- get_stock_master_categories()
- get_stock_master_diagnostics()
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
import json

import pandas as pd
import requests
import streamlit as st


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
CACHE_FILE = DATA_DIR / "stock_master_cache.json"
DIAG_FILE = DATA_DIR / "stock_master_diagnostics.json"


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["code", "name", "market", "industry", "category", "source", "updated_at"])
    for col in ["code", "name", "market", "industry", "category", "source", "updated_at"]:
        if col not in df.columns:
            df[col] = ""
    df["code"] = df["code"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df = df[df["code"].str.len() > 0]
    df = df.drop_duplicates("code", keep="first").sort_values("code").reset_index(drop=True)
    return df[["code", "name", "market", "industry", "category", "source", "updated_at"]]


def _fetch_twse() -> pd.DataFrame:
    urls = [
        "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
        "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
    ]
    rows = []
    for url in urls:
        try:
            r = requests.get(url, timeout=15)
            if not r.ok:
                continue
            js = r.json()
            if isinstance(js, list):
                for x in js:
                    code = str(x.get("公司代號") or x.get("Code") or x.get("證券代號") or "").strip()
                    name = str(x.get("公司簡稱") or x.get("Name") or x.get("證券名稱") or "").strip()
                    industry = str(x.get("產業別") or x.get("Industry") or "").strip()
                    if code and name:
                        rows.append({
                            "code": code,
                            "name": name,
                            "market": "上市",
                            "industry": industry,
                            "category": industry or "上市",
                            "source": "TWSE OpenAPI",
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        })
                if rows:
                    break
        except Exception:
            pass
    return pd.DataFrame(rows)


def _fetch_tpex() -> pd.DataFrame:
    rows = []
    urls = [
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_company",
        "https://www.tpex.org.tw/openapi/v1/tpex_esb_latest_statistics",
    ]
    for url in urls:
        try:
            r = requests.get(url, timeout=15)
            if not r.ok:
                continue
            js = r.json()
            if isinstance(js, list):
                for x in js:
                    code = str(x.get("SecuritiesCompanyCode") or x.get("CompanyCode") or x.get("代號") or "").strip()
                    name = str(x.get("CompanyName") or x.get("公司名稱") or x.get("Name") or "").strip()
                    industry = str(x.get("Industry") or x.get("產業別") or "").strip()
                    if code and name:
                        rows.append({
                            "code": code,
                            "name": name,
                            "market": "上櫃",
                            "industry": industry,
                            "category": industry or "上櫃",
                            "source": "TPEX OpenAPI",
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        })
        except Exception:
            pass
    return pd.DataFrame(rows)


@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def load_stock_master() -> pd.DataFrame:
    data = _load_json(CACHE_FILE, [])
    if isinstance(data, list) and data:
        return _normalize_df(pd.DataFrame(data))
    return refresh_stock_master()


def refresh_stock_master() -> pd.DataFrame:
    dfs = []
    errors = []
    try:
        dfs.append(_fetch_twse())
    except Exception as e:
        errors.append(f"TWSE：{e}")
    try:
        dfs.append(_fetch_tpex())
    except Exception as e:
        errors.append(f"TPEX：{e}")

    df = pd.concat([x for x in dfs if x is not None and not x.empty], ignore_index=True) if dfs else pd.DataFrame()
    df = _normalize_df(df)

    # fallback：避免主檔空白導致全系統壞掉
    if df.empty:
        fallback = [
            {"code": "2330", "name": "台積電", "market": "上市", "industry": "半導體", "category": "半導體", "source": "fallback", "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            {"code": "2317", "name": "鴻海", "market": "上市", "industry": "電子", "category": "電子", "source": "fallback", "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            {"code": "2454", "name": "聯發科", "market": "上市", "industry": "半導體", "category": "半導體", "source": "fallback", "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            {"code": "3548", "name": "兆利", "market": "上櫃", "industry": "電子零組件", "category": "電子零組件", "source": "fallback", "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
        ]
        df = pd.DataFrame(fallback)

    _save_json(CACHE_FILE, df.to_dict("records"))
    _save_json(DIAG_FILE, {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows": int(len(df)),
        "markets": df["market"].value_counts().to_dict() if "market" in df.columns else {},
        "errors": errors,
    })

    try:
        st.cache_data.clear()
    except Exception:
        pass

    return df


def search_stock_master(keyword: str, market_filter: str = "全部", category_filter: str = "全部", limit: int = 300) -> pd.DataFrame:
    df = load_stock_master()
    if df.empty:
        return df
    if market_filter and market_filter != "全部" and "market" in df.columns:
        df = df[df["market"].astype(str) == market_filter]
    if category_filter and category_filter != "全部" and "category" in df.columns:
        df = df[df["category"].astype(str) == category_filter]
    kw = str(keyword or "").strip()
    if kw:
        mask = pd.Series(False, index=df.index)
        for col in ["code", "name", "industry", "category", "market"]:
            if col in df.columns:
                mask = mask | df[col].astype(str).str.contains(kw, case=False, na=False)
        df = df[mask]
    return df.head(limit).reset_index(drop=True)


def get_stock_master_categories(master_df: pd.DataFrame | None = None) -> List[str]:
    df = master_df if master_df is not None else load_stock_master()
    cats = []
    for col in ["category", "industry"]:
        if col in df.columns:
            cats += [x for x in df[col].dropna().astype(str).unique().tolist() if x and x != "nan"]
    return sorted(set(cats))


def get_stock_master_diagnostics() -> Dict[str, Any]:
    return _load_json(DIAG_FILE, {
        "updated_at": "",
        "rows": 0,
        "markets": {},
        "errors": [],
    })

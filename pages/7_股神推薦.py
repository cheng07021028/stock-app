# -*- coding: utf-8 -*-
from __future__ import annotations

import requests
import pandas as pd
import streamlit as st
from datetime import datetime

st.set_page_config(page_title="主檔驗證專用版", layout="wide")

TWSE_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_O_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TPEX_R_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_R"

INDUSTRY_CODE_MAP = {
    "01": "水泥工業",
    "02": "食品工業",
    "03": "塑膠工業",
    "04": "紡織纖維",
    "05": "電機機械",
    "06": "電器電纜",
    "08": "玻璃陶瓷",
    "09": "造紙工業",
    "10": "鋼鐵工業",
    "11": "橡膠工業",
    "12": "汽車工業",
    "13": "電子工業",
    "14": "建材營造業",
    "15": "航運業",
    "16": "觀光餐旅",
    "17": "金融保險業",
    "18": "貿易百貨業",
    "19": "綜合",
    "20": "其他業",
    "21": "化學工業",
    "22": "生技醫療業",
    "23": "油電燃氣業",
    "24": "半導體業",
    "25": "電腦及週邊設備業",
    "26": "光電業",
    "27": "通信網路業",
    "28": "電子零組件業",
    "29": "電子通路業",
    "30": "資訊服務業",
    "31": "其他電子業",
    "32": "文化創意業",
    "33": "農業科技業",
    "35": "綠能環保",
    "36": "數位雲端",
    "37": "運動休閒",
    "38": "居家生活",
}


def _norm_code(v) -> str:
    s = str(v).strip() if v is not None else ""
    if not s:
        return ""
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits or s


def _fetch_json(url: str):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json(), r.status_code


@st.cache_data(ttl=300, show_spinner=False)
def fetch_twse() -> tuple[pd.DataFrame, dict]:
    raw, status = _fetch_json(TWSE_URL)
    df = pd.DataFrame(raw)
    info = {
        "source": "TWSE",
        "url": TWSE_URL,
        "status": status,
        "rows": len(df),
        "columns": list(df.columns),
    }
    if df.empty:
        return pd.DataFrame(columns=["code", "name", "market", "official_industry_raw", "official_industry", "source", "source_api"]), info

    out = pd.DataFrame()
    out["code"] = df.get("公司代號", "").map(_norm_code)
    out["name"] = df.get("公司簡稱", df.get("公司名稱", "")).astype(str)
    out["market"] = "上市"
    out["official_industry_raw"] = df.get("產業別", "").astype(str).str.strip()
    out["official_industry"] = out["official_industry_raw"].map(lambda x: INDUSTRY_CODE_MAP.get(x, x))
    out["source"] = "twse_official"
    out["source_api"] = "t187ap03_L"
    out["source_rank"] = 1
    out = out[out["code"] != ""].copy()
    info["mapped_rows"] = len(out)
    info["industry_hits"] = int(out["official_industry"].astype(str).str.strip().ne("").sum())
    return out, info


@st.cache_data(ttl=300, show_spinner=False)
def fetch_tpex_o() -> tuple[pd.DataFrame, dict]:
    raw, status = _fetch_json(TPEX_O_URL)
    df = pd.DataFrame(raw)
    info = {
        "source": "TPEX_O",
        "url": TPEX_O_URL,
        "status": status,
        "rows": len(df),
        "columns": list(df.columns),
    }
    if df.empty:
        return pd.DataFrame(columns=["code", "name", "market", "official_industry_raw", "official_industry", "source", "source_api"]), info

    out = pd.DataFrame()
    out["code"] = df.get("SecuritiesCompanyCode", "").map(_norm_code)
    out["name"] = df.get("CompanyAbbreviation", df.get("CompanyName", "")).astype(str)
    out["market"] = "上櫃"
    out["official_industry_raw"] = df.get("SecuritiesIndustryCode", "").astype(str).str.strip()
    out["official_industry"] = out["official_industry_raw"].map(lambda x: INDUSTRY_CODE_MAP.get(x, x))
    out["source"] = "tpex_official"
    out["source_api"] = "mopsfin_t187ap03_O"
    out["source_rank"] = 1
    out = out[out["code"] != ""].copy()
    info["mapped_rows"] = len(out)
    info["industry_hits"] = int(out["official_industry"].astype(str).str.strip().ne("").sum())
    return out, info


@st.cache_data(ttl=300, show_spinner=False)
def fetch_tpex_r() -> tuple[pd.DataFrame, dict]:
    raw, status = _fetch_json(TPEX_R_URL)
    df = pd.DataFrame(raw)
    info = {
        "source": "TPEX_R",
        "url": TPEX_R_URL,
        "status": status,
        "rows": len(df),
        "columns": list(df.columns),
    }
    if df.empty:
        return pd.DataFrame(columns=["code", "name", "market", "official_industry_raw", "official_industry", "source", "source_api"]), info

    out = pd.DataFrame()
    out["code"] = df.get("SecuritiesCompanyCode", "").map(_norm_code)
    out["name"] = df.get("CompanyAbbreviation", df.get("CompanyName", "")).astype(str)
    out["market"] = "興櫃"
    out["official_industry_raw"] = df.get("SecuritiesIndustryCode", "").astype(str).str.strip()
    out["official_industry"] = out["official_industry_raw"].map(lambda x: INDUSTRY_CODE_MAP.get(x, x))
    out["source"] = "tpex_official"
    out["source_api"] = "mopsfin_t187ap03_R"
    out["source_rank"] = 1
    out = out[out["code"] != ""].copy()
    info["mapped_rows"] = len(out)
    info["industry_hits"] = int(out["official_industry"].astype(str).str.strip().ne("").sum())
    return out, info


def combine_master() -> tuple[pd.DataFrame, list[dict]]:
    parts = []
    infos = []
    for fn in [fetch_twse, fetch_tpex_o, fetch_tpex_r]:
        try:
            df, info = fn()
            parts.append(df)
            infos.append(info)
        except Exception as e:
            infos.append({"source": fn.__name__, "error": str(e)})
    if not parts:
        return pd.DataFrame(), infos
    master = pd.concat(parts, ignore_index=True)
    master = master.sort_values(["code", "source_rank"]).drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return master, infos


st.title("主檔驗證專用版")
st.caption(f"驗證時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if st.button("重新抓官方主檔", type="primary"):
    fetch_twse.clear()
    fetch_tpex_o.clear()
    fetch_tpex_r.clear()
    st.rerun()

master, infos = combine_master()

k1, k2, k3 = st.columns(3)
k1.metric("主檔總筆數", len(master))
k2.metric("正式產業有值", int(master.get("official_industry", pd.Series(dtype=str)).astype(str).str.strip().ne("").sum()) if not master.empty else 0)
k3.metric("上市筆數", int((master.get("market", pd.Series(dtype=str)) == "上市").sum()) if not master.empty else 0)

with st.expander("官方抓取診斷", expanded=True):
    for info in infos:
        st.write(info)

st.subheader("合併後主檔前 50 筆")
show_cols = ["code", "name", "market", "official_industry_raw", "official_industry", "source", "source_api"]
st.dataframe(master[show_cols].head(50), use_container_width=True, hide_index=True)

st.subheader("指定股票驗證")
def lookup(code: str):
    code = _norm_code(code)
    if master.empty:
        return pd.DataFrame(columns=show_cols)
    return master[master["code"] == code][show_cols]

c1, c2, c3 = st.columns(3)
with c1:
    st.write("2201")
    st.dataframe(lookup("2201"), use_container_width=True, hide_index=True)
with c2:
    st.write("2880")
    st.dataframe(lookup("2880"), use_container_width=True, hide_index=True)
with c3:
    st.write("2890")
    st.dataframe(lookup("2890"), use_container_width=True, hide_index=True)

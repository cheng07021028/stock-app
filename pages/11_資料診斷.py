# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import json
import pandas as pd
import streamlit as st

try:
    from utils import inject_pro_theme, render_pro_hero
except Exception:
    def inject_pro_theme(): pass
    def render_pro_hero(title, subtitle=""):
        st.title(title)
        if subtitle:
            st.caption(subtitle)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

st.set_page_config(page_title="11_資料診斷", layout="wide")
inject_pro_theme()
render_pro_hero("資料診斷", "檢查快取、JSON、頁面模組與常見串聯問題。")

paths = [
    ("專案根目錄", BASE_DIR),
    ("pages 目錄", BASE_DIR / "pages"),
    ("data 目錄", DATA_DIR),
    ("watchlist.json", BASE_DIR / "watchlist.json"),
    ("godpick_records.json", BASE_DIR / "godpick_records.json"),
    ("godpick_record_ui_config.json", BASE_DIR / "godpick_record_ui_config.json"),
    ("macro_trend_records.json", BASE_DIR / "macro_trend_records.json"),
    ("stock_master_cache.json", BASE_DIR / "stock_master_cache.json"),
    ("data/stock_master_cache.json", DATA_DIR / "stock_master_cache.json"),
    ("last_query_state.json", BASE_DIR / "last_query_state.json"),
]

rows = []
for name, p in paths:
    p = Path(p)
    rows.append({
        "項目": name,
        "路徑": str(p),
        "是否存在": p.exists(),
        "大小KB": round(p.stat().st_size / 1024, 2) if p.exists() and p.is_file() else "",
    })

st.subheader("檔案存在狀態")
st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

st.subheader("JSON 讀取檢查")
json_rows = []
for name, p in paths:
    p = Path(p)
    if p.exists() and p.is_file() and p.suffix.lower() == ".json":
        status = "OK"
        count = ""
        err = ""
        try:
            data = json.loads(p.read_text(encoding="utf-8-sig"))
            if isinstance(data, list):
                count = len(data)
            elif isinstance(data, dict):
                count = len(data.keys())
        except Exception as e:
            status = "錯誤"
            err = str(e)
        json_rows.append({"檔案": name, "狀態": status, "筆數/鍵數": count, "錯誤": err})
if json_rows:
    st.dataframe(pd.DataFrame(json_rows), use_container_width=True, hide_index=True)
else:
    st.info("沒有 JSON 檔可檢查。")

with st.expander("查看 JSON 內容", expanded=False):
    for name, p in paths:
        p = Path(p)
        if p.exists() and p.is_file() and p.suffix.lower() == ".json":
            st.markdown(f"### {name}")
            try:
                st.json(json.loads(p.read_text(encoding="utf-8-sig")))
            except Exception as e:
                st.error(f"JSON 讀取失敗：{e}")
                try:
                    st.code(p.read_text(encoding="utf-8-sig")[:3000])
                except Exception:
                    pass

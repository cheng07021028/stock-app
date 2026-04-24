# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import json
import streamlit as st

from utils import inject_pro_theme, render_pro_hero, ROOT_DIR, DATA_DIR, WATCHLIST_FILE, RECOMMEND_RECORD_FILE, STOCK_MASTER_CACHE

st.set_page_config(page_title="11_資料診斷", layout="wide")
inject_pro_theme()
render_pro_hero("資料診斷", "檢查常見快取與資料檔是否存在，協助判斷模組串聯壞在哪裡。")

paths = [
    ("專案根目錄", ROOT_DIR),
    ("data 目錄", DATA_DIR),
    ("watchlist.json", WATCHLIST_FILE),
    ("推薦紀錄", RECOMMEND_RECORD_FILE),
    ("股票主檔快取", STOCK_MASTER_CACHE),
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

st.dataframe(rows, use_container_width=True, hide_index=True)

for name, p in paths:
    p = Path(p)
    if p.exists() and p.is_file() and p.suffix.lower() == ".json":
        with st.expander(f"查看 {name}"):
            try:
                st.json(json.loads(p.read_text(encoding="utf-8-sig")))
            except Exception as e:
                st.error(f"JSON 讀取失敗：{e}")
                st.code(p.read_text(encoding="utf-8-sig")[:3000])

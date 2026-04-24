# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from utils import (
    inject_pro_theme,
    render_pro_hero,
    render_pro_kpi_row,
    load_stock_master_safe,
    get_history_data,
    score_stock_god_mode,
    append_recommend_records,
)

st.set_page_config(page_title="7_股神推薦", layout="wide")
inject_pro_theme()
render_pro_hero("股神推薦｜起漲初篩恢復版", "恢復昨日權重模式、掃描上限、推薦除錯摘要、匯入推薦紀錄。")

DEFAULT_WEIGHTS = {
    "trend": 25,
    "momentum": 25,
    "volume": 20,
    "kd_macd": 20,
    "risk": 10,
}

if "god_weights" not in st.session_state:
    st.session_state["god_weights"] = DEFAULT_WEIGHTS.copy()
if "god_result_df" not in st.session_state:
    st.session_state["god_result_df"] = pd.DataFrame()
if "god_debug_rows" not in st.session_state:
    st.session_state["god_debug_rows"] = []

st.subheader("一、股神權重設定")
c_reset, c_info = st.columns([1, 5])
with c_reset:
    if st.button("恢復原始設定", use_container_width=True):
        st.session_state["god_weights"] = DEFAULT_WEIGHTS.copy()
        st.rerun()
with c_info:
    st.caption("調整後請按「套用權重」。合計必須等於 100%，否則不會套用。")

draft = {}
cols = st.columns(5)
labels = {
    "trend": "趨勢%",
    "momentum": "動能%",
    "volume": "量能%",
    "kd_macd": "KD/MACD%",
    "risk": "風險%",
}
for i, key in enumerate(DEFAULT_WEIGHTS):
    with cols[i]:
        draft[key] = st.number_input(labels[key], min_value=0, max_value=100, value=int(st.session_state["god_weights"].get(key, 0)), step=1, key=f"draft_{key}")

total = sum(draft.values())
remaining = 100 - total
render_pro_kpi_row([
    ("目前合計", f"{total}%", "必須等於 100%"),
    ("剩餘可分配", f"{remaining}%", "正數代表尚未分完，負數代表超過"),
    ("目前狀態", "可套用" if total == 100 else "不可套用", "按下套用後才生效"),
])

if st.button("套用權重", type="primary", use_container_width=True):
    if total != 100:
        st.error(f"權重合計目前為 {total}%，必須剛好 100% 才能套用。")
    else:
        st.session_state["god_weights"] = draft.copy()
        st.success("權重已套用。")

st.divider()
st.subheader("二、掃描條件")

master = load_stock_master_safe()
if master.empty:
    st.error("股票主檔為空，請先到 9_股票主檔更新.py 更新股票主檔。")
    st.stop()

c1, c2, c3, c4 = st.columns([2, 2, 2, 2])
with c1:
    market = st.selectbox("市場別", ["全部"] + sorted(master["market"].dropna().astype(str).unique().tolist()) if "market" in master.columns else ["全部"])
with c2:
    min_score = st.slider("最低推薦分數", 0, 100, 65)
with c3:
    scan_mode = st.selectbox("掃描筆數", ["100", "300", "500", "1000", "1500", "2000", "全部"], index=2)
with c4:
    sort_mode = st.selectbox("排序", ["分數高到低", "代號小到大"])

kw = st.text_input("搜尋股票代號 / 名稱 / 類別（空白代表依掃描筆數掃描）")

scan_df = master.copy()
if market != "全部" and "market" in scan_df.columns:
    scan_df = scan_df[scan_df["market"].astype(str) == market]
if kw:
    mask = pd.Series(False, index=scan_df.index)
    for col in ["code", "name", "category", "industry"]:
        if col in scan_df.columns:
            mask = mask | scan_df[col].astype(str).str.contains(kw, case=False, na=False)
    scan_df = scan_df[mask]

if scan_mode != "全部":
    scan_df = scan_df.head(int(scan_mode))

run_btn = st.button("開始股神推薦掃描", type="primary", use_container_width=True)

if run_btn:
    results: List[Dict[str, Any]] = []
    debug: List[Dict[str, Any]] = []
    progress = st.progress(0)
    status = st.empty()

    total_rows = len(scan_df)
    for idx, row in enumerate(scan_df.itertuples(index=False), start=1):
        code = str(getattr(row, "code", "")).strip()
        name = str(getattr(row, "name", "")).strip()
        category = str(getattr(row, "category", getattr(row, "industry", ""))).strip()
        status.write(f"掃描中：{idx}/{total_rows}｜{code} {name}")

        try:
            hist = get_history_data(code, days=260)
            if hist.empty or len(hist) < 30:
                debug.append({"code": code, "name": name, "status": "歷史資料不足", "rows": len(hist)})
                continue
            s = score_stock_god_mode(hist, st.session_state["god_weights"])
            latest = hist.iloc[-1]
            debug.append({"code": code, "name": name, "status": "成功", "rows": len(hist), "score": s["score"]})
            if s["score"] >= min_score:
                results.append({
                    "scan_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "code": code,
                    "name": name,
                    "category": category,
                    "close": latest.get("close"),
                    "score": s["score"],
                    "level": s["level"],
                    "reason": s["reason"],
                    "manual_status": "觀察中",
                    "note": "",
                })
        except Exception as e:
            debug.append({"code": code, "name": name, "status": f"錯誤：{e}", "rows": 0})

        progress.progress(idx / max(total_rows, 1))

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df = result_df.sort_values("score", ascending=False if sort_mode == "分數高到低" else True).reset_index(drop=True)
    st.session_state["god_result_df"] = result_df
    st.session_state["god_debug_rows"] = debug
    status.empty()
    progress.empty()

st.divider()
st.subheader("三、推薦結果")

result_df = st.session_state["god_result_df"]
if result_df.empty:
    st.info("尚無推薦結果。請按「開始股神推薦掃描」。")
else:
    show_df = result_df.copy()
    show_df.insert(0, "選取", False)
    edited = st.data_editor(
        show_df,
        use_container_width=True,
        hide_index=True,
        column_config={"選取": st.column_config.CheckboxColumn("選取")},
        disabled=[c for c in show_df.columns if c != "選取"],
        key="god_result_editor",
    )

    selected = edited[edited["選取"] == True].drop(columns=["選取"], errors="ignore")
    c1, c2 = st.columns([1, 3])
    with c1:
        if st.button("匯入推薦紀錄", use_container_width=True, type="primary"):
            if selected.empty:
                st.warning("請先勾選要匯入的股票。")
            else:
                append_recommend_records(selected.to_dict("records"))
                st.success(f"已匯入推薦紀錄：{len(selected)} 筆。")
    with c2:
        st.caption("推薦紀錄可到 8_股神推薦紀錄.py 或 10_推薦清單.py 查看、篩選、刪除、標記。")

st.divider()
with st.expander("推薦除錯摘要", expanded=False):
    dbg = pd.DataFrame(st.session_state.get("god_debug_rows", []))
    if dbg.empty:
        st.caption("尚無除錯資料。")
    else:
        st.dataframe(dbg, use_container_width=True, hide_index=True)

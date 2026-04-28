# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from stock_master_service import (
    load_stock_master,
    refresh_stock_master,
    search_stock_master,
    get_stock_master_categories,
    get_stock_master_diagnostics,
)

st.set_page_config(page_title="股票主檔更新", layout="wide")
st.title("股票主檔更新")

# =========================================================
# 設定區
# =========================================================
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "data"
LOG_DIR.mkdir(parents=True, exist_ok=True)

UPDATE_LOG_FILE = LOG_DIR / "stock_master_update_log.json"
CACHE_FILE = BASE_DIR / "stock_master_cache.json"
MAX_LOG_ROWS = 200


# =========================================================
# 工具函式
# =========================================================
def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_int(v: Any, default: int = 0) -> int:
    try:
        if pd.isna(v):
            return default
        return int(v)
    except Exception:
        return default


def load_update_logs() -> list[dict[str, Any]]:
    if not UPDATE_LOG_FILE.exists():
        return []
    try:
        return json.loads(UPDATE_LOG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []


def save_update_logs(logs: list[dict[str, Any]]) -> None:
    try:
        UPDATE_LOG_FILE.write_text(
            json.dumps(logs[:MAX_LOG_ROWS], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


def add_update_log(
    status: str,
    row_count: int,
    market_summary: dict[str, int] | None = None,
    service_logs: list[str] | None = None,
    note: str = "",
) -> None:
    old_logs = load_update_logs()
    item = {
        "time": now_text(),
        "status": status,
        "row_count": safe_int(row_count),
        "market_summary": market_summary or {},
        "service_logs": service_logs or [],
        "note": note or "",
    }
    old_logs.insert(0, item)
    save_update_logs(old_logs)


def build_market_summary(df: pd.DataFrame) -> dict[str, int]:
    summary = {"上市": 0, "上櫃": 0, "興櫃": 0, "其他": 0}
    if df is None or df.empty:
        return summary

    # 相容新版 stock_master_service 的英文欄位 market，以及舊頁面用的中文欄位 市場別
    market_col = "市場別" if "市場別" in df.columns else ("market" if "market" in df.columns else "")
    if not market_col:
        summary["其他"] = len(df)
        return summary

    vc = df[market_col].fillna("其他").astype(str).str.strip().replace("", "其他").value_counts()
    for k, v in vc.items():
        if k in summary:
            summary[k] = int(v)
        else:
            summary["其他"] += int(v)
    return summary


def get_last_update_text(logs: list[dict[str, Any]]) -> str:
    if logs:
        return str(logs[0].get("time", "未記錄"))
    return "未記錄"


def format_market_summary(summary: dict[str, int]) -> str:
    if not summary:
        return "無"
    return " / ".join([f"{k}:{int(v)}" for k, v in summary.items()])


def clear_update_logs() -> None:
    save_update_logs([])


def get_cache_file_status() -> dict[str, Any]:
    info = {
        "exists": CACHE_FILE.exists(),
        "path": str(CACHE_FILE),
        "size_kb": 0.0,
        "modified": "—",
        "json_ok": False,
        "row_like_count": 0,
        "error": "",
    }
    try:
        if not CACHE_FILE.exists():
            info["error"] = "stock_master_cache.json 不存在"
            return info
        stat = CACHE_FILE.stat()
        info["size_kb"] = round(stat.st_size / 1024, 1)
        info["modified"] = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        payload = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        info["json_ok"] = True
        if isinstance(payload, list):
            info["row_like_count"] = len(payload)
        elif isinstance(payload, dict):
            for key in ["data", "rows", "stocks", "items"]:
                if isinstance(payload.get(key), list):
                    info["row_like_count"] = len(payload[key])
                    break
            if not info["row_like_count"]:
                info["row_like_count"] = len(payload)
        return info
    except Exception as e:
        info["error"] = str(e)
        return info


# =========================================================
# 載入主檔（預設直接讀快取，不會自動重跑）
# =========================================================
update_logs = load_update_logs()
master_df = load_stock_master()

# 若系統第一次沒有紀錄檔，但主檔已有資料，補一筆載入紀錄，避免畫面空白
if master_df is not None and not master_df.empty and not update_logs:
    add_update_log(
        status="載入快取",
        row_count=len(master_df),
        market_summary=build_market_summary(master_df),
        service_logs=["首次進入頁面，自動載入既有股票主檔快取。"],
        note="未執行重新抓取，直接使用現有主檔資料。",
    )
    update_logs = load_update_logs()

all_categories = ["全部"] + get_stock_master_categories(master_df)

# =========================================================
# 頂部資訊
# =========================================================
last_update_text = get_last_update_text(update_logs)
market_summary = build_market_summary(master_df)
row_count = 0 if master_df is None else len(master_df)

k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("主檔筆數", f"{row_count:,}")
with k2:
    st.metric("最近更新時間", last_update_text)
with k3:
    st.metric("上市/上櫃/興櫃", format_market_summary(market_summary))
with k4:
    st.metric("更新紀錄筆數", f"{len(update_logs):,}")

st.caption("本頁預設直接載入既有股票主檔快取，不會每次進頁面都重新抓資料。只有你按「更新股票主檔」時才會重跑。")

cache_status = get_cache_file_status()
with st.expander("主檔快取檔案狀態", expanded=False):
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("快取檔存在", "是" if cache_status["exists"] else "否")
    c2.metric("JSON格式", "正常" if cache_status["json_ok"] else "異常")
    c3.metric("檔案大小", f"{cache_status['size_kb']:,.1f} KB")
    c4.metric("資料筆數估算", f"{cache_status['row_like_count']:,}")
    st.caption(f"路徑：{cache_status['path']}")
    st.caption(f"最後修改：{cache_status['modified']}")
    if cache_status.get("error"):
        st.warning(f"快取檢查訊息：{cache_status['error']}")
    st.info("若主檔筆數異常、分類大量變其他類，請按上方『更新股票主檔』重建；一般查詢頁不會自動重建，避免頁面卡死。")

# =========================================================
# 篩選區
# =========================================================
c1, c2, c3, c4, c5 = st.columns([3, 2, 2, 2, 2])

with c1:
    kw = st.text_input("搜尋股票代號 / 名稱 / 正式產業別 / 主題類別")
with c2:
    market_filter = st.selectbox("市場別", ["全部", "上市", "上櫃", "興櫃"])
with c3:
    category_filter = st.selectbox("類別 / 產業篩選", all_categories)
with c4:
    st.write("")
    st.write("")
    refresh_btn = st.button("更新股票主檔", use_container_width=True, type="primary")
with c5:
    st.write("")
    st.write("")
    clear_log_btn = st.button("清空更新紀錄", use_container_width=True)

# =========================================================
# 按鈕動作
# =========================================================
if refresh_btn:
    with st.spinner("更新股票主檔中..."):
        try:
            master_df, service_logs = refresh_stock_master()
            add_update_log(
                status="更新成功",
                row_count=0 if master_df is None else len(master_df),
                market_summary=build_market_summary(master_df),
                service_logs=service_logs,
                note="手動更新股票主檔完成。",
            )
            st.success("股票主檔已更新，並已寫入更新紀錄。")
            for line in service_logs:
                st.caption(line)
            update_logs = load_update_logs()
            all_categories = ["全部"] + get_stock_master_categories(master_df)
        except Exception as e:
            add_update_log(
                status="更新失敗",
                row_count=0 if master_df is None else len(master_df),
                market_summary=build_market_summary(master_df),
                service_logs=[str(e)],
                note="手動更新股票主檔失敗。",
            )
            st.error(f"更新失敗：{e}")
            update_logs = load_update_logs()

if clear_log_btn:
    clear_update_logs()
    st.success("更新紀錄已清空。")
    update_logs = []

# =========================================================
# 診斷訊息
# =========================================================
with st.expander("主檔診斷訊息", expanded=False):
    for line in get_stock_master_diagnostics():
        st.write(f"- {line}")

# =========================================================
# 更新紀錄
# =========================================================
with st.expander("股票主檔更新紀錄", expanded=True):
    if not update_logs:
        st.info("目前尚無更新紀錄。")
    else:
        log_rows = []
        for item in update_logs:
            summary = item.get("market_summary", {}) or {}
            log_rows.append(
                {
                    "時間": item.get("time", ""),
                    "狀態": item.get("status", ""),
                    "主檔筆數": item.get("row_count", 0),
                    "市場分布": format_market_summary(summary),
                    "備註": item.get("note", ""),
                    "服務訊息筆數": len(item.get("service_logs", []) or []),
                }
            )

        st.dataframe(pd.DataFrame(log_rows), use_container_width=True, height=260)

        selected_idx = st.selectbox(
            "查看指定紀錄明細",
            options=list(range(len(update_logs))),
            format_func=lambda i: f"{update_logs[i].get('time', '')}｜{update_logs[i].get('status', '')}",
            index=0,
        )

        selected_log = update_logs[selected_idx]
        st.markdown(f"**時間：** {selected_log.get('time', '')}")
        st.markdown(f"**狀態：** {selected_log.get('status', '')}")
        st.markdown(f"**主檔筆數：** {selected_log.get('row_count', 0):,}")
        st.markdown(f"**市場分布：** {format_market_summary(selected_log.get('market_summary', {}))}")
        st.markdown(f"**備註：** {selected_log.get('note', '') or '無'}")

        detail_logs = selected_log.get("service_logs", []) or []
        if detail_logs:
            st.markdown("**更新過程訊息：**")
            for line in detail_logs:
                st.caption(line)
        else:
            st.caption("這筆紀錄沒有額外更新訊息。")

# =========================================================
# 搜尋結果
# =========================================================
found_df = search_stock_master(
    master_df,
    keyword=kw,
    market_filter=market_filter,
    category_filter=category_filter,
)

st.subheader("股票主檔資料")
st.dataframe(found_df, use_container_width=True, height=700)

# streamlit_app.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import streamlit as st

from utils import (
    format_number,
    get_all_code_name_map,
    get_normalized_watchlist,
    inject_pro_theme,
    load_last_query_state,
    parse_date_safe,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
    save_last_query_state,
)

PAGE_TITLE = "股票分析系統首頁"
PFX = "home_"


# =========================================================
# 基礎工具
# =========================================================
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


def _to_pydate(v: Any, fallback: date) -> date:
    if v is None:
        return fallback
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, pd.Timestamp):
        if pd.isna(v):
            return fallback
        return v.date()
    try:
        x = pd.to_datetime(v, errors="coerce")
        if pd.notna(x):
            return x.date()
    except Exception:
        pass
    return fallback


def _fmt_num(v: Any, digits: int = 2) -> str:
    return format_number(v, digits)


# =========================================================
# 自選股資料
# =========================================================
def _load_watchlist_data() -> dict[str, list[dict[str, str]]]:
    raw = get_normalized_watchlist()
    result: dict[str, list[dict[str, str]]] = {}

    if isinstance(raw, dict):
        for group_name, items in raw.items():
            g = _safe_str(group_name) or "未分組"
            result[g] = []

            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    code = _safe_str(item.get("code"))
                    name = _safe_str(item.get("name")) or code
                    market = _safe_str(item.get("market")) or "上市"
                    if code:
                        result[g].append(
                            {
                                "code": code,
                                "name": name,
                                "market": market,
                                "label": f"{code} {name}",
                            }
                        )

    if not result:
        all_df = get_all_code_name_map("")
        if isinstance(all_df, pd.DataFrame) and not all_df.empty:
            rows = []
            for _, row in all_df.head(20).iterrows():
                code = _safe_str(row.get("code"))
                name = _safe_str(row.get("name")) or code
                market = _safe_str(row.get("market")) or "上市"
                if code:
                    rows.append(
                        {
                            "code": code,
                            "name": name,
                            "market": market,
                            "label": f"{code} {name}",
                        }
                    )
            if rows:
                result["全部股票"] = rows

    return result


def _build_overview_df(watchlist: dict[str, list[dict[str, str]]]) -> pd.DataFrame:
    rows = []
    for group_name, items in watchlist.items():
        for item in items:
            rows.append(
                {
                    "群組": group_name,
                    "股票代號": _safe_str(item.get("code")),
                    "股票名稱": _safe_str(item.get("name")),
                    "市場別": _safe_str(item.get("market")) or "上市",
                    "股票": _safe_str(item.get("label")),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["群組", "股票代號", "股票名稱", "市場別", "股票"])
    return pd.DataFrame(rows).sort_values(["群組", "股票代號"]).reset_index(drop=True)


def _build_group_summary_df(watchlist: dict[str, list[dict[str, str]]]) -> pd.DataFrame:
    rows = []
    for group_name, items in watchlist.items():
        rows.append(
            {
                "群組": group_name,
                "股票數": len(items),
                "市場別組成": " / ".join(sorted(set([_safe_str(x.get("market")) or "上市" for x in items]))) if items else "—",
            }
        )
    if not rows:
        return pd.DataFrame(columns=["群組", "股票數", "市場別組成"])
    return pd.DataFrame(rows).sort_values(["股票數", "群組"], ascending=[False, True]).reset_index(drop=True)


def _build_search_rows(watchlist: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for group_name, items in watchlist.items():
        for item in items:
            rows.append(
                {
                    "group": group_name,
                    "code": _safe_str(item.get("code")),
                    "name": _safe_str(item.get("name")),
                    "market": _safe_str(item.get("market")),
                    "label": _safe_str(item.get("label")),
                    "blob": f"{group_name} {item.get('code','')} {item.get('name','')} {item.get('label','')}".lower(),
                }
            )
    return rows


def _find_search_target(keyword: str, rows: list[dict[str, str]]) -> dict[str, str] | None:
    q = _safe_str(keyword).lower()
    if not q:
        return None

    for row in rows:
        if q == row["code"].lower():
            return row
    for row in rows:
        if q == row["name"].lower():
            return row
    for row in rows:
        if q == row["label"].lower():
            return row

    prefix_hits = [r for r in rows if r["code"].lower().startswith(q) or r["name"].lower().startswith(q)]
    if prefix_hits:
        return prefix_hits[0]

    contain_hits = [r for r in rows if q in r["blob"]]
    if contain_hits:
        return contain_hits[0]

    return None


# =========================================================
# Session State
# =========================================================
def _init_state():
    saved = load_last_query_state()
    today = date.today()
    default_start = today - timedelta(days=180)
    default_end = today

    if _k("search_input") not in st.session_state:
        st.session_state[_k("search_input")] = ""

    if _k("start_date") not in st.session_state:
        st.session_state[_k("start_date")] = parse_date_safe(saved.get("home_start"), default_start)

    if _k("end_date") not in st.session_state:
        st.session_state[_k("end_date")] = parse_date_safe(saved.get("home_end"), default_end)

    st.session_state[_k("start_date")] = _to_pydate(st.session_state.get(_k("start_date")), default_start)
    st.session_state[_k("end_date")] = _to_pydate(st.session_state.get(_k("end_date")), default_end)


# =========================================================
# 主頁
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()
    _init_state()

    watchlist = _load_watchlist_data()
    overview_df = _build_overview_df(watchlist)
    group_summary_df = _build_group_summary_df(watchlist)
    search_rows = _build_search_rows(watchlist)

    total_groups = len(watchlist)
    total_stocks = len(overview_df)
    listed_count = int((overview_df["市場別"] == "上市").sum()) if not overview_df.empty else 0
    otc_count = int((overview_df["市場別"] == "上櫃").sum()) if not overview_df.empty else 0

    render_pro_hero(
        title="台股分析系統｜股神版首頁",
        subtitle="首頁會直接讀取最新自選股清單，新增刪除後重新整理即可同步。",
    )

    render_pro_kpi_row(
        [
            {"label": "自選股群組", "value": total_groups, "delta": "最新 watchlist", "delta_class": "pro-kpi-delta-flat"},
            {"label": "自選股總數", "value": total_stocks, "delta": "最新 watchlist", "delta_class": "pro-kpi-delta-flat"},
            {"label": "上市檔數", "value": listed_count, "delta": "市場別統計", "delta_class": "pro-kpi-delta-flat"},
            {"label": "上櫃檔數", "value": otc_count, "delta": "市場別統計", "delta_class": "pro-kpi-delta-flat"},
        ]
    )

    render_pro_section("快速搜尋股票")

    s1, s2 = st.columns([5, 1])
    with s1:
        st.text_input(
            "輸入股票代碼或名稱",
            key=_k("search_input"),
            placeholder="例如：2330、台積電、2454 聯發科",
            label_visibility="collapsed",
        )
    with s2:
        search_clicked = st.button("帶入", use_container_width=True, type="primary")

    if search_clicked:
        target = _find_search_target(st.session_state.get(_k("search_input"), ""), search_rows)
        if target:
            save_last_query_state(
                quick_group=target["group"],
                quick_stock_code=target["code"],
                home_start=st.session_state.get(_k("start_date")),
                home_end=st.session_state.get(_k("end_date")),
            )
            st.success(f"已記錄：{target['group']} / {target['label']}。可直接進入其他頁面查詢。")
        else:
            st.warning("找不到對應股票。")

    render_pro_section("快速入口")

    q1, q2, q3, q4 = st.columns(4)
    with q1:
        render_pro_info_card(
            "行情查詢",
            [
                ("用途", "看單股即時資訊、訊號燈號、支撐壓力。", ""),
            ],
            chips=["pages/2_行情查詢.py"],
        )
    with q2:
        render_pro_info_card(
            "歷史K線分析",
            [
                ("用途", "看 K 線、MA、KD、MACD、事件與策略。", ""),
            ],
            chips=["pages/3_歷史K線分析.py"],
        )
    with q3:
        render_pro_info_card(
            "自選股中心",
            [
                ("用途", "新增 / 刪除 / 批次管理自選股。", ""),
            ],
            chips=["pages/4_自選股中心.py"],
        )
    with q4:
        render_pro_info_card(
            "多股比較 / 排行榜",
            [
                ("用途", "做多檔比較、群組排行、找強弱股。", ""),
            ],
            chips=["pages/5_排行榜.py", "pages/6_多股比較.py"],
        )

    left, right = st.columns([1, 1])

    with left:
        render_pro_section("群組總覽")
        if group_summary_df.empty:
            st.info("目前沒有任何自選股群組。")
        else:
            st.dataframe(group_summary_df, use_container_width=True, hide_index=True)

        render_pro_info_card(
            "首頁提醒",
            [
                ("同步邏輯", "首頁每次重新整理都會重新讀取最新 watchlist.json。", ""),
                ("使用方式", "先在自選股中心新增 / 刪除，再回首頁重新整理即可。", ""),
                ("快速搜尋", "搜尋後會把群組與股票記錄到共用查詢狀態。", ""),
            ],
        )

    with right:
        render_pro_section("最新自選股清單")
        if overview_df.empty:
            st.info("目前沒有任何自選股。")
        else:
            st.dataframe(overview_df.head(30), use_container_width=True, hide_index=True)

    render_pro_section("最近常用查詢日期")
    d1, d2 = st.columns([2, 2])
    with d1:
        st.date_input("開始日期", key=_k("start_date"))
    with d2:
        st.date_input("結束日期", key=_k("end_date"))

    if st.button("記錄日期區間", use_container_width=True):
        save_last_query_state(
            quick_group="",
            quick_stock_code="",
            home_start=st.session_state.get(_k("start_date")),
            home_end=st.session_state.get(_k("end_date")),
        )
        st.success("已記錄首頁日期區間。")


if __name__ == "__main__":
    main()

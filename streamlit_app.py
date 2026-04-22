from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import streamlit as st

from firebase_backup import backup_github_repo_to_firebase, list_recent_backups
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
# 快取資料處理
# =========================================================
@st.cache_data(ttl=60, show_spinner=False)
def _build_watchlist_payload(raw_items: tuple) -> dict[str, list[dict[str, str]]]:
    result: dict[str, list[dict[str, str]]] = {}

    for group_name, items in raw_items:
        g = _safe_str(group_name) or "未分組"
        rows = []

        for item in items:
            if not isinstance(item, tuple) or len(item) < 3:
                continue

            code = _safe_str(item[0])
            name = _safe_str(item[1]) or code
            market = _safe_str(item[2]) or "上市"

            if code:
                rows.append(
                    {
                        "code": code,
                        "name": name,
                        "market": market,
                        "label": f"{code} {name}",
                    }
                )

        result[g] = rows

    return result


@st.cache_data(ttl=60, show_spinner=False)
def _build_overview_df_cached(watchlist_items: tuple) -> pd.DataFrame:
    rows = []
    for group_name, items in watchlist_items:
        for item in items:
            if not isinstance(item, tuple) or len(item) < 4:
                continue
            rows.append(
                {
                    "群組": _safe_str(group_name),
                    "股票代號": _safe_str(item[0]),
                    "股票名稱": _safe_str(item[1]),
                    "市場別": _safe_str(item[2]) or "上市",
                    "股票": _safe_str(item[3]),
                }
            )

    if not rows:
        return pd.DataFrame(columns=["群組", "股票代號", "股票名稱", "市場別", "股票"])

    return pd.DataFrame(rows).sort_values(["群組", "股票代號"]).reset_index(drop=True)


@st.cache_data(ttl=60, show_spinner=False)
def _build_group_summary_df_cached(watchlist_items: tuple) -> pd.DataFrame:
    rows = []

    for group_name, items in watchlist_items:
        markets = []
        for item in items:
            if not isinstance(item, tuple) or len(item) < 3:
                continue
            market = _safe_str(item[2]) or "上市"
            markets.append(market)

        rows.append(
            {
                "群組": _safe_str(group_name),
                "股票數": len(items),
                "市場別組成": " / ".join(sorted(set(markets))) if markets else "—",
            }
        )

    if not rows:
        return pd.DataFrame(columns=["群組", "股票數", "市場別組成"])

    return pd.DataFrame(rows).sort_values(["股票數", "群組"], ascending=[False, True]).reset_index(drop=True)


@st.cache_data(ttl=60, show_spinner=False)
def _build_search_rows_cached(watchlist_items: tuple) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    for group_name, items in watchlist_items:
        g = _safe_str(group_name)
        for item in items:
            if not isinstance(item, tuple) or len(item) < 4:
                continue

            code = _safe_str(item[0])
            name = _safe_str(item[1])
            market = _safe_str(item[2])
            label = _safe_str(item[3])

            rows.append(
                {
                    "group": g,
                    "code": code,
                    "name": name,
                    "market": market,
                    "label": label,
                    "blob": f"{g} {code} {name} {label}".lower(),
                }
            )

    return rows


# =========================================================
# 自選股資料
# =========================================================
def _load_watchlist_data() -> dict[str, list[dict[str, str]]]:
    raw = get_normalized_watchlist()
    packed_items = []

    if isinstance(raw, dict):
        for group_name, items in raw.items():
            temp = []
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    temp.append(
                        (
                            _safe_str(item.get("code")),
                            _safe_str(item.get("name")),
                            _safe_str(item.get("market")),
                        )
                    )
            packed_items.append((group_name, tuple(temp)))

    result = _build_watchlist_payload(tuple(packed_items))

    if not result:
        all_df = get_all_code_name_map("")
        if isinstance(all_df, pd.DataFrame) and not all_df.empty:
            rows = []
            sample_df = all_df.head(20)
            for _, row in sample_df.iterrows():
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


def _pack_watchlist_for_cache(watchlist: dict[str, list[dict[str, str]]]) -> tuple:
    packed = []
    for group_name, items in watchlist.items():
        temp = []
        for item in items:
            temp.append(
                (
                    _safe_str(item.get("code")),
                    _safe_str(item.get("name")),
                    _safe_str(item.get("market")),
                    _safe_str(item.get("label")),
                )
            )
        packed.append((group_name, tuple(temp)))
    return tuple(packed)


def _build_overview_df(watchlist: dict[str, list[dict[str, str]]]) -> pd.DataFrame:
    return _build_overview_df_cached(_pack_watchlist_for_cache(watchlist))


def _build_group_summary_df(watchlist: dict[str, list[dict[str, str]]]) -> pd.DataFrame:
    return _build_group_summary_df_cached(_pack_watchlist_for_cache(watchlist))


def _build_search_rows(watchlist: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    return _build_search_rows_cached(_pack_watchlist_for_cache(watchlist))


def _find_search_target(keyword: str, rows: list[dict[str, str]]) -> dict[str, str] | None:
    q = _safe_str(keyword).lower()
    if not q:
        return None

    exact_code = next((row for row in rows if q == row["code"].lower()), None)
    if exact_code:
        return exact_code

    exact_name = next((row for row in rows if q == row["name"].lower()), None)
    if exact_name:
        return exact_name

    exact_label = next((row for row in rows if q == row["label"].lower()), None)
    if exact_label:
        return exact_label

    prefix_hit = next((r for r in rows if r["code"].lower().startswith(q) or r["name"].lower().startswith(q)), None)
    if prefix_hit:
        return prefix_hit

    contain_hit = next((r for r in rows if q in r["blob"]), None)
    if contain_hit:
        return contain_hit

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

    if _k("backup_rows") not in st.session_state:
        st.session_state[_k("backup_rows")] = []

    st.session_state[_k("start_date")] = _to_pydate(st.session_state.get(_k("start_date")), default_start)
    st.session_state[_k("end_date")] = _to_pydate(st.session_state.get(_k("end_date")), default_end)


# =========================================================
# Firebase 備份區
# =========================================================
def _render_backup_section():
    render_pro_section("GitHub 專案一鍵備份到 Firebase")

    repo_owner = st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")
    repo_name = st.secrets.get("GITHUB_REPO_NAME", "stock-app")
    branch = st.secrets.get("GITHUB_REPO_BRANCH", "main")
    bucket_name = st.secrets.get("FIREBASE_STORAGE_BUCKET", "").strip()

    st.caption("按下按鈕後，會將 GitHub 專案打包成 zip，上傳到 Firebase Storage，並同步寫入 Firestore 備份紀錄。")

    c1, c2, c3 = st.columns([1.2, 1.2, 1.2])
    with c1:
        st.text_input("GitHub 擁有者", value=repo_owner, disabled=True, key=_k("repo_owner_view"))
    with c2:
        st.text_input("GitHub 專案", value=repo_name, disabled=True, key=_k("repo_name_view"))
    with c3:
        st.text_input("分支", value=branch, disabled=True, key=_k("repo_branch_view"))

    info1, info2 = st.columns([1.6, 1])
    with info1:
        st.caption(f"Firebase Storage Bucket：{bucket_name or '未設定'}")
    with info2:
        st.caption("若顯示未設定，請先補 Secrets。")

    b1, b2 = st.columns([1.5, 1])
    with b1:
        if st.button("一鍵備份 GitHub 專案到 Firebase", type="primary", use_container_width=True, key=_k("backup_btn")):
            with st.spinner("正在備份 GitHub 專案到 Firebase..."):
                try:
                    result = backup_github_repo_to_firebase(
                        repo_owner=repo_owner,
                        repo_name=repo_name,
                        branch=branch,
                        github_token=st.secrets.get("GITHUB_TOKEN", None),
                    )
                    st.session_state[_k("last_backup_result")] = result
                    st.success("備份成功")
                except Exception as e:
                    st.session_state[_k("last_backup_error")] = str(e)
                    st.error(f"備份失敗：{e}")

    with b2:
        if st.button("重新整理備份紀錄", use_container_width=True, key=_k("backup_refresh_btn")):
            try:
                st.session_state[_k("backup_rows")] = list_recent_backups(limit=10)
                st.success("已重新整理備份紀錄")
            except Exception as e:
                st.error(f"讀取備份紀錄失敗：{e}")

    last_result = st.session_state.get(_k("last_backup_result"))
    if last_result:
        r1, r2, r3 = st.columns(3)
        with r1:
            render_pro_info_card(
                "最新備份時間",
                [("時間", _safe_str(last_result.get("backup_time_tw")), "")],
            )
        with r2:
            render_pro_info_card(
                "最新備份大小",
                [("bytes", f"{int(last_result.get('size_bytes', 0)):,}", "")],
            )
        with r3:
            render_pro_info_card(
                "Storage 路徑",
                [("路徑", _safe_str(last_result.get("storage_path")), "")],
            )

    last_error = st.session_state.get(_k("last_backup_error"))
    if last_error:
        st.warning(f"最近一次錯誤：{last_error}")

    try:
        backup_rows = list_recent_backups(limit=10)
        st.session_state[_k("backup_rows")] = backup_rows
    except Exception:
        backup_rows = st.session_state.get(_k("backup_rows"), [])

    if backup_rows:
        st.dataframe(pd.DataFrame(backup_rows), use_container_width=True, hide_index=True)
    else:
        st.info("目前尚無備份紀錄，或 Firebase 尚未完成設定。")

    render_pro_info_card(
        "Firebase 設定提醒",
        [
            ("Storage", "需先設定 FIREBASE_STORAGE_BUCKET", ""),
            ("金鑰", "需先設定 FIREBASE_SERVICE_ACCOUNT_JSON", ""),
            ("紀錄", "備份成功後會寫入 Firestore：github_repo_backups", ""),
        ],
    )


# =========================================================
# 原首頁內容（免登入版）
# =========================================================
def _render_home_page():
    watchlist = _load_watchlist_data()
    overview_df = _build_overview_df(watchlist)
    group_summary_df = _build_group_summary_df(watchlist)
    search_rows = _build_search_rows(watchlist)

    total_groups = len(watchlist)
    total_stocks = len(overview_df)
    listed_count = int((overview_df["市場別"] == "上市").sum()) if not overview_df.empty else 0
    otc_count = int((overview_df["市場別"] == "上櫃").sum()) if not overview_df.empty else 0

    st.sidebar.markdown("### 📌 系統狀態")
    st.sidebar.success("免登入模式")
    st.sidebar.caption("已取消帳號密碼登入，開啟即直接使用。")

    render_pro_hero(
        title="台股分析系統｜股神版首頁",
        subtitle="免登入版本｜首頁會直接讀取最新自選股清單，新增刪除後重新整理即可同步。",
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
            [("用途", "看單股即時資訊、訊號燈號、支撐壓力。", "")],
            chips=["pages/2_行情查詢.py"],
        )
    with q2:
        render_pro_info_card(
            "歷史K線分析",
            [("用途", "看 K 線、MA、KD、MACD、事件與策略。", "")],
            chips=["pages/3_歷史K線分析.py"],
        )
    with q3:
        render_pro_info_card(
            "自選股中心",
            [("用途", "新增 / 刪除 / 批次管理自選股。", "")],
            chips=["pages/4_自選股中心.py"],
        )
    with q4:
        render_pro_info_card(
            "多股比較 / 排行榜",
            [("用途", "做多檔比較、群組排行、找強弱股。", "")],
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

    _render_backup_section()


# =========================================================
# 主程序
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()
    _init_state()
    _render_home_page()


if __name__ == "__main__":
    main()

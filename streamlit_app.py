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

PAGE_TITLE = "股票分析系統首頁｜升級完整版"
PFX = "home_"


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
# 本頁專用安全卡片渲染
# 說明：覆蓋 utils.render_pro_info_card，避免 utils 舊快取或 HTML escaping 造成 <div> 原始碼顯示在頁面。
# 支援 info_pairs = (label, value) 或 (label, value, css_class)，不影響其他頁面。
# =========================================================
def render_pro_info_card(title, info_pairs, chips=None):
    import html

    safe_title = html.escape(_safe_str(title) or "—")

    chips_html = ""
    if chips:
        if isinstance(chips, str):
            chip_list = [chips]
        else:
            chip_list = list(chips)
        chips_html = "".join(
            f'<span class="pro-chip">{html.escape(_safe_str(x))}</span>'
            for x in chip_list
            if _safe_str(x)
        )

    items_html = ""
    for pair in info_pairs or []:
        label = ""
        value = ""
        css_class = ""
        try:
            if isinstance(pair, (list, tuple)):
                if len(pair) >= 1:
                    label = pair[0]
                if len(pair) >= 2:
                    value = pair[1]
                if len(pair) >= 3:
                    css_class = pair[2]
            elif isinstance(pair, dict):
                label = pair.get("label", "")
                value = pair.get("value", "")
                css_class = pair.get("css_class", "")
            else:
                value = pair
        except Exception:
            value = pair

        safe_label = html.escape(_safe_str(label) or "—")
        safe_value = html.escape(_safe_str(value) or "—")
        safe_css = html.escape(_safe_str(css_class))

        items_html += f"""
        <div class="pro-info-item">
            <div class="pro-info-label">{safe_label}</div>
            <div class="pro-info-value {safe_css}">{safe_value}</div>
        </div>
        """

    if not items_html.strip():
        items_html = """
        <div class="pro-info-item">
            <div class="pro-info-label">狀態</div>
            <div class="pro-info-value">—</div>
        </div>
        """

    st.markdown(
        f"""
        <div class="pro-card">
            <div class="pro-card-title">{safe_title}</div>
            <div style="margin-bottom:10px;">{chips_html}</div>
            <div class="pro-info-grid">
                {items_html}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _godpick_records_github_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("GODPICK_RECORDS_GITHUB_PATH", "godpick_records.json")) or "godpick_records.json",
    }


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


@st.cache_data(ttl=300, show_spinner=False)
def _load_godpick_records_df() -> pd.DataFrame:
    cols = [
        "record_id", "股票代號", "股票名稱", "推薦模式", "推薦等級", "推薦總分",
        "買點分級", "型態名稱", "爆發等級", "推薦日期", "推薦時間", "目前狀態"
    ]
    cfg = _godpick_records_github_config()
    if not cfg["token"]:
        return pd.DataFrame(columns=cols)

    try:
        import base64, json, requests
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(cfg["token"]),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code != 200:
            return pd.DataFrame(columns=cols)

        content = resp.json().get("content", "")
        if not content:
            return pd.DataFrame(columns=cols)

        data = json.loads(base64.b64decode(content).decode("utf-8"))
        df = pd.DataFrame(data if isinstance(data, list) else [])
        for c in cols:
            if c not in df.columns:
                df[c] = None

        df["股票代號"] = df["股票代號"].astype(str).str.extract(r"(\d+)")[0].fillna(df["股票代號"].astype(str))
        df["推薦總分"] = pd.to_numeric(df["推薦總分"], errors="coerce")
        return df[cols].copy()
    except Exception:
        return pd.DataFrame(columns=cols)


def _build_latest_rec_df(rec_df: pd.DataFrame) -> pd.DataFrame:
    if rec_df is None or rec_df.empty:
        return pd.DataFrame(columns=[
            "股票代號", "股票名稱", "推薦模式", "推薦等級", "推薦總分", "買點分級",
            "型態名稱", "爆發等級", "最近推薦時間", "目前狀態"
        ])

    work = rec_df.copy()
    work["_sort_dt"] = pd.to_datetime(
        work["推薦日期"].fillna("").astype(str) + " " + work["推薦時間"].fillna("").astype(str),
        errors="coerce",
    )
    work = work.sort_values(["股票代號", "_sort_dt"], ascending=[True, False], na_position="last")
    work = work.drop_duplicates(subset=["股票代號"], keep="first").reset_index(drop=True)
    work["最近推薦時間"] = (
        work["推薦日期"].fillna("").astype(str) + " " + work["推薦時間"].fillna("").astype(str)
    ).str.strip()

    cols = [
        "股票代號", "股票名稱", "推薦模式", "推薦等級", "推薦總分", "買點分級",
        "型態名稱", "爆發等級", "最近推薦時間", "目前狀態"
    ]
    for c in cols:
        if c not in work.columns:
            work[c] = ""
    return work[cols].copy()

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


def _render_home_page():
    watchlist = _load_watchlist_data()
    overview_df = _build_overview_df(watchlist)
    group_summary_df = _build_group_summary_df(watchlist)
    search_rows = _build_search_rows(watchlist)
    rec_df = _load_godpick_records_df()
    latest_rec_df = _build_latest_rec_df(rec_df)

    total_groups = len(watchlist)
    total_stocks = len(overview_df)
    listed_count = int((overview_df["市場別"] == "上市").sum()) if not overview_df.empty else 0
    otc_count = int((overview_df["市場別"] == "上櫃").sum()) if not overview_df.empty else 0
    total_rec = len(latest_rec_df)
    strong_focus = int((pd.to_numeric(latest_rec_df["推薦總分"], errors="coerce") >= 85).sum()) if not latest_rec_df.empty else 0

    st.sidebar.markdown("### 📌 系統狀態")
    st.sidebar.success("免登入模式")
    st.sidebar.caption("已取消帳號密碼登入，開啟即直接使用。")

    render_pro_hero(
        title="台股分析系統｜升級完整版首頁",
        subtitle="首頁總控面板｜串接自選股、股神推薦紀錄、快速入口與最近查詢狀態。",
    )

    render_pro_kpi_row(
        [
            {"label": "自選股群組", "value": total_groups, "delta": "最新 watchlist", "delta_class": "pro-kpi-delta-flat"},
            {"label": "自選股總數", "value": total_stocks, "delta": "最新 watchlist", "delta_class": "pro-kpi-delta-flat"},
            {"label": "最近推薦股票數", "value": total_rec, "delta": "股神推薦紀錄", "delta_class": "pro-kpi-delta-flat"},
            {"label": "強烈關注數", "value": strong_focus, "delta": "推薦總分 ≥ 85", "delta_class": "pro-kpi-delta-flat"},
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


    render_pro_section("系統總覽")
    a1, a2 = st.columns([1, 1])
    with a1:
        render_pro_info_card(
            "最近推薦摘要",
            [
                ("最近推薦股票數", total_rec, ""),
                ("高分股票數", strong_focus, ""),
                ("最近查詢群組", _safe_str(load_last_query_state().get("quick_group", "—")), ""),
                ("最近查詢股票", _safe_str(load_last_query_state().get("quick_stock_code", "—")), ""),
            ],
            chips=["7頁", "8頁", "首頁總控"],
        )
    with a2:
        render_pro_info_card(
            "首頁提醒",
            [
                ("同步邏輯", "首頁會讀取最新自選股與股神推薦紀錄。", ""),
                ("承接功能", "搜尋後可供 2 / 3 / 7 頁承接查詢狀態。", ""),
                ("建議流程", "7頁選股 → 3頁確認 → 8頁追蹤。", ""),
            ],
            chips=["系統流程"],
        )

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
            [("用途", "看 K 線、MA、KD、MACD、事件與策略。", ""), ("承接", "可接收 4頁 / 7頁送來的焦點股票。", "")],
            chips=["pages/3_歷史K線分析.py"],
        )
    with q3:
        render_pro_info_card(
            "自選股中心",
            [("用途", "新增 / 刪除 / 批次管理自選股。", ""), ("承接", "可顯示最近推薦總分 / 買點分級。", "")],
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
                ("推薦追蹤", "同時讀取股神推薦紀錄，方便從首頁掌握最新高分股票。", ""),
                ("快速搜尋", "搜尋後會把群組與股票記錄到共用查詢狀態。", ""),
            ],
        )

    with right:
        render_pro_section("最新自選股清單")
        if overview_df.empty:
            st.info("目前沒有任何自選股。")
        else:
            st.dataframe(overview_df.head(30), use_container_width=True, hide_index=True)

        render_pro_section("最近高分推薦")
        if latest_rec_df.empty:
            st.info("目前沒有推薦紀錄。")
        else:
            show_cols = [c for c in ["股票代號", "股票名稱", "推薦模式", "推薦等級", "推薦總分", "買點分級", "型態名稱", "爆發等級", "最近推薦時間", "目前狀態"] if c in latest_rec_df.columns]
            top_df = latest_rec_df.sort_values("推薦總分", ascending=False).head(12)
            st.dataframe(top_df[show_cols], use_container_width=True, hide_index=True)

    render_pro_section("最近常用查詢日期")
    d1, d2 = st.columns([2, 2])
    with d1:
        st.date_input("開始日期", key=_k("start_date"))
    with d2:
        st.date_input("結束日期", key=_k("end_date"))

    h1, h2, h3 = st.columns(3)
    with h1:
        if st.button("記錄日期區間", use_container_width=True):
            save_last_query_state(
                quick_group="",
                quick_stock_code="",
                home_start=st.session_state.get(_k("start_date")),
                home_end=st.session_state.get(_k("end_date")),
            )
            st.success("已記錄首頁日期區間。")
    with h2:
        if st.button("帶目前搜尋到 2頁 / 3頁", use_container_width=True):
            target = _find_search_target(st.session_state.get(_k("search_input"), ""), search_rows)
            if target:
                st.session_state["kline_focus_stock_code"] = target["code"]
                st.session_state["kline_focus_stock_name"] = target["name"]
                st.success(f"已設定焦點股票：{target['label']}")
            else:
                st.warning("請先輸入可辨識的股票代號或名稱。")
    with h3:
        if st.button("清除首頁搜尋紀錄", use_container_width=True):
            st.session_state[_k("search_input")] = ""
            st.success("已清除首頁搜尋欄位。")


def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()
    _init_state()
    _render_home_page()


if __name__ == "__main__":
    main()

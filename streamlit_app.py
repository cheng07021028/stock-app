from __future__ import annotations

HOME_V5_LINK_VERSION = "home_v5_link_v1_20260427"

from datetime import date, datetime, timedelta
import base64
import json
from pathlib import Path

import requests
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



def _read_github_json_file(path_name: str, default):
    cfg = _godpick_records_github_config()
    token = cfg["token"]
    if not token:
        return default

    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], path_name),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code != 200:
            return default
        content = resp.json().get("content", "")
        if not content:
            return default
        return json.loads(base64.b64decode(content).decode("utf-8"))
    except Exception:
        return default


def _read_local_json_file(path_name: str, default):
    try:
        p = Path(path_name)
        if not p.exists():
            return default
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _normalize_godpick_payload(payload) -> pd.DataFrame:
    if isinstance(payload, dict):
        if isinstance(payload.get("recommendations"), list):
            payload = payload.get("recommendations")
        elif isinstance(payload.get("records"), list):
            payload = payload.get("records")
        else:
            payload = []
    if not isinstance(payload, list):
        payload = []
    return pd.DataFrame(payload)


@st.cache_data(ttl=300, show_spinner=False)
def _load_godpick_records_df() -> pd.DataFrame:
    """
    首頁最近高分推薦資料源強化：
    1. 優先讀取 godpick_latest_recommendations.json（7頁本輪推薦永久保存）
    2. 再讀取 godpick_recommend_list.json（10頁推薦清單）
    3. 再讀取 godpick_records.json（8頁推薦紀錄）
    4. 欄位自動相容新版：推薦分桶 / 信心等級 / 買點劇本 / 風險說明
    """
    cols = [
        "record_id", "股票代號", "股票名稱", "推薦模式", "推薦等級", "推薦總分",
        "股神決策模式", "股神進場建議", "推薦分層", "建議部位%", "風險報酬比", "追價風險分",
        "大盤參考等級", "大盤加權分", "買點分級", "推薦分桶", "信心等級", "型態名稱", "爆發等級",
        "風險說明", "股神推論邏輯", "推薦日期", "推薦時間", "更新時間", "建立時間", "目前狀態"
    ]

    payloads = []

    # GitHub：新版本輪推薦
    latest_payload = _read_github_json_file("godpick_latest_recommendations.json", {})
    if not latest_payload:
        latest_payload = _read_local_json_file("godpick_latest_recommendations.json", {})
    latest_df = _normalize_godpick_payload(latest_payload)
    if not latest_df.empty:
        latest_df["首頁資料來源"] = "本輪推薦"

    # GitHub：推薦清單
    list_payload = _read_github_json_file("godpick_recommend_list.json", [])
    if not list_payload:
        list_payload = _read_local_json_file("godpick_recommend_list.json", [])
    list_df = _normalize_godpick_payload(list_payload)
    if not list_df.empty:
        list_df["首頁資料來源"] = "推薦清單"

    # GitHub：推薦紀錄
    cfg = _godpick_records_github_config()
    rec_payload = _read_github_json_file(cfg["path"], [])
    if not rec_payload:
        rec_payload = _read_local_json_file(cfg["path"], [])
    rec_df = _normalize_godpick_payload(rec_payload)
    if not rec_df.empty:
        rec_df["首頁資料來源"] = "推薦紀錄"

    for df in [latest_df, list_df, rec_df]:
        if isinstance(df, pd.DataFrame) and not df.empty:
            payloads.append(df)

    if not payloads:
        return pd.DataFrame(columns=cols + ["首頁資料來源"])

    df = pd.concat(payloads, ignore_index=True, sort=False)

    for c in cols + ["首頁資料來源"]:
        if c not in df.columns:
            df[c] = ""

    # 新版欄位相容：如果舊欄位沒有資料，就用新版欄位補。
    df["型態名稱"] = df["型態名稱"].where(df["型態名稱"].fillna("").astype(str).str.strip() != "", df.get("推薦分桶", ""))
    df["爆發等級"] = df["爆發等級"].where(df["爆發等級"].fillna("").astype(str).str.strip() != "", df.get("信心等級", ""))

    if "目前狀態" in df.columns:
        df["目前狀態"] = df["目前狀態"].replace({None: "", "None": ""}).fillna("")
    df["目前狀態"] = df["目前狀態"].where(df["目前狀態"].astype(str).str.strip() != "", "觀察")

    for c in ["股票代號", "股票名稱", "推薦模式", "推薦等級", "買點分級", "型態名稱", "爆發等級", "推薦日期", "推薦時間", "更新時間", "建立時間", "首頁資料來源"]:
        df[c] = df[c].replace({None: "", "None": ""}).fillna("").astype(str)

    df["股票代號"] = df["股票代號"].astype(str).str.extract(r"(\d+)")[0].fillna(df["股票代號"].astype(str))
    df["推薦總分"] = pd.to_numeric(df["推薦總分"], errors="coerce").fillna(0)

    return df[cols + ["首頁資料來源"]].copy()


def _build_latest_rec_df(rec_df: pd.DataFrame) -> pd.DataFrame:
    show_cols = [
        "股票代號", "股票名稱", "推薦模式", "推薦等級", "推薦總分", "買點分級",
        "型態名稱", "爆發等級", "最近推薦時間", "目前狀態", "首頁資料來源"
    ]
    if rec_df is None or rec_df.empty:
        return pd.DataFrame(columns=show_cols)

    work = rec_df.copy()
    for c in ["推薦日期", "推薦時間", "更新時間", "建立時間"]:
        if c not in work.columns:
            work[c] = ""
        work[c] = work[c].fillna("").astype(str)

    work["_sort_dt"] = pd.to_datetime(
        work["推薦日期"].astype(str) + " " + work["推薦時間"].astype(str),
        errors="coerce",
    )
    # 若日期時間欄位沒有，改用更新/建立時間補排序。
    upd_dt = pd.to_datetime(work["更新時間"], errors="coerce")
    crt_dt = pd.to_datetime(work["建立時間"], errors="coerce")
    work["_sort_dt"] = work["_sort_dt"].fillna(upd_dt).fillna(crt_dt)

    # 本輪推薦 / 推薦清單優先於歷史紀錄，避免首頁抓到舊資料。
    source_rank = {"本輪推薦": 3, "推薦清單": 2, "推薦紀錄": 1}
    work["_source_rank"] = work.get("首頁資料來源", "").map(lambda x: source_rank.get(str(x), 0))

    work = work.sort_values(
        ["推薦總分", "_source_rank", "_sort_dt"],
        ascending=[False, False, False],
        na_position="last",
    ).reset_index(drop=True)

    work["最近推薦時間"] = work["_sort_dt"].dt.strftime("%Y-%m-%d %H:%M:%S")
    work["最近推薦時間"] = work["最近推薦時間"].replace("NaT", "")
    work["最近推薦時間"] = work["最近推薦時間"].where(
        work["最近推薦時間"].astype(str).str.strip() != "",
        (work["推薦日期"].fillna("").astype(str) + " " + work["推薦時間"].fillna("").astype(str)).str.strip()
    )

    # 顯示不要再出現 None。
    for c in show_cols:
        if c not in work.columns:
            work[c] = ""
        work[c] = work[c].replace({None: "", "None": ""}).fillna("")

    return work[show_cols].copy()

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


def _clear_home_search_input():
    """使用 callback 清除首頁搜尋欄，避免 widget 建立後改 session_state 造成 StreamlitAPIException。"""
    st.session_state[_k("search_input")] = ""
    st.session_state[_k("search_cleared_notice")] = True



# =========================================================
# v39 首頁大盤趨勢快照串接
# 讀取 0_大盤趨勢 / 01 大盤趨勢輸出的 market_snapshot.json。
# 首頁只讀檔、不主動抓網路資料，避免拖慢首頁。
# =========================================================
def _read_market_snapshot_v39() -> dict[str, Any]:
    candidates = [Path("market_snapshot.json"), Path("macro_mode_bridge.json")]
    for p in candidates:
        try:
            if not p.exists():
                continue
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                data["_snapshot_file"] = str(p)
                return data
        except Exception:
            continue
    return {}


def _market_value_v39(snapshot: dict[str, Any], key: str, default: Any = "—") -> Any:
    if not isinstance(snapshot, dict):
        return default
    v = snapshot.get(key, default)
    if v is None or v == "":
        return default
    return v


def _market_float_v39(snapshot: dict[str, Any], key: str) -> float | None:
    v = _market_value_v39(snapshot, key, None)
    try:
        if v is None or pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(str(v).replace(",", "").replace("%", ""))
    except Exception:
        return None


def _fmt_market_num_v39(v: Any, digits: int = 2) -> str:
    try:
        if v is None or pd.isna(v):
            return "—"
    except Exception:
        pass
    try:
        return f"{float(str(v).replace(',', '').replace('%', '')):,.{digits}f}"
    except Exception:
        return str(v) if str(v).strip() else "—"


def _fmt_market_signed_v39(v: Any, digits: int = 2, suffix: str = "") -> str:
    try:
        if v is None or pd.isna(v):
            return "—"
    except Exception:
        pass
    try:
        n = float(str(v).replace(',', '').replace('%', ''))
        return f"{n:+,.{digits}f}{suffix}"
    except Exception:
        return str(v) if str(v).strip() else "—"


def _render_market_snapshot_home_v39():
    snapshot = _read_market_snapshot_v39()
    render_pro_section("大盤趨勢總控｜0_大盤趨勢串接", "首頁直接讀取 market_snapshot.json，不重新抓資料，避免首頁變慢。")

    if not snapshot:
        render_pro_info_card(
            "大盤快照尚未建立",
            [
                ("狀態", "尚未讀到 market_snapshot.json", ""),
                ("建議", "先進入 0_大盤趨勢，執行更新並寫入股神橋接檔", ""),
                ("首頁行為", "只讀檔，不會在首頁主動抓取大盤資料", ""),
            ],
            chips=["market_snapshot.json", "等待 0_大盤趨勢更新"],
        )
        return

    score = _market_float_v39(snapshot, "market_score")
    trend = _market_value_v39(snapshot, "market_trend")
    risk = _market_value_v39(snapshot, "market_risk_level")
    risk_gate = _market_value_v39(snapshot, "risk_gate", _market_value_v39(snapshot, "risk_gate_mode"))
    session_label = _market_value_v39(snapshot, "market_session_label", _market_value_v39(snapshot, "market_session"))
    usable = _market_value_v39(snapshot, "market_session_usable")
    quality = _market_value_v39(snapshot, "data_quality")
    updated_at = _market_value_v39(snapshot, "updated_at")
    file_name = _market_value_v39(snapshot, "_snapshot_file")

    effect = snapshot.get("godpick_market_effect", {}) if isinstance(snapshot.get("godpick_market_effect"), dict) else {}
    adjustment = effect.get("recommendation_adjustment", snapshot.get("recommendation_adjustment", "—"))
    effect_desc = effect.get("effect_summary", effect.get("description", snapshot.get("trend_comment", "—")))

    render_pro_kpi_row([
        {
            "label": "大盤分數",
            "value": _fmt_market_num_v39(score, 1),
            "delta": f"{trend}｜風險 {risk}",
            "delta_class": "pro-kpi-delta-up" if (score or 0) >= 60 else "pro-kpi-delta-down" if (score or 0) < 45 else "pro-kpi-delta-flat",
        },
        {
            "label": "風控閘門",
            "value": risk_gate,
            "delta": f"交易時段：{session_label}",
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "資料品質",
            "value": quality,
            "delta": f"時段可用：{usable}",
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "推薦調整",
            "value": str(adjustment),
            "delta": "由 0_大盤趨勢輸出",
            "delta_class": "pro-kpi-delta-flat",
        },
    ])

    c1, c2 = st.columns([1, 1])
    with c1:
        render_pro_info_card(
            "大盤對股神推薦影響",
            [
                ("大盤分數", _fmt_market_num_v39(score, 1), ""),
                ("大盤趨勢", trend, ""),
                ("風控閘門", risk_gate, ""),
                ("交易時段", session_label, ""),
                ("推薦調整", adjustment, ""),
                ("影響說明", effect_desc, ""),
            ],
            chips=["market_snapshot", "godpick bridge"],
        )
    with c2:
        render_pro_info_card(
            "盤面漲跌摘要",
            [
                ("加權漲跌點", f"{_fmt_market_signed_v39(snapshot.get('twse_change'))}｜{_fmt_market_signed_v39(snapshot.get('twse_change_pct'), suffix='%')}", ""),
                ("櫃買漲跌點", f"{_fmt_market_signed_v39(snapshot.get('otc_change'))}｜{_fmt_market_signed_v39(snapshot.get('otc_change_pct'), suffix='%')}", ""),
                ("期貨漲跌點", f"{_fmt_market_signed_v39(snapshot.get('futures_change'))}｜{_fmt_market_signed_v39(snapshot.get('futures_change_pct'), suffix='%')}", ""),
                ("更新時間", updated_at, ""),
                ("快照檔案", file_name, ""),
            ],
            chips=["TWSE", "TPEx", "TAIFEX"],
        )

    diagnostics = snapshot.get("data_diagnostics", [])
    if isinstance(diagnostics, list) and diagnostics:
        with st.expander("大盤資料來源診斷", expanded=False):
            st.dataframe(pd.DataFrame(diagnostics), use_container_width=True, hide_index=True)

# =========================================================
# v75：首頁隔夜風控摘要
# 只讀 01 大盤趨勢輸出的快照檔，不重新連外抓資料。
# =========================================================
def _render_overnight_snapshot_home_v75():
    snapshot = _read_market_snapshot_v39()
    if not isinstance(snapshot, dict) or not snapshot:
        return
    has_any = any(k in snapshot for k in ["overnight_score", "overnight_risk_level", "overnight_bias", "overnight_comment", "night_futures_change_pct", "nasdaq_change_pct", "sox_change_pct", "macro_one_click_finished_at"])
    if not has_any:
        return
    render_pro_section("隔夜國際盤風控｜v75", "讀取 01 大盤趨勢的夜盤 / 美盤 / 費半 / 匯率摘要，不重新抓資料。")
    render_pro_kpi_row([
        {"label":"隔夜分數", "value":_fmt_market_num_v39(snapshot.get("overnight_score"),1), "delta":f"風險：{snapshot.get('overnight_risk_level','—')}", "delta_class":"pro-kpi-delta-flat"},
        {"label":"隔夜偏向", "value":str(snapshot.get("overnight_bias") or "—"), "delta":f"更新：{snapshot.get('macro_one_click_finished_at') or snapshot.get('updated_at') or '—'}", "delta_class":"pro-kpi-delta-flat"},
        {"label":"Nasdaq", "value":_fmt_market_signed_v39(snapshot.get("nasdaq_change_pct"),2,"%"), "delta":"美股科技參考", "delta_class":"pro-kpi-delta-flat"},
        {"label":"費半", "value":_fmt_market_signed_v39(snapshot.get("sox_change_pct"),2,"%"), "delta":"半導體參考", "delta_class":"pro-kpi-delta-flat"},
        {"label":"台指夜盤", "value":_fmt_market_signed_v39(snapshot.get("night_futures_change_pct"),2,"%"), "delta":str(snapshot.get("night_futures_source") or "—"), "delta_class":"pro-kpi-delta-flat"},
    ])
    render_pro_info_card("隔夜股神解讀", [("隔夜說明", snapshot.get("overnight_comment") or "—"), ("台指夜盤來源", snapshot.get("night_futures_source") or "—"), ("夜盤備援", snapshot.get("night_futures_fallback_note") or "—"), ("資料品質", snapshot.get("overnight_data_quality") or snapshot.get("data_quality") or "—")], chips=["v75", "overnight", "只讀快照"])

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

    _render_market_snapshot_home_v39()
    _render_overnight_snapshot_home_v75()

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
            show_cols = [c for c in ["股票代號", "股票名稱", "推薦模式", "推薦等級", "推薦總分", "買點分級", "型態名稱", "爆發等級", "最近推薦時間", "目前狀態", "首頁資料來源"] if c in latest_rec_df.columns]
            top_df = latest_rec_df.sort_values("推薦總分", ascending=False).head(12).copy()
            for c in show_cols:
                if c in top_df.columns:
                    top_df[c] = top_df[c].replace({None: "", "None": ""}).fillna("")
            st.dataframe(top_df[show_cols], use_container_width=True, hide_index=True)

            if st.button("🔄 重新載入最近高分推薦", use_container_width=True, key=_k("reload_latest_recommend")):
                try:
                    _load_godpick_records_df.clear()
                except Exception:
                    pass
                st.rerun()

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
        st.button(
            "清除首頁搜尋紀錄",
            use_container_width=True,
            on_click=_clear_home_search_input,
        )

    if st.session_state.get(_k("search_cleared_notice"), False):
        st.success("已清除首頁搜尋欄位。")
        st.session_state[_k("search_cleared_notice")] = False


def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    st.caption(f"首頁股神決策V5串聯版：{HOME_V5_LINK_VERSION}")
    inject_pro_theme()
    _init_state()
    _render_home_page()


if __name__ == "__main__":
    main()

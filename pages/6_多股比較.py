# pages/6_多股比較.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from utils import (
    compute_radar_scores,
    compute_signal_snapshot,
    compute_support_resistance_snapshot,
    format_number,
    get_all_code_name_map,
    get_history_data,
    get_normalized_watchlist,
    get_stock_name_and_market,
    inject_pro_theme,
    load_last_query_state,
    parse_date_safe,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
    save_last_query_state,
    score_to_badge,
)

PAGE_TITLE = "多股比較"
PFX = "compare_"


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


def _fmt_pct(v: Any, digits: int = 2) -> str:
    try:
        if pd.isna(v):
            return "—"
    except Exception:
        pass
    try:
        return f"{float(v):+,.{digits}f}%"
    except Exception:
        return "—"


def _safe_float(v: Any, default: float = np.nan) -> float:
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return default


def _ensure_score_dict(v: Any, default_keys: list[str] | None = None, default_value: float = 50.0) -> dict[str, float]:
    default_keys = default_keys or []
    base = {k: float(default_value) for k in default_keys}

    if isinstance(v, dict):
        out = base.copy()
        for k, val in v.items():
            out[str(k)] = _safe_float(val, default_value)
        return out

    if isinstance(v, (list, tuple)):
        out = base.copy()
        for i, k in enumerate(default_keys):
            if i < len(v):
                out[k] = _safe_float(v[i], default_value)
        return out

    return base.copy()


def _ensure_signal_dict(v: Any) -> dict[str, Any]:
    if isinstance(v, dict):
        return v
    return {"score": 0.0, "label": "中性", "class": "pro-flat"}


def _ensure_sr_dict(v: Any) -> dict[str, Any]:
    if isinstance(v, dict):
        return v
    return {
        "res_20": np.nan,
        "sup_20": np.nan,
        "res_60": np.nan,
        "sup_60": np.nan,
        "pressure_signal": ("中性", "pro-flat"),
        "support_signal": ("中性", "pro-flat"),
        "break_signal": ("區間內", "pro-flat"),
    }


def _signal_label(v: Any, fallback: str) -> str:
    if isinstance(v, (list, tuple)) and len(v) > 0:
        return _safe_str(v[0]) or fallback
    if isinstance(v, dict):
        return _safe_str(v.get("label")) or fallback
    return _safe_str(v) or fallback


# =========================================================
# watchlist / state
# =========================================================
@st.cache_data(ttl=120, show_spinner=False)
def _build_group_stock_map_cached(raw_items: tuple) -> dict[str, list[dict[str, str]]]:
    group_map: dict[str, list[dict[str, str]]] = {}

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

        group_map[g] = rows

    return group_map


@st.cache_data(ttl=120, show_spinner=False)
def _flatten_group_map_cached(group_items: tuple) -> list[dict[str, str]]:
    rows = []
    for group_name, items in group_items:
        g = _safe_str(group_name)
        for item in items:
            if not isinstance(item, tuple) or len(item) < 4:
                continue
            rows.append(
                {
                    "group": g,
                    "code": _safe_str(item[0]),
                    "name": _safe_str(item[1]),
                    "market": _safe_str(item[2]),
                    "label": _safe_str(item[3]),
                }
            )
    return rows


def _build_group_stock_map() -> dict[str, list[dict[str, str]]]:
    watchlist = get_normalized_watchlist()
    packed = []

    if isinstance(watchlist, dict):
        for group_name, items in watchlist.items():
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
            packed.append((group_name, tuple(temp)))

    group_map = _build_group_stock_map_cached(tuple(packed))

    if not group_map:
        all_df = get_all_code_name_map("")
        if isinstance(all_df, pd.DataFrame) and not all_df.empty:
            rows = []
            for _, row in all_df.head(100).iterrows():
                code = _safe_str(row.get("code"))
                name = _safe_str(row.get("name"))
                market = _safe_str(row.get("market")) or "上市"
                if code:
                    rows.append(
                        {
                            "code": code,
                            "name": name or code,
                            "market": market,
                            "label": f"{code} {name or code}",
                        }
                    )
            if rows:
                group_map["全部股票"] = rows

    return group_map


def _flatten_group_map(group_map: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    group_items = tuple(
        (
            group_name,
            tuple(
                (
                    _safe_str(item.get("code")),
                    _safe_str(item.get("name")),
                    _safe_str(item.get("market")),
                    _safe_str(item.get("label")),
                )
                for item in items
            ),
        )
        for group_name, items in group_map.items()
    )
    return _flatten_group_map_cached(group_items)


def _init_state(group_map: dict[str, list[dict[str, str]]]):
    saved = load_last_query_state()
    today = date.today()
    default_start = today - timedelta(days=180)
    default_end = today

    groups = list(group_map.keys())
    if _k("group") not in st.session_state:
        saved_group = _safe_str(saved.get("quick_group", ""))
        st.session_state[_k("group")] = saved_group if saved_group in groups else (groups[0] if groups else "")

    if _k("selected_codes") not in st.session_state:
        saved_code = _safe_str(saved.get("quick_stock_code", ""))
        default_codes = [saved_code] if saved_code else []
        st.session_state[_k("selected_codes")] = default_codes

    if _k("start_date") not in st.session_state:
        st.session_state[_k("start_date")] = parse_date_safe(saved.get("home_start"), default_start)

    if _k("end_date") not in st.session_state:
        st.session_state[_k("end_date")] = parse_date_safe(saved.get("home_end"), default_end)

    if _k("search_input") not in st.session_state:
        st.session_state[_k("search_input")] = ""

    st.session_state[_k("start_date")] = _to_pydate(st.session_state.get(_k("start_date")), default_start)
    st.session_state[_k("end_date")] = _to_pydate(st.session_state.get(_k("end_date")), default_end)

    _repair_state(group_map)


def _repair_state(group_map: dict[str, list[dict[str, str]]]):
    groups = list(group_map.keys())
    current_group = _safe_str(st.session_state.get(_k("group"), ""))

    if current_group not in group_map:
        st.session_state[_k("group")] = groups[0] if groups else ""
        current_group = st.session_state[_k("group")]

    valid_codes = {x["code"] for x in group_map.get(current_group, [])}
    selected_codes = st.session_state.get(_k("selected_codes"), [])
    if not isinstance(selected_codes, list):
        selected_codes = []

    selected_codes = [str(c) for c in selected_codes if str(c) in valid_codes]
    st.session_state[_k("selected_codes")] = selected_codes[:4]


def _on_group_change(group_map: dict[str, list[dict[str, str]]]):
    current_group = _safe_str(st.session_state.get(_k("group"), ""))
    items = group_map.get(current_group, [])
    default_codes = [x["code"] for x in items[:2]]
    st.session_state[_k("selected_codes")] = default_codes[:4]


def _find_search_target(keyword: str, flat_rows: list[dict[str, str]]) -> dict[str, str] | None:
    q = _safe_str(keyword).lower()
    if not q:
        return None

    exact_code = next((row for row in flat_rows if q == row["code"].lower()), None)
    if exact_code:
        return exact_code

    exact_name = next((row for row in flat_rows if q == row["name"].lower()), None)
    if exact_name:
        return exact_name

    exact_label = next((row for row in flat_rows if q == row["label"].lower()), None)
    if exact_label:
        return exact_label

    prefix_hit = next(
        (r for r in flat_rows if r["code"].lower().startswith(q) or r["name"].lower().startswith(q)),
        None,
    )
    if prefix_hit:
        return prefix_hit

    contain_hit = next(
        (r for r in flat_rows if q in f"{r['group']} {r['code']} {r['name']} {r['label']}".lower()),
        None,
    )
    if contain_hit:
        return contain_hit

    return None


# =========================================================
# 資料整理
# =========================================================
def _prepare_history_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    if "日期" not in df.columns:
        return pd.DataFrame()

    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)

    for col in ["成交股數", "開盤價", "最高價", "最低價", "收盤價"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["收盤價"]).copy()
    if df.empty:
        return pd.DataFrame()

    close = df["收盤價"]
    high = df["最高價"]
    low = df["最低價"]

    for n in [5, 10, 20, 60, 120, 240]:
        df[f"MA{n}"] = close.rolling(n).mean()

    low_9 = low.rolling(9).min()
    high_9 = high.rolling(9).max()
    rsv = (close - low_9) / (high_9 - low_9).replace(0, np.nan) * 100
    df["K"] = rsv.ewm(alpha=1 / 3, adjust=False).mean()
    df["D"] = df["K"].ewm(alpha=1 / 3, adjust=False).mean()
    df["J"] = 3 * df["K"] - 2 * df["D"]

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["DIF"] = ema12 - ema26
    df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["DIF"] - df["DEA"]

    df["漲跌幅(%)"] = close.pct_change() * 100
    return df


@st.cache_data(ttl=1800, show_spinner=False)
def _get_code_name_map_df() -> pd.DataFrame:
    return get_all_code_name_map("")


@st.cache_data(ttl=600, show_spinner=False)
def _get_compare_snapshot(code: str, manual_name: str, market_type: str, start_date: date, end_date: date) -> dict[str, Any]:
    all_code_name_df = _get_code_name_map_df()
    stock_name, market_type2 = get_stock_name_and_market(code, all_code_name_df, manual_name)
    market = market_type or market_type2 or "上市"

    raw_df = get_history_data(
        stock_no=code,
        stock_name=stock_name,
        market_type=market,
        start_date=start_date,
        end_date=end_date,
    )
    df = _prepare_history_df(raw_df)
    if df.empty or len(df) < 2:
        return {"ok": False, "code": code, "name": stock_name, "market": market}

    signal_raw = compute_signal_snapshot(df)
    sr_raw = compute_support_resistance_snapshot(df)
    radar_raw = compute_radar_scores(df)

    signal = _ensure_signal_dict(signal_raw)
    sr = _ensure_sr_dict(sr_raw)
    radar = _ensure_score_dict(
        radar_raw,
        default_keys=["trend", "momentum", "volume", "position", "structure"],
        default_value=50.0,
    )

    first_close = _safe_float(df.iloc[0].get("收盤價"))
    last_close = _safe_float(df.iloc[-1].get("收盤價"))
    interval_pct = ((last_close / first_close) - 1) * 100 if pd.notna(first_close) and first_close != 0 else np.nan

    radar_avg = float(
        np.mean(
            [
                _safe_float(radar.get("trend", 50), 50),
                _safe_float(radar.get("momentum", 50), 50),
                _safe_float(radar.get("volume", 50), 50),
                _safe_float(radar.get("position", 50), 50),
                _safe_float(radar.get("structure", 50), 50),
            ]
        )
    )

    signal_score = _safe_float(signal.get("score"), 0.0)

    return {
        "ok": True,
        "code": code,
        "name": stock_name,
        "market": market,
        "label": f"{code} {stock_name}",
        "days": len(df),
        "last_close": last_close,
        "interval_pct": interval_pct,
        "signal_score": signal_score,
        "signal_badge": score_to_badge(signal_score)[0],
        "signal": signal,
        "sr": sr,
        "radar": radar,
        "radar_avg": radar_avg,
        "radar_rows": {
            "趨勢": _safe_float(radar.get("trend", 50), 50),
            "動能": _safe_float(radar.get("momentum", 50), 50),
            "量能": _safe_float(radar.get("volume", 50), 50),
            "位置": _safe_float(radar.get("position", 50), 50),
            "結構": _safe_float(radar.get("structure", 50), 50),
        },
    }



@st.cache_data(ttl=300, show_spinner=False)
def _build_compare_df(items_payload: tuple, start_date: date, end_date: date) -> pd.DataFrame:
    rows = []

    for item in items_payload:
        if not isinstance(item, tuple) or len(item) < 3:
            continue

        code = _safe_str(item[0])
        manual_name = _safe_str(item[1])
        market_type = _safe_str(item[2]) or "上市"

        snap = _get_compare_snapshot(
            code=code,
            manual_name=manual_name,
            market_type=market_type,
            start_date=start_date,
            end_date=end_date,
        )
        if not snap.get("ok"):
            continue

        sr = snap["sr"]
        radar_rows = snap["radar_rows"]

        rows.append(
            {
                "股票": snap["label"],
                "最新收盤": snap["last_close"],
                "區間漲跌幅(%)": snap["interval_pct"],
                "燈號": snap["signal_badge"],
                "訊號分數": snap["signal_score"],
                "雷達均分": snap["radar_avg"],
                "趨勢": radar_rows["趨勢"],
                "動能": radar_rows["動能"],
                "量能": radar_rows["量能"],
                "位置": radar_rows["位置"],
                "結構": radar_rows["結構"],
                "20日壓力": sr.get("res_20"),
                "20日支撐": sr.get("sup_20"),
                "60日壓力": sr.get("res_60"),
                "60日支撐": sr.get("sup_60"),
                "壓力訊號": _signal_label(sr.get("pressure_signal", ("中性", "pro-flat")), "中性"),
                "支撐訊號": _signal_label(sr.get("support_signal", ("中性", "pro-flat")), "中性"),
                "區間訊號": _signal_label(sr.get("break_signal", ("區間內", "pro-flat")), "區間內"),
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    num_cols = [
        "最新收盤",
        "區間漲跌幅(%)",
        "訊號分數",
        "雷達均分",
        "趨勢",
        "動能",
        "量能",
        "位置",
        "結構",
        "20日壓力",
        "20日支撐",
        "60日壓力",
        "60日支撐",
    ]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


@st.cache_data(ttl=120, show_spinner=False)
def _format_compare_table(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    for col in ["最新收盤", "20日壓力", "20日支撐", "60日壓力", "60日支撐"]:
        if col in work.columns:
            work[col] = work[col].apply(lambda x: format_number(x, 2) if pd.notna(x) else "—")

    for col in ["區間漲跌幅(%)"]:
        if col in work.columns:
            work[col] = work[col].apply(lambda x: _fmt_pct(x) if pd.notna(x) else "—")

    for col in ["訊號分數", "雷達均分", "趨勢", "動能", "量能", "位置", "結構"]:
        if col in work.columns:
            work[col] = work[col].apply(lambda x: format_number(x, 1) if pd.notna(x) else "—")
    return work


# =========================================================
# 圖表
# =========================================================
@st.cache_data(ttl=300, show_spinner=False)
def _build_interval_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df["股票"],
            y=df["區間漲跌幅(%)"],
            name="區間漲跌幅(%)",
        )
    )
    fig.update_layout(
        title="區間漲跌幅比較",
        height=420,
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis_title="股票",
        yaxis_title="區間漲跌幅(%)",
    )
    return fig


@st.cache_data(ttl=300, show_spinner=False)
def _build_signal_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df["股票"],
            y=df["訊號分數"],
            name="訊號分數",
        )
    )
    fig.update_layout(
        title="訊號分數比較",
        height=420,
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis_title="股票",
        yaxis_title="訊號分數",
    )
    return fig


@st.cache_data(ttl=300, show_spinner=False)
def _build_radar_avg_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df["股票"],
            y=df["雷達均分"],
            name="雷達均分",
        )
    )
    fig.update_layout(
        title="雷達均分比較",
        height=420,
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis_title="股票",
        yaxis_title="雷達均分",
    )
    return fig


# =========================================================
# 主頁
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    group_map = _build_group_stock_map()
    flat_rows = _flatten_group_map(group_map)
    _init_state(group_map)

    render_pro_hero(
        title="多股比較｜股神版",
        subtitle="2~4 檔股票同場比較，含雷達、燈號、支撐壓力、區間漲跌幅。",
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
        if st.button("帶入", use_container_width=True, type="primary"):
            target = _find_search_target(st.session_state.get(_k("search_input"), ""), flat_rows)
            if target:
                st.session_state[_k("group")] = target["group"]
                current_codes = st.session_state.get(_k("selected_codes"), [])
                if not isinstance(current_codes, list):
                    current_codes = []
                if target["code"] not in current_codes:
                    current_codes = (current_codes + [target["code"]])[:4]
                st.session_state[_k("selected_codes")] = current_codes
                st.rerun()
            else:
                st.warning("找不到對應股票。")

    render_pro_section("查詢條件")
    _repair_state(group_map)

    groups = list(group_map.keys())
    c1, c2, c3, c4 = st.columns([2, 3, 2, 2])

    with c1:
        st.selectbox(
            "群組",
            options=groups,
            key=_k("group"),
            on_change=_on_group_change,
            args=(group_map,),
        )

    current_group = _safe_str(st.session_state.get(_k("group"), ""))
    items = group_map.get(current_group, [])
    code_to_item = {x["code"]: x for x in items}
    code_options = [x["code"] for x in items]

    current_codes = st.session_state.get(_k("selected_codes"), [])
    if not isinstance(current_codes, list):
        current_codes = []
    current_codes = [c for c in current_codes if c in code_options][:4]
    st.session_state[_k("selected_codes")] = current_codes

    with c2:
        st.multiselect(
            "群組股票（2~4 檔）",
            options=code_options,
            default=current_codes,
            key=_k("selected_codes"),
            format_func=lambda code: code_to_item.get(code, {}).get("label", code),
            max_selections=4,
        )

    with c3:
        st.date_input("開始日期", key=_k("start_date"))

    with c4:
        st.date_input("結束日期", key=_k("end_date"))

    selected_codes = st.session_state.get(_k("selected_codes"), [])
    start_date = _to_pydate(st.session_state.get(_k("start_date")), date.today() - timedelta(days=180))
    end_date = _to_pydate(st.session_state.get(_k("end_date")), date.today())

    if start_date > end_date:
        st.error("開始日期不可大於結束日期。")
        st.stop()

    if len(selected_codes) < 2:
        st.warning("請至少選擇 2 檔股票比較。")
        st.stop()

    save_last_query_state(
        quick_group=current_group,
        quick_stock_code=selected_codes[0] if selected_codes else "",
        home_start=start_date,
        home_end=end_date,
    )

    items_payload = tuple(
        (
            _safe_str(code_to_item[code].get("code")),
            _safe_str(code_to_item[code].get("name")),
            _safe_str(code_to_item[code].get("market")),
        )
        for code in selected_codes
        if code in code_to_item
    )

    with st.spinner("計算多股比較中..."):
        compare_df = _build_compare_df(items_payload, start_date, end_date)

    if compare_df.empty:
        st.error("查無可比較資料，請更換股票或日期區間。")
        st.stop()

    best_return = compare_df.sort_values("區間漲跌幅(%)", ascending=False).iloc[0]
    best_signal = compare_df.sort_values("訊號分數", ascending=False).iloc[0]
    best_radar = compare_df.sort_values("雷達均分", ascending=False).iloc[0]

    render_pro_kpi_row(
        [
            {"label": "比較檔數", "value": len(compare_df), "delta": current_group, "delta_class": "pro-kpi-delta-flat"},
            {"label": "最強漲幅", "value": _safe_str(best_return["股票"]), "delta": _fmt_pct(best_return["區間漲跌幅(%)"]), "delta_class": "pro-kpi-delta-flat"},
            {"label": "最高訊號", "value": _safe_str(best_signal["股票"]), "delta": _fmt_num(best_signal["訊號分數"], 1), "delta_class": "pro-kpi-delta-flat"},
            {"label": "最高雷達", "value": _safe_str(best_radar["股票"]), "delta": _fmt_num(best_radar["雷達均分"], 1), "delta_class": "pro-kpi-delta-flat"},
        ]
    )

    top1, top2 = st.columns(2)
    with top1:
        render_pro_info_card(
            "最強區間股",
            [
                ("股票", _safe_str(best_return.get("股票")), ""),
                ("區間漲跌幅", _fmt_pct(best_return.get("區間漲跌幅(%)")), ""),
                ("燈號", _safe_str(best_return.get("燈號")), ""),
                ("雷達均分", _fmt_num(best_return.get("雷達均分"), 1), ""),
                ("壓力訊號", _safe_str(best_return.get("壓力訊號")), ""),
                ("支撐訊號", _safe_str(best_return.get("支撐訊號")), ""),
            ],
        )

    with top2:
        render_pro_info_card(
            "比較提醒",
            [
                ("比較原則", "先看區間漲跌，再看訊號分數與雷達均分。", ""),
                ("支撐壓力", "距壓力近者容易卡關，距支撐近者要看是否守穩。", ""),
                ("燈號判讀", "強多 / 偏多可列優先觀察，強空需保守。", ""),
            ],
        )

    render_pro_section("比較圖表")
    g1, g2, g3 = st.columns(3)

    with g1:
        st.plotly_chart(_build_interval_chart(compare_df), use_container_width=True)
    with g2:
        st.plotly_chart(_build_signal_chart(compare_df), use_container_width=True)
    with g3:
        st.plotly_chart(_build_radar_avg_chart(compare_df), use_container_width=True)

    render_pro_section("比較明細")
    st.dataframe(_format_compare_table(compare_df), use_container_width=True, hide_index=True)

    render_pro_section("支撐壓力比較")
    sr_df = compare_df[
        [
            "股票",
            "20日壓力",
            "20日支撐",
            "60日壓力",
            "60日支撐",
            "壓力訊號",
            "支撐訊號",
            "區間訊號",
        ]
    ].copy()
    st.dataframe(_format_compare_table(sr_df), use_container_width=True, hide_index=True)

    render_pro_section("雷達細項比較")
    radar_df = compare_df[
        [
            "股票",
            "趨勢",
            "動能",
            "量能",
            "位置",
            "結構",
            "雷達均分",
        ]
    ].copy()
    st.dataframe(_format_compare_table(radar_df), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()

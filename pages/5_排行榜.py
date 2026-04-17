# pages/2_行情查詢.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import requests
import streamlit as st

from utils import (
    compute_signal_snapshot,
    compute_support_resistance_snapshot,
    format_number,
    get_all_code_name_map,
    get_history_data,
    get_normalized_watchlist,
    get_realtime_stock_info,
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

PAGE_TITLE = "行情查詢"
PFX = "rt_"


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


def _safe_float(v: Any, default=None):
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return default


def _to_date(v: Any, fallback: date) -> date:
    if v is None:
        return fallback
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    try:
        x = pd.to_datetime(v, errors="coerce")
        if pd.notna(x):
            return x.date()
    except Exception:
        pass
    return fallback


def _html(s: str):
    st.markdown(s, unsafe_allow_html=True)


# =========================================================
# 群組 / 搜尋
# =========================================================
def _build_group_stock_map() -> dict[str, list[dict[str, str]]]:
    watchlist = get_normalized_watchlist()
    group_map: dict[str, list[dict[str, str]]] = {}

    if isinstance(watchlist, dict):
        for group_name, items in watchlist.items():
            g = _safe_str(group_name) or "未分組"
            group_map[g] = []
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    code = _safe_str(item.get("code"))
                    name = _safe_str(item.get("name")) or code
                    market = _safe_str(item.get("market")) or "上市"
                    if code:
                        group_map[g].append(
                            {
                                "code": code,
                                "name": name,
                                "market": market,
                                "label": f"{code} {name}",
                            }
                        )

    if not group_map:
        try:
            all_df = get_all_code_name_map("")
            if isinstance(all_df, pd.DataFrame) and not all_df.empty:
                rows = []
                for _, row in all_df.head(150).iterrows():
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
                    group_map["全部股票"] = rows
        except Exception:
            pass

    return group_map


def _flatten_group_map(group_map: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    rows = []
    for group_name, items in group_map.items():
        for item in items:
            rows.append(
                {
                    "group": group_name,
                    "code": _safe_str(item.get("code")),
                    "name": _safe_str(item.get("name")),
                    "market": _safe_str(item.get("market")),
                    "label": _safe_str(item.get("label")),
                }
            )
    return rows


def _find_search_target(keyword: str, flat_rows: list[dict[str, str]]) -> dict[str, str] | None:
    q = _safe_str(keyword).lower()
    if not q:
        return None

    for row in flat_rows:
        if q == row["code"].lower():
            return row
    for row in flat_rows:
        if q == row["name"].lower():
            return row
    for row in flat_rows:
        if q == row["label"].lower():
            return row

    prefix_hits = [r for r in flat_rows if r["code"].lower().startswith(q) or r["name"].lower().startswith(q)]
    if prefix_hits:
        return prefix_hits[0]

    contain_hits = [
        r for r in flat_rows
        if q in f"{r['group']} {r['code']} {r['name']} {r['label']}".lower()
    ]
    if contain_hits:
        return contain_hits[0]

    return None


# =========================================================
# State
# =========================================================
def _init_state(group_map: dict[str, list[dict[str, str]]]):
    saved = load_last_query_state()
    groups = list(group_map.keys())
    today = date.today()
    default_start = today - timedelta(days=180)
    default_end = today

    if _k("group") not in st.session_state:
        saved_group = _safe_str(saved.get("quick_group", ""))
        st.session_state[_k("group")] = saved_group if saved_group in groups else (groups[0] if groups else "")

    if _k("stock_code") not in st.session_state:
        st.session_state[_k("stock_code")] = _safe_str(saved.get("quick_stock_code", ""))

    if _k("search_input") not in st.session_state:
        st.session_state[_k("search_input")] = ""

    if _k("start_date") not in st.session_state:
        st.session_state[_k("start_date")] = parse_date_safe(saved.get("home_start"), default_start)

    if _k("end_date") not in st.session_state:
        st.session_state[_k("end_date")] = parse_date_safe(saved.get("home_end"), default_end)

    _repair_state(group_map)


def _repair_state(group_map: dict[str, list[dict[str, str]]]):
    groups = list(group_map.keys())
    current_group = _safe_str(st.session_state.get(_k("group"), ""))

    if current_group not in group_map:
        st.session_state[_k("group")] = groups[0] if groups else ""
        current_group = st.session_state[_k("group")]

    items = group_map.get(current_group, [])
    valid_codes = [x["code"] for x in items]
    current_code = _safe_str(st.session_state.get(_k("stock_code"), ""))

    if valid_codes:
        if current_code not in valid_codes:
            st.session_state[_k("stock_code")] = valid_codes[0]
    else:
        st.session_state[_k("stock_code")] = ""


def _on_group_change(group_map: dict[str, list[dict[str, str]]]):
    current_group = _safe_str(st.session_state.get(_k("group"), ""))
    items = group_map.get(current_group, [])
    st.session_state[_k("stock_code")] = items[0]["code"] if items else ""


# =========================================================
# 歷史資料 + fallback
# =========================================================
def _prepare_history_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    if "日期" not in df.columns:
        return pd.DataFrame()

    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)

    numeric_cols = ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "收盤價" not in df.columns:
        return pd.DataFrame()

    df = df.dropna(subset=["收盤價"]).copy()
    if df.empty:
        return pd.DataFrame()

    close = df["收盤價"]
    high = df["最高價"] if "最高價" in df.columns else close
    low = df["最低價"] if "最低價" in df.columns else close

    for n in [5, 10, 20, 60, 120, 240]:
        df[f"MA{n}"] = close.rolling(n).mean()

    low_9 = low.rolling(9).min()
    high_9 = high.rolling(9).max()
    rsv = (close - low_9) / (high_9 - low_9).replace(0, pd.NA) * 100
    df["K"] = rsv.ewm(alpha=1 / 3, adjust=False).mean()
    df["D"] = df["K"].ewm(alpha=1 / 3, adjust=False).mean()
    df["J"] = 3 * df["K"] - 2 * df["D"]

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["DIF"] = ema12 - ema26
    df["DEA"] = df["DIF"].ewm(span=9, adjust=False).mean()
    df["MACD_HIST"] = df["DIF"] - df["DEA"]

    return df


@st.cache_data(ttl=1800, show_spinner=False)
def _get_tpex_history_data(stock_no: str, start_date: date, end_date: date) -> pd.DataFrame:
    stock_no = _safe_str(stock_no)
    if not stock_no:
        return pd.DataFrame()

    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    if end_ts < start_ts:
        return pd.DataFrame()

    month_starts = pd.date_range(start=start_ts.replace(day=1), end=end_ts, freq="MS")
    frames = []
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.tpex.org.tw/"}

    for dt in month_starts:
        roc_year = dt.year - 1911
        roc_date = f"{roc_year}/{dt.month:02d}"
        try:
            r = requests.get(
                "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php",
                params={"l": "zh-tw", "d": roc_date, "stkno": stock_no},
                headers=headers,
                timeout=20,
                verify=False,
            )
            r.raise_for_status()
            data = r.json()
            aa_data = data.get("aaData", [])
            fields = data.get("fields", [])
            if not aa_data:
                continue
            temp = pd.DataFrame(aa_data, columns=fields if fields and len(fields) == len(aa_data[0]) else None)
            frames.append(temp)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    rename_map = {}
    for col in df.columns:
        c = _safe_str(col)
        if c in ["日期", "日 期"]:
            rename_map[col] = "日期"
        elif "成交仟股" in c or "成交股數" in c:
            rename_map[col] = "成交股數"
        elif "成交仟元" in c or "成交金額" in c:
            rename_map[col] = "成交金額"
        elif "開盤" in c:
            rename_map[col] = "開盤價"
        elif "最高" in c:
            rename_map[col] = "最高價"
        elif "最低" in c:
            rename_map[col] = "最低價"
        elif "收盤" in c:
            rename_map[col] = "收盤價"
        elif "成交筆數" in c:
            rename_map[col] = "成交筆數"
    df = df.rename(columns=rename_map)

    if "日期" not in df.columns:
        return pd.DataFrame()

    def convert_roc_date(x):
        x = _safe_str(x)
        if not x:
            return pd.NaT
        if "/" in x:
            parts = x.split("/")
            if len(parts) == 3:
                try:
                    return pd.Timestamp(year=int(parts[0]) + 1911, month=int(parts[1]), day=int(parts[2]))
                except Exception:
                    return pd.NaT
        try:
            return pd.to_datetime(x)
        except Exception:
            return pd.NaT

    df["日期"] = df["日期"].apply(convert_roc_date)
    df = df.dropna(subset=["日期"])

    for col in ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace(" ", "", regex=False)
                .replace(["--", "---", "", "----"], pd.NA)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "成交股數" in df.columns:
        try:
            med = df["成交股數"].dropna().median()
            if pd.notna(med) and med < 100000:
                df["成交股數"] = df["成交股數"] * 1000
        except Exception:
            pass

    df = df[(df["日期"] >= start_ts) & (df["日期"] <= end_ts)]
    df = df.sort_values("日期").drop_duplicates(subset=["日期"], keep="last").reset_index(drop=True)
    return df


@st.cache_data(ttl=900, show_spinner=False)
def _get_history_data_smart(stock_no: str, stock_name: str, market_type: str, start_date: date, end_date: date) -> pd.DataFrame:
    df = get_history_data(
        stock_no=stock_no,
        stock_name=stock_name,
        market_type=market_type,
        start_date=start_date,
        end_date=end_date,
    )
    df = _prepare_history_df(df)
    if not df.empty:
        return df

    if _safe_str(market_type) in ["上櫃", "興櫃"]:
        df2 = _get_tpex_history_data(stock_no, start_date, end_date)
        df2 = _prepare_history_df(df2)
        if not df2.empty:
            return df2

    return pd.DataFrame()


# =========================================================
# 事件摘要
# =========================================================
def _build_recent_event_summary(df: pd.DataFrame) -> list[tuple[str, str, str]]:
    if df is None or df.empty or len(df) < 3:
        return [("最近事件", "資料不足", "")]

    rows: list[tuple[str, str, str]] = []

    try:
        last = df.iloc[-1]
        prev = df.iloc[-2]

        date_text = pd.to_datetime(last["日期"]).strftime("%Y-%m-%d")

        ma5 = _safe_float(last.get("MA5"))
        ma10 = _safe_float(last.get("MA10"))
        ma20 = _safe_float(last.get("MA20"))
        prev_ma5 = _safe_float(prev.get("MA5"))
        prev_ma10 = _safe_float(prev.get("MA10"))

        if ma5 is not None and ma10 is not None and prev_ma5 is not None and prev_ma10 is not None:
            if prev_ma5 <= prev_ma10 and ma5 > ma10:
                rows.append((date_text, "MA黃金交叉", ""))
            elif prev_ma5 >= prev_ma10 and ma5 < ma10:
                rows.append((date_text, "MA死亡交叉", ""))

        k = _safe_float(last.get("K"))
        d = _safe_float(last.get("D"))
        prev_k = _safe_float(prev.get("K"))
        prev_d = _safe_float(prev.get("D"))
        if k is not None and d is not None and prev_k is not None and prev_d is not None:
            if prev_k <= prev_d and k > d:
                rows.append((date_text, "KD黃金交叉", ""))
            elif prev_k >= prev_d and k < d:
                rows.append((date_text, "KD死亡交叉", ""))

        dif = _safe_float(last.get("DIF"))
        dea = _safe_float(last.get("DEA"))
        prev_dif = _safe_float(prev.get("DIF"))
        prev_dea = _safe_float(prev.get("DEA"))
        if dif is not None and dea is not None and prev_dif is not None and prev_dea is not None:
            if prev_dif <= prev_dea and dif > dea:
                rows.append((date_text, "MACD黃金交叉", ""))
            elif prev_dif >= prev_dea and dif < dea:
                rows.append((date_text, "MACD死亡交叉", ""))

        if len(df) >= 20 and all(c in df.columns for c in ["最高價", "最低價", "收盤價"]):
            df20 = df.tail(20)
            high20 = _safe_float(df20["最高價"].max())
            low20 = _safe_float(df20["最低價"].min())
            close_price = _safe_float(last.get("收盤價"))
            if close_price is not None and high20 is not None and close_price >= high20:
                rows.append((date_text, "突破20日高", ""))
            if close_price is not None and low20 is not None and close_price <= low20:
                rows.append((date_text, "跌破20日低", ""))

        if ma20 is not None:
            close_price = _safe_float(last.get("收盤價"))
            if close_price is not None:
                if close_price > ma20:
                    rows.append((date_text, "站上MA20", ""))
                else:
                    rows.append((date_text, "跌落MA20下", ""))
    except Exception:
        pass

    if not rows:
        return [("最近事件", "目前無明確新事件", "")]
    return rows[:6]


# =========================================================
# 即時卡片
# =========================================================
def _render_realtime_hero(info: dict[str, Any], stock_label: str, market_type: str):
    price = _safe_float(info.get("price"))
    prev_close = _safe_float(info.get("prev_close"))
    open_price = _safe_float(info.get("open"))
    high_price = _safe_float(info.get("high"))
    low_price = _safe_float(info.get("low"))
    change = _safe_float(info.get("change"))
    change_pct = _safe_float(info.get("change_pct"))
    total_volume = _safe_float(info.get("total_volume"))
    update_time = _safe_str(info.get("update_time"))

    delta_text = "—"
    if change is not None and change_pct is not None:
        delta_text = f"{change:+.2f} ({change_pct:+.2f}%)"
    elif change is not None:
        delta_text = f"{change:+.2f}"

    render_pro_kpi_row(
        [
            {
                "label": "現價",
                "value": format_number(price, 2),
                "delta": delta_text,
                "delta_class": "pro-kpi-delta-up" if (change or 0) > 0 else ("pro-kpi-delta-down" if (change or 0) < 0 else "pro-kpi-delta-flat"),
            },
            {
                "label": "開盤",
                "value": format_number(open_price, 2),
                "delta": market_type,
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "最高 / 最低",
                "value": f"{format_number(high_price, 2)} / {format_number(low_price, 2)}",
                "delta": f"昨收 {format_number(prev_close, 2)}",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "總量",
                "value": format_number(total_volume, 0),
                "delta": update_time or "—",
                "delta_class": "pro-kpi-delta-flat",
            },
        ]
    )

    _html(
        f"""
        <div style="background:linear-gradient(135deg,#0f172a 0%,#1e293b 100%);border-radius:18px;padding:14px 16px;margin-bottom:14px;">
            <div style="font-size:22px;font-weight:900;color:#f8fafc;">{stock_label}</div>
            <div style="font-size:12px;color:#cbd5e1;margin-top:4px;">市場：{market_type}｜更新時間：{update_time or '—'}</div>
        </div>
        """
    )


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
        title="行情查詢｜股神版",
        subtitle="單股即時資訊、訊號燈號、支撐壓力、最近事件摘要，一頁快速看懂。",
    )

    render_pro_section("快速搜尋股票")
    s1, s2 = st.columns([5, 1])

    with s1:
        st.text_input(
            "輸入股票代碼或名稱",
            key=_k("search_input"),
            placeholder="例如：2330、台積電、3548 兆利",
            label_visibility="collapsed",
        )

    with s2:
        if st.button("帶入", use_container_width=True, type="primary"):
            target = _find_search_target(st.session_state.get(_k("search_input"), ""), flat_rows)
            if target:
                st.session_state[_k("group")] = target["group"]
                st.session_state[_k("stock_code")] = target["code"]
                save_last_query_state(
                    quick_group=target["group"],
                    quick_stock_code=target["code"],
                    home_start=st.session_state.get(_k("start_date")),
                    home_end=st.session_state.get(_k("end_date")),
                )
                st.rerun()
            else:
                st.warning("找不到對應股票。")

    render_pro_section("查詢條件")
    _repair_state(group_map)

    groups = list(group_map.keys())
    current_group = _safe_str(st.session_state.get(_k("group"), ""))
    items = group_map.get(current_group, [])
    code_to_item = {x["code"]: x for x in items}
    code_options = [x["code"] for x in items]

    c1, c2 = st.columns([2, 3])
    with c1:
        st.selectbox("選擇群組", options=groups, key=_k("group"), on_change=_on_group_change, args=(group_map,))
    with c2:
        st.selectbox(
            "群組股票",
            options=code_options if code_options else [""],
            key=_k("stock_code"),
            format_func=lambda code: code_to_item.get(code, {}).get("label", code),
        )

    selected_group = _safe_str(st.session_state.get(_k("group"), ""))
    selected_code = _safe_str(st.session_state.get(_k("stock_code"), ""))

    if not selected_code or selected_code not in code_to_item:
        st.warning("請先選擇股票。")
        st.stop()

    selected_item = code_to_item[selected_code]
    stock_name = _safe_str(selected_item.get("name"))
    market_type = _safe_str(selected_item.get("market")) or "上市"
    stock_label = f"{selected_code} {stock_name}"

    save_last_query_state(
        quick_group=selected_group,
        quick_stock_code=selected_code,
        home_start=st.session_state.get(_k("start_date")),
        home_end=st.session_state.get(_k("end_date")),
    )

    with st.spinner("載入即時資料中..."):
        all_code_name_df = get_all_code_name_map("")
        stock_name2, market_type2 = get_stock_name_and_market(selected_code, all_code_name_df, stock_name)
        info = get_realtime_stock_info(selected_code, stock_name2 or stock_name, market_type2 or market_type)

    start_date = date.today() - timedelta(days=180)
    end_date = date.today()

    history_df = _get_history_data_smart(
        stock_no=selected_code,
        stock_name=stock_name,
        market_type=market_type,
        start_date=start_date,
        end_date=end_date,
    )

    signal_snapshot = compute_signal_snapshot(history_df) if not history_df.empty else {}
    sr_snapshot = compute_support_resistance_snapshot(history_df) if not history_df.empty else {}
    badge_text, _ = score_to_badge(signal_snapshot.get("score", 0)) if signal_snapshot else ("整理", "pro-flat")
    recent_events = _build_recent_event_summary(history_df)

    _render_realtime_hero(info, stock_label, market_type)

    left, right = st.columns([1.15, 1.85])

    with left:
        render_pro_info_card(
            "訊號燈號",
            [
                ("燈號", badge_text, ""),
                ("均線趨勢", _safe_str(signal_snapshot.get("ma_trend", ("—", ""))[0]), ""),
                ("KD交叉", _safe_str(signal_snapshot.get("kd_cross", ("—", ""))[0]), ""),
                ("MACD趨勢", _safe_str(signal_snapshot.get("macd_trend", ("—", ""))[0]), ""),
                ("價位狀態", _safe_str(signal_snapshot.get("price_vs_ma20", ("—", ""))[0]), ""),
                ("量能狀態", _safe_str(signal_snapshot.get("volume_state", ("—", ""))[0]), ""),
            ],
            chips=[badge_text],
        )

        render_pro_info_card(
            "最近事件摘要",
            recent_events,
            chips=[market_type],
        )

    with right:
        render_pro_info_card(
            "支撐壓力",
            [
                ("20日壓力", format_number(sr_snapshot.get("res_20"), 2), ""),
                ("20日支撐", format_number(sr_snapshot.get("sup_20"), 2), ""),
                ("60日壓力", format_number(sr_snapshot.get("res_60"), 2), ""),
                ("60日支撐", format_number(sr_snapshot.get("sup_60"), 2), ""),
                ("壓力訊號", _safe_str(sr_snapshot.get("pressure_signal", ("—", ""))[0]), ""),
                ("支撐訊號", _safe_str(sr_snapshot.get("support_signal", ("—", ""))[0]), ""),
                ("區間判斷", _safe_str(sr_snapshot.get("break_signal", ("—", ""))[0]), ""),
            ],
            chips=["結構位階"],
        )

        render_pro_info_card(
            "股神快速判讀",
            [
                ("目前結論", _safe_str(signal_snapshot.get("comment", "資料不足")), ""),
                ("趨勢觀察", _safe_str(sr_snapshot.get("comment_trend", "資料不足")), ""),
                ("風險提醒", _safe_str(sr_snapshot.get("comment_risk", "資料不足")), ""),
                ("焦點重點", _safe_str(sr_snapshot.get("comment_focus", "資料不足")), ""),
                ("操作提醒", _safe_str(sr_snapshot.get("comment_action", "資料不足")), ""),
            ],
            chips=[badge_text, market_type],
        )

    with st.expander("原始即時資料"):
        raw_df = pd.DataFrame(
            [
                {
                    "股票代號": selected_code,
                    "股票名稱": stock_name,
                    "市場別": market_type,
                    "現價": info.get("price"),
                    "昨收": info.get("prev_close"),
                    "開盤": info.get("open"),
                    "最高": info.get("high"),
                    "最低": info.get("low"),
                    "漲跌": info.get("change"),
                    "漲跌幅(%)": info.get("change_pct"),
                    "總量": info.get("total_volume"),
                    "單量": info.get("trade_volume"),
                    "更新時間": info.get("update_time"),
                    "是否成功": info.get("ok"),
                    "訊息": info.get("message"),
                }
            ]
        )
        st.dataframe(raw_df, use_container_width=True, hide_index=True)

    with st.expander("效能說明"):
        st.write("1. 即時資料與歷史資料分開處理。")
        st.write("2. 歷史資料用 cache，避免每次重抓。")
        st.write("3. 上櫃股票會自動嘗試 fallback。")
        st.write("4. 群組與股票選擇以 session_state 真同步。")


if __name__ == "__main__":
    main()

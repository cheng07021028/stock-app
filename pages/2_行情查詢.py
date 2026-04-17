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
PFX = "quote_"


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
                for _, row in all_df.head(100).iterrows():
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
    today = date.today()
    default_start = today - timedelta(days=180)
    default_end = today

    groups = list(group_map.keys())

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

    st.session_state[_k("start_date")] = _to_date(st.session_state.get(_k("start_date")), default_start)
    st.session_state[_k("end_date")] = _to_date(st.session_state.get(_k("end_date")), default_end)

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
# 歷史資料：上市 + 上櫃 fallback
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

    df["漲跌幅(%)"] = close.pct_change() * 100
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

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.tpex.org.tw/",
    }

    for dt in month_starts:
        roc_year = dt.year - 1911
        month = dt.month
        roc_date = f"{roc_year}/{month:02d}"

        urls = [
            "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php",
            "https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_download.php",
        ]

        ok_df = None

        # 主來源
        try:
            r = requests.get(
                urls[0],
                params={"l": "zh-tw", "d": roc_date, "stkno": stock_no},
                headers=headers,
                timeout=20,
                verify=False,
            )
            r.raise_for_status()
            data = r.json()

            if data.get("aaData"):
                fields = data.get("fields", [])
                aa_data = data.get("aaData", [])
                temp = pd.DataFrame(aa_data, columns=fields if fields and len(fields) == len(aa_data[0]) else None)
                ok_df = temp
        except Exception:
            ok_df = None

        if ok_df is None or ok_df.empty:
            continue

        frames.append(ok_df)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # 上櫃常見欄位名稱轉換
    rename_map = {}
    for col in df.columns:
        col_s = _safe_str(col)
        if col_s in ["日期", "日 期"]:
            rename_map[col] = "日期"
        elif "成交仟股" in col_s or "成交股數" in col_s:
            rename_map[col] = "成交股數"
        elif "成交仟元" in col_s or "成交金額" in col_s:
            rename_map[col] = "成交金額"
        elif "開盤" in col_s:
            rename_map[col] = "開盤價"
        elif "最高" in col_s:
            rename_map[col] = "最高價"
        elif "最低" in col_s:
            rename_map[col] = "最低價"
        elif "收盤" in col_s:
            rename_map[col] = "收盤價"
        elif "成交筆數" in col_s:
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
                    year = int(parts[0]) + 1911
                    month = int(parts[1])
                    day = int(parts[2])
                    return pd.Timestamp(year=year, month=month, day=day)
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

    # 有些上櫃成交量是仟股，換成股數
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


@st.cache_data(ttl=1800, show_spinner=False)
def _get_history_data_smart(stock_no: str, stock_name: str, market_type: str, start_date: date, end_date: date) -> pd.DataFrame:
    # 先走原本 utils
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

    # 上櫃 / 興櫃 fallback
    if _safe_str(market_type) in ["上櫃", "興櫃"]:
        df2 = _get_tpex_history_data(stock_no, start_date, end_date)
        df2 = _prepare_history_df(df2)
        if not df2.empty:
            return df2

    return pd.DataFrame()


# =========================================================
# 最近事件摘要
# =========================================================
def _build_recent_events(df: pd.DataFrame) -> list[dict[str, str]]:
    events = []
    if df is None or df.empty or len(df) < 3:
        return events

    last = df.iloc[-1]
    prev = df.iloc[-2]

    if all(c in df.columns for c in ["MA5", "MA10"]):
        if pd.notna(prev["MA5"]) and pd.notna(prev["MA10"]) and pd.notna(last["MA5"]) and pd.notna(last["MA10"]):
            if prev["MA5"] <= prev["MA10"] and last["MA5"] > last["MA10"]:
                events.append({"事件": "MA黃金交叉", "說明": "MA5 上穿 MA10，短線轉強。"})
            elif prev["MA5"] >= prev["MA10"] and last["MA5"] < last["MA10"]:
                events.append({"事件": "MA死亡交叉", "說明": "MA5 下破 MA10，短線轉弱。"})

    if all(c in df.columns for c in ["K", "D"]):
        if pd.notna(prev["K"]) and pd.notna(prev["D"]) and pd.notna(last["K"]) and pd.notna(last["D"]):
            if prev["K"] <= prev["D"] and last["K"] > last["D"]:
                events.append({"事件": "KD黃金交叉", "說明": "KD 轉強。"})
            elif prev["K"] >= prev["D"] and last["K"] < last["D"]:
                events.append({"事件": "KD死亡交叉", "說明": "KD 轉弱。"})

    if all(c in df.columns for c in ["DIF", "DEA"]):
        if pd.notna(prev["DIF"]) and pd.notna(prev["DEA"]) and pd.notna(last["DIF"]) and pd.notna(last["DEA"]):
            if prev["DIF"] <= prev["DEA"] and last["DIF"] > last["DEA"]:
                events.append({"事件": "MACD黃金交叉", "說明": "DIF 上穿 DEA。"})
            elif prev["DIF"] >= prev["DEA"] and last["DIF"] < last["DEA"]:
                events.append({"事件": "MACD死亡交叉", "說明": "DIF 下破 DEA。"})

    if len(df) >= 20 and all(c in df.columns for c in ["最高價", "最低價", "收盤價"]):
        recent20 = df.tail(20)
        high20 = recent20["最高價"].max()
        low20 = recent20["最低價"].min()
        close_price = last["收盤價"]

        if pd.notna(close_price) and pd.notna(high20) and close_price >= high20:
            events.append({"事件": "突破20日高", "說明": "股價創 20 日新高。"})
        if pd.notna(close_price) and pd.notna(low20) and close_price <= low20:
            events.append({"事件": "跌破20日低", "說明": "股價創 20 日新低。"})

    return events[:8]


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
        subtitle="即時行情、訊號燈號、支撐壓力、最近事件、策略摘要，維持你現有專案架構。",
    )

    render_pro_section("快速搜尋股票")

    s1, s2 = st.columns([5, 1])
    with s1:
        st.text_input(
            "輸入股票代碼或名稱",
            key=_k("search_input"),
            placeholder="例如：2330、台積電、2454 聯發科、3548 兆利",
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

    c1, c2, c3, c4 = st.columns([2, 3, 2, 2])

    with c1:
        st.selectbox(
            "選擇群組",
            options=groups,
            key=_k("group"),
            on_change=_on_group_change,
            args=(group_map,),
        )

    with c2:
        st.selectbox(
            "群組股票",
            options=code_options if code_options else [""],
            key=_k("stock_code"),
            format_func=lambda code: code_to_item.get(code, {}).get("label", code),
        )

    with c3:
        st.date_input("開始日期", key=_k("start_date"))

    with c4:
        st.date_input("結束日期", key=_k("end_date"))

    selected_group = _safe_str(st.session_state.get(_k("group"), ""))
    selected_code = _safe_str(st.session_state.get(_k("stock_code"), ""))
    start_date = _to_date(st.session_state.get(_k("start_date")), date.today() - timedelta(days=180))
    end_date = _to_date(st.session_state.get(_k("end_date")), date.today())

    if start_date > end_date:
        st.error("開始日期不可大於結束日期。")
        st.stop()

    if not selected_code or selected_code not in code_to_item:
        st.warning("請先選擇股票。")
        st.stop()

    selected_item = code_to_item[selected_code]
    stock_name = _safe_str(selected_item.get("name"))
    market_type = _safe_str(selected_item.get("market")) or "上市"

    st.caption(f"目前實際查詢值：群組【{selected_group}】 / 股票【{selected_code} {stock_name}】 / 市場【{market_type}】")

    save_last_query_state(
        quick_group=selected_group,
        quick_stock_code=selected_code,
        home_start=start_date,
        home_end=end_date,
    )

    # -----------------------------------------------------
    # 即時資料
    # -----------------------------------------------------
    realtime = get_realtime_stock_info(selected_code, stock_name, market_type)

    # -----------------------------------------------------
    # 歷史資料（上櫃 fallback）
    # -----------------------------------------------------
    with st.spinner("載入股神資料中..."):
        df = _get_history_data_smart(
            stock_no=selected_code,
            stock_name=stock_name,
            market_type=market_type,
            start_date=start_date,
            end_date=end_date,
        )

    # 先顯示即時區，不要因為歷史空就整頁失敗
    render_pro_section("即時資訊")

    price = realtime.get("price")
    prev_close = realtime.get("prev_close")
    change = realtime.get("change")
    change_pct = realtime.get("change_pct")
    total_volume = realtime.get("total_volume")

    badge_text = "查無燈號"
    badge_class = "pro-flat"
    signal_snapshot = None
    sr_snapshot = None
    recent_events = []

    if not df.empty:
        signal_snapshot = compute_signal_snapshot(df)
        sr_snapshot = compute_support_resistance_snapshot(df)
        badge_text, badge_class = score_to_badge(signal_snapshot.get("score", 0))
        recent_events = _build_recent_events(df)

    render_pro_kpi_row(
        [
            {
                "label": "現價",
                "value": format_number(price, 2),
                "delta": f"{format_number(change, 2)} / {format_number(change_pct, 2)}%",
                "delta_class": "pro-kpi-delta-up" if _safe_float(change, 0) > 0 else ("pro-kpi-delta-down" if _safe_float(change, 0) < 0 else "pro-kpi-delta-flat"),
            },
            {
                "label": "昨收",
                "value": format_number(prev_close, 2),
                "delta": market_type,
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "總量",
                "value": format_number(total_volume, 0),
                "delta": "即時量",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "訊號燈號",
                "value": badge_text,
                "delta": stock_name,
                "delta_class": "pro-kpi-delta-flat",
            },
        ]
    )

    info_pairs = [
        ("股票", f"{selected_code} {stock_name}", ""),
        ("市場別", market_type, ""),
        ("開盤", format_number(realtime.get("open"), 2), ""),
        ("最高", format_number(realtime.get("high"), 2), ""),
        ("最低", format_number(realtime.get("low"), 2), ""),
        ("更新時間", _safe_str(realtime.get("update_time")) or "—", ""),
    ]
    render_pro_info_card("即時摘要", info_pairs, chips=[badge_text])

    if df.empty:
        st.warning("查無歷史資料，請更換股票或日期區間。這通常不是上櫃不能查即時，而是歷史來源不足；本頁已加上櫃 fallback，若仍無資料，多半是該區間來源回傳空值。")
        st.stop()

    # -----------------------------------------------------
    # 訊號燈號 / 支撐壓力
    # -----------------------------------------------------
    render_pro_section("訊號燈號與支撐壓力")

    signal_pairs = [
        ("均線趨勢", signal_snapshot.get("ma_trend", ("—", ""))[0], signal_snapshot.get("ma_trend", ("", "pro-flat"))[1]),
        ("KD交叉", signal_snapshot.get("kd_cross", ("—", ""))[0], signal_snapshot.get("kd_cross", ("", "pro-flat"))[1]),
        ("MACD趨勢", signal_snapshot.get("macd_trend", ("—", ""))[0], signal_snapshot.get("macd_trend", ("", "pro-flat"))[1]),
        ("價位狀態", signal_snapshot.get("price_vs_ma20", ("—", ""))[0], signal_snapshot.get("price_vs_ma20", ("", "pro-flat"))[1]),
        ("突破狀態", signal_snapshot.get("breakout_20d", ("—", ""))[0], signal_snapshot.get("breakout_20d", ("", "pro-flat"))[1]),
        ("量能狀態", signal_snapshot.get("volume_state", ("—", ""))[0], signal_snapshot.get("volume_state", ("", "pro-flat"))[1]),
    ]
    render_pro_info_card(
        "股神訊號摘要",
        signal_pairs,
        chips=[badge_text, f"分數 {signal_snapshot.get('score', 0)}"],
    )

    sr_pairs = [
        ("20日壓力", format_number(sr_snapshot.get("res_20"), 2), ""),
        ("20日支撐", format_number(sr_snapshot.get("sup_20"), 2), ""),
        ("60日壓力", format_number(sr_snapshot.get("res_60"), 2), ""),
        ("60日支撐", format_number(sr_snapshot.get("sup_60"), 2), ""),
        ("壓力訊號", sr_snapshot.get("pressure_signal", ("—", ""))[0], sr_snapshot.get("pressure_signal", ("", "pro-flat"))[1]),
        ("支撐訊號", sr_snapshot.get("support_signal", ("—", ""))[0], sr_snapshot.get("support_signal", ("", "pro-flat"))[1]),
        ("區間判斷", sr_snapshot.get("break_signal", ("—", ""))[0], sr_snapshot.get("break_signal", ("", "pro-flat"))[1]),
    ]
    render_pro_info_card("支撐壓力", sr_pairs)

    # -----------------------------------------------------
    # 最近事件
    # -----------------------------------------------------
    render_pro_section("最近事件")

    if not recent_events:
        st.info("近期沒有明確的新事件。")
    else:
        event_df = pd.DataFrame(recent_events)
        st.dataframe(event_df, use_container_width=True, hide_index=True)

    # -----------------------------------------------------
    # 策略摘要
    # -----------------------------------------------------
    render_pro_section("策略摘要")

    strategy_pairs = [
        ("多空評語", signal_snapshot.get("comment", "—"), ""),
        ("趨勢解讀", sr_snapshot.get("comment_trend", "—"), ""),
        ("風險提醒", sr_snapshot.get("comment_risk", "—"), ""),
        ("觀察重點", sr_snapshot.get("comment_focus", "—"), ""),
        ("操作建議", sr_snapshot.get("comment_action", "—"), ""),
    ]
    render_pro_info_card("股神策略摘要", strategy_pairs, chips=[badge_text, market_type])

    # -----------------------------------------------------
    # 加速資訊
    # -----------------------------------------------------
    with st.expander("效能說明"):
        st.write("這版已做快取與上櫃歷史 fallback，會比原本少很多重複查詢。")
        st.write("若你還要更快，下一步我建議把 3_歷史K線分析.py 也一起改成共用這套歷史抓取邏輯。")


if __name__ == "__main__":
    main()

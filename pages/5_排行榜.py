from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st

from utils import (
    compute_radar_scores,
    compute_signal_snapshot,
    compute_support_resistance_snapshot,
    format_number,
    get_all_code_name_map,
    get_history_data,
    get_normalized_watchlist,
    inject_pro_theme,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
)

PAGE_TITLE = "排行榜"
PFX = "rank_"


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


def _normalize_radar_result(radar: Any) -> dict[str, Any]:
    if isinstance(radar, dict):
        return radar

    if isinstance(radar, tuple):
        # 相容舊版 utils：可能回傳 (summary, score) 或其他 tuple
        if len(radar) >= 1:
            return {
                "summary": _safe_str(radar[0]) if radar[0] is not None else "—",
                "score": _safe_float(radar[1], None) if len(radar) >= 2 else None,
            }

    if isinstance(radar, list):
        if len(radar) >= 1:
            return {
                "summary": _safe_str(radar[0]) if radar[0] is not None else "—",
                "score": _safe_float(radar[1], None) if len(radar) >= 2 else None,
            }

    if radar is None:
        return {"summary": "—"}

    return {"summary": _safe_str(radar) or "—"}


def _normalize_code(v: Any) -> str:
    text = _safe_str(v)
    if not text:
        return ""
    if text.isdigit():
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if 4 <= len(digits) <= 6:
        return digits
    return text


# =========================================================
# watchlist / 主檔
# =========================================================
@st.cache_data(ttl=120, show_spinner=False)
def _build_watchlist_map_cached(raw_items: tuple) -> dict[str, list[dict[str, str]]]:
    result: dict[str, list[dict[str, str]]] = {}

    for group_name, items in raw_items:
        g = _safe_str(group_name)
        if not g:
            continue

        rows = []
        seen = set()

        for item in items:
            if not isinstance(item, tuple) or len(item) < 3:
                continue

            code = _normalize_code(item[0])
            name = _safe_str(item[1]) or code
            market = _safe_str(item[2]) or "上市"

            if not code or code in seen:
                continue

            seen.add(code)
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


def _load_watchlist_map() -> dict[str, list[dict[str, str]]]:
    raw = st.session_state.get("watchlist_data")
    if not isinstance(raw, dict) or not raw:
        raw = get_normalized_watchlist()

    packed = []
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
            packed.append((group_name, tuple(temp)))

    return _build_watchlist_map_cached(tuple(packed))


@st.cache_data(ttl=1800, show_spinner=False)
def _load_master_df() -> pd.DataFrame:
    dfs = []
    for market_arg in ["", "上市", "上櫃", "興櫃"]:
        try:
            df = get_all_code_name_map(market_arg)
            if isinstance(df, pd.DataFrame) and not df.empty:
                temp = df.copy()
                mapping = {
                    "證券代號": "code",
                    "證券名稱": "name",
                    "市場別": "market",
                    "code": "code",
                    "name": "name",
                    "market": "market",
                }
                temp = temp.rename(columns=mapping)

                for col in ["code", "name", "market"]:
                    if col not in temp.columns:
                        temp[col] = ""

                temp["code"] = temp["code"].map(_normalize_code)
                temp["name"] = temp["name"].map(_safe_str)
                temp["market"] = temp["market"].map(_safe_str)
                if market_arg in ["上市", "上櫃", "興櫃"]:
                    temp["market"] = temp["market"].replace("", market_arg)

                dfs.append(temp[["code", "name", "market"]])
        except Exception:
            pass

    if not dfs:
        return pd.DataFrame(columns=["code", "name", "market"])

    out = pd.concat(dfs, ignore_index=True)
    out["code"] = out["code"].map(_normalize_code)
    out["name"] = out["name"].map(_safe_str)
    out["market"] = out["market"].map(_safe_str)

    market_priority = {"上櫃": 3, "上市": 2, "興櫃": 1, "": 0}
    out["_priority"] = out["market"].map(lambda x: market_priority.get(_safe_str(x), 0))
    out = (
        out[out["code"] != ""]
        .sort_values(["code", "_priority"], ascending=[True, False])
        .drop_duplicates(subset=["code"], keep="first")
        .drop(columns=["_priority"])
        .reset_index(drop=True)
    )
    out["market"] = out["market"].replace("", "上市")
    return out


def _find_name_market(code: str, manual_name: str, manual_market: str, master_df: pd.DataFrame) -> tuple[str, str]:
    code = _normalize_code(code)
    manual_name = _safe_str(manual_name)
    manual_market = _safe_str(manual_market)

    if isinstance(master_df, pd.DataFrame) and not master_df.empty:
        matched = master_df[master_df["code"].astype(str) == code]
        if not matched.empty:
            row = matched.iloc[0]
            return (
                _safe_str(row.get("name")) or manual_name or code,
                _safe_str(row.get("market")) or manual_market or "上市",
            )

    return manual_name or code, manual_market or "上市"



def _prepare_history_df_rank(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    if "日期" not in work.columns:
        return pd.DataFrame()
    work["日期"] = pd.to_datetime(work["日期"], errors="coerce")
    work = work.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)

    for c in ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]:
        if c in work.columns:
            work[c] = pd.to_numeric(work[c], errors="coerce")
        else:
            work[c] = pd.NA

    if "收盤價" not in work.columns:
        return pd.DataFrame()
    work = work.dropna(subset=["收盤價"]).copy()
    return work


@st.cache_data(ttl=900, show_spinner=False)
def _get_yahoo_history_rank(stock_no: str, market_type: str, start_date: date, end_date: date) -> tuple[pd.DataFrame, str]:
    code = _normalize_code(stock_no)
    market_type = _safe_str(market_type)
    if not code:
        return pd.DataFrame(), ""

    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date)
    if pd.isna(start_ts) or pd.isna(end_ts) or end_ts < start_ts:
        return pd.DataFrame(), ""

    period1 = int((start_ts - pd.Timedelta(days=5)).timestamp())
    period2 = int((end_ts + pd.Timedelta(days=2)).timestamp())

    # 上櫃股票優先 .TWO；但若自選股市場別誤標，仍會交叉測 .TW。
    symbols = [f"{code}.TWO", f"{code}.TW"] if market_type in ["上櫃", "興櫃"] else [f"{code}.TW", f"{code}.TWO"]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
    }

    for symbol in symbols:
        for host in ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]:
            try:
                url = f"https://{host}/v8/finance/chart/{symbol}"
                r = requests.get(
                    url,
                    params={
                        "period1": period1,
                        "period2": period2,
                        "interval": "1d",
                        "events": "history",
                        "includeAdjustedClose": "true",
                    },
                    headers=headers,
                    timeout=18,
                    verify=False,
                )
                if r.status_code != 200:
                    continue
                data = r.json()
                result = (((data or {}).get("chart") or {}).get("result") or [])
                if not result:
                    continue
                item = result[0]
                timestamps = item.get("timestamp") or []
                quote = (((item.get("indicators") or {}).get("quote") or [{}])[0]) or {}
                if not timestamps or not quote:
                    continue

                rows = []
                for i, ts in enumerate(timestamps):
                    try:
                        d = pd.to_datetime(int(ts), unit="s").tz_localize("UTC").tz_convert("Asia/Taipei").tz_localize(None).normalize()
                    except Exception:
                        d = pd.to_datetime(int(ts), unit="s")

                    def pick(key):
                        arr = quote.get(key) or []
                        return arr[i] if i < len(arr) else None

                    close_v = pick("close")
                    if close_v is None:
                        continue
                    rows.append(
                        {
                            "日期": d,
                            "開盤價": pick("open"),
                            "最高價": pick("high"),
                            "最低價": pick("low"),
                            "收盤價": close_v,
                            "成交股數": pick("volume"),
                            "成交金額": pd.NA,
                            "成交筆數": pd.NA,
                        }
                    )

                df = pd.DataFrame(rows)
                df = _prepare_history_df_rank(df)
                df = df[(df["日期"] >= start_ts) & (df["日期"] <= end_ts)]
                if not df.empty:
                    df["資料源"] = f"yahoo:{symbol}"
                    return df, f"yahoo:{symbol}"
            except Exception:
                continue

    return pd.DataFrame(), ""


@st.cache_data(ttl=900, show_spinner=False)
def _get_history_from_kline_page_rank(stock_no: str, stock_name: str, market_type: str, start_date: date, end_date: date) -> tuple[pd.DataFrame, str]:
    """排行榜最後保險：橋接 3_歷史K線分析.py 的成功抓取邏輯。"""
    try:
        import importlib.util

        page3_path = Path(__file__).with_name("3_歷史K線分析.py")
        if not page3_path.exists():
            return pd.DataFrame(), ""

        spec = importlib.util.spec_from_file_location("rank_kline_bridge", str(page3_path))
        if spec is None or spec.loader is None:
            return pd.DataFrame(), ""

        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        fn = getattr(mod, "_get_history_data_smart", None)
        if fn is None:
            return pd.DataFrame(), ""

        result = fn(stock_no, stock_name, market_type, start_date, end_date)
        if isinstance(result, tuple):
            df = result[0] if len(result) >= 1 else pd.DataFrame()
            source = result[2] if len(result) >= 3 else (result[1] if len(result) >= 2 else "3頁橋接")
        else:
            df = result
            source = "3頁橋接"

        df = _prepare_history_df_rank(df)
        if not df.empty:
            df["資料源"] = f"3頁橋接:{source}"
            return df, f"3頁橋接:{source}"
    except Exception:
        pass

    return pd.DataFrame(), ""


# =========================================================
# 歷史資料抓取
# =========================================================
@st.cache_data(ttl=300, show_spinner=False)
def _get_history_smart(stock_no: str, stock_name: str, market_type: str, start_date: date, end_date: date) -> tuple[pd.DataFrame, str]:
    candidates = []
    for mk in [market_type, "上櫃", "上市", "興櫃", ""]:
        mk = _safe_str(mk)
        if mk not in candidates:
            candidates.append(mk)

    # 1) 共用 utils 原架構
    for mk in candidates:
        try:
            df = get_history_data(
                stock_no=stock_no,
                stock_name=stock_name,
                market_type=mk,
                start_date=start_date,
                end_date=end_date,
            )
        except TypeError:
            try:
                df = get_history_data(
                    stock_no=stock_no,
                    stock_name=stock_name,
                    market_type=mk,
                    start_dt=start_date,
                    end_dt=end_date,
                )
            except Exception:
                df = pd.DataFrame()
        except Exception:
            df = pd.DataFrame()

        df = _prepare_history_df_rank(df)
        if not df.empty:
            df["資料源"] = f"utils:{mk or '空'}"
            return df, (mk or market_type or "未知")

    # 2) Yahoo fallback：補 3548 兆利等上櫃股票
    df_yahoo, src = _get_yahoo_history_rank(stock_no, market_type, start_date, end_date)
    if not df_yahoo.empty:
        used_market = "上櫃" if ".TWO" in src else ("上市" if ".TW" in src else (_safe_str(market_type) or "未知"))
        return df_yahoo, used_market

    # 3) 與 3_歷史K線分析橋接，確保排行與K線頁資料一致
    df_kline, src2 = _get_history_from_kline_page_rank(stock_no, stock_name, market_type, start_date, end_date)
    if not df_kline.empty:
        used_market = "上櫃" if ".TWO" in src2 else ("上市" if ".TW" in src2 else (_safe_str(market_type) or "未知"))
        return df_kline, used_market

    return pd.DataFrame(), (_safe_str(market_type) or "未知")


@st.cache_data(ttl=300, show_spinner=False)
def _analyze_stock_row(
    code: str,
    stock_name: str,
    market_type: str,
    start_dt: date,
    end_dt: date,
) -> dict[str, Any]:
    hist_df, used_market = _get_history_smart(
        stock_no=code,
        stock_name=stock_name,
        market_type=market_type,
        start_date=start_dt,
        end_date=end_dt,
    )

    if hist_df.empty:
        return {
            "股票代號": code,
            "股票名稱": stock_name,
            "市場別": used_market,
            "日期": None,
            "最新價": None,
            "漲跌": None,
            "漲跌幅%": None,
            "成交股數": None,
            "成交金額": None,
            "成交筆數": None,
            "訊號分數": None,
            "20日壓力距離%": None,
            "20日支撐距離%": None,
            "資料源": "none",
            "雷達摘要": "無資料",
        }

    latest = hist_df.iloc[-1]
    prev_close = hist_df.iloc[-2]["收盤價"] if len(hist_df) >= 2 and "收盤價" in hist_df.columns else None
    latest_close = latest.get("收盤價")

    price_change = None
    pct_change = None
    if prev_close is not None and latest_close is not None and prev_close != 0:
        price_change = latest_close - prev_close
        pct_change = (price_change / prev_close) * 100

    signal = compute_signal_snapshot(hist_df)
    sr = compute_support_resistance_snapshot(hist_df)
    radar = _normalize_radar_result(compute_radar_scores(hist_df))

    res20 = _safe_float(sr.get("res_20"))
    sup20 = _safe_float(sr.get("sup_20"))
    close_now = _safe_float(latest_close)

    pressure_dist = None
    support_dist = None
    if close_now is not None and res20 not in [None, 0]:
        pressure_dist = ((res20 - close_now) / res20) * 100
    if close_now is not None and sup20 not in [None, 0]:
        support_dist = ((close_now - sup20) / sup20) * 100

    return {
        "股票代號": code,
        "股票名稱": stock_name,
        "市場別": used_market,
        "日期": latest.get("日期"),
        "最新價": latest_close,
        "漲跌": price_change,
        "漲跌幅%": pct_change,
        "成交股數": latest.get("成交股數"),
        "成交金額": latest.get("成交金額"),
        "成交筆數": latest.get("成交筆數"),
        "訊號分數": signal.get("score"),
        "20日壓力距離%": pressure_dist,
        "20日支撐距離%": support_dist,
        "雷達摘要": _safe_str(radar.get("summary")) or "—",
    }


# =========================================================
# 排行資料
# =========================================================
def _init_state(group_map: dict[str, list[dict[str, str]]]):
    groups = list(group_map.keys())
    if _k("group") not in st.session_state:
        st.session_state[_k("group")] = groups[0] if groups else ""

    if _k("days") not in st.session_state:
        st.session_state[_k("days")] = 40

    if _k("top_n") not in st.session_state:
        st.session_state[_k("top_n")] = 20

    if _k("sort_metric") not in st.session_state:
        st.session_state[_k("sort_metric")] = "訊號分數"

    if _k("group") in st.session_state and st.session_state[_k("group")] not in groups:
        st.session_state[_k("group")] = groups[0] if groups else ""


@st.cache_data(ttl=300, show_spinner=False)
def _build_rank_df(items_payload: tuple, master_rows_payload: tuple, start_dt: date, end_dt: date) -> pd.DataFrame:
    master_df = pd.DataFrame(master_rows_payload, columns=["code", "name", "market"]) if master_rows_payload else pd.DataFrame(columns=["code", "name", "market"])
    rows = []

    for item in items_payload:
        if not isinstance(item, tuple) or len(item) < 3:
            continue

        code = _normalize_code(item[0])
        manual_name = _safe_str(item[1])
        manual_market = _safe_str(item[2])

        if not code:
            continue

        stock_name, market_type = _find_name_market(code, manual_name, manual_market, master_df)
        row = _analyze_stock_row(
            code=code,
            stock_name=stock_name,
            market_type=market_type,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        rows.append(row)

    return pd.DataFrame(rows)


@st.cache_data(ttl=120, show_spinner=False)
def _format_table_cached(df: pd.DataFrame) -> pd.DataFrame:
    show_df = df.copy()

    if "日期" in show_df.columns:
        show_df["日期"] = pd.to_datetime(show_df["日期"], errors="coerce").dt.strftime("%Y-%m-%d")

    for col in ["最新價", "漲跌"]:
        if col in show_df.columns:
            show_df[col] = show_df[col].apply(lambda x: format_number(x, 2) if pd.notna(x) else "")

    for col in ["成交股數", "成交金額", "成交筆數", "訊號分數"]:
        if col in show_df.columns:
            show_df[col] = show_df[col].apply(lambda x: format_number(x, 0) if pd.notna(x) else "")

    for col in ["漲跌幅%", "20日壓力距離%", "20日支撐距離%"]:
        if col in show_df.columns:
            show_df[col] = show_df[col].apply(lambda x: f"{x:,.2f}%" if pd.notna(x) else "")

    return show_df


def _format_table(df: pd.DataFrame) -> pd.DataFrame:
    return _format_table_cached(df)


def _top_table(df: pd.DataFrame, metric: str, ascending: bool, top_n: int) -> pd.DataFrame:
    if metric not in df.columns:
        return pd.DataFrame()
    work = df.dropna(subset=[metric]).sort_values(metric, ascending=ascending).reset_index(drop=True)
    return work.head(top_n).copy()


# =========================================================
# 主畫面
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, page_icon="🏆", layout="wide")
    inject_pro_theme()

    group_map = _load_watchlist_map()
    if not group_map:
        st.warning("目前沒有自選股群組，請先到自選股中心建立清單。")
        st.stop()

    _init_state(group_map)
    master_df = _load_master_df()

    render_pro_hero(
        title="排行榜｜股神版",
        subtitle="依自選股群組做排行，支援漲跌幅、成交金額、成交股數、訊號分數與支撐壓力距離。",
    )

    if st.session_state.get("watchlist_version"):
        st.caption(
            f"自選股同步狀態：watchlist_version = {st.session_state.get('watchlist_version', 0)}"
            + (
                f" / 最後更新：{_safe_str(st.session_state.get('watchlist_last_saved_at', ''))}"
                if _safe_str(st.session_state.get("watchlist_last_saved_at", ""))
                else ""
            )
        )

    render_pro_section("查詢條件")

    groups = list(group_map.keys())
    c1, c2, c3 = st.columns([2, 2, 2])

    with c1:
        st.selectbox("選擇群組", groups, key=_k("group"))

    with c2:
        st.selectbox("排行指標", ["訊號分數", "漲跌幅%", "成交金額", "成交股數", "20日壓力距離%", "20日支撐距離%"], key=_k("sort_metric"))

    with c3:
        st.selectbox("區間天數", [20, 40, 60, 90, 120], key=_k("days"))

    d1, d2 = st.columns([2, 2])
    with d1:
        st.selectbox("Top N", [10, 20, 30, 50], key=_k("top_n"))
    with d2:
        selected_group = _safe_str(st.session_state.get(_k("group"), ""))
        items = group_map.get(selected_group, [])
        st.caption(f"目前群組：{selected_group} / 股票數：{len(items)}")

    items = group_map.get(_safe_str(st.session_state.get(_k("group"), "")), [])
    if not items:
        st.warning("此群組目前沒有股票。")
        st.stop()

    today_dt = date.today()
    start_dt = today_dt - timedelta(days=int(st.session_state.get(_k("days"), 40)))
    end_dt = today_dt

    items_payload = tuple(
        (
            _normalize_code(x.get("code")),
            _safe_str(x.get("name")),
            _safe_str(x.get("market")),
        )
        for x in items
    )
    master_rows_payload = tuple(
        (_safe_str(r["code"]), _safe_str(r["name"]), _safe_str(r["market"]))
        for _, r in master_df.iterrows()
    )

    with st.spinner("正在整理排行榜資料..."):
        rank_df = _build_rank_df(items_payload, master_rows_payload, start_dt, end_dt)

    if rank_df.empty:
        st.warning("查無排行資料。")
        st.stop()

    total_count = len(rank_df)
    valid_up = int(rank_df["漲跌幅%"].notna().sum()) if "漲跌幅%" in rank_df.columns else 0
    valid_amount = int(rank_df["成交金額"].notna().sum()) if "成交金額" in rank_df.columns else 0
    valid_signal = int(rank_df["訊號分數"].notna().sum()) if "訊號分數" in rank_df.columns else 0

    render_pro_kpi_row(
        [
            {"label": "群組股票數", "value": total_count, "delta": _safe_str(st.session_state.get(_k("group"), "")), "delta_class": "pro-kpi-delta-flat"},
            {"label": "可比較漲跌", "value": valid_up, "delta": "有漲跌幅資料", "delta_class": "pro-kpi-delta-flat"},
            {"label": "有成交金額", "value": valid_amount, "delta": "量價排行", "delta_class": "pro-kpi-delta-flat"},
            {"label": "有訊號分數", "value": valid_signal, "delta": "股神燈號", "delta_class": "pro-kpi-delta-flat"},
        ]
    )

    with st.expander("訊號分數怎麼看？", expanded=False):
        st.markdown(
            """
            **訊號分數是技術面綜合排序分數，不是直接買賣指令。**

            - **80 分以上｜強勢優先觀察**：均線、價位、量能、支撐壓力結構通常較完整，適合放進優先追蹤清單。
            - **60～79 分｜偏多或轉強觀察**：有部分條件轉強，但仍要看是否接近壓力、是否量價配合。
            - **40～59 分｜中性整理**：方向不明，通常等突破或回測支撐確認。
            - **20～39 分｜偏弱**：結構偏弱，除非有明確止跌或事件支撐，否則不急。
            - **0～19 分｜弱勢 / 資料不足**：不是首選；若資料源不足也可能造成低分。

            **看法建議：**
            1. 先看 `訊號分數排行` 找強勢股。
            2. 再看 `20日壓力距離%`，距離壓力太近不要追。
            3. 再看 `20日支撐距離%`，離支撐太遠代表追價風險較高。
            4. 最後回到 `2_行情查詢` 或 `3_歷史K線分析` 看買點與停損。
            """
        )

    metric = _safe_str(st.session_state.get(_k("sort_metric"), "訊號分數"))
    top_n = int(st.session_state.get(_k("top_n"), 20))

    render_pro_section("重點排行")
    t1, t2 = st.columns(2)

    with t1:
        if metric in ["20日壓力距離%"]:
            top_df = _top_table(rank_df, metric, True, top_n)
        else:
            top_df = _top_table(rank_df, metric, False, top_n)
        st.dataframe(_format_table(top_df), use_container_width=True, hide_index=True)

    with t2:
        weak_metric = "漲跌幅%"
        weak_df = _top_table(rank_df, weak_metric, True, top_n)
        st.dataframe(_format_table(weak_df), use_container_width=True, hide_index=True)

    tabs = st.tabs(["漲幅排行", "跌幅排行", "成交金額排行", "成交股數排行", "訊號分數排行", "完整排行表"])

    with tabs[0]:
        up_df = _top_table(rank_df, "漲跌幅%", False, top_n)
        st.dataframe(_format_table(up_df), use_container_width=True, hide_index=True)

    with tabs[1]:
        down_df = _top_table(rank_df, "漲跌幅%", True, top_n)
        st.dataframe(_format_table(down_df), use_container_width=True, hide_index=True)

    with tabs[2]:
        amount_df = _top_table(rank_df, "成交金額", False, top_n)
        st.dataframe(_format_table(amount_df), use_container_width=True, hide_index=True)

    with tabs[3]:
        volume_df = _top_table(rank_df, "成交股數", False, top_n)
        st.dataframe(_format_table(volume_df), use_container_width=True, hide_index=True)

    with tabs[4]:
        signal_df = _top_table(rank_df, "訊號分數", False, top_n)
        st.dataframe(_format_table(signal_df), use_container_width=True, hide_index=True)

    with tabs[5]:
        st.dataframe(_format_table(rank_df), use_container_width=True, hide_index=True)

    render_pro_section("排行榜說明")
    render_pro_info_card(
        "股神版排行規則",
        [
            ("資料來源", "以自選股群組為主，逐檔抓取歷史資料後計算。", ""),
            ("訊號分數", "綜合均線、KD/MACD、價位結構、支撐壓力與雷達摘要；分數越高代表技術條件越完整。", ""),
            ("80分以上", "強勢優先觀察，但仍需確認是否過度接近壓力或短線過熱。", ""),
            ("60～79分", "偏多或轉強觀察，適合等待突破或拉回不破。", ""),
            ("40～59分", "中性整理，不急著追，等方向更明確。", ""),
            ("40分以下", "偏弱或資料不足，通常不是優先標的。", ""),
            ("排行指標", "支援漲跌幅、成交金額、成交股數、訊號分數、20日壓力/支撐距離。", ""),
            ("市場 fallback", "個股市場別會以主檔優先，不完全相信自選股內手填市場；必要時用 Yahoo / 3頁橋接。", ""),
            ("同步設計", "若自選股中心已更新，這頁會優先吃 session_state 內的最新 watchlist。", ""),
        ],
        chips=["排行榜", "自選群組", "訊號分數", "股神版"],
    )


if __name__ == "__main__":
    main()

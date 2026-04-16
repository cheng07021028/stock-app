import time


def _safe_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if text in ["", "-", "--", "—", "null", "None"]:
        return ""
    return text


def _safe_num(value):
    text = _safe_text(value)
    if not text:
        return None
    return to_number(text)


def _market_prefix(market_type: str):
    return "tse" if str(market_type).strip() == "上市" else "otc"


@st.cache_data(ttl=15, show_spinner=False)
def get_realtime_stock_info(stock_no: str, stock_name: str = "", market_type: str = "上市") -> dict:
    stock_no = str(stock_no).strip()
    stock_name = str(stock_name).strip()
    market_type = str(market_type).strip() or "上市"

    if not stock_no:
        return {
            "ok": False,
            "code": "",
            "name": stock_name,
            "market": market_type,
            "message": "股票代號為空白",
        }

    ex_ch = f"{_market_prefix(market_type)}_{stock_no}.tw"
    url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://mis.twse.com.tw/stock/",
        "Accept": "application/json,text/plain,*/*",
    }
    params = {
        "ex_ch": ex_ch,
        "json": "1",
        "delay": "0",
        "_": str(int(time.time() * 1000)),
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=20, verify=False)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {
            "ok": False,
            "code": stock_no,
            "name": stock_name,
            "market": market_type,
            "message": f"即時資料取得失敗：{e}",
        }

    msg_array = data.get("msgArray", [])
    if not msg_array:
        return {
            "ok": False,
            "code": stock_no,
            "name": stock_name,
            "market": market_type,
            "message": "查無即時資料",
        }

    raw = msg_array[0]

    code = _safe_text(raw.get("c")) or stock_no
    name = _safe_text(raw.get("n")) or stock_name or f"股票{stock_no}"

    prev_close = _safe_num(raw.get("y"))
    current_price = _safe_num(raw.get("z"))

    if current_price is None:
        current_price = prev_close

    open_price = _safe_num(raw.get("o"))
    high_price = _safe_num(raw.get("h"))
    low_price = _safe_num(raw.get("l"))
    total_volume = _safe_num(raw.get("v"))
    trade_volume = _safe_num(raw.get("tv"))

    change_value = None
    change_pct = None
    if current_price is not None and prev_close not in [None, 0]:
        change_value = current_price - prev_close
        change_pct = (change_value / prev_close) * 100

    update_date = _safe_text(raw.get("d"))
    update_time = _safe_text(raw.get("t"))
    update_text = ""
    if update_date and update_time:
        update_text = f"{update_date} {update_time}"
    elif update_time:
        update_text = update_time

    return {
        "ok": True,
        "code": code,
        "name": name,
        "market": market_type,
        "price": current_price,
        "prev_close": prev_close,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "change": change_value,
        "change_pct": change_pct,
        "total_volume": total_volume,
        "trade_volume": trade_volume,
        "update_time": update_text,
        "raw": raw,
        "message": "",
    }


@st.cache_data(ttl=15, show_spinner=False)
def get_realtime_watchlist_df(watchlist_dict: dict, query_date: str = "") -> pd.DataFrame:
    all_code_name_df = get_all_code_name_map(query_date)
    rows = []

    for group_name, items in watchlist_dict.items():
        for item in items:
            code = str(item.get("code", "")).strip()
            manual_name = str(item.get("name", "")).strip()

            if not code:
                continue

            stock_name, market_type = get_stock_name_and_market(code, all_code_name_df, manual_name)
            info = get_realtime_stock_info(code, stock_name, market_type)

            rows.append({
                "群組": group_name,
                "股票代號": code,
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
                "是否成功": info.get("ok", False),
                "訊息": info.get("message", ""),
            })

    df = pd.DataFrame(rows)

    if not df.empty:
        for col in ["現價", "昨收", "開盤", "最高", "最低", "漲跌", "漲跌幅(%)", "總量", "單量"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def render_realtime_info_card(info: dict, title: str = "即時資訊"):
    if not info:
        st.info("目前沒有即時資訊。")
        return

    if not info.get("ok"):
        st.warning(info.get("message", "查無即時資訊"))
        return

    name = info.get("name", "")
    code = info.get("code", "")
    market = info.get("market", "")
    price = info.get("price")
    open_price = info.get("open")
    high_price = info.get("high")
    low_price = info.get("low")
    change = info.get("change")
    change_pct = info.get("change_pct")
    total_volume = info.get("total_volume")
    update_time = info.get("update_time", "")

    st.markdown(f"### {title}")
    st.caption(f"{name}（{code}）｜{market}｜更新時間：{update_time or '—'}")

    c1, c2, c3, c4 = st.columns(4)

    delta_text = None
    if change is not None and change_pct is not None:
        delta_text = f"{change:+.2f} ({change_pct:+.2f}%)"
    elif change is not None:
        delta_text = f"{change:+.2f}"

    with c1:
        st.metric("現價", format_number(price, 2) if price is not None else "—", delta=delta_text)
    with c2:
        st.metric("開盤", format_number(open_price, 2) if open_price is not None else "—")
    with c3:
        st.metric("最高", format_number(high_price, 2) if high_price is not None else "—")
    with c4:
        st.metric("最低", format_number(low_price, 2) if low_price is not None else "—")

    c5, c6 = st.columns(2)
    with c5:
        st.metric("總量", format_number(total_volume, 0) if total_volume is not None else "—")
    with c6:
        st.metric("昨收", format_number(info.get("prev_close"), 2) if info.get("prev_close") is not None else "—")


def render_realtime_table(df: pd.DataFrame, height: int = 520):
    if df is None or df.empty:
        st.info("目前沒有即時資料。")
        return

    show_cols = [
        "群組", "股票代號", "股票名稱", "市場別",
        "現價", "漲跌", "漲跌幅(%)",
        "開盤", "最高", "最低", "總量", "更新時間"
    ]
    show_cols = [c for c in show_cols if c in df.columns]
    display_df = df[show_cols].copy()

    format_dict = {}
    for col in ["現價", "漲跌", "漲跌幅(%)", "開盤", "最高", "最低"]:
        if col in display_df.columns:
            format_dict[col] = "{:,.2f}"
    if "總量" in display_df.columns:
        format_dict["總量"] = "{:,.0f}"

    def color_change(val):
        if pd.isna(val):
            return ""
        try:
            v = float(val)
        except Exception:
            return ""
        if v > 0:
            return "color: #d32f2f; font-weight: 700;"
        elif v < 0:
            return "color: #00897b; font-weight: 700;"
        return "color: #666;"

    styler = display_df.style.format(format_dict, na_rep="—")

    if "漲跌" in display_df.columns:
        styler = styler.map(color_change, subset=["漲跌"])
    if "漲跌幅(%)" in display_df.columns:
        styler = styler.map(color_change, subset=["漲跌幅(%)"])

    st.dataframe(styler, use_container_width=True, hide_index=True, height=height)

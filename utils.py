def classify_signal(value, positive_text="偏多", negative_text="偏空", neutral_text="中性"):
    if value is True:
        return positive_text, "pro-up"
    if value is False:
        return negative_text, "pro-down"
    return neutral_text, "pro-flat"


def compute_signal_snapshot(df: pd.DataFrame) -> dict:
    result = {
        "ma_trend": ("中性", "pro-flat"),
        "kd_cross": ("中性", "pro-flat"),
        "macd_trend": ("中性", "pro-flat"),
        "price_vs_ma20": ("中性", "pro-flat"),
        "breakout_20d": ("中性", "pro-flat"),
        "volume_state": ("中性", "pro-flat"),
        "score": 0,
        "comment": "資料不足",
    }

    if df is None or df.empty or "收盤價" not in df.columns:
        return result

    last = df.iloc[-1]

    score = 0

    # 1. MA 趨勢
    ma_cols = ["MA5", "MA10", "MA20"]
    if all(col in df.columns for col in ma_cols):
        ma5 = last["MA5"]
        ma10 = last["MA10"]
        ma20 = last["MA20"]

        if pd.notna(ma5) and pd.notna(ma10) and pd.notna(ma20):
            if ma5 > ma10 > ma20:
                result["ma_trend"] = ("多頭排列", "pro-up")
                score += 2
            elif ma5 < ma10 < ma20:
                result["ma_trend"] = ("空頭排列", "pro-down")
                score -= 2
            else:
                result["ma_trend"] = ("均線糾結", "pro-flat")

    # 2. KD 交叉
    if all(col in df.columns for col in ["K", "D"]) and len(df) >= 2:
        prev = df.iloc[-2]
        if pd.notna(prev["K"]) and pd.notna(prev["D"]) and pd.notna(last["K"]) and pd.notna(last["D"]):
            if prev["K"] <= prev["D"] and last["K"] > last["D"]:
                result["kd_cross"] = ("黃金交叉", "pro-up")
                score += 1
            elif prev["K"] >= prev["D"] and last["K"] < last["D"]:
                result["kd_cross"] = ("死亡交叉", "pro-down")
                score -= 1
            else:
                result["kd_cross"] = ("無新交叉", "pro-flat")

    # 3. MACD
    if all(col in df.columns for col in ["DIF", "DEA", "MACD_HIST"]):
        dif = last["DIF"]
        dea = last["DEA"]
        hist = last["MACD_HIST"]

        if pd.notna(dif) and pd.notna(dea) and pd.notna(hist):
            if dif > dea and hist > 0:
                result["macd_trend"] = ("MACD翻多", "pro-up")
                score += 1
            elif dif < dea and hist < 0:
                result["macd_trend"] = ("MACD翻空", "pro-down")
                score -= 1
            else:
                result["macd_trend"] = ("MACD整理", "pro-flat")

    # 4. 價格相對 MA20
    if "MA20" in df.columns and pd.notna(last.get("MA20", None)):
        close_price = last["收盤價"]
        ma20 = last["MA20"]
        if pd.notna(close_price) and pd.notna(ma20):
            if close_price > ma20:
                result["price_vs_ma20"] = ("站上MA20", "pro-up")
                score += 1
            elif close_price < ma20:
                result["price_vs_ma20"] = ("跌破MA20", "pro-down")
                score -= 1
            else:
                result["price_vs_ma20"] = ("貼近MA20", "pro-flat")

    # 5. 近20日突破
    if len(df) >= 20 and all(col in df.columns for col in ["最高價", "最低價", "收盤價"]):
        recent_20 = df.tail(20)
        max_20 = recent_20["最高價"].max()
        min_20 = recent_20["最低價"].min()
        close_price = last["收盤價"]

        if pd.notna(close_price) and pd.notna(max_20) and pd.notna(min_20):
            if close_price >= max_20:
                result["breakout_20d"] = ("突破20日高", "pro-up")
                score += 1
            elif close_price <= min_20:
                result["breakout_20d"] = ("跌破20日低", "pro-down")
                score -= 1
            else:
                result["breakout_20d"] = ("區間內震盪", "pro-flat")

    # 6. 量能
    if "成交股數" in df.columns and len(df) >= 5:
        vol5 = df["成交股數"].tail(5).mean()
        last_vol = last["成交股數"]
        if pd.notna(vol5) and pd.notna(last_vol):
            if last_vol > vol5 * 1.3:
                result["volume_state"] = ("量能放大", "pro-up")
                score += 1
            elif last_vol < vol5 * 0.7:
                result["volume_state"] = ("量能偏弱", "pro-down")
                score -= 1
            else:
                result["volume_state"] = ("量能平穩", "pro-flat")

    result["score"] = score

    if score >= 4:
        result["comment"] = "多方優勢明確，可偏多看待，但仍需留意追價風險。"
    elif score >= 2:
        result["comment"] = "短線偏多，適合觀察是否延續攻擊。"
    elif score <= -4:
        result["comment"] = "空方壓力明確，宜保守應對，避免逆勢承接。"
    elif score <= -2:
        result["comment"] = "短線偏弱，建議先觀察止跌與量價是否改善。"
    else:
        result["comment"] = "多空訊號混合，暫屬整理盤，適合等待方向更明確。"

    return result


def score_to_badge(score: int):
    if score >= 4:
        return "強多", "pro-up"
    if score >= 2:
        return "偏多", "pro-up"
    if score <= -4:
        return "強空", "pro-down"
    if score <= -2:
        return "偏空", "pro-down"
    return "整理", "pro-flat"

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
def compute_support_resistance_snapshot(df: pd.DataFrame) -> dict:
    result = {
        "res_20": None,
        "sup_20": None,
        "res_60": None,
        "sup_60": None,
        "dist_res_20_pct": None,
        "dist_sup_20_pct": None,
        "dist_res_60_pct": None,
        "dist_sup_60_pct": None,
        "pressure_signal": ("中性", "pro-flat"),
        "support_signal": ("中性", "pro-flat"),
        "break_signal": ("區間內", "pro-flat"),
        "comment_trend": "資料不足",
        "comment_risk": "資料不足",
        "comment_focus": "資料不足",
        "comment_action": "資料不足",
    }

    if df is None or df.empty or "收盤價" not in df.columns:
        return result

    last = df.iloc[-1]
    close_price = last["收盤價"]

    if pd.isna(close_price):
        return result

    if len(df) >= 20 and all(col in df.columns for col in ["最高價", "最低價"]):
        df20 = df.tail(20)
        res_20 = df20["最高價"].max()
        sup_20 = df20["最低價"].min()
        result["res_20"] = res_20
        result["sup_20"] = sup_20

        if pd.notna(res_20) and res_20 != 0:
            result["dist_res_20_pct"] = (res_20 - close_price) / res_20 * 100
        if pd.notna(sup_20) and sup_20 != 0:
            result["dist_sup_20_pct"] = (close_price - sup_20) / sup_20 * 100

    if len(df) >= 60 and all(col in df.columns for col in ["最高價", "最低價"]):
        df60 = df.tail(60)
        res_60 = df60["最高價"].max()
        sup_60 = df60["最低價"].min()
        result["res_60"] = res_60
        result["sup_60"] = sup_60

        if pd.notna(res_60) and res_60 != 0:
            result["dist_res_60_pct"] = (res_60 - close_price) / res_60 * 100
        if pd.notna(sup_60) and sup_60 != 0:
            result["dist_sup_60_pct"] = (close_price - sup_60) / sup_60 * 100

    # 接近壓力 / 支撐
    dist_res_20_pct = result["dist_res_20_pct"]
    dist_sup_20_pct = result["dist_sup_20_pct"]

    if dist_res_20_pct is not None:
        if dist_res_20_pct < 1.5 and dist_res_20_pct >= 0:
            result["pressure_signal"] = ("接近20日壓力", "pro-down")
        elif dist_res_20_pct < 0:
            result["pressure_signal"] = ("突破20日壓力", "pro-up")
        else:
            result["pressure_signal"] = ("壓力尚遠", "pro-flat")

    if dist_sup_20_pct is not None:
        if dist_sup_20_pct < 1.5 and dist_sup_20_pct >= 0:
            result["support_signal"] = ("接近20日支撐", "pro-up")
        elif dist_sup_20_pct < 0:
            result["support_signal"] = ("跌破20日支撐", "pro-down")
        else:
            result["support_signal"] = ("支撐尚遠", "pro-flat")

    # 突破 / 跌破判定
    if result["res_20"] is not None and close_price > result["res_20"]:
        result["break_signal"] = ("有效突破20日壓力", "pro-up")
    elif result["sup_20"] is not None and close_price < result["sup_20"]:
        result["break_signal"] = ("跌破20日支撐", "pro-down")
    else:
        result["break_signal"] = ("區間內整理", "pro-flat")

    # 評語
    pressure_text = result["pressure_signal"][0]
    support_text = result["support_signal"][0]
    break_text = result["break_signal"][0]

    if "突破" in break_text:
        result["comment_trend"] = "股價已進入突破型態，短線趨勢偏強。"
    elif "跌破" in break_text:
        result["comment_trend"] = "股價已跌破重要區間，短線結構轉弱。"
    else:
        result["comment_trend"] = "目前仍在區間結構內，趨勢等待進一步表態。"

    if "接近20日壓力" in pressure_text:
        result["comment_risk"] = "現價接近短壓區，追價風險升高。"
    elif "接近20日支撐" in support_text:
        result["comment_risk"] = "現價接近短撐區，需觀察是否有防守買盤。"
    elif "跌破20日支撐" in support_text:
        result["comment_risk"] = "支撐失守，若無量價回穩，弱勢可能延續。"
    else:
        result["comment_risk"] = "目前壓力與支撐距離適中，風險屬中性。"

    if result["dist_res_20_pct"] is not None and result["dist_sup_20_pct"] is not None:
        result["comment_focus"] = (
            f"短壓約在 {format_number(result['res_20'], 2)}，"
            f"短撐約在 {format_number(result['sup_20'], 2)}，"
            "建議觀察價格是否帶量突破壓力，或回測支撐是否止穩。"
        )
    else:
        result["comment_focus"] = "觀察近期高低點位置與量能是否同步放大。"

    if "有效突破" in break_text:
        result["comment_action"] = "可偏多思考，但應防假突破，宜搭配量能確認。"
    elif "跌破20日支撐" in break_text:
        result["comment_action"] = "宜保守應對，等待止跌訊號或重新站回支撐區。"
    elif "接近20日支撐" in support_text:
        result["comment_action"] = "可觀察支撐區反應，不宜在未止穩前過度預設反彈。"
    elif "接近20日壓力" in pressure_text:
        result["comment_action"] = "若無法放量突破壓力，短線宜防拉回。"
    else:
        result["comment_action"] = "可先觀察，等待價格脫離區間後再提高部位判斷。"

    return result

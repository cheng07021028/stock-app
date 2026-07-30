# -*- coding: utf-8 -*-
"""Phase 6.3 formal recommendation purifier.

目的：把「推薦候選 / 雷達 / 回放 / 排除」重新分成可操作清單，避免
完整推薦表裡的 D、弱勢觀察、高風險雷達被使用者誤解為正式買進推薦。

本模組只處理 DataFrame 欄位，不讀寫 JSON、不連網、不覆蓋既有欄位。
"""
from __future__ import annotations

from typing import Any
import pandas as pd

try:
    from godpick_recommendation_rotation import apply_recommendation_rotation_guard
except Exception:
    apply_recommendation_rotation_guard = None

FORMAL_RECOMMENDATION_VERSION = "vnext_phase10_6_post_crash_cooldown_guard_20260730"

FORMAL_RECOMMENDATION_COLUMNS = [
    "最終操作結論",
    "是否正式推薦",
    "操作許可",
    "正式推薦等級",
    "推薦可信度分",
    "股神推薦優先分",
    "股神推薦總排名",
    "股神推薦等級",
    "股神推薦用途",
    "股神推薦分數說明",
    "主流主升優先分",
    "主流主升判定",
    "主流主升操作限制",
    "隔日觸發品質分",
    "隔日觸發品質判定",
    "隔日有效風控距離%",
    "隔日風控基準",
    "紅燈觸發脆弱度分",
    "紅燈觸發管制",
    "盤中二段確認要求",
    "實戰操作品質分",
    "資料完整度評分",
    "建議倉位上限%",
    "風控否決旗標",
    "決策一致性",
    "候選性質",
    "可操作分",
    "正式推薦分區",
    "正式推薦資格",
    "正式推薦動作",
    "下週是否可直接買",
    "準主推薦等級",
    "股神作戰區",
    "股神作戰優先序",
    "股神作戰提示",
    "主要依據工作表",
    "盤中雷達等級",
    "盤中雷達動作",
    "盤中雷達優先級",
    "盤中盯盤順序",
    "盤中雷達分層",
    "盤中雷達分層說明",
    "核心雷達品質檢查",
    "核心雷達降級原因",
    "正式推薦排除原因",
    "正式推薦排序分",
    "原始觸發價",
    "實戰觸發價",
    "觸發價偏離%",
    "觸發價修正原因",
    "隔日雷達回測判斷",
    "股神觸發修正建議",
    "觸發後守價",
    "盤中觸發確認條件",
    "開盤跳空處理",
    "隔日命中修正標籤",
    "高風險雷達保留原因",
    "正式推薦判定來源",
    "流動性參考成交額百萬",
    "隔日可參考分",
    "隔日優勢型態",
    "隔日風險標記",
    "隔日參考判定",
    "觸發距離%",
    "停損距離_隔日%",
    "進場可執行分",
    "進場可執行判定",
    "進場路徑",
    "距最近可執行買點%",
    "進場阻擋原因",
    "主要進場路徑",
    "主要進場參考價",
    "回測承接參考價",
    "突破確認參考價",
    "守價回測參考價",
    "守價回測距離%",
    "隔日耗竭風險分",
    "隔日耗竭風險等級",
    "隔日可執行優先分",
    "進場績效計算口徑",
    "推薦升級判定路徑",
    "路徑風險報酬比",
    "風報比計算口徑",
    "正式與A近門檻說明",
    "強勢動能分",
    "強勢動能判定",
    "動能進場條件",
    "動能風險控制",
    "強勢前兆分",
    "強勢前兆判定",
    "強勢前兆進場條件",
    "強勢前兆風控",
    "紅燈逆勢反轉分",
    "紅燈逆勢反轉判定",
    "恐慌反彈領漲分",
    "恐慌反彈領漲判定",
    "大盤風控層級",
    "官方因子資料日期",
    "官方因子落後交易日",
    "官方因子新鮮度",
    "大盤資料日期",
    "大盤資料落後交易日",
    "大盤資料新鮮度",
    "大盤與K線對齊狀態",
    "股神資料總新鮮度",
    "股神資料警示",
    "紅燈反轉首觸禁買",
    "主流強勢替代進場",
    "大盤條件覆寫",
    "逆勢操作限制",
    "K線最後交易日",
    "K線落後交易日",
    "K線資料新鮮度",
    "K線日期驗證基準",
    "正式推薦版本",
]

NUMERIC_FORMAL_RECOMMENDATION_COLUMNS = {
    "推薦可信度分",
    "股神推薦優先分",
    "股神推薦總排名",
    "主流主升優先分",
    "隔日觸發品質分",
    "隔日有效風控距離%",
    "紅燈觸發脆弱度分",
    "實戰操作品質分",
    "資料完整度評分",
    "建議倉位上限%",
    "可操作分",
    "正式推薦排序分",
    "股神作戰優先序",
    "盤中盯盤順序",
    "原始觸發價",
    "實戰觸發價",
    "觸發價偏離%",
    "觸發後守價",
    "流動性參考成交額百萬",
    "隔日可參考分",
    "觸發距離%",
    "停損距離_隔日%",
    "進場可執行分",
    "距最近可執行買點%",
    "主要進場參考價",
    "回測承接參考價",
    "突破確認參考價",
    "守價回測參考價",
    "守價回測距離%",
    "隔日耗竭風險分",
    "隔日可執行優先分",
    "路徑風險報酬比",
    "強勢動能分",
    "強勢前兆分",
    "紅燈逆勢反轉分",
    "恐慌反彈領漲分",
    "大盤資料落後交易日",
    "K線落後交易日",
}

_BLANK_TEXTS = {"", "none", "nan", "nat", "null", "--", "-", "<na>"}


def _safe_str(v: Any) -> str:
    try:
        if v is None:
            return ""
        if pd.isna(v):
            return ""
    except Exception:
        pass
    s = str(v).strip()
    return "" if s.lower() in _BLANK_TEXTS else s


def _is_blank(v: Any) -> bool:
    return _safe_str(v) == ""


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        x = pd.to_numeric(v, errors="coerce")
        if pd.isna(x):
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def _num(row: pd.Series, col: str, default: float = 0.0) -> float:
    return _safe_float(row.get(col), default)


def _first_numeric_value(row: pd.Series, cols: list[str], default: float = 0.0, prefer_positive: bool = False) -> float:
    fallback = None
    for col in cols:
        if col not in row.index:
            continue
        raw = row.get(col)
        try:
            value = pd.to_numeric(raw, errors="coerce")
            if pd.isna(value):
                continue
            value = float(value)
        except Exception:
            continue
        if fallback is None:
            fallback = value
        if not prefer_positive or value > 0:
            return value
    return float(default if fallback is None else fallback)


def _chase_risk_score(row: pd.Series, default: float = 55.0) -> float:
    # 決策引擎正式輸出、Phase2 暫存欄與舊版欄名都相容。
    return _clamp(_first_numeric_value(
        row,
        ["追價風險分", "_phase2_追價風險分", "追高風險分數_決策", "追價風險分數", "追高風險分_機會"],
        default,
        prefer_positive=True,
    ))


def _reference_turnover_m(row: pd.Series) -> float:
    # 最新成交額為 0（休市/末列空值）時，必須回退 20 日均成交額。
    return max(0.0, _first_numeric_value(row, ["成交額百萬", "20日均成交額百萬"], 0.0, prefer_positive=True))


def _text_blob(row: pd.Series, cols: list[str]) -> str:
    return "｜".join(_safe_str(row.get(c)) for c in cols if c in row.index)


def _contains_any(text: str, keys: list[str]) -> bool:
    return any(k in text for k in keys)


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return lo


def _first_price(row: pd.Series, cols: list[str], default: float = 0.0) -> float:
    for c in cols:
        v = _safe_float(row.get(c), 0.0)
        if v and v > 0:
            return float(v)
    return float(default)



def _stop_distance_pct(row: pd.Series) -> float:
    practical = _num(row, "實戰停損距離%", 0.0)
    if practical > 0:
        return round(practical, 2)
    direct = _num(row, "停損距離%", 0.0)
    if direct > 0:
        return round(direct, 2)
    price = _first_price(row, ["最新價", "推薦價格", "推薦日價格", "建議價位"], 0.0)
    stop = _first_price(row, ["停損價", "停損價_隔日", "停損參考", "失效價位"], 0.0)
    if price <= 0 or stop <= 0 or stop >= price:
        return 0.0
    return round((price - stop) / price * 100.0, 2)


def _upside_space_pct(row: pd.Series) -> float:
    practical = _num(row, "實戰壓力空間%", 0.0)
    if practical > 0:
        return round(practical, 2)
    direct = _num(row, "壓力空間%", 0.0)
    if direct > 0:
        return round(direct, 2)
    price = _first_price(row, ["最新價", "推薦價格", "推薦日價格", "建議價位"], 0.0)
    target = _first_price(row, ["賣出目標1", "第一壓力價", "近端壓力", "突破確認價"], 0.0)
    if price <= 0 or target <= price:
        return 0.0
    return round((target - price) / price * 100.0, 2)


def _risk_reward_ratio(row: pd.Series) -> float:
    """採保守風報比，不讓近端停損重算值把原始風險失真地放大。"""
    practical = _num(row, "實戰風險報酬比", 0.0)
    raw = _first_numeric_value(row, ["風險報酬比", "風險報酬比_決策"], 0.0, prefer_positive=True)
    if practical > 0 and raw > 0:
        return min(practical, raw)
    if raw > 0:
        return raw
    return practical


def _path_risk_reward_profile(row: pd.Series) -> dict[str, Any]:
    """依交易路徑提供風報比。

    傳統拉回／箱型突破仍使用保守風報比；強勢動能與強勢前兆若已突破舊壓力，
    原始靜態 RR 可能失真，因此改以「實戰壓力空間 ÷ 實戰停損距離」評估，
    但仍設上限，避免極小停損把數值無限放大。
    """
    conservative = _risk_reward_ratio(row)
    practical = _num(row, "實戰風險報酬比", 0.0)
    upside = _upside_space_pct(row)
    stop = _stop_distance_pct(row)
    if stop <= 0:
        stop = _num(row, "停損距離_隔日%", 0)
    derived = (upside / stop) if upside > 0 and stop > 0 else 0.0
    trend = max(practical, derived)
    trend = min(trend, 4.0) if trend > 0 else 0.0
    return {
        "conservative": round(max(0.0, conservative), 2),
        "practical": round(max(0.0, practical), 2),
        "derived": round(max(0.0, derived), 2),
        "trend": round(max(0.0, trend), 2),
        "upside": round(max(0.0, upside), 2),
        "stop": round(max(0.0, stop), 2),
    }


def _hard_veto_reasons(exclusion: list[str]) -> list[str]:
    """只保留真正不能升級的硬否決。

    「舊壓力造成 RR 偏低」與「上方空間偏小」對價格發現型股票不一定是硬否決；
    低流動性、興櫃、明確禁買、假強、極端追價與停損失衡則仍不可放寬。
    """
    hard_keys = [
        "興櫃", "低流動性", "冷門", "買進分數過低", "Entry/Risk 同時偏弱",
        "追價風險過高", "停損距離", "角色已判定過熱/禁買", "硬風控或過熱",
        "假強風險", "流動性資料缺失",
    ]
    return [reason for reason in exclusion if any(key in reason for key in hard_keys)]


def _promotion_profile(row: pd.Series, op_score: float, exclusion: list[str]) -> dict[str, Any]:
    """正式／A- 的路徑化升級判斷。

    Phase 9.3 將「個股資格」與「市場操作許可」拆開：
    - 個股可先取得正式候選或 A- 候選資格；
    - 大盤紅燈時，正式候選會降為 A-MD，A- 候選仍保留 A-MD 資格，
      但倉位固定 0%，不得建立新倉；
    - 因此畫面、總排名、摘要與 Excel 不再出現同一檔一邊顯示 A-、
      另一邊又統計為 0 的不同步情況。
    """
    readiness = _entry_readiness_profile(row)
    momentum = _momentum_profile(row)
    prebreak = _prebreakout_profile(row)
    market = _market_risk_info(row)
    liq = _liquidity_info(row)
    hard = _hard_veto_reasons(exclusion)
    rr = _path_risk_reward_profile(row)
    red_reversal = _red_market_reversal_profile(row, op_score, hard, market=market, prebreak=prebreak)

    fresh = bool(readiness["freshness"].get("fresh"))
    gap = _safe_float(readiness.get("nearest_gap"), 99.0)
    stop = rr["stop"]
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    buy = _num(row, "買進分數", 0)
    chase = _chase_risk_score(row, 55)
    amount = _reference_turnover_m(row)
    mainstream = _num(row, "主流資金分", 0)
    sector = max(_num(row, "族群攻擊強度", 0), _num(row, "族群輪動分", 0))
    ret5 = _num(row, "近5日漲幅%", 0)
    ret20 = _num(row, "近20日漲幅%", 0)

    # 個股資料與交易品質先獨立判斷；大盤只決定是否可以執行，
    # 不再把已達標個股直接從 A-/正式候選名單抹除。
    official_freshness = _official_factor_freshness_info(row)
    base_data_ok = (
        fresh and not hard and liq["tradable"]
        and bool(market.get("formal_ready"))
        and bool(official_freshness.get("formal_ready"))
    )

    formal_pullback = bool(
        base_data_ok and "回測" in _safe_str(readiness.get("path"))
        and gap <= 4.0 and rr["conservative"] >= 1.60
        and 0 < stop <= 6.0 and entry >= 62 and risk >= 64 and buy >= 65
        and amount >= 250 and chase <= 45 and op_score >= 70
        and mainstream >= 60 and sector >= 50
    )
    formal_momentum = bool(
        base_data_ok and momentum.get("core_ready") and momentum["score"] >= 82
        and momentum.get("exhaustion_score", 100) < 30
        and gap <= 5.0 and rr["trend"] >= 1.35
        and 0 < stop <= 7.0 and risk >= 64 and buy >= 60
        and amount >= 500 and chase <= 52 and op_score >= 72
    )
    formal_candidate = formal_pullback or formal_momentum

    a_pullback = bool(
        base_data_ok and not formal_candidate and "回測" in _safe_str(readiness.get("path"))
        and gap <= 4.2 and rr["conservative"] >= 1.05
        and 0 < stop <= 8.2 and entry >= 62 and risk >= 55 and buy >= 55
        and amount >= 150 and chase <= 65 and op_score >= 62
        and mainstream >= 55 and sector >= 45
        and ret5 >= -8.0 and ret20 >= -12.0
    )
    a_momentum = bool(
        base_data_ok and not formal_candidate and momentum["radar_ready"] and momentum["score"] >= 78
        and momentum.get("exhaustion_score", 100) < 45
        and gap <= 6.0 and rr["trend"] >= 1.15
        and 0 < stop <= 7.5 and risk >= 58 and buy >= 55
        and amount >= 300 and chase <= 60 and op_score >= 65
        and ret5 >= -6.0 and ret20 >= -10.0
    )
    a_prebreak = bool(
        base_data_ok and not formal_candidate and prebreak["radar_ready"] and prebreak["score"] >= 78
        and not prebreak.get("hot_risk") and gap <= 4.8 and rr["trend"] >= 1.15
        and 0 < stop <= 7.5 and risk >= 58 and buy >= 60
        and amount >= 300 and chase <= 60 and op_score >= 62
        and ret5 >= -8.0 and ret20 >= -12.0
    )
    a_minus_candidate = a_pullback or a_momentum or a_prebreak

    # 一般紅燈不再一律把所有股票封鎖為 0%。只有嚴格的「洗盤後反轉」
    # 路徑可取得 A-R 資格；極端風險仍全面禁買。其餘候選維持 A-MD。
    formal = bool(formal_candidate and not market["severe"])
    red_override = bool(red_reversal["eligible"])
    market_blocked = bool(market["severe"] and (formal_candidate or a_minus_candidate) and not red_override)
    a_minus = bool((a_minus_candidate and not market["severe"]) or red_override or market_blocked)

    if formal_pullback:
        base_route = "正式｜穩健回測承接"
        rr_used = rr["conservative"]
        rr_basis = "保守風報比"
    elif formal_momentum:
        base_route = "正式｜非過熱動能續強"
        rr_used = rr["trend"]
        rr_basis = "實戰壓力/停損路徑風報比"
    elif a_pullback:
        base_route = "A-｜近買點回測承接"
        rr_used = rr["conservative"]
        rr_basis = "保守風報比"
    elif a_momentum:
        base_route = "A-｜非過熱動能條件進場"
        rr_used = rr["trend"]
        rr_basis = "實戰壓力/停損路徑風報比"
    elif a_prebreak:
        base_route = "A-｜強勢前兆待觸發"
        rr_used = rr["trend"]
        rr_basis = "實戰壓力/停損路徑風報比"
    else:
        base_route = "未達正式/A-升級"
        rr_used = rr["conservative"]
        rr_basis = "保守風報比"

    if red_override:
        route = f"A-R｜紅燈逆勢反轉條件推薦｜{base_route}"
    elif market_blocked:
        route = (
            f"A-MD｜正式候選受大盤封鎖｜{base_route}"
            if formal_candidate
            else f"A-MD｜A-候選受大盤封鎖｜{base_route}"
        )
    else:
        route = base_route

    near_reasons: list[str] = []
    if market["severe"]:
        near_reasons.append("大盤紅燈：一般候選封鎖；只有嚴格反轉路徑可極小量")
    if not fresh:
        near_reasons.append("K線非最新交易日")
    if not market.get("formal_ready"):
        near_reasons.append(f"大盤因子未與K線對齊（落後{market.get('lag', 0)}交易日）")
    if not official_freshness.get("formal_ready"):
        near_reasons.append(f"官方因子未與K線對齊（{official_freshness.get('status', '日期未驗證')}）")
    near_reasons.extend(hard)
    if gap > 4.8:
        near_reasons.append(f"距可執行買點{gap:.1f}%")
    if rr_used < 1.05:
        near_reasons.append(f"路徑RR僅{rr_used:.2f}")
    if stop <= 0 or stop > 8.2:
        near_reasons.append(f"停損距離{stop:.1f}%")
    if entry < 62:
        near_reasons.append(f"Entry {entry:.1f}<62")
    if risk < 55:
        near_reasons.append(f"Risk {risk:.1f}<55")
    if amount < 150:
        near_reasons.append(f"成交額{amount:.0f}百萬不足")
    if chase > 65:
        near_reasons.append(f"追價風險{chase:.0f}>65")
    if ret5 < -8.0:
        near_reasons.append(f"近5日跌幅{ret5:.1f}%過深，先等反轉確認")

    return {
        "formal": formal,
        "a_minus": a_minus,
        "formal_candidate": formal_candidate,
        "a_minus_candidate": a_minus_candidate,
        "market_blocked": market_blocked,
        "red_override": red_override,
        "red_reversal": red_reversal,
        "market_blocked_from": "正式候選" if formal_candidate and market_blocked else "A-候選" if market_blocked else "",
        "formal_pullback": formal_pullback,
        "formal_momentum": formal_momentum,
        "a_pullback": a_pullback,
        "a_momentum": a_momentum,
        "a_prebreak": a_prebreak,
        "base_route": base_route,
        "route": route,
        "rr_used": round(rr_used, 2),
        "rr_basis": rr_basis,
        "near_reasons": "、".join(dict.fromkeys(near_reasons[:8])),
        "hard_veto": "、".join(dict.fromkeys(hard)),
    }


def _liquidity_info(row: pd.Series) -> dict[str, Any]:
    """Return evidence-aware liquidity state.

    Missing turnover/volume is not the same as zero liquidity.  Phase 8.3 keeps
    those stocks in a data-pending watch bucket and blocks formal action until
    the data is recovered, instead of falsely labelling them as illiquid.
    """
    amount = _num(row, "成交額百萬", 0)
    avg_amount = _num(row, "20日均成交額百萬", 0)
    volume = max(_num(row, "最新成交量_張", 0), _num(row, "最新成交量張", 0))
    avg_volume = max(_num(row, "20日均量_張", 0), _num(row, "20日均量張", 0))
    price = _first_price(row, ["最新價", "推薦價格", "推薦日價格", "收盤價"], 0.0)
    if amount <= 0 and price > 0 and volume > 0:
        amount = price * volume / 1000.0
    if avg_amount <= 0 and price > 0 and avg_volume > 0:
        avg_amount = price * avg_volume / 1000.0
    known = any(v > 0 for v in [amount, avg_amount, volume, avg_volume])
    blob = _text_blob(row, ["流動性等級", "流動性資料狀態", "主流作戰分區", "冷門股警示", "主流資金角色"])
    ref_amount = amount if amount > 0 else avg_amount
    ref_volume = volume if volume > 0 else avg_volume
    # Quantitative evidence takes precedence over a stale text label left by an
    # earlier engine run.  A high-turnover stock must never remain blocked only
    # because an old column still says "低流動性".
    quantitative_low = known and ((0 < ref_amount < 80) or (ref_amount <= 0 and 0 < ref_volume < 1000))
    text_low = _contains_any(blob, ["低流動性排除", "冷門禁追", "極低量", "低流動性"])
    explicit_low = bool(quantitative_low and text_low) or bool(known and ref_amount > 0 and ref_amount < 50)
    missing = not known
    tradable = known and not explicit_low and (ref_amount >= 100 or ref_volume >= 1000)
    return {
        "known": known,
        "missing": missing,
        "explicit_low": explicit_low,
        "tradable": tradable,
        "amount": float(amount),
        "avg_amount": float(avg_amount),
        "volume": float(volume),
        "avg_volume": float(avg_volume),
    }


def _data_pending_only(reasons: list[str]) -> bool:
    if not reasons:
        return False
    soft = ("資料缺失", "資料待補", "待補成交額", "流動性資料")
    return all(any(key in reason for key in soft) for reason in reasons)


def _date_value(row: pd.Series, names: list[str]) -> pd.Timestamp | None:
    for name in names:
        value = row.get(name)
        if _is_blank(value):
            continue
        try:
            ts = pd.to_datetime(value, errors="coerce")
            if pd.notna(ts):
                if getattr(ts, "tzinfo", None) is not None:
                    ts = ts.tz_localize(None)
                return pd.Timestamp(ts).normalize()
        except Exception:
            continue
    return None


def _business_day_lag(newer: pd.Timestamp | None, older: pd.Timestamp | None) -> int:
    if newer is None or older is None or newer <= older:
        return 0
    try:
        start = older + pd.Timedelta(days=1)
        return int(len(pd.bdate_range(start=start, end=newer)))
    except Exception:
        return max(0, int((newer - older).days))


def _official_factor_freshness_info(row: pd.Series) -> dict[str, Any]:
    """檢查官方因子快取是否與本輪個股 K 線對齊。

    只在有可驗證日期時做硬判定；舊版資料沒有日期時保留雷達，
    但以「日期未驗證」警示，不得宣稱資料完整最新。
    """
    stock_date = _date_value(row, [
        "本輪市場最新交易日", "K線最後交易日", "行情資料日期", "價格資料日期"
    ])
    official_date = _date_value(row, [
        "官方因子資料日期", "官方資料日期", "三大法人資料日期",
        "官方因子更新時間_官方", "官方因子更新時間", "官方資料更新時間"
    ])
    lag = _business_day_lag(stock_date, official_date)
    known = official_date is not None and stock_date is not None
    aligned = bool(known and lag == 0)
    one_day_lag = bool(known and lag == 1)
    stale = bool(known and lag >= 2)
    status_blob = _text_blob(row, ["官方因子資料狀態", "官方資料狀態", "資料完整度"])
    explicit_stale = _contains_any(status_blob, ["過期", "嚴重落後", "待更新", "stale"])
    stale = bool(stale or explicit_stale)
    formal_ready = bool(aligned and not explicit_stale)
    if aligned:
        status = "最新/對齊"
    elif one_day_lag:
        status = "落後1日｜正式/A-待同步"
    elif stale:
        status = f"過期｜落後{lag}交易日" if known else "過期｜日期未驗證"
    else:
        status = "日期未驗證｜僅供雷達"
    return {
        "known": known,
        "aligned": aligned,
        "one_day_lag": one_day_lag,
        "stale": stale,
        "formal_ready": formal_ready,
        "official_date": official_date.strftime("%Y-%m-%d") if official_date is not None else "",
        "stock_date": stock_date.strftime("%Y-%m-%d") if stock_date is not None else "",
        "lag": lag if known else 999,
        "status": status,
    }


def _combined_data_freshness_info(row: pd.Series) -> dict[str, Any]:
    market = _market_risk_info(row)
    official = _official_factor_freshness_info(row)
    kline = _history_freshness_info(row)
    warnings: list[str] = []
    if not kline.get("fresh"):
        warnings.append(f"K線{kline.get('last_date') or '日期未驗證'}不是最新")
    if not market.get("formal_ready"):
        warnings.append(
            f"大盤{market.get('market_date') or '日期未驗證'}／K線{market.get('stock_date') or '未知'}未對齊"
        )
    if official.get("known") and not official.get("formal_ready"):
        warnings.append(
            f"官方因子{official.get('official_date') or '未知'}落後K線{official.get('stock_date') or '未知'}"
        )
    elif not official.get("known"):
        warnings.append("官方因子日期未驗證")
    formal_ready = bool(kline.get("fresh") and market.get("formal_ready") and official.get("formal_ready"))
    status = "READY｜K線/大盤/官方因子同交易日" if formal_ready else "WARNING｜資料未完全對齊，禁止正式/A-"
    return {
        "formal_ready": formal_ready,
        "status": status,
        "warning": "；".join(warnings),
        "market": market,
        "official": official,
        "kline": kline,
    }


def _market_risk_info(row: pd.Series) -> dict[str, Any]:
    blob = _text_blob(row, [
        "大盤風險燈號", "大盤橋接風控", "大盤策略模式", "大盤策略建議",
        "大盤風控建議", "今日大盤結論", "大盤橋接狀態",
    ])
    raw_scores = [_num(row, "大盤橋接分數", 0), _num(row, "大盤多空分數", 0)]
    positive_scores = [x for x in raw_scores if x > 0]
    score = min(positive_scores) if positive_scores else 0.0

    market_date = _date_value(row, ["大盤資料日期", "大盤行情日期", "加權資料日期", "大盤橋接資料日期"])
    stock_date = _date_value(row, ["本輪市場最新交易日", "K線最後交易日", "行情資料日期", "價格資料日期"])
    lag = _business_day_lag(stock_date, market_date)
    freshness_text = _text_blob(row, ["大盤資料新鮮度", "大盤資料品質", "大盤資料診斷摘要"])
    # 精準推薦必須使用與個股 K 線同一交易日的大盤/官方因子。
    # 落後 1 個交易日仍可保留雷達，但不得標示「最新/可用」或升格正式/A-。
    aligned = bool(market_date is not None and stock_date is not None and lag == 0)
    one_day_lag = bool(lag == 1)
    stale = bool(lag >= 2 or _contains_any(freshness_text, ["過期", "嚴重落後", "stale"]))

    severe = _contains_any(blob, ["紅燈", "空方", "全面防守", "禁止進攻", "風險急升"])
    defensive = severe or _contains_any(blob, ["防守", "保守", "震盪控風險", "不宜全面追價"])
    panic = _contains_any(blob, ["崩盤", "極端風險", "系統性風險", "禁止所有新倉", "全面停買", "流動性危機", "LOCKDOWN"])
    twse_pct = _num(row, "加權漲跌%", _num(row, "大盤漲跌幅%", _num(row, "加權指數漲跌幅%", 0)))
    otc_pct = _num(row, "櫃買漲跌幅%", _num(row, "OTC漲跌%", _num(row, "上櫃指數漲跌幅%", 0)))
    breadth_pct = _num(row, "市場上漲家數比例%", _num(row, "上漲家數比例%", 50))
    extreme_lockdown = bool(twse_pct <= -3.5 or otc_pct <= -4.5 or (twse_pct <= -2.5 and breadth_pct <= 15))
    if extreme_lockdown:
        severe = True
        defensive = True
        panic = True
    if score > 0 and score < 42:
        severe = True
        defensive = True
    if score > 0 and score < 25:
        panic = True
    # LOCKDOWN/極端風險必然同時屬於 severe 與 defensive，
    # 避免後續紅燈觸發脆弱度函式因 severe=False 而跳過硬封鎖。
    if panic:
        severe = True
        defensive = True

    # 舊大盤快照不得硬封鎖新一輪個股行情。過期時只保留「縮倉/條件式」提醒，
    # 排名與雷達改由最新個股 K 線、主流資金、族群廣度決定。
    raw_severe, raw_panic = severe, panic
    if stale:
        severe = False
        panic = False
        defensive = True
        score = 0.0
        level = "黃燈｜大盤資料過期，僅保留主流條件雷達"
    elif one_day_lag:
        defensive = True
        level = "資料黃燈｜大盤因子落後1交易日，正式/A-待同步"
    else:
        level = "極端風險｜全面禁買" if panic else "紅燈｜只准條件逆勢" if severe else "防守｜縮小倉位" if defensive else "一般｜依個股條件"
    return {
        "blob": blob,
        "score": score,
        "severe": severe,
        "defensive": defensive,
        "panic": panic,
        "level": level,
        "stale": stale,
        "aligned": aligned,
        "one_day_lag": one_day_lag,
        "formal_ready": aligned,
        "market_date": market_date.strftime("%Y-%m-%d") if market_date is not None else "",
        "stock_date": stock_date.strftime("%Y-%m-%d") if stock_date is not None else "",
        "lag": lag,
        "freshness": (
            "過期｜不使用舊大盤硬封鎖" if stale
            else "落後1日｜正式推薦待同步" if one_day_lag
            else "最新/對齊" if aligned
            else "日期未驗證｜正式推薦待同步"
        ),
        "alignment_status": (
            "PASS｜大盤與K線同交易日" if aligned
            else f"WAIT｜大盤{market_date.strftime('%Y-%m-%d') if market_date is not None else '未知'}／K線{stock_date.strftime('%Y-%m-%d') if stock_date is not None else '未知'}"
        ),
        "raw_severe": raw_severe,
        "raw_panic": raw_panic,
        "lockdown": bool(panic),
        "twse_change_pct": round(twse_pct, 2),
        "otc_change_pct": round(otc_pct, 2),
        "new_position_cap_pct": 0 if panic else 5 if severe else 20 if defensive else 45,
    }


def _mainstream_mainrise_profile(row: pd.Series) -> dict[str, Any]:
    """辨識主流主升/輪動領漲，排序與買進許可分離。

    高分只代表必須優先看見，不代表可以追價；過熱者仍只能等回測或再次突破守價。
    """
    mainstream = _clamp(_num(row, "主流資金分", 50))
    sector_new = max(_num(row, "族群攻擊強度", 0), _num(row, "族群輪動分", 0))
    sector = _clamp(sector_new if sector_new > 0 else _num(row, "類股熱度分數", 0))
    breadth = _clamp(_num(row, "族群廣度分", max(0.0, sector - 10.0)))
    sector_amount = _clamp(_num(row, "族群成交額分", 0))
    amount = _reference_turnover_m(row)
    amount_score = 100.0 if amount >= 5000 else 92.0 if amount >= 2000 else 82.0 if amount >= 800 else 70.0 if amount >= 300 else 56.0 if amount >= 100 else 25.0
    if sector_amount > 0:
        amount_score = max(amount_score, sector_amount * 0.85)

    ret1 = _num(row, "今日漲幅%", 0)
    ret5 = _num(row, "近5日漲幅%", 0)
    ret20 = _num(row, "近20日漲幅%", 0)
    vol_ratio = max(_num(row, "當日量比", 1), _num(row, "5日20日量比", 1))
    close_pos = _clamp(_num(row, "當日收盤位置%", 50))
    route = _clamp(max(
        _num(row, "爆發雷達分", 0), _num(row, "隔日爆發分", 0),
        _num(row, "飆股攻擊分", 0), _num(row, "主流領漲回補分", 0),
        _num(row, "強勢動能分", 0), _num(row, "強勢前兆分", 0),
        _num(row, "候選強度分", 0),
    ))
    trend_score = _clamp(50.0 + _clamp(ret1, -8, 10) * 3.1 + _clamp(ret5, -10, 12) * 1.15 + _clamp(ret20, -25, 25) * 0.22)
    volume_score = _clamp(45.0 + (vol_ratio - 1.0) * 34.0)
    structure_score = _clamp(route * 0.68 + close_pos * 0.32)
    lead_blob = _text_blob(row, ["族群內領頭羊", "類股前3強", "是否領先同類股", "主流資金角色", "資金輪動角色"])
    leadership = 90.0 if _contains_any(lead_blob, ["領頭羊", "是", "主流攻擊"]) else 68.0 if _num(row, "類股內排名", 99) <= 3 else 50.0

    score = _clamp(
        mainstream * 0.25 + sector * 0.18 + breadth * 0.07
        + amount_score * 0.14 + trend_score * 0.14 + volume_score * 0.07
        + structure_score * 0.11 + leadership * 0.04
    )
    exhaustion = _safe_float(row.get("隔日耗竭風險分"), _exhaustion_profile(row)["score"])
    chase = _chase_risk_score(row, 55)
    high_heat = bool(ret1 >= 9.3 or exhaustion >= 55 or chase >= 72)
    liquid = amount >= 300
    trend_integrity = ret5 >= -8.0 and ret20 >= -28.0
    mainstream_ok = mainstream >= 68 and sector >= 55 and liquid and score >= 68 and trend_integrity
    confirmed = bool(mainstream_ok and score >= 78 and (breadth >= 45 or amount_score >= 88))

    if confirmed and high_heat:
        status = "LH｜主流主升高熱待回測"
    elif confirmed:
        status = "L+｜主流主升領漲"
    elif mainstream_ok:
        status = "L｜主流輪動領漲"
    else:
        status = "N｜非主攻/尚待確認"
    if high_heat:
        restriction = "禁止追價；只等充分回測守價，或整理後再突破並站穩。"
    elif confirmed:
        restriction = "優先盯盤；只在觸發價放量站上或回測守價成立後分批。"
    elif mainstream_ok:
        restriction = "列主流輪動雷達；需族群同步與量價確認，不預先買進。"
    else:
        restriction = "一般條件式觀察。"
    return {
        "score": round(score, 1), "status": status, "restriction": restriction,
        "confirmed": confirmed, "mainstream_ok": mainstream_ok, "high_heat": high_heat,
        "mainstream": mainstream, "sector": sector, "breadth": breadth,
        "amount": amount, "amount_score": amount_score,
    }


def _red_market_trigger_fragility_profile(row: pd.Series) -> dict[str, Any]:
    """紅燈市場下的「碰價即反轉」風險。

    此分數只決定觸發方式與排序，不會把高風險股直接改成買進建議。
    重點修正：長上影弱收、連續急漲、高耗竭與超深結構停損，
    在紅燈市場必須採二段確認或只等回測，不能只因盤中碰價就算成功。
    """
    market = _market_risk_info(row)
    red = bool(market.get("severe")) and not bool(market.get("stale"))
    if not red:
        return {
            "score": 0.0, "status": "N/A｜非最新紅燈市場", "requirement": "依一般觸發與守價規則。",
            "two_stage": False, "block_breakout": False,
        }

    upper = _clamp(_num(row, "上影線比例%", 0))
    close_pos = _clamp(_num(row, "當日收盤位置%", 50))
    ret1 = _num(row, "今日漲幅%", 0)
    ret5 = _num(row, "近5日漲幅%", 0)
    ret20 = _num(row, "近20日漲幅%", 0)
    vol_ratio = max(_num(row, "當日量比", 1), _num(row, "5日20日量比", 1))
    washout_day = bool(ret1 <= -5.0)
    crash_day = bool(ret1 <= -7.0)
    exhaustion = _clamp(_num(row, "隔日耗竭風險分", _exhaustion_profile(row)["score"]))
    stop = _stop_distance_pct(row)
    gap = min(99.0, max(0.0, _num(row, "觸發距離%", _num(row, "距最近可執行買點%", 99))))
    amount = _reference_turnover_m(row)
    mainrise = _clamp(_safe_float(row.get("主流主升優先分"), _mainstream_mainrise_profile(row)["score"]))

    score = 10.0
    score += 26.0 if upper >= 55 else 18.0 if upper >= 40 else 8.0 if upper >= 25 else 0.0
    score += 18.0 if close_pos < 45 else 10.0 if close_pos < 60 else 0.0
    score += 18.0 if ret1 >= 9.3 else 8.0 if ret1 >= 6.0 else 0.0
    # 跌深反轉同樣是首觸高風險，不能因追價分低就被誤判為結構穩定。
    score += 34.0 if crash_day else 22.0 if washout_day else 0.0
    score += 18.0 if washout_day and close_pos < 60 else 0.0
    score += 12.0 if washout_day and upper >= 30 else 0.0
    score += 12.0 if washout_day and vol_ratio >= 2.2 else 0.0
    score += 14.0 if ret5 >= 12.0 else 7.0 if ret5 >= 8.0 else 0.0
    score += 12.0 if ret1 >= 8.0 and ret20 <= -10.0 else 0.0
    score += 22.0 if exhaustion >= 60 else 10.0 if exhaustion >= 40 else 0.0
    score += 14.0 if stop > 18.0 else 8.0 if stop > 12.0 else 0.0
    score += 12.0 if stop > 18.0 and vol_ratio >= 3.0 else 0.0
    score += 10.0 if vol_ratio >= 4.0 and close_pos < 75 else 0.0
    score += 10.0 if gap > 6.5 else 6.0 if gap > 5.0 else 0.0

    # 主流、高流動性、收盤靠近最高且上影短者，屬於真正逆勢領漲結構；
    # 只降低「脆弱度」，不取消紅燈市場的觸發與守價要求。
    resilient_leader = bool(mainrise >= 78 and close_pos >= 72 and upper <= 25 and exhaustion < 45 and amount >= 800)
    if resilient_leader:
        score -= 16.0
    if amount >= 1500:
        score -= 4.0
    score = _clamp(score)

    high_heat = bool(ret1 >= 9.3 or exhaustion >= 55 or (ret5 >= 12 and ret1 > 0))
    washout_first_touch_block = bool(
        washout_day and (crash_day or close_pos < 60 or upper >= 30 or vol_ratio >= 2.2 or gap > 4.5)
    )
    crash_cooldown = bool(market.get("panic") or _contains_any(str(market.get("blob", "")), ["LOCKDOWN", "崩跌後冷卻", "冷卻確認"]))
    if crash_cooldown:
        score = max(score, 95.0)
    block_breakout = bool(score >= 65 or high_heat or washout_first_touch_block or crash_cooldown)
    two_stage = bool(score >= 40 or block_breakout)
    leader_open_drive = bool(mainrise >= 80 and amount >= 500 and close_pos >= 65 and upper <= 35)
    if crash_cooldown:
        status = "BLOCK-C1｜崩跌後冷卻期禁止觸發"
        requirement = "前一交易日發生極端崩跌且市場尚未完成廣泛修復；盤中碰觸觸發價一律不算買點，至少等待一個完整交易日止穩，再重新掃描。"
    elif washout_first_touch_block:
        status = "BLOCK-R2｜跌深反轉首觸禁買"
        requirement = "跌深反轉不得碰價即買；需先站穩觸發價至少15分鐘，再回測守價不破或出現二次突破，才可極小量評估。"
    elif block_breakout:
        status = "BLOCK-F｜紅燈禁止碰價追突破"
        requirement = "禁止碰價即買；只接受先回測守價，再重新突破，或連續3根5分K/至少15分鐘站穩觸發價後才評估。"
    elif two_stage:
        status = "F-｜紅燈需二段確認"
        requirement = "觸發價只算第一關；需至少15分鐘站穩、量價未急縮且回測守價不破，才可小量評估。"
    elif score >= 25:
        status = "F｜紅燈觸發需加強確認"
        requirement = "突破後不得立刻追價；需觀察回測守價與收盤位置。"
    else:
        status = "F+｜紅燈觸發結構相對穩定"
        requirement = "仍需觸發、守價與分批，禁止開盤預掛追價。"
    if leader_open_drive and block_breakout and not washout_day and not crash_cooldown:
        requirement += " 另可採早盤強勢替代路徑：平高盤開出後，至少15分鐘站穩前收+1.5%、量價同步且第一次回測不破，才可小量；不得追第一根急拉。"
    return {
        "score": round(score, 1), "status": status, "requirement": requirement,
        "two_stage": two_stage, "block_breakout": block_breakout,
        "resilient_leader": resilient_leader,
        "washout_first_touch_block": washout_first_touch_block,
        "leader_open_drive": leader_open_drive and not washout_day and not crash_cooldown,
        "crash_cooldown": crash_cooldown,
    }


def _nextday_trigger_quality_profile(row: pd.Series) -> dict[str, Any]:
    """評估隔日條件單是否可執行，並區分結構停損與觸發後守價。

    隔日條件單真正的短線風控應優先使用「觸發價→觸發後守價」距離；
    遠端結構停損仍保留作為總風控，但不得重複壓低主流領漲雷達。
    """
    entry = _clamp(max(_num(row, "Entry進場買點分", 0), _num(row, "進場買點分", 0), _num(row, "買進分數", 0)))
    risk = _clamp(max(_num(row, "Risk風控安全分", 0), _num(row, "風控安全分", 0)))
    actionable = _clamp(_num(row, "隔日可執行優先分", _num(row, "進場可執行分", 0)))
    gap = min(99.0, max(0.0, _num(row, "觸發距離%", _num(row, "距最近可執行買點%", 99.0))))
    structural_stop = _stop_distance_pct(row)
    if structural_stop <= 0:
        structural_stop = _num(row, "停損距離_隔日%", 0)
    trigger = max(_num(row, "實戰觸發價", 0), _num(row, "突破確認參考價", 0), _num(row, "突破確認價", 0))
    guard = max(_num(row, "觸發後守價", 0), _num(row, "守價回測參考價", 0))
    guard_dist = ((trigger - guard) / trigger * 100.0) if trigger > 0 and 0 < guard < trigger else 0.0
    if 0.35 <= guard_dist <= 5.0:
        effective_stop = guard_dist
        risk_basis = "觸發後守價"
    else:
        effective_stop = structural_stop
        risk_basis = "結構停損"

    exhaustion = _clamp(_num(row, "隔日耗竭風險分", _exhaustion_profile(row)["score"]))
    chase = _chase_risk_score(row, 55)
    close_pos = _clamp(_num(row, "當日收盤位置%", 50))
    upper = _clamp(_num(row, "上影線比例%", 0))
    fragility = _red_market_trigger_fragility_profile(row)
    market_info = _market_risk_info(row)
    official_freshness = _official_factor_freshness_info(row)

    proximity = 100 if gap <= 0.8 else 90 if gap <= 2.0 else 78 if gap <= 3.5 else 58 if gap <= 5.0 else 38 if gap <= 6.5 else 18
    stop_score = 96 if 0 < effective_stop <= 2.2 else 90 if effective_stop <= 4.5 else 80 if effective_stop <= 6.8 else 62 if effective_stop <= 8.5 else 38 if effective_stop <= 10.5 else 15 if effective_stop > 0 else 8
    score = _clamp(
        actionable * 0.25 + entry * 0.17 + risk * 0.16 + proximity * 0.18
        + stop_score * 0.12 + (100 - exhaustion) * 0.06
        + (100 - chase) * 0.03 + close_pos * 0.03
    )
    if fragility.get("two_stage"):
        score -= max(3.0, (float(fragility.get("score", 0)) - 25.0) * 0.16)
    if fragility.get("block_breakout"):
        score = min(score, 48.0)
    if not market_info.get("formal_ready"):
        score = min(score, 54.0)
    if not official_freshness.get("formal_ready"):
        score = min(score, 56.0)
    score = _clamp(score)

    blockers: list[str] = []
    if not market_info.get("formal_ready"):
        blockers.append(f"大盤因子落後{market_info.get('lag', 0)}日，正式觸發待同步")
    if not official_freshness.get("formal_ready"):
        blockers.append(f"官方因子{official_freshness.get('status', '日期未驗證')}，正式觸發待同步")
    if gap > 6.5:
        blockers.append(f"觸發距離{gap:.1f}%過遠")
    elif gap > 5.0:
        blockers.append(f"觸發距離{gap:.1f}%偏遠")
    if effective_stop <= 0:
        blockers.append("缺有效風控距離")
    elif effective_stop > 10.5:
        blockers.append(f"有效風控距離{effective_stop:.1f}%過深")
    if entry < 52:
        blockers.append(f"Entry {entry:.1f}<52")
    if risk < 52:
        blockers.append(f"Risk {risk:.1f}<52")
    if exhaustion >= 60:
        blockers.append(f"耗竭風險{exhaustion:.0f}")
    if upper >= 50 and close_pos < 55:
        blockers.append(f"長上影{upper:.0f}%且收盤位置{close_pos:.0f}%偏弱")
    if fragility.get("crash_cooldown"):
        blockers.append("崩跌後冷卻期：任何盤中碰價均不視為有效觸發")
    elif fragility.get("block_breakout"):
        blockers.append("紅燈高熱/脆弱結構禁止碰價追突破")

    if fragility.get("block_breakout"):
        status = "BLOCK-F｜紅燈只等回測再突破"
    elif fragility.get("two_stage"):
        status = "Q2｜紅燈需二段確認"
    elif score >= 78 and not blockers:
        status = "Q+｜隔日觸發品質佳"
    elif score >= 65 and len(blockers) <= 1:
        status = "Q｜可列條件雷達"
    elif score >= 52:
        status = "Q-｜只列備援觀察"
    else:
        status = "BLOCK-Q｜隔日執行品質不足"
    return {
        "score": round(score, 1), "status": status, "gap": round(gap, 2),
        "stop": round(structural_stop, 2), "effective_stop": round(effective_stop, 2),
        "risk_basis": risk_basis, "blockers": "、".join(blockers),
        "fragility_score": fragility.get("score", 0), "fragility_status": fragility.get("status", ""),
        "requirement": fragility.get("requirement", ""), "two_stage": fragility.get("two_stage", False),
        "block_breakout": fragility.get("block_breakout", False),
    }


def _red_market_reversal_profile(
    row: pd.Series,
    op_score: float,
    hard_veto: list[str] | None = None,
    *,
    market: dict[str, Any] | None = None,
    prebreak: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Strict trigger-first exception for high-quality leaders in a red market.

    A red index must reduce position size, but it must not erase every stock.
    This path only keeps liquid leaders that suffered a one-day washout while
    their 5-day structure, sector support and pre-breakout evidence remain
    intact.  They are never bought at the close: the next session must break
    the trigger and hold the guard price.
    """
    market = market or _market_risk_info(row)
    prebreak = prebreak or _prebreakout_profile(row)
    hard_veto = list(hard_veto or [])
    fresh = _history_freshness_info(row)
    liq = _liquidity_info(row)
    ret1 = _num(row, "今日漲幅%", 0)
    ret5 = _num(row, "近5日漲幅%", 0)
    ret20 = _num(row, "近20日漲幅%", 0)
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    buy = _num(row, "買進分數", 0)
    candidate = max(_num(row, "候選強度分", 0), _num(row, "推薦總分", 0), _num(row, "Alpha選股潛力分", 0))
    mainstream = _num(row, "主流資金分", 0)
    sector = max(_num(row, "族群攻擊強度", 0), _num(row, "族群輪動分", 0))
    amount = _reference_turnover_m(row)
    chase = _chase_risk_score(row, 55)
    close_pos = _clamp(_num(row, "當日收盤位置%", 50))
    upper = _clamp(_num(row, "上影線比例%", 0))
    vol_ratio = max(_num(row, "當日量比", 1), _num(row, "5日20日量比", 1))
    gap = min(99.0, max(0.0, _num(row, "觸發距離%", _num(row, "距最近可執行買點%", 99))))
    pre_score = max(_safe_float(prebreak.get("score"), 0), _num(row, "強勢前兆分", 0))
    washout = -10.5 <= ret1 <= -5.0
    structure_ok = -3.0 <= ret5 <= 12.0 and -8.0 <= ret20 <= 35.0
    score = (
        candidate * 0.18 + op_score * 0.12 + entry * 0.13 + risk * 0.10 + buy * 0.10
        + pre_score * 0.15 + mainstream * 0.09 + sector * 0.08 + min(100.0, amount / 20.0) * 0.05
    )
    if washout:
        score += 8.0
    if structure_ok:
        score += 4.0
    if ret5 < -3.0:
        score -= min(18.0, abs(ret5 + 3.0) * 2.0)
    if hard_veto:
        score -= 25.0
    score = round(_clamp(score), 1)
    data_ok = bool(fresh.get("fresh") and liq.get("tradable") and not hard_veto and market.get("formal_ready"))
    first_touch_fragile = bool(
        washout and (ret1 <= -7.0 or close_pos < 60 or upper >= 30 or vol_ratio >= 2.2 or gap > 4.5)
    )
    reversal_quality_ok = bool(not first_touch_fragile and close_pos >= 60 and upper <= 28 and vol_ratio <= 2.2 and gap <= 4.5)
    eligible = bool(
        market.get("severe") and not market.get("panic") and data_ok
        and washout and structure_ok and reversal_quality_ok and score >= 72
        and candidate >= 76 and pre_score >= 72 and entry >= 65 and risk >= 57 and buy >= 68
        and mainstream >= 62 and sector >= 62 and amount >= 250 and chase <= 72 and op_score >= 62
    )
    radar_data_ok = bool(fresh.get("fresh") and liq.get("tradable") and not hard_veto)
    radar_override = bool(
        market.get("severe") and not market.get("panic") and radar_data_ok
        and prebreak.get("radar_ready") and pre_score >= 78 and candidate >= 76
        and entry >= 64 and risk >= 52 and buy >= 68 and amount >= 250 and chase <= 72 and op_score >= 55
    )
    if market.get("panic"):
        status = "BLOCK-R｜極端市場全面禁買"
    elif not market.get("formal_ready"):
        status = "DATA-WAIT-R｜大盤因子未與K線對齊"
    elif first_touch_fragile:
        status = "WATCH-R2｜跌深反轉首觸禁買"
    elif eligible:
        status = "READY-R｜紅燈逆勢反轉，僅二段確認後極小量"
    elif radar_override:
        status = "WATCH-R｜紅燈強勢雷達，二段確認後才評估"
    else:
        status = "BLOCK-R｜未達紅燈逆勢條件"
    return {
        "score": score, "eligible": eligible, "radar_override": radar_override, "status": status,
        "washout": washout, "ret1": round(ret1, 2), "ret5": round(ret5, 2),
        "first_touch_fragile": first_touch_fragile,
        "restriction": (
            "跌深反轉禁止第一觸進場；需站穩觸發價至少15分鐘，再回測守價不破或二次突破。單檔上限2%，跌破守價立即取消。"
            if first_touch_fragile else
            "不可預掛追價；只在突破實戰觸發價、至少15分鐘確認且守住守價、大盤跌勢未惡化時進場。單檔上限3%，跌破守價立即取消。"
        ),
    }


def _data_quality_score(row: pd.Series) -> float:
    for c in ["官方資料完整度", "資料完整度分數", "資料完整度評分"]:
        value = _num(row, c, -1)
        if value >= 0:
            return _clamp(value)
    blob = _text_blob(row, ["資料完整度", "大盤資料品質", "官方因子資料狀態"])
    if _contains_any(blob, ["完整", "良好", "高"]):
        return 85.0
    if _contains_any(blob, ["中", "部分"]):
        return 65.0
    if _contains_any(blob, ["低", "缺", "失敗", "未串聯"]):
        return 35.0
    return 60.0


def _execution_quality_score(row: pd.Series, op_score: float) -> float:
    """Actual trade quality, not data completeness or technical excitement."""
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    buy = _num(row, "買進分數", 0)
    rr = _risk_reward_ratio(row)
    chase = _chase_risk_score(row, 55)
    stop_dist = _stop_distance_pct(row)
    upside = _upside_space_pct(row)
    rr_score = _clamp(rr * 45.0)
    stop_score = 100.0 if stop_dist <= 6 else 80.0 if stop_dist <= 9 else 55.0 if stop_dist <= 12 else 25.0 if stop_dist <= 15 else 0.0
    upside_score = _clamp(upside * 8.0)
    score = (
        op_score * 0.24
        + entry * 0.20
        + risk * 0.18
        + buy * 0.12
        + rr_score * 0.12
        + (100.0 - chase) * 0.06
        + stop_score * 0.05
        + upside_score * 0.03
    )
    return round(_clamp(score), 1)


def _confidence_score(row: pd.Series, op_score: float, bucket: str) -> float:
    practical = _num(row, "股神實戰總分", _num(row, "股神決策分數", 50))
    feedback = _num(row, "Feedback績效校正分", _num(row, "績效校正分", 0))
    feedback_norm = _clamp(50.0 + feedback * 2.0)
    quality = _data_quality_score(row)
    score = op_score * 0.58 + practical * 0.22 + quality * 0.12 + feedback_norm * 0.08
    if bucket == "正式下週主推薦":
        score += 5
    elif bucket == "A-｜準主推薦小量試單":
        score += 2
    elif bucket in {"高風險雷達觀察", "正式排除清單"}:
        score -= 12
    return round(_clamp(score), 1)


def _position_cap_pct(row: pd.Series, bucket: str, promotion: dict[str, Any] | None = None) -> float:
    if bucket == "正式下週主推薦":
        existing = max(
            _num(row, "動態建議倉位%", 0),
            _num(row, "建議倉位%", 0),
            _num(row, "建議部位%", 0),
        )
        risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 50))
        cap = 15.0 if risk >= 68 else 10.0
        if _market_risk_info(row)["defensive"]:
            cap = min(cap, 8.0)
        if existing > 0:
            cap = min(cap, existing)
        return round(max(3.0, cap), 1)
    if bucket == "A-｜準主推薦小量試單":
        market = _market_risk_info(row)
        promotion = promotion or {}
        if market["panic"]:
            return 0.0
        if market["severe"] and promotion.get("red_override"):
            return 3.0
        if market["severe"]:
            return 0.0
        if market["defensive"]:
            return 3.0
        return 5.0
    if bucket == "盤中雷達追蹤":
        market = _market_risk_info(row)
        promotion = promotion or {}
        red_profile = promotion.get("red_reversal") or {}
        if market.get("severe") and not market.get("panic") and red_profile.get("radar_override"):
            return 2.0
    return 0.0


def _final_action_meta(row: pd.Series, bucket: str, op_score: float, exclusion_text: str, promotion: dict[str, Any] | None = None) -> dict[str, Any]:
    strength = max(
        _num(row, "推薦總分", 0),
        _num(row, "候選強度分", 0),
        _num(row, "Alpha選股潛力分", 0),
    )
    if bucket == "正式下週主推薦":
        conclusion = "A｜正式推薦：可依進場條件分批進場"
        formal = "是"
        permit = "可分批進場"
        grade = "A｜正式主推薦"
        nature = "正式推薦"
        veto = "否"
        consistency = "一致｜選股、買點、風控與風險報酬同步通過"
    elif bucket == "A-｜準主推薦小量試單":
        market = _market_risk_info(row)
        formal = "否｜準主推薦"
        veto = "否"
        if market["severe"] and (promotion or {}).get("red_override"):
            conclusion = "A-R｜紅燈逆勢反轉：觸發守價後極小量"
            permit = "條件式極小量｜觸發＋守價＋大盤未惡化"
            grade = "A-R｜紅燈逆勢條件推薦"
            nature = "紅燈逆勢條件推薦"
            consistency = "一致｜大盤紅燈但個股符合嚴格洗盤反轉路徑；單檔上限3%"
        elif market["severe"]:
            conclusion = "A-MD｜準主推薦候選：大盤紅燈，等待解除封鎖"
            permit = "禁止新倉｜等大盤解除紅燈"
            grade = "A-｜市場封鎖候選"
            nature = "條件推薦候選"
            consistency = "一致｜個股路徑達標，但大盤風控封鎖；建議倉位0%"
        else:
            conclusion = "A-｜準主推薦：盤中確認後只允許小量試單"
            permit = "觸發且守價後小量試單"
            grade = "A-｜條件推薦"
            nature = "條件推薦"
            consistency = "一致｜接近主推薦，但仍有一項以上門檻未完全通過"
    elif bucket == "盤中雷達追蹤":
        red_profile = (promotion or {}).get("red_reversal") or {}
        market = _market_risk_info(row)
        formal = "否"
        veto = "否"
        if market.get("severe") and not market.get("panic") and red_profile.get("radar_override"):
            conclusion = "R1-R｜紅燈逆勢雷達：觸發守價後才可極小量"
            permit = "條件式極小量｜未觸發不交易"
            grade = "R1-R｜紅燈逆勢條件雷達"
            nature = "紅燈逆勢雷達"
            consistency = "一致｜只保留強勢前兆，單檔上限2%，未觸發不計交易"
        else:
            conclusion = "B+｜盤中雷達：未觸發前不可買"
            permit = "僅盤中觸發後評估"
            grade = "B+｜盤中條件雷達"
            nature = "盤中雷達"
            consistency = "一致｜保留爆發機會，但未達正式推薦門檻"
    elif bucket == "高風險雷達觀察":
        conclusion = "R｜高風險觀察：禁止追價"
        formal = "否"
        permit = "禁止新倉｜只觀察"
        grade = "R｜高風險觀察"
        nature = "高風險觀察"
        veto = "是"
        consistency = "一致｜有爆發訊號但風控不足，已與正式推薦隔離"
    elif bucket == "正式排除清單":
        conclusion = "D｜正式排除：禁止新倉"
        formal = "否"
        permit = "禁止買進"
        grade = "D｜禁止買進"
        nature = "正式排除"
        veto = "是"
        if strength >= 80:
            consistency = "一致｜候選強度高但買點/風控不合格，已隔離而非推薦"
        else:
            consistency = "一致｜未通過正式推薦風控門檻"
    elif bucket == "早期潛伏觀察":
        conclusion = "C+｜早期觀察：不列正式推薦"
        formal = "否"
        permit = "不可直接買｜等待轉強"
        grade = "C+｜早期觀察"
        nature = "早期觀察"
        veto = "否"
        consistency = "一致｜保留早期訊號，等待 Entry/Risk 改善"
    else:
        conclusion = "C｜觀察：不列正式推薦"
        formal = "否"
        permit = "不可直接買"
        grade = "C｜一般觀察"
        nature = "後台觀察"
        veto = "否"
        consistency = "一致｜條件不足，僅保留診斷"
    if exclusion_text and bucket not in {"正式下週主推薦", "A-｜準主推薦小量試單"}:
        consistency += f"｜原因：{exclusion_text}"
    return {
        "最終操作結論": conclusion,
        "是否正式推薦": formal,
        "操作許可": permit,
        "正式推薦等級": grade,
        "候選強度分": max(_num(row, "候選強度分", 0), _num(row, "推薦總分", 0), _num(row, "Alpha選股潛力分", 0)),
        "推薦可信度分": _confidence_score(row, op_score, bucket),
        "實戰操作品質分": _execution_quality_score(row, op_score),
        "資料完整度評分": _data_quality_score(row),
        "建議倉位上限%": _position_cap_pct(row, bucket, promotion),
        "風控否決旗標": veto,
        "決策一致性": consistency,
        "候選性質": nature,
    }


def _tw_tick(price: float) -> float:
    if price < 10:
        return 0.01
    if price < 50:
        return 0.05
    if price < 100:
        return 0.1
    if price < 500:
        return 0.5
    if price < 1000:
        return 1.0
    return 5.0


def _round_up_to_tick(price: float) -> float:
    try:
        import math as _math
        tick = _tw_tick(float(price))
        return round(_math.ceil(float(price) / tick) * tick, 2)
    except Exception:
        return round(float(price or 0), 2)


def _round_down_to_tick(price: float) -> float:
    try:
        import math as _math
        tick = _tw_tick(float(price))
        return round(_math.floor(float(price) / tick) * tick, 2)
    except Exception:
        return round(float(price or 0), 2)


def _support_after_trigger(trigger: float) -> float:
    if not trigger or trigger <= 0:
        return 0.0
    # 6/18 回放：華通盤中觸發後回落，不能只看「碰到觸發價」。
    # 觸發後需守住約 98.5% 的確認價，否則視為假突破，不追。
    return _round_down_to_tick(float(trigger) * 0.985)


def _trigger_cap_pct(row: pd.Series) -> float:
    """實戰觸發價偏離上限。

    6/17 回放發現：原本常用第一壓力/遠端突破價，導致華通、台光電、健鼎這類
    盤中轉強股被放在雷達卻觸發價太遠。這裡只下修「觀察觸發價」，不把它升級成
    直接買進，仍要求放量站上與族群同步。
    """
    radar = max(
        _num(row, "爆發雷達分", 0),
        _num(row, "隔日爆發分", 0),
        _num(row, "飆股攻擊分", 0),
        _num(row, "主流領漲回補分", 0),
        _num(row, "漲停回放分", 0),
    )
    amount = _num(row, "成交額百萬", 0)
    sector = max(_num(row, "族群攻擊強度", 0), _num(row, "族群輪動分", 0))
    chase = _chase_risk_score(row, 55)
    # 主流高成交額且族群同步時，盤中確認價不應離昨晚價太遠。
    if amount >= 5000 and radar >= 76 and sector >= 70 and chase <= 62:
        return 0.035
    if amount >= 800 and radar >= 72 and sector >= 65 and chase <= 68:
        return 0.045
    if radar >= 70 and sector >= 60:
        return 0.055
    return 0.065


def _trigger_info(row: pd.Series) -> dict[str, Any]:
    price = _first_price(row, ["最新價", "推薦價格", "推薦日價格", "建議價位"], 0.0)
    raw = _first_price(row, ["盤中轉強觸發價", "突破確認價", "推薦買點_突破", "突破確認價_隔日", "近端壓力", "第一壓力價"], 0.0)
    if price <= 0:
        return {"raw": raw, "final": raw, "dist": 0.0, "reason": "缺少有效價格，沿用原觸發價"}
    if raw <= price * 1.005:
        raw = price * 1.018
    dist = (raw / price - 1.0) * 100.0 if raw > 0 else 0.0
    cap = _trigger_cap_pct(row)
    final = raw
    reason = "沿用原觸發價"
    if raw <= 0:
        final = price * (1.0 + cap)
        reason = f"缺少原觸發價，依雷達強度建立{cap*100:.1f}%實戰觸發價"
    elif dist > cap * 100.0:
        final = price * (1.0 + cap)
        reason = f"原觸發價偏離{dist:.1f}%，改用{cap*100:.1f}%實戰確認價"
    final = _round_up_to_tick(final)
    final_dist = (final / price - 1.0) * 100.0 if price > 0 else 0.0
    return {"raw": round(float(raw or 0), 2), "final": final, "dist": round(final_dist, 2), "reason": reason}


def _review_text_for(row: pd.Series, bucket: str, trig: dict[str, Any]) -> str:
    strength = _num(row, "強勢股漏選風險分", 0)
    replay = _num(row, "漲停回放分", 0)
    sector = max(_num(row, "族群攻擊強度", 0), _num(row, "族群輪動分", 0))
    amount = _num(row, "成交額百萬", 0)
    if bucket == "盤中雷達追蹤" and strength >= 90 and replay >= 70:
        return "昨晚雷達具隔日強勢股特徵，保留追蹤；重點改為實戰觸發價，不再等遠端壓力。"
    if bucket == "正式排除清單" and strength >= 90 and sector >= 70 and amount >= 300:
        return "有強勢漏選風險但風控/買點仍不足；保留在回放檢討，不得列正式推薦。"
    if trig.get("reason", "").startswith("原觸發價偏離"):
        return "原觸發價過遠，容易錯過隔日轉強；已下修為盤中確認價。"
    return "依正式推薦淨化規則分流。"


def _compute_operability_score(row: pd.Series) -> float:
    """Professional operability score with neutral treatment for missing liquidity."""
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 45))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 45))
    buy = _num(row, "買進分數", 45)
    rr = _risk_reward_ratio(row)
    chase = _chase_risk_score(row, 55)
    mainstream = _num(row, "主流資金分", 50)
    money = _num(row, "資金攻擊有效分", _num(row, "籌碼續航分", 50))
    sector = _num(row, "族群攻擊強度", _num(row, "族群輪動分", 50))
    liq = _liquidity_info(row)
    radar = max(
        _num(row, "爆發雷達分", 0), _num(row, "隔日爆發分", 0),
        _num(row, "飆股攻擊分", 0), _num(row, "主流領漲回補分", 0),
        _num(row, "漲停回放分", 0),
    )
    rr_score = _clamp(rr * 42.0, 0, 100)
    amount = liq["amount"] if liq["amount"] > 0 else liq["avg_amount"]
    if not liq["known"]:
        amount_score = 45.0
    elif amount >= 5000:
        amount_score = 100.0
    elif amount >= 2000:
        amount_score = 88.0
    elif amount >= 800:
        amount_score = 76.0
    elif amount >= 300:
        amount_score = 62.0
    elif amount >= 100:
        amount_score = 45.0
    else:
        amount_score = 20.0
    score = (
        entry * 0.23 + risk * 0.20 + buy * 0.14 + rr_score * 0.14
        + (100 - chase) * 0.08 + mainstream * 0.08 + sector * 0.06
        + money * 0.04 + amount_score * 0.03
    )
    score += max(0.0, radar - 72.0) * 0.08
    if not liq["known"]:
        score -= 4.0
    return round(_clamp(score), 1)


def _exclusion_reasons(row: pd.Series) -> list[str]:
    """Formal-action veto reasons with missing-data/true-risk separation."""
    reasons: list[str] = []
    role_blob = _text_blob(row, ["推薦角色", "穩健推薦角色", "實戰過濾狀態", "主流作戰分區", "飆股雷達角色"])
    veto_blob = _text_blob(row, ["真禁買原因", "過熱原因", "硬否決原因", "主推薦降級原因", "高分禁買原因"])
    buy = _num(row, "買進分數", 0)
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    rr = _risk_reward_ratio(row)
    chase = _chase_risk_score(row, 55)
    stop_dist = _stop_distance_pct(row)
    upside = _upside_space_pct(row)
    liq = _liquidity_info(row)

    if _safe_str(row.get("市場別")).replace(" ", "") in {"興櫃", "Emerging"}:
        reasons.append("興櫃股票不列正式作戰推薦")
    if _contains_any(role_blob, ["D｜過熱禁買", "過熱禁買", "BLOCK", "禁止買進排除"]):
        reasons.append("角色已判定過熱/禁買")
    if _contains_any(veto_blob, ["過熱", "追價", "停損距離過大", "風控失衡", "禁買"]):
        reasons.append("存在硬風控或過熱原因")
    if "假強" in veto_blob and (liq["explicit_low"] or buy < 45 or entry < 45 or chase >= 75):
        reasons.append("量價證據仍符合假強風險")
    if liq["missing"]:
        reasons.append("流動性資料缺失，待補成交額/成交量後重評")
    elif liq["explicit_low"] or (liq["amount"] > 0 and liq["amount"] < 80):
        reasons.append("低流動性或冷門禁追")
    if buy < 30:
        reasons.append("買進分數過低")
    if entry < 35 and risk < 42:
        reasons.append("Entry/Risk 同時偏弱")
    if chase >= 78:
        reasons.append("追價風險過高")
    if rr < 0.25:
        reasons.append("風險報酬比過低")
    if stop_dist > 15:
        reasons.append(f"停損距離{stop_dist:.1f}%過大")
    if 0 < upside < 3:
        reasons.append(f"上方空間僅{upside:.1f}%")

    out: list[str] = []
    for reason in reasons:
        if reason and reason not in out:
            out.append(reason)
    return out


def _exhaustion_profile(row: pd.Series) -> dict[str, Any]:
    """衡量隔日追價耗竭，而不是把「昨天大漲」一律視為更強。

    高動能仍可留在完整雷達，但若單日/五日漲幅、跳空、追價風險與上影線
    同時偏高，必須降為 HOT-WAIT，避免隔日主排名被末端加速股占滿。
    """
    day_gain = _first_numeric_value(row, ["今日漲幅%", "單日漲幅%"], 0.0)
    ret5 = _num(row, "近5日漲幅%", 0)
    ret20 = _num(row, "近20日漲幅%", 0)
    chase = _chase_risk_score(row, 55)
    gap = _first_numeric_value(row, ["開盤跳空%"], 0.0)
    upper = _first_numeric_value(row, ["上影線比例%"], 0.0)
    close_loc = _first_numeric_value(row, ["當日收盤位置%", "收盤位置%"], 50.0)
    vol = _first_numeric_value(row, ["當日量比", "均量比", "量比"], 0.0, prefer_positive=True)
    breakout20 = _first_numeric_value(row, ["突破20日高點%", "20日突破幅度%"], -99.0)

    score = 0.0
    reasons: list[str] = []
    if day_gain >= 9.5:
        score += 40; reasons.append("單日接近漲停")
    elif day_gain >= 8.5:
        score += 34; reasons.append("單日漲幅逾8.5%")
    elif day_gain >= 7.0:
        score += 24; reasons.append("單日漲幅逾7%")
    elif day_gain >= 5.5:
        score += 12
    if ret5 >= 20:
        score += 24; reasons.append("5日漲幅逾20%")
    elif ret5 >= 14:
        score += 17; reasons.append("5日漲幅逾14%")
    elif ret5 >= 9:
        score += 9
    if ret20 >= 55:
        score += 16; reasons.append("20日漲幅過度擴張")
    elif ret20 >= 35:
        score += 9
    if chase >= 75:
        score += 19; reasons.append("追價風險高")
    elif chase >= 65:
        score += 12
    elif chase >= 55:
        score += 6
    if gap >= 5:
        score += 13; reasons.append("跳空幅度過大")
    elif gap >= 3:
        score += 7
    if upper >= 38:
        score += 14; reasons.append("上影線偏長")
    elif upper >= 25:
        score += 7
    if vol and vol < 1.10:
        score += 8; reasons.append("量能未明顯擴張")
    if breakout20 < -10 and day_gain >= 6:
        score += 8; reasons.append("大漲但仍遠離20日高點")
    # 高檔收盤、真正放量且尚未累積過大漲幅，可抵銷部分耗竭疑慮。
    if close_loc >= 90 and vol >= 1.5 and ret5 < 14 and upper <= 12:
        score -= 12
    elif close_loc >= 82 and vol >= 1.3 and ret5 < 10 and upper <= 20:
        score -= 6
    score = round(_clamp(score), 1)
    if score >= 70:
        level = "極高｜隔日禁止追價"
    elif score >= 55:
        level = "高｜只等充分回測"
    elif score >= 35:
        level = "中｜需二次確認"
    else:
        level = "低｜一般條件確認"
    return {
        "score": score, "level": level, "hot": score >= 55, "extreme": score >= 70,
        "reasons": "、".join(dict.fromkeys(reasons)),
    }


def _momentum_profile(row: pd.Series) -> dict[str, Any]:
    """強勢動能路徑，並把「有動能」與「隔日可核心盯盤」分開。"""
    market_type = _safe_str(row.get("市場別")).replace(" ", "")
    day_gain = _first_numeric_value(row, ["今日漲幅%", "單日漲幅%"], 0.0)
    close_loc = _first_numeric_value(row, ["當日收盤位置%", "收盤位置%"], 50.0)
    day_vol = _first_numeric_value(row, ["當日量比", "均量比", "量比"], 0.0, prefer_positive=True)
    breakout20 = _first_numeric_value(row, ["突破20日高點%", "20日突破幅度%"], -99.0)
    upper_shadow = _first_numeric_value(row, ["上影線比例%"], 0.0)
    day_gap = _first_numeric_value(row, ["開盤跳空%"], 0.0)
    rescue = _num(row, "盤後動能救援分", 0)
    amount = _reference_turnover_m(row)
    ret5 = _num(row, "近5日漲幅%", 0)
    ret20 = _num(row, "近20日漲幅%", 0)
    close_ma20 = _num(row, "收盤距MA20%", 0)
    mainstream = _num(row, "主流資金分", 0)
    sector = max(_num(row, "族群攻擊強度", 0), _num(row, "族群輪動分", 0), _num(row, "類股熱度分數", 0))
    radar = max(
        _num(row, "爆發雷達分", 0), _num(row, "隔日爆發分", 0),
        _num(row, "飆股攻擊分", 0), _num(row, "主流領漲回補分", 0),
        _num(row, "漲停回放分", 0), rescue,
    )
    freshness = _history_freshness_info(row)
    market = _market_risk_info(row)
    exhaustion = _exhaustion_profile(row)

    # 4%~7%且放量守高的點火段最適合隔日條件追蹤；近漲停不是自動加分。
    if 4.0 <= day_gain < 7.0:
        gain_score = 94
    elif 2.5 <= day_gain < 4.0:
        gain_score = 82
    elif 7.0 <= day_gain < 8.5:
        gain_score = 78
    elif 8.5 <= day_gain <= 10.3:
        gain_score = 66
    elif 1.5 <= day_gain < 2.5:
        gain_score = 62
    else:
        gain_score = 28
    volume_score = 100 if day_vol >= 2.0 else 90 if day_vol >= 1.5 else 80 if day_vol >= 1.25 else 66 if day_vol >= 1.10 else 42 if day_vol >= 1.0 else 25
    breakout_score = 96 if breakout20 >= 0 else 86 if breakout20 >= -1.0 else 72 if breakout20 >= -3.0 else 42 if breakout20 >= -7 else 24
    liquidity_score = 100 if amount >= 2000 else 90 if amount >= 800 else 78 if amount >= 300 else 65 if amount >= 150 else 48 if amount >= 100 else 10
    score = (
        max(rescue, gain_score) * 0.20 + close_loc * 0.18 + volume_score * 0.18
        + breakout_score * 0.15 + liquidity_score * 0.10 + radar * 0.08
        + mainstream * 0.055 + sector * 0.055
    )
    if upper_shadow > 45:
        score -= 18
    elif upper_shadow > 35:
        score -= 9
    if close_ma20 > 28 or ret5 > 38:
        score -= 16
    if day_gap > 7:
        score -= 8
    score -= exhaustion["score"] * 0.16
    score = round(_clamp(score), 1)

    blockers: list[str] = []
    if market_type in {"興櫃", "Emerging"}:
        blockers.append("興櫃不列強勢動能作戰")
    if amount < 100:
        blockers.append("成交額不足1億元")
    if day_gain < 2.5 or day_gain > 10.3:
        blockers.append("單日漲幅不在有效點火區")
    if close_loc < 68:
        blockers.append("收盤未守在當日高檔")
    if day_vol < 1.10 and amount < 800:
        blockers.append("當日量能未確認")
    if breakout20 < -3.0 and day_gain < 6:
        blockers.append("尚未接近20日突破")
    if upper_shadow > 42:
        blockers.append("上影線過長，追價承接不穩")
    if ret5 > 38 or close_ma20 > 30:
        blockers.append("短線乖離過大")
    if not freshness["known"] or not freshness["fresh"]:
        blockers.append("K線日期未驗證或已過期")

    eligible = not blockers and score >= 66
    radar_ready = bool(eligible and score >= 68)
    strict_hot_ok = bool(
        day_gain < 8.5 or (day_vol >= 1.8 and close_loc >= 94 and upper_shadow <= 8
                           and ret5 <= 14 and _chase_risk_score(row, 55) <= 58)
    )
    core_ready = bool(
        radar_ready and score >= 74 and exhaustion["score"] < 55 and strict_hot_ok
        and close_loc >= 72 and amount >= 150
        and (day_vol >= 1.15 or amount >= 1000)
        and upper_shadow <= 35
    )
    strong = bool(core_ready and score >= 80 and close_loc >= 82 and (day_vol >= 1.30 or amount >= 1000))
    market_wait = bool(radar_ready and market["severe"])
    if exhaustion["hot"]:
        role = "M-HOT｜高熱動能待回測"
    elif day_gain >= 8.5:
        role = "M+｜近漲停續強待確認"
    elif breakout20 >= 0:
        role = "M｜放量突破"
    elif radar_ready:
        role = "M｜強勢點火"
    else:
        role = "N｜非動能作戰"
    entry = "不可開盤追價；只接受首波回測不破、量縮守住，或盤中再突破當日/前高且量能續強。"
    if exhaustion["hot"]:
        entry = "高熱動能只保留觀察；至少完成一次量縮回測、守住點火結構後，再突破盤中高點才評估小量進場。"
    risk_text = "若開高逾5%或跌破點火K低點/觸發後守價，取消交易；以小倉位、移動停利管理。"
    return {
        "score": score, "eligible": eligible, "radar_ready": radar_ready, "core_ready": core_ready,
        "strong": strong, "market_wait": market_wait, "role": role,
        "blockers": "、".join(blockers), "entry": entry, "risk": risk_text,
        "day_gain": round(day_gain, 2), "close_loc": round(close_loc, 2),
        "day_vol": round(day_vol, 2), "breakout20": round(breakout20, 2),
        "upper_shadow": round(upper_shadow, 2), "amount": round(amount, 1),
        "exhaustion_score": exhaustion["score"], "exhaustion_level": exhaustion["level"],
        "hot_risk": exhaustion["hot"], "exhaustion_reasons": exhaustion["reasons"],
    }


def _prebreakout_profile(row: pd.Series) -> dict[str, Any]:
    """第三條路徑：強勢前兆／主流領漲召回。

    目的不是預測所有漲停，而是避免「尚未大漲、但主流資金＋族群＋爆發回放
    已同步」的股票被傳統 RR、靜態停損或 Entry/Risk 一票否決。這類股票只列
    R1-P 條件雷達，不直接升級正式推薦；盤中必須突破/回測確認。
    """
    market_type = _safe_str(row.get("市場別")).replace(" ", "")
    amount = _reference_turnover_m(row)
    missed = _num(row, "強勢股漏選風險分", 0)
    radar = max(
        _num(row, "爆發雷達分", 0), _num(row, "隔日爆發分", 0),
        _num(row, "飆股攻擊分", 0), _num(row, "主流領漲回補分", 0),
        _num(row, "漲停回放分", 0), _num(row, "盤前強勢前兆分", 0),
    )
    mainstream = _num(row, "主流資金分", 0)
    sector = max(_num(row, "族群攻擊強度", 0), _num(row, "類股熱度分數", 0), _num(row, "族群輪動分", 0))
    candidate = max(_num(row, "候選強度分", 0), _num(row, "推薦總分", 0))
    prelaunch = _num(row, "起漲前兆分數", 0)
    trade = _num(row, "交易可行分數", 0)
    technical = _num(row, "技術結構分數", 0)
    ret5 = _num(row, "近5日漲幅%", 0)
    ret20 = _num(row, "近20日漲幅%", 0)
    chase = _chase_risk_score(row, 55)
    freshness = _history_freshness_info(row)
    market = _market_risk_info(row)

    liquidity_score = 100 if amount >= 2000 else 92 if amount >= 800 else 82 if amount >= 300 else 72 if amount >= 150 else 58 if amount >= 100 else 20
    score = (
        missed * 0.27 + radar * 0.20 + mainstream * 0.14 + sector * 0.11
        + candidate * 0.10 + prelaunch * 0.07 + technical * 0.04 + trade * 0.03
        + liquidity_score * 0.04
    )
    if ret5 > 28 or ret20 > 105:
        score -= 13
    elif ret5 > 20 or ret20 > 80:
        score -= 6
    # 追價風險不能把領漲股從「研究/觸發雷達」直接刪除；它只限制是否可進場。
    # 因此僅做溫和降分，真正的禁追條件放在 status / entry / risk 中。
    if chase >= 92:
        score -= 8
    elif chase >= 84:
        score -= 4
    score = round(_clamp(score), 1)

    blockers: list[str] = []
    if market_type in {"興櫃", "Emerging"}:
        blockers.append("興櫃不列強勢前兆作戰")
    if amount < 100:
        blockers.append("成交額不足1億元")
    if missed < 72:
        blockers.append("強勢漏選風險尚低")
    if radar < 64:
        blockers.append("爆發/回放證據不足")
    if mainstream < 56:
        blockers.append("主流資金不足")
    if sector < 45:
        blockers.append("族群同步不足")
    if max(candidate, prelaunch, technical) < 58:
        blockers.append("技術/前兆結構不足")
    if ret5 > 35 or ret20 > 125:
        blockers.append("短中期漲幅過度延伸")
    hot_risk = bool(chase >= 88 or ret5 > 20 or ret20 > 80)

    eligible = bool(not blockers and score >= 68)
    radar_ready = bool(
        eligible and score >= 72 and amount >= 150 and missed >= 78 and radar >= 68
        and (mainstream >= 60 or sector >= 66)
    )
    if radar_ready and not freshness["fresh"]:
        status = "DATA-WAIT-P｜強勢前兆成立但K線待更新"
    elif radar_ready and market["severe"]:
        status = "MARKET-WAIT-P｜強勢前兆成立但大盤禁止追價"
    elif radar_ready and hot_risk:
        status = "HOT-WAIT-P｜領漲證據成立但禁止追價"
    elif radar_ready:
        status = "READY-P｜強勢前兆條件雷達"
    else:
        status = "BLOCK-P｜未達強勢前兆門檻"

    return {
        "score": score,
        "eligible": eligible,
        "radar_ready": radar_ready,
        "status": status,
        "blockers": "、".join(blockers),
        "amount": round(amount, 1),
        "missed": round(missed, 1),
        "radar": round(radar, 1),
        "mainstream": round(mainstream, 1),
        "sector": round(sector, 1),
        "fresh": bool(freshness["fresh"]),
        "hot_risk": hot_risk,
        "entry": "不預掛追價；開盤漲幅宜低於3.5%，只在放量突破前高/觸發價，或首波回測量縮且守住觸發後守價時小量進場。高熱候選只接受充分回測後再突破，不接開盤急拉。",
        "risk": "開高逾5%不追；跌破觸發後守價、點火K低點或進場價約5%即取消。採分批與移動停利，不用舊壓力價的靜態RR否決趨勢股。",
    }

def _next_session_profile(row: pd.Series) -> dict[str, Any]:
    """隔日可參考品質層。

    這一層不是用單日績效反推答案，而是把本次 2026-07-13 檢討暴露出的
    結構問題固定化：興櫃混入、短線已加速仍追、停損過深、沒有明確隔日型態，
    以及雷達為了湊足固定檔數把低品質標的塞進核心名單。
    """
    market = _safe_str(row.get("市場別")).replace(" ", "")
    price = _first_price(row, ["最新價", "推薦價格", "推薦日價格", "建議價位"], 0.0)
    trig = _trigger_info(row)
    trigger = _safe_float(trig.get("final"), 0.0)
    trigger_dist = max(0.0, (trigger / price - 1.0) * 100.0) if price > 0 and trigger > 0 else 99.0
    stop_dist = _stop_distance_pct(row)
    buy = _num(row, "買進分數", 0)
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    practical = _num(row, "股神實戰總分", 0)
    rr = _risk_reward_ratio(row)
    chase = _chase_risk_score(row, 55)
    amount = _reference_turnover_m(row)
    ret5 = _num(row, "近5日漲幅%", 0)
    ret20 = _num(row, "近20日漲幅%", 0)
    mainstream = _num(row, "主流資金分", 0)
    sector = max(_num(row, "族群攻擊強度", 0), _num(row, "族群輪動分", 0))
    momentum = _momentum_profile(row)

    pullback_reset = (
        4 <= ret20 <= 18 and -6 <= ret5 <= 0.5 and rr >= 0.90
        and entry >= 66 and risk >= 59 and amount >= 100 and (0 < stop_dist <= 14.5)
    )
    steady_continuation = (
        0.5 <= ret5 <= 4.5 and 0 <= ret20 <= 13 and rr >= 1.00
        and entry >= 69 and risk >= 60 and amount >= 180 and chase <= 45
        and (0 < stop_dist <= 12.5)
    )
    event_breakout = (
        trigger_dist <= 2.8 and 2 <= ret5 <= 6.5 and 6 <= ret20 <= 18
        and mainstream >= 70 and sector >= 70 and amount >= 500
        and entry >= 58 and risk >= 54
    )
    early_reversal = (
        1 <= ret5 <= 5 and -5 <= ret20 <= 2 and entry >= 72 and risk >= 61
        and amount >= 400 and chase <= 25 and trigger_dist <= 6.0
    )

    if momentum["radar_ready"]:
        pattern = momentum["role"]
        pattern_bonus = 19.0 if momentum["strong"] else 15.0
    elif pullback_reset:
        pattern = "P｜中期多頭拉回重置"
        pattern_bonus = 18.0
    elif steady_continuation:
        pattern = "C｜穩健續強"
        pattern_bonus = 15.0
    elif event_breakout:
        pattern = "E｜事件型近觸發突破"
        pattern_bonus = 17.0
    elif early_reversal:
        pattern = "R｜早期反轉修復"
        pattern_bonus = 13.0
    else:
        pattern = "N｜尚無明確隔日優勢型態"
        pattern_bonus = 0.0

    hard: list[str] = []
    if market in {"興櫃", "Emerging"}:
        hard.append("興櫃波動/流動性制度不同，不列正式作戰清單")
    if ret5 >= 6 and ret20 >= 5 and trigger_dist > 3 and not momentum["radar_ready"]:
        hard.append("近5日已加速且仍離觸發價偏遠，隔日追高風險")
    if stop_dist > 15:
        hard.append(f"隔日停損距離{stop_dist:.1f}%過深")
    if ret20 < -8:
        hard.append("20日趨勢仍明顯受損")
    if rr < 0.55 and not (event_breakout or early_reversal or momentum["radar_ready"]):
        hard.append("風險報酬比不足")
    if trigger_dist > 8 and not momentum["radar_ready"]:
        hard.append("實戰觸發價距現價過遠")

    rr_score = _clamp(rr * 45.0, 0, 100)
    amount_score = 100 if amount >= 2000 else 88 if amount >= 800 else 75 if amount >= 300 else 62 if amount >= 150 else 45 if amount >= 100 else 20
    trigger_score = 90 if trigger_dist <= 2.5 else 78 if trigger_dist <= 4 else 66 if trigger_dist <= 6.8 else 40
    stop_score = 88 if 0 < stop_dist <= 7 else 74 if stop_dist <= 10 else 62 if stop_dist <= 13.5 else 38
    score = (
        entry * 0.22 + risk * 0.18 + buy * 0.12 + practical * 0.10
        + rr_score * 0.10 + amount_score * 0.08 + trigger_score * 0.07
        + stop_score * 0.06 + (100 - chase) * 0.07 + pattern_bonus
    )
    if momentum["radar_ready"]:
        # 動能型以當日量價結構取代部分靜態 RR / 壓力分數。
        score = max(score, momentum["score"] * 0.72 + risk * 0.12 + practical * 0.08 + sector * 0.08)
    if hard:
        score -= min(32.0, 10.0 + 7.0 * len(hard))
    score = round(_clamp(score), 1)
    reference_ok = bool((momentum["radar_ready"] or pattern_bonus > 0) and not hard and score >= 64)
    strong_ok = bool(reference_ok and score >= 72 and (rr >= 1.0 or event_breakout or early_reversal or momentum["strong"]))
    return {
        "score": score,
        "pattern": pattern,
        "risk": "、".join(hard),
        "reference_ok": reference_ok,
        "strong_ok": strong_ok,
        "trigger_dist": round(trigger_dist, 2),
        "stop_dist": round(stop_dist, 2),
    }



def _history_freshness_info(row: pd.Series) -> dict[str, Any]:
    """讀取推薦行情日期，避免混用不同交易日的價格與技術指標。

    舊快取沒有日期證據時，保留診斷但不允許升級成可進場名單；重新推薦後
    page 7 會提供 K線最後交易日、落後交易日與新鮮度欄位。
    """
    last_date = _safe_str(row.get("K線最後交易日") or row.get("行情資料日期") or row.get("價格資料日期"))
    freshness = _safe_str(row.get("K線資料新鮮度") or row.get("行情資料新鮮度"))
    lag_raw = row.get("K線落後交易日")
    lag_known = not _is_blank(lag_raw)
    lag = int(max(0.0, _safe_float(lag_raw, 0.0))) if lag_known else 999
    if freshness:
        fresh = freshness.startswith("即時") or freshness.startswith("最新") or freshness in {"有效", "新鮮", "PASS"}
        if "落後" in freshness or "過期" in freshness or "未知" in freshness:
            fresh = False
    else:
        fresh = bool(lag_known and lag == 0 and last_date)
    return {
        "known": bool(last_date and lag_known),
        "fresh": bool(fresh),
        "last_date": last_date,
        "lag": lag,
        "status": freshness or ("最新交易日" if fresh else "日期未驗證"),
    }


def _guard_retest_profile(row: pd.Series, trig: dict[str, Any] | None = None) -> dict[str, Any]:
    """評估「突破後守價回測」是否比追突破更接近可執行買點。

    趨勢股常在突破前高後回測守價；若仍只等再次追突破，會把進場點推到
    當日高檔。此路徑只在行情最新、流動性足夠且守價離現價很近時成立。
    """
    trig = trig or _trigger_info(row)
    price = _first_price(row, ["最新價", "推薦價格", "推薦日價格", "建議價位"], 0.0)
    breakout = _safe_float(trig.get("final"), 0.0)
    guard = _first_price(row, ["觸發後守價", "突破後守價"], 0.0)
    if guard <= 0 and breakout > 0:
        guard = _support_after_trigger(breakout)
    gap = abs(guard / price - 1.0) * 100.0 if price > 0 and guard > 0 else 99.0
    freshness = _history_freshness_info(row)
    liq = _liquidity_info(row)
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    amount = _reference_turnover_m(row)
    chase = _chase_risk_score(row, 55)
    stop = _stop_distance_pct(row)
    if stop <= 0:
        stop = _num(row, "停損距離_隔日%", 0)
    eligible = bool(
        freshness.get("fresh") and liq.get("tradable")
        and price > 0 and breakout > 0 and guard > 0
        and breakout >= price * 0.995 and guard <= breakout
        and gap <= 2.5 and entry >= 60 and risk >= 55
        and amount >= 150 and chase <= 68 and 0 < stop <= 8.5
    )
    reasons: list[str] = []
    if not freshness.get("fresh"):
        reasons.append("K線非最新")
    if not liq.get("tradable"):
        reasons.append("流動性未通過")
    if gap > 2.5:
        reasons.append(f"守價距現價{gap:.1f}%過遠")
    if entry < 60:
        reasons.append(f"Entry {entry:.1f}<60")
    if risk < 55:
        reasons.append(f"Risk {risk:.1f}<55")
    if amount < 150:
        reasons.append("成交額不足1.5億元")
    if chase > 68:
        reasons.append(f"追價風險{chase:.0f}>68")
    if stop <= 0 or stop > 8.5:
        reasons.append(f"停損距離{stop:.1f}%不合格")
    return {
        "ready": eligible,
        "reference": round(guard, 4) if guard > 0 else 0.0,
        "gap": round(gap, 2),
        "breakout": round(breakout, 4) if breakout > 0 else 0.0,
        "status": "READY-G｜觸發守價回測" if eligible else "BLOCK-G｜守價回測條件未齊",
        "reasons": "、".join(dict.fromkeys(reasons)),
    }


def _entry_readiness_profile(row: pd.Series) -> dict[str, Any]:
    """三路徑進場模型：波段回測／突破／觸發後守價回測。"""
    price = _first_price(row, ["最新價", "推薦價格", "推薦日價格", "建議價位"], 0.0)
    trig = _trigger_info(row)
    breakout = _safe_float(trig.get("final"), 0.0)
    # 停損價不可再被誤當成回測買點；只採真正支撐或明確拉回欄位。
    pullback = _first_price(
        row,
        ["推薦買點_拉回", "預估進場點_拉回", "回測承接價", "近端支撐", "主要支撐", "MA20"],
        0.0,
    )
    breakout_gap = max(0.0, (breakout / price - 1.0) * 100.0) if price > 0 and breakout > 0 else 99.0
    pullback_gap = abs(price / pullback - 1.0) * 100.0 if price > 0 and pullback > 0 else 99.0
    pullback_broken = bool(price > 0 and pullback > 0 and price < pullback * 0.985)

    rr = _risk_reward_ratio(row)
    stop = _stop_distance_pct(row)
    if stop <= 0:
        stop = _num(row, "停損距離_隔日%", 0)
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    buy = _num(row, "買進分數", 0)
    practical = _num(row, "股神實戰總分", 0)
    amount = _reference_turnover_m(row)
    chase = _chase_risk_score(row, 55)
    ret5 = _num(row, "近5日漲幅%", 0)
    ret20 = _num(row, "近20日漲幅%", 0)
    vol_ratio = _first_numeric_value(row, ["當日量比", "均量比", "量比", "5日20日量比"], 0.0, prefer_positive=True)
    market = _market_risk_info(row)
    freshness = _history_freshness_info(row)
    momentum = _momentum_profile(row)
    prebreak = _prebreakout_profile(row)
    guard = _guard_retest_profile(row, trig)

    pullback_setup = (
        not pullback_broken and pullback_gap <= 2.25 and -5.0 <= ret5 <= 3.0
        and -5.0 <= ret20 <= 20.0 and entry >= 68 and risk >= 60
        and amount >= 150 and chase <= 55
    )
    breakout_setup = (
        breakout_gap <= 2.50 and -1.5 <= ret5 <= 5.5 and -5.0 <= ret20 <= 20.0
        and entry >= 68 and risk >= 59 and amount >= 200 and chase <= 60
        and (vol_ratio == 0 or vol_ratio >= 0.80)
    )

    # 已有突破結構時，若守價就在現價附近，先等回測守價，避免追到當日高檔。
    if guard["ready"] and (momentum["radar_ready"] or prebreak["radar_ready"] or breakout_setup):
        path = "觸發守價回測"
        nearest_gap = guard["gap"]
    elif momentum["radar_ready"]:
        path = "動能突破確認"
        nearest_gap = max(0.0, -momentum["breakout20"])
    elif prebreak["radar_ready"]:
        path = "強勢前兆待觸發"
        nearest_gap = max(0.0, _num(row, "觸發距離%", _num(row, "距20日高點%", 3.0)))
    elif pullback_setup and pullback_gap <= breakout_gap:
        path = "回測承接"
        nearest_gap = pullback_gap
    elif breakout_setup:
        path = "突破確認"
        nearest_gap = breakout_gap
    elif pullback_gap <= breakout_gap and pullback_gap < 99:
        path = "等待回測承接"
        nearest_gap = pullback_gap
    else:
        path = "等待突破確認"
        nearest_gap = breakout_gap

    if path == "觸發守價回測" and guard["reference"] > 0:
        primary_reference = guard["reference"]
    elif "回測" in path and pullback > 0:
        primary_reference = pullback
    elif breakout > 0:
        primary_reference = breakout
    else:
        primary_reference = pullback

    rr_score = _clamp((rr - 0.6) * 55.0, 0, 100)
    stop_score = 95 if 0 < stop <= 4.5 else 84 if stop <= 6.5 else 65 if stop <= 8.0 else 35 if stop <= 10.0 else 10
    proximity_score = 100 if nearest_gap <= 0.8 else 90 if nearest_gap <= 1.5 else 78 if nearest_gap <= 2.5 else 58 if nearest_gap <= 4 else 25
    liquidity_score = 100 if amount >= 800 else 88 if amount >= 300 else 75 if amount >= 150 else 35
    score = entry * 0.20 + risk * 0.16 + buy * 0.08 + practical * 0.08 + rr_score * 0.18 + stop_score * 0.14 + proximity_score * 0.10 + liquidity_score * 0.03 + (100 - chase) * 0.03
    if momentum["radar_ready"]:
        score = max(score, momentum["score"] * 0.70 + risk * 0.12 + practical * 0.10 + liquidity_score * 0.08)
    if guard["ready"]:
        score = max(score, entry * 0.22 + risk * 0.18 + practical * 0.10 + proximity_score * 0.20 + liquidity_score * 0.10 + (100 - chase) * 0.10 + stop_score * 0.10)

    blockers: list[str] = []
    if not freshness["known"]:
        blockers.append("K線日期未驗證，須重新推薦")
    elif not freshness["fresh"]:
        blockers.append(f"行情落後{freshness['lag']}個交易日")

    base_ready = bool(
        (pullback_setup or breakout_setup or guard["ready"]) and freshness["fresh"] and rr >= 1.45
        and 0 < stop <= 6.8 and entry >= 68 and risk >= 59 and amount >= 150 and chase <= 60
    )
    momentum_ready = bool(momentum["radar_ready"] and freshness["fresh"])
    prebreak_ready = bool(prebreak["radar_ready"] and freshness["fresh"])
    guard_ready = bool(guard["ready"])

    if not momentum_ready and not prebreak_ready and not guard_ready:
        if not (pullback_setup or breakout_setup):
            if nearest_gap > 2.5:
                blockers.append(f"距最近可執行買點仍有{nearest_gap:.1f}%")
            if pullback_broken:
                blockers.append("現價已跌破回測承接區")
        if rr < 1.45:
            blockers.append(f"實戰RR僅{rr:.2f}，低於1.45")
        if stop <= 0 or stop > 6.8:
            blockers.append(f"停損距離{stop:.1f}%不符合0~6.8%")
        if entry < 68:
            blockers.append("Entry買點分不足68")
        if risk < 59:
            blockers.append("Risk風控分不足59")
        if amount < 150:
            blockers.append("成交額不足1.5億元")
        if chase > 60:
            blockers.append("追價風險偏高")
    elif momentum["blockers"] and not guard_ready:
        blockers.append(momentum["blockers"])

    if base_ready and market["severe"]:
        status = "MARKET-WAIT｜個股接近買點但大盤禁止"
        blockers.append("大盤風控為嚴重/紅燈")
        ready = False
    elif base_ready:
        status = "READY｜接近可執行買點"
        ready = True
    elif guard_ready and market["severe"]:
        status = "MARKET-WAIT-G｜守價回測成立但大盤限制"
        blockers.append("大盤紅燈：守價回測僅列條件雷達")
        ready = False
    elif guard_ready:
        status = "READY-G｜觸發守價回測"
        ready = False  # 核心雷達路徑；是否升級正式/A-仍由 RR/Risk 決定。
    elif momentum_ready and market["severe"]:
        status = "MARKET-WAIT-M｜強勢動能成立但大盤禁止追價"
        blockers.append("大盤紅燈：只列強勢動能雷達")
        ready = False
    elif momentum_ready:
        status = "READY-M｜強勢動能條件進場"
        ready = False
    elif prebreak_ready and market["severe"]:
        status = "MARKET-WAIT-P｜強勢前兆成立但大盤禁止追價"
        blockers.append("大盤紅燈：只列強勢前兆雷達")
        ready = False
    elif prebreak_ready:
        status = "READY-P｜強勢前兆條件雷達"
        ready = False
    elif not freshness["fresh"] and prebreak["radar_ready"]:
        status = "DATA-WAIT-P｜強勢前兆成立但K線待更新"
        blockers.append("強勢前兆保留；更新最新K線後才可判斷進場")
        ready = False
    elif freshness["fresh"] and (pullback_gap <= 4.0 or breakout_gap <= 4.0) and rr >= 1.15 and stop <= 8.5:
        status = "WATCH｜接近買點但條件未齊"
        ready = False
    else:
        status = "BLOCK｜尚非可進場型態"
        ready = False

    if blockers and not momentum_ready and not guard_ready:
        score -= min(35.0, 5.0 * len(blockers))
    if market["severe"]:
        score -= 8.0
    return {
        "score": round(_clamp(score), 1), "status": status, "ready": ready,
        "momentum_ready": momentum_ready, "prebreak_ready": prebreak_ready,
        "guard_retest_ready": guard_ready, "path": path,
        "nearest_gap": round(nearest_gap, 2), "breakout_gap": round(breakout_gap, 2),
        "pullback_gap": round(pullback_gap, 2), "reasons": "、".join(dict.fromkeys(blockers)),
        "freshness": freshness, "primary_reference": round(primary_reference, 4) if primary_reference else 0.0,
        "pullback_reference": round(pullback, 4) if pullback else 0.0,
        "breakout_reference": round(breakout, 4) if breakout else 0.0,
        "guard_reference": guard["reference"], "guard_gap": guard["gap"],
        "guard_status": guard["status"], "guard_reasons": guard["reasons"],
    }

def _direct_ok(row: pd.Series, op_score: float, exclusion: list[str], promotion: dict[str, Any] | None = None) -> bool:
    promotion = promotion or _promotion_profile(row, op_score, exclusion)
    if promotion["formal"]:
        return True
    profile = _next_session_profile(row)
    readiness = _entry_readiness_profile(row)
    if exclusion or not readiness["ready"] or readiness["score"] < 82 or not (profile["strong_ok"] or readiness["score"] >= 86):
        return False
    role_blob = _text_blob(row, ["推薦角色", "飆股雷達角色", "領漲回補角色", "主流作戰分區"])
    liq = _liquidity_info(row)
    amount = liq["amount"] if liq["amount"] > 0 else liq["avg_amount"]
    buy = _num(row, "買進分數", 0)
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    rr = _risk_reward_ratio(row)
    chase = _chase_risk_score(row, 55)
    mainstream = _num(row, "主流資金分", 0)
    sector = max(_num(row, "族群攻擊強度", 0), _num(row, "族群輪動分", 0))
    practical = _num(row, "股神實戰總分", 0)
    candidate = max(_num(row, "推薦總分", 0), _num(row, "候選強度分", 0), _num(row, "Alpha選股潛力分", 0))
    stop_dist = _stop_distance_pct(row)
    upside = _upside_space_pct(row)
    market = _market_risk_info(row)
    strong_role = _contains_any(role_blob, ["A｜股神主推薦", "S｜飆股攻擊候選", "主流攻擊候選", "B+｜盤中點火追蹤", "L｜主流強勢回補"])
    metric_override = candidate >= 78 and practical >= 70 and entry >= 68 and risk >= 64
    return (
        op_score >= 68
        and practical >= 64
        and buy >= 55
        and entry >= 62
        and risk >= 58
        and rr >= 1.20
        and chase <= 62
        and (stop_dist <= 10.5 or stop_dist == 0)
        and upside >= 7
        and mainstream >= 58
        and sector >= 50
        and liq["tradable"]
        and amount >= 200
        and not market["severe"]
        and (strong_role or metric_override)
    )


def _a_minus_ok(row: pd.Series, op_score: float, exclusion: list[str], promotion: dict[str, Any] | None = None) -> bool:
    """A- 準主推薦：只允許盤中觸發後小量試單，不可當成直接買進。"""
    promotion = promotion or _promotion_profile(row, op_score, exclusion)
    if promotion["a_minus"]:
        return True
    profile = _next_session_profile(row)
    readiness = _entry_readiness_profile(row)
    if not readiness["ready"] or readiness["score"] < 74:
        return False
    severe_words = [
        "過熱", "禁買", "低流動性", "冷門", "成交額不足", "買進分數過低",
        "追價風險過高", "Entry/Risk 同時偏弱", "停損距離", "上方空間", "假強",
    ]
    if any(any(key in reason for key in severe_words) for reason in exclusion):
        return False
    role_blob = _text_blob(row, ["推薦角色", "飆股雷達角色", "領漲回補角色", "回放校正角色", "主流作戰分區"])
    liq = _liquidity_info(row)
    amount = liq["amount"] if liq["amount"] > 0 else liq["avg_amount"]
    buy = _num(row, "買進分數", 0)
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    rr = _risk_reward_ratio(row)
    chase = _chase_risk_score(row, 55)
    mainstream = _num(row, "主流資金分", 0)
    sector = max(_num(row, "族群攻擊強度", 0), _num(row, "族群輪動分", 0))
    radar = max(
        _num(row, "爆發雷達分", 0), _num(row, "隔日爆發分", 0),
        _num(row, "飆股攻擊分", 0), _num(row, "主流領漲回補分", 0),
        _num(row, "漲停回放分", 0),
    )
    stop_dist = _stop_distance_pct(row)
    upside = _upside_space_pct(row)
    market = _market_risk_info(row)
    has_attack_role = _contains_any(role_blob, [
        "A｜股神主推薦", "S｜飆股攻擊候選", "B+｜盤中點火追蹤", "L｜主流強勢回補",
        "T｜題材轉強追蹤", "B｜等突破確認", "主流突破追蹤", "主流攻擊候選",
    ])
    return (
        has_attack_role
        and op_score >= 55
        and buy >= 45
        and entry >= 52
        and risk >= 44
        and rr >= 1.45
        and chase <= 60
        and (0 < stop_dist <= 6.8)
        and upside >= 5
        and mainstream >= 56
        and sector >= 52
        and radar >= 64
        and liq["tradable"]
        and amount >= 150
        and not market["severe"]
    )


def _intraday_radar_ok(row: pd.Series, op_score: float, exclusion: list[str]) -> bool:
    profile = _next_session_profile(row)
    readiness = _entry_readiness_profile(row)
    momentum = _momentum_profile(row)
    prebreak = _prebreakout_profile(row)
    if readiness["status"].startswith("BLOCK") and not momentum["radar_ready"] and not prebreak["radar_ready"]:
        return False
    if not (profile["reference_ok"] or readiness["score"] >= 68 or momentum["radar_ready"] or prebreak["radar_ready"]):
        return False
    if _safe_str(row.get("市場別")).replace(" ", "") in {"興櫃", "Emerging"}:
        return False
    if any("過熱" in r or "禁買" in r for r in exclusion) and not momentum["radar_ready"] and not prebreak["radar_ready"]:
        return False
    if any("低流動性" in r for r in exclusion):
        return False
    role_blob = _text_blob(row, ["推薦角色", "飆股雷達角色", "領漲回補角色", "回放校正角色", "主流作戰分區"])
    liq = _liquidity_info(row)
    amount = liq["amount"] if liq["amount"] > 0 else liq["avg_amount"]
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    buy = _num(row, "買進分數", 0)
    rr = _risk_reward_ratio(row)
    chase = _chase_risk_score(row, 55)
    mainstream = _num(row, "主流資金分", 0)
    sector = _num(row, "族群攻擊強度", 0)
    radar = max(
        _num(row, "爆發雷達分", 0),
        _num(row, "隔日爆發分", 0),
        _num(row, "飆股攻擊分", 0),
        _num(row, "主流領漲回補分", 0),
        _num(row, "漲停回放分", 0),
    )
    has_attack_role = _contains_any(role_blob, ["B+｜盤中點火追蹤", "S｜飆股攻擊候選", "M｜強勢漏選追蹤", "M+｜漲停漏選回放", "L｜主流強勢回補", "T｜題材轉強追蹤", "B｜等突破確認"])
    strength = _num(row, "強勢股漏選風險分", 0)
    replay = _num(row, "漲停回放分", 0)
    ret5 = _num(row, "近5日漲幅%", 0)
    momentum_radar = bool(
        momentum["radar_ready"] and liq["tradable"] and amount >= 100
        and op_score >= 35 and risk >= 30 and buy >= 20
    )
    strong_replay_radar = (
        strength >= 92
        and replay >= 70
        and amount >= 300
        and mainstream >= 60
        and sector >= 68
        and radar >= 68
        and chase <= 70
        and ret5 <= 12
        and op_score >= 42
        and has_attack_role
    )
    return (
        (
            has_attack_role
            and liq["tradable"]
            and amount >= 150
            and mainstream >= 55
            and radar >= 68
            and sector >= 48
            and op_score >= 48
            and entry >= 40
            and risk >= 36
            and buy >= 25
            and chase <= 76
            and (rr >= 0.45 or buy >= 45 or entry >= 55)
        )
        or strong_replay_radar
        or momentum_radar
        or (prebreak["radar_ready"] and liq["tradable"] and amount >= 100 and op_score >= 30)
    )


def _official_factor_limited(row: pd.Series) -> bool:
    score = max(
        _num(row, "官方資料完整度", 0),
        _num(row, "官方因子完整度", 0),
        _num(row, "官方因子覆蓋率", 0),
    )
    blob = _text_blob(row, ["資料完整度", "官方因子資料狀態", "官方資料狀態", "流動性資料來源"])
    if score >= 50 or _contains_any(blob, ["官方完整", "官方資料成功", "完整串聯"]):
        return False
    return score <= 0 or _contains_any(blob, ["代理估算", "未串聯", "缺少", "部分", "待補"])


def _objective_metrics(row: pd.Series) -> dict[str, float]:
    return {
        "buy": _num(row, "買進分數", 0),
        "entry": _num(row, "Entry進場買點分", _num(row, "進場買點分", 0)),
        "risk": _num(row, "Risk風控安全分", _num(row, "風控安全分", 0)),
        "practical": _num(row, "股神實戰總分", 0),
        "rr": _risk_reward_ratio(row),
        "stop": _stop_distance_pct(row),
        "upside": _upside_space_pct(row),
        "amount": _reference_turnover_m(row),
        "ret5": _num(row, "近5日漲幅%", 0),
        "ret20": _num(row, "近20日漲幅%", 0),
        "tech": _num(row, "技術結構分數", 0),
        "pre": _num(row, "起漲前兆分數", 0),
        "trade": _num(row, "交易可行分數", 0),
        "radar": max(_num(row, "爆發雷達分", 0), _num(row, "隔日爆發分", 0)),
        "chase": _chase_risk_score(row, 55),
    }


def _objective_severe_block(exclusion: list[str]) -> bool:
    severe = ["過熱", "禁買", "低流動性", "買進分數過低", "Entry/Risk", "追價風險", "停損距離", "上方空間", "假強"]
    return any(any(key in reason for key in severe) for reason in exclusion)


def _objective_direct_ok(row: pd.Series, op_score: float, exclusion: list[str]) -> bool:
    profile = _next_session_profile(row)
    readiness = _entry_readiness_profile(row)
    if exclusion or not readiness["ready"] or readiness["score"] < 82 or not (profile["strong_ok"] or readiness["score"] >= 86) or _market_risk_info(row)["severe"] or not _official_factor_limited(row):
        return False
    if _safe_str(row.get("市場別")) in {"興櫃", "Emerging"}:
        return False
    m = _objective_metrics(row)
    return (
        op_score >= 66 and m["buy"] >= 80 and m["entry"] >= 70 and m["risk"] >= 64
        and m["practical"] >= 62 and m["rr"] >= 2.0 and 0 < m["stop"] <= 7.0
        and m["upside"] >= 8.0 and m["amount"] >= 300 and -4 <= m["ret5"] <= 6
        and -2 <= m["ret20"] <= 18 and m["trade"] >= 54 and m["chase"] <= 65
    )


def _objective_a_minus_ok(row: pd.Series, op_score: float, exclusion: list[str]) -> bool:
    # 大盤紅燈時只保留真正具有隔日優勢型態的少數 A-，不可用高總分硬湊。
    profile = _next_session_profile(row)
    readiness = _entry_readiness_profile(row)
    if _market_risk_info(row)["severe"]:
        return False
    if not readiness["ready"] or readiness["score"] < 74:
        return False
    if _objective_severe_block(exclusion):
        return False
    if _safe_str(row.get("市場別")) in {"興櫃", "Emerging"}:
        return False
    m = _objective_metrics(row)
    return (
        op_score >= 61 and m["buy"] >= 74 and m["entry"] >= 67.5 and m["risk"] >= 60
        and m["practical"] >= 58 and m["rr"] >= 1.45 and 0 < m["stop"] <= 6.8
        and m["upside"] >= 5.0 and m["amount"] >= 150 and -5 <= m["ret5"] <= 8
        and -5 <= m["ret20"] <= 22 and m["trade"] >= 50 and m["chase"] <= 70
        and (m["tech"] >= 45 or m["pre"] >= 55)
    )


def _objective_intraday_ok(row: pd.Series, op_score: float, exclusion: list[str]) -> bool:
    # 雷達採雙路徑：傳統可量化買點，或強勢放量突破條件雷達。
    profile = _next_session_profile(row)
    readiness = _entry_readiness_profile(row)
    momentum = _momentum_profile(row)
    if readiness["status"].startswith("BLOCK") and not momentum["radar_ready"]:
        return False
    if not (profile["reference_ok"] or readiness["score"] >= 68 or momentum["radar_ready"]):
        return False
    if _safe_str(row.get("市場別")).replace(" ", "") in {"興櫃", "Emerging"}:
        return False
    if _objective_severe_block(exclusion) and not momentum["radar_ready"]:
        return False
    m = _objective_metrics(row)
    if momentum["radar_ready"]:
        return bool(m["amount"] >= 100 and op_score >= 35 and m["risk"] >= 30 and m["buy"] >= 20)
    return (
        op_score >= 57 and m["buy"] >= 70 and m["entry"] >= 65 and m["risk"] >= 56
        and m["practical"] >= 53 and m["rr"] >= 1.45 and 0 < m["stop"] <= 6.8
        and m["upside"] >= 4.0 and m["amount"] >= 100 and -6 <= m["ret5"] <= 9
        and -8 <= m["ret20"] <= 25 and m["trade"] >= 48 and m["chase"] <= 74
        and (m["tech"] >= 48 or m["pre"] >= 55 or m["radar"] >= 55)
    )


def _risk_radar_ok(row: pd.Series, op_score: float) -> bool:
    role_blob = _text_blob(row, ["飆股雷達角色", "領漲回補角色", "回放校正角色", "主流作戰分區"])
    radar = max(_num(row, "爆發雷達分", 0), _num(row, "隔日爆發分", 0), _num(row, "主流領漲回補分", 0), _num(row, "漲停回放分", 0))
    liq = _liquidity_info(row)
    amount = liq["amount"] if liq["amount"] > 0 else liq["avg_amount"]
    return liq["known"] and amount >= 100 and radar >= 65 and op_score >= 38 and _contains_any(role_blob, ["R｜高風險爆發觀察", "B+｜盤中點火追蹤", "S｜飆股攻擊候選", "T｜題材轉強追蹤", "L｜主流強勢回補", "M｜強勢漏選追蹤"])


def _strategic_replay_radar_ok(row: pd.Series, op_score: float, reasons: list[str]) -> bool:
    """把「過熱但主流資金/族群仍強」的股票留在高風險雷達，而不是直接消失。

    6/18 回放：南茂前一晚被正式排除，但隔日漲約 9.6%。原因是角色帶有
    過熱/禁買字樣後直接進排除，沒有再保留到高風險雷達。此函式只讓它
    回到「高風險雷達觀察」，不升級成正式推薦，也不給直接買進。
    """
    role_blob = _text_blob(row, ["飆股雷達角色", "領漲回補角色", "回放校正角色", "主流作戰分區", "推薦角色", "穩健推薦角色"])
    if not _contains_any(role_blob, ["S+｜漲停雷達", "S｜飆股攻擊候選", "L+｜領漲回補雷達", "L｜主流強勢回補", "M+｜漲停漏選回放", "M｜強勢漏選追蹤"]):
        return False
    liq = _liquidity_info(row)
    amount = liq["amount"] if liq["amount"] > 0 else liq["avg_amount"]
    buy = _num(row, "買進分數", 0)
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    chase = _chase_risk_score(row, 55)
    rr = _risk_reward_ratio(row)
    mainstream = _num(row, "主流資金分", 0)
    sector = max(_num(row, "族群攻擊強度", 0), _num(row, "族群輪動分", 0))
    strength = _num(row, "強勢股漏選風險分", 0)
    replay = _num(row, "漲停回放分", 0)
    radar = max(_num(row, "爆發雷達分", 0), _num(row, "隔日爆發分", 0), _num(row, "主流領漲回補分", 0), replay)
    # 嚴重假強仍應排除：買進/Entry/Risk 太差、追價過高、RR 極差。
    if buy < 25 or entry < 38 or risk < 39 or chase >= 78 or rr < 0.18:
        return False
    if amount < 250 or mainstream < 62 or sector < 70 or radar < 76:
        return False
    if strength < 92 and replay < 74:
        return False
    # 只允許被「角色/過熱字樣」擋掉的強勢雷達回到觀察；低流動性/買點崩壞不救。
    severe = [r for r in reasons if any(k in r for k in ["低流動性", "成交額不足", "買進分數過低", "Entry/Risk", "追價風險過高", "風險報酬比過低"])]
    return not severe and op_score >= 43


def _mainstream_mainrise_radar_ok(row: pd.Series, profile: dict[str, Any], reasons: list[str]) -> bool:
    """主流主升股票即使買點尚未成熟，也必須留在可見雷達，不得被冷門股淹沒。"""
    fresh = _history_freshness_info(row)
    liq = _liquidity_info(row)
    if not fresh.get("fresh") or not liq.get("tradable"):
        return False
    if _safe_str(row.get("市場別")).replace(" ", "") in {"興櫃", "Emerging"}:
        return False
    if not profile.get("mainstream_ok") or _safe_float(profile.get("score"), 0) < 72:
        return False
    hard_words = ["低流動性", "冷門股", "K線過期", "資料待更新", "硬性禁買", "重大風險", "財務異常"]
    if any(any(word in reason for word in hard_words) for reason in reasons):
        return False
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    return bool(risk >= 30 and _safe_float(profile.get("amount"), 0) >= 300)


def _trigger_confirm_text(trig: dict[str, Any]) -> str:
    final = trig.get("final", 0) or 0
    hold = _support_after_trigger(final)
    if final and hold:
        return f"放量站上 {final} 後，至少守住 {hold}；若只碰價後跌回，視為假突破。"
    return "放量突破後需站穩，不可只因瞬間碰價追買。"


def _gap_plan_text(trig: dict[str, Any]) -> str:
    final = trig.get("final", 0) or 0
    if final:
        gap = _round_up_to_tick(float(final) * 1.02)
        hold = _support_after_trigger(final)
        return f"若開盤高於 {gap}，不直接追；等回測不破 {hold} 或第二波放量再評估。"
    return "若開盤跳空急拉，不直接追，等回測守穩或第二波放量。"


def _hit_tag_for(bucket: str, row: pd.Series) -> str:
    strength = _num(row, "強勢股漏選風險分", 0)
    replay = _num(row, "漲停回放分", 0)
    if bucket == "盤中雷達追蹤" and strength >= 92 and replay >= 70:
        return "命中型雷達｜保留，但要加守價確認"
    if bucket == "高風險雷達觀察" and (strength >= 92 or replay >= 74):
        return "錯殺回補型｜不買，列高風險雷達"
    if bucket == "正式排除清單" and replay >= 80:
        return "高分排除型｜維持排除但列回放檢討"
    return "一般分流"


def _trigger_text(row: pd.Series, trig: dict[str, Any] | None = None) -> str:
    if trig is None:
        trig = _trigger_info(row)
    v = trig.get("final", 0)
    if v:
        return f"放量站上實戰觸發價 {v} 且同族群維持強勢"
    return "放量突破前高/壓力並站穩，未觸發前不買"


def _battle_meta_for(bucket: str) -> dict[str, Any]:
    if bucket == "正式下週主推薦":
        return {"zone": "1｜正式主推薦", "prio": 10, "hint": "可作為第一優先清單；仍須分批、觸發價與停損紀律。", "sheet": "正式下週主推薦"}
    if bucket == "A-｜準主推薦小量試單":
        return {"zone": "2｜A-準主推薦", "prio": 20, "hint": "接近正式推薦但尚未完全過關；只允許小量試單，必須盤中觸發並守價。", "sheet": "準主推薦小量試單"}
    if bucket == "盤中雷達追蹤":
        return {"zone": "3｜盤中觸發追蹤", "prio": 30, "hint": "不是預先買進清單；只在實戰觸發價放量站上且守價後評估。", "sheet": "盤中雷達追蹤"}
    if bucket == "高風險雷達觀察":
        return {"zone": "4｜高風險觀察", "prio": 40, "hint": "有爆發訊號但風險偏高；只看不追，等待下一次買點修復。", "sheet": "高風險雷達觀察"}
    if bucket == "正式排除清單":
        return {"zone": "9｜禁止買進/排除", "prio": 90, "hint": "過熱、低流動、買點或風控不足；不得列入作戰買進。", "sheet": "正式排除清單"}
    return {"zone": "5｜不可直接買觀察", "prio": 50, "hint": "資料保留觀察，非下週作戰主軸。", "sheet": "不可直接買觀察"}


def _entry_action_text(readiness: dict[str, Any], trig: dict[str, Any], *, small: bool = True) -> str:
    prefix = "只允許小量試單；" if small else "可分批執行；"
    path = _safe_str(readiness.get("path"))
    pullback = _safe_float(readiness.get("pullback_reference"), 0)
    breakout = _safe_float(readiness.get("breakout_reference"), 0)
    guard = _safe_float(readiness.get("guard_reference"), 0)
    if "守價回測" in path and guard > 0:
        lower = _round_up_to_tick(guard * 0.985)
        reclaim = _round_up_to_tick(guard * 1.002)
        alt = f"；若未回測，只在再次放量突破 {breakout} 且收盤守價後評估" if breakout > 0 else ""
        return f"{prefix}優先等回測觸發後守價 {lower}～{guard}，量縮不破並重新站回 {reclaim} 才進場{alt}。未成立前不買。"
    if "回測" in path and pullback > 0:
        lower = _round_up_to_tick(pullback * 0.985)
        reclaim = _round_up_to_tick(pullback * 1.005)
        alt = f"；若未回測，僅在放量突破 {breakout} 且守價後評估" if breakout > 0 else ""
        return f"{prefix}優先等回測 {lower}～{pullback}，量縮不破並重新站回 {reclaim} 才進場{alt}。未出現任一路徑前不買。"
    return f"{prefix}{_trigger_text(pd.Series(dtype=object), trig)}，且必須守住觸發後守價；未觸發前不買。"


def _classify(row: pd.Series) -> dict[str, Any]:
    op = _compute_operability_score(row)
    next_profile = _next_session_profile(row)
    readiness = _entry_readiness_profile(row)
    momentum = _momentum_profile(row)
    prebreak = _prebreakout_profile(row)
    mainrise = _mainstream_mainrise_profile(row)
    trigger_quality = _nextday_trigger_quality_profile(row)
    reasons = _exclusion_reasons(row)
    mainrise_radar = _mainstream_mainrise_radar_ok(row, mainrise, reasons)
    trig = _trigger_info(row)
    promotion = _promotion_profile(row, op, reasons)
    direct_primary = _direct_ok(row, op, reasons, promotion)
    direct_objective = False if direct_primary else _objective_direct_ok(row, op, reasons)
    direct = direct_primary or direct_objective
    a_primary = False if direct else _a_minus_ok(row, op, reasons, promotion)
    a_objective = False if (direct or a_primary) else _objective_a_minus_ok(row, op, reasons)
    a_minus = a_primary or a_objective
    intraday_primary = _intraday_radar_ok(row, op, reasons)
    intraday_objective = False if (direct or a_minus or intraday_primary) else _objective_intraday_ok(row, op, reasons)
    intraday = intraday_primary or intraday_objective
    market_info = _market_risk_info(row)
    official_freshness = _official_factor_freshness_info(row)
    combined_freshness = _combined_data_freshness_info(row)
    red_fragility = _red_market_trigger_fragility_profile(row)
    decision_source = (
        promotion["route"] if direct_primary and promotion["formal"] else
        "完整因子正式門檻" if direct_primary else
        "客觀量價備援正式門檻" if direct_objective else
        promotion["route"] if a_primary and promotion["a_minus"] else
        "完整因子A-門檻" if a_primary else
        ("防守市場客觀量價A-" if a_objective and market_info["severe"] else "客觀量價備援A-門檻") if a_objective else
        "強勢動能條件雷達" if intraday and momentum["radar_ready"] else
        "強勢前兆召回雷達" if intraday and prebreak["radar_ready"] else
        "完整因子盤中雷達" if intraday_primary else
        ("防守市場客觀量價雷達" if intraday_objective and market_info["severe"] else "客觀量價備援盤中雷達") if intraday_objective else
        "一般風控分流"
    )
    risk_radar = _risk_radar_ok(row, op)
    role_blob = _text_blob(row, ["推薦角色", "飆股雷達角色", "主流作戰分區"])

    if direct:
        bucket = "正式下週主推薦"
        qual = "PASS｜可列正式推薦" if direct_primary else "PASS-Q｜客觀量價條件通過"
        direct_buy = "可｜但仍需分批與停損"
        if promotion.get("formal_momentum"):
            action = f"正式動能條件推薦；{momentum['entry']} 第一筆不超過建議倉位，跌破點火結構或失效條件立即退出。"
        else:
            action = _entry_action_text(readiness, trig, small=False)
        radar_level = "主攻"
        radar_action = "正式推薦優先追蹤"
        exclude_text = ""
    elif a_minus:
        bucket = "A-｜準主推薦小量試單"
        if market_info["severe"] and promotion.get("red_override"):
            qual = "A-R｜紅燈逆勢反轉條件推薦"
            direct_buy = "極小量｜觸發、守價且大盤跌勢未惡化"
            action = (
                "不可開盤追價或預掛買進；只在實戰觸發價被放量突破、收盤/盤中守住觸發後守價，"
                "且大盤跌幅未持續擴大時，單檔最多3%試單。跌破守價立即取消。"
            )
            radar_level = "A-R｜紅燈逆勢反轉"
            radar_action = "觸發與守價同時成立才極小量；未觸發視為沒有交易"
            exclude_text = ""
        elif market_info["severe"]:
            qual = "A-MD｜個股達標但大盤封鎖"
            direct_buy = "不可｜等大盤解除紅燈"
            underlying_action = (
                momentum["entry"] if promotion.get("formal_momentum") or promotion.get("a_momentum")
                else prebreak["entry"] if promotion.get("a_prebreak")
                else _entry_action_text(readiness, trig, small=True)
            )
            action = (
                f"個股已達{promotion.get('market_blocked_from') or 'A-候選'}資格，但大盤紅燈/全面防守；"
                f"目前建議倉位0%，不得建立新倉。待大盤解除封鎖後再依下列條件重新確認：{underlying_action}"
            )
            radar_level = "A-｜大盤解禁候選"
            radar_action = "保留A-資格，等待大盤解除紅燈後再做盤中觸發確認"
            exclude_text = "大盤紅燈/全面防守：個股保留A-資格，但目前禁止建立新倉"
        else:
            qual = "A-｜接近主推薦，待盤中觸發" if a_primary else "A-Q｜客觀量價備援，待盤中觸發"
            direct_buy = "小量｜需觸發與守價"
            action = (
                momentum["entry"] if promotion.get("a_momentum")
                else prebreak["entry"] if promotion.get("a_prebreak")
                else _entry_action_text(readiness, trig, small=True)
            )
            radar_level = "A-｜準主推薦"
            radar_action = "小量試單優先追蹤，不可重倉"
            exclude_text = "未完全通過正式主推薦 RR/Risk 門檻，降為 A- 準主推薦"
    elif intraday:
        bucket = "盤中雷達追蹤"
        if momentum["radar_ready"]:
            qual = "WAIT-MD｜防守市場強勢動能" if market_info["severe"] else "WAIT-M｜強勢動能條件進場"
            direct_buy = "不可｜大盤紅燈只盯盤" if market_info["severe"] else "條件式｜不可開盤追價"
            action = momentum["entry"] if not market_info["severe"] else f"大盤紅燈只保留強勢股雷達；{momentum['entry']}"
            radar_level = momentum["role"]
            radar_action = "首波回測守住或再突破放量才小量試單；開高急拉不追"
            exclude_text = ""
        elif prebreak["radar_ready"]:
            if not prebreak["fresh"]:
                qual = "DATA-WAIT-P｜強勢前兆成立但行情待更新"
                direct_buy = "不可｜先更新最新K線"
                action = f"列入強勢前兆雷達，但目前K線非最新交易日；先更新資料。更新後僅接受：{prebreak['entry']}"
            elif market_info["severe"] and promotion.get("red_reversal", {}).get("radar_override") and not market_info.get("panic"):
                qual = "WAIT-RD｜紅燈逆勢條件雷達"
                direct_buy = "極小量條件式｜觸發且守價"
                action = "紅燈市場不可預買；只有放量突破實戰觸發價、守住守價且大盤跌勢未惡化時，最多2%試單。"
            elif market_info["severe"]:
                qual = "WAIT-PD｜防守市場強勢前兆"
                direct_buy = "不可｜大盤紅燈只盯盤"
                action = f"大盤紅燈只保留前兆雷達；{prebreak['entry']}"
            elif prebreak.get("hot_risk"):
                qual = "HOT-WAIT-P｜高熱領漲監控，禁止直接追價"
                direct_buy = "不可追價｜只等充分回測後再突破"
                action = prebreak["entry"]
            else:
                qual = "WAIT-P｜強勢前兆條件雷達"
                direct_buy = "條件式｜突破/回測確認"
                action = prebreak["entry"]
            radar_level = "P+｜強勢前兆召回"
            radar_action = "不預買；突破前高或回測守價後才小量試單"
            exclude_text = ""
        else:
            qual = "WAIT｜未觸發不可買" if intraday_primary else ("WAIT-QD｜防守市場精選雷達" if market_info["severe"] else "WAIT-Q｜量價條件式雷達")
            direct_buy = "不可｜防守市場只盯盤" if (intraday_objective and market_info["severe"]) else "不可｜等盤中觸發"
            action = (f"防守市場只列精選雷達；大盤未改善前不買。{_trigger_text(row, trig)}，且大盤同步轉強後才重新評估。" if (intraday_objective and market_info["severe"]) else f"{_trigger_text(row, trig)}；未觸發前只盯盤，不預先買。")
            radar_level = "B+｜盤中點火追蹤"
            radar_action = "只在量價/族群同步確認後小量試單"
            exclude_text = ""
    elif not readiness["freshness"]["known"] or not readiness["freshness"]["fresh"]:
        bucket = "不可直接買觀察"
        qual = "DATA｜K線日期待更新"
        direct_buy = "不可｜先更新行情"
        stale_reason = readiness["reasons"] or "K線日期未驗證，須重新推薦"
        action = "目前價格與技術指標的交易日不一致或已過期；先補抓最新K線再重新評分，不得以舊價列入雷達或推薦。"
        radar_level = "資料待更新"
        radar_action = "不列核心雷達，不進場"
        exclude_text = stale_reason
    elif mainrise_radar:
        if mainrise.get("high_heat"):
            bucket = "高風險雷達觀察"
            qual = "MAIN-HOT｜主流主升高熱待回測"
            direct_buy = "不可追價｜只等回測/整理後再突破"
            action = mainrise.get("restriction") or "主流領漲但已過熱，只等回測守價。"
            radar_level = "LH｜主流主升高熱雷達"
            radar_action = "優先顯示但禁止追價；等待回測守價或整理後再突破"
            exclude_text = "主流主升成立，但追價/耗竭風險偏高，改列高熱主流雷達"
        else:
            bucket = "盤中雷達追蹤"
            qual = "MAIN｜主流主升條件雷達"
            direct_buy = "條件式｜觸發與守價確認"
            action = mainrise.get("restriction") or _entry_action_text(readiness, trig, small=True)
            radar_level = "L｜主流主升領漲雷達"
            radar_action = "主流優先盯盤；量價/族群同步且觸發守價後才小量"
            exclude_text = ""
        decision_source = "主流主升可見性召回"
    elif _strategic_replay_radar_ok(row, op, reasons):
        bucket = "高風險雷達觀察"
        qual = "RISK｜錯殺回補雷達"
        direct_buy = "不可｜高風險觀察"
        action = f"有隔日強勢回放特徵，但仍非正式買點；{_trigger_text(row, trig)}，且需守價確認。"
        radar_level = "R+｜錯殺回補雷達"
        radar_action = "只做盤中盯盤，不可開盤追價"
        exclude_text = "原本因過熱/風控文字被排除，Phase 6.9 改列高風險雷達觀察"
    elif reasons and _data_pending_only(reasons):
        bucket = "不可直接買觀察"
        qual = "DATA｜流動性資料待補"
        direct_buy = "不可｜待補成交額/成交量"
        action = "資料尚未足以判定可交易性；補齊成交額/成交量後重新評分，不得把缺值視為低流動性。"
        radar_level = "資料待補"
        radar_action = "只保留診斷，不進場"
        exclude_text = "、".join(reasons)
    elif reasons:
        bucket = "正式排除清單"
        qual = "BLOCK｜不列推薦"
        direct_buy = "不可"
        action = "不進場；等待過熱降溫、買點修復或下一輪重新掃描。"
        radar_level = "排除"
        radar_action = "不追價"
        exclude_text = "、".join(reasons)
    elif risk_radar:
        bucket = "高風險雷達觀察"
        qual = "RISK｜只看不買"
        direct_buy = "不可｜高風險觀察"
        action = "有爆發訊號但買點/風控不足，只能放雷達；若開高急拉不可追。"
        radar_level = "R｜高風險爆發觀察"
        radar_action = "僅供盯盤與回放檢討"
        exclude_text = "買點或風控尚未達正式推薦門檻"
    elif _contains_any(role_blob, ["C+｜早期潛伏", "早期潛伏"]):
        bucket = "早期潛伏觀察"
        qual = "EARLY｜小量觀察"
        direct_buy = "不可｜最多小量觀察"
        action = "只做小量觀察；需量能放大與 Entry/Risk 改善才升級。"
        radar_level = "潛伏"
        radar_action = "等待量價轉強"
        exclude_text = "尚未達正式推薦門檻"
    else:
        bucket = "不可直接買觀察"
        qual = "WATCH｜觀察不買"
        direct_buy = "不可"
        action = "只觀察，不主動買進；需重新轉強後再評估。"
        radar_level = "觀察"
        radar_action = "等待條件補強"
        exclude_text = "買點、風控或資金條件不足"

    # 紅燈高熱／脆弱突破不得因盤中碰價就升格；保留在雷達，但只等回測再突破。
    if red_fragility.get("block_breakout") and market_info.get("severe") and bucket != "正式排除清單":
        bucket = "高風險雷達觀察"
        qual = "BLOCK-F｜紅燈禁止碰價追突破"
        direct_buy = "不可｜只等回測守價後再突破"
        action = red_fragility.get("requirement") or "紅燈市場只等回測守價後再突破。"
        radar_level = "F｜紅燈假突破防守雷達"
        radar_action = "優先觀察但禁止碰價即買；需回測守價與二次確認"
        exclude_text = "紅燈高熱/脆弱結構：碰價突破容易反轉，改列防守雷達"
        decision_source = "紅燈假突破防守管制"
    elif red_fragility.get("two_stage") and market_info.get("severe"):
        action = f"{action}；{red_fragility.get('requirement', '')}".strip("；")
        radar_action = f"{radar_action}；需二段確認".strip("；")

    sort_score = op + next_profile["score"] * 0.12 + readiness["score"] * 0.18
    if bucket == "正式下週主推薦":
        sort_score += 20
    elif bucket == "A-｜準主推薦小量試單":
        sort_score += 14
    elif bucket == "盤中雷達追蹤":
        sort_score += 10
    elif bucket == "高風險雷達觀察":
        sort_score += 4
    elif bucket == "正式排除清單":
        sort_score -= 20
    battle = _battle_meta_for(bucket)
    final_meta = _final_action_meta(row, bucket, op, exclude_text, promotion)
    effective_readiness_score = readiness["score"]
    effective_readiness_status = readiness["status"]
    effective_readiness_reasons = readiness["reasons"]
    if bucket == "正式下週主推薦" and promotion["formal"]:
        effective_readiness_score = max(float(effective_readiness_score), 84.0)
        effective_readiness_status = "READY-F｜正式條件可執行"
        effective_readiness_reasons = ""
    elif bucket == "A-｜準主推薦小量試單" and promotion["a_minus"]:
        effective_readiness_score = max(float(effective_readiness_score), 76.0)
        effective_readiness_status = "READY-A｜A-條件可執行"
        effective_readiness_reasons = ""
    return {
        **final_meta,
        "主流主升優先分": mainrise.get("score", 0),
        "主流主升判定": mainrise.get("status", ""),
        "主流主升操作限制": mainrise.get("restriction", ""),
        "隔日觸發品質分": trigger_quality.get("score", 0),
        "隔日觸發品質判定": trigger_quality.get("status", ""),
        "隔日有效風控距離%": trigger_quality.get("effective_stop", 0),
        "隔日風控基準": trigger_quality.get("risk_basis", ""),
        "紅燈觸發脆弱度分": trigger_quality.get("fragility_score", red_fragility.get("score", 0)),
        "紅燈觸發管制": trigger_quality.get("fragility_status", red_fragility.get("status", "")),
        "盤中二段確認要求": trigger_quality.get("requirement", red_fragility.get("requirement", "")),
        "可操作分": round(op, 1),
        "正式推薦分區": bucket,
        "正式推薦資格": qual,
        "正式推薦動作": action,
        "下週是否可直接買": direct_buy,
        "準主推薦等級": "A-｜準主推薦" if bucket == "A-｜準主推薦小量試單" else "",
        "股神作戰區": battle["zone"],
        "股神作戰優先序": battle["prio"],
        "股神作戰提示": battle["hint"],
        "主要依據工作表": battle["sheet"],
        "盤中雷達等級": radar_level,
        "盤中雷達動作": radar_action,
        "盤中雷達優先級": "",
        "盤中盯盤順序": 0,
        "盤中雷達分層": "",
        "盤中雷達分層說明": "",
        "核心雷達品質檢查": "",
        "核心雷達降級原因": "",
        "正式推薦排除原因": exclude_text,
        "正式推薦排序分": round(_clamp(sort_score, 0, 120), 1),
        "原始觸發價": trig.get("raw", 0),
        "實戰觸發價": trig.get("final", 0),
        "觸發價偏離%": trig.get("dist", 0),
        "觸發價修正原因": trig.get("reason", ""),
        "隔日雷達回測判斷": _review_text_for(row, bucket, trig),
        "股神觸發修正建議": "正式推薦仍以 Entry/Risk/RR 為準；盤中雷達只在實戰觸發價放量站上、守住觸發後守價後小量試單。",
        "觸發後守價": _support_after_trigger(trig.get("final", 0)),
        "盤中觸發確認條件": f"{_trigger_confirm_text(trig)}；{trigger_quality.get('requirement', '')}".strip("；"),
        "開盤跳空處理": _gap_plan_text(trig),
        "隔日命中修正標籤": _hit_tag_for(bucket, row),
        "高風險雷達保留原因": exclude_text if bucket == "高風險雷達觀察" else "",
        "正式推薦判定來源": decision_source,
        "流動性參考成交額百萬": round(_reference_turnover_m(row), 1),
        "隔日可參考分": next_profile["score"],
        "隔日優勢型態": next_profile["pattern"],
        "隔日風險標記": next_profile["risk"],
        "隔日參考判定": "PASS｜可列隔日參考" if next_profile["reference_ok"] else "BLOCK｜不列核心參考",
        "觸發距離%": next_profile["trigger_dist"],
        "停損距離_隔日%": next_profile["stop_dist"],
        "進場可執行分": round(effective_readiness_score, 1),
        "進場可執行判定": effective_readiness_status,
        "進場路徑": readiness["path"],
        "距最近可執行買點%": readiness["nearest_gap"],
        "進場阻擋原因": effective_readiness_reasons,
        "主要進場路徑": readiness["path"],
        "主要進場參考價": readiness["primary_reference"],
        "回測承接參考價": readiness["pullback_reference"],
        "突破確認參考價": readiness["breakout_reference"],
        "守價回測參考價": readiness["guard_reference"],
        "守價回測距離%": readiness["guard_gap"],
        "隔日耗竭風險分": momentum["exhaustion_score"],
        "隔日耗竭風險等級": momentum["exhaustion_level"],
        "隔日可執行優先分": round(_clamp(effective_readiness_score * 0.62 + next_profile["score"] * 0.23 + (100 - momentum["exhaustion_score"]) * 0.15), 1),
        "進場績效計算口徑": "突破需觸價；收盤站上觸發價才算確認成功，僅守住守價屬待確認；收盤跌破守價為假突破。一般回測需收回支撐，未觸發不計交易勝負。",
        "推薦升級判定路徑": promotion["route"],
        "路徑風險報酬比": promotion["rr_used"],
        "風報比計算口徑": promotion["rr_basis"],
        "正式與A近門檻說明": promotion["near_reasons"],
        "強勢動能分": momentum["score"],
        "強勢動能判定": ("PASS｜" + momentum["role"]) if momentum["radar_ready"] else ("BLOCK｜" + (momentum["blockers"] or "未達動能門檻")),
        "動能進場條件": momentum["entry"],
        "動能風險控制": momentum["risk"],
        "強勢前兆分": prebreak["score"],
        "強勢前兆判定": prebreak["status"],
        "強勢前兆進場條件": prebreak["entry"],
        "強勢前兆風控": prebreak["risk"],
        "紅燈逆勢反轉分": promotion.get("red_reversal", {}).get("score", 0),
        "紅燈逆勢反轉判定": promotion.get("red_reversal", {}).get("status", ""),
        "大盤風控層級": market_info.get("level", ""),
        "官方因子資料日期": official_freshness.get("official_date", ""),
        "官方因子落後交易日": official_freshness.get("lag", 999),
        "官方因子新鮮度": official_freshness.get("status", ""),
        "大盤資料日期": market_info.get("market_date", ""),
        "大盤資料落後交易日": market_info.get("lag", 0),
        "大盤資料新鮮度": market_info.get("freshness", ""),
        "大盤與K線對齊狀態": market_info.get("alignment_status", ""),
        "股神資料總新鮮度": combined_freshness.get("status", ""),
        "股神資料警示": combined_freshness.get("warning", ""),
        "紅燈反轉首觸禁買": "是｜需二段確認" if promotion.get("red_reversal", {}).get("first_touch_fragile") else "否",
        "主流強勢替代進場": (
            "可｜平高盤後15分鐘站穩前收+1.5%、量價同步且回測不破"
            if red_fragility.get("leader_open_drive") else "否｜依原觸發/守價"
        ),
        "大盤條件覆寫": "是｜嚴格二段確認後極小量" if promotion.get("red_override") else "雷達條件式" if promotion.get("red_reversal", {}).get("radar_override") else "否",
        "逆勢操作限制": promotion.get("red_reversal", {}).get("restriction", ""),
        "K線最後交易日": readiness["freshness"]["last_date"],
        "K線落後交易日": readiness["freshness"]["lag"] if readiness["freshness"]["known"] else 999,
        "K線資料新鮮度": readiness["freshness"]["status"],
        "K線日期驗證基準": _safe_str(row.get("K線日期驗證基準")),
        "正式推薦版本": FORMAL_RECOMMENDATION_VERSION,
    }



def _priority_bucket_meta(row: pd.Series) -> tuple[str, float, float]:
    """回傳（用途標籤、加分、分數上限）；買進許可仍由正式分區獨立控制。"""
    bucket = _safe_str(row.get("正式推薦分區"))
    radar = _safe_str(row.get("盤中雷達優先級"))
    mainrise = _safe_float(row.get("主流主升優先分"), _mainstream_mainrise_profile(row)["score"])
    if bucket == "正式下週主推薦":
        return "1｜正式主推薦", 6.0, 100.0
    if bucket == "A-｜準主推薦小量試單":
        return "2｜A-準主推薦", 2.0, 94.0
    if bucket == "盤中雷達追蹤":
        if radar.startswith("R1-L"):
            return "3｜主流主升核心雷達", 4.5, 93.0
        if radar.startswith("R1-M"):
            return "4｜強勢動能核心雷達", 3.0, 90.0
        if radar.startswith("R1-P"):
            return "5｜強勢前兆核心雷達", 2.5, 89.0
        if radar.startswith("R1"):
            return "6｜盤中核心雷達", 2.0, 88.0
        if radar.startswith("R2-HOT") and mainrise >= 72:
            return "7｜主流高熱待回測", 0.0, 82.0
        if radar.startswith("R2-HOT"):
            return "8｜高熱動能待回測", -4.0, 70.0
        if radar.startswith("R2"):
            return "7｜盤中備援雷達", 0.0, 82.0 if mainrise >= 72 else 79.0
        return "9｜低優先雷達", -3.0, 72.0 if mainrise >= 72 else 69.0
    if bucket == "高風險雷達觀察":
        if mainrise >= 72:
            return "6｜主流主升高熱雷達", 0.5, 82.0
        return "10｜高風險只看不買", -8.0, 64.0
    if bucket == "早期潛伏觀察":
        return "11｜早期觀察", -4.0, 68.0
    if bucket == "不可直接買觀察":
        if mainrise >= 72:
            return "8｜主流條件待修復", -1.0, 76.0
        return "12｜一般觀察", -6.0, 63.0
    if bucket == "正式排除清單":
        return "13｜正式排除", -25.0, 39.0
    return "12｜一般觀察", -7.0, 60.0


def _unified_recommendation_priority_score(row: pd.Series) -> tuple[float, str, str, str]:
    """單一推薦優先分：提高主流資金、族群廣度與主升領漲的可見性。"""
    candidate = _clamp(max(
        _num(row, "推薦總分", 0), _num(row, "候選強度分", 0),
        _num(row, "Alpha選股潛力分", 0), _num(row, "股神實戰總分", 0),
    ))
    execution = _clamp(max(
        _num(row, "實戰操作品質分", 0), _num(row, "可操作分", 0), _num(row, "進場可執行分", 0),
    ))
    entry = _clamp(max(
        _num(row, "Entry進場買點分", 0), _num(row, "進場買點分", 0), _num(row, "買進分數", 0),
    ))
    risk = _clamp(max(_num(row, "Risk風控安全分", 0), _num(row, "風控安全分", 0)))
    route = _clamp(max(
        _num(row, "強勢動能分", 0), _num(row, "強勢前兆分", 0),
        _num(row, "隔日可參考分", 0), _num(row, "爆發雷達分", 0), _num(row, "隔日爆發分", 0),
    ))
    mainstream = _clamp(_num(row, "主流資金分", 50))
    sector_new = max(_num(row, "族群攻擊強度", 0), _num(row, "族群輪動分", 0))
    sector = _clamp(sector_new if sector_new > 0 else _num(row, "類股熱度分數", 0))
    mainrise_profile = _mainstream_mainrise_profile(row)
    mainrise = _clamp(_safe_float(row.get("主流主升優先分"), mainrise_profile["score"]))
    rr = _risk_reward_ratio(row)
    rr_score = _clamp(rr * 45.0)
    quality = _data_quality_score(row)
    exhaustion = _safe_float(row.get("隔日耗竭風險分"), _exhaustion_profile(row)["score"])
    actionable = _safe_float(row.get("隔日可執行優先分"), 0)
    trigger_quality = _safe_float(row.get("隔日觸發品質分"), _nextday_trigger_quality_profile(row)["score"])
    fragility_profile = _red_market_trigger_fragility_profile(row)
    fragility = _safe_float(row.get("紅燈觸發脆弱度分"), fragility_profile["score"])
    close_pos = _clamp(_num(row, "當日收盤位置%", 50))
    upper = _clamp(_num(row, "上影線比例%", 0))
    amount = _reference_turnover_m(row)
    liquidity_score = 100.0 if amount >= 3000 else 90.0 if amount >= 1200 else 80.0 if amount >= 500 else 68.0 if amount >= 200 else 55.0 if amount >= 100 else 30.0 if amount > 0 else 15.0

    score = (
        candidate * 0.11 + execution * 0.14 + entry * 0.09 + risk * 0.08
        + route * 0.12 + mainstream * 0.12 + sector * 0.09 + mainrise * 0.11
        + trigger_quality * 0.08 + rr_score * 0.02 + liquidity_score * 0.02
        + quality * 0.01 + actionable * 0.01
    )

    use_label, bucket_bonus, score_cap = _priority_bucket_meta(row)
    score += bucket_bonus
    # 高熱主流領漲只提升「看見順序」，不放寬買進許可。
    leader_visibility = bool(mainrise >= 80 and mainstream >= 78 and sector >= 75 and amount >= 500)
    if leader_visibility:
        score += 8.0
        use_label = "5｜主流高熱核心雷達"
    market_info = _market_risk_info(row)
    if not market_info.get("stale"):
        score += _clamp(_num(row, "隔日大盤預測加減分", 0), -6.0, 6.0)
    if market_info.get("severe"):
        score -= max(0.0, fragility - 25.0) * 0.10
        if fragility <= 25 and mainrise >= 78 and close_pos >= 72 and upper <= 25 and amount >= 800:
            score += 4.0
            use_label = "3｜紅燈逆勢主流領漲雷達"

    # 主流主升高熱仍要排前面，但買進許可維持禁止追價；只減少排序懲罰，不取消風控。
    heat_factor = 0.42 if mainrise >= 78 else 0.68 if mainrise >= 70 else 1.0
    chase = _chase_risk_score(row, 55)
    if exhaustion >= 70:
        score -= 12.0 * heat_factor
    elif exhaustion >= 55:
        score -= 8.0 * heat_factor
    elif exhaustion >= 35:
        score -= 3.5 * heat_factor
    if chase > 60:
        score -= min(8.0, (chase - 60.0) * 0.18) * heat_factor

    stop_dist = _stop_distance_pct(row)
    if stop_dist > 12:
        score -= min(8.0, (stop_dist - 12.0) * 0.55)
    elif stop_dist > 8:
        score -= min(3.0, (stop_dist - 8.0) * 0.45)

    gap = min(99.0, max(0.0, _num(row, "距最近可執行買點%", _num(row, "觸發距離%", 99))))
    if gap > 6.5:
        score -= 5.0
    elif gap > 5.0:
        score -= 2.5
    if trigger_quality < 45:
        score -= 4.0
    elif trigger_quality < 55:
        score -= 2.0

    # 低風報比、深停損、低買點品質的候選只能留在觀察區，
    # 不得因題材或主流加成排到可執行候選前段。
    if rr < 0.30 and (stop_dist > 10.0 or risk < 53.0 or entry < 55.0):
        score -= 5.0
        score_cap = min(score_cap, 70.0)

    # 冷門/非主流準主推薦不再靠分區加成壓過主流領漲股。
    if mainstream < 58 and sector < 55 and amount < 300:
        score -= 7.0
    elif mainrise < 55 and _safe_str(row.get("正式推薦分區")) == "A-｜準主推薦小量試單":
        score -= 3.0

    freshness = _safe_str(row.get("K線資料新鮮度"))
    lag = _num(row, "K線落後交易日", 0)
    freshness_unknown = (not freshness) or _contains_any(freshness, ["未知", "未驗證"]) or lag >= 999
    freshness_stale = _contains_any(freshness, ["過期", "落後", "待更新"]) or (1 <= lag < 999)
    if freshness_stale:
        score_cap = min(score_cap, 35.0)
        use_label = "14｜資料待更新"
    elif freshness_unknown:
        score_cap = min(score_cap, 65.0)
        use_label = f"{use_label}｜資料待驗證"

    score = round(_clamp(min(score, score_cap)), 1)
    if score >= 90:
        grade = "S+"
    elif score >= 85:
        grade = "S"
    elif score >= 80:
        grade = "A+"
    elif score >= 75:
        grade = "A"
    elif score >= 70:
        grade = "B+"
    elif score >= 65:
        grade = "B"
    elif score >= 60:
        grade = "C+"
    else:
        grade = "C"

    explain = (
        f"候選{candidate:.0f}｜操作{execution:.0f}｜買點{entry:.0f}｜風控{risk:.0f}｜"
        f"路徑{route:.0f}｜資金{mainstream:.0f}｜族群{sector:.0f}｜主升{mainrise:.0f}｜"
        f"觸發品質{trigger_quality:.0f}｜紅燈脆弱{fragility:.0f}｜RR {rr:.2f}｜耗竭{exhaustion:.0f}"
    )
    return score, grade, use_label, explain


def _apply_unified_recommendation_ranking(out: pd.DataFrame) -> pd.DataFrame:
    """Attach the single score/rank users should use as their first view."""
    if out is None or not isinstance(out, pd.DataFrame) or out.empty:
        return out
    score_rows = out.apply(_unified_recommendation_priority_score, axis=1)
    out["股神推薦優先分"] = [x[0] for x in score_rows]
    out["股神推薦等級"] = [x[1] for x in score_rows]
    out["股神推薦用途"] = [x[2] for x in score_rows]
    out["股神推薦分數說明"] = [x[3] for x in score_rows]
    out["股神推薦總排名"] = 0
    out["可交易總排名"] = 0
    out["排名用途"] = "診斷排名｜需另看操作許可"
    out["極端市場LOCKDOWN"] = "否"

    bucket_series = out.get("正式推薦分區", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)
    score_series = pd.to_numeric(out["股神推薦優先分"], errors="coerce").fillna(0)
    miss_series = pd.to_numeric(out.get("強勢股漏選風險分", pd.Series([0] * len(out), index=out.index)), errors="coerce").fillna(0)
    eligible = ~bucket_series.eq("正式排除清單")
    # 高爆發漏選樣本仍保留在完整排名尾端，但不因此升級買進許可。
    eligible &= score_series.ge(50) | (score_series.ge(45) & miss_series.ge(75) & bucket_series.isin(["高風險雷達觀察", "不可直接買觀察"]))
    ranked = out.loc[eligible].copy()
    if not ranked.empty:
        for col in ["股神推薦優先分", "隔日觸發品質分", "主流主升優先分", "實戰操作品質分", "強勢動能分", "強勢前兆分", "主流資金分", "成交額百萬"]:
            if col not in ranked.columns:
                ranked[col] = 0.0
            ranked[col] = pd.to_numeric(ranked[col], errors="coerce").fillna(0.0)
        ranked = ranked.sort_values(
            ["股神推薦優先分", "隔日觸發品質分", "主流主升優先分", "實戰操作品質分", "強勢動能分", "強勢前兆分", "主流資金分", "成交額百萬"],
            ascending=[False, False, False, False, False, False, False, False],
            kind="mergesort",
        )
        rank_map = {idx: pos for pos, idx in enumerate(ranked.index, start=1)}
        out.loc[list(rank_map.keys()), "股神推薦總排名"] = [rank_map[idx] for idx in rank_map]
    out["股神推薦總排名"] = pd.to_numeric(out["股神推薦總排名"], errors="coerce").fillna(0).astype(int)

    lockdown_mask = out.apply(lambda r: bool(_market_risk_info(r).get("lockdown")), axis=1)
    out.loc[lockdown_mask, "極端市場LOCKDOWN"] = "是"
    out.loc[lockdown_mask, "排名用途"] = "LOCKDOWN診斷排名｜禁止新倉"
    permission = out.get("操作許可", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)
    formal_bucket = bucket_series.isin(["正式下週主推薦", "A-｜準主推薦小量試單"])
    tradable_mask = (~lockdown_mask) & formal_bucket & ~permission.str.contains("禁止|不可|封鎖|等待|僅", regex=True)
    tradable = out.loc[tradable_mask].sort_values("股神推薦優先分", ascending=False, kind="mergesort")
    if not tradable.empty:
        trade_rank = {idx: pos for pos, idx in enumerate(tradable.index, start=1)}
        out.loc[list(trade_rank.keys()), "可交易總排名"] = [trade_rank[idx] for idx in trade_rank]
    out["可交易總排名"] = pd.to_numeric(out["可交易總排名"], errors="coerce").fillna(0).astype(int)
    return out

def _sector_key_for_row(row: pd.Series) -> str:
    """盤中雷達分層用族群 key，避免同族群塞爆核心盯盤清單。"""
    for c in ["主題族群", "次族群", "類別", "產業", "正式產業別", "族群"]:
        v = _safe_str(row.get(c))
        if v:
            return v
    return "未分類"


def _panic_rebound_leader_profile(row: pd.Series) -> dict[str, Any]:
    """紅燈後的恐慌反彈領漲召回。

    7/20 作戰表顯示，既有 R1-P 只要求「前兆高分」，沒有要求隔日
    可執行優先分、停損品質與買點距離，因此低品質候選會占核心；
    同時，樺漢、精材、南亞這類在弱勢市場中具備流動性、資金與
    洗盤後反彈條件的股票，卻沒有專屬排名。這個 profile 只建立
    條件雷達，不把反彈猜測直接升級為正式買進。
    """
    market = _market_risk_info(row)
    prebreak = _prebreakout_profile(row)
    fresh = _history_freshness_info(row)
    liq = _liquidity_info(row)
    direction = _safe_str(row.get("隔日大盤方向"))
    down_prob = _num(row, "隔日下跌機率%", 0)
    forecast_ok = (
        not _contains_any(direction, ["偏空", "下跌", "空方"])
        and (down_prob <= 38 or down_prob <= 0 or _contains_any(direction, ["震盪", "偏多", "上漲"]))
    )
    next_exec = (
        _num(row, "隔日可執行優先分", 0)
        if not _is_blank(row.get("隔日可執行優先分"))
        else _num(row, "隔日可參考分", 0)
    )
    gap = min(99.0, _num(row, "距最近可執行買點%", _num(row, "觸發距離%", 99)))
    ret1 = _num(row, "今日漲幅%", 0)
    ret5 = _num(row, "近5日漲幅%", 0)
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    buy = _num(row, "買進分數", 0)
    funds = _num(row, "主流資金分", 0)
    sector = max(_num(row, "族群攻擊強度", 0), _num(row, "族群輪動分", 0))
    amount = _reference_turnover_m(row)
    stop = _stop_distance_pct(row)
    if stop <= 0:
        stop = _num(row, "停損距離_隔日%", 0)
    chase = _chase_risk_score(row, 55)
    upper = _num(row, "上影線比例%", 0)
    close_pos = _num(row, "當日收盤位置%", 50)
    pre_score = max(_safe_float(prebreak.get("score"), 0), _num(row, "強勢前兆分", 0))
    hard_blob = _text_blob(row, ["正式推薦排除原因", "排除原因", "風控否決旗標"])
    hard_block = _contains_any(hard_blob, ["興櫃", "低流動性", "冷門股", "K線過期", "資料待更新", "硬性禁買"])

    standard_rebound = bool(
        -1.0 <= ret1 <= 4.5 and gap <= 4.5 and next_exec >= 44
        and pre_score >= 74 and funds >= 68 and sector >= 45
        and risk >= 54 and entry >= 58 and buy >= 60
        and amount >= 500 and 0 < stop <= 8.5 and chase <= 78
        and upper <= 45 and close_pos >= 48
    )
    washout_rebound = bool(
        -8.5 <= ret1 <= -2.0 and -12 <= ret5 <= 10 and gap <= 7.0 and next_exec >= 44
        and pre_score >= 74 and funds >= 75 and sector >= 45
        and risk >= 53 and entry >= 53 and buy >= 55
        and amount >= 500 and 0 < stop <= 8.5 and chase <= 90
        and upper <= 50 and close_pos >= 40
    )
    core_ready = bool(
        market.get("severe") and not market.get("panic") and forecast_ok
        and fresh.get("fresh") and liq.get("tradable") and not hard_block
        and (standard_rebound or washout_rebound)
    )
    score = (
        pre_score * 0.22 + next_exec * 0.20 + funds * 0.16 + sector * 0.10
        + risk * 0.10 + entry * 0.08 + buy * 0.06
        + min(100.0, amount / 20.0) * 0.05 + max(0.0, 100.0 - chase) * 0.03
    )
    if standard_rebound:
        score += 5
    if washout_rebound:
        score += 8
    if gap > 5:
        score -= 2
    score = round(_clamp(score), 1)
    if market.get("panic"):
        status = "BLOCK-RB｜極端市場禁止反彈交易"
    elif core_ready and washout_rebound:
        status = "READY-RB｜恐慌洗盤後領漲條件雷達"
    elif core_ready:
        status = "READY-RB｜紅燈相對強勢領漲條件雷達"
    else:
        status = "BLOCK-RB｜未達恐慌反彈核心條件"
    return {
        "score": score, "core_ready": core_ready, "status": status,
        "standard": standard_rebound, "washout": washout_rebound,
        "gap": round(gap, 2), "next_exec": round(next_exec, 1),
        "restriction": "只在大盤止跌、個股突破觸發價並守住守價後小量試單；開高過大不追，跌破守價取消。",
    }


def _intraday_priority_score(row: pd.Series) -> float:
    """Phase 7.1 盤中雷達優先分。

    目的：盤中雷達可保留較多資料，但人工盯盤只看最多 6 檔真正接近可執行買點的標的。
    這個分數比單純推薦總分更偏向「盤中可操作性、主流資金、族群同步、爆發雷達」。
    """
    op = _num(row, "可操作分", 0)
    formal_sort = _num(row, "正式推薦排序分", op)
    buy = _num(row, "買進分數", 0)
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    rr = _risk_reward_ratio(row)
    chase = _chase_risk_score(row, 55)
    mainstream = _num(row, "主流資金分", 50)
    sector = max(_num(row, "族群攻擊強度", 0), _num(row, "族群輪動分", 0))
    radar = max(
        _num(row, "爆發雷達分", 0),
        _num(row, "隔日爆發分", 0),
        _num(row, "飆股攻擊分", 0),
        _num(row, "主流領漲回補分", 0),
        _num(row, "漲停回放分", 0),
    )
    amount = _num(row, "成交額百萬", 0)
    next_score = _num(row, "隔日可參考分", _next_session_profile(row)["score"])
    readiness_score = _num(row, "進場可執行分", _entry_readiness_profile(row)["score"])
    momentum_score = _num(row, "強勢動能分", _momentum_profile(row)["score"])
    prebreak_score = _num(row, "強勢前兆分", _prebreakout_profile(row)["score"])
    rebound = _panic_rebound_leader_profile(row)
    rebound_score = _safe_float(rebound.get("score"), 0)
    mainrise = _mainstream_mainrise_profile(row)
    mainrise_score = _safe_float(mainrise.get("score"), 0)
    amount_score = 100 if amount >= 5000 else 88 if amount >= 2000 else 76 if amount >= 800 else 62 if amount >= 300 else 45 if amount >= 100 else 20
    rr_score = _clamp(rr * 50.0, 0, 100)
    score = (
        formal_sort * 0.12
        + op * 0.10
        + radar * 0.12
        + mainstream * 0.12
        + sector * 0.10
        + mainrise_score * 0.20
        + buy * 0.05
        + entry * 0.04
        + risk * 0.03
        + rr_score * 0.02
        + amount_score * 0.04
        + next_score * 0.05
        + readiness_score * 0.05
        + momentum_score * 0.08
        + prebreak_score * 0.07
        + rebound_score * 0.06
    )
    if rebound.get("core_ready"):
        score += 8.0
    # 追價風險過高、買進分數過低時不要進核心盯盤，保留到備援或低優先。
    exhaustion = _exhaustion_profile(row)["score"]
    heat_factor = 0.38 if mainrise_score >= 78 else 0.65 if mainrise_score >= 70 else 1.0
    if exhaustion >= 70:
        score -= 16 * heat_factor
    elif exhaustion >= 55:
        score -= 10 * heat_factor
    elif exhaustion >= 35:
        score -= 4 * heat_factor
    if chase >= 76:
        score -= 8 * heat_factor
    elif chase >= 70:
        score -= 4 * heat_factor
    if buy < 30:
        score -= 6
    if rr and rr < 0.30 and momentum_score < 70:
        score -= 3
    return round(_clamp(score, 0, 120), 1)


def _apply_intraday_radar_tiers(out: pd.DataFrame) -> pd.DataFrame:
    """盤中雷達分層：R1 必須同時通過資料新鮮度、流動性與進場品質。

    Phase 9.6 修正三個問題：
    1. K線過期仍被列為 R1；
    2. 隔日參考 BLOCK、距買點過遠仍只靠前兆高分進核心；
    3. 已發動股票只等追突破，忽略更低風險的觸發後守價回測。
    """
    if out is None or out.empty or "正式推薦分區" not in out.columns:
        return out
    text_cols = [
        "盤中雷達優先級", "盤中雷達分層", "盤中雷達分層說明",
        "核心雷達品質檢查", "核心雷達降級原因", "恐慌反彈領漲判定",
    ]
    numeric_cols = ["盤中盯盤順序", "恐慌反彈領漲分"]
    for c in text_cols:
        if c not in out.columns:
            out[c] = ""
    for c in numeric_cols:
        if c not in out.columns:
            out[c] = 0.0

    mask = out["正式推薦分區"].fillna("").astype(str).eq("盤中雷達追蹤")
    if not bool(mask.any()):
        return out
    tmp = out.loc[mask].copy()
    for numeric_col in ["可操作分", "爆發雷達分", "主流資金分", "成交額百萬"]:
        if numeric_col not in tmp.columns:
            tmp[numeric_col] = 0.0
        tmp[numeric_col] = pd.to_numeric(tmp[numeric_col], errors="coerce").fillna(0.0)
    tmp["__priority"] = tmp.apply(_intraday_priority_score, axis=1)
    tmp["__sector"] = tmp.apply(_sector_key_for_row, axis=1)
    tmp = tmp.sort_values(
        ["__priority", "可操作分", "爆發雷達分", "主流資金分", "成交額百萬"],
        ascending=[False] * 5,
        kind="mergesort",
    )

    core_limit, backup_limit, sector_cap_core = 8, 18, 2
    core: list[Any] = []
    backup: list[Any] = []
    route_map: dict[Any, str] = {}
    reason_map: dict[Any, str] = {}
    sector_count: dict[str, int] = {}

    for idx, row in tmp.iterrows():
        sector = _safe_str(row.get("__sector")) or "未分類"
        momentum = _momentum_profile(row)
        prebreak = _prebreakout_profile(row)
        rebound = _panic_rebound_leader_profile(row)
        mainrise = _mainstream_mainrise_profile(row)
        guard = _guard_retest_profile(row)
        freshness = _history_freshness_info(row)
        liq = _liquidity_info(row)
        stop = _stop_distance_pct(row)
        if stop <= 0:
            stop = _num(row, "停損距離_隔日%", 0)
        nearest_gap = _num(row, "距最近可執行買點%", 99)
        entry_score = _num(row, "進場可執行分", 0)
        next_pass = _safe_str(row.get("隔日參考判定")).startswith("PASS")
        risk_score = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
        chase = _chase_risk_score(row, 55)
        core_data_ok = bool(freshness.get("fresh") and liq.get("tradable"))

        out.at[idx, "恐慌反彈領漲分"] = _safe_float(rebound.get("score"), 0)
        out.at[idx, "恐慌反彈領漲判定"] = _safe_str(rebound.get("status"))

        normal_core = bool(
            core_data_ok and _safe_float(row.get("隔日可參考分"), 0) >= 64
            and next_pass
            and _safe_str(row.get("進場可執行判定")).startswith("READY")
            and not _safe_str(row.get("進場可執行判定")).startswith("READY-M")
            and entry_score >= 72 and nearest_gap <= 2.5
            and _risk_reward_ratio(row) >= 1.45 and 0 < stop <= 6.8
        )
        momentum_core = bool(
            core_data_ok and momentum.get("core_ready") and momentum["score"] >= 74
            and momentum["amount"] >= 150 and risk_score >= 55 and chase <= 72
            and 0 < stop <= 8.5 and entry_score >= 55
            and (guard.get("ready") or (next_pass and nearest_gap <= 3.0))
        )
        prebreak_core = bool(
            core_data_ok and prebreak["radar_ready"] and prebreak["score"] >= 74
            and prebreak["amount"] >= 150 and prebreak["missed"] >= 78 and prebreak["radar"] >= 68
            and not prebreak.get("hot_risk")
            and _num(row, "今日漲幅%", 0) <= 6.5
            and _num(row, "近20日漲幅%", 0) >= -5
            and _exhaustion_profile(row)["score"] < 50
            and entry_score >= 50
            and (guard.get("ready") or (next_pass and nearest_gap <= 3.5))
            and risk_score >= 55 and 0 < stop <= 8.0
            and (_risk_reward_ratio(row) >= 0.80 or guard.get("ready")) and chase <= 72
            and _num(row, "上影線比例%", 0) <= 45
            and _num(row, "當日收盤位置%", 50) >= 48
        )
        rebound_core = bool(core_data_ok and rebound.get("core_ready"))
        mainline_core = bool(
            core_data_ok and mainrise.get("mainstream_ok")
            and _safe_float(mainrise.get("score"), 0) >= 72
            and _safe_float(mainrise.get("amount"), 0) >= 500
            and risk_score >= 35 and 0 < stop <= 10.5
            and _num(row, "當日收盤位置%", 50) >= 48
        )

        selected_route = (
            "MAINHOT" if mainline_core and mainrise.get("high_heat") else
            "MAIN" if mainline_core else
            "NORMAL" if normal_core else
            "REBOUND" if rebound_core else
            "MOMENTUM" if momentum_core else
            "PREBREAK" if prebreak_core else ""
        )

        quality_reasons: list[str] = []
        if not freshness.get("fresh"):
            quality_reasons.append(
                f"K線落後{freshness.get('lag', 999)}個交易日" if freshness.get("known") else "K線日期未驗證"
            )
        if not liq.get("tradable"):
            quality_reasons.append("流動性未通過")
        if not next_pass and not guard.get("ready") and not rebound_core:
            quality_reasons.append("隔日參考判定未通過")
        if nearest_gap > 3.5 and not guard.get("ready") and not rebound_core:
            quality_reasons.append(f"距可執行買點{nearest_gap:.1f}%過遠")
        if entry_score < 50 and not rebound_core:
            quality_reasons.append(f"進場可執行分{entry_score:.1f}<50")
        if stop <= 0 or stop > 8.5:
            quality_reasons.append(f"停損距離{stop:.1f}%不合格")
        if risk_score < 55:
            quality_reasons.append(f"Risk {risk_score:.1f}<55")
        if chase > 72:
            quality_reasons.append(f"追價風險{chase:.0f}>72")
        if prebreak.get("hot_risk"):
            quality_reasons.append("強勢前兆已過熱")

        can_core = bool(
            len(core) < core_limit
            and sector_count.get(sector, 0) < sector_cap_core
            and _safe_float(row.get("__priority"), 0) >= 66
            and selected_route
        )
        if can_core:
            core.append(idx)
            route_map[idx] = selected_route
            sector_count[sector] = sector_count.get(sector, 0) + 1
            out.at[idx, "核心雷達品質檢查"] = "PASS｜資料、流動性與進場品質通過"
            reason_map[idx] = ""
        else:
            if len(backup) < backup_limit:
                backup.append(idx)
            reason = "、".join(dict.fromkeys(quality_reasons)) or "未達R1路徑最低分或族群名額"
            reason_map[idx] = reason
            out.at[idx, "核心雷達品質檢查"] = "BLOCK｜降為備援/資料觀察"
            out.at[idx, "核心雷達降級原因"] = reason

    core_set, backup_set = set(core), set(backup)
    order_map = {idx: i + 1 for i, idx in enumerate(list(core) + list(backup))}
    for idx in tmp.index:
        row = tmp.loc[idx]
        pr = _safe_float(row.get("__priority"), 0)
        momentum = _momentum_profile(row)
        freshness = _history_freshness_info(row)
        next_pass = _safe_str(row.get("隔日參考判定")).startswith("PASS")
        out.at[idx, "盤中盯盤順序"] = int(order_map.get(idx, 999))
        if idx in core_set:
            route = route_map.get(idx, "NORMAL")
            out.at[idx, "核心雷達降級原因"] = ""
            if route == "MAINHOT":
                out.at[idx, "盤中雷達優先級"] = "R1-LH｜主流主升高熱核心雷達"
                out.at[idx, "盤中雷達分層說明"] = (
                    f"主流主升高熱優先顯示，主升分 {mainrise['score']:.1f}、優先分 {pr:.1f}；"
                    "禁止追價，只等充分回測守價或整理後再突破。"
                )
            elif route == "MAIN":
                out.at[idx, "盤中雷達優先級"] = "R1-L｜主流主升核心雷達"
                out.at[idx, "盤中雷達分層說明"] = (
                    f"主流主升核心盯盤，主升分 {mainrise['score']:.1f}、優先分 {pr:.1f}；"
                    "需量價與族群同步，觸發並守價後才小量。"
                )
            elif route == "REBOUND":
                rb = _panic_rebound_leader_profile(row)
                out.at[idx, "盤中雷達優先級"] = "R1-RB｜恐慌反彈領漲核心雷達"
                out.at[idx, "盤中雷達分層說明"] = (
                    f"紅燈反彈核心，反彈分 {rb['score']:.1f}、優先分 {pr:.1f}；"
                    "只在大盤止跌且突破觸發後守價成立時小量試單。"
                )
            elif route == "MOMENTUM":
                out.at[idx, "盤中雷達優先級"] = "R1-M｜強勢動能核心雷達"
                out.at[idx, "盤中雷達分層說明"] = (
                    f"動能核心盯盤，優先分 {pr:.1f}；優先採守價回測，無回測才等再突破放量。"
                )
            elif route == "PREBREAK":
                out.at[idx, "盤中雷達優先級"] = "R1-P｜強勢前兆核心雷達"
                out.at[idx, "盤中雷達分層說明"] = (
                    f"前兆核心召回，優先分 {pr:.1f}；資料與隔日參考均通過後，才等待突破/守價回測。"
                )
            else:
                out.at[idx, "盤中雷達優先級"] = "R1｜可執行買點核心雷達"
                out.at[idx, "盤中雷達分層說明"] = (
                    f"可執行買點核心盯盤，優先分 {pr:.1f}；依主要進場路徑與守價確認。"
                )
            out.at[idx, "盤中雷達分層"] = "盤中核心雷達"
        elif idx in backup_set:
            reason = reason_map.get(idx, "")
            if not freshness.get("fresh"):
                out.at[idx, "盤中雷達優先級"] = "R2-DATA｜行情待更新"
                out.at[idx, "盤中雷達分層"] = "資料待更新雷達"
                out.at[idx, "盤中雷達分層說明"] = f"不得列核心；{reason}。更新最新K線後重新分流。"
            elif momentum.get("hot_risk"):
                out.at[idx, "盤中雷達優先級"] = "R2-HOT｜高熱動能待回測"
                out.at[idx, "盤中雷達分層"] = "高熱備援雷達"
                out.at[idx, "盤中雷達分層說明"] = (
                    f"耗竭風險 {momentum['exhaustion_score']:.0f}；保留強勢觀察但不占核心。{reason}"
                )
            elif not next_pass:
                out.at[idx, "盤中雷達優先級"] = "R2-WAIT｜隔日品質未通過"
                out.at[idx, "盤中雷達分層"] = "條件待補雷達"
                out.at[idx, "盤中雷達分層說明"] = f"前兆/動能存在，但不具隔日核心品質；{reason}。"
            else:
                out.at[idx, "盤中雷達優先級"] = "R2｜備援雷達"
                out.at[idx, "盤中雷達分層"] = "盤中備援雷達"
                out.at[idx, "盤中雷達分層說明"] = f"備援輪動名單，優先分 {pr:.1f}；{reason}。"
        else:
            out.at[idx, "盤中雷達優先級"] = "R3｜低優先觀察"
            out.at[idx, "盤中雷達分層"] = "盤中低優先觀察"
            out.at[idx, "盤中雷達分層說明"] = f"保留診斷但不放主盯盤，優先分 {pr:.1f}。"
            if not _safe_str(out.at[idx, "核心雷達品質檢查"]):
                out.at[idx, "核心雷達品質檢查"] = "BLOCK｜低優先"
                out.at[idx, "核心雷達降級原因"] = reason_map.get(idx, "未達R1/R2最低條件")
    return out

def apply_formal_recommendation_engine(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        out = pd.DataFrame(columns=FORMAL_RECOMMENDATION_COLUMNS)
        return out
    if not isinstance(df, pd.DataFrame):
        out = pd.DataFrame(df)
    else:
        out = df.copy()
    if out.empty:
        for c in FORMAL_RECOMMENDATION_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="float64" if c in NUMERIC_FORMAL_RECOMMENDATION_COLUMNS else "object")
        return out
    rows = out.apply(_classify, axis=1, result_type="expand")
    # 一次性回寫正式推薦欄位，避免逐欄 insert 造成 DataFrame fragmentation 與推薦頁卡頓。
    classified = rows.reindex(columns=FORMAL_RECOMMENDATION_COLUMNS).copy()
    for col in FORMAL_RECOMMENDATION_COLUMNS:
        if col in NUMERIC_FORMAL_RECOMMENDATION_COLUMNS:
            classified[col] = pd.to_numeric(classified[col], errors="coerce").fillna(0.0)
        else:
            classified[col] = classified[col].fillna("").astype("object")
    out = out.drop(columns=[c for c in FORMAL_RECOMMENDATION_COLUMNS if c in out.columns], errors="ignore")
    out = pd.concat([out, classified], axis=1)
    out = _apply_intraday_radar_tiers(out)
    out = _apply_unified_recommendation_ranking(out)
    if callable(apply_recommendation_rotation_guard):
        try:
            out = apply_recommendation_rotation_guard(out)
        except Exception:
            # 輪動校正是排名防黏著層；失敗時不得讓正式推薦主流程崩潰。
            pass
    return out

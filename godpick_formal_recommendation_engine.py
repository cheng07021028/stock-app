# -*- coding: utf-8 -*-
"""Phase 6.3 formal recommendation purifier.

目的：把「推薦候選 / 雷達 / 回放 / 排除」重新分成可操作清單，避免
完整推薦表裡的 D、弱勢觀察、高風險雷達被使用者誤解為正式買進推薦。

本模組只處理 DataFrame 欄位，不讀寫 JSON、不連網、不覆蓋既有欄位。
"""
from __future__ import annotations

from typing import Any
import pandas as pd

FORMAL_RECOMMENDATION_VERSION = "vnext_phase8_9_profit_recall_20260715"

FORMAL_RECOMMENDATION_COLUMNS = [
    "最終操作結論",
    "是否正式推薦",
    "操作許可",
    "正式推薦等級",
    "推薦可信度分",
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
    "強勢動能分",
    "強勢動能判定",
    "動能進場條件",
    "動能風險控制",
    "強勢前兆分",
    "強勢前兆判定",
    "強勢前兆進場條件",
    "強勢前兆風控",
    "K線最後交易日",
    "K線落後交易日",
    "K線資料新鮮度",
    "K線日期驗證基準",
    "正式推薦版本",
]

NUMERIC_FORMAL_RECOMMENDATION_COLUMNS = {
    "推薦可信度分",
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
    "強勢動能分",
    "強勢前兆分",
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


def _market_risk_info(row: pd.Series) -> dict[str, Any]:
    blob = _text_blob(row, [
        "大盤風險燈號", "大盤橋接風控", "大盤策略模式", "大盤策略建議",
        "大盤風控建議", "今日大盤結論", "大盤橋接狀態",
    ])
    score = max(_num(row, "大盤橋接分數", 0), _num(row, "大盤多空分數", 0))
    severe = _contains_any(blob, ["紅燈", "空方", "全面防守", "禁止進攻", "風險急升"])
    defensive = severe or _contains_any(blob, ["防守", "保守", "震盪控風險", "不宜全面追價"])
    if score > 0 and score < 42:
        severe = True
        defensive = True
    return {"blob": blob, "score": score, "severe": severe, "defensive": defensive}


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


def _position_cap_pct(row: pd.Series, bucket: str) -> float:
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
        if market["severe"]:
            return 0.0
        if market["defensive"]:
            return 3.0
        return 5.0
    return 0.0


def _final_action_meta(row: pd.Series, bucket: str, op_score: float, exclusion_text: str) -> dict[str, Any]:
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
        conclusion = "A-｜準主推薦：盤中確認後只允許小量試單"
        formal = "否｜準主推薦"
        permit = "觸發且守價後小量試單"
        grade = "A-｜條件推薦"
        nature = "條件推薦"
        veto = "否"
        consistency = "一致｜接近主推薦，但仍有一項以上門檻未完全通過"
    elif bucket == "盤中雷達追蹤":
        conclusion = "B+｜盤中雷達：未觸發前不可買"
        formal = "否"
        permit = "僅盤中觸發後評估"
        grade = "B+｜盤中條件雷達"
        nature = "盤中雷達"
        veto = "否"
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
        "建議倉位上限%": _position_cap_pct(row, bucket),
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


def _momentum_profile(row: pd.Series) -> dict[str, Any]:
    """強勢股第二條決策路徑：放量突破 / 漲停續強。

    傳統波段路徑重視回測買點、靜態壓力與 RR；動能路徑則重視價格發現、
    收盤強度、當日量能、突破與流動性。兩者不可使用同一組硬門檻，否則
    強勢股會在啟動當天被「過熱 / RR 不足」直接刪除。
    """
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

    gain_score = 98 if day_gain >= 9.0 else 92 if day_gain >= 6.5 else 82 if day_gain >= 4.0 else 68 if day_gain >= 2.0 else 30
    volume_score = 100 if day_vol >= 2.0 else 88 if day_vol >= 1.5 else 76 if day_vol >= 1.2 else 60 if day_vol >= 1.0 else 30
    breakout_score = 96 if breakout20 >= 0 else 84 if breakout20 >= -1.0 else 68 if breakout20 >= -3.0 else 30
    liquidity_score = 100 if amount >= 2000 else 90 if amount >= 800 else 78 if amount >= 300 else 65 if amount >= 150 else 48 if amount >= 100 else 10
    score = (
        max(rescue, gain_score) * 0.22 + close_loc * 0.18 + volume_score * 0.17
        + breakout_score * 0.15 + liquidity_score * 0.10 + radar * 0.08
        + mainstream * 0.05 + sector * 0.05
    )
    if upper_shadow > 45:
        score -= 18
    elif upper_shadow > 35:
        score -= 8
    if close_ma20 > 28 or ret5 > 38:
        score -= 16
    if day_gap > 7:
        score -= 8
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
    if day_vol < 1.15 and amount < 500:
        blockers.append("當日量能未確認")
    if breakout20 < -1.5 and day_gain < 6:
        blockers.append("尚未接近20日突破")
    if upper_shadow > 42:
        blockers.append("上影線過長，追價承接不穩")
    if ret5 > 38 or close_ma20 > 30:
        blockers.append("短線乖離過大")
    if not freshness["known"] or not freshness["fresh"]:
        blockers.append("K線日期未驗證或已過期")

    eligible = not blockers and score >= 68
    radar_ready = bool(eligible and score >= 70)
    strong = bool(radar_ready and score >= 78 and close_loc >= 78 and (day_vol >= 1.25 or amount >= 800))
    market_wait = bool(radar_ready and market["severe"])
    if day_gain >= 8.5:
        role = "M+｜漲停/近漲停續強"
    elif breakout20 >= 0:
        role = "M｜放量突破"
    elif radar_ready:
        role = "M｜強勢點火"
    else:
        role = "N｜非動能作戰"
    entry = "不可開盤追價；只接受首波回測不破、量縮守住，或盤中再突破當日/前高且量能續強。"
    risk_text = "若開高逾5%或跌破點火K低點/觸發後守價，取消交易；以小倉位、移動停利管理，不用靜態第一壓力 RR 硬判。"
    return {
        "score": score,
        "eligible": eligible,
        "radar_ready": radar_ready,
        "strong": strong,
        "market_wait": market_wait,
        "role": role,
        "blockers": "、".join(blockers),
        "entry": entry,
        "risk": risk_text,
        "day_gain": round(day_gain, 2),
        "close_loc": round(close_loc, 2),
        "day_vol": round(day_vol, 2),
        "breakout20": round(breakout20, 2),
        "upper_shadow": round(upper_shadow, 2),
        "amount": round(amount, 1),
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


def _entry_readiness_profile(row: pd.Series) -> dict[str, Any]:
    """雙路徑進場模型：波段回測/突破 + 強勢動能條件進場。"""
    price = _first_price(row, ["最新價", "推薦價格", "推薦日價格", "建議價位"], 0.0)
    trig = _trigger_info(row)
    breakout = _safe_float(trig.get("final"), 0.0)
    pullback = _first_price(row, ["推薦買點_拉回", "回測承接價", "近端支撐", "主要支撐", "MA20", "停損參考"], 0.0)
    breakout_gap = max(0.0, (breakout / price - 1.0) * 100.0) if price > 0 and breakout > 0 else 99.0
    pullback_gap = abs(price / pullback - 1.0) * 100.0 if price > 0 and pullback > 0 else 99.0
    pullback_broken = bool(price > 0 and pullback > 0 and price < pullback * 0.985)

    rr = _risk_reward_ratio(row)
    stop = _stop_distance_pct(row)
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

    if momentum["radar_ready"]:
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

    rr_score = _clamp((rr - 0.6) * 55.0, 0, 100)
    stop_score = 95 if 0 < stop <= 4.5 else 84 if stop <= 6.5 else 65 if stop <= 8.0 else 35 if stop <= 10.0 else 10
    proximity_score = 100 if nearest_gap <= 0.8 else 90 if nearest_gap <= 1.5 else 78 if nearest_gap <= 2.5 else 58 if nearest_gap <= 4 else 25
    liquidity_score = 100 if amount >= 800 else 88 if amount >= 300 else 75 if amount >= 150 else 35
    score = entry * 0.20 + risk * 0.16 + buy * 0.08 + practical * 0.08 + rr_score * 0.18 + stop_score * 0.14 + proximity_score * 0.10 + liquidity_score * 0.03 + (100 - chase) * 0.03
    if momentum["radar_ready"]:
        score = max(score, momentum["score"] * 0.70 + risk * 0.12 + practical * 0.10 + liquidity_score * 0.08)

    blockers: list[str] = []
    if not freshness["known"]:
        blockers.append("K線日期未驗證，須重新推薦")
    elif not freshness["fresh"]:
        blockers.append(f"行情落後{freshness['lag']}個交易日")

    base_ready = bool(
        (pullback_setup or breakout_setup) and freshness["fresh"] and rr >= 1.45
        and 0 < stop <= 6.8 and entry >= 68 and risk >= 59 and amount >= 150 and chase <= 60
    )
    momentum_ready = bool(momentum["radar_ready"] and freshness["fresh"])
    prebreak_ready = bool(prebreak["radar_ready"])

    if not momentum_ready and not prebreak_ready:
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
    elif momentum["blockers"]:
        blockers.append(momentum["blockers"])

    if base_ready and market["severe"]:
        status = "MARKET-WAIT｜個股接近買點但大盤禁止"
        blockers.append("大盤風控為嚴重/紅燈")
        ready = False
    elif base_ready:
        status = "READY｜接近可執行買點"
        ready = True
    elif momentum_ready and market["severe"]:
        status = "MARKET-WAIT-M｜強勢動能成立但大盤禁止追價"
        blockers.append("大盤紅燈：只列強勢動能雷達")
        ready = False
    elif momentum_ready:
        status = "READY-M｜強勢動能條件進場"
        ready = False  # 不升成正式/A-，只進 R1-M 條件雷達。
    elif prebreak_ready and not freshness["fresh"]:
        status = "DATA-WAIT-P｜強勢前兆成立但K線待更新"
        blockers.append("強勢前兆保留；更新最新K線後才可判斷進場")
        ready = False
    elif prebreak_ready and market["severe"]:
        status = "MARKET-WAIT-P｜強勢前兆成立但大盤禁止追價"
        blockers.append("大盤紅燈：只列強勢前兆雷達")
        ready = False
    elif prebreak_ready:
        status = "READY-P｜強勢前兆條件雷達"
        ready = False
    elif freshness["fresh"] and (pullback_gap <= 4.0 or breakout_gap <= 4.0) and rr >= 1.15 and stop <= 8.5:
        status = "WATCH｜接近買點但條件未齊"
        ready = False
    else:
        status = "BLOCK｜尚非可進場型態"
        ready = False

    if blockers and not momentum_ready:
        score -= min(35.0, 5.0 * len(blockers))
    if market["severe"]:
        score -= 8.0
    return {
        "score": round(_clamp(score), 1), "status": status, "ready": ready,
        "momentum_ready": momentum_ready, "prebreak_ready": prebreak_ready, "path": path,
        "nearest_gap": round(nearest_gap, 2), "breakout_gap": round(breakout_gap, 2),
        "pullback_gap": round(pullback_gap, 2), "reasons": "、".join(dict.fromkeys(blockers)),
        "freshness": freshness,
    }

def _direct_ok(row: pd.Series, op_score: float, exclusion: list[str]) -> bool:
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


def _a_minus_ok(row: pd.Series, op_score: float, exclusion: list[str]) -> bool:
    """A- 準主推薦：只允許盤中觸發後小量試單，不可當成直接買進。"""
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


def _classify(row: pd.Series) -> dict[str, Any]:
    op = _compute_operability_score(row)
    next_profile = _next_session_profile(row)
    readiness = _entry_readiness_profile(row)
    momentum = _momentum_profile(row)
    prebreak = _prebreakout_profile(row)
    reasons = _exclusion_reasons(row)
    trig = _trigger_info(row)
    direct_primary = _direct_ok(row, op, reasons)
    direct_objective = False if direct_primary else _objective_direct_ok(row, op, reasons)
    direct = direct_primary or direct_objective
    a_primary = False if direct else _a_minus_ok(row, op, reasons)
    a_objective = False if (direct or a_primary) else _objective_a_minus_ok(row, op, reasons)
    a_minus = a_primary or a_objective
    intraday_primary = _intraday_radar_ok(row, op, reasons)
    intraday_objective = False if (direct or a_minus or intraday_primary) else _objective_intraday_ok(row, op, reasons)
    intraday = intraday_primary or intraday_objective
    market_info = _market_risk_info(row)
    decision_source = (
        "完整因子正式門檻" if direct_primary else
        "客觀量價備援正式門檻" if direct_objective else
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
        action = "可依觸發價/支撐分批進攻；第一筆不超過建議倉位，跌破失效條件立即退出。"
        radar_level = "主攻"
        radar_action = "正式推薦優先追蹤"
        exclude_text = ""
    elif a_minus:
        bucket = "A-｜準主推薦小量試單"
        qual = "A-｜接近主推薦，待盤中觸發" if a_primary else ("A-QD｜防守市場條件式參考" if market_info["severe"] else "A-Q｜客觀量價備援，待盤中觸發")
        direct_buy = "不可｜等大盤解除紅燈" if (a_objective and market_info["severe"]) else "小量｜需觸發與守價"
        action = (
            f"防守市場條件式參考；大盤紅燈未解除前不建立新倉。待大盤改善後，{_trigger_text(row, trig)}且守住觸發後守價，才可重新評估小量試單。"
            if (a_objective and market_info["severe"])
            else f"只允許小量試單；{_trigger_text(row, trig)}，且必須守住觸發後守價。未觸發前不買。"
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
    final_meta = _final_action_meta(row, bucket, op, exclude_text)
    return {
        **final_meta,
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
        "正式推薦排除原因": exclude_text,
        "正式推薦排序分": round(_clamp(sort_score, 0, 120), 1),
        "原始觸發價": trig.get("raw", 0),
        "實戰觸發價": trig.get("final", 0),
        "觸發價偏離%": trig.get("dist", 0),
        "觸發價修正原因": trig.get("reason", ""),
        "隔日雷達回測判斷": _review_text_for(row, bucket, trig),
        "股神觸發修正建議": "正式推薦仍以 Entry/Risk/RR 為準；盤中雷達只在實戰觸發價放量站上、守住觸發後守價後小量試單。",
        "觸發後守價": _support_after_trigger(trig.get("final", 0)),
        "盤中觸發確認條件": _trigger_confirm_text(trig),
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
        "進場可執行分": readiness["score"],
        "進場可執行判定": readiness["status"],
        "進場路徑": readiness["path"],
        "距最近可執行買點%": readiness["nearest_gap"],
        "進場阻擋原因": readiness["reasons"],
        "強勢動能分": momentum["score"],
        "強勢動能判定": ("PASS｜" + momentum["role"]) if momentum["radar_ready"] else ("BLOCK｜" + (momentum["blockers"] or "未達動能門檻")),
        "動能進場條件": momentum["entry"],
        "動能風險控制": momentum["risk"],
        "強勢前兆分": prebreak["score"],
        "強勢前兆判定": prebreak["status"],
        "強勢前兆進場條件": prebreak["entry"],
        "強勢前兆風控": prebreak["risk"],
        "K線最後交易日": readiness["freshness"]["last_date"],
        "K線落後交易日": readiness["freshness"]["lag"] if readiness["freshness"]["known"] else 999,
        "K線資料新鮮度": readiness["freshness"]["status"],
        "K線日期驗證基準": _safe_str(row.get("K線日期驗證基準")),
        "正式推薦版本": FORMAL_RECOMMENDATION_VERSION,
    }


def _sector_key_for_row(row: pd.Series) -> str:
    """盤中雷達分層用族群 key，避免同族群塞爆核心盯盤清單。"""
    for c in ["主題族群", "次族群", "類別", "產業", "正式產業別", "族群"]:
        v = _safe_str(row.get(c))
        if v:
            return v
    return "未分類"


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
    amount_score = 100 if amount >= 5000 else 88 if amount >= 2000 else 76 if amount >= 800 else 62 if amount >= 300 else 45 if amount >= 100 else 20
    rr_score = _clamp(rr * 50.0, 0, 100)
    score = (
        formal_sort * 0.18
        + op * 0.16
        + radar * 0.18
        + mainstream * 0.14
        + sector * 0.12
        + buy * 0.08
        + entry * 0.06
        + risk * 0.04
        + rr_score * 0.02
        + amount_score * 0.02
        + next_score * 0.08
        + readiness_score * 0.12
        + momentum_score * 0.16
        + prebreak_score * 0.14
    )
    # 追價風險過高、買進分數過低時不要進核心盯盤，保留到備援或低優先。
    if chase >= 76:
        score -= 8
    elif chase >= 70:
        score -= 4
    if buy < 30:
        score -= 6
    if rr and rr < 0.30 and momentum_score < 70:
        score -= 3
    return round(_clamp(score, 0, 120), 1)


def _apply_intraday_radar_tiers(out: pd.DataFrame) -> pd.DataFrame:
    """Phase 7.1：將盤中雷達分成核心 / 備援 / 低優先。

    - R1 核心雷達：最多 8 檔，且同族群最多 2 檔；包含接近傳統買點或 R1-M 強勢動能條件者。
    - R2 備援雷達：保留輪動機會，避免砍太少造成漏選。
    - R3 低優先觀察：資料保留，不放第一眼主表。
    """
    if out is None or out.empty or "正式推薦分區" not in out.columns:
        return out
    for c in ["盤中雷達優先級", "盤中盯盤順序", "盤中雷達分層", "盤中雷達分層說明"]:
        if c not in out.columns:
            out[c] = "" if c != "盤中盯盤順序" else 0

    mask = out["正式推薦分區"].fillna("").astype(str).eq("盤中雷達追蹤")
    if not bool(mask.any()):
        return out

    tmp = out.loc[mask].copy()
    # 舊快取或客觀量價備援結果可能沒有完整雷達欄位。先補 0，避免排序時
    # 因缺欄位中斷整批推薦；缺值只影響排序，不會因此升級成核心雷達。
    for numeric_col in ["可操作分", "爆發雷達分", "主流資金分", "成交額百萬"]:
        if numeric_col not in tmp.columns:
            tmp[numeric_col] = 0.0
        tmp[numeric_col] = pd.to_numeric(tmp[numeric_col], errors="coerce").fillna(0.0)
    tmp["__priority"] = tmp.apply(_intraday_priority_score, axis=1)
    tmp["__sector"] = tmp.apply(_sector_key_for_row, axis=1)
    tmp = tmp.sort_values(
        ["__priority", "可操作分", "爆發雷達分", "主流資金分", "成交額百萬"],
        ascending=[False, False, False, False, False],
        kind="mergesort",
    )

    core_limit = 8
    backup_limit = 16
    sector_cap_core = 2
    core: list[Any] = []
    backup: list[Any] = []
    sector_count: dict[str, int] = {}

    for idx, row in tmp.iterrows():
        sector = _safe_str(row.get("__sector")) or "未分類"
        normal_core = (
            _safe_float(row.get("隔日可參考分"), 0) >= 64
            and _safe_str(row.get("隔日參考判定")).startswith("PASS")
            and _safe_str(row.get("進場可執行判定")).startswith("READY")
            and not _safe_str(row.get("進場可執行判定")).startswith("READY-M")
            and _safe_float(row.get("進場可執行分"), 0) >= 72
            and _safe_float(row.get("距最近可執行買點%"), 99) <= 2.5
            and _risk_reward_ratio(row) >= 1.45
            and 0 < _stop_distance_pct(row) <= 6.8
        )
        momentum = _momentum_profile(row)
        prebreak = _prebreakout_profile(row)
        momentum_core = bool(
            momentum["radar_ready"] and momentum["score"] >= 74
            and momentum["close_loc"] >= 72 and momentum["amount"] >= 100
            and (momentum["day_vol"] >= 1.15 or momentum["amount"] >= 500)
            and momentum["upper_shadow"] <= 42
        )
        prebreak_core = bool(
            prebreak["radar_ready"] and prebreak["score"] >= 72
            and prebreak["amount"] >= 150 and prebreak["missed"] >= 78
            and prebreak["radar"] >= 68
        )
        can_core = (
            len(core) < core_limit
            and sector_count.get(sector, 0) < sector_cap_core
            and _safe_float(row.get("__priority"), 0) >= 64
            and (normal_core or momentum_core or prebreak_core)
        )
        if can_core:
            core.append(idx)
            sector_count[sector] = sector_count.get(sector, 0) + 1
        elif len(backup) < backup_limit:
            backup.append(idx)

    core_set = set(core)
    backup_set = set(backup)
    order_map = {idx: i + 1 for i, idx in enumerate(list(core) + list(backup))}

    for idx in tmp.index:
        pr = _safe_float(tmp.at[idx, "__priority"], 0)
        out.at[idx, "盤中盯盤順序"] = int(order_map.get(idx, 999))
        if idx in core_set:
            momentum = _momentum_profile(tmp.loc[idx])
            if momentum["radar_ready"]:
                out.at[idx, "盤中雷達優先級"] = "R1-M｜強勢動能核心雷達"
                out.at[idx, "盤中雷達分層說明"] = f"強勢動能核心盯盤，優先分 {pr:.1f}；不可開盤追價，只接受回測守住或再突破放量。"
            elif _prebreakout_profile(tmp.loc[idx])["radar_ready"]:
                out.at[idx, "盤中雷達優先級"] = "R1-P｜強勢前兆核心雷達"
                out.at[idx, "盤中雷達分層說明"] = f"強勢前兆召回，優先分 {pr:.1f}；不預買，只在突破/回測確認後小量試單。"
            else:
                out.at[idx, "盤中雷達優先級"] = "R1｜核心雷達"
                out.at[idx, "盤中雷達分層說明"] = f"傳統買點核心盯盤，優先分 {pr:.1f}；需實戰觸發價與守價確認。"
            out.at[idx, "盤中雷達分層"] = "盤中核心雷達"
        elif idx in backup_set:
            out.at[idx, "盤中雷達優先級"] = "R2｜備援雷達"
            out.at[idx, "盤中雷達分層"] = "盤中備援雷達"
            out.at[idx, "盤中雷達分層說明"] = f"備援輪動名單，優先分 {pr:.1f}；盤中族群轉強時再提高關注。"
        else:
            out.at[idx, "盤中雷達優先級"] = "R3｜低優先觀察"
            out.at[idx, "盤中雷達分層"] = "盤中低優先觀察"
            out.at[idx, "盤中雷達分層說明"] = f"保留資料但不放主盯盤，優先分 {pr:.1f}；避免 30~40 檔清單造成失焦。"

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
    for c in FORMAL_RECOMMENDATION_COLUMNS:
        out[c] = rows[c].values if c in rows.columns else ""
    out = _apply_intraday_radar_tiers(out)
    return out

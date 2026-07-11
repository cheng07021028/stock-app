# -*- coding: utf-8 -*-
"""Phase 8.3 evidence-aware liquidity overlay for formal GodPick decisions."""
from __future__ import annotations
from typing import Any
import pandas as pd
from _phase83_core import godpick_formal_recommendation_engine_core as _core

for _name in dir(_core):
    if not _name.startswith("__"):
        globals()[_name] = getattr(_core, _name)

FORMAL_RECOMMENDATION_VERSION = "vnext_phase8_3_liquidity_recovery_20260712"


def _liquidity_info(row: pd.Series) -> dict[str, Any]:
    amount = _core._num(row, "成交額百萬", 0)
    avg_amount = _core._num(row, "20日均成交額百萬", 0)
    volume = max(_core._num(row, "最新成交量_張", 0), _core._num(row, "最新成交量張", 0))
    avg_volume = max(_core._num(row, "20日均量_張", 0), _core._num(row, "20日均量張", 0))
    price = _core._first_price(row, ["最新價", "推薦價格", "推薦日價格", "收盤價"], 0)
    if amount <= 0 and price > 0 and volume > 0:
        amount = price * volume / 1000.0
    if avg_amount <= 0 and price > 0 and avg_volume > 0:
        avg_amount = price * avg_volume / 1000.0
    known = any(v > 0 for v in [amount, avg_amount, volume, avg_volume])
    blob = _core._text_blob(row, ["流動性等級", "流動性資料狀態", "主流作戰分區", "冷門股警示", "主流資金角色"])
    ref_amount = amount if amount > 0 else avg_amount
    ref_volume = volume if volume > 0 else avg_volume
    quantitative_low = known and ((0 < ref_amount < 80) or (ref_amount <= 0 and 0 < ref_volume < 1000))
    text_low = _core._contains_any(blob, ["低流動性排除", "冷門禁追", "極低量", "低流動性"])
    explicit_low = bool(quantitative_low and text_low) or bool(known and 0 < ref_amount < 50)
    return {
        "known": known, "missing": not known, "explicit_low": explicit_low,
        "tradable": known and not explicit_low and (ref_amount >= 100 or ref_volume >= 1000),
        "amount": float(amount), "avg_amount": float(avg_amount),
        "volume": float(volume), "avg_volume": float(avg_volume),
    }


def _data_pending_only(reasons: list[str]) -> bool:
    soft = ("資料缺失", "資料待補", "待補成交額", "流動性資料")
    return bool(reasons) and all(any(key in reason for key in soft) for reason in reasons)


def _compute_operability_score(row: pd.Series) -> float:
    entry = _core._num(row, "Entry進場買點分", _core._num(row, "進場買點分", 45))
    risk = _core._num(row, "Risk風控安全分", _core._num(row, "風控安全分", 45))
    buy = _core._num(row, "買進分數", 45)
    rr = _core._num(row, "風險報酬比", 0)
    chase = _core._num(row, "追價風險分", 60)
    mainstream = _core._num(row, "主流資金分", 50)
    money = _core._num(row, "資金攻擊有效分", _core._num(row, "籌碼續航分", 50))
    sector = _core._num(row, "族群攻擊強度", _core._num(row, "族群輪動分", 50))
    liq = _liquidity_info(row)
    radar = max(
        _core._num(row, "爆發雷達分", 0), _core._num(row, "隔日爆發分", 0),
        _core._num(row, "飆股攻擊分", 0), _core._num(row, "主流領漲回補分", 0),
        _core._num(row, "漲停回放分", 0),
    )
    rr_score = _core._clamp(rr * 42.0, 0, 100)
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
        entry * .23 + risk * .20 + buy * .14 + rr_score * .14 + (100 - chase) * .08
        + mainstream * .08 + sector * .06 + money * .04 + amount_score * .03
    )
    score += max(0.0, radar - 72.0) * .08
    if not liq["known"]:
        score -= 4.0
    return round(_core._clamp(score), 1)


def _exclusion_reasons(row: pd.Series) -> list[str]:
    reasons: list[str] = []
    role_blob = _core._text_blob(row, ["推薦角色", "穩健推薦角色", "實戰過濾狀態", "主流作戰分區", "飆股雷達角色"])
    veto_blob = _core._text_blob(row, ["真禁買原因", "過熱原因", "正式推薦排除原因", "硬否決原因", "主推薦降級原因"])
    buy = _core._num(row, "買進分數", 0)
    entry = _core._num(row, "Entry進場買點分", _core._num(row, "進場買點分", 0))
    risk = _core._num(row, "Risk風控安全分", _core._num(row, "風控安全分", 0))
    rr = _core._num(row, "風險報酬比", 0)
    chase = _core._num(row, "追價風險分", 60)
    stop_dist = _core._stop_distance_pct(row)
    upside = _core._upside_space_pct(row)
    liq = _liquidity_info(row)
    if _core._contains_any(role_blob, ["D｜過熱禁買", "過熱禁買", "BLOCK", "禁止買進排除"]):
        reasons.append("角色已判定過熱/禁買")
    if _core._contains_any(veto_blob, ["過熱", "追價", "停損距離過大", "風控失衡", "禁買", "假強"]):
        reasons.append("存在硬風控、過熱或假強原因")
    if liq["missing"]:
        reasons.append("流動性資料缺失，待補成交額/成交量後重評")
    elif liq["explicit_low"] or (0 < liq["amount"] < 80):
        reasons.append("低流動性或冷門禁追")
    if buy < 30:
        reasons.append("買進分數過低")
    if entry < 35 and risk < 42:
        reasons.append("Entry/Risk 同時偏弱")
    if chase >= 78:
        reasons.append("追價風險過高")
    if rr < .25:
        reasons.append("風險報酬比過低")
    if stop_dist > 15:
        reasons.append(f"停損距離{stop_dist:.1f}%過大")
    if 0 < upside < 3:
        reasons.append(f"上方空間僅{upside:.1f}%")
    return list(dict.fromkeys(x for x in reasons if x))


def _direct_ok(row: pd.Series, op_score: float, exclusion: list[str]) -> bool:
    if exclusion:
        return False
    liq = _liquidity_info(row)
    role_blob = _core._text_blob(row, ["推薦角色", "飆股雷達角色", "領漲回補角色", "主流作戰分區"])
    amount = liq["amount"] if liq["amount"] > 0 else liq["avg_amount"]
    buy = _core._num(row, "買進分數", 0)
    entry = _core._num(row, "Entry進場買點分", _core._num(row, "進場買點分", 0))
    risk = _core._num(row, "Risk風控安全分", _core._num(row, "風控安全分", 0))
    rr = _core._num(row, "風險報酬比", 0)
    chase = _core._num(row, "追價風險分", 100)
    mainstream = _core._num(row, "主流資金分", 0)
    sector = max(_core._num(row, "族群攻擊強度", 0), _core._num(row, "族群輪動分", 0))
    practical = _core._num(row, "股神實戰總分", 0)
    candidate = max(_core._num(row, "推薦總分", 0), _core._num(row, "候選強度分", 0), _core._num(row, "Alpha選股潛力分", 0))
    stop_dist = _core._stop_distance_pct(row)
    upside = _core._upside_space_pct(row)
    market = _core._market_risk_info(row)
    strong_role = _core._contains_any(role_blob, ["A｜股神主推薦", "S｜飆股攻擊候選", "主流攻擊候選", "B+｜盤中點火追蹤", "L｜主流強勢回補"])
    metric_override = candidate >= 78 and practical >= 70 and entry >= 68 and risk >= 64
    return bool(
        op_score >= 68 and practical >= 64 and buy >= 55 and entry >= 62 and risk >= 58
        and rr >= 1.2 and chase <= 62 and (stop_dist <= 10.5 or stop_dist == 0)
        and upside >= 7 and mainstream >= 58 and sector >= 50 and liq["tradable"]
        and amount >= 200 and not market["severe"] and (strong_role or metric_override)
    )


def _a_minus_ok(row: pd.Series, op_score: float, exclusion: list[str]) -> bool:
    severe = ["過熱", "禁買", "低流動性", "冷門", "成交額不足", "買進分數過低", "追價風險過高", "Entry/Risk 同時偏弱", "停損距離", "上方空間", "假強", "資料缺失", "資料待補"]
    if any(any(k in r for k in severe) for r in exclusion):
        return False
    liq = _liquidity_info(row)
    amount = liq["amount"] if liq["amount"] > 0 else liq["avg_amount"]
    role_blob = _core._text_blob(row, ["推薦角色", "飆股雷達角色", "領漲回補角色", "回放校正角色", "主流作戰分區"])
    buy = _core._num(row, "買進分數", 0)
    entry = _core._num(row, "Entry進場買點分", _core._num(row, "進場買點分", 0))
    risk = _core._num(row, "Risk風控安全分", _core._num(row, "風控安全分", 0))
    rr = _core._num(row, "風險報酬比", 0)
    chase = _core._num(row, "追價風險分", 100)
    mainstream = _core._num(row, "主流資金分", 0)
    sector = max(_core._num(row, "族群攻擊強度", 0), _core._num(row, "族群輪動分", 0))
    radar = max(_core._num(row, "爆發雷達分", 0), _core._num(row, "隔日爆發分", 0), _core._num(row, "飆股攻擊分", 0), _core._num(row, "主流領漲回補分", 0), _core._num(row, "漲停回放分", 0))
    has_role = _core._contains_any(role_blob, ["A｜股神主推薦", "S｜飆股攻擊候選", "B+｜盤中點火追蹤", "L｜主流強勢回補", "T｜題材轉強追蹤", "B｜等突破確認", "主流突破追蹤", "主流攻擊候選"])
    return bool(
        has_role and op_score >= 55 and buy >= 45 and entry >= 52 and risk >= 44
        and rr >= .7 and chase <= 72
        and (_core._stop_distance_pct(row) <= 12.5 or _core._stop_distance_pct(row) == 0)
        and _core._upside_space_pct(row) >= 5 and mainstream >= 56 and sector >= 52
        and radar >= 64 and liq["tradable"] and amount >= 150
        and not _core._market_risk_info(row)["severe"]
    )


for _name, _fn in {
    "_liquidity_info": _liquidity_info,
    "_data_pending_only": _data_pending_only,
    "_compute_operability_score": _compute_operability_score,
    "_exclusion_reasons": _exclusion_reasons,
    "_direct_ok": _direct_ok,
    "_a_minus_ok": _a_minus_ok,
}.items():
    setattr(_core, _name, _fn)
_core.FORMAL_RECOMMENDATION_VERSION = FORMAL_RECOMMENDATION_VERSION

_original_classify = _core._classify

def _classify(row: pd.Series) -> dict[str, Any]:
    result = _original_classify(row)
    reasons = _exclusion_reasons(row)
    if result.get("正式推薦分區") == "正式排除清單" and _data_pending_only(reasons):
        result.update({
            "最終操作結論": "C｜資料待補：不列正式推薦",
            "是否正式推薦": "否",
            "操作許可": "不可直接買｜待補成交額/成交量",
            "正式推薦等級": "C｜資料待補觀察",
            "候選性質": "資料待補觀察",
            "風控否決旗標": "否",
            "正式推薦分區": "不可直接買觀察",
            "正式推薦資格": "DATA｜流動性資料待補",
            "下週是否可直接買": "不可",
            "正式推薦動作": "補齊成交額/成交量後重新評分；缺值不得視為低流動性。",
            "正式推薦排除原因": "、".join(reasons),
            "建議倉位上限%": 0.0,
            "決策一致性": "一致｜資料不足，僅保留診斷，不得進場",
            "正式推薦版本": FORMAL_RECOMMENDATION_VERSION,
        })
    return result

_core._classify = _classify
_classify = _classify
apply_formal_recommendation_engine = _core.apply_formal_recommendation_engine

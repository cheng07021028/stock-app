# -*- coding: utf-8 -*-
"""Phase 6.3 formal recommendation purifier.

目的：把「推薦候選 / 雷達 / 回放 / 排除」重新分成可操作清單，避免
完整推薦表裡的 D、弱勢觀察、高風險雷達被使用者誤解為正式買進推薦。

本模組只處理 DataFrame 欄位，不讀寫 JSON、不連網、不覆蓋既有欄位。
"""
from __future__ import annotations

from typing import Any
import pandas as pd

FORMAL_RECOMMENDATION_VERSION = "vnext_phase6_3_formal_purifier_20260613"

FORMAL_RECOMMENDATION_COLUMNS = [
    "可操作分",
    "正式推薦分區",
    "正式推薦資格",
    "正式推薦動作",
    "下週是否可直接買",
    "盤中雷達等級",
    "盤中雷達動作",
    "正式推薦排除原因",
    "正式推薦排序分",
    "正式推薦版本",
]

NUMERIC_FORMAL_RECOMMENDATION_COLUMNS = {
    "可操作分",
    "正式推薦排序分",
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


def _text_blob(row: pd.Series, cols: list[str]) -> str:
    return "｜".join(_safe_str(row.get(c)) for c in cols if c in row.index)


def _contains_any(text: str, keys: list[str]) -> bool:
    return any(k in text for k in keys)


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return lo


def _compute_operability_score(row: pd.Series) -> float:
    """專業可操作分：不等於推薦總分，重買點、風控、RR、主流資金與族群。"""
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 45))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 45))
    buy = _num(row, "買進分數", 45)
    rr = _num(row, "風險報酬比", 0)
    chase = _num(row, "追價風險分", 60)
    mainstream = _num(row, "主流資金分", 50)
    money = _num(row, "資金攻擊有效分", _num(row, "籌碼續航分", 50))
    sector = _num(row, "族群攻擊強度", _num(row, "族群輪動分", 50))
    amount = _num(row, "成交額百萬", 0)
    radar = max(
        _num(row, "爆發雷達分", 0),
        _num(row, "隔日爆發分", 0),
        _num(row, "飆股攻擊分", 0),
        _num(row, "主流領漲回補分", 0),
        _num(row, "漲停回放分", 0),
    )
    rr_score = _clamp(rr * 42.0, 0, 100)
    amount_score = 0.0
    if amount >= 5000:
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
        entry * 0.23
        + risk * 0.20
        + buy * 0.14
        + rr_score * 0.14
        + (100 - chase) * 0.08
        + mainstream * 0.08
        + sector * 0.06
        + money * 0.04
        + amount_score * 0.03
    )
    # 飆股雷達只提供小幅加分，不能覆蓋差買點/差風控。
    score += max(0.0, radar - 72.0) * 0.08
    return round(_clamp(score), 1)


def _exclusion_reasons(row: pd.Series) -> list[str]:
    reasons: list[str] = []
    role_blob = _text_blob(row, ["推薦角色", "穩健推薦角色", "實戰過濾狀態", "主流作戰分區", "飆股雷達角色"])
    veto_blob = _text_blob(row, ["真禁買原因", "過熱原因", "正式推薦排除原因", "硬否決原因", "主推薦降級原因"])
    liquidity_blob = _text_blob(row, ["主流作戰分區", "冷門股警示", "主流股判定", "主流資金角色"])
    buy = _num(row, "買進分數", 0)
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    rr = _num(row, "風險報酬比", 0)
    chase = _num(row, "追價風險分", 0)
    amount = _num(row, "成交額百萬", 0)

    if _contains_any(role_blob, ["D｜過熱禁買", "過熱禁買", "BLOCK", "禁止買進排除"]):
        reasons.append("角色已判定過熱/禁買")
    if _contains_any(veto_blob, ["過熱", "追價", "停損距離過大", "風控失衡", "禁買"]):
        reasons.append("存在硬風控或過熱原因")
    if _contains_any(liquidity_blob, ["低流動性排除", "冷門禁追"]):
        reasons.append("低流動性或冷門禁追")
    if amount < 80:
        reasons.append("成交額不足，易滑價或假突破")
    if buy and buy < 30:
        reasons.append("買進分數過低")
    if entry < 35 and risk < 42:
        reasons.append("Entry/Risk 同時偏弱")
    if chase >= 78 and buy < 45:
        reasons.append("追價風險過高且買進分數不足")
    if rr and rr < 0.25:
        reasons.append("風險報酬比過低")
    # 去重保序
    out: list[str] = []
    for r in reasons:
        if r and r not in out:
            out.append(r)
    return out


def _direct_ok(row: pd.Series, op_score: float, exclusion: list[str]) -> bool:
    if exclusion:
        return False
    role_blob = _text_blob(row, ["推薦角色", "飆股雷達角色", "領漲回補角色", "主流作戰分區"])
    amount = _num(row, "成交額百萬", 0)
    buy = _num(row, "買進分數", 0)
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    rr = _num(row, "風險報酬比", 0)
    chase = _num(row, "追價風險分", 100)
    mainstream = _num(row, "主流資金分", 0)
    sector = _num(row, "族群攻擊強度", 0)
    practical = _num(row, "股神實戰總分", 0)
    return (
        op_score >= 68
        and practical >= 64
        and buy >= 55
        and entry >= 62
        and risk >= 58
        and rr >= 1.15
        and chase <= 62
        and mainstream >= 62
        and sector >= 50
        and amount >= 250
        and _contains_any(role_blob, ["A｜股神主推薦", "S｜飆股攻擊候選", "主流攻擊候選", "B+｜盤中點火追蹤", "L｜主流強勢回補"])
    )


def _intraday_radar_ok(row: pd.Series, op_score: float, exclusion: list[str]) -> bool:
    if any("過熱" in r or "禁買" in r for r in exclusion):
        return False
    if any("低流動性" in r for r in exclusion):
        return False
    role_blob = _text_blob(row, ["推薦角色", "飆股雷達角色", "領漲回補角色", "回放校正角色", "主流作戰分區"])
    amount = _num(row, "成交額百萬", 0)
    entry = _num(row, "Entry進場買點分", _num(row, "進場買點分", 0))
    risk = _num(row, "Risk風控安全分", _num(row, "風控安全分", 0))
    buy = _num(row, "買進分數", 0)
    rr = _num(row, "風險報酬比", 0)
    chase = _num(row, "追價風險分", 100)
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
    return (
        has_attack_role
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


def _risk_radar_ok(row: pd.Series, op_score: float) -> bool:
    role_blob = _text_blob(row, ["飆股雷達角色", "領漲回補角色", "回放校正角色", "主流作戰分區"])
    radar = max(_num(row, "爆發雷達分", 0), _num(row, "隔日爆發分", 0), _num(row, "主流領漲回補分", 0), _num(row, "漲停回放分", 0))
    amount = _num(row, "成交額百萬", 0)
    return amount >= 100 and radar >= 65 and op_score >= 38 and _contains_any(role_blob, ["R｜高風險爆發觀察", "B+｜盤中點火追蹤", "S｜飆股攻擊候選", "T｜題材轉強追蹤", "L｜主流強勢回補", "M｜強勢漏選追蹤"])


def _trigger_text(row: pd.Series) -> str:
    for c in ["盤中轉強觸發價", "突破確認價", "突破確認價_隔日", "近端壓力", "第一壓力價"]:
        v = _safe_str(row.get(c))
        if v:
            return f"放量站上 {v} 且同族群維持強勢"
    return "放量突破前高/壓力並站穩，未觸發前不買"


def _classify(row: pd.Series) -> dict[str, Any]:
    op = _compute_operability_score(row)
    reasons = _exclusion_reasons(row)
    direct = _direct_ok(row, op, reasons)
    intraday = _intraday_radar_ok(row, op, reasons)
    risk_radar = _risk_radar_ok(row, op)
    role_blob = _text_blob(row, ["推薦角色", "飆股雷達角色", "主流作戰分區"])

    if direct:
        bucket = "正式下週主推薦"
        qual = "PASS｜可列正式推薦"
        direct_buy = "可｜但仍需分批與停損"
        action = "可依觸發價/支撐分批進攻；第一筆不超過建議倉位，跌破失效條件立即退出。"
        radar_level = "主攻"
        radar_action = "正式推薦優先追蹤"
        exclude_text = ""
    elif intraday:
        bucket = "盤中雷達追蹤"
        qual = "WAIT｜未觸發不可買"
        direct_buy = "不可｜等盤中觸發"
        action = f"{_trigger_text(row)}；未觸發前只盯盤，不預先買。"
        radar_level = "B+｜盤中點火追蹤"
        radar_action = "只在量價/族群同步確認後小量試單"
        exclude_text = ""
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

    sort_score = op
    if bucket == "正式下週主推薦":
        sort_score += 20
    elif bucket == "盤中雷達追蹤":
        sort_score += 10
    elif bucket == "高風險雷達觀察":
        sort_score += 4
    elif bucket == "正式排除清單":
        sort_score -= 20
    return {
        "可操作分": round(op, 1),
        "正式推薦分區": bucket,
        "正式推薦資格": qual,
        "正式推薦動作": action,
        "下週是否可直接買": direct_buy,
        "盤中雷達等級": radar_level,
        "盤中雷達動作": radar_action,
        "正式推薦排除原因": exclude_text,
        "正式推薦排序分": round(_clamp(sort_score, 0, 120), 1),
        "正式推薦版本": FORMAL_RECOMMENDATION_VERSION,
    }


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
    return out

# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Iterable, List
import math
import pandas as pd

V72_FACTOR_FIELDS = [
    "大盤狀態",
    "大盤分層",
    "大盤情境說明",
    "上漲機率估計%",
    "上漲機率信心",
    "趨勢因子分數",
    "起漲因子分數",
    "量能因子分數",
    "技術型態分數",
    "類股強度分數",
    "大盤風控分數",
    "風險扣分",
    "R/R分數",
    "績效校正分數",
    "權重校正版本",
]

FACTOR_SCORE_FIELDS = [
    "趨勢因子分數",
    "起漲因子分數",
    "量能因子分數",
    "技術型態分數",
    "類股強度分數",
    "大盤風控分數",
    "風險扣分",
    "R/R分數",
    "績效校正分數",
]

V72_DEFAULT_WEIGHTS = {
    "市場環境": 10,
    "技術結構": 15,
    "起漲前兆": 21,
    "類股熱度": 9,
    "自動因子": 7,
    "交易可行": 13,
    "型態突破": 11,
    "爆發力": 7,
    "風險報酬": 7,
}

def _is_blank(v: Any) -> bool:
    if v is None:
        return True
    try:
        if isinstance(v, float) and math.isnan(v):
            return True
    except Exception:
        pass
    s = str(v).strip()
    return s == "" or s.lower() in {"none", "nan", "null", "na", "--", "—"}

def _to_float(v: Any, default: float = 0.0) -> float:
    if _is_blank(v):
        return default
    try:
        s = str(v).replace("%", "").replace(",", "").strip()
        return float(s)
    except Exception:
        return default

def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(v)))

def infer_market_context(row: Dict[str, Any] | None = None, default_state: str = "盤整") -> Dict[str, str]:
    row = row or {}
    for k in ["大盤狀態", "大盤分層", "大盤情境", "盤勢", "市場環境"]:
        if not _is_blank(row.get(k)):
            state = str(row.get(k)).strip()
            break
    else:
        state = default_state

    if any(x in state for x in ["多", "強", "上升", "偏多"]):
        layer = "多頭"
        desc = "大盤偏多，趨勢與族群強度權重可正常參考。"
    elif any(x in state for x in ["空", "弱", "下跌", "偏空"]):
        layer = "空頭"
        desc = "大盤偏弱，應降低追高權重並提高風控比重。"
    else:
        layer = "盤整"
        desc = "大盤盤整，建議重視拉回、支撐與風險報酬。"

    return {"大盤狀態": state, "大盤分層": layer, "大盤情境說明": desc}

def infer_factor_scores(row: Dict[str, Any]) -> Dict[str, float]:
    score = _to_float(
        row.get("推薦總分", row.get("推薦分數", row.get("股神決策分數", 70))),
        70,
    )
    category_strength = _to_float(
        row.get("市場環境分數", row.get("市場強勢分數", row.get("類股強度分數", score))),
        score,
    )
    rr = _to_float(row.get("風險報酬比", row.get("R/R", row.get("RR", 1.0))), 1.0)

    reason_text = " ".join(str(row.get(k, "")) for k in ["推薦理由", "推薦原因", "股神推論", "建議動作", "等待條件"])

    trend = score
    if any(x in reason_text for x in ["均線多頭", "趨勢", "續強", "轉強", "主升"]):
        trend += 8
    if any(x in reason_text for x in ["破線", "轉弱", "跌破"]):
        trend -= 10

    early = score
    if any(x in reason_text for x in ["起漲", "初步轉強", "剛轉強", "低位", "轉折"]):
        early += 10
    if any(x in reason_text for x in ["過熱", "追高"]):
        early -= 8

    volume = score
    if any(x in reason_text for x in ["量增", "放量", "量能", "資金流"]):
        volume += 8
    if any(x in reason_text for x in ["量縮", "量不足"]):
        volume -= 8

    pattern = score
    if any(x in reason_text for x in ["突破", "平台整理突破", "箱型突破"]):
        pattern += 10
    if any(x in reason_text for x in ["跌破", "假突破"]):
        pattern -= 8

    risk_penalty = 0
    if any(x in reason_text for x in ["高風險", "風險控管", "小部位", "不追高"]):
        risk_penalty += 12
    if score < 70:
        risk_penalty += 8

    rr_score = _clamp(50 + rr * 20, 0, 100)

    return {
        "趨勢因子分數": round(_clamp(trend), 2),
        "起漲因子分數": round(_clamp(early), 2),
        "量能因子分數": round(_clamp(volume), 2),
        "技術型態分數": round(_clamp(pattern), 2),
        "類股強度分數": round(_clamp(category_strength), 2),
        "大盤風控分數": round(_clamp(100 - risk_penalty), 2),
        "風險扣分": round(_clamp(risk_penalty), 2),
        "R/R分數": round(_clamp(rr_score), 2),
        "績效校正分數": round(_clamp(score), 2),
    }

def infer_upside_probability(row: Dict[str, Any], factor_scores: Dict[str, float]) -> Dict[str, Any]:
    for k in ["上漲機率估計%", "上漲機率%", "預估上漲機率", "上漲機率"]:
        if not _is_blank(row.get(k)):
            p = _clamp(_to_float(row.get(k), 50))
            return {"上漲機率估計%": round(p, 2), "上漲機率信心": "原生"}

    vals = [factor_scores.get(k, 0) for k in ["趨勢因子分數", "起漲因子分數", "量能因子分數", "技術型態分數", "類股強度分數", "R/R分數"]]
    vals = [v for v in vals if v > 0]
    p = sum(vals) / max(len(vals), 1)
    # 分數轉機率，避免過度樂觀
    p = 45 + (p - 60) * 0.55
    return {"上漲機率估計%": round(_clamp(p, 35, 85), 2), "上漲機率信心": "代理"}

def enrich_record(row: Dict[str, Any], market_context: Dict[str, str] | None = None) -> Dict[str, Any]:
    out = dict(row)
    mc = market_context or infer_market_context(row)
    for k, v in mc.items():
        if _is_blank(out.get(k)):
            out[k] = v

    factor_scores = infer_factor_scores(out)
    for k, v in factor_scores.items():
        if _is_blank(out.get(k)):
            out[k] = v

    prob = infer_upside_probability(out, factor_scores)
    for k, v in prob.items():
        if _is_blank(out.get(k)):
            out[k] = v

    out["權重校正版本"] = out.get("權重校正版本") or "v72_factor_fields"
    return out

def enrich_dataframe(df: pd.DataFrame, market_context: Dict[str, str] | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    rows = []
    for _, r in df.iterrows():
        rows.append(enrich_record(r.to_dict(), market_context))
    out = pd.DataFrame(rows)
    # 保留原欄位順序，再補新增欄位
    ordered = list(df.columns) + [c for c in V72_FACTOR_FIELDS if c not in df.columns]
    rest = [c for c in out.columns if c not in ordered]
    return out[[c for c in ordered + rest if c in out.columns]]

def ensure_factor_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return df
    for c in V72_FACTOR_FIELDS:
        if c not in df.columns:
            df[c] = ""
    return df

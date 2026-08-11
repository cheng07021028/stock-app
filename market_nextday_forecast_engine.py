# -*- coding: utf-8 -*-
"""Explainable next-session market forecast for the GodPick system.

The engine is deliberately local and deterministic.  It does not fetch data and
never claims certainty.  It combines domestic trend, OTC breadth, futures,
overseas markets, institutional flow and event/risk information into a bounded
probability forecast, then exposes a small, confidence-aware adjustment for the
stock recommendation engine.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
import json
import math

import pandas as pd

FORECAST_VERSION = "nextday_market_forecast_v2_2_durable_v183_20260811"
DEFAULT_RECORDS_FILE = "market_nextday_forecast_records.json"


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").replace("+", "").strip()
            if value.lower() in {"", "-", "--", "none", "nan", "null", "—"}:
                return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _sigmoid(value: float) -> float:
    value = _clip(value, -30, 30)
    return 1.0 / (1.0 + math.exp(-value))


def _softmax(values: Iterable[float]) -> list[float]:
    vals = [float(v) for v in values]
    if not vals:
        return []
    m = max(vals)
    exps = [math.exp(_clip(v - m, -30, 30)) for v in vals]
    total = sum(exps) or 1.0
    return [v / total for v in exps]


def _next_weekday(date_value: Any) -> str:
    dt = pd.to_datetime(date_value, errors="coerce")
    if pd.isna(dt):
        dt = pd.Timestamp.now()
    nxt = dt.date() + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return nxt.strftime("%Y-%m-%d")


def _history_frame(history: Any) -> pd.DataFrame:
    """Normalize page-01 cache/DataFrame into 日期/收盤/漲跌幅%."""
    if history is None:
        return pd.DataFrame(columns=["日期", "收盤", "漲跌幅%"])
    if isinstance(history, pd.DataFrame):
        raw = history.copy()
    elif isinstance(history, dict):
        rows: list[dict[str, Any]] = []
        for key, value in history.items():
            if not isinstance(value, dict):
                continue
            row = dict(value)
            row.setdefault("date", key)
            rows.append(row)
        raw = pd.DataFrame(rows)
    elif isinstance(history, list):
        raw = pd.DataFrame(history)
    else:
        return pd.DataFrame(columns=["日期", "收盤", "漲跌幅%"])
    if raw.empty:
        return pd.DataFrame(columns=["日期", "收盤", "漲跌幅%"])

    def first(names: list[str]) -> pd.Series:
        for name in names:
            if name in raw.columns:
                return raw[name]
        return pd.Series([None] * len(raw), index=raw.index)

    out = pd.DataFrame(index=raw.index)
    out["日期"] = pd.to_datetime(first(["日期", "date", "used_date", "data_date"]), errors="coerce")
    out["收盤"] = pd.to_numeric(first(["收盤", "close", "twse_index"]), errors="coerce")
    out["漲跌幅%"] = pd.to_numeric(first(["漲跌幅%", "pct", "change_pct", "twse_change_pct"]), errors="coerce")
    out = out.dropna(subset=["日期", "收盤"]).drop_duplicates("日期", keep="last").sort_values("日期")
    if not out.empty:
        calculated = out["收盤"].pct_change() * 100
        out["漲跌幅%"] = out["漲跌幅%"].where(out["漲跌幅%"].notna(), calculated)
    return out.reset_index(drop=True)


def _read_records(path: str | Path = DEFAULT_RECORDS_FILE) -> list[dict[str, Any]]:
    try:
        p = Path(path)
        if not p.exists():
            return []
        data = json.loads(p.read_text(encoding="utf-8-sig"))
        return [dict(x) for x in data if isinstance(x, dict)] if isinstance(data, list) else []
    except Exception:
        return []


def _write_records(records: list[dict[str, Any]], path: str | Path = DEFAULT_RECORDS_FILE) -> None:
    p = Path(path)
    tmp = p.with_suffix(p.suffix + ".tmp")
    payload = records[-500:]
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    tmp.replace(p)
    try:
        from godpick_durability_service import persist_json_async
        persist_json_async(str(p), payload, reason="V183 nextday forecast history")
    except Exception:
        pass


def _direction_from_return(value: float | None, flat_band: float = 0.25) -> str:
    if value is None:
        return ""
    if value > flat_band:
        return "上漲"
    if value < -flat_band:
        return "下跌"
    return "震盪"


def _calibration_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    evaluated = [r for r in records if _num(r.get("actual_return_pct")) is not None]
    evaluated = evaluated[-60:]
    if not evaluated:
        return {
            "evaluated_count": 0,
            "direction_hit_rate_pct": None,
            "mean_abs_error_pct": None,
            "expected_return_bias_pct": 0.0,
            "reliability_factor": 0.82,
            "status": "尚無足夠歷史預測績效，先採保守信心。",
        }
    hits = [bool(r.get("direction_hit")) for r in evaluated]
    hit_rate = sum(hits) / len(hits) * 100
    errors = []
    signed_errors = []
    for row in evaluated:
        actual = _num(row.get("actual_return_pct"))
        expected = _num(row.get("expected_return_pct"))
        if actual is None or expected is None:
            continue
        errors.append(abs(actual - expected))
        signed_errors.append(actual - expected)
    mae = sum(errors) / len(errors) if errors else None
    bias = float(pd.Series(signed_errors).median()) if signed_errors else 0.0
    if len(evaluated) < 8:
        reliability = 0.86
        status = "樣本仍少，預測強度已自動收斂。"
    elif hit_rate >= 62:
        reliability = 1.02
        status = "近期方向命中率良好。"
    elif hit_rate >= 54:
        reliability = 0.94
        status = "近期方向命中率尚可。"
    elif hit_rate >= 46:
        reliability = 0.82
        status = "近期方向命中率普通，已降低預測強度。"
    else:
        reliability = 0.68
        status = "近期命中率偏低，已明顯收斂至中性。"
    return {
        "evaluated_count": len(evaluated),
        "direction_hit_rate_pct": round(hit_rate, 1),
        "mean_abs_error_pct": None if mae is None else round(mae, 3),
        "expected_return_bias_pct": round(_clip(bias, -0.35, 0.35), 3),
        "reliability_factor": reliability,
        "status": status,
    }


def evaluate_and_store_forecast(
    forecast: dict[str, Any],
    market_history: Any = None,
    records_path: str | Path = DEFAULT_RECORDS_FILE,
) -> dict[str, Any]:
    """Evaluate older forecasts with the next available close and upsert current forecast."""
    records = _read_records(records_path)
    hist = _history_frame(market_history)
    if not hist.empty:
        dates = hist["日期"].dt.strftime("%Y-%m-%d").tolist()
        closes = hist["收盤"].astype(float).tolist()
        for rec in records:
            if _num(rec.get("actual_return_pct")) is not None:
                continue
            base_date = _text(rec.get("data_date"))
            if not base_date or base_date not in dates:
                continue
            idx = dates.index(base_date)
            if idx + 1 >= len(dates) or closes[idx] == 0:
                continue
            actual = (closes[idx + 1] / closes[idx] - 1) * 100
            actual_dir = _direction_from_return(actual)
            predicted = _text(rec.get("direction"))
            rec["actual_date"] = dates[idx + 1]
            rec["actual_return_pct"] = round(actual, 4)
            rec["actual_direction"] = actual_dir
            rec["direction_hit"] = bool(predicted == actual_dir)
            rec["evaluated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    current = {
        "version": FORECAST_VERSION,
        "created_at": forecast.get("created_at"),
        "data_date": forecast.get("data_date"),
        "forecast_for_date": forecast.get("forecast_for_date"),
        "direction": forecast.get("direction"),
        "direction_score": forecast.get("direction_score"),
        "confidence": forecast.get("confidence"),
        "confidence_score": forecast.get("confidence_score"),
        "up_probability_pct": forecast.get("up_probability_pct"),
        "flat_probability_pct": forecast.get("flat_probability_pct"),
        "down_probability_pct": forecast.get("down_probability_pct"),
        "expected_return_pct": forecast.get("expected_return_pct"),
        "expected_low": forecast.get("expected_low"),
        "expected_high": forecast.get("expected_high"),
        "data_coverage_pct": forecast.get("data_coverage_pct"),
    }
    key = (current.get("data_date"), current.get("forecast_for_date"), current.get("version"))
    replaced = False
    for i, rec in enumerate(records):
        rec_key = (rec.get("data_date"), rec.get("forecast_for_date"), rec.get("version"))
        if rec_key == key:
            # Preserve already evaluated fields while refreshing the prediction.
            for name in ["actual_date", "actual_return_pct", "actual_direction", "direction_hit", "evaluated_at"]:
                if name in rec:
                    current[name] = rec[name]
            records[i] = current
            replaced = True
            break
    if not replaced:
        records.append(current)
    _write_records(records, records_path)
    return _calibration_summary(records)


def _factor(value: float | None, weight: float, label: str, source: str, available: bool = True) -> dict[str, Any]:
    return {
        "label": label,
        "score": None if value is None else round(_clip(value, 0, 100), 2),
        "weight": weight,
        "available": bool(available and value is not None),
        "source": source,
    }


def build_nextday_market_forecast(
    snapshot: dict[str, Any] | None,
    market_history: Any = None,
    records_path: str | Path = DEFAULT_RECORDS_FILE,
    now: datetime | None = None,
) -> dict[str, Any]:
    snapshot = dict(snapshot or {})
    now = now or datetime.now()
    hist = _history_frame(market_history)
    records = _read_records(records_path)
    calibration = _calibration_summary(records)

    close = _num(snapshot.get("twse_index") or snapshot.get("close"))
    ma5 = _num(snapshot.get("twse_ma5") or snapshot.get("ma5"))
    ma20 = _num(snapshot.get("twse_ma20") or snapshot.get("ma20"))
    tw_pct = _num(snapshot.get("twse_change_pct") or snapshot.get("pct"), 0.0) or 0.0
    otc_pct = _num(snapshot.get("otc_change_pct"))
    futures_pct = _num(snapshot.get("night_futures_change_pct"))
    if futures_pct is None:
        futures_pct = _num(snapshot.get("futures_change_pct"))
    nasdaq = _num(snapshot.get("nasdaq_change_pct"))
    sp500 = _num(snapshot.get("sp500_change_pct"))
    sox = _num(snapshot.get("sox_change_pct"))
    vix_pct = _num(snapshot.get("vix_change_pct"))
    overnight_score = _num(snapshot.get("overnight_score"))
    institutional_score = _num(snapshot.get("institutional_score"))
    institutional_total = _num(snapshot.get("institutional_total_100m"))
    event_factor = snapshot.get("event_factor") if isinstance(snapshot.get("event_factor"), dict) else {}
    event_adj = _num(event_factor.get("event_score_adjustment"), 0.0) or 0.0

    ret5 = ret20 = volatility = None
    if not hist.empty:
        closes = hist["收盤"].astype(float)
        if len(closes) >= 2:
            returns = closes.pct_change().dropna() * 100
            volatility = float(returns.tail(10).std(ddof=0)) if len(returns) >= 3 else float(returns.std(ddof=0) or 0)
        if len(closes) >= 6 and closes.iloc[-6] != 0:
            ret5 = (closes.iloc[-1] / closes.iloc[-6] - 1) * 100
        if len(closes) >= 21 and closes.iloc[-21] != 0:
            ret20 = (closes.iloc[-1] / closes.iloc[-21] - 1) * 100
        close = close if close is not None else float(closes.iloc[-1])
        ma5 = ma5 if ma5 is not None else float(closes.tail(5).mean())
        ma20 = ma20 if ma20 is not None else float(closes.tail(20).mean())

    market_score = _num(snapshot.get("market_score"), 50.0) or 50.0
    tech = 50.0 + (market_score - 50.0) * 0.35 + _clip(tw_pct * 4.0, -8, 8)
    if close and ma5:
        tech += 8 if close >= ma5 else -8
    if close and ma20:
        tech += 10 if close >= ma20 else -10
        dist20 = (close / ma20 - 1) * 100
        if dist20 > 6:
            tech -= min(10, (dist20 - 6) * 1.5)
    else:
        dist20 = None
    if ma5 and ma20:
        tech += 7 if ma5 >= ma20 else -7
    if ret5 is not None:
        tech += _clip(ret5 * 1.15, -9, 9)
    if ret20 is not None:
        tech += _clip(ret20 * 0.35, -7, 7)
    if volatility is not None and volatility > 2.3:
        tech -= min(8, (volatility - 2.3) * 3.0)

    breadth = None
    if otc_pct is not None:
        breadth = 50 + _clip(otc_pct * 6.0, -15, 15) + _clip((otc_pct - tw_pct) * 7.0, -16, 16)

    futures = None
    if futures_pct is not None or overnight_score is not None:
        futures = 50.0
        if futures_pct is not None:
            futures += _clip(futures_pct * 12.0, -24, 24)
        if overnight_score is not None:
            futures += (overnight_score - 50.0) * 0.28

    overseas_values = []
    overseas = 50.0
    for val, mult in [(nasdaq, 7.0), (sp500, 6.0), (sox, 6.5)]:
        if val is not None:
            overseas += _clip(val * mult, -12, 12)
            overseas_values.append(val)
    if vix_pct is not None:
        overseas -= _clip(vix_pct * 2.0, -10, 10)
        overseas_values.append(-vix_pct)
    if not overseas_values:
        overseas = None

    institutional = None
    if institutional_score is not None:
        institutional = institutional_score
    elif institutional_total is not None:
        institutional = 50 + _clip(institutional_total / 12.0, -18, 18)

    risk_event = 50.0 + _clip(event_adj * 3.0, -15, 15)
    risk_gate_text = _text(snapshot.get("risk_gate")).lower()
    if any(k in risk_gate_text for k in ["觀望", "高風險", "strict", "defensive"]):
        risk_event -= 12
    elif any(k in risk_gate_text for k in ["保守", "conservative"]):
        risk_event -= 5
    elif any(k in risk_gate_text for k in ["可進場", "normal", "bullish"]):
        risk_event += 6
    overnight_risk = _text(snapshot.get("overnight_risk_level"))
    if any(k in overnight_risk for k in ["高", "偏高"]):
        risk_event -= 8
    elif any(k in overnight_risk for k in ["低", "偏低"]):
        risk_event += 4

    factors = {
        "technical_trend": _factor(tech, 0.30, "台股技術趨勢", "加權收盤、MA5/MA20、近端動能"),
        "market_breadth": _factor(breadth, 0.14, "市場廣度與櫃買強弱", "櫃買相對加權"),
        "futures_overnight": _factor(futures, 0.18, "台指期與隔夜風險", "台指期、夜盤、隔夜分數"),
        "overseas": _factor(overseas, 0.16, "美股與半導體風向", "NASDAQ、S&P500、SOX、VIX"),
        "institutional": _factor(institutional, 0.10, "法人資金方向", "三大法人或法人分數"),
        "risk_event": _factor(risk_event, 0.12, "事件與風控環境", "事件快取、風控閘門、隔夜風險"),
    }
    available = [v for v in factors.values() if v["available"]]
    available_weight = sum(float(v["weight"]) for v in available)
    raw_score = sum(float(v["score"]) * float(v["weight"]) for v in available) / available_weight if available_weight else 50.0

    reliability = _num(calibration.get("reliability_factor"), 0.82) or 0.82
    adjusted_score = 50 + (raw_score - 50) * reliability
    expected_bias = _num(calibration.get("expected_return_bias_pct"), 0.0) or 0.0
    adjusted_score = _clip(adjusted_score, 0, 100)

    vol = _clip(volatility if volatility is not None and volatility > 0 else 1.25, 0.65, 3.2)
    direction_strength = (adjusted_score - 50) / 10.5
    flat_logit = 0.85 - abs(adjusted_score - 50) / 15.5 - max(0, vol - 1.4) * 0.18
    p_up, p_flat, p_down = _softmax([direction_strength, flat_logit, -direction_strength])
    probabilities = [p_up * 100, p_flat * 100, p_down * 100]

    if adjusted_score >= 64 and probabilities[0] >= 52:
        direction = "偏多上漲"
    elif adjusted_score >= 55:
        direction = "震盪偏多"
    elif adjusted_score <= 36 and probabilities[2] >= 52:
        direction = "偏空下跌"
    elif adjusted_score <= 45:
        direction = "震盪偏空"
    else:
        direction = "區間震盪"

    expected_return = _clip((adjusted_score - 50) * 0.035 + expected_bias, -1.8, 1.8)
    range_pct = _clip(vol * 1.12, 0.65, 2.8)
    expected_low = expected_high = None
    if close:
        expected_low = close * (1 + (expected_return - range_pct) / 100)
        expected_high = close * (1 + (expected_return + range_pct) / 100)

    coverage = available_weight / sum(v["weight"] for v in factors.values()) if factors else 0.0
    history_score = min(len(hist), 20) / 20 if not hist.empty else 0.0
    sorted_probs = sorted(probabilities, reverse=True)
    margin = sorted_probs[0] - sorted_probs[1]
    confidence_score = coverage * 55 + history_score * 15 + _clip(margin, 0, 35) * 0.75
    evaluated_count = int(_num(calibration.get("evaluated_count"), 0) or 0)
    if evaluated_count >= 8:
        hit_rate = _num(calibration.get("direction_hit_rate_pct"), 50) or 50
        confidence_score += _clip((hit_rate - 45) * 0.35, -5, 8)
    data_date = _text(snapshot.get("data_date") or snapshot.get("market_date"))
    age_days = None
    if data_date:
        dt = pd.to_datetime(data_date, errors="coerce")
        if pd.notna(dt):
            age_days = max(0, (now.date() - dt.date()).days)
            if age_days > 3:
                confidence_score -= min(25, (age_days - 3) * 6)
    # Without enough local TWSE history, do not label the forecast as high confidence.
    if len(hist) < 5:
        confidence_score = min(confidence_score, 58.0)
    elif len(hist) < 15:
        confidence_score = min(confidence_score, 68.0)
    confidence_score = _clip(confidence_score, 0, 100)
    if confidence_score >= 72:
        confidence = "高"
    elif confidence_score >= 54:
        confidence = "中"
    else:
        confidence = "低"

    # Phase 98: 極端崩跌後不能只看隔日單日反彈就解除封鎖。
    # 2026-07-28 大跌後，7/29 的短暫反彈若未達廣泛且有力的修復，
    # 隔日仍可能出現第二段下殺。以歷史收盤推算前一交易日報酬，
    # 並至少要求加權 +1.5%、櫃買 +1.0% 才視為初步修復。
    prev_tw_pct = None
    try:
        if not hist.empty and "收盤" in hist.columns:
            hist_dates = pd.to_datetime(hist.get("日期"), errors="coerce") if "日期" in hist.columns else None
            hist_ret = pd.to_numeric(hist["收盤"], errors="coerce").pct_change() * 100.0
            data_ts = pd.to_datetime(data_date, errors="coerce") if data_date else pd.NaT
            if hist_dates is not None and pd.notna(data_ts) and len(hist_ret) >= 2:
                same_day = bool(pd.notna(hist_dates.iloc[-1]) and hist_dates.iloc[-1].date() == data_ts.date())
                idx = -2 if same_day and len(hist_ret) >= 3 else -1
                val = hist_ret.iloc[idx]
            else:
                val = hist_ret.iloc[-1] if len(hist_ret) else None
            if val is not None and pd.notna(val):
                prev_tw_pct = float(val)
    except Exception:
        prev_tw_pct = None

    current_extreme = bool(tw_pct <= -3.5 or (otc_pct is not None and otc_pct <= -4.5))
    previous_crash = bool(prev_tw_pct is not None and prev_tw_pct <= -3.5)
    broad_recovery = bool(tw_pct >= 1.5 and (otc_pct is None or otc_pct >= 1.0))
    post_crash_cooldown = bool(previous_crash and not broad_recovery)
    extreme_lockdown = bool(current_extreme or post_crash_cooldown)
    if extreme_lockdown:
        effect_delta = -8.0
        weight_delta = -8
        position_cap = 0
        effect_mode = (
            "LOCKDOWN-C1｜崩跌後冷卻確認"
            if post_crash_cooldown and not current_extreme
            else "LOCKDOWN｜極端市場全面封鎖"
        )
    elif confidence == "低" or coverage < 0.52:
        effect_delta = 0.0
        weight_delta = 0
        position_cap = 20
        effect_mode = "資料保護"
    elif adjusted_score >= 65 and probabilities[0] >= 54:
        effect_delta = 3.0 if confidence == "高" else 2.0
        weight_delta = 3 if confidence == "高" else 2
        position_cap = 70 if confidence == "高" else 60
        effect_mode = "偏多加權"
    elif adjusted_score >= 56:
        effect_delta = 1.0
        weight_delta = 1
        position_cap = 55
        effect_mode = "小幅偏多"
    elif adjusted_score <= 35 and probabilities[2] >= 54:
        effect_delta = -4.0 if confidence == "高" else -3.0
        weight_delta = -4 if confidence == "高" else -3
        position_cap = 20
        effect_mode = "高風險防守"
    elif adjusted_score <= 44:
        effect_delta = -2.0
        weight_delta = -2
        position_cap = 35
        effect_mode = "偏空降權"
    else:
        effect_delta = 0.0
        weight_delta = 0
        position_cap = 45
        effect_mode = "中性選股"

    rel = None if otc_pct is None else otc_pct - tw_pct
    if adjusted_score <= 44:
        style = "大型權值、低波動、支撐明確與法人防守股"
        avoid = "高乖離、無量突破、短線已加速的小型題材股"
    elif sox is not None and sox >= 0.8:
        style = "半導體、AI供應鏈與量價同步的領漲股"
        avoid = "只有題材、沒有成交額與支撐的追價股"
    elif rel is not None and rel >= 0.45:
        style = "櫃買相對強勢的中小型輪動與剛起漲股"
        avoid = "連續急漲且離支撐過遠的中小型股"
    else:
        style = "低位階轉強、量增不失控、風報比完整的個股"
        avoid = "追高與停損距離過大的股票"

    contribution_rows = []
    for key, info in factors.items():
        if not info["available"]:
            continue
        impact = (float(info["score"]) - 50) * float(info["weight"])
        contribution_rows.append({"factor": key, "label": info["label"], "score": info["score"], "weight": info["weight"], "impact": round(impact, 2), "source": info["source"]})
    contribution_rows.sort(key=lambda x: abs(x["impact"]), reverse=True)
    positives = [f"{x['label']}偏多({x['score']:.1f})" for x in contribution_rows if x["impact"] > 0.8][:3]
    negatives = [f"{x['label']}偏弱({x['score']:.1f})" for x in contribution_rows if x["impact"] < -0.8][:3]
    rationale = "；".join(positives + negatives) or "各項因子接近中性，隔日以區間選股處理。"

    return {
        "version": FORECAST_VERSION,
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "data_date": data_date,
        "forecast_for_date": _next_weekday(now.date() if age_days is not None and age_days > 2 else (data_date or now.date())),
        "forecast_scope": "下一個台股交易時段（機率參考，非保證）",
        "direction": direction,
        "direction_score": round(adjusted_score, 1),
        "raw_direction_score": round(raw_score, 1),
        "confidence": confidence,
        "confidence_score": round(confidence_score, 1),
        "up_probability_pct": round(probabilities[0], 1),
        "flat_probability_pct": round(probabilities[1], 1),
        "down_probability_pct": round(probabilities[2], 1),
        "expected_return_pct": round(expected_return, 2),
        "expected_range_pct": round(range_pct, 2),
        "base_index": None if close is None else round(close, 2),
        "expected_low": None if expected_low is None else round(expected_low, 2),
        "expected_high": None if expected_high is None else round(expected_high, 2),
        "estimated_volatility_pct": round(vol, 2),
        "data_coverage_pct": round(coverage * 100, 1),
        "history_rows": len(hist),
        "data_age_days": age_days,
        "rationale": rationale,
        "positive_factors": positives,
        "risk_factors": negatives,
        "factor_details": factors,
        "factor_contributions": contribution_rows,
        "performance_calibration": calibration,
        "godpick_effect": {
            "mode": effect_mode,
            "score_delta": effect_delta,
            "market_weight_delta": weight_delta,
            "position_cap_pct": position_cap,
            "preferred_style": style,
            "avoid_style": avoid,
            "hard_filter": bool(extreme_lockdown),
            "previous_tw_return_pct": None if prev_tw_pct is None else round(prev_tw_pct, 2),
            "post_crash_cooldown": bool(post_crash_cooldown),
            "broad_recovery_confirmed": bool(broad_recovery),
            "lockdown_reason": (
                "前一交易日加權跌幅達-3.5%且本日未完成廣泛修復"
                if post_crash_cooldown and not current_extreme
                else "本日加權或櫃買跌幅達極端門檻" if current_extreme else ""
            ),
            "note": "極端崩跌與崩跌後冷卻期為硬封鎖；其餘狀態只做權重與風控校正。",
        },
        "disclaimer": "隔日預測為多因子機率模型，應搭配開盤缺口、成交量與個股觸發價確認，不構成獲利保證。",
    }


def flatten_forecast_for_bridge(forecast: dict[str, Any] | None) -> dict[str, Any]:
    f = forecast if isinstance(forecast, dict) else {}
    effect = f.get("godpick_effect") if isinstance(f.get("godpick_effect"), dict) else {}
    return {
        "next_day_forecast": f,
        "next_day_forecast_version": f.get("version"),
        "next_day_forecast_date": f.get("forecast_for_date"),
        "next_day_market_direction": f.get("direction"),
        "next_day_market_score": f.get("direction_score"),
        "next_day_confidence": f.get("confidence"),
        "next_day_confidence_score": f.get("confidence_score"),
        "next_day_up_probability_pct": f.get("up_probability_pct"),
        "next_day_flat_probability_pct": f.get("flat_probability_pct"),
        "next_day_down_probability_pct": f.get("down_probability_pct"),
        "next_day_expected_return_pct": f.get("expected_return_pct"),
        "next_day_expected_low": f.get("expected_low"),
        "next_day_expected_high": f.get("expected_high"),
        "next_day_data_coverage_pct": f.get("data_coverage_pct"),
        "next_day_forecast_rationale": f.get("rationale"),
        "next_day_godpick_score_delta": effect.get("score_delta", 0),
        "next_day_market_weight_delta": effect.get("market_weight_delta", 0),
        "next_day_position_cap_pct": effect.get("position_cap_pct"),
        "next_day_preferred_style": effect.get("preferred_style"),
        "next_day_avoid_style": effect.get("avoid_style"),
        "next_day_effect_mode": effect.get("mode"),
        "next_day_previous_tw_return_pct": effect.get("previous_tw_return_pct"),
        "next_day_post_crash_cooldown": effect.get("post_crash_cooldown", False),
        "next_day_broad_recovery_confirmed": effect.get("broad_recovery_confirmed", False),
        "next_day_lockdown_reason": effect.get("lockdown_reason"),
    }

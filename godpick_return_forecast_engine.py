# -*- coding: utf-8 -*-
"""V191-H32 probabilistic individual-stock return forecast engine.

This module deliberately does *not* promise 90% point-forecast accuracy.
It produces:
- next-session expected return and a nominal 90% prediction interval,
- 5/10/20-session expected return intervals,
- a 10-session 'swing' expected return summary,
- empirical validation metrics from matured historical truth rows,
- an explicit validation status that refuses to claim accuracy when samples are
  insufficient.

The forecast is an advisory overlay only.  It never promotes a stock into Formal
or A-, never overrides Entry/Risk/RR/liquidity/data-freshness gates, and never
turns a probability into a guarantee.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import math

import pandas as pd


VERSION = "v191_h38_probabilistic_return_forecast_truth_restore_20260817"
BASE_DIR = Path(__file__).resolve().parent
TRUTH_FILE = "godpick_t1_trade_truth.json"

FORECAST_COLUMNS = [
    "H32隔日預估漲跌幅%",
    "H32隔日90%區間下緣%",
    "H32隔日90%區間上緣%",
    "H32隔日方向",
    "H32隔日上漲機率%",
    "H32隔日預測可信度",
    "H32隔日校準樣本數",
    "H32隔日方向歷史命中率%",
    "H32隔日90%區間歷史覆蓋率%",
    "H32隔日平均絕對誤差%",
    "H32_5日預估報酬%",
    "H32_5日90%區間下緣%",
    "H32_5日90%區間上緣%",
    "H32_10日預估報酬%",
    "H32_10日90%區間下緣%",
    "H32_10日90%區間上緣%",
    "H32_20日預估報酬%",
    "H32_20日90%區間下緣%",
    "H32_20日90%區間上緣%",
    "H32後續波段預估漲幅%",
    "H32波段預測可信度",
    "H32預測驗證狀態",
    "H32預測方法",
    "H32預測版本",
]

_BLANK = {"", "none", "nan", "nat", "null", "--", "-", "<na>"}


def _s(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    t = str(v).strip()
    return "" if t.lower() in _BLANK else t


def _f(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return float(v)
        text = str(v).replace(",", "").replace("％", "%").strip()
        if text.endswith("%"):
            text = text[:-1].strip()
        if not text or text.lower() in _BLANK:
            return default
        x = float(text)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _first_num(row: pd.Series, names: list[str], default: float = 0.0, prefer_positive: bool = False) -> float:
    fallback: float | None = None
    for name in names:
        if name not in row.index:
            continue
        raw = row.get(name)
        x = _f(raw)
        if x is None:
            continue
        if fallback is None:
            fallback = x
        if not prefer_positive or x > 0:
            return float(x)
    return float(default if fallback is None else fallback)


def _first_text(row: pd.Series, names: list[str], default: str = "") -> str:
    for name in names:
        if name in row.index:
            value = _s(row.get(name))
            if value:
                return value
    return default


def _read_truth_rows(base_dir: Path | None = None) -> list[dict[str, Any]]:
    root = Path(base_dir or BASE_DIR)
    p = root / TRUTH_FILE
    try:
        data = json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        # H38: when a Streamlit redeploy has no local truth JSON, restore the
        # permanent T+1 authority instead of silently recalibrating on zero rows.
        try:
            from godpick_t1_trade_truth import load_t1_truth_rows
            return [dict(x) for x in load_t1_truth_rows() if isinstance(x, dict)]
        except Exception:
            return []
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = data.get("records") or data.get("rows") or []
    else:
        rows = []
    return [dict(x) for x in rows if isinstance(x, dict)]


def _quantile(vals: list[float], q: float) -> float | None:
    clean = sorted(float(x) for x in vals if x is not None and math.isfinite(float(x)))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    pos = _clamp(q, 0.0, 1.0) * (len(clean) - 1)
    lo = int(math.floor(pos)); hi = int(math.ceil(pos))
    if lo == hi:
        return clean[lo]
    w = pos - lo
    return clean[lo] * (1.0 - w) + clean[hi] * w


def _median(vals: list[float]) -> float | None:
    return _quantile(vals, 0.5)


def _truth_probability(row: dict[str, Any]) -> float | None:
    return _f(
        row.get("SuperAI校準後上漲機率%")
        or row.get("SuperAI原始上漲機率%")
        or row.get("H32隔日上漲機率%")
    )


def _truth_actual(row: dict[str, Any], horizon: int) -> float | None:
    if horizon == 1:
        # H32-v3: 0.00% is a valid realized return, never a missing value.
        # Using ``a or b`` silently discards zero and biases validation samples.
        first = _f(row.get("隔日候選漲跌%"))
        if first is not None:
            return first
        return _f(row.get("推薦後1日%"))
    for key in (
        f"推薦後{horizon}日%",
        f"可執行交易{horizon}日%",
        f"H32實際{horizon}日報酬%",
    ):
        x = _f(row.get(key))
        if x is not None:
            return x
    return None


def _empirical_sample(rows: list[dict[str, Any]], *, prob: float, horizon: int, role: str, category: str) -> list[float]:
    """Choose a bounded, progressively broadened historical peer sample."""
    candidates: list[tuple[float, str, str, float]] = []
    for r in rows:
        actual = _truth_actual(r, horizon)
        if actual is None:
            continue
        p = _truth_probability(r)
        rp = 50.0 if p is None else float(p)
        rr = _s(r.get("推薦角色") or r.get("正式推薦分區"))
        rc = _s(r.get("類別"))
        candidates.append((rp, rr, rc, float(actual)))
    if not candidates:
        return []

    role_category = [x[3] for x in candidates if role and x[1] == role and category and x[2] == category and abs(x[0] - prob) <= 12.5]
    if len(role_category) >= 12:
        return role_category[-240:]
    role_only = [x[3] for x in candidates if role and x[1] == role and abs(x[0] - prob) <= 15.0]
    if len(role_only) >= 15:
        return role_only[-300:]
    prob_only = [x[3] for x in candidates if abs(x[0] - prob) <= 10.0]
    if len(prob_only) >= 20:
        return prob_only[-400:]
    return [x[3] for x in candidates][-500:]


def _daily_vol_proxy(row: pd.Series) -> float:
    direct = _first_num(row, [
        "ATR%", "ATR14%", "20日平均振幅%", "平均真實波幅%", "日波動率%"
    ], 0.0, prefer_positive=True)
    if direct > 0:
        return _clamp(direct, 1.2, 7.0)
    stop = _first_num(row, ["停損距離_隔日%", "隔日有效風控距離%", "實戰停損距離%"], 0.0, prefer_positive=True)
    if stop > 0:
        return _clamp(stop / 2.5, 1.5, 6.0)
    ret5 = abs(_first_num(row, ["近5日漲幅%", "5日漲幅%"], 0.0))
    return _clamp(2.2 + min(2.0, ret5 * 0.08), 1.8, 4.5)


def _structural_t1(row: pd.Series, prob: float) -> float:
    entry = _first_num(row, ["Entry進場買點分", "Entry進場分", "進場時機分數", "買進分數"], 50.0)
    risk = _first_num(row, ["Risk風控安全分", "Risk風控分", "風控安全分"], 50.0)
    trade = _first_num(row, ["SuperAI Trade分", "SuperAI交易分", "交易可行分數"], 50.0)
    final_ai = _first_num(row, ["SuperAI 最終決策分", "SuperAI最終決策分", "V188股神作戰優先分"], 50.0)
    chase = _first_num(row, ["追價風險分", "追高風險分數_決策", "追價風險分數"], 55.0)
    ret5 = _clamp(_first_num(row, ["近5日漲幅%", "5日漲幅%"], 0.0), -12.0, 12.0)
    value = (
        (prob - 50.0) * 0.035
        + (entry - 50.0) * 0.006
        + (risk - 50.0) * 0.004
        + (trade - 50.0) * 0.010
        + (final_ai - 50.0) * 0.007
        - max(0.0, chase - 55.0) * 0.012
        + ret5 * 0.030
    )
    return _clamp(value, -5.0, 5.0)


def _forecast_horizon(row: pd.Series, truth_rows: list[dict[str, Any]], prob: float, horizon: int, t1_center: float) -> tuple[float, float, float, int, str]:
    role = _first_text(row, ["正式推薦分區", "推薦角色", "盤中雷達優先級"])
    category = _first_text(row, ["類別", "產業類別", "族群名稱"])
    sample = _empirical_sample(truth_rows, prob=prob, horizon=horizon, role=role, category=category)
    n = len(sample)
    if n >= 20:
        med = _median(sample)
        q05 = _quantile(sample, 0.05)
        q95 = _quantile(sample, 0.95)
        if med is not None and q05 is not None and q95 is not None:
            # Small structural tilt, bounded so history remains the dominant anchor.
            entry = _first_num(row, ["Entry進場買點分", "Entry進場分"], 50.0)
            risk = _first_num(row, ["Risk風控安全分", "Risk風控分"], 50.0)
            trade = _first_num(row, ["SuperAI Trade分", "交易可行分數"], 50.0)
            tilt = _clamp(((entry + risk + trade) / 3.0 - 50.0) * 0.015 * math.sqrt(max(1, horizon)), -2.5, 2.5)
            center = _clamp(float(med) + tilt, -25.0, 40.0)
            shift = center - float(med)
            return center, _clamp(float(q05) + shift, -60.0, 50.0), _clamp(float(q95) + shift, -40.0, 80.0), n, "歷史同儕分布校準"

    # Transparent fallback prior.  It remains explicitly unverified until truth
    # samples accumulate; the interval is intentionally wide.
    rr = _first_num(row, ["路徑風險報酬比", "實戰風險報酬比", "風險報酬比"], 1.0)
    ret5 = _clamp(_first_num(row, ["近5日漲幅%", "5日漲幅%"], 0.0), -15.0, 15.0)
    ret20 = _clamp(_first_num(row, ["近20日漲幅%", "20日漲幅%"], 0.0), -30.0, 30.0)
    structural = t1_center * math.sqrt(max(1.0, float(horizon))) * 0.85
    structural += ret5 * 0.05 + ret20 * 0.015 + _clamp(rr - 1.0, -1.0, 4.0) * 0.30
    center = _clamp(structural, -18.0, 32.0)
    vol = _daily_vol_proxy(row)
    half = 1.645 * vol * math.sqrt(max(1.0, float(horizon)))
    return center, _clamp(center - half, -60.0, 50.0), _clamp(center + half, -40.0, 80.0), n, "結構先驗估計｜待歷史校準"


def _validation_metrics(truth_rows: list[dict[str, Any]]) -> dict[str, Any]:
    direction_hits: list[float] = []
    interval_hits: list[float] = []
    abs_errors: list[float] = []
    horizon_cov: dict[int, list[float]] = {5: [], 10: [], 20: []}

    for r in truth_rows:
        actual = _truth_actual(r, 1)
        pred = _f(r.get("H32隔日預估漲跌幅%"))
        low = _f(r.get("H32隔日90%區間下緣%"))
        high = _f(r.get("H32隔日90%區間上緣%"))
        if actual is not None and pred is not None:
            abs_errors.append(abs(actual - pred))
            direction_hits.append(1.0 if (actual > 0) == (pred > 0) else 1.0 if actual == 0 and abs(pred) < 0.15 else 0.0)
        if actual is not None and low is not None and high is not None:
            interval_hits.append(1.0 if min(low, high) <= actual <= max(low, high) else 0.0)
        for h in (5, 10, 20):
            a = _truth_actual(r, h)
            lo = _f(r.get(f"H32_{h}日90%區間下緣%"))
            hi = _f(r.get(f"H32_{h}日90%區間上緣%"))
            if a is not None and lo is not None and hi is not None:
                horizon_cov[h].append(1.0 if min(lo, hi) <= a <= max(lo, hi) else 0.0)

    n = len(abs_errors)
    direction_rate = sum(direction_hits) / len(direction_hits) * 100.0 if direction_hits else None
    coverage = sum(interval_hits) / len(interval_hits) * 100.0 if interval_hits else None
    mae = sum(abs_errors) / n if n else None
    return {
        "samples": n,
        "direction_samples": len(direction_hits),
        "direction_hit_rate_pct": round(direction_rate, 2) if direction_rate is not None else None,
        "interval_samples": len(interval_hits),
        "interval_coverage_pct": round(coverage, 2) if coverage is not None else None,
        "mae_pct": round(mae, 3) if mae is not None else None,
        "horizon_coverage_pct": {
            h: (round(sum(vals) / len(vals) * 100.0, 2) if vals else None)
            for h, vals in horizon_cov.items()
        },
        "horizon_samples": {h: len(vals) for h, vals in horizon_cov.items()},
    }


def _validation_status(metrics: dict[str, Any]) -> str:
    n = int(metrics.get("interval_samples") or 0)
    coverage = metrics.get("interval_coverage_pct")
    direction_n = int(metrics.get("direction_samples") or 0)
    direction = metrics.get("direction_hit_rate_pct")
    if n < 30:
        return f"未驗證｜90%區間成熟樣本 {n}<30；禁止宣稱90%準確"
    if coverage is None:
        return "未驗證｜尚無可計算的區間覆蓋率"
    if coverage >= 90.0:
        suffix = ""
        if direction_n >= 100 and direction is not None:
            suffix = f"；方向命中 {direction:.1f}%"
        return f"90%區間覆蓋達標 {coverage:.1f}%｜這是區間覆蓋，不等於點預測90%準確{suffix}"
    return f"未達90%區間覆蓋｜目前 {coverage:.1f}% / n={n}；持續校準"


def _confidence_label(sample_n: int, validation: dict[str, Any], method: str) -> str:
    if sample_n >= 60 and int(validation.get("interval_samples") or 0) >= 60:
        return "高｜有歷史同儕＋走勢外驗證"
    if sample_n >= 20:
        return "中｜有歷史同儕，整體驗證仍累積"
    if "先驗" in method:
        return "低｜先驗估計，尚未完成歷史校準"
    return "中低｜樣本有限"


def apply_return_forecast(df: pd.DataFrame | None, *, truth_rows: list[dict[str, Any]] | None = None, base_dir: str | Path | None = None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=FORECAST_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        for c in FORECAST_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="object")
        return out

    truths = list(truth_rows) if truth_rows is not None else _read_truth_rows(Path(base_dir) if base_dir else None)
    validation = _validation_metrics(truths)
    status = _validation_status(validation)

    rows: list[dict[str, Any]] = []
    for _, row in out.iterrows():
        prob = _clamp(_first_num(row, [
            "SuperAI校準後隔日上漲機率%", "SuperAI隔日上漲機率%", "模型隔日上漲機率%", "隔日上漲機率%"
        ], 50.0), 1.0, 99.0)
        structural_t1 = _structural_t1(row, prob)
        role = _first_text(row, ["正式推薦分區", "推薦角色", "盤中雷達優先級"])
        category = _first_text(row, ["類別", "產業類別", "族群名稱"])
        t1_sample = _empirical_sample(truths, prob=prob, horizon=1, role=role, category=category)
        if len(t1_sample) >= 20:
            med = _median(t1_sample) or 0.0
            q05 = _quantile(t1_sample, 0.05)
            q95 = _quantile(t1_sample, 0.95)
            blend = 0.75 if len(t1_sample) >= 60 else 0.60
            t1_center = _clamp(float(med) * blend + structural_t1 * (1.0 - blend), -8.0, 8.0)
            if q05 is not None and q95 is not None:
                shift = t1_center - float(med)
                t1_low = _clamp(float(q05) + shift, -10.0, 10.0)
                t1_high = _clamp(float(q95) + shift, -10.0, 10.0)
            else:
                vol = _daily_vol_proxy(row)
                t1_low = _clamp(t1_center - 1.645 * vol, -10.0, 10.0)
                t1_high = _clamp(t1_center + 1.645 * vol, -10.0, 10.0)
            t1_method = "歷史同儕分布＋當前結構混合"
        else:
            t1_center = structural_t1
            vol = _daily_vol_proxy(row)
            t1_low = _clamp(t1_center - 1.645 * vol, -10.0, 10.0)
            t1_high = _clamp(t1_center + 1.645 * vol, -10.0, 10.0)
            t1_method = "結構先驗估計｜待T+1真相校準"

        forecasts: dict[int, tuple[float, float, float, int, str]] = {}
        for h in (5, 10, 20):
            forecasts[h] = _forecast_horizon(row, truths, prob, h, t1_center)
        swing_center = forecasts[10][0]
        t1_conf = _confidence_label(len(t1_sample), validation, t1_method)
        wave_sample_n = forecasts[10][3]
        wave_conf = _confidence_label(wave_sample_n, validation, forecasts[10][4])

        rows.append({
            "H32隔日預估漲跌幅%": round(t1_center, 2),
            "H32隔日90%區間下緣%": round(min(t1_low, t1_high), 2),
            "H32隔日90%區間上緣%": round(max(t1_low, t1_high), 2),
            "H32隔日方向": "上漲" if t1_center > 0.15 else "下跌" if t1_center < -0.15 else "震盪",
            "H32隔日上漲機率%": round(prob, 1),
            "H32隔日預測可信度": t1_conf,
            "H32隔日校準樣本數": len(t1_sample),
            "H32隔日方向歷史命中率%": validation.get("direction_hit_rate_pct"),
            "H32隔日90%區間歷史覆蓋率%": validation.get("interval_coverage_pct"),
            "H32隔日平均絕對誤差%": validation.get("mae_pct"),
            "H32_5日預估報酬%": round(forecasts[5][0], 2),
            "H32_5日90%區間下緣%": round(forecasts[5][1], 2),
            "H32_5日90%區間上緣%": round(forecasts[5][2], 2),
            "H32_10日預估報酬%": round(forecasts[10][0], 2),
            "H32_10日90%區間下緣%": round(forecasts[10][1], 2),
            "H32_10日90%區間上緣%": round(forecasts[10][2], 2),
            "H32_20日預估報酬%": round(forecasts[20][0], 2),
            "H32_20日90%區間下緣%": round(forecasts[20][1], 2),
            "H32_20日90%區間上緣%": round(forecasts[20][2], 2),
            "H32後續波段預估漲幅%": round(swing_center, 2),
            "H32波段預測可信度": wave_conf,
            "H32預測驗證狀態": status,
            "H32預測方法": f"隔日={t1_method}；5/10/20日={forecasts[10][4]}",
            "H32預測版本": VERSION,
        })

    forecast_df = pd.DataFrame(rows, index=out.index)
    for c in FORECAST_COLUMNS:
        out[c] = forecast_df[c]
    return out


def forecast_validation_summary(*, truth_rows: list[dict[str, Any]] | None = None, base_dir: str | Path | None = None) -> dict[str, Any]:
    truths = list(truth_rows) if truth_rows is not None else _read_truth_rows(Path(base_dir) if base_dir else None)
    metrics = _validation_metrics(truths)
    metrics["status"] = _validation_status(metrics)
    metrics["version"] = VERSION
    metrics["target"] = "90% prediction-interval coverage; direction hit rate and point MAE are tracked separately"
    return metrics


__all__ = ["VERSION", "FORECAST_COLUMNS", "apply_return_forecast", "forecast_validation_summary"]

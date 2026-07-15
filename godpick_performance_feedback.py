# -*- coding: utf-8 -*-
"""股神推薦績效回饋校正服務 vNext 2026-05-30.

設計原則：
- 只讀取既有 godpick_records.json，不覆蓋正式紀錄。
- 不刪除舊推薦欄位；只新增績效回饋欄位，讓 7/8/10/12 可共用。
- 讓歷史績效反饋到：選股潛力、進場買點、風控安全、績效校正、買點分級。
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import math

import pandas as pd

PERFORMANCE_FEEDBACK_VERSION = "phase8_9_layered_calibration_feedback_20260715"
DEFAULT_RECORD_PATH = "godpick_records.json"
DEFAULT_CALIBRATION_PATH = "godpick_calibration_samples.json"

FEEDBACK_COLUMNS = [
    "股神實戰總分",
    "Alpha選股潛力分",
    "Entry進場買點分",
    "Risk風控安全分",
    "Feedback績效校正分",
    "選股潛力分",
    "進場買點分",
    "風控安全分",
    "績效校正分",
    "績效校正說明",
    "新買點分級",
    "推薦角色",
    "過熱原因",
    "硬否決原因",
    "真禁買原因",
    "等待突破原因",
    "突破確認狀態",
    "突破確認條件",
    "假陰性檢討",
    "今日決策結論",
    "候選強度分",
    "實戰過濾狀態",
    "主推薦降級原因",
    "冷卻提示",
    "建議動作",
    "建議倉位",
    "建議倉位%",
    "小量試單建議",
    "加碼條件",
    "失效條件",
    "失效條件_績效回饋",
    "績效回饋建議",
    "績效樣本數",
    "績效回饋版本",
    "飆股雷達績效分",
    "領漲回補績效分",
    "主流族群回饋分",
    "漏選修正提醒",
    "Phase6_1回饋說明",
    "漲停回放分",
    "強勢股漏選風險分",
    "候選池覆蓋診斷",
    "漲停漏選原因",
    "漏選原因分類",
    "漏選修正動作",
    "回放校正角色",
    "回放校正分區",
    "回放校正版本",
    "Phase6_2回放說明",
    "決策版本",
]


@dataclass(frozen=True)
class SegmentStat:
    sample: int = 0
    avg_return: float = 0.0
    median_return: float = 0.0
    win_rate: float = 0.0
    target1_rate: float = 0.0
    target2_rate: float = 0.0
    stop_rate: float = 0.0
    boost: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "sample": self.sample,
            "avg_return": round(self.avg_return, 4),
            "median_return": round(self.median_return, 4),
            "win_rate": round(self.win_rate, 4),
            "target1_rate": round(self.target1_rate, 4),
            "target2_rate": round(self.target2_rate, 4),
            "stop_rate": round(self.stop_rate, 4),
            "boost": round(self.boost, 4),
        }


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _safe_float(v: Any, default: float | None = None) -> float | None:
    if v is None:
        return default
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    if isinstance(v, str):
        s = v.strip().replace("%", "").replace(",", "")
        if s.lower() in {"", "none", "nan", "null", "--", "-", "<na>"}:
            return default
        v = s
    try:
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def _score_clip(v: Any, low: float = 0.0, high: float = 100.0) -> float:
    x = _safe_float(v, low)
    if x is None:
        x = low
    return round(max(low, min(high, float(x))), 2)


def _boolish(v: Any) -> bool:
    s = _safe_str(v).lower()
    return s in {"true", "1", "yes", "y", "是", "已買進", "已達", "達標"}


def _load_records_payload(path: str | Path = DEFAULT_RECORD_PATH) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ["records", "items", "data", "rows"]:
            rows = payload.get(key)
            if isinstance(rows, list):
                return [x for x in rows if isinstance(x, dict)]
    return []


def _to_numeric_series(df: pd.DataFrame, col: str, default: float | None = None) -> pd.Series:
    if col not in df.columns:
        return pd.Series([default] * len(df), index=df.index, dtype="float64")
    s = df[col].map(lambda x: _safe_float(x, default))
    return pd.to_numeric(s, errors="coerce")


def _parse_record_date_series(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    out = pd.Series([pd.NaT] * len(df), index=df.index, dtype="datetime64[ns]")
    for col in cols:
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce")
        mask = out.isna() & parsed.notna()
        out.loc[mask] = parsed.loc[mask]
    return out


def _business_age_series(df: pd.DataFrame) -> pd.Series:
    rec = _parse_record_date_series(df, ["推薦日期", "推薦日", "建立時間", "推薦建立時間"])
    updated = _parse_record_date_series(df, ["績效更新時間", "追蹤更新時間", "更新時間", "最後更新時間"])
    updated = updated.fillna(pd.Timestamp.today().normalize())
    ages: list[float] = []
    for r, u in zip(rec, updated):
        if pd.isna(r) or pd.isna(u) or u < r:
            ages.append(float("nan"))
            continue
        try:
            ages.append(float(max(len(pd.bdate_range(r.normalize() + pd.Timedelta(days=1), u.normalize())), 0)))
        except Exception:
            ages.append(float("nan"))
    return pd.Series(ages, index=df.index, dtype="float64")


def _suspicious_horizon_mask(df: pd.DataFrame) -> pd.Series:
    """辨識把『目前損益』回填到所有固定週期的污染紀錄。"""
    horizon_cols = [c for c in ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%"] if c in df.columns]
    if len(horizon_cols) < 3:
        return pd.Series([False] * len(df), index=df.index)
    vals = pd.DataFrame({c: _to_numeric_series(df, c) for c in horizon_cols}, index=df.index)
    counts = vals.notna().sum(axis=1)
    spreads = vals.max(axis=1, skipna=True) - vals.min(axis=1, skipna=True)
    source = df.get("績效資料來源", pd.Series([""] * len(df), index=df.index)).map(_safe_str).str.lower()
    proxy_source = source.str.contains("proxy|代理|即時|目前|最新價|current", regex=True, na=False)
    return ((counts >= 3) & (spreads.abs() < 1e-9)) | proxy_source


def _tracking_return_series(df: pd.DataFrame) -> pd.Series:
    """建立可信任的績效回饋報酬。

    固定週期績效只有在紀錄已滿該交易日數、數值落在合理區間，且不是把
    目前損益代理回填到 1/3/5/10/20 日欄位時才可餵給模型。未滿期或可疑
    紀錄只留在追蹤畫面，不參與權重學習，避免錯誤績效把推薦邏輯帶偏。
    """
    base = pd.Series([pd.NA] * len(df), index=df.index, dtype="object")
    age = _business_age_series(df)
    suspicious = _suspicious_horizon_mask(df)

    horizon_specs = [
        (20, ["推薦後20日%", "20日績效%"], -70.0, 100.0),
        (10, ["推薦後10日%", "10日績效%"], -55.0, 80.0),
        (5, ["推薦後5日%", "5日績效%"], -45.0, 65.0),
        (3, ["推薦後3日%", "3日績效%"], -35.0, 50.0),
        (1, ["推薦後1日%", "1日績效%"], -22.0, 25.0),
    ]
    for horizon, cols, low, high in horizon_specs:
        for col in cols:
            if col not in df.columns:
                continue
            values = _to_numeric_series(df, col)
            valid = (
                base.isna()
                & values.notna()
                & age.ge(float(horizon))
                & values.between(low, high, inclusive="both")
                & ~suspicious
            )
            base.loc[valid] = values.loc[valid]

    # 真正有實際買進紀錄者，實際報酬可覆蓋固定週期；未買進的浮動損益不得餵模型。
    actual = _to_numeric_series(df, "實際報酬%")
    if "是否已實際買進" in df.columns:
        actual_mask = df["是否已實際買進"].map(_boolish) & actual.notna()
    elif "是否已買進" in df.columns:
        actual_mask = df["是否已買進"].map(_boolish) & actual.notna()
    else:
        actual_mask = pd.Series([False] * len(df), index=df.index)
    actual_mask &= actual.between(-80.0, 200.0, inclusive="both")
    base.loc[actual_mask] = actual.loc[actual_mask]
    return pd.to_numeric(base, errors="coerce")

def _load_feedback_records() -> list[dict[str, Any]]:
    """合併正式推薦紀錄與獨立校正研究樣本。

    同日同股若正式紀錄與研究樣本重複，優先保留正式紀錄；研究樣本只補充
    近門檻與市場漏選資料，不污染正式推薦績效。
    """
    formal = _load_records_payload(DEFAULT_RECORD_PATH)
    research = _load_records_payload(DEFAULT_CALIBRATION_PATH)
    merged: dict[str, dict[str, Any]] = {}
    for row in research + formal:
        rec_date = _safe_str(row.get("推薦日期"))[:10]
        code = _safe_str(row.get("股票代號"))
        sample_type = _safe_str(row.get("校正樣本類型")) or _safe_str(row.get("紀錄層級"))
        is_formal_source = _safe_str(row.get("推薦模式")) != "股神校正研究"
        key = f"{rec_date}|{code}" if is_formal_source else f"{rec_date}|{code}|{sample_type}"
        merged[key] = dict(row)
    return list(merged.values())


def _infer_feedback_sample_type(row: pd.Series | dict[str, Any]) -> str:
    explicit = _safe_str(row.get("校正樣本類型"))
    if explicit:
        return explicit
    level = _safe_str(row.get("紀錄層級"))
    bucket = _safe_str(row.get("正式推薦分區"))
    radar = _safe_str(row.get("盤中雷達優先級"))
    if "正式主推薦" in level or bucket == "正式下週主推薦":
        return "A｜正式交易樣本"
    if "A-" in level or bucket == "A-｜準主推薦小量試單":
        return "A-｜準主推薦樣本"
    if "R1-M" in level or radar.startswith("R1-M"):
        return "B｜R1-M強勢動能雷達"
    if "R1" in level or radar.startswith("R1"):
        return "B｜R1核心雷達"
    return level or "舊版推薦樣本"


def _infer_feedback_weight(row: pd.Series | dict[str, Any]) -> float:
    explicit = _safe_float(row.get("校正樣本權重"), None)
    if explicit is not None:
        return max(0.0, min(1.0, float(explicit)))
    sample_type = _infer_feedback_sample_type(row)
    if sample_type.startswith("D"):
        return 0.0
    if sample_type.startswith("C"):
        return 0.45
    if sample_type.startswith("A-"):
        return 0.90
    if sample_type.startswith("A"):
        return 1.00
    if "R1" in sample_type or sample_type.startswith("B"):
        return 0.75
    return 0.70


def _feedback_weight_eligible(row: pd.Series | dict[str, Any]) -> bool:
    explicit = _safe_str(row.get("是否納入權重校正"))
    if explicit:
        return _boolish(explicit)
    return _infer_feedback_weight(row) > 0


def _score_bucket(score: Any) -> str:
    x = _safe_float(score, 0) or 0
    if x >= 95:
        return ">=95"
    if x >= 90:
        return "90-95"
    if x >= 85:
        return "85-90"
    if x >= 80:
        return "80-85"
    if x >= 75:
        return "75-80"
    return "<75"


def _numeric_score_bucket(v: Any, *, prefix: str = "") -> str:
    x = _safe_float(v, None)
    if x is None:
        return f"{prefix}未分類" if prefix else "未分類"
    if x >= 90:
        b = "90+"
    elif x >= 80:
        b = "80-89"
    elif x >= 70:
        b = "70-79"
    elif x >= 60:
        b = "60-69"
    elif x >= 50:
        b = "50-59"
    else:
        b = "<50"
    return f"{prefix}{b}" if prefix else b


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).clip(lower=0.0)
    mask = v.notna() & w.gt(0)
    if not mask.any():
        return 0.0
    return float((v.loc[mask] * w.loc[mask]).sum() / w.loc[mask].sum())


def _weighted_median(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).clip(lower=0.0)
    mask = v.notna() & w.gt(0)
    if not mask.any():
        return 0.0
    order = v.loc[mask].sort_values().index
    vv = v.loc[order]
    ww = w.loc[order]
    cutoff = ww.sum() * 0.5
    return float(vv.loc[ww.cumsum().ge(cutoff)].iloc[0])


def _truth_rate(df: pd.DataFrame, col: str, weights: pd.Series | None = None) -> float:
    if col not in df.columns or df.empty:
        return 0.0
    vals = df[col].map(_boolish).astype(float)
    if weights is None:
        return float(vals.mean())
    return _weighted_mean(vals, weights.reindex(df.index).fillna(0.0))


def _segment_stats(df: pd.DataFrame, col: str, baseline: dict[str, float], *, min_sample: int = 3) -> dict[str, dict[str, Any]]:
    if col not in df.columns or df.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    tmp = df.copy()
    tmp[col] = tmp[col].map(_safe_str).replace("", "未分類")
    if "_feedback_weight" not in tmp.columns:
        tmp["_feedback_weight"] = 1.0
    base_avg = float(baseline.get("avg_return", 0.0) or 0.0)
    base_med = float(baseline.get("median_return", 0.0) or 0.0)
    base_win = float(baseline.get("win_rate", 0.5) or 0.5)
    for key, g in tmp.groupby(col, dropna=False):
        ret = pd.to_numeric(g.get("_feedback_return_pct"), errors="coerce")
        weights = pd.to_numeric(g.get("_feedback_weight"), errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)
        mask = ret.notna() & weights.gt(0)
        ret = ret.loc[mask]
        weights = weights.loc[mask]
        sample = int(len(ret))
        effective_sample = float(weights.sum())
        if sample <= 0 or effective_sample <= 0:
            continue
        ret_clip = ret.clip(lower=-25, upper=35)
        avg = _weighted_mean(ret, weights)
        med = _weighted_median(ret, weights)
        avg_clip = _weighted_mean(ret_clip, weights)
        med_clip = _weighted_median(ret_clip, weights)
        win = _weighted_mean((ret > 0).astype(float), weights)
        hit5 = _weighted_mean((ret >= 5).astype(float), weights)
        loss5 = _weighted_mean((ret <= -5).astype(float), weights)
        t1 = _truth_rate(g.loc[mask], "是否達目標1", weights)
        t2 = _truth_rate(g.loc[mask], "是否達目標2", weights)
        stop = _truth_rate(g.loc[mask], "是否達停損", weights)
        if sample < min_sample or effective_sample < max(2.0, min_sample * 0.45):
            boost = 0.0
        else:
            raw = ((avg_clip - base_avg) / 4.0)
            raw += ((med_clip - base_med) / 3.0)
            raw += (win - base_win) * 7.0
            raw += hit5 * 2.0
            raw -= loss5 * 3.0
            raw += (t1 - baseline.get("target1_rate", 0.0)) * 3.0
            raw += (t2 - baseline.get("target2_rate", 0.0)) * 1.5
            raw -= max(0.0, stop - baseline.get("stop_rate", 0.0)) * 7.0
            shrink = min(1.0, effective_sample / 30.0)
            boost = max(-8.0, min(8.0, raw * shrink))
        stat = SegmentStat(sample, avg, med, win, t1, t2, stop, boost)
        row = stat.as_dict()
        row.update({
            "effective_sample": round(effective_sample, 2),
            "avg_clip": round(avg_clip, 4),
            "median_clip": round(med_clip, 4),
            "hit5_rate": round(hit5, 4),
            "loss5_rate": round(loss5, 4),
        })
        out[_safe_str(key)] = row
    return out


def build_godpick_performance_profile(records: list[dict[str, Any]] | pd.DataFrame | None = None) -> dict[str, Any]:
    if records is None:
        records = _load_feedback_records()
    df = records.copy() if isinstance(records, pd.DataFrame) else pd.DataFrame(records or [])
    if df.empty:
        return _empty_profile("沒有可用股神推薦紀錄")
    df = df.loc[:, ~df.columns.duplicated()].copy()
    df["_feedback_return_pct"] = _tracking_return_series(df)
    df["_feedback_suspicious"] = _suspicious_horizon_mask(df)
    df["_feedback_business_age"] = _business_age_series(df)
    df["_feedback_sample_type"] = df.apply(_infer_feedback_sample_type, axis=1)
    df["_feedback_weight"] = df.apply(_infer_feedback_weight, axis=1)
    df["_feedback_weight_eligible"] = df.apply(_feedback_weight_eligible, axis=1)
    df["_score_bucket"] = df.get("推薦總分", pd.Series([0] * len(df), index=df.index)).map(_score_bucket)
    for _c in ["股神決策分數", "起漲前兆分數", "追價風險分", "Entry進場買點分", "Risk風控安全分", "可操作分"]:
        if _c in df.columns:
            df[f"_{_c}_bucket"] = df[_c].map(lambda v, _p=_c: _numeric_score_bucket(v, prefix=f"{_p}："))
    valid = df[
        df["_feedback_return_pct"].notna()
        & df["_feedback_weight_eligible"].fillna(False)
        & pd.to_numeric(df["_feedback_weight"], errors="coerce").fillna(0.0).gt(0)
    ].copy()
    if valid.empty:
        return _empty_profile("股神推薦紀錄缺少可計算報酬欄位")

    ret = pd.to_numeric(valid["_feedback_return_pct"], errors="coerce")
    weights = pd.to_numeric(valid["_feedback_weight"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)
    ret_clip = ret.clip(lower=-25, upper=35)
    baseline = {
        "sample": int(ret.notna().sum()),
        "effective_sample": float(weights.loc[ret.notna()].sum()),
        "avg_return": _weighted_mean(ret_clip, weights),
        "raw_avg_return": _weighted_mean(ret, weights),
        "median_return": _weighted_median(ret_clip, weights),
        "raw_median_return": _weighted_median(ret, weights),
        "win_rate": _weighted_mean((ret > 0).astype(float), weights),
        "hit5_rate": _weighted_mean((ret >= 5).astype(float), weights),
        "loss5_rate": _weighted_mean((ret <= -5).astype(float), weights),
        "target1_rate": _truth_rate(valid, "是否達目標1", weights),
        "target2_rate": _truth_rate(valid, "是否達目標2", weights),
        "stop_rate": _truth_rate(valid, "是否達停損", weights),
    }
    profile = {
        "version": PERFORMANCE_FEEDBACK_VERSION,
        "available": True,
        "message": "ok",
        "baseline": {k: round(v, 4) if isinstance(v, float) else v for k, v in baseline.items()},
        "data_quality": {
            "total_records": int(len(df)),
            "trusted_records": int(len(valid)),
            "effective_weighted_samples": round(float(pd.to_numeric(valid["_feedback_weight"], errors="coerce").fillna(0.0).sum()), 2),
            "formal_or_radar_records": int((~df["_feedback_sample_type"].astype(str).str.startswith(("C", "D"))).sum()),
            "near_threshold_samples": int(df["_feedback_sample_type"].astype(str).str.startswith("C").sum()),
            "missed_strong_samples": int(df["_feedback_sample_type"].astype(str).str.startswith("D").sum()),
            "weight_eligible_records": int(df["_feedback_weight_eligible"].fillna(False).sum()),
            "suspicious_proxy_records": int(df["_feedback_suspicious"].fillna(False).sum()),
            "trusted_ratio_pct": round(float(len(valid) / max(len(df), 1) * 100.0), 2),
            "sample_type_counts": {str(k): int(v) for k, v in df["_feedback_sample_type"].value_counts(dropna=False).to_dict().items()},
        },
        "by_recommend_type": _segment_stats(valid, "推薦型態", baseline, min_sample=5),
        "by_category": _segment_stats(valid, "類別", baseline, min_sample=8),
        "by_score_bucket": _segment_stats(valid, "_score_bucket", baseline, min_sample=8),
        "by_buy_grade": _segment_stats(valid, "買點分級", baseline, min_sample=5),
        # V158：增加真正影響可操作性的分群，避免只靠類別/總分校正。
        "by_recommend_layer": _segment_stats(valid, "推薦分層", baseline, min_sample=8),
        "by_recommend_bucket": _segment_stats(valid, "推薦分桶", baseline, min_sample=8),
        "by_nextday_action": _segment_stats(valid, "隔日建議動作", baseline, min_sample=8),
        "by_nextday_entry_type": _segment_stats(valid, "進場型態_隔日", baseline, min_sample=8),
        "by_market_guard": _segment_stats(valid, "大盤橋接風控", baseline, min_sample=8),
        "by_market_bucket": _segment_stats(valid, "大盤情境分桶", baseline, min_sample=8),
    }
    for _c in ["股神決策分數", "起漲前兆分數", "追價風險分", "Entry進場買點分", "Risk風控安全分", "可操作分"]:
        _b = f"_{_c}_bucket"
        if _b in valid.columns:
            profile[f"by_{_c}_bucket"] = _segment_stats(valid, _b, baseline, min_sample=8)
    profile["by_calibration_sample_type"] = _segment_stats(valid, "_feedback_sample_type", baseline, min_sample=5)
    missed = df[df["_feedback_sample_type"].astype(str).str.startswith("D") & df["_feedback_return_pct"].notna()].copy()
    if not missed.empty:
        missed_ret = pd.to_numeric(missed["_feedback_return_pct"], errors="coerce").dropna()
        profile["missed_strong_diagnostics"] = {
            "sample": int(len(missed_ret)),
            "avg_return": round(float(missed_ret.mean()), 4) if not missed_ret.empty else 0.0,
            "median_return": round(float(missed_ret.median()), 4) if not missed_ret.empty else 0.0,
            "positive_rate": round(float((missed_ret > 0).mean()), 4) if not missed_ret.empty else 0.0,
            "note": "市場漏選強勢樣本只用於召回率與誤殺診斷，不直接調整獲利權重。",
        }
    else:
        profile["missed_strong_diagnostics"] = {"sample": 0, "avg_return": 0.0, "median_return": 0.0, "positive_rate": 0.0, "note": "尚無成熟市場漏選樣本"}
    profile["top_categories"] = _top_keys(profile["by_category"], positive=True)
    profile["weak_categories"] = _top_keys(profile["by_category"], positive=False)
    profile["top_recommend_types"] = _top_keys(profile["by_recommend_type"], positive=True)
    profile["weak_recommend_types"] = _top_keys(profile["by_recommend_type"], positive=False)
    return profile


def _empty_profile(message: str) -> dict[str, Any]:
    return {
        "version": PERFORMANCE_FEEDBACK_VERSION,
        "available": False,
        "message": message,
        "baseline": {"sample": 0, "avg_return": 0.0, "median_return": 0.0, "win_rate": 0.0, "target1_rate": 0.0, "target2_rate": 0.0, "stop_rate": 0.0},
        "by_recommend_type": {},
        "by_category": {},
        "by_score_bucket": {},
        "by_buy_grade": {},
        "top_categories": [],
        "weak_categories": [],
        "top_recommend_types": [],
        "weak_recommend_types": [],
    }


def _top_keys(stats: dict[str, dict[str, Any]], *, positive: bool) -> list[str]:
    items = []
    for k, v in stats.items():
        boost = _safe_float(v.get("boost"), 0) or 0
        sample = int(_safe_float(v.get("sample"), 0) or 0)
        if sample <= 0:
            continue
        if positive and boost > 0:
            items.append((boost, sample, k))
        elif not positive and boost < 0:
            items.append((boost, sample, k))
    items.sort(key=lambda x: (x[0], x[1]), reverse=positive)
    if not positive:
        items.sort(key=lambda x: (x[0], -x[1]))
    return [k for _, _, k in items[:8]]


def load_godpick_performance_profile(path: str | Path = DEFAULT_RECORD_PATH) -> dict[str, Any]:
    if str(path) == DEFAULT_RECORD_PATH:
        return build_godpick_performance_profile(_load_feedback_records())
    return build_godpick_performance_profile(_load_records_payload(path))


def _lookup_boost(profile: dict[str, Any], section: str, key: Any) -> tuple[float, int]:
    stats = profile.get(section) or {}
    row = stats.get(_safe_str(key)) or {}
    return float(_safe_float(row.get("boost"), 0) or 0), int(_safe_float(row.get("sample"), 0) or 0)


def _num(out: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    return _to_numeric_series(out, col, default).fillna(default)


def _text(out: pd.DataFrame, col: str) -> pd.Series:
    if col not in out.columns:
        return pd.Series([""] * len(out), index=out.index, dtype="object")
    return out[col].map(_safe_str)


def _sync_phase1_feedback_columns(out: pd.DataFrame) -> pd.DataFrame:
    """補齊 Phase 1 決策欄位別名；不刪欄、不寫檔。"""
    if out is None:
        return pd.DataFrame()
    if out.empty:
        for c in FEEDBACK_COLUMNS:
            if c not in out.columns:
                out[c] = ""
        return out

    def _ensure_numeric_alias(target: str, source: str, default: float = 0.0) -> None:
        if target not in out.columns:
            out[target] = pd.to_numeric(out[source], errors="coerce").fillna(default) if source in out.columns else default
        elif source in out.columns:
            blank = out[target].map(lambda v: _safe_str(v) == "")
            if blank.any():
                out.loc[blank, target] = pd.to_numeric(out.loc[blank, source], errors="coerce").fillna(default)

    _ensure_numeric_alias("Alpha選股潛力分", "選股潛力分", 0.0)
    _ensure_numeric_alias("Entry進場買點分", "進場買點分", 0.0)
    _ensure_numeric_alias("Risk風控安全分", "風控安全分", 0.0)

    corr_src = out["績效校正分"] if "績效校正分" in out.columns else pd.Series([0] * len(out), index=out.index)
    corr = pd.to_numeric(corr_src, errors="coerce").fillna(0).clip(-15, 15)
    feedback_score = (50 + corr * 3).clip(0, 100).round(1)
    if "Feedback績效校正分" not in out.columns:
        out["Feedback績效校正分"] = feedback_score
    else:
        feedback_now = pd.to_numeric(out["Feedback績效校正分"], errors="coerce")
        blank = out["Feedback績效校正分"].map(lambda v: _safe_str(v) == "") | feedback_now.fillna(0).eq(0)
        if blank.any():
            out.loc[blank, "Feedback績效校正分"] = feedback_score.loc[blank]

    if "建議動作" not in out.columns:
        out["建議動作"] = out.get("績效回饋建議", "")
    else:
        src = out.get("績效回饋建議", "")
        if isinstance(src, pd.Series):
            blank = out["建議動作"].map(lambda v: _safe_str(v) == "")
            out.loc[blank, "建議動作"] = src.loc[blank]

    if "建議倉位" not in out.columns:
        if "建議倉位%" in out.columns:
            out["建議倉位"] = out["建議倉位%"].map(lambda v: f"{_safe_float(v, 0) or 0:.0f}%")
        elif "建議部位%" in out.columns:
            out["建議倉位"] = out["建議部位%"].map(lambda v: f"{_safe_float(v, 0) or 0:.0f}%")
        else:
            out["建議倉位"] = ""

    if "失效條件" not in out.columns:
        out["失效條件"] = out.get("失效條件_績效回饋", "")
    else:
        src = out.get("失效條件_績效回饋", "")
        if isinstance(src, pd.Series):
            blank = out["失效條件"].map(lambda v: _safe_str(v) == "")
            out.loc[blank, "失效條件"] = src.loc[blank]

    if "失效條件_績效回饋" not in out.columns:
        out["失效條件_績效回饋"] = out.get("失效條件", "")
    if "決策版本" not in out.columns:
        out["決策版本"] = PERFORMANCE_FEEDBACK_VERSION
    else:
        blank = out["決策版本"].map(lambda v: _safe_str(v) == "")
        out.loc[blank, "決策版本"] = PERFORMANCE_FEEDBACK_VERSION
    return out


def _ret5_score(ret5: pd.Series) -> pd.Series:
    # 0~8% 視為健康起漲；過高視為追高風險，負值則降低買點。
    s = pd.Series([55.0] * len(ret5), index=ret5.index)
    s = s.mask((ret5 >= 0) & (ret5 <= 8), 78)
    s = s.mask((ret5 > 8) & (ret5 <= 14), 62)
    s = s.mask(ret5 > 14, 38)
    s = s.mask(ret5 < 0, 48)
    return s.astype(float)


def _support_space_score(support_dist: pd.Series, resistance_space: pd.Series) -> pd.Series:
    support = pd.Series([55.0] * len(support_dist), index=support_dist.index)
    support = support.mask((support_dist >= 0) & (support_dist <= 5.5), 78)
    support = support.mask((support_dist > 5.5) & (support_dist <= 9), 64)
    support = support.mask(support_dist > 12, 40)
    space = pd.Series([55.0] * len(resistance_space), index=resistance_space.index)
    space = space.mask(resistance_space >= 8, 78)
    space = space.mask((resistance_space >= 4) & (resistance_space < 8), 65)
    space = space.mask((resistance_space > 0) & (resistance_space < 3), 38)
    return ((support * 0.55) + (space * 0.45)).clip(0, 100)


def _risk_reward_score(rr: pd.Series) -> pd.Series:
    s = pd.Series([50.0] * len(rr), index=rr.index)
    s = s.mask(rr >= 2.0, 82)
    s = s.mask((rr >= 1.5) & (rr < 2.0), 72)
    s = s.mask((rr >= 1.2) & (rr < 1.5), 58)
    s = s.mask((rr > 0) & (rr < 1.2), 40)
    return s.astype(float)


def _stop_distance_score(stop_dist: pd.Series) -> pd.Series:
    s = pd.Series([60.0] * len(stop_dist), index=stop_dist.index)
    s = s.mask((stop_dist > 0) & (stop_dist <= 5), 80)
    s = s.mask((stop_dist > 5) & (stop_dist <= 8), 66)
    s = s.mask(stop_dist > 8, 42)
    return s.astype(float)


def _build_correction_for_row(row: pd.Series, profile: dict[str, Any]) -> tuple[float, str, int]:
    rec_type = _safe_str(row.get("推薦型態")) or _safe_str(row.get("機會型態"))
    category = _safe_str(row.get("類別")) or _safe_str(row.get("產業"))
    score = _safe_float(row.get("推薦總分"), 0) or 0
    buy_grade = _safe_str(row.get("買點分級"))
    layer = _safe_str(row.get("推薦分層"))
    rec_bucket = _safe_str(row.get("推薦分桶"))
    next_action = _safe_str(row.get("隔日建議動作"))
    next_entry = _safe_str(row.get("進場型態_隔日"))
    market_guard = _safe_str(row.get("大盤橋接風控"))
    market_bucket = _safe_str(row.get("大盤情境分桶"))
    chase = _safe_float(row.get("追價風險分"), _safe_float(row.get("追高風險分數_決策"), 50)) or 50
    ret5 = _safe_float(row.get("近5日漲幅%"), 0) or 0
    no_buy = _safe_str(row.get("高分禁買原因"))
    gd_score = _safe_float(row.get("股神決策分數"), None)
    pre_score = _safe_float(row.get("起漲前兆分數"), _safe_float(row.get("飆股起漲分數"), None))
    entry_score = _safe_float(row.get("Entry進場買點分"), _safe_float(row.get("進場買點分"), None))
    risk_score = _safe_float(row.get("Risk風控安全分"), _safe_float(row.get("風控安全分"), None))

    score_bucket = _score_bucket(score)
    type_boost, type_n = _lookup_boost(profile, "by_recommend_type", rec_type)
    cat_boost, cat_n = _lookup_boost(profile, "by_category", category)
    bucket_boost, bucket_n = _lookup_boost(profile, "by_score_bucket", score_bucket)
    buy_boost, buy_n = _lookup_boost(profile, "by_buy_grade", buy_grade)
    layer_boost, layer_n = _lookup_boost(profile, "by_recommend_layer", layer)
    rec_bucket_boost, rec_bucket_n = _lookup_boost(profile, "by_recommend_bucket", rec_bucket)
    next_action_boost, next_action_n = _lookup_boost(profile, "by_nextday_action", next_action)
    next_entry_boost, next_entry_n = _lookup_boost(profile, "by_nextday_entry_type", next_entry)
    market_guard_boost, market_guard_n = _lookup_boost(profile, "by_market_guard", market_guard)
    market_bucket_boost, market_bucket_n = _lookup_boost(profile, "by_market_bucket", market_bucket)

    gd_boost, gd_n = _lookup_boost(profile, "by_股神決策分數_bucket", _numeric_score_bucket(gd_score, prefix="股神決策分數：")) if gd_score is not None else (0.0, 0)
    pre_boost, pre_n = _lookup_boost(profile, "by_起漲前兆分數_bucket", _numeric_score_bucket(pre_score, prefix="起漲前兆分數：")) if pre_score is not None else (0.0, 0)
    chase_boost, chase_n = _lookup_boost(profile, "by_追價風險分_bucket", _numeric_score_bucket(chase, prefix="追價風險分："))
    entry_boost, entry_n = _lookup_boost(profile, "by_Entry進場買點分_bucket", _numeric_score_bucket(entry_score, prefix="Entry進場買點分：")) if entry_score is not None else (0.0, 0)
    risk_boost, risk_n = _lookup_boost(profile, "by_Risk風控安全分_bucket", _numeric_score_bucket(risk_score, prefix="Risk風控安全分：")) if risk_score is not None else (0.0, 0)

    # V158：校正改為「績效分群 + 可操作分群」雙軌，不讓單一類別或舊總分主導。
    corr = (
        type_boost * 0.18
        + cat_boost * 0.14
        + bucket_boost * 0.08
        + buy_boost * 0.10
        + layer_boost * 0.10
        + rec_bucket_boost * 0.08
        + next_action_boost * 0.10
        + next_entry_boost * 0.10
        + market_guard_boost * 0.05
        + market_bucket_boost * 0.03
        + gd_boost * 0.08
        + pre_boost * 0.08
        + chase_boost * 0.06
        + entry_boost * 0.05
        + risk_boost * 0.05
    )
    reasons: list[str] = []
    if type_n:
        reasons.append(f"型態{rec_type}校正{type_boost:+.1f}/樣本{type_n}")
    if cat_n:
        reasons.append(f"類別{category}校正{cat_boost:+.1f}/樣本{cat_n}")
    if layer_n:
        reasons.append(f"分層{layer}校正{layer_boost:+.1f}/樣本{layer_n}")
    if rec_bucket_n:
        reasons.append(f"分桶{rec_bucket}校正{rec_bucket_boost:+.1f}/樣本{rec_bucket_n}")
    if next_action_n:
        reasons.append(f"隔日動作{next_action}校正{next_action_boost:+.1f}/樣本{next_action_n}")
    if gd_n:
        reasons.append(f"股神決策分數桶校正{gd_boost:+.1f}/樣本{gd_n}")
    if pre_n:
        reasons.append(f"起漲分數桶校正{pre_boost:+.1f}/樣本{pre_n}")
    if chase_n:
        reasons.append(f"追價風險桶校正{chase_boost:+.1f}/樣本{chase_n}")

    # 本次 20260711 紀錄檢查得到的保守實戰規則：
    # B轉強確認/拉回承接比 D尚未起漲與止跌反彈更適合操作；高分過熱仍要降級。
    if "B" in rec_type and "轉強確認" in rec_type:
        corr += 3.0
        reasons.append("B轉強確認歷史中位績效較佳 +3")
    if "拉回承接" in rec_type:
        corr += 2.5
        reasons.append("拉回承接歷史表現穩定 +2.5")
    if "C" in rec_type and "初步轉強" in rec_type:
        corr += 1.5
        reasons.append("C初步轉強保留但不過度加權 +1.5")
    if "D" in rec_type and "尚未起漲" in rec_type:
        corr -= 2.0
        reasons.append("D尚未起漲勝率/中位數偏弱 -2")
    if any(k in rec_type + layer for k in ["止跌反彈"]):
        corr -= 5.0
        reasons.append("止跌反彈績效較弱 -5")
    if buy_grade.startswith("B"):
        corr += 2.0
        reasons.append("B買點歷史風險較佳 +2")
    elif buy_grade.startswith("C"):
        corr -= 1.5
        reasons.append("C買點僅列觀察 -1.5")
    if "高分但過熱" in layer or _safe_str(row.get("高分禁買旗標")) == "是":
        corr -= 7.0
        reasons.append("高分但過熱硬降級 -7")
    if score >= 95 and ("高分但過熱" in layer or no_buy or chase >= 75 or ret5 >= 12):
        corr -= 8.0
        reasons.append("95分以上且過熱/追高風險 -8")
    elif 90 <= score < 95 and chase < 70 and ret5 < 10:
        corr += 1.5
        reasons.append("90-95且未過熱 +1.5")
    if chase >= 78:
        corr -= 8.0
        reasons.append("追高風險高 -8")
    elif chase >= 70:
        corr -= 4.0
        reasons.append("追高風險中 -4")
    if pre_score is not None and pre_score < 55:
        corr -= 2.0
        reasons.append("起漲前兆不足 -2")
    elif pre_score is not None and 60 <= pre_score < 80:
        corr += 1.5
        reasons.append("起漲前兆有效區間 +1.5")

    corr = max(-15.0, min(15.0, corr))
    total_sample = int(max(type_n, cat_n, bucket_n, buy_n, layer_n, rec_bucket_n, next_action_n, next_entry_n, market_guard_n, market_bucket_n, gd_n, pre_n, chase_n, entry_n, risk_n))
    return round(corr, 2), "｜".join(reasons[:10]) if reasons else "無足夠歷史分群資料，採保守校正", total_sample

def _decide_grade_and_role(row: pd.Series) -> tuple[str, str, str, str, str, str]:
    final_score = _safe_float(row.get("股神實戰總分"), 0) or 0
    potential = _safe_float(row.get("選股潛力分"), 0) or 0
    entry = _safe_float(row.get("進場買點分"), 0) or 0
    safety = _safe_float(row.get("風控安全分"), 0) or 0
    tech = _safe_float(row.get("技術結構分數"), 0) or 0
    heat = _safe_float(row.get("類股熱度分數"), 0) or 0
    pre = _safe_float(row.get("起漲前兆分數"), _safe_float(row.get("飆股起漲分數"), 0)) or 0
    chase = _safe_float(row.get("追價風險分"), 50) or 50
    ret5 = _safe_float(row.get("近5日漲幅%"), 0) or 0
    rec_type = _safe_str(row.get("推薦型態")) + _safe_str(row.get("機會型態"))
    layer = _safe_str(row.get("推薦分層")) + _safe_str(row.get("股神推薦層級"))
    no_buy = _safe_str(row.get("高分禁買原因"))
    score = _safe_float(row.get("推薦總分"), 0) or 0

    overheat_reasons = []
    if "高分但過熱" in layer:
        overheat_reasons.append("推薦分層過熱")
    if chase >= 78:
        overheat_reasons.append(f"追高風險{chase:.1f}")
    if ret5 >= 14:
        overheat_reasons.append(f"近5日漲幅{ret5:.1f}%")
    if score >= 95 and entry < 60:
        overheat_reasons.append("95分以上但買點分不足")
    if no_buy:
        overheat_reasons.append(no_buy)

    is_early = (("C" in rec_type and "初步轉強" in rec_type) or ("D" in rec_type and "尚未起漲" in rec_type))

    if overheat_reasons:
        grade = "D｜過熱禁買"
        role = "高分但過熱 / 禁買"
        suggestion = "不追價，等拉回支撐或重新突破確認。"
        trial = "否"
    elif final_score >= 88 and potential >= 85 and entry >= 70 and safety >= 65:
        grade = "A｜股神主買點"
        role = "股神主推薦"
        suggestion = "可列優先追蹤；依突破/回測條件分批執行。"
        trial = "可小量試單"
    elif is_early and potential >= 70 and tech >= 65 and heat >= 60 and pre >= 55 and chase < 75:
        grade = "C+｜早期潛伏"
        role = "早期潛伏股"
        suggestion = "剛起漲潛伏型；可小量試單，不追高，突破加碼。"
        trial = "可小量試單"
    elif potential >= 80 and entry >= 58 and safety >= 55:
        grade = "B｜等突破確認"
        role = "等突破確認"
        suggestion = "條件不差，但需等量價突破或回測承接。"
        trial = "待確認"
    elif potential >= 65:
        grade = "C-｜弱勢觀察"
        role = "觀察不追"
        suggestion = "保留觀察，尚未達進場條件。"
        trial = "否"
    else:
        grade = "C-｜弱勢觀察"
        role = "觀察不追"
        suggestion = "條件不足，不列主推薦。"
        trial = "否"

    add_condition = "放量突破確認價且不爆量開高走低；或回測支撐守穩後轉強。"
    invalid_condition = "跌破停損價/近端支撐，或量縮跌破MA20，取消推薦。"
    return grade, role, "、".join(overheat_reasons), trial, add_condition, suggestion + "｜" + invalid_condition



def _sync_phase61_feedback_columns(out: pd.DataFrame) -> pd.DataFrame:
    """Phase 6.1：讓績效回饋看懂飆股雷達與領漲回補角色。

    這裡只補回饋欄位，不重算推薦、不寫 JSON。
    """
    if out is None or out.empty:
        return out
    radar = _num(out, "爆發雷達分", 0).clip(0, 100)
    next_exp = _num(out, "隔日爆發分", 0).clip(0, 100)
    leader = _num(out, "主流領漲回補分", 0).clip(0, 100)
    theme = _num(out, "漲停族群相似度", 0).clip(0, 100)
    sector = _num(out, "族群攻擊強度", 50).clip(0, 100)
    money = _num(out, "主流資金分", 50).clip(0, 100)
    actual_short = _num(out, "推薦後1日%", 0)
    actual_short = actual_short.where(actual_short.ne(0), _num(out, "推薦後3日%", 0))
    actual_short = actual_short.where(actual_short.ne(0), _num(out, "即時追蹤報酬%", 0))
    miss_boost = actual_short.clip(lower=0, upper=10) * 3.0
    out["飆股雷達績效分"] = (radar * 0.42 + next_exp * 0.30 + miss_boost * 0.28).clip(0, 100).round(1)
    out["領漲回補績效分"] = (leader * 0.52 + theme * 0.22 + sector * 0.16 + money * 0.10).clip(0, 100).round(1)
    out["主流族群回饋分"] = (sector * 0.42 + money * 0.28 + theme * 0.18 + radar * 0.12).clip(0, 100).round(1)

    notes = []
    for _, row in out.iterrows():
        rr = _safe_str(row.get("飆股雷達角色"))
        lr = _safe_str(row.get("領漲回補角色"))
        perf = _safe_float(row.get("推薦後1日%"), None)
        if perf is None:
            perf = _safe_float(row.get("推薦後3日%"), None)
        parts = []
        if rr:
            parts.append(f"雷達={rr}")
        if lr:
            parts.append(f"回補={lr}")
        if perf is not None:
            parts.append(f"短線績效={perf:.2f}%")
        notes.append("｜".join(parts) if parts else "尚無 Phase6.1 雷達績效樣本")
    out["漏選修正提醒"] = [
        "若實際強勢股未進 S/L/T，下一輪提高主流族群/成交額/量能回補權重。" if (_safe_float(v, 0) or 0) >= 70 else "維持觀察；樣本不足不過度校正。"
        for v in out["領漲回補績效分"]
    ]
    out["Phase6_1回饋說明"] = notes
    return out

def apply_performance_feedback(df: pd.DataFrame | None, profile: dict[str, Any] | None = None) -> pd.DataFrame:
    """將歷史績效回饋欄位補到推薦結果。

    不會刪除原欄位；舊的推薦總分保留，新版排序可使用「股神實戰總分」。
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    out = out.loc[:, ~out.columns.duplicated()].copy()
    if profile is None:
        profile = load_godpick_performance_profile(DEFAULT_RECORD_PATH)
    if not profile or not profile.get("available"):
        for c in FEEDBACK_COLUMNS:
            if c not in out.columns:
                out[c] = "" if c not in {"股神實戰總分", "Alpha選股潛力分", "Entry進場買點分", "Risk風控安全分", "Feedback績效校正分", "選股潛力分", "進場買點分", "風控安全分", "績效校正分", "候選強度分", "績效樣本數", "建議倉位%", "漲停回放分", "強勢股漏選風險分"} else 0
        out["績效回饋版本"] = PERFORMANCE_FEEDBACK_VERSION
        out["績效校正說明"] = (profile or {}).get("message", "未載入績效回饋")
        return _sync_phase61_feedback_columns(_sync_phase1_feedback_columns(out))

    tech = _num(out, "技術結構分數", 50)
    pre = _num(out, "起漲前兆分數", 0)
    pre = pre.where(pre > 0, _num(out, "飆股起漲分數", 50))
    heat = _num(out, "類股熱度分數", 50)
    pattern = _num(out, "型態突破分數", 50)
    burst = _num(out, "爆發力分數", 50)
    factor = _num(out, "自動因子總分", 50)
    leader = _num(out, "同類股領先幅度", 50)
    buy_score = _num(out, "買進分數", 0)
    buy_score = buy_score.where(buy_score > 0, _num(out, "實戰買點分數", 0))
    buy_score = buy_score.where(buy_score > 0, _num(out, "交易可行分數", 50))
    entry_score = _num(out, "隔日進場分數", 50)
    chase = _num(out, "追價風險分", 50)
    chase = chase.where(chase > 0, _num(out, "追高風險分數_決策", 50))
    rr = _num(out, "風險報酬比", 0)
    rr = rr.where(rr > 0, _num(out, "風險報酬比_決策", 0))
    stop_dist = _num(out, "停損距離%", 0)
    support_dist = _num(out, "支撐距離%", 0)
    resistance_space = _num(out, "壓力空間%", 0)
    ret5 = _num(out, "近5日漲幅%", 0)
    no_buy = _text(out, "高分禁買原因")

    out["選股潛力分"] = (tech * 0.30 + pre * 0.25 + heat * 0.15 + pattern * 0.10 + burst * 0.08 + factor * 0.05 + leader * 0.07).clip(0, 100).round(1)
    out["進場買點分"] = (buy_score * 0.40 + (100 - chase).clip(0, 100) * 0.20 + _ret5_score(ret5) * 0.15 + _support_space_score(support_dist, resistance_space) * 0.15 + entry_score * 0.10).clip(0, 100).round(1)
    no_buy_penalty = no_buy.str.strip().ne("").map({True: 38.0, False: 76.0}).astype(float)
    out["風控安全分"] = ((100 - chase).clip(0, 100) * 0.35 + _stop_distance_score(stop_dist) * 0.25 + _risk_reward_score(rr) * 0.25 + no_buy_penalty * 0.15).clip(0, 100).round(1)

    correction_rows = out.apply(lambda r: _build_correction_for_row(r, profile), axis=1)
    out["績效校正分"] = [x[0] for x in correction_rows]
    out["績效校正說明"] = [x[1] for x in correction_rows]
    out["績效樣本數"] = [x[2] for x in correction_rows]
    out["股神實戰總分"] = (
        out["選股潛力分"] * 0.50
        + out["進場買點分"] * 0.25
        + out["風控安全分"] * 0.15
        + out["績效校正分"]
    ).clip(0, 100).round(1)

    decisions = out.apply(_decide_grade_and_role, axis=1)
    out["新買點分級"] = [x[0] for x in decisions]
    out["推薦角色"] = [x[1] for x in decisions]
    out["過熱原因"] = [x[2] for x in decisions]
    out["小量試單建議"] = [x[3] for x in decisions]
    out["加碼條件"] = [x[4] for x in decisions]
    out["績效回饋建議"] = [x[5] for x in decisions]
    out["失效條件_績效回饋"] = "跌破停損價/近端支撐或量縮跌破MA20，取消推薦。"
    out["績效回饋版本"] = PERFORMANCE_FEEDBACK_VERSION
    # Phase 6.2 欄位若已由決策/雷達引擎補齊，回饋層只補文字摘要，不重算。
    if "Phase6_2回放說明" not in out.columns:
        out["Phase6_2回放說明"] = "漲停/強勢股回放由 godpick_miss_replay_engine 統一診斷；此處只保留欄位供 8/12/14 做績效回饋。"

    # 不覆蓋原始推論，僅補充績效回饋摘要。
    if "股神推論邏輯" in out.columns:
        base = out["股神推論邏輯"].map(_safe_str)
        add = out["績效回饋建議"].map(_safe_str)
        out["股神推論邏輯"] = [b + ("｜績效回饋：" + a if a and a not in b else "") for b, a in zip(base, add)]
    return _sync_phase61_feedback_columns(_sync_phase1_feedback_columns(out))


def performance_feedback_summary(profile: dict[str, Any] | None) -> list[tuple[str, str, str]]:
    if not profile or not profile.get("available"):
        return [("績效回饋", (profile or {}).get("message", "未載入"), "")]
    b = profile.get("baseline", {})
    top_types = "、".join(profile.get("top_recommend_types", [])[:3]) or "無"
    weak_types = "、".join(profile.get("weak_recommend_types", [])[:3]) or "無"
    top_cats = "、".join(profile.get("top_categories", [])[:3]) or "無"
    return [
        ("歷史樣本", f"{int(_safe_float(b.get('sample'), 0) or 0)} 筆", ""),
        ("平均報酬 / 勝率", f"{_safe_float(b.get('avg_return'), 0):.2f}% / {_safe_float(b.get('win_rate'), 0) * 100:.1f}%", ""),
        ("加權型態", top_types, ""),
        ("降權型態", weak_types, ""),
        ("強勢類別", top_cats, ""),
    ]

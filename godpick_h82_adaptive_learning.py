# -*- coding: utf-8 -*-
"""H82 Adaptive GodPick Learning Core.

Goal
----
Turn permanent Page08 recommendation history into a bounded, explainable
research-ranking feedback loop.

Safety/governance
-----------------
* H82 never creates H64/H68 formal authority.
* H82 never changes H79 price-plan/RR/stop/liquidity/freshness gates.
* H82 separates formal performance from research/calibration performance.
* Mature samples, shrinkage, time decay and data-quality freeze are required
  before any non-zero adaptive ranking adjustment is applied.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
import hashlib
import json
import math
import os

import pandas as pd

VERSION = "v191_h82_adaptive_godpick_learning_core_20260921"
STATE_FILE = "godpick_adaptive_learning_state.json"
STATE_FIRESTORE_DOC = "godpick_adaptive_learning_state"
RECORDS_FILE = "godpick_records.json"
BASE_DIR = Path(__file__).resolve().parent

H82_COLUMNS = [
    "H82版本", "H82市場環境", "H82成熟樣本數", "H82有效樣本權重", "H82學習信心%",
    "H82市場環境加減分", "H82產業加減分", "H82決策狀態加減分", "H82H81分桶加減分",
    "H82錯誤治理加減分", "H82影子建議加減分", "H82自適應加減分", "H82自適應研究排序分",
    "H82主要學習依據", "H82主要錯誤風險", "H82學習摘要", "H82學習狀態", "H82設定版本",
]


def _text(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _num(v: Any, default: float | None = None) -> float | None:
    if v is None:
        return default
    if isinstance(v, bool):
        return default
    if isinstance(v, str):
        v = v.replace(",", "").replace("％", "%").replace("%", "").replace("+", "").strip()
        if not v or v.lower() in {"nan", "none", "null", "--", "-", "<na>"}:
            return default
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _bool(v: Any) -> bool:
    return _text(v).lower() in {"true", "1", "yes", "y", "是", "已買進", "已達", "達標"}


def _clip(x: float, low: float, high: float) -> float:
    return max(low, min(high, float(x)))


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).clip(lower=0.0)
    m = v.notna() & w.gt(0)
    if not m.any():
        return 0.0
    return float((v[m] * w[m]).sum() / w[m].sum())


def _weighted_std(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce")
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).clip(lower=0.0)
    m = v.notna() & w.gt(0)
    if not m.any():
        return 0.0
    v, w = v[m], w[m]
    mean = float((v * w).sum() / w.sum())
    var = float((w * (v - mean) ** 2).sum() / w.sum())
    return math.sqrt(max(0.0, var))


def _parse_date(v: Any) -> pd.Timestamp | None:
    try:
        x = pd.to_datetime(v, errors="coerce")
        return None if pd.isna(x) else pd.Timestamp(x).tz_localize(None) if getattr(x, "tzinfo", None) else pd.Timestamp(x)
    except Exception:
        return None


def _business_age(rec_date: Any, updated: Any) -> int | None:
    r, u = _parse_date(rec_date), _parse_date(updated)
    if r is None:
        return None
    if u is None:
        u = pd.Timestamp.today().normalize()
    if u < r:
        return None
    try:
        return max(0, len(pd.bdate_range(r.normalize() + pd.Timedelta(days=1), u.normalize())))
    except Exception:
        return max(0, int((u - r).days))


def _sample_type(row: dict[str, Any]) -> str:
    explicit = _text(row.get("校正樣本類型"))
    if explicit:
        return explicit
    level = _text(row.get("紀錄層級"))
    mode = _text(row.get("推薦模式"))
    bucket = _text(row.get("正式推薦分區"))
    if mode == "股神校正研究" or "研究推薦" in level:
        return "B｜H79研究推薦校正研究樣本"
    if "正式主推薦" in level or bucket == "正式下週主推薦":
        return "A｜正式交易樣本"
    if "A-" in level:
        return "A-｜準主推薦樣本"
    if "R1" in level:
        return "B｜R1雷達樣本"
    return level or mode or "舊版推薦樣本"


def _sample_weight(row: dict[str, Any], sample_type: str) -> float:
    explicit = _num(row.get("校正樣本權重"), None)
    if explicit is not None:
        return _clip(explicit, 0.0, 1.0)
    if sample_type.startswith("A-"):
        return 0.90
    if sample_type.startswith("A"):
        return 1.00
    if "H79研究推薦" in sample_type:
        return 0.45
    if sample_type.startswith("B") or "R1" in sample_type:
        return 0.75
    if sample_type.startswith(("C", "D")):
        return 0.0
    return 0.60


def _is_research_sample(row: dict[str, Any], sample_type: str | None = None) -> bool:
    sample_type = sample_type or _sample_type(row)
    return (
        _text(row.get("推薦模式")) == "股神校正研究"
        or "研究推薦" in _text(row.get("紀錄層級"))
        or "研究" in sample_type
        or sample_type.startswith(("C", "D"))
    )


def _regime(row: dict[str, Any]) -> str:
    txt = "｜".join(_text(row.get(k)) for k in [
        "H72市場模式", "大盤趨勢模式", "大盤情境", "大盤狀態", "大盤分層",
        "市場狀態", "大盤橋接風控", "市場環境", "大盤模式",
    ]).upper()
    if any(k in txt for k in ["HIGH_VOL", "PANIC", "恐慌", "事件", "高波動", "極端"]):
        return "高波動/事件"
    if any(k in txt for k in ["BEAR", "DEFENSIVE", "RISK-OFF", "空頭", "熊市", "弱勢"]):
        return "空頭/防禦"
    if any(k in txt for k in ["BULL", "RISK-ON", "多頭", "主升", "強勢"]):
        return "多頭/進攻"
    if any(k in txt for k in ["RANGE", "SIDEWAY", "盤整", "震盪", "中性"]):
        return "盤整/中性"
    score = None
    for key in ["市場環境分數", "大盤情境分數", "大盤風控分數", "大盤橋接分數"]:
        score = _num(row.get(key), None)
        if score is not None:
            break
    if score is not None:
        if score >= 65:
            return "多頭/進攻"
        if score <= 35:
            return "空頭/防禦"
        return "盤整/中性"
    return "市場環境未確認"


def _sector(row: dict[str, Any]) -> str:
    for key in ["正式產業別", "類別", "產業", "族群", "主題類別"]:
        value = _text(row.get(key))
        if value:
            return value
    return "未分類"


def _decision_status(row: dict[str, Any]) -> str:
    status = _text(row.get("H79推薦狀態")) or _text(row.get("H79決策層級")) or _text(row.get("紀錄層級"))
    if "等待拉回" in status or "過熱" in status:
        return "等待拉回/過熱"
    if "重建進場" in status or "等待合理買點" in status:
        return "等待價格結構"
    if "等待正式授權" in status:
        return "等待Formal授權"
    if "條件可執行" in status or "正式" in status:
        return "正式/條件可執行"
    if "研究" in status:
        return "研究推薦"
    if "觀察" in status or "等待" in status:
        return "觀察等待"
    return status or "未分類"


def _h81_bucket(row: dict[str, Any]) -> str:
    score = _num(row.get("H81專業研究總分"), None)
    if score is None:
        return "H81未評分"
    if score >= 80:
        return "H81>=80"
    if score >= 70:
        return "H81 70-79"
    if score >= 60:
        return "H81 60-69"
    if score >= 50:
        return "H81 50-59"
    return "H81<50"


def _suspicious_proxy(row: dict[str, Any]) -> bool:
    source = _text(row.get("績效資料來源")).lower()
    if any(k in source for k in ["proxy", "代理", "目前", "最新價", "即時"]):
        return True
    vals = []
    for key in ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%"]:
        v = _num(row.get(key), None)
        if v is not None:
            vals.append(round(v, 8))
    return len(vals) >= 3 and max(vals) - min(vals) < 1e-9


def _tracking_return(row: dict[str, Any], horizon: int) -> tuple[float | None, str]:
    # User's true transaction outcome has first priority.
    actual = _num(row.get("實際報酬%"), None)
    if (_bool(row.get("是否已實際買進")) or _bool(row.get("是否已買進"))) and actual is not None:
        return actual, "實際交易"

    updated = row.get("績效更新時間") or row.get("追蹤更新時間") or row.get("更新時間") or row.get("最新更新時間")
    age = _business_age(row.get("推薦日期") or row.get("推薦日") or row.get("建立時間"), updated)
    if age is None or age < horizon or _suspicious_proxy(row):
        return None, "未成熟/可疑代理"

    keys = [f"可執行交易{horizon}日%", f"推薦後{horizon}日%", f"{horizon}日績效%", f"{horizon}日報酬%"]
    for key in keys:
        value = _num(row.get(key), None)
        if value is not None and -100.0 <= value <= 300.0:
            return value, key
    return None, "缺成熟報酬"


def _error_labels(row: dict[str, Any], ret: float, sample_type: str, cfg: dict[str, Any]) -> list[str]:
    t = cfg["error_thresholds"]
    labels: list[str] = []
    research = _is_research_sample(row, sample_type)
    if ret >= float(t["success_return_pct"]):
        labels.append("成功選股")
    if ret <= float(t["direction_error_return_pct"]):
        labels.append("方向判斷錯誤")
    if research and ret >= float(t["missed_opportunity_return_pct"]):
        labels.append("錯失強勢機會")
    if research and ret <= float(t["direction_error_return_pct"]):
        labels.append("正確拒絕/降級")

    r1 = _num(row.get("推薦後1日%"), None)
    if r1 is not None and r1 < 0 and ret >= float(t["entry_timing_recovery_pct"]):
        labels.append("買點時機偏早")

    chase = None
    for key in ["追價風險分", "追高風險分數_決策", "追高風險分"]:
        chase = _num(row.get(key), None)
        if chase is not None:
            break
    status = _text(row.get("H79推薦狀態")) + "｜" + _text(row.get("推薦分層"))
    if ret < 0 and ((chase is not None and chase >= float(t["overheat_risk_score"])) or any(k in status for k in ["過熱", "等待拉回"])):
        labels.append("過熱追價風險")

    rr = _num(row.get("H79成本後RR"), _num(row.get("風險報酬比"), None))
    if rr is not None and rr < float(t["low_rr"]) and ret <= 0:
        labels.append("低RR交易品質")

    stop_distance = _num(row.get("H79停損距離%"), _num(row.get("停損距離%"), None))
    if _bool(row.get("是否達停損")) and stop_distance is not None and stop_distance <= float(t["tight_stop_pct"]):
        labels.append("停損過緊/雜訊停損")

    text_blob = "｜".join(_text(row.get(k)) for k in ["推薦型態", "進場型態_隔日", "推薦理由摘要", "H79推薦狀態"])
    if ret <= float(t["direction_error_return_pct"]) and any(k in text_blob for k in ["突破", "轉強", "主升"]):
        labels.append("假突破/轉強失敗")
    return list(dict.fromkeys(labels)) or ["無明確錯誤標籤"]


def extract_mature_learning_samples(records: pd.DataFrame | list[dict[str, Any]] | None, settings: dict[str, Any] | None = None) -> pd.DataFrame:
    cfg = _settings(settings)
    df = records.copy() if isinstance(records, pd.DataFrame) else pd.DataFrame(records or [])
    if df.empty:
        return pd.DataFrame()
    df = df.loc[:, ~df.columns.duplicated()].copy()
    horizon = int(cfg["primary_horizon"])
    latest_date = None
    dates = pd.to_datetime(df.get("推薦日期", pd.Series(dtype=object)), errors="coerce")
    if dates.notna().any():
        latest_date = pd.Timestamp(dates.max()).normalize()
    if latest_date is None:
        latest_date = pd.Timestamp.today().normalize()

    rows = []
    for _, s in df.iterrows():
        row = s.to_dict()
        stype = _sample_type(row)
        weight = _sample_weight(row, stype)
        eligible_flag = _text(row.get("是否納入權重校正"))
        if eligible_flag and not _bool(eligible_flag):
            weight = 0.0
        ret, ret_source = _tracking_return(row, horizon)
        if ret is None:
            continue
        rec_date = _parse_date(row.get("推薦日期") or row.get("推薦日") or row.get("建立時間"))
        age_days = max(0, int((latest_date - rec_date.normalize()).days)) if rec_date is not None else 0
        decay = 1.0
        if bool(cfg["features"].get("time_decay", True)):
            decay = 0.5 ** (age_days / max(1.0, float(cfg["half_life_days"])))
        eff_weight = weight * decay
        if eff_weight <= 0:
            continue
        labels = _error_labels(row, ret, stype, cfg)
        rows.append({
            "股票代號": _text(row.get("股票代號")),
            "推薦日期": _text(row.get("推薦日期"))[:10],
            "樣本類型": stype,
            "研究樣本": _is_research_sample(row, stype),
            "正式績效": _text(row.get("是否納入正式推薦績效")) == "是" or stype.startswith(("A｜", "A-｜")),
            "報酬%": float(ret),
            "報酬來源": ret_source,
            "樣本權重": float(weight),
            "時間衰減": float(decay),
            "有效權重": float(eff_weight),
            "可疑代理": bool(_suspicious_proxy(row)),
            "市場環境": _regime(row),
            "產業": _sector(row),
            "決策狀態": _decision_status(row),
            "H81分桶": _h81_bucket(row),
            "錯誤標籤": "；".join(labels),
        })
    return pd.DataFrame(rows)


def _segment_profile(samples: pd.DataFrame, key: str, cfg: dict[str, Any], baseline: dict[str, float]) -> dict[str, dict[str, Any]]:
    if samples.empty or key not in samples.columns:
        return {}
    out: dict[str, dict[str, Any]] = {}
    min_n = int(cfg["minimum_segment_mature_samples"])
    min_eff = float(cfg["minimum_effective_segment_samples"])
    prior = float(cfg["shrinkage_prior_samples"])
    cap = float(cfg["max_component_abs_points"])
    conf_floor = float(cfg["confidence_floor"])
    winsor = float(cfg["winsorize_return_pct"])

    for name, g in samples.groupby(key, dropna=False):
        ret = pd.to_numeric(g["報酬%"], errors="coerce").clip(-winsor, winsor)
        w = pd.to_numeric(g["有效權重"], errors="coerce").fillna(0.0)
        mask = ret.notna() & w.gt(0)
        ret, w = ret[mask], w[mask]
        n = int(mask.sum())
        eff = float(w.sum())
        if not n or eff <= 0:
            continue
        avg = _weighted_mean(ret, w)
        win = _weighted_mean((ret > 0).astype(float), w)
        loss5 = _weighted_mean((ret <= -5).astype(float), w)
        std = _weighted_std(ret, w)
        se = std / max(1.0, math.sqrt(max(eff, 1e-6)))
        reliability = min(1.0, eff / max(min_eff, float(min_n) * 0.6))
        precision = max(0.35, min(1.0, 1.0 - se / max(8.0, abs(avg) + 8.0)))
        confidence = reliability * precision
        shrink = eff / (eff + prior)
        return_edge = _clip((avg - baseline["avg_return"]) / 5.0, -1.0, 1.0)
        win_edge = _clip((win - baseline["win_rate"]) / 0.20, -1.0, 1.0)
        loss_edge = _clip(-(loss5 - baseline["loss5_rate"]) / 0.15, -1.0, 1.0)
        edge = return_edge * 0.45 + win_edge * 0.35 + loss_edge * 0.20
        ready = n >= min_n and eff >= min_eff and confidence >= conf_floor
        adjustment = cap * edge * shrink * confidence if ready else 0.0
        out[_text(name) or "未分類"] = {
            "sample": n,
            "effective_sample": round(eff, 4),
            "avg_return": round(avg, 4),
            "win_rate": round(win, 4),
            "loss5_rate": round(loss5, 4),
            "std_return": round(std, 4),
            "confidence": round(confidence, 4),
            "shrinkage": round(shrink, 4),
            "ready": bool(ready),
            "adjustment": round(_clip(adjustment, -cap, cap), 4),
        }
    return out


def _error_summary(samples: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if samples.empty:
        return {}
    total_w = float(pd.to_numeric(samples["有效權重"], errors="coerce").fillna(0).sum())
    labels = [
        "成功選股", "方向判斷錯誤", "錯失強勢機會", "正確拒絕/降級", "買點時機偏早",
        "過熱追價風險", "低RR交易品質", "停損過緊/雜訊停損", "假突破/轉強失敗",
    ]
    out = {}
    for label in labels:
        mask = samples["錯誤標籤"].fillna("").astype(str).str.contains(label, regex=False)
        weight = float(pd.to_numeric(samples.loc[mask, "有效權重"], errors="coerce").fillna(0).sum())
        out[label] = {
            "sample": int(mask.sum()),
            "effective_weight": round(weight, 4),
            "rate": round(weight / total_w, 4) if total_w > 0 else 0.0,
        }
    return out


def _learning_directives(error_summary: dict[str, dict[str, Any]]) -> list[str]:
    def rate(label: str) -> float:
        return float((error_summary.get(label) or {}).get("rate", 0.0) or 0.0)
    directives = []
    if rate("過熱追價風險") >= 0.12:
        directives.append("過熱追價錯誤偏高：現階段應降低高追價風險/等待拉回標的研究順位。")
    if rate("低RR交易品質") >= 0.10:
        directives.append("低RR失敗偏高：價格計畫未改善前，不因基本面/題材強就提高研究順位。")
    if rate("買點時機偏早") >= 0.10:
        directives.append("買點偏早樣本偏高：增加突破守價/回測承接確認的研究權重。")
    if rate("方向判斷錯誤") >= 0.25:
        directives.append("方向錯誤率偏高：市場環境與產業共振需優先於單股高分。")
    if rate("錯失強勢機會") >= 0.08:
        directives.append("漏選強勢樣本存在：只提高召回研究，不可因此放寬Formal交易閘門。")
    return directives[:5] or ["目前沒有單一錯誤型態達到治理警戒門檻，維持小幅、可解釋的自適應。"]


def build_learning_state(records: pd.DataFrame | list[dict[str, Any]] | None, settings: dict[str, Any] | None = None, *, source_signature: str = "") -> dict[str, Any]:
    cfg = _settings(settings)
    raw_df = records.copy() if isinstance(records, pd.DataFrame) else pd.DataFrame(records or [])
    raw_df = raw_df.loc[:, ~raw_df.columns.duplicated()].copy() if not raw_df.empty else raw_df
    suspicious_all = 0
    proxy_candidate_rows = 0
    if not raw_df.empty:
        for _, _srow in raw_df.iterrows():
            _r = _srow.to_dict()
            has_perf = any(_num(_r.get(_k), None) is not None for _k in [
                "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%",
                "可執行交易1日%", "可執行交易3日%", "可執行交易5日%", "可執行交易10日%", "可執行交易20日%",
            ])
            if has_perf:
                proxy_candidate_rows += 1
                suspicious_all += int(_suspicious_proxy(_r))
    samples = extract_mature_learning_samples(raw_df, cfg)
    winsor = float(cfg["winsorize_return_pct"])
    if samples.empty:
        return {
            "version": VERSION, "available": False, "frozen": True, "freeze_reasons": ["沒有成熟可學習樣本"],
            "generated_at": _now(), "source_signature": source_signature, "settings_fingerprint": _settings_fingerprint(cfg),
            "baseline": {"sample": 0, "effective_sample": 0.0, "avg_return": 0.0, "win_rate": 0.0, "loss5_rate": 0.0},
            "segments": {}, "error_summary": {}, "directives": ["等待成熟樣本後再啟用自適應。"], "data_quality": {"mature_samples": 0},
        }

    ret = pd.to_numeric(samples["報酬%"], errors="coerce").clip(-winsor, winsor)
    w = pd.to_numeric(samples["有效權重"], errors="coerce").fillna(0.0)
    baseline = {
        "sample": int(ret.notna().sum()),
        "effective_sample": round(float(w[ret.notna()].sum()), 4),
        "avg_return": round(_weighted_mean(ret, w), 4),
        "win_rate": round(_weighted_mean((ret > 0).astype(float), w), 4),
        "loss5_rate": round(_weighted_mean((ret <= -5).astype(float), w), 4),
        "formal_samples": int(samples["正式績效"].fillna(False).sum()),
        "research_samples": int(samples["研究樣本"].fillna(False).sum()),
    }
    suspicious = int(suspicious_all)
    suspicious_ratio = suspicious / max(proxy_candidate_rows, 1)
    freeze_reasons: list[str] = []
    if len(samples) < int(cfg["minimum_global_mature_samples"]):
        freeze_reasons.append(f"成熟樣本{len(samples)}<{int(cfg['minimum_global_mature_samples'])}")
    if bool(cfg["features"].get("freeze_on_data_quality", True)) and suspicious_ratio > float(cfg["max_suspicious_proxy_ratio"]):
        freeze_reasons.append(f"可疑代理比例{suspicious_ratio:.1%}>{float(cfg['max_suspicious_proxy_ratio']):.1%}")
    if cfg["mode"] == "frozen" or not bool(cfg.get("enabled", True)):
        freeze_reasons.append("H82設定為凍結/停用")
    frozen = bool(freeze_reasons)

    segments = {
        "market_regime": _segment_profile(samples, "市場環境", cfg, baseline),
        "sector": _segment_profile(samples, "產業", cfg, baseline),
        "decision_status": _segment_profile(samples, "決策狀態", cfg, baseline),
        "h81_bucket": _segment_profile(samples, "H81分桶", cfg, baseline),
    }
    errors = _error_summary(samples)
    return {
        "version": VERSION,
        "available": not frozen,
        "frozen": frozen,
        "freeze_reasons": freeze_reasons,
        "generated_at": _now(),
        "source_signature": source_signature,
        "settings_fingerprint": _settings_fingerprint(cfg),
        "primary_horizon": int(cfg["primary_horizon"]),
        "baseline": baseline,
        "segments": segments,
        "error_summary": errors,
        "directives": _learning_directives(errors),
        "data_quality": {
            "mature_samples": int(len(samples)),
            "suspicious_proxy_samples": suspicious,
            "suspicious_proxy_candidate_rows": int(proxy_candidate_rows),
            "suspicious_proxy_ratio": round(suspicious_ratio, 4),
            "sample_type_counts": {str(k): int(v) for k, v in samples["樣本類型"].value_counts().to_dict().items()},
            "regime_counts": {str(k): int(v) for k, v in samples["市場環境"].value_counts().to_dict().items()},
        },
    }


def _settings(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    if isinstance(settings, dict):
        try:
            from godpick_h82_learning_settings import normalize_settings
            return normalize_settings(settings)
        except Exception:
            return deepcopy(settings)
    try:
        from godpick_h82_learning_settings import load_settings_safe
        return load_settings_safe()
    except Exception:
        from godpick_h82_learning_settings import DEFAULT_SETTINGS, normalize_settings
        return normalize_settings(DEFAULT_SETTINGS)


def _settings_fingerprint(cfg: dict[str, Any]) -> str:
    material = deepcopy(cfg)
    material.pop("updated_at", None)
    material.pop("update_seq", None)
    raw = json.dumps(material, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:20]


def _now() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _records_path(path: str | os.PathLike[str] = RECORDS_FILE) -> Path:
    p = Path(path)
    return p if p.is_absolute() else BASE_DIR / p


def records_source_signature(path: str | os.PathLike[str] = RECORDS_FILE, settings: dict[str, Any] | None = None) -> str:
    cfg = _settings(settings)
    p = _records_path(path)
    try:
        stat = p.stat()
        base = f"{p.resolve()}|{stat.st_mtime_ns}|{stat.st_size}|h={cfg['primary_horizon']}|{VERSION}"
    except Exception:
        base = f"{p}|missing|h={cfg['primary_horizon']}|{VERSION}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _load_records(path: str | os.PathLike[str] = RECORDS_FILE) -> list[dict[str, Any]]:
    p = _records_path(path)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ["records", "items", "data", "rows"]:
            if isinstance(data.get(key), list):
                return [x for x in data[key] if isinstance(x, dict)]
    return []


def _read_state_local(path: str | os.PathLike[str] = STATE_FILE) -> dict[str, Any]:
    p = _records_path(path)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8-sig"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_state_local(state: dict[str, Any], path: str | os.PathLike[str] = STATE_FILE) -> tuple[bool, str]:
    p = _records_path(path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp_h82")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        tmp.replace(p)
        return True, f"local:{p.name}"
    except Exception as exc:
        return False, f"local state write failed:{exc}"


def refresh_learning_state(*, records: pd.DataFrame | list[dict[str, Any]] | None = None, settings: dict[str, Any] | None = None, records_path: str = RECORDS_FILE, persist_remote: bool = False) -> tuple[dict[str, Any], list[str]]:
    cfg = _settings(settings)
    signature = records_source_signature(records_path, cfg) if records is None else "in_memory:" + _settings_fingerprint(cfg)
    source = _load_records(records_path) if records is None else records
    state = build_learning_state(source, cfg, source_signature=signature)
    ok, msg = _write_state_local(state)
    notes = [msg]
    if persist_remote:
        try:
            from godpick_persistence_service import save_named_json_permanent
            report = save_named_json_permanent(STATE_FILE, state, firestore_doc=STATE_FIRESTORE_DOC)
            notes.extend(x for x in [getattr(report, "local_message", ""), getattr(report, "github_message", ""), getattr(report, "firestore_message", "")] if x)
            if not bool(getattr(report, "permanent_ok", False)):
                notes.append("H82學習狀態尚未取得遠端永久確認；本地狀態仍可由永久推薦紀錄重建。")
        except Exception as exc:
            notes.append(f"H82學習狀態遠端保存失敗：{type(exc).__name__}: {exc}")
    elif bool(cfg["features"].get("auto_persist_learning_state", True)) and ok:
        # Non-blocking best-effort remote persistence.  The authority remains
        # reconstructable from godpick_records.json if this queue is unavailable.
        try:
            from godpick_durability_service import persist_json_async
            qok, qmsg = persist_json_async(STATE_FILE, state, firestore_doc=STATE_FIRESTORE_DOC, reason="H82 adaptive learning refresh")
            notes.append(qmsg if qok else f"H82背景永久化未排程：{qmsg}")
        except Exception:
            pass
    return state, notes


def load_learning_state(*, settings: dict[str, Any] | None = None, records_path: str = RECORDS_FILE, auto_refresh: bool | None = None) -> dict[str, Any]:
    cfg = _settings(settings)
    if auto_refresh is None:
        auto_refresh = bool(cfg["features"].get("auto_refresh_from_records", True))
    signature = records_source_signature(records_path, cfg)
    state = _read_state_local()
    valid = (
        isinstance(state, dict)
        and state.get("version") == VERSION
        and state.get("source_signature") == signature
        and state.get("settings_fingerprint") == _settings_fingerprint(cfg)
    )
    if valid or not auto_refresh:
        return state if isinstance(state, dict) else {}
    state, _ = refresh_learning_state(settings=cfg, records_path=records_path, persist_remote=False)
    return state


def _profile_adjustment(state: dict[str, Any], section: str, key: str) -> tuple[float, float, int]:
    row = (((state.get("segments") or {}).get(section) or {}).get(key) or {})
    if not bool(row.get("ready")):
        return 0.0, float(row.get("confidence", 0.0) or 0.0), int(row.get("sample", 0) or 0)
    return float(row.get("adjustment", 0.0) or 0.0), float(row.get("confidence", 0.0) or 0.0), int(row.get("sample", 0) or 0)


def _error_guard(row: dict[str, Any], state: dict[str, Any], cfg: dict[str, Any]) -> tuple[float, list[str]]:
    if not bool(cfg["features"].get("error_taxonomy_guard", True)):
        return 0.0, []
    errors = state.get("error_summary") or {}
    reasons = []
    penalty = 0.0
    def rate(label: str) -> float:
        return float((errors.get(label) or {}).get("rate", 0.0) or 0.0)
    chase = _num(row.get("追價風險分"), _num(row.get("追高風險分數_決策"), None))
    status = _text(row.get("H79推薦狀態")) + "｜" + _text(row.get("推薦分層"))
    if rate("過熱追價風險") >= 0.12 and ((chase is not None and chase >= float(cfg["error_thresholds"]["overheat_risk_score"])) or any(k in status for k in ["過熱", "等待拉回"])):
        p = min(0.55, rate("過熱追價風險") * 2.0)
        penalty -= p
        reasons.append(f"歷史過熱追價錯誤率{rate('過熱追價風險'):.1%}")
    rr = _num(row.get("H79成本後RR"), _num(row.get("風險報酬比"), None))
    if rate("低RR交易品質") >= 0.10 and rr is not None and rr < float(cfg["error_thresholds"]["low_rr"]):
        p = min(0.45, rate("低RR交易品質") * 2.0)
        penalty -= p
        reasons.append(f"歷史低RR錯誤率{rate('低RR交易品質'):.1%}")
    if rate("假突破/轉強失敗") >= 0.12:
        txt = "｜".join(_text(row.get(k)) for k in ["推薦型態", "進場型態_隔日", "推薦理由摘要"])
        if any(k in txt for k in ["突破", "轉強", "主升"]):
            p = min(0.35, rate("假突破/轉強失敗") * 1.5)
            penalty -= p
            reasons.append(f"歷史假突破失敗率{rate('假突破/轉強失敗'):.1%}")
    return max(-0.9, penalty), reasons


def analyze_candidate_learning(row: dict[str, Any] | pd.Series, state: dict[str, Any] | None = None, settings: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
    cfg = _settings(settings)
    state = state if isinstance(state, dict) else load_learning_state(settings=cfg)
    regime, sector, status, bucket = _regime(raw), _sector(raw), _decision_status(raw), _h81_bucket(raw)
    baseline = state.get("baseline") or {}
    mature_n = int(baseline.get("sample", 0) or 0)
    effective = float(baseline.get("effective_sample", 0.0) or 0.0)
    if not bool(cfg.get("enabled", True)) or cfg.get("mode") == "frozen":
        active = False
        state_label = "設定停用/凍結"
    elif not isinstance(state, dict) or not state.get("available") or state.get("frozen"):
        active = False
        state_label = "學習凍結：" + "；".join(state.get("freeze_reasons") or ["樣本/資料品質不足"])
    else:
        active = True
        state_label = "ACTIVE-BOUNDED" if cfg.get("mode") == "active_bounded" else "SHADOW-ONLY"

    feature_map = [
        ("market_regime", regime, "market_regime_learning", "H82市場環境加減分"),
        ("sector", sector, "sector_learning", "H82產業加減分"),
        ("decision_status", status, "decision_status_learning", "H82決策狀態加減分"),
        ("h81_bucket", bucket, "h81_bucket_learning", "H82H81分桶加減分"),
    ]
    weights = cfg.get("segment_weights") or {}
    component_values = {}
    confidence_values = []
    sample_values = []
    evidence = []
    weighted_sum = 0.0
    used_weight = 0.0
    for section, key, flag, output_col in feature_map:
        adj, conf, n = _profile_adjustment(state, section, key)
        if not bool(cfg["features"].get(flag, True)):
            adj = 0.0
        component_values[output_col] = round(adj, 4)
        if n > 0:
            evidence.append(f"{section}:{key} n={n} conf={conf:.0%} adj={adj:+.2f}")
            confidence_values.append(conf)
            sample_values.append(n)
        w = float(weights.get(section, 0) or 0)
        if w > 0 and n > 0:
            weighted_sum += adj * w
            used_weight += w
    shadow = weighted_sum / used_weight if used_weight > 0 else 0.0
    guard, guard_reasons = _error_guard(raw, state, cfg)
    shadow += guard
    max_points = float(cfg["max_abs_rank_points"])
    shadow = _clip(shadow, -max_points, max_points)
    confidence = sum(confidence_values) / len(confidence_values) if confidence_values else 0.0
    # Global sample maturity also controls confidence; this is intentionally
    # conservative when the history is only just above the minimum.
    global_conf = min(1.0, effective / max(float(cfg["minimum_global_mature_samples"]), 1.0))
    confidence *= global_conf
    if not active:
        applied = 0.0
    elif cfg.get("mode") == "shadow_only":
        applied = 0.0
    else:
        applied = shadow * confidence
        applied = _clip(applied, -max_points, max_points)
    base_rank = _num(raw.get("H81研究排序分"), _num(raw.get("H79自適應機會分"), None))
    adaptive_rank = _clip((base_rank if base_rank is not None else 50.0) + applied, 0.0, 100.0)
    risks = guard_reasons or (state.get("directives") or [])[:2]
    summary = (
        f"H82成熟樣本={mature_n}、有效權重={effective:.1f}、信心={confidence:.0%}；"
        f"影子調整={shadow:+.2f}、實際研究排序調整={applied:+.2f}。"
        "不改變Formal權威、價格計畫、RR、停損、流動性或資料新鮮度閘門。"
    )
    return {
        "H82版本": VERSION,
        "H82市場環境": regime,
        "H82成熟樣本數": mature_n,
        "H82有效樣本權重": round(effective, 2),
        "H82學習信心%": round(confidence * 100.0, 2),
        **component_values,
        "H82錯誤治理加減分": round(guard, 4),
        "H82影子建議加減分": round(shadow, 4),
        "H82自適應加減分": round(applied, 4),
        "H82自適應研究排序分": round(adaptive_rank, 4),
        "H82主要學習依據": "；".join(evidence[:4]) or "尚無達成熟/信心門檻的分群證據",
        "H82主要錯誤風險": "；".join(risks[:3]) or "目前無額外錯誤風險標籤",
        "H82學習摘要": summary,
        "H82學習狀態": state_label,
        "H82設定版本": _text(cfg.get("version")) or "default",
    }


def apply_adaptive_learning_overlay(frame: pd.DataFrame | None, state: dict[str, Any] | None = None, settings: dict[str, Any] | None = None) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame(frame)
    out = frame.copy(deep=True)
    if out.empty:
        for col in H82_COLUMNS:
            if col not in out.columns:
                out[col] = pd.Series(dtype="object")
        return out
    cfg = _settings(settings)
    state = state if isinstance(state, dict) else load_learning_state(settings=cfg)
    analyses = [analyze_candidate_learning(row, state=state, settings=cfg) for _, row in out.iterrows()]
    adf = pd.DataFrame(analyses, index=out.index)
    for col in adf.columns:
        out[col] = adf[col]
    return out


def build_learning_health_tables(state: dict[str, Any] | None = None) -> dict[str, pd.DataFrame]:
    state = state if isinstance(state, dict) else load_learning_state()
    baseline = state.get("baseline") or {}
    summary = pd.DataFrame([
        {"項目": "版本", "數值": state.get("version", "")},
        {"項目": "狀態", "數值": "可自適應" if state.get("available") else "凍結"},
        {"項目": "成熟樣本", "數值": baseline.get("sample", 0)},
        {"項目": "有效樣本權重", "數值": baseline.get("effective_sample", 0)},
        {"項目": "平均報酬%", "數值": baseline.get("avg_return", 0)},
        {"項目": "勝率", "數值": baseline.get("win_rate", 0)},
        {"項目": "-5%尾部率", "數值": baseline.get("loss5_rate", 0)},
        {"項目": "凍結原因", "數值": "；".join(state.get("freeze_reasons") or []) or "無"},
    ])
    errors = pd.DataFrame([
        {"錯誤/結果": k, **v} for k, v in (state.get("error_summary") or {}).items()
    ])
    seg_rows = []
    for section, groups in (state.get("segments") or {}).items():
        for name, values in groups.items():
            seg_rows.append({"分群": section, "群組": name, **values})
    segments = pd.DataFrame(seg_rows)
    directives = pd.DataFrame({"H82學習指令": state.get("directives") or []})
    return {"summary": summary, "errors": errors, "segments": segments, "directives": directives}

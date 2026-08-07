# -*- coding: utf-8 -*-
"""V177 / Phase108 full-market AI discovery helpers.

The old recommendation page used signal/risk/prelaunch/trade thresholds as
terminal filters before the cross-sectional AI could see the stock.  This
module turns those legacy thresholds into diagnostic features: every stock with
valid K-line analysis remains in the AI discovery mother-pool.  Hard controls
such as stale K-lines, extreme market LOCKDOWN, abnormal prices and low
liquidity are still enforced by the downstream execution-governance engines.

It also provides empirical-Bayes shrinkage for sector breadth/density so a
1-stock or 2-stock category cannot look like a 100% broad market rotation.
"""
from __future__ import annotations

from typing import Any
import math
import pandas as pd

FULL_MARKET_DISCOVERY_VERSION = "v177_full_market_ai_discovery_v1_20260807"
SECTOR_SHRINKAGE_VERSION = "v177_sector_bayesian_shrinkage_v1_20260807"


def _f(value: Any, default: float = 0.0) -> float:
    try:
        x = float(value)
        return x if math.isfinite(x) else float(default)
    except Exception:
        return float(default)


def evaluate_legacy_soft_gates(
    *,
    signal_score: float,
    min_signal_score: float,
    risk_pass: bool,
    risk_reason: str = "",
    prelaunch_score: float,
    min_prelaunch_score: float,
    trade_score: float,
    min_trade_score: float,
    opportunity_mode: bool = False,
    opportunity_score: float = 0.0,
    opportunity_core: float = 0.0,
    opportunity_chase: float = 50.0,
    rescue_eligible: bool = False,
) -> dict[str, Any]:
    """Reproduce legacy gates without deleting a K-line-valid stock.

    Returns which old gates *would* have removed the stock.  Opportunity-mode
    relaxation and existing momentum/pre-breakout rescue are kept exactly as
    bypass logic, but any remaining old-gate failure becomes a SOFT-HOLD flag.
    """
    soft_statuses: list[str] = []
    soft_stages: list[str] = []
    soft_reasons: list[str] = []
    rescued_stages: list[str] = []

    signal_score = _f(signal_score)
    min_signal_score = _f(min_signal_score)
    prelaunch_score = _f(prelaunch_score)
    min_prelaunch_score = _f(min_prelaunch_score)
    trade_score = _f(trade_score)
    min_trade_score = _f(min_trade_score)
    opportunity_score = _f(opportunity_score)
    opportunity_core = _f(opportunity_core)
    opportunity_chase = _f(opportunity_chase, 50)

    if signal_score < min_signal_score:
        relaxed = max(0.0, min_signal_score - (35.0 if opportunity_mode else 0.0))
        bypass = opportunity_mode and signal_score >= relaxed and opportunity_core >= 60
        if not bypass:
            if rescue_eligible:
                rescued_stages.append("訊號")
            else:
                soft_statuses.append("signal_filtered")
                soft_stages.append("訊號")
                soft_reasons.append(f"舊訊號門檻 {signal_score:.1f} < {min_signal_score:.1f}")

    if not bool(risk_pass):
        bypass = opportunity_mode and opportunity_score >= 62 and opportunity_chase <= 72
        if not bypass:
            if rescue_eligible:
                rescued_stages.append("傳統風控")
            else:
                soft_statuses.append("risk_filtered")
                soft_stages.append("傳統風控")
                soft_reasons.append((risk_reason or "舊傳統風控未通過").strip())

    if prelaunch_score < min_prelaunch_score:
        relaxed = max(0.0, min_prelaunch_score - (35.0 if opportunity_mode else 0.0))
        bypass = opportunity_mode and (opportunity_score >= 62 or opportunity_core >= 66) and prelaunch_score >= relaxed
        if not bypass:
            if rescue_eligible:
                rescued_stages.append("起漲前兆")
            else:
                soft_statuses.append("prelaunch_filtered")
                soft_stages.append("起漲前兆")
                soft_reasons.append(f"舊起漲門檻 {prelaunch_score:.1f} < {min_prelaunch_score:.1f}")

    if trade_score < min_trade_score:
        relaxed = max(0.0, min_trade_score - (25.0 if opportunity_mode else 0.0))
        bypass = opportunity_mode and opportunity_score >= 60 and trade_score >= relaxed
        if not bypass:
            if rescue_eligible:
                rescued_stages.append("交易可行")
            else:
                soft_statuses.append("trade_filtered")
                soft_stages.append("交易可行")
                soft_reasons.append(f"舊交易可行門檻 {trade_score:.1f} < {min_trade_score:.1f}")

    return {
        "soft_statuses": soft_statuses,
        "soft_stages": soft_stages,
        "soft_reasons": soft_reasons,
        "rescued_stages": rescued_stages,
        "soft_count": len(soft_statuses),
        "soft_state": "SOFT-HOLD｜舊規則僅作AI特徵" if soft_statuses else "PASS｜舊規則未攔截",
    }


def bayesian_shrink_pct(raw_pct: Any, sample_n: Any, global_pct: Any, *, prior_n: float = 6.0) -> float:
    raw = min(100.0, max(0.0, _f(raw_pct))) / 100.0
    prior = min(100.0, max(0.0, _f(global_pct, 50.0))) / 100.0
    n = max(0.0, _f(sample_n))
    k = max(0.1, _f(prior_n, 6.0))
    return round(((raw * n + prior * k) / (n + k)) * 100.0, 6)


def bayesian_shrink_value(raw_value: Any, sample_n: Any, global_value: Any, *, prior_n: float = 4.0) -> float:
    raw = _f(raw_value)
    prior = _f(global_value, 50.0)
    n = max(0.0, _f(sample_n))
    k = max(0.1, _f(prior_n, 4.0))
    return round((raw * n + prior * k) / (n + k), 6)


def sector_sample_confidence_pct(sample_n: Any, *, prior_n: float = 5.0) -> float:
    n = max(0.0, _f(sample_n))
    k = max(0.1, _f(prior_n, 5.0))
    return round(100.0 * n / (n + k), 6)


def apply_sector_bayesian_shrinkage(
    grouped: pd.DataFrame,
    *,
    global_strong_pct: float,
    global_candidate_pct: float,
    global_volume_score: float,
) -> pd.DataFrame:
    """Shrink unstable sector breadth/density/volume toward market priors."""
    if grouped is None or grouped.empty:
        return pd.DataFrame() if grouped is None else grouped.copy()
    out = grouped.copy()
    n = pd.to_numeric(out.get("股票數", 0), errors="coerce").fillna(0).clip(lower=0)
    raw_strong = pd.to_numeric(out.get("同族群強勢比例", 0), errors="coerce").fillna(0).clip(0, 100)
    raw_density = pd.to_numeric(out.get("同族群推薦密度", 0), errors="coerce").fillna(0).clip(0, 100)
    raw_volume = pd.to_numeric(out.get("同族群平均量能分", global_volume_score), errors="coerce").fillna(global_volume_score).clip(0, 100)

    out["同族群強勢比例_原始"] = raw_strong.round(2)
    out["同族群推薦密度_原始"] = raw_density.round(2)
    out["同族群平均量能分_原始"] = raw_volume.round(2)
    out["同族群強勢比例"] = [bayesian_shrink_pct(v, k, global_strong_pct, prior_n=6.0) for v, k in zip(raw_strong, n)]
    out["同族群推薦密度"] = [bayesian_shrink_pct(v, k, global_candidate_pct, prior_n=6.0) for v, k in zip(raw_density, n)]
    out["同族群平均量能分"] = [bayesian_shrink_value(v, k, global_volume_score, prior_n=4.0) for v, k in zip(raw_volume, n)]
    out["族群樣本可信度"] = [sector_sample_confidence_pct(k, prior_n=5.0) for k in n]
    out["族群樣本校正說明"] = [
        f"樣本{int(k)}檔｜可信度{sector_sample_confidence_pct(k):.1f}%｜強勢比例{raw:.1f}%→{adj:.1f}%｜推薦密度{rd:.1f}%→{ad:.1f}%"
        for k, raw, adj, rd, ad in zip(n, raw_strong, out["同族群強勢比例"], raw_density, out["同族群推薦密度"])
    ]
    out["族群樣本校正版本"] = SECTOR_SHRINKAGE_VERSION
    return out


__all__ = [
    "FULL_MARKET_DISCOVERY_VERSION", "SECTOR_SHRINKAGE_VERSION",
    "evaluate_legacy_soft_gates", "bayesian_shrink_pct", "bayesian_shrink_value",
    "sector_sample_confidence_pct", "apply_sector_bayesian_shrinkage",
]

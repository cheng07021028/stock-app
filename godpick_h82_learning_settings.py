# -*- coding: utf-8 -*-
"""H82 Adaptive GodPick Learning Core - durable settings.

The adaptive learner is deliberately bounded.  Its output may refine research
ranking only.  It must never create H64/H68 formal authority, bypass execution
price/RR gates, or promote a research sample into formal performance.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

VERSION = "v191_h82_adaptive_learning_settings_20260921"
SETTINGS_FILE = "godpick_adaptive_learning_settings.json"
FIRESTORE_DOC = "godpick_adaptive_learning_settings"

DEFAULT_SETTINGS: dict[str, Any] = {
    "version": VERSION,
    "enabled": True,
    "mode": "active_bounded",  # active_bounded | shadow_only | frozen
    "updated_at": "",
    "update_seq": 0,
    "primary_horizon": 5,
    "minimum_global_mature_samples": 30,
    "minimum_segment_mature_samples": 12,
    "minimum_effective_segment_samples": 6.0,
    "half_life_days": 90,
    "winsorize_return_pct": 20.0,
    "max_abs_rank_points": 2.0,
    "max_component_abs_points": 0.9,
    "shrinkage_prior_samples": 20.0,
    "confidence_floor": 0.55,
    "max_suspicious_proxy_ratio": 0.10,
    "segment_weights": {
        "market_regime": 40,
        "sector": 25,
        "decision_status": 15,
        "h81_bucket": 20,
    },
    "features": {
        "market_regime_learning": True,
        "sector_learning": True,
        "decision_status_learning": True,
        "h81_bucket_learning": True,
        "error_taxonomy_guard": True,
        "time_decay": True,
        "auto_refresh_from_records": True,
        "auto_persist_learning_state": True,
        "freeze_on_data_quality": True,
    },
    "error_thresholds": {
        "success_return_pct": 3.0,
        "direction_error_return_pct": -3.0,
        "missed_opportunity_return_pct": 8.0,
        "entry_timing_recovery_pct": 3.0,
        "overheat_risk_score": 75.0,
        "low_rr": 1.5,
        "tight_stop_pct": 2.5,
    },
    "governance": {
        "formal_permission_immutable": True,
        "research_and_formal_performance_separated": True,
        "never_lower_formal_threshold_to_fill_quota": True,
        "require_mature_samples_before_learning": True,
        "description": "H82只做成熟樣本的研究排序自適應；不得建立/替代H64/H68 Formal權威。",
    },
}


def _float(value: Any, low: float, high: float, default: float) -> float:
    try:
        x = float(value)
    except Exception:
        x = default
    if x != x or x in (float("inf"), float("-inf")):
        x = default
    return max(low, min(high, x))


def _int(value: Any, low: int, high: int, default: int) -> int:
    try:
        x = int(value)
    except Exception:
        x = default
    return max(low, min(high, x))


def _normalise_weights(raw: Any) -> dict[str, int]:
    base = deepcopy(DEFAULT_SETTINGS["segment_weights"])
    if isinstance(raw, dict):
        for key in base:
            base[key] = _int(raw.get(key, base[key]), 0, 100, base[key])
    total = sum(base.values())
    if total <= 0:
        return deepcopy(DEFAULT_SETTINGS["segment_weights"])
    scaled = {k: int(round(v * 100.0 / total)) for k, v in base.items()}
    delta = 100 - sum(scaled.values())
    if delta:
        scaled[max(scaled, key=scaled.get)] += delta
    return scaled


def normalize_settings(payload: Any) -> dict[str, Any]:
    out = deepcopy(DEFAULT_SETTINGS)
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key not in {"segment_weights", "features", "error_thresholds", "governance"}:
                out[key] = value
        for section in ["features", "error_thresholds", "governance"]:
            if isinstance(payload.get(section), dict):
                out[section].update(payload[section])
        if isinstance(payload.get("segment_weights"), dict):
            out["segment_weights"] = payload["segment_weights"]

    out["enabled"] = bool(out.get("enabled", True))
    mode = str(out.get("mode") or "active_bounded").strip()
    out["mode"] = mode if mode in {"active_bounded", "shadow_only", "frozen"} else "active_bounded"
    horizon = _int(out.get("primary_horizon"), 1, 20, 5)
    out["primary_horizon"] = horizon if horizon in {1, 3, 5, 10, 20} else 5
    out["minimum_global_mature_samples"] = _int(out.get("minimum_global_mature_samples"), 8, 5000, 30)
    out["minimum_segment_mature_samples"] = _int(out.get("minimum_segment_mature_samples"), 5, 1000, 12)
    out["minimum_effective_segment_samples"] = _float(out.get("minimum_effective_segment_samples"), 2.0, 1000.0, 6.0)
    out["half_life_days"] = _int(out.get("half_life_days"), 20, 730, 90)
    out["winsorize_return_pct"] = _float(out.get("winsorize_return_pct"), 5.0, 100.0, 20.0)
    out["max_abs_rank_points"] = _float(out.get("max_abs_rank_points"), 0.0, 3.0, 2.0)
    out["max_component_abs_points"] = _float(out.get("max_component_abs_points"), 0.0, 1.5, 0.9)
    out["shrinkage_prior_samples"] = _float(out.get("shrinkage_prior_samples"), 5.0, 200.0, 20.0)
    out["confidence_floor"] = _float(out.get("confidence_floor"), 0.30, 0.90, 0.55)
    out["max_suspicious_proxy_ratio"] = _float(out.get("max_suspicious_proxy_ratio"), 0.0, 0.50, 0.10)
    out["segment_weights"] = _normalise_weights(out.get("segment_weights"))

    features = out["features"]
    for key in DEFAULT_SETTINGS["features"]:
        features[key] = bool(features.get(key, DEFAULT_SETTINGS["features"][key]))

    et = out["error_thresholds"]
    et["success_return_pct"] = _float(et.get("success_return_pct"), 0.5, 20.0, 3.0)
    et["direction_error_return_pct"] = -abs(_float(et.get("direction_error_return_pct"), -20.0, -0.5, -3.0))
    et["missed_opportunity_return_pct"] = _float(et.get("missed_opportunity_return_pct"), 3.0, 30.0, 8.0)
    et["entry_timing_recovery_pct"] = _float(et.get("entry_timing_recovery_pct"), 1.0, 20.0, 3.0)
    et["overheat_risk_score"] = _float(et.get("overheat_risk_score"), 55.0, 95.0, 75.0)
    et["low_rr"] = _float(et.get("low_rr"), 0.8, 3.0, 1.5)
    et["tight_stop_pct"] = _float(et.get("tight_stop_pct"), 0.5, 6.0, 2.5)

    # Governance is not user-overridable in ways that could weaken formal safety.
    out["governance"] = deepcopy(DEFAULT_SETTINGS["governance"])
    out["version"] = VERSION
    try:
        out["update_seq"] = max(0, int(out.get("update_seq", 0) or 0))
    except Exception:
        out["update_seq"] = 0
    return out


def load_settings() -> tuple[dict[str, Any], list[str]]:
    try:
        from godpick_persistence_service import load_named_json_permanent
        payload, details = load_named_json_permanent(
            SETTINGS_FILE,
            DEFAULT_SETTINGS,
            firestore_doc=FIRESTORE_DOC,
        )
        return normalize_settings(payload), list(details or [])
    except Exception as exc:
        return normalize_settings(DEFAULT_SETTINGS), [f"H82設定讀取使用預設值：{type(exc).__name__}: {exc}"]


def load_settings_safe() -> dict[str, Any]:
    return load_settings()[0]


def save_settings(settings: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
    current, _ = load_settings()
    candidate = normalize_settings(settings)
    candidate["update_seq"] = int(current.get("update_seq", 0) or 0) + 1
    candidate["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        from godpick_persistence_service import save_named_json_permanent
        report = save_named_json_permanent(
            SETTINGS_FILE,
            candidate,
            firestore_doc=FIRESTORE_DOC,
        )
        ok = bool(getattr(report, "permanent_ok", False))
        detail = "｜".join(x for x in [
            getattr(report, "local_message", ""),
            getattr(report, "github_message", ""),
            getattr(report, "firestore_message", ""),
        ] if x)
        if not ok:
            return False, f"H82設定未取得永久權威確認。{detail}", candidate
        verify, _ = load_settings()
        if int(verify.get("update_seq", 0) or 0) < int(candidate["update_seq"]):
            return False, "H82設定寫入後回讀版本未更新，禁止宣稱永久保存成功。", candidate
        return True, f"H82 Adaptive Learning設定已永久保存（seq={candidate['update_seq']}）。{detail}", candidate
    except Exception as exc:
        return False, f"H82設定保存失敗：{type(exc).__name__}: {exc}", candidate

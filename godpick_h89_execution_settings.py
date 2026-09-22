# -*- coding: utf-8 -*-
"""H89 Selection/Execution Intelligence durable settings."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any
import json

VERSION = "v191_h89_selection_execution_settings_20260922"
SETTINGS_FILE = "godpick_h89_selection_execution_settings.json"
FIRESTORE_DOC = "godpick_h89_selection_execution_settings"
BASE_DIR = Path(__file__).resolve().parent

DEFAULT_SETTINGS: dict[str, Any] = {
    "version": VERSION,
    "enabled": True,
    "updated_at": "",
    "update_seq": 0,
    "trade_plan": {
        "formal_min_net_rr": 1.50,
        "research_min_net_rr": 1.20,
        "max_stop_distance_pct": 8.0,
        "max_pullback_from_reference_pct": 12.0,
        "max_model_target_upside_pct": 15.0,
        "min_entry_zone_width_pct": 0.35,
        "max_entry_zone_width_pct": 1.50,
        "allow_research_model_target": True,
        "never_move_stop_to_force_rr": True,
        "formal_requires_structural_target": True,
    },
    "learning": {
        "enabled": True,
        "primary_horizon": 5,
        "minimum_clean_samples": 30,
        "minimum_segment_samples": 12,
        "selection_success_return_pct": 2.0,
        "selection_failure_return_pct": -2.0,
        "opportunity_cost_return_pct": 4.0,
        "max_rank_adjustment_points": 1.5,
        "max_entry_bias_pct": 1.5,
        "exclude_suspicious_proxy_instead_of_freezing_all": True,
    },
    "news": {
        "exact_entity_relevance": 1.00,
        "category_relevance": 0.55,
        "minimum_stock_score_relevance": 0.75,
        "generic_market_news_affects_individual_stock": False,
    },
    "governance": {
        "h89_can_create_formal_authority": False,
        "research_plan_is_not_buy_permission": True,
        "model_target_never_grants_formal": True,
        "selection_and_execution_performance_are_separate": True,
    },
}


def _f(v: Any, lo: float, hi: float, default: float) -> float:
    try:
        x = float(v)
    except Exception:
        x = default
    if x != x or x in (float("inf"), float("-inf")):
        x = default
    return max(lo, min(hi, x))


def _i(v: Any, lo: int, hi: int, default: int) -> int:
    try:
        x = int(v)
    except Exception:
        x = default
    return max(lo, min(hi, x))


def normalize_settings(payload: Any) -> dict[str, Any]:
    out = deepcopy(DEFAULT_SETTINGS)
    if isinstance(payload, dict):
        for k, v in payload.items():
            if k not in {"trade_plan", "learning", "news", "governance"}:
                out[k] = v
        for sec in ["trade_plan", "learning", "news", "governance"]:
            if isinstance(payload.get(sec), dict):
                out[sec].update(payload[sec])

    t = out["trade_plan"]
    t["formal_min_net_rr"] = _f(t.get("formal_min_net_rr"), 1.0, 4.0, 1.50)
    t["research_min_net_rr"] = _f(t.get("research_min_net_rr"), 0.8, 3.0, 1.20)
    t["max_stop_distance_pct"] = _f(t.get("max_stop_distance_pct"), 2.0, 20.0, 8.0)
    t["max_pullback_from_reference_pct"] = _f(t.get("max_pullback_from_reference_pct"), 2.0, 25.0, 12.0)
    t["max_model_target_upside_pct"] = _f(t.get("max_model_target_upside_pct"), 3.0, 40.0, 15.0)
    t["min_entry_zone_width_pct"] = _f(t.get("min_entry_zone_width_pct"), 0.1, 3.0, 0.35)
    t["max_entry_zone_width_pct"] = _f(t.get("max_entry_zone_width_pct"), 0.2, 5.0, 1.50)
    for k in ["allow_research_model_target", "never_move_stop_to_force_rr", "formal_requires_structural_target"]:
        t[k] = bool(t.get(k, DEFAULT_SETTINGS["trade_plan"][k]))

    l = out["learning"]
    l["enabled"] = bool(l.get("enabled", True))
    l["primary_horizon"] = _i(l.get("primary_horizon"), 1, 20, 5)
    if l["primary_horizon"] not in {1, 3, 5, 10, 20}:
        l["primary_horizon"] = 5
    l["minimum_clean_samples"] = _i(l.get("minimum_clean_samples"), 10, 1000, 30)
    l["minimum_segment_samples"] = _i(l.get("minimum_segment_samples"), 5, 500, 12)
    l["selection_success_return_pct"] = _f(l.get("selection_success_return_pct"), 0.0, 20.0, 2.0)
    l["selection_failure_return_pct"] = _f(l.get("selection_failure_return_pct"), -20.0, 0.0, -2.0)
    l["opportunity_cost_return_pct"] = _f(l.get("opportunity_cost_return_pct"), 1.0, 30.0, 4.0)
    l["max_rank_adjustment_points"] = _f(l.get("max_rank_adjustment_points"), 0.0, 3.0, 1.5)
    l["max_entry_bias_pct"] = _f(l.get("max_entry_bias_pct"), 0.0, 3.0, 1.5)
    l["exclude_suspicious_proxy_instead_of_freezing_all"] = bool(l.get("exclude_suspicious_proxy_instead_of_freezing_all", True))

    n = out["news"]
    n["exact_entity_relevance"] = _f(n.get("exact_entity_relevance"), 0.0, 1.0, 1.0)
    n["category_relevance"] = _f(n.get("category_relevance"), 0.0, 1.0, 0.55)
    n["minimum_stock_score_relevance"] = _f(n.get("minimum_stock_score_relevance"), 0.0, 1.0, 0.75)
    n["generic_market_news_affects_individual_stock"] = bool(n.get("generic_market_news_affects_individual_stock", False))

    g = out["governance"]
    # hard governance: user settings cannot make H89 a Formal authority.
    g["h89_can_create_formal_authority"] = False
    g["research_plan_is_not_buy_permission"] = True
    g["model_target_never_grants_formal"] = True
    g["selection_and_execution_performance_are_separate"] = True

    out["enabled"] = bool(out.get("enabled", True))
    out["version"] = VERSION
    try:
        out["update_seq"] = max(0, int(out.get("update_seq", 0) or 0))
    except Exception:
        out["update_seq"] = 0
    return out


def _local_read() -> dict[str, Any]:
    try:
        p = BASE_DIR / SETTINGS_FILE
        if p.exists():
            raw = json.loads(p.read_text(encoding="utf-8-sig"))
            if isinstance(raw, dict):
                return raw
    except Exception:
        pass
    return deepcopy(DEFAULT_SETTINGS)


def load_settings() -> tuple[dict[str, Any], list[str]]:
    try:
        from godpick_persistence_service import load_named_json_permanent
        payload, details = load_named_json_permanent(SETTINGS_FILE, DEFAULT_SETTINGS, firestore_doc=FIRESTORE_DOC)
        return normalize_settings(payload), list(details or [])
    except Exception as exc:
        return normalize_settings(_local_read()), [f"H89設定使用本機/預設值：{type(exc).__name__}: {exc}"]


def load_settings_safe() -> dict[str, Any]:
    # Page07 hot path must never wait for GitHub/Firestore.
    # The H83/H87 durability layer keeps the local authority refreshed;
    # management pages may explicitly call load_settings() for remote reconciliation.
    return normalize_settings(_local_read())


def save_settings(settings: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
    current, _ = load_settings()
    candidate = normalize_settings(settings)
    candidate["update_seq"] = int(current.get("update_seq", 0) or 0) + 1
    candidate["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        from godpick_persistence_service import save_named_json_permanent
        report = save_named_json_permanent(SETTINGS_FILE, candidate, firestore_doc=FIRESTORE_DOC)
        ok = bool(getattr(report, "permanent_ok", False))
        if ok:
            verify, _ = load_settings()
            if int(verify.get("update_seq", 0) or 0) < int(candidate["update_seq"]):
                return False, "H89設定回讀版本未更新，禁止宣稱永久保存成功。", candidate
            return True, f"H89設定已永久保存（seq={candidate['update_seq']}）。", candidate
        return False, "H89設定本機已寫入但永久權威尚未確認。", candidate
    except Exception as exc:
        try:
            (BASE_DIR / SETTINGS_FILE).write_text(json.dumps(candidate, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        return False, f"H89永久保存例外，已保留本機：{type(exc).__name__}: {exc}", candidate

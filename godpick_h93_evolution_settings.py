# -*- coding: utf-8 -*-
"""V191-H93 Seven-Direction GodPick Evolution settings.

The seven dimensions are research/advisory overlays only. They may change
research ordering and diagnostics, but must never create Formal authority or
weaken H64/H68/freshness/liquidity/RR/stop governance.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any
import json

VERSION = "v191_h93_seven_direction_evolution_settings_20260923"
SETTINGS_FILE = "godpick_h93_evolution_settings.json"
BASE_DIR = Path(__file__).resolve().parent

DEFAULT_SETTINGS: dict[str, Any] = {
    "version": VERSION,
    "enabled": True,
    "formal_authority_lock": True,
    "weights": {
        "deep_research": 20,
        "learning_plan": 15,
        "advisor_mode": 15,
        "system_optimization": 10,
        "growth_opportunity": 20,
        "continuous_improvement": 10,
        "efficiency": 10,
    },
    "research_overlay": {
        "enabled": True,
        "max_abs_points": 4.0,
        "min_data_coverage_pct": 45.0,
        "priority_thresholds": {"A": 75.0, "B": 65.0, "C": 55.0},
    },
    "learning": {
        "minimum_mature_samples": 12,
        "high_confidence_samples": 40,
        "max_history_rows": 2500,
    },
    "efficiency": {
        "max_candidate_rows_for_live_overlay": 600,
        "use_local_state_only_during_render": True,
        "cache_ttl_minutes": 360,
    },
    "updated_at": "",
}


def _text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def normalize_settings(raw: dict[str, Any] | None) -> dict[str, Any]:
    cfg = deepcopy(DEFAULT_SETTINGS)
    if isinstance(raw, dict):
        for key in ["enabled", "updated_at"]:
            if key in raw:
                cfg[key] = raw[key]
        for section in ["weights", "research_overlay", "learning", "efficiency"]:
            if isinstance(raw.get(section), dict):
                cfg[section].update(raw[section])
    # Immutable governance lock.
    cfg["formal_authority_lock"] = True

    weights = cfg["weights"]
    clean: dict[str, float] = {}
    for key in DEFAULT_SETTINGS["weights"]:
        try:
            clean[key] = max(0.0, float(weights.get(key, 0) or 0))
        except Exception:
            clean[key] = float(DEFAULT_SETTINGS["weights"][key])
    total = sum(clean.values()) or 1.0
    cfg["weights"] = {k: round(v / total * 100.0, 6) for k, v in clean.items()}

    ro = cfg["research_overlay"]
    try: ro["max_abs_points"] = max(0.0, min(6.0, float(ro.get("max_abs_points", 4.0) or 4.0)))
    except Exception: ro["max_abs_points"] = 4.0
    try: ro["min_data_coverage_pct"] = max(0.0, min(100.0, float(ro.get("min_data_coverage_pct", 45.0) or 45.0)))
    except Exception: ro["min_data_coverage_pct"] = 45.0
    th = ro.get("priority_thresholds") if isinstance(ro.get("priority_thresholds"), dict) else {}
    vals = {}
    for name, default in [("A",75.0),("B",65.0),("C",55.0)]:
        try: vals[name] = float(th.get(name, default))
        except Exception: vals[name] = default
    vals["A"] = max(vals["A"], vals["B"] + 1.0)
    vals["B"] = max(vals["B"], vals["C"] + 1.0)
    ro["priority_thresholds"] = vals

    for key, lo, hi, default in [
        ("minimum_mature_samples",1,500,12),
        ("high_confidence_samples",5,1000,40),
        ("max_history_rows",100,10000,2500),
    ]:
        try: cfg["learning"][key] = max(lo, min(hi, int(cfg["learning"].get(key, default) or default)))
        except Exception: cfg["learning"][key] = default
    try:
        cfg["efficiency"]["max_candidate_rows_for_live_overlay"] = max(50, min(2000, int(cfg["efficiency"].get("max_candidate_rows_for_live_overlay",600) or 600)))
    except Exception:
        cfg["efficiency"]["max_candidate_rows_for_live_overlay"] = 600
    try:
        cfg["efficiency"]["cache_ttl_minutes"] = max(30, min(1440, int(cfg["efficiency"].get("cache_ttl_minutes",360) or 360)))
    except Exception:
        cfg["efficiency"]["cache_ttl_minutes"] = 360
    cfg["version"] = VERSION
    return cfg


def load_settings_safe() -> dict[str, Any]:
    path = BASE_DIR / SETTINGS_FILE
    payload: dict[str, Any] = {}
    try:
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8-sig"))
            if isinstance(raw, dict):
                payload = raw
    except Exception:
        payload = {}
    return normalize_settings(payload)


def save_settings_safe(settings: dict[str, Any]) -> tuple[bool, str]:
    payload = normalize_settings(settings)
    payload["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        from godpick_persistence_service import save_named_json_permanent
        report = save_named_json_permanent(SETTINGS_FILE, payload, firestore_doc="godpick_h93_evolution_settings")
        ok = bool(getattr(report, "permanent_ok", False))
        msgs = report.messages() if hasattr(report, "messages") else []
        return ok, "｜".join(str(x) for x in msgs if x)
    except Exception:
        try:
            path = BASE_DIR / SETTINGS_FILE
            tmp = path.with_suffix(".json.tmp_h93")
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)
            return True, "H93設定已保存本機；遠端永久化服務不可用。"
        except Exception as exc:
            return False, f"H93設定保存失敗：{type(exc).__name__}: {exc}"

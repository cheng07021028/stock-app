# -*- coding: utf-8 -*-
"""V191-H94 Research Selection Intelligence settings.

H94 governs research prioritisation only. Formal/A-/R1 authority remains owned
by the existing H64/H68 + freshness/liquidity/RR/stop governance chain.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any
import json

VERSION = "v191_h94_research_selection_settings_20260923"
SETTINGS_FILE = "godpick_h94_research_selection_settings.json"
BASE_DIR = Path(__file__).resolve().parent

DEFAULT_SETTINGS: dict[str, Any] = {
    "version": VERSION,
    "enabled": True,
    "formal_authority_lock": True,
    "quality": {
        "min_research_score": 57.0,
        "a_score": 72.0,
        "b_score": 64.0,
        "max_consensus_risk_for_research": 2,
        "hard_risk_floor": 28.0,
        "hard_execution_floor": 25.0,
        "contrarian_leader_min_score": 52.0,
    },
    "anti_exhaustion": {
        "enabled": True,
        "risk_score_threshold": 45.0,
        "execution_score_threshold": 50.0,
        "penalty_points": 8.0,
        "chase_risk_threshold": 75.0,
    },
    "consensus": {
        "risk_below_40": True,
        "h82_negative_threshold": -0.40,
        "execution_below": 40.0,
        "backtest_below": 40.0,
        "sector_bad_counts": True,
        "overheat_counts": True,
    },
    "portfolio": {
        "max_research_rows": 8,
        "max_per_category": 2,
        "max_per_broad_theme": 4,
        "allow_waiting_promotion": True,
    },
    "weights": {
        "h93": 15.0,
        "h79": 15.0,
        "h81": 15.0,
        "risk": 22.0,
        "h89_selection": 13.0,
        "h89_execution": 15.0,
        "sector": 5.0,
    },
    "news": {
        "missing_is_neutral_evidence": False,
        "missing_confidence_penalty": 1.0,
    },
    "updated_at": "",
}


def normalize_settings(raw: dict[str, Any] | None) -> dict[str, Any]:
    cfg = deepcopy(DEFAULT_SETTINGS)
    if isinstance(raw, dict):
        for key in ["enabled", "updated_at"]:
            if key in raw:
                cfg[key] = raw[key]
        for sec in ["quality", "anti_exhaustion", "consensus", "portfolio", "weights", "news"]:
            if isinstance(raw.get(sec), dict):
                cfg[sec].update(raw[sec])
    cfg["formal_authority_lock"] = True

    for key, lo, hi, default in [
        ("min_research_score", 45, 80, 57), ("a_score", 60, 95, 72), ("b_score", 50, 90, 64),
        ("hard_risk_floor", 0, 60, 28), ("hard_execution_floor", 0, 60, 25),
        ("contrarian_leader_min_score", 45, 75, 52),
    ]:
        try: cfg["quality"][key] = max(lo, min(hi, float(cfg["quality"].get(key, default))))
        except Exception: cfg["quality"][key] = default
    cfg["quality"]["a_score"] = max(cfg["quality"]["a_score"], cfg["quality"]["b_score"] + 1)
    try: cfg["quality"]["max_consensus_risk_for_research"] = max(0, min(6, int(cfg["quality"].get("max_consensus_risk_for_research", 2))))
    except Exception: cfg["quality"]["max_consensus_risk_for_research"] = 2

    for key, lo, hi, default in [
        ("risk_score_threshold", 20, 70, 45), ("execution_score_threshold", 20, 70, 50),
        ("penalty_points", 0, 20, 8), ("chase_risk_threshold", 50, 100, 75),
    ]:
        try: cfg["anti_exhaustion"][key] = max(lo, min(hi, float(cfg["anti_exhaustion"].get(key, default))))
        except Exception: cfg["anti_exhaustion"][key] = default

    try: cfg["consensus"]["h82_negative_threshold"] = max(-3.0, min(0.0, float(cfg["consensus"].get("h82_negative_threshold", -.4))))
    except Exception: cfg["consensus"]["h82_negative_threshold"] = -.4
    for key, default in [("execution_below",40.0),("backtest_below",40.0)]:
        try: cfg["consensus"][key] = max(0.0, min(100.0, float(cfg["consensus"].get(key, default))))
        except Exception: cfg["consensus"][key] = default

    for key, lo, hi, default in [
        ("max_research_rows", 1, 20, 8), ("max_per_category", 1, 5, 2), ("max_per_broad_theme", 1, 10, 4),
    ]:
        try: cfg["portfolio"][key] = max(lo, min(hi, int(cfg["portfolio"].get(key, default))))
        except Exception: cfg["portfolio"][key] = default

    clean = {}
    for key, default in DEFAULT_SETTINGS["weights"].items():
        try: clean[key] = max(0.0, float(cfg["weights"].get(key, default)))
        except Exception: clean[key] = float(default)
    total = sum(clean.values()) or 1.0
    cfg["weights"] = {k: v / total * 100.0 for k, v in clean.items()}
    cfg["version"] = VERSION
    return cfg


def load_settings_safe() -> dict[str, Any]:
    path = BASE_DIR / SETTINGS_FILE
    raw = {}
    try:
        if path.exists():
            obj = json.loads(path.read_text(encoding="utf-8-sig"))
            if isinstance(obj, dict): raw = obj
    except Exception:
        raw = {}
    return normalize_settings(raw)


def save_settings_safe(settings: dict[str, Any]) -> tuple[bool, str]:
    payload = normalize_settings(settings)
    payload["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        from godpick_persistence_service import save_named_json_permanent
        report = save_named_json_permanent(SETTINGS_FILE, payload, firestore_doc="godpick_h94_research_selection_settings")
        ok = bool(getattr(report, "permanent_ok", False))
        msgs = report.messages() if hasattr(report, "messages") else []
        return ok, "｜".join(str(x) for x in msgs if x)
    except Exception:
        try:
            tmp = (BASE_DIR / SETTINGS_FILE).with_suffix(".json.tmp_h94")
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(BASE_DIR / SETTINGS_FILE)
            return True, "H94設定已保存本機；遠端永久化服務不可用。"
        except Exception as exc:
            return False, f"H94設定保存失敗：{type(exc).__name__}: {exc}"

# -*- coding: utf-8 -*-
"""V191-H96 high-conviction research discovery settings.

H96 governs only the *reference value* of Research vs Waiting.  It never creates
Formal/A-/R1 authority and never relaxes freshness/liquidity/RR/stop gates.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any
import json

VERSION = "v191_h96_high_conviction_settings_20260923"
SETTINGS_FILE = "godpick_h96_high_conviction_settings.json"
BASE_DIR = Path(__file__).resolve().parent

DEFAULT_SETTINGS: dict[str, Any] = {
    "version": VERSION,
    "enabled": True,
    "formal_authority_lock": True,
    "discovery": {
        "audit_pool_limit": 120,
        "max_core_rows": 5,
        "max_per_category": 2,
        "max_per_broad_theme": 3,
        "core_score_min": 64.0,
        "core_strength_pct_min": 60.0,
        "core_sector_pct_min": 65.0,
        "core_selection_min": 58.0,
        "core_execution_min": 55.0,
        "core_risk_min": 55.0,
        "leader_strength_pct_min": 78.0,
        "leader_sector_pct_min": 78.0,
        "leader_selection_min": 57.0,
        "leader_execution_min": 60.0,
        "leader_risk_min": 40.0,
        "leader_score_min": 65.0,
        "allow_bad_sector_only_for_leader": True,
    },
    "weights": {
        "strength_pct": 25.0,
        "sector_pct": 20.0,
        "h89_selection": 20.0,
        "h81_technical": 10.0,
        "h81_market_pricing": 8.0,
        "h81_risk": 8.0,
        "h89_execution": 6.0,
        "h93_growth": 3.0,
    },
    "penalties": {
        "sector_bad": 5.0,
        "h82_mild_negative": 2.0,
        "h82_strong_negative": 4.0,
        "overheat": 5.0,
        "news_missing": 1.0,
        "selection_below_55": 4.0,
        "strength_below_50": 5.0,
    },
    "bonuses": {
        "leader_exception": 5.0,
        "sector_good": 3.0,
        "risk_execution_alignment": 2.0,
        "formal_price_plan": 1.0,
    },
    "updated_at": "",
}


def _f(v: Any, default: float, lo: float, hi: float) -> float:
    try:
        x = float(v)
    except Exception:
        x = default
    return max(lo, min(hi, x))


def _i(v: Any, default: int, lo: int, hi: int) -> int:
    try:
        x = int(v)
    except Exception:
        x = default
    return max(lo, min(hi, x))


def normalize_settings(raw: dict[str, Any] | None) -> dict[str, Any]:
    cfg = deepcopy(DEFAULT_SETTINGS)
    if isinstance(raw, dict):
        for key in ["enabled", "updated_at"]:
            if key in raw:
                cfg[key] = raw[key]
        for sec in ["discovery", "weights", "penalties", "bonuses"]:
            if isinstance(raw.get(sec), dict):
                cfg[sec].update(raw[sec])
    cfg["formal_authority_lock"] = True

    d = cfg["discovery"]
    d["audit_pool_limit"] = _i(d.get("audit_pool_limit"), 120, 30, 500)
    d["max_core_rows"] = _i(d.get("max_core_rows"), 5, 1, 10)
    d["max_per_category"] = _i(d.get("max_per_category"), 2, 1, 4)
    d["max_per_broad_theme"] = _i(d.get("max_per_broad_theme"), 3, 1, 6)
    for key, default, lo, hi in [
        ("core_score_min",64,50,85),("core_strength_pct_min",60,0,100),("core_sector_pct_min",65,0,100),
        ("core_selection_min",58,0,100),("core_execution_min",55,0,100),("core_risk_min",55,0,100),
        ("leader_strength_pct_min",78,50,100),("leader_sector_pct_min",78,50,100),
        ("leader_selection_min",57,0,100),("leader_execution_min",60,0,100),("leader_risk_min",40,0,100),
        ("leader_score_min",65,50,90),
    ]:
        d[key] = _f(d.get(key), default, lo, hi)
    d["allow_bad_sector_only_for_leader"] = bool(d.get("allow_bad_sector_only_for_leader", True))

    clean = {}
    for key, default in DEFAULT_SETTINGS["weights"].items():
        clean[key] = max(0.0, _f(cfg["weights"].get(key), default, 0, 1000))
    total = sum(clean.values()) or 1.0
    cfg["weights"] = {k: v / total * 100.0 for k, v in clean.items()}

    for sec in ["penalties", "bonuses"]:
        for key, default in DEFAULT_SETTINGS[sec].items():
            cfg[sec][key] = _f(cfg[sec].get(key), default, 0, 25)
    cfg["version"] = VERSION
    return cfg


def load_settings_safe() -> dict[str, Any]:
    path = BASE_DIR / SETTINGS_FILE
    raw: dict[str, Any] = {}
    try:
        if path.exists():
            obj = json.loads(path.read_text(encoding="utf-8-sig"))
            if isinstance(obj, dict):
                raw = obj
    except Exception:
        raw = {}
    return normalize_settings(raw)


def save_settings_safe(settings: dict[str, Any]) -> tuple[bool, str]:
    payload = normalize_settings(settings)
    payload["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        from godpick_persistence_service import save_named_json_permanent
        report = save_named_json_permanent(
            SETTINGS_FILE, payload, firestore_doc="godpick_h96_high_conviction_settings"
        )
        ok = bool(getattr(report, "permanent_ok", False))
        msgs = report.messages() if hasattr(report, "messages") else []
        return ok, "｜".join(str(x) for x in msgs if x)
    except Exception:
        try:
            tmp = path = BASE_DIR / (SETTINGS_FILE + ".tmp_h96")
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(BASE_DIR / SETTINGS_FILE)
            return True, "H96設定已保存本機；遠端永久化服務不可用。"
        except Exception as exc:
            return False, f"H96設定保存失敗：{type(exc).__name__}: {exc}"

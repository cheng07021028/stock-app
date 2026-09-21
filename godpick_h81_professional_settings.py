# -*- coding: utf-8 -*-
"""H81 professional research settings with durable persistence.

Settings are business state.  They are stored through the existing GodPick
local + GitHub/Firestore authority instead of Streamlit session_state.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

VERSION = "v191_h81_professional_settings_20260921"
SETTINGS_FILE = "godpick_professional_ai_settings.json"
FIRESTORE_DOC = "godpick_professional_ai_settings"

DEFAULT_SETTINGS: dict[str, Any] = {
    "version": VERSION,
    "enabled": True,
    "updated_at": "",
    "update_seq": 0,
    "features": {
        "market_pricing": True,
        "technical": True,
        "news": True,
        "backtest": True,
        "portfolio_risk": True,
        "journal": True,
        "execution_plan": True,
    },
    "ranking_overlay": {
        "enabled": True,
        "max_abs_points": 2.5,
        "min_data_coverage": 0.45,
        "weights": {
            "market_pricing": 16,
            "technical": 20,
            "news": 10,
            "backtest": 14,
            "portfolio_risk": 14,
            "journal": 10,
            "execution_plan": 16,
        },
        "governance": "只調整研究排序；不得建立或替代H64/H68正式交易授權。",
    },
    "backtest": {
        "horizons": [1, 3, 5, 10, 20],
        "minimum_trades": 20,
        "minimum_out_of_sample_trades": 8,
        "train_ratio": 0.70,
        "walk_forward": True,
        "market_regime_split": True,
        "max_single_rule_adjustment_pct": 15.0,
    },
    "portfolio_risk": {
        "max_single_stock_weight_pct": 20.0,
        "max_sector_weight_pct": 35.0,
        "high_correlation_threshold": 0.75,
        "stress_market_correction_pct": -10.0,
        "stress_bear_market_pct": -20.0,
        "stress_recession_pct": -25.0,
        "stress_rate_shock_bp": 100,
    },
    "trade_journal": {
        "recent_trades": 20,
        "minimum_personalized_sample": 10,
        "only_use_mature_outcomes": True,
    },
    "daily_plan": {
        "preopen_enabled": True,
        "open_enabled": True,
        "intraday_enabled": True,
        "close_enabled": True,
        "require_price_plan": True,
        "require_rr_check": True,
        "require_market_regime_check": True,
    },
    "news": {
        "short_term_days": 5,
        "medium_term_days": 30,
        "long_term_days": 180,
        "require_source_label": True,
        "unknown_news_is_neutral": True,
    },
}


def _clip_float(value: Any, low: float, high: float, default: float) -> float:
    try:
        x = float(value)
    except Exception:
        x = default
    if x != x or x in (float("inf"), float("-inf")):
        x = default
    return max(low, min(high, x))


def _to_int(value: Any, low: int, high: int, default: int) -> int:
    try:
        x = int(value)
    except Exception:
        x = default
    return max(low, min(high, x))


def _normalise_weights(raw: Any) -> dict[str, int]:
    base = deepcopy(DEFAULT_SETTINGS["ranking_overlay"]["weights"])
    if isinstance(raw, dict):
        for key in base:
            base[key] = _to_int(raw.get(key, base[key]), 0, 100, base[key])
    total = sum(base.values())
    if total <= 0:
        return deepcopy(DEFAULT_SETTINGS["ranking_overlay"]["weights"])
    # Keep user intent but make the persisted map easy to inspect: sum exactly 100.
    scaled = {k: int(round(v * 100.0 / total)) for k, v in base.items()}
    delta = 100 - sum(scaled.values())
    if delta:
        biggest = max(scaled, key=scaled.get)
        scaled[biggest] += delta
    return scaled


def normalize_settings(payload: Any) -> dict[str, Any]:
    out = deepcopy(DEFAULT_SETTINGS)
    if isinstance(payload, dict):
        out.update({k: v for k, v in payload.items() if k not in {"features", "ranking_overlay", "backtest", "portfolio_risk", "trade_journal", "daily_plan", "news"}})
        for section in ["features", "ranking_overlay", "backtest", "portfolio_risk", "trade_journal", "daily_plan", "news"]:
            if isinstance(payload.get(section), dict):
                out[section].update(payload[section])

    features = out["features"]
    for key in DEFAULT_SETTINGS["features"]:
        features[key] = bool(features.get(key, True))

    ro = out["ranking_overlay"]
    ro["enabled"] = bool(ro.get("enabled", True))
    ro["max_abs_points"] = _clip_float(ro.get("max_abs_points"), 0.0, 5.0, 2.5)
    ro["min_data_coverage"] = _clip_float(ro.get("min_data_coverage"), 0.20, 0.90, 0.45)
    ro["weights"] = _normalise_weights(ro.get("weights"))
    ro["governance"] = DEFAULT_SETTINGS["ranking_overlay"]["governance"]

    bt = out["backtest"]
    horizons = []
    for v in bt.get("horizons", [1, 3, 5, 10, 20]):
        try:
            iv = int(v)
        except Exception:
            continue
        if iv in {1, 3, 5, 10, 20} and iv not in horizons:
            horizons.append(iv)
    bt["horizons"] = horizons or [1, 3, 5, 10, 20]
    bt["minimum_trades"] = _to_int(bt.get("minimum_trades"), 5, 500, 20)
    bt["minimum_out_of_sample_trades"] = _to_int(bt.get("minimum_out_of_sample_trades"), 3, 200, 8)
    bt["train_ratio"] = _clip_float(bt.get("train_ratio"), 0.50, 0.85, 0.70)
    bt["walk_forward"] = bool(bt.get("walk_forward", True))
    bt["market_regime_split"] = bool(bt.get("market_regime_split", True))
    bt["max_single_rule_adjustment_pct"] = _clip_float(bt.get("max_single_rule_adjustment_pct"), 0.0, 25.0, 15.0)

    pr = out["portfolio_risk"]
    pr["max_single_stock_weight_pct"] = _clip_float(pr.get("max_single_stock_weight_pct"), 5, 100, 20)
    pr["max_sector_weight_pct"] = _clip_float(pr.get("max_sector_weight_pct"), 10, 100, 35)
    pr["high_correlation_threshold"] = _clip_float(pr.get("high_correlation_threshold"), 0.30, 0.99, 0.75)
    pr["stress_market_correction_pct"] = -abs(_clip_float(pr.get("stress_market_correction_pct"), -50, 0, -10))
    pr["stress_bear_market_pct"] = -abs(_clip_float(pr.get("stress_bear_market_pct"), -60, 0, -20))
    pr["stress_recession_pct"] = -abs(_clip_float(pr.get("stress_recession_pct"), -70, 0, -25))
    pr["stress_rate_shock_bp"] = _to_int(pr.get("stress_rate_shock_bp"), 25, 500, 100)

    tj = out["trade_journal"]
    tj["recent_trades"] = _to_int(tj.get("recent_trades"), 5, 200, 20)
    tj["minimum_personalized_sample"] = _to_int(tj.get("minimum_personalized_sample"), 5, 100, 10)
    tj["only_use_mature_outcomes"] = bool(tj.get("only_use_mature_outcomes", True))

    for key in ["preopen_enabled", "open_enabled", "intraday_enabled", "close_enabled", "require_price_plan", "require_rr_check", "require_market_regime_check"]:
        out["daily_plan"][key] = bool(out["daily_plan"].get(key, True))

    news = out["news"]
    news["short_term_days"] = _to_int(news.get("short_term_days"), 1, 20, 5)
    news["medium_term_days"] = _to_int(news.get("medium_term_days"), 10, 120, 30)
    news["long_term_days"] = _to_int(news.get("long_term_days"), 60, 730, 180)
    news["require_source_label"] = bool(news.get("require_source_label", True))
    news["unknown_news_is_neutral"] = bool(news.get("unknown_news_is_neutral", True))

    out["enabled"] = bool(out.get("enabled", True))
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
        return normalize_settings(DEFAULT_SETTINGS), [f"H81設定讀取使用預設值：{type(exc).__name__}: {exc}"]


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
        if ok:
            # Read-back verification prevents the UI from claiming success on a stale write.
            verify, _ = load_settings()
            if int(verify.get("update_seq", 0) or 0) < int(candidate["update_seq"]):
                return False, "H81設定寫入後回讀版本未更新，已禁止顯示永久保存成功。", candidate
            return True, f"H81專業研究設定已永久保存（seq={candidate['update_seq']}）。{detail}", candidate
        return False, f"H81設定未取得永久權威確認。{detail}", candidate
    except Exception as exc:
        return False, f"H81設定保存失敗：{type(exc).__name__}: {exc}", candidate

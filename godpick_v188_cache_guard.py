# -*- coding: utf-8 -*-
"""V189 guard for V188 final decision frames.

The V188 ranking must never consume a pre-SuperAI / pre-trade-quality intermediate
DataFrame.  This module is intentionally Streamlit-free so its integrity and
repair logic can be regression-tested without the UI runtime.
"""
from __future__ import annotations

from typing import Any, Callable
import math

import pandas as pd

V189_CACHE_GUARD_VERSION = "v189_v188_final_cache_guard_20260812"

V188_REQUIRED_TEXT_COLUMNS = [
    "V188版本",
    "SuperAI Alpha等級",
    "SuperAI Trade等級",
    "SuperAI最終作戰等級",
    "V188交易許可",
    "V188正式推薦資格",
]
V188_REQUIRED_NUMERIC_COLUMNS = [
    "V188股神作戰優先分",
    "SuperAI Alpha分",
    "SuperAI Trade分",
]
V188_REQUIRED_COLUMNS = [*V188_REQUIRED_TEXT_COLUMNS, *V188_REQUIRED_NUMERIC_COLUMNS]

_BLANK = {"", "nan", "none", "null", "nat", "--", "-", "<na>"}


def _nonblank_ratio(series: pd.Series) -> float:
    if series is None or len(series) == 0:
        return 0.0
    s = series.fillna("").astype(str).str.strip()
    good = ~s.str.lower().isin(_BLANK)
    return float(good.mean()) if len(good) else 0.0


def _numeric_ratio(series: pd.Series) -> tuple[float, float]:
    if series is None or len(series) == 0:
        return 0.0, 0.0
    num = pd.to_numeric(series, errors="coerce")
    finite = num.map(lambda x: bool(pd.notna(x) and math.isfinite(float(x))))
    coverage = float(finite.mean()) if len(finite) else 0.0
    positive = float((num.fillna(0.0) > 0).mean()) if len(num) else 0.0
    return coverage, positive


def inspect_v188_decision_frame(data: Any, min_coverage: float = 0.95) -> dict[str, Any]:
    """Return a deterministic integrity report for a final V188 decision frame.

    A complete frame must carry the actual V188 output columns on almost every
    row.  Merely having the column names is insufficient because the original
    bug created missing columns later and displayed synthetic zeroes.
    """
    if not isinstance(data, pd.DataFrame):
        return {
            "complete": False,
            "rows": 0,
            "reason": "not-a-dataframe",
            "missing_columns": list(V188_REQUIRED_COLUMNS),
        }
    if data.empty:
        return {
            "complete": False,
            "rows": 0,
            "reason": "empty-frame",
            "missing_columns": list(V188_REQUIRED_COLUMNS),
        }

    missing = [c for c in V188_REQUIRED_COLUMNS if c not in data.columns]
    if missing:
        return {
            "complete": False,
            "rows": int(len(data)),
            "reason": "missing-v188-columns: " + ", ".join(missing),
            "missing_columns": missing,
        }

    text_cov = {c: _nonblank_ratio(data[c]) for c in V188_REQUIRED_TEXT_COLUMNS}
    numeric_info = {c: _numeric_ratio(data[c]) for c in V188_REQUIRED_NUMERIC_COLUMNS}
    numeric_cov = {c: v[0] for c, v in numeric_info.items()}
    positive_cov = {c: v[1] for c, v in numeric_info.items()}

    low_text = [c for c, ratio in text_cov.items() if ratio < min_coverage]
    low_numeric = [c for c, ratio in numeric_cov.items() if ratio < min_coverage]
    # Scores from the V188 engine are bounded but non-zero for a populated row.
    # Requiring at least one positive row catches the prior "fill missing with 0"
    # UI fallback while still allowing legitimately blocked stocks to be scored.
    no_positive = [c for c, ratio in positive_cov.items() if ratio <= 0.0]

    complete = not low_text and not low_numeric and not no_positive
    reasons: list[str] = []
    if low_text:
        reasons.append("text-coverage-low:" + ",".join(low_text))
    if low_numeric:
        reasons.append("numeric-coverage-low:" + ",".join(low_numeric))
    if no_positive:
        reasons.append("all-zero-score:" + ",".join(no_positive))

    return {
        "complete": bool(complete),
        "rows": int(len(data)),
        "reason": "ok" if complete else " | ".join(reasons),
        "missing_columns": [],
        "text_coverage": {k: round(v, 4) for k, v in text_cov.items()},
        "numeric_coverage": {k: round(v, 4) for k, v in numeric_cov.items()},
        "positive_coverage": {k: round(v, 4) for k, v in positive_cov.items()},
        "guard_version": V189_CACHE_GUARD_VERSION,
    }


def repair_v188_decision_frame(
    data: Any,
    *,
    super_ai_callable: Callable[[pd.DataFrame], pd.DataFrame] | None,
    official_factor_callable: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    scan_quality_callable: Callable[[pd.DataFrame, dict[str, Any]], pd.DataFrame] | None = None,
    scan_report: dict[str, Any] | None = None,
    canonicalize_callable: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Repair an intermediate/legacy frame into a V188-complete decision frame.

    The order mirrors the Page 7 final pipeline: official factors -> scan quality
    governance -> SuperAI/V188 -> canonical partition.  The function never
    invents V188 zeroes; if the engine fails or output remains incomplete, the
    returned report keeps ``complete=False`` so callers can block the ranking.
    """
    if not isinstance(data, pd.DataFrame):
        frame = pd.DataFrame(data)
    else:
        frame = data.copy()

    before = inspect_v188_decision_frame(frame)
    if before.get("complete"):
        return frame.reset_index(drop=True), {**before, "repaired": False}

    errors: list[str] = []
    if callable(official_factor_callable):
        try:
            frame = official_factor_callable(frame)
        except Exception as exc:  # caller decides whether this is fatal
            errors.append(f"official-factor:{exc}")

    if callable(scan_quality_callable) and isinstance(scan_report, dict) and scan_report:
        try:
            frame = scan_quality_callable(frame, scan_report)
        except Exception as exc:
            errors.append(f"scan-quality:{exc}")

    if callable(super_ai_callable):
        try:
            frame = super_ai_callable(frame)
        except Exception as exc:
            errors.append(f"super-ai:{exc}")
    else:
        errors.append("super-ai:unavailable")

    if callable(canonicalize_callable):
        try:
            frame = canonicalize_callable(frame)
        except Exception as exc:
            errors.append(f"canonicalize:{exc}")

    after = inspect_v188_decision_frame(frame)
    after["repaired"] = bool(after.get("complete"))
    if errors:
        after["repair_errors"] = errors
        if not after.get("complete"):
            after["reason"] = (str(after.get("reason") or "") + " | " + " | ".join(errors)).strip(" |")
    return frame.reset_index(drop=True), after


__all__ = [
    "V189_CACHE_GUARD_VERSION",
    "V188_REQUIRED_COLUMNS",
    "V188_REQUIRED_TEXT_COLUMNS",
    "V188_REQUIRED_NUMERIC_COLUMNS",
    "inspect_v188_decision_frame",
    "repair_v188_decision_frame",
]

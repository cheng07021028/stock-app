# -*- coding: utf-8 -*-
"""Phase 8.3 scan-quality overlay over the proven Phase 8.2 governance core."""
from __future__ import annotations
from typing import Any
import math
import pandas as pd
from _phase83_core import godpick_execution_governance_core as _core

for _name in dir(_core):
    if not _name.startswith("__"):
        globals()[_name] = getattr(_core, _name)

EXECUTION_GOVERNANCE_VERSION = "phase8_3_liquidity_recovery_20260712"
_ORIGINAL_CANONICALIZE = _core.canonicalize_final_partition
_LAST_CANDIDATE_QUALITY: dict[str, float] = {}

CANDIDATE_DIAGNOSIS_COLUMNS = list(dict.fromkeys(list(_core.CANDIDATE_DIAGNOSIS_COLUMNS) + [
    "20日均成交額百萬", "流動性資料狀態", "流動性資料來源",
    "掃描品質等級", "推薦適用範圍", "倉位折減係數", "有效K線資料率%",
    "流動性資料覆蓋率%", "官方因子覆蓋率%",
]))
ACTION_TABLE_COLUMNS = list(dict.fromkeys(list(_core.ACTION_TABLE_COLUMNS) + [
    "成交額百萬", "流動性等級", "流動性資料狀態",
]))


def _safe_float83(value: Any, default: float = 0.0) -> float:
    try:
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return float(default)
        return number
    except Exception:
        return float(default)


def _frame_quality(frame: pd.DataFrame | None) -> dict[str, float]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return {"rows": 0.0, "liquidity_coverage": 0.0, "official_coverage": 0.0}
    idx = frame.index
    def num(col: str) -> pd.Series:
        return pd.to_numeric(frame[col], errors="coerce") if col in frame.columns else pd.Series([0.0] * len(frame), index=idx)
    amount = num("成交額百萬").fillna(0)
    avg_amount = num("20日均成交額百萬").fillna(0)
    volume = num("最新成交量_張").fillna(0)
    avg_volume = num("20日均量_張").fillna(0)
    known = amount.gt(0) | avg_amount.gt(0) | volume.gt(0) | avg_volume.gt(0)
    official = pd.to_numeric(frame["官方資料完整度"], errors="coerce") if "官方資料完整度" in frame.columns else pd.Series([float("nan")] * len(frame), index=idx)
    return {
        "rows": float(len(frame)),
        "liquidity_coverage": float(known.mean() * 100.0),
        "official_coverage": float(official.notna().mean() * 100.0),
    }


def canonicalize_final_partition(df: pd.DataFrame | None) -> pd.DataFrame:
    global _LAST_CANDIDATE_QUALITY
    out = _ORIGINAL_CANONICALIZE(df)
    snap = _frame_quality(out)
    if snap["rows"] >= _safe_float83(_LAST_CANDIDATE_QUALITY.get("rows"), 0):
        _LAST_CANDIDATE_QUALITY = snap
    return out


def build_scan_quality_report(
    summary: dict[str, Any] | None,
    *, universe_size: int | None = None,
    candidate_count: int = 0,
    final_count: int = 0,
    candidate_frame: pd.DataFrame | None = None,
) -> dict[str, Any]:
    data = summary if isinstance(summary, dict) else {}
    expected = int(_safe_float83(data.get("total_count"), universe_size or 0))
    analyzed = int(_safe_float83(data.get("analyzed_ok"), candidate_count or 0))
    processed = sum(max(0, int(_safe_float83(data.get(k), 0))) for k in _core._SCAN_TERMINAL_KEYS)
    if expected > 0:
        processed = min(max(processed, analyzed), expected)
    else:
        processed = max(processed, analyzed)
    coverage = processed / expected * 100.0 if expected else 0.0
    valid_rate = analyzed / expected * 100.0 if expected else 0.0
    action_rate = final_count / analyzed * 100.0 if analyzed else 0.0

    metrics = _frame_quality(candidate_frame)
    if metrics["rows"] <= 0:
        metrics = dict(_LAST_CANDIDATE_QUALITY)
    liquidity = _safe_float83(data.get("liquidity_data_coverage_pct"), metrics.get("liquidity_coverage", 0))
    official = _safe_float83(data.get("official_data_coverage_pct"), metrics.get("official_coverage", 0))
    rows = int(_safe_float83(metrics.get("rows"), 0))
    minimum_pool = max(100, min(300, int(expected * 0.15))) if expected else 100

    if expected <= 0:
        status, level, usable, factor = "未知｜缺少掃描證據", "unknown", False, 0.0
        scope, reason = "不可判定", "缺少掃描總數與成功分析數，不能用目前結果推論市場。"
    elif expected <= 10:
        complete = processed == expected and analyzed == expected and (liquidity >= 80 or rows == 0)
        status = "完整" if complete else "不完整｜禁止正式推薦"
        level, usable, factor = ("complete", True, 1.0) if complete else ("invalid", False, 0.0)
        scope = "手動小範圍"
        reason = "小範圍掃描已逐檔完成。" if complete else "小範圍掃描必須逐檔成功，且具備流動性證據。"
    elif coverage < 95:
        status, level, usable, factor = "掃描未完成｜禁止正式推薦", "invalid", False, 0.0
        scope, reason = "未完成掃描", "仍有大量股票未完成處理，不能以局部結果代表市場。"
    elif analyzed < minimum_pool or valid_rate < 10:
        status, level, usable, factor = "有效資料池過小｜禁止正式推薦", "invalid", False, 0.0
        scope, reason = f"僅{analyzed}檔有效資料", "成功分析樣本過少，無法形成具代表性的選股池。"
    elif rows > 0 and liquidity < 60:
        status, level, usable, factor = "流動性資料異常｜禁止正式推薦", "invalid", False, 0.0
        scope = f"有效K線{analyzed}檔，但流動性覆蓋不足"
        reason = "多數候選缺少成交額/成交量；不能把缺值當成低流動性，也不能產生正式買進結論。"
    elif coverage >= 99 and valid_rate >= 80 and (liquidity >= 90 or rows == 0):
        status, level, usable, factor = "完整", "complete", True, 1.0
        scope, reason = "全掃描有效資料池", "掃描、K線與流動性資料均達正式推薦標準。"
    elif coverage >= 99 and analyzed >= minimum_pool and (liquidity >= 80 or rows == 0):
        status, level, usable, factor = "掃描完成｜限定有效資料池", "limited", True, 0.5
        scope = f"僅適用於{analyzed}檔有效資料股票"
        reason = "全體股票已處理，但部分股票缺少可用K線；推薦只代表有效資料池，倉位上限自動減半。"
    elif coverage >= 95 and valid_rate >= 50 and (liquidity >= 75 or rows == 0):
        status, level, usable, factor = "可用但需注意", "warning", True, 0.7
        scope, reason = "接近完整的有效資料池", "資料大致可用，但未達最佳完整度；正式倉位自動降級。"
    else:
        status, level, usable, factor = "資料品質不足｜禁止正式推薦", "invalid", False, 0.0
        scope, reason = "不可作為正式推薦", "掃描雖可能完成，但有效K線或流動性資料未達操作標準。"

    return {
        "掃描品質狀態": status,
        "掃描品質等級": level,
        "正式推薦可用": bool(usable),
        "推薦適用範圍": scope,
        "倉位折減係數": float(factor),
        "預計掃描數": expected,
        "已處理數": processed,
        "成功分析數": analyzed,
        "完整候選診斷數": int(candidate_count or 0),
        "最終作戰候選數": int(final_count or 0),
        "掃描覆蓋率%": round(coverage, 2),
        "有效K線資料率%": round(valid_rate, 2),
        "歷史資料成功率%": round(valid_rate, 2),
        "流動性資料覆蓋率%": round(liquidity, 2),
        "官方因子覆蓋率%": round(official, 2),
        "作戰候選率%": round(action_rate, 2),
        "掃描品質說明": reason,
        "版本": EXECUTION_GOVERNANCE_VERSION,
    }


def apply_scan_quality_to_frame(df: pd.DataFrame | None, report: dict[str, Any] | None) -> pd.DataFrame:
    out = pd.DataFrame() if df is None else (df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df))
    if out.empty:
        return out
    data = report if isinstance(report, dict) else {}
    for col in [
        "掃描品質狀態", "掃描品質等級", "正式推薦可用", "推薦適用範圍", "倉位折減係數",
        "預計掃描數", "成功分析數", "掃描覆蓋率%", "有效K線資料率%", "歷史資料成功率%",
        "流動性資料覆蓋率%", "官方因子覆蓋率%", "掃描品質說明",
    ]:
        out[col] = data.get(col, "")
    factor = max(0.0, min(1.0, _safe_float83(data.get("倉位折減係數"), 0)))
    usable = bool(data.get("正式推薦可用", False))
    if "建議倉位上限%" in out.columns:
        cap = pd.to_numeric(out["建議倉位上限%"], errors="coerce").fillna(0)
        out["建議倉位上限%"] = (cap * factor).round(1) if usable else 0.0
    if "推薦可信度分" in out.columns:
        confidence = pd.to_numeric(out["推薦可信度分"], errors="coerce").fillna(0)
        penalty = 0 if factor >= .99 else 8 if factor >= .69 else 12 if factor > 0 else 25
        out["推薦可信度分"] = (confidence - penalty).clip(0, 100).round(1)
    if usable and factor < 1 and "決策一致性" in out.columns:
        out["決策一致性"] = _core._series_text(out, "決策一致性") + f"｜掃描範圍受限，倉位乘數{factor:.1f}"
    return out


_core.canonicalize_final_partition = canonicalize_final_partition
_core.build_scan_quality_report = build_scan_quality_report
_core.apply_scan_quality_to_frame = apply_scan_quality_to_frame
_core.EXECUTION_GOVERNANCE_VERSION = EXECUTION_GOVERNANCE_VERSION
_core.CANDIDATE_DIAGNOSIS_COLUMNS = CANDIDATE_DIAGNOSIS_COLUMNS
_core.ACTION_TABLE_COLUMNS = ACTION_TABLE_COLUMNS

build_candidate_diagnosis = _core.build_candidate_diagnosis
build_action_table = _core.build_action_table
build_engine_diagnostic_table = _core.build_engine_diagnostic_table
govern_recommend_list = _core.govern_recommend_list
govern_recommend_records = _core.govern_recommend_records
report_allows_formal_action = _core.report_allows_formal_action

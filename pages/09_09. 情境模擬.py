from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from services.capacity_engine import calculate_capacity
from services.config import AUTHORITY_DIR, PERSISTENT_DIR
from services.data_loader import clear_data_cache, load_all_tables
from services.page_utils import format_numeric_editor_display_dataframe, parse_numeric_display_series, render_configurable_view, render_module_report_download
from services.persistent_store import atomic_write_text, load_json, load_parameters, save_json
from services.powerbi_theme import chart_spec, render_powerbi_chart, style_powerbi_figure
from services.ui_theme import apply_tech_theme, render_hero, render_human_help, render_manpower_gap_cards
from services.year_service import DEFAULT_YEAR, available_years_from_tables

LAST_SCENARIO_STATE_KEY = "scenario_last_result_payload_v228"
SCENARIO_FORCE_FRESH_KEY = "scenario_force_fresh_after_data_update_v1"
SCENARIO_REFRESH_SERIAL_KEY = "scenario_refresh_serial_v1"
SCENARIO_REFRESH_NOTICE_KEY = "scenario_refresh_notice_v1"
SCENARIO_RUNS_PATH = PERSISTENT_DIR / "scenario_runs.json"
SCENARIO_LAST_CACHE_PATH = AUTHORITY_DIR / "scenario_last_result.json"
SCENARIO_PARAMETER_SETTINGS_PATH = AUTHORITY_DIR / "scenario_parameter_settings.json"
MAX_SAVED_SCENARIOS = 50


def _safe_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    clean = df.copy()
    clean = clean.where(pd.notna(clean), None)
    return clean.to_dict(orient="records")


def _df_from_records(records: Any) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    try:
        return pd.DataFrame(records)
    except Exception:
        return pd.DataFrame()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _clamp(value: Any, minimum: float, maximum: float, default: float) -> float:
    number = _safe_float(value, default)
    return min(max(number, minimum), maximum)


def _load_scenario_parameter_settings() -> dict[str, Any]:
    raw = load_json(SCENARIO_PARAMETER_SETTINGS_PATH, default={}) or {}
    if not isinstance(raw, dict):
        raw = {}
    by_year = raw.get("by_year")
    if not isinstance(by_year, dict):
        by_year = {}
    return {
        "version": 1,
        "last_selected_year": raw.get("last_selected_year"),
        "saved_at": raw.get("saved_at"),
        "by_year": by_year,
    }


def _scenario_parameter_defaults(base_params: dict[str, Any]) -> dict[str, float]:
    return {
        "daily_hours": _clamp(base_params.get("daily_hours", 7.0), 1.0, 12.0, 7.0),
        "efficiency": _clamp(base_params.get("efficiency", 1.0), 0.1, 2.0, 1.0),
        "weekday_overtime_ratio": _clamp(base_params.get("weekday_overtime_ratio", 0.3), 0.0, 1.0, 0.3),
        "holiday_overtime_ratio": _clamp(base_params.get("holiday_overtime_ratio", 0.3), 0.0, 1.0, 0.3),
        "standard_hour_factor": 1.0,
        "order_factor": 1.0,
        "target_utilization": 0.90,
        "add_regular_people": 0.0,
        "add_dispatch_people": 0.0,
        "add_outsource_people": 0.0,
        "project_deduct_people": 0.0,
    }


def _scenario_parameter_values_for_year(settings: dict[str, Any], year: int, base_params: dict[str, Any]) -> dict[str, float]:
    values = _scenario_parameter_defaults(base_params)
    by_year = settings.get("by_year", {}) if isinstance(settings, dict) else {}
    saved = by_year.get(str(int(year)), {}) if isinstance(by_year, dict) else {}
    if not isinstance(saved, dict):
        saved = {}
    values.update({
        "daily_hours": _clamp(saved.get("daily_hours", values["daily_hours"]), 1.0, 12.0, values["daily_hours"]),
        "efficiency": _clamp(saved.get("efficiency", values["efficiency"]), 0.1, 2.0, values["efficiency"]),
        "weekday_overtime_ratio": _clamp(saved.get("weekday_overtime_ratio", values["weekday_overtime_ratio"]), 0.0, 1.0, values["weekday_overtime_ratio"]),
        "holiday_overtime_ratio": _clamp(saved.get("holiday_overtime_ratio", values["holiday_overtime_ratio"]), 0.0, 1.0, values["holiday_overtime_ratio"]),
        "standard_hour_factor": _clamp(saved.get("standard_hour_factor", 1.0), 0.5, 2.0, 1.0),
        "order_factor": _clamp(saved.get("order_factor", 1.0), 0.5, 2.0, 1.0),
        "target_utilization": _clamp(saved.get("target_utilization", 0.90), 0.60, 1.20, 0.90),
        "add_regular_people": _clamp(saved.get("add_regular_people", 0.0), -50.0, 100.0, 0.0),
        "add_dispatch_people": _clamp(saved.get("add_dispatch_people", 0.0), -50.0, 100.0, 0.0),
        "add_outsource_people": _clamp(saved.get("add_outsource_people", 0.0), -50.0, 100.0, 0.0),
        "project_deduct_people": _clamp(saved.get("project_deduct_people", 0.0), 0.0, 100.0, 0.0),
    })
    return values


def _save_scenario_parameter_settings(year: int, values: dict[str, Any]) -> dict[str, Any]:
    settings = _load_scenario_parameter_settings()
    by_year = dict(settings.get("by_year", {}))
    saved_at = datetime.now().isoformat(timespec="seconds")
    by_year[str(int(year))] = {
        "daily_hours": float(values["daily_hours"]),
        "efficiency": float(values["efficiency"]),
        "weekday_overtime_ratio": float(values["weekday_overtime_ratio"]),
        "holiday_overtime_ratio": float(values["holiday_overtime_ratio"]),
        "standard_hour_factor": float(values["standard_hour_factor"]),
        "order_factor": float(values["order_factor"]),
        "target_utilization": float(values["target_utilization"]),
        "add_regular_people": float(values["add_regular_people"]),
        "add_dispatch_people": float(values["add_dispatch_people"]),
        "add_outsource_people": float(values["add_outsource_people"]),
        "project_deduct_people": float(values["project_deduct_people"]),
        "saved_at": saved_at,
    }
    payload = {
        "version": 1,
        "last_selected_year": int(year),
        "saved_at": saved_at,
        "by_year": by_year,
    }
    save_json(SCENARIO_PARAMETER_SETTINGS_PATH, payload)
    return payload


def _coerce_numeric_series(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if df is None or df.empty or column not in df.columns:
        return pd.Series([default] * (0 if df is None else len(df)), index=(None if df is None else df.index), dtype="float64")
    return pd.to_numeric(df[column], errors="coerce").fillna(default)


def _ratio_to_percent(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").fillna(0)
    nonzero = numeric[numeric.abs() > 0]
    if not nonzero.empty and nonzero.abs().median() > 2:
        return numeric
    return numeric * 100


def _display_percent_value(value: Any) -> float:
    number = _safe_float(value, 0.0)
    return number if abs(number) > 2 else number * 100


def _with_percent_utilization_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a display/export frame where every 產能負荷率 column is shown as percent points.

    Internal calculation columns keep ratio values (for example 1.09), but user-facing
    tables and exports should show 109 with a (%) column label. Existing percent columns
    are preserved and normalized to numeric percent points.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    source = df.copy()
    result = pd.DataFrame(index=source.index)
    for col in source.columns:
        col_name = str(col)
        if "稼動率" in col_name:
            if "(%)" in col_name:
                pct_col = col_name
            else:
                pct_col = f"{col_name}(%)"
            result[pct_col] = _ratio_to_percent(source[col])
        else:
            result[col] = source[col]
    return result.reset_index(drop=True)

def _rename_capacity_terms_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Display-only terminology correction. Internal calculation columns stay unchanged."""
    if df is None or df.empty:
        return pd.DataFrame()
    return df.rename(columns={
        "正常稼動率(%)": "正常產能負荷率(%)",
        "含加班稼動率(%)": "含加班產能負荷率(%)",
        "原始正常稼動率(%)": "原始正常產能負荷率(%)",  # old saved payload compatibility
        "原始含加班稼動率(%)": "原始含加班產能負荷率(%)",
        "基準正常稼動率(%)": "基準正常產能負荷率(%)",
        "基準含加班稼動率(%)": "基準含加班產能負荷率(%)",
        "正常產能負荷": "正常產能餘額",
        "含加班產能負荷": "含加班產能餘額",
        "原始正常產能負荷": "原始正常產能餘額",  # old saved payload compatibility
        "原始含加班產能負荷": "原始含加班產能餘額",
        "基準正常產能負荷": "基準正常產能餘額",
        "基準含加班產能負荷": "基準含加班產能餘額",
        "缺工工時": "缺口工時",
        "目標稼動率(%)": "目標產能負荷率(%)",
    })


_DECIMAL_DISPLAY_EXCLUDE_COLS = {"年份", "月別數字"}


def _format_numeric_columns_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Display numeric values using the final system-wide rules.

    - 0 and whole numbers show without decimals;
    - decimal values show at most 1 decimal;
    - unit 人 columns are whole numbers;
    - percent columns show whole percentages;
    - values over 1,000 use comma separators.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    integer_hints = {"年份", "月別數字", "月份數字", "週六天數", "週日天數", "法定假日", "補班日", "正常工作日"}

    def _is_people_col(col_name: str) -> bool:
        if "比例" in col_name:
            return False
        return "(人)" in col_name or any(k in col_name for k in ["人力", "人數", "補人", "缺工"])

    def _fmt(value, col_name: str) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        try:
            number = float(str(value).replace(",", "").replace("%", "").strip())
        except Exception:
            return str(value)
        if abs(number) < 1e-9:
            return "0%" if "%" in col_name else "0"
        if "%" in col_name:
            return f"{number:,.0f}%"
        if _is_people_col(col_name) or col_name in integer_hints:
            return f"{number:,.0f}"
        if abs(number - round(number)) <= 1e-9:
            return f"{number:,.0f}"
        return f"{number:,.1f}"

    for col in out.columns:
        col_name = str(col)
        if col_name in _DECIMAL_DISPLAY_EXCLUDE_COLS:
            continue
        numeric = pd.to_numeric(out[col], errors="coerce")
        if numeric.notna().sum() == 0:
            continue
        out[col] = out[col].map(lambda v, c=col_name: _fmt(v, c))
    return out

def _ensure_result_display_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Backfill columns needed by the scenario charts when loading old saved payloads."""
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "月份" not in out.columns:
        out["月份"] = [f"{i + 1}月" for i in range(len(out))]
    numeric_defaults = [
        "原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "需求總工時", "正常可用工時", "含加班可用工時", "正常稼動率", "含加班稼動率",
        "正常產能負荷", "含加班產能負荷", "可用總人力", "直接有效人力", "需求人力", "人力差異", "缺工天數",
    ]
    for col in numeric_defaults:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    if "含加班可用工時" in out.columns and "正常可用工時" in out.columns:
        out["含加班可用工時"] = out[["含加班可用工時", "正常可用工時"]].max(axis=1)
    if "狀態" not in out.columns:
        out["狀態"] = "未設定"
    return out


def _ensure_manpower_display_columns(manpower_df: pd.DataFrame, result_df: pd.DataFrame) -> pd.DataFrame:
    """Ensure saved/old scenario results always have all columns used by charts."""
    if manpower_df is None or manpower_df.empty:
        if result_df is None or result_df.empty:
            return pd.DataFrame()
        out = result_df[[c for c in ["月份", "可用總人力", "直接有效人力", "需求人力", "正常產能負荷", "含加班產能負荷", "人力差異", "缺工天數", "正常稼動率", "含加班稼動率", "狀態"] if c in result_df.columns]].copy()
    else:
        out = manpower_df.copy()

    if "月份" not in out.columns:
        out["月份"] = [f"{i + 1}月" for i in range(len(out))]

    # Rebuild from result if old payload lacks newly added normal-shift columns.
    if result_df is not None and not result_df.empty:
        by_month = result_df.set_index("月份") if "月份" in result_df.columns else pd.DataFrame()
        for target, source in [
            ("原始需求工時", "原始需求工時"),
            ("產能計算排除工時", "產能計算排除工時"),
            ("排除後需求工時", "排除後需求工時"),
            ("調整工時", "調整工時"),
            ("需求總工時", "需求總工時"),
            ("正常產能負荷", "正常產能負荷"),
            ("含加班產能負荷", "含加班產能負荷"),
            ("正常稼動率(%)", "正常稼動率"),
            ("含加班稼動率(%)", "含加班稼動率"),
            ("可用總人力", "可用總人力"),
            ("直接有效人力", "直接有效人力"),
            ("需求人力", "需求人力"),
            ("人力差異", "人力差異"),
            ("缺工天數", "缺工天數"),
            ("狀態", "狀態"),
        ]:
            if target not in out.columns and source in result_df.columns and not by_month.empty:
                mapped = out["月份"].map(by_month[source])
                out[target] = mapped.values

    for col in ["原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "直接有效人力", "需求人力", "需求總工時", "人力差異", "缺工天數", "正常產能負荷", "含加班產能負荷", "目標達標需求人力", "建議補人(人)"]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    if "正常稼動率(%)" not in out.columns:
        if "正常稼動率" in out.columns:
            out["正常稼動率(%)"] = _ratio_to_percent(out["正常稼動率"])
        else:
            out["正常稼動率(%)"] = 0.0
    else:
        out["正常稼動率(%)"] = pd.to_numeric(out["正常稼動率(%)"], errors="coerce").fillna(0.0)

    if "含加班稼動率(%)" not in out.columns:
        if "含加班稼動率" in out.columns:
            out["含加班稼動率(%)"] = _ratio_to_percent(out["含加班稼動率"])
        else:
            out["含加班稼動率(%)"] = 0.0
    else:
        out["含加班稼動率(%)"] = pd.to_numeric(out["含加班稼動率(%)"], errors="coerce").fillna(0.0)

    if "狀態" not in out.columns:
        out["狀態"] = "未設定"
    return out


def _make_payload(
    *,
    scenario_name: str,
    selected_year: int,
    target_utilization: float,
    result: pd.DataFrame,
    comparison: pd.DataFrame,
    manpower_analysis: pd.DataFrame,
    params: dict,
    add_regular_people: float,
    add_dispatch_people: float,
    add_outsource_people: float,
    project_deduct_people: float,
    extra_people: float,
    standard_hour_factor: float,
    order_factor: float,
) -> dict[str, Any]:
    return {
        "name": scenario_name,
        "year": int(selected_year),
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "target_utilization": float(target_utilization),
        "params": params,
        "add_regular_people": float(add_regular_people),
        "add_dispatch_people": float(add_dispatch_people),
        "add_outsource_people": float(add_outsource_people),
        "project_deduct_people": float(project_deduct_people),
        "extra_people": float(extra_people),
        "standard_hour_factor": float(standard_hour_factor),
        "order_factor": float(order_factor),
        "result": _safe_records(result),
        "comparison": _safe_records(comparison),
        "manpower_analysis": _safe_records(manpower_analysis),
    }


def _payload_has_result(payload: Any) -> bool:
    return isinstance(payload, dict) and (bool(payload.get("result")) or bool(payload.get("manpower_analysis")))


def _write_last_payload_cache(payload: dict[str, Any]) -> None:
    """Save the last displayed scenario locally without forcing GitHub sync.

    This file is only used to survive ordinary Streamlit reruns during the current
    deployment. Permanent saves still go to scenario_runs.json when the user checks
    the save option.
    """
    try:
        atomic_write_text(SCENARIO_LAST_CACHE_PATH, json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    except Exception:
        pass


def _load_last_payload_cache() -> dict[str, Any] | None:
    payload = load_json(SCENARIO_LAST_CACHE_PATH, default={}) or {}
    return payload if _payload_has_result(payload) else None


def _load_latest_payload_from_saved(runs: list[dict]) -> dict[str, Any] | None:
    if not runs:
        return None
    for payload in reversed(runs):
        if _payload_has_result(payload):
            return payload
    return None


def _active_payload_from_state_cache_or_saved(runs: list[dict]) -> dict[str, Any] | None:
    payload = st.session_state.get(LAST_SCENARIO_STATE_KEY)
    if _payload_has_result(payload):
        return payload
    payload = _load_last_payload_cache()
    if _payload_has_result(payload):
        st.session_state[LAST_SCENARIO_STATE_KEY] = payload
        return payload
    payload = _load_latest_payload_from_saved(runs)
    if _payload_has_result(payload):
        st.session_state[LAST_SCENARIO_STATE_KEY] = payload
        return payload
    return None


def _data_source_summary(tables: dict[str, pd.DataFrame]) -> str:
    labels = {
        "employees": "01正職",
        "dispatch": "02派遣",
        "schedule": "05排程",
        "standard_hours": "06標準工時",
        "work_calendar": "07工作天數",
        "capacity_adjustments": "04調整",
    }
    parts: list[str] = []
    for key, label in labels.items():
        df = tables.get(key, pd.DataFrame())
        parts.append(f"{label} {0 if df is None else len(df):,} 筆")
    return "｜".join(parts)


def _clear_scenario_display_cache() -> None:
    st.session_state.pop(LAST_SCENARIO_STATE_KEY, None)
    try:
        if SCENARIO_LAST_CACHE_PATH.exists():
            SCENARIO_LAST_CACHE_PATH.unlink()
    except Exception:
        pass


def _prepare_params(base_params: dict, *, daily_hours: float, efficiency: float, weekday_ratio: float, holiday_ratio: float) -> dict:
    params = dict(base_params)
    params.update({
        "daily_hours": float(daily_hours),
        "efficiency": float(efficiency),
        "weekday_overtime_ratio": float(weekday_ratio),
        "holiday_overtime_ratio": float(holiday_ratio),
    })
    return params


def _apply_extra_people_to_monthly_direct_params(params: dict, selected_year: int, extra_people: float) -> dict:
    """Keep scenario add/deduct manpower effective when 08 uses monthly direct manpower.

    08. 人力參數設定 may define an absolute 1~12月 direct manpower map.
    Without this adjustment, the scenario temporary dispatch row would be ignored
    because calculate_capacity correctly uses the 08 monthly override as the
    official source. For scenario simulation only, add the net extra people to
    each selected-year month.
    """
    if abs(float(extra_people or 0.0)) < 1e-9:
        return params
    if not bool(params.get("use_monthly_direct_people_override", params.get("use_direct_people_override", False))):
        return params
    raw = params.get("monthly_direct_people") or params.get("direct_people_by_year")
    if not isinstance(raw, dict):
        return params
    updated = dict(params)
    store = {str(k): (dict(v) if isinstance(v, dict) else v) for k, v in raw.items()}
    year_key = str(int(selected_year))
    months = store.get(year_key)
    if not isinstance(months, dict):
        return params
    adjusted_months = {}
    for idx in range(1, 13):
        month = f"{idx}月"
        base_value = _safe_float(months.get(month, months.get(str(idx), 0.0)), 0.0)
        adjusted_months[month] = max(0.0, base_value + float(extra_people or 0.0))
    store[year_key] = adjusted_months
    updated["monthly_direct_people"] = store
    updated["direct_people_by_year"] = store
    return updated


def _calculate_scenario_payload(
    *,
    tables: dict[str, pd.DataFrame],
    base_params: dict,
    params: dict,
    scenario_name: str,
    selected_year: int,
    target_utilization: float,
    add_regular_people: float,
    add_dispatch_people: float,
    add_outsource_people: float,
    project_deduct_people: float,
    extra_people: float,
    standard_hour_factor: float,
    order_factor: float,
) -> dict[str, Any]:
    base_capacity = calculate_capacity(
        tables.get("schedule", pd.DataFrame()),
        tables.get("standard_hours", pd.DataFrame()),
        tables.get("work_calendar", pd.DataFrame()),
        tables.get("employees", pd.DataFrame()),
        tables.get("dispatch", pd.DataFrame()),
        base_params,
        tables.get("capacity_adjustments"),
        target_year=selected_year,
    )

    dispatch = tables.get("dispatch", pd.DataFrame()).copy()

    schedule = tables.get("schedule", pd.DataFrame()).copy()
    if order_factor != 1.0 and "台數" in schedule.columns:
        schedule["台數"] = pd.to_numeric(schedule["台數"], errors="coerce").fillna(1) * float(order_factor)

    standard = tables.get("standard_hours", pd.DataFrame()).copy()
    if standard_hour_factor != 1.0 and "標準工時" in standard.columns:
        standard["標準工時"] = pd.to_numeric(standard["標準工時"], errors="coerce").fillna(0) * float(standard_hour_factor)

    # 情境增減人力的輸入單位是「直接有效人力」，不是一筆人員資料的
    # 可用比例。舊版把 +10 寫成一個人的可用比例 10，經比例正規化後只會
    # 變成 0.1 人。現在以基準結果的月別直接有效人力為底，明確加減後交由
    # capacity_engine 的手動月別覆寫入口計算；可用總人力仍保留 01/02
    # 目前實際在職總人數，符合全系統定義。
    scenario_params = dict(params)
    if abs(float(extra_people or 0.0)) > 1e-9 and not base_capacity.empty:
        base_direct_map = {
            str(row.get("月份")): max(0.0, _safe_float(row.get("直接有效人力"), 0.0) + float(extra_people))
            for _, row in base_capacity.iterrows()
        }
        scenario_store = dict(scenario_params.get("monthly_direct_people") or {})
        scenario_store[str(int(selected_year))] = base_direct_map
        scenario_params["monthly_direct_people"] = scenario_store
        scenario_params["direct_people_by_year"] = scenario_store
        scenario_params["force_manual_monthly_manpower"] = True
        scenario_params["use_monthly_direct_people_override"] = True
        scenario_params["use_direct_people_override"] = True

    result = calculate_capacity(
        schedule,
        standard,
        tables.get("work_calendar", pd.DataFrame()),
        tables.get("employees", pd.DataFrame()),
        dispatch,
        scenario_params,
        tables.get("capacity_adjustments"),
        target_year=selected_year,
    )

    if base_capacity.empty:
        comparison = pd.DataFrame()
    else:
        comparison_cols = ["月份", "原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "需求總工時", "正常可用工時", "含加班可用工時", "正常稼動率", "含加班稼動率", "正常產能負荷", "含加班產能負荷", "人力差異", "缺工天數"]
        for col in comparison_cols:
            if col not in base_capacity.columns:
                base_capacity[col] = None
        comparison = base_capacity[comparison_cols].copy().rename(columns={
            "原始需求工時": "基準原始需求工時",
            "產能計算排除工時": "基準產能計算排除工時",
            "排除後需求工時": "基準排除後需求工時",
            "調整工時": "基準調整工時",
            "需求總工時": "基準需求總工時",
            "正常可用工時": "基準正常可用工時",
            "含加班可用工時": "基準含加班可用工時",
            "正常稼動率": "基準正常稼動率",
            "含加班稼動率": "基準含加班稼動率",
            "正常產能負荷": "基準正常產能負荷",
            "含加班產能負荷": "基準含加班產能負荷",
            "人力差異": "基準人力差異",
            "缺工天數": "基準缺工天數",
        })
        result_cols = ["月份", "原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "需求總工時", "正常可用工時", "含加班可用工時", "正常稼動率", "含加班稼動率", "正常產能負荷", "含加班產能負荷", "需求人力", "可用總人力", "直接有效人力", "人力差異", "缺工天數", "狀態"]
        for col in result_cols:
            if col not in result.columns:
                result[col] = None
        comparison = comparison.merge(result[result_cols], on="月份", how="left")

    analysis = result.copy()
    if analysis.empty:
        manpower_analysis = pd.DataFrame()
    else:
        for col in ["原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "正常可用工時", "含加班可用工時", "可用總人力", "直接有效人力", "需求總工時", "正常稼動率", "含加班稼動率", "正常產能負荷", "含加班產能負荷", "需求人力", "人力差異", "缺工天數", "狀態"]:
            if col not in analysis.columns:
                analysis[col] = 0 if col != "狀態" else "未設定"
        direct_people = pd.to_numeric(analysis["直接有效人力"], errors="coerce").replace(0, pd.NA)
        per_person_capacity = pd.to_numeric(analysis["含加班可用工時"], errors="coerce") / direct_people
        target_capacity = per_person_capacity * float(target_utilization)
        analysis["目標達標需求人力"] = (pd.to_numeric(analysis["需求總工時"], errors="coerce") / target_capacity).fillna(0)
        analysis["建議補人(人)"] = (analysis["目標達標需求人力"] - pd.to_numeric(analysis["直接有效人力"], errors="coerce").fillna(0)).clip(lower=0)
        analysis["建議補人(人)"] = analysis["建議補人(人)"].apply(lambda x: int(x) if abs(float(x) - int(float(x))) < 0.01 else int(float(x)) + 1)
        analysis["正常稼動率(%)"] = pd.to_numeric(analysis["正常稼動率"], errors="coerce").fillna(0) * 100
        analysis["含加班稼動率(%)"] = pd.to_numeric(analysis["含加班稼動率"], errors="coerce").fillna(0) * 100
        manpower_analysis = analysis[["月份", "原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "需求總工時", "可用總人力", "直接有效人力", "需求人力", "目標達標需求人力", "建議補人(人)", "正常產能負荷", "含加班產能負荷", "人力差異", "缺工天數", "正常稼動率(%)", "含加班稼動率(%)", "狀態"]].copy()

    return _make_payload(
        scenario_name=scenario_name,
        selected_year=int(selected_year),
        target_utilization=float(target_utilization),
        result=result,
        comparison=comparison,
        manpower_analysis=manpower_analysis,
        params=scenario_params,
        add_regular_people=add_regular_people,
        add_dispatch_people=add_dispatch_people,
        add_outsource_people=add_outsource_people,
        project_deduct_people=project_deduct_people,
        extra_people=extra_people,
        standard_hour_factor=standard_hour_factor,
        order_factor=order_factor,
    )


st.set_page_config(page_title="09. 情境模擬", page_icon="🧪", layout="wide")
apply_tech_theme()
render_hero("09. 情境模擬", "調整加班比例、效率、人力投入與風險條件，快速比較需求、人力需求與可用工時。")
render_human_help([
    "滑桿與輸入框不會立即寫入資料，按『執行情境模擬』才計算。",
    "人力模擬可分別輸入新增正職、派遣、外包與專案/間接扣除，系統會推估需求人力與建議補人數。",
    "執行後結果會固定保留在本頁，勾選圖表標籤、套用設定或表格設定不會重新計算，也不會清空結果。",
    "若其他模組資料已修改，先按『更新資料』清除舊快取，再按『執行情境模擬』用最新資料重新計算。",
    "永久保存情境會寫入 scenario_runs.json；一般畫面暫存則只用來防止本頁 rerun 後消失。",
])

st.markdown("### 資料來源更新")
st.caption("當 01/02 人力、04 產能調整、05 排程、06 標準工時、07 工作天數或 08 人力參數有異動時，按此按鈕會重新讀取最新資料，並沿用該年度已永久保存的情境參數重建基準快照。")
refresh_col, refresh_info_col = st.columns([1, 3])
with refresh_col:
    if st.button("更新資料並永久記錄", type="primary", use_container_width=True):
        clear_data_cache()
        _clear_scenario_display_cache()
        fresh_tables = load_all_tables()
        fresh_params = load_parameters()
        fresh_years = available_years_from_tables(fresh_tables, DEFAULT_YEAR) or [DEFAULT_YEAR]
        current_serial = int(st.session_state.get(SCENARIO_REFRESH_SERIAL_KEY, 0))
        selected_year_from_state = st.session_state.get(f"scenario_selected_year_{current_serial}")
        try:
            refresh_year = int(selected_year_from_state) if selected_year_from_state is not None else int(fresh_years[-1])
        except Exception:
            refresh_year = int(fresh_years[-1])
        if refresh_year not in [int(y) for y in fresh_years]:
            refresh_year = int(fresh_years[-1])

        saved_parameter_settings = _load_scenario_parameter_settings()
        refresh_values = _scenario_parameter_values_for_year(saved_parameter_settings, int(refresh_year), fresh_params)
        refresh_params = _prepare_params(
            fresh_params,
            daily_hours=refresh_values["daily_hours"],
            efficiency=refresh_values["efficiency"],
            weekday_ratio=refresh_values["weekday_overtime_ratio"],
            holiday_ratio=refresh_values["holiday_overtime_ratio"],
        )
        refresh_extra_people = (
            refresh_values["add_regular_people"]
            + refresh_values["add_dispatch_people"]
            + refresh_values["add_outsource_people"]
            - refresh_values["project_deduct_people"]
        )
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        refreshed_payload = _calculate_scenario_payload(
            tables=fresh_tables,
            base_params=fresh_params,
            params=refresh_params,
            scenario_name=f"最新資料基準 {timestamp}",
            selected_year=int(refresh_year),
            target_utilization=refresh_values["target_utilization"],
            add_regular_people=refresh_values["add_regular_people"],
            add_dispatch_people=refresh_values["add_dispatch_people"],
            add_outsource_people=refresh_values["add_outsource_people"],
            project_deduct_people=refresh_values["project_deduct_people"],
            extra_people=refresh_extra_people,
            standard_hour_factor=refresh_values["standard_hour_factor"],
            order_factor=refresh_values["order_factor"],
        )
        refreshed_payload["updated_from_modules"] = True
        refreshed_payload["source_summary"] = _data_source_summary(fresh_tables)
        refreshed_payload["updated_at"] = timestamp
        st.session_state[LAST_SCENARIO_STATE_KEY] = refreshed_payload
        save_json(SCENARIO_LAST_CACHE_PATH, refreshed_payload)
        st.session_state[SCENARIO_FORCE_FRESH_KEY] = False
        st.session_state[SCENARIO_REFRESH_SERIAL_KEY] = current_serial + 1
        st.session_state[SCENARIO_REFRESH_NOTICE_KEY] = f"{timestamp}｜已永久記錄最新資料快照｜{_data_source_summary(fresh_tables)}"
        st.rerun()

tables = load_all_tables()
base_params = load_parameters()
years = available_years_from_tables(tables, DEFAULT_YEAR)
if not years:
    years = [DEFAULT_YEAR]
scenario_parameter_settings = _load_scenario_parameter_settings()
saved_parameter_year = scenario_parameter_settings.get("last_selected_year")
try:
    saved_parameter_year = int(saved_parameter_year)
except Exception:
    saved_parameter_year = int(years[-1])
if saved_parameter_year not in [int(y) for y in years]:
    saved_parameter_year = int(years[-1])

refresh_serial = int(st.session_state.get(SCENARIO_REFRESH_SERIAL_KEY, 0))
refresh_notice = st.session_state.pop(SCENARIO_REFRESH_NOTICE_KEY, None)
if refresh_notice:
    with refresh_info_col:
        st.success(str(refresh_notice))
        sync_status = st.session_state.get("last_github_sync_status") or {}
        if sync_status:
            if sync_status.get("ok"):
                st.caption(sync_status.get("message", "已完成 GitHub 同步。"))
            else:
                st.warning(sync_status.get("message", "已寫入本機，但 GitHub 同步狀態未知；Reboot App 後可能回到舊資料。"))
if st.session_state.get(SCENARIO_FORCE_FRESH_KEY):
    st.info("已清除舊情境結果快取。請按『執行情境模擬』，系統會用目前最新 01/02/04/05/06/07/08 資料重新計算。", icon="🔄")

with st.form("scenario_form", clear_on_submit=False):
    default_year_index = [int(y) for y in years].index(int(saved_parameter_year))
    selected_year = st.selectbox("模擬年份", years, index=default_year_index, key=f"scenario_selected_year_{refresh_serial}")
    saved_values = _scenario_parameter_values_for_year(scenario_parameter_settings, int(selected_year), base_params)
    year_key = f"{refresh_serial}_{int(selected_year)}"
    scenario_name = st.text_input("情境名稱", value=f"Scenario {datetime.now().strftime('%m%d_%H%M')}", key=f"scenario_name_{year_key}")
    st.markdown("### 基礎產能條件")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        daily_hours = st.number_input("每日正常工時", 1.0, 12.0, float(saved_values["daily_hours"]), 0.5, key=f"scenario_daily_hours_{year_key}")
        efficiency = st.number_input("工作效率", 0.1, 2.0, float(saved_values["efficiency"]), 0.05, key=f"scenario_efficiency_{year_key}")
    with c2:
        weekday_ratio = st.slider("平日加班人數比例", 0, 100, int(round(saved_values["weekday_overtime_ratio"] * 100)), key=f"scenario_weekday_ratio_{year_key}") / 100
        holiday_ratio = st.slider("假日加班人數比例", 0, 100, int(round(saved_values["holiday_overtime_ratio"] * 100)), key=f"scenario_holiday_ratio_{year_key}") / 100
    with c3:
        standard_hour_factor = st.number_input("標準工時倍率", 0.5, 2.0, float(saved_values["standard_hour_factor"]), 0.05, key=f"scenario_standard_factor_{year_key}")
        order_factor = st.number_input("訂單需求倍率", 0.5, 2.0, float(saved_values["order_factor"]), 0.05, key=f"scenario_order_factor_{year_key}")
    with c4:
        target_utilization = st.slider("目標含加班產能負荷率", 60, 120, int(round(saved_values["target_utilization"] * 100)), 5, key=f"scenario_target_utilization_{year_key}") / 100
        save_run = st.checkbox("儲存本次模擬結果", value=False, help="勾選後才寫入 scenario_runs.json 並同步 GitHub；不勾選時只保留本頁最後結果，速度較快。", key=f"scenario_save_run_{year_key}")

    st.markdown("### 人力模擬")
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        add_regular_people = st.number_input("新增正職有效人力(人)", -50.0, 100.0, float(saved_values["add_regular_people"]), 1.0, key=f"scenario_add_regular_{year_key}")
    with m2:
        add_dispatch_people = st.number_input("新增派遣有效人力(人)", -50.0, 100.0, float(saved_values["add_dispatch_people"]), 1.0, key=f"scenario_add_dispatch_{year_key}")
    with m3:
        add_outsource_people = st.number_input("新增外包有效人力(人)", -50.0, 100.0, float(saved_values["add_outsource_people"]), 1.0, key=f"scenario_add_outsource_{year_key}")
    with m4:
        project_deduct_people = st.number_input("專案/間接扣除人力(人)", 0.0, 100.0, float(saved_values["project_deduct_people"]), 1.0, key=f"scenario_project_deduct_{year_key}")
    extra_people = add_regular_people + add_dispatch_people + add_outsource_people - project_deduct_people
    st.caption(f"本次模擬淨增有效人力：{extra_people:+.1f} 人。只有按下『執行情境模擬』才會計算，不會立即改正式資料。")
    st.caption("提示：先按『永久儲存目前參數』，之後執行資料來源更新、重新整理或 Reboot App，系統都會依年度自動帶回。按『執行情境模擬』時也會自動保存目前參數。")
    save_col, run_col = st.columns([1, 1])
    with save_col:
        save_parameter_settings = st.form_submit_button("永久儲存目前參數", use_container_width=True)
    with run_col:
        run = st.form_submit_button("執行情境模擬", type="primary", use_container_width=True)

current_parameter_values = {
    "daily_hours": float(daily_hours),
    "efficiency": float(efficiency),
    "weekday_overtime_ratio": float(weekday_ratio),
    "holiday_overtime_ratio": float(holiday_ratio),
    "standard_hour_factor": float(standard_hour_factor),
    "order_factor": float(order_factor),
    "target_utilization": float(target_utilization),
    "add_regular_people": float(add_regular_people),
    "add_dispatch_people": float(add_dispatch_people),
    "add_outsource_people": float(add_outsource_people),
    "project_deduct_people": float(project_deduct_people),
}
if save_parameter_settings or run:
    scenario_parameter_settings = _save_scenario_parameter_settings(int(selected_year), current_parameter_values)
    if save_parameter_settings and not run:
        st.success(f"已永久保存 {int(selected_year)} 年情境模擬參數；更新資料、重新整理與 Reboot App 後會自動恢復。")
        sync_status = st.session_state.get("last_github_sync_status") or {}
        if sync_status and not sync_status.get("ok"):
            st.warning(sync_status.get("message", "參數已保存於目前環境，但遠端同步狀態未知。"))

params = _prepare_params(
    base_params,
    daily_hours=daily_hours,
    efficiency=efficiency,
    weekday_ratio=weekday_ratio,
    holiday_ratio=holiday_ratio,
)

runs = load_json(SCENARIO_RUNS_PATH, default=[]) or []
if not isinstance(runs, list):
    runs = []

if run:
    st.session_state[SCENARIO_FORCE_FRESH_KEY] = False
    with st.spinner("正在執行情境模擬，請稍候。這次計算完成後，切換圖表標籤或表格設定不會重新計算。"):
        payload = _calculate_scenario_payload(
            tables=tables,
            base_params=base_params,
            params=params,
            scenario_name=scenario_name,
            selected_year=int(selected_year),
            target_utilization=float(target_utilization),
            add_regular_people=float(add_regular_people),
            add_dispatch_people=float(add_dispatch_people),
            add_outsource_people=float(add_outsource_people),
            project_deduct_people=float(project_deduct_people),
            extra_people=float(extra_people),
            standard_hour_factor=float(standard_hour_factor),
            order_factor=float(order_factor),
        )
        st.session_state[LAST_SCENARIO_STATE_KEY] = payload
        _write_last_payload_cache(payload)

        if save_run:
            runs.append(payload)
            runs = runs[-MAX_SAVED_SCENARIOS:]
            save_json(SCENARIO_RUNS_PATH, runs)
            st.success("本次情境模擬已永久保存到 data/persistent/scenario_runs.json。")
        else:
            st.success("情境模擬完成，結果已保留在目前頁面。若需 Reboot 後仍保留，請勾選『儲存本次模擬結果』後重新執行。")

active_payload = None if st.session_state.get(SCENARIO_FORCE_FRESH_KEY) else _active_payload_from_state_cache_or_saved(runs)
latest_result = pd.DataFrame()
manpower_analysis = pd.DataFrame()
comparison = pd.DataFrame()

if active_payload:
    latest_result = _ensure_result_display_columns(_df_from_records(active_payload.get("result", [])))
    manpower_analysis = _ensure_manpower_display_columns(_df_from_records(active_payload.get("manpower_analysis", [])), latest_result)
    comparison = _df_from_records(active_payload.get("comparison", []))
    active_name = active_payload.get("name", "最後一次情境模擬")
    active_year = active_payload.get("year", selected_year)
    active_target = float(active_payload.get("target_utilization", target_utilization) or target_utilization)
    st.caption(f"目前顯示：{active_name}｜年份：{active_year}｜目標含加班產能負荷率：{active_target:.0%}")
else:
    active_name = scenario_name
    active_year = selected_year
    active_target = target_utilization
    st.info("請調整參數後按『執行情境模擬』。本頁已避免滑桿或圖表設定變動時自動重算。", icon="⚡")


NEGATIVE_VALUE_COLOR = "#FFB86B"  # 柔和橘色：用於負值，不使用太刺眼的紅色。
ZERO_BASELINE_COLOR = "rgba(255,255,255,0.72)"


def _negative_cell_css(value: object) -> str:
    try:
        if value is None or pd.isna(value):
            return ""
        number = float(value)
    except Exception:
        try:
            text = str(value).replace(",", "").replace("%", "").strip()
            number = float(text)
        except Exception:
            return ""
    if number < 0:
        return "color:#FFB86B;background-color:rgba(255,184,107,0.16);font-weight:800;"
    return ""


def _style_negative_values(df: pd.DataFrame):
    """Highlight negative numeric cells while keeping the underlying values unchanged."""
    try:
        return df.style.map(_negative_cell_css)
    except Exception:
        return df.style.applymap(_negative_cell_css)


def _negative_text_color_list(values) -> list[str]:
    safe_values = pd.to_numeric(pd.Series(list(values or [])), errors="coerce").fillna(0).tolist()
    return [NEGATIVE_VALUE_COLOR if float(v) < 0 else "#FFFFFF" for v in safe_values]


def _add_zero_baseline(fig, *, yref: str = "y"):
    try:
        fig.add_shape(
            type="line",
            xref="paper",
            x0=0,
            x1=1,
            yref=yref,
            y0=0,
            y1=0,
            line=dict(color=ZERO_BASELINE_COLOR, width=1.45, dash="solid"),
            layer="above",
        )
    except Exception:
        try:
            fig.add_hline(y=0, line_color=ZERO_BASELINE_COLOR, line_width=1.45)
        except Exception:
            pass
    return fig


def _apply_negative_value_chart_style(fig, *, negative: str = NEGATIVE_VALUE_COLOR):
    """Color negative bars/markers and their value labels with the same muted orange."""
    for trace in fig.data:
        vals = _safe_trace_values(trace)
        if not vals:
            continue
        try:
            positive = trace.marker.color if getattr(trace, "marker", None) and isinstance(trace.marker.color, str) else "#76D7FF"
        except Exception:
            positive = "#76D7FF"
        if trace.type in {"bar", "histogram"}:
            try:
                trace.marker.color = _signed_bar_colors(vals, positive=positive, negative=negative)
                trace.marker.line.color = "rgba(255,255,255,0.34)"
                trace.marker.line.width = 1.15
                trace.textfont.color = _negative_text_color_list(vals)
                trace.outsidetextfont.color = _negative_text_color_list(vals)
                trace.insidetextfont.color = _negative_text_color_list(vals)
            except Exception:
                pass
        elif trace.type in {"scatter", "scattergl"}:
            try:
                # For lines, keep the series line color readable but force negative markers/labels orange.
                trace.marker.color = _signed_bar_colors(vals, positive=positive, negative=negative)
                trace.marker.line.color = "rgba(255,255,255,0.68)"
                trace.textfont.color = _negative_text_color_list(vals)
            except Exception:
                pass
    return fig


def _style_figure_with_negative_values(fig: go.Figure, **style_kwargs: Any) -> go.Figure:
    """Apply Power BI theme first, then re-apply negative colors.

    style_powerbi_figure() intentionally assigns a uniform palette to every trace,
    so negative colors must be applied *after* that theme step; otherwise the
    orange negative bars/markers are overwritten and the chart looks unmodified.
    """
    styled = style_powerbi_figure(fig, **style_kwargs)
    _add_zero_baseline(styled)
    return _apply_negative_value_chart_style(styled)


def _safe_trace_values(trace):
    try:
        values = trace.y
    except Exception:
        return []
    if values is None:
        return []
    try:
        return pd.to_numeric(pd.Series(list(values)), errors="coerce").fillna(0).tolist()
    except Exception:
        return []


def _signed_bar_colors(values, positive="#76D7FF", negative=NEGATIVE_VALUE_COLOR):
    if values is None:
        raw_values = []
    else:
        try:
            raw_values = list(values)
        except Exception:
            raw_values = []
    safe_values = pd.to_numeric(pd.Series(raw_values), errors="coerce").fillna(0).tolist()
    return [negative if float(v) < 0 else positive for v in safe_values]


def _safe_ratio_value(value: Any, default: float = 0.0) -> float:
    number = _safe_float(value, default)
    if number > 1 and number <= 100:
        number = number / 100.0
    return min(max(number, 0.0), 1.0)


def _ensure_scenario_manual_columns(result_df: pd.DataFrame) -> pd.DataFrame:
    if result_df is None or result_df.empty:
        return pd.DataFrame()
    out = result_df.copy()
    if "月份" not in out.columns:
        out["月份"] = [f"{i + 1}月" for i in range(len(out))]
    if "補班日" not in out.columns:
        out["補班日"] = 0
    out["補班日"] = pd.to_numeric(out["補班日"], errors="coerce").fillna(0)
    if "可用總人力" not in out.columns:
        out["可用總人力"] = 0
    out["可用總人力"] = pd.to_numeric(out["可用總人力"], errors="coerce").fillna(0).clip(lower=0)
    if "直接有效人力" not in out.columns:
        out["直接有效人力"] = 0
    out["直接有效人力"] = pd.to_numeric(out["直接有效人力"], errors="coerce").fillna(0)
    if "情境基準正常工作日" not in out.columns:
        if "正常工作日" in out.columns:
            out["情境基準正常工作日"] = pd.to_numeric(out["正常工作日"], errors="coerce").fillna(0) - out["補班日"]
        else:
            out["情境基準正常工作日"] = 0
    out["情境基準正常工作日"] = pd.to_numeric(out["情境基準正常工作日"], errors="coerce").fillna(0).clip(lower=0)
    return out


def _recalculate_scenario_result_from_manual_edits(result_df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
    out = _ensure_scenario_manual_columns(result_df)
    if out.empty:
        return out

    daily_hours = max(_safe_float(params.get("daily_hours", 7.0), 7.0), 0.0)
    efficiency = max(_safe_float(params.get("efficiency", 1.0), 1.0), 0.0)
    weekday_ot_hours = max(_safe_float(params.get("weekday_overtime_hours", 2.0), 2.0), 0.0)
    sat_ot_hours = max(_safe_float(params.get("saturday_overtime_hours", 7.0), 7.0), 0.0)
    sun_ot_hours = max(_safe_float(params.get("sunday_overtime_hours", 7.0), 7.0), 0.0)
    holiday_ot_hours = max(_safe_float(params.get("holiday_overtime_hours", 7.0), 7.0), 0.0)
    weekday_ot_ratio = _safe_ratio_value(params.get("weekday_overtime_ratio", 0.3), 0.3)
    holiday_ot_ratio = _safe_ratio_value(params.get("holiday_overtime_ratio", 0.3), 0.3)
    weekday_ot_day_ratio = _safe_ratio_value(params.get("weekday_overtime_day_ratio", 1.0), 1.0)
    holiday_ot_day_ratio = _safe_ratio_value(params.get("holiday_overtime_day_ratio", 1.0), 1.0)
    leave_ratio = _safe_ratio_value(params.get("leave_ratio", 0.0), 0.0)

    out["補班日"] = pd.to_numeric(out.get("補班日", 0), errors="coerce").fillna(0).clip(lower=0)
    out["直接有效人力"] = pd.to_numeric(out.get("直接有效人力", 0), errors="coerce").fillna(0).clip(lower=0)
    if "正常工作日" not in out.columns:
        out["正常工作日"] = pd.to_numeric(out.get("情境基準正常工作日", 0), errors="coerce").fillna(0).clip(lower=0) + out["補班日"]
    out["正常工作日"] = pd.to_numeric(out["正常工作日"], errors="coerce").fillna(0).clip(lower=0)

    for col in ["週六天數", "週日天數", "法定假日", "原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "需求總工時"]:
        if col not in out.columns:
            out[col] = 0
        numeric = pd.to_numeric(out[col], errors="coerce").fillna(0)
        out[col] = numeric if col == "調整工時" else numeric.clip(lower=0)

    # 09 manual simulation treats user-edited 需求總工時 as the final demand for this scenario.
    # It does not force-rebuild demand from 原始需求工時 + 調整工時, so the user can simulate
    # temporary demand values without changing 04/05/06 authority data.
    direct_people = out["直接有效人力"].clip(lower=0)
    leave_people = (direct_people * leave_ratio).clip(lower=0)
    effective_people = (direct_people - leave_people).clip(lower=0)
    out["請假比例"] = leave_ratio
    out["請假扣除人力"] = leave_people
    out["扣請假後有效人力"] = effective_people

    normal_days = pd.to_numeric(out["正常工作日"], errors="coerce").fillna(0).clip(lower=0)
    sat_days = pd.to_numeric(out.get("週六天數", 0), errors="coerce").fillna(0).clip(lower=0)
    sun_days = pd.to_numeric(out.get("週日天數", 0), errors="coerce").fillna(0).clip(lower=0)
    holiday_days = pd.to_numeric(out.get("法定假日", 0), errors="coerce").fillna(0).clip(lower=0)
    out["平日加班有效天數"] = (normal_days * weekday_ot_day_ratio).clip(lower=0)
    out["週六加班有效天數"] = (sat_days * holiday_ot_day_ratio).clip(lower=0)
    out["週日加班有效天數"] = (sun_days * holiday_ot_day_ratio).clip(lower=0)
    out["法定假日加班有效天數"] = (holiday_days * holiday_ot_day_ratio).clip(lower=0)

    out["正常可用工時"] = (effective_people * normal_days * daily_hours * efficiency).clip(lower=0)
    out["平日加班工時"] = (effective_people * weekday_ot_ratio * out["平日加班有效天數"] * weekday_ot_hours * efficiency).clip(lower=0)
    out["週六加班工時"] = (effective_people * holiday_ot_ratio * out["週六加班有效天數"] * sat_ot_hours * efficiency).clip(lower=0)
    out["週日加班工時"] = (effective_people * holiday_ot_ratio * out["週日加班有效天數"] * sun_ot_hours * efficiency).clip(lower=0)
    out["法定假日加班工時"] = (effective_people * holiday_ot_ratio * out["法定假日加班有效天數"] * holiday_ot_hours * efficiency).clip(lower=0)
    overtime_cols = ["平日加班工時", "週六加班工時", "週日加班工時", "法定假日加班工時"]
    out["加班增加工時"] = out[overtime_cols].sum(axis=1).clip(lower=0)
    out["含加班可用工時"] = out["正常可用工時"] + out["加班增加工時"]

    out["正常產能負荷"] = out["正常可用工時"] - out["需求總工時"]
    out["含加班產能負荷"] = out["含加班可用工時"] - out["需求總工時"]
    out["正常稼動率"] = (out["需求總工時"] / out["正常可用工時"].replace(0, pd.NA)).fillna(0)
    out["含加班稼動率"] = (out["需求總工時"] / out["含加班可用工時"].replace(0, pd.NA)).fillna(0)
    person_capacity = normal_days * daily_hours * efficiency
    out["需求人力"] = (out["需求總工時"] / person_capacity.replace(0, pd.NA)).fillna(0)
    out["人力差異"] = out["扣請假後有效人力"] - out["需求人力"]
    out["缺工工時"] = (out["需求總工時"] - out["含加班可用工時"]).clip(lower=0)
    out["缺工天數"] = (out["缺工工時"] / (out["直接有效人力"].replace(0, pd.NA) * max(daily_hours, 1e-9))).fillna(0)

    warning_utilization = _safe_float(params.get("warning_utilization", 0.85), 0.85)
    danger_utilization = _safe_float(params.get("danger_utilization", 1.0), 1.0)
    red_utilization = _safe_float(params.get("red_utilization", 1.1), 1.1)
    def _status(x: float) -> str:
        return "紅燈" if x >= red_utilization else "橘燈" if x >= danger_utilization else "黃燈" if x >= warning_utilization else "綠燈"
    out["狀態"] = out["含加班稼動率"].map(_status)
    return out


def _build_manpower_analysis_from_result(result_df: pd.DataFrame, target_utilization: float) -> pd.DataFrame:
    result_df = _ensure_result_display_columns(result_df)
    if result_df.empty:
        return pd.DataFrame()
    analysis = result_df.copy()
    direct_people = pd.to_numeric(analysis.get("直接有效人力", 0), errors="coerce").replace(0, pd.NA)
    per_person_capacity = pd.to_numeric(analysis.get("含加班可用工時", 0), errors="coerce") / direct_people
    target_capacity = per_person_capacity * float(target_utilization)
    analysis["目標達標需求人力"] = (pd.to_numeric(analysis.get("需求總工時", 0), errors="coerce") / target_capacity).fillna(0)
    analysis["建議補人(人)"] = (analysis["目標達標需求人力"] - pd.to_numeric(analysis.get("直接有效人力", 0), errors="coerce").fillna(0)).clip(lower=0)
    analysis["建議補人(人)"] = analysis["建議補人(人)"].apply(lambda x: int(x) if abs(float(x) - int(float(x))) < 0.01 else int(float(x)) + 1)
    analysis["正常稼動率(%)"] = pd.to_numeric(analysis.get("正常稼動率", 0), errors="coerce").fillna(0) * 100
    analysis["含加班稼動率(%)"] = pd.to_numeric(analysis.get("含加班稼動率", 0), errors="coerce").fillna(0) * 100
    cols = ["月份", "原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "需求總工時", "可用總人力", "直接有效人力", "需求人力", "目標達標需求人力", "建議補人(人)", "正常產能負荷", "含加班產能負荷", "人力差異", "缺工天數", "正常稼動率(%)", "含加班稼動率(%)", "狀態"]
    return analysis[[c for c in cols if c in analysis.columns]].copy()


def _update_comparison_with_result(comparison_df: pd.DataFrame, result_df: pd.DataFrame) -> pd.DataFrame:
    if comparison_df is None or comparison_df.empty or result_df is None or result_df.empty or "月份" not in result_df.columns:
        return comparison_df.copy() if comparison_df is not None else pd.DataFrame()
    out = comparison_df.copy()
    update_cols = ["原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "需求總工時", "正常可用工時", "含加班可用工時", "正常稼動率", "含加班稼動率", "正常產能負荷", "含加班產能負荷", "需求人力", "可用總人力", "直接有效人力", "人力差異", "缺工天數", "狀態"]
    keep_cols = [c for c in update_cols if c in out.columns]
    if keep_cols:
        out = out.drop(columns=keep_cols)
    merge_cols = ["月份"] + [c for c in update_cols if c in result_df.columns]
    return out.merge(result_df[merge_cols], on="月份", how="left")


def _scenario_editor_display_df(result_df: pd.DataFrame) -> pd.DataFrame:
    out = _with_percent_utilization_columns(_ensure_scenario_manual_columns(result_df))
    preferred = [
        "年份", "月份", "原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "需求總工時", "可用總人力", "直接有效人力", "正常工作日", "週六天數", "週日天數", "法定假日", "補班日",
        "扣請假後有效人力", "正常可用工時", "含加班可用工時", "需求人力", "人力差異",
        "正常稼動率(%)", "含加班稼動率(%)", "正常產能負荷", "含加班產能負荷",
        "狀態",
    ]
    cols = [c for c in preferred if c in out.columns]
    extra = [c for c in out.columns if c not in cols and c != "情境基準正常工作日"]
    return out[cols + extra].copy()


def _apply_data_editor_latest_changes(base_df: pd.DataFrame, returned_df: Any, widget_key: str, editable_cols: set[str]) -> pd.DataFrame:
    """Merge the visible editor return value with Streamlit widget state.

    Streamlit data_editor can report edits in several shapes depending on version
    and whether it is inside a form.  This helper intentionally supports all
    common shapes so the recalculation always uses the value the user typed, such
    as 需求總工時 = 9999, instead of the stale value from the previous payload.
    """
    if base_df is None or base_df.empty:
        return pd.DataFrame()

    try:
        if isinstance(returned_df, pd.DataFrame):
            merged = returned_df.copy()
        elif hasattr(returned_df, "data") and isinstance(returned_df.data, pd.DataFrame):
            merged = returned_df.data.copy()
        else:
            merged = base_df.copy()
    except Exception:
        merged = base_df.copy()

    if len(merged) != len(base_df):
        merged = base_df.copy()
    for col in base_df.columns:
        if col not in merged.columns:
            merged[col] = base_df[col].values
    merged = merged[list(base_df.columns)].copy()

    def _set_cell(row_idx: int, col_name: str, value: Any) -> None:
        if row_idx < 0 or row_idx >= len(merged):
            return
        if col_name not in editable_cols or col_name not in merged.columns:
            return
        try:
            merged.iat[row_idx, merged.columns.get_loc(col_name)] = value
        except Exception:
            # Avoid Arrow/string extension dtype assignment failures by falling back
            # to object dtype for this column, then set the edited value.
            try:
                merged[col_name] = merged[col_name].astype("object")
                merged.iat[row_idx, merged.columns.get_loc(col_name)] = value
            except Exception:
                pass

    state = st.session_state.get(widget_key, {})
    if not isinstance(state, dict):
        return merged

    edited_rows = state.get("edited_rows") or {}
    if isinstance(edited_rows, dict):
        for raw_row, changes in edited_rows.items():
            if not isinstance(changes, dict):
                continue
            try:
                row_idx = int(raw_row)
            except Exception:
                continue
            for col, value in changes.items():
                _set_cell(row_idx, str(col), value)

    edited_cells = state.get("edited_cells") or {}
    if isinstance(edited_cells, dict):
        for raw_key, value in edited_cells.items():
            row_idx = None
            col_name = None
            if isinstance(raw_key, tuple) and len(raw_key) >= 2:
                try:
                    row_idx = int(raw_key[0])
                    col_name = str(raw_key[1])
                except Exception:
                    row_idx = None
            else:
                key_text = str(raw_key)
                for sep in (":", "_", ","):
                    if sep in key_text:
                        left, right = key_text.split(sep, 1)
                        try:
                            row_idx = int(left)
                            col_name = right
                        except Exception:
                            row_idx = None
                        break
            if row_idx is not None and col_name is not None:
                _set_cell(row_idx, col_name, value)

    # Final numeric normalization for editable fields. This is important when the
    # editor returns strings like "9,999" or "9999.0".
    for col in editable_cols:
        if col in merged.columns:
            merged[col] = parse_numeric_display_series(merged[col]).fillna(0.0)
    return merged


def _detect_manual_changed_cells(before_df: pd.DataFrame, after_df: pd.DataFrame, editable_cols: set[str]) -> list[str]:
    """Return short descriptions of manual edits for user feedback."""
    if before_df is None or after_df is None or before_df.empty or after_df.empty:
        return []
    notes: list[str] = []
    row_count = min(len(before_df), len(after_df))
    for row_idx in range(row_count):
        month = str(after_df.iloc[row_idx].get("月份", before_df.iloc[row_idx].get("月份", row_idx + 1)))
        for col in editable_cols:
            if col not in before_df.columns or col not in after_df.columns:
                continue
            before_val = before_df.iloc[row_idx][col]
            after_val = after_df.iloc[row_idx][col]
            try:
                b = float(pd.to_numeric(pd.Series([before_val]), errors="coerce").fillna(0).iloc[0])
                a = float(pd.to_numeric(pd.Series([after_val]), errors="coerce").fillna(0).iloc[0])
                changed = abs(a - b) > 1e-9
                before_text = f"{b:g}"
                after_text = f"{a:g}"
            except Exception:
                changed = str(before_val) != str(after_val)
                before_text = str(before_val)
                after_text = str(after_val)
            if changed:
                notes.append(f"{month}：{col} {before_text} → {after_text}")
            if len(notes) >= 12:
                notes.append("...其餘修改已套用")
                return notes
    return notes


def _render_scenario_manual_editor(active_payload: dict[str, Any], latest_result_df: pd.DataFrame, active_target_value: float) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if latest_result_df is None or latest_result_df.empty:
        return active_payload, latest_result_df, pd.DataFrame(), _df_from_records(active_payload.get("comparison", []))
    st.subheader("情境月別資料修正與重新計算")
    st.caption("基準資料會顯示原始需求工時、產能計算排除工時、排除後需求工時與需求總工時；所有負荷評估使用需求總工時。此處可手動輸入情境最終需求總工時、直接有效人力與工作日參數；情境修改只影響目前模擬，不會回寫 01/02、04、05、06、07 權威資料。")

    current_result = _ensure_scenario_manual_columns(latest_result_df)
    editor_df = _scenario_editor_display_df(current_result)
    editable_cols = {"需求總工時", "直接有效人力", "正常工作日", "週六天數", "週日天數", "法定假日", "補班日", "調整工時"}
    disabled_cols = [c for c in editor_df.columns if c not in editable_cols]
    column_config = {}
    for col in editor_df.columns:
        if col in editable_cols:
            if col == "調整工時":
                column_config[col] = st.column_config.NumberColumn(col, step=0.1, format="%g")
            elif col == "需求總工時":
                column_config[col] = st.column_config.NumberColumn(col, min_value=0.0, step=0.5, format="%g")
            elif col == "直接有效人力":
                column_config[col] = st.column_config.NumberColumn(col, min_value=0.0, step=0.1, format="%.1f")
            else:
                column_config[col] = st.column_config.NumberColumn(col, min_value=0.0, step=1.0, format="%.0f")

    editor_key = "scenario_manual_result_editor_stable"
    editor_display_df = format_numeric_editor_display_dataframe(editor_df, editable_columns=editable_cols)
    styled_editor_df = _style_negative_values(editor_display_df)
    with st.form("scenario_manual_result_edit_form", clear_on_submit=False):
        edited_display = st.data_editor(
            styled_editor_df,
            key=editor_key,
            use_container_width=True,
            height=420,
            hide_index=True,
            num_rows="fixed",
            disabled=disabled_cols,
            column_config=column_config,
        )
        save_manual = st.form_submit_button("儲存情境表格修正並重新計算", type="primary")

    comparison_df = _df_from_records(active_payload.get("comparison", []))
    if save_manual:
        latest_display = _apply_data_editor_latest_changes(editor_df, edited_display, editor_key, editable_cols)
        change_notes = _detect_manual_changed_cells(editor_df, latest_display, editable_cols)

        edited_result = current_result.copy()
        for col in editable_cols:
            if col in latest_display.columns:
                edited_result[col] = pd.to_numeric(latest_display[col], errors="coerce").fillna(0)
        if "正常工作日" in edited_result.columns and "補班日" in edited_result.columns:
            edited_result["情境基準正常工作日"] = (
                pd.to_numeric(edited_result["正常工作日"], errors="coerce").fillna(0)
                - pd.to_numeric(edited_result["補班日"], errors="coerce").fillna(0)
            ).clip(lower=0)
        recalculated = _recalculate_scenario_result_from_manual_edits(edited_result, active_payload.get("params", {}) or {})
        manpower_df = _build_manpower_analysis_from_result(recalculated, active_target_value)
        comparison_df = _update_comparison_with_result(comparison_df, recalculated)

        new_payload = dict(active_payload)
        new_payload["manual_edited_at"] = datetime.now().isoformat(timespec="seconds")
        new_payload["manual_change_notes"] = change_notes
        new_payload["result"] = _safe_records(recalculated)
        new_payload["manpower_analysis"] = _safe_records(manpower_df)
        new_payload["comparison"] = _safe_records(comparison_df)
        st.session_state[LAST_SCENARIO_STATE_KEY] = new_payload
        # Persist the current manual simulation as the latest displayed baseline,
        # so a later rerun/Reboot will not fall back to stale scenario values.
        try:
            save_json(SCENARIO_LAST_CACHE_PATH, new_payload)
        except Exception:
            _write_last_payload_cache(new_payload)
        if change_notes:
            st.success("已依你手動修改的欄位重新計算目前情境。")
            st.caption("本次套用修改：" + "；".join(change_notes[:12]))
        else:
            st.info("沒有偵測到手動欄位變更；目前結果已重新整理。")
        st.rerun()

    manpower_df = _ensure_manpower_display_columns(_df_from_records(active_payload.get("manpower_analysis", [])), latest_result_df)
    return active_payload, latest_result_df, manpower_df, comparison_df


if active_payload and not latest_result.empty:
    active_payload, latest_result, manpower_analysis, comparison = _render_scenario_manual_editor(active_payload, latest_result, active_target)


if not manpower_analysis.empty:
    st.subheader("人力需求判斷")
    if "建議補人(人)" in manpower_analysis.columns:
        peak = manpower_analysis.sort_values("建議補人(人)", ascending=False).iloc[0]
        st.info(f"最大人力缺口月份：{peak['月份']}；依目標含加班產能負荷率 {active_target:.0%} 推估，建議補人 {peak['建議補人(人)']} 人。", icon="🧠")
    render_manpower_gap_cards(manpower_analysis)

    people_cols = [c for c in ["可用總人力", "直接有效人力", "需求人力", "目標達標需求人力"] if c in manpower_analysis.columns]
    fig_people = px.bar(manpower_analysis, x="月份", y=people_cols, barmode="group", title="人力需求模擬（人）")
    for idx, trace in enumerate(fig_people.data):
        vals = _safe_trace_values(trace)
        positive_color = ["#7DD3FC", "#C084FC", "#60A5FA"][idx % 3]
        trace.marker.color = _signed_bar_colors(vals, positive=positive_color, negative=NEGATIVE_VALUE_COLOR)
        trace.marker.opacity = 0.90
    _add_zero_baseline(fig_people)
    _apply_negative_value_chart_style(fig_people)
    render_powerbi_chart(_style_figure_with_negative_values(fig_people, height=430, yaxis_title="人力(人)", legend_title="指標"), key="scenario_people_chart")

    if all(c in manpower_analysis.columns for c in ["月份", "需求人力", "可用總人力", "直接有效人力", "人力差異"]):
        professional_gap = make_subplots(specs=[[{"secondary_y": True}]])
        professional_gap.add_trace(
            go.Bar(
                x=manpower_analysis["月份"],
                y=pd.to_numeric(manpower_analysis["需求人力"], errors="coerce").fillna(0).round(0),
                name="需求人力",
                marker_color="#A78BFA",
                opacity=0.82,
            ),
            secondary_y=False,
        )
        professional_gap.add_trace(
            go.Bar(
                x=manpower_analysis["月份"],
                y=pd.to_numeric(manpower_analysis["直接有效人力"], errors="coerce").fillna(0).round(0),
                name="直接有效人力",
                marker_color="#38BDF8",
                opacity=0.82,
            ),
            secondary_y=False,
        )
        gap_values = pd.to_numeric(manpower_analysis["人力差異"], errors="coerce").fillna(0).round(0)
        professional_gap.add_trace(
            go.Scatter(
                x=manpower_analysis["月份"],
                y=gap_values,
                mode="lines+markers+text",
                text=gap_values.astype("Int64"),
                textposition="top center",
                name="人力差異",
                line=dict(color="#FBBF24", width=3),
                marker=dict(size=9, color=_signed_bar_colors(gap_values.tolist(), positive="#34D399", negative=NEGATIVE_VALUE_COLOR)),
            ),
            secondary_y=True,
        )
        _add_zero_baseline(professional_gap)
        _apply_negative_value_chart_style(professional_gap)
        professional_gap.update_layout(title="需求人力 vs 直接有效人力 + 人力差異")
        professional_gap.update_yaxes(title_text="人力(人)", secondary_y=False, tickformat=",.0f")
        professional_gap.update_yaxes(title_text="人力差異(人)", secondary_y=True, tickformat=",.0f")
        render_powerbi_chart(_style_figure_with_negative_values(professional_gap, height=430, legend_title="指標"), key="scenario_professional_gap_chart")

    gap_fig = px.bar(manpower_analysis, x="月份", y="人力差異", title="每月人力差異：正數為多出、負數為缺少")
    gap_vals = pd.to_numeric(manpower_analysis["人力差異"], errors="coerce").fillna(0).tolist()
    gap_fig.update_traces(marker_color=_signed_bar_colors(gap_vals, positive="#39FF88", negative=NEGATIVE_VALUE_COLOR), marker_opacity=0.90)
    _add_zero_baseline(gap_fig)
    _apply_negative_value_chart_style(gap_fig)
    render_powerbi_chart(_style_figure_with_negative_values(gap_fig, height=380, yaxis_title="人力差異(人)", legend_title="缺口/餘裕"), key="scenario_gap_chart")

    util_cols = [c for c in ["正常稼動率(%)", "含加班稼動率(%)"] if c in manpower_analysis.columns]
    util_fig = make_subplots(specs=[[{"secondary_y": True}]])
    for _col in util_cols:
        display_name = _col.replace("稼動率", "產能負荷率")
        util_fig.add_trace(go.Scatter(x=manpower_analysis["月份"], y=pd.to_numeric(manpower_analysis[_col], errors="coerce").fillna(0).round(0), mode="lines+markers", name=display_name), secondary_y=False)
    if "需求總工時" in manpower_analysis.columns:
        util_fig.add_trace(go.Bar(x=manpower_analysis["月份"], y=pd.to_numeric(manpower_analysis["需求總工時"], errors="coerce").fillna(0).round(0), name="需求總工時(h)", marker_color="#A855F7", opacity=0.58), secondary_y=True)
    util_fig.update_layout(title="正常班 vs 含加班產能負荷率 + 需求總工時")
    _add_zero_baseline(util_fig)
    _apply_negative_value_chart_style(util_fig)
    util_fig.update_yaxes(title_text="產能負荷率(%)", secondary_y=False, tickformat=",.0f")
    util_fig.update_yaxes(title_text="需求總工時(h)", secondary_y=True, tickformat=",.0f")
    render_powerbi_chart(_style_figure_with_negative_values(util_fig, height=380, legend_title="指標"), key="scenario_normal_ot_util_chart")

    load_chart_df = manpower_analysis.copy()
    if "正常產能負荷" in load_chart_df.columns:
        load_chart_df["正常產能餘額"] = load_chart_df["正常產能負荷"]
    if "含加班產能負荷" in load_chart_df.columns:
        load_chart_df["含加班產能餘額"] = load_chart_df["含加班產能負荷"]
    load_cols = [c for c in ["正常產能餘額", "含加班產能餘額"] if c in load_chart_df.columns]
    load_fig = px.bar(load_chart_df, x="月份", y=load_cols, barmode="group", title="正常班 vs 含加班產能餘額（h）")
    for trace in load_fig.data:
        vals = _safe_trace_values(trace)
        if trace.name == "正常產能餘額":
            trace.marker.color = _signed_bar_colors(vals, positive="#7DD3FC", negative=NEGATIVE_VALUE_COLOR)
        else:
            trace.marker.color = _signed_bar_colors(vals, positive="#60A5FA", negative=NEGATIVE_VALUE_COLOR)
        trace.marker.opacity = 0.92
    _add_zero_baseline(load_fig)
    _apply_negative_value_chart_style(load_fig)
    render_powerbi_chart(_style_figure_with_negative_values(load_fig, height=380, yaxis_title="產能餘額(h)", legend_title="指標"), key="scenario_normal_ot_load_chart")
    manpower_analysis_display = _rename_capacity_terms_for_display(_with_percent_utilization_columns(manpower_analysis))
    for _col in manpower_analysis_display.columns:
        if any(_key in _col for _key in ["工時", "負荷", "餘額", "產能負荷率", "天數", "人力", "補人", "差異"]):
            manpower_analysis_display[_col] = pd.to_numeric(manpower_analysis_display[_col], errors="coerce").round(0).astype("Int64")
    render_configurable_view(manpower_analysis_display, "scenario_manpower_analysis", "09. 人力需求判斷", height=420)

if not comparison.empty:
    st.subheader("情境與原始差異")
    comparison_display = _rename_capacity_terms_for_display(_with_percent_utilization_columns(comparison))
    for _col in comparison_display.columns:
        if any(_key in _col for _key in ["工時", "負荷", "餘額", "產能負荷率", "天數", "人力", "補人", "差異"]):
            comparison_display[_col] = pd.to_numeric(comparison_display[_col], errors="coerce").round(0).astype("Int64")
    render_configurable_view(comparison_display, "scenario_comparison", "09. 情境與原始差異", height=360)

if not latest_result.empty:
    fig = go.Figure()
    if "需求總工時" in latest_result.columns:
        fig.add_trace(go.Bar(
            x=latest_result["月份"],
            y=pd.to_numeric(latest_result["需求總工時"], errors="coerce").fillna(0).round(0),
            name="需求總工時",
            marker_color="#A855F7",
            opacity=0.62,
        ))
    for _name, _color in [("正常可用工時", "#7DD3FC"), ("含加班可用工時", "#60A5FA")]:
        if _name in latest_result.columns:
            fig.add_trace(go.Scatter(
                x=latest_result["月份"],
                y=pd.to_numeric(latest_result[_name], errors="coerce").fillna(0).round(0),
                mode="lines+markers",
                name=_name,
                line=dict(color=_color, width=3),
                marker=dict(size=8),
            ))
    _add_zero_baseline(fig)
    _apply_negative_value_chart_style(fig)
    fig.update_layout(title=f"{active_name}：需求總工時 vs 正常/含加班可用工時")
    render_powerbi_chart(_style_figure_with_negative_values(fig, height=430, yaxis_title="工時(h)", legend_title="指標"), key="scenario_result_chart")
    latest_result_display = _rename_capacity_terms_for_display(_with_percent_utilization_columns(latest_result))
    for _col in latest_result_display.columns:
        if any(_key in _col for _key in ["工時", "負荷", "餘額", "產能負荷率", "天數", "人力", "機台", "差異"]):
            latest_result_display[_col] = pd.to_numeric(latest_result_display[_col], errors="coerce").round(0).astype("Int64")
    render_configurable_view(latest_result_display, "scenario_result", "09. 情境模擬結果", height=420)

st.subheader("已保存情境")
if runs:
    runs_df = pd.DataFrame([
        {
            "名稱": r.get("name"),
            "建立時間": r.get("created_at"),
            "年份": r.get("year", ""),
            "新增正職": r.get("add_regular_people", 0),
            "新增派遣": r.get("add_dispatch_people", 0),
            "新增外包": r.get("add_outsource_people", 0),
            "扣除人力": r.get("project_deduct_people", 0),
            "淨增有效人力": r.get("extra_people"),
            "目標產能負荷率(%)": round(_display_percent_value(r.get("target_utilization", 0)), 0),
            "工時倍率": r.get("standard_hour_factor", 1),
            "訂單倍率": r.get("order_factor", 1),
        }
        for r in runs
    ])
    # Saved scenario list is a summary table; show one decimal place only to avoid 0.000000 noise.
    saved_numeric_cols = [
        "新增正職", "新增派遣", "新增外包", "扣除人力",
        "淨增有效人力", "目標產能負荷率(%)", "工時倍率", "訂單倍率",
    ]
    for _col in saved_numeric_cols:
        if _col in runs_df.columns:
            runs_df[_col] = pd.to_numeric(runs_df[_col], errors="coerce").fillna(0).round(1)

    # st.dataframe may still render float columns with its default 6-decimal formatter.
    # Use a display-only copy with explicit one-decimal strings, while keeping runs_df numeric
    # for the module export below.
    render_configurable_view(runs_df, "scenario_saved", "09. 已保存情境", height=320)
else:
    runs_df = pd.DataFrame()
    st.caption("目前尚未保存情境。")

st.subheader("09. 模組完整匯出")
latest_result_export = _rename_capacity_terms_for_display(_with_percent_utilization_columns(latest_result)) if not latest_result.empty else latest_result
manpower_analysis_export = _rename_capacity_terms_for_display(_with_percent_utilization_columns(manpower_analysis)) if not manpower_analysis.empty else manpower_analysis
comparison_export = _rename_capacity_terms_for_display(_with_percent_utilization_columns(comparison)) if not comparison.empty else comparison
export_sheets = {"已保存情境": runs_df, "最後情境結果": latest_result_export, "人力需求判斷": manpower_analysis_export, "情境與原始差異": comparison_export}
chart_specs = []
if not manpower_analysis.empty and "月份" in manpower_analysis.columns:
    chart_specs.append(chart_spec("bar", "人力需求模擬", "人力需求判斷", "月份", ["可用總人力", "直接有效人力", "需求人力", "目標達標需求人力"]))
    chart_specs.append(chart_spec("line", "正常班與含加班產能負荷率", "人力需求判斷", "月份", ["正常產能負荷率(%)", "含加班產能負荷率(%)"]))
    chart_specs.append(chart_spec("bar", "正常班與含加班產能餘額", "人力需求判斷", "月份", ["正常產能餘額", "含加班產能餘額"]))
if not latest_result.empty and "月份" in latest_result.columns:
    chart_specs.append(chart_spec("line", "最後情境需求 vs 正常/含加班可用工時", "最後情境結果", "月份", ["需求總工時", "正常可用工時", "含加班可用工時"]))
    chart_specs.append(chart_spec("bar", "最後情境人力差異", "最後情境結果", "月份", ["人力差異"]))
render_module_report_download(
    "09.情境模擬",
    export_sheets,
    chart_specs=chart_specs,
    metadata={"模組": "09. 情境模擬", "目前情境名稱": active_name, "模擬年份": active_year, "目標含加班產能負荷率": f"{active_target:.0%}"},
    key="export_scenario_module",
)

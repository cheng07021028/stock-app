from __future__ import annotations

from datetime import date
from pathlib import Path
import json
from typing import Any

import numpy as np
import pandas as pd

from .year_service import DEFAULT_YEAR, ensure_year_column, filter_by_year, normalize_year

try:
    from .config import PERSISTENT_DIR
except Exception:
    PERSISTENT_DIR = Path(__file__).resolve().parents[1] / "data" / "persistent"

MONTH_ORDER = [f"{i}月" for i in range(1, 13)]
ASSEMBLY_EXCLUSION_PARAM_KEY = "excluded_assembly_locations"
CATEGORY_EXCLUSION_PARAM_KEY = "excluded_categories"
ASSEMBLY_LOCATION_HOURS_PARAM_KEY = "assembly_location_adjustment_hours"


def _normalize_text_value(value: Any, *, keep_line_breaks: bool = False) -> str:
    """Normalize one user-maintained text value for stable comparison."""
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).replace("\r\n", "\n").replace("\r", "\n").strip()
    if keep_line_breaks:
        return "\n".join(part.strip() for part in text.split("\n") if part.strip())
    return " ".join(part.strip() for part in text.replace("\n", " ").split() if part.strip())


def normalize_assembly_location(value: Any) -> str:
    """Normalize one 組立地點 value for comparison and display."""
    # Keep meaningful line breaks inside values such as 模冠/(聖豐組), but make
    # copy/paste whitespace stable for matching.
    return _normalize_text_value(value, keep_line_breaks=True)


def normalize_category(value: Any) -> str:
    """Normalize one Category value for capacity-exclusion comparison."""
    return _normalize_text_value(value, keep_line_breaks=False)


def normalize_assembly_location_list(values: Any) -> list[str]:
    """Return a stable de-duplicated list of excluded 組立地點 values."""
    if values is None:
        return []
    if isinstance(values, dict):
        raw_values = values.get(ASSEMBLY_EXCLUSION_PARAM_KEY) or values.get("assembly_location_exclusions") or []
    elif isinstance(values, (list, tuple, set, pd.Series)):
        raw_values = list(values)
    else:
        raw_values = [values]
    seen: set[str] = set()
    result: list[str] = []
    for item in raw_values:
        text = normalize_assembly_location(item)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return sorted(result)


def normalize_category_list(values: Any) -> list[str]:
    """Return a stable de-duplicated list of excluded Category values."""
    if values is None:
        return []
    if isinstance(values, dict):
        raw_values = values.get(CATEGORY_EXCLUSION_PARAM_KEY) or values.get("category_exclusions") or values.get("excluded_capacity_categories") or []
    elif isinstance(values, (list, tuple, set, pd.Series)):
        raw_values = list(values)
    else:
        raw_values = [values]
    seen: set[str] = set()
    result: list[str] = []
    for item in raw_values:
        text = normalize_category(item)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return sorted(result)


def normalize_assembly_location_hours_map(values: Any) -> dict[str, float]:
    """Return {normalized 組立地點: per-machine adjustment hours}.

    Backward-compatible reader for the historical「組立地點調整工時/台」設定。
    The value is retained for audit/display only. Current capacity calculation does
    not add it back: total demand is original demand minus excluded assembly-location
    hours plus the monthly Page 04 adjustment.
    """
    if values is None:
        return {}

    raw_values = values
    if isinstance(values, dict):
        for key in [
            ASSEMBLY_LOCATION_HOURS_PARAM_KEY,
            "assembly_location_hours",
            "assembly_adjustment_hours",
            "excluded_assembly_location_hours",
        ]:
            if key in values:
                raw_values = values.get(key)
                break

    result: dict[str, float] = {}

    if isinstance(raw_values, dict):
        for location, hours in raw_values.items():
            loc = normalize_assembly_location(location)
            if not loc:
                continue
            result[loc] = _safe_number(hours, 0.0, minimum=0.0)
        return result

    if isinstance(raw_values, pd.DataFrame):
        records = raw_values.to_dict(orient="records")
    elif isinstance(raw_values, (list, tuple, set)):
        records = list(raw_values)
    else:
        records = []

    for item in records:
        if not isinstance(item, dict):
            continue
        location = None
        for key in ["組立地點", "地點", "assembly_location", "location", "name"]:
            if key in item:
                location = item.get(key)
                break
        loc = normalize_assembly_location(location)
        if not loc:
            continue
        hours_value = 0.0
        for key in ["每台調整工時", "組立地點調整工時/台", "調整工時/台", "調整工時", "工時", "hours", "hour"]:
            if key in item:
                hours_value = item.get(key)
                break
        result[loc] = _safe_number(hours_value, 0.0, minimum=0.0)
    return result


def _false_mask(df: pd.DataFrame) -> pd.Series:
    return pd.Series(False, index=df.index, dtype=bool)


def schedule_assembly_exclusion_mask(schedule_df: pd.DataFrame, excluded_assembly_locations: Any = None) -> pd.Series:
    """Return rows whose 組立地點 should be excluded from capacity calculations."""
    if schedule_df is None or schedule_df.empty:
        return pd.Series(dtype=bool)
    excluded = set(normalize_assembly_location_list(excluded_assembly_locations))
    if not excluded or "組立地點" not in schedule_df.columns:
        return _false_mask(schedule_df)
    locations = schedule_df["組立地點"].map(normalize_assembly_location)
    return locations.isin(excluded)


def schedule_category_exclusion_mask(schedule_df: pd.DataFrame, excluded_categories: Any = None) -> pd.Series:
    """Return rows whose Category should be excluded from capacity calculations."""
    if schedule_df is None or schedule_df.empty:
        return pd.Series(dtype=bool)
    excluded = set(normalize_category_list(excluded_categories))
    if not excluded or "Category" not in schedule_df.columns:
        return _false_mask(schedule_df)
    categories = schedule_df["Category"].map(normalize_category)
    return categories.isin(excluded)


def schedule_capacity_exclusion_mask(
    schedule_df: pd.DataFrame,
    excluded_assembly_locations: Any = None,
    excluded_categories: Any = None,
) -> pd.Series:
    """Return rows excluded by either 組立地點 or Category capacity rules."""
    if schedule_df is None or schedule_df.empty:
        return pd.Series(dtype=bool)
    mask = _false_mask(schedule_df)
    assembly_mask = schedule_assembly_exclusion_mask(schedule_df, excluded_assembly_locations)
    category_mask = schedule_category_exclusion_mask(schedule_df, excluded_categories)
    if not assembly_mask.empty:
        mask = mask | assembly_mask.reindex(schedule_df.index, fill_value=False)
    if not category_mask.empty:
        mask = mask | category_mask.reindex(schedule_df.index, fill_value=False)
    return mask


def apply_capacity_exclusions(
    schedule_df: pd.DataFrame,
    excluded_assembly_locations: Any = None,
    excluded_categories: Any = None,
    assembly_location_hours: Any = None,
) -> pd.DataFrame:
    """Apply capacity exclusions while preserving both original and effective demand.

    Unified demand-hour definitions used by 00/04/05/06/09 and exports:
    - 原始需求工時：台數 × 標準工時，尚未扣除任何「產能計算排除的組立地點」。
    - 產能計算排除工時：命中組立地點排除規則而需要扣除的原始工時。
    - 排除後需求工時／需求工時：原始需求工時 − 產能計算排除工時。
    - Category 排除只影響機台計數，不扣除需求工時。

    ``組立地點調整工時/台`` 與 ``組立地點調整需求工時`` 保留為歷史／備查
    欄位，但不再加回 04 產能負荷，避免「原始需求工時」與「需求總工時」
    因替代工時邏輯混淆。04 的最終需求總工時會以排除後需求工時再加上
    04 月別手動調整工時。
    """
    df = schedule_df.copy()
    if df.empty:
        return df

    for col, default in [
        ("產能計算排除", "否"),
        ("工時計算排除", "否"),
        ("台數計算排除", "否"),
        ("產能計算排除原因", ""),
        ("組立地點調整工時/台", 0.0),
        ("組立地點調整需求工時", 0.0),
        ("產能計算排除工時", 0.0),
    ]:
        if col not in df.columns:
            df[col] = default

    if "機台計數" in df.columns:
        machine_count = pd.to_numeric(df["機台計數"], errors="coerce").fillna(0).clip(lower=0)
        df["排除前機台計數"] = machine_count
    elif "排除前機台計數" not in df.columns:
        df["排除前機台計數"] = 0.0

    if "原始需求工時" in df.columns:
        original_hours = pd.to_numeric(df["原始需求工時"], errors="coerce").fillna(0.0).clip(lower=0)
    elif "需求工時" in df.columns:
        original_hours = pd.to_numeric(df["需求工時"], errors="coerce").fillna(0.0).clip(lower=0)
    else:
        original_hours = pd.Series(0.0, index=df.index, dtype=float)

    df["原始需求工時"] = original_hours.astype(float)
    df["排除前需求工時"] = df["原始需求工時"]  # legacy-compatible audit field
    df["產能計算排除工時"] = 0.0
    df["排除後需求工時"] = df["原始需求工時"]
    df["需求工時"] = df["排除後需求工時"]

    assembly_mask = schedule_assembly_exclusion_mask(df, excluded_assembly_locations)
    category_mask = schedule_category_exclusion_mask(df, excluded_categories)
    assembly_mask = assembly_mask.reindex(df.index, fill_value=False) if not assembly_mask.empty else _false_mask(df)
    category_mask = category_mask.reindex(df.index, fill_value=False) if not category_mask.empty else _false_mask(df)
    combined_mask = assembly_mask | category_mask

    df.loc[:, "產能計算排除"] = "否"
    df.loc[:, "工時計算排除"] = "否"
    df.loc[:, "台數計算排除"] = "否"
    df.loc[:, "產能計算排除原因"] = ""
    df.loc[:, "組立地點調整工時/台"] = 0.0
    df.loc[:, "組立地點調整需求工時"] = 0.0

    if combined_mask.any():
        reasons = pd.Series("", index=df.index, dtype=object)
        if assembly_mask.any():
            locations = (
                df.loc[assembly_mask, "組立地點"].map(normalize_assembly_location)
                if "組立地點" in df.columns
                else pd.Series("", index=df.index[assembly_mask])
            )
            reasons.loc[assembly_mask] = locations.map(
                lambda value: f"組立地點排除工時：{value}" if value else "組立地點排除工時"
            )
        if category_mask.any():
            categories = (
                df.loc[category_mask, "Category"].map(normalize_category)
                if "Category" in df.columns
                else pd.Series("", index=df.index[category_mask])
            )
            category_reasons = categories.map(
                lambda value: f"Category排除台數：{value}" if value else "Category排除台數"
            )
            for idx, reason in category_reasons.items():
                existing = str(reasons.loc[idx]).strip()
                reasons.loc[idx] = f"{existing}；{reason}" if existing else reason

        df.loc[combined_mask, "產能計算排除"] = "是"
        df.loc[assembly_mask, "工時計算排除"] = "是"
        df.loc[category_mask, "台數計算排除"] = "是"
        df.loc[combined_mask, "產能計算排除原因"] = reasons.loc[combined_mask]

    if assembly_mask.any():
        excluded_hours = pd.to_numeric(df.loc[assembly_mask, "原始需求工時"], errors="coerce").fillna(0).clip(lower=0)
        df.loc[assembly_mask, "產能計算排除工時"] = excluded_hours.to_numpy()
        df.loc[assembly_mask, "排除後需求工時"] = 0.0
        df.loc[assembly_mask, "需求工時"] = 0.0

        # Historical/reference-only values. They remain visible for audit but do not
        # participate in 04/09/home capacity calculations.
        location_hours_map = normalize_assembly_location_hours_map(assembly_location_hours)
        locations = (
            df.loc[assembly_mask, "組立地點"].map(normalize_assembly_location)
            if "組立地點" in df.columns
            else pd.Series("", index=df.index[assembly_mask])
        )
        per_machine_hours = locations.map(lambda value: location_hours_map.get(value, 0.0)).astype(float)
        base_quantity = pd.to_numeric(df.loc[assembly_mask, "台數"], errors="coerce").fillna(0).clip(lower=0)
        reference_hours = (base_quantity * per_machine_hours).fillna(0).clip(lower=0)
        df.loc[assembly_mask, "組立地點調整工時/台"] = per_machine_hours.to_numpy()
        df.loc[assembly_mask, "組立地點調整需求工時"] = reference_hours.to_numpy()

    if "機台計數" in df.columns and category_mask.any():
        df.loc[category_mask, "機台計數"] = 0.0

    # Rebuild the effective demand from the explicit audit columns to ensure stale
    # authority values can never survive a recalculation.
    df["原始需求工時"] = pd.to_numeric(df["原始需求工時"], errors="coerce").fillna(0).clip(lower=0)
    df["產能計算排除工時"] = pd.to_numeric(df["產能計算排除工時"], errors="coerce").fillna(0).clip(lower=0)
    df["排除後需求工時"] = (df["原始需求工時"] - df["產能計算排除工時"]).clip(lower=0)
    df["需求工時"] = df["排除後需求工時"]
    return df

def apply_assembly_location_exclusion(
    schedule_df: pd.DataFrame,
    excluded_assembly_locations: Any = None,
    assembly_location_hours: Any = None,
) -> pd.DataFrame:
    """Backward-compatible wrapper for the original 組立地點-only exclusion API."""
    return apply_capacity_exclusions(
        schedule_df,
        excluded_assembly_locations=excluded_assembly_locations,
        excluded_categories=None,
        assembly_location_hours=assembly_location_hours,
    )

def _to_float(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(default)


def _safe_number(value: Any, default: float = 0.0, minimum: float | None = None, maximum: float | None = None) -> float:
    """Return a bounded float for capacity parameters.

    Streamlit settings are user-maintained, so this protects capacity
    calculations from blanks, text, negative numbers, and malformed values.
    """
    try:
        if value is None:
            number = default
        elif isinstance(value, str):
            text = value.strip().replace(",", "")
            if text.endswith("%"):
                number = float(text[:-1]) / 100.0
            else:
                number = float(text)
        else:
            number = float(value)
    except Exception:
        number = default
    if minimum is not None:
        number = max(minimum, number)
    if maximum is not None:
        number = min(maximum, number)
    return number


def _safe_ratio(value: Any, default: float = 0.0) -> float:
    """Return a 0~1 ratio. Values like 30 are treated as 30%."""
    number = _safe_number(value, default, minimum=0.0)
    if number > 1.0 and number <= 100.0:
        number = number / 100.0
    return min(max(number, 0.0), 1.0)



def _truthy_direct(value: Any, default: bool = True) -> bool:
    """Return whether a roster row is direct manpower.

    是否直接人力只用於「直接有效人力」判斷：
    可用總人力計全部在職製造人力；直接有效人力只計直接人力並乘可用比例。
    """
    try:
        if value is None or (not isinstance(value, str) and pd.isna(value)):
            return default
    except Exception:
        if value is None:
            return default
    text = str(value).strip().lower()
    if not text or text in {"nan", "none", "nat", "<na>"}:
        return default
    yes_values = {"是", "y", "yes", "true", "1", "直接", "直接人力", "direct"}
    no_values = {"否", "n", "no", "false", "0", "間接", "非直接", "indirect"}
    if text in yes_values:
        return True
    if text in no_values:
        return False
    return default



def _parse_roster_date_series(series: pd.Series, index: pd.Index | None = None) -> pd.Series:
    if series is None:
        return pd.Series(pd.NaT, index=index, dtype="datetime64[ns]")
    parsed = pd.to_datetime(series, errors="coerce")
    if isinstance(parsed, pd.Series):
        return parsed
    return pd.Series(parsed, index=index, dtype="datetime64[ns]")


def resolve_manpower_as_of_date(target_year: int | str | None = None, today: date | pd.Timestamp | None = None) -> pd.Timestamp:
    """Return the reference date used by annual/personnel manpower summaries.

    Current-year summaries use today so future hires are not counted early and past
    resignations are removed immediately. Past/future selected years use Dec 31 of
    that year so historical/future annual filters remain meaningful.
    """
    today_ts = pd.Timestamp(today or date.today()).normalize()
    if target_year in (None, "全部", "All", "all"):
        return today_ts
    year_value = normalize_year(target_year, int(today_ts.year))
    if int(year_value) == int(today_ts.year):
        return today_ts
    return pd.Timestamp(int(year_value), 12, 31)


def employment_active_mask(df: pd.DataFrame, as_of_date: date | pd.Timestamp | None = None) -> pd.Series:
    """Return rows employed on the reference date.

    Rule: 到職日當天起列入；離職日當天起不再列入。
    Blank 離職日 means the person is still employed. Blank 到職日 is treated as
    already employed, preserving legacy rows that did not have hire dates.
    """
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    as_of = pd.Timestamp(as_of_date or date.today()).normalize()
    hire_col = _first_existing_column(df, ["到職日", "到職日期", "入職日", "入職日期", "到任日", "進場日", "報到日"])
    leave_col = _first_existing_column(df, ["離職日", "離職日期", "退職日", "退場日", "離場日"])
    if hire_col:
        hire_dates = _parse_roster_date_series(df[hire_col], df.index)
    else:
        hire_dates = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    if leave_col:
        leave_dates = _parse_roster_date_series(df[leave_col], df.index)
    else:
        leave_dates = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    # 離職日是正式退場日：離職日當天起不再列入人力。
    mask = (hire_dates.isna() | hire_dates.le(as_of)) & (leave_dates.isna() | leave_dates.gt(as_of))
    return mask.fillna(False).astype(bool)


def _active_people_mask(df: pd.DataFrame) -> pd.Series:
    """Return active roster rows using common status columns."""
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    mask = pd.Series(True, index=df.index)
    disabled_values = {"否", "n", "no", "false", "0", "停用", "離職", "離場", "不啟用", "disabled"}
    for col in ["啟用", "在職", "狀態"]:
        if col not in df.columns:
            continue
        values = df[col].fillna("是").astype(str).str.strip().str.lower()
        mask &= ~values.isin(disabled_values)
    return mask


def _first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None



DEFAULT_TENURE_RATIO_RULES = [
    {"啟用": True, "區間名稱": "0~3個月", "起始月": 0, "結束月": "3", "可用比例": 0.3, "備註": "新進熟悉期"},
    {"啟用": True, "區間名稱": "4~6個月", "起始月": 4, "結束月": "6", "可用比例": 0.5, "備註": "可部分獨立作業"},
    {"啟用": True, "區間名稱": "7~12個月", "起始月": 7, "結束月": "12", "可用比例": 0.8, "備註": "逐步接近穩定產出"},
    {"啟用": True, "區間名稱": "13個月以上", "起始月": 13, "結束月": "", "可用比例": 1.0, "備註": "成熟人力"},
]

_TENURE_RATIO_SETTINGS_CACHE: dict[str, Any] | None = None


def _is_enabled_rule_value(value: Any, default: bool = True) -> bool:
    try:
        if value is None or (not isinstance(value, str) and pd.isna(value)):
            return default
    except Exception:
        if value is None:
            return default
    text = str(value).strip().lower()
    if not text or text in {"nan", "none", "nat", "<na>"}:
        return default
    return text not in {"否", "n", "no", "false", "0", "停用", "disabled"}


def _tenure_rule_end_month(value: Any) -> int | None:
    try:
        if value is None or (not isinstance(value, str) and pd.isna(value)):
            return None
    except Exception:
        if value is None:
            return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat", "<na>", "以上", "max", "∞"}:
        return None
    try:
        return max(int(float(text.replace(",", ""))), 0)
    except Exception:
        return None


def _safe_tenure_ratio(value: Any, default: float = 0.0) -> float:
    """Return a tenure-rule ratio while supporting both 0.3 and 30%.

    The 01/02 tenure UI allows ratios up to 1.5.  Therefore 1.2 should mean
    120% availability, while 30 or 30% should still mean 30%.
    """
    try:
        if value is None or (not isinstance(value, str) and pd.isna(value)):
            number = default
        else:
            text = str(value).strip().replace(",", "")
            if not text or text.lower() in {"nan", "none", "nat", "<na>"}:
                number = default
            elif text.endswith("%"):
                number = float(text[:-1].strip()) / 100.0
            else:
                number = float(text)
                if number > 1.5 and number <= 150:
                    number = number / 100.0
    except Exception:
        number = default
    return max(float(number), 0.0)


def _normalize_tenure_rules(raw_rules: Any) -> list[dict[str, float | int | None]]:
    if isinstance(raw_rules, pd.DataFrame):
        records = raw_rules.to_dict(orient="records")
    elif isinstance(raw_rules, list):
        records = [item for item in raw_rules if isinstance(item, dict)]
    else:
        records = DEFAULT_TENURE_RATIO_RULES
    result: list[dict[str, float | int | None]] = []
    for item in records:
        if not isinstance(item, dict) or not _is_enabled_rule_value(item.get("啟用"), True):
            continue
        start = int(_safe_number(item.get("起始月"), 0, minimum=0.0))
        end = _tenure_rule_end_month(item.get("結束月"))
        ratio = _safe_tenure_ratio(item.get("可用比例"), 0.0)
        result.append({"起始月": start, "結束月": end, "可用比例": ratio})
    if not result:
        return _normalize_tenure_rules(DEFAULT_TENURE_RATIO_RULES)
    return sorted(result, key=lambda row: (int(row.get("起始月") or 0), 10**6 if row.get("結束月") is None else int(row.get("結束月") or 0)))


def _load_tenure_ratio_settings_payload() -> dict[str, Any]:
    global _TENURE_RATIO_SETTINGS_CACHE
    if _TENURE_RATIO_SETTINGS_CACHE is not None:
        return _TENURE_RATIO_SETTINGS_CACHE
    settings_path = Path(PERSISTENT_DIR) / "manpower_tenure_ratio_settings.json"
    try:
        with settings_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}
    _TENURE_RATIO_SETTINGS_CACHE = payload
    return payload


def _tenure_rules_for_roster_source(source: str) -> list[dict[str, float | int | None]]:
    table_name = "dispatch" if "派遣" in str(source) or "外包" in str(source) else "employees"
    settings = _load_tenure_ratio_settings_payload()
    module_settings = settings.get(table_name, {}) if isinstance(settings, dict) else {}
    raw_rules = module_settings.get("rules", DEFAULT_TENURE_RATIO_RULES) if isinstance(module_settings, dict) else DEFAULT_TENURE_RATIO_RULES
    return _normalize_tenure_rules(raw_rules)


def _tenure_months_between(start_date: pd.Timestamp | date | None, reference_date: pd.Timestamp | date | None) -> int | None:
    if start_date is None or reference_date is None:
        return None
    start = pd.Timestamp(start_date)
    ref = pd.Timestamp(reference_date)
    if pd.isna(start) or pd.isna(ref):
        return None
    start = start.normalize()
    ref = ref.normalize()
    if start > ref:
        return 0
    months = (ref.year - start.year) * 12 + (ref.month - start.month)
    if ref.day < start.day:
        months -= 1
    return max(int(months), 0)


def _ratio_from_tenure_rules(tenure_months: int | None, rules: list[dict[str, float | int | None]], fallback: float = 1.0) -> float:
    if tenure_months is None:
        return _safe_ratio(fallback, 1.0)
    for rule in rules:
        start = int(rule.get("起始月") or 0)
        end = rule.get("結束月")
        if tenure_months < start:
            continue
        if end is not None and tenure_months > int(end):
            continue
        return _safe_tenure_ratio(rule.get("可用比例"), fallback)
    return _safe_ratio(fallback, 1.0)


def _monthly_ratio_series(people: pd.DataFrame, month_end: pd.Timestamp) -> pd.Series:
    """Return each active person's availability ratio as of that month.

    04. 產能負荷表必須逐月計算可用人力；不可直接沿用 01/02 表格中
    以年度基準日寫回的可用比例，否則選 2025 年時會把 2026 年的年資比例
    套回 2025 各月份，導致月別可用人力偏高。
    """
    if people is None or people.empty:
        return pd.Series(dtype=float)
    ratio_by_source = {
        str(source): _tenure_rules_for_roster_source(str(source))
        for source in people.get("人力來源", pd.Series("", index=people.index)).fillna("").astype(str).unique().tolist()
    }
    values: list[float] = []
    for _, row in people.iterrows():
        fallback = _safe_ratio(row.get("可用比例"), 1.0)
        months = _tenure_months_between(row.get("到職日"), month_end)
        rules = ratio_by_source.get(str(row.get("人力來源", "")), _normalize_tenure_rules(DEFAULT_TENURE_RATIO_RULES))
        values.append(_ratio_from_tenure_rules(months, rules, fallback))
    return pd.Series(values, index=people.index, dtype=float)


def _prepare_available_manpower_roster(df: pd.DataFrame, target_year: int | None, source: str) -> pd.DataFrame:
    """Normalize 01/02 roster rows for monthly manpower calculation.

    System-wide definitions:
    - 可用總人力：目前在職且啟用的全部製造人力，不區分直接/間接，
      也不乘可用比例。
    - 直接有效人力：在職且啟用、是否直接人力=是的人員，再依
      可用比例換算後加總。
    - 到職日當天開始計入；離職日當天起不再計入。
    """
    cols = ["人力來源", "人員識別", "到職日", "離職日", "是否直接人力", "可用比例"]
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)
    temp = df.copy()
    if target_year is not None and "年份" in temp.columns:
        year_filtered = temp[temp["年份"].map(lambda x: normalize_year(x, DEFAULT_YEAR)).eq(int(target_year))].copy()
        # If the selected/future year has no explicit duplicated roster, keep the
        # current roster and use employment dates to determine monthly activity.
        if not year_filtered.empty:
            temp = year_filtered
    if temp.empty:
        return pd.DataFrame(columns=cols)

    active_mask = _active_people_mask(temp)
    if not active_mask.empty:
        temp = temp[active_mask].copy()
    if temp.empty:
        return pd.DataFrame(columns=cols)

    direct_col = _first_existing_column(temp, ["是否直接人力", "直接人力", "是否直接", "直接/間接"])
    if direct_col:
        temp["_是否直接人力"] = temp[direct_col].map(lambda x: _truthy_direct(x, default=True))
    else:
        temp["_是否直接人力"] = True

    ratio_col = _first_existing_column(temp, ["可用比例", "有效比例", "投入比例", "可用人力比例"])
    if ratio_col:
        temp["_可用比例"] = temp[ratio_col].map(lambda x: _safe_ratio(x, 1.0))
    else:
        temp["_可用比例"] = 1.0
    temp["_可用比例"] = pd.to_numeric(temp["_可用比例"], errors="coerce").fillna(1.0).clip(lower=0)

    year_for_dates = int(target_year or DEFAULT_YEAR)
    hire_col = _first_existing_column(temp, ["到職日", "到職日期", "入職日", "入職日期", "到任日", "進場日", "報到日"])
    leave_col = _first_existing_column(temp, ["離職日", "離職日期", "退職日", "退場日", "離場日"])
    hire_dates = pd.to_datetime(temp[hire_col], errors="coerce") if hire_col else pd.Series(pd.NaT, index=temp.index, dtype="datetime64[ns]")
    hire_dates = hire_dates.fillna(pd.Timestamp(year_for_dates, 1, 1))
    leave_dates = pd.to_datetime(temp[leave_col], errors="coerce") if leave_col else pd.Series(pd.NaT, index=temp.index, dtype="datetime64[ns]")

    id_col = _first_existing_column(temp, ["員工編號", "工號", "姓名"])
    if id_col:
        person_id = temp[id_col].fillna("").astype(str).str.strip()
    else:
        person_id = pd.Series([f"{source}_{idx}" for idx in temp.index], index=temp.index)
    name_col = _first_existing_column(temp, ["姓名", "員工姓名"])
    if name_col:
        names = temp[name_col].fillna("").astype(str).str.strip()
        person_id = pd.Series(person_id, index=temp.index).where(pd.Series(person_id, index=temp.index).astype(str).str.len().gt(0), names)

    normalized = pd.DataFrame({
        "人力來源": source,
        "人員識別": pd.Series(person_id, index=temp.index).astype(str),
        "到職日": hire_dates,
        "離職日": leave_dates,
        "是否直接人力": temp["_是否直接人力"].astype(bool),
        "可用比例": temp["_可用比例"].astype(float),
    }, index=temp.index)
    normalized["_dedupe_key"] = (
        normalized["人力來源"].astype(str) + "|" +
        normalized["人員識別"].astype(str) + "|" +
        normalized["到職日"].astype(str)
    )
    normalized = normalized.drop_duplicates("_dedupe_key", keep="last").drop(columns=["_dedupe_key"])
    return normalized.reset_index(drop=True)


def monthly_manpower_metrics_from_rosters(
    employees: pd.DataFrame,
    dispatch: pd.DataFrame,
    target_year: int | str | None = None,
    month_order: list[str] | None = None,
) -> pd.DataFrame:
    """Return monthly total and direct-effective manufacturing manpower.

    可用總人力 is the active headcount at each month end. 直接有效人力 is
    direct manpower multiplied by the applicable availability ratio and prorated
    for mid-month hire/leave dates, so capacity hours reflect the actual active
    period while the total-headcount field remains an intuitive integer count.
    """
    months = month_order or MONTH_ORDER
    year_value = normalize_year(target_year, DEFAULT_YEAR) if target_year not in (None, "全部", "All", "all") else DEFAULT_YEAR
    roster_frames = [
        _prepare_available_manpower_roster(employees, int(year_value), "超慧正職"),
        _prepare_available_manpower_roster(dispatch, int(year_value), "派遣/外包"),
    ]
    roster_frames = [frame for frame in roster_frames if frame is not None and not frame.empty]
    people = pd.concat(roster_frames, ignore_index=True) if roster_frames else pd.DataFrame(
        columns=["人力來源", "人員識別", "到職日", "離職日", "是否直接人力", "可用比例"]
    )
    rows: list[dict[str, float | int | str]] = []
    for idx, month in enumerate(months, start=1):
        if people.empty:
            rows.append({"月份": month, "可用總人力": 0, "直接人力": 0, "直接有效人力": 0.0})
            continue
        month_start = pd.Timestamp(int(year_value), idx, 1)
        month_end = month_start + pd.offsets.MonthEnd(0)
        next_month_start = month_end + pd.Timedelta(days=1)

        # Headcount is the number actually active at month end. Leaving day is
        # excluded, matching the rule "離職日當天起不再算人力".
        active_at_month_end = people["到職日"].le(month_end) & (people["離職日"].isna() | people["離職日"].gt(month_end))
        total_headcount = int(active_at_month_end.sum())
        direct_flags = people["是否直接人力"].map(lambda value: True if pd.isna(value) else bool(value))
        direct_headcount = int((active_at_month_end & direct_flags).sum())

        overlaps_month = people["到職日"].lt(next_month_start) & (people["離職日"].isna() | people["離職日"].gt(month_start))
        direct_effective = 0.0
        if overlaps_month.any():
            active_people = people.loc[overlaps_month].copy()
            direct_mask = active_people["是否直接人力"].map(lambda value: True if pd.isna(value) else bool(value))
            direct_people = active_people.loc[direct_mask].copy()
            if not direct_people.empty:
                # 直接有效人力必須明確依 01/02 名單目前保存的「可用比例」計算。
                # 不再於產能運算時另套年資級距，避免畫面可用比例與 04/09 結果不一致。
                monthly_ratio = pd.to_numeric(direct_people["可用比例"], errors="coerce").fillna(1.0).clip(lower=0)
                active_start = direct_people["到職日"].where(direct_people["到職日"].gt(month_start), month_start)
                active_end = direct_people["離職日"].where(
                    direct_people["離職日"].notna() & direct_people["離職日"].lt(next_month_start),
                    next_month_start,
                )
                active_days = (active_end - active_start).dt.days.clip(lower=0)
                month_days = max(int((next_month_start - month_start).days), 1)
                active_fraction = (active_days / month_days).clip(lower=0, upper=1)
                direct_effective = float((monthly_ratio * active_fraction).sum())
        rows.append({
            "月份": month,
            "可用總人力": total_headcount,
            "直接人力": direct_headcount,
            "直接有效人力": round(direct_effective, 3),
        })
    return pd.DataFrame(rows)


def monthly_available_manpower_from_rosters(
    employees: pd.DataFrame,
    dispatch: pd.DataFrame,
    target_year: int | str | None = None,
    month_order: list[str] | None = None,
) -> dict[str, float]:
    """Return {月份: 可用總人力}; all active manufacturing headcount."""
    metrics = monthly_manpower_metrics_from_rosters(employees, dispatch, target_year, month_order)
    return {str(row["月份"]): float(row["可用總人力"]) for _, row in metrics.iterrows()}


def monthly_direct_effective_manpower_from_rosters(
    employees: pd.DataFrame,
    dispatch: pd.DataFrame,
    target_year: int | str | None = None,
    month_order: list[str] | None = None,
) -> dict[str, float]:
    """Return {月份: 直接有效人力}; direct manpower × availability ratio."""
    metrics = monthly_manpower_metrics_from_rosters(employees, dispatch, target_year, month_order)
    return {str(row["月份"]): float(row["直接有效人力"]) for _, row in metrics.iterrows()}

def _schedule_machine_count(df: pd.DataFrame) -> pd.Series:
    """Return the monthly machine count used by 04. 產能負荷表.

    「機台計數」是系統計算欄位，不應把既有欄位中的 0 當成永久正確值。
    05. 排程表穩定編輯新增空白列時會先放入 0/空白；使用者填入台數後，
    儲存重新計算時必須依目前排程內容重新產生機台計數，避免 04 與
    Category 統計仍讀到 0。

    優先順序：
    1. 有正式月份且「台數」為正數時，機台計數 = 台數。
    2. 否則沿用台數_raw / Excel J 欄：數字用數字，非空白月份標記算 1。
    3. 若舊資料已有正數機台計數，保留舊值。
    4. 沒有月份也沒有數量時為 0。
    """
    if df is None or df.empty:
        return pd.Series(dtype=float)

    index = df.index
    result = pd.Series(0.0, index=index)

    if "月份" in df.columns:
        month_values = df["月份"].map(normalize_month)
        has_real_month = month_values.isin(MONTH_ORDER)
    else:
        has_real_month = pd.Series(False, index=index)

    if "台數" in df.columns:
        qty = pd.to_numeric(df["台數"], errors="coerce")
        qty_mask = qty.notna() & qty.gt(0) & has_real_month
        result = result.where(~qty_mask, qty.clip(lower=0))

    raw_mask = pd.Series(False, index=index)
    if "台數_raw" in df.columns:
        raw = df["台數_raw"]
        raw_text = raw.astype(str).str.strip()
        raw_mask = raw.notna() & raw_text.ne("") & ~raw_text.str.lower().isin(["nan", "none", "null"])
        numeric_raw = pd.to_numeric(raw, errors="coerce")
        raw_count = pd.Series(np.where(raw_mask, numeric_raw.fillna(1), 0), index=index).clip(lower=0)
        needs_raw = result.le(0) & raw_mask
        result = result.where(~needs_raw, raw_count)

    if "機台計數" in df.columns:
        existing = pd.to_numeric(df["機台計數"], errors="coerce").fillna(0).clip(lower=0)
        keep_existing = result.le(0) & existing.gt(0)
        result = result.where(~keep_existing, existing)

    if "台數" in df.columns:
        # 沒有台數_raw 的手動資料，只要月份有效，就以台數作為機台數。
        qty = pd.to_numeric(df["台數"], errors="coerce")
        manual_qty = result.le(0) & qty.notna() & qty.gt(0) & has_real_month & ~raw_mask
        result = result.where(~manual_qty, qty.clip(lower=0))

    return result.fillna(0).clip(lower=0)


def _monthly_direct_people_map(params: dict[str, Any], year_value: int | str | None, month_order: list[str] | None = None) -> dict[str, float]:
    """Return year/month direct manpower overrides from 08. 人力參數設定.

    Canonical storage in capacity_parameters.json:
    {
      "monthly_direct_people": {
        "2026": {"1月": 53, ..., "12月": 53}
      },
      "use_monthly_direct_people_override": true
    }
    """
    if not isinstance(params, dict):
        return {}
    months = month_order or MONTH_ORDER
    raw = params.get("monthly_direct_people") or params.get("direct_people_by_year") or {}
    if not isinstance(raw, dict):
        return {}
    y = str(normalize_year(year_value if year_value is not None else params.get("year", DEFAULT_YEAR), DEFAULT_YEAR))
    data = raw.get(y) or raw.get(str(params.get("year", y))) or {}
    result: dict[str, float] = {}
    if isinstance(data, dict):
        for month in months:
            month_number = month.replace("月", "")
            if month in data:
                result[month] = _safe_number(data.get(month), 0.0, minimum=0.0)
            elif month_number in data:
                result[month] = _safe_number(data.get(month_number), 0.0, minimum=0.0)
    elif isinstance(data, list):
        for idx, value in enumerate(data[: len(months)], start=1):
            result[f"{idx}月"] = _safe_number(value, 0.0, minimum=0.0)
    return result


def _should_use_monthly_direct_people(params: dict[str, Any]) -> bool:
    if not isinstance(params, dict):
        return False
    if "use_monthly_direct_people_override" in params:
        return bool(params.get("use_monthly_direct_people_override"))
    # Backward compatibility: old UI used use_direct_people_override for a single value.
    return bool(params.get("use_direct_people_override", False)) and bool(params.get("monthly_direct_people") or params.get("direct_people_by_year"))


def _leave_ratio(params: dict[str, Any]) -> float:
    """Return the daily leave ratio from 08. 人力參數設定.

    The ratio reduces the daily available manpower used to calculate normal and
    overtime available hours.  Values like 10 are treated as 10%.
    """
    if not isinstance(params, dict):
        return 0.0
    return _safe_ratio(params.get("leave_ratio", params.get("daily_leave_ratio", 0.0)), 0.0)


def normalize_month(value: Any) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "未設定"
    text = str(value).strip()
    if text.endswith("月"):
        return text
    try:
        num = int(float(text))
        if 1 <= num <= 12:
            return f"{num}月"
    except Exception:
        pass
    return text


def _clean_key_text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _lookup_standard_hours(schedule_df: pd.DataFrame, standard_hours: pd.DataFrame, target_year: int | str | None = None) -> pd.DataFrame:
    """Fill missing/zero standard hours from 06. 標準工時.

    Lookup is intentionally cascading:
    1. Same year + 客戶 + P/N + Type + Category when possible.
    2. Same year + 客戶 + P/N + Type.
    3. 客戶 + P/N + Type + Category ignoring year.
    4. 客戶 + P/N + Type ignoring year.
    5. P/N + Type ignoring year.
    6. P/N only ignoring year.

    This keeps multi-year calculation useful even when a newer year has schedule
    data but the standard-hour master is shared from another year.
    """
    df = schedule_df.copy()
    if standard_hours is None or standard_hours.empty or "標準工時" not in standard_hours.columns:
        df["標準工時來源"] = df.get("標準工時來源", "排程表")
        return df

    std_all = ensure_year_column("standard_hours", standard_hours.copy(), DEFAULT_YEAR)
    std_all["標準工時"] = _to_float(std_all["標準工時"], np.nan)
    std_all = std_all.dropna(subset=["標準工時"])
    if std_all.empty:
        df["標準工時來源"] = df.get("標準工時來源", "排程表")
        return df

    # 06. 標準工時同時維護「標準天數」；05. 排程表的「工期」應在
    # 標準工時補齊時一併補齊。若舊主檔沒有標準天數，才用
    # 標準工時 / 8 小時回推，且只補空白或 0，不覆蓋人工輸入值。
    if "工期" not in df.columns:
        df["工期"] = np.nan
    if "標準天數" in std_all.columns:
        std_all["標準天數"] = pd.to_numeric(std_all["標準天數"], errors="coerce")
    else:
        std_all["標準天數"] = np.nan
    missing_standard_days = std_all["標準天數"].isna() | std_all["標準天數"].le(0)
    std_all.loc[missing_standard_days, "標準天數"] = (std_all.loc[missing_standard_days, "標準工時"] / 8.0).round(1)

    # Keep active standard-hour rows when the column exists.
    if "是否啟用" in std_all.columns:
        active = std_all["是否啟用"].fillna("是").astype(str).str.strip()
        std_all = std_all[~active.isin(["否", "N", "n", "False", "false", "0", "停用"])]

    for candidate in [df, std_all]:
        for col in ["客戶", "P/N", "Type", "Category"]:
            if col in candidate.columns:
                candidate[col] = _clean_key_text(candidate[col])

    if "標準工時來源" not in df.columns:
        df["標準工時來源"] = np.where(_to_float(df["標準工時"], np.nan).fillna(0) > 0, "排程表", "未設定")

    def apply_lookup(keys: list[str], source_df: pd.DataFrame, label: str) -> None:
        nonlocal df
        if not keys or "標準工時" not in source_df.columns:
            return
        if any(k not in df.columns or k not in source_df.columns for k in keys):
            return
        value_cols = ["標準工時"]
        if "標準天數" in source_df.columns:
            value_cols.append("標準天數")
        lookup = source_df[keys + value_cols].copy()
        # Skip rows whose key is fully blank; they should not match everything.
        lookup = lookup[~lookup[keys].apply(lambda row: all(str(v).strip() == "" for v in row), axis=1)]
        if lookup.empty:
            return
        lookup = lookup.drop_duplicates(keys, keep="last")

        hours_numeric = pd.to_numeric(df["標準工時"], errors="coerce")
        duration_numeric = pd.to_numeric(df["工期"], errors="coerce")
        need_hours = hours_numeric.isna() | hours_numeric.le(0)
        need_duration = duration_numeric.isna() | duration_numeric.le(0)
        need = need_hours | need_duration
        if not need.any():
            return

        merged = df.loc[need, keys].merge(lookup, on=keys, how="left")
        idx = df.index[need]

        hour_values = merged["標準工時"].to_numpy()
        fill_hour_mask = need_hours.loc[idx].to_numpy() & pd.notna(hour_values)
        if fill_hour_mask.any():
            target_idx = idx[fill_hour_mask]
            df.loc[target_idx, "標準工時"] = hour_values[fill_hour_mask]
            df.loc[target_idx, "標準工時來源"] = label

        if "標準天數" in merged.columns:
            day_values = pd.to_numeric(merged["標準天數"], errors="coerce").to_numpy()
            fill_day_mask = need_duration.loc[idx].to_numpy() & pd.notna(day_values)
            if fill_day_mask.any():
                target_idx = idx[fill_day_mask]
                df.loc[target_idx, "工期"] = day_values[fill_day_mask]

    std_same_year = filter_by_year(std_all, target_year, DEFAULT_YEAR) if target_year not in (None, "全部", "All", "all") else std_all
    lookup_plans = [
        (["年份", "客戶", "P/N", "Type", "Category"], std_same_year, "06標準工時-同年度完整鍵"),
        (["年份", "客戶", "P/N", "Type"], std_same_year, "06標準工時-同年度"),
        (["客戶", "P/N", "Type", "Category"], std_all, "06標準工時-跨年度完整鍵"),
        (["客戶", "P/N", "Type"], std_all, "06標準工時-跨年度"),
        (["P/N", "Type"], std_all, "06標準工時-PN+Type"),
        (["P/N"], std_all, "06標準工時-PN"),
    ]
    for keys, source, label in lookup_plans:
        apply_lookup([k for k in keys if k in df.columns and k in source.columns], source, label)
    return df


def prepare_schedule(
    schedule: pd.DataFrame,
    standard_hours: pd.DataFrame | None = None,
    target_year: int | str | None = None,
    excluded_assembly_locations: Any = None,
    excluded_categories: Any = None,
    assembly_location_hours: Any = None,
) -> pd.DataFrame:
    if schedule is None or schedule.empty:
        return pd.DataFrame(columns=["年份", "月份", "台數", "標準工時", "標準工時來源", "需求工時"])
    df = ensure_year_column("schedule", schedule.copy(), DEFAULT_YEAR)
    df = filter_by_year(df, target_year, DEFAULT_YEAR)
    if "排程ID" in df.columns:
        # Stable record IDs are authoritative. Duplicate IDs would otherwise count
        # the same schedule twice in 04/09 after a bad import or merge.
        id_text = df["排程ID"].fillna("").astype(str).str.strip()
        with_id = id_text.ne("")
        df = pd.concat([
            df.loc[~with_id],
            df.loc[with_id].drop_duplicates("排程ID", keep="last"),
        ], ignore_index=True, sort=False)
    if "M" in df.columns and "月份" not in df.columns:
        df = df.rename(columns={"M": "月份"})
    if "月份" not in df.columns:
        df["月份"] = df.get("台數_raw", "未設定")
    df["月份"] = df["月份"].map(normalize_month)
    if "台數" not in df.columns:
        df["台數"] = pd.NA
    _raw_qty_for_import = df["台數"].copy()
    df["台數"] = _to_float(df["台數"], 1).clip(lower=0)

    # 10. 資料匯入與版本管理的 05 排程表範例允許直接填「機台計數」
    # 來表示大量台數。舊格式又可能把「台數」欄拿來放 7月/7 這類
    # 月份標記；若 04 在這裡只用台數欄，就會看起來沒有使用 05 的
    # 最新機台計數。此處先把明確填入的機台計數校正回計算台數，
    # 再由下方統一計算需求工時與月別機台數。
    if "機台計數" in df.columns:
        _machine_input = pd.to_numeric(df["機台計數"], errors="coerce")
        _qty_text = _raw_qty_for_import.astype(str).str.strip()
        _qty_blank = (
            _raw_qty_for_import.isna()
            | _qty_text.eq("")
            | _qty_text.str.lower().isin(["nan", "none", "null", "<na>"])
        )
        _month_number = df["月份"].astype(str).str.extract(r"^(\d{1,2})月$", expand=False)
        _qty_as_number = pd.to_numeric(_raw_qty_for_import, errors="coerce")
        _qty_is_month_marker = (
            _machine_input.gt(0).fillna(False)
            & _month_number.notna()
            & _qty_as_number.notna()
            & _qty_as_number.eq(pd.to_numeric(_month_number, errors="coerce"))
        )
        _use_machine_as_qty = (
            _machine_input.gt(0).fillna(False)
            & (
                _qty_blank
                | df["台數"].le(0)
                | _qty_is_month_marker
                | (df["台數"].eq(1) & _machine_input.gt(1).fillna(False))
            )
        )
        df.loc[_use_machine_as_qty, "台數"] = _machine_input.loc[_use_machine_as_qty].clip(lower=0)

    if "標準工時" not in df.columns:
        df["標準工時"] = np.nan
    df["標準工時"] = _to_float(df["標準工時"], np.nan)
    if standard_hours is not None and not standard_hours.empty:
        df = _lookup_standard_hours(df, standard_hours, target_year=target_year)
    if "標準工時來源" not in df.columns:
        df["標準工時來源"] = np.where(_to_float(df["標準工時"], np.nan).fillna(0) > 0, "排程表", "未設定")
    df["標準工時"] = _to_float(df["標準工時"], 0).clip(lower=0)
    if "工期" not in df.columns:
        df["工期"] = np.nan
    df["工期"] = pd.to_numeric(df["工期"], errors="coerce")
    missing_duration = df["工期"].isna() | df["工期"].le(0)
    has_hours = df["標準工時"].gt(0)
    df.loc[missing_duration & has_hours, "工期"] = (df.loc[missing_duration & has_hours, "標準工時"] / 8.0).round(1)
    df["機台計數"] = _schedule_machine_count(df)
    df["原始需求工時"] = (df["台數"] * df["標準工時"]).clip(lower=0)
    df["需求工時"] = df["原始需求工時"]
    df = apply_capacity_exclusions(
        df,
        excluded_assembly_locations=excluded_assembly_locations,
        excluded_categories=excluded_categories,
        assembly_location_hours=assembly_location_hours,
    )
    return df



def recalculate_schedule_demand(
    schedule: pd.DataFrame,
    standard_hours: pd.DataFrame | None = None,
    target_year: int | str | None = None,
    excluded_assembly_locations: Any = None,
    excluded_categories: Any = None,
    assembly_location_hours: Any = None,
) -> pd.DataFrame:
    """Recalculate 05. 排程表 demand hours and preserve authority rows.

    系統規則：原始需求工時 = 台數 × 標準工時；需求工時 = 原始需求工時
    扣除「產能計算排除的組立地點」工時。若標準工時空白或為 0，會依
    06. 標準工時的多階段比對補齊，再重新計算。此函式可用於 05 儲存時，確保權威資料、
    04 產能負荷表與 09 情境模擬使用同一份計算後排程。
    """
    if schedule is None or schedule.empty:
        return pd.DataFrame(columns=["年份", "月份", "台數", "標準工時", "需求工時"])

    base = ensure_year_column("schedule", schedule.copy(), DEFAULT_YEAR)
    if "_選取" in base.columns:
        base = base.drop(columns=["_選取"])

    if target_year in (None, "全部", "All", "all"):
        recalculated = prepare_schedule(
            base,
            standard_hours,
            target_year=None,
            excluded_assembly_locations=excluded_assembly_locations,
            excluded_categories=excluded_categories,
            assembly_location_hours=assembly_location_hours,
        )
        return recalculated.reset_index(drop=True)

    selected_year = normalize_year(target_year, DEFAULT_YEAR)
    target_mask = base["年份"].map(lambda x: normalize_year(x, DEFAULT_YEAR)).eq(selected_year)
    target = prepare_schedule(
        base.loc[target_mask].copy(),
        standard_hours,
        target_year=selected_year,
        excluded_assembly_locations=excluded_assembly_locations,
        excluded_categories=excluded_categories,
        assembly_location_hours=assembly_location_hours,
    )
    other = base.loc[~target_mask].copy()
    if other.empty:
        return target.reset_index(drop=True)
    result = pd.concat([other, target], ignore_index=True, sort=False)
    if "月份" in result.columns:
        order_map = {m: i for i, m in enumerate(MONTH_ORDER, start=1)}
        result["_year_order"] = pd.to_numeric(result.get("年份"), errors="coerce").fillna(DEFAULT_YEAR)
        result["_month_order"] = result["月份"].map(normalize_month).map(order_map).fillna(99)
        result = result.sort_values(["_year_order", "_month_order"]).drop(columns=["_year_order", "_month_order"])
    return result.reset_index(drop=True)

def summarize_manpower(employees: pd.DataFrame, dispatch: pd.DataFrame, target_year: int | str | None = None, as_of_date: date | pd.Timestamp | None = None) -> pd.DataFrame:
    """Summarize current manufacturing manpower using unified definitions.

    可用總人力 = all active manufacturing headcount.
    直接有效人力 = direct manpower multiplied by 可用比例.
    有效人力 is retained as a compatibility alias of 直接有效人力.
    """
    frames = []
    as_of = pd.Timestamp(as_of_date or resolve_manpower_as_of_date(target_year)).normalize()
    for source, df in [("超慧正職", employees), ("派遣/外包", dispatch)]:
        if df is None or df.empty:
            continue
        temp = ensure_year_column("employees" if source == "超慧正職" else "dispatch", df.copy(), DEFAULT_YEAR)
        temp = filter_by_year(temp, target_year, DEFAULT_YEAR)
        if "啟用" in temp.columns:
            active = temp["啟用"].fillna("是").astype(str).str.strip()
            temp = temp[~active.isin(["否", "N", "n", "False", "false", "0", "停用", "離場", "離職"])]
        if not temp.empty:
            temp = temp[employment_active_mask(temp, as_of)].copy()
        if temp.empty:
            continue
        temp["人力來源"] = temp.get("人力來源", source)
        if "是否直接人力" not in temp.columns:
            temp["是否直接人力"] = "是"
        if "可用比例" not in temp.columns:
            temp["可用比例"] = 1.0
        temp["_直接旗標"] = temp["是否直接人力"].map(lambda value: _truthy_direct(value, default=True))
        temp["可用比例"] = temp["可用比例"].map(lambda value: _safe_ratio(value, 1.0))
        temp["可用總人力"] = 1.0
        temp["直接有效人力"] = np.where(temp["_直接旗標"], temp["可用比例"], 0.0)
        temp["有效人力"] = temp["直接有效人力"]
        frames.append(temp)
    if not frames:
        return pd.DataFrame(columns=["課別", "工段", "人力來源", "總人數", "可用總人力", "直接人力", "直接有效人力", "有效人力"])
    all_people = pd.concat(frames, ignore_index=True)
    for col in ["年份", "課別", "工段", "人力來源"]:
        if col not in all_people.columns:
            all_people[col] = "未設定"
        all_people[col] = all_people[col].fillna("未設定").astype(str).str.strip().replace("", "未設定")
    result = all_people.groupby(["年份", "課別", "工段", "人力來源"], as_index=False).agg(
        總人數=("姓名", "count"),
        可用總人力=("可用總人力", "sum"),
        直接人力=("_直接旗標", "sum"),
        直接有效人力=("直接有效人力", "sum"),
        有效人力=("有效人力", "sum"),
    )
    result["可用總人力"] = pd.to_numeric(result["可用總人力"], errors="coerce").fillna(0).round(0)
    result["直接人力"] = pd.to_numeric(result["直接人力"], errors="coerce").fillna(0).round(0)
    result["直接有效人力"] = pd.to_numeric(result["直接有效人力"], errors="coerce").fillna(0).round(3)
    result["有效人力"] = result["直接有效人力"]
    return result.sort_values(["年份", "課別", "工段", "人力來源"])


def calculate_capacity(
    schedule: pd.DataFrame,
    standard_hours: pd.DataFrame,
    work_calendar: pd.DataFrame,
    employees: pd.DataFrame,
    dispatch: pd.DataFrame,
    params: dict[str, Any],
    adjustments: pd.DataFrame | None = None,
    target_year: int | str | None = None,
) -> pd.DataFrame:
    year_value = normalize_year(target_year, DEFAULT_YEAR) if target_year not in (None, "全部", "All", "all") else None
    schedule = ensure_year_column("schedule", schedule, DEFAULT_YEAR)
    standard_hours = ensure_year_column("standard_hours", standard_hours, DEFAULT_YEAR)
    work_calendar = ensure_year_column("work_calendar", work_calendar, DEFAULT_YEAR) if work_calendar is not None else work_calendar
    employees = ensure_year_column("employees", employees, DEFAULT_YEAR)
    dispatch = ensure_year_column("dispatch", dispatch, DEFAULT_YEAR)
    adjustments = ensure_year_column("capacity_adjustments", adjustments, DEFAULT_YEAR) if adjustments is not None else adjustments

    schedule2 = prepare_schedule(
        schedule,
        standard_hours,
        target_year=year_value,
        excluded_assembly_locations=params.get(ASSEMBLY_EXCLUSION_PARAM_KEY),
        excluded_categories=params.get(CATEGORY_EXCLUSION_PARAM_KEY),
        assembly_location_hours=params.get(ASSEMBLY_LOCATION_HOURS_PARAM_KEY),
    )
    if "機台計數" not in schedule2.columns:
        schedule2["機台計數"] = _schedule_machine_count(schedule2)
    demand = schedule2.groupby("月份", as_index=False).agg(
        每月機台數=("機台計數", "sum"),
        原始需求工時=("原始需求工時", "sum"),
        產能計算排除工時=("產能計算排除工時", "sum"),
        排除後需求工時=("需求工時", "sum"),
        工單筆數=("需求工時", "size"),
    )
    if work_calendar is not None and not work_calendar.empty:
        work_calendar = filter_by_year(work_calendar, year_value, DEFAULT_YEAR)
    if work_calendar is None or work_calendar.empty:
        work_calendar = pd.DataFrame({"年份": [year_value or DEFAULT_YEAR] * 12, "月份": MONTH_ORDER, "正常工作日": [21] * 12, "週六天數": [4] * 12, "週日天數": [4] * 12, "法定假日": [0] * 12})
    calendar = work_calendar.copy()
    calendar["月份"] = calendar["月份"].map(normalize_month)
    if "年份" not in calendar.columns:
        calendar["年份"] = int(year_value or DEFAULT_YEAR)
    calendar["年份"] = pd.to_numeric(calendar["年份"], errors="coerce").fillna(int(year_value or DEFAULT_YEAR)).astype(int)
    calendar = calendar[calendar["月份"].isin(MONTH_ORDER)].copy()
    # One authority row per year/month. Duplicate calendar rows previously caused
    # duplicated 04 result months after merge and inflated annual totals.
    calendar = calendar.drop_duplicates(["年份", "月份"], keep="last")
    for col in ["正常工作日", "週六天數", "週日天數", "法定假日"]:
        if col not in calendar.columns:
            calendar[col] = 0
        calendar[col] = _to_float(calendar[col], 0)

    manpower_summary = summarize_manpower(employees, dispatch, target_year=year_value)
    base_total_people = float(manpower_summary["可用總人力"].sum()) if not manpower_summary.empty and "可用總人力" in manpower_summary.columns else 0.0
    base_direct_effective_people = float(manpower_summary["直接有效人力"].sum()) if not manpower_summary.empty and "直接有效人力" in manpower_summary.columns else 0.0

    # Unified manpower definitions used by every module:
    # 可用總人力 = all active manufacturing headcount.
    # 直接有效人力 = active direct manpower × 可用比例.
    roster_total_people = monthly_available_manpower_from_rosters(employees, dispatch, year_value, MONTH_ORDER)
    roster_direct_effective_people = monthly_direct_effective_manpower_from_rosters(employees, dispatch, year_value, MONTH_ORDER)
    if not any(float(v or 0) > 0 for v in roster_total_people.values()) and base_total_people > 0:
        roster_total_people = {month: round(float(base_total_people), 0) for month in MONTH_ORDER}
    if not any(float(v or 0) > 0 for v in roster_direct_effective_people.values()) and base_direct_effective_people > 0:
        roster_direct_effective_people = {month: round(float(base_direct_effective_people), 3) for month in MONTH_ORDER}
    effective_people = max(float(base_direct_effective_people), 0.0)

    # 08 manual monthly values override only 直接有效人力. The total manufacturing
    # headcount always comes from 01/02 and is never replaced by an effective value.
    monthly_direct_people = _monthly_direct_people_map(params, year_value, MONTH_ORDER)
    use_manual_monthly_people = bool(params.get("force_manual_monthly_manpower", False)) and bool(monthly_direct_people)
    if bool(params.get("use_direct_people_override", False)) and bool(params.get("force_manual_direct_people_override", False)):
        effective_people = _safe_number(params.get("direct_people_override", effective_people), effective_people, minimum=0.0)

    daily_hours = _safe_number(params.get("daily_hours", 7.0), 7.0, minimum=0.0)
    efficiency = _safe_number(params.get("efficiency", 1.0), 1.0, minimum=0.0)
    weekday_ot_hours = _safe_number(params.get("weekday_overtime_hours", 2.0), 2.0, minimum=0.0)
    sat_ot_hours = _safe_number(params.get("saturday_overtime_hours", 7.0), 7.0, minimum=0.0)
    sun_ot_hours = _safe_number(params.get("sunday_overtime_hours", 7.0), 7.0, minimum=0.0)
    holiday_ot_hours = _safe_number(params.get("holiday_overtime_hours", 7.0), 7.0, minimum=0.0)
    weekday_ot_ratio = _safe_ratio(params.get("weekday_overtime_ratio", 0.3), 0.3)
    holiday_ot_ratio = _safe_ratio(params.get("holiday_overtime_ratio", 0.3), 0.3)
    weekday_ot_day_ratio = _safe_ratio(
        params.get("weekday_overtime_day_ratio", params.get("weekday_overtime_days_ratio", params.get("overtime_day_ratio", 1.0))),
        1.0,
    )
    holiday_ot_day_ratio = _safe_ratio(
        params.get("holiday_overtime_day_ratio", params.get("holiday_overtime_days_ratio", params.get("overtime_day_ratio", 1.0))),
        1.0,
    )
    leave_ratio = _leave_ratio(params)

    # Calendar numbers must never create negative available capacity.
    # Some imported Excel calendars may contain formula artifacts or negative
    # helper values; clamp all capacity-driving day counts to zero or above.
    for day_col in ["正常工作日", "週六天數", "週日天數", "法定假日"]:
        calendar[day_col] = _to_float(calendar[day_col], 0).clip(lower=0)

    calendar["名單可用總人力"] = calendar["月份"].map(lambda month: roster_total_people.get(normalize_month(month), base_total_people))
    calendar["名單可用總人力"] = _to_float(calendar["名單可用總人力"], base_total_people).clip(lower=0).round(0)
    calendar["名單直接有效人力"] = calendar["月份"].map(lambda month: roster_direct_effective_people.get(normalize_month(month), effective_people))
    calendar["名單直接有效人力"] = _to_float(calendar["名單直接有效人力"], effective_people).clip(lower=0)

    # 可用總人力 is always the complete active manufacturing headcount.
    calendar["可用總人力"] = calendar["名單可用總人力"]
    if use_manual_monthly_people:
        calendar["直接有效人力"] = calendar["月份"].map(lambda month: monthly_direct_people.get(normalize_month(month), roster_direct_effective_people.get(normalize_month(month), effective_people)))
        calendar["人力計算來源"] = "可用總人力=01/02在職總數；直接有效人力=08手動月別"
    else:
        calendar["直接有效人力"] = calendar["名單直接有效人力"]
        calendar["人力計算來源"] = "可用總人力=01/02在職總數；直接有效人力=01/02直接人力×可用比例"
    calendar["直接有效人力"] = _to_float(calendar["直接有效人力"], effective_people).clip(lower=0)

    # All capacity-hour, leave and manpower-gap calculations use direct-effective
    # manpower. Total manpower is displayed for management context only.
    effective_people_series = calendar["直接有效人力"]
    calendar["請假比例"] = float(leave_ratio)
    calendar["請假扣除人力"] = (effective_people_series * leave_ratio).clip(lower=0)
    calendar["扣請假後有效人力"] = (effective_people_series - calendar["請假扣除人力"]).clip(lower=0)
    calendar["請假扣除正常工時"] = (calendar["請假扣除人力"] * calendar["正常工作日"] * daily_hours * efficiency).clip(lower=0)
    available_people_series = calendar["扣請假後有效人力"]
    calendar["正常可用工時"] = (available_people_series * calendar["正常工作日"] * daily_hours * efficiency).clip(lower=0)
    calendar["平日加班人數比例"] = float(weekday_ot_ratio)
    calendar["假日加班人數比例"] = float(holiday_ot_ratio)
    calendar["平日加班天數比例"] = float(weekday_ot_day_ratio)
    calendar["假日加班天數比例"] = float(holiday_ot_day_ratio)
    calendar["平日加班有效天數"] = (calendar["正常工作日"] * weekday_ot_day_ratio).clip(lower=0)
    calendar["週六加班有效天數"] = (calendar["週六天數"] * holiday_ot_day_ratio).clip(lower=0)
    calendar["週日加班有效天數"] = (calendar["週日天數"] * holiday_ot_day_ratio).clip(lower=0)
    calendar["法定假日加班有效天數"] = (calendar["法定假日"] * holiday_ot_day_ratio).clip(lower=0)
    calendar["平日加班工時"] = (available_people_series * weekday_ot_ratio * calendar["平日加班有效天數"] * weekday_ot_hours * efficiency).clip(lower=0)
    calendar["週六加班工時"] = (available_people_series * holiday_ot_ratio * calendar["週六加班有效天數"] * sat_ot_hours * efficiency).clip(lower=0)
    calendar["週日加班工時"] = (available_people_series * holiday_ot_ratio * calendar["週日加班有效天數"] * sun_ot_hours * efficiency).clip(lower=0)
    calendar["法定假日加班工時"] = (available_people_series * holiday_ot_ratio * calendar["法定假日加班有效天數"] * holiday_ot_hours * efficiency).clip(lower=0)
    overtime_cols = ["平日加班工時", "週六加班工時", "週日加班工時", "法定假日加班工時"]
    calendar["加班增加工時"] = calendar[overtime_cols].sum(axis=1).clip(lower=0)
    # Including overtime can only be equal to or higher than normal capacity.
    calendar["含加班可用工時"] = calendar["正常可用工時"] + calendar["加班增加工時"]

    result = pd.DataFrame({"年份": [year_value or DEFAULT_YEAR] * 12, "月份": MONTH_ORDER}).merge(calendar, on=["年份", "月份"], how="left").merge(demand, on="月份", how="left")
    for col in ["每月機台數", "原始需求工時", "產能計算排除工時", "排除後需求工時", "工單筆數"]:
        result[col] = _to_float(result[col], 0)

    # Keep the official schedule-derived machine count visible. 04 can still
    # override the displayed machine count, but recalculation can now tell
    # whether the value came from 05 排程表 or from a 04 manual override.
    result["排程彙總機台數"] = result["每月機台數"]
    result["手動覆寫機台數"] = np.nan
    result["每月機台數來源"] = "05排程表"

    # Manual monthly overrides are persisted in 04. 產能負荷表.
    # Supported editable fields:
    # - 調整工時: added to the post-exclusion demand hours.
    # - 每月機台數: display/management override only; demand still comes from schedule x standard hours.
    # - 正常工作日: recalculates normal capacity and weekday overtime using the same parameter logic.
    # 原始需求工時與需求總工時必須保持不同口徑：
    # 原始 = 排程台數 × 標準工時（未扣除組立地點排除）；
    # 排除後 = 原始 − 產能計算排除工時；
    # 需求總 = 排除後 + 04 月別調整工時。
    result["需求工時扣除差異"] = (result["原始需求工時"] - result["排除後需求工時"]).clip(lower=0)
    result["調整工時"] = 0.0
    if adjustments is not None and not adjustments.empty:
        adj = adjustments.copy()
        if "月份" in adj.columns:
            adj = filter_by_year(adj, year_value, DEFAULT_YEAR)
            adj["月份"] = adj["月份"].map(normalize_month)
            if not adj.empty:
                if "調整工時" not in adj.columns:
                    adj["調整工時"] = 0.0
                adj["調整工時"] = _to_float(adj["調整工時"], 0)
                agg_map = {"調整工時": ("調整工時", "sum")}
                if "每月機台數" in adj.columns:
                    adj["每月機台數"] = pd.to_numeric(adj["每月機台數"], errors="coerce")
                    agg_map["每月機台數_手動"] = ("每月機台數", "last")
                if "正常工作日" in adj.columns:
                    adj["正常工作日"] = pd.to_numeric(adj["正常工作日"], errors="coerce")
                    agg_map["正常工作日_手動"] = ("正常工作日", "last")
                adj_sum = adj.groupby("月份", as_index=False).agg(**agg_map)
                result = result.merge(adj_sum, on="月份", how="left", suffixes=("", "_手動"))
                result["調整工時"] = _to_float(result.get("調整工時_手動", result["調整工時"]), 0)
                for col in ["調整工時_手動"]:
                    if col in result.columns:
                        result = result.drop(columns=[col])
                if "每月機台數_手動" in result.columns:
                    manual_machine = pd.to_numeric(result["每月機台數_手動"], errors="coerce")
                    manual_has_value = manual_machine.notna()
                    result["手動覆寫機台數"] = np.where(manual_has_value, manual_machine.clip(lower=0), result["手動覆寫機台數"])
                    result["每月機台數"] = np.where(manual_has_value, manual_machine.clip(lower=0), result["每月機台數"])
                    result["每月機台數來源"] = np.where(manual_has_value, "04手動覆寫", result["每月機台數來源"])
                    result = result.drop(columns=["每月機台數_手動"])
                if "正常工作日_手動" in result.columns:
                    manual_workdays = pd.to_numeric(result["正常工作日_手動"], errors="coerce")
                    result["正常工作日"] = np.where(manual_workdays.notna(), manual_workdays.clip(lower=0), result["正常工作日"])
                    result = result.drop(columns=["正常工作日_手動"])
                    # Recalculate capacity affected by normal workdays.
                    adjusted_effective_people = _to_float(
                        result["扣請假後有效人力"] if "扣請假後有效人力" in result.columns else result["直接有效人力"],
                        effective_people,
                    ).clip(lower=0)
                    leave_people_for_manual_days = _to_float(
                        result["請假扣除人力"] if "請假扣除人力" in result.columns else pd.Series(0, index=result.index),
                        0,
                    ).clip(lower=0)
                    result["請假扣除正常工時"] = (leave_people_for_manual_days * _to_float(result["正常工作日"], 0).clip(lower=0) * daily_hours * efficiency).clip(lower=0)
                    result["正常可用工時"] = (adjusted_effective_people * _to_float(result["正常工作日"], 0).clip(lower=0) * daily_hours * efficiency).clip(lower=0)
                    result["平日加班有效天數"] = (_to_float(result["正常工作日"], 0).clip(lower=0) * weekday_ot_day_ratio).clip(lower=0)
                    result["平日加班工時"] = (adjusted_effective_people * weekday_ot_ratio * result["平日加班有效天數"] * weekday_ot_hours * efficiency).clip(lower=0)
                    result["加班增加工時"] = result[overtime_cols].sum(axis=1).clip(lower=0)
                    result["含加班可用工時"] = result["正常可用工時"] + result["加班增加工時"]
    result["需求總工時"] = (result["排除後需求工時"] + result["調整工時"]).clip(lower=0)
    result["需求總工時計算口徑"] = "排除後需求工時 + 04月別調整工時"

    result["正常產能負荷"] = result["正常可用工時"] - result["需求總工時"]
    result["含加班產能負荷"] = result["含加班可用工時"] - result["需求總工時"]
    result["正常稼動率"] = np.where(result["正常可用工時"] > 0, result["需求總工時"] / result["正常可用工時"], 0)
    result["含加班稼動率"] = np.where(result["含加班可用工時"] > 0, result["需求總工時"] / result["含加班可用工時"], 0)
    result["需求人力"] = np.where(result["正常工作日"] * daily_hours * efficiency > 0, result["需求總工時"] / (result["正常工作日"] * daily_hours * efficiency), 0)
    result_effective_people = _to_float(
        result["扣請假後有效人力"] if "扣請假後有效人力" in result.columns else result["直接有效人力"],
        effective_people,
    ).clip(lower=0)
    result["人力差異"] = result_effective_people - result["需求人力"]
    result["缺工工時"] = np.maximum(0, result["需求總工時"] - result["含加班可用工時"])
    # 缺工天數必須與正常可用工時計算使用同一個每日有效產出基準，
    # 因此分母需要包含效率；否則效率低於 100% 時，缺工天數會被低估。
    result["缺工天數"] = np.where(result_effective_people * daily_hours * efficiency > 0, result["缺工工時"] / (result_effective_people * daily_hours * efficiency), 0)
    warning_utilization = float(params.get("warning_utilization", 0.85))
    danger_utilization = float(params.get("danger_utilization", 1.0))
    red_utilization = float(params.get("red_utilization", 1.1))
    result["狀態"] = result["含加班稼動率"].map(lambda x: "紅燈" if x >= red_utilization else "橘燈" if x >= danger_utilization else "黃燈" if x >= warning_utilization else "綠燈")
    return result


def calculate_capacity_by_years(
    schedule: pd.DataFrame,
    standard_hours: pd.DataFrame,
    work_calendar: pd.DataFrame,
    employees: pd.DataFrame,
    dispatch: pd.DataFrame,
    params: dict[str, Any],
    adjustments: pd.DataFrame | None = None,
    years: list[int] | None = None,
) -> pd.DataFrame:
    """Calculate a 12-month capacity result for every selected year."""
    if years is None:
        from .year_service import available_years_from_frames
        years = available_years_from_frames([schedule, work_calendar, employees, dispatch])
    frames = []
    for year in years:
        frames.append(calculate_capacity(schedule, standard_hours, work_calendar, employees, dispatch, params, adjustments, target_year=year))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()



def upsert_capacity_results(existing: pd.DataFrame | None, calculated: pd.DataFrame, target_year: int | str | None = None) -> pd.DataFrame:
    """Replace the selected year/month rows in persisted 04 capacity results.

    This prevents repeated recalculation from appending duplicate rows while still
    keeping other years such as 2024/2025/2026 intact.
    """
    if calculated is None or calculated.empty:
        return existing.copy() if existing is not None else pd.DataFrame()
    new_df = calculated.copy()
    if "年份" not in new_df.columns:
        new_df["年份"] = normalize_year(target_year, DEFAULT_YEAR)
    new_df["月份"] = new_df["月份"].map(normalize_month) if "月份" in new_df.columns else "未設定"
    new_df["計算來源"] = "系統計算：原始需求工時=05台數×06標準工時；需求總工時=原始需求工時−組立地點排除工時+04調整工時；產能評估使用需求總工時；可用總人力=01/02全部在職製造人力；直接有效人力=直接人力×可用比例"
    new_df["計算時間"] = pd.Timestamp.now().isoformat(timespec="seconds")

    old_df = existing.copy() if existing is not None and not existing.empty else pd.DataFrame(columns=new_df.columns)
    if not old_df.empty:
        old_df = ensure_year_column("capacity_results", old_df, DEFAULT_YEAR)
        if "月份" in old_df.columns:
            old_df["月份"] = old_df["月份"].map(normalize_month)
        keep = pd.Series(True, index=old_df.index)
        years_to_replace = set(pd.to_numeric(new_df["年份"], errors="coerce").dropna().astype(int).tolist())
        months_to_replace = set(new_df["月份"].astype(str).tolist())
        if "年份" in old_df.columns and "月份" in old_df.columns:
            keep = ~(pd.to_numeric(old_df["年份"], errors="coerce").astype("Int64").isin(years_to_replace) & old_df["月份"].astype(str).isin(months_to_replace))
        old_df = old_df.loc[keep].copy()
    result = new_df.copy() if old_df.empty else pd.concat([old_df, new_df], ignore_index=True, sort=False)
    if "年份" in result.columns and "月份" in result.columns:
        order_map = {m: i for i, m in enumerate(MONTH_ORDER, start=1)}
        result["_month_order"] = result["月份"].map(order_map).fillna(99)
        result["_year_order"] = pd.to_numeric(result["年份"], errors="coerce").fillna(DEFAULT_YEAR)
        result = result.sort_values(["_year_order", "_month_order"]).drop(columns=["_year_order", "_month_order"])
    return result.reset_index(drop=True)


def validate_schedule(schedule: pd.DataFrame, excluded_assembly_locations: Any = None, excluded_categories: Any = None) -> pd.DataFrame:
    if schedule is None or schedule.empty:
        return pd.DataFrame([{"類型": "排程", "狀態": "警示", "訊息": "排程表沒有資料"}])
    checks = []
    required = ["WO", "客戶", "P/N", "Type", "月份", "標準工時"]
    for col in required:
        if col not in schedule.columns:
            checks.append({"類型": "欄位", "狀態": "錯誤", "訊息": f"排程表缺少欄位：{col}"})

    # 所有排程都必須有標準工時，包括「產能計算排除的組立地點」。
    # 因為原始需求工時仍需先以 台數 × 標準工時 完整計算，再將命中排除規則的
    # 原始工時列入「產能計算排除工時」。若排除列沒有標準工時，原始與排除工時
    # 都會被低估，因此不可從標準工時缺漏檢查中排除。
    relevant_schedule = schedule.copy()
    assembly_mask = schedule_assembly_exclusion_mask(relevant_schedule, excluded_assembly_locations)
    category_mask = schedule_category_exclusion_mask(relevant_schedule, excluded_categories)
    if not assembly_mask.empty:
        assembly_mask = assembly_mask.reindex(relevant_schedule.index, fill_value=False)
    if not category_mask.empty:
        category_mask = category_mask.reindex(relevant_schedule.index, fill_value=False)

    asm_count = int(assembly_mask.sum()) if not assembly_mask.empty else 0
    cat_count = int(category_mask.sum()) if not category_mask.empty else 0
    if asm_count or cat_count:
        checks.append({
            "類型": "產能計算排除",
            "狀態": "正常",
            "訊息": (
                f"已套用排除規則：組立地點 {asm_count} 筆自需求總工時扣除但保留台數；"
                f"Category {cat_count} 筆不計台數但保留工時。所有排程仍需標準工時，以正確計算原始與排除工時。"
            ),
        })

    if "標準工時" in schedule.columns:
        std_numeric = pd.to_numeric(relevant_schedule.get("標準工時", pd.Series(dtype=float)), errors="coerce")
        missing = int((std_numeric.isna() | std_numeric.le(0)).sum())
        checks.append({"類型": "標準工時", "狀態": "警示" if missing else "正常", "訊息": f"標準工時缺漏 {int(missing)} 筆"})
    if "WO" in schedule.columns:
        dup = schedule["WO"].duplicated().sum()
        checks.append({"類型": "WO", "狀態": "警示" if dup else "正常", "訊息": f"WO 重複 {int(dup)} 筆"})
    return pd.DataFrame(checks)

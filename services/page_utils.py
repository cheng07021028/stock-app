from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Callable, Iterable, Mapping, Sequence, Any

import pandas as pd
import streamlit as st

from .data_loader import clear_data_cache, load_table
from .export_service import dataframe_to_excel_bytes, module_report_excel_bytes, multi_sheet_excel_bytes
from .persistent_store import save_authority_df
from .schema_service import canonical_column_name, normalize_columns, schema_for_table
from .settings_service import load_table_settings, save_table_settings


SELECT_COL = "_選取"
USER_ROLE_OPTIONS = ["製造部經理", "系統管理員", "課長", "組長", "生管", "工時管理者", "訪客"]

PERSONNEL_REQUIRED_VISIBLE_COLUMNS = {
    "employees": ["年份", "到職日", "離職日", "課別", "部 門", "機型", "工段", "人力來源", "是否直接人力", "可用比例"],
    "dispatch": ["年份", "到職日", "離職日", "職 稱", "課別", "部 門", "外包商年資", "機型", "工段", "人力來源", "是否直接人力", "可用比例"],
}

PERSONNEL_DATE_COLUMNS = ("到職日", "離職日")


def _ensure_required_visible_columns(table_name: str, actual_cols: list[str], visible_cols: list[str], order_cols: list[str]) -> tuple[list[str], list[str], bool]:
    """Keep critical personnel fields visible after old saved settings are loaded.

    Earlier versions allowed users to save a column layout before 工段 / 機型 were
    promoted as official fields. That old layout can hide the new columns after
    reboot, making 01/02 look like they cannot feed the organization chart. This
    migration appends the required fields once and saves the upgraded layout.
    """
    changed = False
    required = [c for c in PERSONNEL_REQUIRED_VISIBLE_COLUMNS.get(table_name, []) if c in actual_cols]
    for col in required:
        if col not in visible_cols:
            visible_cols.append(col)
            changed = True
        if col not in order_cols:
            order_cols.append(col)
            changed = True
    # Preserve a readable schema-first order for personnel pages.
    schema_order = [c for c in schema_for_table(table_name) if c in visible_cols]
    final_order = [c for c in schema_order if c in visible_cols] + [c for c in order_cols if c in visible_cols and c not in schema_order]
    return visible_cols, final_order, changed








NUMERIC_INTEGER_HINTS = {
    "年份", "月別數字", "月份數字", "月份", "週六天數", "週日天數", "六日天數", "正常工作日", "工作日",
    "法定假日", "補班日", "扣除六日工作日", "週天數", "缺工天數", "建議補人", "機台計數", "台數",
}
# Columns whose unit is people must be displayed as whole persons.  This includes
# 人力/人數/補人/缺工等欄位；比例欄位例外，保留最多 1 位小數。
NUMERIC_PEOPLE_KEYWORDS = ["人力", "人數", "補人", "缺工"]
NUMERIC_PEOPLE_EXCLUDE_KEYWORDS = ["比例", "效率", "稼動率", "工時", "負荷", "來源", "說明"]
NEGATIVE_DISPLAY_COLOR = "#FFB86B"
NEGATIVE_DISPLAY_BG = "rgba(255, 184, 107, 0.16)"


def _is_missing_display_value(value: object) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "nat", "none", "<na>"}


def _parse_display_number(value: object) -> float | None:
    if _is_missing_display_value(value):
        return None
    text = str(value).strip().replace(",", "")
    if text.endswith("%"):
        text = text[:-1].strip()
    try:
        return float(text)
    except Exception:
        return None


def _is_people_display_column(column_name: str) -> bool:
    col = str(column_name)
    if "比例" in col:
        return False
    if "(人)" in col or "（人）" in col:
        return True
    if any(key in col for key in NUMERIC_PEOPLE_KEYWORDS) and not any(ex in col for ex in NUMERIC_PEOPLE_EXCLUDE_KEYWORDS):
        return True
    return False


def _format_display_number(value: object, column_name: str = "", *, force_one_decimal: bool = False) -> str:
    """Format displayed numbers consistently across 01~11 modules.

    Final display rules:
    - 0 is displayed as ``0``;
    - whole numbers stay whole numbers;
    - decimal values show at most one decimal place;
    - columns with unit ``人`` are displayed as whole persons;
    - percent columns show whole percentages;
    - values over 1,000 use comma separators.
    """
    number = _parse_display_number(value)
    if number is None:
        return ""
    col = str(column_name)
    if abs(number) < 1e-9:
        return "0%" if "%" in col else "0"
    if "%" in col:
        return f"{number:,.0f}%"
    if _is_people_display_column(col):
        return f"{number:,.0f}"
    integer_hint = any(hint == col or hint in col for hint in NUMERIC_INTEGER_HINTS)
    if integer_hint:
        return f"{number:,.0f}"
    if abs(number - round(number)) <= 1e-9:
        return f"{number:,.0f}"
    return f"{number:,.1f}"


def _column_force_one_decimal(series: pd.Series, column_name: str) -> bool:
    # Kept for backwards compatibility with callers that decide editor step size.
    # Display formatting itself is value-aware in _format_display_number().
    col = str(column_name)
    if "%" in col or _is_people_display_column(col):
        return False
    if any(hint == col or hint in col for hint in NUMERIC_INTEGER_HINTS):
        return False
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().sum() == 0:
        return False
    return bool(((numeric.dropna() % 1).abs() > 1e-9).any())


def format_numeric_display_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Return a display-only frame with numeric values formatted as readable text."""
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    for col in out.columns:
        numeric = pd.to_numeric(out[col], errors="coerce")
        if numeric.notna().sum() == 0:
            continue
        force_decimal = _column_force_one_decimal(out[col], str(col))
        out[col] = out[col].map(lambda v, c=str(col), f=force_decimal: _format_display_number(v, c, force_one_decimal=f))
    return out


def _negative_display_css(value: object) -> str:
    number = _parse_display_number(value)
    if number is not None and number < 0:
        return f"color: {NEGATIVE_DISPLAY_COLOR}; background-color: {NEGATIVE_DISPLAY_BG}; font-weight: 800;"
    return ""




def parse_numeric_display_series(series: pd.Series) -> pd.Series:
    """Parse displayed numeric strings back to numeric values.

    Handles comma separators and percent signs used by the UI display layer.
    Percent signs are kept as percent-point numbers, because user-facing percent
    columns in this project are displayed as 90 for 90%.
    """
    if series is None:
        return pd.Series(dtype="float64")
    text = series.astype("object").map(lambda v: "" if _is_missing_display_value(v) else str(v).strip())
    text = text.str.replace(",", "", regex=False).str.replace("%", "", regex=False)
    return pd.to_numeric(text, errors="coerce")


def format_numeric_display_value(value: object, column_name: str = "") -> str:
    """Public wrapper for consistent display formatting used by custom pages."""
    return _format_display_number(value, column_name)


def format_numeric_editor_display_dataframe(df: pd.DataFrame, editable_columns: set[str] | None = None) -> pd.DataFrame:
    """Return a display frame for st.data_editor without long float tails.

    For editable numeric columns, keep numeric dtype so users can still type values.
    For calculated/disabled numeric columns, convert to formatted display strings,
    because Streamlit data_editor does not reliably apply Styler.format or
    conditional format strings to disabled columns.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    editable_columns = editable_columns or set()
    out = df.copy()
    for col in out.columns:
        col_name = str(col)
        numeric = parse_numeric_display_series(out[col])
        if numeric.notna().sum() == 0:
            continue
        if col_name in editable_columns:
            if "%" in col_name or _is_people_display_column(col_name) or any(h == col_name or h in col_name for h in NUMERIC_INTEGER_HINTS):
                out[col] = numeric.round(0)
            else:
                out[col] = numeric.round(1)
        else:
            out[col] = out[col].map(lambda v, c=col_name: _format_display_number(v, c))
    return out

def numeric_display_styler(df: pd.DataFrame):
    """Build a Styler that keeps source data numeric but displays clean one-decimal values."""
    if df is None:
        df = pd.DataFrame()
    view = df.copy()
    formatters: dict[str, Any] = {}
    for col in view.columns:
        numeric = pd.to_numeric(view[col], errors="coerce")
        if numeric.notna().sum() == 0:
            continue
        force_decimal = _column_force_one_decimal(view[col], str(col))
        formatters[col] = (lambda v, c=str(col), f=force_decimal: _format_display_number(v, c, force_one_decimal=f))
    styler = view.style.format(formatters, na_rep="")
    try:
        return styler.map(_negative_display_css)
    except Exception:
        return styler.applymap(_negative_display_css)


def numeric_column_config_for_dataframe(df: pd.DataFrame, *, editable_columns: set[str] | None = None) -> dict[str, object]:
    """Create Streamlit NumberColumn formats for custom data_editor tables."""
    if df is None or df.empty:
        return {}
    editable_columns = editable_columns or set()
    config: dict[str, object] = {}
    for col in df.columns:
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().sum() == 0:
            continue
        col_name = str(col)
        force_decimal = _column_force_one_decimal(df[col], col_name)
        if "%" in col_name:
            fmt = "%.0f%%"
        elif _is_people_display_column(col_name):
            fmt = "%.0f"
        else:
            fmt = "%,.1f" if force_decimal else "%.0f"
        kwargs: dict[str, Any] = {"format": fmt}
        if col in editable_columns:
            kwargs["step"] = 0.1 if force_decimal else 1
        config[col] = st.column_config.NumberColumn(str(col), **kwargs)
    return config


def _column_config_for_table(table_name: str) -> dict[str, object]:
    config: dict[str, object] = {
        SELECT_COL: st.column_config.CheckboxColumn("選取", help="可用於全選、刪除選取資料。")
    }
    if table_name == "users":
        config["角色"] = st.column_config.SelectboxColumn("角色", options=USER_ROLE_OPTIONS, required=True, help="選擇系統預設角色。")
        config["啟用"] = st.column_config.SelectboxColumn("啟用", options=["是", "否"], required=True)
    if table_name in {"employees", "dispatch"}:
        config["是否直接人力"] = st.column_config.SelectboxColumn("是否直接人力", options=["是", "否"], required=False)
        config["啟用"] = st.column_config.SelectboxColumn("啟用", options=["是", "否"], required=False)
        config["可用比例"] = st.column_config.NumberColumn("可用比例", min_value=-100.0, max_value=100.0, step=0.1, format="%g")
        config["機型"] = st.column_config.TextColumn("機型", help="可填負責機型；沒有資料時可先留空，03 組織圖會顯示未設定機型。")
        config["職 稱"] = st.column_config.TextColumn("職稱", help="人員職稱；02 派遣名單也會提供給 03 組織圖判斷組長 / 組員階層。")
        config["職稱"] = st.column_config.TextColumn("職稱", help="人員職稱；舊欄位會於儲存時自動合併為『職 稱』。")
        config["工段"] = st.column_config.TextColumn("工段", help="產能與組織圖的重要分類欄位，例如 配電、S.T.、NTB、GPTC、BWBS。")
        config["到職日"] = st.column_config.DateColumn("到職日", format="YYYY-MM-DD", help="人力計算：到職日小於等於計算日期後才列入人力。")
        config["離職日"] = st.column_config.DateColumn("離職日", format="YYYY-MM-DD", help="空白代表仍在職；離職日之後不再列入人力計算。")
        config["累積年資"] = st.column_config.TextColumn(
            "累積年資",
            help="系統欄位：由今日日期減到職日自動計算。請修改『到職日』後按儲存資料，不需手填此欄。",
        )
    if table_name == "schedule":
        config["年份"] = st.column_config.NumberColumn("年份", min_value=2000, max_value=2100, step=1, format="%d", help="用於多年度比較。")
        config["月份"] = st.column_config.TextColumn("月份", help="可填 1月~12月，系統會自動標準化。")
        config["台數"] = st.column_config.NumberColumn("台數", min_value=0.0, step=1.0, format="%.0f", help="原始需求工時 = 台數 × 標準工時。")
        config["標準工時"] = st.column_config.NumberColumn("標準工時", min_value=0.0, step=0.1, format="%g", help="可手動填；空白時系統會從 06. 標準工時補齊。")
        config["標準天數"] = st.column_config.NumberColumn("標準天數", min_value=0.0, step=0.1, format="%g", help="機台停留格位的工作天數；12 場地週轉會引用。")
        config["格位數"] = st.column_config.NumberColumn("格位數", min_value=1, max_value=20, step=1, format="%d", help="該 P/N / Type 預排時會佔用幾個場地格位；供 12 場地週轉使用。")
        config["需求工時"] = st.column_config.NumberColumn("需求工時", min_value=0.0, step=0.1, format="%g", help="系統欄位：排除後需求工時；原始需求工時 = 台數 × 標準工時，再扣除產能計算排除組立地點。")
    return config

def _canonicalize_table_columns(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    rename_map = {col: canonical_column_name(table_name, col) for col in out.columns}
    out = out.rename(columns=rename_map)
    if out.columns.duplicated().any():
        merged = pd.DataFrame(index=out.index)
        for col in dict.fromkeys(out.columns):
            same = out.loc[:, out.columns == col]
            merged[col] = same.bfill(axis=1).iloc[:, 0] if same.shape[1] > 1 else same.iloc[:, 0]
        out = merged
    return out


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "nat", "none", "<na>"}:
        return None
    try:
        parsed = pd.to_datetime(text, errors="coerce")
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.date()


def _date_value_for_save(value: object) -> str | None:
    parsed = _parse_date(value)
    return parsed.isoformat() if parsed is not None else None


def _normalize_personnel_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Store personnel date columns as ISO strings or blank values.

    st.data_editor with DateColumn can return pandas Timestamp / datetime.date values.
    Authority JSON should not store pandas objects, and blank 離職日 must remain blank
    to mean the person is still active.
    """
    if df is None or df.empty:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    out = df.copy()
    for col in PERSONNEL_DATE_COLUMNS:
        if col in out.columns:
            out[col] = out[col].map(_date_value_for_save)
    return out


def _prepare_editor_source_for_table(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Return a data_editor-compatible frame without changing the saved state.

    Streamlit DateColumn requires the displayed dataframe column schema to be a
    date/datetime-like dtype. Existing authority JSON stores dates as strings, and
    old blank cells can be None/empty strings. Passing that object/string column to
    DateColumn triggers StreamlitAPIException before the editor is rendered.
    Convert only the temporary editor copy to datetime64[ns]; merge/save paths
    convert edited values back to ISO text.
    """
    if df is None or df.empty:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    out = df.copy()
    if table_name in {"employees", "dispatch"}:
        out = apply_resignation_status_rules(table_name, out)
        for col in PERSONNEL_DATE_COLUMNS:
            if col in out.columns:
                out[col] = pd.to_datetime(out[col], errors="coerce")
    if SELECT_COL in out.columns:
        out[SELECT_COL] = out[SELECT_COL].fillna(False).astype(bool)
    return out


def _reference_date_for_year_value(year_value: object, fallback: date | None = None) -> date:
    """Return the date used to display personnel tenure for a row year.

    Current year uses today; other years use Dec 31 of that row year.  This makes
    01/02 show 2025 tenure when the user is viewing 2025, and 2026 tenure when
    the user is viewing 2026, instead of always calculating from today.
    """
    today = fallback or date.today()
    if year_value is None:
        return today
    text = str(year_value).strip()
    if not text or text.lower() in {"nan", "nat", "none", "<na>", "全部", "all"}:
        return today
    try:
        year = int(float(text))
    except Exception:
        return today
    if year == int(today.year):
        return today
    return date(year, 12, 31)


def _tenure_text(start_value: object, today: date | None = None) -> str:
    start = _parse_date(start_value)
    if start is None:
        return ""
    today = today or date.today()
    if start > today:
        return "0年0月0天"
    years = today.year - start.year
    months = today.month - start.month
    days = today.day - start.day
    if days < 0:
        first_of_month = date(today.year, today.month, 1)
        prev_month_last = first_of_month - timedelta(days=1)
        days += prev_month_last.day
        months -= 1
    if months < 0:
        years -= 1
        months += 12
    return f"{max(years, 0)}年{max(months, 0)}月{max(days, 0)}天"


def _row_tenure_text(row: pd.Series, fallback: date | None = None) -> str:
    ref_date = _reference_date_for_year_value(row.get("年份"), fallback) if "年份" in row.index else (fallback or date.today())
    # If the person already left before the row-year reference date, tenure stops
    # at the resignation date. Blank 離職日 means still employed.
    leave_date = None
    for leave_col in ["離職日", "離職日期", "退職日", "離場日", "退場日"]:
        if leave_col in row.index:
            leave_date = _parse_date(row.get(leave_col))
            if leave_date is not None:
                break
    if leave_date is not None and leave_date < ref_date:
        ref_date = leave_date
    return _tenure_text(row.get("到職日"), ref_date)



def _is_yes_value(value: object, *, default: bool = False) -> bool:
    text = "" if value is None else str(value).strip()
    if not text or text.lower() in {"nan", "nat", "none", "<na>"}:
        return default
    return text in {"是", "Y", "y", "Yes", "YES", "yes", "True", "true", "1", "直接", "直接人力", "啟用"}


def _is_active_value(value: object, *, default: bool = True) -> bool:
    text = "" if value is None else str(value).strip()
    if not text or text.lower() in {"nan", "nat", "none", "<na>"}:
        return default
    return text not in {"否", "N", "n", "No", "NO", "no", "False", "false", "0", "停用", "離職", "離場"}


def _safe_float_value(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    text = str(value).strip().replace(",", "")
    if not text or text.lower() in {"nan", "nat", "none", "<na>"}:
        return default
    if text.endswith("%"):
        try:
            return float(text[:-1].strip()) / 100.0
        except Exception:
            return default
    try:
        return float(text)
    except Exception:
        return default


def _normalize_available_ratio(value: object) -> float | None:
    """Normalize 可用比例 without turning an existing user edit back to 0.00.

    The personnel editors accept 1, 0.5, and 50%.  Blank values should remain
    blank in the master data instead of being forced to 0.00, because forcing a
    default masks whether the value was truly saved.
    """
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text.lower() in {"nan", "nat", "none", "<na>"}:
        return None
    if text.endswith("%"):
        try:
            return round(float(text[:-1].strip()) / 100.0, 6)
        except Exception:
            return None
    try:
        return round(float(text), 6)
    except Exception:
        return None


def _normalize_personnel_ratio_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "可用比例" in out.columns:
        out["可用比例"] = out["可用比例"].apply(_normalize_available_ratio)
    return out


def _resignation_reference_date_for_row(row: pd.Series, fallback: date | None = None) -> date:
    """Return the reference date used to auto-close resigned personnel rows.

    Current-year rows use today so a resignation is reflected immediately.
    Historical/future year rows use that row year's Dec 31, matching the existing
    年資 calculation and avoiding incorrect status changes in old annual records.
    """
    try:
        return _reference_date_for_year_value(row.get("年份"), fallback)
    except Exception:
        return fallback or date.today()


def apply_resignation_status_rules(table_name: str, df: pd.DataFrame, reference_date: date | None = None) -> pd.DataFrame:
    """Automatically remove resigned people from active/direct manpower.

    Business rule:
    - 離職日前仍算人力；
    - 離職日當天起視為不啟用、非直接人力、可用比例 0；
    - 空白離職日代表仍在職。

    This only touches 01/02 personnel tables and keeps other user-edited fields.
    """
    if table_name not in {"employees", "dispatch"}:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    out = _canonicalize_table_columns(table_name, df.copy())
    leave_col = next((c for c in ["離職日", "離職日期", "退職日", "離場日", "退場日"] if c in out.columns), None)
    if not leave_col:
        return out
    for col, default in [("啟用", "是"), ("是否直接人力", "是"), ("可用比例", 1.0)]:
        if col not in out.columns:
            out[col] = default

    inactive_mask = []
    fallback = reference_date or date.today()
    for _, row in out.iterrows():
        leave_date = _parse_date(row.get(leave_col))
        if leave_date is None:
            inactive_mask.append(False)
            continue
        ref = reference_date or _resignation_reference_date_for_row(row, fallback)
        # 離職日當天起扣除人力。
        inactive_mask.append(leave_date <= ref)

    if any(inactive_mask):
        mask = pd.Series(inactive_mask, index=out.index, dtype=bool)
        out.loc[mask, "啟用"] = "否"
        out.loc[mask, "是否直接人力"] = "否"
        out.loc[mask, "可用比例"] = 0.0
    return out


def _personnel_save_columns(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Save personnel pages with a clean schema and preserve edited ratio values.

    Some older imports wrote first-row values into the JSON column list.  Those
    stray columns can keep the editor state unstable and make 可用比例 look like it
    reverted after save.  Personnel authority files are normalized back to their
    official schema at save time while keeping all official editable fields.
    """
    if table_name not in {"employees", "dispatch"}:
        return df
    out = _canonicalize_table_columns(table_name, df.copy())
    if out.columns.duplicated().any():
        merged = pd.DataFrame(index=out.index)
        for col in dict.fromkeys(out.columns):
            same = out.loc[:, out.columns == col]
            if same.shape[1] == 1:
                merged[col] = same.iloc[:, 0]
            elif col == "可用比例":
                # Prefer the right-most non-empty value; the editor column is the
                # most recent one after schema normalization.
                merged[col] = same.ffill(axis=1).iloc[:, -1]
            else:
                merged[col] = same.bfill(axis=1).iloc[:, 0]
        out = merged
    official = [c for c in schema_for_table(table_name) if c in out.columns]
    for col in schema_for_table(table_name):
        if col not in out.columns:
            out[col] = None
            official.append(col)
    out = out[official]
    out = _normalize_personnel_ratio_columns(out)
    out = apply_resignation_status_rules(table_name, out)
    out = _normalize_personnel_date_columns(out)
    return out


def _remove_person_row_available_manpower_column(df: pd.DataFrame) -> pd.DataFrame:
    """Remove the row-level 可用人力 column from 01/02 tables.

    可用總人力 is now shown as a summary KPI on each personnel page. The
    editable personnel tables keep only 可用比例 so users do not see a duplicated
    system-calculated row field in the wrong location. Existing authority JSONs
    that already contain 可用人力 are cleaned on the next save.
    """
    out = df.copy()
    if "可用人力" in out.columns:
        out = out.drop(columns=["可用人力"])
    return out

def recalculate_manpower_tenure(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    out = _canonicalize_table_columns(table_name, df)
    if table_name not in {"employees", "dispatch"}:
        return out
    if "到職日" in out.columns:
        if "年份" in out.columns:
            out["累積年資"] = out.apply(_row_tenure_text, axis=1)
        else:
            out["累積年資"] = out["到職日"].apply(_tenure_text)
    out = _normalize_personnel_ratio_columns(out)
    out = apply_resignation_status_rules(table_name, out)
    out = _remove_person_row_available_manpower_column(out)
    # Keep system-owned columns next to their source fields for readability.
    ordered = []
    for col in out.columns:
        if col in {"累積年資"}:
            continue
        ordered.append(col)
        if col == "到職日" and "累積年資" in out.columns:
            ordered.append("累積年資")
    for col in ["累積年資"]:
        if col in out.columns and col not in ordered:
            ordered.append(col)
    return out[[c for c in ordered if c in out.columns]]

def _table_state_key(table_name: str) -> str:
    return f"managed_table_df_{table_name}"


def _editor_window_key(table_name: str) -> str:
    return f"managed_table_editor_window_{table_name}"


def _editor_revision_key(table_name: str) -> str:
    """Return the revision key used to rebuild a data_editor after form submit.

    Streamlit renders the editor before the submit handler runs.  Without a new
    widget key, the browser can reuse the pre-submit widget state and briefly
    show the old dataframe again even though the authority JSON was saved.
    Incrementing this revision after apply/save/delete forces the next rerun to
    render from the latest managed-table state.
    """
    return f"managed_table_editor_revision_{table_name}"


def _advance_editor_revision(table_name: str) -> int:
    key = _editor_revision_key(table_name)
    try:
        revision = int(st.session_state.get(key, 0)) + 1
    except Exception:
        revision = 1
    st.session_state[key] = revision
    return revision


def _table_flash_key(table_name: str) -> str:
    return f"managed_table_flash_{table_name}"


def _set_table_flash(table_name: str, level: str, message: str, *, show_persistence: bool = False) -> None:
    st.session_state[_table_flash_key(table_name)] = {
        "level": str(level),
        "message": str(message),
        "show_persistence": bool(show_persistence),
    }


def _render_table_flash(table_name: str) -> None:
    payload = st.session_state.pop(_table_flash_key(table_name), None)
    if not isinstance(payload, dict):
        return
    message = str(payload.get("message", "")).strip()
    if not message:
        return
    level = str(payload.get("level", "success")).lower()
    if level == "warning":
        st.warning(message)
    elif level == "error":
        st.error(message)
    elif level == "info":
        st.info(message)
    else:
        st.success(message)
    if bool(payload.get("show_persistence")):
        _display_persistence_result(table_name)


def _render_stable_editor_window(
    table_name: str,
    total_rows: int,
    *,
    title: str,
    description: str,
    default_size: int = 80,
    size_options: list[int] | None = None,
) -> tuple[int, int, bool]:
    """Return a visible row window for editors that should not jump to row 1.

    Streamlit data_editor rebuilds the browser component after a rerun. For long
    manual-edit tables this can move the viewport back to the first row and make
    users think the value disappeared. A persistent row window keeps the same
    block visible across reruns; personnel tables additionally use a form so cell
    edits are applied only when the user submits them.
    """
    if total_rows <= 0:
        return 0, 0, False
    window_key = _editor_window_key(table_name)
    size_options = size_options or [20, 30, 50, 80, 120, 200, 500]
    if default_size not in size_options:
        default_size = size_options[0]
    state = st.session_state.get(window_key, {"start": 0, "size": default_size})
    try:
        start = int(state.get("start", 0))
    except Exception:
        start = 0
    try:
        size = int(state.get("size", default_size))
    except Exception:
        size = default_size
    if size not in size_options:
        size = default_size
    start = max(0, min(start, max(total_rows - 1, 0)))

    st.markdown(f"""
    <div class="stable-editor-card">
      <b>{title}</b><br/>
      <span class="small-muted">{description}</span>
    </div>
    """, unsafe_allow_html=True)
    c0, c1, c2, c3, c4 = st.columns([1.1, 1.2, 1, 1, 2.2])
    with c0:
        size = st.selectbox("每次顯示筆數", size_options, index=size_options.index(size), key=f"{table_name}_editor_page_size")
    max_start = max(total_rows - 1, 0)
    with c1:
        row_number = st.number_input("跳到第幾筆", min_value=1, max_value=max(total_rows, 1), value=min(start + 1, max(total_rows, 1)), step=1, key=f"{table_name}_editor_row_number")
    if c2.button("上一段", key=f"{table_name}_editor_prev", use_container_width=True):
        start = max(0, start - int(size))
    if c3.button("下一段", key=f"{table_name}_editor_next", use_container_width=True):
        start = min(max_start, start + int(size))
    jump_start = max(0, min(int(row_number) - 1, max_start))
    last_key = f"{table_name}_editor_row_number_last"
    if jump_start != start and st.session_state.get(last_key) != int(row_number):
        start = jump_start
    st.session_state[last_key] = int(row_number)
    end = min(start + int(size), total_rows)
    with c4:
        st.info(f"目前顯示第 {start + 1:,} ～ {end:,} 筆，共 {total_rows:,} 筆。新增資料請使用上方『新增空白列』按鈕。", icon="📍")
    st.session_state[window_key] = {"start": int(start), "size": int(size)}
    return int(start), int(end), True


def _render_schedule_editor_window(total_rows: int) -> tuple[int, int, bool]:
    return _render_stable_editor_window(
        "schedule",
        total_rows,
        title="排程表穩定編輯模式",
        description="表格輸入會觸發 Streamlit 重新整理。這裡改用固定列範圍編輯，避免每改一格就跳回第 1 筆資料。",
        default_size=80,
        size_options=[30, 50, 80, 120, 200, 500],
    )


def _render_personnel_editor_window(table_name: str, total_rows: int) -> tuple[int, int, bool]:
    title = "人員名單穩定編輯模式"
    return _render_stable_editor_window(
        table_name,
        total_rows,
        title=title,
        description="01/02 人員表改為固定區段 + 表單送出。可先連續修改多格，按下方『套用編輯暫存』或『儲存資料』後才會更新畫面，避免游標跳回第 1 列與輸入值消失。",
        default_size=30,
        size_options=[20, 30, 50, 80, 120, 200],
    )


def _render_standard_hours_editor_window(total_rows: int) -> tuple[int, int, bool]:
    return _render_stable_editor_window(
        "standard_hours",
        total_rows,
        title="標準工時穩定編輯模式",
        description="06 標準工時改為固定區段 + 表單送出。可先連續修改多格，按下方『套用編輯暫存』或『儲存資料』後才會更新畫面，避免游標跳回第 1 列與輸入值消失。",
        default_size=50,
        size_options=[20, 30, 50, 80, 120, 200, 500],
    )


def _ensure_state_df(table_name: str, source_df: pd.DataFrame) -> pd.DataFrame:
    key = _table_state_key(table_name)
    source_df = _canonicalize_table_columns(table_name, source_df.copy())
    if table_name in {"employees", "dispatch"}:
        source_df = recalculate_manpower_tenure(table_name, source_df)
        source_df = apply_resignation_status_rules(table_name, source_df)
    if SELECT_COL in source_df.columns:
        source_df = source_df.drop(columns=[SELECT_COL])
    if table_name in {"employees", "dispatch"} and "可用人力" in source_df.columns:
        source_df = source_df.drop(columns=["可用人力"])
    for col in schema_for_table(table_name):
        if col not in source_df.columns:
            source_df[col] = None
    source_df = source_df[normalize_columns(table_name, list(source_df.columns))]
    if key not in st.session_state:
        state_df = source_df.copy()
        state_df.insert(0, SELECT_COL, False)
        st.session_state[key] = state_df
    return st.session_state[key]


def _replace_state_df(table_name: str, df: pd.DataFrame) -> None:
    key = _table_state_key(table_name)
    st.session_state[key] = df.copy()


def clear_managed_table_state(table_name: str) -> None:
    """Clear cached editor state so newly saved authority data appears immediately after rerun."""
    key = _table_state_key(table_name)
    if key in st.session_state:
        del st.session_state[key]
    # A new editor revision prevents an old data_editor delta from being replayed
    # after another tool (for example the tenure-ratio tool) updates authority data.
    _advance_editor_revision(table_name)



def _display_persistence_result(table_name: str) -> None:
    """Show whether the save reached GitHub, not only local temporary storage."""
    status = st.session_state.get("last_github_sync_status")
    table_display = {"employees": "01. 超慧員工名單", "dispatch": "02. 派遣名單"}.get(table_name, table_name)
    if isinstance(status, dict):
        msg = str(status.get("message", "")).strip()
        if status.get("ok"):
            if table_name in {"employees", "dispatch"}:
                st.success(f"{table_display} 已寫入權威 JSON，並完成 GitHub 同步。Reboot App 後會保留本次設定。")
            return
        if msg:
            st.warning(f"{table_display} 已寫入本機權威 JSON，但 {msg}")
            return
    if table_name in {"employees", "dispatch"}:
        st.info(f"{table_display} 已寫入本機權威 JSON；若部署在 Streamlit Cloud，請確認 GitHub 同步已啟用，否則 Reboot App 後會回到 GitHub 內舊資料。", icon="💾")


def clean_table_for_save(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if SELECT_COL in out.columns:
        out = out.drop(columns=[SELECT_COL])
    return out.dropna(how="all")


def _safe_file_prefix(file_prefix: str) -> str:
    return str(file_prefix).replace("/", "_").replace("\\", "_").replace(" ", "_")


def render_excel_download(df: pd.DataFrame, file_prefix: str, label: str = "匯出目前資料 Excel", key: str | None = None) -> None:
    """Backward-compatible single-section Excel download.

    New module pages should not call this directly. Use render_module_report_download instead,
    so the export contains the whole module and charts.
    """
    export_df = clean_table_for_save(df)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_prefix = _safe_file_prefix(file_prefix)
    st.download_button(
        label=label,
        data=dataframe_to_excel_bytes(export_df, sheet_name=str(file_prefix)[:31]),
        file_name=f"{safe_prefix}_{stamp}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=key or f"download_{safe_prefix}_{stamp}",
    )


def render_multi_sheet_download(sheets: dict[str, pd.DataFrame], file_prefix: str, label: str = "匯出 Excel", key: str | None = None) -> None:
    """Backward-compatible multi-sheet download.

    New module pages should use render_module_report_download for full module reports with charts.
    """
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_prefix = _safe_file_prefix(file_prefix)
    st.download_button(
        label=label,
        data=multi_sheet_excel_bytes(sheets),
        file_name=f"{safe_prefix}_{stamp}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=key or f"download_multi_{safe_prefix}_{stamp}",
    )


def render_module_report_download(
    module_title: str,
    sheets: Mapping[str, pd.DataFrame],
    *,
    chart_specs: Sequence[Mapping[str, Any]] | None = None,
    metadata: Mapping[str, Any] | None = None,
    label: str = "匯出整個模組 Excel（含完整資料與圖表）",
    key: str | None = None,
) -> None:
    """Render a module-level Excel export section without slowing every rerun.

    Earlier versions generated the Excel bytes directly inside ``st.download_button``.
    Streamlit reruns the whole page after save buttons, so 04/05 paid the full
    Excel workbook generation cost even when the user did not intend to export.

    This version is lazy: users first click ``準備匯出檔``; only then are the
    workbook bytes built and cached in session_state. Normal page saves therefore
    do not spend time creating Excel reports.
    """
    safe_prefix = _safe_file_prefix(module_title)
    base_key = key or f"module_report_{safe_prefix}"
    prepared_key = f"{base_key}_prepared_bytes"
    prepared_name_key = f"{base_key}_prepared_name"

    st.markdown(
        """
        <div class="module-export-card">
          <b>整個模組匯出</b><br/>
          <span class="small-muted">為了加快儲存速度，Excel 檔不會在每次畫面刷新時自動產生。需要下載時，請先按『準備匯出檔』。</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns([1, 3])
    with c1:
        prepare = st.button("準備匯出檔", key=f"{base_key}_prepare", type="secondary", use_container_width=True)
    if prepare:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        with st.spinner("正在產生整個模組 Excel，請稍候..."):
            st.session_state[prepared_key] = module_report_excel_bytes(module_title, sheets, chart_specs=chart_specs, metadata=metadata)
            st.session_state[prepared_name_key] = f"{safe_prefix}_{stamp}.xlsx"
        st.success("匯出檔已準備完成，可按右側下載。")
    with c2:
        if prepared_key in st.session_state:
            st.download_button(
                label=label,
                data=st.session_state[prepared_key],
                file_name=st.session_state.get(prepared_name_key, f"{safe_prefix}.xlsx"),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"{base_key}_download",
                type="primary",
                use_container_width=True,
            )
        else:
            st.info("尚未產生匯出檔；按左側『準備匯出檔』後才會建立 Excel，避免存檔時卡頓。", icon="📦")


def _column_settings_ui(table_name: str, all_columns: list[str]) -> tuple[list[str], int, bool]:
    settings = load_table_settings(table_name)
    actual_cols = [c for c in all_columns if c != SELECT_COL]
    saved_visible = [c for c in settings.get("visible_columns", actual_cols) if c in actual_cols]
    if not saved_visible:
        saved_visible = actual_cols
    saved_order = [c for c in settings.get("column_order", saved_visible) if c in actual_cols]
    saved_order += [c for c in saved_visible if c not in saved_order]
    saved_visible, saved_order, _required_visible_changed = _ensure_required_visible_columns(table_name, actual_cols, saved_visible, saved_order)
    default_height = int(settings.get("height", 520))
    if _required_visible_changed:
        save_table_settings(table_name, {"visible_columns": saved_visible, "column_order": saved_order, "height": default_height}, user="system_migrate_personnel_columns")
    with st.expander("表格欄位、順序與操作設定", expanded=False):
        st.caption("這裡的顯示欄位、欄位順序與表格高度，按下『套用設定』後會永久保存；按『恢復設定』會恢復預設欄位並永久保存。")
        visible = st.multiselect("顯示欄位", actual_cols, default=saved_visible, key=f"visible_cols_{table_name}")
        order = st.multiselect("欄位順序（先選的排前面）", actual_cols, default=saved_order, key=f"order_cols_{table_name}")
        height = st.slider("表格高度", min_value=320, max_value=900, value=default_height, step=20, key=f"height_{table_name}")
        c1, c2, c3 = st.columns([1, 1, 4])
        apply = c1.button("套用設定", type="primary", key=f"apply_col_settings_{table_name}")
        reset = c2.button("恢復設定", key=f"reset_col_settings_{table_name}")
        if reset:
            visible = actual_cols
            order = actual_cols
            height = 520
            save_table_settings(table_name, {"visible_columns": visible, "column_order": order, "height": height}, user="streamlit")
            st.success("已恢復預設設定，並已永久保存。")
            st.rerun()
        if apply:
            final_order = [c for c in order if c in visible] + [c for c in visible if c not in order]
            save_table_settings(table_name, {"visible_columns": visible, "column_order": final_order, "height": height}, user="streamlit")
            st.success("設定已永久套用。")
            st.rerun()
        final_order = [c for c in order if c in visible] + [c for c in visible if c not in order]
        return final_order, height, apply


def render_saveable_table(
    table_name: str,
    title: str,
    height: int = 520,
    helper_text: str | None = None,
    before_save: Callable[[pd.DataFrame], pd.DataFrame | None] | None = None,
    after_save: Callable[[pd.DataFrame], None] | None = None,
    row_filter_column: str | None = None,
    row_filter_value: object | None = None,
    new_row_defaults: Mapping[str, object] | None = None,
    row_filter_label: str | None = None,
    state_transform: Callable[[pd.DataFrame], pd.DataFrame | None] | None = None,
) -> pd.DataFrame:
    _render_table_flash(table_name)
    source_df = load_table(table_name)
    state_df = _ensure_state_df(table_name, source_df)
    # 01/02 are editable master-data tables.  Do not automatically run the
    # tenure-ratio transform during every rerun, because that can overwrite a
    # value the user has just entered before the form submit is merged.  The
    # dedicated tenure-ratio tool remains the explicit way to batch-apply rules.
    if state_transform is not None and table_name not in {"employees", "dispatch"}:
        try:
            transformed_state = state_transform(state_df.copy())
        except Exception as exc:
            transformed_state = None
            st.warning(f"{title} 自動計算欄位時發生例外，已保留原資料：{exc}")
        if isinstance(transformed_state, pd.DataFrame) and not transformed_state.equals(state_df):
            _replace_state_df(table_name, transformed_state)
            state_df = transformed_state
    if helper_text:
        st.info(helper_text, icon="💡")
    st.caption(f"資料來源：data/persistent/authority/{table_name}.json。畫面編輯後需按『儲存資料』才會永久寫入權威資料。")
    if table_name in {"employees", "dispatch"}:
        st.caption("離職日規則：離職日前仍算人力；離職日當天起自動視為『啟用=否、是否直接人力=否、可用比例=0』，並從產能、人力摘要與組織圖扣除。按『儲存資料』後會寫回權威資料。")

    display_cols, table_height, _ = _column_settings_ui(table_name, list(state_df.columns))
    display_cols = [c for c in display_cols if c in state_df.columns]
    editor_cols = [SELECT_COL] + display_cols
    editor_source_full = state_df.reindex(columns=editor_cols)

    row_filter_active = False
    row_filter_suffix = ""
    if row_filter_column and row_filter_value is not None and row_filter_column in editor_source_full.columns:
        row_filter_active = True
        filter_text = str(row_filter_value).strip()
        filter_mask = editor_source_full[row_filter_column].astype(str).str.strip().eq(filter_text)
        total_before_filter = int(len(editor_source_full))
        editor_source_full = editor_source_full.loc[filter_mask].copy()
        safe_filter_text = "".join(ch if ch.isalnum() else "_" for ch in filter_text) or "filter"
        row_filter_suffix = f"_{row_filter_column}_{safe_filter_text}"
        label = row_filter_label or f"{row_filter_column} = {filter_text}"
        st.info(f"目前表格只顯示：{label}；共 {len(editor_source_full):,} 筆 / 全部 {total_before_filter:,} 筆。新增空白列會自動帶入此篩選值，儲存時仍會保存完整全年度資料。", icon="🔎")

    stable_form_table = table_name in {"employees", "dispatch", "standard_hours"}
    c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1.2, 2.8])
    if c1.button("新增空白列", key=f"add_row_{table_name}"):
        new_df = st.session_state[_table_state_key(table_name)].copy()
        blank = {col: None for col in new_df.columns}
        blank[SELECT_COL] = False
        for _default_col, _default_value in (new_row_defaults or {}).items():
            if _default_col in blank:
                blank[_default_col] = _default_value
        new_df.loc[len(new_df)] = blank
        _replace_state_df(table_name, new_df)
        if table_name in {"employees", "dispatch", "schedule", "standard_hours"}:
            window_key = _editor_window_key(table_name)
            current_window = st.session_state.get(window_key, {})
            try:
                if table_name in {"employees", "dispatch"}:
                    default_window_size = 30
                elif table_name == "standard_hours":
                    default_window_size = 50
                else:
                    default_window_size = 80
                current_size = int(current_window.get("size", default_window_size))
            except Exception:
                current_size = 30 if table_name in {"employees", "dispatch"} else (50 if table_name == "standard_hours" else 80)
            st.session_state[window_key] = {"start": max(0, len(new_df) - current_size), "size": current_size}
        st.rerun()
    # 全選 / 取消全選必須遵守目前查詢篩選。
    # 例如 06.標準工時選擇 2025 年時，只能影響 2025 年資料，
    # 不可把 2026 或其他年度一起勾選 / 取消。
    current_query_index = list(editor_source_full.index) if row_filter_active else list(state_df.index)
    current_query_label = row_filter_label or (f"{row_filter_column} = {row_filter_value}" if row_filter_active else "全部資料")
    if c2.button("全選資料", key=f"select_all_{table_name}"):
        new_df = st.session_state[_table_state_key(table_name)].copy()
        if SELECT_COL not in new_df.columns:
            new_df.insert(0, SELECT_COL, False)
        valid_index = [idx for idx in current_query_index if idx in new_df.index]
        if valid_index:
            new_df.loc[valid_index, SELECT_COL] = True
            _replace_state_df(table_name, new_df)
            st.rerun()
        else:
            st.warning("目前查詢沒有可全選的資料。")
    if c3.button("取消全選", key=f"unselect_all_{table_name}"):
        new_df = st.session_state[_table_state_key(table_name)].copy()
        if SELECT_COL not in new_df.columns:
            new_df.insert(0, SELECT_COL, False)
        valid_index = [idx for idx in current_query_index if idx in new_df.index]
        if valid_index:
            new_df.loc[valid_index, SELECT_COL] = False
            _replace_state_df(table_name, new_df)
            st.rerun()
        else:
            st.warning("目前查詢沒有可取消勾選的資料。")
    if stable_form_table:
        c4.button("刪除請用下方", key=f"delete_selected_{table_name}_disabled", disabled=True, help="此表格採表單穩定編輯模式；勾選後請使用表格下方『刪除勾選並套用暫存』，才能讀到尚未送出的勾選狀態。")
    elif c4.button("刪除選取資料", key=f"delete_selected_{table_name}"):
        new_df = st.session_state[_table_state_key(table_name)].copy()
        selected = new_df[SELECT_COL].fillna(False).astype(bool) if SELECT_COL in new_df.columns else pd.Series(False, index=new_df.index)
        if selected.any():
            new_df = new_df.loc[~selected].reset_index(drop=True)
            if SELECT_COL not in new_df.columns:
                new_df.insert(0, SELECT_COL, False)
            _replace_state_df(table_name, new_df)
            st.success(f"已從畫面移除 {int(selected.sum())} 筆，請按『儲存資料』才會永久生效。")
            st.rerun()
        else:
            st.warning("尚未選取資料。")
    with c5:
        st.info("單一表格匯出已移除；請使用頁面下方『整個模組匯出』下載完整資料與圖表。", icon="📘")

    editor_source = editor_source_full
    editor_window_enabled = False
    editor_start = 0
    editor_end = len(editor_source_full)
    if table_name == "schedule" and len(editor_source_full) > 0:
        editor_start, editor_end, editor_window_enabled = _render_schedule_editor_window(len(editor_source_full))
        editor_source = editor_source_full.iloc[editor_start:editor_end].copy()
    elif table_name in {"employees", "dispatch"} and len(editor_source_full) > 0:
        editor_start, editor_end, editor_window_enabled = _render_personnel_editor_window(table_name, len(editor_source_full))
        editor_source = editor_source_full.iloc[editor_start:editor_end].copy()
    elif table_name == "standard_hours" and len(editor_source_full) > 0:
        editor_start, editor_end, editor_window_enabled = _render_standard_hours_editor_window(len(editor_source_full))
        editor_source = editor_source_full.iloc[editor_start:editor_end].copy()
    elif row_filter_active:
        editor_start, editor_end, editor_window_enabled = 0, len(editor_source_full), True
        editor_source = editor_source_full.copy()

    disabled_columns: list[str] = []
    if table_name in {"employees", "dispatch"}:
        for _system_col in ["累積年資"]:
            if _system_col in editor_source.columns:
                disabled_columns.append(_system_col)
    if table_name == "schedule" and "需求工時" in editor_source.columns:
        disabled_columns.append("需求工時")
    try:
        editor_revision = int(st.session_state.get(_editor_revision_key(table_name), 0))
    except Exception:
        editor_revision = 0
    editor_key = f"editor_{table_name}_r{editor_revision}{row_filter_suffix}"
    editor_num_rows = "dynamic"
    if editor_window_enabled:
        # Use a range-specific key so Streamlit keeps a stable editor state within
        # the current row block. Row creation is handled by the explicit
        # 新增空白列 button above to avoid losing off-screen rows.
        # Include the row filter in the key so switching years never reuses the
        # previous year's data_editor widget state.
        editor_key = f"editor_{table_name}_{editor_start}_{editor_end}_r{editor_revision}{row_filter_suffix}"
        editor_num_rows = "fixed"

    def _edited_with_widget_delta(edited_df: pd.DataFrame) -> pd.DataFrame:
        """Apply Streamlit data_editor deltas before merging the form result.

        On some Streamlit versions, especially with NumberColumn in a form, the
        returned DataFrame can lag one edit behind when the user directly presses
        儲存資料.  The widget state keeps the authoritative edited_rows delta, so
        we apply it explicitly.  This prevents fields such as 02.派遣名單「可用比例」
        from visually snapping back to the previous 0.00 after save.
        """
        # Keep editor data mutable as object dtype before applying widget deltas.
        # Streamlit/Pandas can return string columns as ArrowStringArray while
        # DateColumn returns datetime.date / Timestamp values. Assigning those
        # directly into Arrow string columns raises TypeError in Pandas 3 / Python 3.14.
        out = edited_df.copy().astype("object")
        widget_state = st.session_state.get(editor_key)
        if not isinstance(widget_state, dict):
            return out
        edited_rows = widget_state.get("edited_rows") or {}
        if not isinstance(edited_rows, dict):
            return out
        for row_key, changes in edited_rows.items():
            if not isinstance(changes, dict):
                continue
            try:
                row_pos = int(row_key)
            except Exception:
                continue
            if row_pos < 0 or row_pos >= len(out):
                continue
            target_index = out.index[row_pos]
            for col, value in changes.items():
                if col in out.columns:
                    out.at[target_index, col] = value
        return out

    def _merge_editor_result(current_df: pd.DataFrame, edited_df: pd.DataFrame) -> pd.DataFrame:
        edited_df = _edited_with_widget_delta(edited_df)
        if editor_window_enabled or row_filter_active:
            # Use object dtype during merge to avoid Pandas ArrowStringArray setitem
            # errors when the editor returns date objects for 到職日 / 離職日 or
            # None values for text columns. The save pipeline normalizes values after merge.
            merged = current_df.copy().astype("object")
            # Merge only the visible window / filtered year back into the full table,
            # preserving rows from other years. The original row index is kept by
            # iloc/loc slicing, so the same row remains visible after rerun.
            for pos, idx in enumerate(edited_df.index):
                target_idx = idx if idx in merged.index else editor_start + pos
                if target_idx in merged.index:
                    for col in edited_df.columns:
                        merged.loc[target_idx, col] = edited_df.loc[idx, col]
        else:
            current_df = current_df.copy().astype("object")
            edited_df = edited_df.copy().astype("object")
            merged = pd.DataFrame(index=range(len(edited_df)))
            for col in current_df.columns:
                if col in edited_df.columns:
                    merged[col] = edited_df[col].values
                else:
                    merged[col] = current_df[col].reindex(range(len(edited_df))).values
            for col in edited_df.columns:
                if col not in merged.columns:
                    merged[col] = edited_df[col].values
        if SELECT_COL not in merged.columns:
            merged.insert(0, SELECT_COL, False)
        if table_name in {"employees", "dispatch"}:
            # 累積年資是系統計算欄位，不以手填值為準。
            # 只在使用者送出表單後重算，避免每次輸入時重新整理表格造成游標跳位。
            selected_col = merged[SELECT_COL].copy() if SELECT_COL in merged.columns else pd.Series(False, index=merged.index)
            clean_for_calc = merged.drop(columns=[SELECT_COL], errors="ignore")
            clean_for_calc = recalculate_manpower_tenure(table_name, clean_for_calc)
            clean_for_calc.insert(0, SELECT_COL, selected_col.reindex(clean_for_calc.index).fillna(False).astype(bool).values)
            merged = clean_for_calc
        return merged

    def _save_current_state() -> None:
        clean = clean_table_for_save(st.session_state[_table_state_key(table_name)])
        clean = recalculate_manpower_tenure(table_name, clean)
        clean = _personnel_save_columns(table_name, clean)
        # Preserve manual edits in 01/02.  Their former before_save callback
        # recalculated 可用比例 and could immediately replace the submitted value.
        # Batch rule application is handled explicitly by render_tenure_ratio_tool.
        if before_save is not None and table_name not in {"employees", "dispatch"}:
            updated = before_save(clean.copy())
            if isinstance(updated, pd.DataFrame):
                clean = updated
        save_authority_df(table_name, clean, user="streamlit")
        if after_save is not None:
            after_save(clean.copy())
        clear_data_cache()
        # 資料已變更，移除先前準備好的模組匯出檔，避免下載到舊內容。
        for _key in list(st.session_state.keys()):
            if str(_key).endswith("_prepared_bytes") or str(_key).endswith("_prepared_name"):
                del st.session_state[_key]
        refreshed = clean.copy()
        refreshed.insert(0, SELECT_COL, False)
        _replace_state_df(table_name, refreshed)

    if table_name in {"employees", "dispatch", "standard_hours"}:
        if table_name == "standard_hours":
            st.caption("穩定編輯：06 標準工時表格放在表單內，修改多格時不會每格都重新整理；請按下方按鈕套用或儲存。")
        else:
            st.caption("穩定編輯：表格放在表單內，修改多格時不會每格都重新整理；請按下方按鈕套用或儲存。")
        with st.form(f"stable_editor_form_{table_name}", clear_on_submit=False):
            editor_display_source = _prepare_editor_source_for_table(table_name, editor_source)
            edited = st.data_editor(
                editor_display_source,
                use_container_width=True,
                height=table_height or height,
                num_rows=editor_num_rows,
                key=editor_key,
                column_config=_column_config_for_table(table_name),
                disabled=disabled_columns or False,
            )
            f1, f2, f3, f4 = st.columns([1, 1, 1.4, 3.6])
            with f1:
                apply_edit = st.form_submit_button("套用暫存（不永久保存）", type="secondary", use_container_width=True)
            with f2:
                save_edit = st.form_submit_button("儲存資料", type="primary", use_container_width=True)
            with f3:
                delete_edit = st.form_submit_button("刪除勾選並套用暫存", type="secondary", use_container_width=True)
            with f4:
                if table_name == "standard_hours":
                    st.info("可連續編輯多格後再送出；『套用暫存』只保留在目前畫面，Reboot App 不會保留；若要刪除，請在本表勾選後按『刪除勾選並套用暫存』，再按『儲存資料』永久保存。", icon="✅")
                else:
                    st.info("可連續編輯多格後再送出；『套用暫存』只保留在目前畫面，Reboot App 不會保留；要永久保存可用比例等欄位，請按『儲存資料』。『累積年資』會依資料列年份計算：目前年度用今天，其他年度用該年度 12/31；若已填離職日且早於年度基準日，年資計到離職日。", icon="✅")
        if apply_edit or save_edit or delete_edit:
            current = st.session_state[_table_state_key(table_name)].copy()
            merged_state = _merge_editor_result(current, edited)
            if delete_edit:
                selected = merged_state[SELECT_COL].fillna(False).astype(bool) if SELECT_COL in merged_state.columns else pd.Series(False, index=merged_state.index)
                if selected.any():
                    removed_count = int(selected.sum())
                    merged_state = merged_state.loc[~selected].reset_index(drop=True)
                    if SELECT_COL not in merged_state.columns:
                        merged_state.insert(0, SELECT_COL, False)
                    _replace_state_df(table_name, merged_state)
                    window_key = _editor_window_key(table_name)
                    if window_key in st.session_state:
                        current_window = st.session_state.get(window_key, {})
                        try:
                            current_size = int(current_window.get("size", 50 if table_name == "standard_hours" else 30))
                        except Exception:
                            current_size = 50 if table_name == "standard_hours" else 30
                        current_start = max(0, min(int(current_window.get("start", 0)), max(len(merged_state) - 1, 0)))
                        st.session_state[window_key] = {"start": current_start, "size": current_size}
                    _set_table_flash(table_name, "success", f"已從畫面暫存移除 {removed_count} 筆；確認無誤後請按『儲存資料』永久保存。")
                else:
                    _set_table_flash(table_name, "warning", "尚未選取資料。請在表格左側『選取』欄勾選後，再按『刪除勾選並套用暫存』。")
            else:
                _replace_state_df(table_name, merged_state)
                if save_edit:
                    _save_current_state()
                    if table_name == "standard_hours":
                        _set_table_flash(table_name, "success", f"{title} 已永久保存。", show_persistence=True)
                    else:
                        _set_table_flash(
                            table_name,
                            "success",
                            f"{title} 已永久保存；手動輸入內容已保留，並已自動更新『累積年資』。",
                            show_persistence=True,
                        )
                else:
                    _set_table_flash(table_name, "success", "已套用到畫面暫存；確認無誤後再按『儲存資料』永久保存。")
            # The editor was rendered from the pre-submit dataframe earlier in
            # this run.  Rebuild it with a fresh key and current managed state,
            # otherwise Streamlit may display the old value again after submit.
            _advance_editor_revision(table_name)
            st.rerun()
        return clean_table_for_save(st.session_state[_table_state_key(table_name)])

    editor_display_source = _prepare_editor_source_for_table(table_name, editor_source)
    edited = st.data_editor(
        editor_display_source,
        use_container_width=True,
        height=table_height or height,
        num_rows=editor_num_rows,
        key=editor_key,
        column_config=_column_config_for_table(table_name),
        disabled=disabled_columns or False,
    )

    current = st.session_state[_table_state_key(table_name)].copy()
    st.session_state[_table_state_key(table_name)] = _merge_editor_result(current, edited)

    c1, c2 = st.columns([1, 5])
    if c1.button("儲存資料", type="primary", key=f"save_{table_name}"):
        _save_current_state()
        if table_name == "schedule":
            st.success(f"{title} 已永久保存，並已重新計算『需求工時』與同步產能結果。")
        else:
            st.success(f"{title} 已永久保存。")
        _display_persistence_result(table_name)
        # 不強制 st.rerun()：儲存按鈕本身已觸發本次 rerun，
        # 立即再次 rerun 會造成 04/05 畫面閃爍並重複產生圖表/匯出資料。
        # 已保存的 clean 資料會回寫到 session_state，下方分析區可直接使用最新資料。
    with c2:
        st.info("人性化操作：可先調欄位、全選/取消全選、刪除選取、編輯資料；只有按『儲存資料』才會寫入權威資料。", icon="✅")

    return clean_table_for_save(st.session_state[_table_state_key(table_name)])


def render_filter_hint() -> None:
    st.caption("速度設計：篩選條件在表單內調整，只有按下套用/儲存/執行才會重新計算或寫入資料。")


def render_configurable_view(df: pd.DataFrame, table_key: str, title: str, height: int = 460) -> pd.DataFrame:
    view = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if view.empty:
        st.info(f"{title} 目前沒有資料。")
        return view
    display_cols, table_height, _ = _column_settings_ui(f"view_{table_key}", list(view.columns))
    display_cols = [c for c in display_cols if c in view.columns]
    shown = view[display_cols] if display_cols else view
    st.dataframe(numeric_display_styler(shown), use_container_width=True, hide_index=True, height=table_height or height)
    st.caption("本表格屬於模組內容的一部分；可在上方按『套用設定 / 恢復設定』永久保存顯示方式，完整資料與圖表請使用頁面下方『整個模組匯出』。")
    return shown

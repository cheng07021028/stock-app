from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping
import re

import numpy as np
import pandas as pd

from .config import CAPACITY_WORKBOOK, ORG_WORKBOOK
from .persistent_store import dataframe_to_records, load_authority_df, save_authority_records, save_authority_df
from .schema_service import canonical_column_name, normalize_columns, schema_for_table
from .schedule_record_service import (
    SCHEDULE_ID_COL,
    clear_deleted_schedule_tombstones,
    ensure_schedule_record_ids,
    filter_deleted_schedule_import_rows,
    record_deleted_schedule_rows,
)
from .year_service import DEFAULT_YEAR, YEAR_TABLES, ensure_year_column, infer_year_from_text, normalize_year

MONTH_LABELS = [f"{i}月" for i in range(1, 13)]

# Excel sheet -> system authority table mapping used by 10. 資料匯入與版本管理.
IMPORT_TABLE_LABELS = {
    "employees": "01. 超慧員工名單",
    "dispatch": "02. 派遣名單",
    "schedule": "05. 排程表",
    "standard_hours": "06. 標準工時",
    "work_calendar": "07. 工作天數設定",
    "capacity_summary_excel": "04. Excel 原始產能彙整",
}


def _norm_text(value: object) -> str:
    text = str(value or "").strip()
    for ch in [" ", "　", "\t", "\n", "\r", "(", ")", "（", "）", "-", "_"]:
        text = text.replace(ch, "")
    return text.lower()


def resolve_sheet_name(path: Path, desired: str, aliases: Iterable[str] | None = None) -> str:
    """Resolve a worksheet name with safe fuzzy matching.

    Earlier versions required exact sheet names. Uploaded files often have slight naming
    differences such as 標準工時 instead of 標準工時(超), or 2026員工名單 instead of
    員工名單. This resolver keeps exact matching first, then uses conservative fuzzy
    matching so upload import can update the corresponding modules.
    """
    xl = pd.ExcelFile(path, engine="openpyxl")
    candidates = [desired] + list(aliases or [])
    stripped = {sheet.strip(): sheet for sheet in xl.sheet_names}
    for candidate in candidates:
        if candidate in xl.sheet_names:
            return candidate
        if candidate.strip() in stripped:
            return stripped[candidate.strip()]
    normalized_sheets = [(_norm_text(sheet), sheet) for sheet in xl.sheet_names]
    for candidate in candidates:
        norm_candidate = _norm_text(candidate)
        for norm_sheet, sheet in normalized_sheets:
            if norm_candidate and (norm_candidate == norm_sheet or norm_candidate in norm_sheet or norm_sheet in norm_candidate):
                return sheet
    raise ValueError(f"Worksheet named {desired!r} not found in {path.name}; available={xl.sheet_names}")


def _sheet_available(path: Path, desired: str, aliases: Iterable[str] | None = None) -> bool:
    try:
        resolve_sheet_name(path, desired, aliases)
        return True
    except Exception:
        return False


def _strip_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = _coerce_excel_object_frame(df)
    for col in out.select_dtypes(include=["object", "string"]).columns:
        out[col] = out[col].map(lambda x: _cell_to_text(x) if not _is_blank_value(x) else None).astype("object")
    return out.dropna(how="all")


def _excel_date_to_text(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    return value


def _is_blank_value(value: Any) -> bool:
    """Return True for Excel / pandas empty values without raising on arrays."""
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "none", "null", "nat", "<na>"}


def _cell_to_text(value: Any) -> str:
    """Convert one Excel cell to plain text for safe string-column assignment."""
    if _is_blank_value(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    try:
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
    except Exception:
        pass
    text = str(value).strip().replace("\u00a0", " ")
    return re.sub(r"\s+", " ", text)


def _normalize_month_value(value: Any) -> str:
    """Normalize Excel month cells into labels such as 7月 when possible."""
    text = _cell_to_text(value)
    if not text:
        return ""
    match = re.search(r"(?<!\d)(1[0-2]|[1-9])(?:\.0+)?\s*月?", text)
    if match:
        return f"{int(match.group(1))}月"
    return text


def _coerce_excel_object_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Avoid pandas/pyarrow string dtype assignment errors after read_excel.

    Some uploaded workbooks make pandas create extension string columns. Assigning
    numeric Excel cells such as 台數 or 機台計數 back into those columns can raise
    errors like: Invalid value for dtype 'str'.  Use object dtype for import
    normalization, while keeping the original values intact.
    """
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.copy()
    out = df.copy()
    out.columns = [str(c).strip() if not str(c).startswith("Unnamed") else f"欄位_{i+1}" for i, c in enumerate(out.columns)]
    for col in out.columns:
        if str(out[col].dtype).lower() in {"string", "str", "object"}:
            out[col] = out[col].astype("object")
    return out.replace({np.nan: None})


def _schedule_quantity_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize schedule quantity columns from the uploaded 排程表.

    Compatible inputs:
    - Legacy official workbook: 「台數」 holds a month marker such as 1月～12月.
      In that case each non-empty marker represents one scheduled machine.
    - New Page 10 template: 「月份」 holds 1月～12月 and 「機台計數」 holds the
      numeric machine quantity, for example 30 machines in one row.
    - Mixed workbook: 「台數」 keeps the old month marker while 「機台計數」 carries
      the numeric count. The explicit 「機台計數」 value wins and is copied into
      「台數」 so downstream demand-hour calculation still uses the correct count.

    Output rules:
    - 台數_raw preserves the original 「台數」 column for audit/debug.
    - 機台計數 is the monthly machine-count value used by 04/05/09.
    - 台數 is the effective numeric quantity used by 原始需求工時 = 台數 × 標準工時.
    """
    out = df.copy()
    index = out.index

    explicit_count = pd.Series(np.nan, index=index, dtype="float64")
    explicit_has_value = pd.Series(False, index=index)
    if "機台計數" in out.columns:
        explicit_count = pd.to_numeric(out["機台計數"], errors="coerce")
        explicit_has_value = explicit_count.notna() & explicit_count.ge(0)

    if "台數" in out.columns:
        raw = out["台數"].astype("object")
        raw_text = raw.map(_cell_to_text)
        nonblank = raw_text.ne("")
        numeric_qty = pd.to_numeric(raw, errors="coerce")
        marker_count = pd.Series(np.where(nonblank, numeric_qty.fillna(1), 0), index=index, dtype="float64")
        effective_qty = pd.Series(np.where(nonblank, numeric_qty.fillna(1), 1), index=index, dtype="float64")
        out["台數_raw"] = raw
        out["機台計數"] = marker_count.where(~explicit_has_value, explicit_count)
        out["台數"] = effective_qty.where(~explicit_has_value, explicit_count)
    elif "機台計數" in out.columns:
        out["台數_raw"] = None
        out["機台計數"] = explicit_count.where(explicit_has_value, 0)
        out["台數"] = explicit_count.where(explicit_has_value, 1)
    else:
        out["台數_raw"] = None
        out["台數"] = 1
        out["機台計數"] = 1

    out["台數"] = pd.to_numeric(out["台數"], errors="coerce").fillna(1).clip(lower=0)
    out["機台計數"] = pd.to_numeric(out["機台計數"], errors="coerce").fillna(0).clip(lower=0)
    return out

def _canonicalize_import_columns(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Normalize personnel upload columns and add official schema columns.

    This keeps 工段 / 機型 available even when uploaded Excel files use aliases
    such as 工 段, 站別, 製程, Type or 機種. It also prevents older imports from
    storing only legacy column names such as 累計年資.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=schema_for_table(table_name))
    out = df.copy()
    rename_map = {col: canonical_column_name(table_name, col) for col in out.columns}
    out = out.rename(columns=rename_map)
    if out.columns.duplicated().any():
        merged = pd.DataFrame(index=out.index)
        for col in dict.fromkeys(out.columns):
            same = out.loc[:, out.columns == col]
            merged[col] = same.bfill(axis=1).iloc[:, 0] if same.shape[1] > 1 else same.iloc[:, 0]
        out = merged
    for col in schema_for_table(table_name):
        if col not in out.columns:
            out[col] = None
    return out[normalize_columns(table_name, list(out.columns))]


def _add_manpower_flags(employees: pd.DataFrame, dispatch: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    employees = employees.copy()
    dispatch = dispatch.copy()
    indirect_titles = {"經理", "課長", "主任", "助理"}
    employee_title = employees["職 稱"].astype(str).str.strip() if "職 稱" in employees.columns else ""
    employee_station = employees["工段"].fillna("").astype(str).str.strip() if "工段" in employees.columns else ""
    dispatch_station = dispatch["工段"].fillna("").astype(str).str.strip() if "工段" in dispatch.columns else ""
    if not employees.empty:
        employees["人力來源"] = "超慧正職"
        employees["是否直接人力"] = np.where((employee_station != "") & (~employee_title.isin(indirect_titles)), "是", "否")
        employees["可用比例"] = np.where(employees["是否直接人力"].eq("是"), 1.0, 0.0)
        if "啟用" not in employees.columns:
            employees["啟用"] = "是"
    if not dispatch.empty:
        dispatch["人力來源"] = "派遣/外包"
        dispatch["是否直接人力"] = np.where(dispatch_station != "", "是", "否")
        dispatch["可用比例"] = np.where(dispatch["是否直接人力"].eq("是"), 1.0, 0.0)
        if "啟用" not in dispatch.columns:
            dispatch["啟用"] = "是"
    return employees, dispatch


def _match_sheets_by_keywords(path: Path, keywords: Iterable[str]) -> list[str]:
    """Find all worksheet names containing any keyword after loose normalization."""
    try:
        xl = pd.ExcelFile(path, engine="openpyxl")
    except Exception:
        return []
    norm_keywords = [_norm_text(k) for k in keywords if k]
    found: list[str] = []
    for sheet in xl.sheet_names:
        norm_sheet = _norm_text(sheet)
        if any(k and k in norm_sheet for k in norm_keywords):
            found.append(sheet)
    return found




def _read_excel_sheet_auto_header(path: Path, sheet_name: str, table_name: str, max_header_row: int = 6) -> pd.DataFrame:
    """Read an Excel worksheet while tolerating title rows above the real header.

    Some Page 10 templates use row 1 as the header, while organization sheets use
    row 2. Users may also export or edit a workbook that adds a title row before
    the actual columns. The old importer used a fixed header row, so the preview
    could show "worksheet exists but zero rows" even when the sheet had data.
    This helper scores the first few rows and chooses the row that looks most like
    the expected module schema.
    """
    expected_by_table = {
        "employees": ["員工編號", "姓名", "到職日", "離職日", "職 稱", "課別", "部 門", "工段"],
        "dispatch": ["員工編號", "姓名", "到職日", "離職日", "職 稱", "課別", "部 門", "外包商年資", "工段"],
        "schedule": ["年份", "WO", "客戶", "P/N", "Type", "Category", "組立地點", "生產廠區", "機台入庫日", "MOVE IN", "月份", "台數", "機台計數", "標準工時", "需求工時"],
        "standard_hours": ["客戶", "P/N", "Type", "Category", "標準工時", "標準天數", "HOURS", "DAYS", "格位數", "佔用格位數", "占用格位數", "需求格位數", "SLOTS"],
        "work_calendar": ["月份", "月份數字", "月起日", "月迄日", "正常工作日"],
        "capacity_summary_excel": ["每月機台數", "原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "需求總工時", "可用工時", "產能負荷率", "產能餘額"],
    }
    expected = expected_by_table.get(table_name, [])
    raw = _coerce_excel_object_frame(pd.read_excel(path, sheet_name=sheet_name, header=None, engine="openpyxl"))
    if raw.empty:
        return pd.DataFrame()

    def norm(value: Any) -> str:
        return _norm_text(value)

    expected_norm = [norm(x) for x in expected if str(x).strip()]
    best_row = 0
    best_score = -1
    max_row = min(max_header_row, len(raw))
    for row_idx in range(max_row):
        row_values = [norm(x) for x in raw.iloc[row_idx].tolist()]
        score = 0
        for exp in expected_norm:
            if exp and any(exp == cell or exp in cell or cell in exp for cell in row_values if cell):
                score += 1
        # Prefer earlier rows when tied, but row 2 is common for org sheets.
        if score > best_score:
            best_score = score
            best_row = row_idx

    # If no expected headers are found, fall back to the old behavior.
    df = pd.read_excel(path, sheet_name=sheet_name, header=best_row if best_score > 0 else 0, engine="openpyxl")
    return _coerce_excel_object_frame(df)


def _meaningful_row_count(table_name: str, df: pd.DataFrame, fallback_year: int | None = None) -> int:
    """Count rows that really contain importable business data for preview."""
    if df is None or df.empty:
        return 0
    out = df.copy().dropna(how="all")
    if out.empty:
        return 0
    if fallback_year is not None and "年份" in out.columns:
        out = out[out["年份"].map(lambda x: normalize_year(x, fallback_year)).eq(int(fallback_year))].copy()
    if out.empty:
        return 0
    meaningful_cols = {
        "employees": ["員工編號", "姓名", "到職日", "離職日", "職 稱", "課別", "部 門", "工段"],
        "dispatch": ["員工編號", "姓名", "到職日", "離職日", "職 稱", "課別", "部 門", "外包商年資", "工段"],
        "schedule": ["WO", "客戶", "P/N", "Type", "Category", "組立地點", "生產廠區", "機台入庫日", "MOVE IN", "月份", "台數", "機台計數", "標準工時"],
        "standard_hours": ["客戶", "P/N", "Type", "Category", "標準工時", "標準天數", "格位數"],
        "work_calendar": ["月份", "月份數字", "月起日", "月迄日", "正常工作日"],
        "capacity_summary_excel": ["月份", "每月機台數", "原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "需求總工時", "正常可用工時", "正常產能負荷率"],
    }.get(table_name, [])
    present = [c for c in meaningful_cols if c in out.columns]
    if not present:
        return int(len(out))
    check = out[present].copy()
    for col in check.columns:
        check[col] = check[col].map(lambda x: "" if pd.isna(x) else str(x).strip())
    mask = check.ne("").any(axis=1)
    return int(mask.sum())


def _load_single_table_sheet_for_preview(path: Path, table_name: str, sheet_name: str, year: int) -> tuple[pd.DataFrame, str]:
    """Load one matched worksheet for Page 10 preview without failing silently."""
    try:
        if table_name == "schedule":
            df = _load_schedule_sheet(path, sheet_name, year)
        elif table_name == "standard_hours":
            df = _load_standard_hours_sheet(path, sheet_name, year)
        elif table_name == "work_calendar":
            df = _load_work_calendar_sheet(path, sheet_name, year)
        elif table_name == "capacity_summary_excel":
            df = _load_capacity_summary_sheet(path, sheet_name, year)
        elif table_name in {"employees", "dispatch"}:
            df = _read_org_sheet(path, sheet_name, table_name, {_override_key(path, sheet_name, table_name): year})
        else:
            df = pd.DataFrame()
        return df, ""
    except Exception as exc:
        # Last-resort preview fallback for upload screens: when pandas/openpyxl raises
        # a dtype conversion issue, still try to count meaningful rows so the user
        # can see whether the worksheet is structurally readable. Formal import uses
        # the full loaders above after normalization.
        try:
            raw_preview = _read_excel_sheet_auto_header(path, sheet_name, table_name)
            raw_preview = _strip_columns(raw_preview)
            if not raw_preview.empty:
                raw_preview = _with_import_year(table_name, raw_preview, sheet_name, path, year)
                return raw_preview, ""
        except Exception:
            pass
        return pd.DataFrame(), str(exc)[:180]

def _strip_upload_timestamp(filename: str) -> str:
    """Remove Streamlit upload timestamp prefix from saved upload filenames.

    Page 10 stores uploads as YYYYMMDD_HHMMSS_original_name.xlsx. Without stripping
    this prefix, a file named 2025_產能計算.xlsx uploaded on 2026/06/17 is incorrectly
    recognized as 2026. The actual business year should come from the worksheet name
    first, then the original filename after the upload timestamp is removed.
    """
    text = str(filename or "")
    return re.sub(r"^\d{8}_\d{6}_", "", text)


def _find_years(text: Any) -> list[int]:
    return [int(x) for x in re.findall(r"(?<!\d)(20\d{2})(?!\d)", str(text or ""))]


def infer_import_year(sheet_name: str | None = None, path: Path | str | None = None, default: int = DEFAULT_YEAR) -> int:
    """Infer import year for Excel uploads.

    Priority:
      1. Worksheet name, e.g. 2025排程表.
      2. Original uploaded filename after removing Streamlit upload timestamp prefix.
      3. Default year.
    """
    sheet_years = _find_years(sheet_name)
    if sheet_years:
        return sheet_years[0]
    filename = ""
    if path is not None:
        filename = Path(path).name if not isinstance(path, Path) else path.name
    filename = _strip_upload_timestamp(filename)
    file_years = _find_years(filename)
    if file_years:
        return file_years[0]
    return int(default)


def infer_import_year_source(sheet_name: str | None = None, path: Path | str | None = None, default: int = DEFAULT_YEAR) -> str:
    if _find_years(sheet_name):
        return "工作表名稱"
    filename = ""
    if path is not None:
        filename = Path(path).name if not isinstance(path, Path) else path.name
    if _find_years(_strip_upload_timestamp(filename)):
        return "檔名"
    return f"預設值 {default}"


def _override_key(path: Path, sheet_name: str | None, table_name: str | None) -> str:
    return f"{path.name}||{sheet_name or '*'}||{table_name or '*'}"


def _lookup_year_override(year_overrides: Mapping[str, Any] | None, path: Path, sheet_name: str | None, table_name: str | None) -> int | None:
    if not year_overrides:
        return None
    sheet_variants = []
    for value in [sheet_name, str(sheet_name or "").strip()]:
        if value and value not in sheet_variants:
            sheet_variants.append(value)
    if not sheet_variants:
        sheet_variants = ["*"]
    keys = []
    for sheet in sheet_variants:
        keys.extend([
            _override_key(path, sheet, table_name),
            _override_key(path, sheet, "*"),
        ])
    keys.extend([
        _override_key(path, "*", table_name),
        _override_key(path, "*", "*"),
    ])
    for key in keys:
        if key in year_overrides:
            value = normalize_year(year_overrides[key], DEFAULT_YEAR)
            if 2000 <= int(value) <= 2100:
                return int(value)
    return None

def _with_import_year(table_name: str, df: pd.DataFrame, sheet_name: str | None = None, path: Path | None = None, year_override: int | None = None) -> pd.DataFrame:
    year = int(year_override) if year_override is not None else infer_import_year(sheet_name, path, default=DEFAULT_YEAR)
    out = ensure_year_column(table_name, df, year)
    # Import year is the target year selected in Page 10. Force every imported row
    # into that year so upload timestamps or stale source 年份 columns cannot leak in.
    out["年份"] = int(year)
    return out


def _read_org_sheet(path: Path, sheet_name: str, table_name: str, year_overrides: Mapping[str, Any] | None = None) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet_name, header=1, engine="openpyxl")
    df = _canonicalize_import_columns(table_name, _strip_columns(df))
    for _date_col in ["到職日", "離職日"]:
        if _date_col in df.columns:
            df[_date_col] = df[_date_col].map(_excel_date_to_text)
    year_override = _lookup_year_override(year_overrides, path, sheet_name, table_name)
    return _with_import_year(table_name, df, sheet_name, path, year_override)


def load_org_workbook(path: Path = ORG_WORKBOOK, year_overrides: Mapping[str, Any] | None = None) -> dict[str, pd.DataFrame]:
    """Load all yearly employee/dispatch sheets, e.g. 2024員工名單 ~ 2026員工名單."""
    if not path.exists():
        return {"employees": pd.DataFrame(), "dispatch": pd.DataFrame()}
    employee_sheets = _match_sheets_by_keywords(path, ["員工名單", "超慧員工名單"])
    dispatch_sheets = _match_sheets_by_keywords(path, ["派遣名單", "外包名單"])
    # Keep compatibility with the original exact names.
    if not employee_sheets and _sheet_available(path, "2026員工名單", ["員工名單", "超慧員工名單"]):
        employee_sheets = [resolve_sheet_name(path, "2026員工名單", ["員工名單", "超慧員工名單"])]
    if not dispatch_sheets and _sheet_available(path, "2026派遣名單", ["派遣名單", "外包名單"]):
        dispatch_sheets = [resolve_sheet_name(path, "2026派遣名單", ["派遣名單", "外包名單"])]
    emp_frames = []
    dis_frames = []
    for sheet in employee_sheets:
        try:
            emp_frames.append(_read_org_sheet(path, sheet, "employees", year_overrides))
        except Exception:
            continue
    for sheet in dispatch_sheets:
        try:
            dis_frames.append(_read_org_sheet(path, sheet, "dispatch", year_overrides))
        except Exception:
            continue
    employees = pd.concat(emp_frames, ignore_index=True) if emp_frames else pd.DataFrame(columns=schema_for_table("employees"))
    dispatch = pd.concat(dis_frames, ignore_index=True) if dis_frames else pd.DataFrame(columns=schema_for_table("dispatch"))
    employees, dispatch = _add_manpower_flags(employees, dispatch)
    employees = ensure_year_column("employees", employees, DEFAULT_YEAR)
    dispatch = ensure_year_column("dispatch", dispatch, DEFAULT_YEAR)
    return {"employees": employees, "dispatch": dispatch}


def _try_load_org_workbook(path: Path, year_overrides: Mapping[str, Any] | None = None) -> dict[str, pd.DataFrame]:
    tables = load_org_workbook(path, year_overrides=year_overrides)
    return {k: v for k, v in tables.items() if isinstance(v, pd.DataFrame) and not v.empty}


def _load_schedule_sheet(path: Path, sheet_name: str, year_override: int | None = None) -> pd.DataFrame:
    df = _read_excel_sheet_auto_header(path, sheet_name, "schedule")
    df = _strip_columns(df)
    df = df.loc[:, [c for c in df.columns if not str(c).startswith("欄位_") or df[c].notna().any()]]
    # Remove duplicated title/header rows that sometimes remain after users copy or
    # combine Excel templates. This keeps preview counts and imports stable.
    if not df.empty:
        header_like = pd.Series(False, index=df.index)
        for key_col in ["WO", "客戶", "P/N", "Type", "Category", "月份", "機台計數"]:
            if key_col in df.columns:
                header_like = header_like | df[key_col].astype(str).str.strip().eq(key_col)
        df = df.loc[~header_like].copy()
    if "M" in df.columns and "月份" not in df.columns:
        df = df.rename(columns={"M": "月份"})
    # 排程表 J 欄「台數」可能是月份標記；另外產生「機台計數」供 04 月別台數使用。
    df = _schedule_quantity_fields(df)
    if "月份" not in df.columns:
        if "台數_raw" in df.columns:
            df["月份"] = df["台數_raw"].map(_normalize_month_value).replace("", "未設定").astype("object")
        else:
            df["月份"] = "未設定"
    else:
        df["月份"] = df["月份"].astype("object").map(_normalize_month_value)
        blank_month = df["月份"].map(_is_blank_value)
        if "台數_raw" in df.columns:
            fallback_month = df["台數_raw"].map(_normalize_month_value)
            df.loc[blank_month, "月份"] = fallback_month.loc[blank_month].astype("object")
        df["月份"] = df["月份"].map(lambda x: _normalize_month_value(x) or "未設定").astype("object")
    if "標準工時" in df.columns:
        df["標準工時"] = pd.to_numeric(df["標準工時"], errors="coerce")
    else:
        df["標準工時"] = np.nan
    df["原始需求工時"] = pd.to_numeric(df["台數"], errors="coerce").fillna(1) * pd.to_numeric(df["標準工時"], errors="coerce").fillna(0)
    # Import stage has not applied Page 06 exclusions yet. Keep calculated fields
    # internally consistent; recalculate_schedule_demand will rebuild them from
    # current authority exclusion parameters before saving.
    df["產能計算排除工時"] = 0.0
    df["排除後需求工時"] = df["原始需求工時"]
    df["需求工時"] = df["排除後需求工時"]
    return _with_import_year("schedule", df, sheet_name, path, year_override)


def load_schedule(path: Path = CAPACITY_WORKBOOK, year_overrides: Mapping[str, Any] | None = None) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return pd.DataFrame()
    sheets = _match_sheets_by_keywords(path, ["排程表", "生產排程", "訂單排程"])
    if not sheets and _sheet_available(path, "排程表", ["生產排程", "訂單排程"]):
        sheets = [resolve_sheet_name(path, "排程表", ["生產排程", "訂單排程"])]
    frames = []
    for sheet in sheets:
        try:
            frames.append(_load_schedule_sheet(path, sheet, _lookup_year_override(year_overrides, path, sheet, "schedule")))
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_standard_hours_sheet(path: Path, sheet_name: str, year_override: int | None = None) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet_name, header=0, engine="openpyxl")
    df = _strip_columns(df).rename(columns={"HOURS": "標準工時", "DAYS": "標準天數"})
    rename_map = {col: canonical_column_name("standard_hours", col) for col in df.columns}
    df = df.rename(columns=rename_map)
    if df.columns.duplicated().any():
        merged = pd.DataFrame(index=df.index)
        for col in dict.fromkeys(df.columns):
            same = df.loc[:, df.columns == col]
            merged[col] = same.bfill(axis=1).iloc[:, 0] if same.shape[1] > 1 else same.iloc[:, 0]
        df = merged
    if "標準工時" in df.columns:
        df["標準工時"] = pd.to_numeric(df["標準工時"], errors="coerce")
    if "標準天數" in df.columns:
        df["標準天數"] = pd.to_numeric(df["標準天數"], errors="coerce")
    if "格位數" in df.columns:
        df["格位數"] = pd.to_numeric(df["格位數"], errors="coerce")
    for col in schema_for_table("standard_hours"):
        if col not in df.columns:
            df[col] = None
    return _with_import_year("standard_hours", df[normalize_columns("standard_hours", list(df.columns))], sheet_name, path, year_override)


def load_standard_hours(path: Path = CAPACITY_WORKBOOK, year_overrides: Mapping[str, Any] | None = None) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return pd.DataFrame()
    sheets = _match_sheets_by_keywords(path, ["標準工時", "Standard Hours"])
    if not sheets and _sheet_available(path, "標準工時(超)", ["標準工時", "Standard Hours"]):
        sheets = [resolve_sheet_name(path, "標準工時(超)", ["標準工時", "Standard Hours"])]
    frames = []
    for sheet in sheets:
        try:
            frames.append(_load_standard_hours_sheet(path, sheet, _lookup_year_override(year_overrides, path, sheet, "standard_hours")))
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_capacity_summary_sheet(path: Path, sheet_name: str, year_override: int | None = None) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=sheet_name, header=None, engine="openpyxl")
    months = [str(x).strip() for x in raw.iloc[1, 1:13].tolist()]
    row_map = {
        "每月機台數": "每月機台數",
        "月份天數": "工作天數",
        "原始需求工時": "原始需求工時",
        "產能計算排除工時": "產能計算排除工時",
        "排除工時": "產能計算排除工時",
        "排除後需求工時": "排除後需求工時",
        "調整工時": "調整工時",
        "需求總工時": "需求總工時",
        "可用工時": "正常可用工時",
        "稼動率": "正常稼動率",
        "產能負荷率": "正常稼動率",
        "產能負荷": "正常產能負荷",
        "產能餘額": "正常產能負荷",
    }
    data: dict[str, list[Any]] = {}
    for idx in range(len(raw)):
        label = str(raw.iloc[idx, 0]).strip() if pd.notna(raw.iloc[idx, 0]) else ""
        if label in row_map and row_map[label] not in data:
            data[row_map[label]] = raw.iloc[idx, 1:13].tolist()
        if idx > 10 and label == "可用工時" and "含加班可用工時" not in data:
            data["含加班可用工時"] = raw.iloc[idx, 1:13].tolist()
        if idx > 10 and label in {"稼動率", "產能負荷率"} and "含加班稼動率" not in data:
            data["含加班稼動率"] = raw.iloc[idx, 1:13].tolist()
        if idx > 10 and label in {"產能負荷", "產能餘額"} and "含加班產能負荷" not in data:
            data["含加班產能負荷"] = raw.iloc[idx, 1:13].tolist()
    rows = []
    for i, month in enumerate(months):
        item = {"月份": month}
        for key, values in data.items():
            item[key] = values[i] if i < len(values) else None
        rows.append(item)
    return _with_import_year("capacity_summary_excel", pd.DataFrame(rows).replace({np.nan: None}), sheet_name, path, year_override)


def load_capacity_summary_from_excel(path: Path = CAPACITY_WORKBOOK, year_overrides: Mapping[str, Any] | None = None) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return pd.DataFrame()
    sheets = _match_sheets_by_keywords(path, ["彙整表", "產能彙整", "總表"])
    if not sheets and _sheet_available(path, "彙整表", ["產能彙整", "總表"]):
        sheets = [resolve_sheet_name(path, "彙整表", ["產能彙整", "總表"])]
    frames = []
    for sheet in sheets:
        try:
            frames.append(_load_capacity_summary_sheet(path, sheet, _lookup_year_override(year_overrides, path, sheet, "capacity_summary_excel")))
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_work_calendar_sheet(path: Path, sheet_name: str, year_override: int | None = None) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=sheet_name, header=None, engine="openpyxl")
    rows: list[dict[str, Any]] = []
    for r in range(7, min(19, len(raw))):
        month_no = raw.iloc[r, 0]
        if pd.isna(month_no):
            continue
        try:
            month_int = int(float(month_no))
        except Exception:
            continue
        rows.append({
            "月份": f"{month_int}月",
            "月份數字": month_int,
            "月起日": _excel_date_to_text(raw.iloc[r, 1]),
            "月迄日": _excel_date_to_text(raw.iloc[r, 3]),
            "六日天數": raw.iloc[r, 5],
            "週六天數": raw.iloc[r, 6],
            "週日天數": raw.iloc[r, 7],
            "法定假日": raw.iloc[r, 8],
            "扣除六日工作日": raw.iloc[r, 9],
            "正常工作日": raw.iloc[r, 10],
        })
    return _with_import_year("work_calendar", pd.DataFrame(rows).replace({np.nan: None}), sheet_name, path, year_override)


def load_work_calendar(path: Path = CAPACITY_WORKBOOK, year_overrides: Mapping[str, Any] | None = None) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return pd.DataFrame()
    sheets = _match_sheets_by_keywords(path, ["工作天數", "工作日設定", "行事曆"])
    if not sheets and _sheet_available(path, "工作天數", ["工作日設定", "行事曆"]):
        sheets = [resolve_sheet_name(path, "工作天數", ["工作日設定", "行事曆"])]
    frames = []
    for sheet in sheets:
        try:
            frames.append(_load_work_calendar_sheet(path, sheet, _lookup_year_override(year_overrides, path, sheet, "work_calendar")))
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def inspect_excel_import_file(path: Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return pd.DataFrame(columns=["檔案", "工作表", "年份", "年份來源", "更新模組", "資料表", "預估筆數", "狀態"])
    rows: list[dict[str, Any]] = []
    try:
        xl = pd.ExcelFile(path, engine="openpyxl")
        sheet_names = xl.sheet_names
    except Exception as exc:
        return pd.DataFrame([{"檔案": path.name, "工作表": "-", "年份": "-", "年份來源": "-", "更新模組": "-", "資料表": "-", "預估筆數": 0, "狀態": f"無法讀取：{exc}"}])
    table_keywords = {
        "employees": ["員工名單", "超慧員工名單"],
        "dispatch": ["派遣名單", "外包名單"],
        "schedule": ["排程表", "生產排程", "訂單排程"],
        "standard_hours": ["標準工時", "Standard Hours"],
        "work_calendar": ["工作天數", "工作日設定", "行事曆"],
        "capacity_summary_excel": ["彙整表", "產能彙整", "總表"],
    }
    matched = False
    for table_name, keywords in table_keywords.items():
        matched_sheets = _match_sheets_by_keywords(path, keywords)
        if not matched_sheets:
            continue
        matched = True
        for sheet in matched_sheets:
            year = infer_import_year(sheet, path, default=DEFAULT_YEAR)
            df, error = _load_single_table_sheet_for_preview(path, table_name, sheet, year)
            count = _meaningful_row_count(table_name, df, fallback_year=year)
            if count:
                status = "可匯入"
            elif error:
                status = f"解析失敗：{error}"
            else:
                status = "工作表存在但未偵測到可匯入資料列"
            rows.append({
                "檔案": path.name,
                "工作表": sheet,
                "年份": year,
                "年份來源": infer_import_year_source(sheet, path, default=DEFAULT_YEAR),
                "更新模組": IMPORT_TABLE_LABELS.get(table_name, table_name),
                "資料表": table_name,
                "預估筆數": count,
                "狀態": status,
            })
    if not matched:
        rows.append({"檔案": path.name, "工作表": ", ".join(sheet_names[:8]) + ("..." if len(sheet_names) > 8 else ""), "年份": "-", "年份來源": "-", "更新模組": "未偵測到可更新模組", "資料表": "-", "預估筆數": 0, "狀態": "略過；請確認工作表名稱是否包含 員工名單/派遣名單/排程表/標準工時/工作天數/彙整表。"})
    return pd.DataFrame(rows)


def parse_authority_tables_from_excel(path: Path, year_overrides: Mapping[str, Any] | None = None) -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    tables.update(_try_load_org_workbook(path, year_overrides=year_overrides))
    for table_name, loader in {
        "schedule": load_schedule,
        "standard_hours": load_standard_hours,
        "work_calendar": load_work_calendar,
        "capacity_summary_excel": load_capacity_summary_from_excel,
    }.items():
        try:
            df = loader(path, year_overrides=year_overrides)
        except Exception:
            continue
        if not df.empty:
            tables[table_name] = ensure_year_column(table_name, df, DEFAULT_YEAR)
    return tables




# ---------------------------------------------------------------------------
# V2.38 import comparison / duplicate-control layer
# ---------------------------------------------------------------------------
IMPORT_KEY_CANDIDATES: dict[str, list[list[str]]] = {
    "employees": [["年份", "員工編號"], ["年份", "姓名"]],
    "dispatch": [["年份", "員工編號"], ["年份", "姓名", "人力來源"], ["年份", "姓名", "部 門"]],
    "schedule": [["排程ID"], ["年份", "WO", "P/N", "客戶", "月份", "PO"], ["年份", "WO", "P/N", "客戶", "月份", "Type", "Category", "組立地點"], ["年份", "WO", "P/N", "客戶", "月份"], ["年份", "WO", "P/N", "客戶"], ["年份", "客戶", "P/N", "Type", "Category", "月份"]],
    "standard_hours": [["年份", "客戶", "P/N", "Type", "Category"], ["年份", "P/N", "Type", "Category"], ["年份", "P/N"]],
    "work_calendar": [["年份", "月份"]],
    "capacity_summary_excel": [["年份", "月份"]],
}

IMPORT_MODE_LABELS: dict[str, str] = {
    "append_new": "只新增不存在資料（相同/重複保留既有，不覆蓋、不刪除）",
    "schedule_upsert": "05排程同步更新：同Key更新來源欄位＋新增不存在資料（不刪除Excel未列明細）",
    "schedule_replace_years": "05排程完整同步匯入年度：Excel未列出的舊明細會永久刪除",
}


def _is_meaningful_series(series: pd.Series) -> bool:
    if series is None:
        return False
    text = series.fillna("").astype(str).str.strip()
    return bool((text != "").any())


def import_key_columns(table_name: str, df: pd.DataFrame | None = None) -> list[str]:
    """Return the safest available key columns for append-only Excel import matching.

    The import policy is conservative: existing authority records are never deleted
    or overwritten. These keys are only used to decide whether an uploaded row is
    already present and should therefore be skipped. If a table has no known business
    key, fall back to the uploaded row's meaningful columns so duplicate full rows are
    still detected instead of replacing old data.
    """
    columns = set(map(str, [] if df is None else df.columns))
    for candidate in IMPORT_KEY_CANDIDATES.get(table_name, []):
        if all(col in columns for col in candidate):
            # Avoid using empty employee IDs as the key when the workbook does not provide IDs.
            if "員工編號" in candidate and df is not None and not _is_meaningful_series(df.get("員工編號")):
                continue
            if "排程ID" in candidate and df is not None and not _is_meaningful_series(df.get("排程ID")):
                continue
            return list(candidate)
    fallback = [c for c in ["年份", "月份", "姓名", "WO", "P/N"] if c in columns]
    if fallback:
        return fallback
    if df is not None and not df.empty:
        return [str(c) for c in df.columns if str(c) not in {"_選取"}]
    return []


def _clean_import_key_value(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip().replace("\u00a0", " ")
    # Keep Chinese/case but normalize visible spacing to prevent accidental duplicates.
    return re.sub(r"\s+", " ", text)


def _key_series(df: pd.DataFrame, key_cols: list[str]) -> pd.Series:
    if df is None or df.empty or not key_cols:
        return pd.Series([], dtype="object")
    values: list[str] = []
    non_year_cols = [c for c in key_cols if c != "年份"]
    for idx, row in df.iterrows():
        parts = [_clean_import_key_value(row.get(c)) for c in key_cols]
        non_year_parts = [_clean_import_key_value(row.get(c)) for c in non_year_cols]
        # Do not collapse rows whose identifying business key is blank.
        if not any(non_year_parts):
            values.append(f"__blank_row__::{idx}")
        else:
            values.append("||".join(parts))
    return pd.Series(values, index=df.index, dtype="object")


def _deduplicate_incoming(table_name: str, incoming: pd.DataFrame) -> tuple[pd.DataFrame, int, list[str]]:
    if incoming is None or incoming.empty:
        return pd.DataFrame(), 0, []
    incoming = incoming.copy()
    key_cols = import_key_columns(table_name, incoming)
    if not key_cols:
        return incoming.dropna(how="all"), 0, []
    # Rows with no business key should not be imported as unique records.
    non_year_cols = [c for c in key_cols if c != "年份"]
    if non_year_cols:
        blank_mask = pd.Series(False, index=incoming.index)
        for idx, row in incoming.iterrows():
            blank_mask.loc[idx] = not any(_clean_import_key_value(row.get(c)) for c in non_year_cols)
        if bool(blank_mask.any()):
            incoming = incoming.loc[~blank_mask].copy()
    if incoming.empty:
        return incoming, 0, key_cols
    keys = _key_series(incoming, key_cols)
    duplicated = keys.duplicated(keep="last")
    duplicate_count = int(duplicated.sum())
    if duplicate_count:
        incoming = incoming.loc[~duplicated].copy()
    return incoming, duplicate_count, key_cols


def _row_signature(row: pd.Series, columns: list[str]) -> str:
    return "||".join(_clean_import_key_value(row.get(c)) for c in columns)


def _compare_table_import(table_name: str, incoming_raw: pd.DataFrame) -> dict[str, Any]:
    incoming = ensure_year_column(table_name, incoming_raw, DEFAULT_YEAR)
    incoming_dedup, duplicate_count, key_cols = _deduplicate_incoming(table_name, incoming)
    years = sorted({normalize_year(x, DEFAULT_YEAR) for x in incoming["年份"].dropna().tolist()}) if "年份" in incoming.columns else []
    old = ensure_year_column(table_name, load_authority_df(table_name), DEFAULT_YEAR)
    if old.empty:
        old_target = old
    elif years:
        old_target = old[old["年份"].map(lambda x: normalize_year(x, DEFAULT_YEAR)).isin(years)].copy()
    else:
        old_target = old.copy()
    result: dict[str, Any] = {
        "key_cols": key_cols,
        "years": years,
        "incoming_rows": int(len(incoming)),
        "incoming_unique_rows": int(len(incoming_dedup)),
        "incoming_duplicate_rows": duplicate_count,
        "existing_year_rows": int(len(old_target)),
        "matched_rows": 0,
        "new_rows": int(len(incoming_dedup)),
        "changed_rows": 0,
        "unchanged_rows": 0,
        "skip_rows": 0,
    }
    if incoming_dedup.empty or not key_cols:
        return result
    incoming_keys = _key_series(incoming_dedup, key_cols)
    old_keys = _key_series(old_target, key_cols) if not old_target.empty else pd.Series([], dtype="object")
    old_key_set = set(old_keys.tolist())
    matched_mask = incoming_keys.isin(old_key_set)
    result["matched_rows"] = int(matched_mask.sum())
    result["new_rows"] = int((~matched_mask).sum())
    result["skip_rows"] = int(matched_mask.sum())
    if not old_target.empty and result["matched_rows"]:
        common_cols = [c for c in incoming_dedup.columns if c in old_target.columns and c not in {"_選取"}]
        old_sig_by_key: dict[str, str] = {}
        old_keys_series = _key_series(old_target, key_cols)
        for old_idx, key in old_keys_series.items():
            if key not in old_sig_by_key:
                old_sig_by_key[key] = _row_signature(old_target.loc[old_idx], common_cols)
        changed = 0
        unchanged = 0
        for inc_idx, key in incoming_keys.items():
            if key not in old_sig_by_key:
                continue
            inc_sig = _row_signature(incoming_dedup.loc[inc_idx], common_cols)
            if inc_sig == old_sig_by_key.get(key):
                unchanged += 1
            else:
                changed += 1
        result["changed_rows"] = changed
        result["unchanged_rows"] = unchanged
    return result


def inspect_excel_import_comparison(paths: Iterable[Path], year_overrides: Mapping[str, Any] | None = None) -> pd.DataFrame:
    """Preview how an upload will affect existing authority data before writing."""
    collected: dict[str, list[pd.DataFrame]] = {}
    for path in paths:
        try:
            tables = parse_authority_tables_from_excel(Path(path), year_overrides=year_overrides)
        except Exception:
            continue
        for table_name, df in tables.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                collected.setdefault(table_name, []).append(df)
    rows: list[dict[str, Any]] = []
    for table_name, frames in collected.items():
        incoming = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        info = _compare_table_import(table_name, incoming)
        rows.append({
            "更新模組": IMPORT_TABLE_LABELS.get(table_name, table_name),
            "資料表": table_name,
            "年份": ", ".join(map(str, info["years"])) if info["years"] else "-",
            "比對 Key": " + ".join(info["key_cols"]) if info["key_cols"] else "無",
            "目前同年度筆數": info["existing_year_rows"],
            "匯入筆數": info["incoming_rows"],
            "Excel內重複筆數": info["incoming_duplicate_rows"],
            "可匯入唯一筆數": info["incoming_unique_rows"],
            "新增筆數": info["new_rows"],
            "相同Key筆數": info["matched_rows"],
            "內容異動筆數": info["changed_rows"],
            "內容相同筆數": info["unchanged_rows"],
        })
    if not rows:
        return pd.DataFrame(columns=["更新模組", "資料表", "年份", "比對 Key", "目前同年度筆數", "匯入筆數", "Excel內重複筆數", "可匯入唯一筆數", "新增筆數", "相同Key筆數", "內容異動筆數", "內容相同筆數"])
    return pd.DataFrame(rows)




def _clean_standard_key_value(value: Any) -> str:
    """Normalize key cells so blank/NaN values can match reliably."""
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if text.lower() in {"nan", "none", "nat", "<na>"}:
        return ""
    return text


def _standard_hours_relaxed_keys(row: pd.Series) -> list[str]:
    """Build safe fallback keys for 06.標準工時 updates.

    Older authority data may lack 客戶 or Category, while new import files often
    include them.  For 格位數 / 是否啟用 we are allowed to update matched rows,
    so we match first by the strict key and then by 年份 + P/N + Type/Category.
    """
    year = _clean_standard_key_value(row.get("年份"))
    pn = _clean_standard_key_value(row.get("P/N"))
    type_name = _clean_standard_key_value(row.get("Type"))
    category = _clean_standard_key_value(row.get("Category"))
    customer = _clean_standard_key_value(row.get("客戶"))
    if not year or not pn:
        return []
    candidates: list[tuple[str, ...]] = []
    if customer and type_name and category:
        candidates.append(("full", year, customer, pn, type_name, category))
    if type_name and category:
        candidates.append(("type_category", year, pn, type_name, category))
    if type_name:
        candidates.append(("type", year, pn, type_name))
    if category:
        candidates.append(("category", year, pn, category))
    # Last resort is 年份 + P/N.  It is still scoped by year and is needed for
    # legacy rows whose Type/Category were blank or stored in an older temporary column.
    candidates.append(("pn", year, pn))
    keys: list[str] = []
    seen: set[str] = set()
    for parts in candidates:
        key = "||".join(parts)
        if key not in seen:
            seen.add(key)
            keys.append(key)
    return keys


def _standard_hours_relaxed_key_map(df: pd.DataFrame) -> dict[str, list[int]]:
    result: dict[str, list[int]] = {}
    if df is None or df.empty:
        return result
    for idx, row in df.iterrows():
        for key in _standard_hours_relaxed_keys(row):
            result.setdefault(key, []).append(idx)
    return result


def _normalize_standard_enabled(value: Any) -> str | None:
    """Normalize common Excel enable/disable values into 是 / 否."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, bool):
        return "是" if value else "否"
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat", "<na>"}:
        return None
    normalized = text.replace(" ", "").replace("　", "").lower()
    yes_values = {"是", "y", "yes", "true", "1", "啟用", "有效", "active", "enabled", "enable", "開", "on"}
    no_values = {"否", "n", "no", "false", "0", "停用", "停", "無效", "inactive", "disabled", "disable", "關", "off"}
    if normalized in yes_values:
        return "是"
    if normalized in no_values:
        return "否"
    return text


def _apply_standard_hours_field_updates(old: pd.DataFrame, incoming: pd.DataFrame, key_cols: list[str], incoming_keys: pd.Series, old_keys: pd.Series) -> tuple[pd.DataFrame, dict[str, int], set[int]]:
    """Update allowed 06.標準工時 fields for matched rows only.

    Global import remains append-only and never overwrites 標準工時 / 標準天數.
    The allowed same-key updates are:
    - 格位數：for 12 場地週轉格位需求
    - 是否啟用：for enabling/disabling standard-hour rules from Excel

    To support legacy authority rows, matching is strict key plus safe fallback
    keys such as 年份 + P/N + Type.
    """
    if old is None or old.empty or incoming is None or incoming.empty:
        return old.copy(), {"updated_rows": 0, "slot_updated_rows": 0, "enabled_updated_rows": 0}, set()
    final = old.copy()
    for col in ["格位數", "是否啟用"]:
        if col not in final.columns:
            final[col] = pd.NA

    old_key_to_indexes: dict[str, list[int]] = {}
    for idx, key in old_keys.items():
        old_key_to_indexes.setdefault(str(key), []).append(idx)
    relaxed_map = _standard_hours_relaxed_key_map(final)

    touched_rows: set[int] = set()
    slot_updated = 0
    enabled_updated = 0
    matched_incoming: set[int] = set()

    for idx, key in incoming_keys.items():
        if idx not in incoming.index:
            continue
        row = incoming.loc[idx]
        targets: list[int] = []
        if str(key) in old_key_to_indexes:
            targets.extend(old_key_to_indexes[str(key)])
        for relaxed_key in _standard_hours_relaxed_keys(row):
            targets.extend(relaxed_map.get(relaxed_key, []))
        # Preserve order and de-duplicate target row indexes.
        target_indexes: list[int] = []
        seen_targets: set[int] = set()
        for target in targets:
            if target in seen_targets:
                continue
            seen_targets.add(target)
            target_indexes.append(target)
        if not target_indexes:
            continue
        matched_incoming.add(idx)

        slot_value = None
        if "格位數" in incoming.columns:
            raw_slot = row.get("格位數")
            slot_numeric = pd.to_numeric(pd.Series([raw_slot]), errors="coerce").iloc[0]
            if not pd.isna(slot_numeric) and float(slot_numeric) >= 0:
                slot_value = int(round(float(slot_numeric)))

        enabled_value = None
        if "是否啟用" in incoming.columns:
            enabled_value = _normalize_standard_enabled(row.get("是否啟用"))

        for old_idx in target_indexes:
            if slot_value is not None:
                before_slot = pd.to_numeric(pd.Series([final.at[old_idx, "格位數"]]), errors="coerce").iloc[0]
                before_slot_clean = None if pd.isna(before_slot) else int(round(float(before_slot)))
                if before_slot_clean != slot_value:
                    final.at[old_idx, "格位數"] = slot_value
                    slot_updated += 1
                    touched_rows.add(old_idx)
            if enabled_value is not None:
                before_enabled = _normalize_standard_enabled(final.at[old_idx, "是否啟用"])
                if before_enabled != enabled_value:
                    final.at[old_idx, "是否啟用"] = enabled_value
                    enabled_updated += 1
                    touched_rows.add(old_idx)

    return final, {
        "updated_rows": len(touched_rows),
        "slot_updated_rows": slot_updated,
        "enabled_updated_rows": enabled_updated,
    }, matched_incoming



def _normalize_schedule_factory(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat", "<na>"}:
        return None
    return text


def _apply_schedule_factory_updates(
    old: pd.DataFrame,
    incoming: pd.DataFrame,
    key_cols: list[str],
    incoming_keys: pd.Series,
    old_keys: pd.Series,
) -> tuple[pd.DataFrame, dict[str, int], set[int]]:
    """Allow Page 10 to update only 05.排程表的生產廠區.

    Global Excel import remains append-only for schedule content.  The only
    same-key field allowed to change is 生產廠區, so users can allocate existing
    WO/Forecast rows to 一廠、二廠 without duplicating the schedule or overwriting
    dates, quantities, hours, status or notes.  Blank factory cells mean
    "do not change".
    """
    if old is None or old.empty or incoming is None or incoming.empty:
        return old.copy(), {"updated_rows": 0, "factory_updated_rows": 0}, set()
    final = old.copy()
    if "生產廠區" not in final.columns:
        final["生產廠區"] = pd.NA
    if "生產廠區" not in incoming.columns:
        return final, {"updated_rows": 0, "factory_updated_rows": 0}, set()

    old_key_to_indexes: dict[str, list[int]] = {}
    for old_idx, key in old_keys.items():
        old_key_to_indexes.setdefault(str(key), []).append(old_idx)

    # Safe fallback for legacy rows: 年份 + WO, only when the WO is unique.
    fallback_map: dict[str, list[int]] = {}
    if "年份" in final.columns and "WO" in final.columns:
        for old_idx, row in final.iterrows():
            year = _clean_standard_key_value(row.get("年份"))
            wo = _clean_standard_key_value(row.get("WO"))
            if year and wo:
                fallback_map.setdefault(f"{year}||{wo}", []).append(old_idx)

    touched: set[int] = set()
    matched_incoming: set[int] = set()
    updates = 0
    for idx, key in incoming_keys.items():
        if idx not in incoming.index:
            continue
        row = incoming.loc[idx]
        factory = _normalize_schedule_factory(row.get("生產廠區"))
        if factory is None:
            continue
        targets = list(old_key_to_indexes.get(str(key), []))
        if not targets:
            year = _clean_standard_key_value(row.get("年份"))
            wo = _clean_standard_key_value(row.get("WO"))
            fallback_targets = fallback_map.get(f"{year}||{wo}", []) if year and wo else []
            if len(fallback_targets) == 1:
                targets = list(fallback_targets)
        if not targets:
            continue
        matched_incoming.add(idx)
        for old_idx in targets:
            before = _normalize_schedule_factory(final.at[old_idx, "生產廠區"])
            if before != factory:
                final.at[old_idx, "生產廠區"] = factory
                updates += 1
                touched.add(old_idx)

    return final, {"updated_rows": len(touched), "factory_updated_rows": updates}, matched_incoming

SCHEDULE_IMPORT_SOURCE_FIELDS = [
    "年份", "WO", "客戶", "P/N", "Type", "Category", "組立地點", "生產廠區",
    "機台入庫日", "MOVE IN", "月份", "台數_raw", "台數", "PO", "工期",
    "標準工時", "狀態", "備註",
]


def _incoming_has_value(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    return str(value).strip().lower() not in {"", "nan", "none", "nat", "<na>", "null"}


def _apply_schedule_source_updates(
    old: pd.DataFrame,
    incoming: pd.DataFrame,
    incoming_keys: pd.Series,
    old_keys: pd.Series,
) -> tuple[pd.DataFrame, dict[str, int], set[int]]:
    """Update the Excel-maintained fields of a matched 05 schedule row.

    Calculated fields such as 機台計數、需求工時 and exclusion flags are never
    copied from Excel here; Page 10 recalculates them from the imported source fields
    immediately after import. Existing 排程ID is always preserved.
    """
    final = ensure_schedule_record_ids(old)
    old_key_to_indexes: dict[str, list[int]] = {}
    for old_idx, key in old_keys.items():
        old_key_to_indexes.setdefault(str(key), []).append(old_idx)

    fallback_map: dict[str, list[int]] = {}
    if "年份" in final.columns and "WO" in final.columns:
        for old_idx, row in final.iterrows():
            year = _clean_standard_key_value(row.get("年份"))
            wo = _clean_standard_key_value(row.get("WO"))
            if year and wo:
                fallback_map.setdefault(f"{year}||{wo}", []).append(old_idx)

    touched_rows: set[int] = set()
    matched_incoming: set[int] = set()
    updated_cells = 0
    ambiguous_rows = 0
    unchanged_rows = 0

    for inc_idx, key in incoming_keys.items():
        if inc_idx not in incoming.index:
            continue
        row = incoming.loc[inc_idx]
        targets = list(old_key_to_indexes.get(str(key), []))
        if not targets:
            year = _clean_standard_key_value(row.get("年份"))
            wo = _clean_standard_key_value(row.get("WO"))
            fallback_targets = fallback_map.get(f"{year}||{wo}", []) if year and wo else []
            if len(fallback_targets) == 1:
                targets = list(fallback_targets)
        if not targets:
            continue
        matched_incoming.add(int(inc_idx))
        if len(targets) != 1:
            ambiguous_rows += 1
            continue
        old_idx = targets[0]
        row_changed = False
        for col in SCHEDULE_IMPORT_SOURCE_FIELDS:
            if col not in incoming.columns or not _incoming_has_value(row.get(col)):
                continue
            if col not in final.columns:
                final[col] = pd.NA
            incoming_value = row.get(col)
            before = _clean_import_key_value(final.at[old_idx, col])
            after = _clean_import_key_value(incoming_value)
            if before != after:
                final.at[old_idx, col] = incoming_value
                updated_cells += 1
                row_changed = True
        if row_changed:
            touched_rows.add(old_idx)
        else:
            unchanged_rows += 1

    return final, {
        "updated_rows": len(touched_rows),
        "updated_cells": updated_cells,
        "ambiguous_rows": ambiguous_rows,
        "unchanged_matched_rows": unchanged_rows,
    }, matched_incoming



def _replace_schedule_import_years(
    old: pd.DataFrame,
    incoming: pd.DataFrame,
    key_cols: list[str],
    *,
    user: str,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Treat uploaded schedule years as a complete authority snapshot.

    Rows outside imported years are preserved. Within imported years, rows absent
    from the Excel upload are removed and recorded as deletion tombstones. Stable
    排程ID values are preserved for uniquely matched business keys so later edits
    remain attached to the same record.
    """
    old = ensure_schedule_record_ids(old)
    incoming = ensure_schedule_record_ids(incoming)
    years = sorted({int(v) for v in pd.to_numeric(incoming.get("年份"), errors="coerce").dropna().tolist()})
    if not years:
        return old.copy(), {"removed_rows": 0, "replaced_year_rows": 0, "preserved_ids": 0}

    old_year = old[pd.to_numeric(old.get("年份"), errors="coerce").isin(years)].copy()
    old_other = old[~pd.to_numeric(old.get("年份"), errors="coerce").isin(years)].copy()

    effective_keys = [col for col in key_cols if col != SCHEDULE_ID_COL and col in incoming.columns and col in old_year.columns]
    if not effective_keys:
        effective_keys = import_key_columns("schedule", incoming.drop(columns=[SCHEDULE_ID_COL], errors="ignore"))
        effective_keys = [col for col in effective_keys if col != SCHEDULE_ID_COL and col in incoming.columns and col in old_year.columns]

    preserved_ids = 0
    incoming_final = incoming.copy()
    if effective_keys and not old_year.empty:
        old_keys = _key_series(old_year, effective_keys)
        incoming_keys = _key_series(incoming_final, effective_keys)
        old_id_map: dict[str, list[str]] = {}
        for idx, key in old_keys.items():
            record_id = str(old_year.at[idx, SCHEDULE_ID_COL] or "").strip()
            if record_id:
                old_id_map.setdefault(str(key), []).append(record_id)
        for idx, key in incoming_keys.items():
            candidates = list(dict.fromkeys(old_id_map.get(str(key), [])))
            if len(candidates) == 1:
                incoming_final.at[idx, SCHEDULE_ID_COL] = candidates[0]
                preserved_ids += 1
        absent_mask = ~old_keys.astype(str).isin(set(incoming_keys.astype(str).tolist()))
        removed_rows = old_year.loc[absent_mask].copy()
    else:
        removed_rows = old_year.copy()

    # Rows intentionally absent from the complete Excel snapshot must not return
    # when an older file is imported later.
    if not removed_rows.empty:
        record_deleted_schedule_rows(removed_rows, user=user)

    incoming_final = ensure_schedule_record_ids(incoming_final)
    final = pd.concat([old_other, incoming_final], ignore_index=True, sort=False)
    final = ensure_schedule_record_ids(final)
    return final, {
        "removed_rows": int(len(removed_rows)),
        "replaced_year_rows": int(len(old_year)),
        "preserved_ids": int(preserved_ids),
    }


def _merge_import_with_existing(
    table_name: str,
    incoming_raw: pd.DataFrame,
    import_mode: str = "append_new",
    *,
    allow_restore_deleted_schedule: bool = False,
    user: str = "streamlit",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Merge an Excel upload with authority data without silently losing records.

    Non-schedule tables keep the conservative append-only rule. 05 schedule supports
    ``schedule_upsert`` (update/add without deleting absent rows) and
    ``schedule_replace_years`` (uploaded years become the complete authority snapshot).
    Stable 排程ID values are preserved where possible. Exact rows previously deleted
    in 05 remain blocked from re-import unless restore-deleted mode is explicit.
    """
    incoming = ensure_year_column(table_name, incoming_raw, DEFAULT_YEAR)
    tombstone_skipped = 0
    if table_name == "schedule":
        incoming, tombstone_skipped = filter_deleted_schedule_import_rows(
            incoming,
            allow_restore=allow_restore_deleted_schedule,
        )
    incoming, duplicate_count, key_cols = _deduplicate_incoming(table_name, incoming)
    years = sorted({normalize_year(x, DEFAULT_YEAR) for x in incoming["年份"].dropna().tolist()}) if "年份" in incoming.columns else []
    old = ensure_year_column(table_name, load_authority_df(table_name), DEFAULT_YEAR)
    if table_name == "schedule":
        old = ensure_schedule_record_ids(old)
    stats = _compare_table_import(table_name, incoming) if not incoming.empty else {
        "years": years,
        "key_cols": key_cols,
        "incoming_rows": 0,
        "incoming_unique_rows": 0,
        "incoming_duplicate_rows": duplicate_count,
        "existing_year_rows": 0,
        "matched_rows": 0,
        "new_rows": 0,
        "changed_rows": 0,
        "unchanged_rows": 0,
        "skip_rows": 0,
    }
    stats["forced_mode"] = import_mode if table_name == "schedule" else "append_new"
    stats["duplicate_count"] = duplicate_count
    stats["deleted_tombstone_skipped_rows"] = int(tombstone_skipped)

    if incoming.empty:
        stats.update({"imported_rows": 0, "updated_rows": 0, "skipped_rows": int(tombstone_skipped)})
        return old.copy(), stats
    if old.empty:
        final = ensure_schedule_record_ids(incoming) if table_name == "schedule" else incoming.copy()
        if table_name == "schedule" and allow_restore_deleted_schedule:
            clear_deleted_schedule_tombstones(final)
        stats.update({"imported_rows": len(incoming), "new_rows": len(incoming), "updated_rows": 0, "skipped_rows": int(tombstone_skipped)})
        return final, stats
    if not key_cols:
        key_cols = import_key_columns(table_name, incoming)
        stats["key_cols"] = key_cols

    if table_name == "schedule" and import_mode == "schedule_replace_years":
        final, replace_stats = _replace_schedule_import_years(old, incoming, key_cols, user=user)
        if allow_restore_deleted_schedule:
            clear_deleted_schedule_tombstones(incoming)
        stats.update({
            "imported_rows": int(len(incoming)),
            "new_rows": int(max(len(incoming) - int(replace_stats.get("preserved_ids", 0)), 0)),
            "updated_rows": int(replace_stats.get("preserved_ids", 0)),
            "skipped_rows": int(tombstone_skipped),
            "removed_rows": int(replace_stats.get("removed_rows", 0)),
            "replaced_year_rows": int(replace_stats.get("replaced_year_rows", 0)),
            "preserved_ids": int(replace_stats.get("preserved_ids", 0)),
        })
        return final, stats

    incoming_keys = _key_series(incoming, key_cols)
    old_keys = _key_series(old, key_cols) if not old.empty else pd.Series([], dtype="object")
    old_key_set = set(old_keys.tolist())

    old_updated = old.copy()
    standard_update_stats = {"updated_rows": 0, "slot_updated_rows": 0, "enabled_updated_rows": 0}
    standard_matched_incoming: set[int] = set()
    schedule_update_stats = {"updated_rows": 0, "factory_updated_rows": 0, "updated_cells": 0, "ambiguous_rows": 0}
    schedule_matched_incoming: set[int] = set()
    if table_name == "standard_hours":
        old_updated, standard_update_stats, standard_matched_incoming = _apply_standard_hours_field_updates(old_updated, incoming, key_cols, incoming_keys, old_keys)
    elif table_name == "schedule":
        if import_mode == "schedule_upsert":
            old_updated, schedule_update_stats, schedule_matched_incoming = _apply_schedule_source_updates(old_updated, incoming, incoming_keys, old_keys)
        else:
            old_updated, schedule_update_stats, schedule_matched_incoming = _apply_schedule_factory_updates(old_updated, incoming, key_cols, incoming_keys, old_keys)

    new_mask = ~incoming_keys.isin(old_key_set)
    if table_name == "standard_hours" and standard_matched_incoming:
        new_mask = new_mask & ~incoming.index.to_series().isin(standard_matched_incoming)
    if table_name == "schedule" and schedule_matched_incoming:
        new_mask = new_mask & ~incoming.index.to_series().isin(schedule_matched_incoming)
    incoming_new = incoming.loc[new_mask].copy()
    if table_name == "schedule" and not incoming_new.empty:
        incoming_new = ensure_schedule_record_ids(incoming_new)
    skipped = int((~new_mask).sum()) + int(tombstone_skipped)
    final = pd.concat([old_updated, incoming_new], ignore_index=True, sort=False)
    if table_name == "schedule":
        final = ensure_schedule_record_ids(final)
        if allow_restore_deleted_schedule:
            clear_deleted_schedule_tombstones(incoming)
    if table_name == "standard_hours":
        updated_rows = int(standard_update_stats.get("updated_rows", 0))
    elif table_name == "schedule":
        updated_rows = int(schedule_update_stats.get("updated_rows", 0))
    else:
        updated_rows = 0
    stats.update({
        "imported_rows": len(incoming_new),
        "new_rows": len(incoming_new),
        "updated_rows": updated_rows,
        "skipped_rows": max(0, skipped - updated_rows),
    })
    if table_name == "standard_hours":
        stats.update({k: int(v) for k, v in standard_update_stats.items() if k != "updated_rows"})
    if table_name == "schedule":
        stats.update({k: int(v) for k, v in schedule_update_stats.items() if k != "updated_rows"})
    return final, stats

def import_authority_from_excel_files(
    paths: Iterable[Path],
    user: str = "streamlit",
    reset_capacity_adjustments: bool = False,
    year_overrides: Mapping[str, Any] | None = None,
    import_mode: str = "append_new",
    allow_restore_deleted_schedule: bool = False,
) -> pd.DataFrame:
    """Import recognized Excel sheets without silently deleting authority data.

    General tables remain append-only. The 05 schedule table may use either
    ``schedule_upsert`` (update/add, preserve absent rows) or
    ``schedule_replace_years`` (uploaded years become the complete authority source).
    Calculated fields are rebuilt after import. Deleted schedule tombstones remain
    protected unless restore mode is explicitly enabled.
    """
    rows: list[dict[str, Any]] = []
    collected: dict[str, list[pd.DataFrame]] = {}
    file_names: dict[str, list[str]] = {}
    for path in paths:
        try:
            tables = parse_authority_tables_from_excel(path, year_overrides=year_overrides)
        except Exception as exc:
            rows.append({"檔案": path.name, "更新模組": "-", "資料表": "-", "年份": "-", "筆數": 0, "新增": 0, "更新": 0, "跳過": 0, "Excel內重複": 0, "結果": f"讀取失敗：{str(exc)[:160]}"})
            continue
        if not tables:
            rows.append({"檔案": path.name, "更新模組": "-", "資料表": "-", "年份": "-", "筆數": 0, "新增": 0, "更新": 0, "跳過": 0, "Excel內重複": 0, "結果": "沒有可匯入的工作表，未寫入任何資料。"})
            continue
        for table_name, df in tables.items():
            if df.empty:
                continue
            df = ensure_year_column(table_name, df, DEFAULT_YEAR)
            collected.setdefault(table_name, []).append(df)
            file_names.setdefault(table_name, []).append(path.name)
    imported_tables: set[str] = set()
    imported_years: dict[str, set[int]] = {}
    for table_name, frames in collected.items():
        incoming = pd.concat(frames, ignore_index=True)
        incoming = ensure_year_column(table_name, incoming, DEFAULT_YEAR)
        table_mode = import_mode if table_name == "schedule" else "append_new"
        final, stats = _merge_import_with_existing(
            table_name,
            incoming,
            import_mode=table_mode,
            allow_restore_deleted_schedule=allow_restore_deleted_schedule,
            user=user,
        )
        final = ensure_year_column(table_name, final, DEFAULT_YEAR)
        years = sorted({normalize_year(x, DEFAULT_YEAR) for x in incoming["年份"].dropna().tolist()}) if "年份" in incoming.columns else []
        save_authority_records(table_name, dataframe_to_records(final), user=user, columns=list(final.columns))
        imported_tables.add(table_name)
        imported_years[table_name] = set(years)
        rows.append({
            "檔案": ", ".join(sorted(set(file_names.get(table_name, [])))),
            "更新模組": IMPORT_TABLE_LABELS.get(table_name, table_name),
            "資料表": table_name,
            "年份": ", ".join(map(str, years)) if years else "-",
            "匯入模式": IMPORT_MODE_LABELS.get(table_mode, IMPORT_MODE_LABELS["append_new"]),
            "筆數": int(stats.get("imported_rows", len(incoming))),
            "新增": int(stats.get("new_rows", 0)),
            "更新": int(stats.get("updated_rows", stats.get("changed_rows", 0))),
            "跳過": int(stats.get("skipped_rows", 0)),
            "Excel內重複": int(stats.get("duplicate_count", stats.get("incoming_duplicate_rows", 0))),
            "刪除保護跳過": int(stats.get("deleted_tombstone_skipped_rows", 0)),
            "移除舊明細": int(stats.get("removed_rows", 0)),
            "結果": (
                "05排程已依 Excel 完整同步匯入年度；Excel 未列出的舊明細已刪除，並將重算 04/09/12。"
                if table_name == "schedule" and table_mode == "schedule_replace_years"
                else "05排程已同步更新同Key來源欄位並新增新資料；Excel 未列出的舊明細仍保留。"
                if table_name == "schedule" and table_mode == "schedule_upsert"
                else "已依只新增原則匯入；06標準工時相同或同 P/N / Type 僅允許更新格位數與是否啟用。"
            ),
        })
    if reset_capacity_adjustments and imported_tables.intersection({"schedule", "capacity_summary_excel"}):
        years_to_keep: set[int] = set()
        for name in ["schedule", "capacity_summary_excel"]:
            years_to_keep.update(imported_years.get(name, set()))
        rows.append({"檔案": "系統", "更新模組": "04. 產能調整工時", "資料表": "capacity_adjustments", "年份": ", ".join(map(str, sorted(years_to_keep))) if years_to_keep else "-", "匯入模式": IMPORT_MODE_LABELS["append_new"], "筆數": 0, "新增": 0, "更新": 0, "跳過": 0, "Excel內重複": 0, "結果": "已保留既有調整工時；依全域只新增原則，匯入不清空舊資料。"})
    return pd.DataFrame(rows)

def bootstrap_authority_from_excel() -> dict[str, int]:
    """Import data/source workbooks without deleting or overwriting authority data."""
    org = load_org_workbook()
    tables = {
        "employees": org.get("employees", pd.DataFrame()),
        "dispatch": org.get("dispatch", pd.DataFrame()),
        "schedule": load_schedule(),
        "standard_hours": load_standard_hours(),
        "capacity_summary_excel": load_capacity_summary_from_excel(),
        "work_calendar": load_work_calendar(),
    }
    result: dict[str, int] = {}
    for name, df in tables.items():
        df = ensure_year_column(name, df, DEFAULT_YEAR)
        final, stats = _merge_import_with_existing(name, df, import_mode="append_new")
        final = ensure_year_column(name, final, DEFAULT_YEAR)
        save_authority_records(name, dataframe_to_records(final), user="bootstrap_append_only", columns=list(final.columns))
        result[name] = int(stats.get("imported_rows", 0))
    return result

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from services.capacity_engine import (
    ASSEMBLY_EXCLUSION_PARAM_KEY,
    ASSEMBLY_LOCATION_HOURS_PARAM_KEY,
    CATEGORY_EXCLUSION_PARAM_KEY,
    calculate_capacity_by_years,
    prepare_schedule,
    recalculate_schedule_demand,
    upsert_capacity_results,
    validate_schedule,
)
from services.data_loader import clear_data_cache, load_table
from services.factory_turnover_service import load_slots
from services.page_utils import SELECT_COL, render_configurable_view, render_module_report_download
from services.persistent_store import load_authority_df, load_parameters, read_audit_log, save_authority_df
from services.settings_service import load_module_settings, save_module_settings
from services.schedule_record_service import (
    SCHEDULE_ID_COL,
    clear_deleted_schedule_tombstones,
    ensure_schedule_record_ids,
    load_deleted_schedule_records,
    record_deleted_schedule_rows,
    schedule_month_summary,
)
from services.ui_theme import apply_tech_theme, render_hero, render_human_help
from services.year_service import DEFAULT_YEAR, available_years_from_frames, ensure_year_column, normalize_year

st.set_page_config(page_title="05. 排程表", page_icon="🗓️", layout="wide")
apply_tech_theme()
render_hero("05. 排程表", "訂單、WO、客戶、P/N、月份、台數與標準工時；查詢結果可直接穩定編輯、刪除、儲存並串聯產能。")
render_human_help([
    "排程表已合併『穩定編輯模式』與『日期區間明細查詢』：先選日期區間，再直接在查詢結果內編輯或刪除。",
    "『原始需求工時』= 台數 × 標準工時；『需求工時』= 原始需求工時扣除 06 設定的產能計算排除組立地點後的工時。",
    "每月機台數會依官方 Excel 排程表 J 欄『台數』的月份標記計算，例如 J 欄為 6月 的筆數就是 6月機台數。",
    "若標準工時空白，系統會優先依 06. 標準工時主檔補齊，再重新計算需求工時。",
    "若 06 設定排除組立地點，該筆『產能計算排除工時』= 原始需求工時，『排除後需求工時／需求工時』= 0，但機台計數仍保留；Category 排除只讓機台計數歸 0，不扣工時。",
    "生產廠區是 12 場地週轉的正式分廠依據；重要 WO 請直接指定一廠／二廠，空白時才由 12 分廠規則判斷。",
    "按『儲存查詢結果並重新計算』後會同步更新 04. 產能負荷表的系統計算結果，09. 情境模擬也會讀取最新排程。",
])

ROW_ID_COL = SCHEDULE_ID_COL
SCHEDULE_STATE_KEY = "schedule_query_edit_working_df"
SCHEDULE_SIGNATURE_KEY = "schedule_query_edit_source_signature"
SCHEDULE_WINDOW_KEY = "schedule_query_edit_window"
SCHEDULE_LAST_QUERY_KEY = "schedule_query_edit_last_detail"
SCHEDULE_QUERY_META_KEY = "schedule_query_authority_compare_meta_v1"
ASSEMBLY_ANALYSIS_STATE_PREFIX = "schedule_assembly_location_applied_filter_v39"

if st.session_state.get("schedule_authority_save_message"):
    st.success(st.session_state.pop("schedule_authority_save_message"))

MISSING_TEXT_LITERALS = {"", "none", "nan", "nat", "<na>", "null"}
SCHEDULE_TEXT_DEFAULTS = {
    "標準工時來源": "未設定",
    "產能計算排除": "否",
    "工時計算排除": "否",
    "台數計算排除": "否",
    "產能計算排除原因": "",
    "狀態": "",
    "備註": "",
    "PO": "",
}
SCHEDULE_NUMERIC_DEFAULTS = {
    "台數": 0.0,
    "機台計數": 0.0,
    "標準工時": 0.0,
    "原始需求工時": 0.0,
    "產能計算排除工時": 0.0,
    "排除後需求工時": 0.0,
    "需求工時": 0.0,
    "工期": 0.0,
    "排除前機台計數": 0.0,
    "排除前需求工時": 0.0,
    "組立地點調整工時/台": 0.0,
    "組立地點調整需求工時": 0.0,
}


def _is_missing_text_value(value: object) -> bool:
    """Return True for real blanks and legacy stringified missing values."""
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    return str(value).strip().lower() in MISSING_TEXT_LITERALS


def _clean_schedule_editor_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Prevent Python/JSON None from being shown as literal 'None' in 05 editor.

    The authority JSON may contain null values in optional columns such as PO,
    MOVE IN, 備註, 狀態 or old calculated columns.  Streamlit's data_editor can
    render those object cells as the word None, which makes users think data is
    broken.  This function is display/authority-safe: text blanks stay blank,
    system flags get their business defaults, and calculated numeric columns use
    0 until the user or 06.標準工時 supplies a real value.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    for col in out.columns:
        if col in SCHEDULE_NUMERIC_DEFAULTS:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(float(SCHEDULE_NUMERIC_DEFAULTS[col]))
            continue
        if pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col]):
            out[col] = out[col].map(lambda value: "" if _is_missing_text_value(value) else value)
    for col, default in SCHEDULE_TEXT_DEFAULTS.items():
        if col in out.columns:
            out[col] = out[col].map(lambda value, d=default: d if _is_missing_text_value(value) else str(value).strip())
    return out


def _schedule_editor_source(
    source_df: pd.DataFrame,
    standard: pd.DataFrame,
    selected_year: int,
    *,
    excluded_assembly_locations: list[str] | None = None,
    excluded_categories: list[str] | None = None,
    assembly_location_hours: dict | None = None,
) -> pd.DataFrame:
    """Build the editor's starting data from calculated 05 results.

    Root cause of the screenshot issue: the editor was using raw authority rows,
    while charts/KPI used prepared rows.  If authority rows contained JSON nulls or
    old calculated fields, the editor showed many None values even though the
    calculation engine could already fill most of them.  The editor now opens from
    a calculated, cleaned copy; it is still only persisted when the user presses
    save/delete.
    """
    if source_df is None or source_df.empty:
        return pd.DataFrame()
    try:
        recalculated = recalculate_schedule_demand(
            source_df,
            standard_hours=standard,
            target_year=None,
            excluded_assembly_locations=excluded_assembly_locations,
            excluded_categories=excluded_categories,
            assembly_location_hours=assembly_location_hours,
        )
    except Exception:
        recalculated = source_df.copy()
    return _clean_schedule_editor_missing_values(recalculated)


def _schedule_missing_literal_counts(df: pd.DataFrame, selected_year: int) -> pd.DataFrame:
    """Count literal None/null-style values by column for diagnosis."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["欄位", "None/空白字串筆數"])
    temp = ensure_year_column("schedule", df.copy(), DEFAULT_YEAR)
    temp["_year_norm"] = temp["年份"].map(lambda value: normalize_year(value, DEFAULT_YEAR))
    temp = temp[temp["_year_norm"].eq(int(selected_year))].drop(columns=["_year_norm"], errors="ignore")
    rows: list[dict[str, object]] = []
    for col in temp.columns:
        values = temp[col]
        if pd.api.types.is_object_dtype(values) or pd.api.types.is_string_dtype(values):
            count = int(values.map(_is_missing_text_value).sum())
        else:
            count = int(values.isna().sum())
        if count > 0:
            rows.append({"欄位": str(col), "None/空白字串筆數": count})
    return pd.DataFrame(rows).sort_values("None/空白字串筆數", ascending=False).reset_index(drop=True) if rows else pd.DataFrame(columns=["欄位", "None/空白字串筆數"])


def _standard_hours_missing_diagnostics(prepared: pd.DataFrame, selected_year: int) -> pd.DataFrame:
    """Explain rows still missing standard hours after 06 lookup."""
    if prepared is None or prepared.empty or "標準工時" not in prepared.columns:
        return pd.DataFrame()
    temp = ensure_year_column("schedule", prepared.copy(), DEFAULT_YEAR)
    temp["_year_norm"] = temp["年份"].map(lambda value: normalize_year(value, DEFAULT_YEAR))
    temp = temp[temp["_year_norm"].eq(int(selected_year))].drop(columns=["_year_norm"], errors="ignore")
    std = pd.to_numeric(temp.get("標準工時", pd.Series(dtype=float)), errors="coerce").fillna(0)
    missing = temp[std.le(0)].copy()
    if missing.empty:
        return pd.DataFrame()

    def reason(row: pd.Series) -> str:
        pn = str(row.get("P/N", "") or "").strip()
        typ = str(row.get("Type", "") or "").strip()
        cat = str(row.get("Category", "") or "").strip()
        wo = str(row.get("WO", "") or "").strip()
        if not any([pn, typ, cat, wo]):
            return "空白排程列或保留列，未提供 P/N、Type、Category。"
        if not pn and not typ:
            return "P/N 與 Type 皆空白，無法與 06 標準工時比對。"
        return "06 標準工時未找到相同 P/N / Type / Category；請在 06 新增標準工時或於 05 手動填入。"

    missing["缺漏原因"] = missing.apply(reason, axis=1)
    cols = [c for c in ["年份", "月份", "WO", "客戶", "P/N", "Type", "Category", "組立地點", "台數", "機台計數", "標準工時來源", "缺漏原因"] if c in missing.columns]
    return missing[cols].reset_index(drop=True)


def _render_schedule_visibility_diagnostics(raw_df: pd.DataFrame, editor_df: pd.DataFrame, prepared: pd.DataFrame, selected_year: int) -> None:
    """Render a compact root-cause diagnostic for 05 display problems."""
    with st.expander("05 顯示與補值診斷｜為什麼畫面會出現 None / 空白", expanded=False):
        st.markdown(
            """
            **檢查結果說明：** 舊版編輯表直接讀 05 權威原始列，JSON 裡的 `null` 會被 Streamlit 顯示成 `None`；
            但圖表與計算區使用的是補值後資料。本版已改成：編輯表開啟時先套用 06 標準工時補值、需求工時計算與 None 清理，
            按下儲存後才正式寫回 05 並同步 04 / 09。
            """
        )
        raw_counts = _schedule_missing_literal_counts(raw_df, selected_year)
        cleaned_counts = _schedule_missing_literal_counts(editor_df, selected_year)
        c1, c2 = st.columns(2)
        with c1:
            st.caption("修正前來源資料 None/空白欄位統計")
            st.dataframe(raw_counts.head(20), use_container_width=True, height=260)
        with c2:
            st.caption("目前編輯表清理後仍空白欄位統計")
            st.dataframe(cleaned_counts.head(20), use_container_width=True, height=260)
        missing_std_detail = _standard_hours_missing_diagnostics(prepared, selected_year)
        if missing_std_detail.empty:
            st.success("目前查詢年度的標準工時已可由 05 或 06 補齊。")
        else:
            st.warning(f"目前 {selected_year} 年仍有 {len(missing_std_detail):,} 筆標準工時無法由 06 自動補齊。")
            render_configurable_view(missing_std_detail.head(200), "schedule_standard_hours_missing_diagnostics", "標準工時缺漏診斷 Top 200", height=340)


def _render_schedule_authority_diagnostics(
    raw_df: pd.DataFrame,
    editor_df: pd.DataFrame,
    selected_year: int,
) -> None:
    """Explain which records are authoritative, deleted, or only filtered from view."""
    with st.expander("05 權威資料、刪除紀錄與 Excel 匯入檢查", expanded=False):
        authority = ensure_schedule_record_ids(raw_df.copy() if isinstance(raw_df, pd.DataFrame) else pd.DataFrame())
        editor = ensure_schedule_record_ids(editor_df.copy() if isinstance(editor_df, pd.DataFrame) else pd.DataFrame())
        authority_year = ensure_year_column("schedule", authority.copy(), DEFAULT_YEAR)
        editor_year = ensure_year_column("schedule", editor.copy(), DEFAULT_YEAR)
        authority_year_count = int(authority_year["年份"].map(lambda value: normalize_year(value, DEFAULT_YEAR)).eq(int(selected_year)).sum()) if not authority_year.empty else 0
        editor_year_count = int(editor_year["年份"].map(lambda value: normalize_year(value, DEFAULT_YEAR)).eq(int(selected_year)).sum()) if not editor_year.empty else 0
        deleted_records = load_deleted_schedule_records()
        duplicate_ids = 0
        blank_ids = 0
        if not authority.empty and ROW_ID_COL in authority.columns:
            id_text = authority[ROW_ID_COL].fillna("").astype(str).str.strip()
            blank_ids = int(id_text.eq("").sum())
            duplicate_ids = int(id_text[id_text.ne("")].duplicated(keep=False).sum())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("05 權威資料", f"{len(authority):,} 筆")
        c2.metric(f"{selected_year} 年權威資料", f"{authority_year_count:,} 筆")
        c3.metric("目前編輯暫存", f"{len(editor):,} 筆")
        c4.metric("刪除保護紀錄", f"{len(deleted_records):,} 筆")

        checks = pd.DataFrame([
            {"檢查項目": "權威資料與目前編輯暫存總筆數", "結果": "一致" if len(authority) == len(editor) else f"不一致：權威 {len(authority)}／暫存 {len(editor)}"},
            {"檢查項目": f"{selected_year} 年權威與編輯暫存筆數", "結果": "一致" if authority_year_count == editor_year_count else f"不一致：權威 {authority_year_count}／暫存 {editor_year_count}"},
            {"檢查項目": "排程ID 空白", "結果": "通過" if blank_ids == 0 else f"{blank_ids} 筆空白"},
            {"檢查項目": "排程ID 重複", "結果": "通過" if duplicate_ids == 0 else f"{duplicate_ids} 筆重複"},
        ])
        st.dataframe(checks, use_container_width=True, hide_index=True)
        st.info(
            "查詢條件只控制畫面顯示，不會建立另一套排程紀錄。正式資料只有 "
            "data/persistent/authority/schedule.json；刪除保護檔只用來阻止相同 Excel 明細被再次匯入，不會參與 04 計算。",
            icon="ℹ️",
        )
        if not deleted_records.empty:
            tombstone_cols = [c for c in ["刪除時間", "刪除人", "年份", "月份", "WO", "P/N", "機台入庫日", ROW_ID_COL] if c in deleted_records.columns]
            st.caption("最近刪除保護紀錄：只有在 10 模組明確勾選『允許還原已刪除排程』時，才會解除阻擋。")
            st.dataframe(deleted_records[tombstone_cols].tail(50), use_container_width=True, hide_index=True, height=260)

        audit = read_audit_log(max_rows=300)
        if not audit.empty:
            action_text = audit.get("action", pd.Series(dtype=object)).fillna("").astype(str)
            detail_text = audit.get("detail", pd.Series(index=audit.index, dtype=object)).map(lambda value: str(value))
            schedule_audit = audit[action_text.str.contains("schedule|authority", case=False, regex=True) | detail_text.str.contains("schedule", case=False, regex=False)].tail(30).copy()
            if not schedule_audit.empty:
                st.caption("最近 05 排程／權威資料異動紀錄")
                st.dataframe(schedule_audit, use_container_width=True, hide_index=True, height=300)



def _render_schedule_month_authority_cleanup(
    raw_df: pd.DataFrame,
    selected_year: int,
    standard: pd.DataFrame,
    params: dict,
    *,
    expanded: bool = False,
) -> None:
    """Audit and safely remove stale authority months from 05.

    This is intentionally explicit. The app cannot infer that July~December are
    obsolete merely because a newer Excel stops in June. Managers may either use
    Page 10 complete-year synchronization or select stale months here.
    """
    summary = schedule_month_summary(raw_df, int(selected_year))
    with st.expander("05 權威月份稽核與舊月份清理", expanded=expanded):
        st.caption(
            "此表直接讀取 data/persistent/authority/schedule.json。只要某月份仍列在這裡，"
            "04 產能負荷、05 組裝地點分析、09 情境模擬與 12 場地週轉就會把它視為正式排程。"
        )
        if summary.empty:
            st.info(f"目前 {selected_year} 年沒有權威排程資料。")
            return
        st.dataframe(summary, use_container_width=True, hide_index=True, height=380)
        active = [str(value) for value in summary["月份"].tolist() if str(value) != "未設定"]
        max_date = ""
        if "最晚日期" in summary.columns:
            valid = summary["最晚日期"].fillna("").astype(str)
            max_date = max([value for value in valid.tolist() if value], default="")
        st.info(
            f"目前 05 權威月份：{'、'.join(active) if active else '無有效月份'}"
            + (f"；最晚排程日期：{max_date}" if max_date else ""),
            icon="📌",
        )
        st.markdown("**永久刪除整月舊排程（高風險操作）**")
        with st.form(f"schedule_month_cleanup_form_{int(selected_year)}", clear_on_submit=False):
            delete_months = st.multiselect(
                "選擇要從 05 權威資料永久刪除的月份",
                active,
                default=[],
                help="例如現行正式排程只到 6 月，可選 7月~12月。刪除前會建立 tombstone，舊 Excel 預設不能把相同明細帶回。",
            )
            confirm_text = st.text_input(
                "確認文字",
                value="",
                placeholder=f"請輸入：刪除{int(selected_year)}年選取月份",
            )
            confirm = st.checkbox("我確認這些月份不是目前有效排程，並要同步重算 04/09/12", value=False)
            submit = st.form_submit_button("永久刪除選取月份並重新計算", type="primary", use_container_width=True)
        if not submit:
            return
        expected = f"刪除{int(selected_year)}年選取月份"
        if not delete_months:
            st.error("尚未選擇要刪除的月份。")
            return
        if not confirm or str(confirm_text).strip() != expected:
            st.error(f"請勾選確認並輸入：{expected}")
            return

        authority = ensure_schedule_record_ids(load_authority_df("schedule"))
        authority = ensure_year_column("schedule", authority, DEFAULT_YEAR)
        year_mask = authority["年份"].map(lambda value: normalize_year(value, DEFAULT_YEAR)).eq(int(selected_year))
        month_mask = authority.get("月份", pd.Series(index=authority.index, dtype=object)).map(_normalize_month_label).isin(delete_months)
        rows_to_delete = authority.loc[year_mask & month_mask].copy()
        if rows_to_delete.empty:
            st.warning("權威資料中找不到所選月份，未進行刪除。")
            return
        record_deleted_schedule_rows(rows_to_delete, user="schedule_month_authority_cleanup")
        kept = authority.loc[~(year_mask & month_mask)].copy().reset_index(drop=True)
        recalculated = recalculate_schedule_demand(
            kept,
            standard_hours=standard,
            target_year=None,
            excluded_assembly_locations=params.get(ASSEMBLY_EXCLUSION_PARAM_KEY, []),
            excluded_categories=params.get(CATEGORY_EXCLUSION_PARAM_KEY, []),
            assembly_location_hours=params.get(ASSEMBLY_LOCATION_HOURS_PARAM_KEY, {}),
        )
        recalculated = ensure_schedule_record_ids(recalculated)
        st.session_state["schedule_changed_years_for_capacity"] = [int(selected_year)]
        save_authority_df("schedule", recalculated, user="schedule_month_authority_cleanup")
        _sync_capacity_results(recalculated)
        clear_data_cache()
        _cached_prepare_schedule.clear()
        _reset_schedule_working_df(recalculated)
        for key in list(st.session_state.keys()):
            text = str(key)
            if text.startswith(ASSEMBLY_ANALYSIS_STATE_PREFIX) or text.startswith("schedule_assembly_filter_"):
                del st.session_state[key]
        st.session_state["schedule_authority_save_message"] = (
            f"已永久刪除 {selected_year} 年 {'、'.join(delete_months)} 共 {len(rows_to_delete):,} 筆權威排程，"
            "並同步重算 04 產能負荷；05/09/12 將只讀取剩餘正式排程。"
        )
        st.rerun()



def _render_authority_query_difference_banner(raw_df: pd.DataFrame, selected_year: int) -> bool:
    """Explain that a query result is only a view, not the full authority schedule."""
    authority = ensure_schedule_record_ids(raw_df.copy() if isinstance(raw_df, pd.DataFrame) else pd.DataFrame())
    authority = ensure_year_column("schedule", authority, DEFAULT_YEAR)
    if authority.empty:
        return False
    year_mask = authority["年份"].map(lambda value: normalize_year(value, DEFAULT_YEAR)).eq(int(selected_year))
    year_df = authority.loc[year_mask].copy()
    meta = st.session_state.get(SCHEDULE_QUERY_META_KEY, {})
    if not isinstance(meta, dict) or int(meta.get("year", -1)) != int(selected_year):
        return False

    filtered_ids = {str(v).strip() for v in meta.get("filtered_ids", []) if str(v).strip()}
    authority_ids = set()
    if SCHEDULE_ID_COL in year_df.columns:
        authority_ids = set(year_df[SCHEDULE_ID_COL].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().tolist())
    outside_count = max(int(len(year_df)) - int(meta.get("filtered_count", 0)), 0)
    if filtered_ids and authority_ids:
        outside_count = len(authority_ids - filtered_ids)

    month_summary = schedule_month_summary(year_df, int(selected_year))
    active_months = [str(v) for v in month_summary.get("月份", pd.Series(dtype=object)).tolist() if str(v) and str(v) != "未設定"]
    query_text = f"{meta.get('date_col') or '未指定日期欄位'}"
    if meta.get("start") and meta.get("end"):
        try:
            start_text = pd.Timestamp(meta["start"]).strftime("%Y/%m/%d")
            end_text = pd.Timestamp(meta["end"]).strftime("%Y/%m/%d")
            query_text += f" {start_text}～{end_text}"
        except Exception:
            pass

    if outside_count > 0:
        st.error(
            f"目前表格只是在查詢『{query_text}』，顯示 {int(meta.get('filtered_count', 0)):,} 筆；"
            f"但 {selected_year} 年 schedule.json 權威資料共有 {len(year_df):,} 筆，查詢外仍有 {outside_count:,} 筆。"
            f"權威月份為：{'、'.join(active_months) if active_months else '無'}。"
            "因此 04、05 下方分析、09、12 仍會計入查詢外資料。請在緊接下方展開的『05 權威月份稽核與舊月份清理』永久刪除舊月份，或到 10 使用『以Excel完整同步匯入年度』。",
            icon="🚨",
        )
        return True

    st.success(
        f"目前查詢結果與 {selected_year} 年權威資料筆數一致（{len(year_df):,} 筆）；04/09/12 不會再讀到查詢外排程。",
        icon="✅",
    )
    return False


def _frame_for_compare(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Return a stable, comparable frame for one schedule year."""
    if df is None or df.empty:
        return pd.DataFrame()
    temp = ensure_year_column("schedule", df.copy(), DEFAULT_YEAR)
    temp["年份"] = temp["年份"].map(lambda value: normalize_year(value, DEFAULT_YEAR))
    temp = temp[temp["年份"].eq(int(year))].copy()
    if temp.empty:
        return pd.DataFrame()
    temp = temp.drop(columns=[SELECT_COL, ROW_ID_COL], errors="ignore")
    cols = sorted([str(c) for c in temp.columns])
    temp = temp.reindex(columns=cols)
    temp = temp.astype("string").fillna("")
    return temp.sort_values(cols).reset_index(drop=True)


def _changed_schedule_years(old_df: pd.DataFrame, new_df: pd.DataFrame) -> list[int]:
    """Detect which years actually changed, so save does not recalculate all years."""
    old_norm = ensure_year_column("schedule", old_df.copy() if old_df is not None else pd.DataFrame(), DEFAULT_YEAR)
    new_norm = ensure_year_column("schedule", new_df.copy() if new_df is not None else pd.DataFrame(), DEFAULT_YEAR)
    old_years = set(old_norm["年份"].map(lambda value: normalize_year(value, DEFAULT_YEAR)).dropna().astype(int).tolist()) if "年份" in old_norm.columns else set()
    new_years = set(new_norm["年份"].map(lambda value: normalize_year(value, DEFAULT_YEAR)).dropna().astype(int).tolist()) if "年份" in new_norm.columns else set()
    changed: list[int] = []
    for year in sorted(old_years | new_years):
        old_part = _frame_for_compare(old_norm, int(year))
        new_part = _frame_for_compare(new_norm, int(year))
        if old_part.shape != new_part.shape or not old_part.equals(new_part):
            changed.append(int(year))
    return changed


def _schedule_detail_date_columns(df: pd.DataFrame) -> list[str]:
    """Find real schedule date columns for date-range detail lookup."""
    if df is None or df.empty:
        return []
    preferred_order = ["機台入庫日", "MOVE IN", "排程日期", "入庫日", "出貨日", "交期", "預計完成日", "完成日", "日期"]
    allowed_tokens = ["日期", "入庫日", "出貨日", "交期", "完成日", "move in", "movein", "date"]
    excluded = {"年份", "月份", "M", "台數", "台數_raw", "機台計數", "PO", "工期", "標準工時", "需求工時"}
    found: list[str] = []
    for col in df.columns:
        col_text = str(col).strip()
        if col_text in excluded or col_text.startswith("_"):
            continue
        norm = col_text.lower().replace(" ", "")
        is_candidate = col_text in preferred_order or any(token in col_text for token in allowed_tokens) or any(token in norm for token in allowed_tokens)
        if not is_candidate:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce")
        if int(parsed.notna().sum()) > 0:
            found.append(col_text)
    ordered = [col for col in preferred_order if col in found]
    ordered += [col for col in found if col not in ordered]
    return ordered


def _format_detail_numbers(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the detail table readable without changing the source data."""
    out = df.copy()
    integer_like = ["年份", "台數", "機台計數"]
    for col in integer_like:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(0).astype("Int64")
    for col in ["標準工時", "需求工時", "工期"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").round(0).astype("Int64")
    return out


def _normalize_month_label(value: object) -> str | None:
    """Normalize schedule month values into labels like '7月'."""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            month = int(value)
        except Exception:
            month = 0
        if 1 <= month <= 12:
            return f"{month}月"
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "nat"}:
        return None
    compact = text.replace("月", "").strip()
    if compact.isdigit():
        month = int(compact)
        if 1 <= month <= 12:
            return f"{month}月"
    date_value = pd.to_datetime(text, errors="coerce")
    if pd.notna(date_value):
        return f"{int(date_value.month)}月"
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    try:
        month = int(digits)
    except Exception:
        return None
    if 1 <= month <= 12:
        return f"{month}月"
    return None


def _month_sort_key(label: object) -> int:
    normalized = _normalize_month_label(label)
    if not normalized:
        return 99
    return int(str(normalized).replace("月", ""))


def _available_month_labels(df: pd.DataFrame) -> list[str]:
    if df is None or df.empty or "月份" not in df.columns:
        return []
    labels = [label for label in df["月份"].map(_normalize_month_label).dropna().unique().tolist() if label]
    return sorted(labels, key=_month_sort_key)


def _build_monthly_demand_chart_frame(prepared: pd.DataFrame, machine_col: str, selected_year: int) -> tuple[pd.DataFrame, int]:
    """Build the monthly chart from months that truly exist in 05 authority data.

    Missing months are intentionally not padded with zero rows. This prevents the
    Page 05 chart from implying that July~December have official schedule records
    when the current authority schedule only contains January~June.
    """
    columns = ["年份", "年份顯示", "月別數字", "月份", "機台數", "需求工時"]
    if prepared is None or prepared.empty:
        return pd.DataFrame(columns=columns), 0

    temp = prepared.copy()
    temp["月別數字"] = temp.get("月份", pd.Series(index=temp.index, dtype=object)).map(_month_sort_key)
    month_numbers = pd.to_numeric(temp["月別數字"], errors="coerce").fillna(99).astype(int)
    invalid_count = int((month_numbers > 12).sum())
    temp = temp[month_numbers.between(1, 12)].copy()
    if temp.empty:
        return pd.DataFrame(columns=columns), invalid_count

    if "年份" not in temp.columns:
        temp["年份"] = int(selected_year)
    temp["年份"] = temp["年份"].map(lambda value: normalize_year(value, selected_year)).fillna(int(selected_year)).astype(int)
    temp["月別數字"] = pd.to_numeric(temp["月別數字"], errors="coerce").fillna(0).astype(int)
    temp["需求工時"] = pd.to_numeric(temp.get("需求工時", pd.Series(index=temp.index, dtype=float)), errors="coerce").fillna(0.0)
    temp[machine_col] = pd.to_numeric(temp.get(machine_col, pd.Series(index=temp.index, dtype=float)), errors="coerce").fillna(0.0)

    monthly = (
        temp.groupby(["年份", "月別數字"], as_index=False)
        .agg(機台數=(machine_col, "sum"), 需求工時=("需求工時", "sum"))
    )
    monthly["月份"] = monthly["月別數字"].map(lambda value: f"{int(value)}月")
    monthly["年份顯示"] = monthly["年份"].map(lambda value: str(int(value)))
    monthly = monthly.sort_values(["年份", "月別數字"]).reset_index(drop=True)
    return monthly[columns], invalid_count


def _default_one_month_range(df: pd.DataFrame, date_col: str | None, selected_year: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return a one-month default date range for analysis filters."""
    today = pd.Timestamp.today().normalize()
    fallback_start = pd.Timestamp(int(selected_year), 1, 1)
    if date_col and date_col in df.columns:
        dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
        if not dates.empty:
            year_dates = dates[dates.dt.year.eq(int(selected_year))]
            if not year_dates.empty:
                current_month = year_dates[(year_dates.dt.year.eq(today.year)) & (year_dates.dt.month.eq(today.month))]
                if not current_month.empty:
                    start = pd.Timestamp(today.year, today.month, 1)
                else:
                    start = pd.Timestamp(year_dates.min().year, year_dates.min().month, 1)
                end = start + pd.offsets.MonthEnd(0)
                return start, end
    month_labels = _available_month_labels(df)
    if month_labels:
        first_month = _month_sort_key(month_labels[0])
        fallback_start = pd.Timestamp(int(selected_year), int(first_month), 1)
    return fallback_start, fallback_start + pd.offsets.MonthEnd(0)


def _format_date_caption(start_ts: pd.Timestamp | None, end_ts: pd.Timestamp | None) -> str:
    if start_ts is None or end_ts is None:
        return "未套用日期區間"
    return f"{start_ts.strftime('%Y/%m/%d')} ~ {end_ts.strftime('%Y/%m/%d')}"



@st.cache_data(show_spinner=False, max_entries=16)
def _cached_prepare_schedule(
    schedule_df: pd.DataFrame,
    standard_df: pd.DataFrame,
    target_year: int,
    excluded_assembly_locations: tuple[str, ...],
    excluded_categories: tuple[str, ...],
    assembly_location_hours_items: tuple[tuple[str, float], ...],
) -> pd.DataFrame:
    """Cache the read-only yearly schedule preparation used by page analysis.

    The cache key includes the actual schedule/standard-hour frames and all exclusion
    parameters, so saved data or changed settings naturally create a new result.
    This removes repeated standard-hour lookup work from ordinary widget reruns.
    """
    return prepare_schedule(
        schedule_df,
        standard_df,
        target_year=int(target_year),
        excluded_assembly_locations=list(excluded_assembly_locations),
        excluded_categories=list(excluded_categories),
        assembly_location_hours=dict(assembly_location_hours_items),
    )


def _assembly_date_bounds(prepared: pd.DataFrame, date_col: str | None, selected_year: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    fallback_start = pd.Timestamp(int(selected_year), 1, 1)
    fallback_end = pd.Timestamp(int(selected_year), 12, 31)
    if prepared is None or prepared.empty or not date_col or date_col not in prepared.columns:
        return fallback_start, fallback_end
    parsed = pd.to_datetime(prepared[date_col], errors="coerce").dropna()
    if parsed.empty:
        return fallback_start, fallback_end
    return parsed.min().normalize(), parsed.max().normalize()


def _assembly_filter_state_key(selected_year: int) -> str:
    return f"{ASSEMBLY_ANALYSIS_STATE_PREFIX}_{int(selected_year)}"


def _build_assembly_location_analysis_frame(
    prepared: pd.DataFrame,
    *,
    filter_mode: str = "依月份",
    selected_month: str = "全部",
    selected_date_col: str | None = None,
    start_date: object | None = None,
    end_date: object | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    """Aggregate scheduled machine allocation and demand hours by assembly location.

    This helper is read-only. The persisted source column remains ``組立地點`` and
    the display name remains ``組裝地點``. Filtering supports either one selected
    month or an inclusive date range, without changing schedule calculations.
    """
    filter_info: dict[str, object] = {
        "篩選方式": filter_mode,
        "月份": selected_month if filter_mode == "依月份" else "—",
        "日期欄位": selected_date_col or "—",
        "開始日期": "—",
        "結束日期": "—",
        "日期空白排除筆數": 0,
    }
    if prepared is None or prepared.empty or "組立地點" not in prepared.columns:
        return pd.DataFrame(), pd.DataFrame(), filter_info

    detail = prepared.copy()
    if filter_mode == "依日期區間":
        if not selected_date_col or selected_date_col not in detail.columns:
            return pd.DataFrame(), pd.DataFrame(), filter_info
        parsed_dates = pd.to_datetime(detail[selected_date_col], errors="coerce")
        start_ts = pd.Timestamp(start_date).normalize() if start_date is not None else parsed_dates.min()
        end_ts = pd.Timestamp(end_date).normalize() if end_date is not None else parsed_dates.max()
        if pd.isna(start_ts) or pd.isna(end_ts):
            return pd.DataFrame(), pd.DataFrame(), filter_info
        if start_ts > end_ts:
            start_ts, end_ts = end_ts, start_ts
        filter_info["開始日期"] = start_ts.strftime("%Y/%m/%d")
        filter_info["結束日期"] = end_ts.strftime("%Y/%m/%d")
        filter_info["日期空白排除筆數"] = int(parsed_dates.isna().sum())
        detail = detail[parsed_dates.between(start_ts, end_ts, inclusive="both")].copy()
    elif selected_month != "全部" and "月份" in detail.columns:
        normalized_months = detail["月份"].map(_normalize_month_label)
        detail = detail[normalized_months.eq(selected_month)].copy()

    if detail.empty:
        return pd.DataFrame(), detail, filter_info

    detail["組裝地點"] = (
        detail["組立地點"]
        .fillna("未設定")
        .astype(str)
        .str.strip()
        .replace({"": "未設定", "nan": "未設定", "None": "未設定"})
    )
    machine_source = "機台計數" if "機台計數" in detail.columns else "台數"
    detail["_analysis_machine_count"] = pd.to_numeric(detail.get(machine_source, 0), errors="coerce").fillna(0.0)
    detail["_analysis_original_hours"] = pd.to_numeric(detail.get("原始需求工時", detail.get("排除前需求工時", detail.get("需求工時", 0))), errors="coerce").fillna(0.0)
    detail["_analysis_excluded_hours"] = pd.to_numeric(detail.get("產能計算排除工時", 0), errors="coerce").fillna(0.0)
    detail["_analysis_demand_hours"] = pd.to_numeric(detail.get("需求工時", detail.get("排除後需求工時", 0)), errors="coerce").fillna(0.0)

    if "Category" not in detail.columns:
        detail["Category"] = "未分類"
    detail["Category"] = detail["Category"].fillna("未分類").astype(str).str.strip().replace("", "未分類")

    if "產能計算排除" in detail.columns:
        excluded_flag = detail["產能計算排除"].fillna("").astype(str).str.strip().str.lower()
        detail["_analysis_excluded"] = excluded_flag.isin({"是", "yes", "y", "true", "1"})
    else:
        detail["_analysis_excluded"] = False

    summary = (
        detail.groupby("組裝地點", as_index=False, sort=False)
        .agg(
            排程筆數=("組裝地點", "size"),
            機台數=("_analysis_machine_count", "sum"),
            原始需求工時=("_analysis_original_hours", "sum"),
            產能計算排除工時=("_analysis_excluded_hours", "sum"),
            需求工時=("_analysis_demand_hours", "sum"),
            Category數=("Category", "nunique"),
            排除筆數=("_analysis_excluded", "sum"),
        )
    )
    total_machines = float(summary["機台數"].sum()) if not summary.empty else 0.0
    total_hours = float(summary["需求工時"].sum()) if not summary.empty else 0.0
    machine_denominator = summary["機台數"].where(summary["機台數"].gt(0))
    summary["平均工時/台"] = summary["需求工時"].div(machine_denominator).fillna(0.0)
    summary["機台占比(%)"] = summary["機台數"].div(total_machines).mul(100.0) if total_machines > 0 else 0.0
    summary["工時占比(%)"] = summary["需求工時"].div(total_hours).mul(100.0) if total_hours > 0 else 0.0
    summary = summary[
        ["組裝地點", "排程筆數", "Category數", "機台數", "原始需求工時", "產能計算排除工時", "需求工時", "平均工時/台", "機台占比(%)", "工時占比(%)", "排除筆數"]
    ].sort_values(["需求工時", "機台數", "排程筆數"], ascending=[False, False, False]).reset_index(drop=True)
    return summary, detail, filter_info


def _render_assembly_location_allocation_analysis(prepared: pd.DataFrame, selected_year: int) -> dict[str, pd.DataFrame]:
    """Render assembly-location allocation analysis with applied month/date filters."""
    st.subheader("組裝地點機台配置與需求工時分析")
    st.markdown(
        """
        <div class="stable-editor-card">
          <b>分析目的：依月份或日期區間，掌握各組裝地點承接的機台配置量與需求工時</b><br/>
          <span class="small-muted">篩選條件使用表單套用，調整日期時不會每選一次就重跑整個頁面。本區只做統計分析，不修改排程、標準工時或 04 產能負荷計算；底層資料欄位仍維持「組立地點」。</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    empty_result = {
        "assembly_location_summary": pd.DataFrame(),
        "assembly_location_detail": pd.DataFrame(),
        "assembly_location_filter": pd.DataFrame(),
    }
    if prepared is None or prepared.empty:
        st.info(f"目前沒有 {selected_year} 年可分析的排程資料。")
        return empty_result
    if "組立地點" not in prepared.columns:
        st.warning("目前排程資料沒有『組立地點』欄位，因此無法產生組裝地點配置分析。")
        return empty_result

    month_options = ["全部"] + _available_month_labels(prepared)
    date_columns = _schedule_detail_date_columns(prepared)
    mode_options = ["依月份"] + (["依日期區間"] if date_columns else [])
    state_key = _assembly_filter_state_key(selected_year)
    default_date_col = "機台入庫日" if "機台入庫日" in date_columns else (date_columns[0] if date_columns else None)
    default_start, default_end = _assembly_date_bounds(prepared, default_date_col, selected_year)

    applied = st.session_state.get(state_key)
    if not isinstance(applied, dict):
        saved_filter = load_module_settings(f"schedule_assembly_location_analysis_{int(selected_year)}")
        saved_start = pd.to_datetime(saved_filter.get("start_date"), errors="coerce") if saved_filter else pd.NaT
        saved_end = pd.to_datetime(saved_filter.get("end_date"), errors="coerce") if saved_filter else pd.NaT
        applied = {
            "filter_mode": saved_filter.get("filter_mode", "依月份") if saved_filter else "依月份",
            "selected_month": saved_filter.get("selected_month", "全部") if saved_filter else "全部",
            "selected_date_col": saved_filter.get("selected_date_col", default_date_col) if saved_filter else default_date_col,
            "start_date": saved_start.date() if pd.notna(saved_start) else default_start.date(),
            "end_date": saved_end.date() if pd.notna(saved_end) else default_end.date(),
            "sort_metric": saved_filter.get("sort_metric", "需求工時") if saved_filter else "需求工時",
            "selected_top_n": saved_filter.get("selected_top_n", 15) if saved_filter else 15,
        }
        st.session_state[state_key] = applied.copy()

    # Saved analysis conditions can outlive the authority schedule. If an old
    # month/date column no longer exists after a complete Excel synchronization or
    # permanent deletion, repair it immediately instead of continuing to analyse a
    # stale filter that is no longer selectable on screen.
    repaired_filter = False
    if str(applied.get("filter_mode", "依月份")) not in mode_options:
        applied["filter_mode"] = mode_options[0]
        repaired_filter = True
    if str(applied.get("selected_month", "全部")) not in month_options:
        applied["selected_month"] = "全部"
        repaired_filter = True
    selected_saved_date_col = applied.get("selected_date_col")
    if date_columns:
        if selected_saved_date_col not in date_columns:
            applied["selected_date_col"] = default_date_col
            repaired_filter = True
    elif selected_saved_date_col is not None:
        applied["selected_date_col"] = None
        repaired_filter = True
    if repaired_filter:
        st.session_state[state_key] = applied.copy()
        save_module_settings(
            f"schedule_assembly_location_analysis_{int(selected_year)}",
            {
                "filter_mode": str(applied.get("filter_mode", "依月份")),
                "selected_month": str(applied.get("selected_month", "全部")),
                "selected_date_col": applied.get("selected_date_col"),
                "start_date": pd.Timestamp(applied.get("start_date", default_start.date())).strftime("%Y-%m-%d"),
                "end_date": pd.Timestamp(applied.get("end_date", default_end.date())).strftime("%Y-%m-%d"),
                "sort_metric": str(applied.get("sort_metric", "需求工時")),
                "selected_top_n": applied.get("selected_top_n", 15),
            },
            user="schedule_assembly_location_filter_auto_repair",
        )
        st.info("原先保存的分析月份／日期條件已不在目前 05 權威排程中，系統已自動改回有效條件。", icon="🔄")

    current_mode = str(applied.get("filter_mode", "依月份"))
    if current_mode not in mode_options:
        current_mode = mode_options[0]
    mode_widget_key = f"schedule_assembly_filter_mode_v39_{selected_year}"
    if st.session_state.get(mode_widget_key) not in (None, *mode_options):
        st.session_state[mode_widget_key] = current_mode
    filter_mode = st.radio(
        "篩選方式",
        mode_options,
        index=mode_options.index(current_mode),
        horizontal=True,
        key=mode_widget_key,
        help="切換模式會刷新一次頁面；月份或日期內容請設定完成後按『套用分析條件』，避免每次選取都重算整頁。",
    )

    with st.form(key=f"schedule_assembly_filter_form_v39_{selected_year}_{filter_mode}", clear_on_submit=False):
        if filter_mode == "依月份":
            cols = st.columns([1.2, 1.1, 1.1, 2.8])
            with cols[0]:
                applied_month = str(applied.get("selected_month", "全部"))
                if applied_month not in month_options:
                    applied_month = "全部"
                selected_month = st.selectbox(
                    "分析月份",
                    month_options,
                    index=month_options.index(applied_month),
                    help="可選全部或單一月份。",
                )
            selected_date_col = applied.get("selected_date_col") or default_date_col
            start_date = applied.get("start_date", default_start.date())
            end_date = applied.get("end_date", default_end.date())
        else:
            cols = st.columns([1.3, 1.0, 1.0, 1.0, 1.0])
            with cols[0]:
                applied_date_col = str(applied.get("selected_date_col") or default_date_col)
                if applied_date_col not in date_columns:
                    applied_date_col = default_date_col
                selected_date_col = st.selectbox(
                    "日期欄位",
                    date_columns,
                    index=date_columns.index(applied_date_col),
                    help="通常使用『機台入庫日』；也可依資料中的其他有效日期欄位分析。",
                )
            date_min, date_max = _assembly_date_bounds(prepared, selected_date_col, selected_year)
            saved_start = pd.Timestamp(applied.get("start_date", date_min.date()))
            saved_end = pd.Timestamp(applied.get("end_date", date_max.date()))
            if saved_start < date_min or saved_start > date_max:
                saved_start = date_min
            if saved_end < date_min or saved_end > date_max:
                saved_end = date_max
            with cols[1]:
                start_date = st.date_input("開始日期", value=saved_start.date(), min_value=date_min.date(), max_value=date_max.date())
            with cols[2]:
                end_date = st.date_input("結束日期", value=saved_end.date(), min_value=date_min.date(), max_value=date_max.date())
            selected_month = str(applied.get("selected_month", "全部"))

        sort_col_index = 1 if filter_mode == "依月份" else 3
        top_col_index = 2 if filter_mode == "依月份" else 4
        with cols[sort_col_index]:
            sort_options = ["需求工時", "原始需求工時", "產能計算排除工時", "機台數", "平均工時/台"]
            applied_sort = str(applied.get("sort_metric", "需求工時"))
            if applied_sort not in sort_options:
                applied_sort = "需求工時"
            sort_metric = st.selectbox("圖表排序", sort_options, index=sort_options.index(applied_sort))
        with cols[top_col_index]:
            top_options: list[object] = [8, 10, 15, 20, "全部"]
            applied_top = applied.get("selected_top_n", 15)
            if applied_top not in top_options:
                applied_top = 15
            selected_top_n = st.selectbox("圖表顯示地點數", top_options, index=top_options.index(applied_top))

        submitted = st.form_submit_button("套用分析條件", type="primary", use_container_width=True)

    if submitted:
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        if start_ts > end_ts:
            start_ts, end_ts = end_ts, start_ts
        applied = {
            "filter_mode": filter_mode,
            "selected_month": selected_month,
            "selected_date_col": selected_date_col,
            "start_date": start_ts.date(),
            "end_date": end_ts.date(),
            "sort_metric": sort_metric,
            "selected_top_n": selected_top_n,
        }
        st.session_state[state_key] = applied
        save_module_settings(
            f"schedule_assembly_location_analysis_{int(selected_year)}",
            {
                "filter_mode": filter_mode,
                "selected_month": selected_month,
                "selected_date_col": selected_date_col,
                "start_date": start_ts.strftime("%Y-%m-%d"),
                "end_date": end_ts.strftime("%Y-%m-%d"),
                "sort_metric": sort_metric,
                "selected_top_n": selected_top_n,
            },
            user="schedule_assembly_location_analysis",
        )
        st.success("組裝地點分析條件已套用並永久保存。")

    filter_mode = str(applied.get("filter_mode", "依月份"))
    selected_month = str(applied.get("selected_month", "全部"))
    selected_date_col = applied.get("selected_date_col") or default_date_col
    start_date = applied.get("start_date", default_start.date())
    end_date = applied.get("end_date", default_end.date())
    sort_metric = str(applied.get("sort_metric", "需求工時"))
    selected_top_n = applied.get("selected_top_n", 15)
    top_n = 9999 if selected_top_n == "全部" else int(selected_top_n)

    summary, detail, filter_info = _build_assembly_location_analysis_frame(
        prepared,
        filter_mode=filter_mode,
        selected_month=selected_month,
        selected_date_col=selected_date_col,
        start_date=start_date,
        end_date=end_date,
    )
    if filter_mode == "依月份":
        range_caption = f"{selected_year} 年｜月份：{selected_month}"
    else:
        range_caption = f"{selected_year} 年｜{selected_date_col}：{filter_info['開始日期']} ~ {filter_info['結束日期']}"

    if summary.empty:
        st.info(f"目前篩選條件下沒有可分析的組裝地點資料。範圍：{range_caption}")
        filter_df = pd.DataFrame([{"分析年度": selected_year, **filter_info}])
        return {
            "assembly_location_summary": summary,
            "assembly_location_detail": detail,
            "assembly_location_filter": filter_df,
        }

    total_machines = float(pd.to_numeric(summary["機台數"], errors="coerce").fillna(0).sum())
    total_original_hours = float(pd.to_numeric(summary["原始需求工時"], errors="coerce").fillna(0).sum())
    total_excluded_hours = float(pd.to_numeric(summary["產能計算排除工時"], errors="coerce").fillna(0).sum())
    total_hours = float(pd.to_numeric(summary["需求工時"], errors="coerce").fillna(0).sum())
    avg_hours_per_machine = total_hours / total_machines if total_machines > 0 else 0.0
    busiest_location = str(summary.iloc[0]["組裝地點"])

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("組裝地點數", f"{summary['組裝地點'].nunique():,} 處")
    m2.metric("分配機台數", f"{total_machines:,.0f} 台")
    m3.metric("原始需求工時", f"{total_original_hours:,.0f} h")
    m4.metric("排除工時", f"{total_excluded_hours:,.0f} h")
    m5.metric("排除後需求工時", f"{total_hours:,.0f} h", help=f"需求工時最高地點：{busiest_location}；平均 {avg_hours_per_machine:,.1f} h/台")
    st.caption(f"已套用範圍：{range_caption}｜需求工時最高配置地點：{busiest_location}")
    if filter_mode == "依日期區間" and int(filter_info.get("日期空白排除筆數", 0)) > 0:
        st.caption(f"日期區間分析已排除日期空白或無法辨識的資料 {int(filter_info['日期空白排除筆數']):,} 筆。")

    display_summary = summary.copy()
    for col in ["排程筆數", "Category數", "機台數", "原始需求工時", "產能計算排除工時", "需求工時", "排除筆數"]:
        display_summary[col] = pd.to_numeric(display_summary[col], errors="coerce").fillna(0).round(0).astype("Int64")
    for col in ["平均工時/台", "機台占比(%)", "工時占比(%)"]:
        display_summary[col] = pd.to_numeric(display_summary[col], errors="coerce").fillna(0).round(1)
    render_configurable_view(display_summary, "schedule_assembly_location_allocation_summary", "組裝地點機台配置與需求工時分析表", height=390)

    chart_df = summary.sort_values([sort_metric, "需求工時", "機台數"], ascending=False).head(top_n).copy()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=chart_df["組裝地點"],
            y=chart_df["機台數"],
            name="分配機台數",
            text=chart_df["機台數"].round(0),
            texttemplate="%{text:,.0f}",
            textposition="inside",
            insidetextanchor="end",
            textfont={"color": "#FFFFFF", "size": 13},
            constraintext="none",
            hovertemplate="組裝地點：%{x}<br>分配機台數：%{y:,.0f} 台<extra></extra>",
            marker_line_width=0,
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=chart_df["組裝地點"],
            y=chart_df["需求工時"],
            name="需求工時",
            mode="lines+markers",
            cliponaxis=False,
            hovertemplate="組裝地點：%{x}<br>需求工時：%{y:,.0f} h<extra></extra>",
            line={"width": 3},
            marker={"size": 10, "line": {"width": 1.2, "color": "rgba(255,255,255,0.75)"}},
        ),
        secondary_y=True,
    )
    max_machines = float(pd.to_numeric(chart_df["機台數"], errors="coerce").fillna(0).max()) if not chart_df.empty else 0.0
    max_hours = float(pd.to_numeric(chart_df["需求工時"], errors="coerce").fillna(0).max()) if not chart_df.empty else 0.0
    machine_axis_top = max(1.0, max_machines * 1.20)
    hours_axis_top = max(1.0, max_hours * 1.36)
    fig.update_layout(
        template="plotly_dark",
        title={"text": f"{range_caption}｜組裝地點機台與工時配置", "x": 0.01, "xanchor": "left", "y": 0.98},
        height=540,
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.08, "xanchor": "right", "x": 1},
        margin={"l": 58, "r": 78, "t": 150, "b": 94},
        xaxis={"title": "組裝地點", "tickangle": -25, "categoryorder": "array", "categoryarray": chart_df["組裝地點"].tolist(), "automargin": True},
        bargap=0.35,
    )
    fig.update_yaxes(
        title_text="分配機台數（台）",
        tickformat=",.0f",
        range=[0, machine_axis_top],
        automargin=True,
        secondary_y=False,
    )
    fig.update_yaxes(
        title_text="需求工時（h）",
        tickformat=",.0f",
        range=[0, hours_axis_top],
        automargin=True,
        secondary_y=True,
    )
    # Use annotations instead of trace text so the line value and the bar value at the
    # same assembly location have independent vertical spacing and never overlap.
    for location, demand_hours in zip(chart_df["組裝地點"].astype(str), pd.to_numeric(chart_df["需求工時"], errors="coerce").fillna(0)):
        fig.add_annotation(
            x=location,
            y=float(demand_hours),
            yref="y2",
            text=f"{float(demand_hours):,.0f}",
            showarrow=False,
            yshift=18,
            xanchor="center",
            yanchor="bottom",
            font={"color": "#FF9F43", "size": 13},
            bgcolor="rgba(2,6,23,0.78)",
            bordercolor="rgba(255,159,67,0.45)",
            borderwidth=1,
            borderpad=3,
        )
    st.plotly_chart(fig, use_container_width=True)

    export_detail = detail.drop(columns=["_analysis_machine_count", "_analysis_original_hours", "_analysis_excluded_hours", "_analysis_demand_hours", "_analysis_excluded"], errors="ignore")
    filter_df = pd.DataFrame([{"分析年度": selected_year, **filter_info, "圖表排序": sort_metric, "圖表顯示地點數": selected_top_n}])
    return {
        "assembly_location_summary": display_summary,
        "assembly_location_detail": export_detail,
        "assembly_location_filter": filter_df,
    }

def _render_category_machine_count_analysis(
    schedule_df: pd.DataFrame,
    standard: pd.DataFrame,
    prepared_current_year: pd.DataFrame,
    years: list[int],
    selected_year: int,
    excluded_assembly_locations: list[str],
    excluded_categories: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """Render Category machine-count analysis with explicit year/month/date filters."""
    st.subheader("05 排程表 Category 機台計數 >= 1 統計與分析")
    st.markdown(
        """
        <div class="stable-editor-card">
          <b>統計篩選條件：年度 / 月份 / 日期區間</b><br/>
          <span class="small-muted">可依年度、月份、日期欄位、開始日期與結束日期篩選後顯示統計。預設會帶入所選年度第一個有資料月份的一個月區間；此區只改變畫面統計，不會寫入 05 排程表，也不會觸發 04 重新計算。</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    year_options = sorted({int(y) for y in years}) if years else [int(selected_year)]
    default_year_index = year_options.index(int(selected_year)) if int(selected_year) in year_options else len(year_options) - 1

    filter_cols = st.columns([0.85, 0.85, 1.25, 1.0, 1.0])
    with filter_cols[0]:
        selected_analysis_year = int(
            st.selectbox(
                "統計年度",
                year_options,
                index=default_year_index,
                key="schedule_category_analysis_year_v2",
                help="選擇要統計的排程年度。",
            )
        )

    if int(selected_analysis_year) == int(selected_year):
        year_base = prepared_current_year.copy() if isinstance(prepared_current_year, pd.DataFrame) else pd.DataFrame()
    else:
        year_base = _cached_prepare_schedule(
            schedule_df,
            standard,
            int(selected_analysis_year),
            tuple(str(v) for v in (excluded_assembly_locations or [])),
            tuple(str(v) for v in (excluded_categories or [])),
            tuple(sorted((str(k), float(v or 0.0)) for k, v in (assembly_location_hours or {}).items())),
        )
    if year_base is None:
        year_base = pd.DataFrame()

    year_date_columns = _schedule_detail_date_columns(year_base)
    date_col_options = year_date_columns or ["無日期欄位"]
    preferred_date_col = "機台入庫日" if "機台入庫日" in year_date_columns else (year_date_columns[0] if year_date_columns else "無日期欄位")
    with filter_cols[2]:
        selected_date_col = st.selectbox(
            "日期欄位",
            date_col_options,
            index=date_col_options.index(preferred_date_col),
            key=f"schedule_category_date_col_v2_{selected_analysis_year}",
            help="選擇要用來判斷日期區間的欄位，例如機台入庫日或 MOVE IN。",
        )
    if selected_date_col == "無日期欄位":
        selected_date_col = None

    one_month_start, one_month_end = _default_one_month_range(year_base, selected_date_col, selected_analysis_year)
    month_options = ["全部"] + _available_month_labels(year_base)
    default_month = _normalize_month_label(one_month_start.month)
    default_month_index = month_options.index(default_month) if default_month in month_options else 0
    with filter_cols[1]:
        selected_month = st.selectbox(
            "統計月份",
            month_options,
            index=default_month_index,
            key=f"schedule_category_month_v2_{selected_analysis_year}",
            help="選擇月份後，統計會同時套用月份條件；日期區間仍可進一步縮小範圍。",
        )

    if selected_month != "全部":
        month_number = _month_sort_key(selected_month)
        month_start = pd.Timestamp(int(selected_analysis_year), int(month_number), 1)
        month_end = month_start + pd.offsets.MonthEnd(0)
        default_start_date = month_start.date()
        default_end_date = month_end.date()
    else:
        default_start_date = one_month_start.date()
        default_end_date = one_month_end.date()

    date_key_suffix = f"{selected_analysis_year}_{selected_month}_{selected_date_col or 'none'}".replace("/", "_").replace(" ", "_")
    with filter_cols[3]:
        start_date = st.date_input(
            "開始日期",
            value=default_start_date,
            key=f"schedule_category_start_v2_{date_key_suffix}",
        )
    with filter_cols[4]:
        end_date = st.date_input(
            "結束日期",
            value=default_end_date,
            key=f"schedule_category_end_v2_{date_key_suffix}",
        )

    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    if start_ts > end_ts:
        start_ts, end_ts = end_ts, start_ts
    end_ts_inclusive = end_ts + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)

    st.caption(
        f"目前已套用：統計年度 {selected_analysis_year}｜統計月份 {selected_month}｜日期欄位 {selected_date_col or '無'}｜日期區間 {_format_date_caption(start_ts, end_ts)}"
    )

    if year_base.empty:
        st.info(f"目前沒有 {selected_analysis_year} 年可分析排程資料。")
        return {"category_summary": pd.DataFrame(), "category_detail": pd.DataFrame()}

    analysis_df = year_base.copy()
    if "月份" in analysis_df.columns and selected_month != "全部":
        analysis_df = analysis_df[analysis_df["月份"].map(_normalize_month_label).eq(selected_month)].copy()

    if selected_date_col and selected_date_col in analysis_df.columns:
        parsed_dates = pd.to_datetime(analysis_df[selected_date_col], errors="coerce")
        analysis_df = analysis_df[parsed_dates.ge(start_ts) & parsed_dates.le(end_ts_inclusive)].copy()

    machine_col = "機台計數" if "機台計數" in analysis_df.columns else "台數"
    analysis_df["_machine_count_for_category"] = pd.to_numeric(analysis_df.get(machine_col, 0), errors="coerce").fillna(0)
    if "需求工時" not in analysis_df.columns:
        analysis_df["需求工時"] = 0
    if "Category" not in analysis_df.columns:
        analysis_df["Category"] = "未分類"
    analysis_df["Category"] = analysis_df["Category"].fillna("未分類").astype(str).str.strip().replace("", "未分類")

    included = analysis_df[analysis_df["_machine_count_for_category"].ge(1)].copy()
    excluded_count = int(len(analysis_df) - len(included))
    total_machine_count = float(included["_machine_count_for_category"].sum()) if not included.empty else 0.0
    total_demand_hours = float(pd.to_numeric(included.get("需求工時", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not included.empty else 0.0
    category_count = int(included["Category"].nunique()) if not included.empty else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("統計排程筆數", f"{len(included):,} 筆")
    m2.metric("Category 數", f"{category_count:,} 類")
    m3.metric("機台計數合計", f"{total_machine_count:,.0f} 台")
    m4.metric("需求工時合計", f"{total_demand_hours:,.0f} h")
    st.caption(f"目前條件：年度 {selected_analysis_year}｜月份 {selected_month}｜日期 {_format_date_caption(start_ts, end_ts)}｜已排除機台計數 < 1 的資料 {excluded_count:,} 筆。")

    if included.empty:
        st.info("目前篩選條件下沒有機台計數 >= 1 的 Category 資料。")
        return {"category_summary": pd.DataFrame(), "category_detail": pd.DataFrame()}

    category_summary = (
        included.groupby("Category", as_index=False)
        .agg(
            排程筆數=("Category", "size"),
            機台計數=("_machine_count_for_category", "sum"),
            需求工時=("需求工時", "sum"),
        )
        .sort_values(["機台計數", "需求工時", "排程筆數"], ascending=[False, False, False])
        .reset_index(drop=True)
    )

    chart_df = category_summary.head(20).copy()
    fig = px.bar(chart_df, y="Category", x="機台計數", orientation="h", title="Category 機台計數 >= 1 統計", hover_data=["排程筆數", "需求工時"])
    fig.update_layout(template="plotly_dark", height=max(380, min(720, 260 + len(chart_df) * 22)), yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, use_container_width=True)

    render_configurable_view(_format_detail_numbers(category_summary), "schedule_category_ge1_summary", "Category 機台計數 >= 1 統計表", height=320)

    detail_cols = [
        "年份",
        selected_date_col,
        "月份",
        "WO",
        "客戶",
        "P/N",
        "Type",
        "Category",
        "組立地點",
        "生產廠區",
        "台數",
        "機台計數",
        "標準工時",
        "原始需求工時",
        "產能計算排除工時",
        "排除後需求工時",
        "需求工時",
        "產能計算排除",
        "工時計算排除",
        "台數計算排除",
        "產能計算排除原因",
    ]
    detail_cols = [col for col in detail_cols if col and col in included.columns]
    detail_df = included.drop(columns=["_machine_count_for_category"], errors="ignore").reindex(columns=detail_cols + [c for c in included.columns if c not in detail_cols and c != "_machine_count_for_category"])
    render_configurable_view(_format_detail_numbers(detail_df), "schedule_category_ge1_detail", "Category 機台計數 >= 1 明細", height=360)
    return {"category_summary": category_summary, "category_detail": detail_df}

def _schedule_source_signature(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "empty"
    temp = df.drop(columns=[SELECT_COL, ROW_ID_COL], errors="ignore")
    try:
        hashed = pd.util.hash_pandas_object(temp, index=True, categorize=True)
    except Exception:
        hashed = pd.util.hash_pandas_object(temp.fillna("").astype(str), index=True)
    return f"{temp.shape}-{int(hashed.sum())}"


def _initial_schedule_working_df(source_df: pd.DataFrame) -> pd.DataFrame:
    # 排程ID is persisted in authority data. It must not be regenerated from the
    # current row order, otherwise sorting/filtering can update or delete the wrong row.
    df = ensure_schedule_record_ids(source_df.copy() if isinstance(source_df, pd.DataFrame) else pd.DataFrame())
    if SELECT_COL in df.columns:
        df = df.drop(columns=[SELECT_COL])
    df = df.reset_index(drop=True)
    df.insert(0, SELECT_COL, False)
    return df


def _get_schedule_working_df(source_df: pd.DataFrame) -> pd.DataFrame:
    """Keep an editable working copy, resetting only when authority data changes."""
    current_signature = _schedule_source_signature(source_df)
    saved_signature = st.session_state.get(SCHEDULE_SIGNATURE_KEY)
    if (
        SCHEDULE_STATE_KEY not in st.session_state
        or SCHEDULE_SIGNATURE_KEY not in st.session_state
        or saved_signature != current_signature
    ):
        st.session_state[SCHEDULE_STATE_KEY] = _initial_schedule_working_df(source_df)
        st.session_state[SCHEDULE_SIGNATURE_KEY] = current_signature
    return st.session_state[SCHEDULE_STATE_KEY].copy()


def _reset_schedule_working_df(df: pd.DataFrame) -> None:
    st.session_state[SCHEDULE_STATE_KEY] = _initial_schedule_working_df(df)
    st.session_state[SCHEDULE_SIGNATURE_KEY] = _schedule_source_signature(df)


def _schedule_column_config(factory_options: list[str] | None = None) -> dict[str, object]:
    config: dict[str, object] = {
        SELECT_COL: st.column_config.CheckboxColumn("刪除", help="勾選後可按『刪除勾選並儲存』永久刪除。"),
        ROW_ID_COL: None,
        "年份": st.column_config.NumberColumn("年份", min_value=2000, max_value=2100, step=1, format="%d", help="用於多年度比較。"),
        "月份": st.column_config.TextColumn("月份", help="可填 1月~12月，系統會自動標準化。"),
        "生產廠區": st.column_config.SelectboxColumn(
            "生產廠區",
            options=[""] + [str(v) for v in (factory_options or []) if str(v).strip()],
            help="12 場地週轉的正式分廠欄位。直接指定優先於 12 自動分廠規則；空白代表交由分廠規則判斷。",
        ),
        "台數": st.column_config.NumberColumn("台數", min_value=0.0, step=1.0, format="%.0f", help="原始需求工時 = 台數 × 標準工時。"),
        "機台計數": st.column_config.NumberColumn("機台計數", min_value=0.0, step=1.0, format="%.0f", help="系統欄位：每月機台數使用此欄彙總。"),
        "標準工時": st.column_config.NumberColumn("標準工時", min_value=0.0, step=0.1, format="%g", help="可手動填；空白時系統會從 06. 標準工時補齊。"),
        "原始需求工時": st.column_config.NumberColumn("原始需求工時", min_value=0.0, step=0.1, format="%g", help="系統欄位：台數 × 標準工時，尚未扣除組立地點排除。"),
        "產能計算排除工時": st.column_config.NumberColumn("產能計算排除工時", min_value=0.0, step=0.1, format="%g", help="系統欄位：命中產能計算排除組立地點時，自原始需求工時扣除的工時。"),
        "排除後需求工時": st.column_config.NumberColumn("排除後需求工時", min_value=0.0, step=0.1, format="%g", help="系統欄位：原始需求工時 − 產能計算排除工時。"),
        "需求工時": st.column_config.NumberColumn("需求工時", min_value=0.0, step=0.1, format="%g", help="系統欄位：與排除後需求工時相同，供 04/09/首頁產能計算使用。"),
        "產能計算排除": st.column_config.TextColumn("產能計算排除", help="系統欄位：由 06. 標準工時的組立地點與 Category 排除設定決定。"),
        "工時計算排除": st.column_config.TextColumn("工時計算排除", help="系統欄位：組立地點排除時為是；需求工時歸 0，但機台計數保留。"),
        "台數計算排除": st.column_config.TextColumn("台數計算排除", help="系統欄位：Category 排除時為是；機台計數歸 0，但需求工時保留。"),
        "產能計算排除原因": st.column_config.TextColumn("產能計算排除原因", help="系統欄位：說明排除原因與影響欄位。"),
    }
    return config


def _preferred_schedule_columns(df: pd.DataFrame) -> list[str]:
    preferred = [
        SELECT_COL,
        ROW_ID_COL,
        "年份",
        "機台入庫日",
        "月份",
        "WO",
        "客戶",
        "P/N",
        "Type",
        "Category",
        "組立地點",
        "生產廠區",
        "台數_raw",
        "台數",
        "機台計數",
        "標準工時",
        "原始需求工時",
        "產能計算排除工時",
        "排除後需求工時",
        "需求工時",
        "產能計算排除",
        "工時計算排除",
        "台數計算排除",
        "產能計算排除原因",
        "排除前機台計數",
        "排除前需求工時",
        "工期",
        "PO",
    ]
    ordered = [c for c in preferred if c in df.columns]
    ordered += [c for c in df.columns if c not in ordered]
    return ordered


def _render_query_controls(working_df: pd.DataFrame, selected_year: int) -> tuple[pd.DataFrame, str | None, pd.Timestamp | None, pd.Timestamp | None]:
    st.subheader("排程查詢與穩定編輯")
    st.markdown(
        """
        <div class="stable-editor-card">
          <b>排程表穩定編輯模式 + 日期區間明細查詢</b><br/>
          <span class="small-muted">先查詢年/月/日～年/月/日區間，再直接在查詢結果中編輯、刪除、儲存；系統只在按儲存後重新計算並同步 04/09。</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if working_df.empty:
        return working_df.copy(), None, None, None

    df = ensure_year_column("schedule", working_df.copy(), DEFAULT_YEAR)
    df["_year_norm"] = df["年份"].map(lambda value: normalize_year(value, DEFAULT_YEAR))
    year_df = df[df["_year_norm"].eq(int(selected_year))].copy()
    if year_df.empty:
        st.info(f"目前沒有 {selected_year} 年排程資料。可按『新增空白列到查詢年度』建立新資料。")
        return year_df.drop(columns=["_year_norm"], errors="ignore"), None, None, None

    date_columns = _schedule_detail_date_columns(year_df.drop(columns=[ROW_ID_COL, SELECT_COL, "_year_norm"], errors="ignore"))
    date_col: str | None = None
    start_ts: pd.Timestamp | None = None
    end_ts: pd.Timestamp | None = None
    keyword = ""
    only_show_no_date = False

    with st.form("schedule_query_filter_form", clear_on_submit=False):
        c1, c2, c3, c4 = st.columns([1.25, 1, 1, 1.5])
        if date_columns:
            default_col_index = date_columns.index("機台入庫日") if "機台入庫日" in date_columns else 0
            with c1:
                date_col = st.selectbox("日期欄位", date_columns, index=default_col_index, key="schedule_query_date_col")
            parsed = pd.to_datetime(year_df[date_col], errors="coerce")
            valid_dates = parsed.dropna()
            min_date = valid_dates.min().date() if not valid_dates.empty else pd.Timestamp.today().date()
            max_date = valid_dates.max().date() if not valid_dates.empty else pd.Timestamp.today().date()
            with c2:
                start_date = st.date_input("開始日期", value=min_date, key=f"schedule_query_start_{date_col}")
            with c3:
                end_date = st.date_input("結束日期", value=max_date, key=f"schedule_query_end_{date_col}")
            if start_date > end_date:
                start_date, end_date = end_date, start_date
            start_ts = pd.Timestamp(start_date)
            end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        else:
            with c1:
                st.warning("找不到可辨識日期欄位，暫時顯示整年度資料。")
            with c2:
                start_ts = None
            with c3:
                end_ts = None
        with c4:
            keyword = st.text_input("關鍵字（WO / 客戶 / P/N，可空白）", value="", key="schedule_query_keyword")
        d1, d2, d3 = st.columns([1.2, 1.2, 3.6])
        with d1:
            only_show_no_date = st.checkbox("只看日期空白", value=False, key="schedule_query_only_blank_date") if date_col else False
        with d2:
            st.form_submit_button("套用查詢條件", type="primary", use_container_width=True)
        with d3:
            st.info("查詢本身不寫入資料；查詢結果表格按『儲存查詢結果並重新計算』後才會更新 05 並同步 04. 產能負荷表。", icon="🔎")

    filtered = year_df.copy()
    if date_col:
        parsed = pd.to_datetime(filtered[date_col], errors="coerce")
        if only_show_no_date:
            filtered = filtered[parsed.isna()].copy()
        elif start_ts is not None and end_ts is not None:
            filtered = filtered[parsed.ge(start_ts) & parsed.le(end_ts)].copy()
    keyword = str(keyword or "").strip()
    if keyword:
        search_cols = [c for c in ["WO", "客戶", "P/N", "Type", "Category", "PO"] if c in filtered.columns]
        if search_cols:
            joined = filtered[search_cols].fillna("").astype(str).agg(" ".join, axis=1)
            filtered = filtered[joined.str.contains(keyword, case=False, na=False, regex=False)].copy()
    filtered = filtered.drop(columns=["_year_norm"], errors="ignore")
    if date_col and date_col in filtered.columns:
        sort_cols = [date_col]
        if "WO" in filtered.columns:
            sort_cols.append("WO")
        filtered = filtered.sort_values(sort_cols, kind="stable")

    filtered_ids: list[str] = []
    if SCHEDULE_ID_COL in filtered.columns:
        filtered_ids = filtered[SCHEDULE_ID_COL].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().tolist()
    st.session_state[SCHEDULE_QUERY_META_KEY] = {
        "year": int(selected_year),
        "date_col": str(date_col or ""),
        "start": start_ts.isoformat() if start_ts is not None else "",
        "end": end_ts.isoformat() if end_ts is not None else "",
        "keyword": keyword,
        "only_blank": bool(only_show_no_date),
        "filtered_count": int(len(filtered)),
        "filtered_ids": filtered_ids,
    }
    return filtered, date_col, start_ts, end_ts


def _current_query_default_for_new_row(selected_year: int, working_df: pd.DataFrame) -> tuple[str | None, pd.Timestamp]:
    """Use current query date settings to make a new row immediately visible in the query editor."""
    date_col = st.session_state.get("schedule_query_date_col")
    if not date_col or date_col not in working_df.columns:
        candidates = _schedule_detail_date_columns(working_df.drop(columns=[ROW_ID_COL, SELECT_COL], errors="ignore"))
        date_col = candidates[0] if candidates else None

    date_value = None
    if date_col:
        # 新增空白列時優先使用目前查詢結束日，讓列排序後出現在查詢結果尾端，較容易看見。
        date_value = st.session_state.get(f"schedule_query_end_{date_col}")
        if date_value is None:
            date_value = st.session_state.get(f"schedule_query_start_{date_col}")
    try:
        date_ts = pd.Timestamp(date_value)
        if pd.isna(date_ts):
            raise ValueError("empty date")
    except Exception:
        date_ts = pd.Timestamp(int(selected_year), 1, 1)
    return date_col, date_ts


def _render_window_controls(total_rows: int) -> tuple[int, int]:
    if total_rows <= 0:
        return 0, 0
    state = st.session_state.get(SCHEDULE_WINDOW_KEY, {"start": 0, "size": 80})
    size_options = [30, 50, 80, 120, 200, 500]
    try:
        size = int(state.get("size", 80))
    except Exception:
        size = 80
    if size not in size_options:
        size = 80
    try:
        start = int(state.get("start", 0))
    except Exception:
        start = 0
    start = max(0, min(start, max(total_rows - 1, 0)))

    c0, c1, c2, c3, c4 = st.columns([1.1, 1.2, 1, 1, 2.2])
    with c0:
        size = st.selectbox("每次顯示筆數", size_options, index=size_options.index(size), key="schedule_query_page_size")
    with c1:
        row_number = st.number_input("跳到查詢結果第幾筆", min_value=1, max_value=max(total_rows, 1), value=min(start + 1, max(total_rows, 1)), step=1, key="schedule_query_row_number")
    if c2.button("上一段", key="schedule_query_prev", use_container_width=True):
        start = max(0, start - int(size))
    if c3.button("下一段", key="schedule_query_next", use_container_width=True):
        start = min(max(total_rows - 1, 0), start + int(size))
    jump_start = max(0, min(int(row_number) - 1, max(total_rows - 1, 0)))
    if jump_start != start and st.session_state.get("schedule_query_row_number_last") != int(row_number):
        start = jump_start
    st.session_state["schedule_query_row_number_last"] = int(row_number)
    end = min(start + int(size), total_rows)
    with c4:
        st.info(f"目前顯示查詢結果第 {start + 1:,} ～ {end:,} 筆，共 {total_rows:,} 筆。", icon="📍")
    st.session_state[SCHEDULE_WINDOW_KEY] = {"start": int(start), "size": int(size)}
    return int(start), int(end)


def _set_schedule_selection_by_row_ids(row_ids: list[str], selected: bool) -> int:
    """Set delete checkbox state in the working copy without writing authority data."""
    if SCHEDULE_STATE_KEY not in st.session_state:
        return 0
    latest_working = st.session_state[SCHEDULE_STATE_KEY].copy()
    if latest_working.empty or ROW_ID_COL not in latest_working.columns:
        return 0
    if SELECT_COL not in latest_working.columns:
        latest_working[SELECT_COL] = False
    normalized_ids = [str(value).strip() for value in row_ids if str(value).strip()]
    if not normalized_ids:
        return 0
    row_id_series = latest_working[ROW_ID_COL].fillna("").astype(str).str.strip()
    mask = row_id_series.isin(normalized_ids)
    latest_working.loc[mask, SELECT_COL] = bool(selected)
    st.session_state[SCHEDULE_STATE_KEY] = latest_working
    return int(mask.sum())


def _clear_all_schedule_selection() -> int:
    """Clear all temporary delete selections in the working copy."""
    if SCHEDULE_STATE_KEY not in st.session_state:
        return 0
    latest_working = st.session_state[SCHEDULE_STATE_KEY].copy()
    if latest_working.empty:
        return 0
    if SELECT_COL not in latest_working.columns:
        latest_working[SELECT_COL] = False
        st.session_state[SCHEDULE_STATE_KEY] = latest_working
        return 0
    selected_count = int(latest_working[SELECT_COL].fillna(False).astype(bool).sum())
    latest_working[SELECT_COL] = False
    st.session_state[SCHEDULE_STATE_KEY] = latest_working
    return selected_count


def _count_schedule_selection(df: pd.DataFrame | None) -> int:
    if df is None or df.empty or SELECT_COL not in df.columns:
        return 0
    return int(df[SELECT_COL].fillna(False).astype(bool).sum())


def _render_bulk_delete_selection_controls(filtered: pd.DataFrame, working_df: pd.DataFrame) -> None:
    """Render select-all / clear-all controls for delete checkboxes."""
    if filtered is None or filtered.empty or ROW_ID_COL not in filtered.columns:
        return
    query_selected_count = _count_schedule_selection(filtered)
    total_selected_count = _count_schedule_selection(working_df)
    row_ids = filtered[ROW_ID_COL].fillna("").astype(str).str.strip().tolist()

    st.markdown(
        """
        <div class="stable-editor-card">
          <b>批次刪除勾選</b><br/>
          <span class="small-muted">全選只會勾選目前查詢條件下的資料，不會立即刪除；必須再按「刪除勾選並儲存」才會正式刪除並重新計算。取消全部勾選會清除目前編輯暫存內所有刪除勾選。</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    b1, b2, b3 = st.columns([1.2, 1.2, 3.2])
    with b1:
        if st.button("全選目前查詢結果", key="schedule_select_all_filtered_for_delete", use_container_width=True):
            count = _set_schedule_selection_by_row_ids(row_ids, True)
            st.success(f"已勾選目前查詢結果 {count:,} 筆；請再按『刪除勾選並儲存』才會正式刪除。")
            st.rerun()
    with b2:
        if st.button("取消全部勾選", key="schedule_clear_all_delete_selection", use_container_width=True):
            count = _clear_all_schedule_selection()
            st.success(f"已取消 {count:,} 筆刪除勾選。")
            st.rerun()
    with b3:
        st.info(
            f"目前查詢結果已勾選 {query_selected_count:,} 筆；全部暫存資料共已勾選 {total_selected_count:,} 筆。按刪除前仍可在表格內逐筆取消。",
            icon="☑️",
        )


def _sync_capacity_results(recalculated_schedule: pd.DataFrame) -> None:
    """Recalculate and persist only affected 04 capacity years after schedule save."""
    try:
        changed_years = st.session_state.pop("schedule_changed_years_for_capacity", None)
        if not changed_years:
            st.info("排程資料已保存；系統未偵測到年度內容差異，因此略過 04. 產能負荷表重算。", icon="⚡")
            return

        standard = load_table("standard_hours")
        work_calendar = load_table("work_calendar")
        employees = load_table("employees")
        dispatch = load_table("dispatch")
        adjustments = load_table("capacity_adjustments")
        existing_results = load_table("capacity_results")
        params = load_parameters()

        calculated = calculate_capacity_by_years(
            recalculated_schedule,
            standard,
            work_calendar,
            employees,
            dispatch,
            params,
            adjustments=adjustments,
            years=[int(y) for y in changed_years],
        )
        merged = upsert_capacity_results(existing_results, calculated)
        save_authority_df("capacity_results", merged, user="schedule_save_recalculate_capacity")
        st.success(f"已同步重算 04. 產能負荷表：{', '.join(str(y) for y in changed_years)} 年。")
    except Exception as exc:
        # 排程本身仍可保存；同步失敗時讓使用者知道需回 04 手動重算。
        st.warning(f"排程已保存，但同步 04. 產能負荷表時計算失敗，請至 04 按重新計算。原因：{exc}")


def _save_schedule_working_df(
    working_df: pd.DataFrame,
    old_schedule: pd.DataFrame,
    standard: pd.DataFrame,
    *,
    user: str,
    deleted_ids: list[str] | None = None,
) -> pd.DataFrame:
    """Persist schedule edits by stable 排程ID without dropping unseen years/rows.

    Earlier versions replaced schedule.json with the editor working frame. If that
    frame contained only the selected year or a stale query copy, the other year was
    silently removed; a later Excel import then appeared to "restore" old records.
    This version always merges edits into the latest authority table and removes only
    the explicitly selected IDs.
    """
    latest_authority = ensure_schedule_record_ids(load_authority_df("schedule"))
    working = ensure_schedule_record_ids(working_df.copy()).drop(columns=[SELECT_COL], errors="ignore")
    deleted_set = {str(v).strip() for v in (deleted_ids or []) if str(v).strip()}

    latest_by_id = latest_authority.set_index(ROW_ID_COL, drop=False) if not latest_authority.empty else pd.DataFrame()
    working_by_id = working.set_index(ROW_ID_COL, drop=False) if not working.empty else pd.DataFrame()

    if latest_authority.empty:
        merged = working.copy()
    else:
        merged_by_id = latest_by_id.copy()
        for col in working.columns:
            if col not in merged_by_id.columns:
                merged_by_id[col] = pd.NA
        for col in merged_by_id.columns:
            if col not in working_by_id.columns:
                working_by_id[col] = pd.NA
        common_ids = merged_by_id.index.intersection(working_by_id.index)
        if len(common_ids):
            update_cols = [col for col in working_by_id.columns if col != ROW_ID_COL]
            merged_by_id.loc[common_ids, update_cols] = working_by_id.loc[common_ids, update_cols]
        new_ids = working_by_id.index.difference(merged_by_id.index)
        if len(new_ids):
            merged_by_id = pd.concat([merged_by_id, working_by_id.loc[new_ids]], axis=0, sort=False)
        if deleted_set:
            merged_by_id = merged_by_id.loc[~merged_by_id.index.astype(str).isin(deleted_set)].copy()
        merged = merged_by_id.reset_index(drop=True)

    # New rows entered manually are intentional restorations, so clear matching
    # deletion tombstones. Deleted rows are recorded before authority is overwritten.
    old_ids = set(latest_authority.get(ROW_ID_COL, pd.Series(dtype=object)).astype(str).tolist())
    manual_new = working[~working[ROW_ID_COL].astype(str).isin(old_ids)].copy() if not working.empty else pd.DataFrame()
    if not manual_new.empty:
        clear_deleted_schedule_tombstones(manual_new)

    clean = ensure_schedule_record_ids(merged).dropna(how="all")
    params = load_parameters()
    recalculated = recalculate_schedule_demand(
        clean,
        standard_hours=standard,
        target_year=None,
        excluded_assembly_locations=params.get(ASSEMBLY_EXCLUSION_PARAM_KEY, []),
        excluded_categories=params.get(CATEGORY_EXCLUSION_PARAM_KEY, []),
        assembly_location_hours=params.get(ASSEMBLY_LOCATION_HOURS_PARAM_KEY, {}),
    )
    recalculated = ensure_schedule_record_ids(recalculated)
    st.session_state["schedule_changed_years_for_capacity"] = _changed_schedule_years(latest_authority, recalculated)
    save_authority_df("schedule", recalculated, user=user)

    # Read-back verification makes the save result authoritative rather than relying
    # on the in-memory editor frame.
    persisted = ensure_schedule_record_ids(load_authority_df("schedule"))
    persisted_ids = set(persisted.get(ROW_ID_COL, pd.Series(dtype=object)).astype(str).tolist())
    failed_delete = sorted(deleted_set & persisted_ids)
    if failed_delete:
        raise RuntimeError(f"排程刪除驗證失敗，仍存在 {len(failed_delete)} 筆排程ID。")
    if len(persisted) != len(recalculated):
        raise RuntimeError(f"排程保存筆數驗證失敗：預期 {len(recalculated)} 筆，實際 {len(persisted)} 筆。")

    _sync_capacity_results(persisted)
    clear_data_cache()
    for _key in list(st.session_state.keys()):
        if str(_key).endswith("_prepared_bytes") or str(_key).endswith("_prepared_name"):
            del st.session_state[_key]
    _reset_schedule_working_df(persisted)
    return persisted


def _render_schedule_query_editor(
    source_df: pd.DataFrame,
    standard: pd.DataFrame,
    selected_year: int,
    *,
    excluded_assembly_locations: list[str] | None = None,
    excluded_categories: list[str] | None = None,
    assembly_location_hours: dict | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    old_schedule = source_df.copy() if isinstance(source_df, pd.DataFrame) else pd.DataFrame()
    editor_source = _schedule_editor_source(
        old_schedule,
        standard,
        int(selected_year),
        excluded_assembly_locations=excluded_assembly_locations,
        excluded_categories=excluded_categories,
        assembly_location_hours=assembly_location_hours,
    )
    working_df = _get_schedule_working_df(editor_source)

    action_cols = st.columns([1.2, 1.2, 1.4, 3.2])
    if action_cols[0].button("新增空白列到查詢年度", key="schedule_query_add_row", use_container_width=True):
        new_df = st.session_state[SCHEDULE_STATE_KEY].copy()
        if new_df.empty:
            base_cols = [SELECT_COL, ROW_ID_COL, "年份", "機台入庫日", "月份", "WO", "客戶", "P/N", "Type", "Category", "台數_raw", "台數", "機台計數", "標準工時", "原始需求工時", "產能計算排除工時", "排除後需求工時", "需求工時"]
            new_df = pd.DataFrame(columns=base_cols)
        blank = {col: None for col in new_df.columns}
        blank[SELECT_COL] = False
        blank[ROW_ID_COL] = ""
        blank["年份"] = int(selected_year)

        # 讓新增列直接落在目前查詢區間內，不會因日期篩選而看不到。
        date_col, date_ts = _current_query_default_for_new_row(int(selected_year), new_df)
        if date_col:
            if date_col not in new_df.columns:
                new_df[date_col] = None
                blank[date_col] = None
            blank[date_col] = date_ts.date()
            st.session_state[f"schedule_query_start_{date_col}"] = date_ts.date()
            existing_end = st.session_state.get(f"schedule_query_end_{date_col}", date_ts.date())
            try:
                end_date = pd.Timestamp(existing_end).date()
            except Exception:
                end_date = date_ts.date()
            if end_date < date_ts.date():
                end_date = date_ts.date()
            st.session_state[f"schedule_query_end_{date_col}"] = end_date
        blank["月份"] = f"{int(date_ts.month)}月"
        if "台數" in new_df.columns:
            blank["台數"] = 0
        if "機台計數" in new_df.columns:
            blank["機台計數"] = 0
        if "標準工時" in new_df.columns:
            blank["標準工時"] = 0
        if "需求工時" in new_df.columns:
            blank["需求工時"] = 0

        # 清除會把空白新增列排除掉的查詢條件。
        if "schedule_query_keyword" in st.session_state:
            st.session_state["schedule_query_keyword"] = ""
        if "schedule_query_only_blank_date" in st.session_state:
            st.session_state["schedule_query_only_blank_date"] = False

        new_df.loc[len(new_df)] = blank
        new_df = ensure_schedule_record_ids(new_df)
        st.session_state[SCHEDULE_STATE_KEY] = _clean_schedule_editor_missing_values(new_df)
        st.session_state[SCHEDULE_WINDOW_KEY] = {"start": max(len(new_df) - 1, 0), "size": st.session_state.get(SCHEDULE_WINDOW_KEY, {}).get("size", 80)}
        st.success("已新增空白列到目前查詢年度與日期區間；請在查詢結果內編輯後按儲存。")
        st.rerun()
    if action_cols[1].button("重新載入權威資料", key="schedule_query_reload", use_container_width=True):
        _reset_schedule_working_df(editor_source)
        st.success("已重新載入目前權威排程資料，並套用需求工時重算與 None 顯示清理。")
        st.rerun()
    with action_cols[2]:
        st.caption("編輯中不會即時寫入。")
    with action_cols[3]:
        st.info("若你剛從 10 匯入或其他頁面更新排程，可按『重新載入權威資料』同步畫面暫存。", icon="🔄")

    working_df = st.session_state[SCHEDULE_STATE_KEY].copy()
    filtered, date_col, start_ts, end_ts = _render_query_controls(working_df, selected_year)

    if filtered.empty:
        st.info("目前查詢條件下沒有排程明細。")
        st.session_state[SCHEDULE_LAST_QUERY_KEY] = pd.DataFrame()
        return working_df.drop(columns=[SELECT_COL], errors="ignore"), pd.DataFrame()

    # 批次勾選刪除只更新畫面暫存，不會寫入權威資料；正式刪除仍需按「刪除勾選並儲存」。
    _render_bulk_delete_selection_controls(filtered, st.session_state[SCHEDULE_STATE_KEY])

    total_machines = float(pd.to_numeric(filtered.get("機台計數", filtered.get("台數", pd.Series(dtype=float))), errors="coerce").fillna(0).sum())
    total_original_hours = float(pd.to_numeric(filtered.get("原始需求工時", filtered.get("排除前需求工時", filtered.get("需求工時", pd.Series(dtype=float)))), errors="coerce").fillna(0).sum())
    total_excluded_hours = float(pd.to_numeric(filtered.get("產能計算排除工時", pd.Series(0, index=filtered.index)), errors="coerce").fillna(0).sum())
    total_hours = float(pd.to_numeric(filtered.get("需求工時", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    customer_count = int(filtered["客戶"].dropna().astype(str).str.strip().replace("", pd.NA).dropna().nunique()) if "客戶" in filtered.columns else 0
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("查詢明細筆數", f"{len(filtered):,} 筆")
    m2.metric("查詢機台數", f"{total_machines:,.0f} 台")
    m3.metric("原始需求工時", f"{total_original_hours:,.0f} h")
    m4.metric("排除工時", f"{total_excluded_hours:,.0f} h")
    m5.metric("排除後需求工時", f"{total_hours:,.0f} h", help=f"客戶數：{customer_count:,} 家")

    window_start, window_end = _render_window_controls(len(filtered))
    visible = filtered.iloc[window_start:window_end].copy()
    visible = visible.reindex(columns=_preferred_schedule_columns(visible))

    disabled_columns = [ROW_ID_COL]
    for system_col in ["原始需求工時", "產能計算排除工時", "排除後需求工時", "需求工時", "機台計數", "標準工時來源", "產能計算排除", "工時計算排除", "台數計算排除", "產能計算排除原因", "排除前機台計數", "排除前需求工時"]:
        if system_col in visible.columns:
            disabled_columns.append(system_col)

    editor_key = f"schedule_query_editor_{selected_year}_{window_start}_{window_end}"
    save_clicked = False
    delete_clicked = False
    edited = visible.copy()
    with st.form(key=f"form_{editor_key}", clear_on_submit=False):
        st.caption("固定游標編輯：表格內連續輸入不會每格重新整理；按下方按鈕後才儲存、刪除、重新計算並同步 04/09。")
        edited = st.data_editor(
            visible,
            use_container_width=True,
            height=560,
            num_rows="fixed",
            key=editor_key,
            column_config=_schedule_column_config(factory_options),
            disabled=disabled_columns,
            hide_index=True,
        )
        s1, s2, s3 = st.columns([1.35, 1.35, 3.3])
        with s1:
            save_clicked = st.form_submit_button("儲存查詢結果並重新計算", type="primary", use_container_width=True)
        with s2:
            delete_clicked = st.form_submit_button("刪除勾選並儲存", use_container_width=True)
        with s3:
            st.info("儲存後：05 排程表權威資料更新 → 需求工時重算 → 04 產能負荷表重算 → 09 情境模擬讀取最新資料。", icon="✅")

    if ROW_ID_COL not in edited.columns and ROW_ID_COL in visible.columns:
        edited = edited.copy()
        edited[ROW_ID_COL] = visible[ROW_ID_COL].to_numpy()

    if save_clicked or delete_clicked:
        latest_working = st.session_state[SCHEDULE_STATE_KEY].copy()
        if ROW_ID_COL not in edited.columns:
            st.error("系統列識別欄位遺失，請重新載入權威資料後再試。")
        else:
            edited_rows = edited.copy()
            edited_rows[ROW_ID_COL] = edited_rows[ROW_ID_COL].fillna("").astype(str).str.strip()
            edited_rows = edited_rows[edited_rows[ROW_ID_COL].ne("")].copy()
            latest_working[ROW_ID_COL] = latest_working[ROW_ID_COL].fillna("").astype(str).str.strip()
            for col in edited_rows.columns:
                if col != ROW_ID_COL and col not in latest_working.columns:
                    latest_working[col] = None
            if not edited_rows.empty:
                latest_indexed = latest_working.set_index(ROW_ID_COL, drop=False)
                edited_indexed = edited_rows.set_index(ROW_ID_COL, drop=False)
                common_ids = latest_indexed.index.intersection(edited_indexed.index)
                update_cols = [col for col in edited_indexed.columns if col != ROW_ID_COL]
                if len(common_ids) > 0 and update_cols:
                    latest_indexed.loc[common_ids, update_cols] = edited_indexed.loc[common_ids, update_cols]
                latest_working = latest_indexed.reset_index(drop=True)

            deleted_count = 0
            selected_ids: list[str] = []
            if delete_clicked:
                if SELECT_COL in latest_working.columns:
                    selected_mask = latest_working[SELECT_COL].fillna(False).astype(bool)
                    selected_ids = latest_working.loc[selected_mask, ROW_ID_COL].fillna("").astype(str).str.strip().tolist()
                    selected_ids = [value for value in selected_ids if value]
                if not selected_ids:
                    st.warning("目前尚未勾選要刪除的資料；本次未寫入權威資料。")
                    return working_df.drop(columns=[SELECT_COL], errors="ignore"), pd.DataFrame()
                deleted_rows = latest_working[latest_working[ROW_ID_COL].astype(str).isin(selected_ids)].copy()
                record_deleted_schedule_rows(deleted_rows, user="schedule_query_editor_delete")
                latest_working = latest_working[~latest_working[ROW_ID_COL].astype(str).isin(selected_ids)].reset_index(drop=True)
                deleted_count = len(selected_ids)

            st.session_state[SCHEDULE_STATE_KEY] = latest_working
            saved = _save_schedule_working_df(
                latest_working,
                old_schedule,
                standard,
                user="schedule_query_editor_save",
                deleted_ids=selected_ids,
            )
            if delete_clicked:
                st.session_state["schedule_authority_save_message"] = (
                    f"已永久刪除 {deleted_count:,} 筆排程並重新讀回驗證；相同 Excel 明細預設不會自動復活，04 產能負荷已同步重算。"
                )
            else:
                st.session_state["schedule_authority_save_message"] = (
                    f"已永久保存排程修改，目前權威資料共 {len(saved):,} 筆，並已同步重算 04 產能負荷表。"
                )
            st.session_state[SCHEDULE_LAST_QUERY_KEY] = pd.DataFrame()
            st.rerun()

    detail_for_export = edited.copy().drop(columns=[ROW_ID_COL, SELECT_COL], errors="ignore")
    if date_col and date_col in detail_for_export.columns:
        detail_for_export[date_col] = pd.to_datetime(detail_for_export[date_col], errors="coerce").dt.strftime("%Y/%m/%d")
    detail_for_export = _format_detail_numbers(detail_for_export)
    st.session_state[SCHEDULE_LAST_QUERY_KEY] = detail_for_export
    return working_df.drop(columns=[SELECT_COL], errors="ignore"), detail_for_export


_raw_schedule_loaded = load_table("schedule")
_needs_schedule_id_migration = (
    SCHEDULE_ID_COL not in _raw_schedule_loaded.columns
    or _raw_schedule_loaded.get(SCHEDULE_ID_COL, pd.Series(index=_raw_schedule_loaded.index, dtype=object)).fillna("").astype(str).str.strip().eq("").any()
)
raw_schedule = ensure_schedule_record_ids(_raw_schedule_loaded)
if _needs_schedule_id_migration and not raw_schedule.empty:
    save_authority_df("schedule", raw_schedule, user="schedule_record_id_migration")
    clear_data_cache()
standard = load_table("standard_hours")
factory_slots = load_slots()
factory_options = sorted([str(v).strip() for v in factory_slots.get("廠區", pd.Series(dtype=object)).dropna().unique().tolist() if str(v).strip()])
params = load_parameters()
excluded_assembly_locations = params.get(ASSEMBLY_EXCLUSION_PARAM_KEY, [])
excluded_categories = params.get(CATEGORY_EXCLUSION_PARAM_KEY, [])
assembly_location_hours = params.get(ASSEMBLY_LOCATION_HOURS_PARAM_KEY, {})
years = available_years_from_frames([raw_schedule, standard])
selected_year = st.selectbox("顯示/分析年份", years, index=len(years) - 1, key="schedule_year_filter")
st.caption("05 程式來源版本：authority-source-v4。若看不到此行，表示部署仍在執行舊 #U 重複頁面；首頁啟動後會自動同步舊頁面。")

schedule, query_detail = _render_schedule_query_editor(
    raw_schedule,
    standard,
    int(selected_year),
    excluded_assembly_locations=excluded_assembly_locations,
    excluded_categories=excluded_categories,
    assembly_location_hours=assembly_location_hours,
)
_authority_query_mismatch = _render_authority_query_difference_banner(raw_schedule, int(selected_year))
_render_schedule_month_authority_cleanup(raw_schedule, int(selected_year), standard, params, expanded=_authority_query_mismatch)
prepared = _cached_prepare_schedule(
    schedule,
    standard,
    int(selected_year),
    tuple(str(v) for v in (excluded_assembly_locations or [])),
    tuple(str(v) for v in (excluded_categories or [])),
    tuple(sorted((str(k), float(v or 0.0)) for k, v in (assembly_location_hours or {}).items())),
)

c1, c2, c3, c4 = st.columns([1, 1, 1, 3])
with c1:
    if st.button("重新計算需求工時", type="primary", key="recalc_schedule_demand_preview"):
        old_schedule = load_table("schedule")
        recalculated = recalculate_schedule_demand(
            schedule,
            standard_hours=standard,
            target_year=None,
            excluded_assembly_locations=excluded_assembly_locations,
            excluded_categories=excluded_categories,
            assembly_location_hours=assembly_location_hours,
        )
        st.session_state["schedule_changed_years_for_capacity"] = _changed_schedule_years(old_schedule, recalculated)
        save_authority_df("schedule", recalculated, user="manual_recalculate_schedule_demand")
        _sync_capacity_results(recalculated)
        clear_data_cache()
        _reset_schedule_working_df(recalculated)
        st.success("已重新計算排程需求工時，並已同步更新 04. 產能負荷表計算結果。")
        st.rerun()
with c2:
    missing_std = int((pd.to_numeric(prepared.get("標準工時", pd.Series(dtype=float)), errors="coerce").fillna(0) <= 0).sum()) if not prepared.empty else 0
    st.metric("標準工時缺漏", f"{missing_std} 筆")
with c3:
    total_original_hours = float(pd.to_numeric(prepared.get("原始需求工時", prepared.get("排除前需求工時", pd.Series(dtype=float))), errors="coerce").fillna(0).sum()) if not prepared.empty else 0.0
    total_excluded_hours = float(pd.to_numeric(prepared.get("產能計算排除工時", pd.Series(0, index=prepared.index)), errors="coerce").fillna(0).sum()) if not prepared.empty else 0.0
    total_hours = float(pd.to_numeric(prepared.get("需求工時", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not prepared.empty else 0.0
    st.metric("年度排除後需求工時", f"{total_hours:,.0f} h", delta=f"原始 {total_original_hours:,.0f} h｜排除 {total_excluded_hours:,.0f} h", delta_color="off")
with c4:
    st.info("重新計算按鈕可在不進入 04 的情況下，直接將排程變更串聯到產能負荷與情境模擬。", icon="🔄")

st.subheader("排程資料品質")
checks = validate_schedule(prepared, excluded_assembly_locations=excluded_assembly_locations, excluded_categories=excluded_categories)
render_configurable_view(checks, "schedule_quality", "05. 排程資料品質", height=260)
_render_schedule_visibility_diagnostics(raw_schedule, schedule, prepared, int(selected_year))
_render_schedule_authority_diagnostics(raw_schedule, schedule, int(selected_year))

if not prepared.empty:
    c1, c2 = st.columns([1, 1])
    with c1:
        machine_col = "機台計數" if "機台計數" in prepared.columns else "台數"
        monthly, invalid_month_count = _build_monthly_demand_chart_frame(prepared, machine_col, int(selected_year))
        if invalid_month_count > 0:
            st.caption(f"已排除 {invalid_month_count:,} 筆無效月份資料（例如 0、空白、未設定），圖表只顯示 05 權威排程中的有效月份。")
        if monthly.empty:
            st.info(f"目前 {selected_year} 年沒有可繪製的正式排程月份。")
        else:
            actual_month_order = monthly["月份"].dropna().astype(str).drop_duplicates().tolist()
            st.caption(f"圖表只顯示 05 權威排程實際存在的月份：{'、'.join(actual_month_order)}")
            fig = px.bar(
                monthly,
                x="月份",
                y="需求工時",
                title=f"{selected_year}年月別訂單需求工時",
                category_orders={"月份": actual_month_order},
                custom_data=["機台數", "年份顯示"],
                labels={"需求工時": "需求工時", "月份": "月份", "年份顯示": "年份"},
            )
            fig.update_traces(
                hovertemplate="月份：%{x}<br>需求工時：%{y:,.0f} h<br>機台數：%{customdata[0]:,.0f} 台<extra></extra>"
            )
            fig.update_layout(
                template="plotly_dark",
                height=380,
                xaxis={"categoryorder": "array", "categoryarray": actual_month_order},
                yaxis={"tickformat": ",.0f", "title": "需求工時"},
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        if "客戶" in prepared.columns:
            customer = prepared.groupby("客戶", as_index=False).agg(需求工時=("需求工時", "sum")).sort_values("需求工時", ascending=False).head(15)
            fig2 = px.bar(customer, y="客戶", x="需求工時", orientation="h", title="客戶需求工時 Top 15")
            fig2.update_layout(template="plotly_dark", height=380)
            st.plotly_chart(fig2, use_container_width=True)

assembly_location_analysis = _render_assembly_location_allocation_analysis(prepared, int(selected_year))

category_analysis = _render_category_machine_count_analysis(
    schedule,
    standard,
    prepared,
    years,
    int(selected_year),
    excluded_assembly_locations,
    excluded_categories,
)

st.subheader(f"{selected_year}年排程分析匯出")
render_module_report_download(
    "05.排程表",
    {
        "排程權威資料": schedule,
        "排程計算結果": prepared,
        "查詢編輯區明細": query_detail,
        "組裝地點分析條件": assembly_location_analysis.get("assembly_location_filter", pd.DataFrame()),
        "組裝地點配置分析": assembly_location_analysis.get("assembly_location_summary", pd.DataFrame()),
        "組裝地點配置明細": assembly_location_analysis.get("assembly_location_detail", pd.DataFrame()),
        "Category 機台計數>=1統計": category_analysis.get("category_summary", pd.DataFrame()),
        "Category 機台計數>=1明細": category_analysis.get("category_detail", pd.DataFrame()),
        "資料品質": checks,
    },
    chart_specs=[
        {
            "type": "bar",
            "data_sheet": "組裝地點配置分析",
            "category_col": "組裝地點",
            "value_cols": ["機台數"],
            "title": f"{selected_year}年組裝地點機台配置",
            "anchor": "A7",
        },
        {
            "type": "line",
            "data_sheet": "組裝地點配置分析",
            "category_col": "組裝地點",
            "value_cols": ["需求工時"],
            "title": f"{selected_year}年組裝地點需求工時",
            "anchor": "J7",
        },
    ],
    metadata={
        "計算規則": "原始需求工時 = 台數 × 標準工時；產能計算排除工時 = 命中 06 組立地點排除規則的原始需求工時；排除後需求工時/需求工時 = 原始需求工時 − 產能計算排除工時；06 Category 排除只讓機台計數歸 0，不扣工時。04 需求總工時 = 排除後需求工時 + 04 月別調整工時。",
        "組裝地點分析口徑": "依使用者套用的月份或日期區間，按 05 排程表組立地點同時彙總原始需求工時、產能計算排除工時與排除後需求工時；僅供分析，不修改原排程與計算。",
        "分析年份": selected_year,
    },
    label="匯出 05. 排程表完整 Excel",
    key="export_schedule_module_report",
)

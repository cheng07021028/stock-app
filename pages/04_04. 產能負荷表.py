from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from services.capacity_engine import ASSEMBLY_EXCLUSION_PARAM_KEY, ASSEMBLY_LOCATION_HOURS_PARAM_KEY, CATEGORY_EXCLUSION_PARAM_KEY, MONTH_ORDER, calculate_capacity, calculate_capacity_by_years, normalize_month, prepare_schedule, upsert_capacity_results
from services.data_loader import clear_data_cache, load_all_tables, load_table
from services.page_utils import format_numeric_editor_display_dataframe, parse_numeric_display_series, render_configurable_view, render_module_report_download, render_saveable_table
from services.persistent_store import load_parameters, save_authority_df
from services.powerbi_theme import chart_spec, render_powerbi_chart, style_powerbi_figure
from services.schedule_record_service import SCHEDULE_ID_COL, active_schedule_months, schedule_month_summary
from services.ui_theme import apply_tech_theme, render_hero, render_human_help, status_board_html, status_pill
from services.year_service import DEFAULT_YEAR, available_years_from_tables

st.set_page_config(page_title="04. 產能負荷表", page_icon="📊", layout="wide")
apply_tech_theme()
render_hero("04. 產能負荷表", "月別需求工時(h)、可用工時(h)、產能負荷率(%)、產能餘額(h)、人力差異(人)與缺工天數(天)。")
render_human_help([
    "原始需求工時 = 05 排程台數 × 06 標準工時；產能計算排除工時 = 命中排除組立地點的原始工時；排除後需求工時 = 原始 − 排除。",
    "01/02 人力、07 工作天數、08 參數會共同計算正常可用工時、加班可用工時、產能負荷率、人力差異與缺工天數。",
    "需求總工時 = 排除後需求工時 + 04 月別調整工時；所有產能負荷率、產能餘額、需求人力與缺工評估都以需求總工時計算。",
])

tables = load_all_tables()
params = load_parameters()
years = available_years_from_tables(tables, DEFAULT_YEAR)

def _ensure_manual_adjustments(source: pd.DataFrame, year: int) -> pd.DataFrame:
    """Return one editable override row per month for the selected year.

    The authority table is still data/persistent/authority/capacity_adjustments.json.
    Blank values mean no override; current UI pre-fills them with the calculated
    values so managers can edit and save directly from the main capacity table.
    """
    base = pd.DataFrame({"年份": [int(year)] * 12, "月份": MONTH_ORDER})
    if source is None or source.empty:
        source = pd.DataFrame(columns=["年份", "月份", "每月機台數", "正常工作日", "調整工時", "備註"])
    src = source.copy()
    for col in ["年份", "月份", "每月機台數", "正常工作日", "調整工時", "備註"]:
        if col not in src.columns:
            src[col] = pd.NA if col not in ["調整工時", "備註"] else (0.0 if col == "調整工時" else "")
    src["年份"] = pd.to_numeric(src["年份"], errors="coerce").fillna(year).astype(int)
    src["月份"] = src["月份"].map(normalize_month)
    src = src[src["年份"].eq(int(year))].copy()
    if not src.empty:
        src = src.drop_duplicates(["年份", "月份"], keep="last")
    merged = base.merge(src[["年份", "月份", "每月機台數", "正常工作日", "調整工時", "備註"]], on=["年份", "月份"], how="left")
    merged["調整工時"] = pd.to_numeric(merged["調整工時"], errors="coerce").fillna(0.0)
    merged["備註"] = merged["備註"].fillna("").astype(str)
    return merged


def _upsert_manual_adjustments(source: pd.DataFrame, rows: pd.DataFrame, year: int) -> pd.DataFrame:
    if source is None or source.empty:
        source = pd.DataFrame(columns=["年份", "月份", "每月機台數", "正常工作日", "調整工時", "備註"])
    old = source.copy()
    for col in ["年份", "月份", "每月機台數", "正常工作日", "調整工時", "備註"]:
        if col not in old.columns:
            old[col] = pd.NA
    old["年份"] = pd.to_numeric(old["年份"], errors="coerce").fillna(DEFAULT_YEAR).astype(int)
    old["月份"] = old["月份"].map(normalize_month)
    new_rows = rows.copy()
    new_rows["年份"] = int(year)
    new_rows["月份"] = new_rows["月份"].map(normalize_month)
    for col in ["每月機台數", "正常工作日", "調整工時"]:
        new_rows[col] = pd.to_numeric(new_rows[col], errors="coerce")
    new_rows["調整工時"] = new_rows["調整工時"].fillna(0.0)
    if "備註" not in new_rows.columns:
        new_rows["備註"] = ""
    new_rows["備註"] = new_rows["備註"].fillna("").astype(str)
    month_set = set(new_rows["月份"].astype(str).tolist())
    keep = ~(old["年份"].eq(int(year)) & old["月份"].astype(str).isin(month_set))
    result = pd.concat([old.loc[keep], new_rows[["年份", "月份", "每月機台數", "正常工作日", "調整工時", "備註"]]], ignore_index=True, sort=False)
    order_map = {m: i for i, m in enumerate(MONTH_ORDER, start=1)}
    result["_month_order"] = result["月份"].map(order_map).fillna(99)
    result = result.sort_values(["年份", "_month_order"]).drop(columns=["_month_order"]).reset_index(drop=True)
    return result


def _display_number_table(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize 04 editor values without changing calculation logic.

    The editable 04 table must keep numeric dtype for st.data_editor.  Values are
    therefore rounded by rule here, while the NumberColumn config controls the
    on-screen format: unit 人 and % as integers; true decimals to 1 place.
    """
    display = df.copy()
    integer_keywords = ["(台)", "台數", "天數", "工作日", "月別數字", "月份數字"]
    people_keywords = ["(人)", "人力", "人數", "補人", "缺工"]
    for col in display.columns:
        col_name = str(col)
        numeric = pd.to_numeric(display[col], errors="coerce")
        if numeric.notna().sum() == 0:
            continue
        if any(k in col_name for k in ["直接有效人力", "扣請假後有效人力", "請假扣除人力"]):
            display[col] = numeric.round(1)
        elif "可用總人力" in col_name or "%" in col_name or any(k in col_name for k in integer_keywords):
            display[col] = numeric.round(0)
        elif any(k in col_name for k in people_keywords):
            display[col] = numeric.round(1)
        else:
            display[col] = numeric.round(1)
    return display

def _clear_machine_count_overrides(source: pd.DataFrame, year: int | None = None) -> pd.DataFrame:
    """Clear 04 manual machine-count overrides while preserving other adjustments.

    ``year`` clears one selected year. ``None`` clears every year, which is required
    for the multi-year comparison when the UI is set to use 05 as the official
    machine-count source. 調整工時 and 正常工作日 are never changed here.
    """
    if source is None or source.empty:
        return pd.DataFrame(columns=["年份", "月份", "每月機台數", "正常工作日", "調整工時", "備註"])
    result = source.copy()
    for col in ["年份", "月份", "每月機台數", "正常工作日", "調整工時", "備註"]:
        if col not in result.columns:
            result[col] = pd.NA
    result["年份"] = pd.to_numeric(result["年份"], errors="coerce").fillna(DEFAULT_YEAR).astype(int)
    result["月份"] = result["月份"].map(normalize_month)
    mask = pd.Series(True, index=result.index) if year is None else result["年份"].eq(int(year))
    if "每月機台數" in result.columns:
        result.loc[mask, "每月機台數"] = pd.NA
    return result.reset_index(drop=True)


def _machine_override_save_rows(edited_table: pd.DataFrame) -> pd.DataFrame:
    """Build capacity_adjustments rows without freezing schedule machine counts.

    The editable table displays 每月機台數, but if the value is the same as
    排程彙總機台數, it should not be written as a 04 override; otherwise future
    05 排程表 changes would be blocked by old 04 manual values.
    """
    rows = edited_table.rename(columns={
        "每月機台數(台)": "每月機台數",
        "排程彙總機台數(台)": "排程彙總機台數",
        "手動覆寫機台數(台)": "手動覆寫機台數",
        "正常工作日(天)": "正常工作日",
        "調整工時(h)": "調整工時",
    }).copy()
    for col in ["每月機台數", "排程彙總機台數", "正常工作日", "調整工時"]:
        if col in rows.columns:
            rows[col] = parse_numeric_display_series(rows[col])
    if "排程彙總機台數" in rows.columns and "每月機台數" in rows.columns:
        same_as_schedule = rows["每月機台數"].round(6).eq(rows["排程彙總機台數"].round(6)).fillna(False)
        rows.loc[same_as_schedule, "每月機台數"] = pd.NA
    required = ["年份", "月份", "每月機台數", "正常工作日", "調整工時"]
    for col in required:
        if col not in rows.columns:
            rows[col] = pd.NA
    return rows[required].copy()


def _recalculate_and_persist_capacity(year: int, adjustments_df: pd.DataFrame, *, user: str) -> pd.DataFrame:
    """Re-read current authority data, recalculate selected year, and persist results."""
    clear_data_cache()
    fresh_tables = load_all_tables()
    fresh_params = load_parameters()
    recalculated = calculate_capacity(
        fresh_tables["schedule"],
        fresh_tables["standard_hours"],
        fresh_tables["work_calendar"],
        fresh_tables["employees"],
        fresh_tables["dispatch"],
        fresh_params,
        adjustments_df,
        target_year=year,
    )
    existing_results = load_table("capacity_results")
    saved_results = upsert_capacity_results(existing_results, recalculated, target_year=year)
    save_authority_df("capacity_results", saved_results, user=user)
    clear_data_cache()
    return recalculated


@st.cache_data(ttl=300, show_spinner=False)
def _cached_capacity_by_years(
    schedule: pd.DataFrame,
    standard_hours: pd.DataFrame,
    work_calendar: pd.DataFrame,
    employees: pd.DataFrame,
    dispatch: pd.DataFrame,
    params: dict,
    adjustments: pd.DataFrame,
    years_tuple: tuple[int, ...],
) -> pd.DataFrame:
    """Cache multi-year comparison so 04 save reruns do not recalculate every year twice."""
    return calculate_capacity_by_years(
        schedule,
        standard_hours,
        work_calendar,
        employees,
        dispatch,
        params,
        adjustments,
        years=list(years_tuple),
    )


def _render_capacity_reconciliation_diagnostics(
    schedule_detail: pd.DataFrame,
    capacity_df: pd.DataFrame,
    tables: dict[str, pd.DataFrame],
    adjustments_df: pd.DataFrame,
    selected_year: int,
    missing_std_count: int,
) -> None:
    """Show whether 04 is reconciled to the current 05 authority schedule."""
    with st.expander("04/05 計算與權威資料核對", expanded=False):
        month_base = pd.DataFrame({"月份": MONTH_ORDER})
        detail = schedule_detail.copy() if isinstance(schedule_detail, pd.DataFrame) else pd.DataFrame()
        if not detail.empty:
            detail["月份"] = detail.get("月份", pd.Series(index=detail.index, dtype=object)).map(normalize_month)
            detail["機台計數"] = pd.to_numeric(detail.get("機台計數", 0), errors="coerce").fillna(0)
            detail["原始需求工時"] = pd.to_numeric(detail.get("原始需求工時", detail.get("排除前需求工時", detail.get("需求工時", 0))), errors="coerce").fillna(0)
            detail["產能計算排除工時"] = pd.to_numeric(detail.get("產能計算排除工時", 0), errors="coerce").fillna(0)
            detail["需求工時"] = pd.to_numeric(detail.get("需求工時", detail.get("排除後需求工時", 0)), errors="coerce").fillna(0)
            schedule_monthly = detail.groupby("月份", as_index=False).agg(
                **{
                    "05機台數": ("機台計數", "sum"),
                    "05原始需求工時": ("原始需求工時", "sum"),
                    "05產能計算排除工時": ("產能計算排除工時", "sum"),
                    "05排除後需求工時": ("需求工時", "sum"),
                }
            )
        else:
            schedule_monthly = pd.DataFrame(columns=["月份", "05機台數", "05原始需求工時", "05產能計算排除工時", "05排除後需求工時"])
        check = month_base.merge(schedule_monthly, on="月份", how="left")
        cap_cols = [c for c in ["月份", "排程彙總機台數", "原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "需求總工時"] if c in capacity_df.columns]
        cap_monthly = capacity_df[cap_cols].copy() if cap_cols else pd.DataFrame(columns=["月份"])
        if not cap_monthly.empty:
            cap_monthly["月份"] = cap_monthly["月份"].map(normalize_month)
            cap_monthly = cap_monthly.drop_duplicates("月份", keep="last")
        check = check.merge(cap_monthly, on="月份", how="left")
        for col in ["05機台數", "05原始需求工時", "05產能計算排除工時", "05排除後需求工時", "排程彙總機台數", "原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "需求總工時"]:
            if col not in check.columns:
                check[col] = 0.0
            check[col] = pd.to_numeric(check[col], errors="coerce").fillna(0.0)
        check["機台數差異"] = (check["排程彙總機台數"] - check["05機台數"]).round(6)
        check["原始需求工時差異"] = (check["原始需求工時"] - check["05原始需求工時"]).round(6)
        check["排除工時差異"] = (check["產能計算排除工時"] - check["05產能計算排除工時"]).round(6)
        check["排除後需求工時差異"] = (check["排除後需求工時"] - check["05排除後需求工時"]).round(6)
        check["05排除公式差異"] = (check["05原始需求工時"] - check["05產能計算排除工時"] - check["05排除後需求工時"]).round(6)
        check["04需求總公式差異"] = (check["排除後需求工時"] + check["調整工時"] - check["需求總工時"]).round(6)
        check["核對結果"] = check.apply(
            lambda row: "一致" if all(abs(float(row[col])) < 1e-6 for col in ["機台數差異", "原始需求工時差異", "排除工時差異", "排除後需求工時差異", "05排除公式差異", "04需求總公式差異"]) else "不一致",
            axis=1,
        )

        schedule_raw = tables.get("schedule", pd.DataFrame())
        duplicate_schedule_ids = 0
        if isinstance(schedule_raw, pd.DataFrame) and not schedule_raw.empty and SCHEDULE_ID_COL in schedule_raw.columns:
            id_text = schedule_raw[SCHEDULE_ID_COL].fillna("").astype(str).str.strip()
            duplicate_schedule_ids = int(id_text[id_text.ne("")].duplicated(keep=False).sum())

        def _duplicate_year_month_count(frame: pd.DataFrame) -> int:
            if frame is None or frame.empty or not {"年份", "月份"}.issubset(frame.columns):
                return 0
            temp = frame.copy()
            temp["年份"] = pd.to_numeric(temp["年份"], errors="coerce")
            temp["月份"] = temp["月份"].map(normalize_month)
            temp = temp[temp["年份"].eq(int(selected_year)) & temp["月份"].isin(MONTH_ORDER)]
            return int(temp.duplicated(["年份", "月份"], keep=False).sum())

        duplicate_calendar = _duplicate_year_month_count(tables.get("work_calendar", pd.DataFrame()))
        duplicate_adjustments = _duplicate_year_month_count(adjustments_df)
        duplicate_results = _duplicate_year_month_count(tables.get("capacity_results", pd.DataFrame()))
        mismatch_months = int(check["核對結果"].ne("一致").sum())
        exclusion_formula_errors = int(check["05排除公式差異"].abs().gt(1e-6).sum())
        total_formula_errors = int(check["04需求總公式差異"].abs().gt(1e-6).sum())
        summary = pd.DataFrame([
            {"檢查項目": "05月別機台數／原始／排除／排除後工時 vs 04", "結果": "通過" if mismatch_months == 0 else f"{mismatch_months} 個月份不一致"},
            {"檢查項目": "05公式：原始−排除=排除後", "結果": "通過" if exclusion_formula_errors == 0 else f"{exclusion_formula_errors} 個月份錯誤"},
            {"檢查項目": "04公式：排除後+調整=需求總", "結果": "通過" if total_formula_errors == 0 else f"{total_formula_errors} 個月份錯誤"},
            {"檢查項目": "05重複排程ID", "結果": "通過" if duplicate_schedule_ids == 0 else f"{duplicate_schedule_ids} 筆重複ID"},
            {"檢查項目": "07工作天數重複年月", "結果": "通過" if duplicate_calendar == 0 else f"{duplicate_calendar} 筆重複"},
            {"檢查項目": "04調整表重複年月", "結果": "通過" if duplicate_adjustments == 0 else f"{duplicate_adjustments} 筆重複"},
            {"檢查項目": "04結果表重複年月", "結果": "通過" if duplicate_results == 0 else f"{duplicate_results} 筆重複"},
            {"檢查項目": "標準工時缺漏", "結果": "通過" if int(missing_std_count) == 0 else f"{int(missing_std_count)} 筆待補"},
        ])
        st.dataframe(summary, use_container_width=True, hide_index=True)
        if mismatch_months == 0:
            st.success("目前 04 的月別機台數、原始需求工時、產能計算排除工時與排除後需求工時，已和 05 權威排程一致。")
        else:
            st.error("04 與 05 有月份不一致，請先回 05 儲存／匯入後，再按『重新計算產能並更新狀態燈』。")
        st.dataframe(check, use_container_width=True, hide_index=True, height=460)


if st.session_state.get("capacity_manual_save_message"):
    st.success(st.session_state.pop("capacity_manual_save_message"))

adjustments_source = load_table("capacity_adjustments")
selected_year = st.selectbox("顯示年份", years, index=len(years)-1 if years else 0, key="capacity_selected_year")
st.caption("04 程式來源版本：authority-source-v4；月份只取 schedule.json 權威排程。若看不到此行，表示仍在執行舊 #U 重複頁面。")
sync_schedule_machine_count = st.checkbox(
    "以05排程更新台數",
    value=True,
    key="capacity_recalc_sync_schedule_machine_count",
    help="勾選後，04 畫面與重新計算都會優先使用 05 排程表 J 欄/機台計數彙總；舊的 04 手動機台數只保留在權威檔，不再卡住目前畫面。",
)
calculation_adjustments_source = adjustments_source.copy() if adjustments_source is not None else pd.DataFrame()
if sync_schedule_machine_count:
    # Default to the official 05 schedule machine count for display and charts.
    # The old manual override is not deleted until the user presses the recalc/save button.
    calculation_adjustments_source = _clear_machine_count_overrides(calculation_adjustments_source, int(selected_year))
adjustments = _ensure_manual_adjustments(calculation_adjustments_source, int(selected_year))
capacity = calculate_capacity(tables["schedule"], tables["standard_hours"], tables["work_calendar"], tables["employees"], tables["dispatch"], params, adjustments, target_year=selected_year)
authority_month_summary = schedule_month_summary(tables.get("schedule", pd.DataFrame()), int(selected_year))
authority_months = active_schedule_months(tables.get("schedule", pd.DataFrame()), int(selected_year))
show_empty_months = st.checkbox(
    "顯示沒有 05 權威排程的空白月份",
    value=False,
    key=f"capacity_show_empty_months_{int(selected_year)}",
    help="預設關閉，因此 05 只有 1~6 月時，04 不會再顯示 7~12 月空白卡片。若權威月份稽核仍列出 7~12 月，代表 schedule.json 內仍有舊排程，請回 05 清理或在 10 使用完整同步匯入年度。",
)
month_options = list(MONTH_ORDER) if show_empty_months else list(authority_months)
month_state_key = f"capacity_visible_authority_months_v2_{int(selected_year)}_{'all' if show_empty_months else 'active'}"
existing_month_selection = st.session_state.get(month_state_key)
if isinstance(existing_month_selection, list):
    valid_existing = [month for month in existing_month_selection if month in month_options]
    if valid_existing != existing_month_selection:
        st.session_state[month_state_key] = valid_existing or month_options
months = st.multiselect(
    "顯示月份",
    month_options,
    default=month_options,
    key=month_state_key,
    help="月份選項直接來自 05 權威排程；查詢結果、舊 capacity_results 或 session cache 不會新增月份。",
)
capacity_view = capacity[capacity["月份"].isin(months)].copy() if months else capacity.iloc[0:0].copy()
if authority_months:
    st.caption(f"05 權威排程實際月份：{'、'.join(authority_months)}。04 計算來源為 schedule.json，不使用 05 畫面查詢暫存。")
else:
    st.warning(f"目前 {selected_year} 年的 05 權威排程沒有有效月份；04 不顯示月別狀態燈。")
if not authority_month_summary.empty:
    with st.expander("查看 05 權威排程月份來源", expanded=False):
        st.dataframe(authority_month_summary, use_container_width=True, hide_index=True, height=360)
        st.caption("若此表仍有 7~12 月，代表那些明細仍真實存在於 data/persistent/authority/schedule.json，並非 04 自己留下的紀錄。")

schedule_detail = prepare_schedule(
    tables["schedule"],
    tables["standard_hours"],
    target_year=selected_year,
    excluded_assembly_locations=params.get(ASSEMBLY_EXCLUSION_PARAM_KEY, []),
    excluded_categories=params.get(CATEGORY_EXCLUSION_PARAM_KEY, []),
    assembly_location_hours=params.get(ASSEMBLY_LOCATION_HOURS_PARAM_KEY, {}),
)
# 原始需求工時必須包含排除組立地點，因此所有排程（包含排除列）都要有標準工時。
_std_check_detail = schedule_detail.copy()
missing_std_count = int((pd.to_numeric(_std_check_detail.get("標準工時", pd.Series(dtype=float)), errors="coerce").fillna(0) <= 0).sum()) if not _std_check_detail.empty else 0
source_text = "需求總工時 = 05原始需求工時 − 06組立地點排除工時 + 04月別調整工時；Category排除只影響機台數。產能負荷、人力需求與缺工全部使用需求總工時；可用總人力=全部在職製造人力，直接有效人力=直接人力×可用比例"
st.markdown(f"""
<div class="tech-card subtle-card" style="margin: 10px 0 16px 0;">
  <b>計算來源：</b>{source_text}<br/>
  <span style="color:#8fdfff;">Excel 彙整表僅保留為原始參考，不再作為 04 主表資料來源。</span>
</div>
""", unsafe_allow_html=True)
if missing_std_count > 0:
    st.warning(f"目前 {selected_year} 年排程中有 {missing_std_count} 筆標準工時為 0 或缺漏（包含排除組立地點）。這會同時低估原始需求工時與產能計算排除工時，請到 06. 標準工時或 05. 排程表補齊。")

_render_capacity_reconciliation_diagnostics(
    schedule_detail,
    capacity,
    tables,
    adjustments_source,
    int(selected_year),
    missing_std_count,
)

b1, b2, b3 = st.columns([1.15, 1.55, 3.0])
with b1:
    st.caption("目前台數來源：05 排程表" if sync_schedule_machine_count else "目前台數來源：04 手動覆寫可套用")
with b2:
    if st.button("重新計算產能並更新狀態燈", type="primary", use_container_width=True):
        recalculation_adjustments = adjustments_source.copy() if adjustments_source is not None else pd.DataFrame()
        if sync_schedule_machine_count:
            recalculation_adjustments = _clear_machine_count_overrides(recalculation_adjustments, int(selected_year))
            save_authority_df("capacity_adjustments", recalculation_adjustments, user="capacity_recalculate_clear_machine_override")
        recalculated_capacity = _recalculate_and_persist_capacity(int(selected_year), _ensure_manual_adjustments(recalculation_adjustments, int(selected_year)), user="capacity_recalculate_from_page04")
        _cached_capacity_by_years.clear()
        st.session_state["capacity_manual_save_message"] = (
            f"已重新計算 {selected_year} 年月別產能；畫面預設只顯示 05 權威排程實際存在的月份，並更新 capacity_results。"
            + (" 每月機台數已改用 05 排程表彙總。" if sync_schedule_machine_count else "")
        )
        st.rerun()
with b3:
    if st.button("只套用目前畫面結果並永久保存", type="secondary", use_container_width=True):
        existing_results = load_table("capacity_results")
        saved = upsert_capacity_results(existing_results, capacity, target_year=selected_year)
        save_authority_df("capacity_results", saved, user="capacity_apply_current_view")
        clear_data_cache()
        _cached_capacity_by_years.clear()
        st.session_state["capacity_manual_save_message"] = f"已將 {selected_year} 年目前畫面計算結果永久保存到 capacity_results.json。"
        st.rerun()
    st.caption("若 05 排程表已更新，請按『重新計算產能並更新狀態燈』；勾選左側選項可清除舊的 04 手動台數覆寫，避免狀態燈仍顯示舊台數。")

max_ot_util = capacity_view["含加班稼動率"].max() if not capacity_view.empty else 0
max_normal_util = capacity_view["正常稼動率"].max() if not capacity_view.empty and "正常稼動率" in capacity_view.columns else 0
status = "red" if max_ot_util >= 1.1 else "orange" if max_ot_util >= 1 else "yellow" if max_ot_util >= 0.85 else "green"
normal_status = "red" if max_normal_util >= 1.1 else "orange" if max_normal_util >= 1 else "yellow" if max_normal_util >= 0.85 else "green"
s1, s2 = st.columns(2)
with s1:
    st.markdown(status_pill(f"{selected_year}年最高正常班產能負荷率：{max_normal_util:.0%}", normal_status), unsafe_allow_html=True)
with s2:
    st.markdown(status_pill(f"{selected_year}年最高含加班產能負荷率：{max_ot_util:.0%}", status), unsafe_allow_html=True)
st.subheader("月別科技狀態燈")


def _status_legend_html(params: dict) -> str:
    """Render capacity lamp color legend using the same thresholds as capacity_engine."""
    def _pct(value: object, default: float) -> str:
        try:
            return f"{float(value):.0%}"
        except Exception:
            return f"{default:.0%}"

    warning_text = _pct(params.get("warning_utilization", 0.85), 0.85)
    danger_text = _pct(params.get("danger_utilization", 1.0), 1.0)
    red_text = _pct(params.get("red_utilization", 1.1), 1.1)
    return f"""
<div class="tech-card subtle-card" style="margin: 6px 0 12px 0; padding: 12px 14px;">
  <div style="display:flex; align-items:center; justify-content:space-between; gap:14px; flex-wrap:wrap;">
    <div style="font-size:0.92rem; color:#9fb7d6; font-weight:800; letter-spacing:.04em;">燈號顏色說明｜依含加班產能負荷率判斷</div>
    <div style="font-size:0.84rem; color:#8fdfff;">綠燈 &lt; {warning_text}｜黃燈 {warning_text}~{danger_text}｜橘燈 {danger_text}~{red_text}｜紅燈 ≥ {red_text}</div>
  </div>
  <div style="display:grid; grid-template-columns:repeat(4, minmax(150px, 1fr)); gap:10px; margin-top:10px;">
    <div style="border:1px solid rgba(57,255,136,.34); background:rgba(57,255,136,.08); border-radius:14px; padding:10px 12px;">
      <div style="display:flex; align-items:center; gap:8px; font-weight:900; color:#39FF88;"><span class="spt-lamp spt-lamp-green" style="width:18px;height:18px;"></span>綠燈</div>
      <div style="margin-top:6px; color:#d8e7ff; font-size:0.84rem; line-height:1.45;">產能充足，含加班產能負荷率低於 {warning_text}。</div>
    </div>
    <div style="border:1px solid rgba(255,230,109,.34); background:rgba(255,230,109,.08); border-radius:14px; padding:10px 12px;">
      <div style="display:flex; align-items:center; gap:8px; font-weight:900; color:#FFE66D;"><span class="spt-lamp spt-lamp-yellow" style="width:18px;height:18px;"></span>黃燈</div>
      <div style="margin-top:6px; color:#d8e7ff; font-size:0.84rem; line-height:1.45;">產能偏滿，含加班產能負荷率達 {warning_text} 以上。</div>
    </div>
    <div style="border:1px solid rgba(255,181,71,.36); background:rgba(255,181,71,.09); border-radius:14px; padding:10px 12px;">
      <div style="display:flex; align-items:center; gap:8px; font-weight:900; color:#FFB547;"><span class="spt-lamp spt-lamp-orange" style="width:18px;height:18px;"></span>橘燈</div>
      <div style="margin-top:6px; color:#d8e7ff; font-size:0.84rem; line-height:1.45;">已超出含加班可用產能，產能負荷率達 {danger_text} 以上。</div>
    </div>
    <div style="border:1px solid rgba(255,75,110,.38); background:rgba(255,75,110,.09); border-radius:14px; padding:10px 12px;">
      <div style="display:flex; align-items:center; gap:8px; font-weight:900; color:#FF4B6E;"><span class="spt-lamp spt-lamp-red" style="width:18px;height:18px;"></span>紅燈</div>
      <div style="margin-top:6px; color:#d8e7ff; font-size:0.84rem; line-height:1.45;">嚴重超載，含加班產能負荷率達 {red_text} 以上，需優先處理。</div>
    </div>
  </div>
</div>
"""


st.markdown(_status_legend_html(params), unsafe_allow_html=True)

year_machine_count_total = (
    pd.to_numeric(
        capacity.loc[capacity["月份"].isin(authority_months), "每月機台數"] if authority_months and not capacity.empty else pd.Series(dtype=float),
        errors="coerce",
    ).fillna(0).sum()
)
visible_machine_count_total = pd.to_numeric(capacity_view.get("每月機台數", pd.Series(dtype=float)), errors="coerce").fillna(0).sum() if not capacity_view.empty else 0
machine_count_source_label = "05 排程表彙總" if sync_schedule_machine_count else "04 手動覆寫 / 目前畫面計算"
visible_month_note = ""
try:
    selected_month_count = len(capacity_view.index)
    full_month_count = len(capacity.index)
    if selected_month_count and selected_month_count != full_month_count:
        visible_month_note = f"｜目前顯示月份機台數：{visible_machine_count_total:,.0f} 台"
except Exception:
    visible_month_note = ""
st.markdown(f"""
<div class="tech-card subtle-card" style="margin: 8px 0 12px 0; padding: 14px 16px; display:flex; align-items:center; justify-content:space-between; gap:16px; flex-wrap:wrap;">
  <div>
    <div style="font-size:0.92rem;color:#9fb7d6;letter-spacing:.04em;">05權威排程月份總機台數</div>
    <div style="font-size:2.0rem;font-weight:900;color:#76D7FF;line-height:1.2;">{year_machine_count_total:,.0f} 台</div>
  </div>
  <div style="font-size:0.9rem;color:#c9d8f0;text-align:right;">
    台數來源：{machine_count_source_label}{visible_month_note}<br/>
    <span style="color:#8fdfff;">依目前 {selected_year} 年 05 排程表 / 04 調整後計算結果彙總。</span>
  </div>
</div>
""", unsafe_allow_html=True)
st.markdown(status_board_html(capacity_view), unsafe_allow_html=True)

chart_view = capacity_view.copy()
if "可用總人力" not in chart_view.columns and "直接有效人力" in chart_view.columns:
    chart_view["可用總人力"] = chart_view["直接有效人力"]
chart_view["正常產能負荷率%"] = (chart_view["正常稼動率"] * 100).round(0)
chart_view["含加班產能負荷率%"] = (chart_view["含加班稼動率"] * 100).round(0)
for _round_col in ["原始需求工時", "產能計算排除工時", "排除後需求工時", "需求總工時", "正常可用工時", "含加班可用工時", "正常產能負荷", "含加班產能負荷"]:
    if _round_col in chart_view.columns:
        chart_view[_round_col] = pd.to_numeric(chart_view[_round_col], errors="coerce").fillna(0).round(0)
if "正常產能負荷" in chart_view.columns:
    chart_view["正常產能餘額"] = chart_view["正常產能負荷"]
if "含加班產能負荷" in chart_view.columns:
    chart_view["含加班產能餘額"] = chart_view["含加班產能負荷"]


NEGATIVE_VALUE_COLOR = "#FFB86B"  # muted orange: visible but not too red
NEGATIVE_VALUE_COLOR_ALT = "#F59E0B"
ZERO_BASELINE_COLOR = "rgba(255,255,255,0.72)"


def _safe_trace_values(trace):
    try:
        orientation = str(getattr(trace, "orientation", None) or "v")
        values = trace.x if orientation == "h" else trace.y
    except Exception:
        return []
    if values is None:
        return []
    try:
        return pd.to_numeric(pd.Series(list(values)), errors="coerce").fillna(0).tolist()
    except Exception:
        return []


def _base_trace_color(trace, fallback: str = "#76D7FF") -> str:
    for attr_path in [("marker", "color"), ("line", "color")]:
        try:
            obj = getattr(trace, attr_path[0])
            color = getattr(obj, attr_path[1])
            if isinstance(color, str) and color.strip():
                return color
        except Exception:
            pass
    return fallback


def _signed_color_list(values, positive: str, negative: str = NEGATIVE_VALUE_COLOR) -> list[str]:
    safe_values = pd.to_numeric(pd.Series(list(values or [])), errors="coerce").fillna(0).tolist()
    return [negative if float(v) < 0 else positive for v in safe_values]


def _negative_text_color_list(values) -> list[str]:
    safe_values = pd.to_numeric(pd.Series(list(values or [])), errors="coerce").fillna(0).tolist()
    return [NEGATIVE_VALUE_COLOR if float(v) < 0 else "#FFFFFF" for v in safe_values]


def _apply_negative_value_chart_style(fig, *, negative: str = NEGATIVE_VALUE_COLOR):
    """Color negative bars/markers and value labels with a warm warning color."""
    for trace in fig.data:
        vals = _safe_trace_values(trace)
        if not vals:
            continue
        positive = _base_trace_color(trace)
        if trace.type in {"bar", "histogram"}:
            try:
                trace.marker.color = _signed_color_list(vals, positive=positive, negative=negative)
                trace.marker.line.color = "rgba(255,255,255,0.34)"
                trace.marker.line.width = 1.25
                trace.textfont.color = _negative_text_color_list(vals)
                trace.outsidetextfont.color = _negative_text_color_list(vals)
                trace.insidetextfont.color = _negative_text_color_list(vals)
            except Exception:
                pass
        elif trace.type in {"scatter", "scattergl"}:
            try:
                trace.marker.color = _signed_color_list(vals, positive=positive, negative=negative)
                trace.marker.line.color = "rgba(255,255,255,0.68)"
                trace.textfont.color = _negative_text_color_list(vals)
            except Exception:
                pass
    return fig


def _add_zero_baseline(fig, *, yref: str = "y"):
    """Add a clear zero line without changing the chart theme."""
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
    """Highlight negative numeric cells while keeping the table data unchanged."""
    try:
        return df.style.map(_negative_cell_css)
    except Exception:
        return df.style.applymap(_negative_cell_css)
c1, c2 = st.columns([1.2, 1])
with c1:
    fig = px.bar(chart_view, x="月份", y=["原始需求工時", "需求總工時", "正常可用工時", "含加班可用工時"], barmode="group", title="原始／需求總工時 vs 正常／含加班可用工時（h）")
    for trace in fig.data:
        if trace.name == "原始需求工時":
            trace.marker.color = "#64748B"
            trace.marker.opacity = 0.58
        elif trace.name == "需求總工時":
            trace.marker.color = "#C084FC"
            trace.marker.opacity = 0.82
        elif trace.name == "正常可用工時":
            trace.marker.color = "#7DD3FC"
            trace.marker.opacity = 0.88
        elif trace.name == "含加班可用工時":
            trace.marker.color = "#60A5FA"
            trace.marker.opacity = 0.88
    fig = style_powerbi_figure(fig, height=440, legend_title="指標", yaxis_title="工時(h)")
    _add_zero_baseline(fig)
    _apply_negative_value_chart_style(fig, negative=NEGATIVE_VALUE_COLOR)
    render_powerbi_chart(fig, key="capacity_hours_chart")
with c2:
    fig2 = make_subplots(specs=[[{"secondary_y": True}]])
    fig2.add_trace(go.Scatter(x=chart_view["月份"], y=chart_view["正常產能負荷率%"].round(0), mode="lines+markers", name="正常產能負荷率%"), secondary_y=False)
    fig2.add_trace(go.Scatter(x=chart_view["月份"], y=chart_view["含加班產能負荷率%"].round(0), mode="lines+markers", name="含加班產能負荷率%"), secondary_y=False)
    fig2.add_trace(go.Bar(x=chart_view["月份"], y=chart_view["需求總工時"].round(0), name="需求總工時(h)", marker_color="#A855F7", opacity=0.58), secondary_y=True)
    fig2.update_layout(title="正常班 vs 含加班產能負荷率 + 需求總工時")
    fig2.update_yaxes(title_text="產能負荷率(%)", secondary_y=False, tickformat=",.0f")
    fig2.update_yaxes(title_text="需求總工時(h)", secondary_y=True, tickformat=",.0f")
    fig2.add_hline(y=85, line_dash="dash", annotation_text="85%")
    fig2.add_hline(y=100, line_dash="dash", annotation_text="100%")
    fig2 = style_powerbi_figure(fig2, height=440, legend_title="指標")
    _add_zero_baseline(fig2, yref="y")
    _add_zero_baseline(fig2, yref="y2")
    _apply_negative_value_chart_style(fig2, negative=NEGATIVE_VALUE_COLOR)
    render_powerbi_chart(fig2, key="capacity_util_chart")

load_fig = px.bar(chart_view, x="月份", y=["正常產能餘額", "含加班產能餘額"], barmode="group", title="正常班 vs 含加班產能餘額（h）")
load_fig = style_powerbi_figure(load_fig, height=390, yaxis_title="產能餘額(h)", legend_title="指標")
_add_zero_baseline(load_fig)
_apply_negative_value_chart_style(load_fig, negative=NEGATIVE_VALUE_COLOR)
render_powerbi_chart(load_fig, key="capacity_load_chart")

manpower_fig = make_subplots(specs=[[{"secondary_y": True}]])
if {"需求人力", "可用總人力", "直接有效人力", "人力差異"}.issubset(set(chart_view.columns)):
    manpower_fig.add_trace(
        go.Bar(x=chart_view["月份"], y=chart_view["需求人力"].round(0), name="需求人力"),
        secondary_y=False,
    )
    manpower_fig.add_trace(
        go.Bar(x=chart_view["月份"], y=chart_view["可用總人力"].round(0), name="可用總人力（全部製造人力）", opacity=0.45),
        secondary_y=False,
    )
    manpower_fig.add_trace(
        go.Bar(x=chart_view["月份"], y=chart_view["直接有效人力"].round(1), name="直接有效人力（直接×可用比例）"),
        secondary_y=False,
    )
    manpower_fig.add_trace(
        go.Scatter(x=chart_view["月份"], y=chart_view["人力差異"].round(0), mode="lines+markers", name="人力差異"),
        secondary_y=True,
    )
    manpower_fig.update_layout(title="可用總人力 vs 直接有效人力 vs 需求人力 + 人力差異")
    manpower_fig.update_yaxes(title_text="人力(人)", secondary_y=False, tickformat=",.0f")
    manpower_fig.update_yaxes(title_text="人力差異(人)", secondary_y=True, tickformat=",.0f")
    manpower_fig = style_powerbi_figure(manpower_fig, height=390, legend_title="指標")
    _add_zero_baseline(manpower_fig, yref="y")
    _add_zero_baseline(manpower_fig, yref="y2")
    _apply_negative_value_chart_style(manpower_fig, negative=NEGATIVE_VALUE_COLOR)
    render_powerbi_chart(manpower_fig, key="capacity_manpower_gap_chart")

if "人力差異" in chart_view.columns:
    gap_fig = px.bar(chart_view, x="月份", y="人力差異", title="每月人力差異：正數為多出、負數為缺少")
    gap_fig = style_powerbi_figure(gap_fig, height=360, yaxis_title="人力差異(人)", legend_title="指標")
    _add_zero_baseline(gap_fig)
    _apply_negative_value_chart_style(gap_fig, negative=NEGATIVE_VALUE_COLOR)
    render_powerbi_chart(gap_fig, key="capacity_monthly_manpower_gap_chart")

cols = ["年份", "月份", "每月機台數", "排程彙總機台數", "手動覆寫機台數", "每月機台數來源", "人力計算來源", "正常工作日", "可用總人力", "直接有效人力", "請假比例", "請假扣除人力", "扣請假後有效人力", "原始需求工時", "產能計算排除工時", "排除後需求工時", "調整工時", "需求總工時", "正常可用工時", "含加班可用工時", "正常稼動率", "含加班稼動率", "正常產能負荷", "含加班產能負荷", "需求人力", "人力差異", "缺工天數", "狀態"]
existing = [c for c in cols if c in capacity_view.columns]
unit_columns = {
    "每月機台數": "每月機台數(台)",
    "排程彙總機台數": "排程彙總機台數(台)",
    "手動覆寫機台數": "手動覆寫機台數(台)",
    "正常工作日": "正常工作日(天)",
    "可用總人力": "可用總人力(人)",
    "直接有效人力": "直接有效人力(人)",
    "請假比例": "請假比例(%)",
    "請假扣除人力": "請假扣除人力(人)",
    "扣請假後有效人力": "扣請假後有效人力(人)",
    "原始需求工時": "原始需求工時(h)",
    "產能計算排除工時": "產能計算排除工時(h)",
    "排除後需求工時": "排除後需求工時(h)",
    "調整工時": "調整工時(h)",
    "需求總工時": "需求總工時(h)",
    "正常可用工時": "正常可用工時(h)",
    "含加班可用工時": "含加班可用工時(h)",
    "正常稼動率": "正常產能負荷率(%)",
    "含加班稼動率": "含加班產能負荷率(%)",
    "正常產能負荷": "正常產能餘額(h)",
    "含加班產能負荷": "含加班產能餘額(h)",
    "需求人力": "需求人力(人)",
    "人力差異": "人力差異(人)",
    "缺工天數": "缺工天數(天)",
}
capacity_table = capacity_view[existing].copy()
for pct_col in ["正常稼動率", "含加班稼動率", "請假比例"]:
    if pct_col in capacity_table.columns:
        capacity_table[pct_col] = capacity_table[pct_col] * 100
capacity_table = capacity_table.rename(columns=unit_columns)
capacity_table = _display_number_table(capacity_table)
# 人力相容欄位仍保留給舊資料/舊報表使用，但顯示為整數，避免 43.000000 這類長小數影響閱讀。
if "可用總人力(人)" in capacity_table.columns:
    capacity_table["可用總人力(人)"] = pd.to_numeric(capacity_table["可用總人力(人)"], errors="coerce").round(0)
for _manpower_decimal_col in ["直接有效人力(人)", "扣請假後有效人力(人)", "請假扣除人力(人)"]:
    if _manpower_decimal_col in capacity_table.columns:
        capacity_table[_manpower_decimal_col] = pd.to_numeric(capacity_table[_manpower_decimal_col], errors="coerce").round(1)

st.subheader("04. 產能負荷可編輯計算表")
st.caption("可手動修改『每月機台數(台)』『正常工作日(天)』『調整工時(h)』。需求總工時 = 排除後需求工時 + 調整工時；所有產能評估皆使用需求總工時。")
editable_columns = ["每月機台數(台)", "正常工作日(天)", "調整工時(h)"]
disabled_columns = [c for c in capacity_table.columns if c not in editable_columns]
capacity_table_display = format_numeric_editor_display_dataframe(capacity_table, editable_columns=set(editable_columns))
with st.form("capacity_main_table_edit_form"):
    edited_capacity_table = st.data_editor(
        _style_negative_values(capacity_table_display),
        use_container_width=True,
        height=430,
        hide_index=True,
        disabled=disabled_columns,
        column_config={
            "每月機台數(台)": st.column_config.NumberColumn("每月機台數(台)", min_value=0, step=1, format="%d", help="管理用月別機台數；若等於排程彙總機台數，儲存時不寫成手動覆寫。"),
            "排程彙總機台數(台)": st.column_config.NumberColumn("排程彙總機台數(台)", min_value=0, step=1, format="%d", help="來自 05 排程表 J 欄/機台計數彙總。"),
            "手動覆寫機台數(台)": st.column_config.NumberColumn("手動覆寫機台數(台)", min_value=0, step=1, format="%d", help="04 曾手動覆寫才會有值。"),
            "正常工作日(天)": st.column_config.NumberColumn("正常工作日(天)", min_value=0, max_value=31, step=1, format="%d", help="修改後會依 08 參數重新計算正常可用工時與平日加班工時。"),
            "調整工時(h)": st.column_config.NumberColumn("調整工時(h)", step=0.1, format="%g", help="需求總工時 = 排除後需求工時 + 調整工時。可填正數或負數。"),
            "原始需求工時(h)": st.column_config.NumberColumn("原始需求工時(h)", format="%.1f", help="05 台數 × 06 標準工時，尚未扣除排除組立地點。"),
            "產能計算排除工時(h)": st.column_config.NumberColumn("產能計算排除工時(h)", format="%.1f", help="06 設定為產能計算排除組立地點後，自原始需求工時扣除的工時。"),
            "排除後需求工時(h)": st.column_config.NumberColumn("排除後需求工時(h)", format="%.1f", help="原始需求工時 − 產能計算排除工時。"),
            "直接有效人力(人)": st.column_config.NumberColumn("直接有效人力(人)", format="%.1f", help="01/02 中是否直接人力=是的人員，依可用比例換算後加總；04/09 的產能與人力差異以此欄為基準。"),
            "可用總人力(人)": st.column_config.NumberColumn("可用總人力(人)", format="%.0f", help="目前啟用且在職的全部製造人力，不區分直接/間接，也不乘可用比例。"),
            "扣請假後有效人力(人)": st.column_config.NumberColumn("扣請假後有效人力(人)", format="%.1f"),
            "請假扣除人力(人)": st.column_config.NumberColumn("請假扣除人力(人)", format="%.1f"),
        },
        key=f"capacity_edit_table_{selected_year}",
    )
    save_table = st.form_submit_button("儲存手動修正並重新計算", type="primary", use_container_width=True)

if save_table:
    if hasattr(edited_capacity_table, "data"):
        edited_capacity_table = edited_capacity_table.data
    update_rows = _machine_override_save_rows(edited_capacity_table)
    updated_adjustments = _upsert_manual_adjustments(adjustments_source, update_rows, int(selected_year))
    save_authority_df("capacity_adjustments", updated_adjustments, user="capacity_manual_table_save")
    recalculated_adjustments = _ensure_manual_adjustments(updated_adjustments, int(selected_year))
    _recalculate_and_persist_capacity(
        int(selected_year),
        recalculated_adjustments,
        user="capacity_manual_table_recalculate",
    )
    _cached_capacity_by_years.clear()
    st.session_state["capacity_manual_save_message"] = f"已儲存 {selected_year} 年手動修正，並重新計算 12 個月份產能結果。"
    st.rerun()

capacity_export = capacity_table.copy()
st.subheader("多年度比較")
multi_year_adjustments_source = (
    _clear_machine_count_overrides(adjustments_source, None)
    if sync_schedule_machine_count
    else adjustments_source
)
capacity_years = _cached_capacity_by_years(
    tables["schedule"],
    tables["standard_hours"],
    tables["work_calendar"],
    tables["employees"],
    tables["dispatch"],
    params,
    multi_year_adjustments_source,
    tuple(int(y) for y in years),
)
if not capacity_years.empty and capacity_years["年份"].nunique() > 1:
    year_summary = capacity_years.groupby("年份", as_index=False).agg(年度機台數=("每月機台數", "sum"), 年度原始需求工時=("原始需求工時", "sum"), 年度排除工時=("產能計算排除工時", "sum"), 年度需求總工時=("需求總工時", "sum"), 年度可用工時=("含加班可用工時", "sum"), 年度缺工天數=("缺工天數", "sum"), 平均產能負荷率百分比=("含加班稼動率", "mean"))
    year_summary["平均產能負荷率(%)"] = (year_summary["平均產能負荷率百分比"] * 100).round(0)
    year_summary = year_summary.drop(columns=["平均產能負荷率百分比"], errors="ignore")
    for _col in ["年度機台數", "年度原始需求工時", "年度排除工時", "年度需求總工時", "年度可用工時", "年度缺工天數", "平均產能負荷率(%)"]:
        if _col in year_summary.columns:
            year_summary[_col] = pd.to_numeric(year_summary[_col], errors="coerce").round(0).astype("Int64")
    fig_year = px.bar(year_summary, x="年份", y=["年度原始需求工時", "年度需求總工時", "年度可用工時"], barmode="group", title="年度原始／需求總工時 vs 可用工時")
    for trace in fig_year.data:
        trace.marker.color = "#64748B" if trace.name == "年度原始需求工時" else "#C084FC" if trace.name == "年度需求總工時" else "#7DD3FC"
        trace.marker.opacity = 0.86
    fig_year = style_powerbi_figure(fig_year, height=390, legend_title="指標", yaxis_title="工時(h)")
    _add_zero_baseline(fig_year)
    _apply_negative_value_chart_style(fig_year, negative=NEGATIVE_VALUE_COLOR)
    render_powerbi_chart(fig_year, key="capacity_year_compare")
    render_configurable_view(year_summary, "capacity_year_summary", "04. 多年度產能摘要", height=320)
else:
    year_summary = pd.DataFrame()
    st.caption("目前只有一個年度資料；匯入 2024、2025 等年份後，這裡會自動顯示年度比較。")

st.subheader("04. 模組完整匯出")
render_module_report_download(
    "04.產能負荷表",
    {"產能負荷表_系統計算": capacity_export, "多年度產能摘要": year_summary, "排程需求明細": schedule_detail, "月別調整工時": adjustments, "Excel原始彙整參考": tables.get("capacity_summary_excel", pd.DataFrame()), "原始排程": tables["schedule"], "工作天數": tables["work_calendar"]},
    chart_specs=[
        chart_spec("bar", "原始／需求總工時與可用工時月趨勢（h）", "產能負荷表_系統計算", "月份", ["原始需求工時(h)", "需求總工時(h)", "正常可用工時(h)", "含加班可用工時(h)"]),
        chart_spec("line", "正常班與含加班產能負荷率趨勢（%）", "產能負荷表_系統計算", "月份", ["正常產能負荷率(%)", "含加班產能負荷率(%)"]),
        chart_spec("bar", "正常班與含加班產能餘額（h）", "產能負荷表_系統計算", "月份", ["正常產能餘額(h)", "含加班產能餘額(h)"]),
        chart_spec("bar", "月別人力差異（人）", "產能負荷表_系統計算", "月份", ["人力差異(人)"]),
        chart_spec("bar", "總人力、直接有效人力與需求人力（人）", "產能負荷表_系統計算", "月份", ["可用總人力(人)", "直接有效人力(人)", "需求人力(人)"]),
    ],
    metadata={"模組": "04. 產能負荷表", "顯示年份": selected_year, "需求總工時公式": "排除後需求工時 + 04月別調整工時", "產能評估口徑": "全部使用需求總工時", "最高正常班產能負荷率": f"{max_normal_util:.0%}", "最高含加班產能負荷率": f"{max_ot_util:.0%}"},
    key="export_capacity_module",
)

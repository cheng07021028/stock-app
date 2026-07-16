from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from services.capacity_engine import (
    ASSEMBLY_EXCLUSION_PARAM_KEY,
    ASSEMBLY_LOCATION_HOURS_PARAM_KEY,
    CATEGORY_EXCLUSION_PARAM_KEY,
    calculate_capacity_by_years,
    normalize_assembly_location,
    normalize_assembly_location_hours_map,
    normalize_assembly_location_list,
    normalize_category,
    normalize_category_list,
    recalculate_schedule_demand,
    schedule_assembly_exclusion_mask,
    schedule_capacity_exclusion_mask,
    schedule_category_exclusion_mask,
    upsert_capacity_results,
)
from services.data_loader import clear_data_cache, load_table
from services.page_utils import render_configurable_view, render_multi_sheet_download, render_saveable_table
from services.persistent_store import load_parameters, save_authority_df, save_parameters
from services.ui_theme import apply_tech_theme, render_hero, render_human_help
from services.year_service import available_years_from_frames, filter_by_year

st.set_page_config(page_title="06. 標準工時", page_icon="⏱️", layout="wide")
apply_tech_theme()
render_hero("06. 標準工時", "客戶、P/N、Type、Category、標準工時、標準天數與場地格位數主檔，可在系統內版本化維護。")
render_human_help([
    "標準工時是需求總工時的核心，建議修改後立即儲存並建立版本快照。",
    "若排程找不到工時，請在這頁新增對應客戶/P/N/Type/Category。",
    "若某些組立地點屬於外場或外包商，可在本頁設定為產能計算排除：原始需求工時仍完整保留，排除工時會從需求總工時扣除，機台台數仍保留。",
    "若某些 Category 不需要計算機台台數，可在本頁勾選排除台數；排除後機台計數歸 0，但需求工時仍照常計算。",
    "可匯出 Excel 給工程或生管複核，再回系統更新權威資料。",
    "新增『格位數』欄位：代表該 P/N / Type 在場地週轉時會佔用幾個格位，12. 場地週轉模組會優先引用。",
])


def _assembly_location_options(schedule_df: pd.DataFrame) -> list[str]:
    if schedule_df is None or schedule_df.empty or "組立地點" not in schedule_df.columns:
        return []
    values = [normalize_assembly_location(value) for value in schedule_df["組立地點"].tolist()]
    return sorted({value for value in values if value})


def _category_options(schedule_df: pd.DataFrame, standard_df: pd.DataFrame) -> list[str]:
    values: list[str] = []
    for df in [standard_df, schedule_df]:
        if df is None or df.empty or "Category" not in df.columns:
            continue
        values.extend(normalize_category(value) for value in df["Category"].tolist())
    return sorted({value for value in values if value})


def _format_location_label(value: str) -> str:
    text = normalize_assembly_location(value)
    return text.replace("\n", " / ") if text else "未設定"


def _format_category_label(value: str) -> str:
    return normalize_category(value) or "未分類"


def _build_assembly_hours_editor(selected_locations: list[str], current_hours: dict[str, float]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for location in normalize_assembly_location_list(selected_locations):
        rows.append({
            "組立地點": location,
            "顯示名稱": _format_location_label(location),
            "調整工時/台": float(current_hours.get(location, 0.0) or 0.0),
        })
    return pd.DataFrame(rows, columns=["組立地點", "顯示名稱", "調整工時/台"])


def _editor_to_assembly_hours_map(editor_df: pd.DataFrame, selected_locations: list[str]) -> dict[str, float]:
    selected = normalize_assembly_location_list(selected_locations)
    if editor_df is None or editor_df.empty:
        return {location: 0.0 for location in selected}
    raw_map: dict[str, float] = {}
    for _, row in editor_df.iterrows():
        location = normalize_assembly_location(row.get("組立地點"))
        if not location:
            location = normalize_assembly_location(row.get("顯示名稱"))
        if not location or location not in selected:
            continue
        try:
            hours = float(row.get("調整工時/台") or 0)
        except Exception:
            hours = 0.0
        raw_map[location] = max(hours, 0.0)
    return {location: raw_map.get(location, 0.0) for location in selected}


def _recalculate_after_exclusion_save(
    *,
    selected_year: int,
    excluded_locations: list[str],
    excluded_categories: list[str],
    assembly_location_hours: dict[str, float],
    schedule_df: pd.DataFrame,
    standard_df: pd.DataFrame,
    params: dict,
    user: str,
) -> dict[str, int]:
    # 排除設定是全系統規則，不只影響目前頁面年份；按下保存後一次重算
    # 所有既有年度，避免 04/首頁/09 讀到不同年度的新舊混合結果。
    recalculated_schedule = recalculate_schedule_demand(
        schedule_df,
        standard_hours=standard_df,
        target_year=None,
        excluded_assembly_locations=excluded_locations,
        excluded_categories=excluded_categories,
        assembly_location_hours=assembly_location_hours,
    )
    save_authority_df("schedule", recalculated_schedule, user=user)

    work_calendar = load_table("work_calendar")
    employees = load_table("employees")
    dispatch = load_table("dispatch")
    adjustments = load_table("capacity_adjustments")
    existing_results = load_table("capacity_results")
    years_to_recalculate = available_years_from_frames([recalculated_schedule, standard_df, work_calendar])
    calculated_capacity = calculate_capacity_by_years(
        recalculated_schedule,
        standard_df,
        work_calendar,
        employees,
        dispatch,
        params,
        adjustments=adjustments,
        years=years_to_recalculate,
    )
    saved_results = upsert_capacity_results(existing_results, calculated_capacity)
    save_authority_df("capacity_results", saved_results, user=user)
    clear_data_cache()

    year_schedule = filter_by_year(recalculated_schedule, int(selected_year))
    assembly_mask = schedule_assembly_exclusion_mask(year_schedule, excluded_locations)
    category_mask = schedule_category_exclusion_mask(year_schedule, excluded_categories)
    combined_mask = schedule_capacity_exclusion_mask(year_schedule, excluded_locations, excluded_categories)
    return {
        "schedule_rows": int(len(year_schedule)),
        "excluded_rows": int(combined_mask.sum()) if not combined_mask.empty else 0,
        "assembly_excluded_rows": int(assembly_mask.sum()) if not assembly_mask.empty else 0,
        "category_excluded_rows": int(category_mask.sum()) if not category_mask.empty else 0,
        "capacity_months": int(len(calculated_capacity)),
        "years": int(len(years_to_recalculate)),
    }


schedule = load_table("schedule")
std_source_for_years = load_table("standard_hours")
years = available_years_from_frames([std_source_for_years, schedule])
selected_year = st.selectbox("顯示/編輯年份", years, index=len(years)-1, key="standard_year_filter")
st.caption(f"目前 06 標準工時表格只顯示 {selected_year} 年資料；新增空白列會自動帶入 {selected_year}，儲存時仍會保留其他年度資料。")
std = render_saveable_table(
    "standard_hours",
    "06. 標準工時",
    height=540,
    helper_text="系統可自主維護標準工時，不必依賴 Excel 作為唯一資料來源；請維護年份以支援多年度比較。",
    row_filter_column="年份",
    row_filter_value=selected_year,
    new_row_defaults={"年份": int(selected_year), "格位數": 1, "是否啟用": "是"},
    row_filter_label=f"{selected_year} 年標準工時",
)
params = load_parameters()
std_year = filter_by_year(std, selected_year)
schedule_year = filter_by_year(schedule, selected_year)

if "格位數" in std_year.columns:
    slot_values = pd.to_numeric(std_year["格位數"], errors="coerce")
    valid_slot_rows = int(slot_values.fillna(0).gt(0).sum())
    missing_slot_rows = int(slot_values.isna().sum() + slot_values.fillna(0).le(0).sum())
    avg_slot = round(float(slot_values[slot_values.gt(0)].mean()), 2) if valid_slot_rows else 0
    m1, m2, m3 = st.columns(3)
    m1.metric("已維護格位數筆數", valid_slot_rows)
    m2.metric("未維護格位數筆數", missing_slot_rows)
    m3.metric("平均格位數", avg_slot)
    st.caption("格位數只提供 12. 超慧科技場地週轉預排使用，不會改變 05 需求工時與 04 產能負荷計算。")

current_excluded = normalize_assembly_location_list(params.get(ASSEMBLY_EXCLUSION_PARAM_KEY, []))
current_excluded_categories = normalize_category_list(params.get(CATEGORY_EXCLUSION_PARAM_KEY, []))
current_assembly_location_hours = normalize_assembly_location_hours_map(params.get(ASSEMBLY_LOCATION_HOURS_PARAM_KEY, {}))

st.subheader("產能計算排除的組立地點")
all_options = _assembly_location_options(schedule)
option_set = set(all_options)
for value in current_excluded:
    if value not in option_set:
        all_options.append(value)
all_options = sorted(all_options)
selected_excluded = st.multiselect(
    "選擇不計算標準工時、但仍計數台數的組立地點",
    all_options,
    default=[value for value in current_excluded if value in all_options],
    format_func=_format_location_label,
    key="assembly_location_exclusion_selector",
    help="適用於外場或外包商組立地點。儲存後，原始需求工時仍以台數 × 標準工時計算；命中地點的原始工時會列為產能計算排除工時，排除後需求工時歸 0，機台計數仍保留。",
)

with st.form("assembly_location_exclusion_form", clear_on_submit=False):
    normalized_selected_preview = normalize_assembly_location_list(selected_excluded)
    st.caption("『調整工時/台』為舊版相容／備查欄位，會保存並顯示，但不再加回 04、09、首頁的產能需求。正式口徑固定為：需求總工時 = 原始需求工時 − 組立地點排除工時 + 04 月別調整工時。")
    if normalized_selected_preview:
        hours_editor = st.data_editor(
            _build_assembly_hours_editor(normalized_selected_preview, current_assembly_location_hours),
            use_container_width=True,
            hide_index=True,
            key="assembly_location_hours_editor",
            disabled=["組立地點", "顯示名稱"],
            column_config={
                "組立地點": st.column_config.TextColumn("組立地點", help="系統比對用，不可修改。"),
                "顯示名稱": st.column_config.TextColumn("顯示名稱"),
                "調整工時/台": st.column_config.NumberColumn("調整工時/台", min_value=0.0, step=0.5, format="%.2f"),
            },
        )
    else:
        hours_editor = pd.DataFrame(columns=["組立地點", "顯示名稱", "調整工時/台"])
        st.info("請先選擇要從需求總工時扣除的組立地點；調整工時/台僅保留為備查。", icon="ℹ️")
    c1, c2 = st.columns([1.2, 3.8])
    with c1:
        save_location_exclusion = st.form_submit_button("儲存組立地點排除設定並重新計算", type="primary", use_container_width=True)
    with c2:
        st.info("此區採保存模式；選取或輸入不會立即寫入權威資料，也不會立即重算，只有按左側按鈕才會保存並同步 05/04。", icon="✅")

if save_location_exclusion:
    normalized_selected = normalize_assembly_location_list(selected_excluded)
    normalized_hours_map = _editor_to_assembly_hours_map(hours_editor, normalized_selected)
    updated_params = dict(params)
    updated_params[ASSEMBLY_EXCLUSION_PARAM_KEY] = normalized_selected
    updated_params[ASSEMBLY_LOCATION_HOURS_PARAM_KEY] = normalized_hours_map
    updated_params[CATEGORY_EXCLUSION_PARAM_KEY] = current_excluded_categories
    save_parameters(updated_params, user="standard_hours_assembly_location_exclusion")
    result = _recalculate_after_exclusion_save(
        selected_year=int(selected_year),
        excluded_locations=normalized_selected,
        excluded_categories=current_excluded_categories,
        assembly_location_hours=normalized_hours_map,
        schedule_df=schedule,
        standard_df=std,
        params=updated_params,
        user="standard_hours_assembly_location_exclusion",
    )
    st.success(
        f"已保存組立地點排除設定，並重算所有年度 05 排程與 04 產能結果。"
        f"目前頁面年度 {selected_year} 年排程 {result['schedule_rows']:,} 筆，排除規則命中 {result['excluded_rows']:,} 筆；"
        f"其中組立地點排除 {result['assembly_excluded_rows']:,} 筆，Category 排除 {result['category_excluded_rows']:,} 筆；"
        f"04 已更新 {result['years']:,} 個年度、{result['capacity_months']:,} 個月份。"
    )
    st.rerun()

st.subheader("產能計算排除的 Category")
category_options = _category_options(schedule, std)
category_set = set(category_options)
for value in current_excluded_categories:
    if value not in category_set:
        category_options.append(value)
category_options = sorted(category_options)
with st.form("category_exclusion_form", clear_on_submit=False):
    selected_excluded_categories = st.multiselect(
        "選擇不計算台數、但仍計算工時的 Category",
        category_options,
        default=[value for value in current_excluded_categories if value in category_options],
        format_func=_format_category_label,
        help="適用於不需納入機台台數統計的 Category。儲存後，這些 Category 的排程明細仍保留，機台計數會歸 0，但需求工時仍照常計算並納入 04/09/首頁。",
    )
    c1, c2 = st.columns([1.2, 3.8])
    with c1:
        save_category_exclusion = st.form_submit_button("儲存 Category 排除並重新計算", type="primary", use_container_width=True)
    with c2:
        st.info("此區採表單模式；勾選時不會立即寫入或重算，只有按左側按鈕才會保存並同步 05/04。", icon="✅")

if save_category_exclusion:
    normalized_categories = normalize_category_list(selected_excluded_categories)
    updated_params = dict(params)
    updated_params[ASSEMBLY_EXCLUSION_PARAM_KEY] = current_excluded
    updated_params[ASSEMBLY_LOCATION_HOURS_PARAM_KEY] = current_assembly_location_hours
    updated_params[CATEGORY_EXCLUSION_PARAM_KEY] = normalized_categories
    save_parameters(updated_params, user="standard_hours_category_exclusion")
    result = _recalculate_after_exclusion_save(
        selected_year=int(selected_year),
        excluded_locations=current_excluded,
        excluded_categories=normalized_categories,
        assembly_location_hours=current_assembly_location_hours,
        schedule_df=schedule,
        standard_df=std,
        params=updated_params,
        user="standard_hours_category_exclusion",
    )
    st.success(
        f"已保存 Category 排除設定，並重算所有年度 05 排程與 04 產能結果。"
        f"目前頁面年度 {selected_year} 年排程 {result['schedule_rows']:,} 筆，排除規則命中 {result['excluded_rows']:,} 筆；"
        f"其中組立地點排除 {result['assembly_excluded_rows']:,} 筆，Category 排除 {result['category_excluded_rows']:,} 筆；"
        f"04 已更新 {result['years']:,} 個年度、{result['capacity_months']:,} 個月份。"
    )
    st.rerun()

assembly_mask_year = schedule_assembly_exclusion_mask(schedule_year, current_excluded)
category_mask_year = schedule_category_exclusion_mask(schedule_year, current_excluded_categories)
combined_mask_year = schedule_capacity_exclusion_mask(schedule_year, current_excluded, current_excluded_categories)
assembly_detail = schedule_year.loc[assembly_mask_year].copy() if not assembly_mask_year.empty and assembly_mask_year.any() else pd.DataFrame()
category_detail = schedule_year.loc[category_mask_year].copy() if not category_mask_year.empty and category_mask_year.any() else pd.DataFrame()
excluded_detail = schedule_year.loc[combined_mask_year].copy() if not combined_mask_year.empty and combined_mask_year.any() else pd.DataFrame()
summary_cols = st.columns(5)
summary_cols[0].metric("目前排除地點數", f"{len(current_excluded):,}")
summary_cols[1].metric("備查調整工時設定", f"{sum(1 for value in current_assembly_location_hours.values() if float(value or 0) > 0):,}", help="舊版相容欄位，不納入 04/09/首頁需求總工時。")
summary_cols[2].metric(f"{selected_year}年地點排除筆數", f"{len(assembly_detail):,}")
summary_cols[3].metric("目前排除 Category 數", f"{len(current_excluded_categories):,}")
summary_cols[4].metric(f"{selected_year}年Category排除筆數", f"{len(category_detail):,}")
if current_excluded:
    st.caption("目前排除組立地點：" + "、".join(
        f"{_format_location_label(value)}（備查 {float(current_assembly_location_hours.get(value, 0.0) or 0.0):.2f}h/台，不納入產能）"
        for value in current_excluded
    ))
else:
    st.caption("目前未設定排除組立地點。")
if current_excluded_categories:
    st.caption("目前排除 Category：" + "、".join(_format_category_label(value) for value in current_excluded_categories))
else:
    st.caption("目前未設定排除 Category。")
if not excluded_detail.empty:
    render_configurable_view(excluded_detail, "standard_hours_excluded_schedule", "06. 已套用排除規則的排程明細", height=300)

st.subheader("標準工時分析")
analysis_tables: dict[str, pd.DataFrame] = {"標準工時": std, f"{selected_year}年標準工時": std_year}
if not excluded_detail.empty:
    analysis_tables["已套用排除規則排程明細"] = excluded_detail
if not std_year.empty and "標準工時" in std_year.columns:
    temp = std_year.copy()
    temp["標準工時"] = pd.to_numeric(temp["標準工時"], errors="coerce")
    c1, c2 = st.columns([1, 1])
    with c1:
        st.metric("標準工時筆數", f"{len(temp):,}")
        st.metric("標準工時缺漏", f"{temp['標準工時'].isna().sum():,}")
        keys = [k for k in ["客戶", "P/N", "Type"] if k in temp.columns]
        if keys:
            dup = temp.duplicated(keys, keep=False)
            dup_df = temp.loc[dup].sort_values(keys)
            st.metric("可能重複版本", f"{len(dup_df):,}")
            analysis_tables["可能重複版本"] = dup_df
    with c2:
        top = temp.dropna(subset=["標準工時"]).sort_values("標準工時", ascending=False).head(15)
        label_col = "Type" if "Type" in top.columns else top.columns[0]
        fig = px.bar(top, y=label_col, x="標準工時", orientation="h", title="標準工時最高 Top 15")
        fig.update_layout(template="plotly_dark", height=420)
        st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("目前沒有標準工時資料或缺少標準工時欄位。")

if not schedule_year.empty and not std_year.empty:
    # 所有排程都要有標準工時。排除組立地點仍需先算完整原始需求工時，
    # 才能正確形成產能計算排除工時，因此不可從缺漏清單排除。
    relevant_schedule = schedule_year.copy()
    keys = [k for k in ["客戶", "P/N", "Type"] if k in relevant_schedule.columns and k in std.columns]
    if keys:
        lookup = std_year[keys].drop_duplicates()
        missing = relevant_schedule.merge(lookup.assign(_matched=True), on=keys, how="left")
        missing = missing[missing["_matched"].isna()].drop(columns=["_matched"], errors="ignore")
        analysis_tables["排程缺少標準工時"] = missing
        st.subheader("排程找不到標準工時")
        st.caption("這份缺漏清單包含所有排程，也包含產能計算排除的組立地點。因為排除列仍需先以標準工時計算原始需求工時，才能正確扣除。")
        render_configurable_view(missing, "missing_standard_hours", "06. 排程缺少標準工時", height=320)

st.subheader(f"{selected_year}年標準工時資料匯出")
render_multi_sheet_download(analysis_tables, "06.標準工時", label="匯出標準工時 Excel", key="export_standard_all")

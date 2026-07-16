from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from services.legacy_page_sync_service import sync_legacy_duplicate_pages

# Historical deployments may still contain duplicate #U page filenames.
# Synchronize them before users navigate to a module, so the current canonical
# Chinese page code is always the code that runs after Reboot App.
_legacy_page_updates = sync_legacy_duplicate_pages()

from services.capacity_engine import ASSEMBLY_EXCLUSION_PARAM_KEY, CATEGORY_EXCLUSION_PARAM_KEY, calculate_capacity, calculate_capacity_by_years, summarize_manpower, validate_schedule
from services.config import SYSTEM_SUBTITLE
from services.data_loader import ensure_bootstrap, load_all_tables
from services.page_utils import render_configurable_view, render_module_report_download
from services.persistent_store import load_parameters
from services.powerbi_theme import chart_spec, render_powerbi_chart, style_powerbi_figure
from services.settings_service import load_ui_settings, save_ui_settings
from services.year_service import DEFAULT_YEAR, available_years_from_tables, filter_by_year
from services.ui_theme import apply_tech_theme, render_hero, render_human_help, render_system_definition_guide, render_war_room_kpis, status_pill

st.set_page_config(
    page_title="00. 超慧科技製造部產能儀表板",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)
apply_tech_theme()
bootstrap_result = ensure_bootstrap()

st.sidebar.markdown("### 模組快速導覽")
st.sidebar.markdown(
    """
    <div class="sidebar-module-list">
      <div>00. 超慧科技製造部產能儀表板</div>
      <div>01. 超慧員工名單</div>
      <div>02. 派遣名單</div>
      <div>03. 製造部組織圖</div>
      <div>04. 產能負荷表</div>
      <div>05. 排程表</div>
      <div>06. 標準工時</div>
      <div>07. 工作天數設定</div>
      <div>08. 人力參數設定</div>
      <div>09. 情境模擬</div>
      <div>10. 資料匯入與版本管理</div>
      <div>11. 權限與系統設定</div>
    </div>
    """,
    unsafe_allow_html=True,
)

render_hero("00. 超慧科技製造部產能儀表板", SYSTEM_SUBTITLE)
render_human_help([
    "首頁圖表已套用 Power BI 風格：深色報表卡片、商務配色、清楚座標與低干擾圖例。",
    "資料仍只讀取已保存的權威資料與快取計算結果，不會每次切頁重新讀 Excel。",
    "已支援多年度資料，可在左側選擇 2024~2026 等年度並永久保存首頁顯示年份。",
    "頁面下方只保留『整個模組匯出』，匯出的 Excel 會包含資料表與可編輯圖表。",
])
if bootstrap_result:
    st.success(f"已建立或補齊權威資料：{bootstrap_result}")

tables = load_all_tables()
params = load_parameters()
ui_settings = load_ui_settings()
available_years = available_years_from_tables(tables, DEFAULT_YEAR)
saved_year = int(ui_settings.get("home_year", available_years[-1] if available_years else DEFAULT_YEAR))
if saved_year not in available_years and available_years:
    saved_year = available_years[-1]
capacity = calculate_capacity(
    tables["schedule"],
    tables["standard_hours"],
    tables["work_calendar"],
    tables["employees"],
    tables["dispatch"],
    params,
    tables.get("capacity_adjustments"),
    target_year=saved_year,
)
capacity_all_years = calculate_capacity_by_years(
    tables["schedule"], tables["standard_hours"], tables["work_calendar"], tables["employees"], tables["dispatch"], params, tables.get("capacity_adjustments"), years=available_years
)

month_options = capacity["月份"].astype(str).tolist() if not capacity.empty else []
saved_month = str(ui_settings.get("home_month", month_options[0] if month_options else ""))
if saved_month not in month_options and month_options:
    saved_month = month_options[0]

st.sidebar.markdown('<div class="spt-sidebar-apply-card">', unsafe_allow_html=True)
st.sidebar.markdown("#### 首頁年份 / 月份")
st.sidebar.markdown(f'<div class="spt-current-month">目前套用：{saved_year}年 {saved_month or "未設定"}</div>', unsafe_allow_html=True)
with st.sidebar.form("home_year_month_apply_form"):
    draft_year = st.selectbox("選擇首頁年份", available_years, index=available_years.index(saved_year) if saved_year in available_years else 0)
    draft_month = st.selectbox("選擇首頁月份", month_options, index=month_options.index(saved_month) if saved_month in month_options else 0)
    apply_month = st.form_submit_button("套用並永久保存", type="primary", use_container_width=True)
if apply_month:
    ui_settings["home_year"] = int(draft_year)
    ui_settings["home_month"] = draft_month
    save_ui_settings(ui_settings, user="streamlit")
    st.sidebar.success(f"已永久套用首頁：{draft_year}年 {draft_month}")
    st.rerun()
st.sidebar.markdown('</div>', unsafe_allow_html=True)

selected_month = saved_month
month_row = capacity[capacity["月份"].astype(str).eq(selected_month)].iloc[0] if selected_month else capacity.iloc[0]

st.sidebar.markdown("---")
st.sidebar.info("本系統使用 data/persistent/authority 作為權威資料區；有設定 GitHub secrets 時，按儲存/套用會自動同步關鍵權威檔；第 10 頁仍可手動整包同步。", icon="💾")

status = str(month_row["狀態"])
status_color = "red" if status == "紅燈" else "orange" if status == "橘燈" else "yellow" if status == "黃燈" else "green"
st.markdown(status_pill(f"{saved_year}年 {selected_month} 狀態：{status}", status_color), unsafe_allow_html=True)

render_war_room_kpis([
    {"title": "本月機台數", "subtitle": f"{selected_month} 排程總量", "value": f"{month_row['每月機台數']:,.0f}", "unit": "台", "delta": f"狀態：{status}", "kind": "machines", "color": status_color, "delta_class": "danger" if status_color == "red" else "warn" if status_color in {"yellow", "orange"} else ""},
    {"title": "需求總工時", "subtitle": "原始 − 組立地點排除 + 04調整", "value": f"{month_row['需求總工時']:,.0f}", "unit": "h", "delta": f"原始 {month_row.get('原始需求工時', 0):,.0f} h｜排除 {month_row.get('產能計算排除工時', 0):,.0f} h", "kind": "hours", "color": "blue"},
    {"title": "含加班可用工時", "subtitle": "正常 + 平日/假日加班", "value": f"{month_row['含加班可用工時']:,.0f}", "unit": "h", "delta": "依人力參數與工作天數", "kind": "target", "color": "green"},
    {"title": "含加班產能負荷率", "subtitle": "需求總工時 / 含加班可用工時", "value": f"{month_row['含加班稼動率']:.1%}", "unit": "", "delta": "紅/橘/黃/綠燈自動判斷", "kind": "risk", "color": status_color, "delta_class": "danger" if status_color == "red" else "warn" if status_color in {"yellow", "orange"} else ""},
    {"title": "含加班產能餘額", "subtitle": "可用工時 - 需求總工時", "value": f"{month_row['含加班產能負荷']:,.0f}", "unit": "h", "delta": "正數代表仍有餘裕", "kind": "box", "color": "green" if month_row['含加班產能負荷'] >= 0 else "red", "delta_class": "danger" if month_row['含加班產能負荷'] < 0 else ""},
    {"title": "需求人力", "subtitle": "需求總工時換算人力", "value": f"{month_row['需求人力']:,.1f}", "unit": "人", "delta": "依工作日與每日工時計算", "kind": "people", "color": "blue"},
    {"title": "人力差異", "subtitle": "現有人力 - 需求人力", "value": f"{month_row['人力差異']:,.1f}", "unit": "人", "delta": "負數代表需補人或加班", "kind": "people", "color": "green" if month_row['人力差異'] >= 0 else "red", "delta_class": "danger" if month_row['人力差異'] < 0 else ""},
    {"title": "缺工天數", "subtitle": "缺口工時換算天數", "value": f"{month_row['缺工天數']:,.1f}", "unit": "天", "delta": "0 天代表含加班後可承接", "kind": "risk", "color": "green" if month_row['缺工天數'] <= 0 else "orange", "delta_class": "warn" if month_row['缺工天數'] > 0 else ""},
])

st.markdown('<div class="spt-divider"></div>', unsafe_allow_html=True)

chart_df = capacity.copy()
chart_df["含加班產能負荷率%"] = chart_df["含加班稼動率"] * 100
left, right = st.columns([1.15, 1])
with left:
    fig = px.bar(
        chart_df,
        x="月份",
        y=["原始需求工時", "需求總工時", "正常可用工時", "含加班可用工時"],
        barmode="group",
        title="每月需求總工時 vs 可用工時",
    )
    render_powerbi_chart(style_powerbi_figure(fig, height=430, legend_title="指標", yaxis_title="工時"), key="home_capacity_hours")
with right:
    fig2 = px.line(
        chart_df,
        x="月份",
        y="含加班產能負荷率%",
        markers=True,
        title="含加班產能負荷率趨勢",
    )
    fig2.add_hline(
        y=85,
        line_dash="dash",
        annotation_text="85% 警戒",
        annotation_position="top right",
    )
    fig2.add_hline(
        y=100,
        line_dash="dash",
        annotation_text="100% 滿載",
        annotation_position="top right",
    )
    utilization_values = pd.to_numeric(chart_df["含加班產能負荷率%"], errors="coerce").fillna(0).tolist()
    label_positions: list[str] = []
    for idx, value in enumerate(utilization_values):
        if value <= 45:
            label_positions.append("top center")
        elif idx % 2 == 0:
            label_positions.append("top center")
        else:
            label_positions.append("bottom center")
    util_min = min(utilization_values) if utilization_values else 0.0
    util_max = max(utilization_values) if utilization_values else 100.0
    util_axis_bottom = max(0.0, util_min - 18.0)
    util_axis_top = max(120.0, util_max * 1.24)
    fig2 = style_powerbi_figure(fig2, height=450, yaxis_title="產能負荷率 %")
    fig2.update_layout(
        meta={
            "spt_scatter_warning_threshold": 100,
            "spt_scatter_normal_color": "#7DD3FC",
            "spt_scatter_high_color": "#FF9F43",
            "spt_scatter_textpositions": {"含加班產能負荷率%": label_positions, "default": label_positions},
        },
        margin={"l": 72, "r": 92, "t": 138, "b": 72},
    )
    fig2.update_yaxes(range=[util_axis_bottom, util_axis_top])
    render_powerbi_chart(fig2, key="home_utilization")

st.subheader("多年度產能比較")
if not capacity_all_years.empty and capacity_all_years["年份"].nunique() > 1:
    year_summary = capacity_all_years.groupby("年份", as_index=False).agg(原始需求工時=("原始需求工時", "sum"), 產能計算排除工時=("產能計算排除工時", "sum"), 需求總工時=("需求總工時", "sum"), 含加班可用工時=("含加班可用工時", "sum"), 平均含加班產能負荷率=("含加班稼動率", "mean"), 年度機台數=("每月機台數", "sum"))
    fig_year = px.bar(year_summary, x="年份", y=["原始需求工時", "需求總工時", "含加班可用工時"], barmode="group", title="年度原始／需求總工時 vs 可用工時")
    render_powerbi_chart(style_powerbi_figure(fig_year, height=390, legend_title="指標", yaxis_title="工時"), key="home_year_capacity_compare")
else:
    st.caption("目前只有一個年度資料；匯入 2024、2025 等年份後，這裡會自動顯示年度比較。")

st.subheader("人力結構摘要")
manpower = summarize_manpower(filter_by_year(tables["employees"], saved_year), filter_by_year(tables["dispatch"], saved_year))
if manpower.empty:
    st.warning("目前沒有可用人力資料。")
else:
    c1, c2 = st.columns([1, 1])
    with c1:
        group_summary = manpower.groupby("人力來源", as_index=False).agg(可用總人力=("可用總人力", "sum"), 直接有效人力=("直接有效人力", "sum"))
        fig3 = px.pie(group_summary, names="人力來源", values="直接有效人力", title="直接有效人力來源比例", hole=0.48)
        fig3 = style_powerbi_figure(fig3, height=430)
        fig3.update_traces(
            textposition="outside",
            automargin=True,
            domain={"x": [0.08, 0.92], "y": [0.20, 0.94]},
            marker={"line": {"color": "rgba(255,255,255,0.55)", "width": 1}},
        )
        fig3.update_layout(
            meta={"spt_safe_pie_labels": True, "spt_pie_show_mode": "label+percent"},
            legend={
                "orientation": "h",
                "yanchor": "top",
                "y": -0.08,
                "xanchor": "center",
                "x": 0.5,
                "bgcolor": "rgba(2,6,23,0.58)",
                "bordercolor": "rgba(56,189,248,0.25)",
                "borderwidth": 1,
            },
            margin={"l": 86, "r": 86, "t": 82, "b": 118},
        )
        render_powerbi_chart(fig3, key="home_manpower_mix")
    with c2:
        render_configurable_view(manpower, "home_manpower", "首頁人力摘要", height=360)

st.subheader("資料品質檢查")
checks = validate_schedule(
    tables["schedule"],
    excluded_assembly_locations=params.get(ASSEMBLY_EXCLUSION_PARAM_KEY, []),
    excluded_categories=params.get(CATEGORY_EXCLUSION_PARAM_KEY, []),
)
render_configurable_view(checks, "home_quality_checks", "首頁資料品質檢查", height=300)

st.subheader("首頁完整模組匯出")
capacity_export = capacity.rename(columns={"正常稼動率": "正常產能負荷率", "含加班稼動率": "含加班產能負荷率", "正常產能負荷": "正常產能餘額", "含加班產能負荷": "含加班產能餘額", "缺工工時": "缺口工時"})
render_module_report_download(
    "00.超慧科技製造部產能儀表板",
    {
        "產能摘要": capacity_export,
        "人力摘要": manpower,
        "資料品質檢查": checks,
    },
    chart_specs=[
        chart_spec("bar", "每月需求總工時 vs 可用工時", "產能摘要", "月份", ["需求總工時", "正常可用工時", "含加班可用工時"]),
        chart_spec("line", "含加班產能負荷率趨勢", "產能摘要", "月份", ["含加班產能負荷率"]),
        chart_spec("pie", "直接有效人力來源比例", "人力摘要", "人力來源", ["直接有效人力"]),
    ],
    metadata={"模組": "00. 超慧科技製造部產能儀表板", "首頁年份": saved_year, "首頁月份": selected_month, "資料來源": "data/persistent/authority"},
    key="export_home_module",
)


st.markdown('<div class="spt-divider"></div>', unsafe_allow_html=True)
render_system_definition_guide()

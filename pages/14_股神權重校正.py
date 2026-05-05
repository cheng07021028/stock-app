# -*- coding: utf-8 -*-

from __future__ import annotations

# >>> APP_AUTH_GUARD_V84
try:
    from app_auth import require_login
    require_login()
except Exception as _auth_e:
    import streamlit as st
    st.error(f"登入系統載入失敗：{_auth_e}")
    st.stop()
# <<< APP_AUTH_GUARD_V84

# =========================================================
# 14_股神權重校正.py
# v71 Pro：績效代理樣本＋多來源防卡＋防過擬合版
# =========================================================

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import streamlit as st
try:
    from utils import inject_pro_theme
except Exception:
    inject_pro_theme = None

from godpick_weight_calibration import (
    DEFAULT_WEIGHTS,
    calc_category_bundles,
    calc_factor_effectiveness,
    calc_market_bundles,
    calc_profile_bundle,
    confidence_label,
    current_weight_map,
    first_existing_col,
    load_recommendation_records,
    perf_sample_diagnostics,
    best_perf_col,
    numeric_series,
    probability_calibration,
    profile_name_by_horizon,
    rr_analysis,
    save_applied_weights,
    save_suggestion_bundle,
    summarize_returns,
    suggest_weights,
    PERF_COLUMNS,
)

st.set_page_config(page_title="14 股神權重校正｜v71 Pro", layout="wide")


APP_VERSION = "v71_perf_proxy_multisource_antioverfit"


def _ensure_sidebar_numbers_for_this_page() -> None:
    if inject_pro_theme:
        try:
            inject_pro_theme()
            return
        except Exception:
            pass
    st.markdown(
        """
        <style>
        [data-testid="stSidebarNav"] ul li span::before {
            display:inline-block; min-width:2.8em; font-weight:800; color:#334155;
        }
        [data-testid="stSidebarNav"] ul li:nth-of-type(1) span::before { content:"00. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(2) span::before { content:"01. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(3) span::before { content:"02. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(4) span::before { content:"03. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(5) span::before { content:"04. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(6) span::before { content:"05. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(7) span::before { content:"06. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(8) span::before { content:"07. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(9) span::before { content:"08. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(10) span::before { content:"09. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(11) span::before { content:"10. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(12) span::before { content:"11. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(13) span::before { content:"12. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(14) span::before { content:"13. "; }
        [data-testid="stSidebarNav"] ul li:nth-of-type(15) span::before { content:"14. "; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _json_download(data: Any) -> bytes:
    return json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")


def _weights_from_table(table: pd.DataFrame) -> Dict[str, int]:
    if table is None or table.empty:
        return DEFAULT_WEIGHTS.copy()
    return {str(r["因子"]): int(r["建議新權重%"] or 0) for _, r in table.iterrows() if "因子" in r and "建議新權重%" in r}


def _render_header() -> None:
    st.title("14 股神權重校正｜v71 Pro 多來源防卡修正版")
    st.caption("績效回測＋期望值＋分層權重＋防過擬合。只讀取既有推薦紀錄，不連外、不重跑推薦；套用權重需人工確認。")
    st.info("核心邏輯：不只看勝率，也看平均報酬、平均虧損、期望值、樣本數、資料覆蓋率；單次調整設上限，避免短期過擬合。")


def _render_quality(df: pd.DataFrame, horizon: int, current_weights: Dict[str, int]) -> None:
    perf_col = best_perf_col(df, horizon)
    ret = numeric_series(df, perf_col) if perf_col else pd.Series(dtype="float64")
    stat = summarize_returns(ret) if perf_col else {"樣本數": 0, "勝率%": None, "期望值%": None, "平均報酬%": None}
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("總推薦紀錄", f"{len(df):,}")
    c2.metric(f"{horizon}日績效欄", perf_col or "缺少")
    c3.metric("有效樣本", stat.get("樣本數", 0))
    c4.metric("勝率", "—" if stat.get("勝率%") is None else f"{stat['勝率%']}%")
    c5.metric("期望值", "—" if stat.get("期望值%") is None else f"{stat['期望值%']}%")

    st.markdown("**目前 7_股神推薦 已套用權重**")
    st.caption(" / ".join([f"{k}{v}%" for k, v in current_weights.items()]))

    if not perf_col:
        st.warning(f"缺少 {horizon} 日績效欄。請先到 8_股神推薦紀錄 或 10_推薦清單 更新推薦後績效。")
    elif stat.get("樣本數", 0) < 30:
        st.warning("有效樣本低於 30 筆：本頁仍可分析，但建議只觀察，不要直接套用權重。")
    else:
        st.success("必要績效欄已存在，可進行權重校正。")


def _render_weight_table(title: str, weight_df: pd.DataFrame) -> Dict[str, int]:
    st.subheader(title)
    if weight_df.empty:
        st.warning("尚無足夠資料產生建議權重。")
        return DEFAULT_WEIGHTS.copy()
    st.dataframe(weight_df, use_container_width=True, hide_index=True)
    weights = _weights_from_table(weight_df)
    total = sum(weights.values())
    st.info(f"建議權重總和：{total}%｜防過擬合限制：單一因子建議權重原則上介於 5%～25%，單次調整幅度受控。")
    return weights


def main() -> None:
    _ensure_sidebar_numbers_for_this_page()
    _render_header()

    df = load_recommendation_records()
    if df.empty:
        st.error("目前沒有讀到推薦紀錄。請先從 7_股神推薦 匯入 8_股神推薦紀錄 或 10_推薦清單。")
        return

    current_weights = current_weight_map()

    with st.sidebar:
        st.header("v71 校正設定")
        horizon = st.selectbox("主要校正週期", [1, 3, 5, 10, 20], index=2, help="短線看 1/3 日，波段看 5/10 日，趨勢看 20 日。")
        st.caption("建議：先用 5 日或 10 日，不要用樣本太少的週期直接調權重。")
        st.divider()
        allow_apply = st.checkbox("允許從本頁套用權重", value=False, help="安全機制：預設只看建議，不覆蓋 7_股神推薦 權重。")
        st.caption("套用後會寫入 godpick_user_settings.json，7_股神推薦 會讀取 applied_weights。")

    _render_quality(df, horizon, current_weights)

    perf_col = best_perf_col(df, horizon)
    if not perf_col:
        st.stop()

    effect_df = calc_factor_effectiveness(df, horizon)
    weight_df = suggest_weights(effect_df, current_weights)
    bundle = calc_profile_bundle(df, horizons=(1, 3, 5, 10, 20), current_weights=current_weights)
    market_bundle = calc_market_bundles(df, horizon, current_weights)
    category_bundle = calc_category_bundles(df, horizon, current_weights)
    prob_df = probability_calibration(df, horizon)
    rr_df = rr_analysis(df, horizon)

    bundle["market_profiles"] = market_bundle
    bundle["category_profiles"] = category_bundle
    bundle["main_horizon"] = horizon
    bundle["main_weight_table"] = weight_df.to_dict(orient="records") if not weight_df.empty else []

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "總覽建議",
        "因子有效性",
        "短線/波段/趨勢",
        "大盤分層",
        "類股分層",
        "機率/RR校正",
        "輸出/套用",
    ])

    with tab1:
        suggested_weights = _render_weight_table(f"{horizon}日主校正權重建議", weight_df)
        st.markdown("### 權重調整解讀")
        if not weight_df.empty:
            for _, r in weight_df.iterrows():
                if int(r.get("實際差異%", 0) or 0) != 0:
                    st.write(f"- **{r['因子']}**：{r['目前權重%']}% → {r['建議新權重%']}%，{r.get('調整理由', '')}")
            st.caption("沒有列出的因子代表本次建議維持或四捨五入後差異不明顯。")

    with tab2:
        st.subheader("因子有效性排名")
        if effect_df.empty:
            st.warning("缺少績效欄或因子欄位，無法分析。")
        else:
            sort_col = "期望值差%" if "期望值差%" in effect_df.columns else "勝率差%"
            show = effect_df.sort_values(sort_col, ascending=False, na_position="last")
            st.dataframe(show, use_container_width=True, hide_index=True)
            st.caption("高分組與低分組差異越大，代表該因子越能區分後續績效。")

    with tab3:
        st.subheader("短線 / 波段 / 趨勢 三套權重")
        profile_rows = []
        for name, item in bundle.get("profiles", {}).items():
            weights = item.get("weights", {})
            stat = item.get("base_stat", {})
            profile_rows.append({"權重組合": name, "週期": item.get("horizon"), "有效樣本": stat.get("樣本數"), "勝率%": stat.get("勝率%"), "期望值%": stat.get("期望值%"), **weights})
        if profile_rows:
            st.dataframe(pd.DataFrame(profile_rows), use_container_width=True, hide_index=True)
        else:
            st.warning("缺少可用績效週期，無法產生分層權重。")

    with tab4:
        st.subheader("大盤環境分層權重")
        for mode, item in market_bundle.items():
            with st.expander(f"{mode} 權重建議", expanded=(mode == "多頭")):
                if item.get("status") != "ok":
                    st.warning(f"{item.get('status')}｜樣本數：{item.get('樣本數', 0)}")
                else:
                    st.write(f"樣本數：{item.get('樣本數')}")
                    st.dataframe(pd.DataFrame(item.get("table", [])), use_container_width=True, hide_index=True)

    with tab5:
        st.subheader("類股別權重建議")
        if category_bundle.get("status"):
            st.warning(category_bundle.get("status"))
        else:
            st.caption(f"類股欄位：{category_bundle.get('category_col')}")
            for cat, item in category_bundle.get("items", {}).items():
                with st.expander(f"{cat}｜{item.get('樣本數', 0)} 筆", expanded=False):
                    if item.get("status") != "ok":
                        st.warning(item.get("status"))
                    else:
                        st.json(item.get("base_stat", {}), expanded=False)
                        st.dataframe(pd.DataFrame(item.get("table", [])), use_container_width=True, hide_index=True)

    with tab6:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("上漲機率校正")
            if prob_df.empty:
                st.warning("缺少上漲機率或績效欄位。")
            else:
                st.dataframe(prob_df, use_container_width=True, hide_index=True)
        with c2:
            st.subheader("風險報酬比 R/R 校正")
            if rr_df.empty:
                st.warning("缺少 R/R 或績效欄位。")
            else:
                st.dataframe(rr_df, use_container_width=True, hide_index=True)

    with tab7:
        st.subheader("輸出與人工套用")
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("寫出權重建議 JSON", use_container_width=True):
                ok, msg = save_suggestion_bundle(bundle)
                st.success(msg) if ok else st.error(msg)
        with c2:
            st.download_button(
                "下載權重建議 JSON",
                data=_json_download(bundle),
                file_name=f"godpick_weight_suggestions_v66_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                use_container_width=True,
            )
        with c3:
            profile_options = list(bundle.get("profiles", {}).keys())
            chosen_profile = st.selectbox("選擇要套用的權重組合", ["主校正建議"] + profile_options)

        if chosen_profile == "主校正建議":
            weights_to_apply = _weights_from_table(weight_df)
        else:
            weights_to_apply = bundle.get("profiles", {}).get(chosen_profile, {}).get("weights", DEFAULT_WEIGHTS)

        st.markdown("### 即將套用權重")
        st.dataframe(pd.DataFrame([{"因子": k, "權重%": v} for k, v in weights_to_apply.items()]), use_container_width=True, hide_index=True)

        if not allow_apply:
            st.warning("安全鎖尚未開啟。請先到左側勾選『允許從本頁套用權重』，再按套用。")
        apply_btn = st.button("套用建議權重到 7_股神推薦", type="primary", use_container_width=True, disabled=not allow_apply)
        if apply_btn:
            ok, msg = save_applied_weights(weights_to_apply, chosen_profile)
            if ok:
                st.success(f"{msg}。請回到 7_股神推薦，重新整理後會讀取新權重。")
            else:
                st.error(msg)

    st.markdown("---")
    st.caption("v71 Pro：此頁只做績效回測與建議權重，支援有效績效與即時代理樣本。套用權重需人工打開安全鎖並按下套用，不會自動覆蓋。")


if __name__ == "__main__":
    main()

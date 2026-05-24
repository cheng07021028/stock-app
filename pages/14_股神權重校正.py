# -*- coding: utf-8 -*-









from __future__ import annotations
from godpick_factor_schema import enrich_dataframe, ensure_factor_columns, V72_FACTOR_FIELDS

# >>> PAGE_CONFIG_ALREADY_SET_V86
import streamlit as st
st.set_page_config(page_title='14_股神權重校正｜專業績效補值版', layout="wide")
# <<< PAGE_CONFIG_ALREADY_SET_V86

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
    calc_night_accuracy_bundle,
    apply_night_accuracy_feedback,
    calc_official_factor_accuracy_bundle,
    apply_official_factor_feedback,
    calc_quality_accuracy_bundle,
    apply_quality_feedback,
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
    get_weight_calibration_page_settings,
    save_weight_calibration_page_settings,
    summarize_returns,
    suggest_weights,
    PERF_COLUMNS,
)




APP_VERSION = "professional_perf_fallback"



# >>> V72_FACTOR_ENRICH_HELPER
def _v72_enrich_recommendation_df_safe(df):
    try:
        return enrich_dataframe(df)
    except Exception:
        try:
            return ensure_factor_columns(df)
        except Exception:
            return df
# <<< V72_FACTOR_ENRICH_HELPER

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


def _json_safe_value_for_download(obj: Any) -> Any:
    """v105：下載 JSON 前先轉安全型別，避免 DataFrame/Series/NA 造成 TypeError。"""
    try:
        if obj is None:
            return None
        if obj is pd.NA:
            return None
        try:
            if pd.isna(obj) and not isinstance(obj, (list, tuple, dict, pd.Series, pd.DataFrame)):
                return None
        except Exception:
            pass
        if isinstance(obj, pd.DataFrame):
            return [_json_safe_value_for_download(x) for x in obj.to_dict(orient="records")]
        if isinstance(obj, pd.Series):
            return {str(k): _json_safe_value_for_download(v) for k, v in obj.to_dict().items()}
        if isinstance(obj, pd.Index):
            return [_json_safe_value_for_download(x) for x in obj.tolist()]
        if isinstance(obj, (pd.Timestamp, datetime)):
            try:
                return obj.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return str(obj)
        if isinstance(obj, dict):
            return {str(_json_safe_value_for_download(k)): _json_safe_value_for_download(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [_json_safe_value_for_download(x) for x in obj]
        if hasattr(obj, "item"):
            try:
                return _json_safe_value_for_download(obj.item())
            except Exception:
                pass
        if isinstance(obj, float):
            try:
                import math
                if math.isnan(obj) or math.isinf(obj):
                    return None
            except Exception:
                pass
            return obj
        if isinstance(obj, (str, int, bool)):
            return obj
        return str(obj)
    except Exception:
        return str(obj)


def _json_download(data: Any) -> bytes:
    return json.dumps(_json_safe_value_for_download(data), ensure_ascii=False, indent=2).encode("utf-8")




def _is_blank_value(v: Any) -> bool:
    if v is None:
        return True
    try:
        if pd.isna(v):
            return True
    except Exception:
        pass
    s = str(v).strip()
    return s == "" or s.lower() in {"nan", "none", "null", "--", "—"}


def _first_nonblank(row: pd.Series, cols: list[str], default: Any = "") -> Any:
    for c in cols:
        if c in row.index and not _is_blank_value(row.get(c)):
            return row.get(c)
    return default


def _prepare_calibration_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    v92：修正 14 權重校正頁常見缺欄問題。
    只用既有 JSON 紀錄補欄，不連外、不重跑 7_股神推薦。
    補齊重點：大盤分層 / 上漲機率 / R/R / v72 因子分數。
    """
    if raw_df is None or raw_df.empty:
        return raw_df
    df = _v72_enrich_recommendation_df_safe(raw_df.copy())

    # 大盤分層：14 頁的大盤分層權重需要可被 MARKET_COLUMNS 辨識的欄位。
    if "大盤分層" in df.columns:
        if "大盤情境" not in df.columns:
            df["大盤情境"] = df["大盤分層"]
        else:
            df["大盤情境"] = df.apply(
                lambda r: r.get("大盤分層") if _is_blank_value(r.get("大盤情境")) else r.get("大盤情境"),
                axis=1,
            )
    if "大盤狀態" not in df.columns and "大盤情境" in df.columns:
        df["大盤狀態"] = df["大盤情境"]

    # 上漲機率：優先保留原生欄，沒有才用推薦總分/股神分數代理。
    prob_sources = ["上漲機率估計%", "上漲機率%", "預估上漲機率", "上漲機率"]
    if "上漲機率估計%" not in df.columns:
        df["上漲機率估計%"] = ""
    for idx, row in df.iterrows():
        if _is_blank_value(row.get("上漲機率估計%")):
            v = _first_nonblank(row, prob_sources[1:], "")
            if _is_blank_value(v):
                base = _first_nonblank(row, ["推薦總分", "推薦分數", "股神決策分數", "訊號分數"], 70)
                try:
                    base_f = float(str(base).replace("%", "").replace(",", ""))
                    v = max(35, min(85, 45 + (base_f - 60) * 0.55))
                except Exception:
                    v = 55
            df.at[idx, "上漲機率估計%"] = v

    # R/R：優先使用原生風險報酬比；沒有時用風險報酬_拉回 / 突破 或 R/R分數保守代理。
    if "風險報酬比" not in df.columns:
        df["風險報酬比"] = ""
    rr_sources = ["風險報酬比_決策", "R/R", "RR", "風險報酬_拉回", "風險報酬_突破"]
    for idx, row in df.iterrows():
        if _is_blank_value(row.get("風險報酬比")):
            v = _first_nonblank(row, rr_sources, "")
            if _is_blank_value(v) and not _is_blank_value(row.get("R/R分數")):
                try:
                    # R/R分數是 0~100 分，不是倍數；換算成保守倍數，避免全部落在 >=3。
                    score = float(str(row.get("R/R分數")).replace("%", "").replace(",", ""))
                    v = round(max(0.5, min(3.5, score / 35)), 2)
                except Exception:
                    v = ""
            df.at[idx, "風險報酬比"] = v

    return df


def _render_v92_field_diagnostics(df: pd.DataFrame, horizon: int) -> None:
    try:
        perf_diag = perf_sample_diagnostics(df)
        market_col = first_existing_col(df, ["大盤情境", "大盤狀態", "大盤分層", "大盤橋接風控", "市場狀態", "大盤模式"]) or "缺少"
        prob_col = first_existing_col(df, ["上漲機率估計%", "上漲機率%", "預估上漲機率", "上漲機率"]) or "缺少"
        rr_col = first_existing_col(df, ["風險報酬比", "風險報酬比_決策", "R/R", "RR", "風險報酬_拉回", "風險報酬_突破"]) or "缺少"
        v72_ok = sum(1 for c in V72_FACTOR_FIELDS if c in df.columns)
        with st.expander("v92 欄位補齊檢查", expanded=False):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("大盤分層欄", market_col)
            c2.metric("上漲機率欄", prob_col)
            c3.metric("R/R 欄", rr_col)
            c4.metric("v72 因子欄", f"{v72_ok}/{len(V72_FACTOR_FIELDS)}")
            st.dataframe(perf_diag, use_container_width=True, hide_index=True)
            if market_col != "缺少" and prob_col != "缺少" and rr_col != "缺少":
                st.success("大盤分層、上漲機率、R/R 欄位已可供權重校正使用。")
            else:
                st.warning("仍有欄位缺少；請先回 7 股神推薦或 8/10 紀錄補齊資料。")
    except Exception as e:
        st.caption(f"欄位診斷略過：{e}")



def _render_v99_night_field_diagnostics(df: pd.DataFrame) -> None:
    """顯示夜間隔日股神欄位是否已進入 14 權重校正資料池。"""
    try:
        night_cols = [
            "夜間股神總分", "隔日實戰排序分", "隔日進場分數", "波段潛力分數",
            "法人籌碼分數", "大戶鎖碼分數", "基本面成長分數", "營收成長分數",
            "EPS成長分數", "估值風險分數", "進場型態_隔日", "隔日建議動作",
            "預估進場點", "回測承接價", "突破確認價_隔日", "停損價_隔日",
            "第一壓力價", "夜間股神建議", "隔日作戰策略", "資料完整度",
        ]
        rows = []
        for c in night_cols:
            exists = c in df.columns
            valid = 0
            if exists:
                try:
                    valid = int(df[c].notna().sum())
                except Exception:
                    valid = 0
            rows.append({"夜間欄位": c, "是否存在": "有" if exists else "缺", "有效筆數": valid})
        exists_n = sum(1 for r in rows if r["是否存在"] == "有")
        score_n = sum(1 for c in ["夜間股神總分", "隔日實戰排序分", "隔日進場分數", "波段潛力分數"] if c in df.columns)
        with st.expander("v99 夜間隔日欄位同步檢查", expanded=False):
            c1, c2, c3 = st.columns(3)
            c1.metric("夜間欄位存在", f"{exists_n}/{len(night_cols)}")
            c2.metric("核心分數欄", f"{score_n}/4")
            c3.metric("資料來源筆數", f"{len(df):,}")
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            if score_n >= 2:
                st.success("夜間隔日股神分數已納入權重校正資料池，可用來產生 07 權重建議。")
            else:
                st.warning("目前夜間核心分數欄不足；可先回 07 重新推薦，或從 10/8 累積夜間欄位資料後再校正。")
    except Exception as e:
        st.caption(f"夜間欄位診斷略過：{e}")


def _render_v104_accuracy_diagnostics(df: pd.DataFrame, horizon: int) -> None:
    """顯示 10/8 命中追蹤欄位是否可供 14 權重校正參考。"""
    try:
        hit_cols = [
            "作戰追蹤狀態", "進場點命中", "突破價命中", "停損價觸發", "第一壓力命中",
            "隔日最高漲幅%", "3日最高漲幅%", "5日最高漲幅%", "10日最高漲幅%",
            "隔日最低回撤%", "3日最低回撤%", "5日最低回撤%", "10日最低回撤%",
            "作戰命中摘要", "作戰追蹤更新時間",
        ]
        rows = []
        for c in hit_cols:
            exists = c in df.columns
            valid = int(df[c].notna().sum()) if exists else 0
            rows.append({"命中追蹤欄位": c, "是否存在": "有" if exists else "缺", "有效筆數": valid})
        exists_n = sum(1 for r in rows if r["是否存在"] == "有")
        bundle = calc_night_accuracy_bundle(df, horizon=horizon)
        summary = bundle.get("summary", {}) if isinstance(bundle, dict) else {}
        with st.expander("v104 夜間準確率/命中追蹤檢查", expanded=False):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("命中欄位存在", f"{exists_n}/{len(hit_cols)}")
            c2.metric("採用績效欄", summary.get("績效欄", "缺"))
            c3.metric("進場點命中率", "—" if summary.get("進場點命中率%") is None else f"{summary.get('進場點命中率%')}%")
            c4.metric("停損觸發率", "—" if summary.get("停損觸發率%") is None else f"{summary.get('停損觸發率%')}%")
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            if exists_n >= 4:
                st.success("已讀到 V101/V102 命中追蹤資料，可作為權重校正參考。")
            else:
                st.warning("命中追蹤欄位仍不足；請先到 10_推薦清單按『更新隔日命中追蹤』，或到 8_股神推薦紀錄更新績效。")
    except Exception as e:
        st.caption(f"v104 命中追蹤診斷略過：{e}")

def _weights_from_table(table: pd.DataFrame) -> Dict[str, int]:
    if table is None or table.empty:
        return DEFAULT_WEIGHTS.copy()
    return {str(r["因子"]): int(r["建議新權重%"] or 0) for _, r in table.iterrows() if "因子" in r and "建議新權重%" in r}


def _render_header() -> None:
    st.title("14_股神權重校正｜專業績效補值版")
    st.caption("績效回測＋命中追蹤＋分層權重＋防過擬合；自動避開全空白績效欄，優先採用實際有效績效，不連外、不重跑推薦。")
    st.info("核心邏輯：不只看勝率，也看平均報酬、命中率、停損率、期望值、樣本數、資料覆蓋率；若尚未產生明確命中欄，會以實際報酬建立保守代理命中率，避免畫面誤顯示整片 None。")


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

    df = _prepare_calibration_df(load_recommendation_records())
    if df.empty:
        st.error("目前沒有讀到推薦紀錄。請先從 7_股神推薦 匯入 8_股神推薦紀錄 或 10_推薦清單。")
        return

    current_weights = current_weight_map()

    with st.sidebar:
        st.header("14_權重校正設定")
        saved_calib_setting = get_weight_calibration_page_settings()
        horizon_options = [1, 3, 5, 10, 20]
        saved_horizon = int(saved_calib_setting.get("main_horizon", 5) or 5)
        if saved_horizon not in horizon_options:
            saved_horizon = 5
        horizon = st.selectbox(
            "主要校正週期",
            horizon_options,
            index=horizon_options.index(saved_horizon),
            help="短線看 1/3 日，波段看 5/10 日，趨勢看 20 日。",
            key="v106_weight_calibration_main_horizon",
        )
        st.caption("建議：先用 5 日或 10 日，不要用樣本太少的週期直接調權重。")
        st.divider()
        allow_apply = st.checkbox(
            "允許從本頁套用權重",
            value=bool(saved_calib_setting.get("allow_apply_from_page", False)),
            help="安全機制：預設只看建議，不覆蓋 7_股神推薦 權重。",
            key="v106_weight_calibration_allow_apply",
        )
        st.caption("套用後會寫入 godpick_user_settings.json，7_股神推薦 會讀取 applied_weights。")
        if st.button("套用 14_權重校正設定（永久設定）", use_container_width=True, type="primary"):
            ok, msg = save_weight_calibration_page_settings(horizon, allow_apply)
            if ok:
                st.success(msg)
                st.info("已保存主要校正週期與『允許從本頁套用權重』設定；重新整理或換頁後仍會沿用。")
            else:
                st.error(msg)
        updated_at = str(saved_calib_setting.get("updated_at", "") or "")
        if updated_at:
            st.caption(f"目前永久設定更新時間：{updated_at}")

    _render_quality(df, horizon, current_weights)
    _render_v92_field_diagnostics(df, horizon)
    _render_v99_night_field_diagnostics(df)
    _render_v104_accuracy_diagnostics(df, horizon)

    perf_col = best_perf_col(df, horizon)
    if not perf_col:
        st.stop()

    effect_df = calc_factor_effectiveness(df, horizon)
    weight_df = suggest_weights(effect_df, current_weights)
    night_accuracy_bundle = calc_night_accuracy_bundle(df, horizon=horizon)
    weight_df = apply_night_accuracy_feedback(weight_df, night_accuracy_bundle)
    official_factor_bundle = calc_official_factor_accuracy_bundle(df, horizon=horizon)
    weight_df = apply_official_factor_feedback(weight_df, official_factor_bundle)
    quality_bundle = calc_quality_accuracy_bundle(df, horizon=horizon)
    weight_df = apply_quality_feedback(weight_df, quality_bundle)
    bundle = calc_profile_bundle(df, horizons=(1, 3, 5, 10, 20), current_weights=current_weights)
    market_bundle = calc_market_bundles(df, horizon, current_weights)
    category_bundle = calc_category_bundles(df, horizon, current_weights)
    prob_df = probability_calibration(df, horizon)
    rr_df = rr_analysis(df, horizon)

    bundle["market_profiles"] = market_bundle
    bundle["category_profiles"] = category_bundle
    bundle["night_accuracy_feedback"] = night_accuracy_bundle
    bundle["official_factor_feedback"] = official_factor_bundle
    bundle["quality_feedback"] = quality_bundle
    bundle["main_horizon"] = horizon
    bundle["main_weight_table"] = weight_df.to_dict(orient="records") if not weight_df.empty else []

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11 = st.tabs([
        "總覽建議",
        "因子有效性",
        "短線/波段/趨勢",
        "大盤分層",
        "類股分層",
        "機率/RR校正",
        "夜間隔日欄位",
        "夜間準確率回饋",
        "官方因子回饋",
        "實戰主推薦回饋",
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
        st.caption("v93：多頭/空頭如果樣本數為 0，通常代表目前推薦紀錄都落在同一種大盤情境；不是錯誤。此版會顯示全市場保底建議與分層來源。")
        first_item = next(iter(market_bundle.values()), {}) if isinstance(market_bundle, dict) and market_bundle else {}
        source = first_item.get("分層來源", "未偵測")
        dist = first_item.get("分層樣本分布", {})
        if dist:
            st.info("分層來源：" + str(source))
            st.dataframe(pd.DataFrame([{ "大盤分層": k, "樣本數": v } for k, v in dist.items()]), use_container_width=True, hide_index=True)
        for mode, item in market_bundle.items():
            with st.expander(f"{mode} 權重建議", expanded=(mode in ["全市場", "盤整"])):
                if item.get("status") != "ok":
                    st.warning(f"{item.get('status')}｜樣本數：{item.get('樣本數', 0)}")
                    if mode in ["多頭", "空頭"]:
                        st.caption("這通常是推薦紀錄裡缺少該大盤情境，不代表程式壞掉；等未來累積到多頭/空頭推薦績效後，這區會自動產生權重建議。")
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
        st.subheader("夜間隔日股神欄位校正來源")
        st.caption("這裡只顯示 07/08/10 新增夜間欄位如何進入原本 8 大權重。實際套用仍寫回原權重名稱，避免 07 頁不相容。")
        mapping_rows = [
            {"07 權重因子": "技術結構", "夜間來源": "技術趨勢分數 / 夜間股神總分", "用途": "判斷結構是否轉強"},
            {"07 權重因子": "起漲前兆", "夜間來源": "隔日進場分數 / 波段潛力 / 營收與 EPS 成長", "用途": "校正剛起漲與隔日機會"},
            {"07 權重因子": "自動因子", "夜間來源": "夜間股神總分 / 隔日實戰排序分", "用途": "校正整體夜間股神排序"},
            {"07 權重因子": "交易可行", "夜間來源": "隔日進場分數 / 估值風險 / R/R", "用途": "校正是否能實戰進場"},
            {"07 權重因子": "爆發力", "夜間來源": "量價動能 / 法人籌碼 / 大戶鎖碼", "用途": "校正資金推升力"},
            {"07 權重因子": "型態突破", "夜間來源": "進場型態_隔日 / 隔日作戰策略", "用途": "校正突破或回測型態"},
        ]
        st.dataframe(pd.DataFrame(mapping_rows), use_container_width=True, hide_index=True)
        _render_v99_night_field_diagnostics(df)

    with tab8:
        st.subheader("夜間隔日股神準確率回饋")
        st.caption("讀取 10_推薦清單 V101 命中追蹤與 8_股神推薦紀錄 V102/V103 績效欄位；本頁不連外、不重新抓 K 線。")
        summary = night_accuracy_bundle.get("summary", {}) if isinstance(night_accuracy_bundle, dict) else {}
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("採用績效欄", summary.get("績效欄", "缺"))
        c2.metric("進場命中", "—" if summary.get("進場點命中率%") is None else f"{summary.get('進場點命中率%')}%")
        c3.metric("突破命中", "—" if summary.get("突破價命中率%") is None else f"{summary.get('突破價命中率%')}%")
        c4.metric("壓力命中", "—" if summary.get("第一壓力命中率%") is None else f"{summary.get('第一壓力命中率%')}%")
        c5.metric("停損觸發", "—" if summary.get("停損觸發率%") is None else f"{summary.get('停損觸發率%')}%")

        base_stat = summary.get("績效統計", {}) if isinstance(summary.get("績效統計"), dict) else {}
        st.markdown("#### 夜間績效統計")
        st.dataframe(pd.DataFrame([base_stat]) if base_stat else pd.DataFrame(), use_container_width=True, hide_index=True)

        tables = night_accuracy_bundle.get("tables", {}) if isinstance(night_accuracy_bundle, dict) else {}
        if not tables:
            st.warning("尚無可分層的命中追蹤資料。請先到 10_推薦清單更新隔日命中追蹤。")
        else:
            for name, table in tables.items():
                with st.expander(f"{name} 準確率分層", expanded=(name in ["進場型態_隔日", "隔日建議動作"])):
                    if isinstance(table, pd.DataFrame) and not table.empty:
                        st.dataframe(table, use_container_width=True, hide_index=True)
                    else:
                        st.caption("此分層目前樣本不足。")

        weak = night_accuracy_bundle.get("weak", pd.DataFrame()) if isinstance(night_accuracy_bundle, dict) else pd.DataFrame()
        st.markdown("#### 高分失敗 / 停損檢討清單")
        if isinstance(weak, pd.DataFrame) and not weak.empty:
            st.dataframe(weak, use_container_width=True, hide_index=True)
            st.caption("用途：找出夜間高分但隔日表現不佳的樣本，避免後續權重過度偏向單一因子。")
        else:
            st.info("目前沒有足夠的高分失敗樣本，或尚未更新命中追蹤。")

    with tab9:
        st.subheader("官方因子準確率回饋")
        st.caption("讀取 16_官方因子快取中心、07、10、8 已保存的法人 / 營收 / EPS / PER 欄位；本頁不連外、不更新官方網站。")
        summary = official_factor_bundle.get("summary", {}) if isinstance(official_factor_bundle, dict) else {}
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("官方可用樣本", summary.get("官方可用樣本", 0))
        c2.metric("平均官方分", "—" if summary.get("平均官方因子總分") is None else summary.get("平均官方因子總分"))
        c3.metric("平均完整度", "—" if summary.get("平均官方完整度") is None else summary.get("平均官方完整度"))
        c4.metric("官方高分樣本", summary.get("官方高分樣本", 0))
        c5.metric("高分失敗", summary.get("官方高分失敗筆數", 0))

        stat_cols = st.columns(3)
        with stat_cols[0]:
            st.markdown("#### 官方可用績效")
            st.dataframe(pd.DataFrame([summary.get("官方可用績效", {})]), use_container_width=True, hide_index=True)
        with stat_cols[1]:
            st.markdown("#### 官方高分績效")
            st.dataframe(pd.DataFrame([summary.get("官方高分績效", {})]), use_container_width=True, hide_index=True)
        with stat_cols[2]:
            st.markdown("#### 官方低分績效")
            st.dataframe(pd.DataFrame([summary.get("官方低分績效", {})]), use_container_width=True, hide_index=True)

        st.markdown("#### 命中率摘要")
        hit_row = {
            "績效欄": summary.get("績效欄", "缺"),
            "進場點命中率%": summary.get("官方可用進場點命中率%"),
            "突破價命中率%": summary.get("官方可用突破價命中率%"),
            "第一壓力命中率%": summary.get("官方可用第一壓力命中率%"),
            "停損觸發率%": summary.get("官方可用停損觸發率%"),
        }
        st.dataframe(pd.DataFrame([hit_row]), use_container_width=True, hide_index=True)

        tables = official_factor_bundle.get("tables", {}) if isinstance(official_factor_bundle, dict) else {}
        if not tables:
            st.warning("尚無官方因子分層資料。請先到 16_官方因子快取中心更新快取，再由 07/10/8 保存推薦紀錄。")
        else:
            for name, table in tables.items():
                with st.expander(f"{name} 官方因子分層", expanded=(name in ["官方因子級距", "官方資料完整度級距", "法人官方級距"])):
                    if isinstance(table, pd.DataFrame) and not table.empty:
                        st.dataframe(table, use_container_width=True, hide_index=True)
                    else:
                        st.caption("此分層目前樣本不足。")

        high_fail = official_factor_bundle.get("high_fail", pd.DataFrame()) if isinstance(official_factor_bundle, dict) else pd.DataFrame()
        low_success = official_factor_bundle.get("low_success", pd.DataFrame()) if isinstance(official_factor_bundle, dict) else pd.DataFrame()
        st.markdown("#### 官方高分失敗檢討")
        if isinstance(high_fail, pd.DataFrame) and not high_fail.empty:
            st.dataframe(high_fail, use_container_width=True, hide_index=True)
        else:
            st.info("目前沒有足夠的官方高分失敗樣本。")
        st.markdown("#### 官方低分但成功樣本")
        if isinstance(low_success, pd.DataFrame) and not low_success.empty:
            st.dataframe(low_success, use_container_width=True, hide_index=True)
        else:
            st.info("目前沒有足夠的官方低分成功樣本。")


    with tab10:
        st.subheader("實戰品質準確率回饋")
        st.caption("讀取 07 V118、10 V119、8 V120 已保存的量能 / 趨勢 / 實戰降分欄位；本頁不連外、不重跑推薦。")
        summary = quality_bundle.get("summary", {}) if isinstance(quality_bundle, dict) else {}
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("可用樣本", summary.get("實戰品質可用樣本", 0))
        c2.metric("平均品質分", "—" if summary.get("平均實戰品質分") is None else summary.get("平均實戰品質分"))
        c3.metric("平均降分", "—" if summary.get("平均實戰降分") is None else summary.get("平均實戰降分"))
        c4.metric("高品質樣本", summary.get("高品質樣本", 0))
        c5.metric("高品質失敗", summary.get("高品質失敗筆數", 0))

        stat_cols = st.columns(3)
        with stat_cols[0]:
            st.markdown("#### 高品質績效")
            st.dataframe(pd.DataFrame([summary.get("高品質績效", {})]), use_container_width=True, hide_index=True)
        with stat_cols[1]:
            st.markdown("#### 低品質績效")
            st.dataframe(pd.DataFrame([summary.get("低品質績效", {})]), use_container_width=True, hide_index=True)
        with stat_cols[2]:
            st.markdown("#### 高降分績效")
            st.dataframe(pd.DataFrame([summary.get("高降分績效", {})]), use_container_width=True, hide_index=True)

        st.markdown("#### 命中率摘要")
        hit_row = {
            "績效欄": summary.get("績效欄", "缺"),
            "高品質進場點命中率%": summary.get("高品質進場點命中率%"),
            "高品質第一壓力命中率%": summary.get("高品質第一壓力命中率%"),
            "高品質停損觸發率%": summary.get("高品質停損觸發率%"),
        }
        st.dataframe(pd.DataFrame([hit_row]), use_container_width=True, hide_index=True)

        tables = quality_bundle.get("tables", {}) if isinstance(quality_bundle, dict) else {}
        if not tables:
            st.warning("尚無實戰品質分層資料。請先由 07 重新推薦，並讓 10/8 保存 V118 實戰品質欄位。")
        else:
            for name, table in tables.items():
                with st.expander(f"{name} 實戰品質分層", expanded=(name in ["實戰品質級距", "量能狀態", "趨勢狀態", "實戰降分級距"])):
                    if isinstance(table, pd.DataFrame) and not table.empty:
                        st.dataframe(table, use_container_width=True, hide_index=True)
                    else:
                        st.caption("此分層目前樣本不足。")

        weak = quality_bundle.get("weak", pd.DataFrame()) if isinstance(quality_bundle, dict) else pd.DataFrame()
        st.markdown("#### 高實戰品質但失敗檢討")
        if isinstance(weak, pd.DataFrame) and not weak.empty:
            st.dataframe(weak, use_container_width=True, hide_index=True)
            st.caption("用途：檢查高品質仍失敗的股票，避免 07 過度相信量價或趨勢單一條件。")
        else:
            st.info("目前沒有足夠的高品質失敗樣本。")

    with tab11:
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
                file_name=f"godpick_weight_suggestions_v121_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
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
                # V143：同一個 Streamlit session 換到 07 時可偵測這個提示；權威資料仍以 godpick_user_settings.json 為準。
                st.session_state["godpick_weight_reload_notice"] = "14_股神權重校正剛套用新權重，07_股神推薦將自動重新載入。"
                st.success(f"{msg}。已寫入 godpick_user_settings.json；回到 7_股神推薦後會自動偵測，或按『重新載入 14_股神權重校正』立即帶入，不需要 Ctrl+F5。")
                st.info("建議流程：套用成功 → 切到 7_股神推薦 → 確認權重來源時間已更新 → 再按開始推薦。")
            else:
                st.error(msg)

    st.markdown("---")
    st.caption("v121 Pro：此頁只做績效回測、命中追蹤、官方因子回饋、實戰主推薦回饋與建議權重；已同步 V118/V119/V120 量能趨勢防呆欄位；不重跑推薦、不連外。")


if __name__ == "__main__":
    main()

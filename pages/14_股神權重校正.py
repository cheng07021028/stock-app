# -*- coding: utf-8 -*-
from __future__ import annotations

"""
14_股神權重校正.py
v64：v60～v63 準確度強化整合版

整合內容：
- v60：上漲機率回測校正
- v61：分數組成透明化 / 因子有效性分析
- v62：風險報酬比 R/R 分析
- v63：自動權重校正建議

安全原則：
1. 不連外抓資料，不拖慢推薦頁。
2. 不刪除任何推薦紀錄。
3. 不自動覆蓋 7_股神推薦.py 權重，只輸出建議檔。
4. 舊資料缺欄位不報錯，全部安全補空。
"""

import json
import math
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st


st.set_page_config(page_title="14 股神權重校正｜v64", layout="wide")

APP_VERSION = "v64_accuracy_calibration_full"

DATA_FILES = [
    Path("godpick_records.json"),
    Path("godpick_recommend_list.json"),
    Path("godpick_latest_recommendations.json"),
]

SUGGESTION_FILE = Path("godpick_weight_suggestions.json")
PROB_CALIBRATION_FILE = Path("godpick_probability_calibration.json")
SCORE_COMPOSITION_FILE = Path("godpick_score_composition_report.json")
RR_REPORT_FILE = Path("godpick_rr_report.json")

WEIGHT_FACTORS = {
    "市場環境": ["市場環境分數", "大盤橋接分數", "大盤可參考分數", "大盤推薦同步分數"],
    "技術結構": ["技術結構分數"],
    "起漲前兆": ["起漲前兆分數", "機會股分數", "止跌轉強分數"],
    "類股熱度": ["類股熱度分數"],
    "自動因子": ["自動因子分數", "雷達分數"],
    "交易可行": ["交易可行分數", "進場時機分數"],
    "型態突破": ["型態突破分數"],
    "爆發力": ["爆發力分數"],
}

DEFAULT_WEIGHTS = {
    "市場環境": 10,
    "技術結構": 15,
    "起漲前兆": 20,
    "類股熱度": 15,
    "自動因子": 10,
    "交易可行": 10,
    "型態突破": 12,
    "爆發力": 8,
}

PERF_COLUMNS = {
    1: ["推薦後1日報酬%", "1日報酬%", "1日漲跌%", "1日績效%"],
    3: ["推薦後3日報酬%", "3日報酬%", "3日漲跌%", "3日績效%"],
    5: ["推薦後5日報酬%", "5日報酬%", "5日漲跌%", "5日績效%"],
    10: ["推薦後10日報酬%", "10日報酬%", "10日漲跌%", "10日績效%"],
    20: ["推薦後20日報酬%", "20日報酬%", "20日漲跌%", "20日績效%"],
}

PROB_COLS = ["上漲機率估計%", "上漲機率", "上漲機率估計"]
RR_COLS = ["風險報酬比", "風險報酬比_決策", "R/R", "RR"]
GRADE_COLS = ["推薦等級", "買點分級", "上漲機率等級", "大盤橋接風控", "大盤風險濾網"]


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    if v is None:
        return default
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)):
        try:
            x = float(v)
            if math.isnan(x) or math.isinf(x):
                return default
            return x
        except Exception:
            return default
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null", "--", "-", "—"}:
        return default
    s = s.replace("％", "%").replace("+", "").replace(",", "").replace("%", "")
    try:
        return float(s)
    except Exception:
        return default


def _safe_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    s = str(v)
    if s.lower() in {"nan", "none", "null"}:
        return default
    return s


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: Path, data: Any) -> bool:
    try:
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(path)
        return True
    except Exception:
        return False


def _records_from_obj(obj: Any) -> List[dict]:
    if obj is None:
        return []
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        for key in ["records", "data", "items", "recommendations", "rows"]:
            val = obj.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
        # 若是 dict of records
        vals = list(obj.values())
        if vals and all(isinstance(x, dict) for x in vals):
            return [x for x in vals if isinstance(x, dict)]
    return []


@st.cache_data(ttl=30, show_spinner=False)
def load_recommendation_records() -> pd.DataFrame:
    rows: List[dict] = []
    for p in DATA_FILES:
        obj = _read_json(p, [])
        source_rows = _records_from_obj(obj)
        for r in source_rows:
            item = dict(r)
            item["資料來源檔案"] = p.name
            rows.append(item)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # 基礎去重：同一股票、推薦日期、推薦總分大致相同時只留最後一筆。
    for c in ["股票代號", "股票名稱", "推薦日期", "建立時間", "推薦總分"]:
        if c not in df.columns:
            df[c] = None
    df["_dedup_key"] = (
        df["股票代號"].astype(str).fillna("") + "|" +
        df["推薦日期"].astype(str).fillna(df["建立時間"].astype(str)) + "|" +
        df["推薦總分"].astype(str).fillna("")
    )
    df = df.drop_duplicates("_dedup_key", keep="last").drop(columns=["_dedup_key"], errors="ignore")
    return df.reset_index(drop=True)


def first_existing_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def get_perf_col(df: pd.DataFrame, horizon: int) -> Optional[str]:
    return first_existing_col(df, PERF_COLUMNS.get(horizon, []))


def ensure_numeric_series(df: pd.DataFrame, col: Optional[str]) -> pd.Series:
    if not col or col not in df.columns:
        return pd.Series([math.nan] * len(df), index=df.index, dtype="float")
    return pd.to_numeric(df[col].map(_safe_float), errors="coerce")


def factor_series(df: pd.DataFrame, factor: str) -> pd.Series:
    cols = WEIGHT_FACTORS.get(factor, [])
    vals = []
    for c in cols:
        if c in df.columns:
            vals.append(pd.to_numeric(df[c].map(_safe_float), errors="coerce"))
    if not vals:
        return pd.Series([math.nan] * len(df), index=df.index, dtype="float")
    s = pd.concat(vals, axis=1).mean(axis=1, skipna=True)
    return s


def summarize_performance(ret: pd.Series) -> dict:
    valid = ret.dropna()
    if valid.empty:
        return {
            "樣本數": 0,
            "勝率%": None,
            "平均報酬%": None,
            "中位數報酬%": None,
            "達標率_5%以上%": None,
            "停損率_-5%以下%": None,
        }
    return {
        "樣本數": int(len(valid)),
        "勝率%": round(float((valid > 0).mean() * 100), 2),
        "平均報酬%": round(float(valid.mean()), 2),
        "中位數報酬%": round(float(valid.median()), 2),
        "達標率_5%以上%": round(float((valid >= 5).mean() * 100), 2),
        "停損率_-5%以下%": round(float((valid <= -5).mean() * 100), 2),
    }


def probability_band(prob: Any) -> str:
    p = _safe_float(prob)
    if p is None:
        return "無機率資料"
    if p >= 75:
        return "75%以上"
    if p >= 70:
        return "70%~75%"
    if p >= 65:
        return "65%~70%"
    if p >= 60:
        return "60%~65%"
    if p >= 55:
        return "55%~60%"
    if p >= 50:
        return "50%~55%"
    return "50%以下"


def rr_band(rr: Any) -> str:
    x = _safe_float(rr)
    if x is None:
        return "無R/R資料"
    if x >= 3:
        return "R/R ≥ 3"
    if x >= 2:
        return "2 ≤ R/R < 3"
    if x >= 1.5:
        return "1.5 ≤ R/R < 2"
    if x >= 1:
        return "1 ≤ R/R < 1.5"
    if x > 0:
        return "0 < R/R < 1"
    return "R/R ≤ 0"


def confidence_label(n: int) -> str:
    if n >= 120:
        return "高"
    if n >= 50:
        return "中高"
    if n >= 20:
        return "中"
    if n >= 8:
        return "低"
    return "樣本不足"


def calc_probability_calibration(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    perf_col = get_perf_col(df, horizon)
    prob_col = first_existing_col(df, PROB_COLS)
    if not perf_col or not prob_col:
        return pd.DataFrame()
    work = df.copy()
    work["_ret"] = ensure_numeric_series(work, perf_col)
    work["_prob"] = ensure_numeric_series(work, prob_col)
    work = work.dropna(subset=["_ret", "_prob"])
    if work.empty:
        return pd.DataFrame()
    work["上漲機率區間"] = work["_prob"].map(probability_band)
    order = ["75%以上", "70%~75%", "65%~70%", "60%~65%", "55%~60%", "50%~55%", "50%以下"]
    rows = []
    for band in order:
        g = work[work["上漲機率區間"] == band]
        if g.empty:
            continue
        stat = summarize_performance(g["_ret"])
        avg_prob = round(float(g["_prob"].mean()), 2)
        real_win = stat["勝率%"]
        gap = None if real_win is None else round(real_win - avg_prob, 2)
        if gap is None:
            suggestion = "無法校正"
        elif gap >= 8:
            suggestion = "實際勝率高於估計，可小幅上修此區間"
        elif gap <= -8:
            suggestion = "實際勝率低於估計，建議下修此區間"
        else:
            suggestion = "估計接近實際，暫時維持"
        rows.append({
            "上漲機率區間": band,
            "平均估計機率%": avg_prob,
            **stat,
            "勝率-估計差%": gap,
            "樣本信心": confidence_label(stat["樣本數"]),
            "校正建議": suggestion,
        })
    return pd.DataFrame(rows)


def calc_factor_effectiveness(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    perf_col = get_perf_col(df, horizon)
    if not perf_col:
        return pd.DataFrame()
    ret = ensure_numeric_series(df, perf_col)
    rows = []
    baseline = summarize_performance(ret)
    base_avg = baseline.get("平均報酬%")
    base_win = baseline.get("勝率%")

    for factor in WEIGHT_FACTORS:
        s = factor_series(df, factor)
        work = pd.DataFrame({"factor": s, "ret": ret}).dropna()
        if len(work) < 8:
            rows.append({
                "因子": factor,
                "有效樣本": int(len(work)),
                "高分組勝率%": None,
                "低分組勝率%": None,
                "勝率差%": None,
                "高分組平均報酬%": None,
                "低分組平均報酬%": None,
                "報酬差%": None,
                "建議": "樣本不足，暫不調整",
                "樣本信心": confidence_label(len(work)),
            })
            continue

        q70 = work["factor"].quantile(0.70)
        q30 = work["factor"].quantile(0.30)
        high = work[work["factor"] >= q70]
        low = work[work["factor"] <= q30]
        hs = summarize_performance(high["ret"])
        ls = summarize_performance(low["ret"])
        win_gap = None
        avg_gap = None
        if hs["勝率%"] is not None and ls["勝率%"] is not None:
            win_gap = round(hs["勝率%"] - ls["勝率%"], 2)
        if hs["平均報酬%"] is not None and ls["平均報酬%"] is not None:
            avg_gap = round(hs["平均報酬%"] - ls["平均報酬%"], 2)

        if len(work) < 20:
            advice = "樣本偏少，先觀察"
        elif (win_gap or 0) >= 8 and (avg_gap or 0) >= 1:
            advice = "建議加權"
        elif (win_gap or 0) <= -5 or (avg_gap or 0) <= -1:
            advice = "建議降權"
        else:
            advice = "建議維持"

        rows.append({
            "因子": factor,
            "有效樣本": int(len(work)),
            "高分組樣本": int(len(high)),
            "低分組樣本": int(len(low)),
            "高分組勝率%": hs["勝率%"],
            "低分組勝率%": ls["勝率%"],
            "勝率差%": win_gap,
            "高分組平均報酬%": hs["平均報酬%"],
            "低分組平均報酬%": ls["平均報酬%"],
            "報酬差%": avg_gap,
            "樣本信心": confidence_label(len(work)),
            "建議": advice,
        })
    return pd.DataFrame(rows)


def calc_score_composition(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    # v61：透明化每個因子的資料完整度、平均分、與績效相關性。
    perf_col = get_perf_col(df, horizon)
    ret = ensure_numeric_series(df, perf_col) if perf_col else pd.Series([math.nan] * len(df), index=df.index)
    rows = []
    for factor in WEIGHT_FACTORS:
        s = factor_series(df, factor)
        valid_s = s.dropna()
        coverage = 0 if len(df) == 0 else round(float(s.notna().mean() * 100), 2)
        corr = None
        try:
            tmp = pd.DataFrame({"s": s, "ret": ret}).dropna()
            if len(tmp) >= 8:
                corr = round(float(tmp["s"].corr(tmp["ret"])), 4)
        except Exception:
            corr = None
        rows.append({
            "因子": factor,
            "目前權重%": DEFAULT_WEIGHTS.get(factor, 0),
            "資料覆蓋率%": coverage,
            "平均分數": round(float(valid_s.mean()), 2) if not valid_s.empty else None,
            "中位數分數": round(float(valid_s.median()), 2) if not valid_s.empty else None,
            "最高分": round(float(valid_s.max()), 2) if not valid_s.empty else None,
            "最低分": round(float(valid_s.min()), 2) if not valid_s.empty else None,
            f"與{horizon}日報酬相關": corr,
            "透明化說明": "正相關越高代表此因子越能解釋後續報酬；覆蓋率低則不宜過度調權。",
        })
    return pd.DataFrame(rows)


def calc_rr_report(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    perf_col = get_perf_col(df, horizon)
    rr_col = first_existing_col(df, RR_COLS)
    if not perf_col or not rr_col:
        return pd.DataFrame()
    ret = ensure_numeric_series(df, perf_col)
    rr = ensure_numeric_series(df, rr_col)
    work = pd.DataFrame({"rr": rr, "ret": ret}).dropna()
    if work.empty:
        return pd.DataFrame()
    work["R/R區間"] = work["rr"].map(rr_band)
    order = ["R/R ≥ 3", "2 ≤ R/R < 3", "1.5 ≤ R/R < 2", "1 ≤ R/R < 1.5", "0 < R/R < 1", "R/R ≤ 0"]
    rows = []
    for b in order:
        g = work[work["R/R區間"] == b]
        if g.empty:
            continue
        stat = summarize_performance(g["ret"])
        rows.append({
            "R/R區間": b,
            "平均R/R": round(float(g["rr"].mean()), 2),
            **stat,
            "樣本信心": confidence_label(stat["樣本數"]),
            "解讀": "若高R/R區間勝率與平均報酬同步較佳，代表風險報酬比具參考價值；若沒有，需檢查支撐壓力計算。",
        })
    return pd.DataFrame(rows)


def calc_group_performance(df: pd.DataFrame, horizon: int, group_col: str) -> pd.DataFrame:
    perf_col = get_perf_col(df, horizon)
    if not perf_col or group_col not in df.columns:
        return pd.DataFrame()
    ret = ensure_numeric_series(df, perf_col)
    work = df.copy()
    work["_ret"] = ret
    work[group_col] = work[group_col].map(lambda x: _safe_str(x, "未分類"))
    rows = []
    for name, g in work.dropna(subset=["_ret"]).groupby(group_col):
        stat = summarize_performance(g["_ret"])
        if stat["樣本數"] <= 0:
            continue
        rows.append({group_col: name, **stat, "樣本信心": confidence_label(stat["樣本數"])})
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["樣本數", "勝率%", "平均報酬%"], ascending=[False, False, False])
    return out


def build_weight_suggestions(effect_df: pd.DataFrame) -> pd.DataFrame:
    if effect_df.empty:
        return pd.DataFrame()
    rows = []
    total_delta = 0
    for _, r in effect_df.iterrows():
        factor = r.get("因子")
        current = DEFAULT_WEIGHTS.get(factor, 0)
        advice = _safe_str(r.get("建議"))
        sample = int(_safe_float(r.get("有效樣本"), 0) or 0)
        win_gap = _safe_float(r.get("勝率差%"), 0) or 0
        avg_gap = _safe_float(r.get("報酬差%"), 0) or 0
        delta = 0
        if sample >= 50:
            if advice == "建議加權":
                delta = 2 if win_gap >= 12 or avg_gap >= 2 else 1
            elif advice == "建議降權":
                delta = -2 if win_gap <= -8 or avg_gap <= -2 else -1
        elif sample >= 20:
            if advice == "建議加權":
                delta = 1
            elif advice == "建議降權":
                delta = -1
        suggested = max(3, current + delta)
        rows.append({
            "因子": factor,
            "目前權重%": current,
            "建議調整%": delta,
            "初步建議權重%": suggested,
            "建議": advice,
            "樣本信心": r.get("樣本信心"),
            "勝率差%": r.get("勝率差%"),
            "報酬差%": r.get("報酬差%"),
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    # 正規化為 100%，避免使用者直接參考時總和不為100。
    total = out["初步建議權重%"].sum()
    if total > 0:
        out["建議新權重%"] = (out["初步建議權重%"] / total * 100).round(1)
        diff = round(100 - out["建議新權重%"].sum(), 1)
        if abs(diff) >= 0.1:
            idx = out["建議新權重%"].idxmax()
            out.loc[idx, "建議新權重%"] = round(out.loc[idx, "建議新權重%"] + diff, 1)
    else:
        out["建議新權重%"] = out["目前權重%"]
    return out.drop(columns=["初步建議權重%"], errors="ignore")


def render_intro():
    st.title("14 股神權重校正｜v64 準確度強化整合版")
    st.caption("整合 v60 上漲機率回測校正、v61 分數組成透明化、v62 風險報酬比、v63 自動權重校正建議。")
    st.info("此頁只讀取既有推薦紀錄，不連外、不重跑推薦、不自動覆蓋權重；所有建議都需要人工確認後再套用。")


def render_quality(df: pd.DataFrame, horizon: int):
    st.subheader("資料完整度檢查")
    perf_col = get_perf_col(df, horizon)
    prob_col = first_existing_col(df, PROB_COLS)
    rr_col = first_existing_col(df, RR_COLS)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("總推薦紀錄", f"{len(df):,}")
    c2.metric(f"{horizon}日績效欄", perf_col or "缺少")
    c3.metric("上漲機率欄", prob_col or "缺少")
    c4.metric("R/R欄", rr_col or "缺少")

    notes = []
    if not perf_col:
        notes.append(f"缺少 {horizon} 日績效欄，請先到 8 或 10 更新推薦後績效。")
    if not prob_col:
        notes.append("缺少上漲機率欄，請確認 7/8/10 已更新到 v59 以上。")
    if not rr_col:
        notes.append("缺少風險報酬比欄，v62 分析會略過，但不影響其他校正。")
    if notes:
        st.warning("\n".join(notes))
    else:
        st.success("必要欄位已存在，可以進行校正分析。")


def render_download_buttons(payloads: Dict[str, Any]):
    st.subheader("輸出建議檔")
    c1, c2, c3, c4 = st.columns(4)
    if c1.button("寫出權重建議 JSON", use_container_width=True):
        ok = _write_json(SUGGESTION_FILE, payloads.get("weight_suggestions", {}))
        st.success(f"已寫出 {SUGGESTION_FILE}") if ok else st.error("寫出失敗")
    if c2.button("寫出機率校正 JSON", use_container_width=True):
        ok = _write_json(PROB_CALIBRATION_FILE, payloads.get("probability_calibration", {}))
        st.success(f"已寫出 {PROB_CALIBRATION_FILE}") if ok else st.error("寫出失敗")
    if c3.button("寫出分數組成 JSON", use_container_width=True):
        ok = _write_json(SCORE_COMPOSITION_FILE, payloads.get("score_composition", {}))
        st.success(f"已寫出 {SCORE_COMPOSITION_FILE}") if ok else st.error("寫出失敗")
    if c4.button("寫出R/R分析 JSON", use_container_width=True):
        ok = _write_json(RR_REPORT_FILE, payloads.get("rr_report", {}))
        st.success(f"已寫出 {RR_REPORT_FILE}") if ok else st.error("寫出失敗")


def main():
    render_intro()

    df = load_recommendation_records()
    if df.empty:
        st.error("目前沒有讀到推薦紀錄。請先從 7_股神推薦 匯入 8_股神推薦紀錄 或 10_推薦清單。")
        return

    with st.sidebar:
        st.header("v64 校正設定")
        horizon = st.selectbox("校正週期", [1, 3, 5, 10, 20], index=2, help="建議優先看 5日 或 10日。")
        min_sample = st.slider("有效樣本提醒門檻", 5, 200, 30, step=5)
        st.caption("樣本太少時只給觀察，不建議直接調權重。")

    render_quality(df, horizon)

    perf_col = get_perf_col(df, horizon)
    if not perf_col:
        st.stop()

    base_stat = summarize_performance(ensure_numeric_series(df, perf_col))
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("有效績效樣本", base_stat["樣本數"])
    c2.metric("整體勝率", "—" if base_stat["勝率%"] is None else f"{base_stat['勝率%']}%")
    c3.metric("平均報酬", "—" if base_stat["平均報酬%"] is None else f"{base_stat['平均報酬%']}%")
    c4.metric("達標率 ≥5%", "—" if base_stat["達標率_5%以上%"] is None else f"{base_stat['達標率_5%以上%']}%")
    c5.metric("停損率 ≤-5%", "—" if base_stat["停損率_-5%以下%"] is None else f"{base_stat['停損率_-5%以下%']}%")

    if base_stat["樣本數"] < min_sample:
        st.warning(f"目前有效樣本 {base_stat['樣本數']} 筆，低於你設定的 {min_sample} 筆，建議先觀察，不要急著調權重。")

    prob_df = calc_probability_calibration(df, horizon)
    effect_df = calc_factor_effectiveness(df, horizon)
    score_df = calc_score_composition(df, horizon)
    rr_df = calc_rr_report(df, horizon)
    weight_df = build_weight_suggestions(effect_df)

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "v60 上漲機率校正",
        "v61 分數組成透明化",
        "v62 風險報酬比 R/R",
        "v63 權重校正建議",
        "分群勝率",
        "原始資料檢視",
    ])

    with tab1:
        st.subheader("v60 上漲機率回測校正")
        if prob_df.empty:
            st.warning("缺少上漲機率或績效欄位，無法進行機率校正。")
        else:
            st.dataframe(prob_df, use_container_width=True, hide_index=True)
            st.caption("重點看『勝率-估計差%』：負值太大代表上漲機率估太樂觀，正值太大代表估得太保守。")

    with tab2:
        st.subheader("v61 分數組成透明化")
        if score_df.empty:
            st.warning("尚無可分析的分數因子。")
        else:
            st.dataframe(score_df, use_container_width=True, hide_index=True)
            st.caption("這裡不是調權重結果，而是讓你看每個因子資料覆蓋率、平均分與績效相關性。")

        st.markdown("### 因子高低分組績效")
        if effect_df.empty:
            st.warning("缺少績效欄位或樣本不足。")
        else:
            st.dataframe(effect_df, use_container_width=True, hide_index=True)

    with tab3:
        st.subheader("v62 風險報酬比 R/R 分析")
        if rr_df.empty:
            st.warning("缺少風險報酬比或績效欄位，無法分析 R/R。")
        else:
            st.dataframe(rr_df, use_container_width=True, hide_index=True)
            st.caption("如果高 R/R 區間沒有比較好的勝率或平均報酬，代表支撐壓力 / 目標價計算要再校正。")

    with tab4:
        st.subheader("v63 自動權重校正建議")
        if weight_df.empty:
            st.warning("尚無足夠資料產生權重建議。")
        else:
            st.dataframe(weight_df, use_container_width=True, hide_index=True)
            total = pd.to_numeric(weight_df.get("建議新權重%", pd.Series(dtype=float)), errors="coerce").sum()
            st.info(f"建議新權重總和：{total:.1f}%。此建議不會自動寫回 7_股神推薦，請人工確認。")

    with tab5:
        st.subheader("分群勝率檢查")
        for col in GRADE_COLS:
            if col in df.columns:
                with st.expander(f"依 {col} 分群", expanded=False):
                    gdf = calc_group_performance(df, horizon, col)
                    if gdf.empty:
                        st.caption("沒有足夠資料。")
                    else:
                        st.dataframe(gdf, use_container_width=True, hide_index=True)

    with tab6:
        st.subheader("原始推薦資料樣本")
        show_cols = [c for c in ["股票代號", "股票名稱", "推薦日期", "推薦總分", "上漲機率估計%", perf_col, "推薦等級", "買點分級", "風險報酬比", "大盤橋接風控", "資料來源檔案"] if c in df.columns]
        st.dataframe(df[show_cols].head(300), use_container_width=True, hide_index=True)

    payloads = {
        "version": APP_VERSION,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "horizon": horizon,
        "base_stat": base_stat,
        "weight_suggestions": weight_df.to_dict(orient="records") if not weight_df.empty else [],
        "probability_calibration": prob_df.to_dict(orient="records") if not prob_df.empty else [],
        "score_composition": score_df.to_dict(orient="records") if not score_df.empty else [],
        "rr_report": rr_df.to_dict(orient="records") if not rr_df.empty else [],
    }
    render_download_buttons(payloads)

    st.markdown("---")
    st.caption("v64 安全聲明：此頁只產生校正建議，不會修改 7_股神推薦 的權重設定，也不會刪除任何 JSON 紀錄。")


if __name__ == "__main__":
    main()

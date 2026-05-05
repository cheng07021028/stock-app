# -*- coding: utf-8 -*-
from __future__ import annotations

"""
godpick_weight_calibration.py
v66 Pro：績效回測＋期望值＋分層權重＋防過擬合

設計原則：
- 不連外，不重新推薦，只讀既有推薦紀錄 / 推薦清單。
- 不自動覆蓋權重；只有頁面按下套用才寫入 godpick_user_settings.json。
- 權重單次調整有限制，避免短期資料過擬合。
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import json
import math
import os

import pandas as pd

DATA_FILES = [
    Path("godpick_records.json"),
    Path("godpick_recommend_list.json"),
    Path("godpick_latest_recommendations.json"),
]

SETTINGS_FILE = Path("godpick_user_settings.json")
SUGGESTION_FILE = Path("godpick_weight_suggestions.json")

DEFAULT_WEIGHTS: Dict[str, int] = {
    "市場環境": 10,
    "技術結構": 15,
    "起漲前兆": 20,
    "類股熱度": 15,
    "自動因子": 10,
    "交易可行": 10,
    "型態突破": 12,
    "爆發力": 8,
}

FACTOR_COLUMNS: Dict[str, List[str]] = {
    "市場環境": ["市場環境分數", "大盤橋接分數", "大盤可參考分數", "大盤推薦同步分數", "大盤情境分數"],
    "技術結構": ["技術結構分數", "趨勢分數", "均線分數", "技術面分數"],
    "起漲前兆": ["起漲前兆分數", "機會股分數", "止跌轉強分數", "低檔位分數", "拉回承接分數"],
    "類股熱度": ["類股熱度分數", "類股強度分數", "族群強度分數", "族群資金流分數"],
    "自動因子": ["自動因子分數", "雷達分數", "自動因子總分", "股神決策分數"],
    "交易可行": ["交易可行分數", "進場時機分數", "支撐回測分數", "動態建議倉位%"],
    "型態突破": ["型態突破分數", "突破分數", "K線驗證分數", "型態分數"],
    "爆發力": ["爆發力分數", "量能分數", "量價分數", "主升段分數"],
}

PERF_COLUMNS: Dict[int, List[str]] = {
    1: ["推薦後1日報酬%", "推薦後1日%", "1日報酬%", "1日漲跌%", "1日績效%", "1日後報酬%"],
    3: ["推薦後3日報酬%", "推薦後3日%", "3日報酬%", "3日漲跌%", "3日績效%", "3日後報酬%"],
    5: ["推薦後5日報酬%", "推薦後5日%", "5日報酬%", "5日漲跌%", "5日績效%", "5日後報酬%"],
    10: ["推薦後10日報酬%", "推薦後10日%", "10日報酬%", "10日漲跌%", "10日績效%", "10日後報酬%"],
    20: ["推薦後20日報酬%", "推薦後20日%", "20日報酬%", "20日漲跌%", "20日績效%", "20日後報酬%"],
}

MARKET_COLUMNS = ["大盤情境", "大盤狀態", "大盤橋接風控", "市場狀態", "大盤模式"]
CATEGORY_COLUMNS = ["類別", "產業", "族群", "主題類別", "正式產業別"]
DATE_COLUMNS = ["推薦日期", "建立日期", "建立時間", "推薦時間"]
PROB_COLUMNS = ["上漲機率估計%", "上漲機率%", "上漲機率", "上漲機率估計"]
RR_COLUMNS = ["風險報酬比", "風險報酬比_決策", "R/R", "RR"]


def safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
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
        x = float(s)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def safe_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    s = str(v).strip()
    if s.lower() in {"nan", "none", "null"}:
        return default
    return s


def read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def write_json(path: Path, data: Any) -> Tuple[bool, str]:
    try:
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(path)
        return True, f"已寫入 {path}"
    except Exception as e:
        return False, f"寫入 {path} 失敗：{e}"


def records_from_obj(obj: Any) -> List[dict]:
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        for key in ["records", "data", "items", "recommendations", "rows", "latest", "list"]:
            val = obj.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
        vals = list(obj.values())
        if vals and all(isinstance(x, dict) for x in vals):
            return [x for x in vals if isinstance(x, dict)]
    return []


def load_recommendation_records(files: Iterable[Path] = DATA_FILES) -> pd.DataFrame:
    rows: List[dict] = []
    for p in files:
        obj = read_json(p, [])
        for r in records_from_obj(obj):
            item = dict(r)
            item["資料來源檔案"] = p.name
            rows.append(item)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for c in ["股票代號", "股票名稱", "推薦日期", "推薦時間", "推薦總分"]:
        if c not in df.columns:
            df[c] = ""
    # 保守去重：同股票同日期同分數，以最後來源為準。
    key = (
        df["股票代號"].astype(str).fillna("") + "|" +
        df["推薦日期"].astype(str).fillna("") + "|" +
        df["推薦時間"].astype(str).fillna("") + "|" +
        df["推薦總分"].astype(str).fillna("")
    )
    df = df.assign(_dedup_key=key).drop_duplicates("_dedup_key", keep="last").drop(columns=["_dedup_key"], errors="ignore")
    return df.reset_index(drop=True)


def first_existing_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def numeric_series(df: pd.DataFrame, col: Optional[str]) -> pd.Series:
    if not col or col not in df.columns:
        return pd.Series([math.nan] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[col].map(safe_float), errors="coerce")


def factor_series(df: pd.DataFrame, factor: str) -> pd.Series:
    vals = []
    for c in FACTOR_COLUMNS.get(factor, []):
        if c in df.columns:
            vals.append(numeric_series(df, c))
    if not vals:
        return pd.Series([math.nan] * len(df), index=df.index, dtype="float64")
    return pd.concat(vals, axis=1).mean(axis=1, skipna=True)


def summarize_returns(ret: pd.Series) -> Dict[str, Any]:
    v = pd.to_numeric(ret, errors="coerce").dropna()
    if v.empty:
        return {
            "樣本數": 0, "勝率%": None, "平均報酬%": None, "中位數報酬%": None,
            "平均獲利%": None, "平均虧損%": None, "期望值%": None,
            "達標率_5%以上%": None, "停損率_-5%以下%": None, "最大漲幅%": None, "最大回撤%": None,
        }
    win = v[v > 0]
    loss = v[v < 0]
    win_rate = float((v > 0).mean())
    avg_gain = float(win.mean()) if not win.empty else 0.0
    avg_loss_abs = abs(float(loss.mean())) if not loss.empty else 0.0
    expectancy = win_rate * avg_gain - (1 - win_rate) * avg_loss_abs
    return {
        "樣本數": int(len(v)),
        "勝率%": round(win_rate * 100, 2),
        "平均報酬%": round(float(v.mean()), 2),
        "中位數報酬%": round(float(v.median()), 2),
        "平均獲利%": round(avg_gain, 2),
        "平均虧損%": round(avg_loss_abs, 2),
        "期望值%": round(expectancy, 2),
        "達標率_5%以上%": round(float((v >= 5).mean() * 100), 2),
        "停損率_-5%以下%": round(float((v <= -5).mean() * 100), 2),
        "最大漲幅%": round(float(v.max()), 2),
        "最大回撤%": round(float(v.min()), 2),
    }


def confidence_label(n: int) -> str:
    if n >= 120:
        return "高"
    if n >= 50:
        return "中高"
    if n >= 30:
        return "中"
    if n >= 12:
        return "低"
    return "樣本不足"


def profile_name_by_horizon(horizon: int) -> str:
    if horizon <= 3:
        return "短線飆股權重"
    if horizon <= 10:
        return "波段主升權重"
    return "趨勢穩健權重"


def calc_factor_effectiveness(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    perf_col = first_existing_col(df, PERF_COLUMNS.get(horizon, []))
    if not perf_col:
        return pd.DataFrame()
    ret = numeric_series(df, perf_col)
    base = summarize_returns(ret)
    rows: List[dict] = []
    for factor in DEFAULT_WEIGHTS:
        f = factor_series(df, factor)
        work = pd.DataFrame({"factor": f, "ret": ret}).dropna()
        if len(work) < 8:
            rows.append({
                "因子": factor, "有效樣本": int(len(work)), "目前權重%": DEFAULT_WEIGHTS[factor],
                "高分組勝率%": None, "低分組勝率%": None, "勝率差%": None,
                "高分組平均報酬%": None, "低分組平均報酬%": None, "報酬差%": None,
                "高分組期望值%": None, "低分組期望值%": None, "期望值差%": None,
                "資料覆蓋率%": round(float(f.notna().mean() * 100), 2) if len(df) else 0,
                "樣本信心": confidence_label(len(work)), "建議": "樣本不足，暫不調整",
            })
            continue
        q70 = work["factor"].quantile(0.70)
        q30 = work["factor"].quantile(0.30)
        high = work[work["factor"] >= q70]
        low = work[work["factor"] <= q30]
        hs = summarize_returns(high["ret"])
        ls = summarize_returns(low["ret"])
        win_gap = None if hs["勝率%"] is None or ls["勝率%"] is None else round(hs["勝率%"] - ls["勝率%"], 2)
        avg_gap = None if hs["平均報酬%"] is None or ls["平均報酬%"] is None else round(hs["平均報酬%"] - ls["平均報酬%"], 2)
        exp_gap = None if hs["期望值%"] is None or ls["期望值%"] is None else round(hs["期望值%"] - ls["期望值%"], 2)
        if len(work) < 30:
            advice = "樣本偏少，先觀察"
        elif (exp_gap or 0) >= 1.5 and (win_gap or 0) >= 6:
            advice = "建議加權"
        elif (exp_gap or 0) <= -1.0 or (win_gap or 0) <= -5:
            advice = "建議降權"
        else:
            advice = "建議維持"
        rows.append({
            "因子": factor,
            "有效樣本": int(len(work)),
            "目前權重%": DEFAULT_WEIGHTS[factor],
            "資料覆蓋率%": round(float(f.notna().mean() * 100), 2) if len(df) else 0,
            "高分組樣本": int(len(high)),
            "低分組樣本": int(len(low)),
            "高分組勝率%": hs["勝率%"],
            "低分組勝率%": ls["勝率%"],
            "勝率差%": win_gap,
            "高分組平均報酬%": hs["平均報酬%"],
            "低分組平均報酬%": ls["平均報酬%"],
            "報酬差%": avg_gap,
            "高分組期望值%": hs["期望值%"],
            "低分組期望值%": ls["期望值%"],
            "期望值差%": exp_gap,
            "樣本信心": confidence_label(len(work)),
            "建議": advice,
        })
    return pd.DataFrame(rows)


def normalize_weights(weights: Dict[str, float], min_w: int = 5, max_w: int = 25) -> Dict[str, int]:
    vals = {k: max(min_w, min(max_w, float(weights.get(k, DEFAULT_WEIGHTS.get(k, min_w))))) for k in DEFAULT_WEIGHTS}
    total = sum(vals.values())
    if total <= 0:
        return DEFAULT_WEIGHTS.copy()
    scaled = {k: int(round(v / total * 100)) for k, v in vals.items()}
    # 修正四捨五入差異
    diff = 100 - sum(scaled.values())
    guard = 0
    while diff != 0 and guard < 100:
        guard += 1
        if diff > 0:
            k = max(scaled, key=lambda x: vals[x] - scaled[x])
            if scaled[k] < max_w:
                scaled[k] += 1
                diff -= 1
            else:
                break
        else:
            k = max(scaled, key=lambda x: scaled[x])
            if scaled[k] > min_w:
                scaled[k] -= 1
                diff += 1
            else:
                break
    # 若限制導致仍不等於 100，補到最大因子
    if sum(scaled.values()) != 100:
        k = max(scaled, key=scaled.get)
        scaled[k] += 100 - sum(scaled.values())
    return {k: int(v) for k, v in scaled.items()}


def suggest_weights(effect_df: pd.DataFrame, current_weights: Optional[Dict[str, int]] = None, *, max_step: int = 5) -> pd.DataFrame:
    current = normalize_weights(current_weights or DEFAULT_WEIGHTS, min_w=3, max_w=30)
    if effect_df.empty:
        return pd.DataFrame([{"因子": k, "目前權重%": v, "建議新權重%": v, "建議調整%": 0, "調整理由": "無有效績效資料"} for k, v in current.items()])
    raw: Dict[str, float] = {}
    rows = []
    for _, r in effect_df.iterrows():
        factor = safe_str(r.get("因子"))
        if factor not in current:
            continue
        n = int(safe_float(r.get("有效樣本"), 0) or 0)
        win_gap = safe_float(r.get("勝率差%"), 0) or 0
        exp_gap = safe_float(r.get("期望值差%"), 0) or 0
        coverage = safe_float(r.get("資料覆蓋率%"), 0) or 0
        advice = safe_str(r.get("建議"))
        delta = 0.0
        reason = []
        if n < 30:
            delta = 0
            reason.append("樣本低於30，防過擬合不調整")
        else:
            if advice == "建議加權":
                delta += 2.0
                if exp_gap >= 2.5:
                    delta += 1.5
                if win_gap >= 12:
                    delta += 1.0
                reason.append("高分組勝率/期望值優於低分組")
            elif advice == "建議降權":
                delta -= 2.0
                if exp_gap <= -2:
                    delta -= 1.5
                if win_gap <= -10:
                    delta -= 1.0
                reason.append("高分組績效沒有優勢或反向")
            else:
                reason.append("高低分組差異不明顯")
        if coverage < 40:
            delta *= 0.5
            reason.append("覆蓋率偏低，調整幅度減半")
        if n < 50:
            delta *= 0.7
            reason.append("樣本未達50，調整幅度保守")
        delta = max(-max_step, min(max_step, delta))
        raw[factor] = current[factor] + delta
        rows.append({
            "因子": factor,
            "目前權重%": current[factor],
            "建議調整%": round(delta, 1),
            "調整理由": "；".join(reason),
            "樣本數": n,
            "樣本信心": confidence_label(n),
            "勝率差%": r.get("勝率差%"),
            "期望值差%": r.get("期望值差%"),
            "資料覆蓋率%": r.get("資料覆蓋率%"),
        })
    for k in current:
        raw.setdefault(k, current[k])
    final = normalize_weights(raw, min_w=5, max_w=25)
    out = pd.DataFrame(rows)
    if out.empty:
        out = pd.DataFrame([{"因子": k, "目前權重%": current[k], "建議調整%": 0, "調整理由": "無可分析資料"} for k in current])
    out["建議新權重%"] = out["因子"].map(final)
    out["實際差異%"] = out["建議新權重%"] - out["目前權重%"]
    return out[["因子", "目前權重%", "建議新權重%", "實際差異%", "建議調整%", "調整理由", "樣本數", "樣本信心", "勝率差%", "期望值差%", "資料覆蓋率%"]]


def filter_by_market(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == "全部":
        return df
    col = first_existing_col(df, MARKET_COLUMNS)
    if not col:
        return pd.DataFrame()
    s = df[col].astype(str)
    if mode == "多頭":
        mask = s.str.contains("多|強|偏多|風險低|進攻", na=False)
    elif mode == "盤整":
        mask = s.str.contains("盤|震|中性|觀望", na=False)
    elif mode == "空頭":
        mask = s.str.contains("空|弱|風險高|防守", na=False)
    else:
        mask = pd.Series([False] * len(df), index=df.index)
    return df[mask].copy()


def calc_profile_bundle(df: pd.DataFrame, horizons: Iterable[int] = (1, 3, 5, 10, 20), current_weights: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    bundle: Dict[str, Any] = {
        "version": "v66_pro_expectancy_layered_antioverfit",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "base_weights": current_weights or DEFAULT_WEIGHTS,
        "profiles": {},
        "factor_effectiveness": {},
        "quality": {},
    }
    for horizon in horizons:
        perf_col = first_existing_col(df, PERF_COLUMNS.get(horizon, []))
        if not perf_col:
            bundle["quality"][str(horizon)] = {"status": "missing_perf_col", "message": f"缺少{horizon}日績效欄"}
            continue
        ret = numeric_series(df, perf_col)
        base_stat = summarize_returns(ret)
        effect = calc_factor_effectiveness(df, horizon)
        weights = suggest_weights(effect, current_weights)
        name = profile_name_by_horizon(horizon)
        bundle["profiles"][name] = {
            "horizon": horizon,
            "performance_col": perf_col,
            "base_stat": base_stat,
            "weights": dict(zip(weights["因子"], weights["建議新權重%"])),
            "table": weights.to_dict(orient="records"),
            "note": "權重依勝率差、期望值差、覆蓋率、樣本數保守校正；單次調整有限制。",
        }
        bundle["factor_effectiveness"][str(horizon)] = effect.to_dict(orient="records") if not effect.empty else []
        bundle["quality"][str(horizon)] = {"status": "ok", **base_stat}
    return bundle


def calc_market_bundles(df: pd.DataFrame, horizon: int, current_weights: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for mode in ["多頭", "盤整", "空頭"]:
        sub = filter_by_market(df, mode)
        if sub.empty or len(sub) < 12:
            out[mode] = {"status": "樣本不足或缺少大盤情境欄", "樣本數": int(len(sub))}
            continue
        effect = calc_factor_effectiveness(sub, horizon)
        weights = suggest_weights(effect, current_weights)
        out[mode] = {
            "status": "ok",
            "樣本數": int(len(sub)),
            "weights": dict(zip(weights["因子"], weights["建議新權重%"])),
            "table": weights.to_dict(orient="records"),
        }
    return out


def calc_category_bundles(df: pd.DataFrame, horizon: int, current_weights: Optional[Dict[str, int]] = None, top_n: int = 8) -> Dict[str, Any]:
    col = first_existing_col(df, CATEGORY_COLUMNS)
    if not col:
        return {"status": "缺少類別/產業欄位"}
    ret_col = first_existing_col(df, PERF_COLUMNS.get(horizon, []))
    if not ret_col:
        return {"status": f"缺少{horizon}日績效欄"}
    work = df.copy()
    work[col] = work[col].map(lambda x: safe_str(x, "未分類"))
    counts = work[col].value_counts().head(top_n)
    result: Dict[str, Any] = {"category_col": col, "items": {}}
    for cat, n in counts.items():
        sub = work[work[col] == cat]
        if len(sub) < 10:
            result["items"][cat] = {"status": "樣本不足", "樣本數": int(len(sub))}
            continue
        effect = calc_factor_effectiveness(sub, horizon)
        weights = suggest_weights(effect, current_weights)
        stat = summarize_returns(numeric_series(sub, ret_col))
        result["items"][cat] = {
            "status": "ok",
            "樣本數": int(len(sub)),
            "base_stat": stat,
            "weights": dict(zip(weights["因子"], weights["建議新權重%"])),
            "table": weights.to_dict(orient="records"),
        }
    return result


def probability_calibration(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    pcol = first_existing_col(df, PROB_COLUMNS)
    rcol = first_existing_col(df, PERF_COLUMNS.get(horizon, []))
    if not pcol or not rcol:
        return pd.DataFrame()
    prob = numeric_series(df, pcol)
    ret = numeric_series(df, rcol)
    work = pd.DataFrame({"prob": prob, "ret": ret}).dropna()
    if work.empty:
        return pd.DataFrame()
    bins = [0, 50, 55, 60, 65, 70, 75, 100]
    labels = ["≤50", "50-55", "55-60", "60-65", "65-70", "70-75", ">75"]
    work["機率區間"] = pd.cut(work["prob"], bins=bins, labels=labels, include_lowest=True, right=True)
    rows = []
    for label in labels:
        g = work[work["機率區間"].astype(str) == label]
        if g.empty:
            continue
        stat = summarize_returns(g["ret"])
        avg_prob = round(float(g["prob"].mean()), 2)
        gap = None if stat["勝率%"] is None else round(stat["勝率%"] - avg_prob, 2)
        rows.append({"機率區間": label, "平均估計機率%": avg_prob, **stat, "勝率-估計差%": gap, "校正建議": "上修" if gap is not None and gap >= 8 else ("下修" if gap is not None and gap <= -8 else "維持")})
    return pd.DataFrame(rows)


def rr_analysis(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    rr_col = first_existing_col(df, RR_COLUMNS)
    ret_col = first_existing_col(df, PERF_COLUMNS.get(horizon, []))
    if not rr_col or not ret_col:
        return pd.DataFrame()
    rr = numeric_series(df, rr_col)
    ret = numeric_series(df, ret_col)
    work = pd.DataFrame({"rr": rr, "ret": ret}).dropna()
    if work.empty:
        return pd.DataFrame()
    labels = ["<1", "1-1.5", "1.5-2", "2-3", ">=3"]
    bins = [-9999, 1, 1.5, 2, 3, 9999]
    work["R/R區間"] = pd.cut(work["rr"], bins=bins, labels=labels, include_lowest=True, right=False)
    rows = []
    for label in labels:
        g = work[work["R/R區間"].astype(str) == label]
        if g.empty:
            continue
        stat = summarize_returns(g["ret"])
        rows.append({"R/R區間": label, "平均R/R": round(float(g["rr"].mean()), 2), **stat, "樣本信心": confidence_label(stat["樣本數"])})
    return pd.DataFrame(rows)


def load_current_settings() -> Dict[str, Any]:
    payload = read_json(SETTINGS_FILE, {})
    if not isinstance(payload, dict):
        payload = {}
    return payload


def current_weight_map() -> Dict[str, int]:
    payload = load_current_settings()
    raw = payload.get("applied_weights") if isinstance(payload, dict) else None
    if not isinstance(raw, dict):
        raw = DEFAULT_WEIGHTS
    return normalize_weights({k: safe_float(v, DEFAULT_WEIGHTS.get(k, 0)) or 0 for k, v in raw.items()}, min_w=3, max_w=30)


def save_applied_weights(weights: Dict[str, int], profile_name: str = "manual") -> Tuple[bool, str]:
    existing = load_current_settings()
    if not isinstance(existing, dict):
        existing = {}
    payload = {
        **existing,
        "original_default_weights": existing.get("original_default_weights", DEFAULT_WEIGHTS),
        "applied_weights": normalize_weights(weights, min_w=3, max_w=30),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": "godpick_v66_weight_calibration_applied",
        "last_weight_calibration_profile": profile_name,
    }
    return write_json(SETTINGS_FILE, payload)


def save_suggestion_bundle(bundle: Dict[str, Any]) -> Tuple[bool, str]:
    return write_json(SUGGESTION_FILE, bundle)

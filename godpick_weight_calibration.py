# -*- coding: utf-8 -*-
from __future__ import annotations

"""
godpick_weight_calibration.py
v104 Pro：夜間隔日準確率回饋＋績效代理樣本＋多來源防卡＋防過擬合

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
import base64

try:
    import requests
except Exception:
    requests = None

import pandas as pd

DATA_FILES = [
    Path("godpick_records.json"),
    Path("godpick_recommend_list.json"),
    Path("godpick_latest_recommendations.json"),
]

SETTINGS_FILE = Path("godpick_user_settings.json")
SUGGESTION_FILE = Path("godpick_weight_suggestions.json")


# v99：07/8/10 夜間隔日股神新增欄位。
# 14_股神權重校正仍維持 7_股神推薦原本 8 大權重，不新增不相容權重名稱；
# 但校正時會把夜間股神總分、隔日進場分數、法人籌碼、基本面、估值等欄位納入各因子來源。
NIGHT_SCORE_COLUMNS: Dict[str, List[str]] = {
    "市場環境": ["市場環境分數", "大盤環境分數", "大盤風控分數"],
    "技術結構": ["技術趨勢分數", "技術面分數", "趨勢分數", "夜間股神總分"],
    "起漲前兆": ["隔日進場分數", "波段潛力分數", "營收成長分數", "EPS成長分數", "基本面成長分數"],
    "類股熱度": ["類股熱度分數", "類股強度分數", "產業強度分數"],
    "自動因子": ["夜間股神總分", "隔日實戰排序分", "推薦總分"],
    "交易可行": ["隔日進場分數", "估值風險分數", "波段潛力分數", "R/R分數"],
    "型態突破": ["技術趨勢分數", "隔日進場分數", "型態突破分數"],
    "爆發力": ["量價動能分數", "法人籌碼分數", "大戶鎖碼分數", "夜間股神總分"],
}

NIGHT_TEXT_COLUMNS: Dict[str, List[str]] = {
    "市場環境": ["大盤情境", "大盤狀態", "大盤分層"],
    "技術結構": ["進場型態_隔日", "夜間股神建議", "隔日作戰策略", "買點分級"],
    "起漲前兆": ["進場型態_隔日", "隔日建議動作", "夜間股神建議", "隔日作戰策略"],
    "類股熱度": ["類別", "產業", "族群", "主題類別", "正式產業別"],
    "自動因子": ["資料完整度", "夜間股神建議", "股神推論邏輯"],
    "交易可行": ["隔日建議動作", "夜間股神建議", "隔日作戰策略", "風險說明"],
    "型態突破": ["進場型態_隔日", "隔日建議動作", "隔日作戰策略"],
    "爆發力": ["進場型態_隔日", "夜間股神建議", "股神推論邏輯"],
}

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
    "市場環境": ["市場環境分數", "大盤橋接分數", "大盤可參考分數", "大盤推薦同步分數", "大盤情境分數", "大盤風控分數"],
    "技術結構": ["技術結構分數", "趨勢因子分數", "趨勢分數", "均線分數", "技術面分數"],
    "起漲前兆": ["起漲前兆分數", "起漲因子分數", "機會股分數", "止跌轉強分數", "低檔位置分數", "低檔位分數", "拉回承接分數"],
    "類股熱度": ["類股熱度分數", "類股強度分數", "族群強度分數", "族群資金流分數"],
    "自動因子": ["自動因子分數", "雷達分數", "自動因子總分", "股神決策分數", "績效校正分數"],
    "交易可行": ["交易可行分數", "進場時機分數", "支撐回測分數", "動態建議倉位%", "R/R分數"],
    "型態突破": ["型態突破分數", "技術型態分數", "突破分數", "K線驗證分數", "型態分數"],
    "爆發力": ["爆發力分數", "量能因子分數", "量能分數", "量價分數", "主升段分數"],
}


# v99：把夜間隔日股神分數合併到原本 8 大權重因子來源，讓 14 校正結果可直接回寫 7_股神推薦。
for _factor_name, _cols in NIGHT_SCORE_COLUMNS.items():
    FACTOR_COLUMNS.setdefault(_factor_name, [])
    for _col in _cols:
        if _col not in FACTOR_COLUMNS[_factor_name]:
            FACTOR_COLUMNS[_factor_name].insert(0, _col)

# v111：官方因子快取欄位納入 14 權重校正。
# 來源為 16_官方因子快取中心產生、07/10/8 保存的 official_factors_cache.json 欄位；
# 14 只讀既有紀錄，不連外、不即時抓官方網站。
OFFICIAL_SCORE_COLUMNS: Dict[str, List[str]] = {
    "起漲前兆": ["官方基本面成長分數", "營收成長官方分數", "EPS成長分數", "月營收YoY%", "累計營收YoY%"],
    "自動因子": ["官方因子總分", "官方資料完整度"],
    "交易可行": ["官方估值風險分數", "PER本益比", "PBR股價淨值比", "股利殖利率%"],
    "爆發力": ["法人籌碼官方分數", "外資近5日買賣超", "投信近5日買賣超", "三大法人近5日合計", "法人連買天數"],
}

OFFICIAL_GROUP_COLUMNS: List[str] = [
    "官方因子資料狀態", "官方資料狀態", "官方資料完整度級距", "官方因子級距",
    "法人官方級距", "營收官方級距", "EPS官方級距", "估值官方級距", "PER級距",
    "正式產業別", "類別",
]

OFFICIAL_DISPLAY_COLUMNS: List[str] = [
    "股票代號", "股票名稱", "推薦日期", "推薦時間",
    "官方因子總分", "官方資料完整度", "官方因子資料狀態",
    "法人籌碼官方分數", "外資近5日買賣超", "投信近5日買賣超", "三大法人近5日合計", "法人連買天數",
    "月營收YoY%", "月營收MoM%", "累計營收YoY%", "營收成長官方分數",
    "EPS成長分數", "官方基本面成長分數", "PER本益比", "PBR股價淨值比", "估算EPS", "官方估值風險分數",
]

for _factor_name, _cols in OFFICIAL_SCORE_COLUMNS.items():
    FACTOR_COLUMNS.setdefault(_factor_name, [])
    for _col in reversed(_cols):
        if _col not in FACTOR_COLUMNS[_factor_name]:
            FACTOR_COLUMNS[_factor_name].insert(0, _col)

# v121：V118 實戰品質防呆欄位納入 14 權重校正。
# 來源為 07 產生、10/8 保存的量能/趨勢/防呆降分欄位；本模組只讀既有紀錄，不連外、不重跑推薦。
QUALITY_SCORE_COLUMNS: Dict[str, List[str]] = {
    "技術結構": ["實戰品質分", "均線轉強分", "動能翻多分", "收盤距MA20%", "收盤距MA60%"],
    "交易可行": ["實戰品質分", "支撐防守分", "量能啟動分"],
    "爆發力": ["量能啟動分", "均量比", "突破準備分", "實戰品質分"],
    "型態突破": ["突破準備分", "均線轉強分", "動能翻多分", "支撐防守分"],
    "自動因子": ["實戰品質分"],
}

QUALITY_GROUP_COLUMNS: List[str] = [
    "實戰品質級距", "量能狀態", "趨勢狀態", "實戰降分級距",
    "量能啟動級距", "均量比級距", "MA20距離級距", "MA60距離級距",
    "進場型態_隔日", "隔日建議動作", "正式產業別", "類別",
]

QUALITY_DISPLAY_COLUMNS: List[str] = [
    "股票代號", "股票名稱", "推薦日期", "推薦時間",
    "實戰品質分", "量能狀態", "趨勢狀態", "實戰降分", "實戰品質提醒",
    "最新成交量", "5日均量", "20日均量", "均量比", "收盤距MA20%", "收盤距MA60%",
    "量能啟動分", "均線轉強分", "動能翻多分", "突破準備分", "支撐防守分",
]

for _factor_name, _cols in QUALITY_SCORE_COLUMNS.items():
    FACTOR_COLUMNS.setdefault(_factor_name, [])
    for _col in reversed(_cols):
        if _col not in FACTOR_COLUMNS[_factor_name]:
            FACTOR_COLUMNS[_factor_name].insert(0, _col)

PERF_COLUMNS: Dict[int, List[str]] = {
    1: ["隔日最高漲幅%", "推薦後1日報酬%", "推薦後1日%", "1日報酬%", "1日漲跌%", "1日績效%", "1日後報酬%", "即時追蹤報酬%", "目前追蹤報酬%", "目前損益幅%", "損益幅%", "實際報酬%"],
    3: ["3日最高漲幅%", "推薦後3日報酬%", "推薦後3日%", "3日報酬%", "3日漲跌%", "3日績效%", "3日後報酬%"],
    5: ["5日最高漲幅%", "推薦後5日報酬%", "推薦後5日%", "5日報酬%", "5日漲跌%", "5日績效%", "5日後報酬%"],
    10: ["10日最高漲幅%", "推薦後10日報酬%", "推薦後10日%", "10日報酬%", "10日漲跌%", "10日績效%", "10日後報酬%"],
    20: ["推薦後20日報酬%", "推薦後20日%", "20日報酬%", "20日漲跌%", "20日績效%", "20日後報酬%"],
}

# v94：績效保底欄。
# 說明：10_推薦清單有時已經有「損益幅% / 目前損益幅% / 即時追蹤報酬%」，
# 但 5日/10日欄位尚未產生，會造成 14_權重校正顯示有效樣本 0。
# 這裡允許 14 頁在指定週期缺樣本時，改用目前追蹤損益做「暫行績效統計」，
# 避免畫面誤判成完全沒有統計。正式回測仍以推薦後 N 日欄位為優先。
PERF_FALLBACK_COLUMNS: List[str] = ["目前損益幅%", "損益幅%", "即時追蹤報酬%", "目前追蹤報酬%", "實際報酬%"]

MARKET_COLUMNS = ["大盤情境", "大盤狀態", "大盤分層", "大盤策略模式", "大盤橋接風控", "大盤橋接狀態", "市場狀態", "大盤模式"]
CATEGORY_COLUMNS = ["類別", "產業", "族群", "主題類別", "正式產業別"]
DATE_COLUMNS = ["推薦日期", "建立日期", "建立時間", "推薦時間"]
PROB_COLUMNS = ["上漲機率估計%", "上漲機率%", "預估上漲機率", "上漲機率", "上漲機率估計"]
RR_COLUMNS = ["風險報酬比", "風險報酬比_決策", "R/R", "RR", "風險報酬_拉回", "風險報酬_突破"]

# v93：大盤分層校正用欄位。先吃文字欄，沒有文字分層時再吃分數/海外盤代理。
MARKET_SCORE_COLUMNS = [
    "市場環境分數", "大盤風控分數", "大盤橋接分數", "大盤可參考分數",
    "大盤推薦同步分數", "大盤情境分數", "macro_score", "overnight_score",
]
MARKET_RETURN_PROXY_COLUMNS = [
    "台指期漲跌幅%", "夜盤漲跌幅%", "night_futures_change_pct",
    "nasdaq_change_pct", "sox_change_pct", "sp500_change_pct",
]

# v104：10_推薦清單 V101 與 8_股神推薦紀錄 V102/V103 產出的夜間隔日命中欄位。
# 14 不連外、不重新抓 K 線，只讀這些欄位做準確率回饋與權重校正參考。
NIGHT_HIT_COLUMNS: Dict[str, List[str]] = {
    "進場點命中": ["進場點命中", "預估進場點命中"],
    "突破價命中": ["突破價命中", "隔日突破價命中", "突破確認價命中"],
    "停損觸發": ["停損價觸發", "隔日停損觸發", "停損觸發"],
    "第一壓力命中": ["第一壓力命中", "壓力價命中"],
}

NIGHT_HIT_RETURN_COLUMNS: Dict[int, List[str]] = {
    1: ["隔日最高漲幅%", "推薦後1日報酬%", "1日報酬%"],
    3: ["3日最高漲幅%", "推薦後3日報酬%", "3日報酬%"],
    5: ["5日最高漲幅%", "推薦後5日報酬%", "5日報酬%"],
    10: ["10日最高漲幅%", "推薦後10日報酬%", "10日報酬%"],
}

NIGHT_DRAWDOWN_COLUMNS: Dict[int, List[str]] = {
    1: ["隔日最低回撤%"],
    3: ["3日最低回撤%"],
    5: ["5日最低回撤%"],
    10: ["10日最低回撤%"],
}


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


def _json_safe_value(obj: Any) -> Any:
    """
    v105：將 pandas / numpy / DataFrame / Series / Timestamp / NA 等不可直接 JSON 化物件
    轉成可安全輸出的 Python 原生型別，避免 Streamlit Cloud 下載或寫檔時 TypeError。
    """
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
            return [_json_safe_value(x) for x in obj.to_dict(orient="records")]
        if isinstance(obj, pd.Series):
            return {str(k): _json_safe_value(v) for k, v in obj.to_dict().items()}
        if isinstance(obj, pd.Index):
            return [_json_safe_value(x) for x in obj.tolist()]
        if isinstance(obj, (pd.Timestamp, datetime)):
            try:
                return obj.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return str(obj)
        if isinstance(obj, dict):
            return {str(_json_safe_value(k)): _json_safe_value(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [_json_safe_value(x) for x in obj]
        if hasattr(obj, "item"):
            try:
                return _json_safe_value(obj.item())
            except Exception:
                pass
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return obj
        if isinstance(obj, (str, int, bool)):
            return obj
        return str(obj)
    except Exception:
        return str(obj)


def write_json(path: Path, data: Any) -> Tuple[bool, str]:
    try:
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(_json_safe_value(data), f, ensure_ascii=False, indent=2)
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


def best_perf_col(df: pd.DataFrame, horizon: int) -> Optional[str]:
    """
    選擇指定週期中實際有效數值最多的績效欄位。
    v94：若指定週期尚無有效 N 日績效，允許回退到「目前損益 / 即時追蹤報酬」做暫行統計，
    讓權重校正知道目前已經有追蹤績效，而不是顯示有效樣本 0。
    """
    best_col = None
    best_n = 0

    def _scan(cols: Iterable[str]) -> tuple[Optional[str], int]:
        local_col = None
        local_n = 0
        for c in cols:
            if c not in df.columns:
                continue
            n = int(numeric_series(df, c).notna().sum())
            if n > local_n:
                local_n = n
                local_col = c
        return local_col, local_n

    best_col, best_n = _scan(PERF_COLUMNS.get(horizon, []))
    if best_n > 0:
        return best_col

    # 指定週期沒有資料時，先掃較短週期，再掃目前損益。
    fallback_cols: List[str] = []
    for h in [1, 3, 5, 10, 20]:
        if h == horizon:
            continue
        fallback_cols.extend(PERF_COLUMNS.get(h, []))
    fallback_cols.extend(PERF_FALLBACK_COLUMNS)
    best_col, best_n = _scan(fallback_cols)
    return best_col if best_n > 0 else None


def perf_sample_diagnostics(df: pd.DataFrame) -> pd.DataFrame:
    """回傳各績效週期的欄位偵測與有效樣本，用於 14 頁判斷有效樣本為 0 的原因。"""
    rows = []
    for h, candidates in PERF_COLUMNS.items():
        found_cols = [c for c in candidates if c in df.columns]
        best = best_perf_col(df, h)
        valid = int(numeric_series(df, best).notna().sum()) if best else 0
        rows.append({
            "週期": f"推薦後{h}日",
            "可辨識欄位": "、".join(found_cols) if found_cols else "缺欄",
            "採用欄位": best or "—",
            "有效樣本": valid,
            "狀態": ("可校正" if valid > 0 and best in found_cols else ("可暫行校正：採用目前損益/較短週期" if valid > 0 else ("欄位存在但無數值" if found_cols else "缺績效欄"))),
        })
    return pd.DataFrame(rows)




# v70：因子代理樣本修正
# 說明：績效樣本來自 8/10 更新後績效；因子樣本來自 7 推薦輸出的分數欄。
# 舊資料常缺少單一因子分數，或因子分數與績效列不重疊，會導致「有效樣本 0」。
# 因此這裡加入「代理因子」：先用原生分數，若與績效列無交集，再用文字/總分欄位保守估算。

GENERIC_SCORE_COLUMNS = [
    "推薦分數", "推薦總分", "股神決策分數", "股神總分", "總分", "score"
]


def _first_numeric_series(df: pd.DataFrame, candidates: Iterable[str]) -> Tuple[pd.Series, str]:
    """回傳第一個有數值的欄位；沒有則回傳全 NaN。"""
    best = pd.Series([math.nan] * len(df), index=df.index, dtype="float64")
    best_name = ""
    best_n = 0
    for c in candidates:
        if c not in df.columns:
            continue
        s = numeric_series(df, c)
        n = int(s.notna().sum())
        if n > best_n:
            best = s
            best_name = c
            best_n = n
    return best, best_name


def _score_text_value(v: Any, factor: str = "") -> Optional[float]:
    s = safe_str(v)
    if not s:
        return None
    # 等級式文字
    if "A" in s or "優先" in s or "股神級" in s or "高分" in s or "強烈" in s or "隔日突破型" in s or "剛起漲型" in s:
        return 88.0
    if "B" in s or "確認" in s or "拉回可" in s or "可布局" in s or "中高" in s or "回測承接型" in s or "小量" in s:
        return 76.0
    if "C" in s or "觀察" in s or "等待" in s or "中性" in s or "夜間觀察型" in s or "波段潛伏型" in s:
        return 58.0
    if "D" in s or "尚未" in s or "減碼" in s or "風險" in s or "不追" in s or "過熱等待型" in s or "暫不進場" in s:
        return 42.0
    # 大盤/市場字眼
    if factor == "市場環境":
        if any(k in s for k in ["多頭", "偏多", "強", "進攻", "風險低"]):
            return 82.0
        if any(k in s for k in ["盤整", "震盪", "中性", "觀望"]):
            return 58.0
        if any(k in s for k in ["空頭", "偏空", "弱", "防守", "風險高"]):
            return 36.0
    if factor == "型態突破":
        if any(k in s for k in ["突破", "轉強", "主升", "平台整理突破"]):
            return 86.0
        if any(k in s for k in ["整理", "接近", "回測"]):
            return 63.0
    if factor == "爆發力":
        if any(k in s for k in ["爆發", "主升", "強勢", "放量", "熱門"]):
            return 85.0
        if any(k in s for k in ["量縮", "整理", "觀察"]):
            return 58.0
    return None


def _text_score_series(df: pd.DataFrame, columns: Iterable[str], factor: str) -> Tuple[pd.Series, str]:
    vals = []
    used = []
    for c in columns:
        if c not in df.columns:
            continue
        ser = df[c].map(lambda x: _score_text_value(x, factor))
        if int(pd.Series(ser).notna().sum()) > 0:
            vals.append(pd.to_numeric(ser, errors="coerce"))
            used.append(c)
    if not vals:
        return pd.Series([math.nan] * len(df), index=df.index, dtype="float64"), ""
    out = pd.concat(vals, axis=1).mean(axis=1, skipna=True)
    return out, "文字代理:" + "/".join(used[:3])


def _category_density_series(df: pd.DataFrame) -> Tuple[pd.Series, str]:
    col = first_existing_col(df, CATEGORY_COLUMNS)
    if not col or col not in df.columns or len(df) == 0:
        return pd.Series([math.nan] * len(df), index=df.index, dtype="float64"), ""
    s = df[col].map(lambda x: safe_str(x, "未分類"))
    counts = s.value_counts()
    max_n = max(int(counts.max()), 1) if not counts.empty else 1
    # 類股集中度代理：同族群樣本越集中，代表族群熱度越高。限制在 45~85。
    out = s.map(lambda x: 45 + 40 * (counts.get(x, 0) / max_n))
    return pd.to_numeric(out, errors="coerce"), f"類股集中代理:{col}"


def factor_candidate_sources(df: pd.DataFrame, factor: str) -> List[Tuple[pd.Series, str, str]]:
    """產生某因子的候選資料源：(series, source_name, quality)。"""
    out: List[Tuple[pd.Series, str, str]] = []

    # 1) 原生因子欄：最可靠。
    native_vals = []
    native_cols = []
    for c in FACTOR_COLUMNS.get(factor, []):
        if c in df.columns:
            ser = numeric_series(df, c)
            if int(ser.notna().sum()) > 0:
                native_vals.append(ser)
                native_cols.append(c)
    if native_vals:
        out.append((pd.concat(native_vals, axis=1).mean(axis=1, skipna=True), "原生分數:" + "/".join(native_cols[:3]), "原生"))

    # 2) 因子文字代理：適合舊資料只有文字標籤沒有分數。
    text_cols_map = {
        "市場環境": MARKET_COLUMNS + ["大盤情境調權說明", "大盤橋接狀態", "大盤風控說明"],
        "技術結構": ["技術結構", "技術型態", "型態名稱", "K線驗證標記", "買點分級"],
        "起漲前兆": ["起漲等級", "買點分級", "進場時機", "推薦型態", "機會型態"],
        "類股熱度": ["族群策略建議", "族群資金流說明", "類別", "產業"],
        "自動因子": ["推薦等級", "推薦型態", "機會型態", "買點分級"],
        "交易可行": ["建議動作", "等待條件", "進場時機", "股神信心", "買點分級"],
        "型態突破": ["型態名稱", "型態突破", "K線驗證標記", "進場時機", "推薦型態"],
        "爆發力": ["爆發力", "型態突破", "推薦等級", "推薦型態", "機會型態"],
    }
    # v99：補入 07/8/10 夜間隔日作戰文字欄位，舊紀錄也可用文字代理校正。
    merged_text_cols = list(text_cols_map.get(factor, []))
    for _c in NIGHT_TEXT_COLUMNS.get(factor, []):
        if _c not in merged_text_cols:
            merged_text_cols.insert(0, _c)
    ts, ts_name = _text_score_series(df, merged_text_cols, factor)
    if int(ts.notna().sum()) > 0:
        out.append((ts, ts_name, "文字代理"))

    # 3) 類股熱度可用類別集中度代理。
    if factor == "類股熱度":
        cs, cs_name = _category_density_series(df)
        if int(cs.notna().sum()) > 0:
            out.append((cs, cs_name, "類股代理"))

    # 4) 最後保底：總分代理。這不是單一因子，僅避免舊資料完全無法校正。
    gs, gs_name = _first_numeric_series(df, GENERIC_SCORE_COLUMNS)
    if int(gs.notna().sum()) > 0:
        out.append((gs, "總分代理:" + gs_name, "總分代理"))
    return out


def choose_factor_source(df: pd.DataFrame, factor: str, ret: pd.Series) -> Tuple[pd.Series, str, str, int, float]:
    """選擇與績效列重疊最多的因子來源，避免畫面出現樣本 0。"""
    best_series = pd.Series([math.nan] * len(df), index=df.index, dtype="float64")
    best_source = "缺少可用因子欄"
    best_quality = "缺欄"
    best_overlap = 0
    best_coverage = 0.0
    for ser, source, quality in factor_candidate_sources(df, factor):
        ser = pd.to_numeric(ser, errors="coerce")
        overlap = int(pd.DataFrame({"f": ser, "ret": ret}).dropna().shape[0])
        coverage = float(ser.notna().mean() * 100) if len(ser) else 0.0
        # 先看與績效列重疊，再看資料品質。
        quality_rank = {"原生": 4, "文字代理": 3, "類股代理": 2, "總分代理": 1, "缺欄": 0}.get(quality, 0)
        best_rank = {"原生": 4, "文字代理": 3, "類股代理": 2, "總分代理": 1, "缺欄": 0}.get(best_quality, 0)
        if overlap > best_overlap or (overlap == best_overlap and quality_rank > best_rank and coverage >= best_coverage):
            best_series, best_source, best_quality, best_overlap, best_coverage = ser, source, quality, overlap, coverage
    return best_series, best_source, best_quality, best_overlap, round(best_coverage, 2)


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
    perf_col = best_perf_col(df, horizon)
    if not perf_col:
        return pd.DataFrame()
    ret = numeric_series(df, perf_col)
    rows: List[dict] = []
    for factor in DEFAULT_WEIGHTS:
        f, source_name, source_quality, overlap_n, coverage_pct = choose_factor_source(df, factor, ret)
        work = pd.DataFrame({"factor": f, "ret": ret}).dropna()
        if len(work) < 8:
            rows.append({
                "因子": factor,
                "有效樣本": int(len(work)),
                "目前權重%": DEFAULT_WEIGHTS[factor],
                "因子資料來源": source_name,
                "資料品質": source_quality,
                "高分組勝率%": None,
                "低分組勝率%": None,
                "勝率差%": None,
                "高分組平均報酬%": None,
                "低分組平均報酬%": None,
                "報酬差%": None,
                "高分組期望值%": None,
                "低分組期望值%": None,
                "期望值差%": None,
                "資料覆蓋率%": coverage_pct,
                "樣本信心": confidence_label(len(work)),
                "建議": "樣本不足，暫不調整" if len(work) < 8 else "先觀察",
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
        if source_quality != "原生" and len(work) < 50:
            advice = "代理樣本，先觀察"
        elif len(work) < 30:
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
            "因子資料來源": source_name,
            "資料品質": source_quality,
            "資料覆蓋率%": coverage_pct,
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


def _classify_market_text(v: Any) -> str:
    """把各版本的大盤文字欄統一成：多頭 / 盤整 / 空頭 / 未分層。"""
    s = safe_str(v)
    if not s:
        return "未分層"
    if any(k in s for k in ["空頭", "偏空", "轉弱", "弱勢", "風險高", "防守", "保守", "下跌", "破線"]):
        return "空頭"
    if any(k in s for k in ["多頭", "偏多", "轉強", "強勢", "風險低", "進攻", "上攻", "突破"]):
        return "多頭"
    if any(k in s for k in ["盤整", "震盪", "中性", "觀望", "區間", "整理"]):
        return "盤整"
    return "未分層"


def market_regime_series(df: pd.DataFrame) -> Tuple[pd.Series, str]:
    """
    v93：大盤分層不再只靠單一文字欄。
    來源優先序：
    1) 大盤情境/大盤狀態等文字欄。
    2) 大盤/隔夜風控分數欄，>=65 多頭，<=45 空頭，其餘盤整。
    3) 海外盤/期貨漲跌幅代理，平均 >1 多頭，<-1 空頭，其餘盤整。
    4) 都沒有才標示未分層。
    """
    if df is None or df.empty:
        return pd.Series(dtype="object"), "無資料"

    out = pd.Series(["未分層"] * len(df), index=df.index, dtype="object")

    text_col = first_existing_col(df, MARKET_COLUMNS)
    if text_col:
        classified = df[text_col].map(_classify_market_text)
        mask = classified != "未分層"
        out.loc[mask] = classified.loc[mask]
        if int(mask.sum()) > 0:
            # 如果文字欄已經有有效分層，優先採用；未分層列再用分數補。
            source = f"文字欄:{text_col}"
        else:
            source = "文字欄無法辨識"
    else:
        source = "缺文字欄"

    blank = out == "未分層"
    score_cols = [c for c in MARKET_SCORE_COLUMNS if c in df.columns]
    best_score_col = ""
    best_score_valid = 0
    best_score = None
    for c in score_cols:
        s = numeric_series(df, c)
        n = int(s.notna().sum())
        if n > best_score_valid:
            best_score_col, best_score_valid, best_score = c, n, s
    if blank.any() and best_score is not None and best_score_valid > 0:
        def by_score(x: Any) -> str:
            v = safe_float(x)
            if v is None:
                return "未分層"
            if v >= 65:
                return "多頭"
            if v <= 45:
                return "空頭"
            return "盤整"
        score_class = best_score.map(by_score)
        mask = blank & (score_class != "未分層")
        out.loc[mask] = score_class.loc[mask]
        source += f" + 分數欄:{best_score_col}"

    blank = out == "未分層"
    proxy_cols = [c for c in MARKET_RETURN_PROXY_COLUMNS if c in df.columns]
    if blank.any() and proxy_cols:
        proxy_df = pd.DataFrame({c: numeric_series(df, c) for c in proxy_cols})
        avg = proxy_df.mean(axis=1, skipna=True)
        def by_proxy(x: Any) -> str:
            v = safe_float(x)
            if v is None:
                return "未分層"
            if v > 1:
                return "多頭"
            if v < -1:
                return "空頭"
            return "盤整"
        proxy_class = avg.map(by_proxy)
        mask = blank & (proxy_class != "未分層")
        out.loc[mask] = proxy_class.loc[mask]
        source += f" + 海外/期貨代理:{','.join(proxy_cols[:3])}"

    return out.fillna("未分層"), source


def filter_by_market(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode in {"全部", "全市場"}:
        return df.copy()
    regime, _source = market_regime_series(df)
    if regime.empty:
        return pd.DataFrame()
    return df[regime == mode].copy()



# >>> V95_RESTORE_PROFILE_BUNDLE_IMPORT_FIX
def calc_profile_bundle(df: pd.DataFrame, horizons: Iterable[int] = (1, 3, 5, 10, 20), current_weights: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    """建立多週期權重校正建議包。

    v94 曾移除這個函式但 pages/14_股神權重校正.py 仍會匯入，
    造成 ImportError。此函式只讀取既有推薦/績效紀錄，不連外、不重跑推薦。
    """
    base_weights = current_weights or DEFAULT_WEIGHTS
    bundle: Dict[str, Any] = {
        "version": "v95_restore_calc_profile_bundle",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "base_weights": base_weights,
        "profiles": {},
        "factor_effectiveness": {},
        "quality": {},
    }

    if df is None or df.empty:
        for horizon in horizons:
            bundle["quality"][str(horizon)] = {
                "status": "empty_records",
                "message": "目前沒有推薦紀錄可校正",
            }
        return bundle

    for horizon in horizons:
        perf_col = best_perf_col(df, horizon)
        if not perf_col:
            bundle["quality"][str(horizon)] = {
                "status": "missing_perf_col",
                "message": f"缺少{horizon}日績效欄；請先到 10_推薦清單 更新推薦後績效",
            }
            continue

        ret = numeric_series(df, perf_col)
        valid_n = int(ret.notna().sum())
        if valid_n <= 0:
            bundle["quality"][str(horizon)] = {
                "status": "empty_perf_sample",
                "message": f"{horizon}日績效欄存在但沒有有效數值",
                "performance_col": perf_col,
            }
            continue

        base_stat = summarize_returns(ret)
        effect = calc_factor_effectiveness(df, horizon)
        weights = suggest_weights(effect, base_weights)
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
        bundle["quality"][str(horizon)] = {
            "status": "ok",
            "performance_col": perf_col,
            **base_stat,
        }
    return bundle
# <<< V95_RESTORE_PROFILE_BUNDLE_IMPORT_FIX

def calc_market_bundles(df: pd.DataFrame, horizon: int, current_weights: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    """v93：大盤分層加入全市場保底、分層來源、樣本分布，避免多/空樣本為 0 時誤判成錯誤。"""
    out: Dict[str, Any] = {}
    regime, source = market_regime_series(df)
    counts = regime.value_counts(dropna=False).to_dict() if not regime.empty else {}
    modes = ["全市場", "多頭", "盤整", "空頭"]
    for mode in modes:
        sub = df.copy() if mode == "全市場" else df[regime == mode].copy()
        sample_n = int(len(sub))
        base_payload = {
            "樣本數": sample_n,
            "分層來源": source,
            "分層樣本分布": {str(k): int(v) for k, v in counts.items()},
        }
        if sample_n < 8:
            out[mode] = {**base_payload, "status": "樣本不足，暫不調整（不是程式錯誤）"}
            continue
        effect = calc_factor_effectiveness(sub, horizon)
        if effect.empty:
            out[mode] = {**base_payload, "status": f"缺少{horizon}日績效欄或因子欄，暫不調整"}
            continue
        weights = suggest_weights(effect, current_weights)
        out[mode] = {
            **base_payload,
            "status": "ok",
            "weights": dict(zip(weights["因子"], weights["建議新權重%"])),
            "table": weights.to_dict(orient="records"),
        }
    return out


def calc_category_bundles(df: pd.DataFrame, horizon: int, current_weights: Optional[Dict[str, int]] = None, top_n: int = 8) -> Dict[str, Any]:
    col = first_existing_col(df, CATEGORY_COLUMNS)
    if not col:
        return {"status": "缺少類別/產業欄位"}
    ret_col = best_perf_col(df, horizon)
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
    rcol = best_perf_col(df, horizon)
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
    ret_col = best_perf_col(df, horizon)
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


def _bool_hit_series(df: pd.DataFrame, candidates: Iterable[str]) -> Tuple[pd.Series, str]:
    """把命中/觸發欄位轉成 0/1。可接受 True/False、是/否、命中/未命中。"""
    col = first_existing_col(df, candidates)
    if not col:
        return pd.Series([math.nan] * len(df), index=df.index, dtype="float64"), ""
    def conv(v: Any) -> Optional[float]:
        if isinstance(v, bool):
            return 1.0 if v else 0.0
        s = safe_str(v)
        if not s:
            return None
        if any(k in s for k in ["是", "Y", "y", "true", "True", "TRUE", "命中", "觸發", "達成", "到價"]):
            if any(k in s for k in ["未命中", "未觸發", "未達", "沒有"]):
                return 0.0
            return 1.0
        if any(k in s for k in ["否", "N", "n", "false", "False", "FALSE", "未", "無"]):
            return 0.0
        num = safe_float(v)
        if num is not None:
            return 1.0 if num > 0 else 0.0
        return None
    return pd.to_numeric(df[col].map(conv), errors="coerce"), col


def _score_bucket_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(["缺欄"] * len(df), index=df.index, dtype="object")
    s = numeric_series(df, col)
    def bucket(v: Any) -> str:
        x = safe_float(v)
        if x is None:
            return "無資料"
        if x >= 90:
            return "90+"
        if x >= 85:
            return "85-89"
        if x >= 80:
            return "80-84"
        if x >= 75:
            return "75-79"
        if x >= 70:
            return "70-74"
        return "<70"
    return s.map(bucket)


def _group_accuracy_table(df: pd.DataFrame, group_col: str, horizon: int = 5, min_n: int = 3) -> pd.DataFrame:
    """依進場型態/建議動作/分數級距產生夜間隔日準確率。"""
    if df is None or df.empty or group_col not in df.columns:
        return pd.DataFrame()
    ret_col = first_existing_col(df, NIGHT_HIT_RETURN_COLUMNS.get(horizon, [])) or best_perf_col(df, horizon)
    ret = numeric_series(df, ret_col) if ret_col else pd.Series([math.nan] * len(df), index=df.index, dtype="float64")
    entry_hit, entry_col = _bool_hit_series(df, NIGHT_HIT_COLUMNS["進場點命中"])
    break_hit, break_col = _bool_hit_series(df, NIGHT_HIT_COLUMNS["突破價命中"])
    stop_hit, stop_col = _bool_hit_series(df, NIGHT_HIT_COLUMNS["停損觸發"])
    target_hit, target_col = _bool_hit_series(df, NIGHT_HIT_COLUMNS["第一壓力命中"])
    work = pd.DataFrame({
        "grp": df[group_col].map(lambda x: safe_str(x, "未分類")),
        "ret": ret,
        "entry": entry_hit,
        "breakout": break_hit,
        "stop": stop_hit,
        "target": target_hit,
    })
    rows: List[dict] = []
    for grp, g in work.groupby("grp", dropna=False):
        if safe_str(grp) == "":
            grp = "未分類"
        n = int(len(g))
        if n < min_n:
            continue
        stat = summarize_returns(g["ret"])
        def hit_rate(c: str) -> Any:
            s = pd.to_numeric(g[c], errors="coerce").dropna()
            if s.empty:
                return None
            return round(float(s.mean() * 100), 2)
        rows.append({
            "分組欄位": group_col,
            "分組": grp,
            "樣本數": n,
            "績效欄": ret_col or "缺績效欄",
            "勝率%": stat.get("勝率%"),
            "平均報酬%": stat.get("平均報酬%"),
            "期望值%": stat.get("期望值%"),
            "進場點命中率%": hit_rate("entry"),
            "突破價命中率%": hit_rate("breakout"),
            "第一壓力命中率%": hit_rate("target"),
            "停損觸發率%": hit_rate("stop"),
            "樣本信心": confidence_label(n),
        })
    out = pd.DataFrame(rows)
    if not out.empty:
        sort_cols = [c for c in ["期望值%", "勝率%", "進場點命中率%"] if c in out.columns]
        if sort_cols:
            out = out.sort_values(sort_cols, ascending=[False] * len(sort_cols), na_position="last")
    return out


def calc_night_accuracy_bundle(df: pd.DataFrame, horizon: int = 5) -> Dict[str, Any]:
    """v104：把 10/8 已產生的命中追蹤與 V102 準確率欄位轉成 14 可讀的校正包。"""
    if df is None or df.empty:
        return {"status": "empty", "message": "沒有推薦紀錄", "tables": {}, "summary": {}}
    work = df.copy()
    # 分數級距是衍生欄，不回寫檔案，只供本頁分層顯示。
    for score_col, bucket_col in [("夜間股神總分", "夜間分數級距"), ("隔日進場分數", "隔日分數級距"), ("隔日實戰排序分", "實戰排序級距")]:
        if score_col in work.columns and bucket_col not in work.columns:
            work[bucket_col] = _score_bucket_series(work, score_col)

    ret_col = first_existing_col(work, NIGHT_HIT_RETURN_COLUMNS.get(horizon, [])) or best_perf_col(work, horizon)
    ret = numeric_series(work, ret_col) if ret_col else pd.Series([math.nan] * len(work), index=work.index, dtype="float64")
    base_stat = summarize_returns(ret)

    entry_hit, entry_col = _bool_hit_series(work, NIGHT_HIT_COLUMNS["進場點命中"])
    break_hit, break_col = _bool_hit_series(work, NIGHT_HIT_COLUMNS["突破價命中"])
    stop_hit, stop_col = _bool_hit_series(work, NIGHT_HIT_COLUMNS["停損觸發"])
    target_hit, target_col = _bool_hit_series(work, NIGHT_HIT_COLUMNS["第一壓力命中"])

    def rate(s: pd.Series) -> Any:
        v = pd.to_numeric(s, errors="coerce").dropna()
        if v.empty:
            return None
        return round(float(v.mean() * 100), 2)

    group_cols = [
        "進場型態_隔日", "隔日建議動作", "夜間分數級距", "隔日分數級距", "實戰排序級距", "類別", "正式產業別"
    ]
    tables = {c: _group_accuracy_table(work, c, horizon=horizon) for c in group_cols if c in work.columns}

    # 弱勢檢討：高分但績效/命中不佳，提供權重調整時參考。
    weak = pd.DataFrame()
    try:
        night_score = numeric_series(work, "夜間股神總分") if "夜間股神總分" in work.columns else pd.Series([math.nan] * len(work), index=work.index)
        entry_score = numeric_series(work, "隔日進場分數") if "隔日進場分數" in work.columns else pd.Series([math.nan] * len(work), index=work.index)
        mask = ((night_score >= 80) | (entry_score >= 80)) & ((ret < 0) | (stop_hit == 1))
        cols = [c for c in ["股票代號", "股票名稱", "推薦日期", "推薦時間", "夜間股神總分", "隔日進場分數", "進場型態_隔日", "隔日建議動作", ret_col, "停損價觸發", "作戰命中摘要", "風險說明"] if c and c in work.columns]
        weak = work.loc[mask, cols].head(50).copy() if cols else pd.DataFrame()
    except Exception:
        weak = pd.DataFrame()

    summary = {
        "版本": "v104_night_accuracy_feedback",
        "樣本數": int(len(work)),
        "績效欄": ret_col or "缺績效欄",
        "績效統計": base_stat,
        "進場點命中欄": entry_col or "缺欄",
        "突破價命中欄": break_col or "缺欄",
        "停損觸發欄": stop_col or "缺欄",
        "第一壓力命中欄": target_col or "缺欄",
        "進場點命中率%": rate(entry_hit),
        "突破價命中率%": rate(break_hit),
        "第一壓力命中率%": rate(target_hit),
        "停損觸發率%": rate(stop_hit),
        "弱勢檢討筆數": int(len(weak)),
    }
    return {"status": "ok", "summary": summary, "tables": tables, "weak": weak}


def apply_night_accuracy_feedback(weight_df: pd.DataFrame, accuracy_bundle: Dict[str, Any]) -> pd.DataFrame:
    """v104：用命中率/停損率產生權重建議旁註。為避免過擬合，不直接大幅改權重。"""
    if weight_df is None or weight_df.empty or not isinstance(accuracy_bundle, dict):
        return weight_df
    out = weight_df.copy()
    summary = accuracy_bundle.get("summary", {}) if isinstance(accuracy_bundle.get("summary"), dict) else {}
    entry_rate = safe_float(summary.get("進場點命中率%"))
    breakout_rate = safe_float(summary.get("突破價命中率%"))
    stop_rate = safe_float(summary.get("停損觸發率%"))
    weak_n = int(safe_float(summary.get("弱勢檢討筆數"), 0) or 0)
    notes: List[str] = []
    if entry_rate is not None and entry_rate >= 60:
        notes.append("進場點命中率佳，交易可行/起漲前兆可維持或小幅加權")
    if breakout_rate is not None and breakout_rate >= 55:
        notes.append("突破價命中率佳，型態突破因子有效")
    if stop_rate is not None and stop_rate >= 35:
        notes.append("停損觸發率偏高，交易可行與爆發力需保守")
    if weak_n >= 10:
        notes.append("高分失敗樣本偏多，避免一次大幅提高單一因子")
    note_text = "；".join(notes) if notes else "命中追蹤樣本仍在累積，暫不額外調權"
    out["v104命中追蹤回饋"] = note_text
    return out



def _numeric_bucket_series(df: pd.DataFrame, col: str, buckets: Optional[List[Tuple[float, str]]] = None) -> pd.Series:
    """v111：一般數值級距，供官方因子/PER 分層用。"""
    if df is None or df.empty or col not in df.columns:
        return pd.Series(["缺欄"] * (0 if df is None else len(df)), index=(None if df is None else df.index), dtype="object")
    s = numeric_series(df, col)
    default_buckets = buckets or [(90, "90+"), (80, "80-89"), (70, "70-79"), (60, "60-69"), (40, "40-59")]
    def bucket(v: Any) -> str:
        x = safe_float(v)
        if x is None:
            return "無資料"
        for threshold, label in default_buckets:
            if x >= threshold:
                return label
        return f"<{default_buckets[-1][0]:g}"
    return s.map(bucket)


def _per_bucket_series(df: pd.DataFrame, col: str = "PER本益比") -> pd.Series:
    if df is None or df.empty or col not in df.columns:
        return pd.Series(["缺欄"] * (0 if df is None else len(df)), index=(None if df is None else df.index), dtype="object")
    s = numeric_series(df, col)
    def bucket(v: Any) -> str:
        x = safe_float(v)
        if x is None or x <= 0:
            return "無資料/虧損"
        if x < 10:
            return "PER<10"
        if x < 15:
            return "PER10-15"
        if x < 20:
            return "PER15-20"
        if x < 30:
            return "PER20-30"
        if x < 50:
            return "PER30-50"
        return "PER50+"
    return s.map(bucket)


def _official_group_accuracy_table(df: pd.DataFrame, group_col: str, horizon: int = 5, min_n: int = 3) -> pd.DataFrame:
    """v111：官方因子分層命中/績效表。"""
    if df is None or df.empty or group_col not in df.columns:
        return pd.DataFrame()
    ret_col = first_existing_col(df, NIGHT_HIT_RETURN_COLUMNS.get(horizon, [])) or best_perf_col(df, horizon)
    ret = numeric_series(df, ret_col) if ret_col else pd.Series([math.nan] * len(df), index=df.index, dtype="float64")
    entry_hit, _ = _bool_hit_series(df, NIGHT_HIT_COLUMNS["進場點命中"])
    break_hit, _ = _bool_hit_series(df, NIGHT_HIT_COLUMNS["突破價命中"])
    stop_hit, _ = _bool_hit_series(df, NIGHT_HIT_COLUMNS["停損觸發"])
    target_hit, _ = _bool_hit_series(df, NIGHT_HIT_COLUMNS["第一壓力命中"])
    work = pd.DataFrame({
        "grp": df[group_col].map(lambda x: safe_str(x, "未分類")),
        "ret": ret,
        "entry": entry_hit,
        "breakout": break_hit,
        "stop": stop_hit,
        "target": target_hit,
    })
    rows: List[dict] = []
    for grp, g in work.groupby("grp", dropna=False):
        n = int(len(g))
        if n < min_n:
            continue
        stat = summarize_returns(g["ret"])
        def rate(c: str) -> Any:
            ss = pd.to_numeric(g[c], errors="coerce").dropna()
            if ss.empty:
                return None
            return round(float(ss.mean() * 100), 2)
        rows.append({
            "分組欄位": group_col,
            "分組": grp if safe_str(grp) else "未分類",
            "樣本數": n,
            "績效欄": ret_col or "缺績效欄",
            "勝率%": stat.get("勝率%"),
            "平均報酬%": stat.get("平均報酬%"),
            "期望值%": stat.get("期望值%"),
            "進場點命中率%": rate("entry"),
            "突破價命中率%": rate("breakout"),
            "第一壓力命中率%": rate("target"),
            "停損觸發率%": rate("stop"),
            "樣本信心": confidence_label(n),
        })
    out = pd.DataFrame(rows)
    if not out.empty:
        sort_cols = [c for c in ["期望值%", "勝率%", "進場點命中率%"] if c in out.columns]
        if sort_cols:
            out = out.sort_values(sort_cols, ascending=[False] * len(sort_cols), na_position="last")
    return out


def calc_official_factor_accuracy_bundle(df: pd.DataFrame, horizon: int = 5) -> Dict[str, Any]:
    """v111：官方法人/營收/EPS/PER 因子準確率分析包。"""
    if df is None or df.empty:
        return {"status": "empty", "message": "沒有推薦紀錄", "tables": {}, "summary": {}}
    work = df.copy()

    # 衍生級距欄位只供本頁分析，不回寫資料檔。
    bucket_specs = [
        ("官方因子總分", "官方因子級距"),
        ("官方資料完整度", "官方資料完整度級距"),
        ("法人籌碼官方分數", "法人官方級距"),
        ("營收成長官方分數", "營收官方級距"),
        ("EPS成長分數", "EPS官方級距"),
        ("官方估值風險分數", "估值官方級距"),
    ]
    for src, dst in bucket_specs:
        if src in work.columns and dst not in work.columns:
            work[dst] = _numeric_bucket_series(work, src)
    if "PER本益比" in work.columns and "PER級距" not in work.columns:
        work["PER級距"] = _per_bucket_series(work, "PER本益比")

    ret_col = first_existing_col(work, NIGHT_HIT_RETURN_COLUMNS.get(horizon, [])) or best_perf_col(work, horizon)
    ret = numeric_series(work, ret_col) if ret_col else pd.Series([math.nan] * len(work), index=work.index, dtype="float64")
    base_stat = summarize_returns(ret)
    completeness = numeric_series(work, "官方資料完整度") if "官方資料完整度" in work.columns else pd.Series([math.nan] * len(work), index=work.index)
    official_score = numeric_series(work, "官方因子總分") if "官方因子總分" in work.columns else pd.Series([math.nan] * len(work), index=work.index)

    usable_mask = completeness >= 60
    high_mask = (completeness >= 60) & (official_score >= 70)
    low_mask = (completeness >= 60) & (official_score < 50)

    entry_hit, entry_col = _bool_hit_series(work, NIGHT_HIT_COLUMNS["進場點命中"])
    break_hit, break_col = _bool_hit_series(work, NIGHT_HIT_COLUMNS["突破價命中"])
    stop_hit, stop_col = _bool_hit_series(work, NIGHT_HIT_COLUMNS["停損觸發"])
    target_hit, target_col = _bool_hit_series(work, NIGHT_HIT_COLUMNS["第一壓力命中"])

    tables = {c: _official_group_accuracy_table(work, c, horizon=horizon) for c in OFFICIAL_GROUP_COLUMNS if c in work.columns}

    def _stat_for(mask: pd.Series) -> Dict[str, Any]:
        try:
            return summarize_returns(ret.loc[mask.fillna(False)])
        except Exception:
            return {"樣本數": 0}

    def _rate(s: pd.Series, mask: Optional[pd.Series] = None) -> Any:
        try:
            vv = pd.to_numeric(s.loc[mask.fillna(False)] if mask is not None else s, errors="coerce").dropna()
            if vv.empty:
                return None
            return round(float(vv.mean() * 100), 2)
        except Exception:
            return None

    high_fail = pd.DataFrame()
    low_success = pd.DataFrame()
    try:
        fail_mask = high_mask & ((ret < 0) | (stop_hit == 1))
        success_mask = low_mask & (ret >= 3)
        cols = [c for c in OFFICIAL_DISPLAY_COLUMNS + [ret_col, "停損價觸發", "進場型態_隔日", "隔日建議動作", "作戰命中摘要", "風險說明"] if c and c in work.columns]
        high_fail = work.loc[fail_mask, cols].head(50).copy() if cols else pd.DataFrame()
        low_success = work.loc[success_mask, cols].head(50).copy() if cols else pd.DataFrame()
    except Exception:
        high_fail = pd.DataFrame()
        low_success = pd.DataFrame()

    summary = {
        "版本": "v111_official_factor_accuracy_feedback",
        "樣本數": int(len(work)),
        "績效欄": ret_col or "缺績效欄",
        "績效統計": base_stat,
        "官方可用樣本": int(usable_mask.fillna(False).sum()),
        "官方高分樣本": int(high_mask.fillna(False).sum()),
        "官方低分樣本": int(low_mask.fillna(False).sum()),
        "平均官方因子總分": round(float(pd.to_numeric(official_score, errors="coerce").dropna().mean()), 2) if pd.to_numeric(official_score, errors="coerce").dropna().size else None,
        "平均官方完整度": round(float(pd.to_numeric(completeness, errors="coerce").dropna().mean()), 2) if pd.to_numeric(completeness, errors="coerce").dropna().size else None,
        "官方可用績效": _stat_for(usable_mask),
        "官方高分績效": _stat_for(high_mask),
        "官方低分績效": _stat_for(low_mask),
        "進場點命中欄": entry_col or "缺欄",
        "突破價命中欄": break_col or "缺欄",
        "停損觸發欄": stop_col or "缺欄",
        "第一壓力命中欄": target_col or "缺欄",
        "官方可用進場點命中率%": _rate(entry_hit, usable_mask),
        "官方可用突破價命中率%": _rate(break_hit, usable_mask),
        "官方可用第一壓力命中率%": _rate(target_hit, usable_mask),
        "官方可用停損觸發率%": _rate(stop_hit, usable_mask),
        "官方高分失敗筆數": int(len(high_fail)),
        "官方低分成功筆數": int(len(low_success)),
    }
    return {"status": "ok", "summary": summary, "tables": tables, "high_fail": high_fail, "low_success": low_success}


def apply_official_factor_feedback(weight_df: pd.DataFrame, official_bundle: Dict[str, Any]) -> pd.DataFrame:
    """v111：官方因子命中回饋。保守處理，只加註建議，不直接大幅改權重。"""
    if weight_df is None or weight_df.empty or not isinstance(official_bundle, dict):
        return weight_df
    out = weight_df.copy()
    summary = official_bundle.get("summary", {}) if isinstance(official_bundle.get("summary"), dict) else {}
    usable = int(safe_float(summary.get("官方可用樣本"), 0) or 0)
    high_stat = summary.get("官方高分績效", {}) if isinstance(summary.get("官方高分績效"), dict) else {}
    low_stat = summary.get("官方低分績效", {}) if isinstance(summary.get("官方低分績效"), dict) else {}
    high_expect = safe_float(high_stat.get("期望值%"))
    low_expect = safe_float(low_stat.get("期望值%"))
    stop_rate = safe_float(summary.get("官方可用停損觸發率%"))
    high_fail = int(safe_float(summary.get("官方高分失敗筆數"), 0) or 0)
    notes: List[str] = []
    if usable < 30:
        notes.append("官方因子可用樣本仍少，先觀察不大幅調權")
    else:
        if high_expect is not None and low_expect is not None and high_expect > low_expect:
            notes.append("官方高分績效優於低分，法人/基本面/估值因子可納入加權參考")
        if high_expect is not None and high_expect <= 0:
            notes.append("官方高分期望值未轉正，避免過度提高基本面或籌碼權重")
        if stop_rate is not None and stop_rate >= 35:
            notes.append("官方可用樣本停損偏高，估值風險與交易可行需保守")
        if high_fail >= 10:
            notes.append("官方高分失敗樣本偏多，需檢查是否追高或產業輪動失真")
    note_text = "；".join(notes) if notes else "官方因子樣本正常累積中，暫不額外調權"
    out["v111官方因子回饋"] = note_text
    return out


def _quality_bucket_series(df: pd.DataFrame, col: str) -> pd.Series:
    """v121：實戰品質分數級距。"""
    s = numeric_series(df, col)
    def bucket(v: Any) -> str:
        x = safe_float(v)
        if x is None:
            return "缺資料"
        if x >= 80:
            return "A 高品質>=80"
        if x >= 70:
            return "B 可操作70-79"
        if x >= 60:
            return "C 觀察60-69"
        return "D 低品質<60"
    return s.map(bucket)


def _risk_bucket_series(df: pd.DataFrame, col: str) -> pd.Series:
    """v121：實戰降分級距，分數越高代表越需要檢討。"""
    s = numeric_series(df, col)
    def bucket(v: Any) -> str:
        x = safe_float(v)
        if x is None:
            return "缺資料"
        if x <= 0:
            return "無降分"
        if x <= 5:
            return "輕微降分1-5"
        if x <= 15:
            return "中度降分6-15"
        return "重度降分>15"
    return s.map(bucket)


def _ratio_bucket_series(df: pd.DataFrame, col: str) -> pd.Series:
    """v121：均量比級距。"""
    s = numeric_series(df, col)
    def bucket(v: Any) -> str:
        x = safe_float(v)
        if x is None:
            return "缺資料"
        if x >= 2:
            return "量能強>=2"
        if x >= 1.2:
            return "量能啟動1.2-2"
        if x >= 0.8:
            return "量能普通0.8-1.2"
        return "量能不足<0.8"
    return s.map(bucket)


def _ma_distance_bucket_series(df: pd.DataFrame, col: str) -> pd.Series:
    """v121：收盤距均線級距。"""
    s = numeric_series(df, col)
    def bucket(v: Any) -> str:
        x = safe_float(v)
        if x is None:
            return "缺資料"
        if x >= 8:
            return "乖離偏高>=8%"
        if x >= 0:
            return "站上均線0-8%"
        if x >= -5:
            return "小幅跌破0~-5%"
        return "跌破偏弱<-5%"
    return s.map(bucket)


def _quality_group_accuracy_table(df: pd.DataFrame, group_col: str, horizon: int = 5, min_n: int = 3) -> pd.DataFrame:
    """v121：實戰品質分層命中/績效表。"""
    if df is None or df.empty or group_col not in df.columns:
        return pd.DataFrame()
    ret_col = first_existing_col(df, NIGHT_HIT_RETURN_COLUMNS.get(horizon, [])) or best_perf_col(df, horizon)
    ret = numeric_series(df, ret_col) if ret_col else pd.Series([math.nan] * len(df), index=df.index, dtype="float64")
    stop_hit, _ = _bool_hit_series(df, NIGHT_HIT_COLUMNS["停損觸發"])
    target_hit, _ = _bool_hit_series(df, NIGHT_HIT_COLUMNS["第一壓力命中"])
    entry_hit, _ = _bool_hit_series(df, NIGHT_HIT_COLUMNS["進場點命中"])
    work = pd.DataFrame({
        "grp": df[group_col].map(lambda x: safe_str(x, "未分類")),
        "ret": ret,
        "stop": stop_hit,
        "target": target_hit,
        "entry": entry_hit,
    })
    rows: List[dict] = []
    for grp, g in work.groupby("grp", dropna=False):
        n = int(len(g))
        if n < min_n:
            continue
        stat = summarize_returns(g["ret"])
        def rate(c: str) -> Any:
            ss = pd.to_numeric(g[c], errors="coerce").dropna()
            if ss.empty:
                return None
            return round(float(ss.mean() * 100), 2)
        rows.append({
            "分組欄位": group_col,
            "分組": grp if safe_str(grp) else "未分類",
            "樣本數": n,
            "績效欄": ret_col or "缺績效欄",
            "勝率%": stat.get("勝率%"),
            "平均報酬%": stat.get("平均報酬%"),
            "期望值%": stat.get("期望值%"),
            "進場點命中率%": rate("entry"),
            "第一壓力命中率%": rate("target"),
            "停損觸發率%": rate("stop"),
            "樣本信心": confidence_label(n),
        })
    out = pd.DataFrame(rows)
    if not out.empty:
        sort_cols = [c for c in ["期望值%", "勝率%", "第一壓力命中率%"] if c in out.columns]
        if sort_cols:
            out = out.sort_values(sort_cols, ascending=[False] * len(sort_cols), na_position="last")
    return out


def calc_quality_accuracy_bundle(df: pd.DataFrame, horizon: int = 5) -> Dict[str, Any]:
    """v121：實戰品質防呆欄位準確率分析包。"""
    if df is None or df.empty:
        return {"status": "empty", "message": "沒有推薦紀錄", "tables": {}, "summary": {}}
    work = df.copy()
    if "實戰品質分" in work.columns and "實戰品質級距" not in work.columns:
        work["實戰品質級距"] = _quality_bucket_series(work, "實戰品質分")
    if "實戰降分" in work.columns and "實戰降分級距" not in work.columns:
        work["實戰降分級距"] = _risk_bucket_series(work, "實戰降分")
    if "量能啟動分" in work.columns and "量能啟動級距" not in work.columns:
        work["量能啟動級距"] = _numeric_bucket_series(work, "量能啟動分")
    if "均量比" in work.columns and "均量比級距" not in work.columns:
        work["均量比級距"] = _ratio_bucket_series(work, "均量比")
    if "收盤距MA20%" in work.columns and "MA20距離級距" not in work.columns:
        work["MA20距離級距"] = _ma_distance_bucket_series(work, "收盤距MA20%")
    if "收盤距MA60%" in work.columns and "MA60距離級距" not in work.columns:
        work["MA60距離級距"] = _ma_distance_bucket_series(work, "收盤距MA60%")

    ret_col = first_existing_col(work, NIGHT_HIT_RETURN_COLUMNS.get(horizon, [])) or best_perf_col(work, horizon)
    ret = numeric_series(work, ret_col) if ret_col else pd.Series([math.nan] * len(work), index=work.index, dtype="float64")
    quality = numeric_series(work, "實戰品質分") if "實戰品質分" in work.columns else pd.Series([math.nan] * len(work), index=work.index)
    penalty = numeric_series(work, "實戰降分") if "實戰降分" in work.columns else pd.Series([math.nan] * len(work), index=work.index)

    usable_mask = quality.notna()
    high_mask = quality >= 70
    low_mask = quality < 60
    penalty_mask = penalty >= 10

    stop_hit, stop_col = _bool_hit_series(work, NIGHT_HIT_COLUMNS["停損觸發"])
    target_hit, target_col = _bool_hit_series(work, NIGHT_HIT_COLUMNS["第一壓力命中"])
    entry_hit, entry_col = _bool_hit_series(work, NIGHT_HIT_COLUMNS["進場點命中"])

    tables = {c: _quality_group_accuracy_table(work, c, horizon=horizon) for c in QUALITY_GROUP_COLUMNS if c in work.columns}

    def _stat_for(mask: pd.Series) -> Dict[str, Any]:
        try:
            return summarize_returns(ret.loc[mask.fillna(False)])
        except Exception:
            return {"樣本數": 0}

    def _rate(s: pd.Series, mask: Optional[pd.Series] = None) -> Any:
        try:
            vv = pd.to_numeric(s.loc[mask.fillna(False)] if mask is not None else s, errors="coerce").dropna()
            if vv.empty:
                return None
            return round(float(vv.mean() * 100), 2)
        except Exception:
            return None

    weak = pd.DataFrame()
    try:
        fail_mask = high_mask & ((ret < 0) | (stop_hit == 1))
        cols = [c for c in QUALITY_DISPLAY_COLUMNS + [ret_col, "進場型態_隔日", "隔日建議動作", "作戰命中摘要", "風險說明"] if c and c in work.columns]
        weak = work.loc[fail_mask, cols].head(50).copy() if cols else pd.DataFrame()
    except Exception:
        weak = pd.DataFrame()

    summary = {
        "版本": "v121_quality_accuracy_feedback",
        "樣本數": int(len(work)),
        "績效欄": ret_col or "缺績效欄",
        "績效統計": summarize_returns(ret),
        "實戰品質可用樣本": int(usable_mask.fillna(False).sum()),
        "高品質樣本": int(high_mask.fillna(False).sum()),
        "低品質樣本": int(low_mask.fillna(False).sum()),
        "高降分樣本": int(penalty_mask.fillna(False).sum()),
        "平均實戰品質分": round(float(pd.to_numeric(quality, errors="coerce").dropna().mean()), 2) if pd.to_numeric(quality, errors="coerce").dropna().size else None,
        "平均實戰降分": round(float(pd.to_numeric(penalty, errors="coerce").dropna().mean()), 2) if pd.to_numeric(penalty, errors="coerce").dropna().size else None,
        "高品質績效": _stat_for(high_mask),
        "低品質績效": _stat_for(low_mask),
        "高降分績效": _stat_for(penalty_mask),
        "進場點命中欄": entry_col or "缺欄",
        "停損觸發欄": stop_col or "缺欄",
        "第一壓力命中欄": target_col or "缺欄",
        "高品質進場點命中率%": _rate(entry_hit, high_mask),
        "高品質第一壓力命中率%": _rate(target_hit, high_mask),
        "高品質停損觸發率%": _rate(stop_hit, high_mask),
        "高品質失敗筆數": int(len(weak)),
    }
    return {"status": "ok", "summary": summary, "tables": tables, "weak": weak}


def apply_quality_feedback(weight_df: pd.DataFrame, quality_bundle: Dict[str, Any]) -> pd.DataFrame:
    """v121：實戰品質回饋。僅加註建議，維持原本 8 大權重名稱。"""
    if weight_df is None or weight_df.empty or not isinstance(quality_bundle, dict):
        return weight_df
    out = weight_df.copy()
    summary = quality_bundle.get("summary", {}) if isinstance(quality_bundle.get("summary"), dict) else {}
    usable = int(safe_float(summary.get("實戰品質可用樣本"), 0) or 0)
    high_stat = summary.get("高品質績效", {}) if isinstance(summary.get("高品質績效"), dict) else {}
    low_stat = summary.get("低品質績效", {}) if isinstance(summary.get("低品質績效"), dict) else {}
    penalty_stat = summary.get("高降分績效", {}) if isinstance(summary.get("高降分績效"), dict) else {}
    high_expect = safe_float(high_stat.get("期望值%"))
    low_expect = safe_float(low_stat.get("期望值%"))
    penalty_expect = safe_float(penalty_stat.get("期望值%"))
    high_stop = safe_float(summary.get("高品質停損觸發率%"))
    notes: List[str] = []
    if usable < 30:
        notes.append("實戰品質樣本仍少，先觀察不大幅調權")
    else:
        if high_expect is not None and low_expect is not None and high_expect > low_expect:
            notes.append("高實戰品質績效優於低品質，建議維持或提高技術結構/交易可行/爆發力")
        if high_expect is not None and high_expect <= 0:
            notes.append("高實戰品質期望值未轉正，需檢查進場條件或停損設定")
        if penalty_expect is not None and penalty_expect < 0:
            notes.append("高降分樣本績效偏弱，應提高交易可行與量能趨勢防呆權重")
        if high_stop is not None and high_stop >= 30:
            notes.append("高品質樣本停損仍偏高，需強化風險/交易可行因子")
    note_text = "；".join(notes) if notes else "實戰品質樣本正常累積中，暫不額外調權"
    out["v121實戰品質回饋"] = note_text
    return out


# >>> V96_WEIGHT_APPLY_GITHUB_SYNC_FIX
def _github_weight_cfg() -> Dict[str, str]:
    """讀取 GitHub 設定，讓 14_權重校正可直接永久回寫 7_股神推薦使用的設定檔。"""
    try:
        import streamlit as st
        secrets = st.secrets
    except Exception:
        secrets = {}
    def _get(name: str, default: str = "") -> str:
        try:
            return str(secrets.get(name, default) or default)
        except Exception:
            return default
    return {
        "token": _get("GITHUB_TOKEN", ""),
        "owner": _get("GITHUB_REPO_OWNER", "cheng07021028"),
        "repo": _get("GITHUB_REPO_NAME", "stock-app"),
        "branch": _get("GITHUB_REPO_BRANCH", "main"),
        "path": _get("GODPICK_USER_SETTINGS_GITHUB_PATH", str(SETTINGS_FILE)),
    }


def _github_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


def _github_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _write_settings_to_github(payload: Dict[str, Any]) -> Tuple[bool, str]:
    """v96：14 套用權重時同步寫 GitHub，避免只寫 Cloud 暫存檔導致 7 頁讀不到。"""
    cfg = _github_weight_cfg()
    token = cfg.get("token", "")
    if not token:
        return False, "未設定 GITHUB_TOKEN，已只寫入本機暫存；重新部署後可能消失。"
    if requests is None:
        return False, "requests 套件不可用，無法寫入 GitHub。"
    try:
        url = _github_url(cfg["owner"], cfg["repo"], cfg["path"])
        headers = _github_headers(token)
        sha = ""
        get_resp = requests.get(url, headers=headers, params={"ref": cfg["branch"]}, timeout=15)
        if get_resp.status_code == 200:
            sha = str(get_resp.json().get("sha", "") or "")
        elif get_resp.status_code not in (404,):
            # 仍允許嘗試 PUT；若檔案存在且缺 sha，GitHub 會回報錯誤。
            pass
        body: Dict[str, Any] = {
            "message": f"Update godpick_user_settings from weight calibration @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "content": base64.b64encode(json.dumps(_json_safe_value(payload), ensure_ascii=False, indent=2).encode("utf-8")).decode("ascii"),
            "branch": cfg["branch"],
        }
        if sha:
            body["sha"] = sha
        put_resp = requests.put(url, headers=headers, json=body, timeout=25)
        if put_resp.status_code in (200, 201):
            return True, f"GitHub 已永久寫入：{cfg['owner']}/{cfg['repo']}@{cfg['branch']}:{cfg['path']}"
        return False, f"GitHub 寫入失敗：{put_resp.status_code} / {put_resp.text[:300]}"
    except Exception as exc:
        return False, f"GitHub 寫入例外：{exc}"
# <<< V96_WEIGHT_APPLY_GITHUB_SYNC_FIX

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
    applied = normalize_weights(weights, min_w=3, max_w=30)
    payload = {
        **existing,
        "original_default_weights": existing.get("original_default_weights", DEFAULT_WEIGHTS),
        "applied_weights": applied,
        # v99：同步保留 7_股神推薦容易讀取的欄位，並用 updated_at 觸發 7 頁重新載入；夜間欄位仍回寫原本 8 大權重，避免 7 頁不相容。
        "score_weights": applied,
        "weight_source": "14_股神權重校正",
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": "godpick_v104_night_accuracy_feedback_sync",
        "last_weight_calibration_profile": profile_name,
    }
    local_ok, local_msg = write_json(SETTINGS_FILE, payload)
    gh_ok, gh_msg = _write_settings_to_github(payload)
    ok = bool(local_ok or gh_ok)
    detail = "；".join([str(local_msg), str(gh_msg)])
    if ok:
        return True, f"權重已套用到 7_股神推薦設定檔。{detail}"
    return False, f"權重套用失敗。{detail}"




def get_weight_calibration_page_settings() -> Dict[str, Any]:
    """v106：讀取 14_權重校正設定的永久設定。"""
    payload = load_current_settings()
    raw = payload.get("weight_calibration_page_settings", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw, dict):
        raw = {}
    try:
        horizon = int(raw.get("main_horizon", 5) or 5)
    except Exception:
        horizon = 5
    if horizon not in (1, 3, 5, 10, 20):
        horizon = 5
    return {
        "main_horizon": horizon,
        "allow_apply_from_page": bool(raw.get("allow_apply_from_page", False)),
        "updated_at": str(raw.get("updated_at", "") or ""),
    }


def save_weight_calibration_page_settings(horizon: int, allow_apply_from_page: bool) -> Tuple[bool, str]:
    """v106：永久保存 14_權重校正設定，並同步 GitHub。"""
    existing = load_current_settings()
    if not isinstance(existing, dict):
        existing = {}
    try:
        horizon = int(horizon)
    except Exception:
        horizon = 5
    if horizon not in (1, 3, 5, 10, 20):
        horizon = 5
    payload = {
        **existing,
        "weight_calibration_page_settings": {
            "main_horizon": horizon,
            "allow_apply_from_page": bool(allow_apply_from_page),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "version": "v106_weight_calibration_setting_persist",
        },
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": "godpick_v106_weight_calibration_setting_persist",
    }
    local_ok, local_msg = write_json(SETTINGS_FILE, payload)
    gh_ok, gh_msg = _write_settings_to_github(payload)
    ok = bool(local_ok or gh_ok)
    detail = "；".join([str(local_msg), str(gh_msg)])
    if ok:
        return True, f"14_權重校正設定已永久保存。{detail}"
    return False, f"14_權重校正設定保存失敗。{detail}"

def save_suggestion_bundle(bundle: Dict[str, Any]) -> Tuple[bool, str]:
    return write_json(SUGGESTION_FILE, bundle)

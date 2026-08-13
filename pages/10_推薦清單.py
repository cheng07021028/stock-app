# -*- coding: utf-8 -*-









from __future__ import annotations

import streamlit as st
from godpick_factor_schema import enrich_dataframe, ensure_factor_columns, V72_FACTOR_FIELDS

# >>> APP_AUTH_GUARD_V84
try:
    from app_auth import require_login
    require_login()
except Exception as _auth_e:
    st.error(f"登入系統載入失敗：{_auth_e}")
    st.stop()

try:
    from godpick_persistence_service import (
        load_module_sync_state,
        load_named_json_permanent,
        load_records_permanent,
        save_named_json_permanent,
        upsert_records_authority_fast,
    )
except Exception:
    load_module_sync_state = None
    load_named_json_permanent = None
    load_records_permanent = None
    save_named_json_permanent = None
    upsert_records_authority_fast = None
# <<< APP_AUTH_GUARD_V84

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any
import base64
import io
import json
import time
import re

import pandas as pd
import requests
import streamlit as st
from godpick_perf_fast_update_v77 import update_recommendation_perf_fast_v77
from godpick_history_sources import fetch_multi_source_history
try:
    from official_factor_service import FACTOR_COLUMNS as OFFICIAL_FACTOR_SERVICE_COLUMNS, load_factor_frame as _load_official_factor_frame
except Exception:
    OFFICIAL_FACTOR_SERVICE_COLUMNS = []
    _load_official_factor_frame = None

try:
    from godpick_column_schema import (
        UNIFIED_RECOMMEND_DISPLAY_COLUMNS,
        UNIFIED_MANAGEMENT_COLUMNS as SHARED_UNIFIED_MANAGEMENT_COLUMNS,
        normalize_godpick_dataframe,
        effective_display_dataframe,
        unified_display_columns,
        filter_effective_columns,
        V136_EFFECTIVE_REQUIRED_COLUMNS,
        dedupe_keep_order as shared_dedupe_keep_order,
    )
except Exception:
    UNIFIED_RECOMMEND_DISPLAY_COLUMNS = []
    SHARED_UNIFIED_MANAGEMENT_COLUMNS = []
    normalize_godpick_dataframe = None
    effective_display_dataframe = None
    unified_display_columns = None
    filter_effective_columns = None
    V136_EFFECTIVE_REQUIRED_COLUMNS = []
    shared_dedupe_keep_order = None


try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except Exception:
    firebase_admin = None
    credentials = None
    firestore = None

try:
    from utils import inject_pro_theme, render_pro_hero, render_pro_section, render_pro_info_card, format_number, get_history_data
except Exception:
    def inject_pro_theme():
        return None
    def render_pro_hero(title: str, subtitle: str = "", chips=None):
        st.title(title)
        if subtitle:
            st.caption(subtitle)
    def render_pro_section(title: str):
        st.subheader(title)
    def render_pro_info_card(title: str, value: str, desc: str = ""):
        st.metric(title, value, desc)
    def format_number(v, digits=2):
        try:
            return f"{float(v):,.{digits}f}"
        except Exception:
            return ""
    def get_history_data(*args, **kwargs):
        return pd.DataFrame()

PAGE_TITLE = "推薦清單"
PERF_TRACKING_VERSION = "v119_v118_quality_sync"
PFX = "godpick_list_"
NIGHT_BATTLE_LIST_VERSION = "V126_20260517_main_candidate_split_sync"

# V94：07 夜間隔日股神欄位。推薦清單負責保存、顯示、篩選、匯出，
# 不重算 07 推薦核心，避免拖慢頁面。
NIGHT_GODPICK_COLUMNS = [
    "夜間股神總分", "隔日實戰排序分", "隔日進場分數", "波段潛力分數",
    "技術趨勢分數", "量價動能分數", "法人籌碼分數", "大戶鎖碼分數", "基本面成長分數",
    "營收成長分數", "EPS成長分數", "估值風險分數", "PER本益比", "估算EPS",
    "外資近1日買賣超", "投信近1日買賣超", "自營商近1日買賣超", "三大法人近1日合計", "法人買超占量比%",
    "法人連買推估", "籌碼資料來源", "籌碼資料日期", "基本面資料來源", "基本面資料日期", "資料完整度",
    "進場型態_隔日", "隔日建議動作", "預估進場點", "回測承接價", "突破確認價_隔日", "停損價_隔日",
    "第一壓力價", "觀察週期", "夜間股神建議", "隔日作戰策略", "進場條件說明", "不追高條件", "夜間風險提醒",
]

NIGHT_BATTLE_DISPLAY_COLUMNS = [
    "推薦日期", "推薦時間", "股票代號", "股票名稱", "類別", "推薦總分",
    "夜間股神總分", "隔日實戰排序分", "隔日進場分數", "波段潛力分數",
    "進場型態_隔日", "隔日建議動作", "預估進場點", "回測承接價", "突破確認價_隔日", "停損價_隔日", "第一壓力價",
    "夜間股神建議", "隔日作戰策略", "資料完整度", "最新價", "目前狀態",
]

NIGHT_NUMERIC_COLUMNS = [
    "夜間股神總分", "隔日實戰排序分", "隔日進場分數", "波段潛力分數",
    "技術趨勢分數", "量價動能分數", "法人籌碼分數", "大戶鎖碼分數", "基本面成長分數",
    "營收成長分數", "EPS成長分數", "估值風險分數", "PER本益比", "估算EPS",
    "外資近1日買賣超", "投信近1日買賣超", "自營商近1日買賣超", "三大法人近1日合計", "法人買超占量比%",
    "預估進場點", "回測承接價", "突破確認價_隔日", "停損價_隔日", "第一壓力價",
]

# V119：同步 07 V118 實戰品質防呆欄位到 10_推薦清單。
# 用途：保留 07 因低量、無趨勢而降分的原因，方便隔日追蹤與人工判斷。
PRACTICAL_QUALITY_COLUMNS_V119 = [
    "股神推薦層級", "候補等級", "是否主要顯示", "主表篩選", "股神輸出排序", "候補排序分",
    "股神實戰建議", "限制原因", "族群名稱", "資金流熱門族群", "族群熱度排名",
    "族群資金流分數", "族群流動性分數", "族群樣本數", "族群判斷依據", "大盤趨勢模式",
    "成交額百萬", "20日均成交額百萬", "流動性等級", "實戰版本",
    "原始推薦總分", "實戰調整推薦分", "主推薦排序分", "實戰主推薦分", "主推薦不合格原因",
    "實戰品質分", "量能狀態", "趨勢狀態", "實戰降分", "實戰品質提醒",
    "最新成交量", "5日均量", "20日均量", "均量比", "收盤距MA20%", "收盤距MA60%",
    "量能啟動分", "均線轉強分", "動能翻多分", "突破準備分", "支撐防守分",
]
PRACTICAL_QUALITY_NUMERIC_COLUMNS_V119 = [
    "股神輸出排序", "候補排序分", "主推薦排序分", "實戰主推薦分", "實戰品質分", "實戰降分",
    "最新成交量", "5日均量", "20日均量", "均量比", "收盤距MA20%", "收盤距MA60%",
    "量能啟動分", "均線轉強分", "動能翻多分", "突破準備分", "支撐防守分",
]
PRACTICAL_QUALITY_DISPLAY_COLUMNS_V119 = [
    "推薦日期", "推薦時間", "股票代號", "股票名稱", "類別", "產業",
    "股神推薦層級", "候補等級", "是否主要顯示", "主表篩選", "股神輸出排序", "候補排序分",
    "股神實戰建議", "限制原因", "族群名稱", "資金流熱門族群", "族群熱度排名", "族群資金流分數",
    "成交額百萬", "20日均成交額百萬", "流動性等級",
    "推薦總分", "夜間股神總分", "隔日進場分數", "實戰品質分", "量能狀態", "趨勢狀態",
    "實戰降分", "實戰品質提醒", "最新成交量", "5日均量", "20日均量", "均量比", "收盤距MA20%", "收盤距MA60%",
    "進場型態_隔日", "隔日建議動作", "資料完整度", "官方資料完整度",
]
GOD_DECISION_V10_LINK_VERSION = "recommend_list_v10_entry_decision_v1_20260428"
BACKTEST_V12_VERSION = "recommend_list_v53_perf_guard_20260429"
DUPLICATE_COLUMN_FIX_VERSION = "recommend_list_duplicate_column_fix_v1_20260427"
V5_BACKFILL_FIX_VERSION = "recommend_list_v5_backfill_fix_v1_20260427"
READ_FALLBACK_VERSION = "recommend_list_multi_source_read_v1_20260427"
MARKET_TREND_V38_LINK_VERSION = "recommend_list_market_trend_v76_practical_entry_fields_20260430"

GODPICK_RECOMMEND_LIST_FILE = "godpick_recommend_list.json"
GODPICK_RECOMMEND_SOURCE_FILES = [
    "godpick_recommend_list.json",
    "godpick_latest_recommendations.json",
    "godpick_records.json",
]

GODPICK_RECORD_COLUMNS = [
    "record_id",
    "資料來源",
    "股票代號",
    "股票名稱",
    "市場別",
    "類別",
    "推薦模式",
    "推薦型態",
    "機會型態",
    "低檔位置分數",
    "拉回承接分數",
    "支撐回測分數",
    "止跌轉強分數",
    "機會股分數",
    "機會股說明",
    "進場時機",
    "進場時機分數",
    "建議動作",
    "等待條件",
    "近端支撐",
    "主要支撐",
    "近端壓力",
    "突破確認價",
    "停損參考",
    "操作區間",
    "風險報酬比_決策",
    "追高風險分數_決策",
    "追高風險等級",
    "是否建議追價",
    "風險扣分原因",
    "決策說明",
    "推薦等級",
    "推薦總分",
    "大盤橋接分數",
    "大盤橋接狀態",
    "大盤橋接加權",
    "大盤橋接風控",
    "大盤橋接策略",
    "大盤橋接更新時間",
    "大盤交易時段",
    "大盤交易時段可用",
    "大盤資料品質",
    "大盤影響加減分",
    "大盤影響說明",
    "大盤資料診斷摘要",
    "股神決策模式",
    "股神進場建議",
    "推薦分層",
    "建議部位%",
    "建議倉位%",
    "建議投入等級",
    "分批策略",
    "第一筆進場%",
    "第二筆加碼條件",
    "停利策略",
    "停損策略",
    "最大風險%",
    "資金風險說明",
    "單檔風險等級",
    "族群集中警示",
    "組合配置建議", "大盤策略模式", "大盤多空分數", "推薦積極度係數", "適合推薦型態", "大盤策略建議", "大盤風控建議", "市場策略調整說明", "動態建議倉位%",

    "風險報酬比",
    "追價風險分",
    "停損距離%",
    "目標報酬%",
    "不建議買進原因",
    "最佳操作劇本",
    "隔日操作建議",
    "失效價位",
    "轉弱條件",
    "大盤情境調權說明",
    "大盤情境分桶",
    "買點分級",
    "風險說明",
    "股神推論邏輯",
    "權重設定",
    "推薦分桶",
    "飆股起漲分數",
    "起漲等級",
    "起漲摘要",
    "信心等級",
    "技術結構分數",
    "起漲前兆分數",
    "交易可行分數",
    "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", 
    "同類股領先幅度",
    "是否領先同類股",
    "推薦標籤",
    "推薦理由摘要",
    "K線驗證標記",
    "推薦日價格",
    "推薦日支撐壓力摘要",
    "K線查詢參數",
    "K線檢視提示",
    "推薦價格",
    "停損價",
    "賣出目標1",
    "賣出目標2",
    "推薦日期",
    "推薦時間",
    "建立時間",
    "更新時間",
    "目前狀態",
    "是否已實際買進",
    "實際買進價",
    "實際賣出價",
    "實際報酬%",
    "最新價",
    "最新更新時間",
    "損益金額",
    "損益幅%",
    "是否達停損",
    "是否達目標1",
    "是否達目標2",
    "持有天數",
    "推薦後1日%",
    "推薦後3日%",
    "推薦後5日%",
    "推薦後10日%",
    "推薦後20日%",
    "推薦後最大漲幅%",
    "推薦後最大回撤%",
    "是否達標_回測",
    "是否停損_回測",
    "命中結果",
    "績效評語",
    "追蹤更新時間",
    "模式績效標籤",
    "備註",
]

# V101：隔日作戰命中追蹤欄位。
# 只在使用者按下「更新隔日命中追蹤」時抓歷史K線，不在頁面載入時自動抓，避免拖慢顯示。
NIGHT_HIT_TRACKING_COLUMNS = [
    "作戰追蹤狀態", "進場點命中", "進場點命中日期",
    "突破價命中", "突破價命中日期",
    "停損價觸發", "停損價觸發日期",
    "第一壓力命中", "第一壓力命中日期",
    "隔日最高漲幅%", "3日最高漲幅%", "5日最高漲幅%", "10日最高漲幅%",
    "隔日最低回撤%", "3日最低回撤%", "5日最低回撤%", "10日最低回撤%",
    "作戰命中摘要", "作戰追蹤資料源", "作戰追蹤更新時間",
]
for _v101_col in NIGHT_HIT_TRACKING_COLUMNS:
    if _v101_col not in GODPICK_RECORD_COLUMNS:
        GODPICK_RECORD_COLUMNS.append(_v101_col)

# V119：讓 10_推薦清單可保存 07 V118 實戰品質欄位。
for _v119_col in PRACTICAL_QUALITY_COLUMNS_V119:
    if _v119_col not in GODPICK_RECORD_COLUMNS:
        GODPICK_RECORD_COLUMNS.append(_v119_col)

# V110：10_推薦清單保存 16_官方因子快取中心欄位。
# 僅讀 official_factors_cache.json，不在本頁即時連官方網站，避免拖慢顯示。
OFFICIAL_FACTOR_COLUMNS_V110 = list(OFFICIAL_FACTOR_SERVICE_COLUMNS or [
    "官方資料日期", "外資近1日買賣超", "外資近3日買賣超", "外資近5日買賣超",
    "投信近1日買賣超", "投信近3日買賣超", "投信近5日買賣超",
    "自營商近1日買賣超", "自營商近3日買賣超", "自營商近5日買賣超",
    "三大法人近1日合計", "三大法人近3日合計", "三大法人近5日合計",
    "法人連買天數", "法人籌碼官方分數", "當月營收", "月營收MoM%", "月營收YoY%",
    "累計營收YoY%", "營收年月", "營收成長官方分數", "PER本益比", "PBR股價淨值比",
    "股利殖利率%", "估算EPS", "官方估值風險分數", "官方基本面成長分數",
    "官方因子總分", "官方資料完整度", "官方因子資料狀態", "官方因子更新時間", "官方因子資料源",
])
OFFICIAL_FACTOR_COLUMNS_V110 = [c for c in OFFICIAL_FACTOR_COLUMNS_V110 if c not in {"股票代號", "股票名稱", "市場別", "正式產業別"}]
OFFICIAL_FACTOR_NUMERIC_COLUMNS_V110 = [
    "外資近1日買賣超", "外資近3日買賣超", "外資近5日買賣超",
    "投信近1日買賣超", "投信近3日買賣超", "投信近5日買賣超",
    "自營商近1日買賣超", "自營商近3日買賣超", "自營商近5日買賣超",
    "三大法人近1日合計", "三大法人近3日合計", "三大法人近5日合計",
    "法人連買天數", "法人籌碼官方分數", "當月營收", "月營收MoM%", "月營收YoY%",
    "累計營收YoY%", "營收成長官方分數", "PER本益比", "PBR股價淨值比",
    "股利殖利率%", "估算EPS", "官方估值風險分數", "官方基本面成長分數",
    "官方因子總分", "官方資料完整度",
]
for _v110_col in OFFICIAL_FACTOR_COLUMNS_V110:
    if _v110_col not in GODPICK_RECORD_COLUMNS:
        GODPICK_RECORD_COLUMNS.append(_v110_col)


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

def _k(key: str) -> str:
    return f"{PFX}{key}"



# v15 欄位統一：推薦清單欄位與 7_股神推薦、8_股神推薦紀錄、12_股神管理中心一致。
try:
    if UNIFIED_RECOMMEND_DISPLAY_COLUMNS:
        GODPICK_RECORD_COLUMNS = shared_dedupe_keep_order((GODPICK_RECORD_COLUMNS or []) + list(UNIFIED_RECOMMEND_DISPLAY_COLUMNS)) if shared_dedupe_keep_order else list(dict.fromkeys((GODPICK_RECORD_COLUMNS or []) + list(UNIFIED_RECOMMEND_DISPLAY_COLUMNS)))
except Exception:
    pass

# V94：在 main() 執行前即補入夜間隔日欄位，避免舊推薦清單 / 舊快取缺欄導致 KeyError。
try:
    for _c in NIGHT_GODPICK_COLUMNS:
        if _c not in GODPICK_RECORD_COLUMNS:
            GODPICK_RECORD_COLUMNS.append(_c)
except Exception:
    pass

def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _safe_float(v: Any, default=None):
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return default




def _is_blank_value(v: Any) -> bool:
    """判斷畫面用空值：None / NaN / 空字串 / 字串 None 都視為空白。"""
    if v is None:
        return True
    try:
        if pd.isna(v):
            return True
    except Exception:
        pass
    return str(v).strip() in {"", "None", "none", "nan", "NaN", "NAN", "<NA>", "NaT"}


def _cell_safe_value(v: Any) -> Any:
    """V97：寫入 DataFrame 單一儲存格前，先把 list/dict/Series/array 轉成安全字串。
    Streamlit Cloud / pandas 3.x 對 object 欄位寫入 list-like 值時，可能把它當成多欄展開，
    導致 TypeError。本函式確保推薦清單舊 JSON 欄位不會把整頁打掛。
    """
    try:
        if isinstance(v, pd.Series):
            vals = [x for x in v.tolist() if not _is_blank_value(x)]
            if not vals:
                return ""
            return vals[0] if len(vals) == 1 else " / ".join(str(x) for x in vals)
    except Exception:
        pass
    try:
        if isinstance(v, pd.DataFrame):
            return v.to_json(force_ascii=False)
    except Exception:
        pass
    if isinstance(v, dict):
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)
    if isinstance(v, (list, tuple, set)):
        vals = []
        for item in list(v):
            if not _is_blank_value(item):
                vals.append(str(item))
        return " / ".join(vals)
    try:
        # numpy array / pandas ExtensionArray 等 list-like 物件保底處理。
        if hasattr(v, "tolist") and not isinstance(v, (str, bytes)):
            vv = v.tolist()
            if isinstance(vv, list):
                vals = [str(item) for item in vv if not _is_blank_value(item)]
                return " / ".join(vals)
            return vv
    except Exception:
        pass
    return v


def _safe_set_cell(df: pd.DataFrame, row_label: Any, col_name: str, value: Any) -> None:
    """V97：最保守單格寫入，避免 pandas 對舊 JSON 混合型欄位報 TypeError。"""
    if df is None or col_name not in df.columns:
        return
    try:
        if str(df[col_name].dtype) != "object":
            df[col_name] = df[col_name].astype("object")
    except Exception:
        pass
    val = _cell_safe_value(value)
    try:
        rpos = df.index.get_loc(row_label)
        cpos = df.columns.get_loc(col_name)
        if isinstance(rpos, slice):
            rpos = rpos.start
        elif not isinstance(rpos, int):
            try:
                rpos = int(list(rpos)[0])
            except Exception:
                rpos = 0
        if isinstance(cpos, slice):
            cpos = cpos.start
        elif not isinstance(cpos, int):
            try:
                cpos = int(list(cpos)[0])
            except Exception:
                cpos = 0
        df.iat[rpos, cpos] = val
        return
    except Exception:
        pass
    try:
        df.loc[row_label, col_name] = val
    except Exception:
        try:
            df[col_name] = df[col_name].astype("object")
            df.loc[row_label, col_name] = str(val)
        except Exception:
            pass


def _clean_display_df(df: pd.DataFrame, keep_cols: list[str] | None = None, drop_empty_cols: bool = True) -> pd.DataFrame:
    """
    推薦清單畫面專用：
    1. 移除重複欄位
    2. 把 None / NaN / nan 字串改成空白
    3. 整欄都空白的欄位自動隱藏，但保留 keep_cols
    """
    if df is None or df.empty:
        return pd.DataFrame()
    x = df.copy()
    x = x.loc[:, ~x.columns.duplicated()].copy()
    keep = set(keep_cols or [])

    for c in x.columns:
        x[c] = x[c].map(lambda v: "" if _is_blank_value(v) else v)

    if drop_empty_cols:
        cols = []
        for c in x.columns:
            if c in keep:
                cols.append(c)
                continue
            try:
                if not x[c].map(lambda v: _is_blank_value(v)).all():
                    cols.append(c)
            except Exception:
                cols.append(c)
        x = x[cols].copy()
    return x


def _safe_dataframe(df: pd.DataFrame, *, keep_cols: list[str] | None = None, drop_empty_cols: bool = True, **kwargs) -> None:
    """避免 Streamlit 表格顯示 None 與整排空欄位。"""
    out = _clean_display_df(df, keep_cols=keep_cols, drop_empty_cols=drop_empty_cols)
    if out is None or out.empty:
        st.info("目前沒有可顯示的有效資料。")
        return
    st.dataframe(out, **kwargs)


def _normalize_code(v: Any) -> str:
    s = _safe_str(v)
    if not s:
        return ""
    if s.isdigit():
        return s
    digits = "".join(ch for ch in s if ch.isdigit())
    if 4 <= len(digits) <= 6:
        return digits
    return s


def _now_text() -> str:
    return datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M:%S")


def _github_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("GODPICK_RECORDS_GITHUB_PATH", "godpick_records.json")) or "godpick_records.json",
    }


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"



def _read_json_file_from_github(path_name: str, default):
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return default, "未設定 GITHUB_TOKEN"

    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], path_name),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return default, f"{path_name} 尚未建立"
        if resp.status_code != 200:
            return default, f"讀取 {path_name} 失敗：{resp.status_code}"

        content = resp.json().get("content", "")
        if not content:
            return default, f"{path_name} 內容空白"
        payload = json.loads(base64.b64decode(content).decode("utf-8"))
        return payload, f"已讀取 {path_name}"
    except Exception as e:
        return default, f"讀取 {path_name} 例外：{e}"


def _extract_recommend_rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    """支援 list / dict(records|data|items|recommendations) 等多種推薦資料格式。"""
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        for key in ["records", "data", "items", "recommendations", "latest_recommendations", "rows"]:
            if isinstance(payload.get(key), list):
                rows = payload.get(key, [])
                break
        else:
            rows = []
    else:
        rows = []

    clean_rows = []
    for r in rows:
        if isinstance(r, dict):
            clean_rows.append(dict(r))
    return clean_rows


def _row_dedupe_key(row: dict[str, Any]) -> str:
    rid = _safe_str(row.get("record_id") or row.get("rec_id") or row.get("id"))
    if rid:
        return f"id:{rid}"
    code = _normalize_code(row.get("股票代號") or row.get("code"))
    date_s = _safe_str(row.get("推薦日期") or row.get("date"))
    time_s = _safe_str(row.get("推薦時間") or row.get("time") or row.get("建立時間") or row.get("created_at"))
    score_s = _safe_str(row.get("推薦總分") or row.get("score"))
    return f"{code}|{date_s}|{time_s}|{score_s}"


def _read_rows_from_github_or_local(path_name: str) -> tuple[list[dict[str, Any]], str]:
    """同時讀 GitHub 與本機，避免 GitHub 有舊資料時遮住本機最新推薦清單。"""
    all_rows: list[dict[str, Any]] = []
    msgs: list[str] = []

    payload, msg = _read_json_file_from_github(path_name, [])
    github_rows = _extract_recommend_rows_from_payload(payload)
    msgs.append(f"GitHub {msg}｜{len(github_rows)}筆")
    all_rows.extend(github_rows)

    local_rows: list[dict[str, Any]] = []
    try:
        with open(path_name, "r", encoding="utf-8") as f:
            local_payload = json.load(f)
        local_rows = _extract_recommend_rows_from_payload(local_payload)
        msgs.append(f"本機已讀取 {path_name}｜{len(local_rows)}筆")
        all_rows.extend(local_rows)
    except Exception as e:
        msgs.append(f"本機 {path_name} 讀取失敗/不存在：{e}")

    deduped: dict[str, dict[str, Any]] = {}
    for r in all_rows:
        if not isinstance(r, dict):
            continue
        if not _safe_str(r.get("資料來源")):
            r["資料來源"] = path_name
        key = _row_dedupe_key(r)
        if not key or key == "|||":
            key = f"row:{len(deduped)}"
        # 本機在後面讀，若相同 key，以欄位較完整/較新的本機資料補值。
        if key not in deduped:
            deduped[key] = dict(r)
        else:
            base = deduped[key]
            for k, v in r.items():
                if _safe_str(v) != "":
                    base[k] = v

    return list(deduped.values()), "；".join(msgs)


def _read_recommend_list_from_latest() -> tuple[pd.DataFrame, str]:
    """
    推薦清單讀取強化版：
    1. 優先讀 godpick_recommend_list.json
    2. 若清單空白，自動 fallback 讀 godpick_latest_recommendations.json
    3. 再 fallback 讀 godpick_records.json
    4. 多來源合併去重，避免使用者以為7頁沒有匯入
    """
    all_rows: list[dict[str, Any]] = []
    msgs: list[str] = []

    for path_name in GODPICK_RECOMMEND_SOURCE_FILES:
        rows, msg = _read_rows_from_github_or_local(path_name)
        msgs.append(f"{path_name}：{msg}｜{len(rows)}筆")
        if rows:
            all_rows.extend(rows)

    deduped: dict[str, dict[str, Any]] = {}
    for r in all_rows:
        key = _row_dedupe_key(r)
        if not key or key == "|||":
            key = f"row:{len(deduped)}"
        # 後讀來源若資料較完整，補欄位；不覆蓋已有非空值
        if key not in deduped:
            deduped[key] = r
        else:
            base = deduped[key]
            for k, v in r.items():
                if _safe_str(base.get(k)) == "" and _safe_str(v) != "":
                    base[k] = v

    rows = list(deduped.values())
    df = _ensure_record_columns(pd.DataFrame(rows))
    source_msg = "；".join(msgs)
    if rows:
        source_msg = f"已合併讀取 {len(rows)} 筆｜" + source_msg
    else:
        source_msg = "未讀到推薦資料｜" + source_msg
    return df, source_msg


def _derive_list_prelaunch_grade(row: pd.Series) -> str:
    pre = _safe_float(row.get("起漲前兆分數"), 0) or 0
    burst = _safe_float(row.get("爆發力分數"), 0) or 0
    pattern = _safe_float(row.get("型態突破分數"), 0) or 0
    mix = pre * 0.6 + burst * 0.25 + pattern * 0.15
    if mix >= 88:
        return "S｜強烈起漲"
    if mix >= 78:
        return "A｜起漲優先"
    if mix >= 68:
        return "B｜轉強確認"
    if mix >= 55:
        return "C｜初步轉強"
    return "D｜尚未起漲"


def _derive_list_buy_grade(row: pd.Series) -> str:
    score = _safe_float(row.get("推薦總分"), 0) or 0
    pre = _safe_float(row.get("起漲前兆分數"), 0) or 0
    trade = _safe_float(row.get("交易可行分數"), 0) or 0
    if score >= 88 and pre >= 75 and trade >= 70:
        return "A+｜可積極觀察"
    if score >= 80 and trade >= 65:
        return "A｜優先觀察"
    if score >= 72:
        return "B｜等確認"
    if score >= 60:
        return "C｜僅觀察"
    return "D｜暫不追價"


def _derive_list_risk(row: pd.Series) -> str:
    stop_loss = row.get("停損價")
    target1 = row.get("賣出目標1")
    parts = []
    if pd.notna(stop_loss):
        parts.append(f"停損 {format_number(stop_loss, 2)}")
    if pd.notna(target1):
        parts.append(f"目標1 {format_number(target1, 2)}")
    if _safe_float(row.get("交易可行分數"), 0) < 55:
        parts.append("交易可行偏低")
    return "｜".join(parts) if parts else "依原推薦風控"


def _derive_list_logic(row: pd.Series) -> str:
    parts = []
    if _safe_str(row.get("類別")):
        parts.append(_safe_str(row.get("類別")))
    if _safe_float(row.get("起漲前兆分數"), 0) >= 75:
        parts.append("起漲前兆強")
    if _safe_float(row.get("類股熱度分數"), 0) >= 75:
        parts.append("類股熱度高")
    if _safe_str(row.get("是否領先同類股")).lower() in ["true", "1", "是"]:
        parts.append("領先同類股")
    if _safe_float(row.get("交易可行分數"), 0) >= 70:
        parts.append("進出場清楚")
    return "、".join(parts) if parts else _safe_str(row.get("推薦理由摘要")) or "觀察名單"





# =========================================================
# V5 舊資料補值：避免推薦清單顯示 None
# =========================================================
def _derive_v5_from_legacy_row(row: pd.Series) -> dict[str, Any]:
    score = _safe_float(row.get("推薦總分"), 0) or 0
    burst = _safe_float(row.get("飆股起漲分數"), row.get("起漲前兆分數")) or 0
    tech = _safe_float(row.get("技術結構分數"), 0) or 0
    buy_grade = _safe_str(row.get("買點分級"))
    macro_bucket = _safe_str(row.get("大盤情境分桶")) or "舊資料未串聯大盤"
    price = _safe_float(row.get("最新價"), row.get("推薦價格"))
    stop = _safe_float(row.get("停損價"))
    target1 = _safe_float(row.get("賣出目標1"))

    if burst >= 78:
        decision_mode = "飆股起漲模式"
    elif tech >= 72:
        decision_mode = "波段順勢模式"
    elif "C" in buy_grade:
        decision_mode = "觀察等待模式"
    else:
        decision_mode = "綜合精選模式"

    stop_dist = None
    target_ret = None
    rr = None
    if price not in [None, 0] and stop not in [None, 0]:
        stop_dist = max(0, (price - stop) / price * 100)
    if price not in [None, 0] and target1 not in [None, 0]:
        target_ret = max(0, (target1 - price) / price * 100)
    if stop_dist not in [None, 0] and target_ret is not None:
        rr = target_ret / stop_dist

    chase = 35.0
    if burst >= 90:
        chase += 25
    elif burst >= 78:
        chase += 15
    elif burst >= 68:
        chase += 8
    if "C" in buy_grade:
        chase += 8
    chase = max(0, min(100, chase))

    if score >= 88 and chase < 75 and (rr is None or rr >= 1.2):
        advice = "可優先觀察進場"
    elif score >= 80:
        advice = "等突破或回測確認"
    elif score >= 70:
        advice = "列入觀察名單"
    else:
        advice = "暫不建議進場"

    if advice == "可優先觀察進場":
        layer = "今日可進攻"
    elif chase >= 75 and score >= 85:
        layer = "高分但過熱"
    elif score >= 80:
        layer = "等突破確認"
    elif score >= 70:
        layer = "觀察不追"
    else:
        layer = "淘汰但接近條件"

    pos = 0
    if score >= 90:
        pos = 20
    elif score >= 85:
        pos = 15
    elif score >= 78:
        pos = 10
    elif score >= 70:
        pos = 5
    if chase >= 75:
        pos = max(0, pos - 8)
    if rr is not None and rr < 1:
        pos = max(0, pos - 5)

    no_buy = []
    if chase >= 75:
        no_buy.append("追價風險偏高")
    if stop_dist is not None and stop_dist >= 8:
        no_buy.append("停損距離偏大")
    if rr is not None and rr < 1:
        no_buy.append("風險報酬比不足")
    if "C" in buy_grade:
        no_buy.append("買點仍需確認")

    script_parts = [advice]
    if price:
        script_parts.append(f"現價 {price:.2f}")
    if stop:
        script_parts.append(f"失效停損 {stop:.2f}")
    if target1:
        script_parts.append(f"第一目標 {target1:.2f}")

    return {
        "股神決策模式": decision_mode,
        "股神進場建議": advice,
        "推薦分層": layer,
        "建議部位%": round(pos, 1),
        "風險報酬比": round(rr, 2) if rr is not None else "",
        "追價風險分": round(chase, 2),
        "停損距離%": round(stop_dist, 2) if stop_dist is not None else "",
        "目標報酬%": round(target_ret, 2) if target_ret is not None else "",
        "不建議買進原因": "、".join(no_buy) if no_buy else "未觸發主要否決條件",
        "最佳操作劇本": "｜".join(script_parts),
        "隔日操作建議": "開高不追，等量價確認" if chase >= 75 else "等量價確認後再動作",
        "失效價位": stop if stop is not None else "",
        "轉弱條件": f"跌破停損 {stop:.2f}、跌破MA20且量增" if stop else "跌破MA20且量增",
        "大盤情境調權說明": "舊資料未串聯大盤；請由7頁重新推薦可取得完整大盤調權" if "舊資料" in macro_bucket else macro_bucket,
        "大盤情境分桶": macro_bucket,
    }


def _backfill_v10_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    x = df.copy()
    x = x.loc[:, ~x.columns.duplicated()].copy()
    v10_cols = [
        "股神決策模式", "股神進場建議", "進場時機", "進場時機分數", "建議動作", "等待條件", "操作區間", "追高風險等級", "是否建議追價", "推薦分層", "建議部位%", "建議倉位%", "建議投入等級", "分批策略", "最大風險%", "單檔風險等級", "族群集中警示", "組合配置建議", "大盤策略模式", "大盤多空分數", "推薦積極度係數", "適合推薦型態", "大盤策略建議", "大盤風控建議", "市場策略調整說明", "動態建議倉位%", "風險報酬比", "追價風險分",
        "停損距離%", "目標報酬%", "不建議買進原因", "最佳操作劇本", "隔日操作建議",
        "失效價位", "轉弱條件", "大盤情境調權說明", "大盤情境分桶", "大盤橋接分數", "大盤橋接狀態", "大盤橋接加權", "大盤橋接風控", "大盤橋接策略", "大盤橋接更新時間", "大盤交易時段", "大盤交易時段可用", "大盤資料品質", "大盤影響加減分", "大盤影響說明", "大盤資料診斷摘要"
    ]
    for c in v10_cols:
        if c not in x.columns:
            x[c] = ""

    # v16 型別安全修正：
    # 這些欄位有些原本被 pandas 判斷為 float / bool / category，
    # 但補值時會寫入「拉回布局、等待回測」等文字；若不先轉 object，
    # Streamlit Cloud 會在 _safe_set_cell(x, idx, c, v) 時噴 TypeError。
    for c in v10_cols:
        if c in x.columns:
            try:
                x[c] = x[c].astype("object")
            except Exception:
                pass

    for idx, row in x.iterrows():
        need = any(_safe_str(row.get(c)) in ["", "None", "nan", "NaN"] for c in ["股神決策模式", "股神進場建議", "推薦分層"])
        if not need:
            continue
        fill = _derive_v5_from_legacy_row(row)
        for c, v in fill.items():
            if c in x.columns and _safe_str(x.at[idx, c]) in ["", "None", "nan", "NaN"]:
                try:
                    _safe_set_cell(x, idx, c, v)
                except Exception:
                    # 保底：再次轉 object 後寫入，避免 dtype 衝突讓整頁掛掉。
                    x[c] = x[c].astype("object")
                    _safe_set_cell(x, idx, c, v)
    for c in x.columns:
        if x[c].dtype == object:
            x[c] = x[c].replace(["None", "nan", "NaN"], "")
    return x


def _first_available_value(row: Any, cols: list[str], default: Any = "") -> Any:
    for c in cols:
        try:
            if c in row.index:
                v = row.get(c)
            elif isinstance(row, dict):
                v = row.get(c)
            else:
                continue
            if not _is_blank_value(v):
                return v
        except Exception:
            continue
    return default


def _classify_night_action(row: pd.Series) -> str:
    """V94：推薦清單端只做安全補字串，不取代 07 的核心策略判斷。"""
    direct = _first_available_value(row, ["隔日建議動作", "股神建議動作", "建議動作", "今日操作建議"], "")
    if not _is_blank_value(direct):
        return str(direct)
    entry = _safe_float(_first_available_value(row, ["隔日進場分數", "交易可行分數", "進場時機分數"], None), None)
    nscore = _safe_float(_first_available_value(row, ["夜間股神總分", "隔日實戰排序分", "推薦總分", "推薦分數"], None), None)
    risk = str(_first_available_value(row, ["追高風險等級", "單檔風險等級", "夜間風險提醒", "風險說明"], ""))
    if entry is not None and entry >= 82:
        return "隔日高度關注，符合條件可分批"
    if entry is not None and entry >= 72:
        return "等待突破或回測確認"
    if nscore is not None and nscore >= 80:
        return "列入觀察，不追高"
    if "高" in risk or "過熱" in risk:
        return "風險偏高，等拉回"
    return "觀察"


def _classify_night_pattern(row: pd.Series) -> str:
    direct = _first_available_value(row, ["進場型態_隔日", "進場型態", "機會型態", "推薦型態"], "")
    if not _is_blank_value(direct):
        return str(direct)
    entry = _safe_float(_first_available_value(row, ["隔日進場分數", "交易可行分數", "進場時機分數"], None), None)
    pull = _safe_float(_first_available_value(row, ["拉回承接分數", "支撐回測分數"], None), None)
    pre = _safe_float(_first_available_value(row, ["起漲前兆分數", "飆股起漲分數", "機會股分數"], None), None)
    if entry is not None and entry >= 82:
        return "隔日突破型"
    if pull is not None and pull >= 70:
        return "回測承接型"
    if pre is not None and pre >= 70:
        return "剛起漲型"
    return "夜間觀察型"


def _backfill_night_battle_columns(x: pd.DataFrame) -> pd.DataFrame:
    """V97：讓 10_推薦清單完整承接 07 夜間隔日股神欄位，並安全相容舊 JSON 混合型資料。"""
    if x is None:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)
    x = x.copy()
    x = x.loc[:, ~x.columns.duplicated()].copy()
    # V95 hotfix：GitHub/JSON 舊紀錄可能帶有重複或非連續 index，
    # 直接用 loc 指派 apply 結果時會觸發 pandas TypeError。
    # 推薦清單本區塊只需要列資料，不依賴原始 index，因此先重建 index。
    x = x.reset_index(drop=True)
    for c in NIGHT_GODPICK_COLUMNS:
        if c not in x.columns:
            x[c] = None

    # 分數欄位相容舊資料：舊推薦只有推薦總分時，至少讓夜間追蹤表可排序與顯示。
    alias_pairs = {
        "夜間股神總分": ["隔日實戰排序分", "股神決策分數", "推薦總分", "推薦分數"],
        "隔日實戰排序分": ["夜間股神總分", "股神決策分數", "推薦總分", "推薦分數"],
        "隔日進場分數": ["交易可行分數", "進場時機分數", "股神決策分數", "推薦總分"],
        "波段潛力分數": ["起漲前兆分數", "技術結構分數", "飆股起漲分數", "推薦總分"],
        "技術趨勢分數": ["技術結構分數", "型態突破分數", "推薦總分"],
        "量價動能分數": ["起漲前兆分數", "量能訊號", "推薦總分"],
        "預估進場點": ["股神進場區間", "建議切入區", "操作區間", "推薦價格", "推薦日價格", "最新價"],
        "回測承接價": ["推薦買點_拉回", "近端支撐", "主要支撐", "推薦價格"],
        "突破確認價_隔日": ["突破確認價", "推薦買點_突破", "近端壓力"],
        "停損價_隔日": ["停損價", "停損參考", "失效價位"],
        "第一壓力價": ["近端壓力", "賣出目標1", "突破確認價"],
        "夜間股神建議": ["股神進場建議", "股神建議動作", "今日操作建議", "建議動作"],
        "隔日作戰策略": ["最佳操作劇本", "隔日操作建議", "決策說明", "股神推論邏輯"],
        "夜間風險提醒": ["風險說明", "風險扣分原因", "不建議買進原因"],
        "資料完整度": ["資料完整度", "大盤資料品質", "隔夜資料品質"],
    }
    for target, sources in alias_pairs.items():
        if target not in x.columns:
            x[target] = None
        srcs = [c for c in sources if c in x.columns and c != target]
        if not srcs:
            continue
        mask = x[target].map(_is_blank_value)
        if mask.any():
            def _pick(row):
                return _first_available_value(row, srcs, row.get(target, ""))
            vals = x.loc[mask].apply(_pick, axis=1).tolist()
            # V96 hotfix：用逐列 at 指派，避免 pandas 在 object/list/字串混合資料時
            # 將 list 當成可展開陣列而觸發 TypeError。
            for _idx, _val in zip(x.index[mask], vals):
                _safe_set_cell(x, _idx, target, _val)

    if "進場型態_隔日" in x.columns:
        mask = x["進場型態_隔日"].map(_is_blank_value)
        if mask.any():
            vals = x.loc[mask].apply(_classify_night_pattern, axis=1).tolist()
            for _idx, _val in zip(x.index[mask], vals):
                _safe_set_cell(x, _idx, "進場型態_隔日", _val)
    if "隔日建議動作" in x.columns:
        mask = x["隔日建議動作"].map(_is_blank_value)
        if mask.any():
            vals = x.loc[mask].apply(_classify_night_action, axis=1).tolist()
            for _idx, _val in zip(x.index[mask], vals):
                _safe_set_cell(x, _idx, "隔日建議動作", _val)
    if "資料完整度" in x.columns:
        mask = x["資料完整度"].map(_is_blank_value)
        if mask.any():
            for _idx in x.index[mask]:
                _safe_set_cell(x, _idx, "資料完整度", "舊資料相容補欄")

    for c in NIGHT_NUMERIC_COLUMNS:
        if c in x.columns:
            # 價格區間字串不要硬轉掉；只有純數字欄位轉數值。
            if c in {"預估進場點", "回測承接價", "突破確認價_隔日", "停損價_隔日", "第一壓力價"}:
                continue
            x[c] = pd.to_numeric(x[c], errors="coerce")
    return x


def _render_night_battle_tracker(filtered_df: pd.DataFrame) -> None:
    """V94：10_推薦清單新增夜間隔日作戰追蹤區，不影響主表欄位管理。"""
    render_pro_section("夜間隔日作戰追蹤｜承接 07 股神推薦")
    if filtered_df is None or filtered_df.empty:
        st.info("目前篩選條件下沒有資料可追蹤。")
        return
    x = _backfill_night_battle_columns(filtered_df)
    score = pd.to_numeric(x.get("隔日實戰排序分"), errors="coerce")
    entry = pd.to_numeric(x.get("隔日進場分數"), errors="coerce")
    potential = pd.to_numeric(x.get("波段潛力分數"), errors="coerce")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("隔日作戰筆數", len(x))
    with c2:
        st.metric("平均隔日分", format_number(entry.dropna().mean(), 1) if entry.notna().any() else "—")
    with c3:
        st.metric("高度關注", int((entry >= 80).sum()) if entry.notna().any() else 0)
    with c4:
        st.metric("波段潛力>=80", int((potential >= 80).sum()) if potential.notna().any() else 0)
    with c5:
        st.metric("夜間均分", format_number(score.dropna().mean(), 1) if score.notna().any() else "—")

    left, right = st.columns([1, 1])
    with left:
        type_options = ["全部"] + sorted([v for v in x.get("進場型態_隔日", pd.Series(dtype=str)).fillna("").astype(str).unique().tolist() if v])
        type_filter = st.selectbox("進場型態", type_options, key=_k("night_type_filter"))
    with right:
        action_options = ["全部"] + sorted([v for v in x.get("隔日建議動作", pd.Series(dtype=str)).fillna("").astype(str).unique().tolist() if v])
        action_filter = st.selectbox("隔日建議動作", action_options, key=_k("night_action_filter"))
    if type_filter != "全部":
        x = x[x["進場型態_隔日"].astype(str) == str(type_filter)].copy()
    if action_filter != "全部":
        x = x[x["隔日建議動作"].astype(str) == str(action_filter)].copy()

    sort_col = "隔日實戰排序分" if "隔日實戰排序分" in x.columns else "推薦總分"
    x[sort_col] = pd.to_numeric(x[sort_col], errors="coerce")
    x = x.sort_values(sort_col, ascending=False, na_position="last").copy()
    show_cols = [c for c in NIGHT_BATTLE_DISPLAY_COLUMNS if c in x.columns]
    if not show_cols:
        show_cols = list(x.columns[:25])
    _safe_dataframe(_format_show_df(x[show_cols]), keep_cols=show_cols, use_container_width=True, height=360)
    st.caption("V94：此區只追蹤與顯示 07 已產生的夜間隔日欄位；若資料來源是舊快取，會以既有欄位安全補值，不會重新推薦、不會拖慢頁面。")


# =========================================================
# V101：夜間隔日作戰命中追蹤
# =========================================================
def _extract_price_range_v101(value: Any) -> tuple[float | None, float | None]:
    """從 50.5～51.2 / 50.5-51.2 / 約50.5 這類文字擷取價格區間。"""
    try:
        if value is None or _is_blank_value(value):
            return None, None
        text = str(value).replace(",", "").replace("－", "-").replace("—", "-").replace("～", "-").replace("~", "-")
        nums = [float(x) for x in re.findall(r"\d+(?:\.\d+)?", text)]
        if not nums:
            return None, None
        if len(nums) == 1:
            return nums[0], nums[0]
        return min(nums[:2]), max(nums[:2])
    except Exception:
        return None, None


def _row_recommend_date_v101(row: Any) -> date | None:
    for c in ["推薦日期", "推薦時間", "建立時間", "更新時間"]:
        try:
            v = row.get(c) if hasattr(row, "get") else None
            d = pd.to_datetime(v, errors="coerce")
            if pd.notna(d):
                return d.date()
        except Exception:
            continue
    return None


def _base_price_v101(row: Any) -> float | None:
    for c in ["推薦價格", "推薦日價格", "預估進場點", "最新價", "收盤價"]:
        try:
            lo, hi = _extract_price_range_v101(row.get(c) if hasattr(row, "get") else None)
            if lo is not None and hi is not None and hi > 0:
                return (lo + hi) / 2
        except Exception:
            continue
    return None


def _first_hit_date_v101(hist: pd.DataFrame, low_bound: float | None = None, high_bound: float | None = None, mode: str = "range") -> str:
    """回傳第一個觸價日期。mode=range / break_high / break_low。"""
    if hist is None or hist.empty:
        return ""
    try:
        for _, r in hist.iterrows():
            high = _safe_float(r.get("最高價"), None)
            low = _safe_float(r.get("最低價"), None)
            d = r.get("日期")
            if high is None or low is None:
                continue
            hit = False
            if mode == "break_high" and high_bound is not None:
                hit = high >= float(high_bound)
            elif mode == "break_low" and low_bound is not None:
                hit = low <= float(low_bound)
            else:
                if low_bound is not None and high_bound is not None:
                    hit = (low <= float(high_bound)) and (high >= float(low_bound))
            if hit:
                return pd.to_datetime(d).strftime("%Y-%m-%d")
    except Exception:
        return ""
    return ""


def _horizon_perf_v101(hist: pd.DataFrame, base_price: float | None, n: int) -> tuple[float | None, float | None]:
    if hist is None or hist.empty or base_price is None or base_price <= 0:
        return None, None
    try:
        h = hist.head(int(n)).copy()
        if h.empty:
            return None, None
        max_high = pd.to_numeric(h.get("最高價"), errors="coerce").max()
        min_low = pd.to_numeric(h.get("最低價"), errors="coerce").min()
        up = ((float(max_high) - float(base_price)) / float(base_price)) * 100 if pd.notna(max_high) else None
        dd = ((float(min_low) - float(base_price)) / float(base_price)) * 100 if pd.notna(min_low) else None
        return up, dd
    except Exception:
        return None, None


def _calc_night_hit_for_row_v101(row: Any, timeout: int = 7) -> dict[str, Any]:
    code = _normalize_code(row.get("股票代號") if hasattr(row, "get") else "")
    if not code:
        return {"作戰追蹤狀態": "缺股票代號", "作戰追蹤更新時間": _now_text()}
    rec_d = _row_recommend_date_v101(row)
    if rec_d is None:
        return {"作戰追蹤狀態": "缺推薦日期", "作戰追蹤更新時間": _now_text()}
    # 推薦當日不算命中，從隔一個日曆日開始抓，實際交易日由資料源決定。
    start = rec_d + timedelta(days=1)
    end = date.today()
    if end < start:
        return {"作戰追蹤狀態": "尚未到隔日", "作戰追蹤更新時間": _now_text()}
    market = _safe_str(row.get("市場別") if hasattr(row, "get") else "")
    name = _safe_str(row.get("股票名稱") if hasattr(row, "get") else "")
    hist, source = fetch_multi_source_history(code, name, market, start_date=start, end_date=end, timeout=timeout)
    if hist is None or hist.empty:
        return {"作戰追蹤狀態": "歷史K線不足", "作戰追蹤資料源": source, "作戰追蹤更新時間": _now_text()}
    hist = hist.copy()
    hist["日期"] = pd.to_datetime(hist["日期"], errors="coerce")
    hist = hist.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)
    base = _base_price_v101(row)
    entry_lo, entry_hi = _extract_price_range_v101(_first_available_value(row, ["預估進場點", "股神進場區間", "推薦價格", "推薦日價格", "最新價"], ""))
    pull_lo, pull_hi = _extract_price_range_v101(_first_available_value(row, ["回測承接價", "近端支撐", "主要支撐"], ""))
    break_lo, break_hi = _extract_price_range_v101(_first_available_value(row, ["突破確認價_隔日", "突破確認價", "近端壓力"], ""))
    stop_lo, stop_hi = _extract_price_range_v101(_first_available_value(row, ["停損價_隔日", "停損價", "停損參考", "失效價位"], ""))
    pressure_lo, pressure_hi = _extract_price_range_v101(_first_available_value(row, ["第一壓力價", "賣出目標1", "近端壓力"], ""))
    # 進場點若沒有明確區間，用回測承接價補；仍沒有才用推薦價。
    if entry_lo is None and pull_lo is not None:
        entry_lo, entry_hi = pull_lo, pull_hi
    entry_date = _first_hit_date_v101(hist, entry_lo, entry_hi, mode="range") if entry_lo is not None else ""
    break_date = _first_hit_date_v101(hist, high_bound=break_hi, mode="break_high") if break_hi is not None else ""
    stop_date = _first_hit_date_v101(hist, low_bound=stop_lo, mode="break_low") if stop_lo is not None else ""
    pressure_date = _first_hit_date_v101(hist, high_bound=pressure_hi, mode="break_high") if pressure_hi is not None else ""
    result: dict[str, Any] = {
        "進場點命中": "是" if entry_date else "否",
        "進場點命中日期": entry_date,
        "突破價命中": "是" if break_date else "否",
        "突破價命中日期": break_date,
        "停損價觸發": "是" if stop_date else "否",
        "停損價觸發日期": stop_date,
        "第一壓力命中": "是" if pressure_date else "否",
        "第一壓力命中日期": pressure_date,
        "作戰追蹤資料源": source,
        "作戰追蹤更新時間": _now_text(),
    }
    for n, label in [(1, "隔日"), (3, "3日"), (5, "5日"), (10, "10日")]:
        up, dd = _horizon_perf_v101(hist, base, n)
        result[f"{label}最高漲幅%"] = round(up, 2) if up is not None else None
        result[f"{label}最低回撤%"] = round(dd, 2) if dd is not None else None
    if stop_date:
        status = "已觸發停損"
    elif pressure_date:
        status = "第一壓力命中"
    elif break_date:
        status = "突破價命中"
    elif entry_date:
        status = "進場點命中"
    else:
        status = "追蹤中"
    result["作戰追蹤狀態"] = status
    result["作戰命中摘要"] = f"進場:{result['進場點命中']}｜突破:{result['突破價命中']}｜停損:{result['停損價觸發']}｜壓力:{result['第一壓力命中']}"
    return result


def _update_night_hit_tracking_v101(df: pd.DataFrame, max_rows: int = 60, show_progress: bool = True) -> tuple[pd.DataFrame, dict[str, Any]]:
    x = _ensure_record_columns(df).copy() if df is not None else pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)
    for c in NIGHT_HIT_TRACKING_COLUMNS:
        if c not in x.columns:
            x[c] = ""
    if x.empty:
        return x, {"processed": 0, "success": 0, "fail": 0, "messages": ["無推薦清單資料"]}
    # 優先更新尚未追蹤、有夜間欄位或近期推薦的資料。
    work = x.copy()
    work["_rec_dt"] = pd.to_datetime(work.get("推薦日期"), errors="coerce")
    needs = work["作戰追蹤更新時間"].map(_is_blank_value) if "作戰追蹤更新時間" in work.columns else pd.Series([True] * len(work), index=work.index)
    candidates = work[needs].sort_values("_rec_dt", ascending=False, na_position="last").head(int(max_rows))
    if candidates.empty:
        candidates = work.sort_values("_rec_dt", ascending=False, na_position="last").head(int(max_rows))
    bar = st.progress(0.0, text="準備更新隔日命中追蹤...") if show_progress else None
    processed = success = fail = 0
    errors: list[str] = []
    total = max(len(candidates), 1)
    for i, (idx, row) in enumerate(candidates.iterrows(), start=1):
        try:
            if bar is not None:
                bar.progress(min(i / total, 1.0), text=f"更新 {i}/{total}：{row.get('股票代號', '')} {row.get('股票名稱', '')}")
            res = _calc_night_hit_for_row_v101(row, timeout=7)
            for c, v in res.items():
                if c not in x.columns:
                    x[c] = ""
                _safe_set_cell(x, idx, c, v)
            processed += 1
            if str(res.get("作戰追蹤狀態", "")).strip() in {"歷史K線不足", "缺股票代號", "缺推薦日期"}:
                fail += 1
            else:
                success += 1
            # Streamlit Cloud 防卡：每批最多跑 max_rows，不在這裡長時間休眠。
        except Exception as e:
            processed += 1
            fail += 1
            errors.append(f"{row.get('股票代號', '')}:{str(e)[:80]}")
    if bar is not None:
        bar.empty()
    x = x.drop(columns=[c for c in ["_rec_dt"] if c in x.columns], errors="ignore")
    return _ensure_record_columns(x), {"processed": processed, "success": success, "fail": fail, "messages": errors[:8]}


def _render_night_hit_tracker_v101(filtered_df: pd.DataFrame) -> None:
    render_pro_section("隔日命中追蹤｜進場點 / 突破價 / 停損價")
    if filtered_df is None or filtered_df.empty:
        st.info("目前篩選條件下沒有資料可追蹤。")
        return
    x = _ensure_record_columns(filtered_df).copy()
    for c in NIGHT_HIT_TRACKING_COLUMNS:
        if c not in x.columns:
            x[c] = ""
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("已追蹤", int((~x["作戰追蹤更新時間"].map(_is_blank_value)).sum()))
    with c2:
        st.metric("進場命中", int((x["進場點命中"].astype(str) == "是").sum()))
    with c3:
        st.metric("突破命中", int((x["突破價命中"].astype(str) == "是").sum()))
    with c4:
        st.metric("壓力命中", int((x["第一壓力命中"].astype(str) == "是").sum()))
    with c5:
        st.metric("停損觸發", int((x["停損價觸發"].astype(str) == "是").sum()))
    show_cols = [
        "推薦日期", "股票代號", "股票名稱", "進場型態_隔日", "隔日建議動作",
        "預估進場點", "突破確認價_隔日", "停損價_隔日", "第一壓力價",
        "作戰追蹤狀態", "進場點命中", "突破價命中", "第一壓力命中", "停損價觸發",
        "隔日最高漲幅%", "3日最高漲幅%", "5日最高漲幅%", "10日最高漲幅%",
        "作戰命中摘要", "作戰追蹤更新時間",
    ]
    show_cols = [c for c in show_cols if c in x.columns]
    try:
        sort_col = "作戰追蹤更新時間" if "作戰追蹤更新時間" in x.columns else "推薦日期"
        x = x.sort_values(sort_col, ascending=False, na_position="last")
    except Exception:
        pass
    _safe_dataframe(_format_show_df(x[show_cols]), keep_cols=show_cols, use_container_width=True, height=330)
    st.caption("V101：此區只顯示已追蹤結果；只有按下『更新隔日命中追蹤』才會抓歷史K線並寫回，避免影響 10_推薦清單開啟速度。")



def _load_official_factor_map_v110() -> dict[str, dict[str, Any]]:
    """V110：載入 16_官方因子快取中心產生的 official_factors_cache.json。"""
    if _load_official_factor_frame is None:
        return {}
    try:
        fdf = _load_official_factor_frame()
    except Exception:
        return {}
    if fdf is None or fdf.empty or "股票代號" not in fdf.columns:
        return {}
    fdf = fdf.copy()
    fdf["股票代號"] = fdf["股票代號"].map(_normalize_code)
    out: dict[str, dict[str, Any]] = {}
    keep_cols = [c for c in OFFICIAL_FACTOR_COLUMNS_V110 if c in fdf.columns]
    for _, r in fdf.drop_duplicates("股票代號", keep="last").iterrows():
        code = _normalize_code(r.get("股票代號"))
        if not code:
            continue
        out[code] = {c: _cell_safe_value(r.get(c)) for c in keep_cols}
    return out


def _apply_official_factor_backfill_v110(df: pd.DataFrame) -> pd.DataFrame:
    """V110：把官方因子快取安全補進推薦清單；只補空欄，不覆蓋 07 已寫入的值。"""
    if df is None or df.empty:
        return df
    x = df.copy()
    x = x.loc[:, ~x.columns.duplicated()].copy()
    for c in OFFICIAL_FACTOR_COLUMNS_V110:
        if c not in x.columns:
            x[c] = None
    fmap = _load_official_factor_map_v110()
    if not fmap:
        if "官方因子資料狀態" in x.columns:
            mask = x["官方因子資料狀態"].map(_is_blank_value)
            for idx in x.index[mask]:
                _safe_set_cell(x, idx, "官方因子資料狀態", "未讀到官方快取")
        return x
    if "股票代號" not in x.columns:
        return x
    for idx, row in x.iterrows():
        code = _normalize_code(row.get("股票代號"))
        rec = fmap.get(code)
        if not rec:
            continue
        for c, v in rec.items():
            if c not in x.columns:
                x[c] = None
            try:
                if _is_blank_value(x.at[idx, c]):
                    _safe_set_cell(x, idx, c, v)
            except Exception:
                pass
    for c in OFFICIAL_FACTOR_NUMERIC_COLUMNS_V110:
        if c in x.columns:
            x[c] = pd.to_numeric(x[c], errors="coerce")
    return x


def _render_official_factor_tracker_v110(filtered_df: pd.DataFrame) -> None:
    """V110：推薦清單官方因子保存狀態，僅讀快取後顯示，不連外。"""
    render_pro_section("官方因子追蹤｜法人 / 營收 / EPS / PER")
    if filtered_df is None or filtered_df.empty:
        st.info("目前篩選條件下沒有官方因子資料可顯示。")
        return
    x = _apply_official_factor_backfill_v110(filtered_df.copy())
    if x.empty:
        st.info("目前沒有資料。")
        return
    completeness = pd.to_numeric(x.get("官方資料完整度"), errors="coerce") if "官方資料完整度" in x.columns else pd.Series([], dtype="float64")
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("官方因子筆數", len(x))
    with k2:
        st.metric("完整度>=60", int((completeness >= 60).sum()) if not completeness.empty else 0)
    with k3:
        avg_score = pd.to_numeric(x.get("官方因子總分"), errors="coerce").dropna().mean() if "官方因子總分" in x.columns else None
        st.metric("平均官方分", format_number(avg_score, 1) if avg_score == avg_score else "—")
    with k4:
        avg_foreign = pd.to_numeric(x.get("外資近5日買賣超"), errors="coerce").dropna().mean() if "外資近5日買賣超" in x.columns else None
        st.metric("外資5日均買超", format_number(avg_foreign, 0) if avg_foreign == avg_foreign else "—")
    with k5:
        avg_rev = pd.to_numeric(x.get("月營收YoY%"), errors="coerce").dropna().mean() if "月營收YoY%" in x.columns else None
        st.metric("月營收YoY均值", format_number(avg_rev, 1) if avg_rev == avg_rev else "—")
    show_cols = [
        "推薦日期", "股票代號", "股票名稱", "官方因子總分", "官方資料完整度", "官方因子資料狀態",
        "外資近5日買賣超", "投信近5日買賣超", "三大法人近5日合計", "法人連買天數",
        "月營收YoY%", "月營收MoM%", "累計營收YoY%", "PER本益比", "PBR股價淨值比", "估算EPS", "官方因子更新時間",
    ]
    show_cols = [c for c in show_cols if c in x.columns]
    if show_cols:
        try:
            x = x.sort_values(["官方資料完整度", "官方因子總分"], ascending=[False, False], na_position="last")
        except Exception:
            pass
        _safe_dataframe(_format_show_df(x[show_cols].head(300)), keep_cols=show_cols, use_container_width=True, height=320)
    st.caption("V110：此區只讀 16_官方因子快取中心的 official_factors_cache.json，不會在 10 頁即時連官方網站。")


def _derive_practical_quality_v119(df: pd.DataFrame) -> pd.DataFrame:
    """V119：舊資料缺少 V118 欄位時，做安全補欄與輕量推估。

    不重算 07 推薦核心；只用現有欄位回填狀態，避免舊推薦清單顯示空白或 KeyError。
    """
    if df is None or df.empty:
        return df
    x = df.copy()
    for c in PRACTICAL_QUALITY_COLUMNS_V119:
        if c not in x.columns:
            x[c] = None

    def num(col: str, default=0.0):
        if col in x.columns:
            return pd.to_numeric(x[col], errors="coerce").fillna(default)
        return pd.Series([default] * len(x), index=x.index, dtype="float64")

    vol20 = num("20日均量", 0)
    ratio = num("均量比", 0)
    vol_score = num("量能啟動分", 0)
    tech = num("技術結構分數", 0)
    trend = num("均線轉強分", 0)
    momentum = num("動能翻多分", 0)
    ma20 = num("收盤距MA20%", 0)
    ma60 = num("收盤距MA60%", 0)
    rec_score = num("推薦總分", 0)

    # 只在欄位空白時補，不覆蓋 07 已產生的 V118 判斷。
    for idx in x.index:
        try:
            low_liq = ((vol20.loc[idx] > 0 and vol20.loc[idx] < 300000) or (vol_score.loc[idx] < 45 and ratio.loc[idx] < 0.9))
            no_trend = ((tech.loc[idx] < 55 and trend.loc[idx] < 52 and momentum.loc[idx] < 52 and ma20.loc[idx] <= 0) or (ma20.loc[idx] < -3 and ma60.loc[idx] < -3))
            if _is_blank_value(x.at[idx, "量能狀態"]):
                _safe_set_cell(x, idx, "量能狀態", "量能不足" if low_liq else "量能可接受")
            if _is_blank_value(x.at[idx, "趨勢狀態"]):
                _safe_set_cell(x, idx, "趨勢狀態", "無明確上升趨勢" if no_trend else "趨勢可接受")
            if _is_blank_value(x.at[idx, "實戰降分"]):
                penalty = (10 if low_liq else 0) + (12 if no_trend else 0) + (6 if low_liq and no_trend else 0)
                _safe_set_cell(x, idx, "實戰降分", float(min(penalty, 28)))
            if _is_blank_value(x.at[idx, "實戰品質分"]):
                penalty_val = pd.to_numeric(pd.Series([x.at[idx, "實戰降分"]]), errors="coerce").fillna(0).iloc[0]
                base = 100 - float(penalty_val)
                if rec_score.loc[idx] and rec_score.loc[idx] < 55:
                    base -= 5
                _safe_set_cell(x, idx, "實戰品質分", round(max(0, min(100, base)), 1))
            if _is_blank_value(x.at[idx, "實戰品質提醒"]):
                notes = []
                if low_liq:
                    notes.append("量能未確認")
                if no_trend:
                    notes.append("尚未形成上升趨勢")
                _safe_set_cell(x, idx, "實戰品質提醒", "；".join(notes) if notes else "OK")
        except Exception:
            continue

    for c in PRACTICAL_QUALITY_NUMERIC_COLUMNS_V119:
        if c in x.columns:
            x[c] = pd.to_numeric(x[c], errors="coerce")
    return x


def _render_practical_quality_tracker_v119(filtered_df: pd.DataFrame) -> None:
    """V119：推薦清單實戰品質追蹤區。"""
    render_pro_section("實戰品質追蹤｜量能 / 趨勢 / 防呆降分")
    if filtered_df is None or filtered_df.empty:
        st.info("目前篩選條件下沒有實戰品質資料可顯示。")
        return
    x = _derive_practical_quality_v119(_ensure_record_columns(filtered_df).copy())
    if x is None or x.empty:
        st.info("目前沒有資料。")
        return

    q = pd.to_numeric(x.get("實戰品質分"), errors="coerce") if "實戰品質分" in x.columns else pd.Series([], dtype="float64")
    penalty = pd.to_numeric(x.get("實戰降分"), errors="coerce") if "實戰降分" in x.columns else pd.Series([], dtype="float64")
    vol_bad = x.get("量能狀態", pd.Series([], dtype="object")).astype(str).str.contains("不足|低量|冷門", na=False) if "量能狀態" in x.columns else pd.Series([], dtype="bool")
    trend_bad = x.get("趨勢狀態", pd.Series([], dtype="object")).astype(str).str.contains("無明確|弱|未", na=False) if "趨勢狀態" in x.columns else pd.Series([], dtype="bool")

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("平均實戰品質", format_number(q.dropna().mean(), 1) if not q.dropna().empty else "—")
    with k2:
        st.metric("品質>=70", int((q >= 70).sum()) if not q.empty else 0)
    with k3:
        st.metric("量能警示", int(vol_bad.sum()) if not vol_bad.empty else 0)
    with k4:
        st.metric("趨勢警示", int(trend_bad.sum()) if not trend_bad.empty else 0)
    with k5:
        st.metric("平均降分", format_number(penalty.dropna().mean(), 1) if not penalty.dropna().empty else "—")

    c1, c2, c3 = st.columns([1, 1, 1.4])
    with c1:
        quality_filter = st.selectbox(
            "實戰品質篩選",
            ["全部", "品質>=70", "品質<70", "量能警示", "趨勢警示", "有降分"],
            key=_k("v119_quality_filter"),
        )
    with c2:
        min_quality = st.slider("最低實戰品質分", 0, 100, 0, 5, key=_k("v119_min_quality"))
    with c3:
        st.caption("V119：此區承接 07 V118 實戰品質防呆欄位，不重新推薦、不即時抓資料。")

    show = x.copy()
    if min_quality > 0 and "實戰品質分" in show.columns:
        show = show[pd.to_numeric(show["實戰品質分"], errors="coerce").fillna(0) >= min_quality]
    if quality_filter == "品質>=70" and "實戰品質分" in show.columns:
        show = show[pd.to_numeric(show["實戰品質分"], errors="coerce").fillna(0) >= 70]
    elif quality_filter == "品質<70" and "實戰品質分" in show.columns:
        show = show[pd.to_numeric(show["實戰品質分"], errors="coerce").fillna(0) < 70]
    elif quality_filter == "量能警示" and "量能狀態" in show.columns:
        show = show[show["量能狀態"].astype(str).str.contains("不足|低量|冷門", na=False)]
    elif quality_filter == "趨勢警示" and "趨勢狀態" in show.columns:
        show = show[show["趨勢狀態"].astype(str).str.contains("無明確|弱|未", na=False)]
    elif quality_filter == "有降分" and "實戰降分" in show.columns:
        show = show[pd.to_numeric(show["實戰降分"], errors="coerce").fillna(0) > 0]

    show_cols = [c for c in PRACTICAL_QUALITY_DISPLAY_COLUMNS_V119 if c in show.columns]
    if show_cols:
        try:
            show = show.sort_values(["實戰品質分", "推薦總分"], ascending=[False, False], na_position="last")
        except Exception:
            pass
        _safe_dataframe(_format_show_df(show[show_cols].head(300)), keep_cols=show_cols, use_container_width=True, height=340)
    if show.empty:
        st.info("目前篩選後沒有符合條件的實戰品質資料。")

def _ensure_record_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)
    x = df.copy()
    x = x.loc[:, ~x.columns.duplicated()].copy()
    if "record_id" not in x.columns and "rec_id" in x.columns:
        x["record_id"] = x["rec_id"]
    for c in GODPICK_RECORD_COLUMNS:
        if c not in x.columns:
            x[c] = None

    # V110：補入官方因子快取欄位，只補空值，不連外、不覆蓋既有推薦值。
    try:
        x = _apply_official_factor_backfill_v110(x)
    except Exception:
        pass

    # V119：同步 / 輕量補齊 07 V118 實戰品質欄位。
    try:
        x = _derive_practical_quality_v119(x)
    except Exception:
        pass


    if "起漲等級" in x.columns:
        x["起漲等級"] = x["起漲等級"].fillna("").astype(str)
        mask = x["起漲等級"].str.strip() == ""
        if mask.any():
            _vals = x.loc[mask].apply(_derive_list_prelaunch_grade, axis=1).tolist()
            for _idx, _val in zip(x.index[mask], _vals):
                _safe_set_cell(x, _idx, "起漲等級", _val)

    if "買點分級" in x.columns:
        x["買點分級"] = x["買點分級"].fillna("").astype(str)
        mask = x["買點分級"].str.strip() == ""
        if mask.any():
            _vals = x.loc[mask].apply(_derive_list_buy_grade, axis=1).tolist()
            for _idx, _val in zip(x.index[mask], _vals):
                _safe_set_cell(x, _idx, "買點分級", _val)

    if "風險說明" in x.columns:
        x["風險說明"] = x["風險說明"].fillna("").astype(str)
        mask = x["風險說明"].str.strip() == ""
        if mask.any():
            _vals = x.loc[mask].apply(_derive_list_risk, axis=1).tolist()
            for _idx, _val in zip(x.index[mask], _vals):
                _safe_set_cell(x, _idx, "風險說明", _val)

    if "股神推論邏輯" in x.columns:
        x["股神推論邏輯"] = x["股神推論邏輯"].fillna("").astype(str)
        mask = x["股神推論邏輯"].str.strip() == ""
        if mask.any():
            _vals = x.loc[mask].apply(_derive_list_logic, axis=1).tolist()
            for _idx, _val in zip(x.index[mask], _vals):
                _safe_set_cell(x, _idx, "股神推論邏輯", _val)
    num_cols = [
        "推薦總分", "上漲機率估計%", "大盤橋接分數", "大盤可參考分數", "大盤加權分", "大盤影響加減分", "族群資金流分數", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", 
        "同類股領先幅度", "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2",
        "實際買進價", "實際賣出價", "實際報酬%", "最新價", "損益金額", "損益幅%", "持有天數", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%"
    ]
    # v52 安全補欄：舊推薦資料 / Firestore 回補資料可能缺少 v50/v51 新欄位，
    # 任何欄位都必須先建立，再做型態轉換，避免 KeyError 造成整頁掛掉。
    for c in num_cols:
        if c not in x.columns:
            x[c] = None
        x[c] = pd.to_numeric(x[c], errors="coerce")

    bool_cols = ["是否領先同類股", "是否已實際買進", "是否達停損", "是否達目標1", "是否達目標2", "是否達標_回測", "是否停損_回測"]
    for c in bool_cols:
        if c not in x.columns:
            x[c] = False
        x[c] = x[c].fillna(False).map(lambda v: str(v).strip().lower() in {"true", "1", "yes", "y", "是"})

    text_cols = ["推薦日期", "推薦時間", "建立時間", "更新時間", "最新更新時間", "目前狀態", "模式績效標籤", "命中結果", "績效評語", "追蹤更新時間", "備註", "大盤橋接狀態", "大盤橋接加權", "大盤橋接風控", "大盤橋接策略", "大盤橋接更新時間", "大盤交易時段", "大盤交易時段可用", "大盤資料品質", "大盤影響說明", "大盤資料診斷摘要"]
    for c in text_cols:
        if c not in x.columns:
            x[c] = ""
        x[c] = x[c].fillna("").astype(str)

    for c in ["股票代號", "股票名稱"]:
        if c not in x.columns:
            x[c] = ""
    x["股票代號"] = x["股票代號"].map(_normalize_code)
    x["股票名稱"] = x["股票名稱"].fillna("").astype(str)
    x = _backfill_v10_columns(x)
    # v15 欄位統一：共用 schema 回補 7/8/10/12 不同版本欄位名稱。
    try:
        if normalize_godpick_dataframe is not None:
            x = normalize_godpick_dataframe(x, add_missing=True)
    except Exception:
        pass
    x = _backfill_night_battle_columns(x)
    x = x.loc[:, ~x.columns.duplicated()].copy()
    for c in GODPICK_RECORD_COLUMNS:
        if c not in x.columns:
            x[c] = None
    return x[GODPICK_RECORD_COLUMNS].copy()


def _read_records_from_github() -> tuple[pd.DataFrame, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), "未設定 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=25,
        )
        if resp.status_code == 404:
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), "尚未建立 godpick_records.json"
        if resp.status_code != 200:
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), f"讀取推薦清單失敗：{resp.status_code} / {resp.text[:300]}"
        data = resp.json()
        content = data.get("content", "")
        if not content:
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), "推薦清單為空"
        payload = json.loads(base64.b64decode(content).decode("utf-8"))
        if not isinstance(payload, list):
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), "推薦清單格式不是 list"
        return _ensure_record_columns(pd.DataFrame(payload)), ""
    except Exception as e:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), f"讀取推薦清單例外：{e}"


def _get_records_sha() -> tuple[str, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return "", "未設定 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 200:
            return _safe_str(resp.json().get("sha")), ""
        if resp.status_code == 404:
            return "", ""
        return "", f"讀取 SHA 失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return "", f"讀取 SHA 例外：{e}"


def _firebase_ready() -> tuple[bool, str]:
    if firebase_admin is None or credentials is None or firestore is None:
        return False, "firebase_admin 未安裝或不可用"
    return True, ""


def _clean_private_key(raw_key: str) -> str:
    private_key = _safe_str(raw_key).replace("\\n", "\n").strip()
    if private_key.startswith("\ufeff"):
        private_key = private_key.lstrip("\ufeff")
    return private_key


def _init_firebase_app():
    ok, msg = _firebase_ready()
    if not ok:
        raise ValueError(msg)
    try:
        return firebase_admin.get_app()
    except Exception:
        pass
    project_id = _safe_str(st.secrets.get("FIREBASE_PROJECT_ID", ""))
    client_email = _safe_str(st.secrets.get("FIREBASE_CLIENT_EMAIL", ""))
    private_key = _clean_private_key(_safe_str(st.secrets.get("FIREBASE_PRIVATE_KEY", "")))
    if not project_id or not client_email or not private_key:
        raise ValueError("Firebase secrets 不完整")
    cred = credentials.Certificate({
        "type": "service_account",
        "project_id": project_id,
        "private_key": private_key,
        "client_email": client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
    })
    return firebase_admin.initialize_app(cred, {"projectId": project_id})


def _write_records_to_firestore(records: list[dict[str, Any]]) -> tuple[bool, str]:
    try:
        _init_firebase_app()
        db = firestore.client()
        batch = db.batch()
        now = firestore.SERVER_TIMESTAMP
        summary_ref = db.collection("system").document("godpick_records_summary")
        batch.set(summary_ref, {"count": len(records), "updated_at": now, "source": "streamlit_godpick_list"}, merge=True)
        records_ref = db.collection("godpick_records")
        existing = list(records_ref.stream())
        existing_ids = {doc.id for doc in existing}
        new_ids = set()
        for row in records:
            rec_id = _safe_str(row.get("record_id"))
            if not rec_id:
                continue
            new_ids.add(rec_id)
            doc_ref = records_ref.document(rec_id)
            payload = dict(row)
            payload["updated_at"] = now
            batch.set(doc_ref, payload, merge=True)
        for old_id in existing_ids - new_ids:
            batch.delete(records_ref.document(old_id))
        batch.commit()
        return True, "已同步寫入 Firestore"
    except Exception as e:
        return False, f"Firestore 同步失敗：{e}"


def _write_records_to_github(df: pd.DataFrame) -> tuple[bool, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN"
    sha, err = _get_records_sha()
    if err:
        return False, err
    work = _ensure_record_columns(df)
    content_text = json.dumps(work.to_dict(orient="records"), ensure_ascii=False, indent=2)
    encoded = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")
    body: dict[str, Any] = {
        "message": f"update godpick records from 推薦清單 at {_now_text()}",
        "content": encoded,
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha
    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已回寫 GitHub：{cfg['path']}"
        return False, f"GitHub 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"GitHub 寫入例外：{e}"


def _sync_records(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """V191-H3 safe Page10 sync.

    Module 10 owns the *current recommendation list* and its tracking fields; it
    does NOT own the full historical Page08 authority.  Therefore performance
    updates are upserted into matching history rows and may never full-replace
    ``godpick_records.json``.  An empty current list is a safe no-op, never an
    instruction to erase history.
    """
    work = _ensure_record_columns(df)
    messages: list[str] = []
    if not callable(save_named_json_permanent) or not callable(upsert_records_authority_fast):
        messages.append("V191-H3安全永久保存服務未載入")
        st.session_state[_k("last_sync_msgs")] = messages
        return False, messages

    if work.empty:
        messages.append("V191-H3防歸零：本輪推薦清單為 0 筆；略過所有永久覆蓋，不清空08歷史、不清空10清單、不改寫最新推薦快照。")
        st.session_state[_k("last_sync_msgs")] = messages
        return True, messages

    # Update only the matching current recommendation rows in Page08 authority.
    rec_report, rec_stats = upsert_records_authority_fast(
        work.to_dict(orient="records"),
        reason="Page10 tracking metrics incremental upsert (V191-H3)",
    )
    messages.extend([
        f"推薦紀錄增量｜before={rec_stats.get('before',0)} after={rec_stats.get('after',0)} "
        f"added={rec_stats.get('added',0)} updated={rec_stats.get('updated',0)}",
        *["推薦紀錄增量｜" + x for x in rec_report.messages()],
    ])

    # Page10 current list can be rewritten because this file is explicitly the
    # current operational list, not cumulative history.
    list_df = work.copy()
    if "資料來源" in list_df.columns:
        current_mask = list_df["資料來源"].astype(str).isin(["本輪推薦清單", "最新推薦快照", "godpick_recommend_list.json"])
        if current_mask.any():
            list_df = list_df[current_mask].copy()
    if not list_df.empty and "推薦日期" in list_df.columns:
        date_s = pd.to_datetime(list_df["推薦日期"], errors="coerce")
        if date_s.notna().any():
            list_df = list_df[date_s.dt.date == date_s.max().date()].copy()
    list_rows = list_df.drop(columns=["資料來源"], errors="ignore").to_dict(orient="records")
    if not list_rows:
        messages.append("V191-H3防歸零：治理後清單為0筆，保留原10頁清單與最新推薦快照。")
        st.session_state[_k("last_sync_msgs")] = messages
        return bool(rec_report.local_ok), messages

    list_report = save_named_json_permanent("godpick_recommend_list.json", list_rows)
    messages.extend(["推薦清單｜" + x for x in list_report.messages()])

    # Tracking is not a new recommendation run.  Preserve saved_at / execution
    # context / candidate diagnosis.  Only merge metrics into recommendations
    # when the list belongs to the same recommendation date.
    latest_payload, latest_load_msgs = load_named_json_permanent("godpick_latest_recommendations.json", {})
    messages.extend(["最新推薦讀取｜" + str(x) for x in (latest_load_msgs or [])[-4:]])
    latest_payload = dict(latest_payload) if isinstance(latest_payload, dict) else {}
    list_dates = pd.to_datetime(pd.Series([r.get("推薦日期") for r in list_rows]), errors="coerce").dropna()
    list_date = list_dates.max().strftime("%Y-%m-%d") if not list_dates.empty else ""
    snapshot_date = _safe_str(latest_payload.get("recommendation_date"))[:10] or _safe_str(latest_payload.get("saved_at"))[:10]
    latest_report_ok = True
    if latest_payload and list_date and snapshot_date == list_date:
        latest_payload["recommendations"] = list_rows
        latest_payload["performance_updated_at"] = _now_text()
        latest_payload["performance_update_owner"] = "10_推薦清單"
        latest_report = save_named_json_permanent("godpick_latest_recommendations.json", latest_payload)
        latest_report_ok = bool(latest_report.permanent_ok)
        messages.extend(["最新推薦績效增量｜" + x for x in latest_report.messages()])
    else:
        messages.append(
            f"最新推薦快照未改寫：tracking清單日期={list_date or '未取得'}；"
            f"推薦快照日期={snapshot_date or '未取得'}。避免把舊績效追蹤冒充新一輪推薦。"
        )

    st.session_state[_k("last_sync_msgs")] = messages
    # Local authority preservation is mandatory; remote list/snapshot failures
    # are surfaced instead of silently declaring full success.
    return bool(rec_report.local_ok and list_report.permanent_ok and latest_report_ok), messages

def _recommend_sources_signature_v171() -> str:
    parts = []
    for name in ["godpick_records.json", "godpick_recommend_list.json", "godpick_latest_recommendations.json"]:
        try:
            stat = Path(name).stat()
            parts.append(f"{name}:{stat.st_mtime_ns}:{stat.st_size}")
        except Exception:
            parts.append(f"{name}:missing")
    return "|".join(parts)


def _load_records_cached(force: bool = False) -> pd.DataFrame:
    current_sig = _recommend_sources_signature_v171()
    last_sig = _safe_str(st.session_state.get(_k("records_source_sig_v171")))
    if current_sig != last_sig:
        force = True
    if force or _k("records_df") not in st.session_state:
        frames = []
        details = []
        if callable(load_records_permanent):
            try:
                records, d0 = load_records_permanent()
                details.extend(d0)
                if records:
                    rdf = pd.DataFrame(records)
                    rdf["資料來源"] = "股神推薦紀錄"
                    frames.append(rdf)
            except Exception as exc:
                details.append(f"推薦紀錄永久來源失敗：{exc}")
        if callable(load_named_json_permanent):
            for path_name, label, default in [
                ("godpick_recommend_list.json", "本輪推薦清單", []),
                ("godpick_latest_recommendations.json", "最新推薦快照", {}),
            ]:
                try:
                    payload, dd = load_named_json_permanent(path_name, default)
                    details.extend(dd)
                    if isinstance(payload, dict):
                        rows = payload.get("recommendations") or payload.get("data") or payload.get("rows") or []
                    else:
                        rows = payload
                    if isinstance(rows, list) and rows:
                        temp = pd.DataFrame(rows)
                        temp["資料來源"] = label
                        frames.append(temp)
                except Exception as exc:
                    details.append(f"{label}讀取失敗：{exc}")
        if frames:
            merged = pd.concat(frames, ignore_index=True, sort=False)
            def _merge_key(row):
                rid = _safe_str(row.get("record_id"))
                if rid:
                    return "id:" + rid
                return "biz:" + "|".join([
                    _safe_str(row.get("股票代號")),
                    _safe_str(row.get("推薦日期")),
                    _safe_str(row.get("推薦時間")),
                    _safe_str(row.get("推薦模式")),
                    _safe_str(row.get("正式推薦分區")),
                ])
            merged["_merge_key"] = merged.apply(_merge_key, axis=1)
            merged = merged.drop_duplicates(subset=["_merge_key"], keep="last").drop(columns=["_merge_key"], errors="ignore")
        else:
            merged = pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)
        st.session_state[_k("records_df")] = _ensure_record_columns(merged).copy()
        st.session_state[_k("load_msg")] = "｜".join(details)
        st.session_state[_k("load_detail")] = details
        st.session_state[_k("loaded_at")] = _now_text()
        st.session_state[_k("records_source_sig_v171")] = current_sig
    rec = st.session_state.get(_k("records_df"), pd.DataFrame(columns=GODPICK_RECORD_COLUMNS))
    return _ensure_record_columns(rec)




def _filter_df(df: pd.DataFrame, start_date: date, end_date: date, mode: str, status: str, kw: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)
    work = df.copy()
    work["推薦日期_dt"] = pd.to_datetime(work["推薦日期"], errors="coerce").dt.date
    if start_date:
        work = work[work["推薦日期_dt"] >= start_date]
    if end_date:
        work = work[work["推薦日期_dt"] <= end_date]
    if mode and mode != "全部":
        work = work[work["推薦模式"].astype(str) == mode]
    if status and status != "全部":
        work = work[work["目前狀態"].astype(str) == status]
    kw = _safe_str(kw)
    if kw:
        work = work[
            work["股票代號"].astype(str).str.contains(kw, case=False, na=False)
            | work["股票名稱"].astype(str).str.contains(kw, case=False, na=False)
            | work["推薦理由摘要"].astype(str).str.contains(kw, case=False, na=False)
            | work["類別"].astype(str).str.contains(kw, case=False, na=False)
        ]
    return work.sort_values(["推薦日期", "推薦時間", "推薦總分"], ascending=[False, False, False]).drop(columns=["推薦日期_dt"], errors="ignore").reset_index(drop=True)


def _is_blank_series(s: pd.Series) -> bool:
    """判斷整欄是否完全沒有有效資料，避免畫面出現大量空白 / None 欄位。"""
    try:
        if s is None:
            return True
        x = s.astype("object").copy()
        x = x.where(pd.notna(x), "")
        x = x.astype(str).str.strip()
        x = x.replace(["None", "none", "nan", "NaN", "NaT", "<NA>"], "")
        return bool((x == "").all())
    except Exception:
        return False


def _format_show_df(df: pd.DataFrame, drop_empty_cols: bool = True) -> pd.DataFrame:
    """
    推薦清單畫面專用格式化。

    修正重點：
    1. 舊紀錄缺欄位時，_ensure_record_columns 會補 None；畫面不應直接顯示 None。
    2. 大盤橋接、族群、K線驗證等欄位不是每筆資料都有；整欄沒資料時直接隱藏。
    3. 保留有資料的欄位，不影響下載完整 Excel / CSV。
    """
    if df is None or df.empty:
        return pd.DataFrame()

    show = df.copy()
    show = show.loc[:, ~show.columns.duplicated()].copy()
    show = _backfill_v10_columns(show)
    show = show.drop(columns=[c for c in ["record_id"] if c in show.columns])

    # 先把真正的 None / NaN / NaT 全部轉空白，避免 st.dataframe 顯示 None。
    show = show.where(pd.notna(show), "")
    show = show.replace([None, "None", "none", "nan", "NaN", "NaT", "<NA>"], "")

    num1_cols = list(dict.fromkeys([
        "推薦總分", "上漲機率估計%", "族群資金流分數", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分",
        "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群輪動狀態",
        "同類股領先幅度", "實際報酬%", "損益幅%", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%",
        "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "即時追蹤報酬%", "大盤橋接分數", "大盤影響加減分",
        "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "進場時機分數",
        "建議部位%", "建議倉位%", "最大風險%", "大盤多空分數", "推薦積極度係數", "動態建議倉位%",
        "風險報酬比", "追價風險分", "飆股起漲分數"
    ]))
    price_cols = list(dict.fromkeys([
        "推薦價格", "推薦日價格", "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2",
        "實際買進價", "實際賣出價", "最新價", "損益金額"
    ]))

    for c in num1_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: format_number(x, 1) if _safe_str(x) else "")
    for c in price_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: format_number(x, 2) if _safe_str(x) else "")

    # 再清一次，避免格式化後留下 None 字串。
    show = show.where(pd.notna(show), "")
    show = show.replace([None, "None", "none", "nan", "NaN", "NaT", "<NA>"], "")

    if drop_empty_cols:
        keep_always = {
            "資料來源", "推薦日期", "推薦時間", "股票代號", "股票名稱", "市場別", "類別",
            "推薦模式", "推薦型態", "推薦等級", "推薦總分", "推薦價格", "最新價",
            "目前狀態", "建議動作", "股神信心", "股神進場建議", "推薦理由摘要", "風險說明", "備註"
        }
        keep_cols = []
        for c in show.columns:
            if c in keep_always or not _is_blank_series(show[c]):
                keep_cols.append(c)
        show = show[keep_cols]

    return show




def _fetch_yahoo_history_direct_v72(stock_no: str, market_type: str, start_date_value: date, end_date_value: date) -> pd.DataFrame:
    """V72：推薦清單績效更新專用 Yahoo 直接備援。"""
    code = _normalize_code(stock_no)
    if not code:
        return pd.DataFrame()
    mk = _safe_str(market_type)
    suffix_candidates = ["TWO", "TW"] if mk in ["上櫃", "興櫃", "OTC", "TPEX"] else ["TW", "TWO"]
    try:
        p1 = int(pd.Timestamp(start_date_value).timestamp())
        p2 = int((pd.Timestamp(end_date_value) + pd.Timedelta(days=1)).timestamp())
    except Exception:
        return pd.DataFrame()
    headers = {"User-Agent": "Mozilla/5.0"}
    for suffix in suffix_candidates:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}.{suffix}"
            r = requests.get(url, params={"period1": p1, "period2": p2, "interval": "1d", "events": "history", "includeAdjustedClose": "true"}, headers=headers, timeout=8)
            if r.status_code != 200:
                continue
            js = r.json()
            result = (((js or {}).get("chart") or {}).get("result") or [])
            if not result:
                continue
            item = result[0]
            ts = item.get("timestamp") or []
            quote = (((item.get("indicators") or {}).get("quote") or [{}])[0])
            if not ts or not isinstance(quote, dict):
                continue
            rows = []
            for i, t in enumerate(ts):
                close = (quote.get("close") or [None])[i] if i < len(quote.get("close") or []) else None
                if close is None:
                    continue
                rows.append({
                    "日期": pd.to_datetime(int(t), unit="s").tz_localize("UTC").tz_convert("Asia/Taipei").tz_localize(None).date(),
                    "開盤價": (quote.get("open") or [None])[i] if i < len(quote.get("open") or []) else None,
                    "最高價": (quote.get("high") or [None])[i] if i < len(quote.get("high") or []) else None,
                    "最低價": (quote.get("low") or [None])[i] if i < len(quote.get("low") or []) else None,
                    "收盤價": close,
                    "成交量": (quote.get("volume") or [None])[i] if i < len(quote.get("volume") or []) else None,
                })
            df = pd.DataFrame(rows)
            if not df.empty:
                return df
        except Exception:
            continue
    return pd.DataFrame()

def _fetch_history_for_backtest(stock_no: str, stock_name: str, market_type: str, rec_date_text: str) -> pd.DataFrame:
    rec_date = pd.to_datetime(rec_date_text, errors="coerce")
    if pd.isna(rec_date):
        return pd.DataFrame()
    start_date = rec_date.date() - timedelta(days=5)
    end_date = rec_date.date() + timedelta(days=90)
    tried = []
    primary = _safe_str(market_type)
    if primary:
        tried.append(primary)
    for mk in ["上市", "上櫃", "興櫃", ""]:
        if mk not in tried:
            tried.append(mk)
    for mk in tried:
        try:
            try:
                df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_date=start_date, end_date=end_date)
            except TypeError:
                try:
                    df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=mk, start_dt=start_date, end_dt=end_date)
                except Exception:
                    df = get_history_data(code=stock_no, start_date=start_date, end_date=end_date)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return df
        except Exception:
            pass
    # V72：utils 失敗時用 Yahoo chart 直接備援，避免績效更新全部失敗。
    for mk in tried:
        df = _fetch_yahoo_history_direct_v72(stock_no, mk or primary, start_date, end_date)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df

    # V72：第二層備援，加入 Stooq CSV + TWSE/TPEx 官方日行情逐日補抓。
    try:
        alt_df, alt_msg = fetch_multi_source_history(stock_no, stock_name, primary, start_date, end_date)
        if isinstance(alt_df, pd.DataFrame) and not alt_df.empty:
            return alt_df
    except Exception:
        pass
    return pd.DataFrame()


def _calc_proxy_perf_metrics_v71(src: dict[str, Any], reason: str = "") -> dict[str, Any]:
    """v71：推薦清單歷史K線失敗時，用推薦價與最新價產生即時代理績效。"""
    rec_px = _safe_float(src.get("推薦價格")) or _safe_float(src.get("推薦日價格")) or _safe_float(src.get("建議價位"))
    latest = _safe_float(src.get("最新價")) or _safe_float(src.get("最新價格"))
    if rec_px in [None, 0] or latest in [None, 0]:
        return {}
    rec_date = pd.to_datetime(_safe_str(src.get("推薦日期")), errors="coerce")
    age_days = 0
    if pd.notna(rec_date):
        try:
            age_days = max((date.today() - rec_date.date()).days, 0)
        except Exception:
            age_days = 0
    ret = round((latest - rec_px) / rec_px * 100, 2)
    out = {
        "即時追蹤報酬%": ret,
        "績效資料型態": "即時代理",
        "績效資料來源": "推薦價_vs_最新價",
        "績效評語": f"歷史K線暫不可用，先以最新價代理追蹤報酬；原因：{_safe_str(reason)[:80]}",
        "追蹤更新時間": _now_text(),
    }
    if age_days >= 1 and _safe_float(src.get("推薦後1日%")) is None:
        out["推薦後1日%"] = ret
    return out


def _calc_backtest_metrics(row: pd.Series | dict[str, Any]) -> dict[str, Any]:
    src = dict(row)
    rec_date = pd.to_datetime(_safe_str(src.get("推薦日期")), errors="coerce")
    if pd.isna(rec_date):
        return {}
    code = _normalize_code(src.get("股票代號"))
    name = _safe_str(src.get("股票名稱"))
    market = _safe_str(src.get("市場別"))
    df = _fetch_history_for_backtest(code, name, market, _safe_str(src.get("推薦日期")))
    if df.empty:
        return _calc_proxy_perf_metrics_v71(src, "ONLINE_FAIL / 無歷史K線")
    temp = df.copy()
    rename_map = {}
    for c in temp.columns:
        low = str(c).lower()
        if low in {"date", "日期"}:
            rename_map[c] = "日期"
        elif low in {"close", "收盤價"}:
            rename_map[c] = "收盤價"
        elif low in {"high", "最高價"}:
            rename_map[c] = "最高價"
        elif low in {"low", "最低價"}:
            rename_map[c] = "最低價"
    if rename_map:
        temp = temp.rename(columns=rename_map)
    if "日期" not in temp.columns or "收盤價" not in temp.columns:
        return _calc_proxy_perf_metrics_v71(src, "歷史K線欄位不足")
    temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
    for c in ["收盤價", "最高價", "最低價"]:
        if c in temp.columns:
            temp[c] = pd.to_numeric(temp[c], errors="coerce")
    temp = temp.dropna(subset=["日期", "收盤價"]).sort_values("日期").reset_index(drop=True)
    window = temp[temp["日期"].dt.date >= rec_date.date()].reset_index(drop=True)
    if window.empty:
        return _calc_proxy_perf_metrics_v71(src, "推薦日之後無K線資料")
    base_px = _safe_float(window.iloc[0].get("收盤價"))
    if base_px in [None, 0]:
        return _calc_proxy_perf_metrics_v71(src, "基準價異常")
    out: dict[str, Any] = {}
    for d in [1, 3, 5, 10, 20]:
        if len(window) > d:
            px = _safe_float(window.iloc[d].get("收盤價"))
            out[f"推薦後{d}日%"] = None if px in [None, 0] else round((px - base_px) / base_px * 100, 2)
    use_window = window.head(min(len(window), 21)).copy()
    high_col = "最高價" if "最高價" in use_window.columns else "收盤價"
    low_col = "最低價" if "最低價" in use_window.columns else "收盤價"
    max_high = _safe_float(use_window[high_col].max())
    min_low = _safe_float(use_window[low_col].min())
    max_gain = None if max_high in [None, 0] else round((max_high - base_px) / base_px * 100, 2)
    max_drawdown = None if min_low in [None, 0] else round((min_low - base_px) / base_px * 100, 2)
    out["推薦後最大漲幅%"] = max_gain
    out["推薦後最大回撤%"] = max_drawdown
    target = _safe_float(src.get("賣出目標1")) or _safe_float(src.get("近端壓力"))
    stop = _safe_float(src.get("停損參考")) or _safe_float(src.get("停損價"))
    target_hit = bool(target not in [None, 0] and max_high is not None and max_high >= target) if target not in [None, 0] else bool(max_gain is not None and max_gain >= 8)
    stop_hit = bool(stop not in [None, 0] and min_low is not None and min_low <= stop) if stop not in [None, 0] else bool(max_drawdown is not None and max_drawdown <= -6)
    out["是否達標_回測"] = target_hit
    out["是否停損_回測"] = stop_hit
    benchmark = out.get("推薦後20日%") or out.get("推薦後10日%") or out.get("推薦後5日%")
    if target_hit and not stop_hit:
        hit = "達標"
    elif stop_hit and not target_hit:
        hit = "停損"
    elif benchmark is not None and benchmark >= 5:
        hit = "有效"
    elif benchmark is not None and benchmark <= -5:
        hit = "偏弱"
    else:
        hit = "觀察中"
    out["命中結果"] = hit
    out["績效評語"] = {
        "達標": "推薦後已達標，型態有效",
        "停損": "推薦後觸及停損，需檢討風險",
        "有效": "推薦後報酬為正，持續觀察",
        "偏弱": "推薦後轉弱，需檢討等待條件",
        "觀察中": "尚未形成明確績效",
    }.get(hit, "")
    out["追蹤更新時間"] = _now_text()
    return out




# >>> V98_FORMAL_N_DAY_PERF_BACKFILL
FORMAL_N_DAY_COLUMNS_V98 = ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%"]


def _calc_formal_n_day_metrics_v98(row: pd.Series | dict[str, Any]) -> dict[str, Any]:
    """V98：正式 N 日績效回補。

    原則：
    1. 只用歷史K線計算正式 N 日績效，不用即時損益偽裝正式績效。
    2. 推薦價 / 推薦日價格優先作為基準價；若沒有，才用推薦日後第一根收盤價。
    3. 未滿期的 N 日欄位保持空白，避免把尚未發生的績效寫成 0。
    4. 失敗不阻塞整批，保留原資料並寫入績效評語。
    """
    src = dict(row)
    rec_date = pd.to_datetime(_safe_str(src.get("推薦日期")), errors="coerce")
    if pd.isna(rec_date):
        return {"績效評語": "V98 正式N日回補略過：缺推薦日期", "追蹤更新時間": _now_text()}

    code = _normalize_code(src.get("股票代號"))
    name = _safe_str(src.get("股票名稱"))
    market = _safe_str(src.get("市場別"))
    if not code:
        return {"績效評語": "V98 正式N日回補略過：缺股票代號", "追蹤更新時間": _now_text()}

    df = _fetch_history_for_backtest(code, name, market, _safe_str(src.get("推薦日期")))
    if df is None or df.empty:
        old_msg = _safe_str(src.get("績效評語"))
        msg = "V98 正式N日回補：歷史K線不足，保留即時暫行績效"
        return {"績效評語": (old_msg + "｜" + msg).strip("｜") if old_msg else msg, "追蹤更新時間": _now_text()}

    temp = df.copy()
    rename_map = {}
    for c in temp.columns:
        low = str(c).lower().strip()
        if low in {"date", "日期", "datetime", "time"}:
            rename_map[c] = "日期"
        elif low in {"close", "收盤價", "收盤", "收盤價(元)"}:
            rename_map[c] = "收盤價"
        elif low in {"high", "最高價", "最高"}:
            rename_map[c] = "最高價"
        elif low in {"low", "最低價", "最低"}:
            rename_map[c] = "最低價"
    if rename_map:
        temp = temp.rename(columns=rename_map)
    if "日期" not in temp.columns or "收盤價" not in temp.columns:
        old_msg = _safe_str(src.get("績效評語"))
        msg = "V98 正式N日回補：K線缺日期或收盤價欄位"
        return {"績效評語": (old_msg + "｜" + msg).strip("｜") if old_msg else msg, "追蹤更新時間": _now_text()}

    temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
    for c in ["收盤價", "最高價", "最低價"]:
        if c in temp.columns:
            temp[c] = pd.to_numeric(temp[c], errors="coerce")
    temp = temp.dropna(subset=["日期", "收盤價"]).sort_values("日期").reset_index(drop=True)
    window = temp[temp["日期"].dt.date >= rec_date.date()].reset_index(drop=True)
    if window.empty:
        old_msg = _safe_str(src.get("績效評語"))
        msg = "V98 正式N日回補：推薦日之後無K線資料"
        return {"績效評語": (old_msg + "｜" + msg).strip("｜") if old_msg else msg, "追蹤更新時間": _now_text()}

    rec_px = _safe_float(src.get("推薦價格")) or _safe_float(src.get("推薦日價格")) or _safe_float(src.get("建議價位"))
    base_px = rec_px or _safe_float(window.iloc[0].get("收盤價"))
    if base_px in [None, 0]:
        old_msg = _safe_str(src.get("績效評語"))
        msg = "V98 正式N日回補：基準價異常"
        return {"績效評語": (old_msg + "｜" + msg).strip("｜") if old_msg else msg, "追蹤更新時間": _now_text()}

    out: dict[str, Any] = {}
    periods = [1, 3, 5, 10, 20]
    for d in periods:
        # N 代表推薦日後第 N 個交易日；未滿 N 根K線就不寫入。
        if len(window) > d:
            px = _safe_float(window.iloc[d].get("收盤價"))
            if px not in [None, 0]:
                out[f"推薦後{d}日%"] = round((px - base_px) / base_px * 100, 2)

    use_window = window.head(min(len(window), 21)).copy()
    high_col = "最高價" if "最高價" in use_window.columns else "收盤價"
    low_col = "最低價" if "最低價" in use_window.columns else "收盤價"
    max_high = _safe_float(use_window[high_col].max())
    min_low = _safe_float(use_window[low_col].min())
    if max_high not in [None, 0]:
        out["推薦後最大漲幅%"] = round((max_high - base_px) / base_px * 100, 2)
    if min_low not in [None, 0]:
        out["推薦後最大回撤%"] = round((min_low - base_px) / base_px * 100, 2)

    target = _safe_float(src.get("賣出目標1")) or _safe_float(src.get("近端壓力"))
    stop = _safe_float(src.get("停損參考")) or _safe_float(src.get("停損價"))
    max_gain = _safe_float(out.get("推薦後最大漲幅%"))
    max_drawdown = _safe_float(out.get("推薦後最大回撤%"))
    target_hit = bool(target not in [None, 0] and max_high is not None and max_high >= target) if target not in [None, 0] else bool(max_gain is not None and max_gain >= 8)
    stop_hit = bool(stop not in [None, 0] and min_low is not None and min_low <= stop) if stop not in [None, 0] else bool(max_drawdown is not None and max_drawdown <= -6)
    out["是否達標_回測"] = target_hit
    out["是否停損_回測"] = stop_hit

    benchmark = None
    for c in ["推薦後20日%", "推薦後10日%", "推薦後5日%", "推薦後3日%", "推薦後1日%"]:
        if _safe_float(out.get(c)) is not None:
            benchmark = _safe_float(out.get(c))
            break
    if target_hit and not stop_hit:
        hit = "達標"
    elif stop_hit and not target_hit:
        hit = "停損"
    elif benchmark is not None and benchmark >= 5:
        hit = "有效"
    elif benchmark is not None and benchmark <= -5:
        hit = "偏弱"
    elif benchmark is not None:
        hit = "觀察中"
    else:
        hit = _safe_str(src.get("命中結果")) or "觀察中"
    out["命中結果"] = hit
    out["績效評語"] = {
        "達標": "V98 正式N日回補：推薦後已達標，型態有效",
        "停損": "V98 正式N日回補：推薦後觸及停損，需檢討風險",
        "有效": "V98 正式N日回補：推薦後報酬為正，持續觀察",
        "偏弱": "V98 正式N日回補：推薦後轉弱，需檢討等待條件",
        "觀察中": "V98 正式N日回補：已回補可用正式N日績效，持續觀察",
    }.get(hit, "V98 正式N日回補完成")
    out["追蹤更新時間"] = _now_text()
    return out


def _row_needs_formal_n_day_update_v98(payload: dict[str, Any]) -> bool:
    """V98：判斷是否需要正式 N 日績效回補。"""
    code = _normalize_code(payload.get("股票代號"))
    if not code:
        return False
    rec_date = pd.to_datetime(_safe_str(payload.get("推薦日期")), errors="coerce")
    if pd.isna(rec_date):
        return False
    age_days = (date.today() - rec_date.date()).days
    if age_days < 1:
        return False
    due_map = [(1, "推薦後1日%"), (3, "推薦後3日%"), (5, "推薦後5日%"), (10, "推薦後10日%"), (20, "推薦後20日%")]
    for d, col in due_map:
        if age_days >= d and _safe_float(payload.get(col)) is None:
            return True
    # 已有即時損益但沒有命中結果，也可以補正式回測欄位。
    if _safe_str(payload.get("命中結果")) == "" and any(_safe_float(payload.get(c)) is not None for c in FORMAL_N_DAY_COLUMNS_V98):
        return True
    return False


def _update_formal_n_day_metrics_v98(df: pd.DataFrame, max_rows: int = 80, show_progress: bool = True) -> tuple[pd.DataFrame, dict[str, Any]]:
    """V98：正式 N 日績效回補批次處理。"""
    if df is None or df.empty:
        return _ensure_record_columns(pd.DataFrame()), {"candidates": 0, "processed": 0, "success": 0, "fail": 0, "messages": ["無資料可回補"]}
    work = _ensure_record_columns(df.copy()).reset_index(drop=True)
    candidates = [i for i, row in work.iterrows() if _row_needs_formal_n_day_update_v98(dict(row))]
    max_rows = int(max(1, min(max_rows or 80, 300)))
    targets = set(candidates[:max_rows])
    rows = []
    done = ok_count = fail_count = 0
    max_seconds = 35
    started_ts = time.time()
    stopped_by_time_guard = False
    prog = st.progress(0, text="V98：準備正式 N 日績效回補...") if show_progress and targets else None
    status_box = st.empty() if show_progress and targets else None

    for i, row in work.iterrows():
        payload = dict(row)
        if i not in targets:
            rows.append(payload)
            continue
        if time.time() - started_ts > max_seconds:
            stopped_by_time_guard = True
            rows.append(payload)
            continue

        code = _normalize_code(payload.get("股票代號"))
        name = _safe_str(payload.get("股票名稱"))
        try:
            metrics = _calc_formal_n_day_metrics_v98(payload)
        except Exception as e:
            metrics = {"績效評語": f"V98 正式N日回補失敗：{str(e)[:80]}", "追蹤更新時間": _now_text()}
        if metrics:
            for k, v in metrics.items():
                payload[k] = v
            if any(_safe_float(metrics.get(c)) is not None for c in FORMAL_N_DAY_COLUMNS_V98):
                ok_count += 1
            else:
                fail_count += 1
        else:
            fail_count += 1
            payload["績效評語"] = "V98 正式N日回補：本次未取得足夠K線資料"
            payload["追蹤更新時間"] = _now_text()
        rows.append(payload)
        done += 1
        if prog is not None:
            prog.progress(min(1.0, done / max(len(targets), 1)), text=f"V98：正式N日績效回補 {done}/{len(targets)}｜成功 {ok_count}｜略過/不足 {fail_count}｜目前 {code} {name}")
        if status_box is not None and (done == len(targets) or done % 5 == 0):
            status_box.caption(f"本次上限 {max_rows} 筆；時間防呆 {max_seconds} 秒；總待回補 {len(candidates)} 筆。")

    out_df = _ensure_record_columns(pd.DataFrame(rows))
    summary = {
        "candidates": len(candidates),
        "batch_limit": max_rows,
        "processed": done,
        "success": ok_count,
        "fail": fail_count,
        "remaining": max(0, len(candidates) - done),
        "time_guard": bool(stopped_by_time_guard),
        "updated_at": _now_text(),
    }
    st.session_state[_k("v98_formal_n_day_update_summary")] = summary
    return out_df, summary
# <<< V98_FORMAL_N_DAY_PERF_BACKFILL

def _row_needs_backtest_update(payload: dict[str, Any]) -> bool:
    code = _normalize_code(payload.get("股票代號"))
    if not code:
        return False
    rec_date = pd.to_datetime(_safe_str(payload.get("推薦日期")), errors="coerce")
    if pd.isna(rec_date):
        return False
    age_days = (date.today() - rec_date.date()).days
    if age_days < 1:
        return False
    has_any = any(_safe_float(payload.get(c)) is not None for c in ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%"])
    last = pd.to_datetime(_safe_str(payload.get("追蹤更新時間")), errors="coerce")
    if has_any and not pd.isna(last):
        try:
            if (datetime.now() - last.to_pydatetime()).total_seconds() < 12 * 3600:
                return False
        except Exception:
            pass
    if not has_any:
        return True
    if age_days >= 20 and _safe_float(payload.get("推薦後20日%")) is None:
        return True
    if age_days >= 10 and _safe_float(payload.get("推薦後10日%")) is None:
        return True
    if age_days >= 5 and _safe_float(payload.get("推薦後5日%")) is None:
        return True
    if age_days >= 3 and _safe_float(payload.get("推薦後3日%")) is None:
        return True
    return False


def _update_backtest_metrics(df: pd.DataFrame, max_rows: int = 30, show_progress: bool = True) -> pd.DataFrame:
    """V71：多來源防卡更新推薦清單績效，避免一次全表連外造成一直跑。"""
    if df is None or df.empty:
        return _ensure_record_columns(pd.DataFrame())
    work = _ensure_record_columns(df.copy()).reset_index(drop=True)
    candidates = []
    for i, row in work.iterrows():
        if _row_needs_backtest_update(dict(row)):
            candidates.append(i)
    max_rows = int(max(1, min(max_rows or 30, 200)))
    targets = set(candidates[:max_rows])
    rows = []
    done = ok_count = fail_count = 0
    total = len(targets)
    prog = st.progress(0, text="V71：準備多來源防卡更新推薦清單績效...") if show_progress and total else None
    status_box = st.empty() if show_progress and total else None
    max_seconds = 28
    started_ts = time.time()
    stopped_by_time_guard = False
    time_guard_skip_count = 0

    for i, row in work.iterrows():
        payload = dict(row)
        if i not in targets:
            rows.append(payload)
            continue
        if time.time() - started_ts > max_seconds:
            stopped_by_time_guard = True
            time_guard_skip_count += 1
            rows.append(payload)
            continue

        code = _normalize_code(payload.get("股票代號"))
        name = _safe_str(payload.get("股票名稱"))
        try:
            metrics = _calc_backtest_metrics(payload)
        except Exception as e:
            metrics = {}
            payload["績效評語"] = f"績效更新失敗：{str(e)[:60]}"
        if metrics:
            ok_count += 1
            for k, v in metrics.items():
                payload[k] = v
            for d in [1, 3, 5, 10, 20]:
                old_key = f"{d}日績效%"
                new_key = f"推薦後{d}日%"
                if _safe_float(payload.get(old_key)) is None and _safe_float(payload.get(new_key)) is not None:
                    payload[old_key] = payload.get(new_key)
        else:
            fail_count += 1
            if not _safe_str(payload.get("績效評語")):
                payload["績效評語"] = "本次未取得足夠歷史資料，已略過，不阻塞整批更新"
            payload["追蹤更新時間"] = _now_text()
        rows.append(payload)
        done += 1
        if prog is not None:
            prog.progress(min(1.0, done / max(total, 1)), text=f"V71：更新推薦清單績效 {done}/{total}｜成功 {ok_count}｜略過/失敗 {fail_count}｜目前 {code} {name}")
        if status_box is not None and (done == total or done % 5 == 0):
            status_box.caption(f"本次分批上限 {max_rows} 筆；本批時間防呆 {max_seconds} 秒；剩餘待更新約 {max(0, len(candidates)-done)} 筆。")
    st.session_state[_k("v51_perf_update_summary")] = {
        "待更新總數": len(candidates),
        "本次更新上限": max_rows,
        "本次處理": done,
        "成功": ok_count,
        "略過或失敗": fail_count,
        "剩餘": max(0, len(candidates)-done),
        "時間防呆觸發": bool(stopped_by_time_guard),
        "時間防呆略過": int(time_guard_skip_count),
        "單批秒數上限": max_seconds,
        "更新時間": _now_text(),
    }
    return _ensure_record_columns(pd.DataFrame(rows))


def _to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        _ensure_record_columns(df).to_excel(writer, sheet_name="推薦清單", index=False)
    return output.getvalue()


# ============================================================
# V50：推薦後績效追蹤總控
# ============================================================
def _render_v50_performance_tracker(df: pd.DataFrame, title: str = "V98 推薦後績效追蹤總控") -> None:
    """V97：正式 N 日績效 + 即時損益雙軌統計。

    修正點：
    - 推薦清單常已經有「目前損益幅% / 損益幅%」，但尚未產生推薦後 1/3/5/10/20 日欄位。
    - 舊版只看 N 日欄位，所以畫面會出現「平均目前績效有數值，但有效績效樣本 = 0」。
    - 本版把「即時追蹤損益」列為暫行有效績效，不偽裝成正式 N 日績效。
    """
    if df is None or df.empty:
        st.info("V98：目前沒有資料可做推薦後績效追蹤。")
        return

    x = df.copy()
    x = x.loc[:, ~x.columns.duplicated()].copy()
    perf_base_cols = ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%"]
    current_perf_cols = ["目前損益幅%", "損益幅%", "即時追蹤報酬%", "目前追蹤報酬%", "實際報酬%", "平均目前績效%"]

    def _num_col(col: str) -> pd.Series:
        if col not in x.columns:
            return pd.Series([float('nan')] * len(x), index=x.index)
        return pd.to_numeric(x[col], errors="coerce")

    def _first_numeric_series(cols: list[str]) -> tuple[str, pd.Series]:
        best_name = ""
        best_s = pd.Series([float('nan')] * len(x), index=x.index)
        best_n = -1
        for col in cols:
            if col not in x.columns:
                continue
            s = _num_col(col)
            n = int(s.notna().sum())
            if n > best_n:
                best_name, best_s, best_n = col, s, n
        return best_name, best_s

    perf_cols = [c for c in perf_base_cols if c in x.columns]
    current_col, current_s = _first_numeric_series(current_perf_cols)
    current_mask = current_s.notna()

    any_period_mask = pd.Series(False, index=x.index)
    for c in perf_cols:
        any_period_mask = any_period_mask | _num_col(c).notna()
    effective_mask = any_period_mask | current_mask

    def _valid_mask_for(col: str) -> pd.Series:
        return _num_col(col).notna()

    def _avg_series(s: pd.Series):
        s = pd.to_numeric(s, errors="coerce").dropna()
        return None if s.empty else float(s.mean())

    def _wr_series(s: pd.Series):
        s = pd.to_numeric(s, errors="coerce").dropna()
        return None if s.empty else float((s > 0).mean() * 100)

    def _avg(col: str):
        return _avg_series(_num_col(col))

    def _wr(col: str):
        return _wr_series(_num_col(col))

    def _fmt_pct(v, digits=1):
        return "—" if v is None or pd.isna(v) else f"{v:.{digits}f}%"

    def _bool_rate(col: str) -> float | None:
        if col not in x.columns or len(x) == 0:
            return None
        base = x.loc[effective_mask, col] if effective_mask.any() else pd.Series([], dtype=object)
        if base.empty:
            return None
        def _b(v):
            if isinstance(v, bool):
                return v
            return str(v).strip().lower() in {"true", "1", "yes", "y", "是", "達標", "停損"}
        return float(base.map(_b).mean() * 100)

    with st.expander(title, expanded=True):
        kpi_payload = []

        # 先顯示正式 N 日欄位；若尚無到期樣本，不再誤導成 0%，而是明確標註。
        for col in perf_base_cols:
            if col in x.columns:
                s = _num_col(col)
                wr = _wr_series(s)
                avg = _avg_series(s)
                n = int(s.notna().sum())
                kpi_payload.append({
                    "label": f"{col.replace('%','')} 勝率",
                    "value": _fmt_pct(wr),
                    "delta": f"正式樣本 {n}｜平均 {_fmt_pct(avg, 2)}" if n > 0 else "正式樣本 0｜尚未到期或尚未回補",
                    "delta_class": "pro-kpi-delta-flat",
                })

        # 新增即時追蹤績效，解決「平均目前績效有數值，但有效樣本=0」的畫面矛盾。
        current_n = int(current_mask.sum())
        if current_n > 0:
            kpi_payload.insert(0, {
                "label": f"即時追蹤勝率｜{current_col}",
                "value": _fmt_pct(_wr_series(current_s)),
                "delta": f"暫行樣本 {current_n}｜平均 {_fmt_pct(_avg_series(current_s), 2)}",
                "delta_class": "pro-kpi-delta-flat",
            })

        if kpi_payload:
            if 'render_pro_kpi_row' in globals() and callable(globals().get('render_pro_kpi_row')):
                try:
                    render_pro_kpi_row(kpi_payload[:6])
                except Exception:
                    cols = st.columns(max(1, min(len(kpi_payload), 5)))
                    for c, item in zip(cols, kpi_payload):
                        c.metric(item["label"], item["value"], item["delta"])
            else:
                cols = st.columns(max(1, min(len(kpi_payload), 5)))
                for c, item in zip(cols, kpi_payload):
                    c.metric(item["label"], item["value"], item["delta"])
        else:
            st.info("尚無正式 N 日績效欄位，也沒有即時損益欄位。")

        valid_n = int(effective_mask.sum())
        period_valid_n = int(any_period_mask.sum())
        current_valid_n = int(current_mask.sum())
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("追蹤總筆數", int(len(x)))
        k2.metric("有效績效樣本", valid_n, f"正式 {period_valid_n}｜即時 {current_valid_n}")
        target_rate = _bool_rate('是否達標_回測')
        if target_rate is None:
            target_rate = _bool_rate('是否達目標1')
        stop_rate = _bool_rate('是否停損_回測')
        if stop_rate is None:
            stop_rate = _bool_rate('是否達停損')
        k3.metric("達標率", _fmt_pct(target_rate))
        dd = _avg('推薦後最大回撤%') if '推薦後最大回撤%' in x.columns else None
        k4.metric("平均最大回撤", _fmt_pct(dd, 2))

        if valid_n == 0:
            st.warning("目前沒有可用績效數值。請先按『更新推薦後績效』，或確認推薦清單已有最新價 / 損益幅%。")
        elif period_valid_n == 0 and current_valid_n > 0:
            st.info("V99：目前只有即時損益可用，尚無正式 1/3/5/10/20 日到期績效；請按上方或側邊「正式N日績效回補」產生正式績效。")
        else:
            st.caption("V98：正式 N 日績效優先；尚未到期時才使用即時損益作暫行統計，不把空白績效偽裝成 0%。")

        def _row_effective_perf(g: pd.DataFrame) -> pd.Series:
            # 每列優先取最長可用 N 日績效，再退回即時損益。
            s = pd.Series([float('nan')] * len(g), index=g.index)
            for col in ["推薦後20日%", "推薦後10日%", "推薦後5日%", "推薦後3日%", "推薦後1日%"]:
                if col in g.columns:
                    cs = pd.to_numeric(g[col], errors="coerce")
                    s = s.where(s.notna(), cs)
            for col in current_perf_cols:
                if col in g.columns:
                    cs = pd.to_numeric(g[col], errors="coerce")
                    s = s.where(s.notna(), cs)
            return s

        def _group_table(group_col: str) -> pd.DataFrame:
            if group_col not in x.columns:
                return pd.DataFrame()
            rows = []
            for key, g in x.groupby(group_col, dropna=False):
                row = {group_col: "未分類" if _is_blank_value(key) else key, "總筆數": len(g)}
                eff = _row_effective_perf(g)
                row["有效績效樣本"] = int(eff.notna().sum())
                row["暫行平均績效%"] = round(float(eff.dropna().mean()), 2) if eff.notna().any() else ""
                row["暫行勝率%"] = round(float((eff.dropna() > 0).mean() * 100), 1) if eff.notna().any() else ""
                for col in perf_base_cols:
                    if col in g.columns:
                        s = pd.to_numeric(g[col], errors="coerce").dropna()
                        row[f"平均{col}"] = round(float(s.mean()), 2) if not s.empty else ""
                        row[f"{col.replace('%','')}勝率"] = round(float((s > 0).mean() * 100), 1) if not s.empty else ""
                if current_col and current_col in g.columns:
                    s0 = pd.to_numeric(g[current_col], errors="coerce").dropna()
                    row[f"平均{current_col}"] = round(float(s0.mean()), 2) if not s0.empty else ""
                if "推薦後最大漲幅%" in g.columns:
                    s1 = pd.to_numeric(g["推薦後最大漲幅%"], errors="coerce").dropna()
                    row["平均最大漲幅%"] = round(float(s1.mean()), 2) if not s1.empty else ""
                if "推薦後最大回撤%" in g.columns:
                    s2 = pd.to_numeric(g["推薦後最大回撤%"], errors="coerce").dropna()
                    row["平均最大回撤%"] = round(float(s2.mean()), 2) if not s2.empty else ""
                rows.append(row)
            out = pd.DataFrame(rows)
            if "有效績效樣本" in out.columns:
                out = out.sort_values("有效績效樣本", ascending=False, na_position="last")
            return out

        common_keep = ["總筆數", "有效績效樣本", "暫行平均績效%", "暫行勝率%"]
        tabs_v50 = st.tabs(["依推薦模式", "依推薦等級", "依類別", "依大盤風控", "弱勢檢討清單"])
        with tabs_v50[0]:
            tbl = _group_table("推薦模式")
            try:
                _safe_dataframe(tbl, keep_cols=["推薦模式"] + common_keep, use_container_width=True, hide_index=True)
            except Exception:
                st.dataframe(tbl, use_container_width=True, hide_index=True)
        with tabs_v50[1]:
            tbl = _group_table("推薦等級")
            try:
                _safe_dataframe(tbl, keep_cols=["推薦等級"] + common_keep, use_container_width=True, hide_index=True)
            except Exception:
                st.dataframe(tbl, use_container_width=True, hide_index=True)
        with tabs_v50[2]:
            tbl = _group_table("類別")
            try:
                _safe_dataframe(tbl, keep_cols=["類別"] + common_keep, use_container_width=True, hide_index=True)
            except Exception:
                st.dataframe(tbl, use_container_width=True, hide_index=True)
        with tabs_v50[3]:
            mcol = "大盤橋接風控" if "大盤橋接風控" in x.columns else ("大盤橋接狀態" if "大盤橋接狀態" in x.columns else "大盤趨勢")
            if mcol in x.columns:
                tbl = _group_table(mcol)
                try:
                    _safe_dataframe(tbl, keep_cols=[mcol] + common_keep, use_container_width=True, hide_index=True)
                except Exception:
                    st.dataframe(tbl, use_container_width=True, hide_index=True)
            else:
                st.info("尚無大盤風控欄位可分群。")
        with tabs_v50[4]:
            weak = x.copy()
            weak["_暫行績效%"] = _row_effective_perf(weak)
            weak = weak.dropna(subset=["_暫行績效%"]).sort_values("_暫行績效%", ascending=True).head(30)
            if not weak.empty:
                candidate_cols = ["股票代號", "股票名稱", "類別", "推薦模式", "推薦等級", "推薦總分", "_暫行績效%", current_col, "推薦後5日%", "推薦後10日%", "推薦後最大回撤%", "命中結果", "績效評語", "推薦日期", "推薦理由摘要", "風險說明"]
                cols = [c for c in candidate_cols if c and c in weak.columns]
                try:
                    st.dataframe(_safe_display_df(weak[cols]), use_container_width=True, hide_index=True)
                except Exception:
                    st.dataframe(weak[cols], use_container_width=True, hide_index=True)
            else:
                st.info("尚無正式或即時績效可列弱勢檢討清單。")

def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")

    # v40：啟用欄位管理極速模式；不再全頁攔截所有表格
    try:
        from godpick_column_manager import install_auto_column_manager
        install_auto_column_manager("10_推薦清單")
    except Exception:
        pass
    inject_pro_theme()

    render_pro_hero(
        title="推薦清單",
        subtitle="集中查看股神推薦紀錄，支援日期篩選、批次刪除、匯出備份與 GitHub/Firestore 同步。",
        chips=["日期篩選", "批次刪除", "推薦分數", "推薦後績效", "GitHub 同步"],
    )

    st.caption(f"推薦清單 V136 統一有效欄位版：{PERF_TRACKING_VERSION}｜{NIGHT_BATTLE_LIST_VERSION}")
    if callable(load_module_sync_state):
        try:
            _sync_state, _sync_detail = load_module_sync_state()
            _module10 = (_sync_state.get("modules") or {}).get("10推薦清單", {}) if isinstance(_sync_state, dict) else {}
            if _sync_state:
                if _module10.get("ok"):
                    st.success(f"08 一鍵同步最近狀態｜{_sync_state.get('finished_at') or _sync_state.get('updated_at') or '—'}｜10推薦清單已永久同步 {_module10.get('count', 0)} 筆")
                else:
                    st.warning(f"08 一鍵同步最近狀態｜10推薦清單未完成；請到 08 的同步檢查查看明細。")
        except Exception as _sync_e:
            st.caption(f"08 一鍵同步狀態暫無法載入：{_sync_e}")

    if _k("last_sync_msgs") not in st.session_state:
        st.session_state[_k("last_sync_msgs")] = []

    with st.sidebar:
        st.subheader("操作區")
        reload_btn = st.button("重新讀取推薦清單", use_container_width=True, type="primary")
        if reload_btn:
            _load_records_cached(force=True)
        df = _load_records_cached(force=False)
        batch_n = st.number_input("每次更新筆數", min_value=5, max_value=200, value=80, step=5, key=_k("perf_update_batch_size"))
        batch_stock_n = st.number_input("單批股票上限", min_value=5, max_value=60, value=30, step=5, key=_k("perf_update_stock_limit_v77"))
        if st.button("🧮 更新推薦後績效", use_container_width=True):
            with st.spinner("V77：快速防卡更新推薦後績效中，只更新缺資料 / 過期資料..."):
                summary = update_recommendation_perf_fast_v77(
                    json_files=["godpick_recommend_list.json", "godpick_records.json", "godpick_latest_recommendations.json"],
                    max_records=int(batch_n),
                    batch_limit=int(batch_stock_n),
                    max_workers=12,
                    stale_minutes=60,
                )
                try:
                    _load_records_cached(force=True)
                    df = _load_records_cached(force=False)
                    st.session_state[_k("records_df")] = df
                except Exception as _v77_reload_e:
                    st.warning(f"V77 已更新 JSON，但重新載入畫面資料失敗：{_v77_reload_e}")

            st.success(
                f"V77 已完成快速績效更新：候選 {summary.get('candidates', 0)} 筆，"
                f"成功 {summary.get('success', 0)} 筆，失敗 {summary.get('fail', 0)} 筆；"
                f"更新檔案：{', '.join(summary.get('updated_files', [])) or '無'}。"
            )
            if summary.get("messages"):
                st.info("；".join(summary.get("messages", [])))
            if summary.get("fail", 0):
                st.warning("部分股票線上抓取失敗，V77 已略過並保留原資料，不會拖住整批。")
            st.rerun()

        if st.button("📅 正式N日績效回補", use_container_width=True):
            with st.spinner("V98：正式 N 日績效回補中，只補已到期且缺欄位的資料..."):
                formal_df, formal_summary = _update_formal_n_day_metrics_v98(df, max_rows=int(batch_n), show_progress=True)
                ok, msgs = _sync_records(formal_df)
                st.session_state[_k("records_df")] = formal_df
                _load_records_cached(force=True)
            if ok:
                st.success(
                    f"V98 正式N日績效回補完成：待回補 {formal_summary.get('candidates', 0)} 筆，"
                    f"本次處理 {formal_summary.get('processed', 0)} 筆，成功 {formal_summary.get('success', 0)} 筆，"
                    f"不足/略過 {formal_summary.get('fail', 0)} 筆，剩餘 {formal_summary.get('remaining', 0)} 筆。"
                )
            else:
                st.warning(
                    f"V98 已在畫面資料中完成回補，但 GitHub/Firestore 寫回未成功；"
                    f"本次處理 {formal_summary.get('processed', 0)} 筆。"
                )
            if msgs:
                st.info("；".join(msgs))
            if formal_summary.get("time_guard"):
                st.warning("本次觸發時間防呆，請再按一次『正式N日績效回補』繼續補剩餘資料。")
            st.rerun()

        if st.button("🎯 更新隔日命中追蹤", use_container_width=True):
            with st.spinner("V101：更新進場點 / 突破價 / 停損價 / 壓力價命中追蹤中..."):
                hit_df, hit_summary = _update_night_hit_tracking_v101(
                    df,
                    max_rows=int(batch_stock_n),
                    show_progress=True,
                )
                ok, msgs = _sync_records(hit_df)
                st.session_state[_k("records_df")] = hit_df
                _load_records_cached(force=True)
            if ok:
                st.success(
                    f"V101 隔日命中追蹤完成：處理 {hit_summary.get('processed', 0)} 筆，"
                    f"成功 {hit_summary.get('success', 0)} 筆，失敗/不足 {hit_summary.get('fail', 0)} 筆。"
                )
            else:
                st.warning("V101 已完成畫面資料更新，但寫回 GitHub/Firestore 未完全成功。")
            all_msgs = (hit_summary.get("messages") or []) + (msgs or [])
            if all_msgs:
                st.info("；".join([str(m) for m in all_msgs[:8]]))
            st.rerun()
        load_msg = _safe_str(st.session_state.get(_k("load_msg"), ""))
        if load_msg:
            st.caption(load_msg)
        st.caption(f"最近載入時間：{_safe_str(st.session_state.get(_k('loaded_at'), ''))}")

    df = _load_records_cached(force=False)

    if df.empty:
        render_pro_section("推薦清單資料")
        st.warning("目前沒有推薦資料。已嘗試讀取 godpick_recommend_list.json、godpick_latest_recommendations.json、godpick_records.json；若仍空白，請先到 7_股神推薦重新推薦並寫入推薦清單。")
        return

    mode_options = ["全部"] + sorted([x for x in df["推薦模式"].dropna().astype(str).unique().tolist() if x])
    status_options = ["全部"] + sorted([x for x in df["目前狀態"].dropna().astype(str).unique().tolist() if x])
    rec_dates = pd.to_datetime(df["推薦日期"], errors="coerce").dropna()
    min_d = rec_dates.min().date() if not rec_dates.empty else (date.today() - timedelta(days=30))
    max_d = rec_dates.max().date() if not rec_dates.empty else date.today()

    c1, c2, c3, c4, c5 = st.columns([1.1, 1.1, 1.1, 1.1, 1.4])
    with c1:
        start_date = st.date_input("開始日期", value=min_d, key=_k("start_date"))
    with c2:
        end_date = st.date_input("結束日期", value=max_d, key=_k("end_date"))
    with c3:
        mode = st.selectbox("推薦模式", mode_options, key=_k("mode_filter"))
    with c4:
        status = st.selectbox("目前狀態", status_options, key=_k("status_filter"))
    with c5:
        kw = st.text_input("搜尋代號 / 名稱 / 類別 / 理由", key=_k("kw"))

    filtered_df = _filter_df(df, start_date, end_date, mode, status, kw)

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("目前筆數", len(filtered_df))
    with k2:
        st.metric("平均推薦總分", format_number(filtered_df["推薦總分"].mean(), 1) if not filtered_df.empty else "0")
    with k3:
        st.metric("股神級 / 強烈關注", int(filtered_df["推薦等級"].isin(["股神級", "強烈關注"]).sum()) if not filtered_df.empty else 0)
    with k4:
        st.metric("達停損筆數", int(filtered_df["是否達停損"].fillna(False).sum()) if not filtered_df.empty else 0)
    with k5:
        avg20 = pd.to_numeric(filtered_df.get("推薦後20日%"), errors="coerce").dropna().mean() if not filtered_df.empty and "推薦後20日%" in filtered_df.columns else None
        if avg20 is None or pd.isna(avg20):
            fb_col = next((c for c in ["目前損益幅%", "損益幅%", "即時追蹤報酬%", "目前追蹤報酬%"] if c in filtered_df.columns and pd.to_numeric(filtered_df[c], errors="coerce").notna().sum() > 0), None)
            avg_fb = pd.to_numeric(filtered_df.get(fb_col), errors="coerce").dropna().mean() if fb_col else None
            st.metric("平均目前績效%", format_number(avg_fb, 2) if avg_fb is not None and pd.notna(avg_fb) else "—")
        else:
            st.metric("平均推薦後20日%", format_number(avg20, 2))

    _render_night_battle_tracker(filtered_df)

    _render_night_hit_tracker_v101(filtered_df)

    _render_practical_quality_tracker_v119(filtered_df)

    _render_official_factor_tracker_v110(filtered_df)

    _render_v50_performance_tracker(filtered_df, "V50 推薦後績效追蹤總控｜10_推薦清單")

    # >>> V99_MAIN_FORMAL_BACKFILL_BUTTON
    # v99 修正：v98 只把正式 N 日回補放在 sidebar，使用者容易看不到，
    # 導致畫面一直維持「正式樣本 0」。此區直接放在績效總控下方，
    # 且只處理既有推薦清單，不重跑 7_股神推薦、不重新推薦。
    try:
        formal_candidates_v99 = sum(1 for _, _r in df.iterrows() if _row_needs_formal_n_day_update_v98(dict(_r)))
    except Exception:
        formal_candidates_v99 = 0
    if formal_candidates_v99 > 0:
        st.warning(
            f"偵測到 {formal_candidates_v99} 筆推薦紀錄尚未產生正式 N 日績效。"
            "請按下方按鈕回補，回補後 1/3/5/10/20 日勝率才會從暫行績效改成正式績效。"
        )
    else:
        st.info("目前沒有待回補的正式 N 日績效候選；若仍顯示正式樣本 0，代表篩選區間尚未到期或歷史K線來源不足。")

    b1, b2, b3 = st.columns([1.2, 1.2, 2])
    with b1:
        run_formal_backfill_main_v99 = st.button(
            "📅 正式N日績效回補",
            key=_k("formal_backfill_main_v99"),
            use_container_width=True,
            type="primary",
            disabled=(formal_candidates_v99 <= 0),
        )
    with b2:
        run_formal_backfill_force_v99 = st.button(
            "🔁 強制重新檢查正式績效",
            key=_k("formal_backfill_force_main_v99"),
            use_container_width=True,
        )
    with b3:
        st.caption("v99：此按鈕只回補 10_推薦清單既有資料並寫回 GitHub/Firestore，不會重跑股神推薦。")

    if run_formal_backfill_main_v99 or run_formal_backfill_force_v99:
        with st.spinner("V99：正式 N 日績效回補中，只補已到期且缺欄位的資料..."):
            formal_df, formal_summary = _update_formal_n_day_metrics_v98(
                df,
                max_rows=int(st.session_state.get(_k("perf_update_batch_size"), 80)),
                show_progress=True,
            )
            ok, msgs = _sync_records(formal_df)
            st.session_state[_k("records_df")] = formal_df
            _load_records_cached(force=True)
        if ok:
            st.success(
                f"V99 正式N日績效回補完成：待回補 {formal_summary.get('candidates', 0)} 筆，"
                f"本次處理 {formal_summary.get('processed', 0)} 筆，成功 {formal_summary.get('success', 0)} 筆，"
                f"不足/略過 {formal_summary.get('fail', 0)} 筆，剩餘 {formal_summary.get('remaining', 0)} 筆。"
            )
        else:
            st.warning(
                f"V99 已在畫面資料中完成回補，但 GitHub/Firestore 寫回未成功；"
                f"本次處理 {formal_summary.get('processed', 0)} 筆。"
            )
        if msgs:
            st.info("；".join(msgs))
        if formal_summary.get("time_guard"):
            st.warning("本次觸發時間防呆，請再按一次『正式N日績效回補』繼續補剩餘資料。")
        st.rerun()
    # <<< V99_MAIN_FORMAL_BACKFILL_BUTTON

    render_pro_section("推薦清單明細")
    # v15 欄位統一：推薦清單明細使用與 7/8/12 一致的欄位順序。
    show_cols = [c for c in (UNIFIED_RECOMMEND_DISPLAY_COLUMNS or list(filtered_df.columns)) if c in filtered_df.columns]
    if callable(filter_effective_columns):
        show_cols = filter_effective_columns(show_cols)
    if not show_cols:
        show_cols = list(filtered_df.columns)
    existing_cols = []
    for c in show_cols:
        if c in filtered_df.columns and c not in existing_cols:
            existing_cols.append(c)
    filtered_show_df = filtered_df.loc[:, ~filtered_df.columns.duplicated()].copy()
    # v46：推薦清單明細正式加入欄位管理。
    # 重點：只管理這張主表，不再全頁攔截所有 dataframe，避免每個模組變慢。
    detail_show_df = _format_show_df(filtered_show_df[existing_cols])
    try:
        from godpick_column_manager import managed_dataframe
        managed_dataframe(
            detail_show_df,
            table_key="page10_recommend_list_detail",
            table_label="推薦清單明細",
            default_cols=list(detail_show_df.columns),
            use_container_width=True,
            height=620,
        )
    except Exception as exc:
        st.warning(f"欄位管理載入失敗，已改用一般表格顯示：{exc}")
        st.dataframe(detail_show_df, use_container_width=True, height=620)

    ex1, ex2 = st.columns(2)
    with ex1:
        st.download_button(
            label="下載目前篩選結果 Excel",
            data=_to_excel_bytes(filtered_df.loc[:, ~filtered_df.columns.duplicated()].copy()),
            file_name=f"推薦清單_{_now_text().replace(':','-').replace(' ','_')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with ex2:
        st.download_button(
            label="下載目前篩選結果 CSV",
            data=filtered_df.loc[:, ~filtered_df.columns.duplicated()].copy().to_csv(index=False, encoding="utf-8-sig"),
            file_name=f"推薦清單_{_now_text().replace(':','-').replace(' ','_')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    render_pro_section("批次刪除")
    st.caption("這裡會依你上面目前的篩選條件，一次刪除符合條件的紀錄，不需要一筆一筆點。")
    d1, d2 = st.columns([1.4, 1])
    with d1:
        st.info(f"目前將刪除 {len(filtered_df)} 筆：日期 {start_date} ~ {end_date}，模式 {mode}，狀態 {status}，關鍵字 {_safe_str(kw) or '無'}")
    with d2:
        confirm_delete = st.checkbox("我確認要刪除目前篩選結果", key=_k("confirm_delete"))

    if st.button("批次刪除目前篩選結果", use_container_width=True, type="primary"):
        if filtered_df.empty:
            st.warning("目前沒有符合篩選條件的資料可刪除。")
        elif not confirm_delete:
            st.error("請先勾選確認刪除。")
        else:
            remain_df = df[~df["record_id"].astype(str).isin(filtered_df["record_id"].astype(str))].copy()
            ok, msgs = _sync_records(remain_df)
            if ok:
                st.session_state[_k("records_df")] = _ensure_record_columns(remain_df)
                st.session_state[_k("confirm_delete")] = False
                st.success(f"已刪除 {len(filtered_df)} 筆推薦紀錄。")
            else:
                st.error("批次刪除失敗。")
            with st.expander("同步明細", expanded=False):
                for m in msgs:
                    st.write(f"- {m}")

    if st.session_state.get(_k("last_sync_msgs")):
        with st.expander("最近一次同步明細", expanded=False):
            for m in st.session_state[_k("last_sync_msgs")]:
                st.write(f"- {m}")


# =========================================================
# v71：隔夜國際盤欄位相容補強
# 由 07 股神推薦 v69/v71 寫入，8/10 只負責保存與顯示，避免舊資料缺欄位 KeyError。
# =========================================================
OVERNIGHT_V71_COLUMNS = [
    "隔夜風控分數", "隔夜風險等級", "隔夜偏向", "隔夜解讀", "隔夜資料品質", "台指夜盤資料來源", "台指夜盤備援說明",
    "台指夜盤漲跌", "NASDAQ漲跌%", "S&P500漲跌%", "道瓊漲跌%", "費半漲跌%",
    "Nasdaq期貨偏向", "S&P期貨偏向", "匯率風險等級",
]
try:
    for _c in OVERNIGHT_V71_COLUMNS:
        if _c not in GODPICK_RECORD_COLUMNS:
            GODPICK_RECORD_COLUMNS.append(_c)
        if "DEFAULT_STANDARD_COLS" in globals() and _c not in DEFAULT_STANDARD_COLS:
            DEFAULT_STANDARD_COLS.append(_c)
        if "DEFAULT_ADVANCED_COLS" in globals() and _c not in DEFAULT_ADVANCED_COLS:
            DEFAULT_ADVANCED_COLS.append(_c)
except Exception:
    pass


if __name__ == "__main__":
    main()

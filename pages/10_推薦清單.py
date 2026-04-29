# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any
import base64
import io
import json
import time

import pandas as pd
import requests
import streamlit as st

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
PFX = "godpick_list_"
GOD_DECISION_V10_LINK_VERSION = "recommend_list_v10_entry_decision_v1_20260428"
BACKTEST_V12_VERSION = "recommend_list_v53_perf_guard_20260429"
DUPLICATE_COLUMN_FIX_VERSION = "recommend_list_duplicate_column_fix_v1_20260427"
V5_BACKFILL_FIX_VERSION = "recommend_list_v5_backfill_fix_v1_20260427"
READ_FALLBACK_VERSION = "recommend_list_multi_source_read_v1_20260427"
MARKET_TREND_V38_LINK_VERSION = "recommend_list_market_trend_v38_full_fields_20260429"

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

def _k(key: str) -> str:
    return f"{PFX}{key}"


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
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
    for idx, row in x.iterrows():
        need = any(_safe_str(row.get(c)) in ["", "None", "nan", "NaN"] for c in ["股神決策模式", "股神進場建議", "推薦分層"])
        if not need:
            continue
        fill = _derive_v5_from_legacy_row(row)
        for c, v in fill.items():
            if c in x.columns and _safe_str(x.at[idx, c]) in ["", "None", "nan", "NaN"]:
                x.at[idx, c] = v
    for c in x.columns:
        if x[c].dtype == object:
            x[c] = x[c].replace(["None", "nan", "NaN"], "")
    return x


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


    if "起漲等級" in x.columns:
        x["起漲等級"] = x["起漲等級"].fillna("").astype(str)
        mask = x["起漲等級"].str.strip() == ""
        if mask.any():
            x.loc[mask, "起漲等級"] = x.loc[mask].apply(_derive_list_prelaunch_grade, axis=1)

    if "買點分級" in x.columns:
        x["買點分級"] = x["買點分級"].fillna("").astype(str)
        mask = x["買點分級"].str.strip() == ""
        if mask.any():
            x.loc[mask, "買點分級"] = x.loc[mask].apply(_derive_list_buy_grade, axis=1)

    if "風險說明" in x.columns:
        x["風險說明"] = x["風險說明"].fillna("").astype(str)
        mask = x["風險說明"].str.strip() == ""
        if mask.any():
            x.loc[mask, "風險說明"] = x.loc[mask].apply(_derive_list_risk, axis=1)

    if "股神推論邏輯" in x.columns:
        x["股神推論邏輯"] = x["股神推論邏輯"].fillna("").astype(str)
        mask = x["股神推論邏輯"].str.strip() == ""
        if mask.any():
            x.loc[mask, "股神推論邏輯"] = x.loc[mask].apply(_derive_list_logic, axis=1)
    num_cols = [
        "推薦總分", "大盤橋接分數", "大盤可參考分數", "大盤加權分", "大盤影響加減分", "族群資金流分數", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", 
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
    github_ok, github_msg = _write_records_to_github(df)
    fs_ok, fs_msg = _write_records_to_firestore(_ensure_record_columns(df).to_dict(orient="records"))
    st.session_state[_k("last_sync_msgs")] = [
        f"GitHub: {'成功' if github_ok else '失敗'} | {github_msg}",
        f"Firestore: {'成功' if fs_ok else '失敗'} | {fs_msg}",
    ]
    return (github_ok or fs_ok), st.session_state[_k("last_sync_msgs")]


def _load_records_cached(force: bool = False) -> pd.DataFrame:
    if force or _k("records_df") not in st.session_state:
        df, msg = _read_records_from_github()
        latest_df, latest_msg = _read_recommend_list_from_latest()

        frames = []
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = df.copy()
            df["資料來源"] = "股神推薦紀錄"
            frames.append(df)
        if isinstance(latest_df, pd.DataFrame) and not latest_df.empty:
            latest_df = latest_df.copy()
            latest_df["資料來源"] = "本輪推薦清單"
            frames.append(latest_df)

        if frames:
            merged = pd.concat(frames, ignore_index=True)
            if "record_id" in merged.columns:
                merged = merged.drop_duplicates(subset=["record_id"], keep="last")
            else:
                merged = merged.drop_duplicates(subset=["股票代號", "推薦日期", "推薦時間", "推薦模式"], keep="last")
        else:
            merged = pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)

        st.session_state[_k("records_df")] = _ensure_record_columns(merged).copy()
        st.session_state[_k("load_msg")] = f"{msg}｜{latest_msg}"
        st.session_state[_k("loaded_at")] = _now_text()
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


def _format_show_df(df: pd.DataFrame) -> pd.DataFrame:
    show = df.copy()
    show = show.loc[:, ~show.columns.duplicated()].copy()
    show = _backfill_v10_columns(show)
    show = show.drop(columns=[c for c in ["record_id"] if c in show.columns])
    num1_cols = ["推薦總分", "族群資金流分數", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明",  "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", "同類股領先幅度", "實際報酬%", "損益幅%", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%"]
    price_cols = ["推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2", "實際買進價", "實際賣出價", "最新價", "損益金額"]
    for c in num1_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: format_number(x, 1) if pd.notna(x) else "")
    for c in price_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: format_number(x, 2) if pd.notna(x) else "")
    show = show.replace(["None", "nan", "NaN"], "")
    return show




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
    return pd.DataFrame()


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
        return {}
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
        return {}
    temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
    for c in ["收盤價", "最高價", "最低價"]:
        if c in temp.columns:
            temp[c] = pd.to_numeric(temp[c], errors="coerce")
    temp = temp.dropna(subset=["日期", "收盤價"]).sort_values("日期").reset_index(drop=True)
    window = temp[temp["日期"].dt.date >= rec_date.date()].reset_index(drop=True)
    if window.empty:
        return {}
    base_px = _safe_float(window.iloc[0].get("收盤價"))
    if base_px in [None, 0]:
        return {}
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
    """V51：分批更新推薦清單績效，避免一次全表連外造成一直跑。"""
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
    prog = st.progress(0, text="V53：準備更新推薦清單績效...") if show_progress and total else None
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
                if k in GODPICK_RECORD_COLUMNS:
                    payload[k] = v
        else:
            fail_count += 1
            if not _safe_str(payload.get("績效評語")):
                payload["績效評語"] = "本次未取得足夠歷史資料，已略過，不阻塞整批更新"
            payload["追蹤更新時間"] = _now_text()
        rows.append(payload)
        done += 1
        if prog is not None:
            prog.progress(min(1.0, done / max(total, 1)), text=f"V53：更新推薦清單績效 {done}/{total}｜成功 {ok_count}｜略過/失敗 {fail_count}｜目前 {code} {name}")
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
def _render_v50_performance_tracker(df: pd.DataFrame, title: str = "V50 推薦後績效追蹤總控") -> None:
    """顯示推薦後 1/3/5/10/20 日績效、達標/停損、最大漲幅/回撤與分群勝率。"""
    if df is None or df.empty:
        st.info("V50：目前沒有資料可做推薦後績效追蹤。")
        return

    x = df.copy()
    x = x.loc[:, ~x.columns.duplicated()].copy()

    def _num_col(col: str) -> pd.Series:
        if col not in x.columns:
            return pd.Series([float('nan')] * len(x))
        return pd.to_numeric(x[col], errors="coerce")

    def _bool_rate(col: str) -> float:
        if col not in x.columns or len(x) == 0:
            return 0.0
        s = x[col]
        def _b(v):
            if isinstance(v, bool):
                return v
            return str(v).strip().lower() in {"true", "1", "yes", "y", "是", "達標", "停損"}
        return float(s.map(_b).mean() * 100)

    def _avg(col: str) -> float:
        s = _num_col(col).dropna()
        return float(s.mean()) if not s.empty else 0.0

    def _wr(col: str) -> float:
        s = _num_col(col).dropna()
        return float((s > 0).mean() * 100) if not s.empty else 0.0

    perf_cols = [c for c in ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%"] if c in x.columns]
    if not perf_cols:
        st.info("V50：目前尚未產生推薦後績效欄位，請先按『更新推薦後績效』。")
        return

    with st.expander(title, expanded=True):
        kpi_payload = []
        for col in ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%"]:
            if col in x.columns:
                kpi_payload.append({
                    "label": f"{col.replace('%','')} 勝率",
                    "value": f"{_wr(col):.1f}%",
                    "delta": f"平均 {_avg(col):.2f}%",
                    "delta_class": "pro-kpi-delta-flat",
                })
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

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("追蹤樣本數", int(len(x)))
        k2.metric("達標率", f"{_bool_rate('是否達標_回測') or _bool_rate('是否達目標1'):.1f}%")
        k3.metric("停損率", f"{_bool_rate('是否停損_回測') or _bool_rate('是否達停損'):.1f}%")
        k4.metric("平均最大回撤", f"{_avg('推薦後最大回撤%'):.2f}%")

        st.caption("此區只做績效追蹤與驗證，不改變推薦結果。若欄位空白，請先更新推薦後績效。")

        def _group_table(group_col: str) -> pd.DataFrame:
            if group_col not in x.columns:
                return pd.DataFrame()
            rows = []
            for key, g in x.groupby(group_col, dropna=False):
                row = {group_col: key if str(key).strip() else "未分類", "筆數": len(g)}
                for col in ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%"]:
                    if col in g.columns:
                        s = pd.to_numeric(g[col], errors="coerce").dropna()
                        row[f"平均{col}"] = round(float(s.mean()), 2) if not s.empty else None
                        row[f"{col.replace('%','')}勝率"] = round(float((s > 0).mean() * 100), 1) if not s.empty else None
                if "推薦後最大漲幅%" in g.columns:
                    s1 = pd.to_numeric(g["推薦後最大漲幅%"], errors="coerce").dropna()
                    row["平均最大漲幅%"] = round(float(s1.mean()), 2) if not s1.empty else None
                if "推薦後最大回撤%" in g.columns:
                    s2 = pd.to_numeric(g["推薦後最大回撤%"], errors="coerce").dropna()
                    row["平均最大回撤%"] = round(float(s2.mean()), 2) if not s2.empty else None
                rows.append(row)
            out = pd.DataFrame(rows)
            sort_col = "平均推薦後20日%" if "平均推薦後20日%" in out.columns else ("平均推薦後10日%" if "平均推薦後10日%" in out.columns else None)
            if sort_col:
                out = out.sort_values(sort_col, ascending=False, na_position="last")
            return out

        tabs_v50 = st.tabs(["依推薦模式", "依推薦等級", "依類別", "依大盤風控", "弱勢檢討清單"])
        with tabs_v50[0]:
            st.dataframe(_group_table("推薦模式"), use_container_width=True, hide_index=True)
        with tabs_v50[1]:
            st.dataframe(_group_table("推薦等級"), use_container_width=True, hide_index=True)
        with tabs_v50[2]:
            st.dataframe(_group_table("類別"), use_container_width=True, hide_index=True)
        with tabs_v50[3]:
            mcol = "大盤橋接風控" if "大盤橋接風控" in x.columns else ("大盤橋接狀態" if "大盤橋接狀態" in x.columns else "大盤趨勢")
            if mcol in x.columns:
                st.dataframe(_group_table(mcol), use_container_width=True, hide_index=True)
            else:
                st.info("尚無大盤風控欄位可分群。")
        with tabs_v50[4]:
            weak_col = "推薦後10日%" if "推薦後10日%" in x.columns else ("推薦後5日%" if "推薦後5日%" in x.columns else None)
            if weak_col:
                weak = x.copy()
                weak[weak_col] = pd.to_numeric(weak[weak_col], errors="coerce")
                weak = weak.sort_values(weak_col, ascending=True).head(30)
                cols = [c for c in ["股票代號", "股票名稱", "類別", "推薦模式", "推薦等級", "推薦總分", weak_col, "推薦後最大回撤%", "命中結果", "績效評語", "推薦日期", "推薦理由摘要", "風險說明"] if c in weak.columns]
                st.dataframe(weak[cols], use_container_width=True, hide_index=True)
            else:
                st.info("尚無 5日/10日績效欄位可列弱勢檢討清單。")


def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    render_pro_hero(
        title="推薦清單",
        subtitle="集中查看股神推薦紀錄，支援日期篩選、批次刪除、匯出備份與 GitHub/Firestore 同步。",
        chips=["日期篩選", "批次刪除", "推薦分數", "推薦後績效", "GitHub 同步"],
    )

    st.caption(f"推薦清單V12回測校正版：{BACKTEST_V12_VERSION}")

    if _k("last_sync_msgs") not in st.session_state:
        st.session_state[_k("last_sync_msgs")] = []

    with st.sidebar:
        st.subheader("操作區")
        reload_btn = st.button("重新讀取推薦清單", use_container_width=True, type="primary")
        if reload_btn:
            _load_records_cached(force=True)
        df = _load_records_cached(force=False)
        batch_n = st.number_input("每次更新筆數", min_value=5, max_value=200, value=30, step=5, key=_k("perf_update_batch_size"))
        if st.button("更新推薦後績效", use_container_width=True):
            with st.spinner("V51：分批更新推薦後績效中，避免一次全表卡住..."):
                updated_df = _update_backtest_metrics(df, max_rows=int(batch_n), show_progress=True)
                st.session_state[_k("records_df")] = updated_df
                ok, msgs = _sync_records(updated_df)
            summary = st.session_state.get(_k("v51_perf_update_summary"), {})
            if ok:
                st.success(f"V51 已完成本批績效更新：處理 {summary.get('本次處理', 0)} 筆，成功 {summary.get('成功', 0)} 筆，剩餘約 {summary.get('剩餘', 0)} 筆。")
            else:
                st.warning("已在本頁分批更新，但遠端同步可能失敗，請查看同步明細。")
            if summary.get("剩餘", 0):
                if summary.get("時間防呆觸發"):
                    st.warning(f"V53 時間防呆已啟動：本批超過 {summary.get('單批秒數上限', 28)} 秒，自動保留剩餘資料，請再按一次更新下一批。")
                st.info("仍有舊紀錄待補績效，可再次按更新；每次分批處理可避免頁面一直跑。")
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
        avg20 = pd.to_numeric(filtered_df.get("推薦後20日%"), errors="coerce").dropna().mean() if not filtered_df.empty and "推薦後20日%" in filtered_df.columns else 0
        st.metric("平均推薦後20日%", format_number(avg20, 2) if pd.notna(avg20) else "0")

    _render_v50_performance_tracker(filtered_df, "V50 推薦後績效追蹤總控｜10_推薦清單")

    render_pro_section("推薦清單明細")
    show_cols = [
        "資料來源", "推薦日期", "推薦時間", "股票代號", "股票名稱", "推薦模式", "推薦型態", "機會型態", "推薦等級", "推薦總分", "大盤橋接分數", "大盤橋接狀態", "大盤橋接風控", "大盤交易時段", "大盤資料品質", "大盤影響加減分", "大盤影響說明", "大盤資料診斷摘要", "機會股分數", "低檔位置分數", "拉回承接分數", "支撐回測分數", "止跌轉強分數", "股神決策模式", "股神進場建議", "進場時機", "進場時機分數", "建議動作", "等待條件", "操作區間", "追高風險等級", "是否建議追價", "推薦分層", "建議部位%", "建議倉位%", "建議投入等級", "分批策略", "最大風險%", "單檔風險等級", "族群集中警示", "組合配置建議", "大盤策略模式", "大盤多空分數", "推薦積極度係數", "適合推薦型態", "大盤策略建議", "大盤風控建議", "市場策略調整說明", "動態建議倉位%", "風險報酬比", "追價風險分", "飆股起漲分數", "起漲等級", "起漲摘要",
        "買點分級", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數", "強勢族群等級", "族群資金流分數", "族群輪動狀態", "同族群強勢比例", "同族群推薦密度", "同族群平均量能分", "族群策略建議", "族群資金流說明", 
        "推薦價格", "K線驗證標記", "推薦日價格", "推薦日支撐壓力摘要", "K線查詢參數", "K線檢視提示", "近端支撐", "近端壓力", "突破確認價", "停損參考", "停損價", "賣出目標1", "賣出目標2", "最新價", "目前狀態", "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%", "推薦後最大漲幅%", "推薦後最大回撤%", "命中結果", "績效評語", "追蹤更新時間",
        "機會股說明", "股神推論邏輯", "風險說明", "推薦理由摘要", "備註"
    ]
    existing_cols = []
    for c in show_cols:
        if c in filtered_df.columns and c not in existing_cols:
            existing_cols.append(c)
    filtered_show_df = filtered_df.loc[:, ~filtered_df.columns.duplicated()].copy()
    st.dataframe(_format_show_df(filtered_show_df[existing_cols]), use_container_width=True, height=620)

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


if __name__ == "__main__":
    main()

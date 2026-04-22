# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
import base64
import hashlib
import json
import re
import xml.etree.ElementTree as ET

import pandas as pd
import requests
import streamlit as st

from utils import (
    inject_pro_theme,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
)

PAGE_TITLE = "大盤走勢"
PFX = "macro_trend_"
UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    )
}
TIMEOUT = 10
RECORD_COLUMNS = [
    "record_id", "預測日期", "建立時間", "更新時間",
    "推估分數", "推估方向", "推估漲跌點", "推估漲跌%", "推估區間低", "推估區間高",
    "操作建議", "風險等級",
    "台股加權", "台股漲跌%", "NASDAQ漲跌%", "SOX漲跌%", "NQ漲跌%", "ES漲跌%", "TSM ADR漲跌%", "USD/TWD漲跌%", "VIX漲跌%",
    "新聞分數", "新聞風險", "因子摘要",
    "實際收盤", "實際漲跌點", "實際漲跌%", "實際方向",
    "方向命中", "點數誤差", "區間命中",
    "開盤是否適合進場", "收盤檢討", "備註",
]

NEGATIVE_KEYWORDS = {
    "war": -6, "attack": -6, "missile": -7, "tariff": -5, "sanction": -5,
    "earthquake": -4, "crash": -7, "recession": -6, "inflation": -4,
    "hawkish": -4, "layoff": -3, "ban": -4, "investigation": -3,
    "downgrade": -3, "strike": -3,
    "戰爭": -7, "衝突": -5, "空襲": -6, "飛彈": -7, "關稅": -5,
    "制裁": -5, "通膨": -4, "升息": -4, "衰退": -6, "地震": -4,
    "下修": -3, "裁員": -3, "禁令": -4, "調查": -3,
}
POSITIVE_KEYWORDS = {
    "beat": 4, "surge": 4, "growth": 3, "record": 4, "upgrade": 3,
    "eases": 3, "cooling": 2, "deal": 2, "stimulus": 4, "rebound": 3,
    "AI": 2, "chip": 2, "semiconductor": 2,
    "優於預期": 4, "成長": 3, "新高": 4, "上修": 3, "降息": 5,
    "降溫": 2, "合作": 2, "刺激": 4, "反彈": 3, "AI": 2,
    "晶片": 2, "半導體": 2,
}
BIG_TECH_NAMES = [
    "NVIDIA", "NVDA", "TSMC", "台積電", "Apple", "AAPL", "Microsoft", "MSFT",
    "Amazon", "AMZN", "Meta", "Google", "Alphabet", "AMD", "Broadcom", "AVGO",
    "Intel", "Qualcomm", "Tesla", "ASML",
]


# =========================================================
# 基礎工具
# =========================================================
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


def _safe_float(v: Any, default: float | None = None) -> float | None:
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return default


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today_text() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _score_clip(v: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, v))


def _fmt_num(v: Any, digits: int = 2) -> str:
    x = _safe_float(v)
    if x is None:
        return "-"
    return f"{x:,.{digits}f}"


def _fmt_pct(v: Any, digits: int = 2) -> str:
    x = _safe_float(v)
    if x is None:
        return "-"
    return f"{x:,.{digits}f}%"


def _fmt_bool(v: Any) -> str:
    if isinstance(v, bool):
        return "是" if v else "否"
    s = _safe_str(v).lower()
    return "是" if s in {"true", "1", "yes", "y", "是"} else "否"


def _set_status(msg: str, level: str = "info"):
    st.session_state[_k("status_msg")] = msg
    st.session_state[_k("status_type")] = level


def _show_status():
    msg = _safe_str(st.session_state.get(_k("status_msg"), ""))
    level = _safe_str(st.session_state.get(_k("status_type"), "info"))
    if not msg:
        return
    if level == "success":
        st.success(msg)
    elif level == "warning":
        st.warning(msg)
    elif level == "error":
        st.error(msg)
    else:
        st.info(msg)


def _normalize_direction_from_points(points: float | None) -> str:
    x = _safe_float(points)
    if x is None:
        return "未知"
    if x > 15:
        return "偏多"
    if x < -15:
        return "偏空"
    return "震盪"


def _create_record_id(pred_date: str) -> str:
    raw = f"macro|{_safe_str(pred_date)}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _date_context_text(target_date: str) -> str:
    target = pd.to_datetime(target_date, errors="coerce")
    today = pd.to_datetime(_today_text(), errors="coerce")
    if pd.isna(target) or pd.isna(today):
        return ""
    diff = int((target.date() - today.date()).days)
    if diff == 0:
        return "以最新可得市場資料推估今日走勢。"
    if diff > 0:
        return f"以最新可得市場資料，預推 {target.strftime('%Y-%m-%d')} 走勢。距今天 {diff} 天。"
    return f"此日期早於今天，屬於回顧式推估檢查。距今天 {abs(diff)} 天。"


# =========================================================
# GitHub 儲存
# =========================================================
def _github_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("MACRO_TREND_RECORDS_GITHUB_PATH", "macro_trend_records.json")) or "macro_trend_records.json",
    }


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


def _read_json_from_github(path: str) -> tuple[Any, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return None, "未設定 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], path),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return None, ""
        if resp.status_code != 200:
            return None, f"GitHub 讀取失敗：{resp.status_code} / {resp.text[:300]}"
        data = resp.json()
        content = _safe_str(data.get("content"))
        if not content:
            return None, ""
        decoded = base64.b64decode(content).decode("utf-8")
        return json.loads(decoded), ""
    except Exception as e:
        return None, f"GitHub 讀取例外：{e}"


def _get_file_sha(path: str) -> tuple[str, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return "", "缺少 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], path),
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


def _write_json_to_github(path: str, payload: Any, message: str) -> tuple[bool, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN"
    sha, err = _get_file_sha(path)
    if err:
        return False, err
    content_text = json.dumps(payload, ensure_ascii=False, indent=2)
    encoded = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")
    body: dict[str, Any] = {
        "message": message,
        "content": encoded,
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha
    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], path),
            headers=_github_headers(token),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已回寫 GitHub：{path}"
        return False, f"GitHub 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"GitHub 寫入例外：{e}"


# =========================================================
# 市場資料
# =========================================================
@st.cache_data(ttl=180, show_spinner=False)
def _get_yahoo_chart(symbol: str, interval: str = "1d", rng: str = "3mo", refresh_key: str = "") -> pd.DataFrame:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "interval": interval,
        "range": rng,
        "includePrePost": "true",
        "events": "div,splits",
    }
    resp = requests.get(url, headers=UA, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    raw = resp.json()

    result = (((raw or {}).get("chart") or {}).get("result") or [None])[0] or {}
    ts = result.get("timestamp") or []
    quote = (((result.get("indicators") or {}).get("quote") or [None])[0] or {})
    adj = (((result.get("indicators") or {}).get("adjclose") or [None])[0] or {})

    df = pd.DataFrame(
        {
            "timestamp": ts,
            "open": quote.get("open", []),
            "high": quote.get("high", []),
            "low": quote.get("low", []),
            "close": quote.get("close", []),
            "volume": quote.get("volume", []),
            "adjclose": adj.get("adjclose", []),
        }
    )
    if df.empty:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume", "adjclose"])

    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
    for c in ["open", "high", "low", "close", "volume", "adjclose"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    return df[["datetime", "open", "high", "low", "close", "volume", "adjclose"]].copy()


@st.cache_data(ttl=180, show_spinner=False)
def _get_market_snapshot(refresh_key: str = "") -> dict[str, dict[str, Any]]:
    symbol_map = {
        "台股加權": "^TWII",
        "那斯達克": "^IXIC",
        "費半": "^SOX",
        "標普500期": "ES=F",
        "那指期": "NQ=F",
        "道瓊": "^DJI",
        "台積電ADR": "TSM",
        "美元台幣": "TWD=X",
        "VIX恐慌": "^VIX",
    }
    out: dict[str, dict[str, Any]] = {}

    for name, symbol in symbol_map.items():
        try:
            df = _get_yahoo_chart(symbol, interval="1d", rng="3mo", refresh_key=refresh_key)
            if df.empty:
                out[name] = {"symbol": symbol, "error": "empty"}
                continue
            temp = df.dropna(subset=["close"]).copy()
            if len(temp) < 2:
                out[name] = {"symbol": symbol, "error": "insufficient"}
                continue

            latest = temp.iloc[-1]
            prev = temp.iloc[-2]
            close_now = _safe_float(latest["close"])
            close_prev = _safe_float(prev["close"])
            chg = None if close_now is None or close_prev is None else close_now - close_prev
            chg_pct = None if chg is None or close_prev in [None, 0] else chg / close_prev * 100

            temp["ma5"] = temp["close"].rolling(5).mean()
            temp["ma10"] = temp["close"].rolling(10).mean()
            temp["ma20"] = temp["close"].rolling(20).mean()

            ma5 = _safe_float(temp.iloc[-1]["ma5"])
            ma10 = _safe_float(temp.iloc[-1]["ma10"])
            ma20 = _safe_float(temp.iloc[-1]["ma20"])

            out[name] = {
                "symbol": symbol,
                "close": close_now,
                "prev_close": close_prev,
                "change": chg,
                "change_pct": chg_pct,
                "ma5": ma5,
                "ma10": ma10,
                "ma20": ma20,
                "time": latest["datetime"],
                "df": temp.tail(60).copy(),
            }
        except Exception as e:
            out[name] = {"symbol": symbol, "error": str(e)}
    return out


@st.cache_data(ttl=1800, show_spinner=False)
def _get_twii_history(refresh_key: str = "") -> pd.DataFrame:
    df = _get_yahoo_chart("^TWII", interval="1d", rng="1y", refresh_key=refresh_key)
    if df.empty:
        return pd.DataFrame(columns=["date", "close", "prev_close", "change", "change_pct"])
    temp = df.dropna(subset=["close"]).copy().reset_index(drop=True)
    temp["date"] = pd.to_datetime(temp["datetime"], errors="coerce").dt.date
    temp["prev_close"] = temp["close"].shift(1)
    temp["change"] = temp["close"] - temp["prev_close"]
    temp["change_pct"] = temp["change"] / temp["prev_close"] * 100
    temp = temp.dropna(subset=["date"]).copy()
    return temp[["date", "close", "prev_close", "change", "change_pct"]]


# =========================================================
# 新聞資料
# =========================================================
@st.cache_data(ttl=300, show_spinner=False)
def _fetch_google_news_rss(query: str, refresh_key: str = "") -> list[dict[str, Any]]:
    url = "https://news.google.com/rss/search"
    params = {
        "q": query,
        "hl": "zh-TW",
        "gl": "TW",
        "ceid": "TW:zh-Hant",
    }
    resp = requests.get(url, headers=UA, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)

    rows: list[dict[str, Any]] = []
    for item in root.findall(".//item")[:12]:
        rows.append(
            {
                "title": _safe_str(item.findtext("title")),
                "link": _safe_str(item.findtext("link")),
                "pub_date": _safe_str(item.findtext("pubDate")),
                "source": _safe_str(item.findtext("source")),
            }
        )
    return rows


def _score_news_title(title: str) -> tuple[int, list[str]]:
    text = _safe_str(title)
    if not text:
        return 0, []
    score = 0
    hits: list[str] = []
    low = text.lower()
    for k, v in NEGATIVE_KEYWORDS.items():
        if (k.lower() in low) if re.match(r"^[A-Za-z0-9_=-]+$", k) else (k in text):
            score += v
            hits.append(k)
    for k, v in POSITIVE_KEYWORDS.items():
        if (k.lower() in low) if re.match(r"^[A-Za-z0-9_=-]+$", k) else (k in text):
            score += v
            hits.append(k)
    return score, hits


@st.cache_data(ttl=300, show_spinner=False)
def _get_news_bundle(refresh_key: str = "") -> dict[str, Any]:
    queries = {
        "國際風險": '(war OR tariff OR sanctions OR fed OR inflation OR recession OR geopolitics) (stocks OR semiconductor OR Taiwan)',
        "科技半導體": '(NVIDIA OR TSMC OR semiconductor OR chip OR AI) (stocks OR market)',
        "台股市場": '(Taiwan stocks OR Taiex OR 台股 OR 加權指數 OR 台積電)',
    }
    out: dict[str, Any] = {"items": [], "summary": []}
    seen = set()

    for bucket, query in queries.items():
        try:
            rows = _fetch_google_news_rss(query, refresh_key=refresh_key)
        except Exception:
            rows = []
        for row in rows:
            key = _safe_str(row.get("title"))
            if not key or key in seen:
                continue
            seen.add(key)
            score, hits = _score_news_title(key)
            big_tech_hit = any(x.lower() in key.lower() for x in BIG_TECH_NAMES)
            if big_tech_hit:
                score += 2
                hits.append("世界大廠")
            row["bucket"] = bucket
            row["score"] = score
            row["hits"] = "、".join(sorted(set(hits)))
            out["items"].append(row)

    news_df = pd.DataFrame(out["items"])
    if news_df.empty:
        out["df"] = pd.DataFrame(columns=["bucket", "title", "source", "score", "hits", "pub_date"])
        out["news_score"] = 50.0
        out["risk_level"] = "中性"
        return out

    news_df["score"] = pd.to_numeric(news_df["score"], errors="coerce").fillna(0)
    news_df = news_df.sort_values(["score", "pub_date"], ascending=[True, False]).reset_index(drop=True)
    raw_sum = float(news_df["score"].head(12).sum())
    news_score = _score_clip(50 + raw_sum * 1.8, 0, 100)
    if news_score >= 60:
        risk_level = "偏多"
    elif news_score <= 40:
        risk_level = "偏空"
    else:
        risk_level = "中性"

    out["df"] = news_df[["bucket", "title", "source", "score", "hits", "pub_date", "link"]].copy()
    out["news_score"] = news_score
    out["risk_level"] = risk_level
    return out


# =========================================================
# 推估模型
# =========================================================
def _pct_to_score(x: float | None, center: float = 0.0, scale: float = 4.0, invert: bool = False) -> float | None:
    if x is None:
        return None
    z = ((x - center) / scale) * 25
    val = 50 + (-z if invert else z)
    return _score_clip(val)


def _build_factor_table(snapshot: dict[str, dict[str, Any]], news_bundle: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    def add_factor(name: str, value: float | None, weight: float, comment: str):
        if value is None:
            rows.append({"因子": name, "分數": None, "權重": weight, "加權分": None, "說明": comment})
            return
        score = _score_clip(value)
        rows.append({"因子": name, "分數": score, "權重": weight, "加權分": score * weight, "說明": comment})

    twii = snapshot.get("台股加權", {})
    nasdaq = snapshot.get("那斯達克", {})
    sox = snapshot.get("費半", {})
    es = snapshot.get("標普500期", {})
    nq = snapshot.get("那指期", {})
    tsm = snapshot.get("台積電ADR", {})
    usdtwd = snapshot.get("美元台幣", {})
    vix = snapshot.get("VIX恐慌", {})

    tw_close = _safe_float(twii.get("close"))
    tw_ma20 = _safe_float(twii.get("ma20"))
    trend_pct = None if tw_close is None or tw_ma20 in [None, 0] else (tw_close - tw_ma20) / tw_ma20 * 100
    add_factor("台股中期趨勢", _pct_to_score(trend_pct, scale=3.0), 0.22, f"加權相對 MA20：{_fmt_pct(trend_pct)}")

    nas_pct = _safe_float(nasdaq.get("change_pct"))
    sox_pct = _safe_float(sox.get("change_pct"))
    tech_mix = None if nas_pct is None and sox_pct is None else ((nas_pct or 0) * 0.45 + (sox_pct or 0) * 0.55)
    add_factor("美股科技氛圍", _pct_to_score(tech_mix, scale=2.5), 0.18, f"NASDAQ {_fmt_pct(nas_pct)} / SOX {_fmt_pct(sox_pct)}")

    es_pct = _safe_float(es.get("change_pct"))
    nq_pct = _safe_float(nq.get("change_pct"))
    night_mix = None if es_pct is None and nq_pct is None else ((es_pct or 0) * 0.40 + (nq_pct or 0) * 0.60)
    add_factor("夜盤 / 期貨代理", _pct_to_score(night_mix, scale=2.0), 0.18, f"ES {_fmt_pct(es_pct)} / NQ {_fmt_pct(nq_pct)}")

    tsm_pct = _safe_float(tsm.get("change_pct"))
    add_factor("台積電 ADR", _pct_to_score(tsm_pct, scale=2.5), 0.10, f"TSM {_fmt_pct(tsm_pct)}")

    twd_pct = _safe_float(usdtwd.get("change_pct"))
    add_factor("美元台幣", _pct_to_score(twd_pct, scale=1.2, invert=True), 0.08, f"USD/TWD {_fmt_pct(twd_pct)}")

    vix_pct = _safe_float(vix.get("change_pct"))
    add_factor("風險偏好 / VIX", _pct_to_score(vix_pct, scale=8.0, invert=True), 0.09, f"VIX {_fmt_pct(vix_pct)}")

    add_factor("國際新聞風險", _safe_float(news_bundle.get("news_score"), 50.0), 0.15, f"新聞風險：{_safe_str(news_bundle.get('risk_level'))}")
    return pd.DataFrame(rows)


def _build_prediction(snapshot: dict[str, dict[str, Any]], factor_df: pd.DataFrame) -> dict[str, Any]:
    twii = snapshot.get("台股加權", {})
    tw_close = _safe_float(twii.get("close")) or 22000.0

    valid = factor_df.dropna(subset=["加權分", "權重"]).copy()
    total_weight = valid["權重"].sum() if not valid.empty else 1.0
    total_score = float(valid["加權分"].sum() / total_weight) if total_weight else 50.0

    bias = total_score - 50.0
    expected_pct = max(-2.5, min(2.5, bias * 0.03))
    expected_points = tw_close * expected_pct / 100

    day_range_pct = 0.55 + abs(expected_pct) * 0.75
    up_target = tw_close + expected_points + (tw_close * day_range_pct / 100) * 0.25
    down_target = tw_close + expected_points - (tw_close * day_range_pct / 100) * 0.25

    if total_score >= 68:
        stance = "偏多可進場"
        action = "可分批布局強勢股 / AI / 半導體，但不要一次滿倉。"
        risk = "中"
    elif total_score >= 58:
        stance = "偏多但不追高"
        action = "可以找拉回分批進，優先看量價結構完整個股。"
        risk = "中"
    elif total_score >= 45:
        stance = "震盪觀察"
        action = "先看盤後半小時方向，避免開高追價或急跌亂接。"
        risk = "中高"
    elif total_score >= 35:
        stance = "偏空保守"
        action = "以減碼、短打、快進快出為主，不適合重押。"
        risk = "高"
    else:
        stance = "明顯偏空"
        action = "先保資金，少接刀，已有持股優先看停損與避險。"
        risk = "很高"

    return {
        "score": round(total_score, 2),
        "expected_pct": round(expected_pct, 2),
        "expected_points": round(expected_points, 0),
        "range_low": round(down_target, 0),
        "range_high": round(up_target, 0),
        "stance": stance,
        "action": action,
        "risk": risk,
    }


def _build_entry_rules(pred: dict[str, Any], snapshot: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    twii = snapshot.get("台股加權", {})
    close_v = _safe_float(twii.get("close"))
    ma5 = _safe_float(twii.get("ma5"))
    ma20 = _safe_float(twii.get("ma20"))

    rules = []
    rules.append({"策略": "開盤判斷", "建議": f"若加權開盤後 15~30 分鐘站穩前收附近，且量能不失速，可按 {pred['stance']} 執行。"})
    if close_v is not None and ma5 is not None and ma20 is not None:
        rules.append({"策略": "均線位階", "建議": f"目前加權 {close_v:,.0f}，MA5 {ma5:,.0f}，MA20 {ma20:,.0f}；站穩 MA5 才適合積極，跌破 MA20 轉保守。"})
    rules.append({"策略": "買點", "建議": "優先挑強於大盤、量增、族群同步轉強的個股，不追單一利多爆量長紅末端。"})
    rules.append({"策略": "賣點", "建議": "若大盤預估轉弱，先處理弱勢股、破五日線股、爆量不漲股；強勢股改移動停利。"})
    rules.append({"策略": "風控", "建議": "單筆先小倉，確認盤勢延續再加碼；若與預估相反，照紀律停損，不要凹單。"})
    return rules


# =========================================================
# 紀錄 / 回測
# =========================================================
def _ensure_record_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=RECORD_COLUMNS)
    x = df.copy()
    for c in RECORD_COLUMNS:
        if c not in x.columns:
            x[c] = None
    numeric_cols = [
        "推估分數", "推估漲跌點", "推估漲跌%", "推估區間低", "推估區間高",
        "台股加權", "台股漲跌%", "NASDAQ漲跌%", "SOX漲跌%", "NQ漲跌%", "ES漲跌%", "TSM ADR漲跌%", "USD/TWD漲跌%", "VIX漲跌%",
        "新聞分數", "實際收盤", "實際漲跌點", "實際漲跌%", "點數誤差",
    ]
    for c in numeric_cols:
        x[c] = pd.to_numeric(x[c], errors="coerce")
    for c in ["方向命中", "區間命中", "開盤是否適合進場"]:
        x[c] = x[c].fillna(False).map(lambda v: str(v).lower() in {"true", "1", "yes", "y", "是"})
    x["預測日期"] = x["預測日期"].fillna("").astype(str)
    need_id = x["record_id"].isna() | (x["record_id"].astype(str).str.strip() == "")
    for idx in x[need_id].index:
        x.at[idx, "record_id"] = _create_record_id(_safe_str(x.at[idx, "預測日期"]))
    return x[RECORD_COLUMNS].copy()


@st.cache_data(ttl=120, show_spinner=False)
def _load_prediction_records(refresh_key: str = "") -> tuple[pd.DataFrame, str]:
    payload, err = _read_json_from_github(_github_config()["path"])
    if err:
        return pd.DataFrame(columns=RECORD_COLUMNS), err
    if isinstance(payload, list):
        return _ensure_record_columns(pd.DataFrame(payload)), ""
    return pd.DataFrame(columns=RECORD_COLUMNS), ""


def _save_prediction_records(df: pd.DataFrame) -> bool:
    clean = _ensure_record_columns(df)
    ok, msg = _write_json_to_github(
        _github_config()["path"],
        clean.to_dict(orient="records"),
        f"update macro trend records at {_now_text()}",
    )
    if ok:
        try:
            _load_prediction_records.clear()
        except Exception:
            pass
        _set_status(msg, "success")
        return True
    _set_status(msg, "error")
    return False


def _build_factor_summary(factor_df: pd.DataFrame) -> str:
    if factor_df is None or factor_df.empty:
        return ""
    parts = []
    temp = factor_df.copy().sort_values("權重", ascending=False)
    for _, r in temp.head(4).iterrows():
        parts.append(f"{_safe_str(r.get('因子'))}:{_fmt_num(r.get('分數'), 1)}")
    return "｜".join(parts)


def _build_record_row(pred_date: str, pred: dict[str, Any], snapshot: dict[str, dict[str, Any]], news_bundle: dict[str, Any], factor_df: pd.DataFrame) -> dict[str, Any]:
    return {
        "record_id": _create_record_id(pred_date),
        "預測日期": pred_date,
        "建立時間": _now_text(),
        "更新時間": _now_text(),
        "推估分數": pred.get("score"),
        "推估方向": _normalize_direction_from_points(pred.get("expected_points")),
        "推估漲跌點": pred.get("expected_points"),
        "推估漲跌%": pred.get("expected_pct"),
        "推估區間低": pred.get("range_low"),
        "推估區間高": pred.get("range_high"),
        "操作建議": pred.get("action"),
        "風險等級": pred.get("risk"),
        "台股加權": _safe_float(snapshot.get("台股加權", {}).get("close")),
        "台股漲跌%": _safe_float(snapshot.get("台股加權", {}).get("change_pct")),
        "NASDAQ漲跌%": _safe_float(snapshot.get("那斯達克", {}).get("change_pct")),
        "SOX漲跌%": _safe_float(snapshot.get("費半", {}).get("change_pct")),
        "NQ漲跌%": _safe_float(snapshot.get("那指期", {}).get("change_pct")),
        "ES漲跌%": _safe_float(snapshot.get("標普500期", {}).get("change_pct")),
        "TSM ADR漲跌%": _safe_float(snapshot.get("台積電ADR", {}).get("change_pct")),
        "USD/TWD漲跌%": _safe_float(snapshot.get("美元台幣", {}).get("change_pct")),
        "VIX漲跌%": _safe_float(snapshot.get("VIX恐慌", {}).get("change_pct")),
        "新聞分數": _safe_float(news_bundle.get("news_score")),
        "新聞風險": _safe_str(news_bundle.get("risk_level")),
        "因子摘要": _build_factor_summary(factor_df),
        "實際收盤": None,
        "實際漲跌點": None,
        "實際漲跌%": None,
        "實際方向": "",
        "方向命中": False,
        "點數誤差": None,
        "區間命中": False,
        "開盤是否適合進場": False,
        "收盤檢討": "",
        "備註": "",
    }


def _upsert_record(df: pd.DataFrame, row: dict[str, Any]) -> pd.DataFrame:
    x = _ensure_record_columns(df)
    rid = _safe_str(row.get("record_id"))
    if rid and rid in x["record_id"].astype(str).tolist():
        idx = x.index[x["record_id"].astype(str) == rid][0]
        old_create = _safe_str(x.at[idx, "建立時間"])
        for k, v in row.items():
            if k in x.columns:
                x.at[idx, k] = v
        x.at[idx, "建立時間"] = old_create or _now_text()
        x.at[idx, "更新時間"] = _now_text()
        return _ensure_record_columns(x)
    return _ensure_record_columns(pd.concat([x, pd.DataFrame([row])], ignore_index=True))


def _apply_actual_result_to_row(row: dict[str, Any], twii_history: pd.DataFrame) -> dict[str, Any]:
    x = dict(row)
    pred_date = pd.to_datetime(_safe_str(x.get("預測日期")), errors="coerce")
    if pd.isna(pred_date) or twii_history.empty:
        return x
    hit = twii_history[twii_history["date"] == pred_date.date()]
    if hit.empty:
        return x
    r = hit.iloc[-1]
    actual_close = _safe_float(r.get("close"))
    actual_points = _safe_float(r.get("change"))
    actual_pct = _safe_float(r.get("change_pct"))
    pred_points = _safe_float(x.get("推估漲跌點"))
    pred_dir = _safe_str(x.get("推估方向")) or _normalize_direction_from_points(pred_points)
    actual_dir = _normalize_direction_from_points(actual_points)
    low = _safe_float(x.get("推估區間低"))
    high = _safe_float(x.get("推估區間高"))
    range_hit = False
    if actual_close is not None and low is not None and high is not None:
        range_hit = min(low, high) <= actual_close <= max(low, high)
    point_error = None
    if pred_points is not None and actual_points is not None:
        point_error = abs(pred_points - actual_points)

    x["實際收盤"] = actual_close
    x["實際漲跌點"] = actual_points
    x["實際漲跌%"] = actual_pct
    x["實際方向"] = actual_dir
    x["方向命中"] = pred_dir == actual_dir
    x["點數誤差"] = point_error
    x["區間命中"] = range_hit
    x["更新時間"] = _now_text()
    return x


def _backfill_actual_results(records_df: pd.DataFrame, twii_history: pd.DataFrame) -> pd.DataFrame:
    x = _ensure_record_columns(records_df)
    if x.empty or twii_history.empty:
        return x
    rows = []
    for _, row in x.iterrows():
        rows.append(_apply_actual_result_to_row(row.to_dict(), twii_history))
    return _ensure_record_columns(pd.DataFrame(rows))


def _build_accuracy_summary(df: pd.DataFrame) -> dict[str, Any]:
    x = _ensure_record_columns(df)
    if x.empty:
        return {"樣本數": 0, "方向命中率": 0.0, "區間命中率": 0.0, "平均點數誤差": None, "平均實際漲跌點": None}
    done = x.dropna(subset=["實際漲跌點"]).copy()
    if done.empty:
        return {"樣本數": 0, "方向命中率": 0.0, "區間命中率": 0.0, "平均點數誤差": None, "平均實際漲跌點": None}
    return {
        "樣本數": int(len(done)),
        "方向命中率": float(done["方向命中"].fillna(False).mean() * 100),
        "區間命中率": float(done["區間命中"].fillna(False).mean() * 100),
        "平均點數誤差": float(pd.to_numeric(done["點數誤差"], errors="coerce").dropna().mean()) if not pd.to_numeric(done["點數誤差"], errors="coerce").dropna().empty else None,
        "平均實際漲跌點": float(pd.to_numeric(done["實際漲跌點"], errors="coerce").dropna().mean()) if not pd.to_numeric(done["實際漲跌點"], errors="coerce").dropna().empty else None,
    }


def _format_records_for_show(df: pd.DataFrame) -> pd.DataFrame:
    x = _ensure_record_columns(df).copy()
    for c in ["推估分數", "推估漲跌點", "推估漲跌%", "推估區間低", "推估區間高", "實際收盤", "實際漲跌點", "實際漲跌%", "點數誤差", "新聞分數"]:
        if c in x.columns:
            x[c] = x[c].apply(lambda v: "" if pd.isna(v) else round(float(v), 2))
    for c in ["方向命中", "區間命中", "開盤是否適合進場"]:
        if c in x.columns:
            x[c] = x[c].apply(_fmt_bool)
    return x


# =========================================================
# 主畫面
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    if _k("status_msg") not in st.session_state:
        st.session_state[_k("status_msg")] = ""
        st.session_state[_k("status_type")] = "info"
    if _k("refresh_key") not in st.session_state:
        st.session_state[_k("refresh_key")] = _now_text()
    if _k("record_refresh_key") not in st.session_state:
        st.session_state[_k("record_refresh_key")] = _now_text()

    render_pro_hero(
        title="大盤走勢",
        subtitle="以台股大盤、美股科技、夜盤、美盤、國際新聞與世界大廠消息，推估當日漲跌方向、點數與是否適合進場，並可記錄回測準確率。",
    )
    _show_status()

    top_cols = st.columns([1.0, 1.0, 1.0, 1.0, 3.0])
    with top_cols[0]:
        if st.button("🔄 重新抓資料", use_container_width=True):
            st.session_state[_k("refresh_key")] = _now_text()
            try:
                _get_market_snapshot.clear()
                _get_news_bundle.clear()
                _fetch_google_news_rss.clear()
                _get_yahoo_chart.clear()
                _get_twii_history.clear()
            except Exception:
                pass
            _set_status("已重新抓取大盤 / 美盤 / 新聞資料", "success")
            st.rerun()
    with top_cols[1]:
        st.toggle("快顯模式", value=True, key=_k("fast_mode"))
    with top_cols[2]:
        st.toggle("顯示新聞明細", value=False, key=_k("show_news_detail"))
    with top_cols[3]:
        if st.button("📂 重新讀紀錄", use_container_width=True):
            st.session_state[_k("record_refresh_key")] = _now_text()
            try:
                _load_prediction_records.clear()
            except Exception:
                pass
            _set_status("已重新讀取推估紀錄", "success")
            st.rerun()
    with top_cols[4]:
        st.caption(f"資料更新：{st.session_state.get(_k('refresh_key'), '-') } ｜ 紀錄更新：{st.session_state.get(_k('record_refresh_key'), '-')}")

    target_cols = st.columns([1.2, 2.8])
    with target_cols[0]:
        target_date = st.date_input("推估日期", value=datetime.now().date(), key=_k("target_date"))
    with target_cols[1]:
        st.info(_date_context_text(str(target_date)))

    refresh_key = _safe_str(st.session_state.get(_k("refresh_key"), ""))
    record_refresh_key = _safe_str(st.session_state.get(_k("record_refresh_key"), ""))
    snapshot = _get_market_snapshot(refresh_key=refresh_key)
    news_bundle = _get_news_bundle(refresh_key=refresh_key)
    factor_df = _build_factor_table(snapshot, news_bundle)
    pred = _build_prediction(snapshot, factor_df)
    rules = _build_entry_rules(pred, snapshot)
    twii_history = _get_twii_history(refresh_key=refresh_key)
    records_df, records_err = _load_prediction_records(refresh_key=record_refresh_key)
    if records_err:
        st.warning(records_err)
    records_df = _backfill_actual_results(records_df, twii_history)
    acc = _build_accuracy_summary(records_df)

    twii = snapshot.get("台股加權", {})
    nasdaq = snapshot.get("那斯達克", {})
    sox = snapshot.get("費半", {})
    nq = snapshot.get("那指期", {})

    render_pro_kpi_row([
        {"label": "大盤推估分數", "value": f"{pred['score']:.1f}", "delta": pred["stance"], "delta_class": "pro-kpi-delta-flat"},
        {"label": "推估漲跌點", "value": f"{pred['expected_points']:+,.0f} 點", "delta": f"{pred['expected_pct']:+.2f}%", "delta_class": "pro-kpi-delta-flat"},
        {"label": "台股加權", "value": _fmt_num(twii.get("close"), 0), "delta": _fmt_pct(twii.get("change_pct")), "delta_class": "pro-kpi-delta-flat"},
        {"label": "方向命中率", "value": f"{acc['方向命中率']:.1f}%", "delta": f"樣本 {acc['樣本數']}", "delta_class": "pro-kpi-delta-flat"},
        {"label": "平均點數誤差", "value": "-" if acc['平均點數誤差'] is None else f"{acc['平均點數誤差']:.1f}", "delta": f"區間命中 {acc['區間命中率']:.1f}%", "delta_class": "pro-kpi-delta-flat"},
    ])

    st.info(
        f"股神判讀：**{pred['stance']}**｜推估 {target_date.strftime('%Y-%m-%d')} 大盤合理區間約 **{pred['range_low']:,.0f} ~ {pred['range_high']:,.0f}**。"
        f" 建議：{pred['action']}｜風險等級：{pred['risk']}"
    )

    tabs = st.tabs(["📌 總覽", "📊 因子評分", "📝 紀錄追蹤", "📰 新聞風險", "🧠 股神判斷因子", "📈 市場明細"])

    with tabs[0]:
        render_pro_section(f"{target_date.strftime('%Y-%m-%d')} 進場結論")
        left, right = st.columns([1.35, 1.0])
        with left:
            st.dataframe(pd.DataFrame(rules), use_container_width=True, hide_index=True)
        with right:
            render_pro_info_card(
                "核心結論",
                [
                    ("推估日期", target_date.strftime('%Y-%m-%d'), ""),
                    ("大盤偏向", pred["stance"], ""),
                    ("推估漲跌點", f"{pred['expected_points']:+,.0f}", ""),
                    ("預估區間", f"{pred['range_low']:,.0f} ~ {pred['range_high']:,.0f}", ""),
                    ("新聞風險", _safe_str(news_bundle.get("risk_level")), ""),
                    ("操作重點", pred["action"], ""),
                ],
                chips=["大盤走勢", "進場依據", "可記錄", "可回測"],
            )

    with tabs[1]:
        render_pro_section("因子評分")
        show_factor = factor_df.copy()
        if not show_factor.empty:
            show_factor["分數"] = show_factor["分數"].apply(lambda x: None if pd.isna(x) else round(float(x), 2))
            show_factor["加權分"] = show_factor["加權分"].apply(lambda x: None if pd.isna(x) else round(float(x), 2))
            st.dataframe(show_factor, use_container_width=True, hide_index=True)
        else:
            st.info("目前無法產生因子評分。")

    with tabs[2]:
        render_pro_section("推估紀錄 / 回測檢討", "把每天推估存起來，之後回填實際結果，檢討準確率。")
        pred_date = st.date_input("紀錄日期", value=target_date, key=_k("pred_date"))
        c1, c2, c3 = st.columns([1.2, 1.2, 2.6])
        with c1:
            if st.button("💾 儲存此日期推估", use_container_width=True, type="primary"):
                row = _build_record_row(str(pred_date), pred, snapshot, news_bundle, factor_df)
                new_df = _upsert_record(records_df, row)
                if _save_prediction_records(new_df):
                    st.session_state[_k("record_refresh_key")] = _now_text()
                    st.rerun()
        with c2:
            if st.button("🧮 回填實際結果", use_container_width=True):
                filled = _backfill_actual_results(records_df, twii_history)
                if _save_prediction_records(filled):
                    st.session_state[_k("record_refresh_key")] = _now_text()
                    st.rerun()
        with c3:
            st.caption("可指定任意日期先存推估；若是歷史日期，可於收盤資料完整後回填檢查模型偏差。")

        render_pro_kpi_row([
            {"label": "已回測樣本", "value": acc["樣本數"], "delta": "有實際結果", "delta_class": "pro-kpi-delta-flat"},
            {"label": "方向命中率", "value": f"{acc['方向命中率']:.2f}%", "delta": "偏多 / 偏空 / 震盪", "delta_class": "pro-kpi-delta-flat"},
            {"label": "區間命中率", "value": f"{acc['區間命中率']:.2f}%", "delta": "實際收盤落入預估區間", "delta_class": "pro-kpi-delta-flat"},
            {"label": "平均點數誤差", "value": "-" if acc['平均點數誤差'] is None else f"{acc['平均點數誤差']:.2f}", "delta": "越低越好", "delta_class": "pro-kpi-delta-flat"},
        ])

        if records_df.empty:
            st.info("目前還沒有推估紀錄。")
        else:
            show_records = _format_records_for_show(records_df.sort_values("預測日期", ascending=False))
            st.dataframe(
                show_records[[c for c in [
                    "預測日期", "推估方向", "推估漲跌點", "推估區間低", "推估區間高",
                    "實際方向", "實際漲跌點", "實際收盤", "方向命中", "區間命中", "點數誤差",
                    "新聞風險", "因子摘要", "更新時間"
                ] if c in show_records.columns]],
                use_container_width=True,
                hide_index=True,
            )

            edit_cols = [
                "record_id", "預測日期", "開盤是否適合進場", "收盤檢討", "備註",
                "推估方向", "推估漲跌點", "實際方向", "實際漲跌點", "方向命中", "點數誤差"
            ]
            editor_df = records_df[[c for c in edit_cols if c in records_df.columns]].copy().sort_values("預測日期", ascending=False)
            edited = st.data_editor(
                editor_df,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key=_k("record_editor"),
                column_config={
                    "record_id": st.column_config.TextColumn("record_id", disabled=True),
                    "預測日期": st.column_config.TextColumn("預測日期", disabled=True),
                    "開盤是否適合進場": st.column_config.CheckboxColumn("開盤是否適合進場"),
                    "收盤檢討": st.column_config.TextColumn("收盤檢討", width="large"),
                    "備註": st.column_config.TextColumn("備註", width="large"),
                    "推估方向": st.column_config.TextColumn("推估方向", disabled=True),
                    "推估漲跌點": st.column_config.NumberColumn("推估漲跌點", format="%.2f", disabled=True),
                    "實際方向": st.column_config.TextColumn("實際方向", disabled=True),
                    "實際漲跌點": st.column_config.NumberColumn("實際漲跌點", format="%.2f", disabled=True),
                    "方向命中": st.column_config.CheckboxColumn("方向命中", disabled=True),
                    "點數誤差": st.column_config.NumberColumn("點數誤差", format="%.2f", disabled=True),
                },
            )
            if st.button("✅ 儲存檢討內容", use_container_width=True):
                base = records_df.copy()
                edit_map = {str(r["record_id"]): dict(r) for _, r in edited.iterrows()}
                for idx in base.index:
                    rid = _safe_str(base.at[idx, "record_id"])
                    if rid not in edit_map:
                        continue
                    base.at[idx, "開盤是否適合進場"] = bool(edit_map[rid].get("開盤是否適合進場"))
                    base.at[idx, "收盤檢討"] = _safe_str(edit_map[rid].get("收盤檢討"))
                    base.at[idx, "備註"] = _safe_str(edit_map[rid].get("備註"))
                    base.at[idx, "更新時間"] = _now_text()
                if _save_prediction_records(base):
                    st.session_state[_k("record_refresh_key")] = _now_text()
                    st.rerun()

    with tabs[3]:
        render_pro_section("國際新聞 / 世界大廠消息風險")
        news_df = news_bundle.get("df")
        if isinstance(news_df, pd.DataFrame) and not news_df.empty:
            show_news = news_df.copy()
            if st.session_state.get(_k("fast_mode"), True) and not st.session_state.get(_k("show_news_detail"), False):
                show_news = show_news.head(8).copy()
            st.dataframe(show_news[[c for c in ["bucket", "title", "source", "score", "hits", "pub_date"] if c in show_news.columns]], use_container_width=True, hide_index=True)
            if st.session_state.get(_k("show_news_detail"), False):
                for _, row in show_news.head(12).iterrows():
                    st.markdown(f"- [{_safe_str(row.get('title'))}]({_safe_str(row.get('link'))}) ｜ {_safe_str(row.get('source'))} ｜ 分數 {(_safe_float(row.get('score'), 0) or 0):+.0f}")
        else:
            st.info("目前抓不到新聞資料。")

    with tabs[4]:
        render_pro_section("股神角度還要看的判斷因子")
        extra_factors = pd.DataFrame([
            {"因子": "開盤半小時量價", "用途": "判斷預估方向有沒有被市場確認", "是否建議加入": "強烈建議"},
            {"因子": "台積電 / 聯發科 / 鴻海三權值同步性", "用途": "避免只看指數忽略權值股帶動力", "是否建議加入": "強烈建議"},
            {"因子": "族群同步強弱", "用途": "AI、半導體、機器人、重電是否整體共振", "是否建議加入": "強烈建議"},
            {"因子": "融資增減與隔日沖熱度", "用途": "判斷籌碼是否過熱", "是否建議加入": "建議"},
            {"因子": "外資期貨未平倉 / 現貨買賣超", "用途": "判斷大資金方向", "是否建議加入": "強烈建議"},
            {"因子": "重大事件日曆", "用途": "Fed、CPI、NVIDIA財報、台積電法說", "是否建議加入": "強烈建議"},
            {"因子": "前一日強勢股隔日延續率", "用途": "判斷市場願不願意追價", "是否建議加入": "建議"},
            {"因子": "恐慌事件等級", "用途": "戰爭、制裁、關稅、禁運是否屬於短期雜訊或中期趨勢", "是否建議加入": "強烈建議"},
        ])
        st.dataframe(extra_factors, use_container_width=True, hide_index=True)
        st.warning("這一版已先把大盤 / 美盤 / 夜盤 / 世界大廠 / 國際新聞整合進來，並加上每日記錄與回測。下一階段最值得補的是：外資籌碼、正式台指夜盤、重大事件日曆。")

    with tabs[5]:
        render_pro_section("市場明細")
        rows = []
        for name, info in snapshot.items():
            rows.append(
                {
                    "市場": name,
                    "代號": _safe_str(info.get("symbol")),
                    "收盤/最新": _safe_float(info.get("close")),
                    "昨收": _safe_float(info.get("prev_close")),
                    "漲跌": _safe_float(info.get("change")),
                    "漲跌%": _safe_float(info.get("change_pct")),
                    "MA5": _safe_float(info.get("ma5")),
                    "MA20": _safe_float(info.get("ma20")),
                    "資料時間": _safe_str(info.get("time")),
                    "錯誤": _safe_str(info.get("error")),
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()

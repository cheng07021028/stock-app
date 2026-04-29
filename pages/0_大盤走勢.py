# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Any
from pathlib import Path
import base64
import hashlib
import io
import json
import math

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

try:
    from utils import get_normalized_watchlist
except Exception:
    get_normalized_watchlist = None

try:
    from utils import get_history_data
except Exception:
    get_history_data = None

try:
    from utils import get_realtime_stock_info
except Exception:
    get_realtime_stock_info = None

try:
    from stock_master_service import load_stock_master
except Exception:
    load_stock_master = None

PAGE_TITLE = "大盤走勢｜股神Pro因子強化版"
PFX = "macro_godpro_factor_"
MACRO_ADVISOR_VERSION = "macro_advisor_reference_v2_pathfix_20260427"

RECORD_COLUMNS = [
    "record_id",
    "推估日期",
    "建立時間",
    "更新時間",
    "模式名稱",
    "市場情境",
    "推估方向",
    "方向強度",
    "是否適合進場",
    "是否適合續抱",
    "是否適合減碼",
    "是否適合出場",
    "建議動作",
    "股神模式分數",
    "股神信心度",
    "預估漲跌點",
    "預估高點",
    "預估低點",
    "預估區間寬度",
    "風險等級",
    "國際新聞風險分",
    "美股因子分",
    "夜盤因子分",
    "技術面因子分",
    "籌碼面因子分",
    "事件因子分",
    "結構面因子分",
    "風險面因子分",
    "大盤基準點",
    "加權收盤",
    "加權漲跌%",
    "成交量估分",
    "VIX",
    "美元台幣",
    "NASDAQ漲跌%",
    "SOX漲跌%",
    "SP500漲跌%",
    "台積電ADR漲跌%",
    "ES夜盤漲跌%",
    "NQ夜盤漲跌%",
    "外資買賣超估分",
    "期貨選擇權估分",
    "類股輪動估分",
    "外資買賣超(億)",
    "三大法人合計(億)",
    "外資期貨淨單",
    "PCR",
    "融資增減(億)",
    "融券增減張",
    "強勢族群",
    "弱勢族群",
    "重大事件清單",
    "因子來源狀態",
    "加權資料日期",
    "美股資料日期",
    "夜盤資料日期",
    "法人資料日期",
    "期權資料日期",
    "融資券資料日期",
    "新聞資料區間",
    "股神推論邏輯",
    "進場確認條件",
    "出場警訊",
    "主要風險",
    "建議倉位",
    "實際方向",
    "實際漲跌點",
    "實際高點",
    "實際低點",
    "方向是否命中",
    "區間是否命中",
    "點數誤差",
    "建議動作是否合適",
    "進場建議績效分",
    "出場建議績效分",
    "整體檢討分",
    "誤判主因類別",
    "誤判主因",
    "收盤檢討",
    "總體風險分桶",
    "進攻防守 regime",
    "資金流向判斷",
    "隔日劇本",
    "盤中確認訊號",
    "倉位上限%",
    "ETF參考動作",
    "個股操作總則",
    "模型一致性分數",
    "模型分歧警示",
    "大盤可參考分數",
    "大盤參考等級",
    "市場廣度分數",
    "類股輪動分數",
    "量價確認分數",
    "權值支撐分數",
    "推薦同步分數",
    "推薦加權建議",
    "推薦降權原因",
    "今日適合操作風格",
    "廣度樣本數",
    "上漲家數",
    "下跌家數",
    "站上MA20比例",
    "站上MA60比例",
    "創20日新高家數",
    "創20日新低家數",
    "推薦強勢比例",
    "推薦平均分數",
    "推薦起漲比例",
    "備註",
]

MODEL_NAMES = [
    "股神平衡版",
    "技術面優先",
    "美夜盤優先",
    "新聞風險優先",
    "籌碼事件優先",
]

DIRECTION_OPTIONS = ["偏多", "偏空", "震盪"]
RISK_OPTIONS = ["低", "中", "高", "極高"]
ERROR_CAUSES = ["", "新聞", "夜盤", "美股", "技術", "籌碼", "事件", "風險控管", "點數高估", "點數低估", "其他"]


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


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today_text() -> str:
    return date.today().strftime("%Y-%m-%d")


def _create_record_id(pred_date: str, model_name: str) -> str:
    raw = f"{pred_date}|{model_name}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _set_status(msg: str, level: str = "info"):
    st.session_state[_k("status_msg")] = msg
    st.session_state[_k("status_type")] = level


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


def _get_file_sha() -> tuple[str, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return "", "缺少 GITHUB_TOKEN"
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
        return "", f"讀取 GitHub SHA 失敗：{resp.status_code} / {resp.text[:200]}"
    except Exception as e:
        return "", f"讀取 GitHub SHA 例外：{e}"


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=RECORD_COLUMNS)
    x = df.copy()
    for c in RECORD_COLUMNS:
        if c not in x.columns:
            x[c] = None
    numeric_cols = [
        "股神模式分數", "股神信心度", "預估漲跌點", "預估高點", "預估低點", "預估區間寬度",
        "國際新聞風險分", "美股因子分", "夜盤因子分", "技術面因子分", "籌碼面因子分", "事件因子分",
        "結構面因子分", "風險面因子分", "大盤基準點", "加權收盤", "加權漲跌%", "成交量估分",
        "VIX", "美元台幣", "NASDAQ漲跌%", "SOX漲跌%", "SP500漲跌%", "台積電ADR漲跌%", "ES夜盤漲跌%",
        "NQ夜盤漲跌%", "外資買賣超估分", "期貨選擇權估分", "類股輪動估分",
        "外資買賣超(億)", "三大法人合計(億)", "外資期貨淨單", "PCR", "融資增減(億)", "融券增減張",
        "實際漲跌點", "實際高點", "實際低點", "點數誤差", "進場建議績效分", "出場建議績效分", "整體檢討分",
        "倉位上限%", "模型一致性分數", "大盤可參考分數", "市場廣度分數", "類股輪動分數", "量價確認分數", "權值支撐分數", "推薦同步分數", "廣度樣本數", "上漲家數", "下跌家數", "站上MA20比例", "站上MA60比例", "創20日新高家數", "創20日新低家數", "推薦強勢比例", "推薦平均分數", "推薦起漲比例",
    ]
    for c in numeric_cols:
        x[c] = pd.to_numeric(x[c], errors="coerce")
    bool_cols = ["方向是否命中", "區間是否命中", "建議動作是否合適"]
    for c in bool_cols:
        x[c] = x[c].fillna(False).map(lambda v: str(v).lower() in {"true", "1", "yes", "y", "是"})
    for c in [
        "推估日期", "建立時間", "更新時間", "模式名稱", "市場情境", "推估方向", "方向強度", "是否適合進場", "是否適合續抱",
        "是否適合減碼", "是否適合出場", "建議動作", "風險等級", "股神推論邏輯", "進場確認條件", "出場警訊", "主要風險",
        "建議倉位", "強勢族群", "弱勢族群", "重大事件清單", "因子來源狀態",
        "實際方向", "誤判主因類別", "誤判主因", "收盤檢討", "大盤參考等級", "推薦加權建議", "推薦降權原因", "今日適合操作風格", "備註"
    ]:
        x[c] = x[c].fillna("").astype(str)
    return x[RECORD_COLUMNS].copy()


def _read_records_from_github() -> tuple[pd.DataFrame, str]:
    cfg = _github_config()
    if not cfg["token"]:
        return pd.DataFrame(columns=RECORD_COLUMNS), "未設定 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(cfg["token"]),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return pd.DataFrame(columns=RECORD_COLUMNS), ""
        if resp.status_code != 200:
            return pd.DataFrame(columns=RECORD_COLUMNS), f"GitHub 讀取失敗：{resp.status_code} / {resp.text[:200]}"
        content = resp.json().get("content", "")
        if not content:
            return pd.DataFrame(columns=RECORD_COLUMNS), ""
        payload = json.loads(base64.b64decode(content).decode("utf-8"))
        if isinstance(payload, list):
            return _ensure_columns(pd.DataFrame(payload)), ""
        return pd.DataFrame(columns=RECORD_COLUMNS), ""
    except Exception as e:
        return pd.DataFrame(columns=RECORD_COLUMNS), f"GitHub 讀取例外：{e}"


def _write_records_to_github(df: pd.DataFrame) -> tuple[bool, str]:
    cfg = _github_config()
    if not cfg["token"]:
        return False, "未設定 GITHUB_TOKEN"
    sha, err = _get_file_sha()
    if err:
        return False, err
    body: dict[str, Any] = {
        "message": f"update macro trend records at {_now_text()}",
        "content": base64.b64encode(json.dumps(_ensure_columns(df).to_dict(orient="records"), ensure_ascii=False, indent=2).encode("utf-8")).decode("utf-8"),
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha
    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(cfg["token"]),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已同步 GitHub：{cfg['path']}"
        return False, f"GitHub 寫入失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return False, f"GitHub 寫入例外：{e}"


def _save_state_df(df: pd.DataFrame):
    st.session_state[_k("records_df")] = _ensure_columns(df)
    st.session_state[_k("saved_at")] = _now_text()


def _get_state_df() -> pd.DataFrame:
    df = st.session_state.get(_k("records_df"))
    if isinstance(df, pd.DataFrame):
        return _ensure_columns(df)
    return pd.DataFrame(columns=RECORD_COLUMNS)


@st.cache_data(ttl=900, show_spinner=False)
def _iter_recent_dates(pred_date_text: str, lookback_days: int = 10) -> list[pd.Timestamp]:
    dt = pd.to_datetime(pred_date_text, errors="coerce")
    if pd.isna(dt):
        dt = pd.Timestamp.today().normalize()
    return [dt - pd.Timedelta(days=i) for i in range(lookback_days + 1)]


@st.cache_data(ttl=900, show_spinner=False)
def _fetch_stooq(symbol: str, pred_date_text: str) -> dict[str, Any]:
    pred_dt = pd.to_datetime(pred_date_text, errors="coerce")
    if pd.isna(pred_dt):
        pred_dt = pd.Timestamp.today().normalize()
    start_dt = pred_dt - pd.Timedelta(days=80)

    def _finalize_df(df: pd.DataFrame, source_name: str) -> dict[str, Any]:
        if df is None or df.empty:
            return {}
        x = df.copy()
        x.columns = [str(c).strip().lower() for c in x.columns]
        if "date" not in x.columns:
            return {}
        x["date"] = pd.to_datetime(x["date"], errors="coerce")
        for c in ["open", "high", "low", "close", "volume"]:
            if c in x.columns:
                x[c] = pd.to_numeric(x[c], errors="coerce")
        x = x.dropna(subset=["date"]).sort_values("date")
        x = x[(x["date"] >= start_dt) & (x["date"] <= pred_dt)]
        if x.empty:
            return {}
        valid_close = x.dropna(subset=["close"]).copy()
        if valid_close.empty:
            return {}
        row = valid_close.iloc[-1]
        prev_close = valid_close.iloc[-2]["close"] if len(valid_close) >= 2 else None
        close = _safe_float(row.get("close"))
        pct = ((close - prev_close) / prev_close * 100) if (close is not None and prev_close not in [None, 0]) else None
        ma5 = valid_close["close"].tail(5).mean() if "close" in valid_close.columns else None
        ma20 = valid_close["close"].tail(20).mean() if "close" in valid_close.columns else None
        vol = _safe_float(row.get("volume"))
        vol_ma20 = None
        vol_ratio20 = None
        if "volume" in valid_close.columns:
            vol_series = pd.to_numeric(valid_close["volume"], errors="coerce").dropna()
            if not vol_series.empty:
                vol_ma20 = vol_series.tail(20).mean()
                if vol not in [None, 0] and vol_ma20 not in [None, 0]:
                    vol_ratio20 = vol / vol_ma20
        return {
            "date": pd.to_datetime(row.get("date")).strftime("%Y-%m-%d") if row.get("date") is not None else "",
            "open": _safe_float(row.get("open")),
            "high": _safe_float(row.get("high")),
            "low": _safe_float(row.get("low")),
            "close": close,
            "volume": vol,
            "vol_ma20": _safe_float(vol_ma20),
            "vol_ratio20": _safe_float(vol_ratio20),
            "pct": pct,
            "ma5": _safe_float(ma5),
            "ma20": _safe_float(ma20),
            "source": source_name,
        }

    try:
        yahoo_map = {
            "^twii": "^TWII",
            "^ixic": "^IXIC",
            "^sox": "^SOX",
            "^spx": "^GSPC",
            "^vix": "^VIX",
            "tsm.us": "TSM",
            "es.f": "ES=F",
            "nq.f": "NQ=F",
            "usdtwd": "TWD=X",
        }
        yahoo_symbol = yahoo_map.get(_safe_str(symbol).lower(), _safe_str(symbol))
        period1 = int((start_dt - pd.Timedelta(days=5)).timestamp())
        period2 = int((pred_dt + pd.Timedelta(days=2)).timestamp())
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(yahoo_symbol, safe='^=')}"
        resp = requests.get(
            url,
            params={
                "period1": period1,
                "period2": period2,
                "interval": "1d",
                "includePrePost": "false",
                "events": "div,splits",
            },
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=5,
        )
        data = resp.json()
        result = (((data or {}).get("chart") or {}).get("result") or [{}])[0]
        timestamps = result.get("timestamp") or []
        quote = (((result.get("indicators") or {}).get("quote") or [{}])[0]) or {}
        if timestamps and quote:
            df = pd.DataFrame({
                "date": pd.to_datetime(timestamps, unit="s", utc=True).tz_convert("Asia/Taipei").tz_localize(None),
                "open": quote.get("open", []),
                "high": quote.get("high", []),
                "low": quote.get("low", []),
                "close": quote.get("close", []),
                "volume": quote.get("volume", []),
            })
            parsed = _finalize_df(df, "yahoo")
            if parsed:
                return parsed
    except Exception:
        pass

    try:
        url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
        df = pd.read_csv(url)
        parsed = _finalize_df(df, "stooq")
        if parsed:
            return parsed
    except Exception:
        pass
    return {}

def _search_news_headlines(pred_date_text: str, max_items: int = 8) -> list[dict[str, str]]:
    # 輕量新聞：Google News RSS 關鍵字搜尋
    pred_dt = pd.to_datetime(pred_date_text, errors="coerce")
    if pd.isna(pred_dt):
        pred_dt = pd.Timestamp.today()
    ymd = pred_dt.strftime("%Y-%m-%d")
    keywords = [
        "Taiwan stock market",
        "NASDAQ semiconductor",
        "war tariff chip export",
        "TSMC Nvidia Apple Microsoft earnings",
    ]
    rows: list[dict[str, str]] = []
    for kw in keywords:
        url = f"https://news.google.com/rss/search?q={requests.utils.quote(kw + ' after:' + ymd)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
        try:
            resp = requests.get(url, timeout=5)
            text = resp.text
            items = text.split("<item>")[1:3]
            for it in items:
                title = it.split("<title>", 1)[1].split("</title>", 1)[0] if "<title>" in it else ""
                pub = it.split("<pubDate>", 1)[1].split("</pubDate>", 1)[0] if "<pubDate>" in it else ""
                link = it.split("<link>", 1)[1].split("</link>", 1)[0] if "<link>" in it else ""
                rows.append({"title": title.replace("<![CDATA[", "").replace("]]>", ""), "pubDate": pub, "link": link})
        except Exception:
            pass
    unique = []
    seen = set()
    for r in rows:
        key = _safe_str(r.get("title"))
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(r)
    return unique[:max_items]


def _news_risk_score(news_rows: list[dict[str, str]]) -> tuple[float, str, str]:
    if not news_rows:
        return 0.0, "新聞平淡，未見重大風險關鍵詞", "無明顯新聞衝擊"
    negative_words = {
        "war": 4.0, "missile": 4.0, "attack": 4.0, "tariff": 3.0, "sanction": 3.5,
        "crash": 3.0, "slump": 2.5, "ban": 3.0, "restrict": 2.8, "inflation": 2.0,
        "recession": 3.0, "downgrade": 2.5, "probe": 2.0, "earthquake": 2.5,
        "strike": 2.2, "關稅": 3.0, "戰爭": 4.0, "制裁": 3.5, "禁令": 3.0, "衝突": 3.0,
    }
    positive_words = {
        "beat": 2.2, "surge": 2.4, "growth": 1.8, "rebound": 2.0, "upgrade": 1.8,
        "expand": 1.5, "AI": 1.2, "strong": 1.5, "record": 1.8, "訂單": 1.8, "成長": 1.5,
    }
    risk = 0.0
    positives = 0.0
    tags = []
    for row in news_rows:
        title = _safe_str(row.get("title")).lower()
        for w, score in negative_words.items():
            if w.lower() in title:
                risk += score
                tags.append(f"風險:{w}")
        for w, score in positive_words.items():
            if w.lower() in title:
                positives += score
                tags.append(f"偏多:{w}")
    net = risk - positives * 0.6
    logic = "、".join(tags[:8]) if tags else "新聞未出現極端關鍵詞"
    main_risk = "國際新聞風險偏高" if net >= 4 else ("國際新聞中性" if net > 0 else "國際新聞偏正向")
    return max(-5.0, min(10.0, net)), logic, main_risk


def _score_from_pct(pct: float | None, scale: float = 2.0, cap: float = 10.0) -> float:
    if pct is None:
        return 0.0
    return max(-cap, min(cap, pct * scale))


@st.cache_data(ttl=900, show_spinner=False)
def _fetch_json(url: str, timeout: int = 5) -> Any:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def _tw_now() -> datetime:
    """Taiwan local time; Streamlit Cloud server may use UTC."""
    return datetime.utcnow() + timedelta(hours=8)


def _num_tw(v: Any):
    s = _safe_str(v).replace(",", "").replace("+", "").replace("%", "")
    if not s or s in {"-", "--", "None", "nan"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _macro_close_cache_path() -> Path:
    return Path("macro_market_close_cache.json")


def _read_macro_close_cache() -> dict[str, Any]:
    try:
        p = _macro_close_cache_path()
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}


def _write_macro_close_cache(data: dict[str, Any]):
    try:
        _macro_close_cache_path().write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass


@st.cache_data(ttl=20, show_spinner=False)
def _fetch_twse_realtime_taiex() -> dict[str, Any]:
    """
    即時加權指數：盤中優先使用 TWSE MIS。
    若晚上 / 收盤後 z 可能為 '-'，會交給收盤資料函式處理。
    """
    now_ms = int(_tw_now().timestamp() * 1000)
    url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
    params = {
        "ex_ch": "tse_t00.tw",
        "json": "1",
        "delay": "0",
        "_": now_ms,
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?stock=t00",
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=2.5)
        if r.status_code != 200:
            return {"ok": False, "source": "TWSE MIS", "error": f"HTTP {r.status_code}"}
        data = r.json()
        arr = data.get("msgArray") or []
        if not arr:
            return {"ok": False, "source": "TWSE MIS", "error": "msgArray empty"}
        row = arr[0]
        current = _num_tw(row.get("z"))
        y_close = _num_tw(row.get("y"))
        open_v = _num_tw(row.get("o"))
        high_v = _num_tw(row.get("h"))
        low_v = _num_tw(row.get("l"))
        if current is None:
            # 有些非盤中時間 z 會是 '-'，若有 h/l/y 不直接當即時，交給收盤 API。
            return {"ok": False, "source": "TWSE MIS", "error": "no realtime price", "prev_close": y_close}
        pct = ((current - y_close) / y_close * 100) if y_close not in [None, 0] else None
        date_text = _safe_str(row.get("d"))
        used_date = ""
        if len(date_text) == 8:
            used_date = f"{date_text[:4]}-{date_text[4:6]}-{date_text[6:8]}"
        return {
            "ok": True,
            "source": "TWSE MIS 即時",
            "date": used_date or _tw_now().strftime("%Y-%m-%d"),
            "used_date": used_date or _tw_now().strftime("%Y-%m-%d"),
            "close": current,
            "open": open_v,
            "high": high_v,
            "low": low_v,
            "prev_close": y_close,
            "pct": pct,
            "time": _safe_str(row.get("t")),
            "is_realtime": True,
            "raw_name": _safe_str(row.get("n")),
        }
    except Exception as e:
        return {"ok": False, "source": "TWSE MIS", "error": str(e)}


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_twse_after_close_taiex(date_text: str) -> dict[str, Any]:
    """
    收盤後加權指數：晚上 / 非盤中優先抓 TWSE MI_INDEX。
    會回寫 macro_market_close_cache.json，避免晚上重複等待外部端點。
    """
    dt = pd.to_datetime(date_text, errors="coerce")
    if pd.isna(dt):
        dt = pd.Timestamp(_tw_now().date())
    ymd = dt.strftime("%Y%m%d")
    cache = _read_macro_close_cache()
    if ymd in cache and isinstance(cache.get(ymd), dict):
        row = dict(cache[ymd])
        row["source"] = _safe_str(row.get("source")) or "macro_market_close_cache"
        return row

    urls = [
        f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={ymd}&type=IND&response=json",
        f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={ymd}&type=IND",
    ]

    def _walk_rows(obj):
        if isinstance(obj, list):
            if obj and all(not isinstance(x, (list, dict)) for x in obj):
                yield obj
            for x in obj:
                yield from _walk_rows(x)
        elif isinstance(obj, dict):
            for v in obj.values():
                yield from _walk_rows(v)

    for url in urls:
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=3.5)
            if r.status_code != 200:
                continue
            data = r.json()
            best = None
            for row in _walk_rows(data):
                joined = " ".join(_safe_str(x) for x in row)
                if "發行量加權股價指數" in joined or "TAIEX" in joined.upper():
                    nums = [_num_tw(x) for x in row]
                    nums = [x for x in nums if x is not None]
                    if nums:
                        # 通常第一個有效數字是收盤指數；後面可能是漲跌點/百分比。
                        close = nums[0]
                        pct = None
                        if len(nums) >= 3 and abs(nums[-1]) < 20:
                            pct = nums[-1]
                        best = {
                            "ok": True,
                            "source": "TWSE 收盤",
                            "date": dt.strftime("%Y-%m-%d"),
                            "used_date": dt.strftime("%Y-%m-%d"),
                            "close": close,
                            "pct": pct,
                            "is_realtime": False,
                            "raw_name": "發行量加權股價指數",
                        }
                        break
            if best:
                cache[ymd] = best
                _write_macro_close_cache(cache)
                return best
        except Exception:
            continue

    return {"ok": False, "source": "TWSE 收盤", "date": dt.strftime("%Y-%m-%d"), "used_date": "", "close": None, "pct": None, "is_realtime": False}


def _fetch_taiex_realtime_or_close(date_text: str) -> dict[str, Any]:
    """
    大盤主資料源：
    - 今日盤中：TWSE MIS 即時
    - 收盤後 / 晚上：TWSE MI_INDEX 收盤
    - 失敗時：交回原 yahoo/stooq fallback
    """
    dt = pd.to_datetime(date_text, errors="coerce")
    if pd.isna(dt):
        dt = pd.Timestamp(_tw_now().date())
    tw_today = pd.Timestamp(_tw_now().date())
    now = _tw_now()
    is_today = dt.strftime("%Y-%m-%d") == tw_today.strftime("%Y-%m-%d")
    # 台股常規：09:00~13:35。13:35 後先嘗試收盤資料；若交易所尚未發布，再 fallback。
    if is_today and (9 <= now.hour < 14):
        rt = _fetch_twse_realtime_taiex()
        if rt.get("ok"):
            return rt
    close = _fetch_twse_after_close_taiex(dt.strftime("%Y-%m-%d"))
    if close.get("ok"):
        return close
    if is_today:
        rt = _fetch_twse_realtime_taiex()
        if rt.get("ok"):
            return rt
    return {"ok": False, "source": "TWSE 即時/收盤 fallback", "date": dt.strftime("%Y-%m-%d"), "used_date": "", "close": None, "pct": None, "is_realtime": False}


def _apply_taiex_override(twii: dict[str, Any], taiex_now: dict[str, Any]) -> dict[str, Any]:
    out = dict(twii or {})
    if not isinstance(taiex_now, dict) or taiex_now.get("close") is None:
        return out
    out["close"] = _safe_float(taiex_now.get("close"))
    out["pct"] = _safe_float(taiex_now.get("pct"), _safe_float(out.get("pct")))
    out["date"] = _safe_str(taiex_now.get("date")) or _safe_str(out.get("date"))
    out["used_date"] = _safe_str(taiex_now.get("used_date")) or _safe_str(out.get("used_date"))
    out["source"] = _safe_str(taiex_now.get("source")) or _safe_str(out.get("source"))
    for k in ["open", "high", "low", "prev_close"]:
        if taiex_now.get(k) is not None:
            out[k] = _safe_float(taiex_now.get(k))
    out["is_realtime"] = bool(taiex_now.get("is_realtime"))
    return out


def _to_roc_date(dt: pd.Timestamp) -> str:
    return f"{dt.year-1911}/{dt.month:02d}/{dt.day:02d}"


def _extract_num(text: Any, div: float = 1.0):
    s = _safe_str(text).replace(',', '').replace('+', '')
    if not s:
        return None
    try:
        return float(s) / div
    except Exception:
        return None




def _normalize_stooq_symbol(symbol: str) -> str:
    s = _safe_str(symbol).strip()
    mapping = {
        "TSM": "TSM",
        "^TWII": "^TWII",
        "^IXIC": "^IXIC",
        "^SOX": "^SOX",
        "^GSPC": "^GSPC",
        "ES.F": "ES.F",
        "NQ.F": "NQ.F",
        "^VIX": "^VIX",
        "USDTWD": "USDTWD",
    }
    return mapping.get(s, s)

def _price_on_or_before(symbol: str, pred_date_text: str, lookback_days: int = 15) -> dict[str, Any]:
    normalized = _normalize_stooq_symbol(symbol)
    data = _fetch_stooq(normalized, pred_date_text)
    if data:
        data = dict(data)
        data["used_date"] = _safe_str(data.get("date"))
        data["symbol"] = normalized
        return data

    pred_dt = pd.to_datetime(pred_date_text, errors="coerce")
    if pd.isna(pred_dt):
        pred_dt = pd.Timestamp.today().normalize()

    for i in range(1, lookback_days + 1):
        dt_text = (pred_dt - pd.Timedelta(days=i)).strftime("%Y-%m-%d")
        data = _fetch_stooq(normalized, dt_text)
        if data:
            data = dict(data)
            data["used_date"] = _safe_str(data.get("date"))
            data["symbol"] = normalized
            return data

    return {"date": "", "used_date": "", "pct": None, "close": None, "symbol": normalized, "source": "fallback"}


@st.cache_data(ttl=1800, show_spinner=False)
def _fetch_twse_institutional(pred_date_text: str) -> dict[str, Any]:
    out = {"foreign": None, "total3": None, "source": "fallback", "used_date": ""}
    for dt in _iter_recent_dates(pred_date_text, 8):
        ymd = dt.strftime('%Y%m%d')
        urls = [
            f'https://www.twse.com.tw/rwd/zh/fund/BFI82U?dayDate={ymd}&type=day&response=json',
            f'https://www.twse.com.tw/fund/BFI82U?dayDate={ymd}&type=day&response=json',
        ]
        data = None
        for u in urls:
            data = _fetch_json(u)
            if data and data.get('data'):
                break
        if not data or 'data' not in data:
            continue

        foreign = None
        total3 = None
        for row in data.get('data', []):
            if not isinstance(row, list) or len(row) < 2:
                continue
            label = _safe_str(row[0])
            vals = [_extract_num(x, 100000000) for x in row[1:]]
            nums = [x for x in vals if x is not None]
            if not nums:
                continue
            if ('外資' in label or '陸資' in label) and '自營商' not in label and foreign is None:
                foreign = nums[-1]
            if ('三大法人' in label or label == '合計') and total3 is None:
                total3 = nums[-1]
        if foreign is not None or total3 is not None:
            out.update({"foreign": foreign, "total3": total3, "source": 'twse', "used_date": dt.strftime('%Y-%m-%d')})
            return out
    # fallback：用 ADR / 美股夜盤代理估算，避免頁面只剩空殼
    try:
        tsm = _price_on_or_before('TSM', pred_date_text, 15)
        sox = _price_on_or_before('^SOX', pred_date_text, 15)
        ixic = _price_on_or_before('^IXIC', pred_date_text, 15)
        spx = _price_on_or_before('^GSPC', pred_date_text, 15)
        pct_mix = float(tsm.get('pct', 0) or 0) * 0.40 + float(sox.get('pct', 0) or 0) * 0.25 + float(ixic.get('pct', 0) or 0) * 0.20 + float(spx.get('pct', 0) or 0) * 0.15
        out['foreign'] = round(pct_mix * 28.0, 2)
        out['total3'] = round(pct_mix * 36.0, 2)
        out['source'] = 'fallback_est'
        out['used_date'] = _safe_str(tsm.get('used_date') or sox.get('used_date') or ixic.get('used_date') or spx.get('used_date') or pred_date_text)
    except Exception:
        pass
    return out


def _fetch_taifex_sentiment(pred_date_text: str) -> dict[str, Any]:
    out = {"foreign_fut_net": None, "pcr": None, "source": 'fallback', "used_date": ""}
    import re
    for dt in _iter_recent_dates(pred_date_text, 8):
        date_slash = dt.strftime('%Y/%m/%d')
        candidates = [
            f'https://www.taifex.com.tw/cht/3/pcRatio?queryStartDate={date_slash}&queryEndDate={date_slash}',
            f'https://www.taifex.com.tw/cht/3/pcRatio?queryStartDate={date_slash}&queryEndDate={date_slash}&commodityId=TXO',
        ]
        for u in candidates:
            try:
                txt = requests.get(u, timeout=5, headers={"User-Agent": "Mozilla/5.0"}).text
                nums = re.findall(r'([0-9]+\.[0-9]+)', txt)
                hit = None
                for x in nums:
                    v = float(x)
                    if 0.4 <= v <= 2.5:
                        hit = v
                        break
                if hit is not None:
                    out.update({'pcr': hit, 'source': 'taifex_html', 'used_date': dt.strftime('%Y-%m-%d')})
                    return out
            except Exception:
                pass
    try:
        es = _price_on_or_before('ES.F', pred_date_text, 15)
        nq = _price_on_or_before('NQ.F', pred_date_text, 15)
        mix = float(es.get('pct', 0) or 0) * 0.45 + float(nq.get('pct', 0) or 0) * 0.55
        est = round(max(0.7, min(1.3, 1.0 + (-mix / 20.0))), 2)
        out.update({'pcr': est, 'source': 'fallback_est', 'used_date': _safe_str(es.get('used_date') or nq.get('used_date') or pred_date_text)})
    except Exception:
        pass
    return out


def _fetch_twse_margin(pred_date_text: str) -> dict[str, Any]:
    out = {"margin_change": None, "short_change": None, "source": 'fallback', "used_date": ""}
    for dt in _iter_recent_dates(pred_date_text, 8):
        ymd = dt.strftime('%Y%m%d')
        urls = [
            f'https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date={ymd}&selectType=MS&response=json',
            f'https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date={ymd}&selectType=MS',
        ]
        for u in urls:
            data = _fetch_json(u)
            if not data or 'data' not in data:
                continue
            try:
                rows = data.get('data', [])
                cand = None
                for row in reversed(rows):
                    if not isinstance(row, list):
                        continue
                    nums = [_extract_num(x) for x in row]
                    vals = [x for x in nums if x is not None]
                    if len(vals) >= 2:
                        cand = vals
                        break
                if cand:
                    out['margin_change'] = cand[-2] / 100000000 if abs(cand[-2]) > 10000 else cand[-2]
                    out['short_change'] = cand[-1]
                    out['source'] = 'twse_margin'
                    out['used_date'] = dt.strftime('%Y-%m-%d')
                    return out
            except Exception:
                pass
    # fallback：用 ADR / 美股夜盤代理估算融資券變化
    try:
        tsm = _price_on_or_before('TSM', pred_date_text, 15)
        es = _price_on_or_before('ES.F', pred_date_text, 15)
        nq = _price_on_or_before('NQ.F', pred_date_text, 15)
        mix = float(tsm.get('pct', 0) or 0) * 0.50 + float(es.get('pct', 0) or 0) * 0.25 + float(nq.get('pct', 0) or 0) * 0.25
        out['margin_change'] = round(-mix * 8.0, 2)
        out['short_change'] = round(max(-30.0, min(30.0, -mix * 6.0)), 2)
        out['source'] = 'fallback_est'
        out['used_date'] = _safe_str(tsm.get('used_date') or es.get('used_date') or nq.get('used_date') or pred_date_text)
    except Exception:
        pass
    return out


def _derive_chip_scores(inst: dict[str, Any], futopt: dict[str, Any], margin: dict[str, Any], adr_pct: float, es_pct: float, nq_pct: float) -> dict[str, Any]:
    foreign_amt = _safe_float(inst.get('foreign'))
    total3_amt = _safe_float(inst.get('total3'))
    foreign_fut_net = _safe_float(futopt.get('foreign_fut_net'))
    pcr = _safe_float(futopt.get('pcr'))
    margin_change = _safe_float(margin.get('margin_change'))
    short_change = _safe_float(margin.get('short_change'))

    if foreign_amt is None:
        foreign_amt = round((float(adr_pct or 0) * 16.0) + (float(es_pct or 0) * 10.0) + (float(nq_pct or 0) * 10.0), 2)
    if total3_amt is None:
        total3_amt = round(foreign_amt * 1.25, 2)
    if pcr is None:
        pcr = round(max(0.7, min(1.3, 1.0 + (-(float(es_pct or 0) + float(nq_pct or 0)) / 20.0))), 2)
    if margin_change is None:
        margin_change = round(-(float(adr_pct or 0) * 3.0 + float(nq_pct or 0) * 2.0), 2)
    if short_change is None:
        short_change = round(max(-30.0, min(30.0, -(float(es_pct or 0) * 2.0 + float(nq_pct or 0) * 1.5))), 2)

    foreign_score = 0.0
    if foreign_amt is not None:
        foreign_score += max(-8, min(8, foreign_amt / 35.0))
    else:
        foreign_score += _score_from_pct(adr_pct, 1.6, 5)
    if total3_amt is not None:
        foreign_score += max(-5, min(5, total3_amt / 45.0))

    futures_score = 0.0
    if foreign_fut_net is not None:
        futures_score += max(-7, min(7, foreign_fut_net / 12000.0))
    else:
        futures_score += _score_from_pct(es_pct, 1.6, 4) + _score_from_pct(nq_pct, 1.8, 4)
    if pcr is not None:
        if pcr >= 1.2:
            futures_score += 1.5
        elif pcr <= 0.8:
            futures_score -= 1.5

    margin_score = 0.0
    if margin_change is not None:
        margin_score += max(-4, min(4, -margin_change / 12.0))
    if short_change is not None:
        margin_score += max(-3, min(3, short_change / 15000.0))

    return {
        'foreign_amt': foreign_amt, 'total3_amt': total3_amt, 'foreign_fut_net': foreign_fut_net, 'pcr': pcr,
        'margin_change': margin_change, 'short_change': short_change,
        'foreign_score': foreign_score, 'futures_score': futures_score, 'margin_score': margin_score,
        'inst_used_date': _safe_str(inst.get('used_date')),
        'futopt_used_date': _safe_str(futopt.get('used_date')),
        'margin_used_date': _safe_str(margin.get('used_date')),
    }


def _derive_sector_tags(sox_pct: float, nas_pct: float, adr_pct: float, spx_pct: float) -> tuple[str, str, float]:
    strong = []
    weak = []
    score = 0.0
    if sox_pct >= 1.0:
        strong.append('半導體')
        score += 3.0
    elif sox_pct <= -1.0:
        weak.append('半導體')
        score -= 3.0
    if nas_pct >= 0.8:
        strong.append('AI/科技')
        score += 2.4
    elif nas_pct <= -0.8:
        weak.append('AI/科技')
        score -= 2.4
    if adr_pct >= 1.0:
        strong.append('權值電子')
        score += 2.8
    elif adr_pct <= -1.0:
        weak.append('權值電子')
        score -= 2.8
    if spx_pct <= -0.8 and not weak:
        weak.append('廣泛市場')
        score -= 1.2
    if not strong:
        strong.append('無明顯強勢')
    if not weak:
        weak.append('無明顯弱勢')
    return ' / '.join(strong), ' / '.join(weak), score


def _derive_event_list(news_rows: list[dict[str, str]]) -> str:
    hits = []
    keys = ['war', 'tariff', 'sanction', 'nvidia', 'tsmc', 'apple', 'microsoft', 'fed', 'cpi', '關稅', '戰爭', '制裁', '法說']
    for row in news_rows:
        t = _safe_str(row.get('title'))
        tl = t.lower()
        if any(k.lower() in tl for k in keys):
            hits.append(t[:50])
    return '｜'.join(hits[:5]) if hits else '無重大事件關鍵字'




# =========================================================
# 投顧級大盤參考因子：市場廣度 / 權值 / 量價 / 推薦同步
# =========================================================
HEAVYWEIGHT_STOCKS = [
    {"股票代號": "2330", "股票名稱": "台積電", "市場別": "上市"},
    {"股票代號": "2317", "股票名稱": "鴻海", "市場別": "上市"},
    {"股票代號": "2454", "股票名稱": "聯發科", "市場別": "上市"},
    {"股票代號": "2382", "股票名稱": "廣達", "市場別": "上市"},
    {"股票代號": "2308", "股票名稱": "台達電", "市場別": "上市"},
    {"股票代號": "2881", "股票名稱": "富邦金", "市場別": "上市"},
    {"股票代號": "2882", "股票名稱": "國泰金", "市場別": "上市"},
]

RECOMMENDATION_JSON_FILES = [
    "godpick_latest_recommendations.json",
    "godpick_recommend_list.json",
    "godpick_records.json",
]


def _clip_score(v: Any, low: float = 0.0, high: float = 100.0) -> float:
    x = _safe_float(v, 0) or 0
    return float(max(low, min(high, x)))


def _load_recommendation_rows() -> list[dict[str, Any]]:
    base_dir = Path(__file__).resolve().parent.parent
    rows: list[dict[str, Any]] = []
    for fn in RECOMMENDATION_JSON_FILES:
        p = base_dir / fn
        if not p.exists():
            continue
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, list):
            data_rows = payload
        elif isinstance(payload, dict):
            if isinstance(payload.get("records"), list):
                data_rows = payload.get("records", [])
            elif isinstance(payload.get("data"), list):
                data_rows = payload.get("data", [])
            elif isinstance(payload.get("items"), list):
                data_rows = payload.get("items", [])
            else:
                data_rows = []
        else:
            data_rows = []
        for r in data_rows:
            if isinstance(r, dict):
                r = dict(r)
                r["_來源檔"] = fn
                rows.append(r)
    return rows


def _stock_snapshot_for_macro(code: str, name: str, market: str) -> dict[str, Any]:
    """輕量個股快照，供市場廣度 / 權值股支撐使用。"""
    out = {"code": code, "name": name, "market": market, "price": None, "pct": None, "ma20": None, "ma60": None, "high20": None, "low20": None, "source": ""}
    if get_realtime_stock_info is not None:
        try:
            info = get_realtime_stock_info(code, name, market, refresh_token="macro_advisor")
            out["price"] = _safe_float(info.get("price"))
            out["pct"] = _safe_float(info.get("pct_chg"), _safe_float(info.get("change_percent"), _safe_float(info.get("漲跌幅"))))
            out["source"] = _safe_str(info.get("price_source")) or "realtime"
        except Exception:
            pass

    if get_history_data is not None:
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=120)
            try:
                df = get_history_data(stock_no=code, stock_name=name, market_type=market, start_date=start_date, end_date=end_date)
            except TypeError:
                try:
                    df = get_history_data(stock_no=code, stock_name=name, market_type=market, start_dt=start_date, end_dt=end_date)
                except Exception:
                    df = get_history_data(code=code, start_date=start_date, end_date=end_date)
            if isinstance(df, pd.DataFrame) and not df.empty:
                temp = df.copy()
                if "收盤價" not in temp.columns:
                    for c in temp.columns:
                        if str(c).lower() in {"close", "收盤價"}:
                            temp = temp.rename(columns={c: "收盤價"})
                            break
                if "最高價" not in temp.columns:
                    for c in temp.columns:
                        if str(c).lower() in {"high", "最高價"}:
                            temp = temp.rename(columns={c: "最高價"})
                            break
                if "最低價" not in temp.columns:
                    for c in temp.columns:
                        if str(c).lower() in {"low", "最低價"}:
                            temp = temp.rename(columns={c: "最低價"})
                            break
                if "收盤價" in temp.columns:
                    temp["收盤價"] = pd.to_numeric(temp["收盤價"], errors="coerce")
                    temp = temp.dropna(subset=["收盤價"]).copy()
                    if not temp.empty:
                        close = temp["收盤價"]
                        if out["price"] is None:
                            out["price"] = _safe_float(close.iloc[-1])
                        if out["pct"] is None and len(close) >= 2 and close.iloc[-2] not in [0, None]:
                            out["pct"] = _safe_float((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100)
                        out["ma20"] = _safe_float(close.tail(20).mean()) if len(close) >= 20 else None
                        out["ma60"] = _safe_float(close.tail(60).mean()) if len(close) >= 60 else None
                        if "最高價" in temp.columns:
                            high = pd.to_numeric(temp["最高價"], errors="coerce").dropna()
                            out["high20"] = _safe_float(high.tail(20).max()) if not high.empty else None
                        if "最低價" in temp.columns:
                            low = pd.to_numeric(temp["最低價"], errors="coerce").dropna()
                            out["low20"] = _safe_float(low.tail(20).min()) if not low.empty else None
                        out["source"] = out["source"] or "history"
        except Exception:
            pass
    return out


@st.cache_data(ttl=1800, show_spinner=False)
def _build_market_breadth_snapshot(max_scan: int = 180) -> dict[str, Any]:
    """市場廣度：用股票主檔抽樣掃描，避免整個系統卡死。"""
    rows: list[dict[str, str]] = []
    if load_stock_master is not None:
        try:
            master = load_stock_master()
            if isinstance(master, pd.DataFrame) and not master.empty:
                code_col = "股票代號" if "股票代號" in master.columns else ("code" if "code" in master.columns else "")
                name_col = "股票名稱" if "股票名稱" in master.columns else ("name" if "name" in master.columns else "")
                market_col = "市場別" if "市場別" in master.columns else ("market" if "market" in master.columns else "")
                if code_col:
                    temp = master.copy()
                    if market_col:
                        temp = temp[temp[market_col].astype(str).isin(["上市", "上櫃"])]
                    for _, r in temp.head(max_scan).iterrows():
                        rows.append({
                            "股票代號": _safe_str(r.get(code_col)),
                            "股票名稱": _safe_str(r.get(name_col)) if name_col else _safe_str(r.get(code_col)),
                            "市場別": _safe_str(r.get(market_col)) if market_col else "上市",
                        })
        except Exception:
            rows = []

    # 若主檔不可用，退回用自選股樣本
    if not rows and get_normalized_watchlist is not None:
        try:
            data = get_normalized_watchlist()
            if isinstance(data, dict):
                for _, items in data.items():
                    if not isinstance(items, list):
                        continue
                    for item in items:
                        if isinstance(item, dict):
                            rows.append({
                                "股票代號": _safe_str(item.get("code")),
                                "股票名稱": _safe_str(item.get("name")) or _safe_str(item.get("code")),
                                "市場別": _safe_str(item.get("market")) or "上市",
                            })
        except Exception:
            pass
        rows = rows[:max_scan]

    sample_n = 0
    up_n = down_n = ma20_n = ma60_n = high20_n = low20_n = 0
    for r in rows:
        code = _safe_str(r.get("股票代號"))
        if not code:
            continue
        snap = _stock_snapshot_for_macro(code, _safe_str(r.get("股票名稱")) or code, _safe_str(r.get("市場別")) or "上市")
        price = _safe_float(snap.get("price"))
        if price is None:
            continue
        sample_n += 1
        pct = _safe_float(snap.get("pct"), 0) or 0
        if pct > 0:
            up_n += 1
        elif pct < 0:
            down_n += 1
        ma20 = _safe_float(snap.get("ma20"))
        ma60 = _safe_float(snap.get("ma60"))
        high20 = _safe_float(snap.get("high20"))
        low20 = _safe_float(snap.get("low20"))
        if ma20 not in [None, 0] and price >= ma20:
            ma20_n += 1
        if ma60 not in [None, 0] and price >= ma60:
            ma60_n += 1
        if high20 not in [None, 0] and price >= high20:
            high20_n += 1
        if low20 not in [None, 0] and price <= low20:
            low20_n += 1

    if sample_n <= 0:
        return {
            "市場廣度分數": 50.0,
            "廣度樣本數": 0,
            "上漲家數": 0,
            "下跌家數": 0,
            "站上MA20比例": 0.0,
            "站上MA60比例": 0.0,
            "創20日新高家數": 0,
            "創20日新低家數": 0,
            "市場廣度摘要": "市場廣度資料不足，降級為中性參考",
        }

    up_ratio = up_n / sample_n * 100
    ma20_ratio = ma20_n / sample_n * 100
    ma60_ratio = ma60_n / sample_n * 100
    high_low_balance = (high20_n - low20_n) / sample_n * 100
    score = 35 + up_ratio * 0.25 + ma20_ratio * 0.25 + ma60_ratio * 0.15 + high_low_balance * 0.25
    score = _clip_score(score)

    return {
        "市場廣度分數": round(score, 2),
        "廣度樣本數": sample_n,
        "上漲家數": up_n,
        "下跌家數": down_n,
        "站上MA20比例": round(ma20_ratio, 2),
        "站上MA60比例": round(ma60_ratio, 2),
        "創20日新高家數": high20_n,
        "創20日新低家數": low20_n,
        "市場廣度摘要": f"樣本{sample_n}檔｜上漲{up_n} / 下跌{down_n}｜MA20上方{ma20_ratio:.1f}%｜20日新高{high20_n}檔",
    }


@st.cache_data(ttl=900, show_spinner=False)
def _build_heavyweight_support_score() -> dict[str, Any]:
    rows = []
    for item in HEAVYWEIGHT_STOCKS:
        snap = _stock_snapshot_for_macro(item["股票代號"], item["股票名稱"], item["市場別"])
        price = _safe_float(snap.get("price"))
        pct = _safe_float(snap.get("pct"), 0) or 0
        ma20 = _safe_float(snap.get("ma20"))
        support = 0.0
        if pct > 0:
            support += min(12, pct * 3)
        elif pct < 0:
            support += max(-12, pct * 3)
        if price is not None and ma20 not in [None, 0]:
            support += 8 if price >= ma20 else -8
        rows.append({**item, **snap, "權值支撐估分": support})

    valid = [r for r in rows if _safe_float(r.get("price")) is not None]
    if not valid:
        return {"權值支撐分數": 50.0, "權值支撐摘要": "權值股資料不足", "權值股明細": rows}

    avg = sum(_safe_float(r.get("權值支撐估分"), 0) or 0 for r in valid) / len(valid)
    score = _clip_score(50 + avg * 2.2)
    strong = [f"{r['股票名稱']}({(_safe_float(r.get('pct'),0) or 0):.1f}%)" for r in valid if (_safe_float(r.get("權值支撐估分"), 0) or 0) > 3]
    weak = [f"{r['股票名稱']}({(_safe_float(r.get('pct'),0) or 0):.1f}%)" for r in valid if (_safe_float(r.get("權值支撐估分"), 0) or 0) < -3]
    return {
        "權值支撐分數": round(score, 2),
        "權值支撐摘要": f"支撐:{'、'.join(strong[:4]) or '無明顯'}｜拖累:{'、'.join(weak[:4]) or '無明顯'}",
        "權值股明細": rows,
    }


def _build_recommendation_sync_snapshot() -> dict[str, Any]:
    rows = _load_recommendation_rows()
    if not rows:
        return {
            "推薦同步分數": 50.0,
            "推薦強勢比例": 0.0,
            "推薦平均分數": 0.0,
            "推薦起漲比例": 0.0,
            "推薦同步摘要": "尚未讀到股神推薦清單，暫以中性處理",
        }

    scores = []
    burst_scores = []
    strong_count = 0
    burst_count = 0
    for r in rows:
        total = _safe_float(r.get("推薦總分"), _safe_float(r.get("股神決策分數")))
        burst = _safe_float(r.get("飆股起漲分數"), _safe_float(r.get("起漲前兆分數")))
        if total is not None:
            scores.append(total)
            if total >= 70:
                strong_count += 1
        if burst is not None:
            burst_scores.append(burst)
            if burst >= 68:
                burst_count += 1

    n = max(len(rows), 1)
    avg_score = sum(scores) / len(scores) if scores else 0.0
    strong_ratio = strong_count / n * 100
    burst_ratio = burst_count / n * 100
    score = _clip_score(avg_score * 0.45 + strong_ratio * 0.30 + burst_ratio * 0.25)
    return {
        "推薦同步分數": round(score, 2),
        "推薦強勢比例": round(strong_ratio, 2),
        "推薦平均分數": round(avg_score, 2),
        "推薦起漲比例": round(burst_ratio, 2),
        "推薦同步摘要": f"推薦{len(rows)}筆｜平均分{avg_score:.1f}｜強勢{strong_ratio:.1f}%｜起漲{burst_ratio:.1f}%",
    }


def _volume_price_confirm_score(twii: dict[str, Any]) -> tuple[float, str]:
    pct = _safe_float(twii.get("pct"), 0) or 0
    vol_ratio = _safe_float(twii.get("vol_ratio20"))
    score = 50.0
    notes = []

    if pct >= 1.0:
        score += 14
        notes.append("價強")
    elif pct >= 0.3:
        score += 7
        notes.append("小漲")
    elif pct <= -1.0:
        score -= 14
        notes.append("價弱")
    elif pct <= -0.3:
        score -= 7
        notes.append("小跌")

    if vol_ratio is not None:
        if pct > 0 and vol_ratio >= 1.15:
            score += 18
            notes.append("價漲量增")
        elif pct > 0 and vol_ratio < 0.85:
            score -= 8
            notes.append("價漲量縮")
        elif pct < 0 and vol_ratio >= 1.15:
            score -= 16
            notes.append("價跌量增")
        elif pct < 0 and vol_ratio < 0.85:
            score += 5
            notes.append("價跌量縮")
        notes.append(f"量比{vol_ratio:.2f}")
    else:
        notes.append("量能基準不足")

    return round(_clip_score(score), 2), "、".join(notes)


def _data_integrity_score(ctx: dict[str, Any]) -> tuple[float, str]:
    fields = [
        ctx.get("twii_date"),
        ctx.get("us_data_date"),
        ctx.get("night_data_date"),
        ctx.get("inst_used_date"),
        ctx.get("futopt_used_date"),
        ctx.get("margin_used_date"),
    ]
    hit = sum(1 for x in fields if _safe_str(x))
    score = hit / len(fields) * 100
    return round(score, 2), f"可用因子 {hit}/{len(fields)}"


def _reference_grade(score: float) -> str:
    if score >= 80:
        return "A｜可作主要參考"
    if score >= 65:
        return "B｜可作輔助參考"
    if score >= 50:
        return "C｜僅作風險濾網"
    return "D｜不建議作推薦依據"


def _operation_style(score: float, total_score: float, risk_level: str) -> str:
    if risk_level in {"極高", "高"} or score < 45:
        return "降低持股 / 嚴控風險"
    if score >= 80 and total_score >= 12:
        return "積極進攻"
    if score >= 65 and total_score >= 5:
        return "精選強勢股"
    if score >= 50:
        return "低接不追高"
    return "只看不做"


def _reference_weight(score: float, risk_level: str) -> tuple[str, str]:
    if risk_level in {"極高", "高"}:
        return "建議降權至 20% 以下", "風險等級偏高，大盤僅作風險控管，不宜作進攻依據"
    if score >= 80:
        return "建議權重 40%~55%", "資料完整且多數因子同向，可作主要參考"
    if score >= 65:
        return "建議權重 25%~40%", "可作輔助參考，但仍需個股條件確認"
    if score >= 50:
        return "建議權重 10%~25%", "方向不夠集中，只作風險濾網"
    return "建議權重 0%~10%", "模型參考性不足，避免用大盤推動推薦"


def _build_macro_reference_pack(ctx: dict[str, Any], top_score: float, risk_level: str) -> dict[str, Any]:
    breadth = _build_market_breadth_snapshot()
    heavy = _build_heavyweight_support_score()
    rec_sync = _build_recommendation_sync_snapshot()
    vol_score, vol_note = _volume_price_confirm_score(ctx.get("twii", {}))
    integrity, integrity_note = _data_integrity_score(ctx)

    sector_score_raw = _safe_float(ctx.get("sector_score"), 0) or 0
    sector_score = _clip_score(50 + sector_score_raw * 6)

    reference_score = (
        (_safe_float(breadth.get("市場廣度分數"), 50) or 50) * 0.22
        + sector_score * 0.16
        + vol_score * 0.14
        + (_safe_float(heavy.get("權值支撐分數"), 50) or 50) * 0.16
        + (_safe_float(rec_sync.get("推薦同步分數"), 50) or 50) * 0.20
        + integrity * 0.12
    )

    # 風險降級
    vix = _safe_float(ctx.get("vix_val"), 18) or 18
    news = _safe_float(ctx.get("news_score"), 0) or 0
    if risk_level in {"高", "極高"}:
        reference_score -= 8
    if vix >= 25:
        reference_score -= 6
    if news >= 5:
        reference_score -= 6

    reference_score = round(_clip_score(reference_score), 2)
    grade = _reference_grade(reference_score)
    weight, reason = _reference_weight(reference_score, risk_level)

    return {
        "大盤可參考分數": reference_score,
        "大盤參考等級": grade,
        "市場廣度分數": breadth.get("市場廣度分數"),
        "類股輪動分數": round(sector_score, 2),
        "量價確認分數": vol_score,
        "權值支撐分數": heavy.get("權值支撐分數"),
        "推薦同步分數": rec_sync.get("推薦同步分數"),
        "推薦加權建議": weight,
        "推薦降權原因": reason,
        "今日適合操作風格": _operation_style(reference_score, top_score, risk_level),
        "廣度樣本數": breadth.get("廣度樣本數"),
        "上漲家數": breadth.get("上漲家數"),
        "下跌家數": breadth.get("下跌家數"),
        "站上MA20比例": breadth.get("站上MA20比例"),
        "站上MA60比例": breadth.get("站上MA60比例"),
        "創20日新高家數": breadth.get("創20日新高家數"),
        "創20日新低家數": breadth.get("創20日新低家數"),
        "推薦強勢比例": rec_sync.get("推薦強勢比例"),
        "推薦平均分數": rec_sync.get("推薦平均分數"),
        "推薦起漲比例": rec_sync.get("推薦起漲比例"),
        "市場廣度摘要": breadth.get("市場廣度摘要"),
        "量價確認摘要": vol_note,
        "權值支撐摘要": heavy.get("權值支撐摘要"),
        "推薦同步摘要": rec_sync.get("推薦同步摘要"),
        "資料完整度分": integrity,
        "資料完整度摘要": integrity_note,
        "權值股明細": heavy.get("權值股明細", []),
    }


def _calc_market_context(pred_date_text: str) -> dict[str, Any]:
    twii = _price_on_or_before("^TWII", pred_date_text, 15)
    nas = _price_on_or_before("^IXIC", pred_date_text, 15)
    sox = _price_on_or_before("^SOX", pred_date_text, 15)
    spx = _price_on_or_before("^GSPC", pred_date_text, 15)
    adr = _price_on_or_before("TSM", pred_date_text, 15)
    es = _price_on_or_before("ES.F", pred_date_text, 15)
    nq = _price_on_or_before("NQ.F", pred_date_text, 15)
    vix = _price_on_or_before("^VIX", pred_date_text, 15)
    usdtwd = _price_on_or_before("USDTWD", pred_date_text, 15)
    news_rows = _search_news_headlines(pred_date_text)
    news_score, news_logic, main_risk = _news_risk_score(news_rows)

    inst = _fetch_twse_institutional(pred_date_text)
    futopt = _fetch_taifex_sentiment(pred_date_text)
    margin = _fetch_twse_margin(pred_date_text)

    tech_score = 0.0
    structure_notes = []
    tw_close = _safe_float(twii.get("close"), 22000.0)
    tw_pct = _safe_float(twii.get("pct"), 0.0)
    tw_ma5 = _safe_float(twii.get("ma5"), tw_close)
    tw_ma20 = _safe_float(twii.get("ma20"), tw_close)
    if tw_close >= tw_ma5:
        tech_score += 2.5
        structure_notes.append("加權站上MA5")
    else:
        tech_score -= 2.5
        structure_notes.append("加權跌破MA5")
    if tw_close >= tw_ma20:
        tech_score += 3.0
        structure_notes.append("加權站上MA20")
    else:
        tech_score -= 3.0
        structure_notes.append("加權跌破MA20")
    tech_score += _score_from_pct(tw_pct, scale=1.3, cap=6)

    adr_pct = _safe_float(adr.get("pct"), 0.0)
    es_pct = _safe_float(es.get("pct"), 0.0)
    nq_pct = _safe_float(nq.get("pct"), 0.0)
    nas_pct = _safe_float(nas.get("pct"), 0.0)
    sox_pct = _safe_float(sox.get("pct"), 0.0)
    spx_pct = _safe_float(spx.get("pct"), 0.0)

    us_score = (
        _score_from_pct(nas_pct, 1.8, 8)
        + _score_from_pct(sox_pct, 2.2, 9)
        + _score_from_pct(spx_pct, 1.2, 6)
        + _score_from_pct(adr_pct, 2.0, 8)
    )
    night_score = _score_from_pct(es_pct, 2.0, 8) + _score_from_pct(nq_pct, 2.4, 9)

    vix_val = _safe_float(vix.get("close"), 18.0)
    risk_score = 0.0
    if vix_val >= 30:
        risk_score -= 8
    elif vix_val >= 24:
        risk_score -= 5
    elif vix_val >= 20:
        risk_score -= 2
    elif vix_val <= 15:
        risk_score += 1.5

    fx_val = _safe_float(usdtwd.get("close"), 32.0)
    if fx_val >= 32.5:
        risk_score -= 2.5
    elif fx_val <= 31.8:
        risk_score += 1.0

    chip = _derive_chip_scores(inst, futopt, margin, adr_pct, es_pct, nq_pct)
    sector_strong, sector_weak, sector_score = _derive_sector_tags(sox_pct, nas_pct, adr_pct, spx_pct)
    event_list = _derive_event_list(news_rows)

    foreign_score = chip['foreign_score']
    futures_score = chip['futures_score']
    margin_score = chip['margin_score']
    event_score = -news_score * 1.8
    if vix_val >= 25:
        event_score -= 2.0
    if chip['pcr'] is not None:
        if chip['pcr'] >= 1.2:
            event_score += 0.8
        elif chip['pcr'] <= 0.8:
            event_score -= 0.8

    if tw_pct >= 1.2 and us_score > 3 and foreign_score >= 0:
        scenario = "多頭延續日"
    elif tw_pct <= -1.2 and us_score < -3 and foreign_score <= 0:
        scenario = "空頭延續日"
    elif news_score >= 4 or vix_val >= 25:
        scenario = "重大風險事件日"
    elif abs(tw_pct) <= 0.5 and abs(us_score) <= 3:
        scenario = "高檔/低檔震盪日"
    else:
        scenario = "一般趨勢日"

    source_status = (
        f"加權:{_safe_str(twii.get('date')) or 'fallback'}｜"
        f"法人:{_safe_str(chip.get('inst_used_date')) or inst.get('source','fallback')}｜"
        f"期權:{_safe_str(chip.get('futopt_used_date')) or futopt.get('source','fallback')}｜"
        f"融資券:{_safe_str(chip.get('margin_used_date')) or margin.get('source','fallback')}"
    )

    return {
        "twii": twii, "taiex_now": taiex_now, "nas": nas, "sox": sox, "spx": spx, "adr": adr, "es": es, "nq": nq, "vix": vix, "usdtwd": usdtwd,
        "news_rows": news_rows,
        "news_score": news_score,
        "news_logic": news_logic,
        "main_risk": main_risk,
        "tech_score": tech_score,
        "us_score": us_score,
        "night_score": night_score,
        "risk_score": risk_score,
        "foreign_score": foreign_score,
        "futures_score": futures_score + margin_score,
        "sector_score": sector_score,
        "event_score": event_score,
        "scenario": scenario,
        "structure_notes": structure_notes,
        "tw_close": tw_close,
        "tw_pct": tw_pct,
        "vix_val": vix_val,
        "fx_val": fx_val,
        "foreign_amt": chip['foreign_amt'],
        "total3_amt": chip['total3_amt'],
        "foreign_fut_net": chip['foreign_fut_net'],
        "pcr": chip['pcr'],
        "margin_change": chip['margin_change'],
        "short_change": chip['short_change'],
        "sector_strong": sector_strong,
        "sector_weak": sector_weak,
        "event_list": event_list,
        "source_status": source_status,
        "market_data_date": _safe_str(twii.get('date')),
        "twii_date": _safe_str(twii.get('date')),
        "us_data_date": " / ".join([x for x in [_safe_str(nas.get('date')), _safe_str(sox.get('date')), _safe_str(spx.get('date')), _safe_str(adr.get('date'))] if x]),
        "night_data_date": " / ".join([x for x in [_safe_str(es.get('date')), _safe_str(nq.get('date'))] if x]),
        "inst_used_date": _safe_str(inst.get('used_date')),
        "futopt_used_date": _safe_str(futopt.get('used_date')),
        "margin_used_date": _safe_str(margin.get('used_date')),
        "news_range": f"{pred_date_text} 前後搜尋" if news_rows else "無新聞命中",
    }




def _calc_market_context_quick(pred_date_text: str) -> dict[str, Any]:
    """
    v25.4 快顯模式：
    只抓必要市場價格與少量風險代理資料，避免 Google News / TWSE / TAIFEX 外部端點等待過久，
    讓 01_大盤趨勢可以先顯示。關閉「快顯模式」仍會走完整資料源。
    """
    # v26.6：大盤加權指數優先使用 TWSE 即時 / 收盤，不再只依賴 Yahoo/Stooq 延遲資料。
    taiex_now = _fetch_taiex_realtime_or_close(pred_date_text)
    twii = _apply_taiex_override(_price_on_or_before("^TWII", pred_date_text, 5), taiex_now)
    nas = _price_on_or_before("^IXIC", pred_date_text, 3)
    sox = _price_on_or_before("^SOX", pred_date_text, 3)
    spx = _price_on_or_before("^GSPC", pred_date_text, 3)
    adr = _price_on_or_before("TSM", pred_date_text, 3)
    es = _price_on_or_before("ES.F", pred_date_text, 3)
    nq = _price_on_or_before("NQ.F", pred_date_text, 3)
    vix = _price_on_or_before("^VIX", pred_date_text, 3)
    usdtwd = _price_on_or_before("USDTWD", pred_date_text, 3)

    news_rows: list[dict[str, str]] = []
    news_score, news_logic, main_risk = 0.0, "快顯模式略過新聞即時抓取", "快顯模式：未納入新聞端點"

    tech_score = 0.0
    structure_notes = []
    tw_close = _safe_float(twii.get("close"), 22000.0)
    tw_pct = _safe_float(twii.get("pct"), 0.0)
    tw_ma5 = _safe_float(twii.get("ma5"), tw_close)
    tw_ma20 = _safe_float(twii.get("ma20"), tw_close)
    if tw_close >= tw_ma5:
        tech_score += 2.5
        structure_notes.append("加權站上MA5")
    else:
        tech_score -= 2.5
        structure_notes.append("加權跌破MA5")
    if tw_close >= tw_ma20:
        tech_score += 3.0
        structure_notes.append("加權站上MA20")
    else:
        tech_score -= 3.0
        structure_notes.append("加權跌破MA20")
    tech_score += _score_from_pct(tw_pct, scale=1.3, cap=6)

    adr_pct = _safe_float(adr.get("pct"), 0.0)
    es_pct = _safe_float(es.get("pct"), 0.0)
    nq_pct = _safe_float(nq.get("pct"), 0.0)
    nas_pct = _safe_float(nas.get("pct"), 0.0)
    sox_pct = _safe_float(sox.get("pct"), 0.0)
    spx_pct = _safe_float(spx.get("pct"), 0.0)

    us_score = (
        _score_from_pct(nas_pct, 1.8, 8)
        + _score_from_pct(sox_pct, 2.2, 9)
        + _score_from_pct(spx_pct, 1.2, 6)
        + _score_from_pct(adr_pct, 2.0, 8)
    )
    night_score = _score_from_pct(es_pct, 2.0, 8) + _score_from_pct(nq_pct, 2.4, 9)

    vix_val = _safe_float(vix.get("close"), 18.0)
    risk_score = 0.0
    if vix_val >= 30:
        risk_score -= 8
    elif vix_val >= 24:
        risk_score -= 5
    elif vix_val >= 20:
        risk_score -= 2
    elif vix_val <= 15:
        risk_score += 1.5

    fx_val = _safe_float(usdtwd.get("close"), 32.0)
    if fx_val >= 32.5:
        risk_score -= 2.5
    elif fx_val <= 31.8:
        risk_score += 1.0

    # 快顯模式用外盤與ADR代理籌碼估分，不等待法人/期權/融資券端點。
    foreign_score = _score_from_pct(adr_pct * 0.45 + sox_pct * 0.30 + nas_pct * 0.25, scale=1.7, cap=8)
    futures_score = _score_from_pct(es_pct * 0.45 + nq_pct * 0.55, scale=1.8, cap=8)
    margin_score = _score_from_pct(-(adr_pct * 0.5 + es_pct * 0.25 + nq_pct * 0.25), scale=0.8, cap=4)
    sector_strong, sector_weak, sector_score = _derive_sector_tags(sox_pct, nas_pct, adr_pct, spx_pct)
    event_list = ["快顯模式：新聞/法人/期權端點略過，關閉快顯模式可完整抓取"]

    event_score = -news_score * 1.8
    if vix_val >= 25:
        event_score -= 2.0

    if tw_pct >= 1.2 and us_score > 3 and foreign_score >= 0:
        scenario = "多頭延續日"
    elif tw_pct <= -1.2 and us_score < -3 and foreign_score <= 0:
        scenario = "空頭延續日"
    elif news_score >= 4 or vix_val >= 25:
        scenario = "重大風險事件日"
    elif abs(tw_pct) <= 0.5 and abs(us_score) <= 3:
        scenario = "高檔/低檔震盪日"
    else:
        scenario = "一般趨勢日"

    source_status = (
        f"快顯模式｜加權:{_safe_str(twii.get('source')) or 'fallback'} / {_safe_str(twii.get('date')) or 'fallback'}｜"
        f"外盤:{_safe_str(nas.get('date')) or 'fallback'}｜"
        f"法人/期權/融資券:代理估算"
    )

    return {
        "twii": twii, "nas": nas, "sox": sox, "spx": spx, "adr": adr, "es": es, "nq": nq, "vix": vix, "usdtwd": usdtwd,
        "news_rows": news_rows,
        "news_score": news_score,
        "news_logic": news_logic,
        "main_risk": main_risk,
        "tech_score": tech_score,
        "us_score": us_score,
        "night_score": night_score,
        "risk_score": risk_score,
        "foreign_score": foreign_score,
        "futures_score": futures_score + margin_score,
        "sector_score": sector_score,
        "event_score": event_score,
        "scenario": scenario,
        "structure_notes": structure_notes,
        "tw_close": tw_close,
        "tw_pct": tw_pct,
        "vix_val": vix_val,
        "fx_val": fx_val,
        "foreign_amt": None,
        "total3_amt": None,
        "foreign_fut_net": None,
        "pcr": None,
        "margin_change": None,
        "short_change": None,
        "sector_strong": sector_strong,
        "sector_weak": sector_weak,
        "event_list": event_list,
        "source_status": source_status,
        "market_data_date": _safe_str(twii.get('date')),
        "twii_date": _safe_str(twii.get('date')),
        "us_data_date": " / ".join([x for x in [_safe_str(nas.get('date')), _safe_str(sox.get('date')), _safe_str(spx.get('date')), _safe_str(adr.get('date'))] if x]),
        "night_data_date": " / ".join([x for x in [_safe_str(es.get('date')), _safe_str(nq.get('date'))] if x]),
        "inst_used_date": "快顯代理估算",
        "futopt_used_date": "快顯代理估算",
        "margin_used_date": "快顯代理估算",
        "news_range": "快顯模式略過新聞端點",
    }



def _calc_market_context_instant(pred_date_text: str) -> dict[str, Any]:
    """
    v26.7：零等待大盤模式。
    不呼叫 Yahoo / Stooq / TWSE / Google News / TAIFEX。
    先用本機收盤快取 macro_market_close_cache.json 或安全預設值讓頁面立即顯示。
    使用者可再按「更新即時大盤」補抓資料。
    """
    cache = _read_macro_close_cache()
    dt = pd.to_datetime(pred_date_text, errors="coerce")
    if pd.isna(dt):
        dt = pd.Timestamp(_tw_now().date())
    ymd = dt.strftime("%Y%m%d")
    cached = cache.get(ymd, {}) if isinstance(cache, dict) else {}

    close = _safe_float(cached.get("close"), None) if isinstance(cached, dict) else None
    pct = _safe_float(cached.get("pct"), 0.0) if isinstance(cached, dict) else 0.0
    used_date = _safe_str(cached.get("used_date") or cached.get("date")) if isinstance(cached, dict) else ""
    source = _safe_str(cached.get("source")) if isinstance(cached, dict) else ""

    if close is None:
        close = 22000.0
        pct = 0.0
        used_date = pred_date_text
        source = "安全預設值｜等待手動更新即時大盤"

    twii = {
        "ok": bool(cached),
        "source": source or "本機快取",
        "date": used_date or pred_date_text,
        "used_date": used_date or pred_date_text,
        "close": close,
        "pct": pct,
        "ma5": close,
        "ma20": close,
        "is_realtime": False,
    }
    empty_asset = {"date": "", "used_date": "", "pct": 0.0, "close": None, "source": "instant_skip"}

    tech_score = _score_from_pct(pct, scale=1.3, cap=6)
    us_score = 0.0
    night_score = 0.0
    risk_score = 0.0
    foreign_score = 0.0
    futures_score = 0.0
    sector_score = 0.0
    event_score = 0.0
    scenario = "即時快顯等待更新"
    if pct >= 0.8:
        scenario = "大盤偏多"
    elif pct <= -0.8:
        scenario = "大盤偏空"
    elif abs(pct) <= 0.3:
        scenario = "大盤震盪"

    return {
        "twii": twii,
        "taiex_now": twii,
        "nas": empty_asset, "sox": empty_asset, "spx": empty_asset, "adr": empty_asset,
        "es": empty_asset, "nq": empty_asset, "vix": empty_asset, "usdtwd": empty_asset,
        "news_rows": [],
        "news_score": 0.0,
        "news_logic": "零等待模式略過新聞端點",
        "main_risk": "零等待模式：未納入新聞/法人/期權端點",
        "tech_score": tech_score,
        "us_score": us_score,
        "night_score": night_score,
        "risk_score": risk_score,
        "foreign_score": foreign_score,
        "futures_score": futures_score,
        "sector_score": sector_score,
        "event_score": event_score,
        "scenario": scenario,
        "structure_notes": ["零等待模式先顯示頁面，請按更新即時大盤補抓資料"],
        "tw_close": close,
        "tw_pct": pct,
        "vix_val": 18.0,
        "fx_val": 32.0,
        "foreign_amt": None,
        "total3_amt": None,
        "foreign_fut_net": None,
        "pcr": None,
        "margin_change": None,
        "short_change": None,
        "sector_strong": "零等待",
        "sector_weak": "零等待",
        "event_list": ["零等待模式：先顯示，不等待外部 API"],
        "source_status": f"零等待模式｜加權:{source or '本機快取/安全預設'} / {used_date or pred_date_text}",
        "market_data_date": used_date or pred_date_text,
        "twii_date": used_date or pred_date_text,
        "us_data_date": "零等待略過",
        "night_data_date": "零等待略過",
        "inst_used_date": "零等待略過",
        "futopt_used_date": "零等待略過",
        "margin_used_date": "零等待略過",
        "news_range": "零等待略過新聞端點",
    }


def _predict_all_models_instant(pred_date_text: str, records_df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    ctx = _calc_market_context_instant(pred_date_text)
    weights = _get_dynamic_weights(records_df)
    rows = [_predict_for_model(model, ctx, weights, pred_date_text) for model in MODEL_NAMES]
    pred_df = pd.DataFrame(rows)
    pred_df = _enrich_macro_predictions(pred_df, ctx)
    pred_df["綜合排名分"] = pred_df["股神模式分數"] * 0.55 + pred_df["股神信心度"] * 0.45
    pred_df = pred_df.sort_values(["綜合排名分", "股神模式分數"], ascending=[False, False]).reset_index(drop=True)
    return pred_df, ctx


def _try_refresh_taiex_cache_now(pred_date_text: str) -> tuple[bool, str]:
    """
    手動更新即時/收盤大盤。失敗不讓整頁卡住。
    """
    try:
        data = _fetch_taiex_realtime_or_close(pred_date_text)
        if isinstance(data, dict) and data.get("close") is not None:
            dt = pd.to_datetime(data.get("date") or pred_date_text, errors="coerce")
            if pd.isna(dt):
                dt = pd.to_datetime(pred_date_text, errors="coerce")
            if pd.isna(dt):
                dt = pd.Timestamp(_tw_now().date())
            ymd = dt.strftime("%Y%m%d")
            cache = _read_macro_close_cache()
            cache[ymd] = data
            _write_macro_close_cache(cache)
            return True, f"已更新大盤資料：{data.get('source')}｜{data.get('date')}｜{_safe_float(data.get('close'), 0):,.2f}"
        return False, "即時 / 收盤大盤更新失敗，已保留本機快顯資料。"
    except Exception as e:
        return False, f"即時 / 收盤大盤更新例外：{e}"


def _predict_all_models_quick(pred_date_text: str, records_df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    ctx = _calc_market_context_quick(pred_date_text)
    weights = _get_dynamic_weights(records_df)
    rows = [_predict_for_model(model, ctx, weights, pred_date_text) for model in MODEL_NAMES]
    pred_df = pd.DataFrame(rows)
    pred_df = _enrich_macro_predictions(pred_df, ctx)
    pred_df["綜合排名分"] = pred_df["股神模式分數"] * 0.55 + pred_df["股神信心度"] * 0.45
    pred_df = pred_df.sort_values(["綜合排名分", "股神模式分數"], ascending=[False, False]).reset_index(drop=True)
    return pred_df, ctx



def _get_dynamic_weights(records_df: pd.DataFrame) -> dict[str, dict[str, float]]:
    default = {
        "股神平衡版": {"tech": 0.24, "us": 0.18, "night": 0.15, "news": 0.10, "chip": 0.12, "event": 0.10, "sector": 0.06, "risk": 0.05},
        "技術面優先": {"tech": 0.34, "us": 0.12, "night": 0.10, "news": 0.08, "chip": 0.14, "event": 0.08, "sector": 0.09, "risk": 0.05},
        "美夜盤優先": {"tech": 0.15, "us": 0.28, "night": 0.22, "news": 0.08, "chip": 0.10, "event": 0.07, "sector": 0.05, "risk": 0.05},
        "新聞風險優先": {"tech": 0.14, "us": 0.14, "night": 0.10, "news": 0.22, "chip": 0.08, "event": 0.17, "sector": 0.05, "risk": 0.10},
        "籌碼事件優先": {"tech": 0.15, "us": 0.12, "night": 0.10, "news": 0.09, "chip": 0.22, "event": 0.18, "sector": 0.08, "risk": 0.06},
    }
    df = _ensure_columns(records_df)
    if df.empty:
        return default
    hist = df[df["方向是否命中"].fillna(False) == True].copy()
    if hist.empty:
        return default
    out = json.loads(json.dumps(default))
    for model in MODEL_NAMES:
        hit_df = df[df["模式名稱"].astype(str) == model].copy()
        if len(hit_df) < 8:
            continue
        win = hit_df["方向是否命中"].fillna(False).mean()
        point_err = pd.to_numeric(hit_df["點數誤差"], errors="coerce").dropna().mean()
        adj = 1.0
        if win >= 0.65:
            adj += 0.08
        elif win <= 0.45:
            adj -= 0.06
        if point_err is not None and not pd.isna(point_err):
            if point_err <= 60:
                adj += 0.03
            elif point_err >= 120:
                adj -= 0.03
        base = out[model]
        if model == "新聞風險優先":
            base["news"] *= adj
            base["event"] *= adj
        elif model == "技術面優先":
            base["tech"] *= adj
            base["sector"] *= adj
        elif model == "美夜盤優先":
            base["us"] *= adj
            base["night"] *= adj
        elif model == "籌碼事件優先":
            base["chip"] *= adj
            base["event"] *= adj
        else:
            base["tech"] *= adj
            base["us"] *= adj
        total = sum(base.values())
        out[model] = {k: v / total for k, v in base.items()}
    return out


def _direction_strength(score: float) -> tuple[str, str]:
    if score >= 15:
        return "偏多", "強多"
    if score >= 5:
        return "偏多", "弱多"
    if score <= -15:
        return "偏空", "強空"
    if score <= -5:
        return "偏空", "弱空"
    return "震盪", "中性震盪"


def _risk_level(total_score: float, news_score: float, vix_val: float) -> str:
    if news_score >= 5 or vix_val >= 30:
        return "極高"
    if news_score >= 3 or vix_val >= 24 or total_score <= -12:
        return "高"
    if abs(total_score) < 5:
        return "中"
    return "低"


def _build_action_pack(direction: str, strength: str, risk_level: str, scenario: str, tech_score: float) -> tuple[str, str, str, str, str, str, str]:
    if risk_level in {"極高", "高"} and direction != "偏多":
        return ("否", "否", "是", "是", "觀望或減碼", "10%~20%", "風險事件優先，避免追價")
    if direction == "偏多" and strength == "強多" and scenario != "重大風險事件日":
        return ("是", "是", "否", "否", "可分批進場與續抱", "40%~70%", "偏多結構完整，可分批布局")
    if direction == "偏多":
        return ("是", "是", "否", "否", "小倉分批進場", "20%~40%", "偏多但強度一般，避免重壓")
    if direction == "震盪":
        if tech_score >= 0:
            return ("條件式", "是", "視個股", "否", "低接不追價", "10%~30%", "震盪盤偏向低接高出")
        return ("否", "否", "是", "否", "觀望等待確認", "0%~20%", "方向不明確，降低交易頻率")
    return ("否", "否", "是", "是", "減碼或出場防守", "0%~15%", "偏空結構不利持股")


def _predict_for_model(model_name: str, ctx: dict[str, Any], weight_map: dict[str, dict[str, float]], pred_date_text: str) -> dict[str, Any]:
    w = weight_map.get(model_name, weight_map["股神平衡版"])
    component = {
        "tech": ctx["tech_score"],
        "us": ctx["us_score"],
        "night": ctx["night_score"],
        "news": -ctx["news_score"] * 2.2,
        "chip": ctx["foreign_score"] + ctx["futures_score"],
        "event": ctx["event_score"],
        "sector": ctx["sector_score"],
        "risk": ctx["risk_score"],
    }
    total_score = sum(component[k] * w[k] for k in component) * 3.2

    direction, strength = _direction_strength(total_score)
    risk_level = _risk_level(total_score, ctx["news_score"], ctx["vix_val"])
    fit_entry, fit_hold, fit_trim, fit_exit, action, position, action_note = _build_action_pack(direction, strength, risk_level, ctx["scenario"], ctx["tech_score"])

    base = ctx["tw_close"] or 22000.0
    day_vol = max(80.0, min(420.0, abs(ctx["tw_pct"] or 0) * 120 + abs(ctx["us_score"]) * 6 + abs(ctx["night_score"]) * 5 + ctx["news_score"] * 12))
    predicted_points = total_score * 8.5
    if direction == "震盪":
        predicted_points = max(-80, min(80, predicted_points))
    high = base + max(20.0, predicted_points + day_vol * 0.45)
    low = base + min(-20.0, predicted_points - day_vol * 0.45)
    confidence = max(35.0, min(92.0, 55 + abs(total_score) * 1.3 - ctx["news_score"] * 1.2 - (ctx["vix_val"] - 18) * 0.6))
    reference_pack = _build_macro_reference_pack(ctx, total_score, risk_level)

    logic_lines = [
        f"情境：{ctx['scenario']}",
        f"技術面 {ctx['tech_score']:.1f}（{' / '.join(ctx['structure_notes'][:3]) or '結構中性'}）",
        f"美股/ADR {ctx['us_score']:.1f}，夜盤 {ctx['night_score']:.1f}",
        f"新聞風險 {ctx['news_score']:.1f}（{ctx['news_logic']}）",
        f"VIX {ctx['vix_val']:.2f}、美元台幣 {ctx['fx_val']:.2f}，風險面 {ctx['risk_score']:.1f}",
        f"外資現貨 {(_safe_float(ctx.get('foreign_amt')) or 0):.1f} 億 / 三大法人 {(_safe_float(ctx.get('total3_amt')) or 0):.1f} 億 / PCR {(_safe_float(ctx.get('pcr')) or 0):.2f}",
        f"強勢族群 {ctx.get('sector_strong','')}；弱勢族群 {ctx.get('sector_weak','')}",
        f"模式 {model_name} 權重下總分 {total_score:.1f}",
    ]
    entry_confirm = "、".join([
        "加權至少站穩 MA5" if ctx["tw_close"] >= _safe_float(ctx["twii"].get("ma5"), ctx["tw_close"]) else "需先站回 MA5",
        "美股科技不轉弱" if ctx["us_score"] >= -2 else "美股仍偏弱需保守",
        "夜盤不急轉空" if ctx["night_score"] >= -2 else "夜盤偏空避免搶反彈",
        "VIX 不高於 24" if ctx["vix_val"] < 24 else "VIX 過高需降倉位",
    ])
    exit_alerts = "、".join([
        "若開高走低跌破前低應減碼",
        "若 VIX 再拉高或國際風險升溫應防守",
        "若美股/夜盤同步轉空可提高出場意願",
    ])

    return {
        "record_id": _create_record_id(pred_date_text, model_name),
        "推估日期": pred_date_text,
        "建立時間": _now_text(),
        "更新時間": _now_text(),
        "模式名稱": model_name,
        "市場情境": ctx["scenario"],
        "推估方向": direction,
        "方向強度": strength,
        "是否適合進場": fit_entry,
        "是否適合續抱": fit_hold,
        "是否適合減碼": fit_trim,
        "是否適合出場": fit_exit,
        "建議動作": action,
        "股神模式分數": round(total_score, 2),
        "股神信心度": round(confidence, 2),
        "預估漲跌點": round(predicted_points, 2),
        "預估高點": round(high, 2),
        "預估低點": round(low, 2),
        "預估區間寬度": round(high - low, 2),
        "風險等級": risk_level,
        "國際新聞風險分": round(ctx["news_score"], 2),
        "美股因子分": round(ctx["us_score"], 2),
        "夜盤因子分": round(ctx["night_score"], 2),
        "技術面因子分": round(ctx["tech_score"], 2),
        "籌碼面因子分": round(ctx["foreign_score"] + ctx["futures_score"], 2),
        "事件因子分": round(ctx["event_score"], 2),
        "結構面因子分": round(ctx["sector_score"], 2),
        "風險面因子分": round(ctx["risk_score"], 2),
        "大盤基準點": round(base, 2),
        "加權收盤": round(_safe_float(ctx["twii"].get("close"), base), 2),
        "加權漲跌%": round(_safe_float(ctx["twii"].get("pct"), 0.0), 2),
        "成交量估分": round(abs(_safe_float(ctx["twii"].get("pct"), 0.0)) * 4.5, 2),
        "VIX": round(ctx["vix_val"], 2),
        "美元台幣": round(ctx["fx_val"], 4),
        "NASDAQ漲跌%": round(_safe_float(ctx["nas"].get("pct"), 0.0), 2),
        "SOX漲跌%": round(_safe_float(ctx["sox"].get("pct"), 0.0), 2),
        "SP500漲跌%": round(_safe_float(ctx["spx"].get("pct"), 0.0), 2),
        "台積電ADR漲跌%": round(_safe_float(ctx["adr"].get("pct"), 0.0), 2),
        "ES夜盤漲跌%": round(_safe_float(ctx["es"].get("pct"), 0.0), 2),
        "NQ夜盤漲跌%": round(_safe_float(ctx["nq"].get("pct"), 0.0), 2),
        "外資買賣超估分": round(ctx["foreign_score"], 2),
        "期貨選擇權估分": round(ctx["futures_score"], 2),
        "類股輪動估分": round(ctx["sector_score"], 2),
        "加權資料日期": _safe_str(ctx.get("twii_date")),
        "美股資料日期": _safe_str(ctx.get("us_data_date")),
        "夜盤資料日期": _safe_str(ctx.get("night_data_date")),
        "法人資料日期": _safe_str(ctx.get("inst_used_date")),
        "期權資料日期": _safe_str(ctx.get("futopt_used_date")),
        "融資券資料日期": _safe_str(ctx.get("margin_used_date")),
        "新聞資料區間": _safe_str(ctx.get("news_range")),
        "股神推論邏輯": "\n".join(logic_lines),
        "進場確認條件": entry_confirm,
        "出場警訊": exit_alerts,
        "主要風險": ctx["main_risk"],
        "建議倉位": position,
        "外資買賣超(億)": None if ctx.get("foreign_amt") is None else round(ctx.get("foreign_amt"), 2),
        "三大法人合計(億)": None if ctx.get("total3_amt") is None else round(ctx.get("total3_amt"), 2),
        "外資期貨淨單": ctx.get("foreign_fut_net"),
        "PCR": None if ctx.get("pcr") is None else round(ctx.get("pcr"), 3),
        "融資增減(億)": None if ctx.get("margin_change") is None else round(ctx.get("margin_change"), 2),
        "融券增減張": ctx.get("short_change"),
        "強勢族群": ctx.get("sector_strong", ""),
        "弱勢族群": ctx.get("sector_weak", ""),
        "重大事件清單": ctx.get("event_list", ""),
        "因子來源狀態": ctx.get("source_status", ""),
        "實際方向": "",
        "實際漲跌點": None,
        "實際高點": None,
        "實際低點": None,
        "方向是否命中": False,
        "區間是否命中": False,
        "點數誤差": None,
        "建議動作是否合適": False,
        "誤判主因類別": "",
        "誤判主因": "",
        "收盤檢討": action_note,
        "大盤可參考分數": reference_pack.get("大盤可參考分數"),
        "大盤參考等級": reference_pack.get("大盤參考等級"),
        "市場廣度分數": reference_pack.get("市場廣度分數"),
        "類股輪動分數": reference_pack.get("類股輪動分數"),
        "量價確認分數": reference_pack.get("量價確認分數"),
        "權值支撐分數": reference_pack.get("權值支撐分數"),
        "推薦同步分數": reference_pack.get("推薦同步分數"),
        "推薦加權建議": reference_pack.get("推薦加權建議"),
        "推薦降權原因": reference_pack.get("推薦降權原因"),
        "今日適合操作風格": reference_pack.get("今日適合操作風格"),
        "廣度樣本數": reference_pack.get("廣度樣本數"),
        "上漲家數": reference_pack.get("上漲家數"),
        "下跌家數": reference_pack.get("下跌家數"),
        "站上MA20比例": reference_pack.get("站上MA20比例"),
        "站上MA60比例": reference_pack.get("站上MA60比例"),
        "創20日新高家數": reference_pack.get("創20日新高家數"),
        "創20日新低家數": reference_pack.get("創20日新低家數"),
        "推薦強勢比例": reference_pack.get("推薦強勢比例"),
        "推薦平均分數": reference_pack.get("推薦平均分數"),
        "推薦起漲比例": reference_pack.get("推薦起漲比例"),
        "備註": "",
    }



# =========================================================
# 華爾街級大盤決策引擎：regime / risk bucket / playbook
# =========================================================
def _score_to_regime(total_score: float, risk_level: str, ctx: dict[str, Any]) -> tuple[str, str]:
    vix = _safe_float(ctx.get("vix_val"), 18) or 18
    news = _safe_float(ctx.get("news_score"), 0) or 0
    if risk_level in {"極高", "高"} or vix >= 26 or news >= 5:
        return "Risk-Off｜防守模式", "風險優先，降低槓桿與追價行為"
    if total_score >= 18:
        return "Risk-On｜進攻模式", "順勢偏多，允許核心強勢股分批進攻"
    if total_score >= 6:
        return "Selective Long｜精選偏多", "只做強勢族群與低風險買點"
    if total_score <= -12:
        return "De-risk｜降風險模式", "偏空結構，優先保護本金"
    return "Neutral｜震盪模式", "等待方向確認，以低接高出與風控為主"


def _risk_bucket(total_score: float, risk_level: str, ctx: dict[str, Any]) -> str:
    vix = _safe_float(ctx.get("vix_val"), 18) or 18
    fx = _safe_float(ctx.get("fx_val"), 32) or 32
    news = _safe_float(ctx.get("news_score"), 0) or 0
    if risk_level == "極高" or vix >= 30 or news >= 6:
        return "紅燈｜重大風險"
    if risk_level == "高" or vix >= 24 or fx >= 32.8:
        return "橘燈｜高波動"
    if total_score >= 12 and vix < 22:
        return "綠燈｜可進攻"
    if abs(total_score) < 6:
        return "黃燈｜震盪觀察"
    return "藍燈｜選股優先"


def _capital_flow_judgement(ctx: dict[str, Any]) -> str:
    foreign = _safe_float(ctx.get("foreign_amt"), 0) or 0
    total3 = _safe_float(ctx.get("total3_amt"), 0) or 0
    pcr = _safe_float(ctx.get("pcr"), 1) or 1
    margin = _safe_float(ctx.get("margin_change"), 0) or 0
    parts = []
    if foreign > 80:
        parts.append("外資強買")
    elif foreign > 20:
        parts.append("外資偏買")
    elif foreign < -80:
        parts.append("外資強賣")
    elif foreign < -20:
        parts.append("外資偏賣")
    else:
        parts.append("外資中性")

    if total3 > 100:
        parts.append("三大法人同步偏多")
    elif total3 < -100:
        parts.append("三大法人同步偏空")

    if pcr >= 1.18:
        parts.append("選擇權避險偏多")
    elif pcr <= 0.82:
        parts.append("選擇權偏空/過熱警戒")

    if margin > 20:
        parts.append("融資增加需防追價")
    elif margin < -20:
        parts.append("融資下降籌碼較乾淨")
    return "｜".join(parts)


def _position_ceiling(total_score: float, risk_level: str, ctx: dict[str, Any]) -> float:
    vix = _safe_float(ctx.get("vix_val"), 18) or 18
    news = _safe_float(ctx.get("news_score"), 0) or 0
    base = 35.0
    if total_score >= 18:
        base = 70.0
    elif total_score >= 10:
        base = 55.0
    elif total_score >= 3:
        base = 35.0
    elif total_score <= -10:
        base = 10.0
    else:
        base = 20.0

    if risk_level in {"高", "極高"}:
        base -= 20
    if vix >= 25:
        base -= 15
    if news >= 4:
        base -= 10
    return float(max(0, min(80, base)))


def _build_next_day_playbook(row: dict[str, Any], ctx: dict[str, Any]) -> tuple[str, str, str, str]:
    score = _safe_float(row.get("股神模式分數"), 0) or 0
    direction = _safe_str(row.get("推估方向"))
    risk_level = _safe_str(row.get("風險等級"))
    base = _safe_float(row.get("大盤基準點"), _safe_float(ctx.get("tw_close"), 0)) or 0
    pred_high = _safe_float(row.get("預估高點"), base) or base
    pred_low = _safe_float(row.get("預估低點"), base) or base

    if direction == "偏多" and risk_level not in {"高", "極高"}:
        playbook = (
            f"開盤若站穩 {base:,.0f} 且量能不縮，採分批偏多；"
            f"接近 {pred_high:,.0f} 不追價，回測不破再加碼。"
        )
        intraday = "盤中確認：金融/電子權值同步強、台積電ADR/半導體族群不轉弱、外資期貨未急殺。"
        etf_action = "ETF參考：可偏多分批，避免單筆滿倉；若急拉接近高點則等回測。"
        stock_rule = "個股總則：優先強勢族群龍頭、突破後回測不破者；避開爆量長上影。"
    elif direction == "偏空" or risk_level in {"高", "極高"}:
        playbook = (
            f"若跌破 {base:,.0f} 或盤中反彈無量，降低持股；"
            f"接近 {pred_low:,.0f} 觀察是否止跌，不急著攤平。"
        )
        intraday = "盤中確認：反彈量不足、權值股弱於大盤、VIX/匯率走升時提高現金。"
        etf_action = "ETF參考：不追空但降低曝險；有槓桿部位優先降風險。"
        stock_rule = "個股總則：跌破MA20或支撐轉弱者先減碼；只留低位階強勢股。"
    else:
        playbook = (
            f"區間震盪：上緣參考 {pred_high:,.0f}、下緣參考 {pred_low:,.0f}；"
            "不追高，等量價確認方向。"
        )
        intraday = "盤中確認：等待突破區間且成交量放大；若多空不同步，以保守倉位處理。"
        etf_action = "ETF參考：以小倉位區間操作，不做重倉方向押注。"
        stock_rule = "個股總則：只做有事件/族群支撐的個股，沒有量能不進場。"
    return playbook, intraday, etf_action, stock_rule


def _model_consensus(pred_df: pd.DataFrame) -> tuple[float, str]:
    if pred_df is None or pred_df.empty or "推估方向" not in pred_df.columns:
        return 0.0, "模型不足"
    direction_counts = pred_df["推估方向"].astype(str).value_counts()
    top_dir = direction_counts.index[0]
    top_ratio = float(direction_counts.iloc[0] / max(1, len(pred_df)) * 100)
    if top_ratio >= 80:
        msg = f"模型高度一致：{top_dir} {top_ratio:.0f}%"
    elif top_ratio >= 60:
        msg = f"模型偏向一致：{top_dir} {top_ratio:.0f}%"
    else:
        msg = f"模型分歧：最大共識僅 {top_dir} {top_ratio:.0f}%"
    return top_ratio, msg


def _enrich_macro_predictions(pred_df: pd.DataFrame, ctx: dict[str, Any]) -> pd.DataFrame:
    if pred_df is None or pred_df.empty:
        return pred_df
    out = pred_df.copy()
    consensus, consensus_msg = _model_consensus(out)
    enriched_rows = []
    for _, row in out.iterrows():
        r = dict(row)
        score = _safe_float(r.get("股神模式分數"), 0) or 0
        risk = _safe_str(r.get("風險等級"))
        regime, regime_note = _score_to_regime(score, risk, ctx)
        playbook, intraday, etf_action, stock_rule = _build_next_day_playbook(r, ctx)

        r["總體風險分桶"] = _risk_bucket(score, risk, ctx)
        r["進攻防守 regime"] = regime
        r["資金流向判斷"] = _capital_flow_judgement(ctx)
        r["隔日劇本"] = playbook
        r["盤中確認訊號"] = intraday
        r["倉位上限%"] = _position_ceiling(score, risk, ctx)
        r["ETF參考動作"] = etf_action
        r["個股操作總則"] = stock_rule
        r["模型一致性分數"] = consensus
        r["模型分歧警示"] = consensus_msg
        if _safe_str(r.get("股神推論邏輯")):
            r["股神推論邏輯"] = _safe_str(r.get("股神推論邏輯")) + f"\n進攻防守：{regime}｜{regime_note}\n模型共識：{consensus_msg}"
        enriched_rows.append(r)
    return pd.DataFrame(enriched_rows)


def _predict_all_models(pred_date_text: str, records_df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    ctx = _calc_market_context(pred_date_text)
    weights = _get_dynamic_weights(records_df)
    rows = [_predict_for_model(model, ctx, weights, pred_date_text) for model in MODEL_NAMES]
    pred_df = pd.DataFrame(rows)
    pred_df = _enrich_macro_predictions(pred_df, ctx)
    pred_df["綜合排名分"] = pred_df["股神模式分數"] * 0.55 + pred_df["股神信心度"] * 0.45
    pred_df = pred_df.sort_values(["綜合排名分", "股神模式分數"], ascending=[False, False]).reset_index(drop=True)
    return pred_df, ctx


def _upsert_predictions(base_df: pd.DataFrame, pred_df: pd.DataFrame) -> pd.DataFrame:
    base_df = _ensure_columns(base_df)
    pred_df = _ensure_columns(pred_df)
    if pred_df.empty:
        return base_df.copy()
    merged = pd.concat([base_df, pred_df], ignore_index=True)
    merged = merged.sort_values(["推估日期", "模式名稱", "更新時間"], ascending=[True, True, False], na_position="last")
    merged = merged.drop_duplicates(subset=["推估日期", "模式名稱"], keep="first")
    return _ensure_columns(merged)


def _delete_records_by_ids(df: pd.DataFrame, record_ids: list[str]) -> pd.DataFrame:
    x = _ensure_columns(df)
    ids = {_safe_str(v) for v in (record_ids or []) if _safe_str(v)}
    if x.empty or not ids:
        return x.copy()
    out = x[~x["record_id"].astype(str).isin(ids)].copy()
    return _ensure_columns(out)


def _action_family(action: str) -> str:
    a = _safe_str(action)
    if a in {"可分批買進", "小倉試單", "條件式低接", "低接不追價", "可進場", "分批布局"}:
        return "buy"
    if a in {"減碼觀望", "賣出/減碼", "減碼/出場", "出場", "賣出"}:
        return "sell"
    return "hold"


def _score_entry_exit(action: str, actual_points: float | None) -> tuple[float | None, float | None, float | None, bool | None]:
    if actual_points is None:
        return None, None, None, None
    family = _action_family(action)
    if family == "buy":
        entry = max(0.0, min(100.0, 50.0 + actual_points * 0.8))
        exit_score = max(0.0, min(100.0, 50.0 - actual_points * 0.8))
        fit = actual_points >= -20
    elif family == "sell":
        entry = max(0.0, min(100.0, 50.0 - actual_points * 0.8))
        exit_score = max(0.0, min(100.0, 50.0 + actual_points * 0.8))
        fit = actual_points <= 20
    else:
        entry = max(0.0, min(100.0, 85.0 - abs(actual_points) * 0.7))
        exit_score = entry
        fit = abs(actual_points) <= 60
    overall = round(entry * 0.4 + exit_score * 0.2 + (100.0 if fit else 35.0) * 0.4, 2)
    return round(entry, 2), round(exit_score, 2), overall, fit


def _build_review_summary(df: pd.DataFrame) -> dict[str, Any]:
    x = _ensure_columns(df)
    if x.empty:
        return {"樣本數": 0, "進場適合率": 0.0, "方向命中率": 0.0, "區間命中率": 0.0, "平均檢討分": 0.0}
    valid_action = x[~x["建議動作是否合適"].isna()].copy()
    return {
        "樣本數": int(len(x)),
        "進場適合率": float(pd.Series(x["建議動作是否合適"]).fillna(False).mean() * 100) if len(x) else 0.0,
        "方向命中率": float(pd.Series(x["方向是否命中"]).fillna(False).mean() * 100) if len(x) else 0.0,
        "區間命中率": float(pd.Series(x["區間是否命中"]).fillna(False).mean() * 100) if len(x) else 0.0,
        "平均檢討分": float(pd.to_numeric(x["整體檢討分"], errors="coerce").dropna().mean()) if len(x) else 0.0,
    }


def _derive_actual_direction(actual_points: float | None) -> str:
    if actual_points is None:
        return ""
    if actual_points >= 35:
        return "偏多"
    if actual_points <= -35:
        return "偏空"
    return "震盪"


def _apply_actual_result(row: pd.Series | dict[str, Any]) -> dict[str, Any]:
    src = dict(row)
    pred_dir = _safe_str(src.get("推估方向"))
    pred_points = _safe_float(src.get("預估漲跌點"))
    pred_high = _safe_float(src.get("預估高點"))
    pred_low = _safe_float(src.get("預估低點"))
    actual_points = _safe_float(src.get("實際漲跌點"))
    actual_high = _safe_float(src.get("實際高點"))
    actual_low = _safe_float(src.get("實際低點"))
    actual_dir = _safe_str(src.get("實際方向")) or _derive_actual_direction(actual_points)
    src["實際方向"] = actual_dir
    if actual_dir:
        src["方向是否命中"] = (pred_dir == actual_dir)
    if pred_points is not None and actual_points is not None:
        src["點數誤差"] = abs(pred_points - actual_points)
    if pred_high is not None and pred_low is not None and actual_high is not None and actual_low is not None:
        src["區間是否命中"] = (actual_high <= pred_high + 60) and (actual_low >= pred_low - 60)
    entry_score, exit_score, overall_score, fit = _score_entry_exit(_safe_str(src.get("建議動作")), actual_points)
    src["進場建議績效分"] = entry_score
    src["出場建議績效分"] = exit_score
    src["整體檢討分"] = overall_score
    if fit is not None and not bool(src.get("建議動作是否合適")):
        src["建議動作是否合適"] = fit
    src["更新時間"] = _now_text()
    return src


def _build_scoreboard(df: pd.DataFrame) -> pd.DataFrame:
    x = _ensure_columns(df)
    if x.empty:
        return pd.DataFrame(columns=["模式名稱", "樣本數", "方向命中率", "區間命中率", "平均點數誤差", "適合動作命中率", "綜合表現分數"])
    out = x.groupby("模式名稱", dropna=False).agg(
        樣本數=("record_id", "count"),
        方向命中率=("方向是否命中", lambda s: float(pd.Series(s).fillna(False).mean() * 100)),
        區間命中率=("區間是否命中", lambda s: float(pd.Series(s).fillna(False).mean() * 100)),
        平均點數誤差=("點數誤差", "mean"),
        適合動作命中率=("建議動作是否合適", lambda s: float(pd.Series(s).fillna(False).mean() * 100)),
    ).reset_index()
    out["平均點數誤差"] = pd.to_numeric(out["平均點數誤差"], errors="coerce")
    out["綜合表現分數"] = (
        out["方向命中率"].fillna(0) * 0.45
        + out["區間命中率"].fillna(0) * 0.25
        + out["適合動作命中率"].fillna(0) * 0.20
        + (120 - out["平均點數誤差"].fillna(120).clip(upper=120)) * 0.10
    )
    return out.sort_values(["綜合表現分數", "方向命中率"], ascending=[False, False]).reset_index(drop=True)


def _format_pred_df(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    for c in [
        "股神模式分數", "股神信心度", "預估漲跌點", "預估高點", "預估低點", "預估區間寬度", "國際新聞風險分",
        "美股因子分", "夜盤因子分", "技術面因子分", "籌碼面因子分", "事件因子分", "結構面因子分", "風險面因子分",
        "加權漲跌%", "VIX", "美元台幣", "NASDAQ漲跌%", "SOX漲跌%", "SP500漲跌%", "台積電ADR漲跌%", "ES夜盤漲跌%", "NQ夜盤漲跌%",
        "倉位上限%", "模型一致性分數", "大盤可參考分數", "市場廣度分數", "類股輪動分數", "量價確認分數", "權值支撐分數", "推薦同步分數", "站上MA20比例", "站上MA60比例", "推薦強勢比例", "推薦平均分數", "推薦起漲比例"
    ]:
        if c in x.columns:
            x[c] = x[c].apply(lambda v: "" if pd.isna(v) else f"{float(v):,.2f}")
    return x


def _build_export_bytes(df: pd.DataFrame, scoreboard: pd.DataFrame, pred_df: pd.DataFrame, stock_df: pd.DataFrame | None = None) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        _ensure_columns(df).to_excel(writer, sheet_name="歷史紀錄", index=False)
        scoreboard.to_excel(writer, sheet_name="模式排行榜", index=False)
        pred_df.to_excel(writer, sheet_name="本次推估", index=False)
        if stock_df is not None and isinstance(stock_df, pd.DataFrame) and not stock_df.empty:
            stock_df.to_excel(writer, sheet_name="個股連動", index=False)
        try:
            for ws in writer.book.worksheets:
                ws.freeze_panes = "A2"
                for col_cells in ws.columns:
                    col_letter = col_cells[0].column_letter
                    max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col_cells)
                    ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), 40)
        except Exception:
            pass
    output.seek(0)
    return output.getvalue()




def _safe_pct_text(v: Any) -> str:
    f = _safe_float(v)
    return "-" if f is None else f"{f:.2f}%"


def _load_watchlist_items() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if get_normalized_watchlist is None:
        return rows
    try:
        data = get_normalized_watchlist()
        if not isinstance(data, dict):
            return rows
        seen = set()
        for group_name, items in data.items():
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                code = _safe_str(item.get("code"))
                if not code:
                    continue
                key = (group_name, code)
                if key in seen:
                    continue
                seen.add(key)
                rows.append({
                    "群組": _safe_str(group_name) or "未分組",
                    "股票代號": code,
                    "股票名稱": _safe_str(item.get("name")) or code,
                    "市場別": _safe_str(item.get("market")) or "上市",
                })
    except Exception:
        return []
    return rows


def _parse_custom_stock_text(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for line in _safe_str(text).splitlines():
        s = line.strip()
        if not s:
            continue
        s = s.replace("，", ",").replace("	", ",")
        parts = [x.strip() for x in s.split(",") if x.strip()]
        if not parts:
            continue
        code = _safe_str(parts[0])
        name = _safe_str(parts[1]) if len(parts) >= 2 else code
        market = _safe_str(parts[2]) if len(parts) >= 3 else "上市"
        rows.append({"群組": "自訂", "股票代號": code, "股票名稱": name, "市場別": market})
    return rows


@st.cache_data(ttl=600, show_spinner=False)
def _get_stock_linkage_snapshot(stock_no: str, stock_name: str, market_type: str) -> dict[str, Any]:
    out = {
        "最新價": None,
        "漲跌%": None,
        "MA5": None,
        "MA20": None,
        "技術分": 0.0,
        "來源": "fallback",
    }
    stock_no = _safe_str(stock_no)
    stock_name = _safe_str(stock_name)
    market_type = _safe_str(market_type) or "上市"
    # 即時價
    if get_realtime_stock_info is not None:
        try:
            info = get_realtime_stock_info(stock_no, stock_name, market_type, refresh_token="macro_link")
            price = _safe_float(info.get("price"))
            chg_pct = _safe_float(info.get("pct_chg"))
            if chg_pct is None:
                chg_pct = _safe_float(info.get("change_percent"))
            if chg_pct is None:
                chg_pct = _safe_float(info.get("漲跌幅"))
            out["最新價"] = price
            out["漲跌%"] = chg_pct
            out["來源"] = _safe_str(info.get("price_source")) or out["來源"]
        except Exception:
            pass
    # 歷史技術面
    if get_history_data is not None:
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=80)
            try:
                df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=market_type, start_date=start_date, end_date=end_date)
            except TypeError:
                try:
                    df = get_history_data(stock_no=stock_no, stock_name=stock_name, market_type=market_type, start_dt=start_date, end_dt=end_date)
                except Exception:
                    df = get_history_data(code=stock_no, start_date=start_date, end_date=end_date)
            if isinstance(df, pd.DataFrame) and not df.empty:
                temp = df.copy()
                if "日期" not in temp.columns:
                    for c in temp.columns:
                        if str(c).lower() in {"date", "日期"}:
                            temp = temp.rename(columns={c: "日期"})
                            break
                for c in temp.columns:
                    if str(c).lower() == "close":
                        temp = temp.rename(columns={c: "收盤價"})
                if "收盤價" in temp.columns:
                    temp["收盤價"] = pd.to_numeric(temp["收盤價"], errors="coerce")
                    temp = temp.dropna(subset=["收盤價"]).copy()
                    if not temp.empty:
                        temp["MA5"] = temp["收盤價"].rolling(5).mean()
                        temp["MA20"] = temp["收盤價"].rolling(20).mean()
                        last = temp.iloc[-1]
                        px = out["最新價"] if out["最新價"] is not None else _safe_float(last.get("收盤價"))
                        ma5 = _safe_float(last.get("MA5"))
                        ma20 = _safe_float(last.get("MA20"))
                        out["MA5"] = ma5
                        out["MA20"] = ma20
                        tech = 0.0
                        if px is not None and ma5 is not None:
                            tech += 1.6 if px >= ma5 else -1.6
                        if px is not None and ma20 is not None:
                            tech += 2.4 if px >= ma20 else -2.4
                        chg_pct = out["漲跌%"]
                        if chg_pct is not None:
                            tech += 1.2 if chg_pct > 2 else (0.6 if chg_pct > 0 else (-1.0 if chg_pct < -2 else -0.4 if chg_pct < 0 else 0))
                        out["技術分"] = tech
        except Exception:
            pass
    return out


def _macro_stock_action(total_score: float, risk_level: str, stock_tech_score: float, stock_chg_pct: float | None) -> tuple[str, str, str]:
    c = stock_chg_pct if stock_chg_pct is not None else 0.0
    if risk_level in {"極高", "高"} and total_score <= 4:
        if stock_tech_score <= -1:
            return "賣出/減碼", "防守", "大盤高風險且個股轉弱"
        return "觀望", "保守", "大盤風險高，先觀察"
    if total_score >= 14 and stock_tech_score >= 2.0:
        if c >= 4:
            return "不追高，等拉回", "中性", "大盤偏多但個股短線過熱"
        return "可分批買進", "偏多", "大盤與個股同步偏強"
    if total_score >= 6 and stock_tech_score >= 0:
        return "小倉試單", "偏多", "大盤偏多，個股維持強勢"
    if -5 < total_score < 6:
        if stock_tech_score >= 2:
            return "條件式低接", "中性", "大盤震盪，僅強股可低接"
        if stock_tech_score <= -2:
            return "減碼觀望", "偏空", "大盤震盪但個股偏弱"
        return "觀望", "中性", "大盤訊號不夠集中"
    if stock_tech_score <= -1:
        return "減碼/出場", "偏空", "大盤偏空且個股轉弱"
    return "續抱不加碼", "保守", "大盤轉弱，先控倉"


def _build_stock_linkage_df(top_pick: dict[str, Any], ctx: dict[str, Any], use_watchlist: bool, custom_text: str) -> pd.DataFrame:
    rows = _load_watchlist_items() if use_watchlist else []
    rows.extend(_parse_custom_stock_text(custom_text))
    if not rows:
        return pd.DataFrame(columns=["群組", "股票代號", "股票名稱", "市場別", "最新價", "漲跌%", "MA5", "MA20", "個股技術分", "大盤模式", "大盤方向", "股神分數", "風險等級", "建議動作", "偏向", "推論"])
    out = []
    macro_score = _safe_float(top_pick.get("股神模式分數"), 0) or 0.0
    macro_dir = _safe_str(top_pick.get("推估方向"))
    macro_mode = _safe_str(top_pick.get("模式名稱"))
    risk_level = _safe_str(top_pick.get("風險等級"))
    for r in rows:
        snap = _get_stock_linkage_snapshot(r["股票代號"], r["股票名稱"], r["市場別"])
        action, bias, reason = _macro_stock_action(macro_score, risk_level, _safe_float(snap.get("技術分"), 0) or 0.0, _safe_float(snap.get("漲跌%")))
        out.append({
            "群組": r["群組"],
            "股票代號": r["股票代號"],
            "股票名稱": r["股票名稱"],
            "市場別": r["市場別"],
            "最新價": snap.get("最新價"),
            "漲跌%": snap.get("漲跌%"),
            "MA5": snap.get("MA5"),
            "MA20": snap.get("MA20"),
            "個股技術分": snap.get("技術分"),
            "大盤模式": macro_mode,
            "大盤方向": macro_dir,
            "股神分數": macro_score,
            "風險等級": risk_level,
            "建議動作": action,
            "偏向": bias,
            "推論": reason,
            "資料來源": snap.get("來源"),
        })
    df = pd.DataFrame(out)
    action_order = {"可分批買進": 1, "小倉試單": 2, "條件式低接": 3, "續抱不加碼": 4, "觀望": 5, "減碼觀望": 6, "賣出/減碼": 7, "減碼/出場": 8, "不追高，等拉回": 9}
    if not df.empty:
        df["_sort"] = df["建議動作"].map(lambda x: action_order.get(_safe_str(x), 99))
        df = df.sort_values(["_sort", "群組", "股票代號"], ascending=[True, True, True]).drop(columns=["_sort"])
    return df


def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    render_pro_hero(
        title="大盤走勢｜股神Pro因子強化版",
        subtitle="v26.8 緊急快顯版：先顯示目前大盤資訊，不自動等待外部 API；需要完整分析再手動啟動。",
    )
    st.caption(f"大盤投顧參考版：{MACRO_ADVISOR_VERSION}")

    status_msg = _safe_str(st.session_state.get(_k("status_msg"), ""))
    status_type = _safe_str(st.session_state.get(_k("status_type"), "info"))
    if status_msg:
        getattr(st, status_type if status_type in {"success", "warning", "error", "info"} else "info")(status_msg)

    c0, c1, c2, c3 = st.columns([1.2, 1.2, 1.2, 2.4])
    with c0:
        refresh_now = st.button("⚡ 更新即時/收盤大盤", use_container_width=True, type="primary")
    with c1:
        run_quick = st.button("產生快顯分析", use_container_width=True)
    with c2:
        run_full = st.button("產生完整分析", use_container_width=True)
    with c3:
        pred_date_input = st.date_input(
            "分析日期",
            value=st.session_state.get(_k("active_pred_date"), date.today()),
            key=_k("pred_date_fast"),
        )

    pred_date_text = pd.to_datetime(pred_date_input).strftime("%Y-%m-%d")
    st.session_state[_k("active_pred_date")] = pred_date_input

    if refresh_now:
        with st.spinner("正在手動更新 TWSE 即時 / 收盤大盤資料..."):
            ok, msg = _try_refresh_taiex_cache_now(pred_date_text)
        _set_status(msg, "success" if ok else "warning")
        st.rerun()

    # v26.8：預設完全不等待外部 API，只讀本機快取或安全預設值。
    pred_df, ctx = _predict_all_models_instant(pred_date_text, pd.DataFrame())

    taiex_live = ctx.get("taiex_now") if isinstance(ctx, dict) else {}
    live_label = "收盤快取 / 安全預設"
    if isinstance(taiex_live, dict):
        live_label = "盤中即時" if taiex_live.get("is_realtime") else (_safe_str(taiex_live.get("source")) or "收盤快取 / 安全預設")

    render_pro_kpi_row([
        {
            "label": f"目前大盤｜{live_label}",
            "value": f"{_safe_float((taiex_live or {}).get('close'), 0):,.2f}",
            "delta": f"{_safe_float((taiex_live or {}).get('pct'), 0):+.2f}%",
            "delta_class": "pro-kpi-delta-up" if _safe_float((taiex_live or {}).get("pct"), 0) >= 0 else "pro-kpi-delta-down",
        },
        {
            "label": "大盤資料日期",
            "value": _safe_str((taiex_live or {}).get("used_date") or (taiex_live or {}).get("date") or pred_date_text),
            "delta": _safe_str((taiex_live or {}).get("source")),
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "目前模式",
            "value": "零等待快顯",
            "delta": "不自動呼叫外部 API",
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "操作建議",
            "value": _safe_str(ctx.get("scenario", "先顯示再更新")),
            "delta": "需完整因子請按下方按鈕",
            "delta_class": "pro-kpi-delta-flat",
        },
    ])

    render_pro_info_card(
        "大盤資料狀態",
        [
            ("目前資料來源", _safe_str((taiex_live or {}).get("source")) or "本機快取 / 安全預設", ""),
            ("目前指數", f"{_safe_float((taiex_live or {}).get('close'), 0):,.2f}", ""),
            ("漲跌幅", f"{_safe_float((taiex_live or {}).get('pct'), 0):+.2f}%", ""),
            ("說明", "v26.8 預設不自動等待 Yahoo / Stooq / TWSE / Google News / TAIFEX；避免頁面一直轉圈。", ""),
        ],
        chips=["零等待", "先顯示", "手動更新"],
    )

    st.info("頁面已先載入完成。若要補抓目前大盤，按「更新即時/收盤大盤」；若要跑模型，按「產生快顯分析」或「產生完整分析」。")

    if not run_quick and not run_full:
        st.stop()

    base_df = _get_state_df()
    if base_df.empty:
        base_df = pd.DataFrame()

    if run_full:
        with st.spinner("正在產生完整大盤趨勢分析，會讀取較多外部資料，可能較久..."):
            pred_df, ctx = _predict_all_models(pred_date_text, base_df)
    else:
        with st.spinner("正在產生快顯大盤趨勢分析..."):
            pred_df, ctx = _predict_all_models_quick(pred_date_text, base_df)

    top_pick = pred_df.iloc[0].to_dict() if not pred_df.empty else {}
    scoreboard = _build_scoreboard(base_df)

    render_pro_kpi_row([
        {"label": "推估日期", "value": pred_date_text, "delta": ctx.get("scenario", ""), "delta_class": "pro-kpi-delta-flat"},
        {"label": "最佳模式", "value": _safe_str(top_pick.get("模式名稱")), "delta": _safe_str(top_pick.get("建議動作")), "delta_class": "pro-kpi-delta-flat"},
        {"label": "股神分數", "value": f"{_safe_float(top_pick.get('股神模式分數'), 0):.2f}", "delta": _safe_str(top_pick.get("推估方向")), "delta_class": "pro-kpi-delta-flat"},
        {"label": "信心度", "value": f"{_safe_float(top_pick.get('股神信心度'), 0):.1f}%", "delta": _safe_str(top_pick.get("風險等級")), "delta_class": "pro-kpi-delta-flat"},
        {"label": "預估點數", "value": f"{_safe_float(top_pick.get('預估漲跌點'), 0):.0f} 點", "delta": f"區間 { _safe_float(top_pick.get('預估低點'), 0):.0f} ~ { _safe_float(top_pick.get('預估高點'), 0):.0f}", "delta_class": "pro-kpi-delta-flat"},
    ])

    render_pro_section("大盤模式推估結果")
    if pred_df is not None and not pred_df.empty:
        show_cols = [
            "模式名稱", "推估方向", "建議動作", "股神模式分數", "股神信心度",
            "預估漲跌點", "預估低點", "預估高點", "風險等級", "推估理由"
        ]
        st.dataframe(pred_df[[c for c in show_cols if c in pred_df.columns]], use_container_width=True, hide_index=True)
    else:
        st.warning("目前沒有產生推估結果。")

    render_pro_section("資料來源明細")
    st.write(_safe_str(ctx.get("source_status", "")))
    with st.expander("完整 ctx 除錯資料", expanded=False):
        st.json({k: v for k, v in ctx.items() if k not in {"news_rows"}})

if __name__ == "__main__":
    main()

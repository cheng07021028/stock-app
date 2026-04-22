# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Any
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

PAGE_TITLE = "大盤走勢｜股神Pro因子強化版"
PFX = "macro_godpro_factor_"

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
    "誤判主因類別",
    "誤判主因",
    "收盤檢討",
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
        "實際漲跌點", "實際高點", "實際低點", "點數誤差",
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
        "實際方向", "誤判主因類別", "誤判主因", "收盤檢討", "備註"
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
def _fetch_stooq(symbol: str, pred_date_text: str) -> dict[str, Any]:
    # 歷史/當日統一走 stooq CSV，避免太多來源造成延遲
    pred_dt = pd.to_datetime(pred_date_text, errors="coerce")
    if pd.isna(pred_dt):
        pred_dt = pd.Timestamp.today()
    start = (pred_dt - pd.Timedelta(days=40)).strftime("%Y%m%d")
    end = pred_dt.strftime("%Y%m%d")
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
    try:
        df = pd.read_csv(url)
        if df.empty:
            return {}
        df.columns = [str(c).strip().lower() for c in df.columns]
        if "date" not in df.columns:
            return {}
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for c in ["open", "high", "low", "close", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date")
        df = df[(df["date"] >= pd.to_datetime(start)) & (df["date"] <= pd.to_datetime(end))]
        if df.empty:
            return {}
        row = df.iloc[-1]
        prev_close = pd.to_numeric(df["close"], errors="coerce").dropna().iloc[-2] if len(df.dropna(subset=["close"])) >= 2 else None
        close = _safe_float(row.get("close"))
        pct = ((close - prev_close) / prev_close * 100) if (close is not None and prev_close not in [None, 0]) else None
        ma5 = df["close"].tail(5).mean() if "close" in df.columns else None
        ma20 = df["close"].tail(20).mean() if "close" in df.columns else None
        return {
            "date": row.get("date"),
            "open": _safe_float(row.get("open")),
            "high": _safe_float(row.get("high")),
            "low": _safe_float(row.get("low")),
            "close": close,
            "volume": _safe_float(row.get("volume")),
            "pct": pct,
            "ma5": _safe_float(ma5),
            "ma20": _safe_float(ma20),
        }
    except Exception:
        return {}


@st.cache_data(ttl=900, show_spinner=False)
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
            resp = requests.get(url, timeout=12)
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
def _fetch_json(url: str, timeout: int = 12) -> Any:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


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


@st.cache_data(ttl=1800, show_spinner=False)
def _fetch_twse_institutional(pred_date_text: str) -> dict[str, Any]:
    dt = pd.to_datetime(pred_date_text, errors='coerce')
    if pd.isna(dt):
        dt = pd.Timestamp.today()
    ymd = dt.strftime('%Y%m%d')
    urls = [
        f'https://www.twse.com.tw/rwd/zh/fund/BFI82U?dayDate={ymd}&type=day&response=json',
        f'https://www.twse.com.tw/fund/BFI82U?dayDate={ymd}&type=day&response=json',
    ]
    data = None
    for u in urls:
        data = _fetch_json(u)
        if data:
            break
    out = {"foreign": None, "total3": None, "source": "fallback"}
    if not data or 'data' not in data:
        return out
    foreign = None
    total3 = None
    for row in data.get('data', []):
        label = _safe_str(row[0]) if isinstance(row, list) and row else ''
        if '外資' in label and '自營商' not in label and len(row) >= 4:
            nums = [_extract_num(x, 100000000) for x in row[1:4]]
            nums = [x for x in nums if x is not None]
            foreign = sum(nums) if nums else foreign
        if ('三大法人' in label or '合計' in label) and len(row) >= 4:
            nums = [_extract_num(x, 100000000) for x in row[1:4]]
            nums = [x for x in nums if x is not None]
            total3 = sum(nums) if nums else total3
    out.update({"foreign": foreign, "total3": total3, "source": 'twse'})
    return out


@st.cache_data(ttl=1800, show_spinner=False)
def _fetch_taifex_sentiment(pred_date_text: str) -> dict[str, Any]:
    # 公開來源不穩，抓不到就自動降級
    out = {"foreign_fut_net": None, "pcr": None, "source": 'fallback'}
    dt = pd.to_datetime(pred_date_text, errors='coerce')
    if pd.isna(dt):
        dt = pd.Timestamp.today()
    date_slash = dt.strftime('%Y/%m/%d')
    # put/call 嘗試公開CSV/json樣式端點
    candidates = [
        f'https://www.taifex.com.tw/cht/3/pcRatio?queryStartDate={date_slash}&queryEndDate={date_slash}',
        f'https://www.taifex.com.tw/cht/3/futContractsDate?queryDate={date_slash}',
    ]
    for u in candidates:
        try:
            txt = requests.get(u, timeout=10, headers={"User-Agent": "Mozilla/5.0"}).text
            if 'Put/Call Ratio' in txt or 'PCR' in txt:
                import re
                m = re.search(r'(?:Put/Call Ratio|PCR)[^0-9]{0,20}([0-9]+\.[0-9]+)', txt, re.I)
                if m:
                    out['pcr'] = float(m.group(1))
                    out['source'] = 'taifex_html'
                    break
        except Exception:
            pass
    return out


@st.cache_data(ttl=1800, show_spinner=False)
def _fetch_twse_margin(pred_date_text: str) -> dict[str, Any]:
    dt = pd.to_datetime(pred_date_text, errors='coerce')
    if pd.isna(dt):
        dt = pd.Timestamp.today()
    ymd = dt.strftime('%Y%m%d')
    out = {"margin_change": None, "short_change": None, "source": 'fallback'}
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
            # 末行常有合計；抓可解析的最後一筆
            cand = None
            for row in reversed(rows):
                vals = [_extract_num(x) for x in row]
                if sum(v is not None for v in vals) >= 2:
                    cand = row
                    break
            if cand:
                nums = [_extract_num(x) for x in cand]
                nums = [x for x in nums if x is not None]
                if len(nums) >= 2:
                    out['margin_change'] = nums[-2] / 100000000 if abs(nums[-2]) > 10000 else nums[-2]
                    out['short_change'] = nums[-1]
                    out['source'] = 'twse_margin'
                    return out
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
        # 融資大增偏熱、融資下降偏冷
        margin_score += max(-4, min(4, -margin_change / 12.0))
    if short_change is not None:
        margin_score += max(-3, min(3, short_change / 15000.0))

    return {
        'foreign_amt': foreign_amt, 'total3_amt': total3_amt, 'foreign_fut_net': foreign_fut_net, 'pcr': pcr,
        'margin_change': margin_change, 'short_change': short_change,
        'foreign_score': foreign_score, 'futures_score': futures_score, 'margin_score': margin_score,
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


def _calc_market_context(pred_date_text: str) -> dict[str, Any]:
    twii = _fetch_stooq("^twii", pred_date_text)
    nas = _fetch_stooq("^ixic", pred_date_text)
    sox = _fetch_stooq("^sox", pred_date_text)
    spx = _fetch_stooq("^spx", pred_date_text)
    adr = _fetch_stooq("tsm.us", pred_date_text)
    es = _fetch_stooq("es.f", pred_date_text)
    nq = _fetch_stooq("nq.f", pred_date_text)
    vix = _fetch_stooq("^vix", pred_date_text)
    usdtwd = _fetch_stooq("usdtwd", pred_date_text)
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

    source_status = f"法人:{inst.get('source','fallback')}｜期權:{futopt.get('source','fallback')}｜融資券:{margin.get('source','fallback')}"

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
    }



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
        "備註": "",
    }


def _predict_all_models(pred_date_text: str, records_df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    ctx = _calc_market_context(pred_date_text)
    weights = _get_dynamic_weights(records_df)
    rows = [_predict_for_model(model, ctx, weights, pred_date_text) for model in MODEL_NAMES]
    pred_df = pd.DataFrame(rows)
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
    base = _safe_float(src.get("大盤基準點"))
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
        "加權漲跌%", "VIX", "美元台幣", "NASDAQ漲跌%", "SOX漲跌%", "SP500漲跌%", "台積電ADR漲跌%", "ES夜盤漲跌%", "NQ夜盤漲跌%"
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

    if _k("status_msg") not in st.session_state:
        st.session_state[_k("status_msg")] = ""
        st.session_state[_k("status_type")] = "info"

    render_pro_hero(
        title="大盤走勢｜股神Pro因子強化版",
        subtitle="補強法人 / 期貨選擇權 / 融資融券 / 類股輪動 / 事件因子，抓不到資料時自動降級。",
    )

    status_msg = _safe_str(st.session_state.get(_k("status_msg"), ""))
    status_type = _safe_str(st.session_state.get(_k("status_type"), "info"))
    if status_msg:
        getattr(st, status_type if status_type in {"success", "warning", "error", "info"} else "info")(status_msg)

    top = st.columns([1.1, 1.2, 1.0, 1.0, 2.2])
    with top[0]:
        if st.button("🔄 重新載入紀錄", use_container_width=True):
            df, err = _read_records_from_github()
            _save_state_df(df)
            _set_status("已重新載入紀錄" if not err else err, "success" if not err else "warning")
            st.rerun()
    with top[1]:
        if st.button("🧹 清除資料快取", use_container_width=True):
            try:
                _fetch_stooq.clear()
                _search_news_headlines.clear()
            except Exception:
                pass
            _set_status("資料快取已清除", "success")
            st.rerun()
    with top[2]:
        quick_mode = st.toggle("快顯模式", value=True, key=_k("quick_mode"))
    with top[3]:
        auto_save = st.toggle("儲存即同步", value=False, key=_k("auto_save"))
    with top[4]:
        pred_date = st.date_input("推估日期", value=date.today(), key=_k("pred_date"))

    base_df = _get_state_df()
    if base_df.empty:
        df, _ = _read_records_from_github()
        _save_state_df(df)
        base_df = _get_state_df()

    pred_date_text = pd.to_datetime(pred_date).strftime("%Y-%m-%d")
    pred_df, ctx = _predict_all_models(pred_date_text, base_df)
    top_pick = pred_df.iloc[0].to_dict() if not pred_df.empty else {}
    scoreboard = _build_scoreboard(base_df)

    render_pro_kpi_row([
        {"label": "推估日期", "value": pred_date_text, "delta": ctx.get("scenario", ""), "delta_class": "pro-kpi-delta-flat"},
        {"label": "最佳模式", "value": _safe_str(top_pick.get("模式名稱")), "delta": _safe_str(top_pick.get("建議動作")), "delta_class": "pro-kpi-delta-flat"},
        {"label": "股神分數", "value": f"{_safe_float(top_pick.get('股神模式分數'), 0):.2f}", "delta": _safe_str(top_pick.get("推估方向")), "delta_class": "pro-kpi-delta-flat"},
        {"label": "信心度", "value": f"{_safe_float(top_pick.get('股神信心度'), 0):.1f}%", "delta": _safe_str(top_pick.get("風險等級")), "delta_class": "pro-kpi-delta-flat"},
        {"label": "預估點數", "value": f"{_safe_float(top_pick.get('預估漲跌點'), 0):.0f} 點", "delta": f"區間 { _safe_float(top_pick.get('預估低點'), 0):.0f} ~ { _safe_float(top_pick.get('預估高點'), 0):.0f}", "delta_class": "pro-kpi-delta-flat"},
    ])


    factor_cols = st.columns(4)
    with factor_cols[0]:
        st.metric("外資買賣超(億)", "-" if ctx.get("foreign_amt") is None else f"{ctx.get('foreign_amt'):.1f}", delta=f"三大法人 {0 if ctx.get('total3_amt') is None else ctx.get('total3_amt'):.1f}")
    with factor_cols[1]:
        st.metric("PCR", "-" if ctx.get("pcr") is None else f"{ctx.get('pcr'):.2f}", delta=f"期貨估分 {pred_df.iloc[0]['期貨選擇權估分']:.1f}" if not pred_df.empty else "")
    with factor_cols[2]:
        st.metric("融資增減(億)", "-" if ctx.get("margin_change") is None else f"{ctx.get('margin_change'):.2f}", delta=f"融券 {0 if ctx.get('short_change') is None else ctx.get('short_change'):.0f}")
    with factor_cols[3]:
        st.metric("因子來源", ctx.get("source_status", "fallback"))


    a1, a2 = st.columns([1.4, 1.2])
    with a1:
        render_pro_section("股神模式結論")
        chip_text = " ｜ ".join([x for x in [_safe_str(top_pick.get("模式名稱")), _safe_str(top_pick.get("市場情境")), _safe_str(top_pick.get("方向強度"))] if x])
        if chip_text:
            st.caption(chip_text)
        g1, g2, g3, g4 = st.columns(4)
        with g1:
            st.metric("建議動作", _safe_str(top_pick.get("建議動作")) or "-", _safe_str(top_pick.get("建議倉位")) or None)
        with g2:
            st.metric("是否適合進場", _safe_str(top_pick.get("是否適合進場")) or "-", _safe_str(top_pick.get("是否適合續抱")) or None)
        with g3:
            st.metric("是否適合減碼/出場", f"{_safe_str(top_pick.get('是否適合減碼')) or '-'} / {_safe_str(top_pick.get('是否適合出場')) or '-'}", _safe_str(top_pick.get("風險等級")) or None)
        with g4:
            st.metric("主要風險", _safe_str(top_pick.get("主要風險")) or "-")
        st.text_area("股神推論邏輯", value=_safe_str(top_pick.get("股神推論邏輯")), height=180, disabled=True)
        st.text_area("進場確認條件", value=_safe_str(top_pick.get("進場確認條件")), height=95, disabled=True)
        st.text_area("出場警訊", value=_safe_str(top_pick.get("出場警訊")), height=95, disabled=True)
    with a2:
        render_pro_section("本次模式比較")
        show_cols = [
            "模式名稱", "推估方向", "方向強度", "建議動作", "股神模式分數", "股神信心度",
            "預估漲跌點", "風險等級", "技術面因子分", "美股因子分", "夜盤因子分",
            "籌碼面因子分", "事件因子分", "國際新聞風險分",
        ]
        st.dataframe(_format_pred_df(pred_df[show_cols]), use_container_width=True, hide_index=True)
        with st.expander("查看 Pro 因子細節", expanded=False):
            st.write(f"**強勢族群**：{ctx.get('sector_strong','')}  ")
            st.write(f"**弱勢族群**：{ctx.get('sector_weak','')}  ")
            st.write(f"**重大事件清單**：{ctx.get('event_list','')}  ")
            st.write(f"**因子來源狀態**：{ctx.get('source_status','')}  ")


    stock_link_df = _build_stock_linkage_df(top_pick, ctx, bool(st.session_state.get(_k("use_watchlist"), True)), _safe_str(st.session_state.get(_k("custom_stock_text"), "")))

    tabs = st.tabs(["🎯 個股連動", "📌 儲存推估", "🧪 回填實際結果", "🏆 模式排行榜", "📚 歷史紀錄", "📤 Excel匯出"])

    with tabs[0]:
        render_pro_section("個股連動｜真正式籌碼 + 大盤風險同步")
        left_link, right_link = st.columns([1.1, 1.4])
        with left_link:
            st.toggle("納入 4_自選股中心 watchlist", value=True, key=_k("use_watchlist"))
            st.text_area("自訂股票（每行：代號,名稱,市場）", value="", height=120, key=_k("custom_stock_text"), placeholder="2330,台積電,上市\n2454,聯發科,上市")
            st.caption("邏輯：先用本頁最佳模式判斷大盤，再疊加個股技術分，輸出買進 / 觀望 / 減碼建議。")
        with right_link:
            if stock_link_df.empty:
                st.info("目前沒有可連動的股票。可開啟 watchlist 或輸入自訂股票。")
            else:
                k1, k2, k3 = st.columns(3)
                with k1:
                    st.metric("可買進", int(stock_link_df[stock_link_df["建議動作"].isin(["可分批買進", "小倉試單", "條件式低接"])].shape[0]))
                with k2:
                    st.metric("觀望/續抱", int(stock_link_df[stock_link_df["建議動作"].isin(["觀望", "續抱不加碼", "不追高，等拉回"])].shape[0]))
                with k3:
                    st.metric("減碼/出場", int(stock_link_df[stock_link_df["建議動作"].isin(["減碼觀望", "賣出/減碼", "減碼/出場"])].shape[0]))
        if not stock_link_df.empty:
            st.dataframe(stock_link_df, use_container_width=True, hide_index=True)

    with tabs[1]:
        render_pro_section("儲存本次推估")
        st.caption("會將 5 套模式一起存入紀錄檔，後續可比較哪一套最近最準。")
        c1, c2 = st.columns([1.2, 4])
        with c1:
            if st.button("💾 儲存本次推估", use_container_width=True, type="primary"):
                merged = _upsert_predictions(base_df, pred_df)
                _save_state_df(merged)
                if auto_save:
                    ok, msg = _write_records_to_github(merged)
                    _set_status(msg, "success" if ok else "error")
                else:
                    _set_status("已存入本頁狀態，尚未同步 GitHub", "success")
                st.rerun()
        with c2:
            if st.button("☁️ 立即同步 GitHub", use_container_width=True):
                merged = _upsert_predictions(_get_state_df(), pred_df)
                _save_state_df(merged)
                ok, msg = _write_records_to_github(merged)
                _set_status(msg, "success" if ok else "error")
                st.rerun()
        st.dataframe(_format_pred_df(pred_df[[c for c in RECORD_COLUMNS if c in pred_df.columns][:25]]), use_container_width=True, hide_index=True)

    with tabs[2]:
        render_pro_section("回填實際結果 / 追蹤準確率")
        hist_df = _get_state_df()
        fill_date = st.selectbox("選擇要回填的日期", options=sorted(hist_df["推估日期"].dropna().astype(str).unique().tolist(), reverse=True) if not hist_df.empty else [pred_date_text], key=_k("fill_date"))
        current_fill = hist_df[hist_df["推估日期"].astype(str) == str(fill_date)].copy() if not hist_df.empty else pd.DataFrame()
        if current_fill.empty:
            st.info("目前這個日期還沒有儲存推估。")
        else:
            edit_df = current_fill[[
                "record_id", "模式名稱", "推估方向", "建議動作", "預估漲跌點", "預估高點", "預估低點",
                "實際方向", "實際漲跌點", "實際高點", "實際低點", "建議動作是否合適", "誤判主因類別", "誤判主因", "收盤檢討", "備註"
            ]].copy()
            edited = st.data_editor(
                edit_df,
                use_container_width=True,
                hide_index=True,
                key=_k("actual_editor"),
                column_config={
                    "record_id": st.column_config.TextColumn("record_id", disabled=True),
                    "模式名稱": st.column_config.TextColumn("模式名稱", disabled=True),
                    "推估方向": st.column_config.TextColumn("推估方向", disabled=True),
                    "建議動作": st.column_config.TextColumn("建議動作", disabled=True),
                    "預估漲跌點": st.column_config.NumberColumn("預估漲跌點", disabled=True, format="%.1f"),
                    "預估高點": st.column_config.NumberColumn("預估高點", disabled=True, format="%.1f"),
                    "預估低點": st.column_config.NumberColumn("預估低點", disabled=True, format="%.1f"),
                    "實際方向": st.column_config.SelectboxColumn("實際方向", options=[""] + DIRECTION_OPTIONS),
                    "實際漲跌點": st.column_config.NumberColumn("實際漲跌點", format="%.1f"),
                    "實際高點": st.column_config.NumberColumn("實際高點", format="%.1f"),
                    "實際低點": st.column_config.NumberColumn("實際低點", format="%.1f"),
                    "建議動作是否合適": st.column_config.CheckboxColumn("建議動作是否合適"),
                    "誤判主因類別": st.column_config.SelectboxColumn("誤判主因類別", options=ERROR_CAUSES),
                    "誤判主因": st.column_config.TextColumn("誤判主因", width="large"),
                    "收盤檢討": st.column_config.TextColumn("收盤檢討", width="large"),
                    "備註": st.column_config.TextColumn("備註", width="large"),
                },
            )
            if st.button("✅ 套用回填結果", use_container_width=True):
                master = hist_df.copy()
                edit_map = {str(r["record_id"]): dict(r) for _, r in edited.iterrows()}
                for idx in master.index:
                    rec_id = _safe_str(master.at[idx, "record_id"])
                    if rec_id not in edit_map:
                        continue
                    src = edit_map[rec_id]
                    for c in ["實際方向", "實際漲跌點", "實際高點", "實際低點", "建議動作是否合適", "誤判主因類別", "誤判主因", "收盤檢討", "備註"]:
                        master.at[idx, c] = src.get(c)
                    applied = _apply_actual_result(master.loc[idx].to_dict())
                    for k2, v2 in applied.items():
                        if k2 in master.columns:
                            master.at[idx, k2] = v2
                _save_state_df(master)
                if auto_save:
                    ok, msg = _write_records_to_github(master)
                    _set_status(msg, "success" if ok else "error")
                else:
                    _set_status("已套用回填結果，尚未同步 GitHub", "success")
                st.rerun()

    with tabs[3]:
        render_pro_section("模式排行榜 / 自我修正參考")
        scoreboard = _build_scoreboard(_get_state_df())
        if scoreboard.empty:
            st.info("尚未累積足夠回填結果，排行榜還沒有資料。")
        else:
            st.dataframe(scoreboard, use_container_width=True, hide_index=True)
            best = scoreboard.iloc[0]
            st.success(f"目前最近最強模式：{best['模式名稱']}｜方向命中率 {best['方向命中率']:.1f}%｜平均點數誤差 {best['平均點數誤差'] if not pd.isna(best['平均點數誤差']) else 0:.1f}")

    with tabs[4]:
        render_pro_section("歷史紀錄")
        hist = _get_state_df().copy()
        if hist.empty:
            st.info("目前沒有歷史紀錄。")
        else:
            left, right, right2 = st.columns([1.2, 1.2, 1.6])
            with left:
                mode_filter = st.selectbox("模式篩選", ["全部"] + MODEL_NAMES, key=_k("hist_mode_filter"))
            with right:
                dir_filter = st.selectbox("方向篩選", ["全部"] + DIRECTION_OPTIONS, key=_k("hist_dir_filter"))
            with right2:
                kw = st.text_input("搜尋日期 / 檢討 / 風險 / 備註", value="", key=_k("hist_kw"))
            if mode_filter != "全部":
                hist = hist[hist["模式名稱"].astype(str) == mode_filter].copy()
            if dir_filter != "全部":
                hist = hist[hist["推估方向"].astype(str) == dir_filter].copy()
            if kw:
                mask = (
                    hist["推估日期"].astype(str).str.contains(kw, case=False, na=False)
                    | hist["收盤檢討"].astype(str).str.contains(kw, case=False, na=False)
                    | hist["主要風險"].astype(str).str.contains(kw, case=False, na=False)
                    | hist["備註"].astype(str).str.contains(kw, case=False, na=False)
                )
                hist = hist[mask].copy()
            hist = hist.sort_values(["推估日期", "模式名稱"], ascending=[False, True])
            show_cols = [
                "推估日期", "模式名稱", "市場情境", "推估方向", "建議動作", "股神模式分數", "股神信心度", "預估漲跌點",
                "實際方向", "實際漲跌點", "方向是否命中", "區間是否命中", "點數誤差", "建議動作是否合適", "誤判主因類別", "收盤檢討", "備註"
            ]
            st.dataframe(hist[show_cols], use_container_width=True, hide_index=True)

    with tabs[5]:
        render_pro_section("Excel 匯出")
        export_bytes = _build_export_bytes(_get_state_df(), _build_scoreboard(_get_state_df()), pred_df, stock_link_df)
        st.download_button(
            "📥 下載 Excel（歷史紀錄 / 模式排行榜 / 本次推估）",
            data=export_bytes,
            file_name=f"大盤走勢_股神Pro_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )


if __name__ == "__main__":
    main()

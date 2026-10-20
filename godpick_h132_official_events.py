# -*- coding: utf-8 -*-
"""V191-H132.0: read-only TWSE official material disclosures for six-engine research.

Fixed HTTPS OpenAPI endpoint only. One user-triggered fetch; no background polling,
trade decisions, model-trained sentiment, historical point-in-time simulation,
or storage of cloud credentials. Publication is distinct from event occurrence.
"""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Callable
from zoneinfo import ZoneInfo
import hashlib
import json
import re

import pandas as pd

from godpick_h130_six_engine import _code, _text

VERSION = "V191-H132.0"
SOURCE_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap04_L"
SOURCE_LABEL = "TWSE OpenAPI｜上市公司每日重大訊息（非上櫃／非一般新聞）"
TAIPEI = ZoneInfo("Asia/Taipei")
MAX_CONTENT_BYTES = 16 * 1024 * 1024
MAX_SOURCE_ROWS = 30000
MAX_CODES = 30
MAX_EVENTS = 300
EVENT_COLS = [
    "股票代號", "公司名稱", "發布時間_台北", "公告日期", "事實發生日", "出表日期",
    "公告類別", "公告標題", "公告原文節錄", "符合條款", "來源網址", "公告識別碼",
    "資料狀態", "研究提醒", "來源識別",
]


def _summary(status: str, *, message: str, fetched_at: datetime, rows: int = 0,
             selected: int = 0, excluded: int = 0) -> pd.DataFrame:
    return pd.DataFrame([{
        "資料狀態": status, "說明": message, "資料來源": SOURCE_LABEL,
        "抓取時間_台北": fetched_at.isoformat(timespec="seconds"),
        "原始資料筆數": rows, "符合條件筆數": selected, "隔離筆數": excluded,
        "自動交易授權": "無｜新聞不得覆蓋原有H109/H126決策",
    }])


def _roc_date(value: Any) -> date | None:
    s = str(value or "").strip()
    if not s:
        return None
    digits = re.sub(r"[/\-.\s]", "", s)
    try:
        if len(digits) == 7 and digits.isdigit():
            year = int(digits[:3]) + 1911
            return date(year, int(digits[3:5]), int(digits[5:7]))
        if len(digits) == 8 and digits.isdigit():
            return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
    except ValueError:
        pass
    return None


def _hms(value: Any) -> time | None:
    value = str(value if value is not None else "").strip()
    if not value:
        return None
    try:
        if ":" in value:
            pieces = value.split(":")
            if len(pieces) == 2:
                return time(int(pieces[0]), int(pieces[1]), 0)
            if len(pieces) == 3:
                return time(int(pieces[0]), int(pieces[1]), int(pieces[2]))
            return None
        if not value.isascii() or not value.isdigit() or len(value) > 6:
            return None
        value = value.zfill(6)
        return time(int(value[:2]), int(value[2:4]), int(value[4:6]))
    except ValueError:
        return None


def _classify(title: str) -> tuple[str, str]:
    # Pure rule-based *topic* classification, never a bullish/bearish prediction.
    rules = (
        (r"澄清|更正|補充公告", "更正／澄清", "核對原公告與更正內容，不可沿用舊事件方向"),
        (r"訴訟|搜索|停工|違約|處罰|裁罰|處分", "法遵／營運風險", "核對金額、財務曝險及影響期間"),
        (r"財務報告|自結|營收|獲利|損益|財報", "財報／營運", "核對實際數字、基期與市場預期差"),
        (r"重大合約|接獲訂單|取得訂單|重大訂單|供貨合約", "訂單／合約", "核對實際金額、履約期間與確認收入時點"),
        (r"增資|減資|私募|可轉換|發行新股|籌資", "資本／股本", "核對稀釋、資金用途及生效條件"),
        (r"董事|監察|股東會|法人代表|經理人", "公司治理", "核對公告決議與治理風險"),
    )
    for pattern, category, caution in rules:
        if re.search(pattern, title, flags=re.IGNORECASE):
            return category, caution
    return "其他公告", "確認原文事件是否具財務實質性；不可只看主旨推估行情"


def _clean_record(record: dict[str, Any]) -> dict[str, Any]:
    return {str(k).strip(): v for k, v in record.items()}


def parse_twse_disclosures(payload: Any, *, asof: date, fetched_at: datetime,
                           codes: list[str] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fail-closed as-of rules. A *current* API response is not a historic PIT archive.

    Fetched_at is the actual completed HTTP receipt time (timezone aware); neither
    content timestamp nor the event's occurrence date substitutes for publication.
    """
    if fetched_at.tzinfo is None or fetched_at.utcoffset() is None:
        raise ValueError("FETCH_TIME_MUST_HAVE_TIMEZONE")
    fetched = fetched_at.astimezone(TAIPEI)
    empty = pd.DataFrame(columns=EVENT_COLS)
    if not isinstance(asof, date) or isinstance(asof, datetime):
        raise ValueError("ASOF_DATE_REQUIRED")
    if asof > fetched.date():
        return empty, _summary("FUTURE_ASOF_REFUSED", message="研究截止日在抓取日之後，禁止查詢未來公告", fetched_at=fetched)
    if asof < fetched.date():
        return empty, _summary("HISTORICAL_PIT_UNAVAILABLE", message="每日API當期快照不能重建歷史研究當日完整訊息；請使用當時留存的原始時間戳證據", fetched_at=fetched)
    if not isinstance(payload, list) or len(payload) > MAX_SOURCE_ROWS:
        return empty, _summary("SOURCE_SCHEMA_INVALID", message="公告資料不是有限長度的JSON陣列", fetched_at=fetched)
    if codes is not None:
        requested = [_code(c) for c in codes]
        if len(codes) > MAX_CODES or any(not c for c in requested):
            return empty, _summary("INVALID_CODES", message="最多30檔有效台股代號", fetched_at=fetched, rows=len(payload))
        allowed = set(requested)
    else:
        allowed = None
    output = []
    excluded = 0
    for raw in payload:
        if not isinstance(raw, dict):
            excluded += 1
            continue
        r = _clean_record(raw)
        code = _code(r.get("公司代號"))
        if not code:
            excluded += 1
            continue
        if allowed is not None and code not in allowed:
            continue
        published = _roc_date(r.get("發言日期"))
        published_time = _hms(r.get("發言時間"))
        batch_day = _roc_date(r.get("出表日期"))
        occurred = _roc_date(r.get("事實發生日"))
        title = _text(r.get("主旨"))
        if not published or not title or len(title) > 600 or not batch_day:
            excluded += 1
            continue
        if published > asof or batch_day > asof or published > batch_day:
            excluded += 1
            continue
        when = datetime.combine(published, published_time or time(0), tzinfo=TAIPEI)
        # Time missing on the current day is ambiguous, not a verified publication.
        if published_time and when > fetched:
            excluded += 1
            continue
        status = ("TIME_UNKNOWN_RECHECK" if published_time is None
                  else "BATCH_STALE_RECHECK" if (asof - batch_day).days > 3
                  else "BATCH_PRIOR_DAY_RECHECK" if batch_day != asof
                  else "OFFICIAL_ENDPOINT_RECEIVED")
        category, warning = _classify(title)
        digest = hashlib.sha256(f"{code}|{published}|{published_time}|{title}".encode("utf-8")).hexdigest()[:16]
        output.append({
            "股票代號": code, "公司名稱": _text(r.get("公司名稱")),
            "發布時間_台北": when.isoformat(timespec="seconds") if published_time else "發布時間缺漏",
            "公告日期": published.isoformat(),
            "事實發生日": occurred.isoformat() if occurred else "UNKNOWN（不得當發布日期）",
            "出表日期": batch_day.isoformat(),
            "公告類別": category, "公告標題": title,
            "公告原文節錄": _text(r.get("說明"))[:240],
            "符合條款": _text(r.get("符合條款")), "來源網址": SOURCE_URL,
            "公告識別碼": digest, "資料狀態": status,
            "研究提醒": warning + "；非方向預測，必須核對正式交易閘門",
            "來源識別": SOURCE_LABEL,
        })
    events = pd.DataFrame(output, columns=EVENT_COLS)
    if not events.empty:
        events = (events.drop_duplicates("公告識別碼")
                  .sort_values(["公告日期", "發布時間_台北", "股票代號"], ascending=[False, False, True])
                  .head(MAX_EVENTS).reset_index(drop=True))
    status = "RECEIVED_WITH_EXCLUSIONS" if excluded else "RECEIVED"
    if not events.empty and events["資料狀態"].ne("OFFICIAL_ENDPOINT_RECEIVED").all():
        status = "RECEIVED_RECHECK_REQUIRED"
    if events.empty:
        status = "NO_MATCHING_EVENTS"
    return events, _summary(status, message="只解析TWSE上市公司當期公開重大訊息；不保證原文事件對股價方向的影響；請逐項核對", fetched_at=fetched,
                            rows=len(payload), selected=len(events), excluded=excluded)


def _read_limited_json(response: Any) -> Any:
    status = getattr(response, "status_code", 0)
    if status != 200:
        raise ValueError("HTTP_NON_200")
    headers = getattr(response, "headers", {}) or {}
    location = headers.get("Location") or headers.get("location")
    if 300 <= status < 400 or location:
        raise ValueError("REDIRECT_REFUSED")
    content_type = str(headers.get("Content-Type", "")).lower()
    if "json" not in content_type:
        raise ValueError("NOT_JSON_CONTENT_TYPE")
    size = headers.get("Content-Length")
    if size is not None and int(size) > MAX_CONTENT_BYTES:
        raise ValueError("DATA_OVERSIZE")
    body = bytearray()
    for chunk in response.iter_content(chunk_size=128 * 1024):
        if chunk:
            body.extend(chunk)
            if len(body) > MAX_CONTENT_BYTES:
                raise ValueError("DATA_OVERSIZE")
    return json.loads(bytes(body).decode("utf-8-sig"))


def fetch_twse_disclosures(*, asof: date, codes: list[str],
                           http_get: Callable[..., Any] | None = None,
                           clock: Callable[[], datetime] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Explicit one-shot fetch. Fixed HTTPS host, no redirects, timeout, bounded JSON.

    http_get and clock injected for deterministic tests; no background I/O.
    """
    now = (clock or (lambda: datetime.now(TAIPEI)))()
    empty = pd.DataFrame(columns=EVENT_COLS)
    if now.tzinfo is None:
        return empty, _summary("INVALID_CLOCK", message="時鐘不含時區，拒絕讀取", fetched_at=now.replace(tzinfo=TAIPEI))
    now = now.astimezone(TAIPEI)
    if asof != now.date():
        return empty, _summary("NOT_TODAY_NO_HISTORY", message="官方每日端點為當期快照，僅支援抓取日研究，不提供歷史復原", fetched_at=now)
    clean_codes = [_code(c) for c in codes]
    if not codes or len(codes) > MAX_CODES or any(not c for c in clean_codes):
        return empty, _summary("INVALID_CODES", message="請提供1～30檔有效上市代號；不能以產業全文搜尋取代公告實證", fetched_at=now)
    if http_get is None:
        import requests
        http_get = requests.get
    response = None
    try:
        response = http_get(SOURCE_URL, timeout=(5, 15), allow_redirects=False, stream=True,
                            headers={"Accept": "application/json", "User-Agent": "SPT-H132-Research/1.0"})
        payload = _read_limited_json(response)
        received_at = (clock or (lambda: datetime.now(TAIPEI)))()
        return parse_twse_disclosures(payload, asof=asof, fetched_at=received_at, codes=clean_codes)
    except Exception as exc:
        # Every unexpected response/provider error fails closed and is visible to user.
        return empty, _summary("FETCH_FAILED", message=f"TWSE資料取得／解析未完成：{type(exc).__name__}；不建立新聞交易訊號", fetched_at=now)
    finally:
        if response is not None:
            try:
                response.close()
            except Exception:
                pass


def candidate_event_watch(candidates: pd.DataFrame | None, events: pd.DataFrame | None) -> pd.DataFrame:
    """Join *watch-only* event counts; never change H126 decision or rank by headline."""
    columns = ["股票代號", "股票名稱", "H126研究層別_原樣", "H109決策_原樣", "當期公告筆數", "最近公告日期", "事件類別", "研究動作"]
    if not isinstance(candidates, pd.DataFrame) or candidates.empty or "股票代號" not in candidates:
        return pd.DataFrame(columns=columns)
    counts = {}
    if isinstance(events, pd.DataFrame) and not events.empty and {"股票代號", "公告日期", "公告類別"}.issubset(events.columns):
        for code, df in events.groupby("股票代號"):
            counts[code] = (len(df), df["公告日期"].max(), "、".join(sorted(set(df["公告類別"].astype(str)))))
    rows = []
    for _, r in candidates.head(250).iterrows():
        code = _code(r.get("股票代號"))
        if not code:
            continue
        n, last, categories = counts.get(code, (0, "無匹配", "無公告證據"))
        layer = _text(r.get("H126權威層別"))
        risk = _text(r.get("H109決策權威"))
        rows.append({"股票代號": code, "股票名稱": _text(r.get("股票名稱")),
                     "H126研究層別_原樣": layer or "未認證", "H109決策_原樣": risk or "未知",
                     "當期公告筆數": n, "最近公告日期": last, "事件類別": categories,
                     "研究動作": "有公告：人工核對原文與量價；仍維持原H126/H109限制" if n else "當期資料未找到匹配公告，不代表公司沒有其他消息"})
    return pd.DataFrame(rows, columns=columns).drop_duplicates("股票代號")

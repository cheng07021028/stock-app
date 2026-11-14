# -*- coding: utf-8 -*-
"""H158. TAIFEX official JSON/CSV evidence for SuperAI; no guessed positions.

Handles API CSV fallback, validates dates, contract, institution and numeric units.
A partial response is research context only and is never claimed as complete.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from io import StringIO
from zoneinfo import ZoneInfo
import csv
import json
import math
import re
from typing import Any

import requests

BASE = "https://openapi.taifex.com.tw/v1/"
PCR_PATH = "PutCallRatio"
FUT_PATH = "MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate"
TZ = ZoneInfo("Asia/Taipei")


def _number(v: Any) -> float | None:
    try:
        s = str(v).strip().replace(",", "").replace("%", "")
        if not s or s.lower() in {"none", "nan", "null", "--", "-"}:
            return None
        n = float(s)
        return n if math.isfinite(n) else None
    except (TypeError, ValueError):
        return None


def _normal(s: Any) -> str:
    return re.sub(r"[\s_\-./()%％：:]+", "", str(s or "")).lower()


def _get(row: dict[str, Any], *aliases: str) -> Any:
    normalized = {_normal(k): v for k, v in row.items()}
    for a in aliases:
        if _normal(a) in normalized:
            return normalized[_normal(a)]
    return None


def _date(value: Any) -> str:
    raw = str(value or "").strip()
    digits = re.sub(r"\D", "", raw)
    if len(digits) < 8:
        return ""
    try:
        return datetime.strptime(digits[:8], "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""


def _recent(d: str, now: datetime | None = None) -> bool:
    if not d:
        return False
    now = (now or datetime.now(TZ)).astimezone(TZ)
    day = datetime.strptime(d, "%Y-%m-%d").date()
    # A weekend can legitimately use the preceding Friday. A longer data outage
    # is never silently called fresh, even when the HTTP request succeeds.
    return now.date() - timedelta(days=4) <= day <= now.date()


def _records(response: Any) -> list[dict[str, Any]]:
    try:
        payload = response.json()
    except Exception:
        payload = None
    if isinstance(payload, dict):
        payload = next((payload[k] for k in ("data", "Data", "items", "records", "result") if isinstance(payload.get(k), list)), None)
    if isinstance(payload, list):
        return [dict(r) for r in payload if isinstance(r, dict)]
    raw = getattr(response, "content", None)
    if isinstance(raw, bytes) and raw:
        text = ""
        for enc in ("utf-8-sig", "big5", "cp950"):
            try:
                text = raw.decode(enc)
                break
            except UnicodeDecodeError:
                pass
    else:
        text = str(getattr(response, "text", "") or "")
    # CSV may contain a BOM or quoted field names including parentheses.
    parsed = csv.DictReader(StringIO(text.lstrip("\ufeff")))
    return [dict(row) for row in parsed if isinstance(row, dict) and any(row.values())]


def _select_latest(rows: list[dict[str, Any]], now: datetime | None = None) -> tuple[list[dict[str, Any]], str]:
    dated = [(r, _date(_get(r, "Date", "日期", "交易日期"))) for r in rows]
    valid = [(r, d) for r, d in dated if _recent(d, now)]
    if not valid:
        return [], ""
    latest = max(d for _, d in valid)
    return [r for r, d in valid if d == latest], latest


def parse_pcr(rows: list[dict[str, Any]], now: datetime | None = None) -> dict[str, Any]:
    newest, d = _select_latest(rows, now)
    if not newest:
        return {}
    row = newest[0]
    pv = _number(_get(row, "PutVolume", "Put Volume", "賣權成交量"))
    cv = _number(_get(row, "CallVolume", "Call Volume", "買權成交量"))
    poi = _number(_get(row, "PutOpenInterest", "PutOI", "Put OI", "賣權未平倉量"))
    coi = _number(_get(row, "CallOpenInterest", "CallOI", "Call OI", "買權未平倉量"))
    volume = _number(_get(row, "PutCallRatio(Volume)", "PutCallVolumeRatio", "PutCallVolumeRatio%", "買賣權成交量比率%"))
    oi = _number(_get(row, "PutCallRatio(OpenInterest)", "PutCallOpenInterestRatio", "PutCallOpenInterestRatio%", "買賣權未平倉量比率%"))
    if volume is None and pv is not None and cv and cv > 0:
        volume = round(pv / cv * 100, 4)
    if oi is None and poi is not None and coi and coi > 0:
        oi = round(poi / coi * 100, 4)
    out: dict[str, Any] = {"TAIFEX_PCR資料日期": d}
    if volume is not None and 1 <= volume <= 1500:
        out["PCR成交量比%"] = volume
    if oi is not None and 1 <= oi <= 1500:
        out["PCR未平倉量比%"] = oi
    return out if len(out) > 1 else {}


def parse_futures(rows: list[dict[str, Any]], now: datetime | None = None) -> dict[str, Any]:
    newest, d = _select_latest(rows, now)
    if not newest:
        return {}
    # Do not sum different contract multipliers, or use a random numeric HTML
    # column: only the official TX / foreign net OI field has this meaning.
    for row in newest:
        contract = str(_get(row, "ContractCode", "契約", "商品名稱") or "").strip().replace("台", "臺")
        investor = str(_get(row, "Item", "身分別", "身份別", "交易人類別") or "").strip()
        if contract not in {"臺股期貨", "TX"} or not ("外資" in investor or "FINI" in investor.upper()):
            continue
        net = _number(_get(row, "OpenInterest(Net)", "OpenInterestNet", "未平倉淨口數"))
        if net is not None and net == int(net) and abs(net) <= 10_000_000:
            return {"外資臺指期未平倉淨口數": int(net), "TAIFEX期貨資料日期": d}
    return {}


def complete(payload: dict[str, Any], now: datetime | None = None) -> bool:
    if not isinstance(payload, dict):
        return False
    return bool(
        _recent(str(payload.get("TAIFEX_PCR資料日期") or ""), now)
        and _recent(str(payload.get("TAIFEX期貨資料日期") or ""), now)
        and _number(payload.get("PCR成交量比%")) is not None
        and _number(payload.get("PCR未平倉量比%")) is not None
        and _number(payload.get("外資臺指期未平倉淨口數")) is not None
        and payload.get("來源") == "TAIFEX_OPENAPI_VERIFIED"
    )


def fetch_taifex_openapi(*, timeout: int = 12, http_get: Any = None, now: datetime | None = None) -> tuple[dict[str, Any], list[str]]:
    get = http_get or requests.get
    headers = {"User-Agent": "Mozilla/5.0 (compatible; GodPickV191H158/1.0)", "Accept": "application/json,text/csv;q=0.9"}
    result: dict[str, Any] = {}
    diagnostics: list[str] = []
    for path, parser, label in ((PCR_PATH, parse_pcr, "PCR"), (FUT_PATH, parse_futures, "外資臺指期淨未平倉")):
        try:
            response = get(BASE + path, timeout=(5, int(timeout)), headers=headers)
            response.raise_for_status()
            rows = _records(response)
            parsed = parser(rows, now=now)
            if parsed:
                result.update(parsed)
                diagnostics.append(f"TAIFEX OpenAPI {label}：有效 {len(rows)} 筆；日期 {parsed.get('TAIFEX_PCR資料日期') or parsed.get('TAIFEX期貨資料日期')}")
            else:
                diagnostics.append(f"TAIFEX OpenAPI {label}：缺少新鮮可驗證欄位，保留未知（{len(rows)}筆）")
        except Exception as exc:
            diagnostics.append(f"TAIFEX OpenAPI {label} 失敗：{type(exc).__name__}: {exc}")
    if (result.get("TAIFEX_PCR資料日期") and result.get("TAIFEX期貨資料日期")
            and all(k in result for k in ("PCR成交量比%", "PCR未平倉量比%", "外資臺指期未平倉淨口數"))):
        result["來源"] = "TAIFEX_OPENAPI_VERIFIED"
    return result, diagnostics


__all__ = ["fetch_taifex_openapi", "parse_pcr", "parse_futures", "complete"]

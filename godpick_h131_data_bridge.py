# -*- coding: utf-8 -*-
"""H131 read-only research bridge: batch history, calendar, evidence audit, forward observation.

Not an official tick-data certification, no background polling, no cloud writes,
no trading authorization. No HTTP inside this module; callers inject the existing
history provider. Source quality is reported, never silently promoted.
"""
from __future__ import annotations

from datetime import date, timedelta, datetime, time
from zoneinfo import ZoneInfo
from urllib.parse import urlparse
from pathlib import Path
from typing import Any, Callable
import gzip
import io
import json
import re

import pandas as pd

from godpick_h130_six_engine import _code, _date, _text, normalize_prices

VERSION = "V191-H131.0"
MAX_BATCH = 5
MAX_CANDIDATES = 250


def read_h126_workbook(content: bytes, filename: str) -> tuple[pd.DataFrame, str]:
    """Prefer H126's single authority; refuse silently replacing it with H102 legacy rank."""
    if not isinstance(content, bytes) or not content or len(content) > 20_000_000:
        return pd.DataFrame(), "INPUT_MISSING_OR_OVERSIZE"
    try:
        if filename.lower().endswith(".csv"):
            frame = pd.read_csv(io.BytesIO(content), encoding="utf-8-sig", dtype={"股票代號": str}, nrows=MAX_CANDIDATES + 1)
            sheet = "CSV"
        elif filename.lower().endswith(".xlsx"):
            wb = pd.ExcelFile(io.BytesIO(content))
            preferred = [s for s in wb.sheet_names if s.startswith("00_H126主管決策")]
            preferred += [s for s in wb.sheet_names if "H126" in s and s not in preferred]
            if not preferred:
                return pd.DataFrame(), "NO_H126_AUTHORITY_SHEET"
            frame, sheet = pd.DataFrame(), ""
            for candidate in preferred:
                frame = pd.read_excel(wb, sheet_name=candidate, dtype={"股票代號": str}, nrows=MAX_CANDIDATES + 1)
                if {"股票代號", "H126權威層別"}.issubset(frame.columns):
                    sheet = candidate
                    break
            if not sheet:
                return pd.DataFrame(), "H126_SCHEMA_MISSING"
        else:
            return pd.DataFrame(), "UNSUPPORTED_FORMAT"
    except (ValueError, OSError, KeyError, UnicodeError) as exc:
        return pd.DataFrame(), f"READ_FAILED:{type(exc).__name__}"
    if not {"股票代號", "H126權威層別"}.issubset(frame.columns):
        return pd.DataFrame(), "H126_SCHEMA_MISSING"
    if len(frame) > MAX_CANDIDATES:
        return pd.DataFrame(), "CANDIDATE_LIMIT_EXCEEDED"
    frame = frame.copy()
    frame["股票代號"] = frame["股票代號"].map(_code)
    frame = frame[frame["股票代號"] != ""].drop_duplicates("股票代號", keep="first")
    return frame.reset_index(drop=True), f"H126_AUTHORITY:{sheet}"


def verified_calendar(root: Path, *, asof: date) -> dict[str, Any]:
    """Checks local three-source H117 archive manifests; no day-from-weekday invention."""
    result = {"status": "NO_VERIFIED_H117", "official_market_day": "UNKNOWN", "archive_count": 0,
              "description": "尚未有完整三市場、通過SHA驗證的H117封存；本機SHA不能獨立證明資料真偽"}
    base = Path(root) / "data" / "h117" / "daily"
    sources = ("TWSE", "TPEX", "TAIEX")
    try:
        from godpick_h117_official_market_closure import verify_manifest, MIN_COUNTS, MARKET_URLS
        files = {}
        for src in sources:
            path = base / src
            if not path.is_dir():
                return result
            files[src] = {p.name.removesuffix(".json.gz"): p for p in path.glob("*.json.gz")
                          if re.fullmatch(r"\d{4}-\d{2}-\d{2}\.json\.gz", p.name)}
        common = sorted(set.intersection(*(set(v) for v in files.values())))
        valid = []
        for day in common[-12:]:
            if day > asof.isoformat():
                continue
            try:
                if not all(_check_archive(files[src][day], day, src, verify_manifest, MIN_COUNTS[src], MARKET_URLS[src])
                           for src in sources):
                    continue
                valid.append(day)
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
                continue
        result["archive_count"] = len(valid)
        if valid:
            latest = valid[-1]
            result["official_market_day"] = latest
            result["status"] = "VERIFIED_ASOF" if latest == asof.isoformat() else "VERIFIED_PRIOR_SESSION_ONLY"
            result["description"] = ("三市場本機封存SHA、官方網址欄位及收集時間核驗通過；不能單靠本機SHA證明來源真實性，也非逐股法人／TDCC認證；"
                                     "若分析日仍在盤中，僅為前交易日資料" )
        return result
    except (ImportError, OSError, ValueError):
        return result


def _check_archive(path: Path, day: str, src: str, checker: Callable,
                   min_rows: int, official_url: str) -> bool:
    if path.stat().st_size > 20_000_000:
        return False
    with gzip.open(path, "rt", encoding="utf-8") as f:
        payload = json.load(f)
    source_url = str(payload.get("source_url") or "")
    if source_url != official_url or urlparse(source_url).scheme != "https":
        return False
    try:
        when = datetime.fromisoformat(str(payload.get("collected_at") or "").replace("Z", "+00:00"))
        if when.tzinfo is None:
            return False
        taiwan = when.astimezone(ZoneInfo("Asia/Taipei"))
        if taiwan.date().isoformat() != day or taiwan.time() < time(15, 0):
            return False
    except (TypeError, ValueError):
        return False
    return bool(checker(payload) and payload.get("date") == day
                and payload.get("source") == src
                and isinstance(payload.get("rows"), list)
                and len(payload["rows"]) >= min_rows)


def candidate_evidence_audit(candidates: pd.DataFrame | None, *, asof: date) -> pd.DataFrame:
    """Inspect row evidence labels without rewriting authority or certifying source."""
    columns = ["股票代號", "H126層別", "H109風控", "法人原列狀態", "TDCC原列狀態", "研究截止日", "查核結論"]
    if not isinstance(candidates, pd.DataFrame) or candidates.empty or "股票代號" not in candidates:
        return pd.DataFrame(columns=columns)
    output = []
    for _, row in candidates.head(MAX_CANDIDATES).iterrows():
        code = _code(row.get("股票代號"))
        if not code:
            continue
        d = _date(row.get("H126研究資訊截止日") or row.get("H110研究資訊截止日"))
        veto = _text(row.get("H109決策權威"))
        layer = _text(row.get("H126權威層別"))
        flow = _text(row.get("H123法人實證狀態")) or "MISSING"
        tdcc = _text(row.get("H124大戶實證狀態")) or "MISSING"
        if layer.startswith("禁止") or any(x in veto for x in ("VETO", "QUARANTINE", "SHOCK")):
            decision = "風控否決：不得買進"
        elif d != asof:
            decision = "研究日不符：重新查證"
        elif flow not in ("PROVEN_FLOW", "VERIFIED_BUT_NOT_BUYING") or tdcc not in ("VERIFIED_RISING", "VERIFIED_NOT_RISING"):
            decision = "法人／TDCC尚未完整實證"
        else:
            decision = "原列聲稱驗證；尚須H123/H124原始證據回讀（非買進授權）"
        output.append(dict(zip(columns, [code, layer, veto, flow, tdcc,
                                         str(d) if d else "UNKNOWN", decision])))
    return pd.DataFrame(output, columns=columns)


def _raw_history_anomalies(raw: pd.DataFrame, *, asof: date) -> tuple[bool, bool]:
    """Flag contradictory daily closes, future-dated rows before canonical dedup."""
    date_col = next((s for s in ("日期", "date", "Date", "datetime") if s in raw), None)
    close_col = next((s for s in ("收盤價", "收盤", "close", "Close", "Adj Close") if s in raw), None)
    if not date_col or not close_col:
        return False, False
    days = pd.to_datetime(raw[date_col], errors="coerce", utc=True).dt.date
    prices = pd.to_numeric(raw[close_col].astype(str).str.replace(",", "", regex=False), errors="coerce")
    temp = pd.DataFrame({"day": days, "price": prices}).dropna()
    future = bool((temp["day"] > asof).any())
    for _, group in temp.groupby("day"):
        if len(group) > 1:
            if group["price"].min() <= 0:
                return True, future
            if (group["price"].max() / group["price"].min() - 1) > 0.0001:
                return True, future
    return False, future


def batch_history(codes: list[str], *, asof: date, fetcher: Callable[..., Any],
                  market_by_code: dict[str, str] | None = None, lookback_days: int = 750,
                  expected_session: str = "UNKNOWN") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Explicit one-click batch, max 5, no silent ticker/source substitution."""
    market_by_code = market_by_code or {}
    if not isinstance(lookback_days, int) or not 200 <= lookback_days <= 1500:
        return pd.DataFrame(), pd.DataFrame([{"資料狀態": "INVALID_LOOKBACK", "說明": "歷史讀取限200至1500日"}])
    seen: set[str] = set()
    targets: list[str] = []
    for raw in codes:
        code = _code(raw)
        if code and code not in seen:
            targets.append(code)
            seen.add(code)
    if not targets or len(targets) > MAX_BATCH:
        return pd.DataFrame(), pd.DataFrame([{"資料狀態": "INVALID_BATCH", "說明": f"僅允許1至{MAX_BATCH}檔有效代號"}])
    start = asof - timedelta(days=lookback_days)
    frames, reports = [], []
    for code in targets:
        market = market_by_code.get(code, "上市")
        if market not in ("上市", "上櫃"):
            market = "上市"
        try:
            raw = fetcher(code, market_type=market, start_date=start, end_date=asof)
            if not isinstance(raw, pd.DataFrame) or raw.empty:
                reports.append({"股票代號": code, "資料狀態": "NO_DATA", "資料筆數": 0,
                                "最後K線日": "UNKNOWN", "來源": "get_history_data（來源可能混合）", "說明": "行情服務未回傳有效資料"})
                continue
            code_col = next((x for x in ("股票代號", "代號", "code", "Code") if x in raw), None)
            if code_col is not None:
                observed = {str(x).strip().removesuffix(".0") for x in raw[code_col].dropna().unique()}
                if observed != {code}:
                    reports.append({"股票代號": code, "資料狀態": "TICKER_MISMATCH", "資料筆數": 0,
                                    "最後K線日": "UNKNOWN", "來源": "get_history_data", "說明": "傳回其他股票代號，拒絕合併"})
                    continue
            raw = raw.copy()
            raw["股票代號"] = code
            # Never let the H130 deduplication silently select one of two
            # materially different prices for the same trade date.
            collision, future = _raw_history_anomalies(raw, asof=asof)
            if collision:
                reports.append({"股票代號": code, "資料狀態": "CONFLICTING_DAILY_BAR", "資料筆數": 0,
                                "最後K線日": "UNKNOWN", "來源": "get_history_data", "說明": "同一日期有衝突收盤價；禁止直接合併"})
                continue
            canon = normalize_prices(raw, asof=asof)
            if not canon.empty:
                canon = canon[canon["日期"].dt.date >= start].reset_index(drop=True)
            if canon.empty:
                status, explanation, last = "BAD_CANDLES", "無有效K線", "UNKNOWN"
            else:
                last = canon["日期"].max().date().isoformat()
                if len(canon) < 60:
                    status, explanation = "INSUFFICIENT_BARS", "不足60筆日線，不判斷技術趨勢"
                elif canon["開盤價"].isna().any():
                    status, explanation = "MISSING_OPEN", "無開盤價，禁止使用次日開盤回測"
                elif expected_session == "UNKNOWN":
                    status, explanation = "SESSION_NOT_VERIFIED", "未有H117三市場日終核驗，不能宣稱今日最新"
                elif last != expected_session:
                    status, explanation = "STALE_OR_DIFFERENT_DAY", "個股最後K線與經驗證市場日不同"
                else:
                    status, explanation = "DATE_ALIGNED_SOURCE_UNCERTIFIED", "交易日一致，但行情仍可能含Yahoo/快取，不代表官方逐筆認證"
                if future:
                    status = "FUTURE_DATES_REMOVED_RECHECK"
                    explanation = "來源出現研究日之後的K線；已剔除但必須稽核來源"
                frames.append(canon)
            reports.append({"股票代號": code, "資料狀態": status, "資料筆數": len(canon),
                            "最後K線日": last, "來源": "get_history_data（Yahoo/官方/快取可能混合）", "說明": explanation})
        except Exception as exc:
            reports.append({"股票代號": code, "資料狀態": f"FETCH_ERROR:{type(exc).__name__}",
                            "資料筆數": 0, "最後K線日": "UNKNOWN",
                            "來源": "get_history_data", "說明": "個別股票載入失敗；不影響其他代號"})
    return (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(), pd.DataFrame(reports))


def historical_forward_observations(candidates: pd.DataFrame | None, prices: pd.DataFrame | None,
                                    *, asof: date, horizons: tuple[int, ...] = (1, 3, 5)) -> pd.DataFrame:
    """Research-close forward observation only; NEVER actual fill or execution P&L.

    Excludes market day 0 and dates later than asof. No re-using old ranks as
    confirmed BUY, no synthetic weekend entries, no missing-future fill.
    """
    cols = ["股票代號", "訊號研究日", "訊號收盤價_非成交", "研究層別", "研究資格", "資料最新日"] + [f"T+{i}收盤變化%" for i in horizons]
    if not isinstance(candidates, pd.DataFrame) or candidates.empty or "股票代號" not in candidates:
        return pd.DataFrame(columns=cols)
    frame = normalize_prices(prices, asof=asof)
    if frame.empty:
        return pd.DataFrame(columns=cols)
    outputs = []
    for _, row in candidates.head(MAX_CANDIDATES).iterrows():
        code = _code(row.get("股票代號"))
        if not code:
            continue
        cutoff = _date(row.get("H126研究資訊截止日") or row.get("H110研究資訊截止日"))
        subset = frame[frame["股票代號"] == code].sort_values("日期").reset_index(drop=True)
        if subset.empty:
            continue
        day_list = subset["日期"].dt.date.tolist()
        if cutoff is None or cutoff >= asof or cutoff not in day_list:
            continue
        offset = day_list.index(cutoff)
        day0 = float(subset.iloc[offset]["收盤價"])
        result = {"股票代號": code, "訊號研究日": str(cutoff),
                  "訊號收盤價_非成交": day0,
                  "研究層別": _text(row.get("H126權威層別")),
                  "研究資格": "僅研究觀測；不等於交易績效",
                  "資料最新日": str(day_list[-1])}
        for i in horizons:
            result[f"T+{i}收盤變化%"] = round((float(subset.iloc[offset+i]["收盤價"]) / day0-1)*100, 3) if offset+i < len(subset) else None
        outputs.append(result)
    return pd.DataFrame(outputs, columns=cols)

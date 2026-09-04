# -*- coding: utf-8 -*-
"""V191-H60 TDCC holder truth service.

This module turns TDCC's official shareholder-dispersion CSV into a reusable,
cache-backed large-holder truth layer.  It never treats a price/volume proxy as
an official holder observation.  When live TDCC data is unavailable, callers
receive an explicit PROXY/NONE status and may fall back to existing model
signals without fabricating a large-holder percentage.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import csv
import io
import json
import os
import time

VERSION = "v191_h60_tdcc_holder_truth_20260904"
SOURCE_URL = os.environ.get("GODPICK_TDCC_HOLDER_CSV_URL", "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5")
CACHE_DIR = Path(os.environ.get("GODPICK_TDCC_CACHE_DIR", Path(__file__).resolve().parent / "data" / "cache" / "tdcc_holder_truth"))
LATEST_FILE = CACHE_DIR / "latest.json"
PREVIOUS_FILE = CACHE_DIR / "previous.json"
FETCH_META_FILE = CACHE_DIR / "fetch_meta.json"
_MIN_FETCH_INTERVAL_SECONDS = 6 * 60 * 60
_MEM_CACHE: dict[str, Any] | None = None
_MEM_CACHE_AT: float = 0.0


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _s(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _f(v: Any, default: float | None = None) -> float | None:
    try:
        t = str(v).strip().replace(",", "").replace("％", "%")
        if t.endswith("%"):
            t = t[:-1]
        if not t:
            return default
        return float(t)
    except Exception:
        return default


def _read_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def parse_tdcc_csv_bytes(raw: bytes) -> dict[str, Any]:
    """Parse official TDCC CSV and return the latest class-15 snapshot.

    Class 15 means 1,000,001 shares and above in the official dispersion table.
    The parser is intentionally tolerant of BOM/Big5 and slight header naming
    differences, but it never infers missing official values.
    """
    text = None
    for enc in ("utf-8-sig", "utf-8", "cp950", "big5"):
        try:
            text = raw.decode(enc)
            break
        except Exception:
            continue
    if text is None:
        raise ValueError("TDCC CSV encoding could not be decoded")

    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for rec in reader:
        if not isinstance(rec, dict):
            continue
        norm = {str(k).strip(): v for k, v in rec.items() if k is not None}
        date_value = _s(norm.get("資料日期") or norm.get("資料年月日") or norm.get("Date"))
        code = _s(norm.get("證券代號") or norm.get("股票代號") or norm.get("SecurityCode"))
        level = _s(norm.get("持股分級") or norm.get("HoldingLevel") or norm.get("持股級距"))
        if not code or level != "15":
            continue
        ratio = _f(norm.get("占集保庫存數比例%") or norm.get("占集保庫存數比例") or norm.get("比例%") or norm.get("Percentage"))
        if ratio is None:
            continue
        rows.append({
            "資料日期": date_value,
            "證券代號": code,
            "持股分級": "15",
            "人數": int(_f(norm.get("人數") or norm.get("People"), 0) or 0),
            "股數": int(_f(norm.get("股數") or norm.get("Shares"), 0) or 0),
            "占集保庫存數比例%": float(ratio),
        })
    if not rows:
        raise ValueError("TDCC CSV has no class-15 rows")

    dates = sorted({_s(r.get("資料日期")) for r in rows if _s(r.get("資料日期"))})
    latest_date = dates[-1] if dates else ""
    latest_rows = [r for r in rows if not latest_date or _s(r.get("資料日期")) == latest_date]
    payload = {
        "version": VERSION,
        "source": "TDCC_OPEN_DATA",
        "source_url": SOURCE_URL,
        "fetched_at": _now(),
        "data_date": latest_date,
        "rows": {r["證券代號"]: r for r in latest_rows},
    }
    return payload


def _fetch_csv(timeout: float = 4.0) -> bytes:
    try:
        import requests
        resp = requests.get(SOURCE_URL, timeout=max(1.0, float(timeout)), headers={"User-Agent": "Mozilla/5.0 GodPick-H60"})
        resp.raise_for_status()
        return bytes(resp.content)
    except ImportError:
        from urllib.request import Request, urlopen
        req = Request(SOURCE_URL, headers={"User-Agent": "Mozilla/5.0 GodPick-H60"})
        with urlopen(req, timeout=max(1.0, float(timeout))) as resp:
            return resp.read()


def refresh_tdcc_holder_cache(*, force: bool = False, timeout: float = 4.0) -> tuple[bool, str, dict[str, Any]]:
    global _MEM_CACHE, _MEM_CACHE_AT
    latest = _read_json(LATEST_FILE, {})
    meta = _read_json(FETCH_META_FILE, {})
    last_attempt = _f(meta.get("last_attempt_epoch"), 0.0) or 0.0
    now_epoch = time.time()
    if not force and latest and now_epoch - last_attempt < _MIN_FETCH_INTERVAL_SECONDS:
        _MEM_CACHE = latest
        _MEM_CACHE_AT = now_epoch
        return True, "TDCC cache fresh enough; skipped network refresh", latest

    try:
        raw = _fetch_csv(timeout=timeout)
        fresh = parse_tdcc_csv_bytes(raw)
        old_date = _s(latest.get("data_date")) if isinstance(latest, dict) else ""
        new_date = _s(fresh.get("data_date"))
        if latest and old_date and new_date and new_date != old_date:
            _write_json(PREVIOUS_FILE, latest)
        _write_json(LATEST_FILE, fresh)
        _write_json(FETCH_META_FILE, {"last_attempt_epoch": now_epoch, "last_ok_at": _now(), "status": "OK", "data_date": new_date})
        _MEM_CACHE = fresh
        _MEM_CACHE_AT = now_epoch
        return True, f"TDCC official holder cache updated: {new_date or 'date unknown'}", fresh
    except Exception as exc:
        _write_json(FETCH_META_FILE, {"last_attempt_epoch": now_epoch, "last_error_at": _now(), "status": "FAILED", "error": f"{type(exc).__name__}: {exc}"})
        if latest:
            _MEM_CACHE = latest
            _MEM_CACHE_AT = now_epoch
            return False, f"TDCC refresh failed; using cached official snapshot: {type(exc).__name__}: {exc}", latest
        return False, f"TDCC refresh failed and no official cache exists: {type(exc).__name__}: {exc}", {}


def load_tdcc_holder_cache(*, allow_network: bool = False, timeout: float = 4.0) -> tuple[dict[str, Any], dict[str, Any], str]:
    global _MEM_CACHE, _MEM_CACHE_AT
    if _MEM_CACHE is not None and time.time() - _MEM_CACHE_AT < 60.0:
        latest = _MEM_CACHE
    else:
        latest = _read_json(LATEST_FILE, {})
        _MEM_CACHE = latest if isinstance(latest, dict) else {}
        _MEM_CACHE_AT = time.time()
    message = "TDCC official cache loaded" if latest else "TDCC official cache missing"
    if allow_network:
        ok, message, fresh = refresh_tdcc_holder_cache(force=False, timeout=timeout)
        if fresh:
            latest = fresh
    previous = _read_json(PREVIOUS_FILE, {})
    return latest if isinstance(latest, dict) else {}, previous if isinstance(previous, dict) else {}, message


def enrich_tdcc_holder_truth(frame: Any, *, allow_network: bool = False, timeout: float = 4.0):
    """Return a DataFrame enriched with actual TDCC class-15 holder truth.

    No official row => status MISSING.  The scoring engine may later use PROXY,
    but this service never fills an official percentage from a model proxy.
    """
    try:
        import pandas as pd
    except Exception:
        return frame
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame
    latest, previous, msg = load_tdcc_holder_cache(allow_network=allow_network, timeout=timeout)
    latest_rows = latest.get("rows", {}) if isinstance(latest, dict) else {}
    prev_rows = previous.get("rows", {}) if isinstance(previous, dict) else {}
    data_date = _s(latest.get("data_date")) if isinstance(latest, dict) else ""
    prev_date = _s(previous.get("data_date")) if isinstance(previous, dict) else ""
    out = frame.copy()
    statuses = []
    ratios = []
    deltas = []
    dates = []
    prev_dates = []
    messages = []
    for _, row in out.iterrows():
        code = _s(row.get("股票代號") or row.get("代號")).split(".")[0]
        rec = latest_rows.get(code) if isinstance(latest_rows, dict) else None
        old = prev_rows.get(code) if isinstance(prev_rows, dict) else None
        ratio = _f((rec or {}).get("占集保庫存數比例%")) if isinstance(rec, dict) else None
        old_ratio = _f((old or {}).get("占集保庫存數比例%")) if isinstance(old, dict) else None
        statuses.append("ACTUAL" if ratio is not None else "MISSING")
        ratios.append(ratio)
        deltas.append(round(ratio - old_ratio, 4) if ratio is not None and old_ratio is not None else None)
        dates.append(data_date if ratio is not None else "")
        prev_dates.append(prev_date if ratio is not None and old_ratio is not None else "")
        messages.append(msg if ratio is not None else "TDCC未取得此代號class-15官方值；不得冒充真實大戶持股")
    out["TDCC大戶資料狀態"] = statuses
    out["TDCC千張大戶持股比%"] = ratios
    out["TDCC千張大戶週變化pp"] = deltas
    out["TDCC大戶資料日期"] = dates
    out["TDCC大戶前期日期"] = prev_dates
    out["TDCC大戶資料說明"] = messages
    out["TDCC大戶資料版本"] = VERSION
    return out


__all__ = [
    "VERSION", "SOURCE_URL", "parse_tdcc_csv_bytes", "refresh_tdcc_holder_cache",
    "load_tdcc_holder_cache", "enrich_tdcc_holder_truth",
]

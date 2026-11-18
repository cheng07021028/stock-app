# -*- coding: utf-8 -*-
"""H169 official PE-only PIT observation and non-trading Excel cross-check.

TWSE/TPEx public P/E feeds are NOT a verified quarterly/TTM EPS ledger.
Never reverse engineer EPS from price/PE and call it an official EPS value.
Never use a later first-observed snapshot in a historical decision.
"""
from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Mapping
import json
import math
import re
from zoneinfo import ZoneInfo

VERSION = "V191-H169-PE-ONLY-SOURCE-AUDIT"
TZ = ZoneInfo("Asia/Taipei")
ROOT = Path(__file__).resolve().parent
PE_ONLY_FILE = ROOT / "data" / "h115" / "pe_only_latest.json"
ENDPOINTS = {
    "TWSE": "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL",
    "TPEx": "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis",
}


def _clean(v: Any) -> str:
    return str(v or "").strip().replace(",", "")


def _code(v: Any) -> str:
    t = _clean(v).removesuffix(".0")
    return t if re.fullmatch(r"[1-9][0-9]{3}", t) else ""


def _numeric(v: Any) -> float | None:
    try:
        x = float(_clean(v))
        return round(x, 4) if math.isfinite(x) and x > 0 else None
    except (ValueError, TypeError, OverflowError):
        return None


def _date(v: Any) -> str:
    t = _clean(v).replace("-", "").replace("/", "")
    if len(t) == 7 and t.isdigit():
        y = int(t[:3]) + 1911
        t = f"{y:04d}" + t[3:]
    try:
        return date(int(t[:4]), int(t[4:6]), int(t[6:8])).isoformat() if len(t)==8 and t.isdigit() else ""
    except ValueError:
        return ""


def _get(row: Mapping, *aliases: str) -> Any:
    for a in aliases:
        if a in row:
            return row[a]
    return None


def parse_pe_rows(rows: Any, *, market: str, observed_at: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Strictly classify disclosed PE data; never backfill EPS."""
    if market not in ENDPOINTS:
        raise ValueError("unknown official market")
    if not isinstance(rows, list):
        raise ValueError("official response is not JSON list")
    observed = datetime.fromisoformat(observed_at)
    if observed.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    observed_date = observed.astimezone(TZ).date()
    unique: dict[str, dict] = {}
    dates: set[str] = set()
    duplicates = 0
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        code = _code(_get(raw, "Code", "SecuritiesCompanyCode", "StockId", "stock_id",
                          "股票代號", "證券代號", "代號"))
        day = _date(_get(raw, "Date", "date", "資料日期", "日期", "TradeDate"))
        pe = _numeric(_get(raw, "PEratio", "PERatio", "P/E", "本益比", "PER", "PE"))
        if not (code and day and pe):
            continue
        if date.fromisoformat(day) > observed_date:
            continue  # never accept future-issued market evidence
        item = {
            "code": code,
            "market": "上市" if market == "TWSE" else "上櫃",
            "name": str(_get(raw, "Name", "股票名稱", "證券名稱", "公司名稱") or "")[:100],
            "pe": pe,
            "asof_date": day,
            "available_at": observed_at,
            "availability_basis": "FIRST_OBSERVED",
            "source": market,
            "source_url": ENDPOINTS[market],
            "eps_ttm": None,
            "eps_status": "NOT_IN_THIS_FEED",
        }
        dates.add(day)
        if code in unique:
            duplicates += 1
            if day < unique[code]["asof_date"]:
                continue
        unique[code] = item
    result = sorted(unique.values(), key=lambda x: x["code"])
    return result, {
        "market": market, "source_rows": len(rows), "verified_pe_rows": len(result),
        "duplicate_codes": duplicates, "data_dates": sorted(dates),
        "status": "PE_ONLY_SOURCE_VERIFIED" if len(result) >= 20 else "COVERAGE_INSUFFICIENT",
        "eps_independent_verification": False,
    }


def merge_pe_observations(parts: Mapping[str, Any], *, observed_at: str) -> dict[str, Any]:
    """One market may be unavailable. Never claim partial coverage as full."""
    rows: list[dict[str, Any]] = []
    sources: dict[str, dict[str, Any]] = {}
    for market in ENDPOINTS:
        value = parts.get(market) if isinstance(parts, Mapping) else None
        if isinstance(value, list):
            parsed, summary = parse_pe_rows(value, market=market, observed_at=observed_at)
            rows.extend(parsed)
            sources[market] = summary
        else:
            sources[market] = {
                "market": market, "source_rows": 0, "verified_pe_rows": 0,
                "status": "SOURCE_MISSING_OR_UNAVAILABLE",
                "eps_independent_verification": False,
            }
    good = all(sources[k]["status"] == "PE_ONLY_SOURCE_VERIFIED" for k in ENDPOINTS)
    return {
        "version": VERSION,
        "updated_at": observed_at,
        "status": "PE_ONLY_BOTH_MARKETS" if good else "PE_ONLY_PARTIAL",
        "sources": sources,
        "pe_only": sorted(rows, key=lambda x: (x["market"], x["code"])),
        "eps_verified": False,
        "h115_valuation_ledger_imported": False,
        "h126_trade_permission_changed": False,
        "notice": "僅官方PE，沒有獨立近四季EPS或季度公告時點；不得充當H115完整官方PE/EPS或H126買進資格。",
    }


def load_pe_snapshot(path: Path = PE_ONLY_FILE) -> dict[str, Any]:
    try:
        path = Path(path)
        if not path.is_file() or path.stat().st_size > 8_000_000:
            return {"status": "PE_ONLY_NOT_COLLECTED", "pe_only": [], "sources": {}}
        v = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(v, dict) and isinstance(v.get("pe_only"), list):
            return v
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
        pass
    return {"status": "PE_ONLY_INVALID", "pe_only": [], "sources": {}}


def validate_observation_for_research(item: Mapping, *, research_day: str) -> str:
    study = _date(research_day)
    issue = _date(item.get("asof_date"))
    available = str(item.get("available_at") or "")
    try:
        known = datetime.fromisoformat(available)
        if known.tzinfo is None:
            return "可用時點無時區，不可使用"
        if not study or not issue:
            return "資料日期無法驗證"
        at = known.astimezone(TZ).date()
        ref = date.fromisoformat(study)
        # The exported workbook gives a research DATE, not its exact decision
        # timestamp. Evidence first observed later that SAME day cannot
        # retroactively count for an unknown earlier intraday scan.
        if at >= ref or date.fromisoformat(issue) > ref:
            return "本研究日當天或之後才觀察到，缺決策時刻，不得回填"
        if (ref - date.fromisoformat(issue)).days > 5:
            return "資料過期，需更新官方PE"
        return "可作PE研究參考，獨立EPS仍待補證"
    except (ValueError, TypeError):
        return "可用時點無法核實"


def append_pe_audit(wb: Any, *, research_day: Any) -> dict[str, Any]:
    """Append evidence-only H169 section to existing Sheet11; no new sheets.

    Existing table names, prior rows, H154 rankings and H126 are untouched.
    The PE source is not independently verified EPS, and never adds ranking.
    """
    from openpyxl.styles import Font, Alignment, PatternFill
    from openpyxl.utils import get_column_letter
    tab = "11_交易員重點導航"
    if tab not in wb.sheetnames:
        return {"sheet": tab, "checked_codes": 0, "reason": "缺現有導航表，不建立替代報表"}
    snap = load_pe_snapshot()
    index = {(str(r.get("code") or ""), str(r.get("market") or "")): r
             for r in snap.get("pe_only", []) if isinstance(r, Mapping)}
    study = str(research_day or "")[:10]
    ws = wb[tab]
    start = ws.max_row + 3
    ws.cell(start, 1, "H169｜官方PE來源稽核（獨立EPS未驗證，不改原研究排序）")
    ws.cell(start, 2, "研究日 " + study + "｜PE快照 " +
            str(snap.get("updated_at") or "尚未取得") +
            "｜資料狀態 " + str(snap.get("status") or "未驗證"))
    ws.cell(start, 1).fill = PatternFill("solid", fgColor="17324D")
    ws.cell(start, 1).font = Font(name="Microsoft JhengHei", bold=True, color="FFFFFF")
    ws.row_dimensions[start].height = 32
    ws.cell(start+1, 1, "TWSE 已核 PE")
    ws.cell(start+1, 2, (snap.get("sources") or {}).get("TWSE", {}).get("verified_pe_rows", 0))
    ws.cell(start+2, 1, "TPEx 已核 PE")
    ws.cell(start+2, 2, (snap.get("sources") or {}).get("TPEx", {}).get("verified_pe_rows", 0))
    ws.cell(start+3, 1, "正式資格")
    ws.cell(start+3, 2, "僅官方PE，沒有獨立近四季EPS；不能作H115低PE×高EPS認證或H126交易授權。")
    headers = ["股票代號","股票名稱","市場別","研究日","官方PE","官方PE資料日",
               "首次實際觀察時間","官方來源","PE證據狀態","獨立官方EPS","正式買進資格"]
    header_row = start + 5
    for j, title in enumerate(headers, 1):
        cell = ws.cell(header_row, j, title)
        cell.fill = PatternFill("solid", fgColor="0B6E75")
        cell.font = Font(name="Microsoft JhengHei", bold=True, color="FFFFFF")
    ws.row_dimensions[header_row].height = 30
    source = wb["01_具名強勢研究"] if "01_具名強勢研究" in wb.sheetnames else None
    seen: set[str] = set()
    found = 0
    if source is not None:
        hdr = [str(cell.value or "") for cell in source[1]]
        columns = {name: hdr.index(name) for name in ("股票代號","股票名稱","市場別") if name in hdr}
        for row in source.iter_rows(min_row=2, values_only=True):
            code = _code(row[columns["股票代號"]]) if "股票代號" in columns else ""
            if not code or code in seen:
                continue
            seen.add(code)
            name = str(row[columns["股票名稱"]] or "") if "股票名稱" in columns else ""
            market = str(row[columns["市場別"]] or "") if "市場別" in columns else ""
            item = index.get((code, market))
            if item is None and not market:
                choices = [rec for (c,m), rec in index.items() if c == code]
                item = choices[0] if len(choices) == 1 else None
            if item:
                found += 1
            state = validate_observation_for_research(item, research_day=study) if item else (
                "此代號／市場別未取得完整官方PE來源"
            )
            values = [code, name, market, study, item.get("pe") if item else None,
                      item.get("asof_date") if item else "",
                      item.get("available_at") if item else "",
                      item.get("source_url") if item else "",
                      state, "未獨立認證", "禁止買進｜H126仍為唯一權威"]
            r = header_row + len(seen)
            for j, value in enumerate(values, 1):
                cell = ws.cell(r, j, value)
                cell.alignment = Alignment(vertical="center", wrap_text=j >= 7)
            ws.row_dimensions[r].height = 35
    # Keep original Sheet11 A/B widths and existing filters/freeze/title.
    for j in range(3, 12):
        letter = get_column_letter(j)
        old = ws.column_dimensions[letter].width or 13
        ws.column_dimensions[letter].width = min(52, max(old, 27 if j in (7, 8, 9) else 17))
    return {"sheet": tab, "start_row": start, "checked_codes": len(seen),
            "with_official_pe_only": found, "eps_independently_verified": 0,
            "snapshot_status": snap.get("status")}


__all__ = [
    "ENDPOINTS", "parse_pe_rows", "merge_pe_observations", "load_pe_snapshot",
    "validate_observation_for_research", "append_pe_audit",
]

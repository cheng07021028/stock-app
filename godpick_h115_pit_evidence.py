# -*- coding: utf-8 -*-
"""V191-H115.3 immutable-ish, as-of financial evidence reader and import validation.

This file makes no network calls. Source observations are not retroactively
considered known. The caller must provide verified first-availability times.
Git-tracked JSONL snapshots (or a durable mounted directory) are REQUIRED for
Streamlit Cloud reboot persistence. No recommendation or Formal permission here.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping
import json
import math
import os
import tempfile
import hashlib
from urllib.parse import urlsplit

VERSION = "v191_h115_pit_evidence_and_peer_value_20261014"
TZ_TW = timezone(timedelta(hours=8))
MAX_BYTES = 64_000_000
MIN_PEERS = 5
MAX_VALUATION_LAG = 14


def _datetime(x: Any, end_of_date: bool = False) -> datetime | None:
    if isinstance(x, datetime):
        d = x
    elif isinstance(x, date):
        d = datetime(x.year, x.month, x.day, 23, 59, 59 if end_of_date else 0)
    else:
        s = str(x or "").strip().replace("Z", "+00:00")
        if not s: return None
        try:
            if len(s) == 8 and s.isdecimal():
                d = datetime.strptime(s, "%Y%m%d")
            else:
                d = datetime.fromisoformat(s.replace("/", "-"))
        except ValueError:
            return None
    if d.tzinfo is None: d = d.replace(tzinfo=TZ_TW)
    if end_of_date and d.hour == 0 and d.minute == 0 and d.second == 0 and (isinstance(x, date) and not isinstance(x, datetime) or isinstance(x, str) and len(x.strip()) <= 10):
        d = d.replace(hour=23, minute=59, second=59)
    return d.astimezone(TZ_TW)


def _num(v: Any) -> float | None:
    try:
        if v is None or isinstance(v, bool) or str(v).strip() in {"", "-", "--"}: return None
        n = float(str(v).replace(",", ""))
        return n if math.isfinite(n) else None
    except (TypeError, ValueError):
        return None


def _code(v: Any) -> str:
    s = str(v or "").strip().replace(".TW", "").replace(".TWO", "")
    if s.endswith(".0"): s = s[:-2]
    return s if s.isalnum() and len(s) <= 10 else ""


def _period(v: Any) -> str:
    s = str(v or "").strip().replace("/", "").replace("-", "")
    if not s.isdecimal() or len(s) not in (5, 6): return ""
    y, m = int(s[:-2]), int(s[-2:]);y = y + 1911 if y < 1911 else y
    return f"{y:04d}{m:02d}" if 1 <= m <= 12 and 1900 <= y <= 2200 else ""


def _source(v: Any) -> str:
    s = str(v or "").strip().upper()
    return s if s in {"TWSE", "TPEX", "MOPS"} else ""


def _url(v: Any) -> str:
    s = str(v or "").strip()
    if not s.startswith("https://") or len(s) > 2048: return ""
    host = (urlsplit(s).hostname or "").lower()
    return s if host in {"twse.com.tw", "tpex.org.tw"} or host.endswith((".twse.com.tw", ".tpex.org.tw")) else ""


def _canonical(r: Mapping[str, Any], category: str) -> dict[str, Any]:
    x = dict(r)
    code = _code(x.get("code") or x.get("stock_id") or x.get("股票代號"))
    # Date-only availability is conservative: treat as known only at that day's end.
    avail = _datetime(x.get("available_at") or x.get("可用時間"), end_of_date=True)
    src = _source(x.get("source") or x.get("來源"))
    url = _url(x.get("source_url") or x.get("來源網址"))
    basis = str(x.get("availability_basis") or "").upper()
    if not (code and avail and src and basis in {"PUBLISHED_VERIFIED", "FIRST_OBSERVED"}):
        raise ValueError("股票代號、可用時間(含時區)、官方來源、可用時間依據皆為必要")
    if basis == "PUBLISHED_VERIFIED" and not url:
        raise ValueError("公告已驗證必須提供可追溯的 https 官方來源網址")
    if avail > datetime.now(TZ_TW) + timedelta(days=1):
        raise ValueError("不能保存未來可用時間的證據")
    common = {"code": code, "available_at": avail.isoformat(timespec="seconds"),
              "source": src, "source_url": url, "availability_basis": basis}
    if category == "revenue":
        ym = _period(x.get("year_month") or x.get("資料年月"))
        revenue = _num(x.get("revenue_twd") if "revenue_twd" in x else x.get("營收元"))
        if not ym or revenue is None or revenue < 0:
            raise ValueError("營收年月/金額無效；營收必須使用新台幣元，不接受未註明單位")
        if int(ym[:4]) * 12 + int(ym[4:]) > avail.year * 12 + avail.month:
            raise ValueError("營收資料年月不能晚於首次可用日")
        common.update({"year_month": ym, "revenue_twd": int(round(revenue))})
    elif category == "valuation":
        day = _datetime(x.get("asof_date") or x.get("資料日期"))
        pe = _num(x.get("pe") if "pe" in x else x.get("本益比"))
        eps = _num(x.get("eps_ttm") if "eps_ttm" in x else x.get("近四季EPS"))
        sector = str(x.get("sector") or x.get("產業別") or "").strip()
        market = str(x.get("market") or x.get("市場別") or "").strip()
        if not day or not sector or len(sector) > 100 or market not in {"上市", "上櫃"}:
            raise ValueError("估值需要有效的資料日期、產業別與上市/上櫃市場別")
        if pe is None or pe <= 0 or eps is None or eps <= 0:
            raise ValueError("無正值本益比/EPS不適用成長低估策略；請保留原始資料供其他研究")
        if day > avail:
            raise ValueError("估值資料日期不能晚於首次可用日")
        common.update({"asof_date": day.date().isoformat(), "sector": sector, "market": market,
                       "pe": pe, "eps_ttm": eps})
    else:
        raise ValueError("未知證據類別")
    return common


def _jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.is_file(): return []
    if path.stat().st_size > MAX_BYTES: raise ValueError(f"資料檔過大：{path.name}")
    out = []
    with path.open("r", encoding="utf-8-sig") as f:
        for n, line in enumerate(f, 1):
            if not line.strip():continue
            if n > 1_000_000: raise ValueError("單一歷史資料檔筆數超過上限")
            try: item = json.loads(line)
            except json.JSONDecodeError as exc: raise ValueError(f"資料錯誤 {path.name}:{n}: {exc}") from exc
            if not isinstance(item, dict): raise ValueError(f"資料列必須為object {path.name}:{n}")
            out.append(item)
    return out


def _append_canonical(path: Path, items: Iterable[Mapping[str, Any]], category: str) -> dict[str, int]:
    # Single-writer only: never concurrently import from two processes.
    incoming = [_canonical(x, category) for x in items]
    # First-observed imports must really be captured near ingestion time.
    # Backdated CSV data requires documented PUBLISHED_VERIFIED provenance;
    # otherwise imported data could be used to invent ex-ante signals.
    now = datetime.now(TZ_TW)
    for item in incoming:
        if item["availability_basis"] == "FIRST_OBSERVED":
            lag = (now - _datetime(item["available_at"])).total_seconds()
            if lag > 86400 or lag < -86400:
                raise ValueError("FIRST_OBSERVED首次觀察日與實際匯入相差超過24小時；歷史證據須用可驗證公告來源")
    before = _jsonl_rows(path)
    old = {json.dumps(i, ensure_ascii=False, sort_keys=True) for i in before}
    # A daily official snapshot may repeat an unchanged monthly revenue value.
    # Keep FIRST availability for the same fact; preserve new versions of any
    # revised value with their own later first-observed timestamp.
    def fact_key(v: Mapping[str, Any]):
        if category == "revenue":
            return (v.get("code"),v.get("year_month"),v.get("revenue_twd"),v.get("source"))
        return (v.get("code"),v.get("asof_date"),v.get("pe"),v.get("eps_ttm"),v.get("source"))
    indexed={fact_key(v) for v in before}
    newly=[]
    for v in sorted(incoming,key=lambda x:x["available_at"]):
        k=fact_key(v)
        if k not in indexed:
            indexed.add(k);newly.append(v)
    unique={json.dumps(i,ensure_ascii=False,sort_keys=True) for i in newly}
    combined=sorted(old|unique)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".h115_", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write("\n".join(combined) + "\n" if combined else "")
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    return {"received": len(incoming), "inserted": len(combined) - len(old), "duplicates": len(incoming) - (len(combined)-len(old)), "total": len(combined)}


def append_revenue(store_dir: Path, rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    return _append_canonical(Path(store_dir) / "revenue.jsonl", rows, "revenue")


def append_valuations(store_dir: Path, rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    return _append_canonical(Path(store_dir) / "valuations.jsonl", rows, "valuation")


def build_h114_evidence(root: Path, info_cutoff: Any, *, min_peers: int = MIN_PEERS) -> dict[str, dict[str, Any]]:
    """Returns only evidence that was AVAILABLE at info_cutoff.

    Latest valuation is compared with peers on the EXACT SAME source pricing day
    and sector/market, never a present-day cross-sectional median for a past day.
    """
    cutoff = _datetime(info_cutoff, end_of_date=True)
    if not cutoff: return {}
    store = Path(root) / "data" / "h115"
    result: dict[str, dict[str, Any]] = defaultdict(dict)
    revenues: dict[tuple[str, str], dict[str, Any]] = {}
    for row in _jsonl_rows(store / "revenue.jsonl"):
        try: r = _canonical(row, "revenue")
        except ValueError: continue
        av = _datetime(r["available_at"])
        if av and av <= cutoff:
            k=(r["code"],r["year_month"])
            if k not in revenues or (r["available_at"],r["source"]) > (revenues[k]["available_at"],revenues[k]["source"]):
                revenues[k]=r
    for (code, _ym), row in revenues.items():
        result[code].setdefault("H114月營收歷史", []).append({
            "year_month": row["year_month"], "revenue_twd": row["revenue_twd"],
            "released_at": row["available_at"], "source": row["source"]})
    for item in result.values():
        item["H114月營收歷史"].sort(key=lambda x:x["year_month"])

    valuations: dict[tuple[str, str], dict[str, Any]] = {}
    for row in _jsonl_rows(store / "valuations.jsonl"):
        try:r = _canonical(row, "valuation")
        except ValueError:continue
        av = _datetime(r["available_at"])
        if av and av <= cutoff and (cutoff.date() - date.fromisoformat(r["asof_date"])).days <= MAX_VALUATION_LAG:
            key=(r["code"],r["asof_date"])
            if key not in valuations or r["available_at"] > valuations[key]["available_at"]:
                valuations[key]=r
    by_code={}
    for (code, _),r in valuations.items():
        if code not in by_code or (r["asof_date"],r["available_at"]) > (by_code[code]["asof_date"],by_code[code]["available_at"]):
            by_code[code]=r
    cohorts=defaultdict(list)
    for r in by_code.values():
        cohorts[(r["asof_date"],r["market"],r["sector"])].append(r)
    for code,r in by_code.items():
        peers=[p["pe"] for p in cohorts[(r["asof_date"],r["market"],r["sector"])] if p["code"]!=code]
        result[code]["H115同業有效樣本數"] = len(peers)
        result[code]["H115同業資料日期"] = r["asof_date"]
        result[code]["H115同業估值依據"] = "同市場／同產業／同一資料日，排除自己，取同業中位數" if len(peers)>=min_peers else "同業樣本不足；不產生低估訊號"
        if len(peers) < min_peers: continue
        result[code].update({
            "H114本益比實值":r["pe"],
            "H114同業本益比中位數_官方":round(median(peers),5),
            "H114近四季EPS實值":r["eps_ttm"],
            "H114估值資料來源": "OFFICIAL_VERIFIED",
            "H114估值資料日期": r["asof_date"],
        })
    return dict(result)


def evidence_digest(root: Path) -> dict[str, Any]:
    store = Path(root)/"data"/"h115"
    out={"version":VERSION,"storage":"LOCAL_ONLY_UNTIL_COMMITTED_OR_DURABLE_MOUNT","files":{}}
    for n in ("revenue.jsonl", "valuations.jsonl"):
        p=store/n
        if p.is_file():
            out["files"][n]={"rows":len(_jsonl_rows(p)),"sha256":hashlib.sha256(p.read_bytes()).hexdigest()}
        else:out["files"][n]={"rows":0,"status":"MISSING"}
    return out

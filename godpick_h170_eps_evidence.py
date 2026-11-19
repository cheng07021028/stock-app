# -*- coding: utf-8 -*-
"""H170 official exchange financial statements: EPS YTD, NOT four-quarter EPS.

The TWSE/TPEx t187ap06 general-industry income statements are cumulative
year-to-date for Q2/Q3. Do not relabel Q3 YTD as single-quarter EPS or TTM.
This is a first-observed evidence snapshot only; no historic PIT backfill.
"""
from __future__ import annotations
import json
import math
import re
from datetime import date,datetime
from pathlib import Path
from typing import Any,Mapping

VERSION="V191-H170-OFFICIAL-EPS-YTD-SNAPSHOT-NON_TTM"
ROOT=Path(__file__).resolve().parent
EPS_SNAPSHOT=ROOT/"data"/"h115"/"eps_ytd_latest.json"
SOURCES={
    "TWSE":"https://openapi.twse.com.tw/v1/opendata/t187ap06_L_ci",
    "TPEx":"https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_ci",
}


def _date(text:Any)->str:
    s=str(text or "").strip().replace("-","").replace("/","")
    if s.isdigit() and len(s)==7:s=f"{int(s[:3])+1911}{s[3:]}"
    try:
        return date(int(s[:4]),int(s[4:6]),int(s[6:8])).isoformat() if len(s)==8 else ""
    except (ValueError,TypeError):
        return ""


def _asof_q(row:Mapping)->tuple[int,int]:
    v=str(row.get("年度") or row.get("Year") or "").strip()
    q=str(row.get("季別") or row.get("Season") or row.get("Quarter") or "").strip().replace("Q","")
    if not(v.isdigit() and q.isdigit()):return (0,0)
    y=int(v);q=int(q)
    if y<1911:y+=1911
    return (y,q) if 2000<=y<=2100 and 1<=q<=4 else (0,0)


def _float(v:Any)->float|None:
    try:
        f=float(str(v or "").strip().replace(",",""))
        return round(f,4) if math.isfinite(f) and abs(f)<1e5 else None
    except (ValueError,TypeError,OverflowError):
        return None


def parse_official_eps_ytd(raw:Any,*,market:str,observed_at:str)->tuple[list[dict],dict]:
    if market not in SOURCES:raise ValueError("unknown market")
    if not isinstance(raw,list):raise ValueError("official EPS response not list")
    observed=datetime.fromisoformat(observed_at)
    if observed.tzinfo is None:raise ValueError("observation timestamp requires timezone")
    result:dict[str,dict]={};periods=set()
    for item in raw:
        if not isinstance(item,dict):continue
        code=str(item.get("公司代號") or item.get("CompanyCode") or "").strip()
        year,q=_asof_q(item)
        report_day=_date(item.get("出表日期") or item.get("Date"))
        eps=_float(item.get("基本每股盈餘（元）") if "基本每股盈餘（元）" in item else
                   item.get("基本每股盈餘") if "基本每股盈餘" in item else item.get("BasicEPS"))
        if not re.fullmatch(r"[1-9]\d{3}",code) or not year or eps is None or not report_day:continue
        if report_day>observed.date().isoformat():continue
        row={
            "code":code,"market":"上市" if market=="TWSE" else "上櫃",
            "year_quarter":f"{year}Q{q}",
            "eps_ytd":eps,
            "unit":"元／股",
            "basis":"CUMULATIVE_YTD_NOT_SINGLE_QUARTER",
            "report_date":report_day,
            "available_at":observed_at,
            "availability_basis":"FIRST_OBSERVED",
            "source":market,
            "source_url":SOURCES[market],
            "eps_ttm":None,
            "eps_ttm_status":"NOT_VERIFIED",
        }
        if code not in result or row["year_quarter"]>=result[code]["year_quarter"]:
            result[code]=row
            periods.add(row["year_quarter"])
    rows=sorted(result.values(),key=lambda x:x["code"])
    return rows,{
        "source":market,"source_rows":len(raw),"usable_eps_ytd_rows":len(rows),
        "year_quarters":sorted(periods),
        "status":"YTD_OBSERVED" if len(rows)>=20 else "INSUFFICIENT_COVERAGE",
        "ttm_eps_certified":False,
    }


def combine_sources(values:Mapping,*,observed_at:str)->dict:
    all_rows=[];health={}
    for market in SOURCES:
        raw=values.get(market) if isinstance(values,Mapping) else None
        if isinstance(raw,list):
            subset,diag=parse_official_eps_ytd(raw,market=market,observed_at=observed_at)
            all_rows.extend(subset);health[market]=diag
        else:
            health[market]={"source":market,"usable_eps_ytd_rows":0,"status":"NOT_AVAILABLE",
                            "ttm_eps_certified":False}
    healthy=all(health[m].get("status")=="YTD_OBSERVED" for m in SOURCES)
    return {
        "version":VERSION,"observed_at":observed_at,
        "status":"EPS_YTD_BOTH_MARKETS" if healthy else "EPS_YTD_PARTIAL",
        "sources":health,"eps_ytd_rows":all_rows,
        "ttm_eps_certified":False,"official_low_pe_high_eps_certified":False,
        "trade_authority_changed":False,
        "notice":"此為一般業財報累計EPS/YTD；不可當單季EPS或近四季TTM。金融保險業未完整覆蓋；未經發表時點驗證不得回填過去排名。",
    }


def load_snapshot(path:Path=EPS_SNAPSHOT)->dict:
    try:
        p=Path(path)
        if p.is_file() and p.stat().st_size<=8_000_000:
            raw=json.loads(p.read_text("utf-8-sig"))
            if isinstance(raw,dict) and isinstance(raw.get("eps_ytd_rows"),list):return raw
    except (OSError,ValueError,UnicodeError,json.JSONDecodeError):
        pass
    return {"status":"NOT_COLLECTED","eps_ytd_rows":[],"sources":{}}


def asof_read(item:Mapping,*,study_day:str)->str:
    """Date-only Excel decision can't use same-day first-observed data."""
    date_str=_date(study_day)
    at=str(item.get("available_at") or "")
    try:
        d=datetime.fromisoformat(at)
        if not d.tzinfo or not date_str:return "EVIDENCE_TIME_UNVERIFIED"
        if d.date().isoformat()>=date_str:return "OBSERVED_SAME_OR_LATER_DAY_NOT_PIT"
        if _date(item.get("report_date"))>date_str:return "FUTURE_REPORT_DATE"
        return "OFFICIAL_YTD_PIT_ONLY_NOT_TTM"
    except (TypeError,ValueError):
        return "EVIDENCE_TIME_UNVERIFIED"


__all__=["VERSION","SOURCES","parse_official_eps_ytd","combine_sources","load_snapshot","asof_read"]

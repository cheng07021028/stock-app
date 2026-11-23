# -*- coding: utf-8 -*-
"""H186: read-only historical OHLCV quality and SAME-RUN export scope proof.

Never grants H109/H126 permissions, fills prices/volumes, or uses a future quote.
Each data-quality result describes the input, not the fundamental prospects.
"""
from __future__ import annotations
from collections.abc import Mapping
from typing import Any
import re
import pandas as pd

VERSION = "V191-H186-EVIDENCE-INTEGRITY"
_SHARE_COLUMNS=("成交股數","成交量(股)","VolumeShares")
_LOT_COLUMNS=("成交量(張)","成交量_張","VolumeLots")


def volume_quality(frame: Any, *, bars: int = 60) -> dict[str, Any]:
    if not isinstance(frame,pd.DataFrame) or frame.empty:
        return {"status":"NO_HISTORY", "bars":0,"sample":0,"positive":0,"missing":0,
                "nonpositive":0,"column":"","unit":"UNVERIFIED","sources":[]}
    data=frame.copy(deep=False)
    dt_col=next((c for c in ("日期","date","Date") if c in data),None)
    if dt_col:
        data=data.assign(__h186_day=pd.to_datetime(data[dt_col],errors="coerce"))
        data=data.dropna(subset=["__h186_day"]).sort_values("__h186_day").drop_duplicates("__h186_day",keep="last")
    col=next((c for c in _SHARE_COLUMNS+_LOT_COLUMNS if c in data.columns),None)
    unit="SHARES" if col in _SHARE_COLUMNS else "LOTS" if col in _LOT_COLUMNS else "UNVERIFIED"
    n=min(len(data),bars)
    if col:
        series=pd.to_numeric(data[col],errors="coerce").tail(bars)
        good=int(((series>0)&series.notna()).sum())
        missing=int(series.isna().sum())
        nonpos=int(((series<=0)&series.notna()).sum())
    else:
        good=0; missing=n;nonpos=0
    sources=[]
    if "資料源" in data.columns:
        sources=sorted({str(x).strip()[:90] for x in data["資料源"].dropna().unique() if str(x).strip()})[:12]
    # Price coherence matters too. A 60-volume-positive cache with a bad high/low
    # must not qualify as H114's 60 valid OHLCV bars.
    aliases={"o":("開盤價","open","Open"),"h":("最高價","high","High"),
             "l":("最低價","low","Low"),"c":("收盤價","close","Close")}
    selected={k:next((x for x in vs if x in data.columns),None) for k,vs in aliases.items()}
    bad_price=n
    if all(selected.values()):
        last=data.tail(bars)
        nums={k:pd.to_numeric(last[column],errors="coerce") for k,column in selected.items()}
        valid=(nums['o'].gt(0)&nums['h'].gt(0)&nums['l'].gt(0)&nums['c'].gt(0)&
               nums['h'].ge(pd.concat([nums['o'],nums['l'],nums['c']],axis=1).max(axis=1))&
               nums['l'].le(pd.concat([nums['o'],nums['h'],nums['c']],axis=1).min(axis=1)))
        bad_price=int((~valid).sum())
    status=("VALID_60_DATED_POSITIVE_VOLUME" if col and len(data)>=bars and good==bars and bad_price==0 and dt_col
            else "MISSING_DATE_OR_OHLC" if not dt_col or not all(selected.values())
            else "SHORT_HISTORY" if len(data)<bars
            else "INVALID_OHLC" if bad_price
            else "MISSING_VOLUME_UNIT" if not col
            else "INCOMPLETE_VOLUME")
    return {"status":status,"bars":len(data),"sample":n,"positive":good,
            "missing":missing,"nonpositive":nonpos,"bad_ohlc":bad_price,
            "column":col or "", "unit":unit,"sources":sources}


def verified_same_run_export_frame(limited: Any, full: Any, *,
                                   expected_count: int, current_signature: str,
                                   full_signature: str, current_run_id: str,
                                   scan_run_id: str) -> tuple[Any,dict[str,Any]]:
    """No historical/reconstructed frames; fail closed if provenance or identity fails."""
    a=limited if isinstance(limited,pd.DataFrame) else pd.DataFrame()
    b=full if isinstance(full,pd.DataFrame) else pd.DataFrame()
    meta={"version":VERSION,"source":"ORIGINAL_EXPORT_INPUT", "limited_rows":len(a),
          "full_rows":len(b),"expected":int(expected_count or 0),
          "status":"NO_SAME_RUN_FULL_DIAGNOSIS","authority_changed":False}
    if not(current_run_id and scan_run_id and current_run_id==scan_run_id and
           current_signature and current_signature==full_signature):return a,meta
    if int(expected_count or 0)<=0 or len(b)!=int(expected_count):return a,meta
    if "股票代號" not in b or "股票代號" not in a:
        return a,meta
    def codes(df:pd.DataFrame)->list[str]:
        return df["股票代號"].astype(str).str.strip().str.removesuffix(".0").tolist()
    a_codes=codes(a);b_codes=codes(b)
    # The scan universe can include 0050/0052 ETFs even when the H145 stock
    # authority deliberately excludes codes beginning with zero. Preserve those
    # rows in the diagnosis denominator; never promote them to ordinary stocks.
    valid=lambda c:bool(re.fullmatch(r"\d{4}",c)) and c!="0000"
    meta["zero_prefixed_codes_not_formal_stocks"]=sum(c.startswith("0") for c in b_codes)
    if (not all(valid(c) for c in b_codes) or len(set(b_codes))!=len(b_codes)
            or not set(a_codes).issubset(set(b_codes))):
        meta["status"]="FULL_DIAGNOSIS_CODE_CONFLICT";return a,meta
    if not {"H114K線資料狀態","H114K線證據日"}.issubset(b.columns):
        meta["status"]="FULL_DIAGNOSIS_NO_DATED_EVIDENCE";return a,meta
    # Merely reading the complete proof cannot authorize a buy. H145 audits
    # the scan report, context signatures, and final candidate coverage again.
    meta.update(source="VERIFIED_CURRENT_RUN_FULL_DIAGNOSIS",status="RECOVERED_FOR_H145_RECHECK")
    return b.copy(deep=False),meta

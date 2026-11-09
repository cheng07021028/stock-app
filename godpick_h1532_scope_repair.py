# -*- coding: utf-8 -*-
"""H153.2: strictly bounded full-diagnosis research scope recovery.

This never alters H145/H126 authority. A larger decision frame can be used
solely as independent research evidence if the run signature matches and all
shared per-stock H114 measurements agree. Never promote a smaller H113 audit.
"""
from __future__ import annotations
from typing import Any
import pandas as pd
from godpick_h147_five_gate_review import _code

VERSION='V191-H153.2-RESEARCH-SCOPE'
FIELDS=('H114K線證據日','H114K線資料狀態','H114當日收盤價','H114K線量比')

def _codes(frame:pd.DataFrame):
    codes=frame.get('股票代號',pd.Series(dtype=str)).map(_code)
    return codes, set(codes[codes!=''])

def restore_research_scope(diagnosis:Any,full_diagnosis:Any,*,
                           current_signature:Any,full_signature:Any):
    base=diagnosis.copy(deep=False) if isinstance(diagnosis,pd.DataFrame) else pd.DataFrame()
    other=full_diagnosis.copy(deep=False) if isinstance(full_diagnosis,pd.DataFrame) else pd.DataFrame()
    meta={'status':'ORIGINAL_DIAGNOSIS','diagnosis_rows':len(base),'full_rows':len(other),
          'selected_rows':len(base),'research_only':True,'h126_modified':False}
    if other.empty or len(other)<=len(base):return base,meta
    if not current_signature or not full_signature or str(current_signature)!=str(full_signature):
        meta['status']='BLOCKED_DIFFERENT_SCAN_SIGNATURE';return base,meta
    if '股票代號' not in base or '股票代號' not in other:
        meta['status']='BLOCKED_NO_STOCK_ID';return base,meta
    a,aset=_codes(base);b,bset=_codes(other)
    if (a.eq('').any() or b.eq('').any() or a.duplicated().any() or b.duplicated().any()
        or not aset.issubset(bset)):
        meta['status']='BLOCKED_DUPLICATE_OR_NOT_SUPERSET';return base,meta
    left=base.set_index('股票代號');right=other.set_index('股票代號')
    missing_contract=[f for f in FIELDS if f in left.columns and f not in right.columns]
    if missing_contract:
        meta['status']='BLOCKED_FULL_SOURCE_LOST_H114_EVIDENCE'
        meta['conflict_field']=missing_contract[0]
        return base,meta
    for field in FIELDS:
        if field in left and field in right:
            for code in aset:
                x=left.at[code,field];y=right.at[code,field]
                if pd.notna(x) and pd.isna(y):
                    meta['status']='BLOCKED_FULL_SOURCE_LOST_H114_EVIDENCE';meta['conflict_field']=field
                    return base,meta
                if pd.notna(x) and pd.notna(y) and str(x)!=str(y):
                    meta['status']='BLOCKED_OVERLAP_EVIDENCE_CONFLICT';meta['conflict_field']=field
                    return base,meta
    # Enrich same-code rows only from trusted current run. Missing H114 fields
    # must stay missing; this is NOT a price-data fetch or approval.
    meta['status']='RESTORED_VERIFIED_SUPERSET_RESEARCH_ONLY';meta['selected_rows']=len(other)
    return other,meta

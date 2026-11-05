# -*- coding: utf-8 -*-
"""H153.1 | compare 3/5-session stock resilience ONLY with SHA-verified H117 TAIEX.

No TAIEX-to-OTC substitution, network call, probability, or H126 change.
If either stock bar baseline or matching official benchmark is missing, fail closed.
"""
from __future__ import annotations
from collections import Counter
from datetime import date
from math import isfinite
from pathlib import Path
from typing import Any
import gzip, json
import pandas as pd
from godpick_h117_official_market_closure import verify_manifest
from godpick_h147_five_gate_review import _code, _day, _s

VERSION='V191-H153.1-MULTIDAY-EVIDENCE'
OUT_COLUMNS=('3日相對抗跌pp','5日相對抗跌pp','多日抗跌來源狀態')


def _num(x:Any)->float|None:
    if x is None or isinstance(x,bool):return None
    try:
        n=float(str(x).strip().replace(',',''))
        return n if isfinite(n) else None
    except (ValueError,TypeError,OverflowError):return None


def _official_benchmark(root:Any, day:Any) -> tuple[dict[int,tuple[str,float]],str]:
    study=_day(day)
    if study is None:return {},'研究交易日未核'
    folder=Path(root)/'TAIEX'
    paths=sorted(x for x in folder.glob('*.json.gz') if x.name[:10]<=str(study))[-6:]
    if len(paths)<4 or paths[-1].name!=str(study)+'.json.gz':
        return {},'H117同日有效官方指數4～6交易日不足'
    rows=[]
    try:
        for path in paths:
            with gzip.open(path,'rt',encoding='utf-8') as handle:m=json.load(handle)
            if not (verify_manifest(m) and m.get('source')=='TAIEX' and
                    m.get('date')+'.json.gz'==path.name and m.get('row_count')==1 and len(m.get('rows') or [])==1):
                return {},'H117官方指數SHA／日期／列數有衝突'
            row=m['rows'][0]
            n=_num(row.get('close'))
            if row.get('date')!=m['date'] or row.get('code')!='BENCHMARK' or n is None or n<=0:
                return {},'H117官方指數內容不完整'
            rows.append((m['date'],n))
    except (OSError,ValueError,TypeError,KeyError,json.JSONDecodeError,EOFError):
        return {},'H117官方指數來源讀取失敗'
    if len({d for d,_ in rows})!=len(rows) or rows!=sorted(rows):
        return {},'H117官方指數交易日期重複或逆序'
    out={}
    for window in (3,5):
        if len(rows)>=window+1:
            d,prev=rows[-window-1]
            out[window]=(d,round((rows[-1][1]/prev-1)*100,4))
    return out,'H117台灣上市加權指數已驗SHA，僅相同市場／交易日期可比較'


def enrich_multiday_resilience(ranked:Any,diagnosis_frame:Any, *, research_day:Any,root:Any):
    frame=ranked.copy(deep=True) if isinstance(ranked,pd.DataFrame) else pd.DataFrame()
    source=diagnosis_frame if isinstance(diagnosis_frame,pd.DataFrame) else pd.DataFrame()
    for c in OUT_COLUMNS:
        if c not in frame.columns:frame[c]=None
    bench,state=_official_benchmark(root,research_day)
    counts=Counter();rec={};dupes=set()
    for x in source.to_dict('records'):
        code=_code(x.get('股票代號'))
        if code in rec:dupes.add(code)
        else:rec[code]=x
    for i,x in frame.iterrows():
        code=_code(x.get('股票代號'))
        orig=rec.get(code) if code not in dupes else None
        if not orig or _day(orig.get('H114K線證據日'))!=_day(research_day) or _s(orig.get('H114K線資料狀態'))!='VALID_OHLCV':
            reason='個股同日有效K線／唯一代號不足'
        elif _s(orig.get('市場別'))!='上市':
            reason='上櫃或市場未證：不得使用加權指數代替櫃買指數'
        elif not bench:
            reason=state
        else:
            known=0
            for window in (3,5):
                b=bench.get(window)
                stock_day=_day(orig.get(f'H114近{window}日基準交易日'))
                stock_ret=_num(orig.get('H114近3日漲跌幅%' if window==3 else 'H114當前5日漲跌幅%'))
                if b and stock_day==_day(b[0]) and stock_ret is not None and -100<=stock_ret<=100:
                    frame.at[i, f'{window}日相對抗跌pp']=round(stock_ret-b[1],3)
                    known+=1
            reason=('同日上市個股與H117已驗證指數匹配，3/5日只作相對研究' if known else
                    '個股與指數3/5日基準日不一致或資料缺漏')
        frame.at[i,'多日抗跌來源狀態']=reason
        counts[reason]+=1
    summary=pd.DataFrame([
        {'檢查項目':'H153.1多日抗跌研究來源','結果':state,'說明':'不借用上櫃指數，不承諾隔日漲幅'},
        {'檢查項目':'H153.1三日相對強度已核個股','結果':int(frame['3日相對抗跌pp'].notna().sum()),'說明':'同股票同交易基準；非H126授權'},
        {'檢查項目':'H153.1五日相對強度已核個股','結果':int(frame['5日相對抗跌pp'].notna().sum()),'說明':'若沒有5日官方封存則不產生數值'},
    ] + [{'檢查項目':'H153.1抗跌來源-'+k,'結果':v,'說明':'缺來源不能認證抗跌'} for k,v in counts.items()])
    return frame, summary, {'version':VERSION,'benchmark':state,'matched_3d':int(frame['3日相對抗跌pp'].notna().sum()),'matched_5d':int(frame['5日相對抗跌pp'].notna().sum()),'formal_h126_modified':False}

__all__=['VERSION','OUT_COLUMNS','enrich_multiday_resilience']

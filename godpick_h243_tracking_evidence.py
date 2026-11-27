# -*- coding: utf-8 -*-
"""Strict recommendation dates; recover only exact verified record identity."""
from datetime import date, datetime
import re
from zoneinfo import ZoneInfo
import pandas as pd


def explicit_date(v):
    if isinstance(v, (datetime,date)) and not pd.isna(v):
        d=v.date() if isinstance(v,datetime) else v
    else:
        m=re.match(r'^\s*(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:[ T].*)?\s*$', _text(v))
        if not m:return None
        try:d=date(*map(int,m.groups()))
        except ValueError:return None
    return d if date(2000,1,1)<=d<=datetime.now(ZoneInfo('Asia/Taipei')).date() else None


def row_date(row):
    # Preserve established precedence but never let a clock-only value parse as today.
    for k in ('推薦日期','推薦時間','建立時間'):
        d=explicit_date(row.get(k))
        if d:return d
    return None


def missing_ids(df):
    return any(not explicit_date(r.get('推薦日期')) and _text(r.get('record_id')) and _text(r.get('股票代號')) for r in df.to_dict('records'))


def _text(v):
    return '' if v is None or pd.isna(v) else str(v).strip()


def eligible(row):
    return bool(_text(row.get('record_id')) and _text(row.get('股票代號')) and _text(row.get('市場別')) and explicit_date(row.get('推薦日期')))


def repair(df, history):
    x=df.copy(); index={}; dup=set(); count=0
    for row in history:
        rid=_text(row.get('record_id'))
        if not rid:continue
        if rid in index:dup.add(rid)
        index[rid]=row
    if '推薦日期' not in x:x['推薦日期']=''
    for i,row in x.iterrows():
        if explicit_date(row.get('推薦日期')):continue
        rid=_text(row.get('record_id')); src=index.get(rid)
        if not rid or rid in dup or src is None:continue
        code=_text(row.get('股票代號')); market=_text(row.get('市場別'))
        if not code or not market or code!=_text(src.get('股票代號')) or market!=_text(src.get('市場別')):continue
        d=explicit_date(src.get('推薦日期'))
        if not d:continue
        x.at[i,'推薦日期']=d.isoformat();count+=1
    return x,count

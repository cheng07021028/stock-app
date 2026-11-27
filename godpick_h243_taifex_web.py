# -*- coding: utf-8 -*-
"""Official HTML fallback; schema, date and arithmetic evidence, never positional guess."""
from __future__ import annotations
from datetime import datetime, timedelta
from io import StringIO
import re
from zoneinfo import ZoneInfo
import pandas as pd
from bs4 import BeautifulSoup

TZ=ZoneInfo('Asia/Taipei')
PCR_URL='https://www.taifex.com.tw/cht/3/pcRatio'
FUT_URL='https://www.taifex.com.tw/cht/3/futContractsDate'


def expected(now=None):
    t=(now or datetime.now(TZ)).astimezone(TZ)
    day=t.date()
    if t.hour*60+t.minute<990 or day.weekday()>=5:
        day-=timedelta(days=1)
    while day.weekday()>=5: day-=timedelta(days=1)
    return day.isoformat()


def _norm(x):
    return re.sub(r'\s+','',str(x)).replace('台','臺').replace('％','%')


def _day(x):
    try:
        return datetime.strptime(str(x).strip().replace('-','/'),'%Y/%m/%d').date().isoformat()
    except ValueError:
        return ''


def _int(x):
    s=str(x).strip().replace(',','').replace('−','-')
    if not re.fullmatch(r'[+-]?\d+(?:\.0+)?',s): raise ValueError('invalid integer')
    return int(float(s))


def _ratio(a,b,v):
    a,b=_int(a),_int(b)
    n=float(str(v).replace('%','').replace(',',''))
    if not (a>=0 and b>0 and 1<=n<=1500 and abs(a/b*100-n)<=0.015):
        raise ValueError('ratio arithmetic mismatch')
    return n


def parse_pcr_html(html,day):
    soup=BeautifulSoup(html,'html.parser')
    if not re.search(r'臺指選擇權\s*\(?TXO\)?\s*Put/Call',_norm(soup.get_text(' ')),re.I):
        # _norm removes spaces, the official title remains unambiguous.
        if '臺指選擇權(TXO)Put/Call' not in _norm(soup.get_text(' ')): return {}
    cols=['日期','賣權成交量','買權成交量','買賣權成交量比率%','賣權未平倉量','買權未平倉量','買賣權未平倉量比率%']
    found=[]
    for table in soup.find_all('table'):
        try: frames=pd.read_html(StringIO(str(table)))
        except ValueError: continue
        for df in frames:
            names=[_norm(c) for c in df.columns]
            if names!=cols: continue
            for row in df.itertuples(index=False,name=None):
                if _day(row[0])!=day: continue
                try: found.append((_ratio(row[1],row[2],row[3]),_ratio(row[4],row[5],row[6])))
                except (ValueError,TypeError,OverflowError): return {}
    if len(found)!=1:return {}
    return {'TAIFEX_PCR資料日期':day,'PCR成交量比%':found[0][0],'PCR未平倉量比%':found[0][1]}


def parse_futures_html(html,day):
    soup=BeautifulSoup(html,'html.parser')
    # Actual report date, not form/input/default query parameter. Refuse ambiguous dates.
    labels=set()
    for m in re.finditer(r'日期\s*[:：]?\s*(\d{4}/\d{1,2}/\d{1,2})',soup.get_text(' ',strip=True)):
        labels.add(_day(m.group(1)))
    if labels!={day}:return {}
    found=[]
    for table in soup.find_all('table'):
        try: frames=pd.read_html(StringIO(str(table)),header=[0,1,2])
        except (ValueError,IndexError):continue
        for df in frames:
            if not isinstance(df.columns,pd.MultiIndex):continue
            names=[tuple(_norm(c) for c in x) for x in df.columns]
            def idx(pred):
                result=[i for i,c in enumerate(names) if pred(c)]
                if len(result)!=1:raise ValueError('ambiguous header')
                return result[0]
            try:
                product=idx(lambda c:c[-1]=='商品名稱')
                investor=idx(lambda c:c[-1] in {'身份別','身分別'})
                long=idx(lambda c:'未平倉餘額' in c and '多方' in c and c[-1]=='口數')
                short=idx(lambda c:'未平倉餘額' in c and '空方' in c and c[-1]=='口數')
                net=idx(lambda c:'未平倉餘額' in c and '多空淨額' in c and c[-1]=='口數')
            except ValueError:continue
            for row in df.itertuples(index=False,name=None):
                if _norm(row[product])!='臺股期貨' or _norm(row[investor]) not in {'外資','外資及陸資'}:continue
                try:
                    a,b,n=map(_int,(row[long],row[short],row[net]))
                    if a<0 or b<0 or a-b!=n or abs(n)>10_000_000:return {}
                    found.append(n)
                except (ValueError,TypeError,OverflowError):return {}
    if len(found)!=1:return {}
    return {'TAIFEX期貨資料日期':day,'外資臺指期未平倉淨口數':found[0]}


def fallback(api, *, get, now=None, timeout=12):
    """Use exact expected-day web rows only where API group is stale/incomplete."""
    day=expected(now); result=dict(api); diag=[]
    groups=[('TAIFEX_PCR資料日期',('PCR成交量比%','PCR未平倉量比%'),PCR_URL,parse_pcr_html),('TAIFEX期貨資料日期',('外資臺指期未平倉淨口數',),FUT_URL,parse_futures_html)]
    sources={}
    for datekey, keys, url, parser in groups:
        if day <= str(result.get(datekey) or '') <= (now or datetime.now(TZ)).astimezone(TZ).date().isoformat() and all(result.get(k) is not None for k in keys):
            sources[datekey]='OPENAPI';continue
        try:
            r=get(url,timeout=(5,min(12,int(timeout))),headers={'User-Agent':'Mozilla/5.0','Accept':'text/html'},allow_redirects=False)
            r.raise_for_status()
            if getattr(r,'status_code',200)!=200:raise ValueError('non-200')
            raw=getattr(r,'content',b'') or b''
            if len(raw)>8_000_000:raise ValueError('HTML too large')
            html=r.text
            # never substitute an older or ambiguous date
            parsed=parser(html,day)
            if not parsed:raise ValueError('date/header/arithmetic verification failed')
            for k in (datekey,*keys):result.pop(k,None)
            result.update(parsed);sources[datekey]='OFFICIAL_HTML';diag.append(f'H243期交所官方網頁核實 {datekey}={day}')
        except Exception as exc:diag.append(f'H243官方網頁備援未核實 {datekey}：{type(exc).__name__}: {str(exc)[:100]}')
    if len(sources)==2 and 'OFFICIAL_HTML' in sources.values():
        result['來源']='TAIFEX_OFFICIAL_DUAL_VERIFIED';result['H243來源']=sources
    elif 'OFFICIAL_HTML' in sources.values():
        result['來源']='TAIFEX_OFFICIAL_PARTIAL';result['H243來源']=sources
    return result,diag

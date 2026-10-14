# -*- coding: utf-8 -*-
"""V191-H117: official EOD bar snapshots, immutable daily source and H116 CSV export.

This service NEVER creates trading instructions or rewrites immutable H116 decisions.
Uses ONLY end-of-day market trade dates carried by TWSE/TPEx, not fetch-time dates.
"""
from __future__ import annotations

from datetime import date, datetime, time, timezone, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence
from collections import Counter
import csv
import gzip
import hashlib
import io
import json
import math
import os
import re
import tempfile
from zoneinfo import ZoneInfo

VERSION = 'V191-H117.1-official-ohlcv-closed-loop-20261014'
TZ = ZoneInfo('Asia/Taipei')
MARKET_URLS = {
    'TWSE': 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
    'TPEX': 'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes',
    'TAIEX': 'https://openapi.twse.com.tw/v1/indicesReport/MI_5MINS_HIST',
}
CSV_COLUMNS = ('code', 'date', 'open', 'high', 'low', 'close')
MIN_COUNTS = {'TWSE': 100, 'TPEX': 100, 'TAIEX': 1}


def _numeric(v: Any) -> float | None:
    try:
        if v is None or isinstance(v, bool): return None
        t = str(v).replace(',', '').strip()
        if t in ('', '--', '-', '---', 'N/A', 'null'): return None
        x = float(t)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _date(v: Any) -> str:
    s = str(v or '').strip().replace('-', '').replace('/', '')
    if not s.isdecimal() or len(s) not in (7,8):
        raise ValueError('無效的行情交易日期')
    year = int(s[:-4]); month = int(s[-4:-2]); day = int(s[-2:])
    if year < 1911: year += 1911
    d = date(year, month, day)
    if not date(2000,1,1) <= d <= date(2100,1,1): raise ValueError('異常交易年份')
    return d.isoformat()


def _field(row: Mapping[str,Any], *options: str) -> Any:
    for k in options:
        if k in row and row[k] not in (None, ''): return row[k]
    return None


def _stock_row(source: str, row: Mapping[str,Any]) -> dict[str, Any] | None:
    if source == 'TWSE':
        code = str(_field(row,'Code','證券代號') or '').strip()
        day = _field(row,'Date','日期')
        o = _field(row,'OpeningPrice','開盤價')
        h = _field(row,'HighestPrice','最高價')
        l = _field(row,'LowestPrice','最低價')
        c = _field(row,'ClosingPrice','收盤價')
        v = _field(row,'TradeVolume','成交股數')
        amt = _field(row,'TradeValue','成交金額')
    else:
        code = str(_field(row,'SecuritiesCompanyCode','SecuritiesCode','Code','代號') or '').strip()
        day = _field(row,'Date','資料日期','日期')
        o = _field(row,'Open','OpeningPrice','開盤')
        h = _field(row,'High','HighestPrice','最高')
        l = _field(row,'Low','LowestPrice','最低')
        c = _field(row,'Close','ClosingPrice','收盤')
        v = _field(row,'TradingShares','TradeVolume','成交股數')
        amt = _field(row,'TransactionAmount','TradeValue','成交金額')
    # 普通股主池；排除權證、ETF、ETN以及字母混合代碼，不改寫股票代號。
    if not re.fullmatch(r'[1-9][0-9]{3}', code): return None
    d = _date(day)
    ohcl = [_numeric(t) for t in (o,h,l,c)]
    if any(x is None or x <= 0 for x in ohcl): return None
    a,b,lo,cl = ohcl
    if b + 1e-9 < max(a,lo,cl) or lo > min(a,b,cl) + 1e-9: return None
    volume = _numeric(v)
    amount = _numeric(amt)
    if volume is None or volume <= 0 or (amount is not None and amount < 0): return None
    return {'code':code,'date':d,'open':a,'high':b,'low':lo,'close':cl,
            'volume_shares':int(volume),'turnover_twd':int(amount) if amount is not None else None,
            'exchange':source}


def _index_row(row: Mapping[str,Any]) -> dict[str,Any] | None:
    d = _date(_field(row,'Date','日期'))
    values = [_numeric(_field(row,*ks)) for ks in [
        ('OpeningIndex','開盤指數'),('HighestIndex','最高指數'),
        ('LowestIndex','最低指數'),('ClosingIndex','收盤指數')]]
    if any(x is None or x <= 0 for x in values): return None
    o,h,l,c=values
    if h < max(o,l,c) or l > min(o,h,c): return None
    return {'code':'BENCHMARK','date':d,'open':o,'high':h,'low':l,'close':c,
            'exchange':'TAIEX','index_name':'TAIEX_PRICE_INDEX'}


def normalize_official(source: str, raw: Any, *, minimum: int | None = None) -> dict[str,Any]:
    if source not in MARKET_URLS: raise ValueError('未知行情資料來源')
    if not isinstance(raw,list) or not raw: raise ValueError(f'{source}: 非預期JSON格式或空白')
    cleaned = []; skipped = 0
    for row in raw:
        if not isinstance(row,dict): skipped += 1; continue
        try:
            item = _index_row(row) if source == 'TAIEX' else _stock_row(source,row)
        except (TypeError,ValueError):
            item=None
        if item is None: skipped += 1; continue
        cleaned.append(item)
    if len(cleaned) < (MIN_COUNTS[source] if minimum is None else minimum):
        raise ValueError(f'{source}: 有效行情僅{len(cleaned)}筆，不符合最低資料量；拒絕封存')
    days = {r['date'] for r in cleaned}
    if source == 'TAIEX':
        # Monthly TWSE index endpoint legitimately returns many historical days.
        # Pick latest; reject records from tomorrow by separate elapsed-time guard.
        current=max(days)
        cleaned = [r for r in cleaned if r['date']==current]
    else:
        if len(days)!=1: raise ValueError(f'{source}: 同次快照含多個交易日，不得混合')
        current=next(iter(days))
    keys=[(r['date'],r['code']) for r in cleaned]
    if len(keys)!=len(set(keys)):
        raise ValueError(f'{source}: 重複股票代號／日期，拒絕靜默覆蓋')
    cleaned.sort(key=lambda r:(r['date'],r['code']))
    return {'source':source,'day':current,'rows':cleaned,'skipped':skipped,
            'source_url':MARKET_URLS[source], 'benchmark_kind':'TAIEX_PRICE_INDEX' if source=='TAIEX' else ''}


def fetch_sources(http: Any=None, *, timeout: int = 25, attempts: int=2) -> dict[str,Any]:
    if http is None:
        import requests
        http=requests.Session()
    if timeout<1 or attempts<1 or attempts>4: raise ValueError('無效重試或超時設定')
    fetched={};errors={}
    for src,url in MARKET_URLS.items():
        for attempt in range(attempts):
            try:
                response=http.get(url,headers={'Accept':'application/json','User-Agent':'StockApp-H117-research/1.0'},timeout=timeout)
                if response.status_code!=200: raise RuntimeError(f'HTTP {response.status_code}')
                normalized=normalize_official(src,response.json())
                fetched[src]=normalized
                break
            except Exception as exc:
                errors[src]=f'{type(exc).__name__}: {exc}'[:220]
        if src in fetched: errors.pop(src,None)
    return {'sources':fetched,'errors':errors}


def resolve_freshness(fetched: Mapping[str,Any], *, now: datetime|None=None) -> dict[str,Any]:
    now = now or datetime.now(TZ)
    now = now.astimezone(TZ) if now.tzinfo else now.replace(tzinfo=TZ)
    # Waiting for ALL official post-close files; don't mistake intraday snapshots as settled.
    if now.time()<time(18,15):return {'status':'NOT_EOD_YET','details':{},'date':now.date().isoformat()}
    available=fetched.get('sources',{})
    if fetched.get('errors') or set(available)!=set(MARKET_URLS):
        return {'status':'INCOMPLETE_OFFICIAL_SOURCES','details':dict(fetched.get('errors') or {}),'date':now.date().isoformat()}
    expected=now.date().isoformat()
    got={src:v['day'] for src,v in available.items()}
    if any(d>expected for d in got.values()):
        return {'status':'FUTURE_DATE_REJECTED','details':got,'date':expected}
    if all(d<expected for d in got.values()):
        if len(set(got.values())) != 1:
            return {'status':'PARTIAL_MARKET_DAY','details':got,'date':expected}
        # Same stale date does not prove an exchange holiday; check official calendar separately.
        return {'status':'NO_NEW_MARKET_SESSION','details':got,'date':expected,
                'holiday_verified':False}
    if any(d!=expected for d in got.values()):
        return {'status':'PARTIAL_MARKET_DAY','details':got,'date':expected}
    return {'status':'READY_FOR_IMMUTABLE_PUBLISH','details':got,'date':expected}


def _sha(raw:bytes)->str:return hashlib.sha256(raw).hexdigest()


def _json_bytes(value:Any)->bytes:
    return (json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode('utf-8')


def make_official_day_manifest(normalized:Mapping[str,Any],*, collected_at:str)->dict[str,Any]:
    m={'schema':VERSION,'source':normalized['source'],'date':normalized['day'],
       'collected_at':collected_at,'source_url':normalized['source_url'],
       'row_count':len(normalized['rows']),'skipped':normalized.get('skipped',0),
       'rows':normalized['rows'],'status':'OFFICIAL_EOD_OBSERVED_NOT_ADJUSTED'}
    m['content_sha256']=_sha(_json_bytes(m))
    return m


def verify_manifest(m:Mapping[str,Any])->bool:
    if not isinstance(m,Mapping): return False
    body=dict(m); digest=body.pop('content_sha256',None)
    return bool(digest) and _sha(_json_bytes(body))==digest


def _atomic_write(target:Path, blob:bytes)->None:
    target.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(prefix='.'+target.name+'.',suffix='.tmp',dir=target.parent)
    try:
        with os.fdopen(fd,'wb') as f:
            f.write(blob);f.flush();os.fsync(f.fileno())
        os.replace(tmp,target)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)


def persist_official_daily(root:Path, batch:Mapping[str,Any], *, collected_at:str, require_day:str)->dict[str,Any]:
    """Preflight ALL files before any write; never overwrite an existing day, even after corrections.

    A partial disk error can cause an incomplete batch; next run preflight reconciles
    only identical manifests; different byte content becomes explicit conflict.
    """
    if set(batch)!=set(MARKET_URLS): raise ValueError('來源不齊全，拒絕發布')
    root=Path(root)
    pending=[];existing=0
    for src in MARKET_URLS:
        data=batch[src]
        if data['day']!=require_day:raise ValueError('交易日不一致')
        m=make_official_day_manifest(data,collected_at=collected_at)
        target=root/src/(require_day+'.json.gz')
        if target.exists():
            try:
                with gzip.open(target,'rt',encoding='utf-8') as f: old=json.load(f)
            except Exception as exc: raise ValueError(f'既存日資料不可驗證：{target}: {exc}')
            if not verify_manifest(old):raise ValueError(f'既存官方資料SHA錯誤：{target}')
            # Collection time can differ; content rows/source/day must not.
            fields=('source','date','row_count','rows','source_url')
            if any(old.get(k)!=m.get(k) for k in fields):
                raise ValueError(f'IMMUTABLE_OFFICIAL_CONFLICT: {src} {require_day}; 必須人工稽核差異')
            existing+=1
        else: pending.append((target,m))
    for target,m in pending:
        raw=_json_bytes(m)
        _atomic_write(target,gzip.compress(raw,mtime=0))
        with gzip.open(target,'rt',encoding='utf-8') as f: verify=json.load(f)
        if not verify_manifest(verify):raise RuntimeError(f'封存後回讀驗證失敗：{target}')
    return {'status':'UNCHANGED_VERIFIED' if not pending else 'WRITTEN_READBACK_VERIFIED',
            'new_files':len(pending),'existing_files':existing,'day':require_day,
            'durability':'LOCAL_VERIFIED_ONLY_UNTIL_GIT_COMMIT_OR_PERSISTENT_STORAGE'}


def load_official_days(root:Path)->dict[str,dict[str,Any]]:
    result={}
    for src in MARKET_URLS:
        dates={}
        for file in sorted((Path(root)/src).glob('*.json.gz')):
            with gzip.open(file,'rt',encoding='utf-8') as f:m=json.load(f)
            if not verify_manifest(m) or m['source']!=src or file.name!=m['date']+'.json.gz':
                raise ValueError(f'行情資料遭修改或路徑日期錯誤：{file}')
            if m['date'] in dates: raise ValueError('重複行情日期')
            dates[m['date']]=m
        result[src]=dates
    return result


def _csv_bytes(rows:list[dict[str,Any]])->bytes:
    f=io.StringIO(newline='')
    writer=csv.DictWriter(f,fieldnames=CSV_COLUMNS,lineterminator='\n')
    writer.writeheader()
    for r in rows:writer.writerow({k:r.get(k) for k in CSV_COLUMNS})
    return f.getvalue().encode('utf-8-sig')


def export_verified_h116_csv(root:Path,decisions_root:Path,outdir:Path)->dict[str,Any]:
    """Generate tiny H116 compatible tracked-stock OHLC and true OHLC benchmark.

    Skip days lacking both market stock snapshots and verified TAIEX benchmark.
    Never interpolate suspended/missing stock bars; gaps become H115 missing status.
    """
    from godpick_h116_daily_feedback import load_snapshot_files
    snapshots=load_snapshot_files(Path(decisions_root))
    codes={str(e.get('code')) for s in snapshots for e in s.get('decisions',[])
           if re.fullmatch(r'[1-9][0-9]{3}',str(e.get('code','')))}
    dailies=load_official_days(root)
    bm=[];tracked=[];missing=[]
    for day in sorted(dailies['TAIEX']):
        if day not in dailies['TWSE'] or day not in dailies['TPEX']:
            missing.append(day);continue
        ib=dailies['TAIEX'][day]['rows']
        if len(ib)!=1:raise ValueError('同日存在多筆基準指數，拒絕推算')
        bm.extend(ib)
        for src in ('TWSE','TPEX'):
            for row in dailies[src][day]['rows']:
                if row['code'] in codes:tracked.append(row)
    tracked.sort(key=lambda r:(r['date'],r['code']))
    bm.sort(key=lambda r:r['date'])
    if len({(r['date'],r['code']) for r in tracked})!=len(tracked):
        raise ValueError('TWSE/TPEX同日相同股票代碼，需人工確認上市櫃狀態')
    out=Path(outdir)
    result={'status':'NO_OFFICIAL_COMPLETE_DAYS' if not bm else 'EXPORTED_WITHOUT_SYNTHETIC_QUOTES',
            'session_days':len(bm),'tracked_codes':len(codes),'stock_bars':len(tracked),
            'missing_partial_market_days':missing,'immutable_snapshots':len(snapshots),
            'benchmark':'TAIEX_PRICE_INDEX_TRUE_OFFICIAL_OHLC',
            'note':'無快照、缺少盤後官方行情或尚未完成T+n時不計入績效'}
    # Deterministic output. Do not rewrite if unchanged, avoiding endless deployments.
    for name,rows in [('verified_ohlc.csv',tracked),('benchmark_ohlc.csv',bm)]:
        data=_csv_bytes(rows)
        p=out/name
        if not p.exists() or p.read_bytes()!=data:_atomic_write(p,data)
    return result

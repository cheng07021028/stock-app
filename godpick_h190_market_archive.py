# -*- coding: utf-8 -*-
"""H190 official date-batch archive, raw unadjusted quotes, no trading authority.

The public dated reports are collected off the interactive scan path. Archive
payloads are immutable, digest checked, market-specific and calendar aligned.
A download timestamp is never substituted for a historical publication date.
"""
from __future__ import annotations
from datetime import date, datetime, timedelta, time as clock
from pathlib import Path
from zoneinfo import ZoneInfo
from collections import defaultdict
import gzip, hashlib, json, math, os, re, tempfile, threading, time

VERSION = 'V191-H190-OFFICIAL-DAILY-MARKET-ARCHIVE'
ROOT = Path(__file__).resolve().parent / 'data' / 'h190'
TZ = ZoneInfo('Asia/Taipei')
URLS = {'TWSE':'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX',
        'TPEX':'https://www.tpex.org.tw/www/zh-tw/afterTrading/otc'}
CAL_URL = 'https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST'
MIN_ROWS = {'TWSE':700, 'TPEX':500}
CODE = re.compile(r'(?:[1-9]\d{3}|00\d{2,4}[A-Z]?)\Z')
COLS = ['日期','開盤價','最高價','最低價','收盤價','成交股數','成交金額','成交筆數','資料源']

def _bytes(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(',',':'), allow_nan=False).encode('utf-8')

def _seal(obj):
    return {**obj, 'sha256':hashlib.sha256(_bytes(obj)).hexdigest()}

def _verify(obj):
    if not isinstance(obj,dict): raise ValueError('invalid manifest')
    body=dict(obj); digest=body.pop('sha256',None)
    if digest!=hashlib.sha256(_bytes(body)).hexdigest(): raise ValueError('manifest digest mismatch')
    return body

def _write(path, obj):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(prefix='.'+path.name+'.',dir=path.parent)
    try:
        with os.fdopen(fd,'wb') as f:
            f.write(gzip.compress(_bytes(obj),mtime=0) if path.suffix=='.gz' else _bytes(obj)); f.flush(); os.fsync(f.fileno())
        os.replace(tmp,path)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)

def _read(path):
    path=Path(path)
    if path.stat().st_size>8_000_000: raise ValueError('archive too large')
    if path.suffix=='.gz':
        with gzip.open(path,'rb') as f: blob=f.read(16_000_001)
    else: blob=path.read_bytes()
    if len(blob)>16_000_000: raise ValueError('archive expanded too large')
    return _verify(json.loads(blob))

def _day(v):
    s=str(v or '').strip()
    m=re.fullmatch(r'(\d{2,4})[-/年](\d{1,2})[-/月](\d{1,2})日?',s)
    if m: y,mo,d=map(int,m.groups())
    elif re.fullmatch(r'\d{7,8}',s): y,mo,d=int(s[:-4]),int(s[-4:-2]),int(s[-2:])
    else: raise ValueError('date missing or unrecognized')
    if y<1911: y+=1911
    if not 2000<=y<=2100: raise ValueError('invalid year')
    return date(y,mo,d).isoformat()

def _date_evidence(raw, table):
    found=set()
    for obj in (raw,table):
        for key in ('date','Date','dataDate','reportDate','資料日期','日期'):
            if obj.get(key):
                try: found.add(_day(obj[key]))
                except ValueError: pass
        for key in ('title','subtitle','name'):
            for s in re.findall(r'\d{2,4}年\d{1,2}月\d{1,2}日|\d{2,4}[/\-]\d{1,2}[/\-]\d{1,2}',str(obj.get(key,''))):
                found.add(_day(s))
    return found

def _num(v):
    try:
        if v is None or isinstance(v,bool): return None
        f=float(str(v).strip().replace(',',''))
        return f if math.isfinite(f) else None
    except (ValueError,TypeError,OverflowError): return None

def _field(v):
    return re.sub(r'\s+','',str(v)).replace('（','(').replace('）',')')

ALIASES = {
    'code':('證券代號','代號','股票代號'),
    'open':('開盤價','開盤'), 'high':('最高價','最高'),
    'low':('最低價','最低'), 'close':('收盤價','收盤'),
    'shares':('成交股數','成交股數(股)','成交量(股)'),
    'amount':('成交金額','成交金額(元)'),
    'trades':('成交筆數','成交筆數(筆)','筆數'),
}

def parse_daily(raw, market, day, *, observed_at):
    """Select named columns only; no inferred units and no request-date substitution."""
    day=_day(day)
    observed=datetime.fromisoformat(observed_at)
    if observed.tzinfo is None or day>observed.astimezone(TZ).date().isoformat(): raise ValueError('invalid observation time')
    if day==observed.astimezone(TZ).date().isoformat() and observed.astimezone(TZ).time()<clock(18,15): raise ValueError('current session not yet eligible for EOD collection')
    if market not in URLS or not isinstance(raw,dict): raise ValueError('unexpected daily response')
    if str(raw.get('stat','')).strip().lower() not in ('','ok'): raise ValueError('upstream: '+str(raw.get('stat'))[:80])
    tables=raw.get('tables') or [raw]
    candidates=[]
    for table in tables:
        if not isinstance(table,dict): continue
        fields=[_field(x) for x in (table.get('fields') or table.get('headers') or [])]
        idx={k:next((fields.index(a) for a in aliases if a in fields),None) for k,aliases in ALIASES.items()}
        if all(idx[k] is not None for k in ('code','open','high','low','close','shares','amount')):
            candidates.append((table,idx))
    if len(candidates)!=1: raise ValueError('missing or ambiguous named full-market table; units not proven')
    table,idx=candidates[0]
    if _date_evidence(raw,table)!={day}: raise ValueError('source trade date does not match request')
    rows=[]; seen=set(); eligible=0; invalid=0
    for row in table.get('data',[]):
        if not isinstance(row,(list,tuple)) or len(row)<=max(i for i in idx.values() if i is not None): continue
        code=str(row[idx['code']]).strip()
        if not CODE.fullmatch(code): continue
        eligible+=1
        if code in seen: raise ValueError('duplicate code on market day')
        seen.add(code)
        o,h,l,c,v,a=[_num(row[idx[k]]) for k in ('open','high','low','close','shares','amount')]
        n=_num(row[idx['trades']]) if idx['trades'] is not None else None
        # Zero-volume/suspended securities remain gaps, not synthetic bars.
        if (any(x is None or x<=0 for x in (o,h,l,c,v,a)) or h<max(o,l,c) or l>min(o,h,c)
            or not v.is_integer() or not a.is_integer() or (n is not None and (n<0 or not n.is_integer()))):
            invalid+=1; continue
        # Detect common thousand-share / thousand-dollar mismatches without estimating turnover.
        if not l*0.5<=a/v<=h*2: invalid+=1; continue
        rows.append([code,o,h,l,c,int(v),int(a),int(n) if n is not None else None])
    if len(rows)<MIN_ROWS[market] or len(rows)<eligible*0.85: raise ValueError(f'coverage insufficient: valid={len(rows)}, eligible={eligible}')
    return _seal({'version':VERSION,'market':market,'day':day,'observed_at':observed_at,'source_url':URLS[market],
        'basis':'RAW_UNADJUSTED_FIRST_OBSERVED_BACKFILL','session_scope':'TPEX_ROUND_LOT_REPORT_EW' if market=='TPEX' else 'TWSE_MI_INDEX_ALLBUT0999','row_count':len(rows),'eligible_rows':eligible,
        'invalid_rows':invalid,'rows':sorted(rows,key=lambda r:r[0])})

def save_day(root, payload):
    p=_verify(payload); path=Path(root)/'daily'/p['market']/(p['day']+'.json.gz')
    if path.exists():
        old=_read(path)
        if any(old[k]!=p[k] for k in ('market','day','rows','source_url')): raise ValueError('immutable day conflict')
        return False
    _write(path,payload); _read(path); _invalidate(root); return True

def parse_calendar(raw, month, *, observed_at):
    """Historical TAIEX daily index dates, not guessed Mon-Fri sessions."""
    if not isinstance(raw,dict) or str(raw.get('stat','')).strip().lower()!='ok': raise ValueError('calendar source unavailable')
    obs=datetime.fromisoformat(observed_at)
    if obs.tzinfo is None: raise ValueError('calendar time missing timezone')
    fields=[_field(x) for x in raw.get('fields',[])]
    if '日期' not in fields or not any(x in fields for x in ('收盤指數','收盤價')): raise ValueError('calendar fields missing')
    i=fields.index('日期'); j=next(fields.index(x) for x in ('收盤指數','收盤價') if x in fields)
    days=[]
    for r in raw.get('data',[]):
        d=_day(r[i]); close=_num(r[j]);
        if d[:7]!=month or d>obs.astimezone(TZ).date().isoformat() or close is None or close<=0: raise ValueError('invalid calendar session')
        days.append(d)
    if not days or len(days)!=len(set(days)): raise ValueError('empty/duplicate calendar')
    return _seal({'version':VERSION,'month':month,'observed_at':observed_at,'source_url':CAL_URL,'days':sorted(days)})

def save_calendar(root,payload):
    p=_verify(payload); path=Path(root)/'calendar'/(p['month']+'.json')
    if path.exists():
        old=_read(path)
        if not set(old['days']).issubset(p['days']): raise ValueError('calendar revision conflict')
        if old['days']==p['days']:
            old_obs=datetime.fromisoformat(old['observed_at']).astimezone(TZ)
            new_obs=datetime.fromisoformat(p['observed_at']).astimezone(TZ)
            month_finalized=old['observed_at'][:7]<=p['month']<p['observed_at'][:7]
            # The index can appear before 18:15. Same dates are NOT evidence of
            # post-close confirmation until a fresh official response is received.
            eod_confirm=(new_obs.date().isoformat()==max(p['days']) and new_obs.time()>=clock(18,15)
                         and (old_obs.date()!=new_obs.date() or old_obs.time()<clock(18,15)))
            if new_obs<=old_obs or not (month_finalized or eod_confirm): return False
    _write(path,payload); _invalidate(root); return True

def _months(start,end):
    y,m=start.year,start.month
    while (y,m)<=(end.year,end.month):
        yield f'{y:04d}-{m:02d}'
        y,m=(y+1,1) if m==12 else (y,m+1)

def _get(http,url,params,timeout):
    resp=http.get(url,params=params,headers={'User-Agent':'StockApp-H190/1.0','Accept':'application/json'},timeout=timeout)
    if resp.status_code!=200: raise RuntimeError(f'HTTP {resp.status_code}')
    return resp.json()

def collect(root=ROOT, *, http=None, now=None, budget=24, target=250, timeout=12, pause=1.2):
    """Scheduled date-batch collection, bounded requests and independent market failures."""
    if http is None:
        import requests
        http=requests.Session()
    if not 1<=budget<=60 or not 85<=target<=250: raise ValueError('invalid budget or target')
    now=(now or datetime.now(TZ)).astimezone(TZ); end=now.date() if now.time()>=clock(18,15) else now.date()-timedelta(days=1)
    start=end-timedelta(days=430); root=Path(root); observed=now.isoformat(timespec='seconds')
    errors={}; days=set(); months_missing=[]; requests=0
    for month in reversed(list(_months(start,end))):
        path=root/'calendar'/(month+'.json')
        try:
            existing=_read(path) if path.is_file() else None
            if existing is None or month==end.isoformat()[:7] or existing['observed_at'][:7]<=month:
                requests+=1
                try: save_calendar(root,parse_calendar(_get(http,CAL_URL,{'date':month.replace('-','')+'01','response':'json'},timeout),month,observed_at=observed))
                finally: time.sleep(pause)
            days.update(_read(path)['days'])
        except Exception as e:
            errors['calendar:'+month]=str(e)[:150]; months_missing.append(month)
            # Preserve existing verified dates, but current month freshness is not certified.
            try: days.update(_read(path)['days'])
            except Exception: pass
    ordered=sorted((d for d in days if start.isoformat()<=d<=end.isoformat()),reverse=True)[:target]
    # Always check newest missing dates. Persist failures so a bad older date cannot starve all other history.
    state=root/'health.json'; attempts={}
    try: attempts=_read(state).get('failed_attempts',{})
    except Exception: pass
    candidates=[]; saved=0; checked=0; consecutive={m:0 for m in URLS}
    for day in ordered:
        for market in URLS:
            path=root/'daily'/market/(day+'.json.gz')
            if path.is_file(): continue
            key=market+':'+day
            # Prioritize the latest 60 sessions for BOTH markets; older untried days
            # must not starve the recent missing side after a source repair.
            priority = (0 if day in ordered[:2] else 1 if day in ordered[:60] else 2,
                        attempts.get(key,0))
            candidates.append((priority,day,market,key))
    candidates.sort(key=lambda x:(x[0],-date.fromisoformat(x[1]).toordinal(),x[2]))
    # Two requests per date maximum; budget measured in requests to guarantee bound.
    for _,day,market,key in candidates:
        if checked>=budget*2: break
        if consecutive[market]>=3: continue
        checked+=1; requests+=1
        try:
            params={'date':day.replace('-',''),'type':'ALLBUT0999','response':'json'} if market=='TWSE' else {'date':day.replace('-','/'),'type':'EW','response':'json'}
            saved+=int(save_day(root,parse_daily(_get(http,URLS[market],params,timeout),market,day,observed_at=observed)))
            consecutive[market]=0; attempts.pop(key,None)
        except Exception as e:
            errors[key]=str(e)[:150]; attempts[key]=attempts.get(key,0)+1; consecutive[market]+=1
        finally: time.sleep(pause)
    coverage={m:sum((root/'daily'/m/(d+'.json.gz')).is_file() for d in ordered) for m in URLS}
    paired=sum(all((root/'daily'/m/(d+'.json.gz')).is_file() for m in URLS) for d in ordered)
    status='TARGET_FILES_PRESENT_READER_VERIFICATION_REQUIRED' if paired>=target and not months_missing else 'BACKFILL_PENDING_OR_SOURCE_GAP'
    result={'version':VERSION,'observed_at':observed,'status':status,'target_sessions':target,'calendar_sessions':len(ordered),
        'latest_calendar_session':ordered[0] if ordered else None,'paired_files_present':paired,'market_files_present':coverage,
        'new_files':saved,'requests':requests,'calendar_missing':months_missing,'errors':errors,'failed_attempts':attempts,
        'corporate_actions':'UNADJUSTED; historical PIT / total return not certified','trade_authority_changed':False}
    _write(state,_seal(result)); return result

# Read only: a single process-level index shared across scan threads.  No HTTP.
_LOCK=threading.RLock(); _CACHE={}

def _invalidate(root):
    with _LOCK: _CACHE.pop(str(Path(root).resolve()),None)

def _index(root):
    root=Path(root); key=str(root.resolve())
    with _LOCK:
        old=_CACHE.get(key); now=time.monotonic()
        if old and now-old[0]<60: return old[1]
        paths=sorted((root/'calendar').glob('*.json'))+sorted((root/'daily').glob('*/*.json.gz'))
        sig=tuple((str(p),p.stat().st_mtime_ns,p.stat().st_size) for p in paths)
        if old and old[2]==sig:
            _CACHE[key]=(now,old[1],sig); return old[1]
        calendars={}; markets={m:{} for m in URLS}; rows=defaultdict(list); errors=[]
        for p in paths:
            try:
                v=_read(p)
                if v.get('version')!=VERSION: raise ValueError('unknown schema')
                if p.parent.name=='calendar':
                    if p.stem!=v['month'] or v['source_url']!=CAL_URL: raise ValueError('calendar provenance mismatch')
                    if v['month']<datetime.now(TZ).date().isoformat()[:7] and v['observed_at'][:7]<=v['month']:
                        raise ValueError('past month not finalized by later observation')
                    if any(_day(d)!=d or d[:7]!=v['month'] for d in v['days']): raise ValueError('invalid calendar day')
                    calendars[v['month']]=set(v['days']); continue
                m=p.parent.name; d=v['day']
                if _day(d)!=d or d>datetime.fromisoformat(v['observed_at']).astimezone(TZ).date().isoformat(): raise ValueError('day after observation')
                if m not in URLS or v['market']!=m or p.name!=d+'.json.gz' or v['source_url']!=URLS[m] or v['row_count']!=len(v['rows']) or len(v['rows'])<MIN_ROWS[m] or v.get('basis')!='RAW_UNADJUSTED_FIRST_OBSERVED_BACKFILL': raise ValueError('day provenance/coverage mismatch')
                codes=[r[0] for r in v['rows']]
                if len(set(codes))!=len(codes): raise ValueError('duplicate day keys')
                markets[m][d]=len(codes)
                for r in v['rows']: rows[m,r[0]].append((d,*r[1:]))
            except Exception as e: errors.append(p.name+': '+str(e)[:100])
        data=(calendars,markets,rows,errors)
        _CACHE.clear(); _CACHE[key]=(now,data,sig); return data

def archive_history(code,market,start,end,*,root=ROOT):
    """Return only full requested calendar span with exact stock session coverage.

    Missing days, new listing, suspension, stale calendar, unknown market or
    corrupted partitions return no archive result; original fallback remains.
    """
    import pandas as pd
    empty=pd.DataFrame(columns=COLS)
    market={'上市':'TWSE','上櫃':'TPEX','TWSE':'TWSE','TPEX':'TPEX','TPEx':'TPEX'}.get(str(market),'')
    if not market or not CODE.fullmatch(str(code)): return empty
    start=date.fromisoformat(str(start)[:10]); end=date.fromisoformat(str(end)[:10])
    if start>end or end>datetime.now(TZ).date(): return empty
    calendars,markets,rows,errors=_index(root)
    months=list(_months(start,end))
    if any(m not in calendars for m in months): return empty
    expected=sorted(d for m in months for d in calendars[m] if start.isoformat()<=d<=end.isoformat())
    if not expected or any(d not in markets[market] for d in expected): return empty
    # Current month's calendar may lag. The caller's existing freshness guard is also applied.
    subset=sorted((r for r in rows.get((market,str(code)),[]) if start.isoformat()<=r[0]<=end.isoformat()),key=lambda r:r[0])
    if [r[0] for r in subset]!=expected: return empty
    f=pd.DataFrame(subset,columns=COLS[:-1]); f['日期']=pd.to_datetime(f['日期']); f['資料源']='H190_'+market+'_OFFICIAL_RAW_UNADJUSTED'
    return f

def inspect(root=ROOT):
    cal,markets,rows,errors=_index(root); days=sorted(set().union(*cal.values())) if cal else []
    paired=sorted(set(markets['TWSE'])&set(markets['TPEX'])&set(days[-250:]))
    latest=days[-1] if days else None
    recent_months=list(_months(date.fromisoformat(days[-250]),date.fromisoformat(latest))) if len(days)>=250 else []
    missing_months=[m for m in recent_months if m not in cal]
    recent=set(days[-60:]); valid60={m:sum(len({r[0] for r in seq}&recent)==60 for (market,code),seq in rows.items() if market==m and re.fullmatch(r'[1-9]\d{3}',code)) if len(recent)==60 else 0 for m in URLS}
    latest_counts={m:sum(any(r[0]==latest for r in seq) for (market,code),seq in rows.items() if market==m and re.fullmatch(r'[1-9]\d{3}',code)) for m in URLS}
    return {'version':VERSION,'calendar_sessions':len(days),'latest_calendar_session':latest,'paired_verified_sessions':len(paired),
        'latest_paired_session':paired[-1] if paired else None,'calendar_gap_months_in_target':missing_months,
        'target_250_archived_sessions_ready':len(paired)>=250 and not missing_months and not errors,'market_verified_sessions':{m:len(x) for m,x in markets.items()},
        'complete_recent_60_symbols':valid60,'latest_session_ordinary_code_counts':latest_counts,'read_errors':errors[:30], 'corporate_actions':'UNADJUSTED_NOT_CERTIFIED'}

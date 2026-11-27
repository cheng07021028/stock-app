# -*- coding: utf-8 -*-
"""Official raw-price N-day tracking. No signal generation or current-list replacement."""
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo
from bisect import bisect_right
from collections import Counter
import json, math, re, time as wall
import pandas as pd
TZ=ZoneInfo('Asia/Taipei')
ROOT=Path(__file__).resolve().parent
PERIODS=(1,3,5,10,20)
COLS=tuple(f'推薦後{n}日%' for n in PERIODS)
PROOF='H250R2_N日證據'
VERSION='H250R2_OFFICIAL_RAW_NDAY_V1'
MARKETS={'上市':'TWSE','上櫃':'TPEX','TWSE':'TWSE','TPEX':'TPEX','TPEx':'TPEX'}

def text(v):
    if v is None:return ''
    try:
        if pd.isna(v):return ''
    except (ValueError,TypeError):return ''
    s=str(v).strip()
    return "" if s.lower() in {"nan","none","null","<na>","nat"} else s

def number(v):
    try:
        if isinstance(v,bool):return None
        x=float(str(v).replace(',',''));return x if math.isfinite(x) else None
    except (ValueError,TypeError):return None

def positive(v):
    n=number(v);return n if n is not None and n>0 else None

def identity(r,now):
    rid=text(r.get('record_id')); code=text(r.get('股票代號')); m=MARKETS.get(text(r.get('市場別')))
    raw=text(r.get('推薦日期')); match=re.fullmatch(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?:[ T].*)?',raw)
    if not rid or not re.fullmatch(r'\d{4,6}',code) or not m or not match:return None
    try:d=date(*map(int,match.groups()))
    except ValueError:return None
    if not date(2000,1,1)<=d<=now.date():return None
    return rid,m,code,d.isoformat()

def months(a,b):
    y,m=map(int,a[:7].split('-'));end=b[:7]
    while f'{y:04d}-{m:02d}'<=end:
        yield f'{y:04d}-{m:02d}';y,m=(y+1,1) if m==12 else (y,m+1)

class Calendar:
    def __init__(self,cal,observed,now):
        self.cal=cal;self.observed=observed;self.now=now;today=now.date().isoformat()
        self.days=sorted({d for ds in cal.values() for d in ds if d<today or (d==today and now.time()>=time(18,15) and observed.get(d[:7],datetime.min.replace(tzinfo=TZ)).date()==now.date() and observed[d[:7]].time()>=time(18,15))});self.dayset=set(self.days)
    def span(self,d,n):
        if d not in self.dayset:return None
        i=bisect_right(self.days,d);ds=self.days[i:i+n]
        if len(ds)!=n or any(m not in self.cal for m in months(d,ds[-1])):return None
        return [d,*ds]
    def reason(self,d):
        if d[:7] not in self.cal:return 'CALENDAR_GAP'
        if d not in self.dayset:
            return 'NOT_DUE' if d==self.now.date().isoformat() else 'CALENDAR_GAP'
        # A missing intervening month must not masquerade as an immature row.
        if any(m not in self.cal for m in months(d,self.now.date().isoformat())):return 'CALENDAR_GAP'
        return 'NOT_DUE'

def load_calendar(root=ROOT,now=None):
    import godpick_h190_market_archive as ar
    now=now or datetime.now(TZ);cal={};obs={}
    for p in sorted((Path(root)/'data/h190/calendar').glob('*.json')):
        try:
            v=ar._read(p);t=datetime.fromisoformat(v['observed_at']).astimezone(TZ);ds=v['days'];m=p.stem
            if v['version']!=ar.VERSION or v['month']!=m or v['source_url']!=ar.CAL_URL or t>now or not ds or len(ds)!=len(set(ds)):continue
            if m<now.date().isoformat()[:7] and t.date().isoformat()[:7]<=m:continue
            if any(ar._day(d)!=d or d[:7]!=m or d>t.date().isoformat() for d in ds):continue
            cal[m]=set(ds);obs[m]=t
        except (OSError,ValueError,KeyError,TypeError):continue
    return Calendar(cal,obs,now)

def proof(r,now):
    try:
        p=json.loads(text(r.get(PROOF)));ident=identity(r,now)
        if not isinstance(p,dict) or not ident or p.get('version')!=VERSION or tuple(p['identity'])!=ident or not isinstance(p['periods'],dict):return {}
        if p['base']!=base_price(r) and base_price(r) is not None:return {}
        return p
    except (ValueError,TypeError,KeyError,AttributeError):return {}

def base_price(r):
    return next((v for k in ('推薦價格','推薦日價格','建議價位') if (v:=positive(r.get(k))) is not None),None)

def verified_value(r,n,now=None):
    now=now or datetime.now(TZ);p=proof(r,now);entry=p.get('periods',{}).get(str(n),{});v=number(r.get(f'推薦後{n}日%'))
    try:
        if v is None or entry['value']!=v or not positive(entry['close']) or not positive(p['base']):return None
        if not p['identity'][3]<entry['target']<=now.date().isoformat():return None
        if round((entry['close']/p['base']-1)*100,2)!=v:return None
        return v
    except (KeyError,TypeError,ValueError):return None

def mask_formal(df):
    out=df.copy();now=datetime.now(TZ);rows=out.to_dict('records')
    for n,c in zip(PERIODS,COLS):out[c]=pd.Series([verified_value(r,n,now) for r in rows],index=out.index,dtype=float)
    return out

def series(df,col):
    if col not in COLS:return pd.to_numeric(df[col],errors='coerce') if col in df else pd.Series(float('nan'),index=df.index)
    n=PERIODS[COLS.index(col)];now=datetime.now(TZ)
    return pd.Series([verified_value(r,n,now) for r in df.to_dict('records')],index=df.index,dtype=float)

def due(r,cal):
    ident=identity(r,cal.now)
    if not ident:return [],'IDENTITY_PENDING'
    missing=[n for n in PERIODS if verified_value(r,n,cal.now) is None]
    if not missing:return [],'VERIFIED'
    ds=[n for n in missing if cal.span(ident[3],n)]
    return (ds,'DUE') if ds else ([],cal.reason(ident[3]))

def diagnose(df,root=ROOT,cal=None):
    cal=cal or load_calendar(root);out=Counter();seen=Counter(text(r.get('record_id')) for r in df.to_dict('records'))
    for r in df.to_dict('records'):
        if seen[text(r.get('record_id'))]>1:out['IDENTITY_PENDING']+=1;continue
        ns,status=due(r,cal);out[status]+=1;out['due_periods']+=len(ns)
    return dict(out)

def compute(r,cal,quotes,force=False):
    ident=identity(r,cal.now);ns,status=due(r,cal)
    if force and ident:ns=[n for n in PERIODS if cal.span(ident[3],n)]
    if not ns:return {},status
    _,m,c,d=ident;base=base_price(r) or positive(quotes.get((m,c),{}).get(d,()))
    # quotes contain close only. No Yahoo-adjusted/proxy price and no skipped sessions.
    if base is None:return {},'BASE_PENDING'
    p=proof(r,cal.now);entries={str(n):p['periods'][str(n)] for n in PERIODS if verified_value(r,n,cal.now) is not None and p.get('base')==base}
    result={}
    for n in ns:
        ds=cal.span(d,n);prices=[positive(quotes.get((m,c),{}).get(x)) for x in ds]
        if any(x is None for x in prices):continue
        value=round((prices[-1]/base-1)*100,2);result[f'推薦後{n}日%']=value
        entries[str(n)]={'target':ds[-1],'close':prices[-1],'value':value}
    if not result:return {},'QUOTE_PENDING'
    if entries==p.get('periods') and all(number(r.get(k))==v for k,v in result.items()):return {},'VERIFIED'
    result[PROOF]=json.dumps({'version':VERSION,'identity':ident,'base':base,'periods':entries,'observed_at':cal.now.isoformat(),'basis':'H190_RAW_UNADJUSTED_PRICE_RETURN_NOT_NET'},ensure_ascii=False,sort_keys=True,separators=(',',':'))
    return result,'UPDATED'

def plan(rows,cal,quotes,limit=300,force=False):
    """Scan all rows to avoid first-N starvation; only cap actual changed rows."""
    counts=Counter(text(r.get('record_id')) for r in rows);stats=Counter();patches={}
    for r in rows:
        rid=text(r.get('record_id'))
        if counts[rid]>1:stats['IDENTITY_PENDING']+=1;continue
        delta,status=compute(r,cal,quotes,force);stats[status]+=1
        if delta:
            if len(patches)<limit:
                patches[rid]=delta
                if any(f'推薦後{n}日%' not in delta for n in due(r,cal)[0]):stats['PARTIAL_PENDING']+=1
            else:stats['DEFERRED']+=1
    return patches,dict(stats)

def run_authority(root=ROOT,max_rows=300,force=False):
    """Only update existing exact H241 record IDs under its fixed-head CAS; no list/snapshot write."""
    import godpick_h190_market_archive as ar
    import godpick_h241_github_records as h
    import godpick_persistence_service as p
    now=datetime.now(TZ);limit=max(1,min(int(max_rows),300));start=wall.monotonic()
    rows,_,_=h.ensure()
    if p.records_authority_verification_h240().get('status')!='VERIFIED':return False,{'status':'AUTHORITY_PENDING','success':0},['H241 歷史權威未核實；不寫入']
    sig=p.records_authority_signature();cal,markets,index,errors=ar._index(Path(root)/'data/h190',asof=now)
    if errors:return False,{'status':'ARCHIVE_PENDING','success':0},['H190 原始行情驗證未通過；不寫入']
    calendar=load_calendar(root,now)
    if {m:set(v) for m,v in cal.items()}!=calendar.cal:return False,{'status':'CALENDAR_PENDING','success':0},['H190 日曆版本不一致；不寫入']
    quotes={k:{r[0]:r[4] for r in rs} for k,rs in index.items()}
    patches,stats=plan(rows,calendar,quotes,limit,force);summary={'candidates':stats.get('UPDATED',0)+stats.get('QUOTE_PENDING',0)+stats.get('BASE_PENDING',0),'processed':len(patches),'success':0,'fail':0,'remaining':stats.get('DEFERRED',0)+stats.get('QUOTE_PENDING',0)+stats.get('BASE_PENDING',0)+stats.get('PARTIAL_PENDING',0),'retryable_remaining':stats.get('DEFERRED',0),'diagnostics':stats,'status':'NO_VERIFIED_CHANGE','elapsed_seconds':round(wall.monotonic()-start,2)}
    if not patches:return True,summary,['沒有可安全回寫的新正式績效；保留資料缺口']
    try:
        with p._RECORDS_LOCAL_LOCK,p._RECORDS_GITHUB_UPLOAD_LOCK:
            base,head,sha=h._base(sig)
            ids={r['record_id'] for r in base}
            if not set(patches)<=ids:raise ValueError('ID changed')
            final=[dict(r,**patches[r['record_id']]) if r['record_id'] in patches else r for r in base]
            report=h._commit(base,head,sha,final,set(),'H250R2 official N-day exact-ID metrics only')
        ok=bool(report.permanent_ok and report.local_ok and report.github_ok)
        summary.update(success=len(patches) if ok else 0,fail=0 if ok else len(patches),remaining=summary['remaining']+(0 if ok else len(patches)-stats.get('PARTIAL_PENDING',0)),status='VERIFIED_SAVED' if ok else 'REMOTE_PENDING',elapsed_seconds=round(wall.monotonic()-start,2))
        return ok,summary,report.messages()
    except Exception as e:
        summary.update(status='CONFLICT_OR_ERROR',fail=len(patches),remaining=summary['remaining']+len(patches)-stats.get('PARTIAL_PENDING',0));return False,summary,['H250R2 權威衝突或保存異常：'+type(e).__name__]

def overlay_verified_history(frame,history):
    """Only display verified matching historical metric fields; current snapshot unchanged."""
    if frame.empty or history.empty:return frame
    now=datetime.now(TZ);rows=history.to_dict('records');ids=Counter(text(r.get('record_id')) for r in rows);by={identity(r,now):r for r in rows if ids[text(r.get('record_id'))]==1 and identity(r,now)}
    out=frame.copy()
    if PROOF not in out:out[PROOF]=None
    for i,r in out.iterrows():
        src=by.get(identity(r,now))
        if src is None or base_price(r)!=base_price(src):continue
        valid={f'推薦後{n}日%':v for n in PERIODS if (v:=verified_value(src,n,now)) is not None}
        if valid:
            for k,v in valid.items():out.at[i,k]=v
            out.at[i,PROOF]=src[PROOF]
    return out

def preserve_verified(old,out):
    """Generic upserts cannot overwrite proof-bearing metrics or import a proof from a projection.
    Only the exact-ID H250R2 commit publishes new evidence.
    """
    if PROOF not in old and PROOF not in out:return out
    now=datetime.now(TZ)
    if PROOF in old:out[PROOF]=old[PROOF]
    else:out.pop(PROOF,None)
    for n in PERIODS:
        v=verified_value(old,n,now)
        if v is not None:out[f'推薦後{n}日%']=v
    return out

def display_status(df):
    now=datetime.now(TZ);result=[]
    for r in df.to_dict('records'):
        good=[str(n) for n in PERIODS if verified_value(r,n,now) is not None]
        old=[str(n) for n,c in zip(PERIODS,COLS) if str(n) not in good and number(r.get(c)) is not None]
        result.append(('官方原價已核 '+ '/'.join(good)+'日' if good else '尚無本版核實N日')+('｜舊值待核 '+ '/'.join(old)+'日' if old else ''))
    return pd.Series(result,index=df.index,dtype=object)

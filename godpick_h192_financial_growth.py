# -*- coding: utf-8 -*-
"""H192 financial evidence and independent research pools. No trade authority.
All web access is in collect(), never in evaluate()/run(). Observed time is not
publication time. Latest official statements are NOT a historical backfill.
"""
from __future__ import annotations
import calendar, json, math, re, statistics, unicodedata
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, time
from pathlib import Path
from zoneinfo import ZoneInfo
import godpick_h190_market_archive as archive

VERSION='V191-H192-FINANCIAL-GROWTH-VALUATION'
ROOT=Path(__file__).resolve().parent
TZ=ZoneInfo('Asia/Taipei')
POOLS=('營收成長','營收近13月新高','連兩季三率三升','獲利突破','低PE高TTM EPS','H191財技共振','單月營收觀察','月增待年增補證')
URLS={
 ('revenue','TWSE'):'https://openapi.twse.com.tw/v1/opendata/t187ap05_L',
 ('revenue','TPEX'):'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O',
 ('margin','TWSE'):'https://openapi.twse.com.tw/v1/opendata/t187ap17_L',
 ('margin','TPEX'):'https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O',
 ('ytd','TWSE'):'https://openapi.twse.com.tw/v1/opendata/t187ap06_L_ci',
 ('ytd','TPEX'):'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_ci',
 ('pe','TWSE'):'https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL',
 ('pe','TPEX'):'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis',
}
METRICS={'revenue':('revenue_twd','previous_month_twd','prior_year_month_twd'), 'margin':('gross_margin_pct','operating_margin_pct','net_margin_pct'), 'ytd':('eps_ytd','operating_profit_ytd','net_profit_ytd'), 'pe':('pe',)}
MARKETS={'TWSE':'TWSE','TPEX':'TPEX','TPEx':'TPEX','上市':'TWSE','上櫃':'TPEX'}
MIN_ROWS={'TWSE':500,'TPEX':350}

def num(v):
    if isinstance(v,bool) or v is None:return None
    try:
        f=float(str(v).strip().replace(',',''))
        return f if math.isfinite(f) and abs(f)<1e15 else None
    except (TypeError,ValueError,OverflowError):return None

def moment(v):
    try:
        t=datetime.fromisoformat(str(v).replace('Z','+00:00'))
        return t.astimezone(TZ) if t.tzinfo else None
    except (TypeError,ValueError,OverflowError):return None

def day(v):
    try:return archive._day(v)
    except (TypeError,ValueError):return ''

def month(v):
    s=str(v or '').strip().replace('/','').replace('-','')
    if not re.fullmatch(r'\d{5,6}',s):return None
    y,m=int(s[:-2]),int(s[-2:]); y=y+1911 if y<1911 else y
    return y*12+m-1 if 2000<=y<=2100 and 1<=m<=12 else None

def monthstr(n):return f'{n//12:04d}{n%12+1:02d}'

def quarter(v):
    m=re.fullmatch(r'(\d{3,4})Q0?([1-4])',str(v or '').strip().upper())
    if not m:return None
    y=int(m[1]);y=y+1911 if y<1911 else y
    return y*4+int(m[2])-1 if 2000<=y<=2100 else None

def qstr(n):return f'{n//4}Q{n%4+1}'

def period_end(kind,n):
    if kind=='pe':return date.fromisoformat(n)
    y,m=divmod(n,12) if kind=='revenue' else (n//4,(n%4+1)*3-1)
    return date(y,m+1,calendar.monthrange(y,m+1)[1])

def expected_periods(now):
    # Conservative general-industry reporting windows. Not a company-specific deadline.
    y,m=now.year,now.month
    ym=y*12+m-1-(1 if now.day>10 else 2)
    dates=[(date(y,4,1),(y-1)*4+3),(date(y,5,16),y*4),(date(y,8,15),y*4+1),(date(y,11,15),y*4+2)]
    q=max([((y-1)*4+2)]+[n for d,n in dates if d<=now.date()]);return ym,q

def value(r,*keys):
    for k in keys:
        if k in r:return r[k]
    return None

def parse(kind,market,raw,observed_at):
    """Only named official columns. Comparatives stay on current release row."""
    t=moment(observed_at)
    if not t or (kind,market) not in URLS or not isinstance(raw,list):raise ValueError('invalid source response')
    out=[]
    for r in raw:
        if not isinstance(r,dict):continue
        # Only remove presentation noise; do not resolve collided field names.
        names=[unicodedata.normalize('NFKC',str(k)).strip().lstrip('\ufeff').strip() for k in r]
        if len(set(names))!=len(names):continue
        # Preserve raw exact labels for YTD comparison with existing archives.
        if kind in ('margin','pe'):r=dict(zip(names,r.values()))
        c=str(value(r,'公司代號','CompanyCode','Code','SecuritiesCompanyCode','StockId','stock_id','股票代號','證券代號','代號') or '').strip()
        if not re.fullmatch(r'[1-9]\d{3}',c):continue
        item={'code':c,'source':market,'source_url':URLS[kind,market],'availability_basis':'FIRST_OBSERVED','available_at':t.isoformat(timespec='seconds')}
        if kind=='revenue':
            p=month(value(r,'資料年月','RevenueYearMonth'))
            for k,rawkey in zip(METRICS[kind],('營業收入-當月營收','營業收入-上月營收','營業收入-去年當月營收')):
                v=num(r.get(rawkey));item[k]=int(round(v*1000)) if v is not None and v>=0 else None
            if p is None or item['revenue_twd'] is None:continue
            item['year_month']=monthstr(p)
        elif kind=='pe':
            p=day(value(r,'Date','date','TradeDate','資料日期','日期'));pe=num(value(r,'PEratio','PERatio','PER','PE','P/E','本益比','本益比(倍)','PriceEarningRatio'))
            if not p or pe is None or pe<=0:continue
            item.update(asof_date=p,pe=pe)
        else:
            p=quarter(f"{value(r,'年度','Year')}Q{str(value(r,'季別','Season','Quarter')).upper().removeprefix('Q')}")
            if p is None:continue
            item['year_quarter']=qstr(p);item['industry_type']='GENERAL'
            report=day(value(r,'出表日期','Date'))
            if report and report>t.date().isoformat():continue
            if kind=='margin':
                aliases=('毛利率(%)','營業利益率(%)','稅後純益率(%)')
                for k,prefix in zip(METRICS[kind],aliases):
                    # H224: TPEX actual response (2026-11-26) uses bare ratio names.
                    # Accept only exact named aliases; reject two aliases in one row.
                    bare=prefix.removesuffix('(%)')
                    labels={prefix,bare} if market=='TPEX' else {prefix}
                    vals=[num(v) for key,v in r.items() if str(key).replace(' ','').replace('（','(').replace('）',')') in labels or (market=='TWSE' and str(key).replace(' ','').startswith(prefix))]
                    item[k]=vals[0] if len(vals)==1 else None
                if any(item[k] is None or not -100<=item[k]<=1000 for k in METRICS[kind]):continue
                item['statement_basis']='SINGLE_QUARTER'
            else:
                # Same general-industry income-statement endpoint, source-disclosed
                # amounts; compare only like fields on that source. No TWD-unit claim.
                fields={'eps_ytd':('基本每股盈餘（元）','基本每股盈餘(元)','基本每股盈餘','BasicEPS'), 'operating_profit_ytd':('營業利益（損失）','營業利益(損失)','營業利益'), 'net_profit_ytd':('本期淨利（淨損）','本期淨利(淨損)','本期淨利','本期稅後淨利')}
                for k,aliases in fields.items():
                    keys=[a for a in aliases if a in r];item[k]=num(r[keys[0]]) if len(keys)==1 else None
                    if k!='eps_ytd':item[k+'_label']=keys[0] if len(keys)==1 else None
                if not report or all(item[k] is None for k in METRICS[kind]):continue
                item.update(basis='CUMULATIVE_YTD_NOT_SINGLE_QUARTER',report_date=report,unit='元／股',profit_basis='SAME_SOURCE_DISCLOSED_UNIT')
        if period_end(kind,p)>t.date() or (kind!='pe' and period_end(kind,p)==t.date()):continue
        out.append(item)
    # Don't resolve contradictory rows from the same response arbitrarily.
    counts=Counter((r['code'],period_key(kind,r)) for r in out)
    return [r for r in out if counts[r['code'],period_key(kind,r)]==1]

def period_key(kind,r):
    return month(r.get('year_month')) if kind=='revenue' else day(r.get('asof_date')) if kind=='pe' else quarter(r.get('year_quarter'))

def read_json(path,default,errors):
    try:
        if path.stat().st_size>50_000_000:raise ValueError('size limit')
        return json.loads(path.read_text('utf-8-sig'))
    except FileNotFoundError:return default
    except (OSError,ValueError,UnicodeError) as e:errors.append(f'{path.name}: {type(e).__name__}');return default

def read_jsonl(path,errors):
    try:
        if path.stat().st_size>50_000_000:raise ValueError('size limit')
        with path.open(encoding='utf-8-sig') as f:return [json.loads(l) for l in f if l.strip()]
    except FileNotFoundError:return []
    except (OSError,ValueError,UnicodeError) as e:errors.append(f'{path.name}: {type(e).__name__}');return []

def fp(kind,r):
    keys=METRICS[kind]+(('operating_profit_ytd_label','net_profit_ytd_label','profit_basis') if kind=='ytd' else ())
    return (r['code'],period_key(kind,r),*(r.get(k) for k in keys))

def feed_path(root,kind,market):return Path(root)/'data/h192'/f'{kind}_{market}.json.gz'

def read_feed(root,kind,market):
    p=feed_path(root,kind,market)
    if not p.exists():return []
    v=archive._read(p)
    if v.get('kind')!=kind or v.get('market')!=market or not isinstance(v.get('rows'),list) or any(not isinstance(r,dict) for r in v['rows']):raise ValueError('invalid feed archive')
    return v['rows']

def collect(root=ROOT,*,get=None,now=None,min_rows=None):
    """Eight bulk public requests; only per-source healthy observations written.
    Fundamentals append new observations. PE retains latest successful daily batch.
    No historic publication timestamps are manufactured; failures retain old bytes.
    """
    if get is None:
        import requests
        get=requests.get
    fixed_now=now is not None
    now=now or datetime.now(TZ)
    if now.tzinfo is None:raise ValueError('timezone required')
    now=now.astimezone(TZ);limits=min_rows or MIN_ROWS; health={}
    for (kind,market),url in URLS.items():
        key=f'{kind}_{market}'
        diagnostic={}
        try:
            old=read_feed(root,kind,market)
            response=get(url,timeout=(4,15),headers={'Accept':'application/json','User-Agent':'StockApp-H192-public-financial/1.0'})
            if response.status_code!=200:raise ValueError(f'HTTP_{response.status_code}')
            observed=now if fixed_now else datetime.now(TZ)
            # Use actual completion timestamp in production (explicit fixed now for fixtures).
            raw=response.json()
            diagnostic={'raw_rows':len(raw) if isinstance(raw,list) else None,'root_type':type(raw).__name__,
                        'fields':list(raw[0])[:40] if isinstance(raw,list) and raw and isinstance(raw[0],dict) else [],
                        'http_status':response.status_code}
            rows=parse(kind,market,raw,observed.isoformat())
            diagnostic['parsed_rows']=len(rows)
            unique=len({r['code'] for r in rows});latest=max((period_key(kind,r) for r in rows),default=None)
            expected=expected_periods(now)[0 if kind=='revenue' else 1] if kind!='pe' else None
            if unique<limits[market]:raise ValueError(f'LOW_COVERAGE_{unique}')
            if kind=='pe':
                # A mixed-date response is not a full same-day market snapshot.
                if len({r['asof_date'] for r in rows})!=1:raise ValueError('MIXED_PE_DATES')
                prev=max((r['asof_date'] for r in old),default='')
                if latest<prev:raise ValueError('PE_DATE_ROLLBACK')
                if prev==latest and {fp(kind,r) for r in old}=={fp(kind,r) for r in rows}:merged=old
                else:merged=rows
            else:
                latest_facts={};merged=old.copy()
                for rec in old:
                    k=(rec['code'],period_key(kind,rec));t=moment(rec.get('available_at'))
                    if t is None:raise ValueError('invalid archive observation')
                    if k not in latest_facts or t>=latest_facts[k][0]:latest_facts[k]=(t,fp(kind,rec))
                for r in rows:
                    f=fp(kind,r);k=(r['code'],period_key(kind,r));t=moment(r['available_at']);prev=latest_facts.get(k)
                    if prev and t<prev[0]:raise ValueError('OBSERVATION_TIME_ROLLBACK')
                    if prev and t==prev[0] and f!=prev[1]:raise ValueError('SAME_TIME_CONFLICT')
                    if not prev or f!=prev[1]:merged.append(r);latest_facts[k]=(t,f)
            if merged!=old:
                body=archive._seal({'version':VERSION,'kind':kind,'market':market,'rows':merged})
                # The reader enforces a 16 MB decompressed bound. Never write unusable data.
                if len(archive._bytes(body))>16_000_000:raise ValueError('ARCHIVE_LIMIT_REQUIRES_MAINTENANCE')
                archive._write(feed_path(root,kind,market),body)
            fresh_count=len({r['code'] for r in rows if (period_key(kind,r)>=expected if expected is not None else period_key(kind,r)==latest)})
            stale=(fresh_count<limits[market] or (now.date()-date.fromisoformat(latest)).days>7) if kind=='pe' else fresh_count<limits[market]
            health[key]={'status':'STALE' if stale else 'OK','rows':len(rows),'fresh_count':fresh_count,'stored':len(merged),'latest':str(latest),'saved':merged!=old,'url':url}
        except Exception as e:health[key]={'status':'ERROR','error':str(e)[:180],'url':url,'retained_previous':True,'diagnostic':diagnostic}
    result={'version':VERSION,'checked_at':now.isoformat(),'sources':health,'status':'ALL_OK' if all(v['status']=='OK' for v in health.values()) else 'PARTIAL_OR_STALE'}
    archive._write(Path(root)/'data/h192/health.json',archive._seal(result));return result

def load_inputs(root):
    root=Path(root);errors=[];data={k:[] for k in METRICS}
    for kind,file in [('revenue','revenue.jsonl'),('margin','quarterly_margins.jsonl')]:data[kind].extend(read_jsonl(root/'data/h115'/file,errors))
    for kind,file,key in [('pe','pe_only_latest.json','pe_only'),('ytd','eps_ytd_latest.json','eps_ytd_rows')]:
        d=read_json(root/'data/h115'/file,{},errors)
        if isinstance(d,dict) and isinstance(d.get(key),list):data[kind].extend(d[key])
    # H171 observations can provide independently recorded YTD EPS; their profit
    # lines have no per-field label/unit contract, so don't use them for breakout.
    d=read_json(root/'data/h115/official_ytd_archive.json',{},errors)
    if isinstance(d,dict) and isinstance(d.get('rows'),list):
        for r in d['rows']:
            if isinstance(r,dict) and r.get('unit_eps')=='TWD_PER_SHARE':data['ytd'].append({**r,'basis':'CUMULATIVE_YTD_NOT_SINGLE_QUARTER','unit':'元／股','profit_basis':None})
    for kind,market in URLS:
        try:data[kind].extend(read_feed(root,kind,market))
        except (OSError,ValueError,KeyError,TypeError) as e:errors.append(f'{kind}_{market}: {type(e).__name__}')
    # H222 independently sealed historical periods, with actual first observation.
    from godpick_h222_financial_history import load_rows
    historical,history_errors=load_rows(root)
    for kind,rows in historical.items():data[kind].extend(rows)
    errors.extend(history_errors)
    return data,errors

def canonical(kind,row,now):
    if not isinstance(row,dict):return None
    market=MARKETS.get(str(row.get('source','')));code=str(row.get('code',''));t=moment(row.get('available_at'));p=period_key(kind,row)
    if not market or not re.fullmatch(r'[1-9]\d{3}',code) or not t or t>now or p is None or p=='':return None
    from godpick_h222_financial_history import historical_row
    history=historical_row(kind,market,p,row)
    if row.get('availability_basis') not in ('FIRST_OBSERVED','PUBLISHED_VERIFIED','PUBLICATION_VERIFIED') or (row.get('source_url')!=URLS[kind,market] and not history):return None
    if row.get('market') and MARKETS.get(str(row['market']))!=market:return None
    if period_end(kind,p)>t.date() or (kind!='pe' and period_end(kind,p)==t.date()):return None
    r={**row,'source':market,'code':code,'_time':t,'_period':p}
    for k in METRICS[kind]:r[k]=num(row.get(k))
    if kind=='revenue':
        if r['revenue_twd'] is None or r['revenue_twd']<0:return None
        for k in METRICS[kind][1:]:
            if r[k] is not None and r[k]<0:r[k]=None
    elif kind=='margin':
        if r.get('statement_basis')!='SINGLE_QUARTER' or r.get('industry_type')!='GENERAL' or any(r[k] is None or not -100<=r[k]<=1000 for k in METRICS[kind]):return None
    elif kind=='ytd':
        if r.get('basis')!='CUMULATIVE_YTD_NOT_SINGLE_QUARTER' or r.get('unit')!='元／股' or (not history and not day(r.get('report_date'))) or day(r.get('report_date'))>t.date().isoformat():return None
        if r.get('profit_basis')!='SAME_SOURCE_DISCLOSED_UNIT':
            for k in METRICS[kind][1:]:r[k]=None
        if all(r[k] is None for k in METRICS[kind]):return None
    elif r['pe'] is None or r['pe']<=0:return None
    return r

def build_ledgers(data,now):
    groups=defaultdict(list);rejected=Counter()
    for kind,rows in data.items():
        if kind not in METRICS:continue
        for row in rows:
            try:r=canonical(kind,row,now)
            except (ValueError,TypeError,OverflowError):r=None
            if r is None:rejected[kind]+=1;continue
            groups[kind,r['source'],r['code'],r['_period']].append(r)
    # H231: choose source family only AFTER filtering actual observation times.
    # A future supplement cannot erase previously visible bulk evidence.
    import godpick_h231_company_history as company
    individual=defaultdict(set); cross_conflicts=set()
    for (kind,market,code,p),rs in groups.items():
        if kind!='ytd':continue
        h=[r for r in rs if r.get('historical_source') and r.get('history_financial_values')]
        if len({r['source_url'] for r in h})>1 and any(r['history_financial_values']!=h[0]['history_financial_values'] for r in h):cross_conflicts.add((market,code,p))
        if any(r.get('historical_source')==company.VERSION for r in h):individual[market,code].add(p)
    for market,code,p in cross_conflicts:individual[market,code].discard(p)
    coherent={k for k,ps in individual.items() if any(set(range(end-7,end+1))<=ps for end in ps)}
    ledgers=defaultdict(dict);conflicts=Counter()
    for (kind,market,code,p),rs in groups.items():
        # Resolve each field only from actual same-period observations. Missing
        # supplementary fields do not erase a prior observation. If a later
        # base revision differs, old comparative fields are not reused.
        # Historical conflicting EPS/profit revisions are not silently differenced.
        keys=METRICS[kind]
        if kind in ('ytd','margin') and ((market,code,p) in cross_conflicts or (kind=='margin' and p%4 and (market,code,p-1) in cross_conflicts)):
            conflicts[kind]+=1;continue
        if kind in ('ytd','margin'):
            company_rows=[r for r in rs if r.get('historical_source')==company.VERSION]
            bulk_rows=[r for r in rs if r.get('historical_source') and r.get('historical_source')!=company.VERSION]
            if company_rows and bulk_rows:
                # Reject inconsistent sources rather than picking by observation time.
                tol=.011 if kind=='margin' else 1e-8
                if any(abs(a[k]-b[k])>tol for a in company_rows for b in bulk_rows for k in keys if a.get(k) is not None and b.get(k) is not None):
                    conflicts[kind]+=1;continue
                preferred=company_rows if (market,code) in coherent else bulk_rows
                rs=[r for r in rs if not r.get('historical_source')]+preferred
        # Backfill cannot silently replace a conflicting previously observed fact.
        hist=[r for r in rs if r.get('historical_source')]; live=[r for r in rs if not r.get('historical_source')]
        if hist and live:
            h=max(hist,key=lambda r:r['_time']); l=max(live,key=lambda r:r['_time'])
            # Ratios may be rounded to two decimals by official current feed.
            tol=.011 if kind=='margin' else 1e-8
            if any(abs(h[k]-l[k])>tol for k in keys if h.get(k) is not None and l.get(k) is not None):
                conflicts[kind]+=1;continue
            # Prefer coherent quarterly history only when observed overlap agrees;
            # do not merge independent source values inside one cumulative statement.
            if kind in ('margin','ytd') and all(h.get(k) is not None for k in keys):rs=hist
            else:rs=live
        latest=max(r['_time'] for r in rs); picked=[r for r in rs if r['_time']==latest]
        primary=keys[0]; candidates={r[primary] for r in picked if r[primary] is not None}
        if len(candidates)>1:conflicts[kind]+=1;continue
        base=dict(max(picked,key=lambda r:sum(r.get(k) is not None for k in keys)))
        bad=False;base['_field_evidence']={}
        for k in keys:
            if kind=='revenue' and k!=primary:
                eligible=[r for r in rs if r[primary]==base[primary] and r[k] is not None]
            else:eligible=[r for r in rs if r[k] is not None]
            if not eligible:base[k]=None;continue
            # For cumulative statements, conflicting restatements are not
            # mixed across quarters, even when each release is official.
            if kind=='ytd' and len({r[k] for r in eligible})>1:
                base[k]=None;conflicts[kind]+=1;continue
            t=max(r['_time'] for r in eligible);latest_rows=[r for r in eligible if r['_time']==t]
            if len({r[k] for r in latest_rows})>1:
                base[k]=None;conflicts[kind]+=1
                if k==primary:bad=True
                continue
            r=latest_rows[0];base[k]=r[k];base['_field_evidence'][k]=r['available_at']
            if kind=='ytd' and k!=primary:
                labels={a.get(k+'_label') for a in eligible}
                if len(labels)!=1 or None in labels:base[k]=None;conflicts[kind]+=1
                else:base[k+'_label']=r.get(k+'_label')
        if not bad and not (kind=='margin' and any(base[k] is None for k in keys)):ledgers[kind,market,code][p]=base
    return ledgers,dict(rejected),dict(conflicts)

def yoy(v,prev):return round((v/prev-1)*100,4) if v is not None and prev is not None and prev>0 else None

def comparable_series(rows):
    # No cross-source cumulative difference or ratio comparison unless the whole
    # requested window comes from one source family (legacy direct fixtures allowed).
    return len({r.get('source_url') for r in rows})<=1

def single_q(l,p,k):
    r=l.get(p,{});v=r.get(k)
    if v is None:return None
    if p%4==0:return v
    prev=l.get(p-1,{});prior=prev.get(k)
    if prior is None or not comparable_series((r,prev)):return None
    if k!='eps_ytd' and (not r.get(k+'_label') or r.get(k+'_label')!=prev.get(k+'_label')):return None
    return v-prior

def same_profit_label(l,periods,k):
    need={p for p in periods}|{p-1 for p in periods if p%4!=0}
    labels={l.get(p,{}).get(k+'_label') for p in need}
    return len(labels)==1 and None not in labels and comparable_series([l.get(p,{}) for p in need])

def three_month_values(rev,r):
    # Use explicitly reported previous-month values only from already-visible
    # actual release records. Never insert synthetic rows into historical ledger.
    seq=[]
    for i in range(r-2,r+1):
        actual=rev.get(i,{}).get('revenue_twd')
        comparison=rev.get(i+1,{}).get('previous_month_twd') if i<r else None
        if actual is not None and comparison is not None and actual!=comparison:seq.append(None)
        else:seq.append(actual if actual is not None else comparison)
    return seq

def financial_profile(rev,mar,ytd,pe,expected_m,expected_q,market_day):
    r=max(rev,default=None); q=max(mar,default=None); y=max(ytd,default=None); d=max(pe,default=None)
    # The current month may already have a release during the standard grace window.
    fresh_r=r is not None and r>=expected_m; fresh_q=q is not None and q>=expected_q; fresh_y=y is not None and y>=expected_q
    R=rev.get(r,{});M=mar.get(q,{});Y=ytd.get(y,{});P=pe.get(d,{})
    cv=R.get('revenue_twd');prev=rev.get(r-1,{}).get('revenue_twd') if r is not None else None;ly=rev.get(r-12,{}).get('revenue_twd') if r is not None else None
    # Current official release comparatives are valid as of that release, not
    # earlier historic observations. Use them for YoY/MoM, not record highs.
    py=R.get('prior_year_month_twd'); pm=R.get('previous_month_twd')
    ym=yoy(cv,py if py is not None else ly);mm=yoy(cv,pm if pm is not None else prev)
    growth=None;high=None;three=None;profit=None;ttm=None;eps_yoy=None;high_edge=None;three_edge=None;profit_edge=None
    if fresh_r:
        seq=three_month_values(rev,r)
        if all(v is not None for v in seq) and ym is not None:growth=seq[0]<seq[1]<seq[2] and ym>=10
        history=[rev.get(i,{}).get('revenue_twd') for i in range(r-12,r+1)]
        if all(v is not None for v in history):
            high=history[-1]>max(history[:-1]);high_edge=yoy(history[-1],max(history[:-1]))
    margins=[M.get(k) for k in METRICS['margin']]
    if fresh_q and all(i in mar for i in (q,q-1,q-4,q-5)) and comparable_series([mar[i] for i in (q,q-1,q-4,q-5)]):
        three=all(mar[i][k]>mar[i-4][k] for i in (q,q-1) for k in METRICS['margin']) and margins[1]>0 and margins[2]>0
        three_edge=sum(mar[i][k]-mar[i-4][k] for i in (q,q-1) for k in METRICS['margin'])
    if fresh_y:
        a=[single_q(ytd,i,'eps_ytd') for i in range(y-3,y+1)]
        if all(v is not None for v in a) and comparable_series([ytd[i] for i in range(y-3,y+1)]):ttm=round(sum(a),4)
        eps_yoy=yoy(Y.get('eps_ytd'),ytd.get(y-4,{}).get('eps_ytd'))
        periods=list(range(y-4,y+1));ops=[single_q(ytd,i,'operating_profit_ytd') for i in periods];nets=[single_q(ytd,i,'net_profit_ytd') for i in periods]
        if all(v is not None for v in ops+nets) and all(same_profit_label(ytd,periods,k) for k in ('operating_profit_ytd','net_profit_ytd')):
            profit=max(ops[:-1])>0 and ops[-1]>max(0,*ops[:-1]) and nets[-1]>max(0,nets[0])
            profit_edge=yoy(ops[-1],max(ops[:-1]))
    positive=bool(fresh_q and margins[1] is not None and margins[1]>0 and margins[2]>0)
    from godpick_h232_eps_comparability import audit as eps_audit
    return {**eps_audit(ytd,y,ttm),'營收年月':monthstr(r) if r is not None else '', '營收新鮮':fresh_r,'月營收元':cv,'比較上月營收元':pm if pm is not None else prev,'營收年增%':ym,'營收月增%':mm,'營收比較口徑':'本次公告比較欄優先，否則歷史實際觀測','營收來源':R.get('source_url',''),'營收可得時間':R.get('available_at',''), '營收歷史月數':len(rev),'連3月比較口徑':'同時點實際月報及明示上月比較值；衝突待核','連3月成長':growth,'近13月新高':high,'營收超前12月高點%':high_edge,'三率季度':qstr(q) if q is not None else '', '三率新鮮':fresh_q,'毛利率%':margins[0],'營益率%':margins[1],'淨利率%':margins[2],'三率來源':M.get('source_url',''),'三率可得時間':M.get('available_at',''),'三率季度數':len(mar),'連兩季三率年增':three,'兩季三率改善合計pp':three_edge,'EPS季度':qstr(y) if y is not None else '', 'EPS新鮮':fresh_y and Y.get('eps_ytd') is not None,'累計EPS':Y.get('eps_ytd'),'累計EPS年增%':eps_yoy,'TTM EPS重建':ttm,'TTM狀態':'累計差分重建／未獨立核實股本變動' if ttm is not None else '缺同口徑歷史累計EPS','EPS來源':Y.get('source_url',''),'EPS可得時間':Y.get('available_at',''),'YTD季度數':len(ytd),'獲利突破':profit,'營業利益超前4季高點%':profit_edge,'PE日期':d or '', '官方PE':P.get('pe'),'PE同交易日':bool(market_day and d==market_day),'PE來源':P.get('source_url',''),'PE可得時間':P.get('available_at',''),'獲利率為正':positive,'現金流負債':'未取得獨立財報證據','交易狀態':'僅研究／未授權'}

def evaluate(*,master,data,now,market_day='',opportunities=None,top=30,errors=()):
    from godpick_h191_opportunity_engine import master_map
    if now.tzinfo is None:raise ValueError('timezone required')
    now=now.astimezone(TZ);expected_m,expected_q=expected_periods(now);m=master_map(master)
    ledger,rejected,conflicts=build_ledgers(data,now); profiles=[];pools={p:[] for p in POOLS}; peers=defaultdict(list)
    for c,identity in m.items():
        market=identity['市場'];r=financial_profile(*(ledger[k,market,c] for k in ('revenue','margin','ytd','pe')),expected_m,expected_q,market_day)
        r.update(identity);sector=r['產業'];r['同業可比基礎']=bool(sector and not sector.isdigit() and sector not in ('其他','未分類','待分類','櫃其他','櫃其他電子') and not any(x in sector for x in ('金融','銀行','保險','證券')) and r['三率新鮮'] and r['EPS新鮮'] and r['三率季度']==r['EPS季度'] and r['累計EPS'] is not None and r['累計EPS']>0 and r['PE同交易日'])
        if r['同業可比基礎']:peers[market,sector,r['EPS季度']].append((c,r['官方PE']))
        profiles.append(r)
    op=defaultdict(list)
    if isinstance(opportunities,dict) and opportunities.get('calendar_fresh') and opportunities.get('asof')==market_day and market_day:
        for name,rows in opportunities.get('pools',{}).items():
            for item in rows:
                if item.get('資料日期')==market_day:op[item.get('股票代號')].append(name)
    for r in profiles:
        c=r['股票代號'];others=[v for k,v in peers[r['市場'],r['產業'],r['EPS季度']] if k!=c] if r['同業可比基礎'] else []
        med=statistics.median(others) if len(others)>=5 else None; ratio=r['官方PE']/med if med and r['官方PE'] is not None else None
        r.update({'同市場同產業其他同業數':len(others),'同業PE中位數':med,'PE相對同業比':round(ratio,4) if ratio is not None else None,'H191研究池':'、'.join(op[c])})
        selected=[]
        def add(p,s,why):
            selected.append(p);pools[p].append({**r,'研究分':round(float(s),4),'研究理由':why})
        if r['連3月成長'] is True:add(POOLS[0],r['營收年增%'],'連續3個實際月營收成長，當期年增至少10%')
        if r['近13月新高'] is True and r['營收超前12月高點%'] is not None:add(POOLS[1],r['營收超前12月高點%'],'當月實際營收嚴格高於前12個月；不是歷史最高或未來預測')
        if r['連兩季三率年增'] is True:add(POOLS[2],r['兩季三率改善合計pp'],'連兩季各自較去年同季三率提升，最新營益及淨利率為正')
        if r['獲利突破'] is True and r['營業利益超前4季高點%'] is not None:add(POOLS[3],r['營業利益超前4季高點%'],'單季營業利益創近5季高，單季淨利為正且較去年同期增加；同來源累計差分')
        t=r['TTM EPS重建'];pe=r['官方PE']
        if t is not None and t>=5 and pe is not None and pe<=15 and ratio is not None and ratio<=.8 and r['獲利率為正'] and r['三率季度']==r['EPS季度'] and r['累計EPS年增%'] is not None and r['累計EPS年增%']>0:
            add(POOLS[4],(1-ratio)*100,'官方PE≤15且≤同業中位數80%，重建TTM EPS≥5元、累計EPS年增與獲利率為正；非內在價值')
        if selected and op[c]:add(POOLS[5],len(selected)*10+len(op[c]),'H191當日已顯示研究池與H192財報條件交集；非全母體交集')
        if r['營收新鮮'] and r['營收年增%'] is not None and r['營收年增%']>=10 and r['營收月增%'] is not None and r['營收月增%']>0:
            add(POOLS[6],r['營收年增%'],'當期月營收年增至少10%且月增為正；僅單月觀察，不代表連續成長或預測')
        elif r['營收新鮮'] and r['營收年增%'] is None and r['營收月增%'] is not None and r['營收月增%']>=10:
            add(POOLS[7],r['營收月增%'],'兩個可核對月份的月營收成長至少10%，但缺同期年增；可能有季節性或低基期，僅補證線索')
        r['研究池']='、'.join(selected);r['缺口']='、'.join(k for k,v in [('月營收過期或缺失',not r['營收新鮮']),('連3月營收或年增缺證',r['連3月成長'] is None),('13月營收缺證',r['近13月新高'] is None),('三率同比缺證',r['連兩季三率年增'] is None),('獲利突破歷史缺證',r['獲利突破'] is None),('TTM缺證',t is None),('PE日期未確認',not r['PE同交易日']),('可比同業不足',med is None)] if v)
    counts={p:len(v) for p,v in pools.items()}
    for p,rs in pools.items():
        rs.sort(key=lambda r:(-r['研究分'],r['股票代號']));pools[p]=[{**r,'研究排名':i} for i,r in enumerate(rs[:top],1)]
    coverage={market:{'母體':len(g),'當期月營收':sum(r['營收新鮮'] for r in g),'當期三率':sum(r['三率新鮮'] for r in g),'當期EPS':sum(r['EPS新鮮'] for r in g),'同日PE':sum(r['PE同交易日'] for r in g),'TTM重建':sum(r['TTM EPS重建'] is not None for r in g)} for market in ('TWSE','TPEX') for g in [[r for r in profiles if r['市場']==market]]}
    evidence=[]
    for (kind,market,code),periods in ledger.items():
        if code not in m or m[code]['市場']!=market:continue
        for p,r in sorted(periods.items()):
            for k,t in r.get('_field_evidence',{}).items():
                if r.get(k) is not None:evidence.append({'股票代號':code,'市場':market,'種類':kind,'期間':monthstr(p) if kind=='revenue' else p if kind=='pe' else qstr(p),'欄位':k,'數值':r[k],'首次觀察或公告時間':t,'來源':r['source_url'],'科目':r.get(k+'_label','')})
    return {'evidence':evidence,'version':VERSION,'asof':now.isoformat(),'expected_month':monthstr(expected_m),'expected_quarter':qstr(expected_q),'market_day':market_day,'pools':pools,'counts':counts,'coverage':coverage,'rejected':rejected,'conflicts':conflicts,'errors':list(errors),'profiles':profiles,'trade_authority_changed':False,'status':'RESEARCH_AVAILABLE' if any(counts.values()) else 'NO_MATCH_OR_EVIDENCE_PENDING'}

def run(root=ROOT,now=None,top=30,include_h191=True):
    root=Path(root);now=now or datetime.now(TZ);data,errors=load_inputs(root)
    market_day='';opp=None
    # H191 calendar validity is separate from 60-session coverage.
    try:
        from godpick_h191_opportunity_engine import run as run191
        if include_h191:
            opp=run191(root,now=now,top=50)
            if opp.get('calendar_fresh'):market_day=opp['asof']
    except Exception as e:errors.append('H191: '+type(e).__name__)
    master=read_json(root/'stock_master_cache.json',[],errors)
    result=evaluate(master=master,data=data,now=now,market_day=market_day,opportunities=opp,top=top,errors=errors)
    try:result['health']=archive._read(root/'data/h192/health.json')
    except (OSError,ValueError,KeyError,TypeError):result['health']={'status':'NOT_COLLECTED_OR_INVALID','sources':{}}
    return result

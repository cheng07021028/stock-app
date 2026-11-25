# -*- coding: utf-8 -*-
"""H191 research-only rankings, local official archive; no order or authority writes.
Raw price returns, not adjusted performance. No missing-value imputation.
"""
from datetime import date, datetime, timedelta, time
from pathlib import Path
from collections import defaultdict, Counter
import json, math, re, gzip
import numpy as np
import pandas as pd
import godpick_h190_market_archive as archive
import godpick_h198_stage_review as stage198

VERSION='V191-H191-MAINSTREAM-BLACKHORSE'
ROOT=Path(__file__).resolve().parent
POOLS=('主流強勢','低位階蓄勢','技術突破','大盤抗跌','大戶持股增加')
MARKETS={'上市':'TWSE','上櫃':'TPEX','TWSE':'TWSE','TPEX':'TPEX','TPEx':'TPEX'}

def read_json(p,default):
    try: return json.loads(Path(p).read_text('utf-8'))
    except (OSError,ValueError): return default

def num(v):
    try:
        x=float(v); return x if math.isfinite(x) else None
    except (TypeError,ValueError,OverflowError): return None

def day(v):
    try: return archive._day(v)
    except (ValueError,TypeError): return ''

def master_map(items):
    out={}; duplicates=set()
    for r in items if isinstance(items,list) else []:
        if not isinstance(r,dict): continue
        c=str(r.get('code','')); m=MARKETS.get(str(r.get('market','')))
        if not re.fullmatch('[1-9][0-9]{3}',c) or not m: continue
        if c in out: duplicates.add(c); continue
        out[c]={'股票代號':c,'股票名稱':str(r.get('name') or c),'市場':m,'產業':str(r.get('official_industry') or '').strip()}
    return {c:r for c,r in out.items() if c not in duplicates}

def calc_features(seq,expected):
    """Exact 60 sessions; avoid sorting away duplicates, reject suspicious raw gaps."""
    if len(expected)!=60: return None,'不足60個交易日'
    s=sorted((r for r in seq if expected[0]<=r[0]<=expected[-1]),key=lambda r:r[0])
    if [r[0] for r in s]!=expected: return None,'個股歷史缺日或重複'
    try: a=np.array([r[1:7] for r in s],dtype=float)
    except (ValueError,TypeError): return None,'量價格式錯誤'
    if a.shape!=(60,6) or not np.isfinite(a).all() or (a<=0).any(): return None,'量價缺值或非正值'
    o,h,l,c,v,amt=a.T
    if ((h<np.maximum.reduce([o,l,c]))|(l>np.minimum.reduce([o,h,c]))|(amt/v<l*.5)|(amt/v>h*2)|(v%1!=0)|(amt%1!=0)).any(): return None,'OHLCV或金額單位異常'
    # Ex-date changes can be smaller than 12%; this detects only large gaps, not corporate actions.
    if (np.abs(o[1:]/c[:-1]-1)>.12).any() or (np.abs(c[1:]/c[:-1]-1)>.15).any(): return None,'原始價格異常跳空待查'
    r=lambda n: float((c[-1]/c[-1-n]-1)*100)
    ma5=float(c[-5:].mean());ma20=float(c[-20:].mean());ma60=float(c.mean())
    prior_high=float(h[-21:-1].max()); prior_low=float(l[-21:-1].min()); high60=float(h.max()); low60=float(l.min())
    return {'收盤':float(c[-1]),'日漲跌%':r(1),'5日漲跌%':r(5),'20日漲跌%':r(20),
        '當日成交額':float(amt[-1]),'20日均成交額':float(amt[-20:].mean()),'前20日均成交額':float(amt[-21:-1].mean()),
        '成交額比':float(amt[-5:].mean()/amt[-25:-5].mean()),'量比':float(v[-1]/v[-21:-1].mean()),
        'MA5':ma5,'MA20':ma20,'MA60':ma60,'MA20前5日':float(c[-25:-5].mean()),
        '前20日箱頂':prior_high,'前20日箱底':prior_low,'箱體振幅%':(prior_high/prior_low-1)*100,
        '距箱頂%':float((c[-1]/prior_high-1)*100),'60日區間位置%':float((c[-1]-low60)/(high60-low60)*100) if high60>low60 else None,
        '均線收斂%':abs(ma5/ma20-1)*100,'突破':bool(c[-1]>prior_high),'黃金交叉':bool(ma5>ma20 and c[-6:-1].mean()<=c[-21:-1].mean()),
        '日報酬序列':np.diff(c)/c[:-1]*100},''

def _factor_map(payload):
    rows=payload.get('records',[]) if isinstance(payload,dict) else []
    counts=Counter(str(r.get('股票代號','')) for r in rows if isinstance(r,dict))
    return {str(r['股票代號']):r for r in rows if isinstance(r,dict) and counts[str(r.get('股票代號',''))]==1 and '股票代號' in r}

def institutional(r,asof):
    if day(r.get('法人資料日期'))!=asof: return None,None
    src=str(r.get('法人資料源','')).upper()
    if src not in ('TWSE_T86+DAILY_HISTORY','TPEX_OPENAPI_3INSTI+DAILY_HISTORY'): return None,None
    x=num(r.get('三大法人近5日合計')); n=num(r.get('法人連買天數'))
    return x,n if n is not None and n>=0 and n.is_integer() else None

def tdcc_delta(code,latest,prior,asof):
    if not all(isinstance(x,dict) and x.get('source')=='TDCC_OPEN_DATA' and x.get('source_url')=='https://opendata.tdcc.com.tw/getOD.ashx?id=1-5' for x in (latest,prior)): return None,''
    d1,d0=day(latest.get('data_date')),day(prior.get('data_date'))
    if not d0 or not d1 or not d0<d1<=asof: return None,''
    if (date.fromisoformat(asof)-date.fromisoformat(d1)).days>10 or not 5<=(date.fromisoformat(d1)-date.fromisoformat(d0)).days<=10: return None,''
    vals=[]
    for p,d in ((latest,d1),(prior,d0)):
        r=p.get('rows',{}).get(code,{})
        if str(r.get('證券代號'))!=code or str(r.get('持股分級'))!='15' or day(r.get('資料日期'))!=d: return None,''
        x=num(r.get('占集保庫存數比例%'))
        if x is None or not 0<=x<=100: return None,''
        vals.append(x)
    return round(vals[0]-vals[1],4),f'{d0} → {d1}'

def benchmark(root,expected):
    """H117 verified TAIEX price index only, never use an ETF or a market average."""
    from godpick_h117_official_market_closure import verify_manifest,MARKET_URLS
    values=[]
    for d in expected[-21:]:
        try:
            with gzip.open(Path(root)/'data/h117/daily/TAIEX'/f'{d}.json.gz','rt',encoding='utf-8') as f: p=json.load(f)
            if not verify_manifest(p) or p['source']!='TAIEX' or p['source_url']!=MARKET_URLS['TAIEX'] or p['date']!=d or p['row_count']!=1 or len(p['rows'])!=1: return None
            r=p['rows'][0]; x=num(r.get('close'))
            if r.get('date')!=d or r.get('index_name')!='TAIEX_PRICE_INDEX' or x is None or x<=0:return None
            values.append(x)
        except (OSError,ValueError,KeyError,TypeError):return None
    return np.diff(values)/np.array(values[:-1])*100

def evaluate(*, master, calendars, markets, rows, asof, factors=None, latest_tdcc=None, prior_tdcc=None, index_returns=None, min_amount=20_000_000, errors=(), top=20, latest_ready=True):
    """Market-level comparisons require >=80% coverage per market, industry >=70%.
    Individual setups may be shown with explicit partial-universe label; no trade authority.
    """
    if not 1<=top<=100 or not math.isfinite(min_amount) or min_amount<0: raise ValueError('invalid settings')
    universe=master_map(master); denominators=Counter(r['市場'] for r in universe.values()); industry_den=Counter(r['產業'] for r in universe.values() if r['產業'])
    sessions=sorted(set(d for ds in calendars.values() for d in ds if d<=asof)); expected=sessions[-60:]
    out={'version':VERSION,'asof':asof,'status':'DATA_PENDING','trade_authority_changed':False,'universe':len(universe),'coverage':{},'reasons':{},'sectors':[],'pools':{p:[] for p in POOLS},'diagnostic':[],'archive_errors':list(errors),'qualified':0,'scope':'研究觀察，非正式推薦','index_available':False,'h198':stage198.summarize([],top,len(universe))}
    if len(expected)!=60 or expected[-1]!=asof or not latest_ready or errors:
        out['reasons']={'交易日不足、日期未核實或封存錯誤':len(universe)};out['diagnostic']=[{**r,'資料日期':asof,'缺口':'交易日不足、日期未核實或封存錯誤'} for r in universe.values()];return out
    # Require every calendar month across the window, not just 60 rows from a gapped calendar.
    if any(m not in calendars for m in archive._months(date.fromisoformat(expected[0]),date.fromisoformat(asof))):
        out['reasons']={'交易月缺口':len(universe)};out['diagnostic']=[{**r,'資料日期':asof,'缺口':'交易月缺口'} for r in universe.values()];return out
    features=[]; gaps=Counter(); fm=_factor_map(factors)
    for code,meta in universe.items():
        market=meta['市場']
        if any(d not in markets.get(market,{}) for d in expected):
            gaps['市場日封存不足']+=1;out['diagnostic'].append({**meta,'資料日期':asof,'缺口':'市場日封存不足'});continue
        f,why=calc_features(rows.get((market,code),[]),expected)
        if f is None:
            gaps[why]+=1;out['diagnostic'].append({**meta,'資料日期':asof,'缺口':why});continue
        f.update(meta); f['資料日期']=asof;f['資料源']='H190官方未還原OHLCV';f['法人5日買超股數'],f['法人連買天數']=institutional(fm.get(code,{}),asof)
        f['法人資料日期']=asof if f['法人5日買超股數'] is not None else ''; f['法人資料源']=str(fm.get(code,{}).get('法人資料源','')) if f['法人資料日期'] else ''
        f['TDCC持股變化pp'],f['TDCC比較期間']=tdcc_delta(code,latest_tdcc,prior_tdcc,asof)
        features.append(f)
    valid=Counter(f['市場'] for f in features); out['coverage']={m:{'valid':valid[m],'total':denominators[m],'ratio':valid[m]/denominators[m] if denominators[m] else 0} for m in ('TWSE','TPEX')};out['reasons']=dict(gaps);out['qualified']=len(features)
    full=all(denominators[m]>=100 and out['coverage'][m]['ratio']>=.8 for m in ('TWSE','TPEX'))
    out['status']='CROSS_MARKET_RESEARCH_READY' if full else 'PARTIAL_RESEARCH' if features else 'DATA_PENDING'
    groups=defaultdict(list)
    for f in features:
        if f['產業'] and f['產業'] not in ('其他','未分類','待分類'):groups[f['產業']].append(f)
    market_r5=float(np.median([f['5日漲跌%'] for f in features])) if features else 0
    # Aggregate actual traded value across the SAME constituents; turnover is activity, not inflow.
    for name,g in groups.items():
        n=len(g); coverage=n/industry_den[name]
        breadth=sum(f['5日漲跌%']>0 for f in g)/n
        r5=float(np.median([f['5日漲跌%'] for f in g]));activity=sum(f['當日成交額'] for f in g)/sum(f['前20日均成交額'] for f in g)
        qualified=full and n>=5 and coverage>=.7 and r5>0 and r5>market_r5 and breadth>=.6 and activity>=1.1
        out['sectors'].append({'產業':name,'有效檔數':n,'主檔檔數':industry_den[name],'覆蓋率%':coverage*100,'5日中位漲跌%':r5,'上漲家數比%':breadth*100,'成交額活躍比':activity,'主流成立':qualified,'相對母體中位數pp':r5-market_r5})
    out['sectors'].sort(key=lambda x:(not x['主流成立'],-x['相對母體中位數pp'],-x['成交額活躍比'],x['產業']))
    leaders={r['產業'] for r in out['sectors'] if r['主流成立']}
    h211_sectors={r['產業']:r for r in out['sectors']}
    idx=np.asarray(index_returns,dtype=float) if index_returns is not None else np.array([])
    idx_ok=idx.shape==(20,) and np.isfinite(idx).all() and (idx>-100).all(); out['index_available']=bool(idx_ok)
    down=idx<0 if idx_ok else np.zeros(20,dtype=bool)
    stage_rows=[]
    for f in features:
        liquid=f['當日成交額']>=min_amount and f['20日均成交額']>=min_amount
        c=f['收盤']; r5=f['5日漲跌%']; r20=f['20日漲跌%']; pos=f['60日區間位置%']; vol=f['量比']; signal=[]
        def add(pool,score,why):
            item={k:v for k,v in f.items() if k!='日報酬序列'}; item.update({'研究分':round(float(score),2),'研究理由':why,'研究池':pool,'資料範圍':out['status'],'交易狀態':'僅研究／未授權'});out['pools'][pool].append(item);signal.append(pool)
        if liquid and f['產業'] in leaders and r5>market_r5 and r5>0 and c>f['MA20'] and vol>=1:
            add('主流強勢',r5-market_r5+min(vol,4)*2,'產業廣度、成交額活躍、個股相對強度成立；成交額不是淨流入')
        if liquid and pos is not None and 20<=pos<=75 and -6<=f['距箱頂%']<=0 and f['箱體振幅%']<=18 and f['均線收斂%']<=3 and f['MA20']>=f['MA20前5日'] and -8<=r20<=12 and .5<=vol<=1.5:
            add('低位階蓄勢',30-f['箱體振幅%']-f['均線收斂%']*2+f['距箱頂%'],'未突破箱頂、低至中位階、均線收斂；需後續確認，不預測必漲')
        if liquid and f['突破'] and vol>=1.5 and c>f['MA20'] and r5<=15 and c/f['MA20']<=1.15:
            add('技術突破',min(vol,4)*5+min(r5,15),'突破前20日高點且放量；排除過度乖離')
        if idx_ok and down.sum()>=3 and liquid:
            excess=f['日報酬序列'][-20:][down]-idx[down]; absolute=f['日報酬序列'][-20:][down]
            win=float((excess>0).mean()); avg=float(excess.mean()); abswin=float((absolute>=0).mean())
            if avg>=.5 and win>=.65:
                add('大盤抗跌',avg*10+win*10,f'加權指數下跌{int(down.sum())}日，超額為正比例{win:.0%}，個股不跌比例{abswin:.0%}；非保證不跌')
        delta=f['TDCC持股變化pp']; net=f['法人5日買超股數']; streak=f['法人連買天數']
        if liquid and delta is not None and delta>=.3 and net is not None and net>0 and streak is not None and streak>=2:
            add('大戶持股增加',delta*10+min(streak,5),'TDCC千張以上兩期持股比例增加，法人5日淨買超且連買；不等同主力鎖碼')
        h198=stage198.assess(rows.get((f['市場'],f['股票代號']),[]),expected,f,liquid=liquid)
        stage_rows.append({**{k:f[k] for k in ('股票代號','股票名稱','市場','產業','收盤','量比','5日漲跌%','20日漲跌%','當日成交額','20日均成交額')},**h198})
        sector=h211_sectors.get(f['產業'],{})
        h211={k:f.get(k) for k in ('收盤','MA20','MA20前5日','5日漲跌%','20日漲跌%','60日區間位置%','量比')}
        h211.update({'全市場技術覆蓋':full,'流動性通過':liquid,**{'產業'+k:sector.get(k) for k in ('覆蓋率%','有效檔數','成交額活躍比','上漲家數比%','主流成立')}})
        out['diagnostic'].append({**h198,'H211技術證據':h211,'股票代號':f['股票代號'],'股票名稱':f['股票名稱'],'市場':f['市場'],'產業':f['產業'],'流動性通過':liquid,'研究池':'、'.join(signal),'資料日期':asof,'法人資料可用':net is not None,'TDCC資料可用':delta is not None})
    out['h198']=stage198.summarize(stage_rows,top,len(universe))
    out['pool_counts']={p:len(g) for p,g in out['pools'].items()}
    for p,g in out['pools'].items():
        g.sort(key=lambda r:(-r['研究分'],r['股票代號']));out['pools'][p]=[{**r,'研究排名':i} for i,r in enumerate(g[:top],1)]
    return out

def run(root=ROOT,now=None,top=20,min_amount=20_000_000):
    root=Path(root); now=(now or datetime.now(archive.TZ)).astimezone(archive.TZ); ar=root/'data/h190'
    cal,markets,rows,errors=archive._index(ar)
    cutoff=(now.date() if now.time()>=time(18,15) else now.date()-timedelta(days=1)).isoformat()
    sessions=sorted(d for ds in cal.values() for d in ds if d<=cutoff);asof=sessions[-1] if sessions else cutoff
    # Calendar must have been checked after the target cutoff (even on a holiday), not just an old archive date.
    ready=False
    try:
        m=archive._read(ar/'calendar'/(cutoff[:7]+'.json'));obs=datetime.fromisoformat(m['observed_at']).astimezone(archive.TZ)
        ready=obs<=now and obs.date()>=date.fromisoformat(cutoff) and (obs.date()>date.fromisoformat(cutoff) or obs.time()>=time(18,15)) and (date.fromisoformat(cutoff)-date.fromisoformat(asof)).days<=7
    except (OSError,ValueError,KeyError,TypeError): pass
    # Unchanged holiday calendar files may retain earlier observed_at. A sealed,
    # successful current-month calendar refresh in H190 health can attest the check.
    if not ready and sessions and cutoff[:7] in cal:
        try:
            health=archive._read(ar/'health.json'); obs=datetime.fromisoformat(health['observed_at']).astimezone(archive.TZ)
            ready=(health.get('version')==archive.VERSION and cutoff[:7] not in health.get('calendar_missing',[]) and 'calendar:'+cutoff[:7] not in health.get('errors',{}) and health.get('latest_calendar_session')==asof and obs<=now and obs.date()>=date.fromisoformat(cutoff) and (obs.date()>date.fromisoformat(cutoff) or obs.time()>=time(18,15)) and (date.fromisoformat(cutoff)-date.fromisoformat(asof)).days<=7)
        except (OSError,ValueError,KeyError,TypeError): pass
    # Without an authoritative holiday roster, do not certify a weekday gap.
    # Weekend research may use the last observed trading session, never relabel it.
    if date.fromisoformat(cutoff).weekday()<5 and asof!=cutoff: ready=False
    durable=read_json(root/'godpick_tdcc_holder_snapshots.json',{})
    if not isinstance(durable,dict): durable={}
    tc=root/'data/cache/tdcc_holder_truth'; latest=read_json(tc/'latest.json',durable.get('latest',{}));prior=read_json(tc/'previous.json',durable.get('previous',{}))
    idx=benchmark(root,sessions[-60:]) if len(sessions)>=60 else None
    result=evaluate(master=read_json(root/'stock_master_cache.json',[]),calendars=cal,markets=markets,rows=rows,asof=asof,factors=read_json(root/'official_factors_cache.json',{}),latest_tdcc=latest,prior_tdcc=prior,index_returns=idx,errors=errors,latest_ready=ready,top=top,min_amount=min_amount)
    result.update({'cutoff':cutoff,'calendar_fresh':ready,'paired_sessions':sum(d in markets['TWSE'] and d in markets['TPEX'] for d in sessions[-60:]),'corporate_actions':'未還原；尚無完整除權息驗證；非回測績效'})
    return result

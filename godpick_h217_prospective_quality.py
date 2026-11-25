# -*- coding: utf-8 -*-
"""Prospective research audit only. No H126, weights or backdated signal writes.
Daily leader evidence uses the then-current master, never today's master for old days.
H194 and H209 memberships are compared separately; H209 cash model is hypothetical.
"""
from collections import Counter, defaultdict
from datetime import datetime, time
from pathlib import Path
import math, re, statistics, io, json
import godpick_h190_market_archive as ar
import godpick_h194_performance as perf
import godpick_h209_rolling_backtest as back
import godpick_h206_market_gap as gap

VERSION='V191-H217-PROSPECTIVE-QUALITY'
ROOT=Path(__file__).resolve().parent
REL=Path('data/h217/daily')
HORIZONS=perf.HORIZONS
SOURCES=('H194','H209')
LOOKBACK=10
MIN_PER_GROUP=3
BASIS='原始收盤強勢榜（非含息）；當日主檔，非歷史成分；H194前三研究分級及H209獨立召回分開；H209次日開盤模擬含成本淨報酬；非成交或正式推薦績效。'


def key(r):return r['market'],r['code']
def valid_key(m,c):return m in ar.URLS and isinstance(c,str) and re.fullmatch('[1-9][0-9]{3}',c) is not None
def mean(xs):return statistics.mean(xs) if xs else None

def prior_audit(root,day,now):
    """Read new optional H194 audit only from the same immutable decision file."""
    p=Path(root)/'data/h194/signals'/(day+'.json.gz')
    if not p.exists():return {},'NO_PARTITION'
    b=ar._read(p);obs=perf.stamp(b['created_at'])
    if obs>now or obs.date().isoformat()!=day or obs.time()<time(18,15) or b['version']!=perf.VERSION or b['day']!=day:return {},'INVALID_OR_FUTURE'
    a=b.get('h217_audit')
    if not isinstance(a,dict):return {},'LEGACY_NO_AUDIT'
    if a.get('decision_day')!=day or a.get('market_day')!=day or a.get('same_session') is not True:return {},'SESSION_NOT_VERIFIED'
    rows=a['rows'];out={}
    for r in rows:
        k=key(r)
        if not valid_key(*k) or k in out:raise ValueError('H217 invalid audit identity')
        out[k]=r
    return out,'FROZEN_DECISION_AUDIT'


def source_index(signals):
    ix=defaultdict(dict)
    for s in signals:
        k=(s['source'],s['signal_day'],s['market']);c=s['code']
        if s['source'] not in SOURCES or not valid_key(s['market'],c) or c in ix[k]:raise ValueError('duplicate or invalid source membership')
        ix[k][c]=s
    return ix


def compare_leaders(b,ix,audit,audit_state,now):
    """Report membership absence, not a causal or formal buy miss."""
    day,prev=b['day'],b['previous_day'];details=[];summary=[]
    if not day or not prev:return summary,details
    for m in ar.URLS:
        cov=b['coverage'].get(m,{})
        denom=cov.get('主檔一般股',0);num=cov.get('雙日有價',0)
        ratio=num/denom if denom and 0<=num<=denom else None
        complete=ratio is not None and ratio>=.95 and not b['errors']
        leaders=[r for r in b['rows'] if r['市場']==m]
        for src in SOURCES:
            prior=ix.get((src,prev,m),{});hits=0;misses=0;unknown=0
            for r in leaders:
                c=r['股票代號'];s=prior.get(c);a=audit.get((m,c),{}) if src=='H194' else {}
                if s is not None:
                    if perf.stamp(s['created_at']).date().isoformat()!=prev or perf.stamp(s['created_at'])>=now or not math.isclose(s['base_close'],r['前收盤'],rel_tol=1e-9):status='基準或時點衝突';why='原始決策基準待核';unknown+=1
                    else:status='已入前日研究';why='前瞻封存命中';hits+=1
                elif not prior:
                    status='無前日具名封存';why='無法判定漏選';unknown+=1
                else:
                    status='未入前日研究封存';misses+=1
                    if not a:why='缺當時逐股原因／待核'
                    elif a.get('tier') not in ('','未入池'):why='當時入池但未封存／核對封存缺口'
                    elif a.get('tech_gap'):why='當時技術缺口：'+a['tech_gap']
                    elif a.get('liquid') is False:why='當時流動性未通過'
                    elif a.get('financial_gap'):why='當時財報缺口：'+a['financial_gap']+'；未入研究池'
                    else:why='當時未入研究池／具體規則待核'
                details.append({**r,'訊號來源':src,'前日封存狀態':status,'當時原因證據':why,'原因來源':audit_state if a else '無逐股證據','前日研究分級':s.get('tier','') if s else '', '正式買進漏選':'未驗證H126'})
            n=len(leaders);valid=bool(complete and prior and n and unknown==0)
            summary.append({'交易日':day,'前交易日':prev,'市場':m,'訊號來源':src,'雙日覆蓋率':ratio,'強勢榜樣本':n,'前日研究股數':len(prior),'命中':hits,'未入榜':misses,'待核':unknown,'研究召回率':hits/n if valid else None,'狀態':'VALID_RESEARCH_RECALL' if valid else 'DATA_PENDING'})
    return summary,details


def freshness(s,ix,market):
    day=s['signal_day'];prior=[d for d in market.days if d<day][-LOOKBACK:]
    # Calendar completeness across the entire lookback; missing source cohorts stay unknown.
    if not prior:return '歷史待補'
    seen=[d for d in prior if s['code'] in ix.get((s['source'],d,s['market']),{})]
    if seen:return '近窗曾入研究'
    if len(prior)==LOOKBACK and market.span(prior[0],LOOKBACK)==prior+[day] and all(ix.get((s['source'],d,s['market'])) for d in prior):return '近窗封存未見'
    return '歷史待補'


def outcomes(signals,ix,market,e,now):
    """One source/market/stock/day/horizon row regardless of strategy count."""
    detail=[];groups=defaultdict(list);pending=Counter()
    for s in signals:
        fresh=freshness(s,ix,market)
        for n in HORIZONS:
            ds=market.span(s['signal_day'],n)
            if not ds or (ds[-1]==now.date().isoformat() and now.time()<time(18,15)):
                pending['未成熟或日曆不足']+=1;continue
            t=back.trade(s,ds,e)
            r={'研究日':s['signal_day'],'市場':s['market'],'股票代號':s['code'],'股票名稱':s['name'],'訊號來源':s['source'],'期間':n,'出場日':ds[-1],'研究新鮮度':fresh,'決策時間':s['created_at'],**t}
            detail.append(r);groups[(s['source'],s['market'],n,fresh)].append(r)
    summary=[]
    for (src,m,n,fresh),rr in sorted(groups.items()):
        ok=[r for r in rr if r['status']=='VALID_CASH_MODEL'];cov=len(ok)/len(rr)
        summary.append({'訊號來源':src,'市場':m,'期間':n,'研究新鮮度':fresh,'成熟候選':len(rr),'覆蓋率':cov,'有效':len(ok),'獨立研究日':len({r['研究日'] for r in ok}),'平均淨報酬':mean([r['net_return'] for r in ok]) if cov>=.95 else None,'勝率':mean([float(r['net_return']>0) for r in ok]) if cov>=.95 else None,'狀態':'DESCRIPTIVE_ONLY' if cov>=.95 else 'DATA_PENDING'})
    # Same source, market, signal day and horizon. Entire cohorts, no survivor-only comparison.
    pairs=defaultdict(lambda:defaultdict(list))
    for r in detail:pairs[(r['訊號來源'],r['市場'],r['期間'],r['研究日'])][r['研究新鮮度']].append(r)
    paired=[]
    for (src,m,n,d),g in sorted(pairs.items()):
        a,b=g['近窗曾入研究'],g['近窗封存未見'];ok=len(a)>=MIN_PER_GROUP and len(b)>=MIN_PER_GROUP and all(r['status']=='VALID_CASH_MODEL' for r in a+b)
        av=mean([r['net_return'] for r in a]) if ok else None;bv=mean([r['net_return'] for r in b]) if ok else None
        paired.append({'訊號來源':src,'市場':m,'期間':n,'研究日':d,'出場日':(a+b+g['歷史待補'])[0]['出場日'],'決策時間':min(r['決策時間'] for r in a+b) if a+b else '', '結果觀測時間':max((r.get('resolved_at','') for r in a+b),default='') if ok and all(r.get('resolved_at') for r in a+b) else '', '重複組候選':len(a),'近窗未見組候選':len(b),'歷史待補':len(g['歷史待補']),'重複組平均淨報酬':av,'近窗未見組平均淨報酬':bv,'未見組相對差':bv-av if ok else None,'狀態':'DESCRIPTIVE_ONLY' if ok else 'DATA_PENDING'})
    if not signals:pending['尚無合格前瞻研究訊號']=1
    return detail,summary,paired,dict(pending)


def review_windows(paired):
    """Fixed, predeclared nonoverlap and 20/10 time split. Human review only.
    Dates selected before validity filtering; unknown/failed cohorts stay denominators.
    """
    groups=defaultdict(list)
    for r in paired:groups[r['訊號來源'],r['市場'],r['期間']].append(r)
    out=[]
    for (src,m,n),rs in sorted(groups.items()):
        chosen=[];end=''
        for r in sorted(rs,key=lambda x:x['研究日']):
            if r['研究日']>end:chosen.append(r);end=r['出場日']
        state='BASELINE_ONLY';train=[];test=[];eligible_train=[];eligible_test=[]
        if len(chosen)>=30:
            train,test=chosen[-30:-10],chosen[-10:]
            cut=perf.stamp(test[0]['決策時間']) if test[0]['決策時間'] else None
            def good(r):return r['狀態']=='DESCRIPTIVE_ONLY' and r['未見組相對差'] is not None and r['結果觀測時間']
            eligible_train=[r for r in train if good(r) and cut and perf.stamp(r['結果觀測時間'])<cut]
            eligible_test=[r for r in test if good(r)]
            if len(eligible_train)==20 and len(eligible_test)==10:
                a=[r['未見組相對差'] for r in eligible_train];b=[r['未見組相對差'] for r in eligible_test]
                if mean(a)>=.005 and mean(b)>=.005 and mean([float(v>0) for v in a])>=.6 and mean([float(v>0) for v in b])>=.6:state='REVIEW_NEW_PRIORITY'
                elif mean(a)<=-.005 and mean(b)<=-.005 and mean([float(v<0) for v in a])>=.6 and mean([float(v<0) for v in b])>=.6:state='REVIEW_REPEAT_PRIORITY'
                else:state='NO_CONFIRMED_EDGE'
        out.append({'訊號來源':src,'市場':m,'期間':n,'非重疊窗':len(chosen),'訓練有效窗':len(eligible_train),'留出有效窗':len(eligible_test),'留出開始':test[0]['研究日'] if test else None,'訓練平均差':mean([r['未見組相對差'] for r in eligible_train]) if len(eligible_train)==20 else None,'留出平均差':mean([r['未見組相對差'] for r in eligible_test]) if len(eligible_test)==10 else None,'狀態':state,'權重倍率':1.0})
    return out


def daily_snapshot(root,market,now,ix):
    b=gap.build(root,now);day=now.date().isoformat();prev=b['previous_day']
    if b['day']!=day or not market.today_confirmed(day) or market.span(prev,1)!=[prev,day] or b['errors'] or set(b['coverage'])!=set(ar.URLS):return None
    a,state=prior_audit(root,prev,now)
    summary,detail=compare_leaders(b,ix,a,state,now)
    return {'version':VERSION,'day':day,'asof':now.isoformat(),'previous_day':prev,'market_report_hash':perf.digest(b),'summary':summary,'detail':detail,'trade_authority_changed':False}


def freeze(root,b):
    # exclusive atomic publication; never overwrite a prior observed leader audit
    import gzip,os,tempfile
    p=Path(root)/REL/(b['day']+'.json.gz');p.parent.mkdir(parents=True,exist_ok=True)
    blob=ar._bytes(ar._seal(b))
    if len(blob)>16_000_000:raise ValueError('H217 archive too large')
    fd,tmp=tempfile.mkstemp(dir=p.parent,prefix='.h217-')
    try:
        with os.fdopen(fd,'wb') as f:f.write(gzip.compress(blob,mtime=0));f.flush();os.fsync(f.fileno())
        try:os.link(tmp,p)
        except FileExistsError:return False
    finally:os.unlink(tmp)
    return True


def read_daily(root,now):
    hist=[]
    for p in sorted((Path(root)/REL).glob('*.json.gz')):
        b=ar._read(p);obs=perf.stamp(b['asof'])
        if b['version']!=VERSION or b['trade_authority_changed'] is not False or p.name!=b['day']+'.json.gz' or obs.date().isoformat()!=b['day'] or obs.time()<time(18,15) or b['previous_day']>=b['day']:raise ValueError('H217 invalid daily audit')
        if obs<=now:hist.append(b)
    return hist


def run(root=ROOT,now=None,*,capture=False):
    live=now is None;root=Path(root);now=perf.stamp(now or datetime.now(ar.TZ));m=perf.Market(root,now)
    archive_signature=ar._archive_manifest(root/'data/h190')[1]
    s194,o194=perf.read_ledger(root);eligible=back.read_signals(root,now,ledger=(s194,o194))
    # H194 research membership must not be narrowed by H209's strategy filter.
    ss=[{**s,'source':'H194'} for s in s194.values() if s['tier'] in perf.TIERS[:3] and perf.stamp(s['created_at'])<=now]+[s for s in eligible if s['source']=='H209']
    ix=source_index(ss);hist=read_daily(root,now)
    cap='READ_ONLY'
    if capture:
        day=now.date().isoformat();cap='ALREADY_CAPTURED' if (root/REL/(day+'.json.gz')).exists() else 'WAITING_SAME_SESSION'
        if cap!='ALREADY_CAPTURED' and not m.errors and now.time()>=time(18,15):
            b=daily_snapshot(root,m,now,ix)
            if b:
                # Capture the observed result, including pending markets/sources;
                # a missing H209 must not prevent H194 recall history.
                ready=bool(b['summary'])
                if ready:
                    end=perf.stamp(datetime.now(ar.TZ)) if live else now
                    if end.date()==now.date() and end>=now and ar._archive_manifest(root/'data/h190')[1]==archive_signature:
                        b['asof']=end.isoformat();b['evidence_cutoff']=now.isoformat();cap='CAPTURED' if freeze(root,b) else 'ALREADY_CAPTURED';hist=read_daily(root,end)
                    else:cap='WINDOW_EXPIRED_OR_ARCHIVE_CHANGED'
                else:cap='WAITING_PRIOR_OR_COVERAGE'
    if m.errors:detail,summary,paired,pending=[],[],[],{'行情封存驗證異常':len(m.errors)}
    else:detail,summary,paired,pending=outcomes(ss,ix,m,back.Evidence(root,m,now),now)
    if ar._archive_manifest(root/'data/h190')[1]!=archive_signature:raise RuntimeError('archive changed during H217; retry')
    current=gap.build(root,now) if not m.errors else {'day':'','previous_day':'','coverage':{},'rows':[],'errors':m.errors}
    if ar._archive_manifest(root/'data/h190')[1]!=archive_signature:raise RuntimeError('archive changed during H217 current report; retry')
    a,astate=prior_audit(root,current['previous_day'],now) if current['previous_day'] else ({},'NO_SESSION')
    recall,miss=compare_leaders(current,ix,a,astate,now)
    valid=sum(r['status']=='VALID_CASH_MODEL' for r in detail);pairs=sum(r['狀態']=='DESCRIPTIVE_ONLY' for r in paired)
    health={}
    for rel in ('h190/session_health.json','h194/latest.json.gz','h209/collector.json'):
        try:
            v=ar._read(root/'data'/rel);ts=v.get('asof',v.get('observed_at'))
            if ts and perf.stamp(ts)<=now:health[rel]={k:v[k] for k in ('asof','observed_at','day','status','markets','capture_gate','signals','mature','pending','saved','errors','invalid_existing_count','unverified_total') if k in v}
        except (OSError,ValueError,TypeError,KeyError):pass
    counts={'h194_signals':sum(perf.stamp(s['created_at'])<=now for s in s194.values()),'h194_raw_mature':sum(perf.stamp(o['resolved_at'])<=now for o in o194.values()),'h194_eligible_research':sum(s['source']=='H194' for s in ss),'h209_research_signals':sum(s['source']=='H209' for s in ss),'mature_unique_source_rows':len(detail),'valid_net_rows':valid,'valid_paired_windows':pairs,'frozen_leader_days':len(hist)}
    return {'version':VERSION,'asof':(max(now,perf.stamp(hist[-1]['asof'])) if hist else now).isoformat(),'evidence_cutoff':now.isoformat(),'day':current['day'],'status':'DESCRIPTIVE_ONLY' if valid and not m.errors else 'DATA_PENDING','basis':BASIS,'capture':cap,'counts':counts,'pending':pending,'recall':recall,'miss':miss,'history':[r for b in hist for r in b['summary']],'history_detail':[r for b in hist for r in b['detail']],'summary':summary,'detail':detail,'paired':paired,'review':review_windows(paired),'pipeline':health,'errors':list(m.errors)+list(current['errors']),'weight_multiplier':1.0,'trade_authority_changed':False}


def excel(r):
    import openpyxl
    from openpyxl.styles import Font,PatternFill
    from openpyxl.utils import get_column_letter
    w=openpyxl.Workbook();w.remove(w.active)
    tables=[('01_主管摘要',[{'版本':r['version'],'資料日':r['day'],'觀測時間':r['asof'],'狀態':r['status'],'封存':r['capture'],'範圍':r['basis'],**r['counts'],'權重倍率':r['weight_multiplier'],'錯誤':r['errors'],'待成熟':r['pending']}]),('02_當日研究召回',r['recall']),('03_強勢漏選證據',r['miss']),('04_歷史召回',r['history']),('05_歷史逐股',r['history_detail']),('06_成熟績效',r['summary']),('07_同日新舊對照',r['paired']),('08_成熟逐筆',r['detail']),('09_樣本外複核',r['review']),('10_管線健康',[{'來源':k,**v} for k,v in r['pipeline'].items()])]
    for name,rows in tables:
        s=w.create_sheet(name);cols=list(dict.fromkeys(k for row in rows for k in row)) or ['資料狀態'];s.append(cols)
        for row in rows:
            vals=[]
            for k in cols:
                x=row.get(k);x=json.dumps(x,ensure_ascii=False) if isinstance(x,(list,dict)) else x
                if isinstance(x,str):x=x[:32000];x="'"+x if x.lstrip().startswith(('=','+','-','@')) else x
                vals.append(x)
            s.append(vals)
        if not rows:s.append(['尚無有效資料；非零勝率'])
        for c in s[1]:c.font=Font(bold=True,color='FFFFFF');c.fill=PatternFill('solid',fgColor='174765')
        s.freeze_panes='C2';s.auto_filter.ref=s.dimensions
        for i,k in enumerate(cols,1):
            s.column_dimensions[get_column_letter(i)].width=45 if any(x in k for x in ('原因','範圍','證據','錯誤')) else 18
            if k in ('研究召回率','雙日覆蓋率','覆蓋率','平均淨報酬','勝率','重複組平均淨報酬','近窗未見組平均淨報酬','未見組相對差','訓練平均差','留出平均差','raw_return','net_return','reference_return','trade_mdd'):
                for c in list(s.columns)[i-1][1:]:c.number_format='0.00%'
    b=io.BytesIO();w.save(b);return b.getvalue()

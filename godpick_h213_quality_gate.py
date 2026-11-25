# -*- coding: utf-8 -*-
"""H213 alternate official report evidence + daily research quality, no authority writes.
Monthly and daily market reports may have different trading scopes: evidence is
quarantined, never injected into H190 solely because prices agree.
"""
from collections import Counter, defaultdict
from datetime import datetime, date, time, timedelta
from pathlib import Path
import io, re, time as wall
import godpick_h190_market_archive as ar
import godpick_h194_performance as perf
VERSION='V191-H213-QUALITY-GATE'
ROOT=Path(__file__).resolve().parent
REL=Path('data/h213')
URLS={'TWSE':'https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY','TPEX':'https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock'}
WORKFLOWS={'H190':'h190_market_archive.yml','H194':'h194_daily_learning.yml','H209':'h209_rolling_backtest.yml','H195':'h195_daily_command.yml'}


def monthly(raw,m,code,month,now):
    """Require response-side code, dated named columns, explicit units; no synthetic bars."""
    now=perf.stamp(now)
    if m not in URLS or not re.fullmatch(r'[1-9]\d{3}',code) or not re.fullmatch(r'\d{4}-\d{2}',month) or month>now.date().isoformat()[:7] or not isinstance(raw,dict):raise ValueError('invalid monthly request')
    if str(raw.get('stat','')).lower() not in ('','ok'):raise ValueError('upstream rejected request')
    candidates=[]
    for t in raw.get('tables') or [raw]:
        if not isinstance(t,dict):continue
        f=[ar._field(x) for x in t.get('fields',t.get('headers',[]))]
        aliases={'d':('日期','成交日期'),'o':ar.ALIASES['open'],'h':ar.ALIASES['high'],'l':ar.ALIASES['low'],'c':ar.ALIASES['close'],'v':('成交股數','成交股數(股)','成交量(股)','成交仟股','成交千股','成交股數(仟股)','成交股數(千股)','成交量(仟股)','成交量(千股)','成交量(張)'),'a':('成交金額','成交金額(元)','成交金額(仟元)','成交金額(千元)','成交仟元','成交千元'),'n':ar.ALIASES['trades']+('成交筆數(筆)','成交筆數(次)')}
        idx={k:next((i for i,x in enumerate(f) if x in names),None) for k,names in aliases.items()}
        if all(i is not None for i in idx.values()):candidates.append((t,f,idx))
    if len(candidates)!=1:raise ValueError('monthly fields absent/ambiguous')
    t,f,i=candidates[0]
    ids=set()
    for obj in (raw,t):
        for k in ('stockNo','code','stockCode','股票代號','證券代號'):
            if obj.get(k) is not None:ids.add(str(obj[k]).strip())
        for k in ('title','subtitle','name'):
            ids.update(re.findall(r'(?<!\d)([1-9]\d{3})(?!\d|年)',str(obj.get(k,''))))
    if ids!={code}:raise ValueError('response stock identity not proven')
    vf,af=f[i['v']],f[i['a']]
    # TPEX unlabeled amount is ambiguous; do not assume thousands from position.
    if m=='TPEX' and af=='成交金額':raise ValueError('TPEX amount unit not explicit')
    vs=1000 if any(x in vf for x in ('千','仟','張')) else 1
    a_s=1000 if any(x in af for x in ('千','仟')) else 1
    rows={};states={}
    for x in t.get('data',[]):
        if not isinstance(x,(list,tuple)) or len(x)<=max(i.values()):raise ValueError('short monthly row')
        d=ar._day(str(x[i['d']]).replace('*','').replace('＊',''))
        if d[:7]!=month or d in states:raise ValueError('monthly date mismatch/duplicate')
        if d>now.date().isoformat() or (d==now.date().isoformat() and now.time()<time(18,15)):raise ValueError('future or intraday monthly row')
        o,h,l,c,v,a,n=[ar._num(x[i[k]]) for k in ('o','h','l','c','v','a','n')]
        if v==0 and a==0 and n==0:states[d]='OFFICIAL_ZERO_VOLUME';continue
        if any(y is None or y<=0 for y in (o,h,l,c,v,a,n)) or h<max(o,l,c) or l>min(o,h,c) or not (v*vs).is_integer() or not (a*a_s).is_integer() or not n.is_integer() or not l*.5<=a*a_s/(v*vs)<=h*2:
            states[d]='INVALID_OR_NO_QUOTE';continue
        rows[d]=[code,o,h,l,c,int(v*vs),int(a*a_s),int(n)];states[d]='POSITIVE_QUOTE'
    if not states:raise ValueError('empty monthly report; not proof of suspension')
    return rows,states


def compare(root,m,code,month,raw,now,missing):
    now=perf.stamp(now);p=Path(root)/'data/h190';rows,states=monthly(raw,m,code,month,now)
    cal,markets,_,errors=ar._index(p,asof=now)
    if errors:raise ValueError('base archive errors')
    overlap=0;conflicts=[];result=[]
    for d,r in rows.items():
        if d not in cal.get(month,set()) or d not in markets[m]:continue
        old=ar.quote_rows(p,m,d,now).get(code)
        if old:
            if old[0]==r:overlap+=1
            else:conflicts.append(d)
    for d in sorted(set(missing)):
        if d[:7]!=month or d not in cal.get(month,set()) or d not in markets[m]:continue
        if code in ar.quote_rows(p,m,d,now):continue
        state=states.get(d,'NOT_LISTED_IN_REPORT')
        if state=='POSITIVE_QUOTE':state='SCOPE_REVIEW' if conflicts or overlap<3 else 'CROSS_REPORT_CANDIDATE'
        result.append({'市場':m,'股票代號':code,'交易日':d,'補證狀態':state,'一致重疊日':overlap,'衝突日數':len(conflicts),'來源':URLS[m],'限制':'月表與整日成交範圍未獨立證實；不注入H190；停牌及上市狀態需公告核實'})
    return result,conflicts


def collect(root=ROOT,now=None,*,budget=6,http=None,pause=1.5):
    from godpick_h212_data_recovery import gaps
    if not 1<=budget<=8:raise ValueError('budget 1..8')
    live=now is None;now=perf.stamp(now or datetime.now(ar.TZ));root=Path(root)
    if http is None:
        import requests
        http=requests.Session()
    g=gaps(root,now);q=defaultdict(set);state={};p=root/REL/'monthly_health.json'
    try:
        old=ar._read(p)
        if old['version']==VERSION and perf.stamp(old['asof'])<=now:state=old['attempts']
    except (OSError,ValueError,KeyError,TypeError):pass
    state={k:v for k,v in state.items() if k.split(':')[-1]>=(now.date()-timedelta(days=120)).isoformat()[:7]}
    for r in g['stock_gaps']:
        for d in r['缺日期'].split('、'):
            if d:q[r['市場'],r['股票代號'],d[:7]].add(d)
    # Reserve two thirds for TPEX initially, but never starve TWSE.
    queues={m:[] for m in URLS}
    for k,ds in sorted(q.items(),key=lambda x:(-len(x[1]),x[0])):
        old=state.get(':'.join(k),{})
        try:
            if perf.stamp(old['at'])+timedelta(days=1 if old['status']=='ERROR' else 7)>now:continue
        except (ValueError,KeyError,TypeError):pass
        queues[k[0]].append((k,ds))
    tries=[];fail=Counter();order=('TPEX','TWSE','TPEX');n=0
    while n<budget and any(queues.values()):
        for m in order:
            if n>=budget:break
            if fail[m]>=2:queues[m]=[]
            if not queues[m]:continue
            (m,c,month),ds=queues[m].pop(0);k=':'.join((m,c,month));n+=1
            try:
                params={'date':month.replace('-','')+'01','stockNo':c,'response':'json'} if m=='TWSE' else {'date':month.replace('-','/')+'/01','code':c,'response':'json'}
                raw=ar._get(http,URLS[m],params,12);obs=perf.stamp(datetime.now(ar.TZ)) if live else now
                detail,conflicts=compare(root,m,c,month,raw,obs,ds)
                body={'version':VERSION,'asof':obs.isoformat(),'market':m,'code':c,'month':month,'source':URLS[m],'raw':raw,'detail':detail,'conflicts':conflicts}
                out=root/REL/'monthly'/m/c/month/(obs.strftime('%Y%m%dT%H%M%S%f')+'.json.gz')
                perf.immutable(out,body);v={'status':'EVIDENCE_ONLY','detail':detail,'conflicts':conflicts};fail[m]=0
            except Exception as e:v={'status':'ERROR','reason':str(e)[:160],'detail':[]};fail[m]+=1
            state[k]={**v,'at':(perf.stamp(datetime.now(ar.TZ)) if live else now).isoformat()};tries.append({'market':m,'code':c,'month':month,**state[k]});wall.sleep(pause)
    result={'version':VERSION,'asof':(perf.stamp(datetime.now(ar.TZ)) if live else now).isoformat(),'requests':n,'attempts':state,'tried':tries,'promoted_rows':0,'trade_authority_changed':False}
    ar._write(p,ar._seal(result));return result


def recovery_plan(now,day,checks):
    """Only on the same evening; dispatch the earliest unresolved prerequisite.
    No request after midnight, no guessed holiday, no authority mutation.
    """
    now=perf.stamp(now)
    if now.time()<time(18,15) or now.time()>=time(23,35) or now.weekday()>=5:return []
    if not checks.get('archive_ok'):return [] # corruption requires manual audit
    if day!=now.date().isoformat() or not checks.get('calendar') or not checks.get('both_quotes'):return ['H190']
    # H194 and H195 can proceed independently after official quotes; H209 follows H194.
    out=[]
    if not checks.get('H194'):out.append('H194')
    elif not checks.get('H209'):out.append('H209')
    if not checks.get('H195'):out.append('H195')
    return out



def jump_audit(root,now,gap):
    from godpick_h209_rolling_backtest import read_actions
    p=Path(root)/'data/h190';_,_,rows,errors=ar._index(p,asof=now);out=[];cache={}
    if errors:return out
    for r in gap['stock_gaps']:
        if '跳空' not in r['原診斷']:continue
        m,c=r['市場'],r['股票代號'];seq=sorted(rows.get((m,c),[]))[-60:]
        for a,b in zip(seq,seq[1:]):
            if not a[4] or (abs(b[1]/a[4]-1)<=.12 and abs(b[4]/a[4]-1)<=.15):continue
            d=b[0];k=(m,d)
            if k not in cache:
                try:cache[k]=read_actions(root,m,d,now)[0]
                except (OSError,ValueError,KeyError,TypeError):cache[k]=None
            v=cache[k];event=v.get(c) if v is not None else None
            status='權息證據待抓' if v is None else '官方表無該股權息／其他事件待查' if event is None else '前收盤一致／權息事件已核' if event['previous_close'] is not None and abs(event['previous_close']-a[4])<=.011 else '權息前價衝突／人工核對'
            out.append({'市場':m,'股票代號':c,'交易日':d,'原始前收盤':a[4],'原始開盤':b[1],'原始收盤':b[4],'權息狀態':status,'權息事件':event,'限制':'不改寫原始價格或自動解除技術跳空排除'})
    return out

def quality(root=ROOT,now=None):
    from godpick_h212_data_recovery import run
    from godpick_h206_market_gap import build
    from godpick_h209_rolling_backtest import read_signals
    from godpick_h211_counterevidence import read_history
    from godpick_h195_command_center import VERSION as JV,REL as JR
    root=Path(root);now=perf.stamp(now or datetime.now(ar.TZ));h=run(root,now);m=perf.Market(root,now);b=build(root,now);day=b['day'];today=now.date().isoformat();obs=m.cal_observed.get(today[:7]);closed=bool(obs and obs.date().isoformat()==today and obs.time()>=time(18,15) and today not in m.days);target=today if now.time()>=time(18,15) and now.weekday()<5 and not closed else day;checks={'archive_ok':not m.errors,'calendar':m.today_confirmed(target) if target==today else target in m.days,'both_quotes':bool(target) and all(m.quotes(x,target) for x in URLS)};errors=[]
    try:
        ss,_=perf.read_ledger(root);checks['H194']=any(k[0]==target and perf.stamp(v['created_at'])<=now for k,v in ss.items())
        s2=read_signals(root,now);checks['H209']=any(s['source']=='H209' and s['signal_day']==target for s in s2)
        prior={k for k,v in ss.items() if perf.stamp(v['created_at'])<=now and v['tier'] in perf.TIERS[:3]}
    except Exception as e:errors.append('ledger: '+str(e)[:150]);prior=set();checks['H194']=checks['H209']=False
    try:
        j=ar._read(root/JR);ts=perf.stamp(j['asof']);checks['H195']=j['version']==JV and j['trade_authority_changed'] is False and ts<=now and ts.date().isoformat()>=target and j['market_day']==target and j['technical_current'] is True and j['status']=='RESEARCH_AVAILABLE' and j['health'].get('technical_status')=='CROSS_MARKET_RESEARCH_READY' and not j.get('errors') and not j['health'].get('financial_errors')
    except FileNotFoundError:checks['H195']=False
    except Exception as e:checks['H195']=False;errors.append('H195: '+str(e)[:150])
    hist,errs=read_history(root,[target] if target else [],now);checks['H211']=bool(hist.get(target));errors+=errs
    coverage=h['gap']['coverage'];complete=all(coverage.get(x,{}).get('ratio',0)>=.95 for x in URLS)
    leader=[]
    for x in URLS:
        v=b['prior_research'].get(x,{});cv=b['coverage'].get(x,{});rr=[r for r in b['rows'] if r['市場']==x];prev=b['previous_day'];prior_n=sum(k[:2]==(prev,x) for k in prior)
        eligible=b['status']=='MARKET_READY' and not errors and prior_n>0 and bool(rr) and v.get('可比較強勢股')==len(rr) and (cv.get('雙日覆蓋率%') or 0)>=95
        hits=sum((prev,x,r['股票代號']) in prior and r['前日研究對帳']=='前日已入研究封存' for r in rr) if eligible else None
        leader.append({'市場':x,'日期':day,'前日':prev,'強勢榜樣本':len(rr),'前日研究數':prior_n or None,'命中數':hits,'研究榜召回率':hits/len(rr) if eligible else None,'狀態':'VALID_RESEARCH_RECALL' if eligible else 'PENDING','限制':'前日已存在研究清單之命中比例，不代表完整策略漏選率；H206每市場最多20檔、原始價格強勢榜；非全市場預測勝率或正式買進績效'})
    gates=[('歷史60日雙市場覆蓋',complete),('原始封存完整',checks['archive_ok'] and not errors),('驗收日官方交易日曆',checks['calendar']),('驗收日雙市場行情',checks['both_quotes']),('H194驗收日研究',checks['H194']),('H209驗收日召回封存',checks['H209']),('H195驗收日研究',checks['H195']),('H211驗收日證據',checks['H211']),('成熟含成本淨績效',h['counts']['valid_rows']>0)]
    # No arbitrary weighted score masquerading as selection accuracy.
    status='RESEARCH_PIPELINE_READY' if all(v for _,v in gates) else 'WAITING_EVIDENCE'
    plan=recovery_plan(now,day,checks) if not errors else []
    try:
        mh=ar._read(root/REL/'monthly_health.json');mh=mh if mh['version']==VERSION and perf.stamp(mh['asof'])<=now else {}
    except (OSError,ValueError,KeyError,TypeError):mh={}
    return {'version':VERSION,'asof':now.isoformat(),'session':target,'leader_session':day,'status':status,'checks':[{'環節':k,'狀態':'PASS' if v else 'PENDING'} for k,v in gates],'passed':sum(bool(v) for _,v in gates),'total':len(gates),'coverage':coverage,'counts':h['counts'],'capture':h['capture'],'recall':leader,'monthly':mh.get('tried',[]),'monthly_detail':[r for v in mh.get('attempts',{}).values() if perf.stamp(v['at'])<=now for r in v.get('detail',[])],'jumps':jump_audit(root,now,h['gap']),'repair_plan':plan,'errors':errors+m.errors,'trade_authority_changed':False}


def excel(r):
    import openpyxl
    from openpyxl.styles import Font,PatternFill
    from openpyxl.utils import get_column_letter
    w=openpyxl.Workbook();w.remove(w.active)
    for name,rs in [('01_主管驗收',[{'日期':r['session'],'觀測':r['asof'],'狀態':r['status'],'通過':r['passed'],'環節':r['total'],**r['counts']}]),('02_品質閘門',r['checks']),('03_行情覆蓋',[{'市場':k,**v} for k,v in r['coverage'].items()]),('04_封存管線',r['capture']),('05_研究召回',r['recall']),('06_月表補證',r['monthly_detail']),('07_復原計畫',[{'工作流':v} for v in r['repair_plan']]),('08_錯誤',[{'錯誤':v} for v in r['errors']]),('09_權息跳空',r['jumps'])]:
        s=w.create_sheet(name);keys=list(dict.fromkeys(k for x in rs for k in x))
        if not keys:s.append(['無資料／待補證']);continue
        s.append(keys)
        for x in rs:
            vals=[]
            for k in keys:
                v=x.get(k)
                if isinstance(v,(list,dict)):v=str(v)
                if isinstance(v,str) and v[:1] in ('=','+','-','@'):v="'"+v
                vals.append(v)
            s.append(vals)
        s.freeze_panes='A2';s.auto_filter.ref=s.dimensions
        for i,k in enumerate(keys,1):
            c=s.cell(1,i);c.font=Font(bold=True,color='FFFFFF');c.fill=PatternFill('solid',fgColor='173A56');s.column_dimensions[get_column_letter(i)].width=min(50,max(14,len(k)*2+4))
            if k in ('研究榜召回率','ratio'):
                for cell in list(s.columns)[i-1][1:]:cell.number_format='0.00%'
    buf=io.BytesIO();w.save(buf);return buf.getvalue()

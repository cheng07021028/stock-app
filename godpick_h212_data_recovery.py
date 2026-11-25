# -*- coding: utf-8 -*-
"""H212 bounded official repair, evidence gap and descriptive outcome attribution.
Never writes trade authority, fills suspended bars, or retroactively freezes signals.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, time
from pathlib import Path
import io, math, time as wall
import godpick_h190_market_archive as ar
import godpick_h191_opportunity_engine as tech
import godpick_h194_performance as perf
VERSION='V191-H212-DATA-RECOVERY'
ROOT=Path(__file__).resolve().parent
REL=Path('data/h212/repair.json')


def gaps(root,now,technical=None):
    root=Path(root);now=perf.stamp(now);p=root/'data/h190';t=technical if technical is not None else tech.run(root,now=now,top=100)
    cal,markets,rows,errors=ar._index(p,asof=now);day=t['asof'];expected=sorted({d for ds in cal.values() for d in ds if d<=day})[-60:]
    u=tech.master_map(tech.read_json(root/'stock_master_cache.json',[]));diagnostics={(r['市場'],r['股票代號']):r for r in t['diagnostic']};detail=[];byday=Counter();counts=Counter();missing_days=[]
    for m in ar.URLS:
        missing_days.extend({'市場':m,'交易日':d} for d in expected if d not in markets[m])
    for c,r in u.items():
        m=r['市場'];d=diagnostics.get((m,c),{});reason=d.get('缺口','')
        if not d:reason='無技術診斷'
        if not reason:continue
        available={x[0] for x in rows.get((m,c),[]) if x[0] in expected};absent=[d for d in expected if d not in available]
        kind='行情未齊' if absent else '價格異常待權息核對' if '跳空' in reason else '其他資料條件待核'
        if len(expected)!=60:kind='交易日曆不足'
        elif absent and any(d not in markets[m] for d in absent):kind='市場整日缺口'
        elif absent and not available:kind='60日無有效行情／上市狀態待核'
        elif absent and available==set(expected[expected.index(min(available)):]):kind='期初連續缺口／上市狀態待核'
        elif absent:kind='個股缺日／無成交或來源漏列待核'
        counts[m,kind]+=1
        for d in absent:
            if d in markets[m]:byday[m,d]+=1
        detail.append({**r,'資料日':day,'原診斷':reason,'缺口類型':kind,'有效日數':len(available),'缺日數':len(absent),'缺日期':'、'.join(absent)})
    return {'day':day,'coverage':t['coverage'],'calendar_days':len(expected),'market_missing':missing_days,'archive_errors':errors,'stock_gaps':detail,'gap_summary':[{'市場':m,'原因':k,'檔數':v} for (m,k),v in sorted(counts.items())],'day_queue':[{'市場':m,'交易日':d,'缺檔數':n} for (m,d),n in sorted(byday.items(),key=lambda x:(-x[1],x[0]))], 'technical_status':t['status']}


def repair(root=ROOT,now=None,*,budget=8,http=None,pause=1.2,timeout=12):
    """Re-fetch dated official batches; additions only; cooldown stops futile repeats.
    Missing whole dates remain H190's responsibility; corrupt original files require manual audit.
    """
    if not 1<=budget<=12:raise ValueError('budget outside 1..12')
    if http is None:
        import requests
        http=requests.Session()
    root=Path(root);live=now is None;now=perf.stamp(now or datetime.now(ar.TZ));p=root/'data/h190';before=gaps(root,now);state={}
    try:
        old=ar._read(root/REL)
        if old.get('version')==VERSION and perf.stamp(old['asof'])<=now:state=old.get('attempts',{})
    except (OSError,ValueError,TypeError,KeyError):pass
    # Expire old attempt metadata, retain recent diagnostics. No archive deletion.
    state={k:v for k,v in state.items() if k.split(':')[-1]>= (now.date()-timedelta(days=120)).isoformat()}
    tried=[];added=0;fail=Counter();n=0
    # Round-robin market queues to avoid one market consuming the entire budget.
    qs={m:[] for m in ar.URLS}
    for row in before['day_queue']:
        m,d=row['市場'],row['交易日'];k=m+':'+d
        if ar.supplemental_path(p,m,d).exists():continue
        prev=state.get(k,{})
        try:due=perf.stamp(prev['at'])+timedelta(days=7 if prev['status']=='NO_NEW_VALID_ROWS' else 1)<=now
        except (ValueError,KeyError,TypeError):due=True
        if due:qs[m].append(row)
    while n<budget and any(qs.values()):
        for m in ar.URLS:
            if n>=budget:break
            if not qs[m] or fail[m]>=3:qs[m]=[];continue
            x=qs[m].pop(0);d=x['交易日'];k=m+':'+d;n+=1
            try:
                params={'date':d.replace('-',''),'type':'ALLBUT0999','response':'json'} if m=='TWSE' else {'date':d.replace('-','/'),'type':'EW','response':'json'}
                raw=ar._get(http,ar.URLS[m],params,timeout)
                # Clock for live download is actual observation, not the run start.
                obs=perf.stamp(datetime.now(ar.TZ)) if live else now
                count=ar.save_supplement(p,m,d,raw,obs.isoformat());added+=count;fail[m]=0
                v={'status':'ADDED' if count else 'NO_NEW_VALID_ROWS','added':count}
            except Exception as e:
                fail[m]+=1;v={'status':'CONFLICT_OR_SOURCE_ERROR','added':0,'reason':str(e)[:180]}
            state[k]={**v,'at':(perf.stamp(datetime.now(ar.TZ)) if live else now).isoformat()};tried.append({**x,**state[k]})
            wall.sleep(pause)
    now=perf.stamp(datetime.now(ar.TZ)) if live else now;after=gaps(root,now);out={'version':VERSION,'asof':now.isoformat(),'before_coverage':before['coverage'],'after_coverage':after['coverage'],'new_valid_rows':added,'requests':n,'attempts':state,'tried':tried,'archive_errors':after['archive_errors'],'trade_authority_changed':False}
    ar._write(root/REL,ar._seal(out));return out


def capture_audit(root,now,t,j,b):
    market=perf.Market(root,now);day=now.date().isoformat();cut=now.time()>=time(18,15);rows=[]
    def add(stage,status,detail):rows.append({'環節':stage,'狀態':status,'說明':detail})
    add('當日盤後窗口','READY' if cut else 'WAIT','18:15後才可建立當日研究，不回填')
    add('官方當日交易日曆','READY' if market.today_confirmed(day) else 'WAIT','當日盤後觀測需核實，非工作日不猜測')
    for m in ar.URLS:
        q=market.quotes(m,day);add(m+' 同日行情','READY' if q else 'WAIT',f'{len(q)} 檔有效行情')
    add('H193 財技研究','READY' if j['same_session'] and j['market_day']==day else 'WAIT',str(j['counts']))
    for name,path in [('H194','h194/signals'),('H208','h209/signals'),('H211','h211/daily')]:
        f=Path(root)/'data'/path/(day+'.json.gz')
        try:
            v=ar._read(f);ts=perf.stamp(v.get('created_at',v.get('asof')));valid=ts<=now and ts.date().isoformat()==day and ts.time()>=time(18,15) and bool(v.get('rows'))
            add(name+' 當日封存','PRESENT' if valid else 'WAIT',f"{len(v.get('rows',[]))} 筆；封存存在不等於完整策略驗證")
        except FileNotFoundError:add(name+' 當日封存','WAIT','未建立；不可事後回填')
        except Exception as e:add(name+' 當日封存','ERROR',str(e)[:160])
    add('H209 成熟淨績效','READY' if b['valid_rows'] else 'WAIT',f"有效 {b['valid_rows']}／成熟策略列 {b['mature_strategy_rows']}；{b['pending']}")
    for name,rel in [('H190','h190/health.json'),('H194','h194/latest.json.gz'),('H209','h209/latest.json.gz')]:
        try:
            v=ar._read(Path(root)/'data'/rel);ts=perf.stamp(v.get('asof',v.get('observed_at')))
            if ts<=now:add(name+' 上次排程','OBSERVED',ts.isoformat()+'｜'+str(v.get('capture_status',v.get('status','')))[:200])
        except (OSError,ValueError,TypeError,KeyError):pass
    return rows


def attribution(detail):
    """Describe realized simulated outcomes only; NOT causal explanations."""
    out=[];summary=Counter()
    for r in detail:
        status=r['status'];net=r.get('net_return');raw=r.get('raw_return')
        if status!='VALID_CASH_MODEL':kind='不可歸因／'+status
        elif not isinstance(net,(int,float)) or not math.isfinite(net):kind='淨報酬不合法／待核'
        elif net>=0:kind='非虧損'
        elif isinstance(raw,(float,int)) and raw>=0:kind='原價非負但含成本淨損（模型差額）'
        else:kind='原始價格下跌且淨損'
        summary[r['市場'],r['策略'],r['期間'],kind]+=1
        out.append({k:r.get(k) for k in ('研究日','市場','股票代號','股票名稱','策略','期間','進場日','出場日','訊號來源','status','raw_return','net_return','cash_dividend','net_pnl','events')}|{'結果分類':kind,'限制':'描述性分類，非因果推論；策略列可重疊'})
    return out,[{'市場':m,'策略':s,'期間':n,'結果分類':k,'策略列數':v} for (m,s,n,k),v in sorted(summary.items())]


def run(root=ROOT,now=None):
    from godpick_h193_joint_decision import run as joint
    from godpick_h206_market_gap import build
    from godpick_h209_rolling_backtest import run as backtest
    root=Path(root);now=perf.stamp(now or datetime.now(ar.TZ));t=tech.run(root,now=now,top=100);g=gaps(root,now,t);j,_=joint(root,now=now);b=backtest(root,now=now);m=build(root,now)
    intersections=[];cnt=Counter();audit={(x['市場'],x['股票代號']):x for x in j['audit']}
    for x in j['audit']:
        if x['研究分級'] not in ('財報研究','雙證據研究','技術研究'):continue
        c=x.get('H211技術證據') or {};kind='已入雙證據' if x['研究分級']=='雙證據研究' else '財報條件未交叉' if x['研究分級']=='技術研究' else '技術資料缺口' if not c else '流動性未通過' if not c.get('流動性通過') else '技術已計算／未入池'
        cnt[x['市場'],kind]+=1;intersections.append({k:x.get(k) for k in ('市場','股票代號','股票名稱','研究分級','技術研究池','財報研究池','技術缺口','財報缺口','H198階段')}|{'交叉診斷':kind})
    leaders=[]
    for x in m['rows']:
        a=audit.get((x['市場'],x['股票代號']),{});same=j['market_day']==m['day'] and j['same_session'];c=a.get('H211技術證據') or {};prior=x.get('前日研究對帳','')
        why='同日技術證據未核' if not same else '已入同日技術研究' if a.get('技術研究池') else '技術資料缺口' if not c else '流動性未通過' if not c.get('流動性通過') else '已計算未入技術池'
        leaders.append({**x,'同日技術診斷':why,'前日判定':prior,'限制':'事後強勢比較，不是前日可預測或正式漏選證明'})
    outcomes,summ=attribution(b['detail']);h={}
    try:
        v=ar._read(root/REL)
        if v.get('version')==VERSION and perf.stamp(v['asof'])<=now:h=v
    except (OSError,ValueError,TypeError,KeyError):pass
    return {'version':VERSION,'asof':now.isoformat(),'status':'RESEARCH_READY' if all(g['coverage'].get(m,{}).get('ratio',0)>=.95 for m in ar.URLS) and b['valid_rows'] and not g['archive_errors'] else 'DATA_PENDING','trade_authority_changed':False,'gap':g,'capture':capture_audit(root,now,t,j,b),'intersection_summary':[{'市場':m,'交叉診斷':k,'檔數':v} for (m,k),v in sorted(cnt.items())],'intersection':intersections,'leaders':leaders,'leader_coverage':m['coverage'],'prior_research':m['prior_research'],'outcomes':outcomes,'outcome_summary':summ,'repair':h,'counts':{'signals':b['signal_count'],'mature_strategy_rows':b['mature_strategy_rows'],'valid_rows':b['valid_rows'],'dual':j['counts']['雙證據研究']}}


def excel(r):
    import openpyxl
    from openpyxl.styles import Font,PatternFill
    from openpyxl.utils import get_column_letter
    w=openpyxl.Workbook();w.remove(w.active)
    data=[('01_主管摘要',[{'版本':r['version'],'觀測時間':r['asof'],'狀態':r['status'],**r['counts']}]),('02_缺口統計',r['gap']['gap_summary']),('03_逐股缺日',r['gap']['stock_gaps']),('04_補抓日期',r['gap']['day_queue']),('05_整日缺口',r['gap']['market_missing']),('06_封存診斷',r['capture']),('07_財技交叉統計',r['intersection_summary']),('08_財技逐股',r['intersection']),('09_強勢對帳',r['leaders']),('10_結果歸因',r['outcome_summary']),('11_成熟逐筆',r['outcomes']),('12_補抓結果',r['repair'].get('tried',[]))]
    for name,rs in data:
        s=w.create_sheet(name);keys=list(dict.fromkeys(k for x in rs for k in x))
        if not keys:s.append(['目前無有效資料；不代表零風險或零勝率']);continue
        s.append(keys)
        for x in rs:
            row=[]
            for k in keys:
                v=x.get(k)
                if isinstance(v,str) and v[:1] in ('=','+','-','@'):v="'"+v
                if isinstance(v,(dict,list)):v=str(v)
                row.append(v)
            s.append(row)
        s.freeze_panes='A2';s.auto_filter.ref=s.dimensions
        for i,k in enumerate(keys,1):
            c=s.cell(1,i);c.font=Font(bold=True,color='FFFFFF');c.fill=PatternFill('solid',fgColor='173A56');s.column_dimensions[get_column_letter(i)].width=min(45,max(13,len(k)*2+3))
            if k in ('raw_return','net_return'):
                for cell in list(s.columns)[i-1][1:]:cell.number_format='0.00%'
    b=io.BytesIO();w.save(b);return b.getvalue()

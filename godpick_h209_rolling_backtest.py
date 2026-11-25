# -*- coding: utf-8 -*-
"""H209 prospective, fixed-rule cohort backtest. Public research only.
Never reconstruct old recommendations from today's master or financial statements.
Cash-only entitlements and official reference-price proxies are distinct.
"""
from __future__ import annotations
from collections import defaultdict, Counter
from datetime import datetime, date, time, timedelta
from pathlib import Path
import io, math, re, statistics, json
import godpick_h190_market_archive as ar
import godpick_h194_performance as perf

VERSION='V191-H209-ROLLING-BACKTEST'  # immutable archive schema
REVISION='V191-H209.1'
ROOT=Path(__file__).resolve().parent
REL=Path('data/h209/latest.json.gz')
LEGACY_TWSE='https://www.twse.com.tw/rwd/zh/announcement/TWT49U'
URLS={'TWSE':'https://www.twse.com.tw/rwd/zh/exRight/TWT49U',
      'TPEX':'https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQ'}
HORIZONS=(1,3,5,10,20)
COST={'shares':1000,'commission':.001425,'min_commission':20.,'sell_tax':.003,'slippage':.001}
BASIS='前瞻封存研究；T+1開盤至T+N收盤，現金股利應收、固定1000股、保守交易成本；非真實成交。複雜權息僅參考價代理，不納入淨績效。'


def _dates(raw,table):
    found=ar._date_evidence(raw,table)
    for obj in (raw,table):
        for a,b in [('startDate','endDate'),('start_date','end_date')]:
            if a in obj and b in obj:
                found.update([ar._day(obj[a]),ar._day(obj[b])])
    return found


def parse_actions(raw,market,day,now,*,start=None,end=None,source_url=None):
    """A successful HTTP call or an unqualified empty array is NOT coverage.
    Accept only one dated named results table, with complete rows/counts.
    """
    now=perf.stamp(now);day=ar._day(day);start=ar._day(start or day);end=ar._day(end or day)
    if not start<=day<=end or (date.fromisoformat(end)-date.fromisoformat(start)).days>14 or end>now.date().isoformat() or (end==now.date().isoformat() and now.time()<time(18,15)):raise ValueError('invalid range')
    if market not in URLS or not isinstance(raw,dict) or day>now.date().isoformat() or (day==now.date().isoformat() and now.time()<time(18,15)):
        raise ValueError('invalid market/day/observation')
    source_url=source_url or URLS[market]
    if source_url!=URLS[market] and not (market=='TWSE' and source_url==LEGACY_TWSE):raise ValueError('invalid source URL')
    stat=str(raw.get('stat','')).strip().lower()
    if stat not in ('','ok','success'):raise ValueError('source status: '+stat[:100])
    aliases={'date':('資料日期','日期','除權息日期','除權除息日期'),'code':('股票代號','證券代號','代號'),'prev':('除權息前收盤價','除權除息前收盤價','除權息前一日收盤價'),'ref':('除權息參考價','除權除息參考價'),'kind':('權/息','權息','權或息','除權息','除權息別','除權息類別'),'value':('權值+息值','權值＋息值','權息值','權值加息值','息值','現金股利'),'cash':('現金股利','息值')}
    candidates=[]
    for t in raw.get('tables') or [raw]:
        if not isinstance(t,dict):continue
        f=[ar._field(x) for x in t.get('fields',t.get('headers',[]))]
        ix={k:next((f.index(a) for a in aa if a in f),None) for k,aa in aliases.items()}
        if all(ix[k] is not None for k in ('date','code','prev','ref','kind')):candidates.append((t,ix))
    if len(candidates)!=1:raise ValueError('missing/ambiguous official named action table')
    t,ix=candidates[0]
    if _dates(raw,t)!={start,end}:raise ValueError('source range not explicitly confirmed')
    rows=t.get('data')
    if not isinstance(rows,list):raise ValueError('missing source data')
    counts=[]
    for obj in (raw,t):
        for key in ('total','totalCount','recordsTotal','recordsFiltered'):
            if key in obj:
                count=int(obj[key]);counts.append(count)
                if count!=len(rows):raise ValueError('partial/paginated source')
    if not rows and not counts:raise ValueError('empty table without explicit zero count')
    # Explicit dated empty table with zero rows is allowed; generic no-data response is not.
    out=[];seen=set()
    for row in rows:
        if not isinstance(row,list) or len(row)!=len(t.get('fields',t.get('headers',[]))):raise ValueError('malformed action row')
        d=ar._day(row[ix['date']]);c=str(row[ix['code']]).strip()
        if not start<=d<=end or not re.fullmatch(r'[0-9A-Z]{4,8}',c) or (d,c) in seen:raise ValueError('action identity/date conflict')
        seen.add((d,c));prev=ar._num(row[ix['prev']]);ref=ar._num(row[ix['ref']]);kind=str(row[ix['kind']]).strip()
        value=ar._num(row[ix['value']]) if ix['value'] is not None else None
        if ix['cash'] is not None:
            explicit=ar._num(row[ix['cash']])
            value=explicit if explicit is not None and (value is None or abs(value-explicit)<=.011) else None
        if not kind:raise ValueError('action type missing')
        # A problematic row is retained and blocks that security, not silently dropped.
        cash=value if kind in ('息','除息') and value is not None and value>=0 and prev and ref and abs(prev-ref-value)<=.011 else None
        if d==day:out.append({'code':c,'kind':kind,'previous_close':prev,'reference':ref,'cash':cash})
    return {'version':VERSION,'market':market,'day':day,'observed_at':now.isoformat(),'source_url':source_url,'range_start':start,'range_end':end,'raw':raw,'rows':out}


def action_path(root,m,d):return Path(root)/'data/h209/actions'/m/(d+'.json.gz')


def read_actions(root,m,d,now):
    v=ar._read(action_path(root,m,d));obs=perf.stamp(v['observed_at'])
    if obs>now or v!=parse_actions(v['raw'],m,d,obs,start=v['range_start'],end=v['range_end'],source_url=v['source_url']):raise ValueError('action provenance mismatch')
    return {r['code']:r for r in v['rows']},obs


def collect_actions(root,market,now,required,*,budget=24,http=None):
    import requests
    http=http or requests.Session();errors={};saved=0;attempted=0
    # Most recent 20 trading days also bootstrap when no research has matured.
    keys=set(required)|{(m,d) for m in URLS for d in market.days[-20:]}
    keys={(m,d) for m,d in keys if d<now.date().isoformat() or (d==now.date().isoformat() and now.time()>=time(18,15))}
    missing=[]
    for m,d in sorted(keys,key=lambda k:(k[1],k[0]),reverse=True):
        try:read_actions(root,m,d,now)
        except FileNotFoundError:missing.append((m,d))
        except (ValueError,TypeError,KeyError,OSError) as e:errors[m+':'+d]=str(e)[:160]
    # rotate failures across runs so a bad endpoint/day cannot starve every other day
    hp=Path(root)/'data/h209/collector.json'
    try:previous=ar._read(hp).get('attempts',{})
    except (OSError,ValueError,TypeError):previous={}
    missing.sort(key=lambda k:previous.get(':'.join(k),''))
    attempts=dict(previous)
    limit=max(0,min(48,budget));remaining=set(missing)
    for m,d in missing:
        if attempted>=limit:break
        if (m,d) not in remaining:continue
        # Query a bounded past week first: on no-event days the official table
        # often omits data entirely. A dated range with rows can attest those days.
        start=max(d[:8]+'01',(date.fromisoformat(d)-timedelta(days=6)).isoformat())
        end=d;key=m+':'+d;attempted+=1
        attempts[key]=now.isoformat()
        try:
            fmt=lambda x:x.replace('-','') if m=='TWSE' else x.replace('-','/')
            def fetch(lo,hi):
                res=http.get(URLS[m],params={'startDate':fmt(lo),'endDate':fmt(hi),'response':'json'},timeout=12);res.raise_for_status()
                raw=res.json();parse_actions(raw,m,d,now,start=lo,end=hi);return raw
            try:raw=fetch(start,end)
            except Exception:
                if start==end or attempted>=limit:raise
                attempted+=1;start=end;raw=fetch(start,end)
            for md,dd in sorted(remaining.copy()):
                if md!=m or not start<=dd<=end:continue
                v=parse_actions(raw,m,dd,now,start=start,end=end);p=action_path(root,m,dd)
                if p.exists():raise ValueError('action archive appeared concurrently; no overwrite')
                ar._write(p,ar._seal(v));read_actions(root,m,dd,now);saved+=1;remaining.remove((m,dd));attempts[m+':'+dd]=now.isoformat()
        except Exception as e:errors[key]=str(e)[:180]
    health={'version':VERSION,'asof':now.isoformat(),'attempted':attempted,'saved':saved,'errors':errors,'attempts':attempts,'pending':len(missing)-saved}
    ar._write(hp,ar._seal(health));return health


def capture_recall(root,market,now):
    """Freeze H208/H198 memberships only on the actual decision evening."""
    day=now.date().isoformat();p=Path(root)/'data/h209/signals'/(day+'.json.gz')
    if p.exists():return '當日已封存'
    if now.time()<time(18,15) or not market.today_confirmed(day):return '等待當日官方盤後證據'
    from godpick_h206_market_gap import build
    from godpick_h208_recall_review import build as recall
    from godpick_h198_stage_review import POOLS as stages
    b=build(root,now);h=recall(root,b,now)
    if b.get('day')!=day or h.get('day')!=day or h.get('status')!='RESEARCH_READY':return 'H208完整研究待補證'
    rows=[]
    for r in h['rows']:
        m,c=r['市場'],r['股票代號'];price=market.quotes(m,day).get(c)
        if not perf.num(price) or not math.isclose(price,r['收盤'],rel_tol=1e-8):raise ValueError('H208 base conflict')
        pools=['H208｜'+r['召回池']]
        if r.get('H198階段') in stages[:3]:pools.append('H198｜'+r['H198階段'])
        rows.append({'signal_day':day,'created_at':now.isoformat(),'market':m,'code':c,'name':r['股票名稱'],'base_close':price,'strategies':pools,'authority':'RESEARCH_ONLY'})
    perf.immutable(p,{'version':VERSION,'day':day,'created_at':now.isoformat(),'rows':rows,'source_hash':perf.digest(h)})
    return f'H208封存 {len(rows)} 檔'


def read_signals(root,now):
    from godpick_h197_strategy_review import memberships
    ss,_=perf.read_ledger(root);out=[]
    for s in ss.values():
        if perf.stamp(s['created_at'])<=now and s['tier'] in perf.TIERS[:3]:
            strategies=sorted(memberships(s))
            from godpick_h197_strategy_review import FIN
            strategies+=['H192｜'+x for x in sorted(set(s['financial_pools'].split('、')) & FIN)]
            if strategies:out.append({**s,'strategies':strategies,'source':'H194'})
    for f in sorted((Path(root)/'data/h209/signals').glob('*.json.gz')):
        v=ar._read(f);obs=perf.stamp(v['created_at']);day=v['day']
        if v['version']!=VERSION or f.name!=day+'.json.gz' or obs.date().isoformat()!=day or obs.time()<time(18,15):raise ValueError('H209 signal manifest mismatch')
        seen=set()
        for s in v['rows']:
            k=perf.sid(s)
            if k in seen or k[0]!=day or s['created_at']!=v['created_at'] or k[1] not in ar.URLS or not re.fullmatch('[1-9][0-9]{3}',k[2]) or s['authority']!='RESEARCH_ONLY' or not perf.num(s['base_close']) or not s['strategies'] or any(not x.startswith(('H198｜','H208｜')) for x in s['strategies']):raise ValueError('H209 signal row mismatch')
            seen.add(k)
            if obs<=now:out.append({**s,'source':'H209'})
    return out


class Evidence:
    def __init__(self,root,market,now):self.root=Path(root);self.market=market;self.now=now;self.bars={};self.events={}
    def quote(self,m,d,c):
        k=m,d
        if k not in self.bars:
            self.bars[k]={}
            if not self.market.errors and d in self.market.market.get(m,{}):
                self.bars[k]=ar.quote_rows(self.root/'data/h190',m,d,self.now)
        return self.bars[k].get(c,(None,None))
    def action(self,m,d,c):
        k=m,d
        if k not in self.events:
            try:self.events[k]=read_actions(self.root,m,d,self.now)
            except (OSError,ValueError,KeyError,TypeError):self.events[k]=(None,None)
        rows,obs=self.events[k];return (rows.get(c) if rows is not None else None),obs


def fee(value):return max(COST['min_commission'],value*COST['commission'])
def drawdown(values):
    peak=values[0];worst=0.
    for v in values:
        peak=max(peak,v);worst=min(worst,v/peak-1)
    return worst


def trade(s,days,e):
    """One hypothetical round lot, entry next open, exit target close. No skip days.
    All event days AFTER entry through exit need explicit coverage. Cash entitlement
    accrues on ex date; no payment-date/reinvestment claim. Complex action excluded
    from cash-net metrics but reference-price-adjusted proxy is kept separately.
    """
    m,c=s['market'],s['code'];bars=[];obs=[perf.stamp(s['created_at'])]
    for d in days:
        r,t=e.quote(m,d,c)
        if not r or t is None or any(not perf.num(x) for x in r[1:7]) or r[2]<max(r[1],r[3],r[4]) or r[3]>min(r[1],r[2],r[4]):return {'status':'行情缺口'}
        if d==days[0] and t>perf.stamp(s['created_at']):return {'status':'研究基準行情晚於決策'}
        bars.append(r);obs.append(t)
    if not math.isclose(bars[0][4],s['base_close'],rel_tol=1e-9):return {'status':'基準價衝突'}
    # No assumed fill for locked/one-price daily bars. Intraday liquidity still unproven.
    if bars[1][2]==bars[1][3] or bars[-1][2]==bars[-1][3]:return {'status':'單一價格／成交待核'}
    raw=bars[-1][4]/bars[1][1]-1;result={'raw_return':raw,'entry_open':bars[1][1],'exit_close':bars[-1][4]}
    n=COST['shares'];buy=bars[1][1]*(1+COST['slippage'])*n;initial=buy+fee(buy);cash=0.;proxy=1.;complex_event=False;marks=[initial];events=[]
    for i in range(1,len(bars)):
        d=days[i];r=bars[i];ev,t=e.action(m,d,c)
        if t is None:return {**result,'status':'除權息涵蓋待補證'}
        obs.append(t)
        if ev:
            events.append(d+':'+ev['kind'])
            prev=ev['previous_close'];ref=ev['reference']
            if not perf.num(prev) or not perf.num(ref) or not math.isclose(prev,bars[i-1][4],rel_tol=1e-8,abs_tol=.005):return {**result,'status':'權息參考價／前收衝突'}
            if i>1:
                proxy*=prev/ref
                if ev['cash'] is None:complex_event=True
                else:cash+=ev['cash']*n
        sell=r[4]*(1-COST['slippage'])*n;marks.append(sell-fee(sell)-sell*COST['sell_tax']+cash)
    result.update(reference_return=(1+raw)*proxy-1,events='、'.join(events),resolved_at=max(obs).isoformat())
    if complex_event:return {**result,'status':'複雜權息／僅參考價代理'}
    if any(x<=0 for x in marks):return {**result,'status':'成本／估值異常'}
    return {**result,'status':'VALID_CASH_MODEL','cash_dividend':cash,'initial_cost':initial,'net_pnl':marks[-1]-initial,'net_return':marks[-1]/initial-1,'trade_mdd':drawdown(marks)}


def stats(rows):
    v=[r['net_return'] for r in rows];wins=sum(x>0 for x in v);loss=sum(-x for x in v if x<0)
    pnl=[r['net_pnl'] for r in rows if 'net_pnl' in r];pnl_loss=sum(-x for x in pnl if x<0)
    return {'有效交易':len(v),'固定千股淨損益合計':sum(pnl) if pnl and len(pnl)==len(v) else None,'現金損益獲利因子':sum(x for x in pnl if x>0)/pnl_loss if pnl_loss and len(pnl)==len(v) else None,'勝率':wins/len(v) if v else None,'平均淨報酬':statistics.mean(v) if v else None,'中位數淨報酬':statistics.median(v) if v else None,'同樣本原始均值':statistics.mean(r['raw_return'] for r in rows) if rows and all('raw_return' in r for r in rows) else None,'同樣本股利應收合計':sum(r['cash_dividend'] for r in rows) if rows and all('cash_dividend' in r for r in rows) else None,'標準化報酬獲利因子':sum(x for x in v if x>0)/loss if loss else None,'最差單筆日收盤回撤':min((r['trade_mdd'] for r in rows),default=None)}


def evaluate(signals,market,e,now):
    detail=[];groups=defaultdict(list);required=set()
    for s in signals:
        for n in HORIZONS:
            ds=market.span(s['signal_day'],n)
            if not ds or ds[-1]>now.date().isoformat() or (ds[-1]==now.date().isoformat() and now.time()<time(18,15)):continue
            required.update((s['market'],d) for d in ds[1:])
            t=trade(s,ds,e)
            for strategy in s['strategies']:
                r={'研究日':s['signal_day'],'市場':s['market'],'股票代號':s['code'],'股票名稱':s['name'],'策略':strategy,'期間':n,'進場日':ds[1],'出場日':ds[-1],'決策時間':s['created_at'],'訊號來源':s['source'],**t};detail.append(r);groups[s['market'],strategy,n].append(r)
    summary=[];folds=[];cohorts=[]
    for (m,strategy,n),rr in sorted(groups.items()):
        daily=defaultdict(list)
        for r in rr:daily[r['研究日']].append(r)
        # Select windows by calendar, NOT by success; missing cohorts stay denominators.
        chosen=[];end=''
        for d,rs in sorted(daily.items()):
            if d<=end:continue
            end=rs[0]['出場日'];valid=[r for r in rs if r['status']=='VALID_CASH_MODEL'];cov=len(valid)/len(rs)
            # No survivor-only compounded return: all cohort positions must be resolved.
            chosen.append({'研究日':d,'出場日':end,'決策時間':min((r['決策時間'] for r in rs),key=perf.stamp),'覆蓋率':cov,'候選':len(rs),'有效':len(valid),'淨報酬':statistics.mean(r['net_return'] for r in valid) if cov==1 else None,'已觀測':max((r['resolved_at'] for r in valid),default='') if cov==1 else ''})
        for x in chosen:cohorts.append({'市場':m,'策略':strategy,'期間':n,**x})
        valid=[r for r in rr if r['status']=='VALID_CASH_MODEL'];vals=[x['淨報酬'] for x in chosen];eq=[1.]
        if vals and all(x is not None for x in vals):
            for x in vals:eq.append(eq[-1]*(1+x))
        summary.append({'市場':m,'策略':strategy,'期間':n,'成熟候選':len(rr),'覆蓋率':len(valid)/len(rr),'非重疊窗':len(chosen),'非重疊完整窗':sum(x is not None for x in vals),'期末再平衡淨報酬':eq[-1]-1 if len(eq)>1 else None,'期末再平衡最大回撤':drawdown(eq) if len(eq)>1 else None,**stats(valid)})
        # Fixed-rule walk-forward: 20 prior windows, 10 unseen windows, no parameter fitting.
        for i in range(20,len(chosen)-9,10):
            train=chosen[i-20:i];test=chosen[i:i+10];cut=perf.stamp(test[0]['決策時間']);ok=[x for x in train if x['淨報酬'] is not None and perf.stamp(x['已觀測'])<cut];hold=[x for x in test if x['淨報酬'] is not None]
            oos_ok=len(ok)==20 and len(hold)==10
            oos_returns=[x['淨報酬'] for x in hold] if oos_ok else []
            equity=[1.]
            for x in oos_returns:equity.append(equity[-1]*(1+x))
            folds.append({'市場':m,'策略':strategy,'期間':n,'訓練開始':train[0]['研究日'],'測試開始':test[0]['研究日'],'測試結束':test[-1]['出場日'],'訓練有效窗':len(ok),'測試有效窗':len(hold),'訓練均值':statistics.mean(x['淨報酬'] for x in ok) if len(ok)==20 else None,'樣本外均值':statistics.mean(x['淨報酬'] for x in hold) if len(ok)==20 and len(hold)==10 else None,'樣本外勝窗率':sum(x>0 for x in oos_returns)/len(oos_returns) if oos_ok else None,'樣本外累積淨報酬':equity[-1]-1 if oos_ok else None,'樣本外期末回撤':drawdown(equity) if oos_ok else None,'狀態':'FIXED_RULE_OOS' if oos_ok else 'DATA_PENDING'})
    return detail,summary,cohorts,folds,required


def run(root=ROOT,now=None,*,update=False,budget=24,http=None):
    now=perf.stamp(now or datetime.now(ar.TZ));root=Path(root);m=perf.Market(root,now);capture='唯讀';health={}
    if update and not m.errors:
        try:capture=capture_recall(root,m,now)
        except (OSError,ValueError,TypeError,KeyError) as ex:capture='H208封存待查：'+str(ex)[:180]
    ss=read_signals(root,now);required={(s['market'],d) for s in ss for n in HORIZONS for d in m.span(s['signal_day'],n)[1:]}
    if update:health=collect_actions(root,m,now,required,budget=budget,http=http)
    rows,summary,cohorts,folds,_=evaluate(ss,m,Evidence(root,m,now),now)
    valid=sum(r['status']=='VALID_CASH_MODEL' for r in rows)
    pending=Counter()
    for s in ss:
        for n in HORIZONS:
            ds=m.span(s['signal_day'],n)
            if not ds or ds[-1]>now.date().isoformat() or (ds[-1]==now.date().isoformat() and now.time()<time(18,15)):
                pending['未成熟／交易日曆缺口']+=len(s['strategies'])
    if not ss:pending['尚無前瞻研究封存']=1
    if m.errors:pending['行情封存驗證異常']=len(m.errors)
    upstream={}
    for name in ('h190/health.json','h194/latest.json.gz','h206/latest.json.gz'):
        try:
            v=ar._read(root/'data'/name);when=v.get('asof',v.get('observed_at'))
            if when and perf.stamp(when)<=now:upstream[name]={k:v[k] for k in ('asof','observed_at','status','capture_status','signals','latest_calendar_session','day','errors') if k in v}
        except (OSError,ValueError,TypeError,KeyError):pass
    out={'version':VERSION,'revision':REVISION,'pending':dict(pending),'upstream':upstream,'source_counts':dict(Counter(s['source'] for s in ss)),'asof':now.isoformat(),'basis':BASIS,'cost':COST,'capture':capture,'signal_count':len(ss),'mature_strategy_rows':len(rows),'valid_rows':valid,'status':'DATA_PENDING' if not valid else 'RESEARCH_ONLY','trade_authority_changed':False,'archive_errors':m.errors,'collector':health,'reasons':dict(Counter(r['status'] for r in rows)),'summary':summary,'folds':folds,'cohorts':cohorts,'detail':rows}
    if update:ar._write(root/REL,ar._seal({k:v for k,v in out.items() if k not in ('detail','cohorts')}))
    return out


def excel(r):
    from openpyxl import Workbook
    from openpyxl.styles import Font,PatternFill,Alignment
    from openpyxl.utils import get_column_letter
    w=Workbook();w.remove(w.active)
    def safe(x):
        if isinstance(x,(dict,list,tuple)):x=json.dumps(x,ensure_ascii=False)
        return "'"+x if isinstance(x,str) and x.startswith(('=','+','-','@','\t','\r')) else x
    for name,rows in [('研究摘要',[{k:v for k,v in r.items() if k not in ('detail','cohorts','folds','summary')}]),('策略績效',r['summary']),('滾動樣本外',r['folds']),('非重疊研究窗',r['cohorts']),('逐筆研究',r['detail'])]:
        ws=w.create_sheet(name);cols=list(dict.fromkeys(k for x in rows for k in x)) or ['狀態'];ws.append(cols)
        for x in rows:ws.append([safe(x.get(k)) for k in cols])
        if not rows:ws.append(['尚無成熟證據'])
        for cell in ws[1]:cell.font=Font(bold=True,color='FFFFFF');cell.fill=PatternFill('solid',fgColor='17365D')
        for i,k in enumerate(cols,1):ws.column_dimensions[get_column_letter(i)].width=min(40,max(14,len(k)*2+3))
        ws.freeze_panes='C2';ws.auto_filter.ref=ws.dimensions
        percent_cols={'覆蓋率','淨報酬','勝率','平均淨報酬','中位數淨報酬','同樣本原始均值','期末再平衡淨報酬','期末再平衡最大回撤','最差單筆日收盤回撤','訓練均值','樣本外均值','樣本外勝窗率','樣本外累積淨報酬','樣本外期末回撤','raw_return','reference_return','net_return','trade_mdd'}
        for i,k in enumerate(cols,1):
            if k in percent_cols:
                for cells in ws.iter_cols(min_col=i,max_col=i,min_row=2):
                    for cell in cells:cell.number_format='0.00%;[Red]-0.00%' 
    b=io.BytesIO();w.save(b);return b.getvalue()

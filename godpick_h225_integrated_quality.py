# -*- coding: utf-8 -*-
"""H225 consolidated evidence, read-only; no H126, score or archive writes.
Distinct research sources are not independent votes. H224 outcomes are hypothetical.
"""
from collections import Counter, defaultdict
from datetime import datetime, date, time
from pathlib import Path
import io, math, statistics
import godpick_h190_market_archive as ar
import godpick_h194_performance as p
import godpick_h195_command_center as command
import godpick_h209_rolling_backtest as back
import godpick_h218_evidence as evidence
import godpick_h219_first_review as first
import godpick_h220_growth_rotation as rotation
import godpick_h221_evidence_closure as closure
import godpick_h223_growth_scenarios as scenarios
import godpick_h224_growth_resonance as resonance
VERSION='V191-H225-INTEGRATED-QUALITY'
ROOT=Path(__file__).resolve().parent
LIMIT=10
SOURCES=('H195','H220','H223','H224')

def day_for(m,now):
    return max((d for d in m.days if d<now.date().isoformat() or (d==now.date().isoformat() and m.today_confirmed(d))),default='') if not m.errors else ''

def same_price(v,expected):
    return bool(p.num(v) and p.num(expected) and math.isclose(float(v),float(expected),rel_tol=1e-9,abs_tol=1e-8))

def snapshots(root,m,now):
    day=day_for(m,now);items={};health=[];errors=[];hist=[]
    for src in SOURCES:
        try:
            if not day:raise ValueError('official session unavailable')
            if src=='H195':
                body,state,_=command.load(root,now)
                if state!='CURRENT' or body.get('market_day')!=day or body.get('expected_session')!=day or body.get('same_session') is not True or body.get('technical_current') is not True:
                    health.append({'來源':src,'狀態':'NOT_CURRENT','具名':None});continue
                # Full rows for counterevidence, strict manager shortlist for priority.
                allrows=rotation.indexed(body['rows']);
                if dict(Counter(r['研究分級'] for r in allrows.values()))!={k:v for k,v in body['counts'].items() if v}:raise ValueError('H195 tier count mismatch')
                short,_=first.manager_rows(body,day);shortkeys={rotation.key(r) for r in short}
                items[src]={k:{**r,'精簡技術入選':k in shortkeys} for k,r in allrows.items()}
            else:
                reader={'H220':rotation.read_history,'H223':scenarios.read_history,'H224':resonance.history}[src]
                history=reader(root,now)
                if src=='H224':hist=history
                match=next((v for v in history if v['day']==day),None)
                if match is None:
                    health.append({'來源':src,'狀態':'NO_SAME_DAY_FREEZE','具名':None});continue
                items[src]=rotation.indexed(match['ranked'] if src=='H223' else match['rows'])
            health.append({'來源':src,'狀態':'VERIFIED_SOURCE','具名':len(items[src])})
        except Exception as exc:
            errors.append(src+': '+type(exc).__name__+': '+str(exc)[:150]);health.append({'來源':src,'狀態':'INVALID_OR_UNAVAILABLE','具名':None})
    return day,items,health,hist,errors

def consolidate(items,m,day):
    """At most one identity per row; conflicting evidence never promotes a candidate."""
    out=[];counts=Counter();keys=set().union(*(set(x) for x in items.values())) if items else set()
    g_rank={k:i for i,k in enumerate(items.get('H220',{}),1)}
    for market,code in sorted(keys):
        k=market,code;a,g,f,r=(items.get(src,{}).get(k,{}) for src in SOURCES)
        px=m.quotes(market,day).get(code);names={str(x.get('股票名稱','')).strip() for x in (a,g,f,r) if x.get('股票名稱')};name=next(iter(names)) if len(names)==1 else ''
        conflicts=[]
        if len(names)>1:conflicts.append('名稱不一致')
        if g and not same_price(g.get('原始收盤'),px):conflicts.append('H220基準價衝突')
        if r.get('H220財報交叉一致') is True and (not g or not same_price(r.get('H220原始收盤'),px) or not same_price(g.get('原始收盤'),px)):
            conflicts.append('H224技術交集未重驗')
        if r.get('H224分層') in resonance.ORDER[:2] and (not g or g.get('籌碼分組')!=rotation.FULL or r.get('籌碼分組')!=rotation.FULL or any(not r.get(k) or r[k]!=g.get(k) for k in ('法人來源雜湊','TDCC來源雜湊'))):conflicts.append('籌碼封存雜湊衝突')
        if (a.get('精簡技術入選') or a.get('研究分級') in p.TIERS[:2]) and not same_price(a.get('官方原始收盤價'),px):conflicts.append('H195基準價衝突')
        for x,y in ((f,r),(g,r),(g,f)):
            if x and y:
                for field in ('營收年月','營收年增%','TTM EPS重建'):
                    if x.get(field) is not None and y.get(field) is not None and x[field]!=y[field]:conflicts.append(field+'衝突')
        risk=bool(a and (a.get('H198階段')=='突破反證與追高警示' or (a.get('H211反證') not in (None,'','未觸發本規則（非無風險）'))))
        if conflicts or risk:group='證據衝突／反證待核'
        elif r and r.get('H224分層') in resonance.ORDER[:2] and r.get('H220財報交叉一致') is True and g:group='財技籌碼共振'
        elif g:group='高成長起漲研究'
        elif a.get('精簡技術入選'):group='技術優先複核'
        elif r or f:group='財報成長複核'
        else:group='原研究保留／未入精簡'
        counts[group]+=1
        out.append({'市場':market,'股票代號':code,'股票名稱':name,'研究分層':group,'來源':'、'.join(src for src in SOURCES if k in items.get(src,{})),'官方收盤':px,'資料日':day,'H224分層':r.get('H224分層'),'H195研究分級':a.get('研究分級'),'H195研究原序':a.get('研究順序'),'H198階段':g.get('H198階段') or a.get('H198階段'),'籌碼分組':r.get('籌碼分組') or g.get('籌碼分組'),'營收加速度pp':r.get('營收加速度pp'),'營收年增%':(r or f or g or a).get('營收年增%'),'TTM EPS重建':(r or f or g or a).get('TTM EPS重建'),'官方PE':(r or f or g or a).get('官方PE'),'原研究順位':r.get('H224研究順位') or g_rank.get(k) or a.get('H211研究序') or f.get('分市場研究順位'),'反證／衝突':'；'.join(sorted(set(conflicts)))+('；H195反證待核' if risk else ''),'用途':'研究複核／非正式買進'})
    order=['財技籌碼共振','高成長起漲研究','技術優先複核','財報成長複核','證據衝突／反證待核','原研究保留／未入精簡']
    out.sort(key=lambda r:(r['市場'],order.index(r['研究分層']),r['原研究順位'] if first.finite(r['原研究順位']) else math.inf,r['股票代號']))
    used=Counter();short=[]
    for r in out:
        if r['研究分層'] not in order[:4]:continue
        market=r['市場'];used[market]+=1
        if used[market]<=LIMIT:short.append({**r,'主管研究序':used[market]})
    return out,short,dict(counts)

def mature(history,m,ev,now,h220_history=()):
    detail=[];groups=defaultdict(list)
    early={v['day']:(p.stamp(v['created_at']),rotation.indexed(v['rows'])) for v in h220_history}
    for v in history:
        for r in v['rows']:
            eligible=bool(r.get('H220財報交叉一致') is True and p.num(r.get('H220原始收盤')))
            ts,prior=early.get(v['day'],(None,{}));g=prior.get(rotation.key(r),{})
            corroborated=bool(eligible and ts and ts<=p.stamp(v['created_at']) and g and same_price(g.get('原始收盤'),r.get('H220原始收盤')) and all(g.get(k)==r.get(k) for k in ('營收年月','營收年增%','TTM EPS重建')) and g.get('連兩季三率三升') is r.get('連兩季三率年增') and g.get('獲利突破') is r.get('獲利突破'))
            for n in p.HORIZONS:
                ds=evidence.due_span(m,v['day'],n,now);ok=False;x=None
                if ds and ds[-1]==now.date().isoformat() and not m.today_confirmed(ds[-1]):ds=[]
                state='財報複核／無封存技術基準' if not eligible else '未到期／日曆待核'
                if eligible and ds and not corroborated:state='H220原始交集待重驗'
                elif corroborated and ds and not m.errors and (ds[-1]!=now.date().isoformat() or m.today_confirmed(ds[-1])):
                    t=back.trade({'market':r['市場'],'code':r['股票代號'],'created_at':v['created_at'],'base_close':r['H220原始收盤']},ds,ev);state=t['status'];x=t.get('net_return');obs=t.get('resolved_at')
                    ok=bool(state=='VALID_CASH_MODEL' and first.finite(x) and x>-1 and obs and p.stamp(v['created_at'])<p.stamp(obs)<=now and p.stamp(obs).date().isoformat()>=ds[-1])
                    if state=='VALID_CASH_MODEL' and not ok:state='淨績效時點或數值異常'
                line={'市場':r['市場'],'股票代號':r['股票代號'],'股票名稱':r['股票名稱'],'研究日':v['day'],'H224分層':r['H224分層'],'期間':n,'出場日':ds[-1] if ds else None,'可模擬交易':eligible,'淨報酬':x if ok else None,'狀態':state}
                detail.append(line);groups[(r['市場'],v['day'],r['H224分層'],n)].append(line)
    summary=[]
    for (market,d,tier,n),rr in sorted(groups.items()):
        due=[r for r in rr if r['可模擬交易'] and r['出場日']];valid=[r['淨報酬'] for r in due if r['淨報酬'] is not None];full=bool(due and len(valid)==len(due) and not m.errors)
        summary.append({'市場':market,'研究日':d,'H224分層':tier,'期間':n,'封存':len(rr),'可模擬交易':sum(r['可模擬交易'] for r in rr),'到期':len(due),'有效':len(valid),'平均淨報酬':statistics.mean(valid) if full else None,'淨勝率':statistics.mean(x>0 for x in valid) if full else None,'狀態':'DESCRIPTIVE_ONLY' if full else 'DATA_PENDING'})
    return detail,summary

def gate(label,valid,total,action,market='雙市場',*,ready=True,at_least=False):
    ok=ready and type(total) is int and total>0 and type(valid) is int and (valid>=total if at_least else valid==total)
    return {'項目':label,'市場':market,'有效':valid,'應有':total,'狀態':'PASS' if ok else 'PENDING','下一步':action}

def fingerprint(root):
    return (ar._archive_manifest(Path(root)/'data/h190')[1],rotation.source_fingerprint(root),scenarios.signature(root),evidence.fingerprint(root),tuple((str(f),f.stat().st_size,f.stat().st_mtime_ns,f.stat().st_ino) for f in sorted([*Path(root,'data/h224/daily').glob('*.json.gz'),*Path(root,'data/h195').glob('*.json.gz')])))

def run(root=ROOT,now=None,*,include_detail=False):
    root=Path(root);now=p.stamp(now or datetime.now(ar.TZ));sig=fingerprint(root);m=p.Market(root,now);day,items,source,history,errors=snapshots(root,m,now)
    allrows,short,counts=consolidate(items,m,day);review=first.run(root,now);c=closure.inspect(root,now);scope=scenarios.audit(root,now);
    from godpick_h191_opportunity_engine import run as technical
    tech=technical(root,now=now,top=100);
    ss,oo=p.read_ledger(root);signals=[{**s,'source':'H194'} for s in ss.values() if p.stamp(s['created_at'])<=now]+[s for s in back.read_signals(root,now,ledger=(ss,oo)) if s['source']=='H209']
    h224_signals=[{'market':r['市場'],'signal_day':v['day'],'source':'H224'} for v in history for r in v['rows'] if r.get('H220財報交叉一致') is True and p.num(r.get('H220原始收盤'))]
    action_rows,actions=evidence.audit_actions(root,m,signals+h224_signals,now)
    gates=[]
    # Health collector status is not historical completeness; keep actual observation time.
    health=c['financial_health'];fresh=False
    try:fresh=p.stamp(health['checked_at'])<=now and (now-p.stamp(health['checked_at'])).total_seconds()<=36*3600
    except (KeyError,TypeError,ValueError):pass
    for market in ar.URLS:
        src=health.get('sources',{});n=sum(fresh and src.get(k+'_'+market,{}).get('status')=='OK' for k in ('revenue','margin','ytd','pe'))
        gates.append(gate('最新財報來源健康（非逐股完整）',n,4,'H192 更新及來源診斷',market))
        rs=[r for r in scope if r['市場']==market and r['必要範圍']];gates.append(gate('歷史財報分期獨立重驗',sum(r['狀態']=='VERIFIED' for r in rs),len(rs),'H222 回補及原始HTML重驗',market))
        flows=[r for r in c['flows'] if r['市場']==market];gates.append(gate('法人最近5日',sum(r['狀態']=='已核實' for r in flows),5,'H221 官方法人缺日',market))
        tc=tech.get('coverage',{}).get(market,{});den=tc.get('total');need=(den*95+99)//100 if type(den) is int and den>0 else None
        gates.append(gate('60日原始技術覆蓋95%門檻',tc.get('valid'),need,'H212 個股行情缺口（原始未還原）',market,ready=not m.errors and not tech.get('archive_errors') and tech.get('asof')==day and tech.get('calendar_fresh') is True,at_least=True))
        rr=next((r for r in review['recall_gaps'] if r['市場']==market),{});gates.append(gate('H208召回品質95%門檻',rr.get('有效60日'),rr.get('95%所需'),'H212 缺口修復／H208 召回',market,ready=not m.errors,at_least=True))
        ac=next((r for r in actions if r['市場']==market and r['範圍']=='60日技術窗'),{});gates.append(gate('權息60日窗獨立重驗',ac.get('有效'),59,'H209 權息補證',market,ready=ac.get('狀態')=='VERIFIED_SCOPE'))
    gates.append(gate('同日研究來源',sum(r['狀態']=='VERIFIED_SOURCE' for r in source),4,'依既有工作流封存；不事後補造'))
    try:early=rotation.read_history(root,now)
    except Exception as exc:early=[];errors.append('H220 history: '+str(exc)[:160])
    detail,maturity=mature(history,m,back.Evidence(root,m,now),now,early)
    errors+=list(m.errors)+review['errors']+c['errors']+[r['原因'] for r in scope if r['狀態']=='INVALID']
    extra={}
    if include_detail:
        from godpick_h212_data_recovery import gaps
        recovery=gaps(root,now,technical=tech)
        extra={'first_errors':list(m.errors),'first_detail':review['first_detail'],'first':review['first'],'miss':review['miss'],'flows':c['flows'],'action_rows':action_rows,'recovery':recovery,'financial_health':health,'first_signals':[s for s in signals if s['source']=='H194' and s['signal_day']==review['counts']['首批研究日']]}
    if sig!=fingerprint(root):raise RuntimeError('H225 source changed during review; retry')
    return {**extra,'version':VERSION,'asof':now.isoformat(),'day':day,'status':'DATA_GATES_PASS' if gates and all(r['狀態']=='PASS' for r in gates) and not errors else 'EVIDENCE_PENDING','gates':gates,'sources':source,'rows':allrows,'shortlist':short,'counts':counts,'first_counts':review['counts'],'recall':review['recall'],'recall_gaps':review['recall_gaps'],'financial_periods':c['financial'],'scope':scope,'actions':actions,'maturity':maturity,'detail':detail,'errors':errors,'trade_authority_changed':False,'weight_multiplier':1.0}

def excel(r):
    from openpyxl import Workbook
    w=Workbook();w.remove(w.active)
    for name,rs in [('01_主管總覽',[{'資料日':r['day'],'狀態':r['status'],'研究分層':r['counts'],**r['first_counts'],'正式買進':'僅第7頁原H126授權','權重倍率':1.0}]),('02_整合研究',r['shortlist']),('03_資料驗收',r['gates']),('04_來源狀態',r['sources']),('05_全量整合',r['rows']),('06_首批召回',r['recall']),('07_H224成熟績效',r['maturity']),('08_H224逐筆',r['detail']),('09_財報期間',r['financial_periods']),('10_歷史財報',r['scope']),('11_權息驗收',r['actions']),('12_異常',[{'錯誤':x} for x in r['errors']])]:first.table(w,name,rs)
    f=io.BytesIO();w.save(f);return f.getvalue()

def append_manager(w,root,research_day,comparison=None,now=None):
    """One front page; no heavy historical performance calculation on export path."""
    from openpyxl.styles import Font,PatternFill,Alignment
    from openpyxl.utils import get_column_letter
    now=p.stamp(now or datetime.now(ar.TZ));m=p.Market(root,now);day,items,source,_,errors=snapshots(root,m,now)
    try:rd=date.fromisoformat(str(research_day)[:10]).isoformat()
    except (TypeError,ValueError):rd=''
    matched=bool(rd and day==rd);allrows,short,counts=consolidate(items,m,day) if matched else ([],[],{})
    comp=comparison or {};verified=bool(matched and comp.get('day')==day and comp.get('market_day')==day and comp.get('same_run_verified') is True and comp.get('complete_scope') is True and type(comp.get('h126_buyable')) is int and comp['h126_buyable']>=0)
    ws=w.create_sheet('00_H225主管總覽',0)
    ws.append(['H225 主管決策索引（研究不等於正式買進）']);ws.append(['原推薦資料日',rd or '待核','官方研究日',day or '待核']);ws.append(['本輪H126授權數',comp['h126_buyable'] if verified else '未核實','權威來源','00_H126主管決策（原表不變）']);ws.append(['研究來源','；'.join(f"{r['來源']}:{r['狀態']}({r['具名'] if r['具名'] is not None else '待核'})" for r in source),'研究狀態','同日獨立封存；非原掃描同輪']);ws.append(['使用順序','先查原H126；再查下列研究；詳表及原主管榜均保留','原輪母體完整', '已核實' if verified else '待核']);ws.append(['研究篩選','每市場最多10檔，不湊數；有衝突或反證不列前段', '異常',command.safe(errors)]);ws.append(['市場','股票名稱','股票代號','研究分層','來源','資料日','官方收盤','營收加速度pp','TTM EPS重建','官方PE','用途'])
    for r in short:ws.append([command.safe(r.get(k)) for k in ('市場','股票名稱','股票代號','研究分層','來源','資料日','官方收盤','營收加速度pp','TTM EPS重建','官方PE','用途')])
    if not short:ws.append(['無同日具名精簡研究／待證；不代表市場無機會'])
    last_short=ws.max_row
    # Retain a small, separately labelled sample of original technical research requiring evidence.
    pending=[r for r in allrows if r['研究分層']=='原研究保留／未入精簡' and r['H195研究分級'] in p.TIERS[:2]]
    if pending:
        ws.append(['原H195技術研究補證（不是前段研究／不是授權）']);ws.append(['市場','股票名稱','股票代號','研究分級','H198階段','資料日','官方收盤','補證原因'])
        c=Counter()
        for r in sorted(pending,key=lambda x:(x['市場'],x['H195研究原序'] if first.finite(x['H195研究原序']) else math.inf,x['股票代號'])):
            c[r['市場']]+=1
            if c[r['市場']]<=5:ws.append([command.safe(r.get(k)) for k in ('市場','股票名稱','股票代號','H195研究分級','H198階段','資料日','官方收盤')]+['未通過H219完整前段證據'])
    for i,width in enumerate([28,25,15,30,32,16,16,20,18,15,35],1):ws.column_dimensions[get_column_letter(i)].width=width
    for row in (1,7):
        for cell in ws[row]:cell.font=Font(color='FFFFFF',bold=True);cell.fill=PatternFill('solid',fgColor='185C70');cell.alignment=Alignment(wrap_text=True)
    for row in (4,5,6):
        for cell in ws[row]:cell.alignment=Alignment(wrap_text=True)
        ws.row_dimensions[row].height=42
    ws.freeze_panes='D8';ws.auto_filter.ref=f'A7:K{last_short}';return ws

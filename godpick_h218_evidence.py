# -*- coding: utf-8 -*-
"""H218: read-only independent acceptance; never reconstruct historical decisions.
Price labels and hypothetical net returns are separate. No production weight writes.
"""
from collections import Counter, defaultdict
from datetime import datetime, time
from pathlib import Path
import math, statistics, json, io
import godpick_h190_market_archive as ar
import godpick_h194_performance as p
import godpick_h209_rolling_backtest as b

VERSION='V191-H218-EVIDENCE-ACCEPTANCE'
ROOT=Path(__file__).resolve().parent
TRAIN,HOLD,MIN_GROUP=20,10,3
EDGE,WIN=.005,.6

def due_span(m,day,n,now):
    ds=m.span(day,n)
    if not ds or len(ds)!=n+1 or ds[-1]>now.date().isoformat() or (ds[-1]==now.date().isoformat() and now.time()<time(18,15)):return []
    return ds

def scope(m,signals,now):
    days=[d for d in m.days if d<now.date().isoformat() or (d==now.date().isoformat() and now.time()>=time(18,15))]
    base={(x,d) for x in ar.URLS for d in days[-60:][1:]}
    required={(s['market'],d) for s in signals if s['source']!='H194' or s.get('tier') in p.TIERS[:3] for n in p.HORIZONS for d in due_span(m,s['signal_day'],n,now)[1:]}
    return base,required,days

def audit_actions(root,m,signals,now):
    base,required,days=scope(m,signals,now);rows=[];summary=[]
    # Entire scope, not just collector.pending. Old invalid files remain intact.
    for market,d in sorted(base|required):
        state='VERIFIED';obs=None;events=None;err=''
        try:
            rr,t=b.read_actions(root,market,d,now);events=len(rr);obs=t.isoformat()
        except FileNotFoundError:state='MISSING';err='尚無官方封存'
        except (OSError,ValueError,KeyError,TypeError,OverflowError) as e:state='INVALID';err=str(e)[:180]
        rows.append({'市場':market,'交易日':d,'60日技術窗':(market,d) in base,'成熟績效需求':(market,d) in required,'狀態':state,'官方事件數':events,'觀測時間':obs,'原因':err})
    for market in ar.URLS:
        for label,flag in [('60日技術窗','60日技術窗'),('成熟績效需求','成熟績效需求'),('聯集',None)]:
            rs=[r for r in rows if r['市場']==market and (flag is None or r[flag])];c=Counter(r['狀態'] for r in rs);n=len(rs);full=(len(days)>=60 and m.span(days[-60],59)==days[-60:] and not m.errors) if label!='成熟績效需求' else not m.errors
            summary.append({'市場':market,'範圍':label,'應驗證日數':n,'有效':c['VERIFIED'],'明確零事件日':sum(r['狀態']=='VERIFIED' and r['官方事件數']==0 for r in rs),'缺檔':c['MISSING'],'驗證異常':c['INVALID'],'覆蓋率':c['VERIFIED']/n if n else None,'狀態':'VERIFIED_SCOPE' if full and n and c['VERIFIED']==n else ('NO_DUE_SCOPE' if not n and label=='成熟績效需求' else 'DATA_PENDING')})
    return rows,summary

def reconcile(signals,ledger,m,e,now):
    details=[];groups=defaultdict(list)
    for s in signals:
        src=s['source'];d=s['signal_day'];k=p.sid(s)
        for n in p.HORIZONS:
            ds=due_span(m,d,n,now);o=ledger.get((*k,n)) if src=='H194' else None
            if o and p.stamp(o['resolved_at'])>now:o=None
            r={'訊號來源':src,'研究日':d,'市場':s['market'],'股票代號':s['code'],'股票名稱':s['name'],'研究分級':s.get('tier',''),'期間':n,'決策時間':s['created_at'],'出場日':ds[-1] if ds else None,'原始收盤績效':None,'原始績效狀態':'不適用' if src!='H194' else '未成熟／日曆待核','淨績效狀態':'未成熟／日曆待核','淨報酬':None,'結果觀測時間':None}
            if ds and not m.errors:
                if src=='H194':
                    r['原始績效狀態']='成熟待結算'
                    if o:
                        # Check ledger target against official calendar AND official close.
                        px=m.quotes(s['market'],ds[-1]).get(s['code']);base=m.quotes(s['market'],d).get(s['code'])
                        if o['target_day']==ds[-1] and px is not None and base is not None and math.isclose(px,o['target_close'],rel_tol=1e-9) and math.isclose(base,s['base_close'],rel_tol=1e-9):
                            r.update(原始績效狀態='VERIFIED_RAW',原始收盤績效=o['raw_return'])
                        else:r['原始績效狀態']='結算／官方基準衝突'
                if src=='H194' and s.get('tier') not in p.TIERS[:3]:r['淨績效狀態']='觀察級不納入淨績效'
                else:
                    t=b.trade(s,ds,e);r['淨績效狀態']=t['status']
                    if t['status']=='VALID_CASH_MODEL':
                        v=t.get('net_return');obs=p.stamp(t['resolved_at'])
                        if isinstance(v,(int,float)) and math.isfinite(v) and v>-1 and p.stamp(s['created_at'])<obs<=now and obs.date().isoformat()>=ds[-1]:r.update(淨報酬=v,結果觀測時間=obs.isoformat())
                        else:r['淨績效狀態']='淨績效時點或數值異常'
            elif m.errors:r['淨績效狀態']='行情封存異常'
            if o and not ds:r['原始績效狀態']='結算／交易日待核'
            details.append(r);groups[(src,s['market'],n)].append(r)
    summary=[]
    for (src,mkt,n),rs in sorted(groups.items()):
        due=[r for r in rs if r['出場日']];eligible=[r for r in due if src!='H194' or r['研究分級'] in p.TIERS[:3]];valid=[r for r in eligible if r['淨績效狀態']=='VALID_CASH_MODEL' and r['淨報酬'] is not None];raw=[r for r in due if r['原始績效狀態']=='VERIFIED_RAW'];full=bool(eligible and len(valid)==len(eligible) and not m.errors)
        summary.append({'訊號來源':src,'市場':mkt,'期間':n,'已封存':len(rs),'到期候選':len(due),'未到期或日曆待核':len(rs)-len(due),'原價有效':len(raw) if src=='H194' else None,'淨績效應結算':len(eligible),'淨績效有效':len(valid),'淨績效待核':len(eligible)-len(valid),'淨績效覆蓋率':len(valid)/len(eligible) if eligible else None,'平均淨報酬':statistics.mean(r['淨報酬'] for r in valid) if full else None,'淨勝率':statistics.mean(float(r['淨報酬']>0) for r in valid) if full else None,'狀態':'DESCRIPTIVE_ONLY' if full else 'DATA_PENDING'})
    return details,summary

def source_pairs(detail):
    by=defaultdict(lambda:defaultdict(dict));out=[]
    for r in detail:
        if not r['出場日'] or (r['訊號來源']=='H194' and r['研究分級'] not in p.TIERS[:3]):continue
        g=by[(r['市場'],r['期間'],r['研究日'])][r['訊號來源']];c=r['股票代號']
        if c in g:raise ValueError('duplicate H218 source identity')
        g[c]=r
    for (m,n,d),g in sorted(by.items()):
        a=g['H194'];b_=g['H209'];shared=set(a)&set(b_);left=[a[c] for c in sorted(set(a)-shared)];right=[b_[c] for c in sorted(set(b_)-shared)];rs=list(a.values())+list(b_.values());end={r['出場日'] for r in rs}
        if len(end)!=1:raise ValueError('H218 conflicting target dates')
        # No independent-source inference if one source absent, common holdings excluded.
        ok=len(left)>=MIN_GROUP and len(right)>=MIN_GROUP and all(r['淨績效狀態']=='VALID_CASH_MODEL' and r['淨報酬'] is not None and r['結果觀測時間'] for r in left+right)
        av=statistics.mean(r['淨報酬'] for r in left) if ok else None;bv=statistics.mean(r['淨報酬'] for r in right) if ok else None
        out.append({'市場':m,'期間':n,'研究日':d,'出場日':next(iter(end)),'決策時間':min(r['決策時間'] for r in rs),'結果觀測時間':max(r['結果觀測時間'] for r in left+right) if ok else None,'H194獨有':len(left),'H209獨有':len(right),'共同股票':len(shared),'H194獨有均值':av,'H209獨有均值':bv,'H209相對差':bv-av if ok else None,'狀態':'DESCRIPTIVE_ONLY' if ok else 'DATA_PENDING'})
    return out

def review(pairs):
    by=defaultdict(list);out=[]
    for r in pairs:by[(r['市場'],r['期間'])].append(r)
    for (m,n),rs in sorted(by.items()):
        chosen=[];end=''
        for r in sorted(rs,key=lambda x:x['研究日']):
            if r['研究日']>end:chosen.append(r);end=r['出場日']
        a=chosen[-(TRAIN+HOLD):-HOLD] if len(chosen)>=TRAIN+HOLD else [];h=chosen[-HOLD:] if a else [];cut=p.stamp(h[0]['決策時間']) if h else None
        def good(r):return r['狀態']=='DESCRIPTIVE_ONLY' and r['H209相對差'] is not None and r['結果觀測時間']
        aa=[r for r in a if good(r) and p.stamp(r['結果觀測時間'])<cut];hh=[r for r in h if good(r)]
        av=statistics.mean(r['H209相對差'] for r in aa) if len(aa)==TRAIN else None;hv=statistics.mean(r['H209相對差'] for r in hh) if len(hh)==HOLD else None;state='BASELINE_ONLY'
        if av is not None and hv is not None:
            state='NO_CONFIRMED_EDGE'
            for sign,name in [(1,'REVIEW_H209_EDGE'),(-1,'REVIEW_H194_EDGE')]:
                if sign*av>=EDGE and sign*hv>=EDGE and all(statistics.mean(float(sign*r['H209相對差']>0) for r in rr)>=WIN for rr in (aa,hh)):state=name
        out.append({'市場':m,'期間':n,'非重疊窗':len(chosen),'訓練有效':len(aa),'留出有效':len(hh),'訓練差':av,'留出差':hv,'狀態':state,'權重倍率':1.0})
    return out

def fingerprint(root):
    paths=[]
    for rel in ('data/h194/signals','data/h194/outcomes','data/h209/signals','data/h209/actions'):
        paths.extend(Path(root,rel).rglob('*.json.gz'))
    return tuple((str(f),f.stat().st_size,f.stat().st_mtime_ns,f.stat().st_ino) for f in sorted(paths))

def run(root=ROOT,now=None):
    root=Path(root);now=p.stamp(now or datetime.now(ar.TZ));sig=fingerprint(root);market_sig=ar._archive_manifest(root/'data/h190')[1];m=p.Market(root,now);ss,oo=p.read_ledger(root)
    h194=[{**s,'source':'H194'} for s in ss.values() if p.stamp(s['created_at'])<=now];h209=[s for s in b.read_signals(root,now,ledger=(ss,oo)) if s['source']=='H209'];signals=h194+h209
    action_rows,action_summary=audit_actions(root,m,signals,now);detail,summary=reconcile(signals,oo,m,b.Evidence(root,m,now),now);pairs=source_pairs(detail);reviews=review(pairs)
    # Same-date recall readiness is a diagnostic only, never backdate a missing partition.
    day=max((d for d in m.days if d<now.date().isoformat() or (d==now.date().isoformat() and now.time()>=time(18,15))),default='');recall={}
    if day and not m.errors:
        from godpick_h206_market_gap import build as gap
        from godpick_h208_recall_review import build as recall_build
        h=recall_build(root,gap(root,now),now);recall={'資料日':h.get('day'),'H208狀態':h.get('status'),'覆蓋':h.get('coverage'),'原因':h.get('reasons'),'錯誤':h.get('errors'),'該日H209具名封存':sum(s['signal_day']==day for s in h209),'口徑':'現時診斷，非當時漏選證據'}
    if fingerprint(root)!=sig or ar._archive_manifest(root/'data/h190')[1]!=market_sig:raise RuntimeError('H218 evidence changed during audit; retry')
    counts={'H194封存':len(h194),'H209封存':len(h209),'到期樣本':sum(r['出場日'] is not None for r in detail),'原價有效':sum(r['原始績效狀態']=='VERIFIED_RAW' for r in detail),'淨績效有效':sum(r['淨績效狀態']=='VALID_CASH_MODEL' for r in detail),'未到期或日曆待核':sum(r['出場日'] is None for r in detail),'可比較來源窗':sum(r['狀態']=='DESCRIPTIVE_ONLY' for r in pairs)}
    return {'version':VERSION,'asof':now.isoformat(),'day':day,'counts':counts,'action_summary':action_summary,'action_rows':action_rows,'summary':summary,'detail':detail,'pairs':pairs,'review':reviews,'recall':recall,'errors':list(m.errors),'status':'DESCRIPTIVE_ONLY' if counts['淨績效有效'] and not m.errors else 'DATA_PENDING','weight_multiplier':1.0,'trade_authority_changed':False}

def excel(r):
    from openpyxl import Workbook
    from openpyxl.styles import Font,PatternFill
    from openpyxl.utils import get_column_letter
    w=Workbook();w.remove(w.active)
    tables=[('01_主管驗收',[{'版本':r['version'],'觀測時間':r['asof'],'資料日':r['day'],'狀態':r['status'],**r['counts'],'權重倍率':1.0,'錯誤':r['errors']}]),('02_權息驗收',r['action_summary']),('03_權息逐日',r['action_rows']),('04_成熟對帳',r['summary']),('05_成熟逐筆',r['detail']),('06_來源優勢',r['pairs']),('07_樣本外複核',r['review']),('08_召回封存診斷',[r['recall']] if r['recall'] else [])]
    for name,rows in tables:
        s=w.create_sheet(name);cols=list(dict.fromkeys(k for row in rows for k in row)) or ['狀態'];s.append(cols)
        for rownum,row in enumerate(rows or [{'狀態':'尚無資料／非零勝率'}],2):
            for i,k in enumerate(cols,1):
                x=row.get(k);x=json.dumps(x,ensure_ascii=False) if isinstance(x,(list,dict)) else x;c=s.cell(rownum,i,x)
                if isinstance(x,str):c.value=x[:32000];c.data_type='s'
        for i,k in enumerate(cols,1):
            c=s.cell(1,i);c.fill=PatternFill('solid',fgColor='174765');c.font=Font(bold=True,color='FFFFFF');s.column_dimensions[get_column_letter(i)].width=46 if any(x in k for x in ('原因','錯誤','覆蓋','狀態')) else 19
            if k in ('覆蓋率','淨績效覆蓋率','平均淨報酬','淨勝率','原始收盤績效','淨報酬','H194獨有均值','H209獨有均值','H209相對差','訓練差','留出差'):
                for c in list(s.columns)[i-1][1:]:c.number_format='0.00%'
        s.freeze_panes='C2';s.auto_filter.ref=s.dimensions
    buf=io.BytesIO();w.save(buf);return buf.getvalue()

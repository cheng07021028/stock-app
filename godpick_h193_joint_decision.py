# -*- coding: utf-8 -*-
"""H193: combine full diagnostic membership, not truncated rankings. Research only.
No network, brokerage, persistence of personal data, or trading authority writes.
"""
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
import io, json, re, math
import pandas as pd
import godpick_h190_market_archive as archive
import godpick_h191_opportunity_engine as tech
import godpick_h192_financial_growth as fin
import godpick_h184_research_lab as lab
import godpick_h185_auto_sources as sources
from godpick_h198_stage_review import FIELDS as STAGE_FIELDS

VERSION='V191-H193-JOINT-RESEARCH'
ROOT=Path(__file__).resolve().parent
TIERS=('雙證據研究','技術研究','財報研究','補證觀察')
STRONG_FIN=set(fin.POOLS[:5]); SINGLE=fin.POOLS[6]; PENDING=fin.POOLS[7]

def memberships(value, allowed):
    return [p for p in allowed if p in str(value or '').split('、')]

def combine(t, f):
    """Use identity (market, code), same market session for cross-evidence.
    No pooling scores with different scales, no missing-as-zero votes.
    """
    def index(rows):
        d={}; duplicate=set()
        for r in rows:
            c=str(r.get('股票代號',''));m=r.get('市場')
            if not re.fullmatch('[1-9][0-9]{3}',c) or m not in ('TWSE','TPEX'):continue
            k=(m,c)
            if k in d:duplicate.add(k)
            d[k]=r
        return {k:v for k,v in d.items() if k not in duplicate}, duplicate
    ti,td=index(t.get('diagnostic',[])); fi,fd=index(f.get('profiles',[]))
    date_ok=bool(t.get('calendar_fresh') and t.get('asof') and t.get('status') in ('PARTIAL_RESEARCH','CROSS_MARKET_RESEARCH_READY') and not t.get('archive_errors'))
    same=bool(date_ok and f.get('market_day')==t['asof'])
    visible={}
    for pool in tech.POOLS:
        for item in t.get('pools',{}).get(pool,[]):
            k=(item.get('市場'),str(item.get('股票代號','')),pool)
            n=tech.num(item.get('研究排名'))
            if n is not None and n>=1 and item.get('資料日期')==t.get('asof'):visible[k]=int(n)
    rank={p:[] for p in TIERS};audit=[]
    for k in sorted(ti.keys()|fi.keys()):
        a,b=ti.get(k,{}),fi.get(k,{})
        tp=memberships(a.get('研究池'),tech.POOLS) if date_ok and a.get('流動性通過') is True and a.get('資料日期')==t['asof'] and k not in td else []
        if t.get('status')!='CROSS_MARKET_RESEARCH_READY':tp=[p for p in tp if p!='主流強勢']
        fp=memberships(b.get('研究池'),fin.POOLS) if k not in fd else []
        # Do not count H192's derived H191 intersection as an independent vote.
        fp=[p for p in fp if p!=fin.POOLS[5] and (p!=fin.POOLS[4] or same)]
        # Market identity must match; ambiguous symbols never cross-match.
        strong=bool(STRONG_FIN.intersection(fp)); single=SINGLE in fp
        tier=TIERS[0] if tp and strong and same else TIERS[1] if tp else TIERS[2] if strong or single else TIERS[3] if PENDING in fp else ''
        r={'股票代號':k[1],'市場':k[0],'股票名稱':str((b or a).get('股票名稱',k[1])),'產業':str((b or a).get('產業','')),
           '研究分級':tier or '未入池','技術研究池':'、'.join(tp),'財報研究池':'、'.join(fp),'技術日':t.get('asof','') if tp else '',
           '技術池最佳已顯示名次':min((visible[k+(p,)] for p in tp if k+(p,) in visible),default=None),
           '財報研究時間':f.get('asof',''),'營收年月':b.get('營收年月',''),'三率季度':b.get('三率季度',''),
           '營收年增%':b.get('營收年增%'),'營收月增%':b.get('營收月增%'),'官方PE':b.get('官方PE') if b.get('PE同交易日') and same else None,
           'TTM EPS重建':b.get('TTM EPS重建'),'技術缺口':a.get('缺口','') if a else '無技術診斷',
           '財報缺口':b.get('缺口','') if b else '無財報診斷','交易狀態':'僅研究／未授權','資料一致日':same}
        # H198 is counter-evidence only; never alter H193 membership, H194 signal or H126.
        if date_ok and a.get('H198資料日期')==t.get('asof') and k not in td:
            r.update({field:a.get(field) for field in STAGE_FIELDS})
        audit.append(r)
        if tier:rank[tier].append(r)
    # Ordering is a transparent research triage, NOT predicted return or probability.
    for p,rs in rank.items():
        def key(r):
            v=tech.num(r['營收年增%']) if r['財報研究池'] else None
            w=tech.num(r['營收月增%']) if r['財報研究池'] else None
            return (-(v if v is not None else -1e12),-(w if w is not None else -1e12),r['股票代號'],r['市場'])
        # Technical rank uses ordinal within-pool rank, not incomparable raw scores.
        def technical_key(r):
            return (r['技術池最佳已顯示名次'] if r['技術池最佳已顯示名次'] is not None else 10**9,r['股票代號'],r['市場'])
        rs.sort(key=technical_key if p==TIERS[1] else key)
        for i,r in enumerate(rs,1):r['研究順序']=i
    return {'version':VERSION,'asof':f.get('asof',''),'market_day':t.get('asof','') if date_ok else '', 'same_session':same,
            'technical_status':t.get('status','UNKNOWN'),'financial_status':f.get('status','UNKNOWN'),
            'technical_coverage':t.get('coverage',{}),'financial_coverage':f.get('coverage',{}),
            'financial_errors':f.get('errors',[]),'financial_conflicts':f.get('conflicts',{}),'archive_errors':t.get('archive_errors',[]),
            'counts':{p:len(v) for p,v in rank.items()},'pools':rank,'audit':audit,
            'duplicate_identities':sorted(td|fd),'trade_authority_changed':False,'status':'RESEARCH_AVAILABLE' if any(rank.values()) else 'NO_MATCH_OR_EVIDENCE_PENDING'}

def run(root=ROOT, now=None):
    root=Path(root);now=now or datetime.now(archive.TZ)
    if now.tzinfo is None:raise ValueError('timezone required')
    now=now.astimezone(archive.TZ)
    t=tech.run(root,now=now,top=100)
    data,errors=fin.load_inputs(root)
    master=fin.read_json(root/'stock_master_cache.json',[],errors)
    f=fin.evaluate(master=master,data=data,now=now,market_day=t['asof'] if t.get('calendar_fresh') else '',opportunities=t,top=100,errors=errors)
    result=combine(t,f)
    # These contexts are read-only and omitted from public summary exports.
    return result, f

def _observed(v,now):
    try:
        d=datetime.fromisoformat(str(v));return d.tzinfo is not None and d<=now
    except (TypeError,ValueError):return False

def _events(root,code,market,now):
    snap=sources.load_source(market+'_EVENTS',root=root)
    if not snap or not _observed(snap.get('observed_at'),now):return []
    return [r for r in snap['rows'] if r.get('code')==code and r.get('published_at','9999')<=now.date().isoformat() and _observed(r.get('available_at'),now)]

def _bars(root,code,market,day):
    if not day:return pd.DataFrame()
    ar=root/'data/h190';cal,markets,rows,errors=archive._index(ar)
    sessions=sorted({d for ds in cal.values() for d in ds if d<=day})[-60:]
    if errors or len(sessions)!=60 or sessions[-1]!=day:return pd.DataFrame()
    seq=rows.get((market,code),[])
    if any(d not in markets.get(market,{}) for d in sessions) or tech.calc_features(seq,sessions)[0] is None:return pd.DataFrame()
    # Reuse exactly the H191 feature window; no unadjusted P&L.
    raw=archive.archive_history(code,market,sessions[0],day,root=ar)
    if len(raw)!=60:return pd.DataFrame()
    return raw.rename(columns=dict(zip(archive.COLS,['date','open','high','low','close','volume','amount','trades','source']))).assign(code=code)

def detail(result,financial,market,code,root=ROOT,now=None):
    root=Path(root);now=now or datetime.fromisoformat(result['asof']);now=now.astimezone(archive.TZ)
    r=next((x for x in result['audit'] if x['市場']==market and x['股票代號']==code),None)
    if not r:raise ValueError('unknown identity')
    profile=next((x for x in financial['profiles'] if x['市場']==market and x['股票代號']==code),{})
    ev=[x for x in financial['evidence'] if x['市場']==market and x['股票代號']==code and _observed(x.get('首次觀察或公告時間'),now)]
    bars=_bars(root,code,market,result['market_day'])
    events=_events(root,code,market,now)
    data={'ohlcv':bars,'events':pd.DataFrame(events)}
    opts={'code':code,'asof':now.date().isoformat()}
    roles={}
    def put(i,status,conclusion,gaps=(),metrics=None,tables=None,provenance=()):
        roles[i]={'role':i,'status':status,'conclusion':conclusion,'gaps':list(gaps),'metrics':metrics or {},'tables':tables or {},'provenance':list(provenance),'warnings':[]}
    for i in ('01','02','03','11'):
        # H184 outputs are research computations/templates, not independent source verification.
        o=lab.run_role(i,data,opts).tabular()
        o['provenance']=['H190 官方原始未還原60日行情'] if i in ('01','02') and len(bars)==60 else ['H185 官方重大訊息快照（原文未獨立複查）'] if i in ('03','11') and events else []
        if i=='11':o['conclusion']='已發布重大訊息後續查核清單，不把事實發生日當未來法說會日。';o['gaps'].append('未取得已確認未來事件日期')
        if i=='01' and o['tables']:
            for table in o['tables'].values():
                for row in table:
                    if any(tech.num(row.get(k)) is None or tech.num(row[k])<=0 for k in ('條件式參考進場','條件式參考目標','失效參考價')):
                        for k in ('條件式參考進場','條件式參考目標','失效參考價','假設報酬風險比'):row[k]=None
            o['gaps'].append('原始未還原價，先核實公司行動、交易限制與成本；不是可下單價格')
        roles[i]=o
    put('04','待補證','不使用未還原行情宣稱回測績效',['缺除權息、交易限制與可成交證據；H194待接入樣本外績效'])
    put('05','待授權','無真實持股，不以推薦股冒充投資組合',['缺授權持股、現金、風險承受度'])
    put('06','待授權','無真實成交紀錄，不以推薦紀錄冒充交易',['缺授權券商成交回報'])
    put('07','財報證據盤點' if ev else '待補證',r['財報研究池'] or '沒有通過財報研究條件',[r['財報缺口'],'現金流、負債、股本及一次性項目未完整核實'],tables={'財報概況':[profile] if profile else [],'逐項財報證據':ev},provenance=['H192 公告／首次觀測帳本'] if ev else [])
    thesis=f"{code} 技術研究：{r['技術研究池'] or '無'}；財報研究：{r['財報研究池'] or '無'}。僅研究，證據缺口待核。"
    roles['08']=lab.run_role('08',{},dict(opts,thesis=thesis)).tabular()
    roles['08']['tables']['個股反方查核']=[{'事項':'技術失效','條件':'跌回突破區或失去均線支撐；原始價需核對公司行動','現況':r['技術缺口'] or '待後續行情'},{'事項':'成長失效','條件':'新公告營收或利潤率反轉；核對一次性損益','現況':r['財報缺口'] or '待下期公告'},{'事項':'估值風險','條件':'同業折價可能源自景氣循環、負債或盈利品質','現況':'待商模與現金流核對'}]
    if r.get('H198階段'):
        roles['08']['tables']['個股反方查核'].append({'事項':'H198主升段／假突破反證','條件':r.get('H198確認條件',''),'現況':str(r['H198階段'])+'｜'+str(r.get('H198反證','')),'失效參考':r.get('H198失效參考'),'突破基準日':r.get('H198突破基準日')})
    med=profile.get('同業PE中位數') if result['same_session'] else None
    put('09','可比估值研究' if med is not None else '待補證','同市場同產業同期可比PE；非內在價值',['商業模式、股本調整與現金流仍須複核'],metrics={'同業PE中位數':med,'官方PE':r['官方PE'],'可比同業數':profile.get('同市場同產業其他同業數') if med is not None else None,'TTM EPS重建':r['TTM EPS重建']},provenance=['H192 同業估值'] if med is not None else [])
    put('10','待補證','未提供DCF數字或目標價',['缺資本支出、現金流、股數及明示折現假設'])
    put('12','待原文複核','查核H191／H192身分、日期及資料範圍；不是獨立官方原文認證', ['H192衝突為全母體統計，不能歸因特定股票','原始未還原價格及財報來源仍需複核'],metrics={'行情筆數':len(bars),'財報證據筆數':len(ev),'公告筆數':len(events),'同交易日':result['same_session']},tables={'證據核對':ev},provenance=['H190／H192 驗證後的封存資料'])
    evidence_rows=[]
    for x in ev:
        field=x.get('欄位','');kind=x.get('種類','');unit=('TWD' if kind=='revenue' else '%' if kind=='margin' else '倍' if kind=='pe' else '元／股' if field=='eps_ytd' else '')
        evidence_rows.append({'code':code,'metric':field,'value':x.get('數值'),'unit':unit,'period':x.get('期間'),'asof_date':x.get('首次觀察或公告時間'),'source_url':x.get('來源')})
    audit=lab.run_role('12',{'evidence':pd.DataFrame(evidence_rows)},opts).tabular()
    roles['12']['tables'].update(audit['tables']);roles['12']['gaps'].extend(audit['gaps'])
    return {'identity':r,'roles':{i:roles[i] for i in lab.ROLES},'asof':now.isoformat(),'trade_authority_changed':False}

def excel(result,review=None):
    def safe(rows):
        f=pd.DataFrame(rows)
        return f.apply(lambda s:s.map(lambda x:"'"+x if isinstance(x,str) and x.lstrip().startswith(('=','+','-','@')) else json.dumps(x,ensure_ascii=False,default=str) if isinstance(x,(dict,list)) else x))
    b=io.BytesIO()
    with pd.ExcelWriter(b,engine='openpyxl') as w:
        safe([{'版本':VERSION,'研究時間':result['asof'],'技術日':result['market_day'],'同交易日':result['same_session'],'交易授權':'無',**result['counts']}]).to_excel(w,sheet_name='00_摘要',index=False)
        for i,p in enumerate(TIERS,1):safe(result['pools'][p]).to_excel(w,sheet_name=f'{i:02d}_{p}',index=False)
        safe(result['audit']).to_excel(w,sheet_name='05_全母體稽核',index=False)
        if review:
            safe([{'角色':i,'名稱':lab.ROLES[i][0],'狀態':o['status'],'結論':o['conclusion'],'缺口':o['gaps'],'數據':o['metrics'],'來源':o['provenance']} for i,o in review['roles'].items()]).to_excel(w,sheet_name='06_十二研究員',index=False)
            for i,o in review['roles'].items():
                rows=[{'表格':name,**row} for name,rs in o['tables'].items() for row in rs]
                if rows:safe(rows).to_excel(w,sheet_name='R'+i,index=False)
    return b.getvalue()

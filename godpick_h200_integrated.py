# -*- coding: utf-8 -*-
"""H200 research priority + formal funnel reconciliation; never grants authority.
No network, no I/O to recommendation records. Page and workbook use sealed H195 data.
"""
from collections import Counter, defaultdict
from datetime import datetime
import math
import godpick_h197_strategy_review as review

VERSION='V191-H200-INTEGRATED-RESEARCH'
FIELDS=('H200研究優先序','H200研究權重','H200權重狀態','H200風險複核','H200策略','H200研究優先依據')
MARKETS=('TWSE','TPEX')
STRATEGIES=review.STRATEGIES
PASS='通過留出／研究權重建議（未套用）'

def finite(v):return isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v)

def strategy_keys(row):
    return sorted(review.memberships({'tech_pools':str(row.get('技術研究池') or ''),'financial_pools':str(row.get('財報研究池') or '')}))

def validated(report,day,now):
    """Accept only a same-session, independently validated H199 model; no inferred weights."""
    if not isinstance(report,dict) or report.get('version')!='V191-H199-REGIME-ADAPTIVE-REVIEW' or report.get('expected_session')!=day or report.get('weights_changed') is not False or report.get('trade_authority_changed') is not False or report.get('archive_errors')!=[]:
        return {},'H199未核實／基準1.0'
    try:
        obs=review.p.stamp(report['asof']); now=review.p.stamp(now)
        if obs>now or obs.date()!=now.date():return {},'H199時點不一致／基準1.0'
    except Exception:return {},'H199時點無效／基準1.0'
    good={}
    for m in MARKETS:
        regs=[x for x in report.get('current_regime',[]) if x.get('市場')==m and x.get('研究日')==day and x.get('狀態') in ('多頭廣度','空頭廣度','盤整／分歧') and finite(x.get('覆蓋率')) and x['覆蓋率']>=.9 and finite(x.get('有效檔數')) and x['有效檔數']>=100]
        if len(regs)!=1:continue
        candidates=[x for x in report.get('models',[]) if x.get('市場')==m and x.get('市場狀態')==regs[0]['狀態']]
        if len(candidates)!=1:continue
        x=candidates[0];w=x.get('weights');keys=x.get('strategies_tested',[])
        checks={'train_windows':20,'holdout_windows':10,'train_coverage':.95,'holdout_coverage':.95,'歷史狀態覆蓋率':.95,'train_improvement_pp':.10,'holdout_improvement_pp':.10,'holdout_win_rate':.60}
        if x.get('status')!=PASS or not all(finite(x.get(k)) and x[k]>=v for k,v in checks.items()):continue
        if not isinstance(w,dict) or set(w)!=set(STRATEGIES) or not all(finite(v) and v in (.9,1.,1.1) for v in w.values()):continue
        if not isinstance(keys,list) or len(set(keys))<2 or not set(keys)<=set(STRATEGIES) or any(w[k]!=1 for k in STRATEGIES if k not in keys):continue
        if not any(v!=1 for v in w.values()):continue
        good[m]=w.copy()
    return good,'H199同日驗證權重可供研究排序' if good else 'H199樣本不足或未通過／基準1.0'

def enrich(body,result,adaptive,now):
    """Preserve every candidate, tier and original rank; prioritize only within tier.
    H198 warning pushes a current technical candidate to risk-review tail, never deletes it.
    H199 weight is averaged across independent strategy memberships, not added as votes.
    """
    day=body['expected_session'];weights,state=validated(adaptive,day,now)
    grouped=defaultdict(list)
    for r in body['rows']:
        keys=strategy_keys(r);rank=r.get('研究順序');m=r['市場']
        use=bool(body.get('technical_current') and r['研究分級']!='補證觀察' and keys and m in weights)
        factor=sum(weights[m][k] for k in keys)/len(keys) if use else 1.0
        risk=bool(body.get('technical_current') and r.get('H198資料日期')==day and r.get('H198階段')=='突破反證與追高警示')
        validrank=finite(rank) and rank>=1
        r.update({'H200研究優先序':None,'H200研究權重':round(factor,6),'H200權重狀態':'同日樣本外驗證／僅研究' if use else '基準1.0（未套用模型）','H200風險複核':'反證優先查核／不列前段' if risk else ('未觸發本排序反證（非安全認證）' if body.get('technical_current') and r.get('H198資料日期')==day and r.get('H198階段') not in (None,'','待補證') else '反證資料待補／非安全認證'),'H200策略':'、'.join(keys),'H200研究優先依據':'同級原名次倒數 × 平均策略權重；反證後置；非預期報酬'})
        grouped[r['研究分級']].append((r,risk,factor/rank if validrank else -1))
    for group in grouped.values():
        group.sort(key=lambda z:(z[1],-z[2],z[0]['研究順序'] if finite(z[0].get('研究順序')) else 10**12,z[0]['市場'],z[0]['股票代號']))
        for i,(r,_,__) in enumerate(group,1):r['H200研究優先序']=i
    # Do not mutate legacy rows' order or H193 rank. H200 displays its own order.
    h=body.get('health',{});tech=h.get('technical_coverage') or {};fin=h.get('financial_coverage') or {}
    audit=result.get('audit',[]); reasons=Counter()
    for r in audit:
        if r.get('研究分級') in ('','未入池',None):
            for k in ('技術缺口','財報缺口'):
                v=r.get(k)
                if v:reasons[k+'：'+str(v)[:180]]+=1
    issues=[]
    if not body.get('technical_current'):issues.append('TECH_SESSION_NOT_CURRENT')
    if h.get('technical_status')!='CROSS_MARKET_RESEARCH_READY':issues.append('TECH_CROSS_MARKET_NOT_READY')
    if h.get('financial_errors'):issues.append('FINANCIAL_SOURCE_ERRORS')
    if h.get('archive_errors'):issues.append('MARKET_ARCHIVE_ERRORS')
    if body.get('errors'):issues.append('UPSTREAM_ERRORS')
    if not body['rows']:issues.append('NO_NAMED_RESEARCH_CHECK_EVIDENCE' if issues else 'NO_RULE_MATCH_NOT_MARKET_CERTIFICATION')
    if not body.get('performance',{}).get('mature'):issues.append('NO_MATURE_PERFORMANCE')
    if not weights:issues.append('WEIGHTS_BASELINE_ONLY')
    body['h200']={'version':VERSION,'state':'NAMED_RESEARCH_AVAILABLE' if body['rows'] else 'NO_NAMED_RESEARCH','authority':'H126獨立，未取得本輪正式授權數','universe':body['universe'],'joint_pool_counts':result.get('counts',{}),'published_counts':body['counts'],'excluded_before_publication':max(0,sum(result.get('counts',{}).values())-len(body['rows'])),'technical_coverage':tech,'financial_coverage':fin,'issues':issues,'top_gap_counts':dict(reasons.most_common(12)),'weight_status':state,'validated_markets':list(weights),'weight_map':weights,'market_regime':adaptive.get('current_regime',[]) if isinstance(adaptive,dict) else [],'rotation':adaptive.get('rotation',[]) if isinstance(adaptive,dict) else [],'repetition':adaptive.get('repetition_summary',[]) if isinstance(adaptive,dict) else [],'trade_authority_changed':False}
    return body

def ordered(rows):
    return sorted(rows,key=lambda r:(r.get('H200研究優先序') if finite(r.get('H200研究優先序')) else r.get('研究順序') if finite(r.get('研究順序')) else 10**12,r.get('市場',''),r.get('股票代號','')))

def formal_diagnosis(*,buyable=None,h145=None,run_id=None,research_day=None):
    """Only caller's same-export H126/H145 facts; missing is UNKNOWN, never zero."""
    try:count=int(buyable) if not isinstance(buyable,bool) and buyable is not None and float(buyable).is_integer() and int(buyable)>=0 else None
    except (ValueError,TypeError,OverflowError):count=None
    h145=h145 if isinstance(h145,dict) else {};status=str(h145.get('status') or '未確認')
    complete=h145.get('candidate_complete'); final=h145.get('audit_final_coverage') or {};verified=complete is True and isinstance(final,dict) and final.get('complete') is True and status!='NO_SCAN_CONTEXT' and not status.startswith('BLOCKED')
    if count is None:reason='H126本輪授權數未知'
    elif not verified:reason='H145母體未證實完整；不得外推全市場無機會'
    elif count==0:reason='本輪H126授權0；查閱原H126及H109逐股阻擋原因'
    else:reason='本輪H126有授權；以原H126表為準'
    return {'run_id':str(run_id or '未確認'),'research_day':str(research_day or '未確認'),'h126_buyable':count,'h145_status':status,'h145_complete':verified,'reason':reason}

def displayed_diagnosis(frame):
    """Count reasons from the actual displayed diagnostic rows, not an inferred full universe.
    No network, mutation or historical-record writes; missing columns explicitly reported.
    """
    import pandas as pd
    if not isinstance(frame,pd.DataFrame):return {'顯示列數':0,'狀態':'無可讀診斷'}
    fields=('H114K線資料狀態','H169_H114量價契約','H113流動性合格','H109決策權威','正式推薦阻擋原因')
    stats={};missing=[]
    for k in fields:
        if k not in frame.columns:missing.append(k);continue
        s=frame[k]
        if isinstance(s,pd.DataFrame):missing.append(k+'（重複欄名）');continue
        values=s.fillna('').astype(str).str.strip().replace('','未提供');stats[k]={str(x)[:200]:int(n) for x,n in values.value_counts().head(10).items()}
    return {'顯示列數':len(frame),'統計口徑':'僅目前顯示的診斷列；非去重全市場／不確認同輪完整性','原因前10類':stats,'缺少診斷欄位':missing}

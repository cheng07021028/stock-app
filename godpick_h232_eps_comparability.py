# -*- coding: utf-8 -*-
"""H232 EPS denominator evidence from revalidated official company HTML.
A consistent disclosed weighted denominator is not proof of all capital events.
Never infer shares from equity capital, rounded EPS, or total consolidated profit.
No trading authority, no historical revision or auto tuning.
"""
from math import isclose
import godpick_h192_financial_growth as fin

VERSION='V191-H232-EPS-COMPARABILITY'
CONSISTENT='分子分母一致／權益事件仍待核'
MISSING='缺明示EPS分子或加權股數'
CHANGED='加權股數變動／禁止差分實證'
CONFLICT='EPS數值或來源衝突'
# Exact labels and explicit units only. Global table TWD thousands already checked
# by H231, but EPS numerator and denominator units are explicit here as well.
NUM={'基本每股盈餘之分子(仟元)','基本每股盈餘之分子(千元)','歸屬於母公司普通股股東之損益(仟元)','歸屬於母公司普通股股東之損益(千元)'}
DEN={'普通股加權平均流通在外股數(仟股)':1000,'普通股加權平均流通在外股數(千股)':1000,'普通股加權平均流通在外股數(股)':1,'加權平均流通在外普通股股數(仟股)':1000,'加權平均流通在外普通股股數(千股)':1000,'加權平均流通在外普通股股數(股)':1}
EPS={'基本每股盈餘(元)','基本每股盈餘','基本每股盈餘合計'}

def extract(html,p,reported_eps):
    """Called only after H231 identity, cumulative period, hash and original rows pass.
    Require all three fields in a unique explicit cumulative amount column.
    Absence is expected for summary-only reports; no inference or synthetic evidence.
    """
    from godpick_h231_company_history import _date_range,_grid
    from godpick_h222_financial_history import soup_of,norm
    results=[]
    for table in soup_of(html).find_all('table'):
        rows=_grid(table,norm)
        for i,row in enumerate(rows):
            hits=[j for j,v in enumerate(row) if _date_range(v,p)]
            if len(hits)>1 and i+1<len(rows):hits=[j for j in hits if j<len(rows[i+1]) and rows[i+1][j] in ('金額','金額(仟元)','金額(千元)')]
            if len(hits)!=1:continue
            col=hits[0];vals={};bad=False
            for rr in rows[i+1:]:
                if col>=len(rr):continue
                labels=[v for v in rr[:col] if v in NUM or v in DEN or v in EPS]
                if not labels:continue
                if len(labels)!=1:bad=True;break
                label=labels[0];k='numerator_twd' if label in NUM else 'shares' if label in DEN else 'reported_eps'
                v=fin.num(rr[col]);scale=1000 if label in NUM else DEN.get(label,1)
                if k in vals or v is None:bad=True;break
                vals[k]=v*scale
            if not bad and set(vals)=={'numerator_twd','shares','reported_eps'}:
                results.append(vals)
    if len(results)!=1:return None
    v=results[0]
    if v['shares']<=0 or not isclose(v['reported_eps'],reported_eps,rel_tol=0,abs_tol=1e-8):return None
    # Preserve mismatching evidence so audit can distinguish conflict from absence.
    return v

def audit(ytd,y,ttm):
    r={'EPS可比性':MISSING,'EPS分母核實期數':0,'EPS分母變動%':None,'EPS同分母TTM':None,'EPS權益事件':'尚未獨立核實完整股本／分割／追溯重編','EPS證據期間':''}
    if y is None or ttm is None:return r
    periods=range(y-4,y+1);rows=[ytd.get(p,{}) for p in periods]
    e=[a.get('h232_eps_basis') for a in rows];r['EPS證據期間']='、'.join(fin.qstr(p) for p,a in zip(periods,e) if isinstance(a,dict))
    r['EPS分母核實期數']=sum(isinstance(a,dict) for a in e)
    if not all(isinstance(a,dict) for a in e):return r
    if not fin.comparable_series(rows):r['EPS可比性']=CONFLICT;return r
    for row,a in zip(rows,e):
        n,s,v=(fin.num(a.get(k)) for k in ('numerator_twd','shares','reported_eps'))
        if n is None or s is None or s<=0 or v is None or row.get('eps_ytd') is None or not isclose(v,row['eps_ytd'],rel_tol=0,abs_tol=1e-8) or abs(n/s-v)>.005001:
            r['EPS可比性']=CONFLICT;return r
    shares=[a['shares'] for a in e];r['EPS分母變動%']=round((max(shares)/min(shares)-1)*100,6)
    if len(set(shares))!=1:r['EPS可比性']=CHANGED;return r
    # Annual EPS numerator + current YTD - prior-year same YTD.
    if y%4==3:n=e[-1]['numerator_twd']
    else:
        annual=next((a for p,a in zip(periods,e) if p%4==3),None)
        if annual is None:return r
        n=annual['numerator_twd']+e[-1]['numerator_twd']-e[0]['numerator_twd']
    val=n/shares[-1]
    if abs(val-ttm)>.020001:r['EPS可比性']=CONFLICT;return r
    r.update({'EPS可比性':CONSISTENT,'EPS同分母TTM':round(val,4)})
    return r

def high_growth(r):
    """Additional stricter research subset; original provisional pools untouched."""
    return bool(r.get('EPS可比性')==CONSISTENT and r.get('EPS同分母TTM') is not None and r['EPS同分母TTM']>=5 and r.get('連兩季三率年增') is True and (r.get('連3月成長') is True or r.get('近13月新高') is True) and r.get('獲利率為正') and r.get('EPS季度')==r.get('三率季度'))

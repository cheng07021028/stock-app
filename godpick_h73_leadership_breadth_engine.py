# -*- coding: utf-8 -*-
"""V191-H73 Leadership Breadth / Cross-sectional Acceleration research overlay.

Purpose
-------
H72 successfully improves stock *selection*, but its 8-model level score can still
rank a steady high-quality name ahead of a stock whose sector/price/flow signals are
accelerating together right now. H73 therefore adds a bounded research-only overlay:
  1. sector breadth / group confirmation,
  2. cross-sectional acceleration,
  3. defensive-regime transition breadth,
  4. institutional-vs-momentum distribution contradiction,
  5. H72 model coherence.

No future outcome is used. H73 never creates Formal and never bypasses H68.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import pandas as pd

VERSION = "v191_h73_leadership_breadth_distribution_truth_20260916"

H73_COLUMNS = [
    "H73族群廣度分","H73領先加速分","H73Regime轉折分","H73模型一致性分","H73分布矛盾扣分",
    "H73覆蓋調整分","H73研究排序分","H73全市場百分位%","H73全市場順位","H73族群內百分位%","H73族群內順位",
    "H73研究層級","H73Regime轉折狀態","H73相對H72順位變化","H73主要優勢","H73主要警示","H73研究建議",
    "H73學習快照狀態","H73權威邊界","H73版本",
]
_BLANK={"","none","nan","nat","null","--","-","<na>"}

def _s(v:Any)->str:
    if v is None:return ""
    try:
        if pd.isna(v):return ""
    except Exception:pass
    t=str(v).strip(); return "" if t.lower() in _BLANK else t

def _f(v:Any,default:float|None=None)->float|None:
    try:
        t=str(v).strip().replace(",","").replace("％","%")
        if t.endswith("%"):t=t[:-1].strip()
        if not t or t.lower() in _BLANK:return default
        x=float(t); return x if math.isfinite(x) else default
    except Exception:return default

def _clip(x:float,lo:float=0.,hi:float=100.)->float:return max(lo,min(hi,float(x)))

def _num(row,names:Iterable[str],default=None):
    idx=row.index if isinstance(row,pd.Series) else row.keys()
    for c in names:
        if c in idx:
            x=_f(row.get(c),None)
            if x is not None:return x
    return default

def _txt(row,names:Iterable[str],default=""):
    idx=row.index if isinstance(row,pd.Series) else row.keys()
    for c in names:
        if c in idx:
            x=_s(row.get(c))
            if x:return x
    return default

def _series_num(work:pd.DataFrame,col:str,default:float=50.)->pd.Series:
    if col not in work.columns:return pd.Series([default]*len(work),index=work.index,dtype=float)
    return pd.to_numeric(work[col],errors="coerce").fillna(default).astype(float).clip(0,100)

def _sector_col(work:pd.DataFrame)->str|None:
    for c in ["族群名稱","類別","產業別"]:
        if c in work.columns:return c
    return None

def _sector_breadth(work:pd.DataFrame)->pd.Series:
    """Decision-time group confirmation from existing H72 component truths.

    No future return is used. Small groups (<3 names in the current universe) are
    shrunk toward neutral instead of being allowed to look perfect from one stock.
    """
    sec=_sector_col(work)
    if not sec:return pd.Series([50.]*len(work),index=work.index)
    m=_series_num(work,"H72動能相對強度模型分")
    b=_series_num(work,"H72突破時機模型分")
    inst=_series_num(work,"H72法人需求模型分")
    risk=_series_num(work,"H72風險Regime模型分")
    tmp=pd.DataFrame({"sec":work[sec].fillna("").astype(str).replace("","未分類"),"m":m,"b":b,"i":inst,"r":risk},index=work.index)
    rows={}
    for key,g in tmp.groupby("sec",sort=False):
        n=len(g)
        # Breadth = peer participation, not just the group leader's own score.
        participation=((g.m>=60).mean()*0.30+(g.b>=55).mean()*0.25+(g.i>=60).mean()*0.30+(g.r>=55).mean()*0.15)*100
        med=(g.m.median()*0.28+g.b.median()*0.24+g.i.median()*0.30+g.r.median()*0.18)
        raw=0.55*participation+0.45*med
        shrink=min(1.0,n/6.0)
        rows[key]=50+(raw-50)*shrink
    return tmp.sec.map(rows).fillna(50).clip(0,100)

def _transition(work:pd.DataFrame)->tuple[pd.Series,str]:
    m=_series_num(work,"H72動能相對強度模型分")
    b=_series_num(work,"H72突破時機模型分")
    i=_series_num(work,"H72法人需求模型分")
    r=_series_num(work,"H72風險Regime模型分")
    breadth=((m>=60).mean()*0.30+(b>=55).mean()*0.25+(i>=60).mean()*0.30+(r>=55).mean()*0.15)*100
    mode=_s(work.iloc[0].get("H72市場模式")) if len(work) else "NEUTRAL"
    if mode=="DEFENSIVE":
        if breadth>=62: status="REVERSAL_READY"; base=68
        elif breadth>=52: status="DEFENSIVE_STABILIZING"; base=56
        else: status="STABLE_DEFENSIVE"; base=40
    elif mode=="ATTACK": status="ATTACK_CONFIRMED"; base=76 if breadth>=55 else 65
    else: status="NEUTRAL"; base=58 if breadth>=52 else 50
    # Same global state applies to all stocks; sector breadth later differentiates.
    return pd.Series([float(base)]*len(work),index=work.index),status

def _distribution_penalty(row:pd.Series)->tuple[float,list[str]]:
    inst=_num(row,["H72法人需求模型分"],50) or 50
    holder=_num(row,["H72大戶鎖碼模型分"],50) or 50
    mom=_num(row,["H72動能相對強度模型分"],50) or 50
    brk=_num(row,["H72突破時機模型分"],50) or 50
    growth=_num(row,["H72成長動能模型分"],50) or 50
    pen=0.; why=[]
    if inst<40 and mom>=68:
        pen+=6.; why.append("高動能但法人需求弱")
    if inst<40 and brk>=62:
        pen+=4.; why.append("突破但法人未確認")
    if inst<45 and holder<55 and (mom>=65 or growth>=78):
        pen+=4.; why.append("法人/大戶與價格成長背離")
    if inst<50 and holder<45 and growth>=82:
        pen+=2.; why.append("成長強但籌碼承接不足")
    return min(16.,pen),why

def apply_h73_leadership_breadth(frame:pd.DataFrame)->pd.DataFrame:
    if frame is None or not isinstance(frame,pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame,pd.DataFrame) else pd.DataFrame()
    work=frame.copy().reset_index(drop=True)
    try:
        from godpick_h72_multi_model_alpha_ensemble import VERSION as H72V, apply_h72_multi_model_alpha_ensemble
        hv=work.get("H72版本",pd.Series([""]*len(work))).fillna("").astype(str)
        if not hv.eq(H72V).all(): work=apply_h72_multi_model_alpha_ensemble(work)
    except Exception: pass

    sector_b=_sector_breadth(work)
    trans_s,trans_status=_transition(work)
    mom=_series_num(work,"H72動能相對強度模型分")
    brk=_series_num(work,"H72突破時機模型分")
    inst=_series_num(work,"H72法人需求模型分")
    close=_series_num(work,"H66收盤品質分")
    t1=_series_num(work,"H66T1自適應排序分")
    div=_series_num(work,"H72模型分歧度",10).clip(0,40)
    acceleration=(mom*.30+brk*.30+inst*.20+close*.10+t1*.10).clip(0,100)
    coherence=(100-div*3.0).clip(0,100)
    base=_series_num(work,"H72風險調整分")

    penalties=[]; alert_text=[]
    for _,row in work.iterrows():
        p,w=_distribution_penalty(row); penalties.append(p); alert_text.append("；".join(w))
    pen=pd.Series(penalties,index=work.index,dtype=float)

    # Bounded overlay. H72 remains the base; H73 improves near-term *ordering* only.
    # A low-divergence acceleration can add up to ~15; distribution contradiction can remove 16.
    overlay=((sector_b-50)*.10+(acceleration-50)*.13+(trans_s-50)*.035+(coherence-50)*.035-pen).clip(-18,15)
    score=(base+overlay).clip(0,100)
    pct=score.rank(method="average",pct=True).mul(100)
    rank=score.rank(method="first",ascending=False).astype("Int64")

    sec=_sector_col(work)
    if sec:
        sec_series=work[sec].fillna("").astype(str).replace("","未分類")
        sec_rank=score.groupby(sec_series).rank(method="first",ascending=False).astype("Int64")
        sec_pct=score.groupby(sec_series).rank(method="average",pct=True).mul(100)
    else:
        sec_rank=pd.Series([1]*len(work),index=work.index,dtype="Int64"); sec_pct=pd.Series([50.]*len(work),index=work.index)

    h72rank=pd.to_numeric(work.get("H72全市場順位",pd.Series([None]*len(work))),errors="coerce")
    rank_delta=(h72rank-pd.to_numeric(rank,errors="coerce"))  # positive = promoted
    tiers=[]; advice=[]; strengths=[]; warnings=[]
    for i,row in work.iterrows():
        sc=float(score.iat[i]); pv=float(pct.iat[i]); acc=float(acceleration.iat[i]); sb=float(sector_b.iat[i]); co=float(coherence.iat[i]); pp=float(pen.iat[i])
        if sc>=74 and pv>=98 and acc>=65 and co>=60 and pp<=5:
            tier="L1｜領先共振核心"; adv="領先研究核心｜仍需H64/H68獨立確認"
        elif sc>=68 and pv>=93 and acc>=58 and pp<=10:
            tier="L2｜領先優先觀察"; adv="優先觀察｜確認族群延續與盤前價格"
        elif sc>=62 and pv>=80:
            tier="L3｜候選追蹤"; adv="候選追蹤｜等待加速/籌碼再確認"
        else:
            tier="R0｜一般研究"; adv="僅研究｜不得視為正式推薦"
        st=[]
        if acc>=70:st.append(f"領先加速{acc:.0f}")
        if sb>=65:st.append(f"族群廣度{sb:.0f}")
        if co>=70:st.append(f"模型一致{co:.0f}")
        if trans_status in {"REVERSAL_READY","ATTACK_CONFIRMED"}:st.append(trans_status)
        wt=[]
        if alert_text[i]:wt.append(alert_text[i])
        if sb<42:wt.append(f"族群廣度弱{sb:.0f}")
        if co<45:wt.append(f"模型分歧高{100-co:.0f}")
        tiers.append(tier); advice.append(adv); strengths.append("；".join(st) or "H72基礎排名延續"); warnings.append("；".join(wt) or "無重大H73警示")

    work["H73族群廣度分"]=sector_b.round(2)
    work["H73領先加速分"]=acceleration.round(2)
    work["H73Regime轉折分"]=trans_s.round(2)
    work["H73模型一致性分"]=coherence.round(2)
    work["H73分布矛盾扣分"]=pen.round(2)
    work["H73覆蓋調整分"]=overlay.round(2)
    work["H73研究排序分"]=score.round(2)
    work["H73全市場百分位%"] = pct.round(2)
    work["H73全市場順位"] = rank
    work["H73族群內百分位%"] = sec_pct.round(2)
    work["H73族群內順位"] = sec_rank
    work["H73研究層級"] = tiers
    work["H73Regime轉折狀態"] = trans_status
    work["H73相對H72順位變化"] = rank_delta.round(0).astype("Int64")
    work["H73主要優勢"] = strengths
    work["H73主要警示"] = warnings
    work["H73研究建議"] = advice
    snap=(work.get("H72學習快照狀態",pd.Series([""]*len(work))).fillna("").astype(str).str.startswith("SNAPSHOT-READY"))
    work["H73學習快照狀態"] = snap.map(lambda x:"SNAPSHOT-READY" if x else "SNAPSHOT-INCOMPLETE")
    work["H73權威邊界"] = "H73只改善橫截面研究排序；不得建立Formal，不得解除H68執行否決。"
    work["H73版本"] = VERSION
    return work

def build_h73_leadership_table(frame:pd.DataFrame,max_rows:int=30,max_per_sector:int=3)->pd.DataFrame:
    w=apply_h73_leadership_breadth(frame)
    if w.empty:return w
    pri=w["H73研究層級"].fillna("").astype(str).map(lambda x:40 if x.startswith("L1") else 30 if x.startswith("L2") else 20 if x.startswith("L3") else 10)
    w=w.assign(_p=pri).sort_values(["_p","H73研究排序分","H73全市場百分位%"],ascending=False,kind="mergesort")
    sec=_sector_col(w)
    if sec and max_per_sector:
        pick=[]; cnt={}
        for idx,r in w.iterrows():
            s=_s(r.get(sec)) or "未分類"
            if cnt.get(s,0)>=max_per_sector:continue
            pick.append(idx);cnt[s]=cnt.get(s,0)+1
            if len(pick)>=max_rows:break
        w=w.loc[pick]
    else:w=w.head(max_rows)
    cols=[c for c in ["股票代號","股票名稱","市場別","族群名稱","H73研究層級","H73研究排序分","H73全市場百分位%","H73全市場順位","H73族群內順位","H73族群廣度分","H73領先加速分","H73Regime轉折狀態","H73模型一致性分","H73分布矛盾扣分","H73相對H72順位變化","H73主要優勢","H73主要警示","H73研究建議","H72研究層級","H72風險調整分","H73權威邊界","H73版本"] if c in w.columns]
    return w[cols].reset_index(drop=True)

def build_h73_governance_summary(frame:pd.DataFrame)->pd.DataFrame:
    w=apply_h73_leadership_breadth(frame)
    if w.empty:return pd.DataFrame({"治理項目":["H73"],"目前狀態":["無資料"]})
    t=w["H73研究層級"].fillna("").astype(str)
    return pd.DataFrame([
        {"治理項目":"L1領先共振核心","目前狀態":int(t.str.startswith("L1").sum()),"治理原則":"H72高基礎＋領先加速＋模型一致，仍非Formal"},
        {"治理項目":"L2領先優先觀察","目前狀態":int(t.str.startswith("L2").sum()),"治理原則":"優先研究，不繞過H68"},
        {"治理項目":"Regime轉折","目前狀態":_s(w.iloc[0].get("H73Regime轉折狀態")),"治理原則":"只用決策當時全市場內部廣度，不使用未來資料"},
        {"治理項目":"分布矛盾中位扣分","目前狀態":round(pd.to_numeric(w["H73分布矛盾扣分"],errors="coerce").median(),2),"治理原則":"高動能但法人/大戶未確認時降序"},
        {"治理項目":"Formal/Execution權威","目前狀態":"UNCHANGED","治理原則":"H64/H63 Formal＋H68執行否決仍唯一權威"},
    ])

__all__=["VERSION","H73_COLUMNS","apply_h73_leadership_breadth","build_h73_leadership_table","build_h73_governance_summary"]

# -*- coding: utf-8 -*-
"""V183 Super AI advisory engine for executable entry/exit and scenario probabilities.

The engine does **not** claim deterministic prediction.  It converts existing
full-market evidence plus chip/macro/ETF context into calibrated scenario
probabilities and explicit conditional actions.  Hard data-quality / liquidity /
LOCKDOWN blocks always win over model scores.
"""
from __future__ import annotations

from typing import Any, Iterable
import math

import pandas as pd

try:
    from godpick_super_ai_market_context import load_super_ai_market_context
except Exception:
    load_super_ai_market_context = None
try:
    from godpick_super_ai_experience import load_super_ai_experience_profile
except Exception:
    load_super_ai_experience_profile = None

SUPER_AI_VERSION = "super_ai_godpick_v183_20260811"
SUPER_AI_COLUMNS = [
    "SuperAI模型版本", "SuperAI進場狀態", "SuperAI進場信心%", "SuperAI建議進場區間下", "SuperAI建議進場區間上",
    "SuperAI進場觸發說明", "SuperAI出場狀態", "SuperAI動態停損價", "SuperAI第一減碼價", "SuperAI第二目標價", "SuperAI出場說明",
    "SuperAI開高走高%", "SuperAI開高走低%", "SuperAI開低走高%", "SuperAI開低走低%", "SuperAI平開震盪%",
    "SuperAI隔日上漲機率%", "SuperAI隔日主情境", "SuperAI隔日情境信心%", "SuperAI本週進場適合度", "SuperAI本週進場等級",
    "SuperAI融資影響分", "SuperAI融資影響", "SuperAI_ETF確認分", "SuperAI_ETF確認", "SuperAI期貨情境分", "SuperAI期貨情境",
    "SuperAI市場情境分", "SuperAI資料覆蓋率%", "SuperAI最終決策分", "SuperAI最終決策理由", "SuperAI反對理由", "SuperAI取消條件",
]
_NUMERIC = {c for c in SUPER_AI_COLUMNS if c.endswith("%") or c.endswith("分") or c.endswith("價") or c.endswith("度") or c in {"SuperAI建議進場區間下","SuperAI建議進場區間上"}}

BLANK={"","nan","none","null","nat","--","-","<na>"}

def _s(v:Any)->str:
    if v is None:return ""
    try:
        if pd.isna(v):return ""
    except Exception:pass
    t=str(v).strip();return "" if t.lower() in BLANK else t

def _f(v:Any,default:float=0.0)->float:
    try:
        if v is None:return default
        if isinstance(v,(int,float)):
            x=float(v);return x if math.isfinite(x) else default
        t=str(v).strip().replace(",","").replace("％","%")
        if t.endswith("%"):t=t[:-1]
        x=float(t);return x if math.isfinite(x) else default
    except Exception:return default

def _first(row:dict[str,Any],names:Iterable[str],default:float=0.0,positive:bool=False)->float:
    fallback=None
    for n in names:
        if n not in row:continue
        if not _s(row.get(n)):continue
        x=_f(row.get(n),float("nan"))
        if x!=x:continue
        if fallback is None:fallback=x
        if not positive or x>0:return x
    return default if fallback is None else fallback

def _clip(x:float,lo=0.0,hi=100.0)->float:return max(lo,min(hi,float(x)))

def _text(row:dict[str,Any],names:Iterable[str])->str:return "｜".join(_s(row.get(n)) for n in names if _s(row.get(n)))

def _contains(text:str,keys:Iterable[str])->bool:
    low=text.lower();return any(str(k).lower() in low for k in keys)

def _softmax(logits:list[float])->list[float]:
    m=max(logits); ex=[math.exp(max(-20,min(20,x-m))) for x in logits]; total=sum(ex) or 1
    return [x/total*100 for x in ex]

def _shrink_probs(probs:list[float],confidence:float)->list[float]:
    # Low-data predictions contract toward the five-state 20% prior.
    w=_clip(confidence,20,95)/100
    out=[20+(p-20)*w for p in probs]
    total=sum(out) or 100
    return [p/total*100 for p in out]

def _round_tick(price:float)->float:
    if price<=0:return 0.0
    tick=0.01 if price<10 else 0.05 if price<50 else 0.1 if price<100 else 0.5 if price<500 else 1.0 if price<1000 else 5.0
    return round(round(price/tick)*tick,2)

def _hard_block(row:dict[str,Any])->tuple[bool,str]:
    blob=_text(row,["操作許可","正式推薦排除原因","真禁買原因","硬否決原因","掃描品質狀態","K線資料狀態","流動性資料狀態","極端市場LOCKDOWN"])
    reasons=[]
    for key,label in [("LOCKDOWN","極端市場LOCKDOWN"),("低流動","低流動性"),("興櫃","興櫃不列正式新倉"),("K線落後","K線落後"),("資料待更新","資料待更新"),("禁止所有新倉","全面禁止新倉")]:
        if key.lower() in blob.lower():reasons.append(label)
    lag=_first(row,["K線落後交易日"],0)
    if lag>=2:reasons.append(f"K線落後{lag:.0f}日")
    amount=_first(row,["流動性參考成交額百萬","成交額百萬","20日均成交額百萬"],0,positive=True)
    if amount and amount<80:reasons.append("成交額過低")
    return bool(reasons),"、".join(dict.fromkeys(reasons))

def _margin_context(row:dict[str,Any],context:dict[str,Any])->tuple[float|None,str,dict[str,Any]]:
    code=_s(row.get("股票代號")); info=((context.get("margin_by_stock") or {}).get(code) or {}) if isinstance(context,dict) else {}
    if not info:return None,"資料待補",{}
    prev=_f(info.get("融資前日餘額張"),0); cur=_f(info.get("融資今日餘額張"),0); delta=_f(info.get("融資增減張"),cur-prev)
    sprev=_f(info.get("融券前日餘額張"),0); scur=_f(info.get("融券今日餘額張"),0); sdelta=_f(info.get("融券增減張"),scur-sprev)
    dp=delta/max(prev,1)*100 if prev>0 else 0; sp=sdelta/max(sprev,1)*100 if sprev>0 else 0
    ret1=_first(row,["今日漲幅%","單日漲幅%"],0); vol=_first(row,["當日量比","量比"],1)
    score=50.0; reasons=[]
    # Financing acceleration is not automatically bullish. Crowding + price weakness is negative;
    # price strength with financing stable/decreasing is healthier.
    if dp>=8:
        score-=12 if ret1<=1 else 7; reasons.append(f"融資增{dp:.1f}%偏擁擠")
    elif dp>=3:
        score-=5; reasons.append(f"融資增{dp:.1f}%")
    elif dp<=-5 and ret1>=0:
        score+=10; reasons.append(f"價穩且融資減{abs(dp):.1f}%")
    elif dp<=-3:
        score+=4; reasons.append("融資降溫")
    if sp>=10:
        if ret1>=2 and vol>=1.2: score+=4; reasons.append("融券增但股價強，具軋空觀察")
        elif ret1<0: score-=7; reasons.append("融券增且價格轉弱")
    if scur>0 and cur>0:
        ratio=scur/cur*100
        if ratio>=15 and ret1>0: score+=3; reasons.append(f"券資比{ratio:.1f}%")
    return round(_clip(score),1),"、".join(reasons) or "融資券變化中性",{"margin_delta_pct":dp,"short_delta_pct":sp}

def _etf_context(row:dict[str,Any],context:dict[str,Any])->tuple[float|None,str]:
    etf=context.get("etf") or {} if isinstance(context,dict) else {}
    broad=etf.get("ETF市場確認分")
    scores=[]
    if broad is not None:scores.append(_f(broad,50))
    # Tech/semiconductor candidates additionally look at tech ETF 0052/00881 if cached.
    cat=_s(row.get("類別")); etfs=etf.get("ETFs") or {}
    if any(k in cat for k in ["半導體","AI","電子","PCB","伺服器","封測","光電","電腦"]):
        for code in ["0052","00881"]:
            sc=((etfs.get(code) or {}).get("score")) if isinstance(etfs,dict) else None
            if sc is not None:scores.append(_f(sc,50))
    if not scores:return None,"ETF資料待補"
    score=sum(scores)/len(scores)
    return round(_clip(score),1),f"ETF市場確認 {score:.1f}"

def _futures_context(context:dict[str,Any])->tuple[float|None,str]:
    t=context.get("taifex") or {} if isinstance(context,dict) else {}
    oi=t.get("外資臺指期未平倉淨口數"); pcr=t.get("PCR未平倉量比%")
    known=0; score=50; reasons=[]
    if oi is not None:
        oi=_f(oi,0);known+=1
        # Absolute levels depend on contract structure; use bounded contribution only.
        score+=max(-15,min(15,oi/5000)); reasons.append(f"外資臺指期淨OI {oi:.0f}")
    if pcr is not None:
        pcr=_f(pcr,100);known+=1
        # PCR is contextual, not a one-direction signal. Extreme readings raise uncertainty/risk.
        if 90<=pcr<=120: score+=2
        elif pcr>150 or pcr<65: score-=6; reasons.append(f"PCR極端 {pcr:.1f}%")
        else: score-=1
    if not known:return None,"期貨/PCR資料待補"
    return round(_clip(score),1),"、".join(reasons) or "期貨情境中性"

def _scenario(row:dict[str,Any],market_score:float,data_conf:float,cal_bias:float)->dict[str,Any]:
    timing=_first(row,["AI Timing時機分","Entry進場買點分","進場買點分"],50)
    risk=_first(row,["AI Risk風控分","Risk風控安全分","風控安全分"],50)
    cont=_first(row,["AI Continuation延續分","主流主升優先分","強勢動能分"],50)
    alpha=_first(row,["AI Alpha品質分","候選強度分"],50)
    close=_first(row,["當日收盤位置%","收盤位置%"],50); vol=_first(row,["當日量比","量比"],1)
    chase=_first(row,["追價風險分","追高風險分數_決策"],55); ex=_first(row,["隔日耗竭風險分"],chase)
    gap=_first(row,["開盤跳空%"],0); ret1=_first(row,["今日漲幅%"],0); ret5=_first(row,["近5日漲幅%"],0)
    bullish=(timing*.24+risk*.13+cont*.22+alpha*.13+close*.13+market_score*.15)/100
    exhaustion=(chase*.55+ex*.45)/100
    vol_strength=max(0,min(1.5,(vol-.8)/1.2))
    gap_heat=max(0,min(1.2,(abs(gap)+max(0,ret1-3))/8))
    pullback=_contains(_text(row,["主要進場路徑","進場型態","買點狀態"]),["回測","承接","二次買點"])
    logits=[
        1.35*bullish + .42*vol_strength + .30*(close/100) - .85*exhaustion - .15*gap_heat, # high-high
        .45*bullish + .75*gap_heat + .95*exhaustion + .25*max(0,ret5/20) - .35*(risk/100), # high-low
        1.05*bullish + .50*(risk/100) + (.35 if pullback else 0) - .25*exhaustion, # low-high
        1.10*(1-bullish) + .45*(1-risk/100) + .35*exhaustion + .20*max(0,-ret1/5), # low-low
        .70*(1-abs(bullish-.5)*2) + .30*(1-vol_strength) + .15*(1-abs(ret1)/10), # flat
    ]
    probs=_shrink_probs(_softmax(logits),data_conf)
    names=["開高走高","開高走低","開低走高","開低走低","平開震盪"]
    mapping={n:round(p,1) for n,p in zip(names,probs)}
    up=mapping["開高走高"]+mapping["開低走高"] + mapping["平開震盪"]*.42 + cal_bias
    up=round(_clip(up,15,85),1)
    top=max(mapping,key=mapping.get); top_prob=mapping[top]
    scen_conf=round(_clip(data_conf*.55 + (top_prob-20)*1.1 + abs(up-50)*.45,20,92),1)
    return {"probs":mapping,"up":up,"top":top,"confidence":scen_conf}

def score_super_ai_row(row:dict[str,Any],context:dict[str,Any]|None=None,experience:dict[str,Any]|None=None)->dict[str,Any]:
    context=context or {}; experience=experience or {}
    hard,hard_reason=_hard_block(row)
    entry=_first(row,["AI Timing時機分","Entry進場買點分","進場買點分"],50)
    risk=_first(row,["AI Risk風控分","Risk風控安全分","風控安全分"],50)
    alpha=_first(row,["AI Alpha品質分","候選強度分","股神實戰總分"],50)
    cont=_first(row,["AI Continuation延續分","主流主升優先分","強勢動能分"],50)
    op=_first(row,["可操作分","實戰操作品質分"],50)
    rr=_first(row,["AI戰術風報比","路徑風險報酬比","實戰風險報酬比","風險報酬比"],0,positive=True)
    chase=_first(row,["追價風險分","追高風險分數_決策"],55)
    exhaustion=_first(row,["隔日耗竭風險分"],chase)
    mainrise=_first(row,["主流主升優先分","主流資金分"],50)
    data_quality=_first(row,["資料完整度評分","AI資料信心分","推薦可信度分"],0)
    official_text=_text(row,["官方因子新鮮度","股神資料總新鮮度","掃描品質狀態"])
    if not data_quality:data_quality=65 if _contains(official_text,["最新","對齊"]) else 45
    if _contains(official_text,["日期未驗證","可信度不足","過期","落後"]):data_quality=min(data_quality,48)
    # V183：資料治理是交易許可的上層閘門。即使個股分數很高，
    # 掃描報告若明確寫『正式推薦暫停』，SuperAI 只能提供研究情境，不能輸出 READY。
    quality_text=_text(row,["掃描品質狀態","股神資料警示","官方因子新鮮度","股神資料總新鮮度"])
    formal_pause=_contains(quality_text,["正式推薦暫停","禁止正式推薦","資料品質不足","日期/可信度不足","資料待更新"])
    formal_usable_text=_s(row.get("正式推薦可用")).lower()
    if formal_usable_text in {"false","0","否","no"}: formal_pause=True
    margin_score,margin_text,_=_margin_context(row,context)
    etf_score,etf_text=_etf_context(row,context)
    futures_score,futures_text=_futures_context(context)
    market_parts=[x for x in [etf_score,futures_score] if x is not None]
    macro_row=_first(row,["大盤橋接分數","大盤多空分數"],0)
    if macro_row>0:market_parts.append(macro_row)
    market_score=sum(market_parts)/len(market_parts) if market_parts else 50
    known_macro=sum(x is not None for x in [margin_score,etf_score,futures_score])
    macro_risk=bool(known_macro>=2 and market_score<45)
    coverage_fields=[entry,risk,alpha,cont,rr if rr>0 else None,data_quality,margin_score,etf_score,futures_score]
    coverage=sum(x is not None and not (isinstance(x,float) and math.isnan(x)) for x in coverage_fields)/len(coverage_fields)*100
    cal_bias=_f(experience.get("applied_probability_bias_pp"),0)
    scenario=_scenario(row,market_score,coverage,cal_bias)
    trend_score=_clip(alpha*.22+entry*.22+risk*.16+cont*.18+op*.10+mainrise*.12)
    if margin_score is not None:trend_score=trend_score*.92+margin_score*.08
    if etf_score is not None:trend_score=trend_score*.92+etf_score*.08
    if futures_score is not None:trend_score=trend_score*.95+futures_score*.05
    trend_score-=max(0,chase-65)*.25 + max(0,exhaustion-65)*.15
    trend_score+=max(-8,min(8,(rr-1.3)*5)) if rr>0 else -5
    decision=_clip(trend_score)
    price=_first(row,["最新價","推薦價格","推薦日價格"],0,positive=True)
    trigger=_first(row,["實戰觸發價","突破確認參考價","突破確認價"],0,positive=True)
    guard=_first(row,["觸發後守價","守價回測參考價","回測承接參考價","近端支撐"],0,positive=True)
    path=_s(row.get("主要進場路徑")) or _s(row.get("進場型態"))
    ref=guard if guard>0 and ("回測" in path or "守價" in path) else trigger if trigger>0 else guard if guard>0 else price
    lower=_round_tick(ref*.992) if ref>0 else 0; upper=_round_tick(ref*1.006) if ref>0 else 0
    stop=_first(row,["實戰停損參考","停損參考","失效價位"],0,positive=True)
    if stop<=0 and price>0:
        stop_dist=_first(row,["AI戰術停損距離%","實戰停損距離%","停損距離%"],0,positive=True)
        if stop_dist>0:stop=_round_tick(price*(1-stop_dist/100))
    pressure=_first(row,["第一壓力價","近端壓力","AI趨勢延伸目標價"],0,positive=True)
    target1=pressure if pressure>price else _round_tick(price*(1+max(2.5,min(10,rr*3.0))/100)) if price>0 else 0
    target2=_round_tick(max(target1,price)*(1+max(2,min(8,rr*2.0))/100)) if price>0 else 0
    permission=_text(row,["操作許可","進場可執行判定","隔日參考判定","正式推薦分區"])
    trigger_ready=_contains(permission,["READY","允許","條件式"]) and not _contains(permission,["禁止新倉","禁止買進"])
    nearest=_first(row,["距最近可執行買點%","守價回測距離%","觸發距離%"],99)
    if hard:
        entry_state="AVOID｜硬風控禁止新倉"
    elif formal_pause:
        entry_state="WAIT-DATA｜正式資料閘門未解除"
    elif coverage<45:
        entry_state="WAIT-DATA｜資料不足"
    elif _contains(permission,["禁止新倉","禁止買進"]):
        entry_state="AVOID｜現行風控禁止新倉"
    elif macro_risk and trigger_ready:
        entry_state="WAIT-MARKET｜籌碼/ETF/期貨未確認"
    elif trigger_ready and decision>=72 and risk>=60 and rr>=1.2 and chase<=68 and nearest<=3.0:
        entry_state="READY-COND｜條件成立可分批"
    elif "回測" in path or guard>0:
        entry_state="WAIT-PULLBACK｜等待回測守價"
    elif trigger>0:
        entry_state="WAIT-BREAKOUT｜等待突破確認"
    else:
        entry_state="TRACK｜持續觀察"
    entry_conf=_clip(coverage*.38+decision*.32+risk*.15+scenario["confidence"]*.15)
    if hard:entry_conf=min(entry_conf,35)
    if price>0 and stop>0 and price<=stop: exit_state="EXIT-NOW｜失效價已跌破"
    elif chase>=80 or exhaustion>=75: exit_state="TRIM/NO-CHASE｜高耗竭先減碼"
    elif price>0 and target1>0 and price>=target1*.995: exit_state="TRIM｜接近第一目標"
    elif entry_state.startswith("AVOID"): exit_state="NO-POSITION｜不建立新倉"
    else: exit_state="HOLD/TRAIL｜守停損續抱"
    weekly=_clip(alpha*.23+cont*.23+risk*.18+mainrise*.16+market_score*.10+min(100,rr*40)*.10 - max(0,chase-65)*.22)
    weekly_grade="A｜本週可等條件進場" if weekly>=75 else "B｜本週偏多等待買點" if weekly>=62 else "C｜本週觀察" if weekly>=50 else "D｜本週不宜新倉"
    if formal_pause:
        weekly_grade="DATA｜本週結構僅供研究，先補齊正式資料"
    elif macro_risk:
        weekly_grade="C-MKT｜本週個股可追蹤，但市場/籌碼未確認"
    oppose=[]
    if hard_reason:oppose.append(hard_reason)
    if chase>=70:oppose.append(f"追價風險{chase:.0f}")
    if exhaustion>=65:oppose.append(f"隔日耗竭{exhaustion:.0f}")
    if rr and rr<1.2:oppose.append(f"風報比{rr:.2f}不足")
    if coverage<70:oppose.append(f"資料覆蓋{coverage:.0f}%")
    if formal_pause:oppose.append("掃描/官方資料治理尚未解除正式推薦")
    if margin_score is None:oppose.append("融資券未納入最新資料")
    elif margin_score<45:oppose.append(f"融資券結構偏不利{margin_score:.0f}")
    if etf_score is None:oppose.append("ETF確認資料不足")
    elif etf_score<45:oppose.append(f"ETF市場確認偏弱{etf_score:.0f}")
    if futures_score is not None and futures_score<45:oppose.append(f"期貨/PCR情境偏弱{futures_score:.0f}")
    reasons=[f"Alpha {alpha:.0f}",f"Timing {entry:.0f}",f"Risk {risk:.0f}",f"延續 {cont:.0f}",f"本週 {weekly:.0f}",f"隔日上漲情境 {scenario['up']:.1f}%"]
    if margin_score is not None:reasons.append(f"融資券 {margin_score:.0f}")
    if etf_score is not None:reasons.append(f"ETF {etf_score:.0f}")
    probs=scenario["probs"]
    return {
        "SuperAI模型版本":SUPER_AI_VERSION,"SuperAI進場狀態":entry_state,"SuperAI進場信心%":round(entry_conf,1),
        "SuperAI建議進場區間下":lower,"SuperAI建議進場區間上":upper,"SuperAI進場觸發說明":f"{path or '條件式進場'}｜參考{ref:.2f}；未觸發/未守價不交易" if ref else "缺少有效觸發/支撐價，等待資料",
        "SuperAI出場狀態":exit_state,"SuperAI動態停損價":_round_tick(stop),"SuperAI第一減碼價":_round_tick(target1),"SuperAI第二目標價":_round_tick(target2),"SuperAI出場說明":"跌破動態停損或觸發後失守立即取消；達第一目標分批減碼，其餘移動停損續抱。",
        "SuperAI開高走高%":probs["開高走高"],"SuperAI開高走低%":probs["開高走低"],"SuperAI開低走高%":probs["開低走高"],"SuperAI開低走低%":probs["開低走低"],"SuperAI平開震盪%":probs["平開震盪"],
        "SuperAI隔日上漲機率%":scenario["up"],"SuperAI隔日主情境":scenario["top"],"SuperAI隔日情境信心%":scenario["confidence"],"SuperAI本週進場適合度":round(weekly,1),"SuperAI本週進場等級":weekly_grade,
        "SuperAI融資影響分":round(margin_score,1) if margin_score is not None else None,"SuperAI融資影響":margin_text,"SuperAI_ETF確認分":round(etf_score,1) if etf_score is not None else None,"SuperAI_ETF確認":etf_text,"SuperAI期貨情境分":round(futures_score,1) if futures_score is not None else None,"SuperAI期貨情境":futures_text,
        "SuperAI市場情境分":round(market_score,1),"SuperAI資料覆蓋率%":round(coverage,1),"SuperAI最終決策分":round(decision,1),"SuperAI最終決策理由":"｜".join(reasons),"SuperAI反對理由":"｜".join(oppose[:5]) if oppose else "無重大反對證據；仍需盤中確認","SuperAI取消條件":"K線/官方資料過期、跌破停損、觸發失敗、族群轉弱、大盤LOCKDOWN或成交量結構惡化",
    }

def apply_super_ai_engine(data:Any,context:dict[str,Any]|None=None,experience:dict[str,Any]|None=None):
    is_df=isinstance(data,pd.DataFrame)
    if data is None:return pd.DataFrame(columns=SUPER_AI_COLUMNS) if is_df else []
    if is_df:
        out=data.copy(); records=out.to_dict(orient="records")
    elif isinstance(data,list):records=[dict(x) for x in data if isinstance(x,dict)]; out=None
    else:
        out=pd.DataFrame(data); records=out.to_dict(orient="records"); is_df=True
    if context is None and callable(load_super_ai_market_context):
        try:context=load_super_ai_market_context()
        except Exception:context={}
    if experience is None and callable(load_super_ai_experience_profile):
        try:experience=load_super_ai_experience_profile()
        except Exception:experience={}
    scored=[score_super_ai_row(r,context or {},experience or {}) for r in records]
    if not is_df:
        return [{**r,**s} for r,s in zip(records,scored)]
    score_df=pd.DataFrame(scored,index=out.index)
    out=out.drop(columns=[c for c in SUPER_AI_COLUMNS if c in out.columns],errors="ignore")
    return pd.concat([out,score_df.reindex(columns=SUPER_AI_COLUMNS)],axis=1)

__all__=["SUPER_AI_VERSION","SUPER_AI_COLUMNS","score_super_ai_row","apply_super_ai_engine"]

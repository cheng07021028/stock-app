# -*- coding: utf-8 -*-
"""H198 deterministic early-stage and counter-evidence research; no trading authority.
Only uses the exact validated 60-session H191 window. Raw prices, not adjusted.
"""
import numpy as np
from collections import Counter
VERSION='V191-H198-STAGE-COUNTEREVIDENCE'
POOLS=('蓄勢待突破','初期突破待確認','突破回測守穩','突破反證與追高警示')
FIELDS=('H198階段','H198反證','H198突破基準日','H198箱頂參考','H198失效參考','H198確認條件','H198補充證據','H198資料日期')

def assess(seq, expected, feature, *, liquid):
    """Caller has validated H191 OHLCV. Recheck dates, not use future bars.
    All thresholds are heuristic research rules; no probability or buy approval.
    """
    s=sorted((r for r in seq if expected[0]<=r[0]<=expected[-1]),key=lambda r:r[0]) if len(expected)==60 else []
    if len(s)!=60 or [r[0] for r in s]!=expected:
        return {'H198階段':'待補證','H198反證':'60日證據不足'}
    try:a=np.asarray([r[1:7] for r in s],dtype=float)
    except (ValueError,TypeError):return {'H198階段':'待補證','H198反證':'量價格式錯誤'}
    if a.shape!=(60,6) or not np.isfinite(a).all() or (a<=0).any():
        return {'H198階段':'待補證','H198反證':'量價證據不足'}
    o,h,l,c,v,amount=a.T
    price=float(c[-1]); resistance=float(h[-21:-1].max()); support=float(l[-21:-1].min())
    # Freeze the FIRST observed closing breakout in the last five sessions;
    # never move the reference to today's new high after the original event.
    events=[i for i in range(55,60) if c[i]>h[i-20:i].max()*1.005 and c[i-1]<=h[i-20:i].max()]
    event=events[0] if events else None
    anchor=float(h[event-20:event].max()) if event is not None else resistance
    ev_vol=float(v[event]/v[event-20:event].mean()) if event is not None else None
    vol=float(feature['量比']); r5=float(feature['5日漲跌%']); r20=float(feature['20日漲跌%'])
    ma20=float(feature['MA20']); pos=feature['60日區間位置%']
    # Previous 20 sessions exclude current session; no future information.
    contraction=float(v[-6:-1].mean()/v[-21:-6].mean())
    wick=float((h[-1]-max(o[-1],c[-1]))/(h[-1]-l[-1])) if h[-1]>l[-1] else 0.
    close_pos=float((c[-1]-l[-1])/(h[-1]-l[-1])) if h[-1]>l[-1] else .5
    invalidation=anchor*.99 if event is not None else support
    risk=[]; flags=[]
    if h[-1]>resistance*1.005 and price<=resistance: risk.append('盤中越箱頂但收盤未站穩')
    if event is not None and event<59 and (c[event+1:]<invalidation).any():risk.append('突破後曾收盤跌破原箱頂容忍線')
    if event is not None and ev_vol<1.5:risk.append('突破日量能未達1.5倍')
    if wick>=.45 and (h[-1]-max(o[-1],c[-1]))/price>=.01 and close_pos<=.55 and (h[-1]>=resistance or event is not None) and vol>=1.5:risk.append('放量長上影且收盤偏弱')
    if r5>15 or price/ma20>1.15 or r20>30:risk.append('短期漲幅或均線乖離過大')
    if vol>=1.5 and c[-1]<c[-2] and close_pos<=.35:risk.append('放量下跌且收盤靠近低點')
    if event is not None and c[-1]<anchor:risk.append('當日收盤未守原突破箱頂')
    if not liquid:flags.append('當日或20日均成交額未達門檻')
    if feature.get('法人5日買超股數') is None:flags.append('法人證據待補')
    elif feature['法人5日買超股數']<=0:flags.append('近5日法人未淨買超')
    if feature.get('TDCC持股變化pp') is None:flags.append('TDCC兩期證據待補')
    elif feature['TDCC持股變化pp']<=0:flags.append('TDCC大戶比例未增加')
    # Missing chip evidence is an explicit limitation, not fake negative or positive evidence.
    phase='其他型態'; condition='未符合H198初期研究規則'; score=None
    if risk:
        phase=POOLS[3]; condition='待收盤重新站穩原壓力、量價反證解除後再複核'
    elif not liquid:
        phase='流動性待確認'; condition='成交額達研究門檻後再複核'
    elif event is not None and event<59 and price>=anchor and ev_vol>=1.5 and any(l[j]<=anchor*1.02 and c[j]>=anchor and v[j]<=v[event]*.8 for j in range(event+1,60)) and price>=ma20 and ma20>=feature['MA20前5日']:
        phase=POOLS[2];condition='維持原箱頂上方；觀察再放量與失效價';score=30+min(ev_vol,3)*2-max(0,(price/anchor-1)*100)
    elif event is not None and price>=anchor and ev_vol>=1.5 and price>=ma20 and ma20>=feature['MA20前5日'] and (price/anchor-1)<=.08:
        phase=POOLS[1];condition='未來交易日觀察收盤守原箱頂及回測量縮';score=25+min(ev_vol,3)*2-max(0,(price/anchor-1)*100)
    elif event is None and pos is not None and 20<=pos<=75 and -6<=feature['距箱頂%']<=0 and feature['箱體振幅%']<=18 and feature['均線收斂%']<=3 and ma20>=feature['MA20前5日'] and -8<=r20<=12 and .5<=vol<=1.5 and contraction<=.85:
        phase=POOLS[0];condition='收盤越過前20日箱頂0.5%且量比至少1.5；核對是否真突破';score=30-feature['箱體振幅%']-feature['均線收斂%']*2+feature['距箱頂%']+(1-contraction)*5
    return {'H198階段':phase,'H198反證':'；'.join(risk) or '未觸發本規則反證（非無風險）','H198突破基準日':expected[event] if event is not None else '',
            'H198箱頂參考':round(anchor,4),'H198失效參考':round(invalidation,4),'H198確認條件':condition,'H198補充證據':'；'.join(flags),
            'H198突破日量比':round(ev_vol,4) if ev_vol is not None else None,'H198整理量縮比':round(contraction,4),'H198上影比例':round(wick,4),
            'H198研究分':round(score,4) if score is not None else None,'H198資料日期':expected[-1], 'H198資料限制':'官方未還原價格；除權息及可成交性未完整驗證', 'H198交易狀態':'僅研究／未授權'}

def summarize(rows,top,universe=None):
    counts=Counter(r.get('H198階段','待補證') for r in rows)
    if universe is not None and universe>len(rows):counts['待補證']+=universe-len(rows)
    pools={p:[] for p in POOLS}
    for r in rows:
        p=r.get('H198階段')
        if p in pools:pools[p].append(r)
    for p,rs in pools.items():
        rs.sort(key=lambda r:(-(r.get('H198研究分') or 0),r['市場'],r['股票代號']))
        pools[p]=[{**r,'H198研究排名':i if p!=POOLS[3] else None} for i,r in enumerate(rs[:top],1)]
    return {'version':VERSION,'valid':len(rows),'universe':universe if universe is not None else len(rows),'counts':dict(counts),'pool_counts':{p:counts[p] for p in POOLS},'pools':pools,'trade_authority_changed':False}

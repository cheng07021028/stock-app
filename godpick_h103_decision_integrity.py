"""H103: pure decision evidence and price normalization; no trading authority."""
from __future__ import annotations

from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
import hashlib
import json
import math

VERSION = 'v191_h103_decision_integrity_20261002'
COLUMNS = ['H103版本','H103決策快照','H103最終層別','H103未通過原因',
           'H103價格處理','H103計畫有效至','H103TDCC證據','H103新聞證據',
           'H103Momentum進場','H103Momentum停損','H103Momentum目標','H103Momentum成本後RR',
           'H103Momentum狀態']

def number(value):
    try:
        if isinstance(value,bool): return None
        x=float(value)
        return x if math.isfinite(x) else None
    except (TypeError,ValueError): return None

def text(value):
    if value is None: return ''
    s=str(value).strip()
    return '' if s.lower() in ('nan','none','null','<na>') else s

def flag(value):
    return text(value).lower() in ('true','1','1.0','yes','是')

def tick_price(value, *, up=False):
    """Taiwan ordinary-share price grid. Never applied to isolated emerging shares."""
    x=number(value)
    if x is None or x<=0: return None
    step = '0.01' if x<10 else '0.05' if x<50 else '0.1' if x<100 else '0.5' if x<500 else '1' if x<1000 else '5'
    d=Decimal(str(x)); unit=Decimal(step)
    return float((d/unit).to_integral_value(rounding=ROUND_CEILING if up else ROUND_FLOOR)*unit)

def net_rr(entry,stop,target):
    if any(number(v) is None for v in (entry,stop,target)) or not 0<stop<entry<target:return None
    buy=entry*1.001*1.001425
    sell=.999*(1-.001425-.003)
    return (target*sell-buy)/(buy-stop*sell)

def decision_evidence(row,pool):
    """Export only evidence that exists; a missing source never becomes neutral proof."""
    def first(names):
        return next((row.get(k) for k in names if number(row.get(k)) is not None),None)
    delta=first(['TDCC千張大戶週變化pp','TDCC千張大戶週變化','千張大戶週變化pp'])
    current=text(row.get('TDCC大戶資料日期')); previous=text(row.get('TDCC大戶前期日期'))
    valid_dates=bool(current and previous and previous.replace('-','')<current.replace('-',''))
    tdcc='MISSING｜缺少TDCC前後期持股差或有效日期，不能判定鎖碼' if delta is None or not valid_dates else f'AVAILABLE｜{previous}至{current}千張大戶持股差{number(delta):+.3f}pp'
    news=text(row.get('H94新聞證據狀態')) or 'MISSING｜未提供個股事件證據'
    e=first(['H103原始Momentum進場','突破確認參考價','突破確認價'])
    s=first(['H103原始Momentum停損','突破後守價','觸發後守價'])
    t=first(['H103原始Momentum目標','突破第一目標','第一壓力價'])
    isolated=text(row.get('市場別'))=='興櫃'
    e,s,t=(None,None,None) if isolated else (tick_price(e,up=True),tick_price(s),tick_price(t))
    rr=net_rr(e,s,t)
    status=('MISSING｜缺少獨立突破價、守價或上方結構目標' if rr is None else
            'BLOCK｜獨立突破計畫成本後RR不足1.50' if rr<1.5 else
            'CONDITIONAL｜僅數值計畫；須量價確認與盤前重驗，非買進許可')
    if pool=='actionable': reason=''
    elif pool=='emerging_watch': reason='興櫃隔離研究'
    else:
        reason=text(row.get('H103未通過原因')) or text(row.get('H79未入選原因'))
        if not reason: reason='缺少H64/H68逐關拒絕碼；未取得正式授權'
        if text(row.get('H99執行狀態')).startswith('BLOCK'):reason += '；'+text(row.get('H99執行狀態'))
    return {'H103版本':VERSION,'H103最終層別':pool.upper(),'H103未通過原因':reason,
            'H103價格處理':'興櫃隔離，未套用上市櫃跳動單位' if isolated else 'A進場/停損/目標向下取合法價；B進場向上取合法價；成本後RR重算',
            'H103計畫有效至':text(row.get('H99目標交易日'))+'｜當日失效條件成立即取消，隔日不得自動沿用',
            'H103TDCC證據':tdcc,'H103新聞證據':news,
            'H103Momentum進場':e,'H103Momentum停損':s,'H103Momentum目標':t,
            'H103Momentum成本後RR':round(rr,4) if rr is not None else None,'H103Momentum狀態':status}

def snapshot_id(row):
    keys=['股票代號','H99市場資料日','H99目標交易日','H99主進場','H99防守停損','H99第一目標','H99成本後RR',
          'H101同層順位','H101推薦優先分','H102同層順位','H102動態優先分','H102動態層級','H103最終層別']
    payload={k:text(row.get(k)) for k in keys}
    return hashlib.sha256(json.dumps(payload,sort_keys=True,ensure_ascii=False).encode()).hexdigest()[:24]

def replay_t1_plan(plan, history):
    """Published A limit-entry plan, valid for its target session only.

    This is OHLC simulation, never broker fill evidence. Stop/target order that
    cannot be proven from a daily bar is marked ambiguous and excluded from
    execution learning. No inference of a successful B breakout from A levels.
    """
    result={'H103績效版本':VERSION,'H103績效口徑':'A限價條件回放，目標日收盤出場；非券商實際成交',
            'H103執行學習可用':False,'H103T1成本後報酬%':None,'H103T1結果':'PENDING',
            'H103路徑歧義':False,'是否納入可執行績效':False,
            '進場觸發狀態':'PENDING｜目標日行情未成熟','進場觸發日期':'',
            '執行基準價':None,'執行價可成交驗證':False}
    target_day=text(plan.get('H99目標交易日'))[:10]
    bars=[r for r in history if text(r.get('日期'))[:10]==target_day]
    if not bars:return result
    row=bars[0]
    e,s,t=[number(plan.get(k)) for k in ('H99主進場','H99防守停損','H99第一目標')]
    o,h,l,c=[number(row.get(k)) for k in ('開盤價','最高價','最低價','收盤價')]
    rr=net_rr(e,s,t)
    if rr is None or rr<1.5 or any(v is None or v<=0 for v in (o,h,l,c)) or not l<=min(o,c)<=max(o,c)<=h:
        result.update({'H103T1結果':'INVALID','進場觸發狀態':'INVALID｜價格計畫或行情無效'})
        return result
    # A split/ex-right adjustment needs a separately versioned plan; do not
    # compare a pre-action limit to post-action raw prices.
    rec_day=text(plan.get('H99市場資料日'))[:10]
    previous=next((r for r in history if text(r.get('日期'))[:10]==rec_day),None)
    if previous:
        pc,pa=number(previous.get('收盤價')),number(previous.get('還原收盤價'))
        ca=number(row.get('還原收盤價'))
        if pc and pa and ca and abs((pa/pc)/(ca/c)-1)>.002:
            result.update({'H103T1結果':'REVALIDATE','進場觸發狀態':'REVALIDATE｜除權息/分割，需新計畫'})
            return result
    if text(plan.get('H99執行狀態')).startswith('BLOCK') or o<=s or l>e or h==l:
        result.update({'H103T1結果':'NO-TRADE','進場觸發狀態':'NO-TRADE｜未觸價、跳空失效或無法驗證成交量排隊'})
        return result
    fill=o if o<=e else e
    from_open=o<=e
    stop_hit=l<=s
    target_hit=h>=t
    ambiguous=target_hit and (stop_hit or not from_open)
    exit_price=s if stop_hit else t if target_hit and from_open else c
    net=(exit_price*.999*(1-.001425-.003)/(fill*1.001*1.001425)-1)*100
    outcome='AMBIGUOUS' if ambiguous else 'WIN' if net>0 else 'LOSS' if net<0 else 'FLAT'
    result.update({'是否納入可執行績效':False,'進場觸發日期':target_day,'執行基準價':fill,
                   '執行價來源':'H103｜日K條件成交模擬','執行價可成交驗證':False,
                   '進場評估路徑':'H103 A限價回測','H103T1成本後報酬%':round(net,4),
                   'H103T1結果':outcome,'H103執行學習可用':not ambiguous,
                   'H103路徑歧義':ambiguous,'H103出場價':exit_price,
                   'H103出場原因':'停損（歧義時保守處理）' if stop_hit else '目標' if target_hit and from_open else '目標日收盤',
                   '進場觸發狀態':'AMBIGUOUS｜已模擬成交；日K先後不明，停用執行學習' if ambiguous else
                       'FILLED-LOSS｜已模擬成交，失守仍列入損益' if net<0 else 'FILLED｜已模擬成交，依成本後損益判定'})
    return result


def merge_final_actionable_frame(source, core):
    """Record only the published actionable pool, preserving full source evidence."""
    import pandas as pd
    final = core.get('actionable', []) if isinstance(core, dict) else []
    final = final.copy() if isinstance(final, pd.DataFrame) else pd.DataFrame(final)
    if final.empty or '股票代號' not in final: return pd.DataFrame()
    source = source if isinstance(source, pd.DataFrame) else pd.DataFrame(source or [])
    lookup = {text(r.get('股票代號')):r for r in source.to_dict('records')}
    return pd.DataFrame([{**lookup.get(text(r.get('股票代號')), {}), **r} for r in final.to_dict('records')])

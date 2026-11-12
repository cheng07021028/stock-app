# -*- coding: utf-8 -*-
"""H153.9: auditable mainstream and repeat quality for NON-TRADING research.

Never infer sector money flow from volume alone; never treat a past recommendation
snapshot as proof of a same-model H153 rank; and never convert research into H126.
All current-data comparisons use the same research date. Missing history/source
is UNKNOWN rather than a bullish or bearish assertion.
"""
from __future__ import annotations
from collections import defaultdict
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from statistics import median
from math import isfinite
import json
import pandas as pd
from godpick_h147_five_gate_review import _code, _day, _s
from godpick_h1531_quality_repair import classify_h109

VERSION='V191-H153.9-MAINSTREAM-REPEAT-AUDIT'
NEW_COLUMNS=(
 'H153原研究排名','H1539主流證據等級','H1539同輪族群相對強度',
 'H1539跨日快照次數','H1539跨日快照來源','H1539新證據狀態',
 'H1539排序理由','H1539主流認證說明',
)


def _number(v):
    if v is None or isinstance(v,bool):return None
    try:
        x=float(str(v).strip().replace(',','').replace('%',''))
        return x if isfinite(x) else None
    except (TypeError,ValueError,OverflowError):return None


def read_local_rotation_snapshots(root, *, max_bytes=8_000_000):
    """Read existing rotation snapshots only, NEVER sync network in Excel export."""
    path=Path(root)/'godpick_rotation_history.json'
    try:
        if not path.is_file() or path.stat().st_size>max_bytes:
            return [],'本機輪動歷史未找到或過大（未經遠端驗證）'
        obj=json.loads(path.read_text(encoding='utf-8-sig'))
        snapshots=obj.get('snapshots',[]) if isinstance(obj,dict) else []
        return ([dict(x) for x in snapshots if isinstance(x,dict)]
                if isinstance(snapshots,list) else []),'既有輪動快照（僅本機讀取、持久性待確認）'
    except (OSError,ValueError,TypeError):
        return [],'輪動歷史讀取失敗（不得當作零次入榜）'


def _peer_breadth(diagnosis, day):
    """Same-session, cross-sectional 5-day peer breadth; NOT proven fund flow."""
    samples=defaultdict(list); codes=set(); duplicates=set()
    for r in diagnosis.to_dict('records'):
        code=_code(r.get('股票代號'))
        if not code or code.startswith(('00','01')) or code in codes:
            if code:duplicates.add(code)
            continue
        codes.add(code)
        if (_day(r.get('H114K線證據日'))!=day or
            _s(r.get('H114K線資料狀態'))!='VALID_OHLCV' or
            (_number(r.get('H114K線有效根數')) or 0)<60):continue
        ret=_number(r.get('H114當前5日漲跌幅%'))
        industry=_s(r.get('類別'))
        amount=_number(r.get('H113當日成交額百萬'))
        lots=_number(r.get('H114當日成交量張'))
        if ret is None or not industry or industry.isdigit() or industry.strip().upper() in {'其他','未分類','未知','OTHERS','UNKNOWN'}:continue
        if amount is None or amount<150 or lots is None or lots<500:continue
        samples[industry].append((code,ret))
    for group in list(samples):
        samples[group]=[(c,r) for c,r in samples[group] if c not in duplicates]
    all_returns=[value for rows in samples.values() for _,value in rows]
    benchmark=median(all_returns) if len(all_returns)>=30 else None
    leaders={}
    if benchmark is None:return leaders,{'market_sample':len(all_returns),'median_ret5':None}
    for industry,rows in samples.items():
        if len(rows)<5:continue
        returns=[r for _,r in rows]
        m=median(returns);positive=sum(x>0 for x in returns)/len(returns)
        # Conservative research hypothesis, NOT an independent mainstream audit.
        support=(m>=benchmark+1.0 and m>0 and positive>=0.60)
        leaders[industry]={'sample':len(rows),'median':m,'breadth':positive,
                           'relative':m-benchmark,'support':support}
    return leaders,{'market_sample':len(all_returns),'median_ret5':benchmark}


def _internal_h108(rec, day):
    phase=_s(rec.get('H108族群生命週期')).upper()
    prior=_day(rec.get('H108族群前次資料日'))
    flow=_number(rec.get('H108族群資金流變化'))
    breadth=_number(rec.get('H108族群廣度變化'))
    confidence=_number(rec.get('H108內部樣本可信度'))
    return bool(day and prior and 0<(day-prior).days<=21 and
                ('IGNITION' in phase or 'EXPANSION' in phase) and
                flow is not None and flow>0 and breadth is not None and breadth>0 and
                confidence is not None and confidence>=50)


def _institutional_cross(rec, day):
    inst=_s(rec.get('H123法人實證狀態'))
    holder=_s(rec.get('H124大戶實證狀態'))
    inst_date=_day(rec.get('H123法人資料日期'))
    holder_date=_day(rec.get('H124大戶資料日期')) or _day(rec.get('TDCC大戶資料日期'))
    ok_inst=bool(inst=='PROVEN_FLOW' and day and inst_date and 0<=(day-inst_date).days<=5)
    ok_holder=bool(holder=='VERIFIED_RISING' and day and holder_date and 0<=(day-holder_date).days<=14)
    return ok_inst or ok_holder


def _rotation_counts(history, day):
    prior=[]
    for item in history or []:
        if not isinstance(item,Mapping):continue
        d=_day(item.get('date'))
        if d and day and d<day and (day-d).days<=16:
            prior.append((d,item))
    byday={d:it for d,it in prior}
    prior=sorted(byday.items(),key=lambda x:x[0])[-5:]
    codes=defaultdict(set)
    for d,item in prior:
        for row in item.get('rows',[]) if isinstance(item.get('rows'),list) else []:
            if isinstance(row,Mapping):
                c=_code(row.get('股票代號'))
                if c:codes[c].add(d.isoformat())
    return {k:len(v) for k,v in codes.items()},len(prior)


def audit_mainstream_repetition(ranked, diagnosis, *, research_day, snapshots=None,
                                history_source='未取得', limit=None):
    """Re-order only A/B research, with tied ranks; preserve C veto and all rows.

    Industry strength derived from same-run peers is explicitly 'internal'. A
    verified mainstream label REQUIRES additional dated institutional/TDCC
    confirmation. Repeats lacking new evidence may be deprioritized but never
    silently removed. Historical rotation snapshots are not H153 rank records.
    """
    frame=ranked.copy(deep=True) if isinstance(ranked,pd.DataFrame) else pd.DataFrame()
    diagnosis=diagnosis if isinstance(diagnosis,pd.DataFrame) else pd.DataFrame()
    day=_day(research_day)
    if frame.empty:
        for c in NEW_COLUMNS:frame[c]=pd.Series(dtype=object)
        return frame,pd.DataFrame([{'檢查項目':'H153.9研究候選','結果':0,'說明':'無候選不造股票'}]),{'count':0}
    if not day:
        raise ValueError('H153.9 無有效研究交易日，不能驗證主流及重複')
    valid={};duplicates=set()
    for rec in diagnosis.to_dict('records'):
        code=_code(rec.get('股票代號'))
        if not code:continue
        if code in valid:duplicates.add(code)
        else:valid[code]=rec
    for code in duplicates:valid.pop(code,None)
    peer,peer_meta=_peer_breadth(diagnosis,day)
    repeats,prior_days=_rotation_counts(snapshots,day)
    rows=[]
    for r in frame.to_dict('records'):
        code=_code(r.get('股票代號'))
        raw=valid.get(code,{})
        same=bool(_day(raw.get('H114K線證據日'))==day and _s(raw.get('H114K線資料狀態'))=='VALID_OHLCV')
        veto=classify_h109(_s(raw.get('H109決策權威')) or _s(r.get('H109風控'))) in {'EXPLICIT_VETO','EVIDENCE_CONFLICT'}
        industry=_s(raw.get('類別')) or _s(r.get('產業'))
        p=peer.get(industry,{}) if same else {}
        internal_h108=bool(same and _internal_h108(raw,day))
        peer_up=bool(p.get('support'))
        cross=bool(internal_h108 and _institutional_cross(raw,day))
        grade='A｜H108跨期＋法人/集保同日佐證（研究）' if cross and not veto else (
            'B｜同輪族群領先線索／待H108或籌碼確認' if peer_up and not veto else (
            'I｜H108內部支持／仍待法人集保驗證' if internal_h108 and not veto else (
            'VETO｜原風控否決保留' if veto else 'U｜主流未認證／資料不足')))
        repeat=repeats.get(code) if prior_days else None
        # Separate *observed* repeat from *unproven* improvement. Never assert
        # that a missing earlier research snapshot means 'new stock'.
        recent_status=(f'近{prior_days}次既有輪動快照同碼出現{repeat or 0}次；非H153研究名次歷史'
                       if prior_days else '缺前輪快照，無法判定是否反覆上榜')
        has_new=bool(cross or peer_up)
        evidence_status=('已驗證本輪族群交叉佐證' if cross else
                         '僅當輪同組相對強度（未證法人流入）' if peer_up else
                         '尚無獨立主流/新增證據（不代表股票必跌）')
        # Do not penalize persistence if same-session sector corroboration exists.
        repeated_stale=bool(repeat is not None and repeat>=2 and not has_new and not veto)
        key=(0 if grade.startswith('A') else 1 if grade.startswith('B') else
             2 if grade.startswith('I') else 3)
        reason=(grade+'；'+recent_status+'；'+evidence_status)
        row=dict(r)
        row.update({'H153原研究排名':r.get('研究排名'),
                    'H1539主流證據等級':grade,
                    'H1539同輪族群相對強度':
                        (f"同輪{p['sample']}檔，5日中位數{p['median']:+.2f}%／對母體{p['relative']:+.2f}pp；上漲廣度{p['breadth']*100:.0f}%" if p else '樣本/日期不足，不可宣稱當期主流'),
                    'H1539跨日快照次數':repeat if prior_days else None,
                    'H1539跨日快照來源':history_source if prior_days else '輪動歷史不可讀／不代表今日新上榜',
                    'H1539新證據狀態':evidence_status,
                    'H1539排序理由':reason,
                    'H1539主流認證說明':'同輪相對強度僅描述性證據；H108/法人/集保來源必須逐項核對；不代表預測或買進',
                    '__rank_grade':key,'__repeat_stale':int(repeated_stale)})
        # Existing H109 /H126/ scores stay unchanged; integrate MAINSTREAM
        # directly into the existing research explanation and Excel report.
        row['反證與待補']=(reason+'；'+_s(r.get('反證與待補')))[:2400]
        rows.append(row)
    tier=lambda r:({'A':0,'B':1,'C':2}.get(_s(r.get('排名等級'))[:1],3))
    def sortkey(r):
        # Stable in-tier priorities: mainstream verification > unsupported
        # repeats > existing score. Code only stabilizes equal display order.
        return (tier(r),int(r['__rank_grade']),int(r['__repeat_stale']),
                -(_number(r.get('研究證據分')) or 0),_code(r.get('股票代號')))
    rows.sort(key=sortkey)
    last=None;rank=0
    for i,r in enumerate(rows,1):
        identity=sortkey(r)[:-1]
        if identity!=last:rank=i;last=identity
        r['研究排名']=rank
        r.pop('__rank_grade',None);r.pop('__repeat_stale',None)
    if limit is not None:rows=rows[:max(1,int(limit))]
    result=pd.DataFrame(rows)
    grade_counts={k:int(result['H1539主流證據等級'].str.startswith(k).sum()) for k in ('A','B','I','U','VETO')}
    summary=pd.DataFrame([
      {'檢查項目':'H153.9主流研究跨期＋籌碼交叉','結果':grade_counts['A'],'說明':'H108跨期兩項內部正向，並需H123/TDCC已核實日期；研究不是H126買進'},
      {'檢查項目':'H153.9同輪產業相對強度待核','結果':grade_counts['B'],'說明':'同一交易日、至少5檔同業，對照當輪中位數／上漲廣度；非官方族群資金流'},
      {'檢查項目':'H153.9主流未認證股票','結果':grade_counts['U'],'說明':'不能把未驗證直接稱作非主流或已證主流'},
      {'檢查項目':'H153.9歷史輪動快照數','結果':prior_days,'說明':history_source+'；非H153研究排名完整保存'},
      {'檢查項目':'H153.9跨日重複且缺新佐證','結果':sum(bool(_number(r.get('H1539跨日快照次數')) and _number(r.get('H1539跨日快照次數'))>=2 and r['H1539主流證據等級'].startswith(('I','U'))) for r in rows),'說明':'研究排名延後、不刪股票、不解除H126；歷史缺證不能視為沒有重複'},
      {'檢查項目':'H153.9同輪廣度有效樣本','結果':peer_meta['market_sample'],'說明':'同日有效H114且當日成交額>=1.5億元、500張；研究池範圍不是全市場保證'},
    ])
    return result,summary,{'count':len(rows),'grades':grade_counts,'rotation_days':prior_days,'peer':peer_meta}

__all__=['VERSION','NEW_COLUMNS','audit_mainstream_repetition','read_local_rotation_snapshots']

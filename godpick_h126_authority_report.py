# -*- coding: utf-8 -*-
"""V191-H126: one authoritative manager research view, no trading permissions.

Conservative, source-specific gate. H102 scores/legacy previews NEVER grant
research certification. H123 daily flow + H124 TDCC are independently proven.
No HTTP and no persistence writes during report building.
"""
from __future__ import annotations
from typing import Any
import math
import re
from datetime import datetime
import pandas as pd

VERSION = 'V191-H126.1'
GATED_COLUMNS = ['H126順位','股票代號','股票名稱','市場別','類別','H126權威層別',
    'H126能否買進','H126認證資格','H126最後研究分','H126策略來源',
    'H113流動性合格','H113大戶鎖碼合格','H113法人連買合格',
    'H123法人實證狀態','H124大戶實證狀態','H126風險狀態',
    'H126缺失與處置','H126研究資訊截止日','H126證據來源','H126原候選池']
BLOCKED_AUTH = {'FADE_VETO','SHOCK_QUARANTINE','COOLDOWN_QUARANTINE',
                'FUTURE_DATA_VETO','FAST_SHOCK_RECHECK'}


def _t(v: Any) -> str:
    if v is None: return ''
    s=str(v).strip()
    return '' if s.lower() in ('nan','nat','none','null','<na>') else s


def _number(v: Any) -> float | None:
    try:
        if isinstance(v, bool): return None
        x=float(_t(v).replace(',',''))
        return x if math.isfinite(x) else None
    except (ValueError,TypeError): return None


def _code(v: Any) -> str:
    text=_t(v)
    if text.endswith('.0') and text[:-2].isdigit(): text=text[:-2]
    return text if len(text)==4 and text.isdigit() else ''


def _rows(src: dict, key: str):
    df=src.get(key)
    if not isinstance(df,pd.DataFrame) or df.empty or '股票代號' not in df.columns:
        return []
    return df.to_dict('records')


def _has_veto(r: dict)->tuple[bool,str]:
    auth=_t(r.get('H109決策權威'))
    if auth in BLOCKED_AUTH:return True, 'H109風險否決：'+auth
    if _t(r.get('H110證據狀態')).startswith('CONFLICT'):return True,'H110來源證據衝突'
    if _t(r.get('H105今日強勢排除'))=='是':return True,'H105已發動／過熱排除'
    if _t(r.get('H113入榜資格'))=='BLOCKED':return True,'H113未通過硬門檻'
    if _t(r.get('市場別')) not in ('上市','上櫃'):return True,'非上市櫃，僅得隔離觀察'
    return False,''


def _facts(r:dict, origins:set[str])->dict:
    st113=_t(r.get('H113入榜資格'))
    flow=_t(r.get('H123法人實證狀態'))
    tdcc=_t(r.get('H124大戶實證狀態'))
    author=_t(r.get('H109決策權威'))
    missing=[]
    if st113 != 'QUALIFIED':missing.append('H113完整准入未通過：'+(st113 or '無資料'))
    if flow != 'PROVEN_FLOW':missing.append('H123官方逐日連買未證實：'+(flow or '未接入'))
    if tdcc != 'VERIFIED_RISING':missing.append('H124官方兩週TDCC正向集中未證實：'+(tdcc or '未接入'))
    # A status string alone is insufficient. Require verifiable source hashes,
    # five complete daily sessions and a matching source time window.
    sha=lambda x: bool(re.fullmatch(r'[0-9a-fA-F]{64}',_t(r.get(x))))
    if not sha('H123來源雜湊') or _number(r.get('H123法人逐日有效天數'))!=5:
        missing.append('H123缺官方逐日來源雜湊或完整5日紀錄')
    if not sha('H124大戶來源雜湊') or (_number(r.get('H124大戶變化pp')) or 0)<0.05:
        missing.append('H124缺兩週原始來源雜湊或大戶週增未達0.05pp')
    cutoff=_t(r.get('H110研究資訊截止日'))[:10]
    captured=_t(r.get('H123研究實際時間'))[:10]
    if not captured or not cutoff or captured>cutoff:
        missing.append('H123證據取得日期晚於研究截止日或未提供時點')
    if author != 'RESEARCH_ELIGIBLE':missing.append('H109預發動權威不合格：'+(author or '未提供'))
    if _t(r.get('H105今日強勢排除'))=='是':missing.append('已發動／過熱，禁止當成未來黑馬')
    if _t(r.get('H110證據狀態')).startswith('CONFLICT'):missing.append('H110資料衝突')
    if _t(r.get('H105發動階段')).startswith('IG'):missing.append('個股已發動，非預發動')
    if _t(r.get('市場別')) not in ('上市','上櫃'):missing.append('市場類型不合')
    if _t(r.get('H113缺失原因')) and st113!='QUALIFIED':
        missing.append('H113：'+_t(r.get('H113缺失原因'))[:160])
    certified=(not missing and st113=='QUALIFIED' and flow=='PROVEN_FLOW'
               and tdcc=='VERIFIED_RISING' and author=='RESEARCH_ELIGIBLE')
    veto,reasons=_has_veto(r)
    if veto:
        classification='禁止新倉／隔離觀察'
    elif certified:
        classification='A｜認證黑馬研究（非買進）'
    elif st113=='WATCH_ONLY' or any(o in origins for o in ('research','h114_parallel_watch','waiting')):
        classification='B｜待驗證觀察（非買進）'
    else:
        classification='C｜稽核候選（非推薦）'
    # Only originating Formals have any trade-approval authority. H126 never grants it.
    score=next((_number(r.get(x)) for x in (
        'H110最終研究優先分','H114研究優先分','H105黑馬預發動分')
        if _number(r.get(x)) is not None),None)
    reason=('；'.join(dict.fromkeys(missing)) or '來源齊備但仍需原正式交易計畫及當日盤前重驗')
    if veto:reason=reasons+'；'+reason
    return {
        '股票代號':_code(r.get('股票代號')),
        '股票名稱':_t(r.get('股票名稱')),
        '市場別':_t(r.get('市場別')),
        '類別':_t(r.get('類別')),
        'H126權威層別':classification,
        'H126能否買進':'禁止以研究榜買進｜僅01正式交易計畫具備核准權',
        'H126認證資格':'PASS' if certified else 'BLOCKED' if veto else 'PENDING_EVIDENCE',
        'H126最後研究分':score,
        'H126策略來源':_t(r.get('H114主導策略')) or '既有研究／風險稽核',
        'H113流動性合格':_t(r.get('H113流動性合格')) or '未核對',
        'H113大戶鎖碼合格':_t(r.get('H113大戶鎖碼合格')) or '未核對',
        'H113法人連買合格':_t(r.get('H113法人連買合格')) or '未核對',
        'H123法人實證狀態':flow or 'NOT_DEPLOYED_OR_NO_DATA',
        'H124大戶實證狀態':tdcc or 'NOT_DEPLOYED_OR_NO_DATA',
        'H126風險狀態':reasons if veto else '未見硬否決（不等於准買）',
        'H126缺失與處置':reason[:440],
        'H126研究資訊截止日':_t(r.get('H110研究資訊截止日')) or _t(r.get('H123研究實際時間')),
        'H126證據來源': ('H113原始驗證＋H123法人逐日＋H124 TDCC週證據；'
             '三來源缺一不可，H102高分不得授權'),
        'H126原候選池':'、'.join(sorted(origins)),
    }


def _merge_sources(tables:dict):
    # H113 diagnostic Audit is source of truth when it carries recent audited
    # columns; legacy priority and old B+/A-/H57 never enter this contract.
    order=('h112_audit_radar','h114_parallel_watch','waiting','research',
           'blackhorse_overview','audit','actionable')
    candidates={}
    for name in order:
        for r in _rows(tables,name):
            code=_code(r.get('股票代號'))
            if not code:continue
            entry=candidates.setdefault(code,{'values':{},'origins':set(),'votes':[]})
            entry['origins'].add(name)
            entry['votes'].append(r)
            for k,v in r.items():
                if _t(v):entry['values'][k]=v
    for entry in candidates.values():
        # Conflicting rows are never sufficient for PASS. A newer positive row
        # must not silently clear an explicit negative H113/H109 on another pool.
        vs=entry['votes'];combined=entry['values']
        for k,bad in (('H113入榜資格','BLOCKED'),('H113法人連買合格','否'),
                      ('H113大戶鎖碼合格','否'),('H113流動性合格','否')):
            if any(_t(row.get(k))==bad for row in vs):combined[k]=bad
        veto=[r for r in vs if _has_veto(r)[0]]
        if veto:
            row=veto[0]
            for k in ('H109決策權威','H105今日強勢排除','H110證據狀態','H105發動階段','市場別'):
                if _t(row.get(k)):combined[k]=row[k]
        # Distinct positive official statuses must coexist in the same row
        # (or be independently accompanied by source digests) to avoid joins.
        if not any(_t(row.get('H123法人實證狀態'))=='PROVEN_FLOW'
                   and _t(row.get('H124大戶實證狀態'))=='VERIFIED_RISING' for row in vs):
            combined['H126分離證據未聯合驗證']='是'
    return candidates


def build_authority(tables:dict[str,Any], *,watch_limit:int=18)->dict[str,pd.DataFrame]:
    data=_merge_sources(tables or {})
    rows=[]
    for code,entry in data.items():
        f=_facts(entry['values'],entry['origins'])
        if entry['values'].get('H126分離證據未聯合驗證')=='是' and f['H126認證資格']=='PASS':
            f['H126認證資格']='PENDING_EVIDENCE'
            f['H126權威層別']='B｜待驗證觀察（非買進）'
            f['H126缺失與處置']='H123/H124證據出現在不同來源列；尚未聯合核對，禁止認證'
        rows.append(f)
    priority={'A｜認證黑馬研究（非買進）':0,'B｜待驗證觀察（非買進）':1,
              'C｜稽核候選（非推薦）':2,'禁止新倉／隔離觀察':3}
    rows.sort(key=lambda r:(priority.get(r['H126權威層別'],9),
             -(r['H126最後研究分'] or -1),r['股票代號']))
    counts={s:sum(r['H126權威層別']==s for r in rows) for s in priority}
    promoted=[r for r in rows if r['H126認證資格']=='PASS']
    watch=[r for r in rows if r['H126權威層別'].startswith('B｜')][:max(0,watch_limit)]
    final=promoted+watch
    if not final:
        final=[{'股票代號':'','股票名稱':'本輪沒有完成全部官方證據認證的黑馬；不可依舊版高分進場',
                'H126權威層別':'無合格推薦','H126能否買進':'禁止買進',
                'H126認證資格':'NO_QUALIFIED','H126缺失與處置':'請看 H126資料缺口及05原始稽核；研究候選不等於交易授權'}]
    frame=pd.DataFrame(final)
    frame.insert(0,'H126順位',[i if r.get('股票代號') else None for i,r in enumerate(final,1)])
    for k in GATED_COLUMNS:
        if k not in frame.columns:frame[k]=''
    frame=frame[GATED_COLUMNS]
    actionable=tables.get('actionable') if isinstance(tables,dict) else None
    count_actionable=0
    if isinstance(actionable,pd.DataFrame) and '股票代號' in actionable.columns:
        count_actionable=sum(bool(_code(v)) for v in actionable['股票代號'])
    missing_flow=sum(not _t(entry['values'].get('H123法人實證狀態')) for entry in data.values())
    missing_tdcc=sum(not _t(entry['values'].get('H124大戶實證狀態')) for entry in data.values())
    details=[
        ('H126版本',VERSION),('原候選去重數',len(data)),
        ('A認證黑馬研究',len(promoted)),('B待驗證觀察',counts['B｜待驗證觀察（非買進）']),
        ('C稽核候選',counts['C｜稽核候選（非推薦）']),
        ('禁止新倉／隔離觀察',counts['禁止新倉／隔離觀察']),
        ('原正式可執行表個股數（不代表H126批准）',count_actionable),
        ('H123法人證據無狀態',missing_flow),('H124大戶證據無狀態',missing_tdcc),
        ('H126正式買進權限','LOCKED；只能依01原正式交易計畫＋盤前驗證'),
        ('H102舊總覽說明','僅歷史族群／研究排序；與H126認證不一致時一律以H126風險資格為準'),
        ('認證原則','H113 QUALIFIED ＋ H123 PROVEN_FLOW ＋ H124 VERIFIED_RISING ＋ H109 RESEARCH_ELIGIBLE；且無過熱／證據衝突'),
        ('官方資料缺失規則','法人／TDCC日期或原始封存缺口一律 PENDING，不補分、不回填歷史、不冒充交易權限'),
    ]
    health=pd.DataFrame([{'項目':k,'數值':v} for k,v in details])
    return {'h126_authority':frame,'h126_health':health,
            'h126_legacy_priority':tables.get('priority_overview',pd.DataFrame()) if isinstance(tables,dict) else pd.DataFrame()}


def attach_authority(tables:dict[str,Any], **kwargs)->dict[str,Any]:
    out=dict(tables or {})
    out.update(build_authority(out,**kwargs))
    return out

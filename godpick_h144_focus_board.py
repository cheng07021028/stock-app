# -*- coding: utf-8 -*-
"""H146: evidence-first, two-sheet manager watch board.

Does NOT create trade authority, manufacture scores, make HTTP requests, or
write permanent data. A displayed stock is NOT a recommendation. Equal scores
are ties and cannot acquire distinct priority merely from stock-code ordering.
"""
from __future__ import annotations
import math
import re
from collections import Counter
from typing import Any, Mapping
import pandas as pd
from godpick_h147_five_gate_review import evaluate_five_gates

VERSION = 'V191-H146.0-RANKING-RELIABILITY'
COLS = [
    '觀察順位','股票代號','股票名稱','產業','H110研究分','排序可信度',
    '分數依據','研究型態','H110證據狀態','H109風控','H123法人','H124大戶',
    'H126正式資格','研究資料日','下一個必查條件','買進資格','榜單可信度',
]
BLOCKING_AUTH = {'FADE_VETO','SHOCK_QUARANTINE','COOLDOWN_QUARANTINE',
                 'FUTURE_DATA_VETO','FAST_SHOCK_RECHECK'}


def _s(v: Any) -> str:
    if v is None: return ''
    try:
        if bool(pd.isna(v)): return ''
    except (TypeError, ValueError): pass
    return str(v).strip()


def _frame(value: Any) -> pd.DataFrame:
    return value if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _count(v: Any) -> int:
    try:
        n = int(v)
        return n if n >= 0 else 0
    except (TypeError, ValueError, OverflowError): return 0


def _score(v: Any) -> float | None:
    try:
        n = float(_s(v).replace(',',''))
        return round(n, 2) if math.isfinite(n) and 0 <= n <= 100 else None
    except (TypeError, ValueError, OverflowError): return None


def _code(v: Any) -> str:
    text = _s(v).removesuffix('.0')
    return text if re.fullmatch(r'[1-9][0-9]{3}',text) else ''


def _metric(frame: pd.DataFrame, key: str):
    if frame.empty or not {'項目','數值'}.issubset(frame.columns): return None
    a=frame.loc[frame['項目'].astype(str)==key,'數值']
    return a.iloc[-1] if not a.empty else None


def _scope_issues(state: Mapping[str,Any], audit_count:int) -> list[str]:
    scan, diag, final, received = (_count(state.get(k)) for k in
        ('scan_count','diagnosis_count','reported_final','export_count'))
    issues=[]
    if scan == 0 and (diag or final or received): issues.append('掃描數為0但後續階段有資料')
    if diag == 0 and (final or received): issues.append('診斷數為0但作戰或主管有資料')
    if scan and diag > scan: issues.append('診斷數超過掃描數')
    if diag and final > diag: issues.append('作戰候選超過診斷母體')
    if diag and received > diag: issues.append('主管輸入超過診斷母體')
    if state.get('scope_incomplete'): issues.append('同輪資料交接未獲完整驗證')
    if audit_count and not any((scan,diag,final,received)):
        issues.append('稽核有股票但本輪掃描漏斗沒有可核實筆數')
    if audit_count and received and audit_count > received: issues.append('稽核股票數超過聲稱主管輸入')
    return list(dict.fromkeys(issues))


def _authority_map(frame:pd.DataFrame) -> tuple[dict[str,dict],list[str]]:
    """Reject contradictory duplicates; never let a last-row positive win."""
    result={}; conflicts=set()
    if frame.empty or '股票代號' not in frame: return result,[]
    for r in frame.to_dict('records'):
        c=_code(r.get('股票代號'))
        if not c:continue
        old=result.get(c)
        if old is None:
            result[c]=r
        elif any(_s(old.get(k))!=_s(r.get(k)) for k in
                 ('H126能否買進','H126權威層別','H126認證資格')):
            conflicts.add(c)
    for code in conflicts:
        old=result[code].copy()
        old['H126能否買進']='否'
        old['H126權威層別']='同代號H126資料矛盾／隔離'
        result[code]=old
    return result,sorted(conflicts)


def _one_row(raw:dict, authority:dict | None, *, scoped:bool) ->dict:
    c=_code(raw.get('股票代號'))
    stage=_s(raw.get('H105發動階段'))
    if 'ALREADY-IGNITED' in stage or _s(raw.get('H105今日強勢排除'))=='是':
        kind='已發動／不當預發動'
    elif 'BH1' in stage or 'BH2' in stage:kind='預發動待驗證'
    else: kind='資料待補／尚無分型'
    risk=_s(raw.get('H109決策權威')) or 'UNKNOWN'
    institution=_s(raw.get('H123法人實證狀態')) or 'UNKNOWN'
    holdings=_s(raw.get('H124大戶實證狀態')) or 'UNKNOWN'
    h110=_s(raw.get('H110證據狀態')) or '未載明'
    score=_score(raw.get('H110最終研究優先分'))
    day=_s(raw.get('H110研究資訊截止日')) or _s(raw.get('H126研究資訊截止日')) or '未載明'
    a=authority or {}
    layer=_s(a.get('H126權威層別')) or '未列入H126合格決策'
    if authority and _s(a.get('H126能否買進')) in ('是','允許買進','可買進'):
        # This board never overrides H109/H123/H124. The canonical H126 sheet
        # remains the only place to determine formal entitlement.
        layer += '｜請核對H126原表'
    tasks=[]
    if risk and risk not in ('ALLOW','PASS','NONE'):
        tasks.append('查H109否決與H108前後期證據')
    if institution!='PROVEN_FLOW':tasks.append('查H123法人日期與原始來源')
    if holdings!='VERIFIED_RISING':tasks.append('查H124不同週次與截止日')
    if _s(raw.get('H113流動性合格'))!='是':tasks.append('核對成交金額／量價')
    if day=='未載明':tasks.append('核對真實研究資料日')
    # H147: five orthogonal observations. They NEVER override the original
    # research score, H109 risk or H126 trading authority. Explicit '待補證'
    # is vital: a missing TDCC source is not proof that institutions sold.
    five = evaluate_five_gates(raw, research_day=day if day != '未載明' else None)
    labels = {'成交量能':'量', '起漲位置':'起', '強勢族群':'群', '大戶鎖碼':'戶', '技術結構':'技'}
    five_short = '｜'.join(f'{abbr}:{five[full]}' for full,abbr in labels.items())
    return {
        '股票代號':c, '股票名稱':_s(raw.get('股票名稱')),
        '產業':_s(raw.get('類別')), 'H110研究分':score,
        '排序可信度':'待判斷', '分數依據':'H110當輪數值（未認證交易優勢）' if score is not None else '無有效H110分數',
        '研究型態':kind,'H110證據狀態':h110,'H109風控':risk,
        'H123法人':institution,'H124大戶':holdings,'H126正式資格':layer,
        '研究資料日':day,'下一個必查條件':'五關｜'+five_short+'；查核：'+('；'.join(tasks[:2]) or five['優先查核']),
        '買進資格':'禁止依本榜買進',
        '榜單可信度':'局部／不完整母體' if scoped else '官方資格尚須核對',
    }


def build_focus_board(tables:Mapping[str,Any],state:Mapping[str,Any],*,limit:int=12,all_rows:bool=False):
    """A research-only display; no false ordinal rank when all scores tie.

    Ranks use competition ranking 1,1,3 when distinguishable.  A sole score
    may be shown as 1 of known rows; NEVER assert it is top of the whole market.
    Duplicate contradictory scored rows are quarantined, not cherry-picked.
    """
    tables=tables if isinstance(tables,Mapping) else {}
    state=state if isinstance(state,Mapping) else {}
    audit=_frame(tables.get('audit'))
    authority=_frame(tables.get('h126_authority'))
    health=_frame(tables.get('h126_health'))
    issues=_scope_issues(state,len(audit))
    scoped=bool(issues or _s(state.get('status'))!='SCOPE_RECONCILED_FOR_DISPLAY')
    a_map,authority_conflicts=_authority_map(authority)
    if authority_conflicts:
        issues.append('H126同代號決策衝突：'+','.join(authority_conflicts[:6]))
    rows_by_code={}
    if not audit.empty and '股票代號' in audit:
        for raw in audit.to_dict('records'):
            c=_code(raw.get('股票代號'))
            if not c:continue
            rows_by_code.setdefault(c,[]).append(_one_row(raw,a_map.get(c),scoped=scoped))
    candidates=[];conflict_codes=[]
    for c,rr in rows_by_code.items():
        r=rr[0].copy()
        numeric={x['H110研究分'] for x in rr if x['H110研究分'] is not None}
        if len(numeric)>1:
            r['H110研究分']=None
            r['分數依據']='同代號分數衝突：隔離，不挑選較高分'
            r['排序可信度']='同代號資料衝突'
            conflict_codes.append(c)
        elif r['H110研究分'] is None and len(numeric)==1:
            r['H110研究分']=next(iter(numeric))
            r['分數依據']='H110當輪數值（未認證交易優勢）'
        risks={x['H109風控'] for x in rr}
        if len(risks)>1:
            r['H109風控']='資料衝突／需稽核（不解除否決）'
            r['排序可信度']='同代號資料衝突'
            r['H110研究分']=None
            r['分數依據']='同代號風控衝突：隔離，不建立名次'
            if c not in conflict_codes:conflict_codes.append(c)
        candidates.append(r)
    candidates.sort(key=lambda r:(r['H110研究分'] is None,
                                  -(r['H110研究分'] or 0),r['股票代號']))
    score_counts=Counter(r['H110研究分'] for r in candidates if r['H110研究分'] is not None)
    total_scored=sum(score_counts.values()); distinct=len(score_counts)
    all_equal=total_scored>1 and distinct==1
    missing=0
    for idx,r in enumerate(candidates):
        val=r['H110研究分']
        if r['排序可信度']=='同代號資料衝突':
            r['觀察順位']='資料衝突／不排序'
        elif val is None:
            r['觀察順位']='無分／不排序';r['排序可信度']='缺少有效分數';missing+=1
        elif all_equal:
            r['觀察順位']='並列／無法區分'
            r['排序可信度']='全部同分；沒有有效優先序'
        else:
            higher=sum(n for s,n in score_counts.items() if s>val)
            rank=1+higher
            r['觀察順位']=f'並列第{rank}' if score_counts[val]>1 else rank
            r['排序可信度']='局部相對研究序（非買進）' if scoped else '同輪相對研究序（非買進）'
    buyable=sum(1 for a in a_map.values() if _s(a.get('H126能否買進')) in ('是','允許買進','可買進'))
    h126_health_count=_metric(health,'H126授權進場檔數')
    if h126_health_count is not None and _count(h126_health_count)!=buyable:
        issues.append('H126摘要數與實際合格個股不一致')
    displayed=(candidates if all_rows else candidates[:max(1,min(20,_count(limit) or 12))])
    info={
        'scope_status':_s(state.get('status')) or 'SCOPE_NOT_PROVEN',
        'scanned':_count(state.get('scan_count')),
        'diagnosed':_count(state.get('diagnosis_count')),
        'final':_count(state.get('reported_final')),
        'received':_count(state.get('export_count')),
        'audit_count':len(audit),'authority_count':len(a_map),
        'h126_buyable':buyable,
        'score_basis':'H110原始分僅供研究；同分並列，不依股票代號強行分名次',
        'scope_issues':list(dict.fromkeys(issues)),
        'score_distinct':distinct,'valid_score_rows':total_scored,
        'all_scores_tied':all_equal,'conflict_codes':sorted(conflict_codes),
        'ranked':len(displayed),'ranking_usable':bool(distinct>1 and not issues and len(a_map)>0),
    }
    if issues:
        info['warning']='資料母體未通過：'+'；'.join(info['scope_issues'][:3])+'。本表只能作為資料查核線索，不得宣稱全市場排名。'
    else:
        info['warning']='此榜僅為研究相對排序，非交易推薦；仍須核實官方證據。'
    if all_equal: info['warning']+=' 全部有效研究分相同，不具個股優先區分能力。'
    if not a_map:info['warning']+=' H126正式表未提供可識別合格個股，不能把觀察名單當成推薦。'
    if conflict_codes:info['warning']+=f' {len(conflict_codes)}檔同代號資料衝突已隔離。'
    if not total_scored:info['warning']+=' 無有效H110分數，不建立虛假的排序。'
    return pd.DataFrame(displayed,columns=COLS),info

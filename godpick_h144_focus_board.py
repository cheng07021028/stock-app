# -*- coding: utf-8 -*-
"""H144 focused research watchlist: one comparable score, no fabricated stock names.

Fail-closed: incomplete full-scan core means a ranking of PRESENT rows only.
Never promote H102 legacy scores, FADE veto, or missing H123/H124 proofs to buys.
"""
from __future__ import annotations
import math
import re
from typing import Any, Mapping
import pandas as pd

VERSION = 'V191-H144.1-FOCUSED-WATCHLIST'
COLS = ['觀察順位','股票代號','股票名稱','產業','H110研究分','分數依據','研究型態',
        'H109風控','H123法人','H124大戶','下一個必查條件','買進資格','榜單可信度']


def _s(v: Any) -> str:
    if v is None: return ''
    try:
        if bool(pd.isna(v)): return ''
    except (TypeError, ValueError): pass
    return str(v).strip()


def _frame(value: Any) -> pd.DataFrame:
    return value if isinstance(value, pd.DataFrame) else pd.DataFrame()


def _score(v: Any):
    try:
        v = float(str(v).replace(',','').strip())
        return round(v, 2) if math.isfinite(v) and 0 <= v <= 100 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _metric(df: pd.DataFrame, name: str):
    if df.empty or not {'項目','數值'}.issubset(df.columns): return None
    rows = df.loc[df['項目'].astype(str)==name,'數值']
    return rows.iloc[-1] if not rows.empty else None


def build_focus_board(tables: Mapping[str,Any], state: Mapping[str,Any], *, limit: int=12):
    """Return (ranked rows, metadata) from the same already-decoded H79/H126 core.

    Data basis: H110 research score for ALL rows.  Unscored candidates never
    receive made-up numbers; if one-row fallback, #1 means only 1 of 1 known.
    """
    tables = tables if isinstance(tables,Mapping) else {}
    state = state if isinstance(state,Mapping) else {}
    audit = _frame(tables.get('audit'))
    authority = _frame(tables.get('h126_authority'))
    health = _frame(tables.get('h126_health'))
    info = {
        'scope_status':_s(state.get('status')) or 'SCOPE_NOT_PROVEN',
        'scanned':int(state.get('scan_count') or 0),
        'diagnosed':int(state.get('diagnosis_count') or 0),
        'final':int(state.get('reported_final') or 0),
        'received':int(state.get('export_count') or 0),
        'h126_buyable':0,
        'score_basis':'H110最終研究優先分（只比較同一來源）',
        'warning':'此為本輪已取得的股票觀察順序，並非全市場強勢股排名或買進許可。',
    }
    if not authority.empty and 'H126能否買進' in authority:
        info['h126_buyable']=int(authority['H126能否買進'].map(_s).isin(['是','允許買進','可買進']).sum())
    if not info['h126_buyable']:
        b=_metric(health,'H126授權進場檔數')
        if b is not None:
            try: info['h126_buyable'] = max(0,int(float(b)))
            except (TypeError,ValueError): pass
    if state.get('scope_incomplete'):
        info['warning']='嚴重：本輪完整候選母體未送到主管核心；只能排序已收到的局部股票，不可宣稱全市場只有這些機會。'
    elif info['scope_status']!='SCOPE_RECONCILED_FOR_DISPLAY':
        info['warning']='本輪母體／資料日尚未完整核對；以下只供補證觀察，不可視為正式推薦。'
    rows=[]
    if not audit.empty and {'股票代號','H110最終研究優先分'}.issubset(audit.columns):
        for row in audit.to_dict('records'):
            code=_s(row.get('股票代號'))
            if not re.fullmatch(r'[1-9][0-9]{3}',code):continue
            score=_score(row.get('H110最終研究優先分'))
            stage=_s(row.get('H105發動階段'))
            blocked=_s(row.get('H109決策權威'))
            org=_s(row.get('H123法人實證狀態')) or 'UNKNOWN'
            holder=_s(row.get('H124大戶實證狀態')) or 'UNKNOWN'
            if 'ALREADY-IGNITED' in stage or _s(row.get('H105今日強勢排除'))=='是':
                typ='已發動觀察（非預發動）'
            elif 'BH1' in stage or 'BH2' in stage:
                typ='預發動研究（未認證）'
            else:typ='待分類／補證'
            tasks=[]
            if blocked and blocked not in ('ALLOW','PASS','NONE'):
                tasks.append('檢核'+blocked+'及H108跨期證據')
            if org!='PROVEN_FLOW':tasks.append('H123法人逐日證據')
            if holder!='VERIFIED_RISING':tasks.append('H124兩週週資料')
            if _s(row.get('H113流動性合格'))!='是':tasks.append('確認流動性')
            if _s(row.get('H113主升起漲合格'))!='是':tasks.append('起漲條件')
            rows.append({'股票代號':code,'股票名稱':_s(row.get('股票名稱')),'產業':_s(row.get('類別')),
                         'H110研究分':score,'分數依據':'H110同輪研究分' if score is not None else '無可比研究分',
                         '研究型態':typ,'H109風控':blocked or '未驗證','H123法人':org,'H124大戶':holder,
                         '下一個必查條件':'；'.join(tasks[:3]) or '查證當輪完整行情與資料時點',
                         '買進資格':'禁止依本榜買進',
                         '榜單可信度':'局部母體' if state.get('scope_incomplete') else '待官方驗證'})
    # Do not reanimate old H102 scores, nor grade different score families as one list.
    by_code={}
    for r in rows:
        code=r['股票代號']; old=by_code.get(code)
        if old is None or (old['H110研究分'] is None and r['H110研究分'] is not None):by_code[code]=r
    ranked=sorted(by_code.values(), key=lambda r:(r['H110研究分'] is None,
        -(r['H110研究分'] if r['H110研究分'] is not None else 0),r['股票代號']))[:max(1,min(20,limit))]
    for i,r in enumerate(ranked,1):r['觀察順位']=i
    if not ranked:
        info['warning']+=' 本輪沒有可比較的H110股票分數，不以H102舊榜湊數。'
    info['ranked']=len(ranked)
    return pd.DataFrame(ranked,columns=COLS),info

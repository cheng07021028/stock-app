# -*- coding: utf-8 -*-
"""V191-H129 | Wall Street manager audit (paper research, no buy permissions).

This layer NEVER changes Formal qualification, market history, or persistence.
It distinguishes missing evidence from vetoed trades, and quarantines implausible
legacy T+1 comparisons instead of allowing them into valid performance means.
"""
from __future__ import annotations
import math
from datetime import date
from typing import Any, Mapping
import pandas as pd

VERSION = 'V191-H129.0'
MAX_SINGLE_SESSION_RETURN_ABS_PCT = 35.0
MAX_SINGLE_SESSION_ALPHA_ABS_PP = 35.0
HARD_VETO = {'FADE_VETO', 'SHOCK_QUARANTINE', 'COOLDOWN_QUARANTINE',
             'FUTURE_DATA_VETO', 'FAST_SHOCK_RECHECK'}


def _str(v: Any) -> str:
    if v is None: return ''
    text = str(v).strip()
    return '' if text.lower() in ('nan', 'nat', 'none', 'null', '<na>') else text


def _num(v: Any) -> float | None:
    try:
        if isinstance(v, bool): return None
        x = float(_str(v).replace(',', ''))
        return x if math.isfinite(x) else None
    except (ValueError, TypeError):
        return None


def _date(v: Any) -> date | None:
    try: return date.fromisoformat(_str(v)[:10])
    except (ValueError, TypeError): return None


def audit_t1_history(source: pd.DataFrame | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Annotate T+1 truth, preserving original numbers for traceability.

    H103 learning flag stays authoritative: legacy rows are never certified by
    mere WIN/LOSS or inclusion flag. Extreme values cannot count as valid alpha.
    """
    result = source.copy(deep=True) if isinstance(source, pd.DataFrame) else pd.DataFrame()
    diagnostics = []
    for _, row in result.iterrows():
        issues = []
        day, t1 = _date(row.get('推薦日期')), _date(row.get('隔日日期'))
        if day is None or t1 is None or day >= t1 or day.weekday() >= 5 or t1.weekday() >= 5:
            issues.append('INVALID_SESSION_DATE')
        ret = _num(row.get('隔日候選漲跌%'))
        alpha = _num(row.get('Selection Alpha%'))
        if (ret is not None and abs(ret) > MAX_SINGLE_SESSION_RETURN_ABS_PCT) or (
            alpha is not None and abs(alpha) > MAX_SINGLE_SESSION_ALPHA_ABS_PP
        ):
            issues.append('EXTREME_RETURN_OR_ALPHA')
        basis = _str(row.get('H103績效口徑'))
        flag = _str(row.get('H103執行學習可用'))
        if flag != '是': issues.append('H103_NOT_VALIDATED')
        if basis.startswith('LEGACY') or not basis: issues.append('LEGACY_NO_FIXED_PLAN')
        if _str(row.get('H103路徑歧義')) == '是': issues.append('AMBIGUOUS_INTRADAY_PATH')
        status = 'VALIDATED_T1' if not issues and flag == '是' else 'QUARANTINED'
        diagnostics.append((status, '｜'.join(issues) or 'H103已驗證',
                            alpha if status == 'VALIDATED_T1' else None))
    result['H129績效資料資格'] = [r[0] for r in diagnostics]
    result['H129隔離原因'] = [r[1] for r in diagnostics]
    result['H129有效Selection Alpha%'] = [r[2] for r in diagnostics]
    rows = [
        ('H129版本', VERSION), ('H129原T+1檢討筆數', len(result)),
        ('H129經H103驗證可用筆數', sum(r[0]=='VALIDATED_T1' for r in diagnostics)),
        ('H129隔離筆數', sum(r[0]=='QUARANTINED' for r in diagnostics)),
        ('H129極端報酬/Alpha異常筆數', sum('EXTREME_RETURN_OR_ALPHA' in r[1] for r in diagnostics)),
        ('H129無效或非交易日筆數', sum('INVALID_SESSION_DATE' in r[1] for r in diagnostics)),
        ('H129驗證口徑', '必須H103執行學習可用＝是、交易日有效、單日報酬及Alpha合理且無路徑歧義'),
        ('H129績效性質', '紙上研究/固定計畫回放；未提供券商成交紀錄，不得聲稱真實交易獲利'),
        ('H129舊原始欄位', '不改寫原始Selection Alpha、WIN/LOSS、H103旗標；只新增可用欄位及隔離原因')
    ]
    vals = [r[2] for r in diagnostics if r[0] == 'VALIDATED_T1' and r[2] is not None]
    rows.append(('H129有效Alpha平均pp', round(sum(vals)/len(vals), 4) if vals else None))
    return result, pd.DataFrame(rows, columns=['項目','數值'])


def audit_risk_watch(tables: Mapping[str, Any] | None, *, limit: int = 25) -> pd.DataFrame:
    """Show what to recheck, *not* what to buy. No veto gets overturned."""
    src = (tables or {}).get('audit')
    if not isinstance(src, pd.DataFrame) or src.empty or '股票代號' not in src.columns:
        return pd.DataFrame()
    rows = []
    for _, r in src.iterrows():
        code = _str(r.get('股票代號'))
        if not (len(code)==4 and code.isdigit()): continue
        auth = _str(r.get('H109決策權威'))
        hard = auth in HARD_VETO or _str(r.get('H105今日強勢排除'))=='是' or _str(r.get('H110證據狀態')).startswith('CONFLICT')
        st113 = _str(r.get('H113入榜資格'))
        absent = []
        if _str(r.get('H123法人實證狀態'))!='PROVEN_FLOW': absent.append('H123官方法人')
        if _str(r.get('H124大戶實證狀態'))!='VERIFIED_RISING': absent.append('H124官方大戶')
        if st113!='QUALIFIED': absent.append('H113硬條件')
        if _str(r.get('H110證據狀態')).startswith(('PARTIAL','UNKNOWN')): absent.append('H110來源不足')
        score = _num(r.get('H110最終研究優先分'))
        rows.append({
            '股票代號': code,'股票名稱':_str(r.get('股票名稱')),'類別':_str(r.get('類別')),
            'H129主管操作': '禁止新倉｜僅供風險複核' if hard else '禁止買進｜待驗證研究',
            'H129阻擋層級': 'HARD_VETO' if hard else 'EVIDENCE_PENDING',
            'H129H109否決': auth or 'UNKNOWN',
            'H129原研究分_不可授權': score,
            'H129缺口分類': '、'.join(absent) or '仍須交易前驗證',
            'H129風險處置': _str(r.get('H109否決原因'))[:160] or _str(r.get('H113缺失原因'))[:160],
            'H129H105發動階段':_str(r.get('H105發動階段')),
            'H129市場資訊截止': _str(r.get('H110研究資訊截止日'))[:10] or 'UNKNOWN',
            'H129不變原則':'不構成買入名單；不得使用H102排名建倉',
        })
    rows.sort(key=lambda r:(r['H129阻擋層級'] != 'EVIDENCE_PENDING', -(r['H129原研究分_不可授權'] if r['H129原研究分_不可授權'] is not None else -1), r['股票代號']))
    return pd.DataFrame(rows[:max(0,int(limit))])

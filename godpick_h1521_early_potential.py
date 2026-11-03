# -*- coding: utf-8 -*-
"""H152.1 | pre-breakout potential, distinct from already-rising momentum.

Read-only same-session diagnostic evidence. No prediction, credentials, user data,
network fetch, official buy signal or modification of H109/H126. A quiet stock
is only a research lead when it has verified liquidity and *multiple* setup
observations. Do not use unverified fund-flow/TDCC/news as positive facts.
"""
from __future__ import annotations
from collections import Counter
from math import isfinite
from typing import Any, Mapping
import pandas as pd
from godpick_h147_five_gate_review import _code, _day, _s

VERSION = 'V191-H152.1-PREBREAKOUT-EVIDENCE'
OBSERVATION_ONLY = '潛力蓄勢研究／非H126買進授權／無上漲保證'
EXTRA_COLUMNS = ('潛力型態', '蓄勢證據數', '距前高%', '當前5日漲幅%', '當日漲跌幅%',
                 '預發動風險反證', '潛力研究聲明')

def _frame(value: Any) -> pd.DataFrame:
    return value if isinstance(value, pd.DataFrame) else pd.DataFrame()

def _num(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).strip().replace(',', '').replace('%', ''))
        return number if isfinite(number) else None
    except (ValueError, TypeError, OverflowError):
        return None

def _get(row: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _num(row.get(key))
        if value is not None:
            return value
    return None

def _validated(row: Mapping[str,Any], asof: Any) -> bool:
    day = _day(asof)
    return bool(day and _day(row.get('H114K線證據日')) == day
                and _s(row.get('H114K線資料狀態')).upper() == 'VALID_OHLCV'
                and (_get(row, 'H114K線有效根數') or 0) >= 60
                and (_get(row, 'H114當日收盤價') or 0) > 0)

def build_early_potential(*, diagnosis_frame: Any, research_frame: Any,
                          research_day: Any, limit: int=12) -> tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame,dict]:
    """Return pre-ignition candidates, priority-first merged research, and audit.

    Research only; never changes or infers H126, and never forces a target count.
    Volume-contraction/price-consolidation are *setup hypotheses*, not bullish
    probabilities or evidence of future institutional buying.
    """
    raw, existing = _frame(diagnosis_frame), _frame(research_frame).copy(deep=True)
    day = _day(research_day)
    found: dict[str,dict] = {}; duplicate = set(); counts = Counter(); picks = []
    for row in raw.to_dict('records'):
        code = _code(row.get('股票代號'))
        if not code: counts['無效代號'] += 1; continue
        if code in found: duplicate.add(code)
        else: found[code] = row
    oldcodes = {_code(r.get('股票代號')) for r in existing.to_dict('records')}
    for code, rec in found.items():
        if code in duplicate:
            counts['重複代號隔離'] += 1; continue
        if not _validated(rec, day):
            counts['非同日有效K線'] += 1; continue
        amount = _get(rec, 'H113當日成交額百萬', '成交額百萬')
        average = _get(rec, 'H113二十日均成交額百萬', '20日均成交額百萬')
        lots = _get(rec, 'H114當日成交量張')
        ratio = _get(rec, 'H114K線量比')
        if (amount is None or average is None or lots is None or ratio is None or
            amount < 150 or average < 150 or lots < 500 or not 0.55 <= ratio <= 1.45):
            counts['流動性／量比缺證或過低'] += 1; continue
        price5 = _get(rec, 'H114當前5日漲跌幅%')
        dayret = _get(rec, 'H114當日漲跌幅%')
        gap = _get(rec, 'H114距20日高點%')
        if price5 is None or dayret is None or gap is None:
            counts['蓄勢價位缺證'] += 1; continue
        # No strong present momentum; seek potential *before* an already-large run.
        if not (-4 <= price5 <= 7 and -2.5 <= dayret <= 3 and 0 <= gap <= 10):
            counts['已發動過熱或遠離壓力區'] += 1; continue
        compress = _s(rec.get('H114波動收斂')) == '是'
        base = _s(rec.get('H114量縮墊高')) == '是'
        near = gap <= 6
        confirmations = int(compress) + int(base) + int(near)
        if confirmations < 2 or not (compress or base):
            counts['多項蓄勢共同證據不足'] += 1; continue
        # H105 ALREADY-IGNITED is a known contrary signal; skip, not a pre-run.
        if 'ALREADY-IGNITED' in _s(rec.get('H105發動階段')).upper():
            counts['已發動不得冒充預發動'] += 1; continue
        risk = _s(rec.get('H109決策權威')) or 'UNKNOWN'
        inst = _s(rec.get('H123法人實證狀態')) or 'PENDING_EVIDENCE'
        holder = _s(rec.get('H124大戶實證狀態')) or 'PENDING_EVIDENCE'
        label = 'P1｜量縮墊高與波動收斂' if compress and base else 'P2｜準突破蓄勢待確認'
        evidence = [v for condition,v in ((compress,'五日波動收斂'),(base,'量縮墊高'),(near,'距20日高點6%內')) if condition]
        objections = [OBSERVATION_ONLY, '尚未確認突破；需觀察量能回升與壓力站穩',
                      '整理型態可能向下突破，跌破整理區應重審']
        if risk not in {'ALLOW','PASS','NONE','RESEARCH_ELIGIBLE'}:
            objections.append('H109:'+risk+' 未解除')
        if inst != 'PROVEN_FLOW': objections.append('法人連買尚未實證')
        if holder != 'VERIFIED_RISING': objections.append('TDCC週增尚未實證，非大戶鎖碼結論')
        objections.append('H108主流族群資金廣度仍須同輪核對，不以題材自動升級')
        note = '；'.join(objections)
        pick = {'研究層級':label,'股票代號':code,
                '股票名稱':_s(rec.get('股票名稱')), '產業':_s(rec.get('類別')),
                'H114行情日期':str(day),'來源證據':'同日H114有效K線／來源仍須官方對帳；'+'、'.join(evidence),
                '當日成交額百萬':amount,'20日均成交額百萬':average,
                '成交量張':lots,'量比':ratio,'近5日漲幅%':price5,
                '突破／續攻判定':'尚未突破／預發動蓄勢研究',
                'H109風控':risk,'法人／大戶狀態':inst+'／'+holder,
                '尚需驗證與不得買進原因':note,'潛力型態':label,
                '蓄勢證據數':confirmations,'距前高%':round(gap,3),
                '當前5日漲幅%':round(price5,3),'當日漲跌幅%':round(dayret,3),
                '預發動風險反證':note,'潛力研究聲明':OBSERVATION_ONLY}
        picks.append(pick)
    # Rank by mutually verifiable setup, then explicit veto, then liquidity;
    # not by old H110 tied scores or future outcomes.
    picks.sort(key=lambda p:(p['H109風控'] not in {'ALLOW','PASS','NONE','RESEARCH_ELIGIBLE'},
                             0 if p['潛力型態'].startswith('P1') else 1,
                             -p['蓄勢證據數'],p['距前高%'], -p['20日均成交額百萬'],p['股票代號']))
    cap=max(1,min(int(limit),30)); selected=picks[:cap]
    existing_records=existing.to_dict('records')
    # Same stock is shown only once. Preserve original evidence rows, even if
    # they also meet the pre-breakout screen; add P evidence without downgrading.
    already = {p['股票代號'] for p in selected}
    merged_rows=selected+[r for r in existing_records if _code(r.get('股票代號')) not in already]
    cols=list(dict.fromkeys(list(existing.columns)+list(selected[0].keys() if selected else [])+list(EXTRA_COLUMNS)))
    if not cols:
        from godpick_h1483_research_recall import RESEARCH_COLUMNS
        cols=list(RESEARCH_COLUMNS)+list(EXTRA_COLUMNS)
    merged=pd.DataFrame(merged_rows,columns=cols)
    # A display-level research gate is not a formal change to H113/H126.
    stats=[
        {'檢查項目':'H152.1預發動候選總數','結果':len(picks),'說明':'非當日漲幅排名，不代表後續必大漲'},
        {'檢查項目':'H152.1預發動新增研究股','結果':sum(p['股票代號'] not in oldcodes for p in selected),'說明':'從當輪診斷母體新增；不限H113前120檔'},
        {'檢查項目':'H152.1同日行情／量能門檻','結果':'有效K≥60；成交額日/均≥150百萬；成交量≥500張','說明':'防止無量冷門股冒充潛力'},
        {'檢查項目':'H152.1不預測保證上漲','結果':'研究假設，未校準機率','說明':'建議檢驗T+5/T+10/T+20樣本外報酬與最大逆行'},
    ]+ [{'檢查項目':'H152.1排除-'+k,'結果':v,'說明':'篩選缺口不是股票必跌'} for k,v in counts.most_common()]
    meta={'version':VERSION,'day':str(day) if day else '未確認','found':len(picks),
          'visible':len(selected),'added':sum(p['股票代號'] not in oldcodes for p in selected),
          'excluded':dict(counts),'formal_h126_modified':False,
          'warning':'預發動潛力 ≠ 次日漲停或正式推薦；請再核對法人、族群與 H126。'}
    return pd.DataFrame(selected,columns=list(selected[0].keys()) if selected else list(cols)), merged, pd.DataFrame(stats),meta

__all__ = ['VERSION','EXTRA_COLUMNS','build_early_potential']

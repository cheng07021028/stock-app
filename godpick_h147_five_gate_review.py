# -*- coding: utf-8 -*-
"""H147 read-only five-factor trading blind-spot review.

This module never changes H109/H113/H123/H124/H126, writes data, fetches
quotes, or awards buy permission. Missing input is UNKNOWN, not a failed
stock, and dated snapshots never become current merely by exporting them.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime
import math
import re
from typing import Any, Mapping

import pandas as pd

VERSION = 'V191-H147.0-FIVE-GATE-RESEARCH-ONLY'
FIELDS = ('成交量能','起漲位置','強勢族群','大戶鎖碼','技術結構')
# Reuse H113 existing hard-liquidity numbers, NOT new trading permissions.
MIN_DAY_AMOUNT_M = 150.0
MIN_20D_AMOUNT_M = 150.0
MIN_DAY_LOTS = 500.0
# An exploratory breakout-volume benchmark; absence is not a liquidity veto.
OBSERVED_VOLUME_RATIO = 1.2


def _s(value: Any) -> str:
    if value is None: return ''
    try:
        if bool(pd.isna(value)): return ''
    except (ValueError, TypeError): pass
    text = str(value).strip()
    return '' if text.lower() in ('nan','none','nat','null','<na>') else text


def _n(row: Mapping[str,Any], *names: str) -> float | None:
    for name in names:
        s = _s(row.get(name)).replace(',','').replace('%','')
        if not s: continue
        try:
            v = float(s)
            if math.isfinite(v): return v
        except (ValueError, TypeError, OverflowError): pass
    return None


def _day(v: Any) -> date | None:
    if isinstance(v, datetime): return v.date()
    if isinstance(v, date): return v
    s = _s(v).replace('/','-')[:10]
    if len(s)==8 and s.isdigit(): s = f'{s[:4]}-{s[4:6]}-{s[6:8]}'
    try: return date.fromisoformat(s)
    except (ValueError, TypeError): return None


def _code(v: Any) -> str:
    s = _s(v).removesuffix('.0')
    return s if re.fullmatch(r'[1-9][0-9]{3}',s) else ''


def _status(label: str, why: str, value: Any = '') -> dict[str,Any]:
    return {'status':label,'reason':why,'value':value}


def evaluate_five_gates(row: Mapping[str,Any], *, research_day: Any = None) -> dict[str,Any]:
    """Evaluate actual row evidence; cannot infer volume, TDCC, or launch from scores.

    PASS means the named observable supports a research condition, not a formal
    trade. Missing date/source may turn a numerical pass into UNVERIFIED.
    """
    r = row if isinstance(row, Mapping) else {}
    day = _day(research_day) or _day(r.get('H110研究資訊截止日'))
    quote_day = (_day(r.get('H114K線證據日')) or _day(r.get('K線日期驗證基準')) or
                 _day(r.get('資料交易日')) or _day(r.get('行情日期')))
    dated_quote = bool(day and quote_day and quote_day == day)
    future_quote = bool(day and quote_day and quote_day > day)
    h110 = _s(r.get('H110證據狀態')).upper()
    conflicting = h110.startswith('CONFLICT')
    # Quantity/amount units are the identical explicit units used by H113.
    amount = _n(r,'H113當日成交額百萬','成交額百萬','流動性參考成交額百萬')
    avg_amount = _n(r,'H113二十日均成交額百萬','20日均成交額百萬')
    lots = _n(r,'H113當日成交量張','最新成交量_張')
    if lots is None:
        shares = _n(r,'最新成交量')
        if shares is not None: lots = shares / 1000
    ratio = _n(r,'H114K線量比','當日量比','均量比')
    enough = (amount is not None and avg_amount is not None and lots is not None)
    liquid = enough and amount >= MIN_DAY_AMOUNT_M and avg_amount >= MIN_20D_AMOUNT_M and lots >= MIN_DAY_LOTS
    volume_info = ('當日/20日成交額(百萬)=%s/%s、張數=%s、量比=%s' %
                   (amount if amount is not None else '缺',avg_amount if avg_amount is not None else '缺',
                    round(lots,2) if lots is not None else '缺',round(ratio,2) if ratio is not None else '缺'))
    if (future_quote or conflicting or (liquid and _s(r.get('H113流動性合格')) == '否')):
        volume = _status('資料衝突', '行情未符合研究截止日或H110證據衝突；'+volume_info)
    elif not enough:
        volume = _status('待補證','成交額/20日成交額/張數缺一不可；'+volume_info)
    elif not liquid:
        volume = _status('未過門檻',f'H113既有底線150/150百萬及500張；{volume_info}')
    elif not dated_quote:
        volume = _status('待核日期','量能數值過H113底線，但本列缺同日K線可驗日期；'+volume_info)
    elif ratio is None:
        volume = _status('流動性過／量比缺','既有最低流動性過關；但不能宣稱放量；'+volume_info)
    elif ratio >= OBSERVED_VOLUME_RATIO:
        volume = _status('量能支持','當日量比達1.2倍研究參考，非交易授權；'+volume_info)
    else:
        volume = _status('流動性過／未放量','流動性合格，但相對20日均量未達1.2倍；'+volume_info)

    stage = _s(r.get('H105發動階段')).upper()
    already = ('ALREADY-IGNITED' in stage or _s(r.get('H105今日強勢排除')) == '是')
    tech = _n(r,'H105技術蓄勢分')
    prep = _n(r,'突破準備分','H51Pivot起漲分')
    window = _s(r.get('H105未來發動窗口'))
    if future_quote or conflicting:
        launch = _status('資料衝突','未核實本交易日行情，禁止起漲推論')
    elif already:
        launch = _status('已發動／防追高','強勢股與未發動黑馬必須分池，禁止冒充起漲買點')
    elif stage.startswith(('BH1','BH2')):
        if tech is not None and tech >= 60 and (prep is not None and prep >= 60) and window and not any(x in window.upper() for x in ('未定','NO-TIMING','已發動')):
            launch = _status('預發動條件待驗','BH1/BH2＋技術蓄勢/突破準備/窗口；仍非真實突破成交認證')
        else:
            launch = _status('起漲證據不足','BH1/BH2尚缺技術>=60、突破準備>=60或有效起漲窗口')
    else:
        launch = _status('待辨識','無同輪已發動或BH1/BH2的有效分型，不能宣稱起漲')

    phase = _s(r.get('H108族群生命週期')).upper()
    sector_confirm = _n(r,'H108族群確認分')
    risk = _s(r.get('H109決策權威'))
    if risk == 'FADE_VETO' or 'FADE' in phase or '退潮' in phase:
        if _s(r.get('H142退潮證據級別')) == 'TWO_INTERNAL_METRICS':
            sector = _status('退潮否決／有內部證據','風控保留；仍需獨立來源驗證')
        else:
            sector = _status('退潮否決／待查來源','H109封鎖仍有效；不能將FADE標籤當成官方量化退潮證據')
    elif any(x in phase for x in ('IGNITION','EXPANSION')):
        sector = (_status('族群方向支持','H108處點火/擴張；仍需核對樣本與時點',sector_confirm)
                  if sector_confirm is not None else
                  _status('族群方向待驗','有階段標籤但無H108族群確認分'))
    elif phase:
        sector = _status('族群非起漲確認','H108階段='+phase[:48]+'；須區分成熟/退潮/重新點火')
    else:
        sector = _status('待補證','缺H108同輪族群生命週期與確認分')

    holder = _s(r.get('H124大戶實證狀態'))
    tdcc_source = _s(r.get('TDCC大戶資料狀態'))
    tdcc_date = _day(r.get('TDCC大戶資料日期')) or _day(r.get('H60大戶資料日期'))
    tdcc_delta = _n(r,'H124大戶變化pp','TDCC千張大戶週變化pp','H60千張大戶週變化pp')
    if holder == 'VERIFIED_RISING':
        ownership = _status('官方週增已驗','H124認證上升；僅集保持股級距集中度，不代表確認特定大戶鎖單',tdcc_delta)
    elif holder in ('VERIFIED_FALLING','VERIFIED_NOT_RISING','VERIFIED_FLAT'):
        ownership = _status('週增未成立','H124已驗證，但未證明大戶級距上升',tdcc_delta)
    elif tdcc_delta is not None and tdcc_source in ('ACTUAL','VERIFIED','OFFICIAL') and tdcc_date and day and 0 <= (day-tdcc_date).days <= 14:
        ownership = _status('數值待H124驗證','週差有數值但尚無H124正式認證，不能宣稱鎖碼',tdcc_delta)
    else:
        ownership = _status('待補證','H124='+ (holder or '缺') +'；須兩個已公布週次、來源與週變化')

    gap20 = _n(r,'收盤距MA20%','MA20乖離%')
    gap60 = _n(r,'收盤距MA60%','MA60乖離%')
    technical_score = _n(r,'H105技術蓄勢分','H51技術分')
    if future_quote or conflicting:
        technical = _status('資料衝突','K線日期或來源衝突，技術判讀暫停')
    elif not dated_quote:
        technical = _status('待核日期','均線或技術分不能證明屬本交易日；先核對同日K線來源')
    elif gap20 is not None and gap20 > 12:
        technical = _status('追高風險',f'MA20正乖離{gap20:.1f}%，須查突破追價/回測；不可直接進場')
    elif gap20 is not None and gap20 < -6:
        technical = _status('偏弱／待站回',f'MA20乖離{gap20:.1f}%，不是健康起漲證據')
    elif technical_score is not None and technical_score >= 60 and dated_quote and gap60 is not None and gap60 >= 0:
        technical = _status('技術研究支持',f'同日K線＋技術蓄勢{technical_score:.0f}＋位於MA60上；仍未檢核交易成本')
    elif technical_score is None or not dated_quote:
        technical = _status('待補證','需同日K線、20/60日線、突破型態與技術證據')
    else:
        technical = _status('技術未確認','技術分或MA60趨勢尚不足以證實上升結構')

    gates = {'成交量能':volume,'起漲位置':launch,'強勢族群':sector,'大戶鎖碼':ownership,'技術結構':technical}
    positive = sum(v['status'] in ('量能支持','流動性過／量比缺','流動性過／未放量','族群方向支持','官方週增已驗','技術研究支持') for v in gates.values())
    missing = sum('待' in v['status'] or '缺' in v['status'] for v in gates.values())
    hard = sum(v['status'] in ('未過門檻','資料衝突','退潮否決／有內部證據','退潮否決／待查來源',
                              '追高風險','偏弱／待站回','週增未成立') for v in gates.values())
    if missing >= 3:
        category = '資料缺口｜優先補證'
    elif already:
        category = '已發動強勢｜不冒稱起漲'
    elif hard:
        category = '存在風險或條件不足'
    elif positive >= 3:
        category = '多因子研究觀察｜非買進'
    else:
        category = '研究待確認｜非買進'
    risk_status = risk or 'UNKNOWN'
    gap_reasons = [f'{k}:{v["status"]}' for k,v in gates.items() if v['status'] not in
                   ('量能支持','族群方向支持','官方週增已驗','技術研究支持')]
    if not gap_reasons: gap_reasons=['五項研究訊號已觀測；仍須H126逐項核准']
    return {
        '股票代號':_code(r.get('股票代號')),
        '研究分類':category,
        '成交量能':volume['status'], '起漲位置':launch['status'],
        '強勢族群':sector['status'], '大戶鎖碼':ownership['status'],
        '技術結構':technical['status'],
        '量能明細':volume['reason'], '起漲明細':launch['reason'],
        '族群明細':sector['reason'], '大戶明細':ownership['reason'],
        '技術明細':technical['reason'],
        '研究日期':str(day) if day else '未載明',
        '行情日期':str(quote_day) if quote_day else '未核實',
        '證據支持項數':positive, '待補項數':missing, '風險項數':hard,
        '優先查核':'；'.join(gap_reasons[:3]),
        'H109原權威':risk_status,
        'H126買進資格':'未經本模組授權；僅以H126實際原表為準',
        '五項證據':gates,
    }


def build_five_gate_review(audit: Any, *, research_day: Any = None) -> tuple[pd.DataFrame,dict[str,Any]]:
    """Evaluate existing audit rows without inventing a missing full-market pool."""
    frame = audit if isinstance(audit,pd.DataFrame) else pd.DataFrame()
    result: list[dict] = []
    seen: set[str] = set()
    conflicts: set[str] = set()
    for raw in frame.to_dict('records'):
        code=_code(raw.get('股票代號'))
        if not code:continue
        if code in seen:
            conflicts.add(code)
            continue
        seen.add(code)
        result.append({k:v for k,v in evaluate_five_gates(raw,research_day=research_day).items() if k!='五項證據'})
    out=pd.DataFrame(result)
    if not out.empty and conflicts:
        for code in conflicts:
            out.loc[out['股票代號']==code,'研究分類']='同代號重複／待核對原始檔'
            out.loc[out['股票代號']==code,'H126買進資格']='禁止由H147認證；原H126不變'
    if not out.empty:
        out=out.sort_values(['證據支持項數','待補項數','風險項數','股票代號'],
                            ascending=[False,True,True,True],kind='stable').reset_index(drop=True)
    statuses=Counter(out['研究分類']) if not out.empty else Counter()
    meta={'version':VERSION,'reviewed':len(out),'duplicate_codes':sorted(conflicts),
          'data_gaps':int(out['待補項數'].gt(0).sum()) if not out.empty else 0,
          'positive_two':int(out['證據支持項數'].ge(2).sum()) if not out.empty else 0,
          'already_ignited':int(out['起漲位置'].eq('已發動／防追高').sum()) if not out.empty else 0,
          'status_counts':dict(statuses), 'trade_granted':0,
          'warning':'H147僅稽核已提供股票；無掃描母體證據時不代表全市場；五項研究條件不等於H126買進資格。'}
    return out,meta


__all__=['VERSION','FIELDS','evaluate_five_gates','build_five_gate_review']

# -*- coding: utf-8 -*-
"""V191-H119 - evidence-gated cash/stock entitlement return estimates.

Never infer cash dividend from TWSE 權值+息值 or from reference-price gaps.
Cash dividends accrue as receivables on the ex-date, NOT necessarily as cash
already received. Stock bonus may be not yet deliverable/tradable on exit; hence
stock entitlement valuation is indicative and excluded from verified scorecards.
No order placement, no position sizing, no live weight changes.
"""
from __future__ import annotations
from datetime import date
from typing import Any, Iterable, Mapping
import hashlib
import json
import math
import re
from godpick_h118_performance_authority import _iso_day, _official_url, _evidence_time, MARKETS

VERSION='V191-H119.1-EVIDENCE_GATED_ENTITLEMENT_20261014'
ADJUSTMENT_COLUMNS=('code','market','ex_date','cash_dividend_per_share','stock_bonus_ratio',
                    'source_url','verified_at','verification_status')
SUPPORTED=frozenset(('CASH_DIVIDEND','STOCK_DIVIDEND'))
UNSUPPORTED=frozenset(('RIGHTS_ISSUE','SPLIT','REVERSE_SPLIT','OTHER_PRICE_ADJUSTMENT'))


def _amount(value:Any,*,name:str,max_value:float)->float:
    if isinstance(value,bool):raise ValueError(f'{name}不可為布林值')
    try:
        x=float(str(value).replace(',','').strip())
    except (TypeError,ValueError) as e:raise ValueError(f'缺少有效的{name}') from e
    if not math.isfinite(x) or not 0 <= x <= max_value:
        raise ValueError(f'{name}超出安全範圍')
    return x


def load_adjustments(records:Iterable[Mapping[str,Any]])->list[dict[str,Any]]:
    """Import audited, concrete per-share cash dividend and shares-per-share bonus ratio.

    An official document URL alone is NOT an automated authentication of its
    numeric fields. Only independently reviewed, timestamped inputs qualify.
    """
    result=[];seen=set()
    for raw in records:
        code=str(raw.get('code') or '').strip()
        if not re.fullmatch(r'[1-9]\d{3}',code):raise ValueError('股票代號不合法')
        market=str(raw.get('market') or '').upper().strip()
        if market not in MARKETS:raise ValueError('上市／上櫃市場不合法')
        day=_iso_day(raw.get('ex_date'))
        if raw.get('verification_status')!='OFFICIAL_NUMERIC_REVIEWED':
            raise ValueError('未完成官方現金與配股實際數值獨立核對')
        cash=_amount(raw.get('cash_dividend_per_share'),name='每股現金股利',max_value=1_000_000)
        stock=_amount(raw.get('stock_bonus_ratio'),name='每股無償配股率',max_value=10)
        if cash==0 and stock==0:raise ValueError('無股利事件不能列入調整表')
        if any(str(raw.get(k) or '').strip() not in ('','0','0.0') for k in
               ('rights_subscription_ratio','rights_subscription_price','other_adjustment')):
            raise ValueError('含現金增資或其他資本事件，拒絕自動估值')
        url=_official_url(raw.get('source_url'))
        stamp=_evidence_time(raw.get('verified_at'),day,require_day_passed=False)
        key=(code,market,day)
        if key in seen:raise ValueError('同股票同日事件重複，需合併為單一筆')
        seen.add(key)
        result.append(dict(code=code,market=market,ex_date=day,cash_dividend_per_share=cash,
                           stock_bonus_ratio=stock,source_url=url,verified_at=stamp,
                           verification_status='OFFICIAL_NUMERIC_REVIEWED'))
    return sorted(result,key=lambda x:(x['market'],x['code'],x['ex_date']))


def assess_entitlement_window(*,code:str,market:str,entry_day:str,exit_day:str,
                market_days:Iterable[str],stock_bars:Iterable[Mapping[str,Any]],
                previous_close:float|None,action_events:Iterable[Mapping[str,Any]],
                coverage:Iterable[Mapping[str,Any]],adjustments:Iterable[Mapping[str,Any]])->dict[str,Any]:
    """Quality-gate action-adjusted entitlement valuation, fail closed on gaps."""
    entry=_iso_day(entry_day);end=_iso_day(exit_day)
    if entry>end:raise ValueError('進場日不得晚於出場日')
    if market not in MARKETS:return dict(status='MARKET_PROVENANCE_UNKNOWN',reason='來源市場不明')
    days=sorted(set(_iso_day(d) for d in market_days if entry<=_iso_day(d)<=end))
    bars=sorted([dict(b) for b in stock_bars],key=lambda b:str(b.get('date')))
    if not days or days[0]!=entry or days[-1]!=end or len(days)!=len(bars) or [str(b.get('date')) for b in bars]!=days:
        return dict(status='UNVERIFIED_MARKET_SESSIONS',reason='缺少或錯配官方大盤與個股交易日')
    if any(str(b.get('exchange') or '').upper() != market for b in bars):
        return dict(status='MARKET_PROVENANCE_CONFLICT',reason='持有期間官方行情來源市場不一致')
    covered={(str(x['market']),str(x['date'])) for x in coverage}
    missing=[d for d in days if (market,d) not in covered]
    if missing:return dict(status='CORPORATE_ACTION_COVERAGE_UNVERIFIED',reason=f'官方企業行動核對缺少 {missing[0]} 等 {len(missing)} 日')
    catalog=[x for x in action_events if str(x['code'])==code and entry<=str(x['ex_date'])<=end]
    adjustments=[x for x in adjustments if str(x['code'])==code and str(x['market'])==market and entry<=str(x['ex_date'])<=end]
    adj_by_day={x['ex_date']:x for x in adjustments}
    cat_by_day={d:{x['action_type'] for x in catalog if x['ex_date']==d} for d in {x['ex_date'] for x in catalog}}
    for d,kinds in cat_by_day.items():
        if kinds & UNSUPPORTED or not kinds<=SUPPORTED:
            return dict(status='UNSUPPORTED_CORPORATE_ACTION',reason=f'{d} 存在不支援資本事件：{sorted(kinds)}')
        adj=adj_by_day.get(d)
        if adj is None:return dict(status='CORPORATE_COMPONENTS_UNVERIFIED',reason=f'{d} 官方事件欠缺每股現金／配股數值')
        expected=({'CASH_DIVIDEND'} if adj['cash_dividend_per_share']>0 else set()) | \
                 ({'STOCK_DIVIDEND'} if adj['stock_bonus_ratio']>0 else set())
        if expected!=kinds:return dict(status='ACTION_EVIDENCE_MISMATCH',reason=f'{d} 行動類型與實際金額不一致')
    if any(d not in cat_by_day for d in adj_by_day):
        return dict(status='ACTION_EVIDENCE_MISMATCH',reason='現金與配股調整表未在 H118 官方事件清單找到對應')
    prev=previous_close
    for b in bars:
        day=str(b['date'])
        try: op=float(b['open']);close=float(b['close'])
        except (ValueError,TypeError,KeyError):return dict(status='BAD_MARKET_BAR',reason=f'{day} OHLC無效')
        if not (math.isfinite(op) and math.isfinite(close) and op>0 and close>0):
            return dict(status='BAD_MARKET_BAR',reason=f'{day} OHLC無效')
        # A documented action can explain an ex-date gap, but never a gap on unrelated days.
        if prev is not None and prev>0 and (op/prev<.8 or op/prev>1.2) and day not in cat_by_day:
            return dict(status='UNEXPLAINED_PRICE_DISCONTINUITY',reason=f'{day} 非除權息日跳空逾20%')
        prev=close
    shares=1.0;cash_entitlement=0.0;stock_events=[];cash_events=[]
    for d in sorted(adj_by_day):
        if d<=entry:continue  # buy at ex-date OPEN => no prior shareholder entitlement
        a=adj_by_day[d]
        cash_entitlement += shares*a['cash_dividend_per_share']
        shares *= (1+a['stock_bonus_ratio'])
        if a['cash_dividend_per_share']>0: cash_events.append(d)
        if a['stock_bonus_ratio']>0:stock_events.append(d)
    # End-value assumes all economic entitlements retained; bonus shares may not
    # be credited or tradeable yet. This is explicitly not an executable P&L.
    return dict(status='STOCK_RIGHTS_DELIVERY_UNVERIFIED' if stock_events else
                ('CASH_ENTITLEMENT_VALUED' if cash_events else 'CERTIFIED_UNADJUSTED_PRICE_WINDOW'),
                reason='股票股利股數尚未證明出場前可交易' if stock_events else
                       ('現金股利為應收權益估值，尚非入帳現金' if cash_events else '本期官方核對無企業行動'),
                cash_entitlement_per_initial_share=round(cash_entitlement,8),
                theoretical_share_count=round(shares,10),
                cash_event_dates=cash_events,stock_event_dates=stock_events,
                economic_entitlement_basis='EX_DATE_RECEIVABLE_AND_THEORETICAL_SHARE_VALUE_NOT_CASH_SETTLEMENT')


def fingerprint(rows:Iterable[Mapping[str,Any]])->str:
    canonical=json.dumps(list(rows),ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')
    return hashlib.sha256(canonical).hexdigest()

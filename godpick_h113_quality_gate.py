# -*- coding: utf-8 -*-
"""H113 Strict Future-Blackhorse Evidence Gate.

Research/radar evidence admission only. Never grants Formal trade permission.
Never infer consecutive buying from 3D/5D aggregates or proxy scores.
No missing numeric value is silently treated as a neutral valid signal.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any
import math

VERSION = 'v191_h113_liquidity_holder_institution_ignition_gate_20261013'

# NT$ million, NOT dollars or ten-thousands. Default strategy guardrails,
# subject to out-of-sample validation; these are not universal market laws.
MIN_DAY_VALUE_M = 150.0
MIN_20D_VALUE_M = 150.0
MIN_DAY_LOTS = 500.0
MIN_TDCC_DELTA_PP = 0.05
MAX_TDCC_AGE_DAYS = 14
MIN_TECH_SCORE = 60.0
MIN_BREAKOUT_PREP = 60.0


def _text(v: Any) -> str:
    if v is None:
        return ''
    s = str(v).strip()
    return '' if s.lower() in ('', 'nan', 'none', 'nat', 'null', '<na>') else s


def _number(v: Any) -> float | None:
    try:
        if isinstance(v, bool) or _text(v) == '':
            return None
        n = float(str(v).replace(',', ''))
        return n if math.isfinite(n) else None
    except (TypeError, ValueError):
        return None


def _date(v: Any) -> date | None:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = _text(v).replace('/', '-').replace('T', ' ')[:10]
    try:
        if len(s) == 8 and s.isdigit():
            return datetime.strptime(s, '%Y%m%d').date()
        return datetime.strptime(s, '%Y-%m-%d').date()
    except ValueError:
        return None


def _num(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        n = _number(row.get(key))
        if n is not None:
            return n
    return None


def _consecutive_from_daily(v: Any, asof: date | None) -> int | None:
    """Accept only dated, individual OFFICIAL daily net flows, never 3/5D sums.
    Each datum must have a unique publication/trading date; latest observation
    must be no later than the decision's information cutoff.
    """
    if not isinstance(v, (list, tuple)) or not v:
        return None
    records = []
    for item in v:
        if not isinstance(item, dict):
            return None
        day = _date(item.get('date') or item.get('trade_date') or item.get('資料日期'))
        net = _number(item.get('net') if 'net' in item else item.get('買賣超'))
        if day is None or net is None or (asof and day > asof):
            return None
        records.append((day, net))
    if len({x[0] for x in records}) != len(records):
        return None
    records.sort(key=lambda x: x[0], reverse=True)
    if asof and (asof - records[0][0]).days > 4:
        return None
    count = 0
    for _, net in records:
        if net <= 0:
            break
        count += 1
    return count


def _verified_streak(row: dict[str, Any], label: str, asof: date | None, official_ok: bool) -> int | None:
    if not official_ok:
        return None
    # Prefer real dated daily observations. A summary of '連買天數_官方' is
    # accepted only if official source/date passed H110's provenance check.
    series = row.get(f'{label}逐日買賣超_官方')
    actual = _consecutive_from_daily(series, asof)
    if actual is not None:
        return actual
    n = _num(row, f'{label}連買天數_官方')
    return int(n) if n is not None and 0 <= n <= 60 else None


def audit(row: dict[str, Any], *, info_asof: str | date | None = None) -> dict[str, Any]:
    today = _date(info_asof) or _date(row.get('H110研究資訊截止日')) or _date(row.get('K線日期驗證基準'))
    notes: list[str] = []
    market = _text(row.get('市場別'))
    day_value = _num(row, '成交額百萬', '流動性參考成交額百萬')
    avg_value = _num(row, '20日均成交額百萬')
    lots = _num(row, '最新成交量_張')
    if lots is None:
        rawshares = _num(row, '最新成交量')
        lots = rawshares / 1000 if rawshares is not None else None
    liquidity = (day_value is not None and avg_value is not None and lots is not None
                 and day_value >= MIN_DAY_VALUE_M and avg_value >= MIN_20D_VALUE_M
                 and lots >= MIN_DAY_LOTS)
    if not liquidity:
        notes.append(f'流動性未過：今日/20日成交額={day_value}/{avg_value}百萬元，當日={lots}張；門檻=150/150百萬元及500張')
    if market not in ('上市', '上櫃'):
        notes.append('市場別不是上市/上櫃（興櫃不得進研究主榜）')

    tdcc_pp = _num(row, 'TDCC千張大戶週變化pp', 'H60千張大戶週變化pp')
    tdcc_source = _text(row.get('TDCC大戶資料狀態'))
    tdcc_day = _date(row.get('TDCC大戶資料日期')) or _date(row.get('H60大戶資料日期'))
    tdcc_age = (today-tdcc_day).days if today and tdcc_day else None
    tdcc_fresh = tdcc_age is not None and 0 <= tdcc_age <= MAX_TDCC_AGE_DAYS
    holder = (tdcc_pp is not None and tdcc_pp >= MIN_TDCC_DELTA_PP and
              tdcc_source in ('ACTUAL','VERIFIED','OFFICIAL') and tdcc_fresh)
    if not holder:
        notes.append(f'TDCC大戶集中未確認：週變化={tdcc_pp}pp，來源={tdcc_source or "缺"}，距資料日={tdcc_age}天；要求官方實值且≥+0.05pp')
    # Holder *level* (e.g. 60% already held) is not a rising/increasing holder ratio.

    flow_status = _text(row.get('H110法人方向驗證'))
    official_ok = flow_status == 'OFFICIAL_DATED'
    foreign_3 = _num(row, '外資近3日買賣超_官方', '外資近3日買賣超')
    foreign_5 = _num(row, '外資近5日買賣超_官方', '外資近5日買賣超')
    fund_3 = _num(row, '投信近3日買賣超_官方', '投信近3日買賣超')
    fund_5 = _num(row, '投信近5日買賣超_官方', '投信近5日買賣超')
    combo_5 = _num(row, '三大法人近5日合計_官方', '三大法人近5日合計')
    foreign_streak = _verified_streak(row, '外資', today, official_ok)
    fund_streak = _verified_streak(row, '投信', today, official_ok)
    # A verified fund or foreign buying streak is required. A positive 5D
    # aggregate is merely a trend (it never proves 2 consecutive buy days).
    foreign_ok = (foreign_streak is not None and foreign_streak >= 2 and
                  foreign_3 is not None and foreign_3 > 0 and foreign_5 is not None and foreign_5 > 0)
    fund_ok = (fund_streak is not None and fund_streak >= 2 and
               fund_3 is not None and fund_3 > 0 and fund_5 is not None and fund_5 > 0)
    institutional = (official_ok and (foreign_ok or fund_ok) and combo_5 is not None and combo_5 > 0)
    if not institutional:
        notes.append('法人連續買進未獲證實：要求官方日期/來源、外資或投信至少連買2日、同機構3/5日淨買超且法人5日合計>0；代理分不計')

    stage = _text(row.get('H105發動階段'))
    window = _text(row.get('H105未來發動窗口'))
    tech = _num(row,'H105技術蓄勢分')
    signal = _text(row.get('主升起漲候選'))
    breakout = _num(row,'突破準備分')
    pivot = _num(row,'H51Pivot起漲分')
    ignition = (stage.startswith(('BH1','BH2')) and window and
                '未定' not in window and '已發動' not in window and
                'NO-TIMING' not in window and tech is not None and tech>=MIN_TECH_SCORE and
                (signal in ('是','YES','True','TRUE') or
                 (breakout is not None and breakout>=MIN_BREAKOUT_PREP) or
                 (pivot is not None and pivot>=65)))
    if not ignition:
        notes.append('主升預發動未確認：需BH1/BH2＋1～5日窗口＋技術分≥60＋起漲候選或突破準備≥60')

    authority = _text(row.get('H109決策權威'))
    h110 = _text(row.get('H110證據狀態'))
    already = _text(row.get('H105今日強勢排除')) == '是'
    blocked = (authority != 'RESEARCH_ELIGIBLE' or h110.startswith('CONFLICT') or already)
    if blocked:
        notes.append('上游風控否決/資料衝突/已發動，不得入未來黑馬主榜')
    qualified = (market in ('上市','上櫃') and not blocked and liquidity and holder and institutional and ignition)
    if qualified:
        tier = 'QUALIFIED'
    elif market not in ('上市','上櫃') or blocked or not liquidity:
        tier = 'BLOCKED'
    else:
        tier = 'WATCH_ONLY'
    return {
        'H113版本':VERSION,
        'H113入榜資格':tier,
        'H113流動性合格': '是' if liquidity else '否',
        'H113當日成交額百萬':day_value,
        'H113二十日均成交額百萬':avg_value,
        'H113當日成交量張':lots,
        'H113千張大戶週變化pp':tdcc_pp,
        'H113大戶鎖碼合格':'是' if holder else '否',
        'H113法人逐日來源驗證':'OFFICIAL_DATED' if official_ok else 'UNVERIFIED',
        'H113外資連買日數': foreign_streak,
        'H113投信連買日數': fund_streak,
        'H113法人連買合格':'是' if institutional else '否',
        'H113外資連買合格':'是' if foreign_ok else '否',
        'H113投信連買合格':'是' if fund_ok else '否',
        'H113雙法人同步連買':'是' if foreign_ok and fund_ok else '否',
        'H113主升起漲合格':'是' if ignition else '否',
        'H113缺失原因': '；'.join(notes) if notes else '核心證據齊備，仍需既有正式風控及實際交易日驗證',
        'H113正式買進權限':'UNCHANGED｜本模組不核准買進',
    }


__all__=['VERSION','audit','MIN_DAY_VALUE_M','MIN_20D_VALUE_M','MIN_DAY_LOTS','MIN_TDCC_DELTA_PP']

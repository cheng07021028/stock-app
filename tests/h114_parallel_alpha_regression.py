# -*- coding: utf-8 -*-
"""H114 fixed, offline fixtures: strictly point-in-time and research-only."""
from __future__ import annotations
import sys
from pathlib import Path
from datetime import date,timedelta
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pandas as pd
from godpick_h114_parallel_alpha import analyze_history,evaluate,build_parallel_watch,VERSION
from tools.apply_h114_history_hook import patch,D


def hist_fixture(lots=2200):
    days=pd.bdate_range(end="2026-10-13",periods=30)
    rows=[]
    for i,day in enumerate(days):
        p=103.0-i*.07
        rows.append({"日期":str(day.date()),"開盤價":p+.4,"最高價":p+.8,"最低價":p-.6,
                     "收盤價":p-.10,"成交股數":lots*1000})
    # For a strictly bullish engulfing after multi-day weakening.
    rows[-2].update({"開盤價":104.,"收盤價":99.0,"最高價":105.,"最低價":98.})
    rows[-1].update({"開盤價":98.,"收盤價":105.,"最高價":106.,"最低價":97.,"成交股數":1800*1000})
    return pd.DataFrame(rows)


def monthly_fixture():
    ans=[]
    for idx in range(2025*12,2026*12+9): # Jan 2025 - Sep 2026
        y=idx//12;m=idx%12+1
        # Increasing revenue with new record high, YoY ~ 50% and acceleration.
        revenue=100.+(idx-2025*12)*3.0
        if idx>=2026*12+6:revenue+=25
        publication=date(y+1,1,10) if m==12 else date(y,m+1,10)
        ans.append({'year_month':f'{y}{m:02d}','revenue_twd':revenue*1000000,'released_at':str(publication),'source':'TWSE'})
    return ans


def rich_row(**kw):
    base={'股票代號':'1234','股票名稱':'測試股','市場別':'上市','類別':'其他電子',
          'H109決策權威':'RESEARCH_ELIGIBLE','H110證據狀態':'VERIFIED',
          'H105今日強勢排除':'否','H113入榜資格':'WATCH_ONLY',
          '最新成交量_張':2100,'20日均量_張':1800,
          '成交額百萬':450,'20日均成交額百萬':420,
          'H114本益比實值':11.,'H114同業本益比中位數_官方':20.,
          'H114近四季EPS實值':4.,'H114估值資料來源':'TWSE','H114估值資料日期':'2026-10-10',
          'H114月營收歷史':monthly_fixture()}
    base.update(kw)
    return base


def test_history():
    a=analyze_history(hist_fixture(),asof='2026-10-13')
    assert a['H114K線資料狀態']=='VALID_OHLCV',a
    assert a['H114紅K吞噬'].startswith('確認形態'),a
    assert a['H114當日成交量張']==1800,a
    assert a['H11420日平均量張']>=1000,a
    b=analyze_history(hist_fixture(),asof='2026-10-12')
    assert b['H114紅K吞噬']=='無',b  # tomorrow's green candle not visible
    x=hist_fixture(); x['成交量']=x['成交股數'];x=x.drop(columns=['成交股數'])
    b=analyze_history(x,asof='2026-10-13')
    assert b['H114K線資料狀態']=='VALID_OHLC_ONLY_VOLUME_UNIT_UNKNOWN',b
    assert b['H114當日成交量張'] is None
    return a


def test_revenue_value():
    x=rich_row()
    a=evaluate(x,info_asof='2026-10-14')
    assert a['H114營收創高']=='近12月新高',a
    assert a['H114成長低估分'] is not None,a
    assert a['H114入榜身分']=='B_ENHANCED_RESEARCH_ONLY',a
    # Future month with a fantastically large value must NOT leak into earlier report.
    x['H114月營收歷史'].append({'year_month':'202610','revenue_twd':999000000000.,
                                'released_at':'2026-11-10','source':'TWSE'})
    b=evaluate(x,info_asof='2026-10-14')
    assert a['H114營收年增率%']==b['H114營收年增率%']
    assert a['H114營收資料年月']==b['H114營收資料年月']=='202609'
    # Last year's revenue shouldn't be silently used as if current.
    st=evaluate(rich_row(H114月營收歷史=monthly_fixture()[:14]),info_asof='2026-10-14')
    assert st['H114營收資料狀態']=='STALE_REVENUE',st


def test_gate_and_volume():
    technical=analyze_history(hist_fixture(),asof='2026-10-13')
    x=rich_row(**technical)
    a=evaluate(x,info_asof='2026-10-13')
    assert a['H114K線反轉分'] is not None,a
    assert a['H114入榜身分']=='B_ENHANCED_RESEARCH_ONLY'
    x['H113入榜資格']='QUALIFIED'
    a=evaluate(x,info_asof='2026-10-13')
    assert a['H114入榜身分']=='A_CERTIFIED_RESEARCH'
    x['H109決策權威']='FADE_VETO'
    a=evaluate(x,info_asof='2026-10-13')
    assert a['H114入榜身分']=='BLOCKED',a
    x['H109決策權威']='RESEARCH_ELIGIBLE';x['最新成交量_張']=150
    # H114 real historical volume is valid and takes priority; explicitly update it to low lot count.
    x['H114當日成交量張']=150
    a=evaluate(x,info_asof='2026-10-13')
    assert a['H114入榜身分']=='BLOCKED',a
    # No red K proof if OHLCV is absent, even if a summary pattern says bullish.
    x=rich_row(H114紅K吞噬='確認形態｜需後續確認',H114K線資料狀態='NO_FULL_OHLCV')
    a=evaluate(x,info_asof='2026-10-13')
    assert a['H114K線反轉分'] is None


def test_watch():
    rows=[]
    for code,group,score in [('1111','被動元件',87.),('2222','被動元件',86.),('3333','PCB',81.),('4444','封測',77.)]:
        rows.append({'股票代號':code,'股票名稱':code,'類別':group,'市場別':'上市',
                     'H114入榜身分':'B_ENHANCED_RESEARCH_ONLY','H114研究優先分':score,'H114主導策略':'VOLUME_BASE'})
    frame=build_parallel_watch({'research':pd.DataFrame(rows[:2]),'waiting':pd.DataFrame(rows[2:])})
    assert list(frame['股票代號'][:3])==['1111','3333','4444'],frame[['股票代號','類別']]
    assert set(frame['H114研究聲明'])=={'研究候選｜不得視為正式買進，須通過原有Formal治理'}


def test_patcher():
    template='''def example():\n    if True:\n        signal_snapshot = compute_signal_snapshot(hist_df)\n        return {\n            "signal_snapshot": signal_snapshot,\n            "K線最後交易日": _safe_str(bundle.get("history_last_date")),\n        }\n    sheets = [\n'''+D+'\n    ]\n'
    updated,status=patch(template)
    assert status=='PATCH_READY'
    assert patch(updated)[1]=='ALREADY_PATCHED'
    assert 'h114_parallel_watch' in updated
    assert 'godpick_h114_parallel_alpha' in updated
    # Without a version-compatible anchor, patcher must refuse rather than damage file.
    try:patch(template.replace(D,''))
    except RuntimeError:pass
    else:raise AssertionError('patch should refuse incompatible page')


if __name__=='__main__':
    test_history();test_revenue_value();test_gate_and_volume();test_watch();test_patcher()
    print('H114 REGRESSION PASS: history, engulfing, time cutoff, low volume, A/B, veto, sector diversity, patcher')

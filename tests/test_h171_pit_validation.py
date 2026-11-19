# -*- coding: utf-8 -*-
from datetime import date, timedelta
import sys
from pathlib import Path
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from godpick_h171_pit_validation import (normalize_official_ytd, ttm_eps,
    profit_breakout, forward_performance)

STUDY = '2026-11-19'
OBSERVED = '2026-11-18T16:45:00+08:00'


def record(period, *, eps=None, rev=100, gross=40, op=20, net=12, observed=OBSERVED, code='3227', market='上市'):
    return dict(code=code, market=market, year_quarter=period,
                report_date='2026-11-17', available_at=observed,
                availability_basis='FIRST_OBSERVED', source='TWSE' if market=='上市' else 'TPEx',
                eps_ytd=eps, revenue_ytd=rev, gross_profit_ytd=gross,
                operating_profit_ytd=op, net_profit_ytd=net)


def eps_ledger():
    return [record('2025Q3',eps=3), record('2025Q4',eps=5),
            record('2026Q1',eps=2), record('2026Q2',eps=5), record('2026Q3',eps=9)]


def finance_ledger():
    rows=[]
    qnums=[(2025,3,100,35,16,10),(2025,4,100,36,18,11),
           (2026,1,100,40,19,12),(2026,2,100,41,20,13),(2026,3,100,46,25,15)]
    sums={'r':200,'g':70,'op':32,'net':20}  # 2025Q2 cumulative basis
    for year, q, r, g, op, net in qnums:
        if q == 1: sums={k:0 for k in ('r','g','op','net')}
        for k, x in zip(('r','g','op','net'),(r,g,op,net)):sums[k]+=x
        rows.append(record(f'{year}Q{q}',rev=sums['r'],gross=sums['g'],op=sums['op'],net=sums['net']))
    # Recover 2025Q2 so 2025Q3 single-quarter can be reconstructed.
    rows.insert(0, record('2025Q2',rev=200,gross=70,op=32,net=20))
    return rows


def test_h171_eps_five_ytd_rebuilds_four_quarters():
    z=ttm_eps(eps_ledger(),code='3227',market='上市',study_day=STUDY)
    assert z['status']=='RECONSTRUCTED_TTM_PIT_RESEARCH_ONLY'
    assert z['eps_ttm']==11.0 # Q4 2 + Q1 2 + Q2 3 + Q3 4
    assert z['quarter_eps']=={'2025Q4':2,'2026Q1':2,'2026Q2':3,'2026Q3':4}
    assert z['trade_authority_changed'] is False


def test_h171_eps_missing_prior_ytd_blocks_ttm():
    rows=[r for r in eps_ledger() if r['year_quarter']!='2025Q3']
    z=ttm_eps(rows,code='3227',market='上市',study_day=STUDY)
    assert z['eps_ttm'] is None and '2025Q3' in z['missing_ytd_periods']


def test_h171_eps_future_observation_not_backfilled():
    rows=eps_ledger();rows[0]['available_at']='2026-11-19T01:00:00+08:00'
    z=ttm_eps(rows,code='3227',market='上市',study_day=STUDY)
    assert z['eps_ttm'] is None
    assert z['source_status']['rejected']['NOT_POINT_IN_TIME']==1


def test_h171_eps_unverified_source_not_accepted():
    rows=eps_ledger();rows[0]['source']='WEB_UNVERIFIED'
    assert ttm_eps(rows,code='3227',market='上市',study_day=STUDY)['eps_ttm'] is None


def test_h171_eps_conflicting_restatement_fails_closed():
    rows=eps_ledger()+[record('2026Q3',eps=9.2)]
    assert ttm_eps(rows,code='3227',market='上市',study_day=STUDY)['eps_ttm'] is None


def test_h171_eps_does_not_mix_market():
    rows=eps_ledger();rows[0]['market']='上櫃';rows[0]['source']='TPEx'
    assert ttm_eps(rows,code='3227',market='上市',study_day=STUDY)['eps_ttm'] is None


def test_h171_eps_negative_is_valid():
    rows=eps_ledger();rows[-1]['eps_ytd']=-2
    assert ttm_eps(rows,code='3227',market='上市',study_day=STUDY)['eps_ttm']==0


def test_h171_breakout_latest_margin_and_positive_profit_yoy():
    z=profit_breakout(finance_ledger(),code='3227',market='上市',study_day=STUDY)
    assert z['five_quarter_margin_breakout'] and z['profit_growth_yoy']
    assert z['certified_for_research'] and z['trade_authority_changed'] is False


def test_h171_breakout_requires_five_quarters():
    rows=[r for r in finance_ledger() if r['year_quarter']!='2025Q2']
    assert not profit_breakout(rows,code='3227',market='上市',study_day=STUDY)['certified_for_research']


def test_h171_breakout_net_yoy_not_positive():
    rows=finance_ledger()
    rows[-1]['net_profit_ytd']=rows[-2]['net_profit_ytd']-2 # latest Q3 negative
    assert not profit_breakout(rows,code='3227',market='上市',study_day=STUDY)['certified_for_research']


def test_h171_normalizer_never_fabricates_missing_financial_fields():
    data=[{'出表日期':'1151118','年度':'115','季別':'3','公司代號':'3227','基本每股盈餘（元）':'5.25'}]
    z=normalize_official_ytd(data,market='TWSE',observed_at=OBSERVED)
    assert len(z)==1 and z[0]['eps_ytd']==5.25 and z[0]['revenue_ytd'] is None
    assert z[0]['year_quarter']=='2026Q3'


def test_h171_normalizer_blocks_future_report():
    data=[{'出表日期':'1151120','年度':'115','季別':'3','公司代號':'3227','基本每股盈餘（元）':'5.25'}]
    assert normalize_official_ytd(data,market='TWSE',observed_at=OBSERVED)==[]


def make_prices():
    return [dict(code='3227',trade_date=d,close=p,available_at='2026-11-18T16:00:00+08:00')
            for d,p in [('2026-11-09',100),('2026-11-10',101),('2026-11-11',98),
                        ('2026-11-12',102),('2026-11-13',104),('2026-11-16',99)]]


def test_h171_t_plus_uses_sessions_not_calendar_days():
    prices=make_prices()
    sessions=[r['trade_date'] for r in prices]
    z=forward_performance([dict(code='3227',run_id='r1',signal_date='2026-11-09')],
                          prices,sessions,evaluated_at=OBSERVED,horizons=(1,5,10))
    result=z['signals'][0]['returns']
    assert result['T+5']['target_session']=='2026-11-16' and result['T+5']['return_pct']==-1
    assert result['T+10']['status']=='NOT_MATURED'
    assert z['summary']['T+1']['win_rate_pct']==100
    assert z['trade_authority_changed'] is False


def test_h171_t_plus_does_not_skip_missing_trading_session():
    prices=make_prices()
    sessions=[r['trade_date'] for r in prices]
    prices=[p for p in prices if p['trade_date']!='2026-11-10']
    z=forward_performance([dict(code='3227',run_id='r1',signal_date='2026-11-09')],prices,sessions,
                          evaluated_at=OBSERVED,horizons=(1,))
    assert z['signals'][0]['returns']['T+1']['status']=='MISSING_OR_CONFLICTING_EXIT_CLOSE'
    assert z['summary']['T+1']['evaluated']==0


def test_h171_t_plus_unknown_entry_does_not_invent_return():
    z=forward_performance([dict(code='3227',run_id='r1',signal_date='2026-11-09')],[],
                          ['2026-11-09','2026-11-10'],evaluated_at=OBSERVED)
    assert z['signals'][0]['returns']['T+1']['return_pct'] is None


def test_h171_t_plus_conflicting_quotes_block():
    prices=make_prices()+[dict(code='3227',trade_date='2026-11-10',close=200,
                               available_at='2026-11-18T16:00:00+08:00')]
    z=forward_performance([dict(code='3227',run_id='r1',signal_date='2026-11-09')],prices,
                          [r['trade_date'] for r in make_prices()],evaluated_at=OBSERVED,horizons=(1,))
    assert z['summary']['T+1']['evaluated']==0


def test_h171_t_plus_observed_after_evaluation_not_used():
    prices=make_prices()
    prices[1]['available_at']='2026-11-19T09:00:00+08:00'
    z=forward_performance([dict(code='3227',run_id='r1',signal_date='2026-11-09')],prices,
                          [r['trade_date'] for r in make_prices()],evaluated_at=OBSERVED,horizons=(1,))
    assert z['summary']['T+1']['evaluated']==0


def test_h171_t_plus_unsorted_sessions_rejected():
    with pytest.raises(ValueError):
        forward_performance([], [], ['2026-11-10','2026-11-09'],evaluated_at=OBSERVED)

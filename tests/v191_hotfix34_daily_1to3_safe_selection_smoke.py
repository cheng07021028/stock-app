import pandas as pd
from godpick_daily_safe_selection import apply_daily_safe_selection


def row(code, *, market=60, entry=70, risk=68, buy=65, op=72, rr=1.8, stop=5.0, amount=800, chase=35, gap=2.0, prob=58, veto='', bucket='盤中雷達追蹤', klag=0, olag=0):
    return {
        '股票代號': code,
        '正式推薦分區': bucket,
        '大盤橋接分數': market,
        '大盤原始橋接狀態': '中性偏多' if market >= 55 else '中性',
        'K線落後交易日': klag,
        'K線資料新鮮度': '最新' if klag == 0 else '落後',
        '官方因子落後交易日': olag,
        '官方因子新鮮度': '有效',
        'Entry進場買點分': entry,
        'Risk風控安全分': risk,
        '買進分數': buy,
        '可操作分': op,
        '進場可執行分': op,
        '路徑風險報酬比': rr,
        '停損距離_隔日%': stop,
        '成交額百萬': amount,
        '追價風險分': chase,
        '距最近可執行買點%': gap,
        '主流資金分': 65,
        '族群攻擊強度': 60,
        '股神推薦優先分': 75,
        'AI綜合決策分': 72,
        'H32隔日上漲機率%': prob,
        'H32隔日預估漲跌幅%': 0.8,
        'H32後續波段預估漲幅%': 5.0,
        '正式推薦排除原因': veto,
    }


def test_neutral_targets_two_and_backfills_safely():
    df = pd.DataFrame([row('1111'), row('2222', entry=63, risk=59, buy=55, op=63, rr=1.3, stop=6.8, chase=55, gap=4.5), row('3333', rr=0.7)])
    out = apply_daily_safe_selection(df)
    picks = out[out['H34每日精選'].eq('是')]
    assert len(picks) == 2
    assert set(picks['正式推薦分區']).issubset({'正式下週主推薦','A-｜準主推薦小量試單'})
    assert out.loc[out['股票代號'].eq('3333'), 'H34每日精選'].iloc[0] == '否'


def test_bullish_targets_three():
    df = pd.DataFrame([row(str(i), market=70, entry=67+i, risk=64+i) for i in range(1,5)])
    out = apply_daily_safe_selection(df)
    assert int(out['H34每日目標檔數'].iloc[0]) == 3
    assert int(out['H34每日精選'].eq('是').sum()) == 3


def test_defensive_targets_one():
    df = pd.DataFrame([row('1', market=46), row('2', market=46, entry=64, risk=60, rr=1.3)])
    out = apply_daily_safe_selection(df)
    assert int(out['H34每日目標檔數'].iloc[0]) == 1
    assert int(out['H34每日精選'].eq('是').sum()) == 1


def test_lockdown_allows_zero():
    r = row('1', market=35)
    r['大盤原始橋接狀態'] = 'LOCKDOWN｜全面禁買'
    out = apply_daily_safe_selection(pd.DataFrame([r]))
    assert int(out['H34每日目標檔數'].iloc[0]) == 0
    assert out['H34每日精選'].eq('是').sum() == 0


def test_hard_veto_never_rescued():
    df = pd.DataFrame([row('1', veto='過熱禁買'), row('2', entry=64, risk=60, buy=55, op=63, rr=1.3, stop=6.5, chase=55)])
    out = apply_daily_safe_selection(df)
    assert out.loc[out['股票代號'].eq('1'), 'H34每日精選'].iloc[0] == '否'


def test_stale_kline_never_rescued():
    df = pd.DataFrame([row('1', klag=1), row('2', entry=64, risk=60, buy=55, op=63, rr=1.3, stop=6.5, chase=55)])
    out = apply_daily_safe_selection(df)
    assert out.loc[out['股票代號'].eq('1'), 'H34每日精選'].iloc[0] == '否'


def test_existing_formal_preserved_and_ranked_first():
    df = pd.DataFrame([row('1', bucket='正式下週主推薦'), row('2', entry=64, risk=60, buy=55, op=63, rr=1.3, stop=6.5, chase=55)])
    out = apply_daily_safe_selection(df)
    x = out[out['股票代號'].eq('1')].iloc[0]
    assert x['正式推薦分區'] == '正式下週主推薦'
    assert x['H34每日精選'] == '是'


def test_no_safe_stock_does_not_force_quota():
    df = pd.DataFrame([row('1', rr=0.5), row('2', stop=12), row('3', chase=80)])
    out = apply_daily_safe_selection(df)
    assert out['H34每日精選'].eq('是').sum() == 0

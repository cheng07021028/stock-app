# -*- coding: utf-8 -*-
from pathlib import Path
import tempfile
import pandas as pd

from godpick_h104_daily_discovery import continuity, load_history, save_discovery_snapshot
from godpick_h105_future_blackhorse_engine import analyze_candidate, decorate_decision_tables


def base(code, name, sector, ret1=0.5, repeat=0, sector_days=0, prev_bh=None):
    d = {
        '股票代號': code, '股票名稱': name, '類別': sector, '市場別': '上市',
        'H99市場資料日': '2026-10-05',
        '法人籌碼官方分數': 82, '三大法人近1日合計': 500, '三大法人近3日合計': 300,
        '三大法人近5日合計': -100, '外資近1日買賣超': 300, '外資近5日買賣超': -20,
        '投信近1日買賣超': 80, '投信近5日買賣超': 0,
        '大戶鎖碼分數': 82, 'TDCC千張大戶週變化pp': 0.7,
        'H57飆股發動前兆分': 78, 'H57波動壓縮分': 80, 'H57壓縮轉擴張分': 70,
        'H57相對強度轉折分': 76, 'H57提前視窗分': 80,
        '族群輪動分': 78, '主流資金分': 75, '類股熱度分數': 70, '類股熱度排名': 6,
        '族群輪動狀態': '升溫', '同族群強勢比例': 68,
        '進場時機分數': 65, 'H89執行品質分': 65, 'H99成本後RR': 1.7,
        '今日漲幅%': ret1, '近5日漲幅%': 3.0, '當日量比': 1.25, '追價風險分數': 35,
        'H104先前入選日數': repeat, 'H104近期族群入選日數': sector_days,
        'H104推薦性質': '延續追蹤｜非首次推薦' if repeat else '近期首次觀察｜已知歷史未見',
    }
    if prev_bh is not None:
        d['H104前次黑馬分'] = prev_bh
    return d


def main():
    # Optional broker/news evidence must not veto a core-complete research candidate.
    a = analyze_candidate(base('1001','A','半導體'))
    assert a['H105核心證據完整度%'] == 100.0, a
    assert a['H105選配證據完整度%'] == 0.0, a
    assert a['H105證據完整度%'] == 80.0, a
    assert a['H105券商分點分'] is None and a['H105催化未反映分'] is None, a

    # Repeat penalty exists; evidence improvement can partially waive it.
    stale = analyze_candidate(base('1002','B','被動元件', repeat=4, sector_days=5, prev_bh=80))
    improved_row = base('1003','C','AI伺服器', repeat=4, sector_days=5, prev_bh=50)
    improved = analyze_candidate(improved_row)
    assert stale['H107重複推薦懲罰'] > improved['H107重複推薦懲罰'], (stale, improved)

    # Build an intentionally bad research pool: two already-fired rows and repeated sectors.
    research = pd.DataFrame([
        base('2001','R1','光學鏡頭', ret1=8.0),
        base('2002','R2','電源供應', ret1=7.5),
        base('2003','R3','被動元件', repeat=4, sector_days=6, prev_bh=80),
        base('2004','R4','被動元件', repeat=3, sector_days=6, prev_bh=78),
        base('2005','R5','AI伺服器', repeat=2, sector_days=4, prev_bh=75),
    ])
    waiting = pd.DataFrame([
        base('3001','W1','半導體'), base('3002','W2','被動元件'), base('3003','W3','AI伺服器'),
        base('3004','W4','散熱'), base('3005','W5','光學鏡頭'), base('3006','W6','電源供應'),
    ])
    out = decorate_decision_tables({'research':research, 'waiting':waiting}, candidate_df=pd.concat([research, waiting], ignore_index=True))
    rr = out['research']
    assert len(rr) == 5, rr[['股票代號','類別','H105今日強勢排除']]
    assert not rr['H105今日強勢排除'].fillna('').eq('是').any(), rr[['股票代號','類別','H105今日強勢排除']]
    assert rr['類別'].nunique() == 5, rr[['股票代號','類別','H107研究池調整']]
    assert {'2001','2002'}.issubset(set(out['waiting']['股票代號'])), out['waiting'][['股票代號','H107研究池調整']]

    # Daily discovery snapshot becomes a real history authority on next run.
    with tempfile.TemporaryDirectory() as td:
        snap = save_discovery_snapshot(td, out)
        assert snap['written'] > 0, snap
        hist = load_history(td)
        assert hist, hist
        row = base('3001','W1','半導體')
        row['H99市場資料日'] = '2026-10-06'
        c = continuity(row, hist)
        assert c['H104先前入選日數'] >= 1, c
        assert '延續追蹤' in c['H104推薦性質'], c

    print('H107_REPEAT_DIVERSITY_SMOKE: PASS')
    print('research=', rr[['股票代號','類別','H107新鮮機會分','H107研究池調整']].to_dict('records'))

if __name__ == '__main__':
    main()

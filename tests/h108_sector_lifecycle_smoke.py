# -*- coding: utf-8 -*-
from pathlib import Path
import tempfile
import pandas as pd

from godpick_h104_daily_discovery import save_sector_snapshot, load_sector_history, sector_continuity
from godpick_h105_future_blackhorse_engine import analyze_candidate, decorate_decision_tables


def base(code, name, sector, *, repeat=0, sector_days=0, ret1=0.4, h102_shock=70, h102_state='ROTATION'):
    return {
        '股票代號': code, '股票名稱': name, '類別': sector, '市場別': '上市', 'H99市場資料日': '2026-10-06',
        '法人籌碼官方分數': 78, '三大法人近1日合計': 500, '三大法人近3日合計': 800, '三大法人近5日合計': -100,
        '外資近1日買賣超': 300, '外資近3日買賣超': 400, '外資近5日買賣超': -20,
        '投信近1日買賣超': 60, '投信近3日買賣超': 80, '投信近5日買賣超': 0,
        '大戶鎖碼分數': 72, 'TDCC千張大戶週變化pp': 0.2,
        'H57飆股發動前兆分': 72, 'H57波動壓縮分': 75, 'H57壓縮轉擴張分': 68,
        'H57相對強度轉折分': 73, 'H57提前視窗分': 76,
        'H102族群衝擊分': h102_shock, 'H102族群動態狀態': h102_state,
        '進場時機分數': 65, 'H89執行品質分': 66, 'H99成本後RR': 1.7,
        '今日漲幅%': ret1, '近5日漲幅%': 4.0, '當日量比': 1.2, '追價風險分數': 35,
        'H104先前入選日數': repeat, 'H104近期族群入選日數': sector_days,
        'H104推薦性質': '延續追蹤｜非首次推薦' if repeat else '近期首次觀察｜已知歷史未見',
    }


def passive_sector():
    # Mirrors the 2026-10-06 export: old broad label was neutral, but H102 shock already said IGNITION.
    return {
        '類別': '被動元件', '類股熱度排名': 2, '類股熱度分數': 61.1491, '類股加速度': 49.0591,
        '族群資金流分數': 61.3634, '同族群強勢比例': 54.0670, '同族群平均量能分': 63.7267,
        '類股平均漲幅': 6.3485, '族群樣本可信度': 76.1905, '族群輪動狀態': '中性輪動',
    }


def pcb_mature_sector():
    return {
        '類別': 'PCB載板', '類股熱度排名': 1, '類股熱度分數': 71.9979, '類股加速度': 50.4964,
        '族群資金流分數': 65.1881, '同族群強勢比例': 69.9690, '同族群平均量能分': 56.8355,
        '類股平均漲幅': 12.9546, '族群樣本可信度': 68.75, '族群輪動狀態': '輪動轉強',
    }


def main():
    # 1) Passive components must be recognized as a true ignition despite the broad table still saying neutral.
    p = base('8042', '金山電', '被動元件', repeat=1, sector_days=4, ret1=-1.27, h102_shock=92.91, h102_state='IGNITION｜族群加速')
    a = analyze_candidate(p, sector_row=passive_sector())
    assert 'IGNITION' in a['H108族群生命週期'], a
    assert a['H108族群確認分'] >= 76, a
    assert 'SECOND-WAVE' in a['H108個股族群角色'], a
    assert a['H108族群波段加分'] > 0, a
    assert a['H108族群懲罰減免'] > 0, a

    # 2) A sector already averaging double-digit gains is mature, not a fresh next-wave sector.
    m = base('4958', '臻鼎-KY', 'PCB載板', h102_shock=88, h102_state='IGNITION｜族群加速')
    ma = analyze_candidate(m, sector_row=pcb_mature_sector())
    assert 'MATURE' in ma['H108族群生命週期'], ma
    assert ma['H108族群波段加分'] < 0, ma

    # 3) Research pool of only two slots must reserve one confirmed sector anchor, then keep cross-sector freshness.
    research = pd.DataFrame([
        base('2467','志聖','其他電子', h102_shock=55, h102_state='NEUTRAL'),
        base('2382','廣達','AI伺服器', h102_shock=58, h102_state='NEUTRAL'),
    ])
    waiting = pd.DataFrame([
        base('8042','金山電','被動元件', repeat=1, sector_days=4, h102_shock=92.91, h102_state='IGNITION｜族群加速'),
        base('3026','禾伸堂','被動元件', repeat=1, sector_days=4, h102_shock=92.91, h102_state='IGNITION｜族群加速'),
        base('3357','臺慶科','被動元件', repeat=1, sector_days=4, h102_shock=92.91, h102_state='IGNITION｜族群加速'),
        base('1319','東陽','車用電子', h102_shock=60, h102_state='NEUTRAL'),
    ])
    source = pd.concat([research, waiting], ignore_index=True)
    sector_df = pd.DataFrame([
        passive_sector(),
        {'類別':'其他電子','類股熱度排名':8,'類股熱度分數':55,'類股加速度':50,'族群資金流分數':52,'同族群強勢比例':48,'同族群平均量能分':50,'類股平均漲幅':2.0,'族群樣本可信度':70,'族群輪動狀態':'中性輪動'},
        {'類別':'AI伺服器','類股熱度排名':10,'類股熱度分數':54,'類股加速度':48,'族群資金流分數':50,'同族群強勢比例':45,'同族群平均量能分':50,'類股平均漲幅':1.5,'族群樣本可信度':70,'族群輪動狀態':'中性輪動'},
        {'類別':'車用電子','類股熱度排名':15,'類股熱度分數':50,'類股加速度':48,'族群資金流分數':48,'同族群強勢比例':45,'同族群平均量能分':48,'類股平均漲幅':1.0,'族群樣本可信度':65,'族群輪動狀態':'中性輪動'},
    ])
    out = decorate_decision_tables({'research':research, 'waiting':waiting, 'health':pd.DataFrame()}, candidate_df=source, sector_df=sector_df)
    rr = out['research']
    assert len(rr) == 2, rr[['股票代號','類別','H108族群生命週期','H108研究優先分']]
    assert (rr['類別'] == '被動元件').any(), rr[['股票代號','類別','H107研究池調整']]
    assert rr['類別'].nunique() >= 2, rr[['股票代號','類別','H107研究池調整']]

    # 4) Sector history has a real previous-day authority and computes changes.
    with tempfile.TemporaryDirectory() as td:
        s1 = pd.DataFrame([passive_sector()])
        save_sector_snapshot(td, s1, '2026-10-06')
        hist = load_sector_history(td)
        s2 = passive_sector(); s2.update({'類股熱度排名':1, '族群資金流分數':72.0, '同族群強勢比例':66.0})
        c = sector_continuity(s2, hist, '2026-10-07')
        assert c['H108族群前次資料日'] == '2026-10-06', c
        assert c['H108族群排名改善'] == 1, c
        assert c['H108族群資金流變化'] > 0 and c['H108族群廣度變化'] > 0, c

    # 5) Formal authority remains untouched.
    formal = pd.DataFrame([base('9991','FormalA','被動元件'), base('9992','FormalB','其他電子')])
    fo = decorate_decision_tables({'actionable':formal.copy(), 'research':pd.DataFrame(), 'waiting':pd.DataFrame()}, candidate_df=formal, sector_df=sector_df)
    assert fo['actionable']['股票代號'].astype(str).tolist() == ['9991','9992']

    print('H108_SECTOR_LIFECYCLE_SMOKE: PASS')
    print(rr[['股票代號','股票名稱','類別','H108族群確認分','H108族群生命週期','H108個股族群角色','H108研究優先分','H107研究池調整']].to_dict('records'))


if __name__ == '__main__':
    main()

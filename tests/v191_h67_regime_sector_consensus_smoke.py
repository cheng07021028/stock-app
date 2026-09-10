# -*- coding: utf-8 -*-
import pandas as pd
from godpick_h67_regime_consensus_engine import VERSION, apply_h67_regime_consensus


def _row(code, h66=80, market=70, risk='低風險', sector=72, sector_status='強勢｜資金流入', chase=40, exhaust=35, formal='A-MINUS'):
    return {
        '股票代號':code,'股票名稱':'T'+code,'類別':'半導體','H66版本':'v191_h66_adaptive_alpha_t1_timing_truth_20260909',
        'H66T1自適應排序分':h66,'H66T1層級':'A1｜T+1優先觀察','H66收盤品質分':78,'H66法人加速度分':76,
        'H66主流點火分':72,'H66技術買點分':74,'H66量能流動性分':80,'H66結構品質分':72,
        '大盤橋接分數':market,'市場環境分數':market,'大盤風險等級':risk,'大盤風險燈號':'綠燈' if market>=60 else '黃燈',
        '族群資金流分數':sector,'H53族群資金分':sector,'H53族群共振分':sector,'H51族群主線分':sector,
        '強勢族群等級':sector_status,'族群輪動狀態':sector_status,'追價風險分':chase,'H54耗竭風險分':exhaust,
        '今日漲幅%':2.5,'當日收盤位置%':82,'H64有效權威':formal,'H56T1確認分':65,'H56盤前重驗需求':'次日重驗',
    }


def main():
    df=pd.DataFrame([
        _row('1111'),
        _row('2222', market=40, risk='中高風險', sector=38, sector_status='弱勢｜資金退潮'),
        _row('3333', market=68, sector=70, chase=82, exhaust=80),
    ])
    out=apply_h67_regime_consensus(df)
    assert len(out)==3
    assert out['H67版本'].eq(VERSION).all()
    assert out['H67全市場順位'].nunique()==3
    assert ((out['H67T1治理分']>=0)&(out['H67T1治理分']<=100)).all()
    strong=out.loc[out['股票代號']=='1111'].iloc[0]
    weak=out.loc[out['股票代號']=='2222'].iloc[0]
    chase=out.loc[out['股票代號']=='3333'].iloc[0]
    assert strong['H67研究優先層級'].startswith('P1')
    assert weak['H67T1治理分'] < strong['H67T1治理分']
    assert not weak['H67研究優先層級'].startswith('P1')
    assert chase['H67追價耗竭扣分'] >= 10
    assert 'Formal' not in str(strong['H67研究優先層級'])
    assert strong['H64有效權威']=='A-MINUS'
    assert '不得' in strong['H67權威邊界']
    print('PASS v191_h67_regime_sector_consensus_smoke')

if __name__=='__main__': main()

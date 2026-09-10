# -*- coding: utf-8 -*-
from godpick_t1_trade_truth import build_h67_rank_learning_summary


def main():
    rows=[]
    for d in ['2026-09-01','2026-09-02','2026-09-03']:
        for rank in range(1,6):
            alpha=3.0-rank*0.7
            rows.append({'T1成熟':True,'推薦日期':d,'H67全市場順位':rank,'H67T1治理分':90-rank*5,
                         'Selection Alpha%':alpha,'隔日候選漲跌%':alpha+0.2,'H67研究優先層級':'P1｜次日優先研究' if rank<=2 else 'P2｜次日次優先研究'})
    out=build_h67_rank_learning_summary(rows)
    assert out['H67排名成熟交易日']==3
    assert out['H67平均RankIC'] is not None and out['H67平均RankIC']>0.8
    assert out['H67_Top3樣本']==9
    assert out['H67_Top1樣本']==3
    assert out['H67_P1成熟樣本']==6
    print('PASS v191_h67_t1_rank_learning_smoke')

if __name__=='__main__': main()

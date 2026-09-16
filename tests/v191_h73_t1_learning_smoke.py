# -*- coding: utf-8 -*-
from godpick_t1_trade_truth import build_h73_learning_summary
rows=[]
for d in range(10):
    for j in range(4):
        score=80-j*5
        alpha=3-j*1.2
        rows.append({"T1成熟":True,"推薦日期":f"2026-10-{d+1:02d}","H73學習快照狀態":"SNAPSHOT-READY",
                     "H73研究層級":"L1｜領先共振核心" if j==0 else "L2｜領先優先觀察" if j<3 else "L3｜候選追蹤",
                     "H73研究排序分":score,"H73全市場順位":j+1,"隔日候選漲跌%":alpha+0.5,"Selection Alpha%":alpha})
out=build_h73_learning_summary(rows)
assert out["H73學習快照成熟樣本"]==40
assert out["H73學習啟用狀態"]=="ACTIVE"
assert out["H73排名成熟交易日"]==10
assert out["H73平均RankIC"] is not None and out["H73平均RankIC"]>0.9
assert out["H73平均NDCG@10"] is not None
print("PASS H73 forward-only T1 learning")

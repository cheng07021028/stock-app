# -*- coding: utf-8 -*-
from godpick_t1_trade_truth import build_h72_learning_summary


def main():
    rows=[]
    # 35 immutable H72 snapshots => learning may activate.
    for i in range(35):
        tier = "E1｜多模型共振核心" if i < 10 else "E2｜多模型優先觀察" if i < 25 else "E3｜多模型候選追蹤"
        score = 90 - (i % 10) * 2
        alpha = 2.0 - (i % 10) * 0.25
        rows.append({
            "T1成熟": True, "H72學習快照狀態":"SNAPSHOT-READY", "H72研究層級":tier,
            "H72全市場順位": i+1, "H72風險調整分": score, "推薦日期": f"2026-09-{1 + i//7:02d}",
            "隔日候選漲跌%": alpha + 0.2, "Selection Alpha%": alpha,
        })
    # Legacy/missing snapshot rows must be ignored, even if T1 matured.
    for _ in range(12):
        rows.append({"T1成熟":True,"H72研究層級":"E1｜多模型共振核心","隔日候選漲跌%":9.9,"Selection Alpha%":9.9})
    s=build_h72_learning_summary(rows)
    assert s["H72學習快照成熟樣本"] == 35
    assert s["H72學習啟用狀態"] == "ACTIVE"
    assert s["H72_E1成熟樣本"] == 10
    assert s["H72_E2成熟樣本"] == 15
    assert s["H72_E3成熟樣本"] == 10
    assert s["H72排名成熟交易日"] >= 5
    assert s["H72平均RankIC"] is not None
    assert s["H72平均NDCG@10"] is not None
    # Under 30 ready rows must remain warmup.
    warm=build_h72_learning_summary(rows[:20])
    assert warm["H72學習快照成熟樣本"] == 20
    assert str(warm["H72學習啟用狀態"]).startswith("WARMUP")
    print("PASS H72 forward-only T1 learning")

if __name__ == "__main__":
    main()

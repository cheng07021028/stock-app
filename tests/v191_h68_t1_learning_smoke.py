# -*- coding: utf-8 -*-
from godpick_t1_trade_truth import build_h68_learning_summary

legacy = [{"T1成熟": True, "隔日候選漲跌%": 1.0, "Selection Alpha%": 0.2} for _ in range(302)]
ready = []
for i in range(30):
    ready.append({
        "T1成熟": True,
        "H68學習快照狀態": "SNAPSHOT-READY",
        "H67研究優先層級": "P1｜次日優先研究" if i < 10 else "P2｜次日次優先研究" if i < 20 else "C1｜條件觀察",
        "隔日候選漲跌%": 1.0 if i % 2 == 0 else -0.5,
        "Selection Alpha%": 0.3 if i % 2 == 0 else -0.2,
    })

warm = build_h68_learning_summary(legacy + ready[:10])
assert warm["H68學習快照成熟樣本"] == 10
assert warm["H68舊樣本無完整快照排除數"] == 302
assert str(warm["H68學習啟用狀態"]).startswith("WARMUP")

active = build_h68_learning_summary(legacy + ready)
assert active["H68學習快照成熟樣本"] == 30
assert active["H68舊樣本無完整快照排除數"] == 302
assert active["H68學習啟用狀態"] == "ACTIVE"
assert active["H68_H67_P1成熟樣本"] == 10
assert active["H68_H67_P2成熟樣本"] == 10
assert active["H68_H67_C1成熟樣本"] == 10
print("PASS v191_h68_t1_learning_smoke")

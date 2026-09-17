# -*- coding: utf-8 -*-
import sys, types
_stub=types.ModuleType("godpick_perf_fast_update_v77")
_stub.update_record_perf=lambda *a, **k: (a[0] if a else {})
sys.modules.setdefault("godpick_perf_fast_update_v77", _stub)
from godpick_t1_trade_truth import build_h74_learning_summary, TRUTH_VERSION

def main():
    rows=[
        {"T1成熟":True,"H74學習快照狀態":"SNAPSHOT-READY","H74研究層級":"F1｜新鮮主流資金核心","H74決策總分":80,"推薦日期":"2026-09-01","隔日候選漲跌%":2.0,"Selection Alpha%":1.4},
        {"T1成熟":True,"H74學習快照狀態":"SNAPSHOT-READY","H74研究層級":"F2｜資金加速優先","H74決策總分":70,"推薦日期":"2026-09-01","隔日候選漲跌%":1.0,"Selection Alpha%":0.5},
        {"T1成熟":True,"H74學習快照狀態":"SNAPSHOT-READY","H74研究層級":"F3｜輪動候選","H74決策總分":62,"推薦日期":"2026-09-01","隔日候選漲跌%":-1.0,"Selection Alpha%":-1.2},
        # Legacy row is intentionally excluded: no H74 snapshot backfill.
        {"T1成熟":True,"H74學習快照狀態":"","H74研究層級":"F1｜新鮮主流資金核心","H74決策總分":99,"推薦日期":"2026-08-01","隔日候選漲跌%":9.0,"Selection Alpha%":8.0},
    ]
    out=build_h74_learning_summary(rows)
    assert TRUTH_VERSION=="godpick_t1_trade_truth_v191_h74_fresh_mainstream_capital_rotation_truth_20260917"
    assert out["H74學習快照成熟樣本"]==3
    assert out["H74_F1成熟樣本"]==1 and out["H74_F2成熟樣本"]==1 and out["H74_F3成熟樣本"]==1
    assert out["H74排名成熟交易日"]==1
    print("H74 T1 FORWARD-ONLY LEARNING: PASS")
if __name__=="__main__": main()

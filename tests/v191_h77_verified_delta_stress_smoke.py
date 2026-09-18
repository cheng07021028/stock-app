# -*- coding: utf-8 -*-
import time

import pandas as pd

from godpick_h77_verified_delta_execution_gate import apply_h77_verified_delta_execution_gate


rows = []
for i in range(1700):
    chase = i % 11 == 0
    missing = i % 13 == 0
    rows.append({
        "股票代號": f"{1000+i:04d}", "股票名稱": f"S{i}", "族群名稱": f"G{i%20}",
        "H75版本": "v191_h75_executive_decision_export_20260917",
        "H74決策總分": 70 + i % 10, "H74強勢加速度分": 72 + i % 12,
        "H74主流新鮮度分": 60 + i % 15, "H74法人資金加速度分": 70 + i % 16,
        "H74成交資金加速度分": 72 + i % 15, "H74訊號新鮮分": 66 + i % 12,
        "H74大戶鎖碼狀態": "UNCONFIRMED_NO_PRIOR", "H74熟面孔慣性扣分": 0,
        "近5次入榜次數": i % 3, "連續入榜次數": 0, "H61重複慣性扣分": 0,
        "H72研究層級": "E2｜多模型優先觀察", "H73研究層級": "L2｜領先觀察",
        "H72風險調整分": 70, "H72品質獲利模型分": 68, "H72成長動能模型分": 72,
        "H72法人需求模型分": 76, "H64核心共振分": 68, "H61RR品質分": 50,
        "H62增量機會分": 72, "H64有效權威": "A-MINUS", "H68次日執行狀態": "NO-FORMAL｜僅研究觀察",
        "3日動能加速度百分點": None if missing else 2.0,
        "成交額3日加速度%": None if missing else 18.0,
        "成交量3日加速度%": None if missing else 12.0,
        "三大法人近1日合計": None if missing else 1200,
        "三大法人近3日合計": None if missing else 3000,
        "今日漲幅%": 9.8 if chase else 3.0, "近5日漲幅%": 20.0 if chase else 6.0,
        "收盤距MA20%": 21.0 if chase else 5.0, "當日收盤位置%": 98.0 if chase else 82.0,
        "Entry進場買點分": 65,
    })

base = pd.DataFrame(rows)
started = time.perf_counter()
out = apply_h77_verified_delta_execution_gate(base)
elapsed = time.perf_counter() - started
assert len(out) == 1700
assert out["H77研究順位"].dropna().is_unique
assert out.loc[base["今日漲幅%"].ge(9.5), "H77研究層級"].astype(str).str.startswith("W1").all()
assert out.loc[base["3日動能加速度百分點"].isna(), "H77增量欄位覆蓋數"].eq(0).all()
print(f"PASS H77 1700 stress rows={len(out)} seconds={elapsed:.4f}")

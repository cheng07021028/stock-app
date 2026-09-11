# -*- coding: utf-8 -*-
import pandas as pd
from godpick_h67_regime_consensus_engine import VERSION as H67_VERSION
from godpick_h68_execution_learning_truth import (
    VERSION,
    apply_h68_execution_learning_truth,
    reconcile_h68_formal_summary,
)

BASE = {
    "H67版本": H67_VERSION,
    "H65觀察層級": "W1｜重點觀察",
    "H65多因子觀察分": 78.0,
    "H65全市場觀察百分位%": 96.0,
    "H66T1層級": "A1｜T+1優先觀察",
    "H66T1自適應排序分": 79.0,
    "H66T1全市場順位": 3,
    "H67研究優先層級": "P1｜次日優先研究",
    "H67T1治理分": 77.0,
    "H67全市場順位": 2,
    "官方因子落後交易日": 0,
    "K線落後交易日": 0,
}

rows = []
r = dict(BASE, 股票代號="1111", H64有效權威="NO-FORMAL")
rows.append(r)
r = dict(BASE, 股票代號="2222", H64有效權威="EFFECTIVE-FORMAL", **{"台指期夜盤漲跌%": -2.4})
rows.append(r)
r = dict(BASE, 股票代號="3333", H64有效權威="EFFECTIVE-FORMAL", H67盤前再確認狀態="CONFIRMED｜已有H56確認，但次日仍需價格守價")
rows.append(r)
r = dict(BASE, 股票代號="4444", H64有效權威="EFFECTIVE-FORMAL")
del r["H67全市場順位"]
rows.append(r)

out = apply_h68_execution_learning_truth(pd.DataFrame(rows))
assert out["H68版本"].eq(VERSION).all()
assert out.loc[out["股票代號"].eq("1111"), "H68次日執行狀態"].iloc[0].startswith("NO-FORMAL")
assert out.loc[out["股票代號"].eq("2222"), "H68次日執行狀態"].iloc[0].startswith("BLOCK")
assert out.loc[out["股票代號"].eq("3333"), "H68次日執行狀態"].iloc[0].startswith("READY-COND")
assert out.loc[out["股票代號"].eq("4444"), "H68學習快照狀態"].iloc[0] == "SNAPSHOT-INCOMPLETE"
assert out.loc[out["股票代號"].eq("3333"), "H68學習快照狀態"].iloc[0] == "SNAPSHOT-READY"

summary = pd.DataFrame([{"正式推薦檔數": 4, "正式推薦可用": "是", "本輪結論": "限定資料池｜有正式推薦"}])
authority = pd.DataFrame({"H64有效權威": ["FORMAL-QUALITY-HOLD"] * 4})
fixed = reconcile_h68_formal_summary(summary, authority, {"正式推薦可用": True}, pd.DataFrame(columns=["H63正式推薦順位"]))
row = fixed.iloc[0]
assert int(row["上游Formal檔數"]) == 4
assert int(row["正式推薦檔數"]) == 0
assert row["正式推薦可用"] == "否"
assert "無正式推薦" in str(row["本輪結論"])
print("PASS v191_h68_execution_learning_truth_smoke")

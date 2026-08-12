# -*- coding: utf-8 -*-
from __future__ import annotations
import sys,time
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))
from godpick_super_ai_trade_quality import apply_trade_quality_governance
from godpick_durability_service import CORE_DURABLE_FILES

n=1710
rows=[]
for i in range(n):
    rows.append({
        "股票代號":f"{1000+i%8999:04d}","類別":f"類股{i%30}","股神推薦優先分":70+(i%20),
        "AI Alpha品質分":70+(i%20),"AI Continuation延續分":65+(i%15),"主流主升優先分":65+(i%15),
        "AI Timing時機分":65+(i%20),"AI Risk風控分":65+(i%18),"可操作分":70,
        "路徑風險報酬比":1.55,"今日漲幅%":1.2,"近5日漲幅%":3,"追價風險分":45,"隔日耗竭風險分":45,
        "SuperAI隔日上漲機率%":57,"官方因子資料日期":"2026-08-11","官方因子落後交易日":0,"官方因子新鮮度":"最新已驗證","每日因子來源可信度":100,
        "大盤資料日期":"2026-08-11","大盤資料落後交易日":0,"大盤與K線對齊狀態":"READY｜對齊","大盤資料新鮮度":"最新",
        "是否正式推薦":False,"正式推薦分區":"盤中核心雷達","操作許可":"RADAR",
    })
df=pd.DataFrame(rows)
t0=time.perf_counter(); out=apply_trade_quality_governance(df,{}); sec=time.perf_counter()-t0
assert len(out)==n and "V188股神作戰優先分" in out.columns
# Reasonable CPU guard, deliberately loose for shared CI/container variance.
assert sec < 4.0, sec
assert "godpick_t1_trade_truth.json" in CORE_DURABLE_FILES
assert "godpick_probability_calibration.json" in CORE_DURABLE_FILES
truth_src=(ROOT/"godpick_t1_trade_truth.py").read_text(encoding="utf-8")
assert "persist_json_permanent" in truth_src, "T+1 learning truth must use confirmed permanent persistence after replay"
page=(ROOT/"pages"/"7_股神推薦.py").read_text(encoding="utf-8")
for token in ["refresh_t1_truth_async", "更新 T+1 實戰真相與 AI 機率校準", "V188股神作戰優先分", "V188交易品質治理", "T+1實戰真相", "AI機率校準"]:
    assert token in page, token
print(f"PASS V188 speed/wiring | rows={n} | trade_quality={sec:.4f}s")

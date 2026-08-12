# -*- coding: utf-8 -*-
from __future__ import annotations
import sys, time
from pathlib import Path
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))

from godpick_super_ai_engine import apply_super_ai_engine, SUPER_AI_VERSION
from godpick_super_ai_trade_quality import canonical_execution_rr, V188_VERSION

TOP8 = [
    # code name legacy grade alpha entry risk rr ret1 ret5 chase
    ("2327","國巨*","S+",83.15,66.2,62.5,1.22,7.6789,9.0106,61),
    ("6147","頎邦","S+",80.99,63.6,59.8,0.90,4.8701,11.7647,61),
    ("3711","日月光投控","S+",81.48,72.9,64.1,0.41,-0.1587,7.5214,29),
    ("2313","華通","S",78.0,67.4,62.7,0.63,-1.6279,6.5491,55),
    ("2409","友達","A+",76.0,73.8,64.1,0.65,-3.3333,2.0,55),
    ("4958","臻鼎-KY","A",77.0,66.1,62.3,1.36,4.1445,10.3604,61),
    ("2368","金像電","A",79.0,63.3,58.2,0.37,4.1037,9.0,65),
    ("2330","台積電","B+",82.0,65.6,60.1,0.35,0.6303,4.0,45),
]

rows=[]
for i,(code,name,grade,alpha,entry,risk,rr,ret1,ret5,chase) in enumerate(TOP8):
    rows.append({
        "股票代號":code,"股票名稱":name,"市場別":"上市","類別":"半導體" if code not in {"2313","4958","2368"} else "PCB",
        "股神推薦等級":grade,"股神推薦優先分":93-i*2,"AI Alpha品質分":alpha,"AI Timing時機分":entry,
        "AI Risk風控分":risk,"AI Continuation延續分":75,"可操作分":70,"主流主升優先分":78,
        "路徑風險報酬比":rr,"AI戰術風報比":12.0,"今日漲幅%":ret1,"近5日漲幅%":ret5,"追價風險分":chase,
        "隔日耗竭風險分":chase,"主要進場路徑":"突破確認","最新價":100,"實戰觸發價":102,"觸發後守價":100,"實戰停損參考":96,"第一壓力價":108,
        # Real 8/11 replay: global report was T-1 ready, but these per-stock evidence fields were missing/unknown.
        "官方因子資料日期":"","官方因子落後交易日":999,"官方因子新鮮度":"日期未驗證｜僅供雷達","因子來源可信度":100,
        "大盤資料日期":"","大盤資料落後交易日":999,"大盤資料新鮮度":"日期未驗證｜正式推薦待同步","大盤與K線對齊狀態":"WAIT｜大盤未知／K線2026-08-11",
        "K線最後交易日":"2026-08-11","K線落後交易日":0,"K線資料新鮮度":"最新",
        "正式推薦可用":True,"是否正式推薦":True,"正式推薦分區":"正式推薦","操作許可":"READY｜舊引擎允許",
    })

df=pd.DataFrame(rows)
t0=time.perf_counter(); out=apply_super_ai_engine(df,context={},experience={}); elapsed=time.perf_counter()-t0
assert SUPER_AI_VERSION.endswith("v188_20260812")
assert len(out)==8
# Canonical RR must ignore huge AI tactical RR when conservative RR exists.
assert abs(float(out.loc[out["股票代號"]=="3711","SuperAI執行風報比"].iloc[0])-0.41)<1e-9
assert out.loc[out["股票代號"]=="3711","SuperAI風報比來源"].iloc[0]=="路徑風險報酬比"
# Every real 8/11 top row has missing per-stock evidence / market alignment => no formal new position.
assert not out["V188正式推薦資格"].astype(str).str.startswith("是").any()
assert out["V188個股資料證據"].astype(str).str.startswith("BLOCK").all()
assert out["V188市場對齊治理"].astype(str).str.startswith("WAIT").all()
# RR truth: high legacy S+ does not rescue bad executable RR.
assert "RR 0.41" in out.loc[out["股票代號"]=="3711","V188RR治理"].iloc[0]
assert out.loc[out["股票代號"]=="3711","SuperAI Trade等級"].iloc[0] in {"C","D"}
assert "RR 0.90" in out.loc[out["股票代號"]=="6147","V188RR治理"].iloc[0]
# Prior-day +7.68% must become pullback-only instead of breakout chase.
assert out.loc[out["股票代號"]=="2327","V188T+1追價治理"].iloc[0].startswith("PULLBACK-ONLY")
# Sector concentration never deletes candidates and penalises only 3rd+ same sector.
assert len(out)==8 and (pd.to_numeric(out["V188類股集中扣分"],errors="coerce").fillna(0)>=0).all()

# Cured data scenario: conservative RR 1.65 + T-1 official + aligned market can stay formal.
good=rows[0].copy()
good.update({
    "股票代號":"9999","股票名稱":"測試優質交易","類別":"測試類股","路徑風險報酬比":1.65,"AI戰術風報比":20.0,
    "今日漲幅%":1.2,"近5日漲幅%":3.0,"追價風險分":42,"隔日耗竭風險分":40,
    "官方因子資料日期":"2026-08-10","官方因子落後交易日":1,"官方因子新鮮度":"T-1已驗證｜降級可用","每日因子來源可信度":100,
    "大盤資料日期":"2026-08-11","大盤資料落後交易日":0,"大盤資料新鮮度":"最新","大盤與K線對齊狀態":"READY｜大盤2026-08-11／K線2026-08-11",
    "AI Timing時機分":82,"AI Risk風控分":80,"可操作分":85,"AI Alpha品質分":88,"AI Continuation延續分":84,
})
good_out=apply_super_ai_engine(pd.DataFrame([good]),context={},experience={}).iloc[0]
assert good_out["V188正式推薦資格"].startswith("是"), good_out.to_dict()
assert good_out["V188交易許可"].startswith("READY-COND")
assert float(good_out["SuperAI執行風報比"])==1.65

# RR 1.22 remains radar even after data cure.
rr122=good.copy(); rr122["股票代號"]="9998"; rr122["路徑風險報酬比"]=1.22
rr122_out=apply_super_ai_engine(pd.DataFrame([rr122]),context={},experience={}).iloc[0]
assert rr122_out["V188交易許可"].startswith("RADAR")
assert not rr122_out["V188正式推薦資格"].startswith("是")

print(f"PASS V188 trade-quality governance | rows=8 | elapsed={elapsed:.4f}s | version={V188_VERSION}")

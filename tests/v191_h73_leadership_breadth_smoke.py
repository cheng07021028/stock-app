# -*- coding: utf-8 -*-
import pandas as pd
from godpick_h73_leadership_breadth_engine import VERSION, apply_h73_leadership_breadth


def base(code, sec, base_score, inst, holder, mom, brk, div, t1=60, close=60):
    return {"股票代號":code,"股票名稱":code,"族群名稱":sec,"H72版本":"v191_h72_multi_model_alpha_ensemble_20260915",
            "H72風險調整分":base_score,"H72全市場順位":1,"H72研究層級":"E2｜多模型優先觀察","H72模型分歧度":div,
            "H72法人需求模型分":inst,"H72大戶鎖碼模型分":holder,"H72動能相對強度模型分":mom,"H72突破時機模型分":brk,
            "H72風險Regime模型分":70,"H72成長動能模型分":80,"H66T1自適應排序分":t1,"H66收盤品質分":close,
            "H72市場模式":"DEFENSIVE","H72學習快照狀態":"SNAPSHOT-READY","H64有效權威":"","H68次日執行狀態":"NO-FORMAL｜僅研究觀察"}

rows=[
    base("A1","PCB",69,90,65,78,68,7,70,72), base("A2","PCB",67,82,60,72,62,9,64,65), base("A3","PCB",64,70,58,66,60,11,60,60),
    base("B1","AI",69,35,52,76,68,14,58,72), base("B2","AI",66,78,63,62,55,10), base("B3","AI",65,68,61,58,52,12),
    base("C1","航運",67,64,68,72,61,6,66,68), base("C2","航運",65,62,65,66,58,8), base("C3","航運",63,58,60,62,54,10),
]
df=pd.DataFrame(rows)
out=apply_h73_leadership_breadth(df)
assert out["H73版本"].eq(VERSION).all()
assert out["H73全市場順位"].nunique()==len(out)
# Strong price momentum with weak institutions/holders must be explicitly penalized.
b1=out[out["股票代號"]=="B1"].iloc[0]
assert float(b1["H73分布矛盾扣分"]) >= 10
# Coherent sector leader receives no distribution penalty and stays ahead of its peers.
a1=out[out["股票代號"]=="A1"].iloc[0]
assert float(a1["H73分布矛盾扣分"]) == 0
assert int(a1["H73族群內順位"]) == 1
# Research overlay cannot manufacture Formal or bypass execution truth.
assert out["H64有效權威"].fillna("").eq("").all()
assert out["H68次日執行狀態"].str.startswith("NO-FORMAL").all()
print("PASS H73 leadership breadth / distribution contradiction")

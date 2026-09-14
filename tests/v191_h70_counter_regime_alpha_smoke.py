# -*- coding: utf-8 -*-
from datetime import date
import pandas as pd
from godpick_h70_counter_regime_session_truth import apply_h70_counter_regime_session_truth


def _row(**kw):
    base = {
        "股票代號":"3016","股票名稱":"嘉晶","類別":"矽晶圓",
        "H65觀察層級":"W1｜重點觀察推薦","H65多因子觀察分":70.45,"H65全市場觀察百分位%":100,"H65資料覆蓋%":100,"H65風險扣分":1.73,
        "H66T1層級":"A2｜T+1次優先","H66T1自適應排序分":68.85,"H66收盤品質分":78.11,"H66法人加速度分":44.53,"H66主流點火分":75.03,"H66技術買點分":78.67,"H66量能流動性分":95.3,"H66短線動能分":78.47,"H66結構品質分":68,"H66矛盾訊號扣分":0,"H66利多不漲扣分":3,
        "H67市場Regime調整":-14,"H67族群資金調整":-2,"H67關鍵訊號一致性分":73.27,"H67追價耗竭扣分":5,"H67研究優先層級":"R0｜僅研究",
        "H68學習快照狀態":"SNAPSHOT-READY","H68版本":"v191_h68_execution_learning_authority_20260911","H68次日執行狀態":"NO-FORMAL｜僅研究觀察",
        "官方因子資料日期":"20260911","H64有效權威":"A-MINUS",
    }
    base.update(kw)
    return base


def main():
    out = apply_h70_counter_regime_session_truth(pd.DataFrame([_row()]), generated_at=date(2026,9,13))
    r=out.iloc[0]
    assert str(r["H70逆勢研究層級"]).startswith("X1"), r["H70逆勢研究層級"]
    assert float(r["H70逆勢Alpha分"]) >= 71
    assert r["H64有效權威"] == "A-MINUS"
    assert str(r["H68次日執行狀態"]).startswith("NO-FORMAL")
    # Hard sector outflow must block the exception lane.
    bad=apply_h70_counter_regime_session_truth(pd.DataFrame([_row(**{"H67族群資金調整":-12})]), generated_at=date(2026,9,13)).iloc[0]
    assert str(bad["H70逆勢研究層級"]).startswith("R0")
    assert "族群資金" in str(bad["H70逆勢阻擋原因"])
    print("PASS H70 counter-regime alpha")

if __name__ == "__main__": main()

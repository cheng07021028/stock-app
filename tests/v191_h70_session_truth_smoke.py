# -*- coding: utf-8 -*-
from datetime import date
import pandas as pd
from godpick_h70_counter_regime_session_truth import apply_h70_counter_regime_session_truth


def main():
    f=pd.DataFrame([{
        "股票代號":"3016","官方因子資料日期":"20260911",
        "H65觀察層級":"W3","H66T1層級":"B1","H67研究優先層級":"R0",
        "H68學習快照狀態":"SNAPSHOT-READY","H68版本":"v191_h68_execution_learning_authority_20260911",
    }])
    r=apply_h70_counter_regime_session_truth(f, generated_at=date(2026,9,13)).iloc[0]
    assert r["H70市場資料錨定日"] == "2026-09-11"
    assert r["H70報告產生日"] == "2026-09-13"
    assert str(r["H70快照時序狀態"]).startswith("NON-TRADING-GENERATION")
    assert r["H70預期T1交易日"] == "2026-09-14"
    print("PASS H70 session truth")

if __name__ == "__main__": main()

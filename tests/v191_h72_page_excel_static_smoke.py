# -*- coding: utf-8 -*-
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
PAGE=ROOT/"pages"/"7_股神推薦.py"
T1=ROOT/"godpick_t1_trade_truth.py"
ENG=ROOT/"godpick_h72_multi_model_alpha_ensemble.py"

def main():
    p=PAGE.read_text(encoding="utf-8")
    t=T1.read_text(encoding="utf-8")
    e=ENG.read_text(encoding="utf-8")
    must_page=[
        "H72_ENSEMBLE_EXPECTED_VERSION", "apply_h72_multi_model_alpha_ensemble", "build_h72_ensemble_table",
        "H72多模型Alpha", "H72治理摘要", "H72_E1多模型共振檔數", "H72學習快照成熟樣本",
        "V191-H72-MULTI-MODEL-ALPHA-ENSEMBLE-EXCEL", "_H72研究優先", "H72多模型Alpha",
    ]
    for x in must_page: assert x in p, x
    for x in ["build_h72_learning_summary","H72研究層級","H72全市場順位","H72風險調整分","H72學習快照狀態"]:
        assert x in t, x
    assert "H72 is a research-ranking ensemble" in e
    assert "MUST NOT create Formal authority" in e
    assert "H64/H63 remain Formal truth" in e and "H68 remains next-session execution truth" in e
    # Export and immutable record paths must apply H72 after prior stages, not only render it.
    assert p.count("apply_h72_multi_model_alpha_ensemble") >= 4
    assert '"_H64有效Formal優先", "_H73研究優先", "H73研究排序分"' in p
    assert p.index('"_H73研究優先"') < p.index('"_H72研究優先"')
    print("PASS H72 Page07/Excel/static authority")

if __name__ == "__main__":
    main()

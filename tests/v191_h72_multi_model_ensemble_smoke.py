# -*- coding: utf-8 -*-
import pandas as pd
from godpick_h72_multi_model_alpha_ensemble import VERSION, WEIGHTS, MODEL_KEYS, apply_h72_multi_model_alpha_ensemble
from godpick_h70_counter_regime_session_truth import apply_h70_counter_regime_session_truth


def _row(code, strength=90, defensive=True):
    weak = 25 if strength >= 70 else 35
    return {
        "股票代號": code, "股票名稱": f"T{code}", "市場別":"上市", "族群名稱":"測試",
        "H65營收分": strength, "H65獲利EPS分": strength-2, "營收成長官方分數": strength-3,
        "月營收YoY%": 80 if strength >= 70 else -20, "累計營收YoY%": 60 if strength >= 70 else -10,
        "估算EPS": 12 if strength >= 70 else 1,
        "H66結構品質分": strength-1, "官方基本面成長分數": strength-4, "20日波動率%": 12 if strength >= 70 else 40,
        "H65風險扣分": 2 if strength >= 70 else 14,
        "H65法人分": strength, "H66法人加速度分": strength-2, "法人籌碼官方分數": strength-3,
        "外資近1日買賣超": 5000 if strength >= 70 else -4000, "外資近5日買賣超": 18000 if strength >= 70 else -12000,
        "投信近5日買賣超": 6000 if strength >= 70 else -3000, "三大法人近5日合計": 22000 if strength >= 70 else -15000,
        "H65大戶分": strength-4, "H60大戶鎖碼真相分": strength-5, "TDCC千張大戶持股比%": 72 if strength >= 70 else 20,
        "TDCC千張大戶週變化pp": 1.2 if strength >= 70 else -1.2,
        "H65估值分": 70 if strength >= 70 else 25, "PER本益比": 16 if strength >= 70 else 110,
        "PBR股價淨值比": 2.1 if strength >= 70 else 8.0, "股利殖利率%": 3.5 if strength >= 70 else 0.1,
        "H65趨勢分": strength, "H47個股相對強度分": strength-2, "H66短線動能分": strength-3,
        "近20日漲幅%": 18 if strength >= 70 else -22, "近60日漲幅%": 32 if strength >= 70 else -35,
        "H65技術分": strength, "H66收盤品質分": strength-1, "H66技術買點分": strength-2,
        "H66量能流動性分": strength, "H66主流點火分": strength-3, "當日量比": 1.8 if strength >= 70 else 0.55,
        "當日收盤位置%": 88 if strength >= 70 else 12,
        "H66矛盾訊號扣分": 0 if strength >= 70 else 20, "H66利多不漲扣分": 0 if strength >= 70 else 10,
        "H67追價耗竭扣分": 0 if strength >= 70 else 10, "H67關鍵訊號一致性分": strength,
        "H67市場Regime調整": -12 if defensive else 5, "H67族群資金調整": 2 if strength >= 70 else -8,
        "H70逆勢研究層級": "X1｜逆勢Alpha重點觀察" if strength >= 70 and defensive else "R0｜無逆勢例外",
        "H68官方資料風險": "OK",
        "H65觀察層級":"W1｜重點觀察", "H66T1層級":"A1｜T+1優先觀察", "H67研究優先層級":"C1｜研究優先",
        "H68學習快照狀態":"SNAPSHOT-READY", "H70學習快照狀態":"SNAPSHOT-READY",
        "H64正式推薦狀態":"A-MINUS｜非正式推薦", "H68次日執行狀態":"NO-FORMAL",
    }


def main():
    assert VERSION == "v191_h72_multi_model_alpha_ensemble_20260915"
    for name, weights in WEIGHTS.items():
        assert set(weights) == set(MODEL_KEYS), name
        assert sum(weights.values()) == 100, (name, weights)
    src = pd.DataFrame([_row("0001", 94, True), _row("0002", 82, True), _row("0003", 45, True), _row("0004", 35, True)])
    # Compare H72 against the immediately preceding H70 authority state. H72 may invoke H70 when absent,
    # but must never itself alter Formal or H68 execution truth.
    baseline = apply_h70_counter_regime_session_truth(src)
    before_formal = baseline["H64正式推薦狀態"].tolist()
    before_exec = baseline["H68次日執行狀態"].tolist()
    out = apply_h72_multi_model_alpha_ensemble(baseline)
    assert len(out) == 4
    assert out["H64正式推薦狀態"].tolist() == before_formal
    assert out["H68次日執行狀態"].tolist() == before_exec
    assert out["H72全市場順位"].notna().all()
    assert out["H72全市場順位"].nunique() == 4
    assert out["H72風險調整分"].between(0,100).all()
    assert out["H72模型覆蓋%"].between(0,100).all()
    assert out["H72市場模式"].isin(["ATTACK","NEUTRAL","DEFENSIVE"]).all()
    assert str(out.loc[0,"H72研究層級"]).startswith(("E1","E2","E3")), out[["股票代號","H72研究層級","H72風險調整分"]].to_dict("records")
    assert str(out.loc[3,"H72研究層級"]).startswith("R0")
    assert (out["H72權威邊界"].str.contains("不得建立Formal")).all()
    assert (out["H72學習快照狀態"] == "SNAPSHOT-READY").all()
    print("PASS H72 multi-model ensemble")

if __name__ == "__main__":
    main()

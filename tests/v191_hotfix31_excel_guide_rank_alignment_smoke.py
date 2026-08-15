# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_super_ai_excel_guide import build_super_ai_excel_guide, VERSION


def _frame():
    return pd.DataFrame([
        {
            "股神推薦總排名": 1, "V188股神作戰優先分": 69.0, "股票代號": "6488", "股票名稱": "環球晶",
            "正式推薦分區": "高風險雷達觀察", "操作許可": "禁止新倉｜只觀察", "V188交易許可": "禁止新倉｜只觀察",
            "Entry進場買點分": 80, "Risk風控安全分": 80, "SuperAI執行風報比": 3.0, "進場可執行分": 80,
            "距最近可執行買點%": 1, "追價風險分": 10, "SuperAI Trade分": 90, "V188股神作戰優先分": 90,
            "SuperAI校準後隔日上漲機率%": 70, "族群攻擊分": 90, "主流資金分": 90,
        },
        {
            "股神推薦總排名": 2, "V188股神作戰優先分": 68.5, "股票代號": "5483", "股票名稱": "中美晶",
            "正式推薦分區": "盤中雷達追蹤", "操作許可": "WAIT-MARKET｜大盤未對齊，Trade最高B+", "V188交易許可": "WAIT-MARKET｜大盤未對齊，Trade最高B+",
            "Entry進場買點分": 66, "Risk風控安全分": 64, "SuperAI執行風報比": 1.71, "進場可執行分": 48,
            "距最近可執行買點%": 4.5, "追價風險分": 61, "SuperAI Trade分": 68, "SuperAI校準後隔日上漲機率%": 49,
            "族群攻擊分": 88, "主流資金分": 84,
        },
        {
            "股神推薦總排名": 3, "V188股神作戰優先分": 68.4, "股票代號": "8383", "股票名稱": "千附",
            "正式推薦分區": "A-準主推薦小量試單", "操作許可": "小量試單", "V188交易許可": "小量試單",
            "Entry進場買點分": 74, "Risk風控安全分": 75, "SuperAI執行風報比": 1.46, "進場可執行分": 55,
            "距最近可執行買點%": 6.5, "追價風險分": 29, "SuperAI Trade分": 68, "SuperAI校準後隔日上漲機率%": 55,
            "族群攻擊分": 62, "主流資金分": 52,
        },
        {
            "股神推薦總排名": 4, "V188股神作戰優先分": 68.1, "股票代號": "3576", "股票名稱": "聯合再生",
            "正式推薦分區": "不可直接買觀察", "操作許可": "WAIT-MARKET｜大盤未對齊", "V188交易許可": "WAIT-MARKET｜大盤未對齊",
            "Entry進場買點分": 70, "Risk風控安全分": 65, "SuperAI執行風報比": 1.3, "進場可執行分": 50,
            "距最近可執行買點%": 2, "追價風險分": 20, "SuperAI Trade分": 68, "SuperAI校準後隔日上漲機率%": 54,
            "族群攻擊分": 50, "主流資金分": 50,
        },
    ])


def test_prohibited_never_becomes_condition_candidate():
    out = build_super_ai_excel_guide(_frame(), max_rows=20)
    assert "6488" not in set(out["股票代號"].astype(str)), out
    assert not (out["超級AI定位"] == "禁止/排除").any(), out


def test_a_minus_stays_first_and_original_rank_is_preserved():
    out = build_super_ai_excel_guide(_frame(), max_rows=20)
    assert out.iloc[0]["股票代號"] == "8383", out
    assert out.iloc[0]["超級AI定位"] == "A-準主推薦", out
    assert int(out.iloc[0]["原股神總排名"]) == 3, out
    # The WAIT-MARKET radar remains a waiting radar; guide cannot call it tradable.
    row = out.loc[out["股票代號"].astype(str) == "5483"].iloc[0]
    assert row["超級AI定位"] == "等待確認/雷達", row
    assert float(row["SuperAI Trade"]) == 68.0, row


def test_page07_source_prefers_master_rank():
    src = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8-sig")
    block_start = src.index("# V191-H31：精選攻略")
    block = src[block_start:block_start + 700]
    assert "master_rank_df" in block
    assert block.index("master_rank_df") < block.index("candidate_diagnosis_export")


def main():
    test_prohibited_never_becomes_condition_candidate()
    test_a_minus_stays_first_and_original_rank_is_preserved()
    test_page07_source_prefers_master_rank()
    print("PASS V191-H31 Excel guide/rank alignment", VERSION)


if __name__ == "__main__":
    main()

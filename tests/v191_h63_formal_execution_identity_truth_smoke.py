# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd

from godpick_h63_execution_truth_engine import (
    VERSION, apply_h63_execution_truth,
    build_h63_formal_execution_table, build_h63_authority_audit_table,
)

ROOT = Path(__file__).resolve().parents[1]


def row(code, name, eff, daily, bucket, auth=None):
    return {
        "股票代號": code, "股票名稱": name, "類別": "測試",
        "H62有效權威": eff, "H62原始權威": auth or ("FORMAL" if "FORMAL" in eff else "A-MINUS"),
        "H64有效權威": eff, "H64版本": "v191_h64_strong_mainstream_holder_core_truth_20260908",
        "H56上游權威層級": auth or ("FORMAL" if "FORMAL" in eff else "A-MINUS"),
        "正式推薦分區": bucket,
        "是否正式推薦": "是" if bucket == "正式下週主推薦" else "否",
        "H34每日精選": "是" if daily else "否", "H34每日精選排名": 1 if daily else 999,
        "H34安全精選分": 72.0, "H62增量機會分": 76.0,
        "H56盤前狀態": "A0｜PREOPEN-PENDING",
        "操作許可": "允許｜盤中觸發、守價與大盤同步後分批" if bucket == "正式下週主推薦" else "觸發且守價後小量試單",
        "正式推薦動作": "只依觸發/守價分批操作",
        "主要進場路徑": "觸發守價回測", "主要進場參考價": 2465.0,
        "回測承接參考價": 2403.0, "實戰觸發價": 2465.0, "觸發後守價": 2440.0,
        "實戰停損參考": 2380.0, "路徑風險報酬比": 2.0,
    }


def main():
    df = pd.DataFrame([
        row("2330", "台積電", "EFFECTIVE-FORMAL", True, "正式下週主推薦", "FORMAL"),
        row("2222", "健康Formal未日選", "EFFECTIVE-FORMAL", False, "正式下週主推薦", "FORMAL"),
        row("1111", "FormalHold", "FORMAL-HOLD", True, "正式下週主推薦", "FORMAL"),
        row("2363", "Aminus", "A-MINUS", True, "A-｜準主推薦小量試單", "A-MINUS"),
        row("3019", "Radar", "RADAR", True, "盤中雷達追蹤", "RADAR"),
    ])
    tagged = apply_h63_execution_truth(df)
    assert tagged["H63版本"].eq(VERSION).all()
    by = tagged.set_index("股票代號")
    assert str(by.at["2330", "H63角色"]).startswith("F1")
    assert str(by.at["2330", "H63是否正式推薦"]).startswith("是")
    assert str(by.at["2222", "H63角色"]).startswith("F0")
    assert str(by.at["1111", "H63角色"]).startswith("FH")
    assert str(by.at["1111", "H63是否正式推薦"]).startswith("否")
    assert str(by.at["2363", "H63角色"]).startswith("A-")
    assert str(by.at["3019", "H63角色"]).startswith("R")

    formal = build_h63_formal_execution_table(tagged, max_rows=10)
    codes = set(formal.get("股票代號", pd.Series([], dtype=str)).astype(str))
    assert codes == {"2330", "2222"}, formal.to_dict("records")
    f = formal.set_index("股票代號")
    assert str(f.at["2330", "H63正式推薦"]).startswith("F1")
    assert str(f.at["2330", "H63是否可直接買"]).startswith("否")
    assert str(f.at["2222", "H63正式推薦"]).startswith("F0")

    audit = build_h63_authority_audit_table(tagged, max_rows=20)
    assert set(audit["股票代號"].astype(str)) == {"2330", "2222", "1111", "2363", "3019"}
    assert audit.set_index("股票代號").at["2363", "H63是否正式推薦"].startswith("否")

    page = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    assert 'PAGE07_SPEED_FIX_VERSION = "page07_v191_h64_strong_mainstream_holder_core_truth_20260908"' in page
    assert 'EXCEL_COLUMN_LAYOUT_VERSION = "V191-H64-STRONG-MAINSTREAM-HOLDER-CORE-TRUTH-20260908"' in page
    assert "本輪真正正式推薦｜H63 唯一作戰清單" in page
    assert "A-/Radar 每日條件候選｜非正式推薦" in page
    assert "權威底層稽核｜Formal／A-／Radar（非第二份推薦清單）" in page
    assert '("正式推薦作戰", formal_execution_df' in page
    # One common H63 builder for UI and Excel, so the web and workbook cannot drift.
    assert page.count("build_h63_formal_execution_table(") >= 2
    print("PASS H63 formal execution identity + Excel truth smoke")


if __name__ == "__main__":
    main()

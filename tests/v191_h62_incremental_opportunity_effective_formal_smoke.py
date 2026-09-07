# -*- coding: utf-8 -*-
import pandas as pd

from godpick_h62_incremental_opportunity_engine import VERSION, apply_h62_incremental_opportunity_engine
from godpick_human_master_engine import build_h62_single_decision_truth_table


def _row(code, name, auth, alpha, n, h61opp, head, rrq, pct, h57, fresh_cash, tier="W1｜一般研究", formal_bucket="盤中雷達追蹤"):
    return {
        "股票代號": code, "股票名稱": name, "類別": "測試族群",
        "H51版本": "v191_h60_mainrise_holder_snowball_truth_20260904",
        "H60版本": "v191_h60_mainrise_holder_snowball_truth_20260904",
        "H60主升階段": "MR1｜主升起漲", "H60三因子層級": "T2｜雙因子以上共振",
        "H60大戶鎖碼層級": "LK2｜鎖碼增強", "H60雪球股層級": "SB2｜雪球成長候選",
        "H56上游權威層級": auth,
        "H56最終參考層級": "A0｜PREOPEN-PENDING" if auth == "FORMAL" else "P0｜AUTHORITY-CAPPED",
        "H56T1確認分": 68.0,
        "H51交易許可": "SETUP-PREP｜test",
        "H51市場地位": "HM-EARLY｜test",
        "H51專業參考分": 78.0,
        "H51發動潛力分": 80.0,
        "H51路徑RR": 2.0,
        "H59唯一決策": "A0｜Formal盤前待確認" if auth == "FORMAL" else "P0｜權威限制等待",
        "H59是否可買": "否",
        "H60唯一決策": "A0｜Formal盤前待確認" if auth == "FORMAL" else "P0｜權威限制等待",
        "H60是否可買": "否",
        "現在該做什麼": "test",
        "H61近期成熟樣本": n,
        "H61近期SelectionAlpha%": alpha,
        "H61近期正Alpha率%": 0.0 if alpha < 0 else 100.0,
        "H61機會價值分": h61opp,
        "H61上漲空間分": head,
        "H61RR品質分": rrq,
        "H61近期Alpha分": 40.0 if alpha < 0 else 75.0,
        "H61新鮮機會分": 80.0,
        "H61重複慣性扣分": 15.0 if alpha < 0 else 0.0,
        "H61機會成本扣分": 12.0 if alpha < 0 else 0.0,
        "H61機會層級": tier,
        "H61前排資格": "是｜Formal保留" if auth == "FORMAL" else "是",
        "H32_10日預估報酬%": 2.8 if alpha < 0 else 7.0,
        "H57全市場前兆百分位%": pct,
        "H57飆股發動前兆分": h57,
        "H57主流形成前兆分": 84.0,
        "H57資金加速度分": fresh_cash,
        "H57相對強度轉折分": 85.0,
        "H57提前視窗分": 82.0,
        "H57前兆階段": "PI3｜PRE-IGNITION",
        "H60主升段分": 85.0,
        "H60三因子共振分": 80.0,
        "H53族群共振分": 82.0,
        "H55雙路徑隔日分": 80.0,
        "近5次入榜次數": 4 if alpha < 0 else 0,
        "連續入榜次數": 2 if alpha < 0 else 0,
        "正式推薦分區": formal_bucket,
        "是否正式推薦": "是" if auth == "FORMAL" else "否",
        "最新價": 100.0, "實戰觸發價": 103.0, "觸發後守價": 102.0, "實戰停損參考": 97.0,
    }


def main():
    df = pd.DataFrame([
        _row("1111", "熟面孔Formal", "FORMAL", -1.2, 3, 45, 61, 75, 99, 88, 85, tier="R0｜重複低效觀察", formal_bucket="正式下週主推薦"),
        _row("2222", "健康Formal", "FORMAL", +1.4, 3, 78, 84, 86, 95, 86, 82, tier="O1｜高潛力新機會", formal_bucket="正式下週主推薦"),
        _row("3333", "新領漲", "RADAR", 0.0, 0, 82, 88, 82, 99.6, 94, 93, tier="O1｜高潛力新機會"),
        _row("4444", "低增量研究", "RADAR", -0.8, 3, 42, 48, 35, 50, 50, 45, tier="R0｜重複低效觀察"),
    ])
    out = apply_h62_incremental_opportunity_engine(df)
    assert out["H62版本"].eq(VERSION).all()
    by = out.set_index("股票代號")
    assert by.at["1111", "H62有效權威"] == "FORMAL-HOLD", by.loc["1111"].to_dict()
    assert str(by.at["1111", "H62前排資格"]).startswith("否")
    assert by.at["2222", "H62有效權威"] == "EFFECTIVE-FORMAL"
    assert str(by.at["2222", "H62前排資格"]).startswith("是")
    assert str(by.at["3333", "H62機會層級"]).startswith(("N1", "N2")), by.loc["3333"].to_dict()
    assert str(by.at["3333", "H62正式作戰資格"]).startswith("否")
    assert str(by.at["4444", "H62前排資格"]).startswith("否")

    console = build_h62_single_decision_truth_table(out, max_rows=10)
    codes = set(console.get("股票代號", pd.Series([], dtype=str)).astype(str))
    assert "1111" not in codes, console.to_dict("records")
    assert "2222" in codes and "3333" in codes, console.to_dict("records")
    c = console.set_index("股票代號")
    assert str(c.at["3333", "H62是否可買"]).startswith("否")
    print("PASS H62 effective-formal + incremental opportunity smoke")


if __name__ == "__main__":
    main()

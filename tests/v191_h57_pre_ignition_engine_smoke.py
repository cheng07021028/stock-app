# -*- coding: utf-8 -*-
from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_pre_ignition_engine import VERSION as H57_VERSION, apply_pre_ignition_engine
from godpick_human_master_engine import VERSION, apply_human_master_engine, build_h51_mainstream_leader_table, build_h51_sector_table


def base_row(code: str, name: str, sector: str) -> dict:
    return {
        "股票代號": code, "股票名稱": name, "類別": sector,
        "H50族群生命週期": "B2｜一般輪動", "H50波段機會階段": "N-RADAR｜觀察",
        "H47主流領先狀態": "L-RADAR｜觀察", "H50族群可買主流分": 58,
        "H45族群主流分": 58, "H50族群新鮮度分": 68, "H50族群回檔再攻分": 60,
        "H47個股相對強度分": 72, "H47族群內領先百分位%": 68, "H47起漲優先分": 64,
        "H45趨勢延續分": 60, "今日漲幅%": 1.8, "近5日漲幅%": 4.2, "近20日漲幅%": 10.0,
        "距20日高點%": 2.2, "突破20日高點%": 0.0, "當日量比": 1.35,
        "當日收盤位置%": 84, "3日平均收盤位置%": 79, "上影線比例%": 10,
        "成交額百萬": 900, "20日均成交額百萬": 520,
        "成交額3日加速度%": 62, "成交額5日加速度%": 44,
        "成交量3日加速度%": 48, "成交量5日加速度%": 32,
        "波動壓縮比": 0.62, "前5日波動壓縮比": 0.54, "當日區間擴張倍數": 1.24,
        "3日動能加速度百分點": 3.2, "收盤距MA20%": 3.0,
        "主流資金分": 67, "法人連買代理分數": 65, "法人籌碼分數": 64,
        "EPS代理分數": 68, "營收動能代理分數": 70, "獲利代理分數": 68,
        "族群攻擊強度": 70, "族群廣度分": 66, "族群成交額分": 68, "族群資金流分數": 69,
        "同族群強勢比例": 0.58, "同族群平均量能分": 70, "H45族群5日上漲比例%": 62,
        "今日訊號新鮮分": 86, "H50重複推薦扣分": 0, "追價風險分": 32,
        "路徑風險報酬比": 1.45, "SuperAI Trade分": 60, "Risk風控安全分": 70,
        "Entry進場買點分": 60, "實戰停損距離%": 5.0, "最新價": 100,
        "隔日可執行優先分": 63, "守價回測距離%": 1.4, "隔日耗竭風險分": 28,
        "隔夜風控分數": 50, "隔夜風險等級": "中性", "隔日大盤分數": 50,
        "隔日下跌機率%": 50, "隔日大盤預測加減分": 0,
        "K線日期": "2026-09-01", "隔日大盤預測日期": "2026-09-02",
        "K線資料新鮮度": "最新交易日",
        # Pre-existing H55 evidence is helpful but not enough alone.
        "強勢前兆分": 72, "起漲前兆分數": 73, "隔日爆發分": 68,
        "局部題材火種分": 66, "主流領漲回補分": 66,
    }


def main():
    assert VERSION == "v191_h57_pre_ignition_acceleration_engine_20260902"
    assert H57_VERSION == "v191_h57_pre_ignition_acceleration_engine_20260902"

    # Three same-sector stocks quietly accelerating together: the engine should
    # recognize sector formation before the mature-mainstream layer is required.
    rows = [base_row("7001", "前兆A", "新題材"), base_row("7002", "前兆B", "新題材"), base_row("7003", "前兆C", "新題材")]
    rows[1].update({"成交額3日加速度%": 52, "成交量3日加速度%": 42, "今日漲幅%": 1.2, "3日動能加速度百分點": 2.6})
    rows[2].update({"成交額3日加速度%": 75, "成交量3日加速度%": 55, "今日漲幅%": 2.4, "3日動能加速度百分點": 3.8})

    # An already-extended stock must not win merely because recent acceleration is huge.
    ext = base_row("7999", "過熱股", "舊熱門")
    ext.update({
        "今日漲幅%": 8.5, "近5日漲幅%": 24, "近20日漲幅%": 42,
        "突破20日高點%": 8.0, "追價風險分": 88, "隔日耗竭風險分": 86,
        "成交額3日加速度%": 160, "成交量3日加速度%": 150, "當日量比": 2.2,
    })

    frame = pd.DataFrame(rows + [ext])
    direct = apply_pre_ignition_engine(frame)
    r = direct.loc[direct["股票代號"].eq("7001")].iloc[0]
    x = direct.loc[direct["股票代號"].eq("7999")].iloc[0]
    assert r["H57資金加速度分"] >= 75, r[["H57資金加速度分", "H57飆股發動前兆分"]].to_dict()
    assert r["H57族群點火廣度分"] > 50, r["H57族群點火廣度分"]
    assert r["H57飆股發動前兆分"] >= 70, r["H57飆股發動前兆分"]
    assert "H57全市場前兆百分位%" in direct.columns
    assert "H57精選雷達層級" in direct.columns
    assert str(r["H57前兆階段"]).startswith(("PI2", "PI3")), r["H57前兆階段"]
    assert str(x["H57前兆階段"]).startswith("EX1"), x["H57前兆階段"]
    assert x["H57飆股發動前兆分"] < r["H57飆股發動前兆分"]

    # Full human-master integration: H57 may elevate research visibility but must
    # not manufacture Formal/V188/H51 buy authority.
    governed = apply_human_master_engine(frame)
    gr = governed.loc[governed["股票代號"].eq("7001")].iloc[0]
    assert "H57飆股發動前兆分" in governed.columns
    assert not str(gr["H51交易許可"]).startswith("BUY-READY"), gr["H51交易許可"]

    leaders = build_h51_mainstream_leader_table(governed, max_rows=10)
    assert "7001" in leaders["股票代號"].astype(str).tolist(), leaders.to_dict("records")
    assert "H57飆股發動前兆分" in leaders.columns
    assert "H57全市場前兆百分位%" in leaders.columns
    sectors = build_h51_sector_table(governed, max_rows=10)
    assert "H57族群前兆機會分" in sectors.columns
    assert sectors.iloc[0]["H57族群前兆機會分"] >= 50

    page = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    assert 'H51_HUMAN_MASTER_EXPECTED_VERSION = "v191_h57_pre_ignition_acceleration_engine_20260902"' in page
    assert 'PAGE07_SPEED_FIX_VERSION = "page07_v191_h57_pre_ignition_acceleration_engine_20260902"' in page
    for col in ["成交額3日加速度%", "成交量3日加速度%", "波動壓縮比", "前5日波動壓縮比", "當日區間擴張倍數", "3日動能加速度百分點"]:
        assert col in page
    assert "H57把『成交額/量能加速度" in page

    print("PASS v191_h57_pre_ignition_engine_smoke")


if __name__ == "__main__":
    main()

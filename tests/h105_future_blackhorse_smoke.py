# -*- coding: utf-8 -*-
"""H105 offline regression tests: future blackhorse != today's strongest stock."""
from __future__ import annotations

import pandas as pd
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_h105_future_blackhorse_engine import (
    VERSION,
    analyze_candidate,
    decorate_decision_tables,
    export_contract_summary,
)
from godpick_h80_record_feedback import build_research_tracking_frame


def _future_setup() -> dict:
    return {
        "股票代號": "9991", "股票名稱": "未來黑馬", "類別": "測試族群", "市場別": "上市",
        "今日漲幅%": 0.8, "近5日漲幅%": 2.3, "近20日漲幅%": 4.0, "均量比": 1.25,
        "外資近1日買賣超": 1800, "外資近3日買賣超": 2400, "外資近5日買賣超": -500,
        "投信近1日買賣超": 300, "投信近3日買賣超": 450, "投信近5日買賣超": 80,
        "三大法人近1日合計": 2200, "三大法人近3日合計": 3200, "三大法人近5日合計": 600,
        "法人連買天數_官方": 2, "法人籌碼官方分數": 72,
        "大戶鎖碼分數": 72, "大戶承接分": 68,
        "起漲前兆分數": 78, "型態突破分數": 66, "技術結構分數": 74,
        "20日壓力距離%": 2.0, "Entry進場買點分": 67, "Risk風控安全分": 70,
        "交易可行分數": 72, "風險報酬比_決策": 1.7,
        "族群熱度排名": 6, "類股加速度": 74, "族群資金流分數": 70,
        "族群輪動分": 73, "同族群強勢比例": 58,
        "營收成長分數": 68, "EPS成長分數": 62,
        "追價風險分數_決策": 35, "近期強勢狀態": "未見近期強勢",
        "H79決策層級": "研究推薦",
    }


def _today_hot() -> dict:
    row = _future_setup()
    row.update({
        "股票代號": "9992", "股票名稱": "今日已噴",
        "今日漲幅%": 8.2, "近5日漲幅%": 19.0, "近20日漲幅%": 36.0,
        "均量比": 2.8, "追價風險分數_決策": 82, "近期強勢狀態": "強勢主升",
        "型態突破分數": 88,
    })
    return row


def test_future_setup_beats_today_hot() -> None:
    future = analyze_candidate(_future_setup(), pool="research")
    hot = analyze_candidate(_today_hot(), pool="research")
    assert future["H105今日強勢排除"] == "否", future
    assert hot["H105今日強勢排除"] == "是", hot
    assert future["H105黑馬預發動分"] > hot["H105黑馬預發動分"], (future, hot)
    assert str(future["H105Formal權限"]).startswith("LOCKED")


def test_interval_return_is_not_daily_return() -> None:
    row = _future_setup()
    row.pop("今日漲幅%", None)
    row["區間漲跌幅%"] = 188.0
    result = analyze_candidate(row, pool="research")
    assert result["H105今日已發動程度"] < 72, result
    assert result["H105今日強勢排除"] == "否", result


def test_blackhorse_overview_and_contract() -> None:
    df = pd.DataFrame([_future_setup(), _today_hot()])
    tables = {
        "research": df.copy(), "waiting": pd.DataFrame(), "actionable": pd.DataFrame(),
        "audit": df.copy(), "health": pd.DataFrame(),
    }
    out = decorate_decision_tables(tables, candidate_df=df, sector_df=pd.DataFrame())
    black = out["blackhorse_overview"]
    assert not black.empty
    assert "9991" in set(black["股票代號"].astype(str))
    assert "9992" not in set(black["股票代號"].astype(str))
    contract = export_contract_summary(out)
    assert contract["ok"] is True, contract


def test_research_record_persists_h105_and_never_grants_buy_authority() -> None:
    row = _future_setup()
    row.update(analyze_candidate(row, pool="research"))
    df = pd.DataFrame([row])
    tracked = build_research_tracking_frame(df, df, excluded_codes=[])
    assert len(tracked) == 1
    rec = tracked.iloc[0]
    assert float(rec["H105黑馬預發動分"]) >= 60
    assert rec["推薦模式"] == "H105未來黑馬Research"
    assert rec["是否可直接買進"] == "否"
    assert str(rec["H105Formal權限"]).startswith("LOCKED")



def test_h105_does_not_reorder_existing_execution_pools() -> None:
    a = _future_setup(); a["股票代號"] = "9993"; a["股票名稱"] = "原順位A"; a["起漲前兆分數"] = 55
    b = _future_setup(); b["股票代號"] = "9994"; b["股票名稱"] = "原順位B"; b["起漲前兆分數"] = 90
    df = pd.DataFrame([a, b])
    out = decorate_decision_tables({"research": df.copy()}, candidate_df=df, sector_df=pd.DataFrame())
    assert out["research"]["股票代號"].astype(str).tolist() == ["9993", "9994"]
    assert out["blackhorse_overview"].iloc[0]["H105黑馬預發動分"] >= out["blackhorse_overview"].iloc[-1]["H105黑馬預發動分"]


def test_broker_branch_is_optional_and_never_faked() -> None:
    base = analyze_candidate(_future_setup(), pool="research")
    assert base["H105券商分點分"] is None
    assert "券商分點" in str(base["H105資料缺口"])
    row = _future_setup()
    row.update({"券商分點分數": 76, "Top5分點淨買比%": 16, "券商分點連買天數": 3, "分點集中度%": 68})
    with_broker = analyze_candidate(row, pool="research")
    assert float(with_broker["H105券商分點分"]) > 50
    assert "券商分點" not in str(with_broker["H105資料缺口"])

def run_all() -> None:
    test_future_setup_beats_today_hot()
    test_interval_return_is_not_daily_return()
    test_blackhorse_overview_and_contract()
    test_research_record_persists_h105_and_never_grants_buy_authority()
    test_h105_does_not_reorder_existing_execution_pools()
    test_broker_branch_is_optional_and_never_faked()
    print(f"H105_SMOKE_PASS | {VERSION}")


if __name__ == "__main__":
    run_all()

# -*- coding: utf-8 -*-
"""Phase107 cross-market leader and tactical-risk regression tests."""
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_learning_system import (  # noqa: E402
    LEARNING_SYSTEM_VERSION,
    apply_daily_learning_overlay,
    apply_learning_admission,
)

EMPTY_PROFILE = {"route_stats": {}, "regime_stats": {}, "eligible_samples": 0, "global_metrics": {}, "error_taxonomy": {}}


def leader_row(code: str = "6805") -> dict:
    return {
        "股票代號": code, "股票名稱": "測試主升股", "市場別": "上市", "類別": "電子零組件",
        "最新價": 1615, "實戰觸發價": 1705, "觸發後守價": 1675, "停損參考": 1205, "第一壓力價": 1725,
        "技術結構分數": 70, "起漲前兆分數": 68, "強勢動能分": 72, "爆發雷達分": 70,
        "Entry進場買點分": 64, "Risk風控安全分": 60, "主流主升優先分": 72, "主流資金分": 72,
        "族群攻擊強度": 70, "類股熱度分數": 68, "營收動能代理分數": 78, "EPS代理分數": 70,
        "官方因子總分": 65, "官方資料完整度": 60, "因子來源可信度": 82,
        "強勢股漏選風險分": 90, "主流領漲回補分": 78, "隔日爆發分": 72,
        "成交額百萬": 2100, "當日量比": 1.2, "當日收盤位置%": 72, "上影線比例%": 8,
        "今日漲幅%": 1.5, "近5日漲幅%": 27, "近20日漲幅%": 18,
        "是否領先同類股": "是", "類股前3強": "是", "類股內排名": 2,
        "追價風險分": 58, "隔日耗竭風險分": 55, "K線落後交易日": 0, "K線資料新鮮度": "最新",
        "官方因子落後交易日": 1, "官方因子新鮮度": "有效｜落後1日", "大盤風險燈號": "黃燈",
        "大盤策略模式": "震盪輪動", "正式推薦分區": "正式排除清單", "正式推薦排除原因": "過熱禁買、結構停損距離過大",
        "進場阻擋原因": "結構停損距離過大", "真禁買原因": "", "硬否決原因": "", "操作許可": "禁止新倉｜等待整理",
        "今日訊號新鮮分": 78, "近5次入榜次數": 0,
    }


def run() -> None:
    # 1. Tactical guard must replace the distant structural stop for entry evaluation.
    one = apply_daily_learning_overlay(pd.DataFrame([leader_row()]), profile=EMPTY_PROFILE)
    r = one.iloc[0]
    assert float(r["AI結構停損距離%"]) > 20
    assert 1.5 <= float(r["AI戰術停損距離%"]) <= 2.0
    assert float(r["AI戰術風報比"]) >= 1.5
    assert r["AI風險口徑"] == "戰術守價"

    # 2. A strong five-day move followed by calm consolidation is a second-entry setup, not automatic blow-off.
    assert r["AI過熱型態"] == "主升整理二次買點", r["AI過熱型態"]
    assert r["AI召回保留旗標"] == "是"
    assert "跨市場新強股" in r["AI召回路徑"]

    # 3. True high-volume upper-shadow blow-off remains blocked from admission.
    blow = leader_row("8039")
    blow.update({"今日漲幅%": 9.5, "當日收盤位置%": 48, "上影線比例%": 38, "當日量比": 3.1,
                 "操作許可": "禁止新倉", "正式推薦排除原因": "過熱禁買"})
    blow_df = apply_daily_learning_overlay(pd.DataFrame([blow]), profile=EMPTY_PROFILE)
    assert blow_df.iloc[0]["AI過熱型態"] == "爆量噴出末升"
    admitted = apply_learning_admission(blow_df)
    assert admitted.iloc[0]["正式推薦分區"] not in {"正式下週主推薦", "A-｜準主推薦小量試單"}

    # 4. Low-liquidity movers cannot use cross-sectional rescue.
    illiquid = leader_row("6680")
    illiquid.update({"成交額百萬": 0.5, "流動性等級": "低流動性", "真禁買原因": "低流動性"})
    ill = apply_daily_learning_overlay(pd.DataFrame([illiquid]), profile=EMPTY_PROFILE)
    assert ill.iloc[0]["AI召回保留旗標"] == "否"
    assert apply_learning_admission(ill).iloc[0]["正式推薦分區"] not in {"正式下週主推薦", "A-｜準主推薦小量試單"}

    # 5. Fresh evidence outranks a repeated name with no new signal.
    fresh = leader_row("7777")
    sticky = leader_row("2317")
    sticky.update({"近5次入榜次數": 5, "連續入榜次數": 5, "今日訊號新鮮分": 35})
    pair = apply_daily_learning_overlay(pd.DataFrame([fresh, sticky]), profile=EMPTY_PROFILE)
    assert float(pair.iloc[0]["AI排名加減分"]) > float(pair.iloc[1]["AI排名加減分"])
    assert float(pair.iloc[1]["AI重複證據衰減分"]) > 0

    # 6. LOCKDOWN and stale K-line are never bypassed by the new leader rescue.
    locked = leader_row("9997")
    locked.update({"極端市場LOCKDOWN": "是", "大盤風險燈號": "極端風險｜全面禁買", "操作許可": "全面禁買"})
    stale = leader_row("9996")
    stale.update({"K線落後交易日": 1, "K線資料新鮮度": "落後1個交易日", "進場阻擋原因": "K線落後"})
    hard = apply_daily_learning_overlay(pd.DataFrame([locked, stale]), profile=EMPTY_PROFILE)
    assert hard.iloc[0]["AI召回保留旗標"] == "否"
    assert hard.iloc[1]["AI召回保留旗標"] == "否"
    admitted_hard = apply_learning_admission(hard)
    assert not admitted_hard["正式推薦分區"].isin(["正式下週主推薦", "A-｜準主推薦小量試單"]).any()

    print(f"PASS {LEARNING_SYSTEM_VERSION}: tactical stop, cross-market recall, blow-off, liquidity, repetition and lockdown")


if __name__ == "__main__":
    run()

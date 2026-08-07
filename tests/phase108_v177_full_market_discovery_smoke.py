# -*- coding: utf-8 -*-
"""V177 full-market discovery simulation/regression tests (offline only)."""
from __future__ import annotations

import tempfile
from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_full_market_discovery import (
    FULL_MARKET_DISCOVERY_VERSION,
    evaluate_legacy_soft_gates,
    apply_sector_bayesian_shrinkage,
)
from godpick_learning_system import apply_daily_learning_overlay
from godpick_recommendation_rotation import apply_recommendation_rotation_guard, save_rotation_snapshot

EMPTY_PROFILE = {"route_stats": {}, "regime_stats": {}, "eligible_samples": 0, "global_metrics": {}, "error_taxonomy": {}}


def strong_row(code: str, sector: str = "AI伺服器") -> dict:
    return {
        "股票代號": code, "股票名稱": f"測試{code}", "市場別": "上市", "類別": sector,
        "股神推薦優先分": 82, "正式推薦分區": "A-｜準主推薦小量試單", "是否正式推薦": "否", "操作許可": "條件式",
        "最新價": 100, "實戰觸發價": 102, "技術結構分數": 82, "起漲前兆分數": 74,
        "型態突破分數": 78, "強勢動能分": 79, "爆發雷達分": 76, "Entry進場買點分": 70,
        "Risk風控安全分": 68, "主流主升優先分": 80, "主流資金分": 82, "族群攻擊強度": 78,
        "類股熱度分數": 76, "族群資金流分數": 74, "成交額百萬": 1200, "當日量比": 1.35,
        "當日收盤位置%": 82, "今日漲幅%": 2.8, "近5日漲幅%": 8, "近20日漲幅%": 15,
        "營收動能代理分數": 76, "EPS代理分數": 70, "官方資料完整度": 60, "因子來源可信度": 75,
        "強勢股漏選風險分": 82, "主流領漲回補分": 76, "隔日爆發分": 74,
        "K線落後交易日": 0, "K線資料新鮮度": "最新交易日", "大盤風險燈號": "黃燈",
        "大盤策略模式": "震盪輪動", "真禁買原因": "", "硬否決原因": "", "進場阻擋原因": "",
        "今日訊號新鮮分": 72, "前置軟篩選數": 0,
        "實戰觸發價": 102, "觸發後守價": 100.5, "停損參考": 97.5, "第一壓力價": 108,
    }


def run() -> None:
    # 1) Old hard prefilters are now diagnostics: four failures still survive to AI mother-pool.
    gates = evaluate_legacy_soft_gates(
        signal_score=20, min_signal_score=50, risk_pass=False, risk_reason="傳統RR不足",
        prelaunch_score=30, min_prelaunch_score=55, trade_score=25, min_trade_score=50,
        opportunity_mode=False, rescue_eligible=False,
    )
    assert gates["soft_count"] == 4, gates
    assert set(gates["soft_statuses"]) == {"signal_filtered", "risk_filtered", "prelaunch_filtered", "trade_filtered"}

    # 2) A legacy-filtered but cross-sectionally strong row can be rescued for diagnosis/radar.
    legacy_filtered = strong_row("8888", "新催化產業")
    legacy_filtered.update({"前置軟篩選數": 3, "技術結構分數": 76, "起漲前兆分數": 65,
                            "強勢股漏選風險分": 92, "營收動能代理分數": 90, "EPS代理分數": 84,
                            "成交額百萬": 1800, "當日量比": 1.6, "當日收盤位置%": 88})
    peer = strong_row("7777", "成熟主流")
    peer.update({"強勢股漏選風險分": 45, "營收動能代理分數": 55, "EPS代理分數": 55})
    discovery = apply_daily_learning_overlay(pd.DataFrame([legacy_filtered, peer]), profile=EMPTY_PROFILE)
    rescued = discovery.loc[discovery["股票代號"] == "8888"].iloc[0]
    assert rescued["AI發現母體"].startswith("FULL-MARKET")
    assert float(rescued["AI舊規則軟篩選數"]) == 3
    assert float(rescued["AI跨市場新強股分"]) >= float(discovery.loc[discovery["股票代號"] == "7777", "AI跨市場新強股分"].iloc[0])

    # 3) Bayesian shrinkage: 1/1 = 100% may not remain 100%; large samples preserve signal better.
    grouped = pd.DataFrame([
        {"類別": "單一小類股", "股票數": 1, "同族群強勢比例": 100.0, "同族群推薦密度": 100.0, "同族群平均量能分": 100.0},
        {"類別": "大型真主流", "股票數": 20, "同族群強勢比例": 80.0, "同族群推薦密度": 70.0, "同族群平均量能分": 82.0},
    ])
    shrunk = apply_sector_bayesian_shrinkage(grouped, global_strong_pct=25.0, global_candidate_pct=30.0, global_volume_score=55.0)
    small = shrunk.iloc[0]; large = shrunk.iloc[1]
    assert float(small["同族群強勢比例"]) < 50.0, small.to_dict()
    assert float(small["同族群推薦密度"]) < 50.0, small.to_dict()
    assert abs(float(large["同族群強勢比例"]) - 80.0) < abs(float(small["同族群強勢比例"]) - 100.0)
    assert float(large["族群樣本可信度"]) > float(small["族群樣本可信度"])

    # 4) Rotation memory: repeated stock without new evidence is TRACK, not a new recommendation event.
    with tempfile.TemporaryDirectory(prefix="v177_rotation_") as td:
        day1 = pd.DataFrame([strong_row("2317"), strong_row("2376"), strong_row("3005")])
        day1["K線最後交易日"] = "2026-08-05"
        first = apply_recommendation_rotation_guard(day1, base_dir=td)
        ok, _ = save_rotation_snapshot(first, base_dir=td, persist_remote=False)
        assert ok

        day2 = day1.copy()
        day2["K線最後交易日"] = "2026-08-06"
        day2["今日訊號新鮮分"] = 45
        day2["股神推薦優先分"] = 81.5
        repeat = apply_recommendation_rotation_guard(day2, base_dir=td)
        r = repeat.loc[repeat["股票代號"] == "2317"].iloc[0]
        assert r["是否新增推薦事件"] == "否", r.to_dict()
        assert r["推薦事件類型"].startswith("TRACK"), r.to_dict()

        # 5) Same repeated leader with material new evidence becomes RECONFIRM.
        day3 = day2.copy()
        day3["K線最後交易日"] = "2026-08-07"
        day3["今日訊號新鮮分"] = 82
        day3["股神推薦優先分"] = 88
        day3["當日量比"] = 1.8
        day3["最新價"] = 104
        reconfirm = apply_recommendation_rotation_guard(day3, base_dir=td)
        rr = reconfirm.loc[reconfirm["股票代號"] == "2317"].iloc[0]
        assert rr["是否新增推薦事件"] == "是", rr.to_dict()
        assert rr["推薦事件類型"].startswith("RECONFIRM"), rr.to_dict()

    # 6) Sector concentration: 3rd/4th same-sector rows receive similarity penalty unless evidence is exceptional.
    many = pd.DataFrame([strong_row(str(9000+i), "AI伺服器") for i in range(4)] + [strong_row("9100", "光通訊")])
    many["K線最後交易日"] = "2026-08-07"
    many.loc[many["股票代號"].isin(["9002", "9003"]), "今日訊號新鮮分"] = 48
    diversified = apply_recommendation_rotation_guard(many)
    same_sector = diversified.loc[diversified["股票代號"].isin(["9002", "9003"])]
    assert (pd.to_numeric(same_sector["類股集中校正分"], errors="coerce") < 0).any(), diversified[["股票代號","類別","類股集中校正分"]].to_dict("records")

    print(f"PASS {FULL_MARKET_DISCOVERY_VERSION}: full-market soft gates, AI rescue, Bayesian sector shrinkage, event memory and diversity")


if __name__ == "__main__":
    run()

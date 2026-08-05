# -*- coding: utf-8 -*-
"""Phase 105 daily-learning AI smoke/regression tests.

Runs without network and writes only into a temporary directory.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_learning_system import (  # noqa: E402
    LEARNING_SYSTEM_VERSION,
    apply_daily_learning_overlay,
    apply_learning_admission,
    build_experience_profile,
    build_learning_summary,
    save_learning_run,
)


def _strong_row() -> dict:
    return {
        "股票代號": "9999", "股票名稱": "測試強股", "市場別": "上市", "類別": "測試產業",
        "技術結構分數": 90, "Alpha選股潛力分": 90, "起漲前兆分數": 88,
        "型態突破分數": 90, "強勢動能分": 88, "Entry進場買點分": 86,
        "Risk風控安全分": 84, "主流主升優先分": 88, "主流資金分": 88,
        "族群攻擊強度": 85, "類股熱度分數": 85, "族群資金流分數": 84,
        "當日量比": 1.5, "當日收盤位置%": 88, "今日漲幅%": 3.5,
        "近5日漲幅%": 8, "近20日漲幅%": 14, "距20日高點%": -1,
        "實戰停損距離%": 4.2, "實戰風險報酬比": 2.0, "成交額百萬": 1500,
        "營收成長官方分數": 88, "EPS成長分數": 84, "官方因子總分": 86,
        "PER本益比": 18, "官方估值風險分數": 85, "法人籌碼官方分數": 86,
        "外資近1日買賣超": 1000, "投信近1日買賣超": 500, "三大法人近5日合計": 5000,
        "官方資料完整度": 90, "因子來源可信度": 100, "K線落後交易日": 0,
        "K線資料新鮮度": "最新", "官方因子落後交易日": 0, "官方因子新鮮度": "最新/對齊",
        "大盤風險燈號": "綠燈", "大盤策略模式": "多頭攻擊", "追價風險分": 35,
        "隔日耗竭風險分": 35, "正式推薦分區": "盤中雷達追蹤", "正式推薦排除原因": "",
        "進場阻擋原因": "", "真禁買原因": "", "硬否決原因": "", "操作許可": "待評估",
        "今日訊號新鮮分": 80, "近5次入榜次數": 0,
    }


def _empty_profile() -> dict:
    return {"route_stats": {}, "regime_stats": {}, "eligible_samples": 0, "global_metrics": {}, "error_taxonomy": {}}


def run() -> None:
    # 1. Multi-evidence strong stock can become formal; no quota stuffing is needed.
    strong = apply_daily_learning_overlay(pd.DataFrame([_strong_row()]), profile=_empty_profile())
    assert strong.iloc[0]["AI推薦資格"].startswith("AI-A｜"), strong.iloc[0]["AI推薦資格"]
    admitted = apply_learning_admission(strong)
    assert admitted.iloc[0]["正式推薦分區"] == "正式下週主推薦"

    # 2. A high-quality company with poor timing must wait, not be blindly promoted.
    wait_row = _strong_row()
    wait_row.update({"股票代號": "9998", "股票名稱": "品質佳買點差", "Entry進場買點分": 32,
                     "型態突破分數": 35, "強勢動能分": 35, "當日收盤位置%": 28,
                     "當日量比": 0.55, "追價風險分": 65})
    wait = apply_daily_learning_overlay(pd.DataFrame([wait_row]), profile=_empty_profile())
    assert wait.iloc[0]["AI推薦資格"].startswith("AI-Q｜"), wait.iloc[0]["AI推薦資格"]
    assert apply_learning_admission(wait).iloc[0]["正式推薦分區"] != "正式下週主推薦"

    # 3. LOCKDOWN must override every stock-level score.
    lock_row = _strong_row()
    lock_row.update({"股票代號": "9997", "股票名稱": "封鎖測試", "極端市場LOCKDOWN": "是",
                     "大盤風險燈號": "極端風險｜全面禁買", "操作許可": "禁止新倉"})
    locked = apply_daily_learning_overlay(pd.DataFrame([lock_row]), profile=_empty_profile())
    assert locked.iloc[0]["AI推薦資格"].startswith("AI-LOCKDOWN")
    locked_admitted = apply_learning_admission(locked)
    assert locked_admitted.iloc[0]["正式推薦分區"] not in {"正式下週主推薦", "A-｜準主推薦小量試單"}

    # 4. Stale K-line cannot be promoted even with excellent factors.
    stale_row = _strong_row()
    stale_row.update({"股票代號": "9996", "股票名稱": "舊K線測試", "K線落後交易日": 1,
                      "K線資料新鮮度": "落後1個交易日"})
    stale = apply_daily_learning_overlay(pd.DataFrame([stale_row]), profile=_empty_profile())
    stale_admitted = apply_learning_admission(stale)
    assert stale_admitted.iloc[0]["正式推薦分區"] not in {"正式下週主推薦", "A-｜準主推薦小量試單"}

    # 5. Repeated names without fresh evidence receive a lower ranking delta.
    fresh_row = _strong_row()
    sticky_row = _strong_row()
    sticky_row.update({"近5次入榜次數": 5, "連續入榜次數": 5, "今日訊號新鮮分": 40})
    pair = apply_daily_learning_overlay(pd.DataFrame([fresh_row, sticky_row]), profile=_empty_profile())
    assert float(pair.iloc[1]["AI排名加減分"]) < float(pair.iloc[0]["AI排名加減分"])

    # 6. Historical experience profile is shrinkage-limited and produces error taxonomy.
    history = [
        {"股票代號": "1001", "AI主要召回路徑": "主升突破", "可執行交易3日%": 4.0,
         "是否納入可執行績效": "是", "模型隔日上漲機率%": 65, "績效判定": "成功"},
        {"股票代號": "1002", "AI主要召回路徑": "主升突破", "可執行交易3日%": -5.0,
         "是否納入可執行績效": "是", "模型隔日上漲機率%": 70, "績效判定": "假突破"},
        {"股票代號": "1003", "AI主要召回路徑": "基本面轉折", "可執行交易3日%": 3.0,
         "是否納入可執行績效": "是", "模型隔日上漲機率%": 60, "未觸發漏選標記": "漏選"},
    ]
    profile = build_experience_profile(history)
    assert profile["eligible_samples"] == 3
    assert profile["global_metrics"]["probability_samples"] == 3
    assert profile["global_metrics"]["brier_score"] is not None
    assert profile["error_taxonomy"].get("假突破／守價失敗", 0) == 1

    # 7. Immutable run snapshots and JSON-safe state are persisted locally without network.
    with tempfile.TemporaryDirectory(prefix="phase105_learning_test_") as td:
        ok1, messages1, state1 = save_learning_run(strong, admitted, base_dir=td, persist_remote=False)
        ok2, messages2, state2 = save_learning_run(wait, wait, base_dir=td, persist_remote=False)
        run_root = Path(td) / "data" / "godpick_learning" / "runs"
        files = sorted(run_root.rglob("*.json"))
        assert ok1 and ok2
        assert len(files) == 2, files
        assert state1["last_run_id"] != state2["last_run_id"]
        for path in files + [Path(td) / "godpick_learning_state.json"]:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            assert isinstance(loaded, dict)
        assert any("不可變決策快照" in msg for msg in messages1 + messages2)

    # 8. Summary remains pure JSON and reports all key tiers.
    summary = build_learning_summary(pd.concat([strong, wait, locked], ignore_index=True), _empty_profile())
    json.dumps(summary, ensure_ascii=False)
    assert summary["candidate_count"] == 3
    assert summary["formal_ai"] == 1
    assert summary["quality_wait"] == 1
    print(f"PASS {LEARNING_SYSTEM_VERSION}: 8 smoke/regression groups")


if __name__ == "__main__":
    run()

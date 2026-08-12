# -*- coding: utf-8 -*-
from __future__ import annotations

import ast
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

from godpick_v188_cache_guard import (
    inspect_v188_decision_frame,
    repair_v188_decision_frame,
)
from godpick_super_ai_engine import apply_super_ai_engine


def build_rows(n: int = 1710) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            "股票代號": f"{1000+i:04d}",
            "股票名稱": f"測試{i}",
            "市場別": "上市" if i % 2 == 0 else "上櫃",
            "類別": ["半導體", "PCB", "電子", "金融", "傳產"][i % 5],
            "股神推薦優先分": 93 - (i % 40) * 0.7,
            "股神實戰總分": 78 + (i % 18),
            "候選強度分": 72 + (i % 24),
            "AI Alpha品質分": 70 + (i % 27),
            "AI Timing時機分": 61 + (i % 31),
            "AI Risk風控分": 64 + (i % 25),
            "AI Continuation延續分": 67 + (i % 28),
            "Entry進場買點分": 63 + (i % 30),
            "Risk風控安全分": 66 + (i % 23),
            "實戰操作品質分": 65 + (i % 26),
            "進場可執行分": 64 + (i % 27),
            "主流主升優先分": 70 + (i % 24),
            "主流資金分": 69 + (i % 25),
            "SuperAI隔日上漲機率%": 54 + (i % 8),
            "路徑風險報酬比": 1.05 + (i % 12) * 0.08,
            "今日漲幅%": (i % 7) - 2,
            "近5日漲幅%": (i % 16) - 3,
            "追價風險分": 45 + (i % 30),
            "隔日耗竭風險分": 43 + (i % 32),
            "當日量比": 1.0 + (i % 8) * 0.15,
            "當日收盤位置%": 55 + (i % 40),
            "最新價": 50 + (i % 300) * 0.5,
            "主要進場路徑": "回測承接" if i % 3 == 0 else "突破確認",
            "正式推薦分區": "正式下週主推薦" if i % 13 == 0 else "盤中雷達追蹤",
            "是否正式推薦": True if i % 13 == 0 else False,
            "正式推薦資格": "是" if i % 13 == 0 else "否",
            "操作許可": "READY｜條件式" if i % 13 == 0 else "雷達觀察",
            "K線落後交易日": 0,
            "K線資料新鮮度": "最新",
            "官方因子資料日期": "2026-08-11",
            "官方因子落後交易日": 0,
            "官方因子新鮮度": "最新可信",
            "因子來源可信度": 100,
            "大盤資料日期": "2026-08-11",
            "大盤資料落後交易日": 0,
            "大盤資料新鮮度": "最新",
            "大盤與K線對齊狀態": "READY｜同日對齊",
            "正式推薦可用": True,
        })
    return pd.DataFrame(rows)


def assert_cache_assignment_after_superai() -> None:
    page = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    tree = ast.parse(page)
    target = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "_build_recommend_df":
            target = node
            break
    assert target is not None, "找不到 _build_recommend_df"
    src = ast.get_source_segment(page, target) or ""
    super_pos = src.find("governed_candidate_df = apply_super_ai_engine(governed_candidate_df)")
    cache_pos = src.find('st.session_state[_k("decision_frame_store_v181")] = governed_candidate_df.copy()')
    assert super_pos >= 0, "找不到 SuperAI 主體呼叫"
    assert cache_pos >= 0, "找不到最終 decision cache 寫入"
    assert cache_pos > super_pos, f"快取仍在 SuperAI 之前：super={super_pos}, cache={cache_pos}"
    assert 'V189_V188最終快取完整' in src


def main() -> None:
    assert_cache_assignment_after_superai()

    raw = build_rows(1710)
    before = inspect_v188_decision_frame(raw)
    assert before["complete"] is False
    assert "missing-v188-columns" in before["reason"]

    start = time.perf_counter()
    repaired, diag = repair_v188_decision_frame(
        raw,
        super_ai_callable=lambda df: apply_super_ai_engine(df, context={}, experience={}),
    )
    elapsed = time.perf_counter() - start

    assert diag["complete"] is True, diag
    assert diag["repaired"] is True
    for col in ["V188股神作戰優先分", "SuperAI Alpha分", "SuperAI Trade分"]:
        assert col in repaired.columns
        vals = pd.to_numeric(repaired[col], errors="coerce")
        assert vals.notna().mean() >= 0.99
        assert (vals > 0).any()
    assert repaired["V188版本"].fillna("").astype(str).str.strip().ne("").mean() >= 0.99
    assert repaired["V188交易許可"].fillna("").astype(str).str.strip().ne("").mean() >= 0.99

    # Regression: a complete frame must be reused and not scored again.
    calls = {"n": 0}
    def should_not_run(df):
        calls["n"] += 1
        return df
    reused, diag2 = repair_v188_decision_frame(repaired, super_ai_callable=should_not_run)
    assert diag2["complete"] is True
    assert diag2["repaired"] is False
    assert calls["n"] == 0
    assert len(reused) == 1710

    # Old UI behavior of manufacturing zeros must be impossible to pass guard.
    fake = raw.copy()
    fake["V188版本"] = ""
    fake["SuperAI Alpha等級"] = ""
    fake["SuperAI Trade等級"] = ""
    fake["SuperAI最終作戰等級"] = ""
    fake["V188交易許可"] = ""
    fake["V188正式推薦資格"] = ""
    fake["V188股神作戰優先分"] = 0
    fake["SuperAI Alpha分"] = 0
    fake["SuperAI Trade分"] = 0
    bad = inspect_v188_decision_frame(fake)
    assert bad["complete"] is False
    assert "all-zero-score" in bad["reason"] or "text-coverage-low" in bad["reason"]

    print("PASS V189 V188 final cache guard")
    print(f"rows={len(repaired)} elapsed_sec={elapsed:.4f}")
    print("alpha_range=", float(repaired["SuperAI Alpha分"].min()), float(repaired["SuperAI Alpha分"].max()))
    print("trade_range=", float(repaired["SuperAI Trade分"].min()), float(repaired["SuperAI Trade分"].max()))
    print("priority_range=", float(repaired["V188股神作戰優先分"].min()), float(repaired["V188股神作戰優先分"].max()))


if __name__ == "__main__":
    main()

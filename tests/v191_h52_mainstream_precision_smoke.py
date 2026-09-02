# -*- coding: utf-8 -*-
from pathlib import Path
import ast
import sys
from typing import Any
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_human_master_engine import apply_human_master_engine, build_h51_final_decision_table, VERSION


def _base_row(code: str, name: str, today: float = 1.5) -> dict:
    return {
        "股票代號": code,
        "股票名稱": name,
        "類別": "AI伺服器",
        "H50族群生命週期": "A1｜新鮮主流",
        "H50波段機會階段": "N-EARLY｜主升起漲",
        "H47主流領先狀態": "L-LEADER｜領漲",
        "H50族群可買主流分": 82,
        "H45族群主流分": 80,
        "H50族群新鮮度分": 84,
        "H50族群回檔再攻分": 78,
        "H47個股相對強度分": 82,
        "H47族群內領先百分位%": 90,
        "H47起漲優先分": 86,
        "H45趨勢延續分": 80,
        "今日漲幅%": today,
        "近5日漲幅%": 4,
        "近20日漲幅%": 15,
        "距20日高點%": 2,
        "當日量比": 1.4,
        "當日收盤位置%": 78,
        "上影線比例%": 15,
        "成交額百萬": 1200,
        "20日均成交額百萬": 900,
        "主流資金分": 82,
        "EPS代理分數": 70,
        "營收動能代理分數": 75,
        "獲利代理分數": 72,
        "族群攻擊強度": 82,
        "今日訊號新鮮分": 88,
        "H50重複推薦扣分": 0,
        "追價風險分": 35,
        "路徑風險報酬比": 1.8,
        "SuperAI Trade分": 78,
        "Risk風控安全分": 76,
        "Entry進場買點分": 74,
        "實戰停損距離%": 5.5,
        "最新價": 100,
    }


def _load_category_funcs():
    page = ROOT / "pages" / "7_股神推薦.py"
    tree = ast.parse(page.read_text(encoding="utf-8"))
    wanted = {"CATEGORY_KEYWORD_RULES", "CANONICAL_CATEGORY_ALIAS"}
    funcs = {"_canonical_category", "_infer_category_from_name", "_infer_category_from_record"}
    nodes = []
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            names = []
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name): names.append(target.id)
            elif isinstance(node.target, ast.Name):
                names.append(node.target.id)
            if wanted.intersection(names): nodes.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in funcs:
            nodes.append(node)
    ns = {
        "Any": Any,
        "_normalize_category": lambda v: "" if v is None else str(v).strip(),
        "_safe_str": lambda v: "" if v is None else str(v).strip(),
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(page), "exec"), ns, ns)
    return ns["_infer_category_from_record"]


def main():
    infer = _load_category_funcs()
    assert infer("南亞科", "半導體業") == "記憶體"
    assert infer("南電", "電子零組件業") == "PCB載板"
    assert infer("大立光", "光電業") == "光學鏡頭"
    assert infer("中揚光", "光電業") == "光學鏡頭"
    assert infer("旺矽", "半導體業") == "半導體測試介面"
    assert infer("鴻海", "AI伺服器") == "AI伺服器"

    shock = _base_row("6223", "旺矽", today=-10.0)
    shock["類別"] = "半導體測試介面"
    out = apply_human_master_engine(pd.DataFrame([shock]))
    assert out.iloc[0]["H51市場地位"].startswith("HM-RECLAIM")
    assert out.iloc[0]["H51交易許可"].startswith("WAIT-RECLAIM")
    assert "SHOCK-DOWN" in out.iloc[0]["H51急跌收復狀態"]

    ready = _base_row("0001", "測試起漲", today=2.0)
    weak = _base_row("0002", "研究領漲", today=2.0)
    weak["路徑風險報酬比"] = 0.4
    weak["SuperAI Trade分"] = 48
    weak["Risk風控安全分"] = 48
    weak["Entry進場買點分"] = 45
    scored = apply_human_master_engine(pd.DataFrame([ready, weak]))
    assert scored.iloc[0]["H51發動潛力分"] > 0
    assert scored.iloc[1]["H51交易許可"].startswith("LEADER-WATCH")
    final = build_h51_final_decision_table(scored, max_rows=6)
    if "H51交易許可" in final.columns:
        assert not final["H51交易許可"].astype(str).str.startswith("LEADER-WATCH").any()
    assert VERSION in {"v191_h52_mainstream_precision_ignition_truth_20260827", "v191_h53_sector_resonance_nextday_priority_20260828", "v191_h54_continuation_exhaustion_overnight_truth_20260831", "v191_h58_single_decision_truth_console_20260902"}
    print("PASS v191_h52_mainstream_precision_smoke")


if __name__ == "__main__":
    main()

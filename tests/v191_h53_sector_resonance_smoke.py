# -*- coding: utf-8 -*-
from pathlib import Path
import ast
import sys
from typing import Any
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_human_master_engine import (
    VERSION,
    apply_human_master_engine,
    build_h51_final_decision_table,
    build_h51_sector_table,
)


def _row(code: str, name: str, sector: str, today: float, strong: bool = True) -> dict:
    return {
        "股票代號": code, "股票名稱": name, "類別": sector,
        "H50族群生命週期": "A1｜新鮮主流" if strong else "B2｜一般輪動",
        "H50波段機會階段": "N-EARLY｜主升起漲" if strong else "N-RADAR｜觀察",
        "H47主流領先狀態": "L-LEADER｜領漲" if strong else "L-RADAR｜觀察",
        "H50族群可買主流分": 82 if strong else 62,
        "H45族群主流分": 80 if strong else 60,
        "H50族群新鮮度分": 84 if strong else 58,
        "H50族群回檔再攻分": 78 if strong else 56,
        "H47個股相對強度分": 82 if strong else 56,
        "H47族群內領先百分位%": 88 if strong else 52,
        "H47起漲優先分": 84 if strong else 55,
        "H45趨勢延續分": 80 if strong else 56,
        "今日漲幅%": today,
        "近5日漲幅%": 5 if strong else 1,
        "近20日漲幅%": 15 if strong else 6,
        "距20日高點%": 2 if strong else 8,
        "當日量比": 1.55 if strong else 0.95,
        "當日收盤位置%": 80 if strong else 50,
        "上影線比例%": 12 if strong else 30,
        "成交額百萬": 900 if strong else 500,
        "20日均成交額百萬": 600 if strong else 500,
        "主流資金分": 82 if strong else 55,
        "EPS代理分數": 70, "營收動能代理分數": 72, "獲利代理分數": 70,
        "族群攻擊強度": 86 if strong else 48,
        "族群廣度分": 84 if strong else 45,
        "族群成交額分": 82 if strong else 50,
        "族群資金流分數": 85 if strong else 48,
        "同族群強勢比例": 0.78 if strong else 0.35,
        "同族群平均量能分": 84 if strong else 48,
        "H45族群5日上漲比例%": 80 if strong else 42,
        "今日訊號新鮮分": 88 if strong else 55,
        "H50重複推薦扣分": 0,
        "追價風險分": 38 if strong else 55,
        "路徑風險報酬比": 1.25 if strong else 1.1,
        "SuperAI Trade分": 62 if strong else 54,
        "Risk風控安全分": 66 if strong else 56,
        "Entry進場買點分": 62 if strong else 55,
        "實戰停損距離%": 6.5,
        "最新價": 100,
    }


def _load_category_infer():
    page = ROOT / "pages" / "7_股神推薦.py"
    tree = ast.parse(page.read_text(encoding="utf-8"))
    wanted = {"CATEGORY_KEYWORD_RULES", "CANONICAL_CATEGORY_ALIAS"}
    funcs = {"_canonical_category", "_infer_category_from_name", "_infer_category_from_record"}
    nodes = []
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            names = []
            if isinstance(node, ast.Assign):
                names = [t.id for t in node.targets if isinstance(t, ast.Name)]
            elif isinstance(node.target, ast.Name):
                names = [node.target.id]
            if wanted.intersection(names):
                nodes.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in funcs:
            nodes.append(node)
    ns = {
        "Any": Any,
        "_normalize_category": lambda v: "" if v is None else str(v).strip(),
        "_safe_str": lambda v: "" if v is None else str(v).strip(),
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(page), "exec"), ns, ns)
    return ns["_infer_category_from_record"]


def main():
    infer = _load_category_infer()
    assert infer("蜜望實", "電子零組件業") == "被動元件"
    assert infer("臺慶科", "電子零組件業") == "被動元件"
    assert infer("鈺邦", "電子零組件業") == "被動元件"
    assert infer("信昌電", "電子零組件業") == "被動元件"

    passive = [
        _row("8043", "蜜望實", "被動元件", 6.0, True),
        _row("2472", "立隆電", "被動元件", 4.8, True),
        _row("3357", "臺慶科", "被動元件", 3.6, True),
        _row("2327", "國巨", "被動元件", 2.8, True),
        _row("2492", "華新科", "被動元件", 3.2, True),
    ]
    broad = [_row(f"90{i:02d}", f"寬類{i}", "電子零組件業", 0.4 if i % 2 else -0.5, False) for i in range(8)]
    scored = apply_human_master_engine(pd.DataFrame(passive + broad))
    assert VERSION == "v191_h56_authority_preopen_two_stage_truth_20260902"
    assert {"H53族群共振分", "H53領漲集群分", "H53隔日優先分", "H53參考層級"}.issubset(scored.columns)

    p = scored[scored["類別"].eq("被動元件")]
    b = scored[scored["類別"].eq("電子零組件業")]
    assert p["H53族群共振分"].mean() > b["H53族群共振分"].mean() + 10
    assert p["H53隔日優先分"].mean() > b["H53隔日優先分"].mean()
    assert b["H53分類稀釋扣分"].max() >= 7

    sec = build_h51_sector_table(scored, max_rows=10)
    assert sec.iloc[0]["類別"] == "被動元件", sec[["類別", "H53族群決策分"]].to_dict("records")

    final = build_h51_final_decision_table(scored, max_rows=6)
    if "股票代號" in final.columns:
        assert final.iloc[0]["類別"] == "被動元件"
        assert "H53隔日優先分" in final.columns
        # H53 may upgrade reference priority, but never rewrites H51 trade permission.
        assert final.iloc[0]["H51交易許可"].startswith(("BUY-READY", "SETUP-PREP"))

    print("PASS v191_h53_sector_resonance_smoke")


if __name__ == "__main__":
    main()

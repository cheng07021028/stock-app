# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_super_ai_excel_guide import build_super_ai_excel_guide

def _row(code, name, entry, risk, rr, exe, gap, chase, trade, final, sector, main, partition="", permit="等待"):
    return {
        "股票代號":code, "股票名稱":name, "市場別":"上市", "類別":"測試",
        "Entry進場買點分":entry, "Risk風控安全分":risk, "路徑風險報酬比":rr,
        "進場可執行分":exe, "距最近可執行買點%":gap, "追價風險分":chase,
        "SuperAI Trade分":trade, "SuperAI 最終決策分":final, "模型隔日上漲機率%":56,
        "族群攻擊強度":sector, "主流資金分":main,
        "正式推薦分區":partition, "操作許可":permit,
        "K線資料新鮮度":"最新", "官方因子新鮮度":"最新/對齊",
        "大盤風控層級":"黃燈｜廣度偏弱但權威大盤可選股，縮倉精選",
        "正式與A近門檻說明":"等待觸發價",
    }

def test_formal_and_a_minus_are_always_first():
    df = pd.DataFrame([
        _row("1001","普通高分",90,90,3,90,1,10,90,90,90,90),
        _row("1002","A減",68,68,1.2,60,3,35,70,70,65,65,"A-｜準主推薦小量試單","條件進場"),
        _row("1003","正式",75,75,1.8,70,2,30,75,75,70,70,"正式下週主推薦","可操作"),
    ])
    out = build_super_ai_excel_guide(df)
    assert list(out["股票代號"][:2]) == ["1003","1002"], out.to_dict("records")
    assert list(out["超級AI定位"][:2]) == ["正式推薦","A-準主推薦"], out.to_dict("records")

def test_conditional_candidate_follows_official_trade_permission():
    # H31 superseded H30 metric-only promotion: the guide is descriptive and may
    # call a row 條件候選 only when the official operation permit is executable.
    df = pd.DataFrame([
        _row("2001","可執行",70,72,1.5,55,4,30,75,70,60,60, permit="條件進場"),
        _row("2002","結構佳但仍等待",80,80,2.0,80,1,10,90,90,90,90, permit="等待"),
        _row("2003","RR太低",80,80,0.6,80,1,10,90,90,90,90, permit="等待"),
    ])
    out = build_super_ai_excel_guide(df)
    m = dict(zip(out["股票代號"], out["超級AI定位"]))
    assert m["2001"] == "條件候選", m
    assert m["2002"] == "觀察雷達", m
    assert m["2003"] == "觀察雷達", m

def test_total_rank_never_overrides_risk_rr():
    df = pd.DataFrame([{
        **_row("6488","高研究排名但未成熟",48.1,59.8,1.34,35.8,5.94,71,67.4,65.5,83.1,69.2),
        "股神推薦總排名":1,
    }])
    out = build_super_ai_excel_guide(df)
    assert out.iloc[0]["超級AI定位"] == "觀察雷達", out.iloc[0].to_dict()
    assert float(out.iloc[0]["追價風險"]) == 71.0

def test_duplicate_numeric_columns_do_not_crash():
    base = _row("3001","重複欄",70,70,1.5,50,4,30,70,70,60,60)
    df = pd.DataFrame([[*base.values(), 75]], columns=[*base.keys(), "Entry進場買點分"])
    out = build_super_ai_excel_guide(df)
    assert len(out) == 1
    assert float(out.iloc[0]["Entry"]) in {70.0,75.0}

def test_page07_places_guide_before_total_rank():
    src=(ROOT/'pages'/'7_股神推薦.py').read_text(encoding='utf-8')
    a=src.index('(\"超級AI股神精選攻略\", super_ai_guide_export')
    b=src.index('(\"股神推薦總排名\", master_rank_df', a)
    assert a < b
    assert 'build_super_ai_excel_guide' in src
    assert '第一優先｜超級AI精選攻略' in src

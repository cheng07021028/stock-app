# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import godpick_formal_recommendation_engine as formal
import market_regime_engine as regime


def _base_market_row():
    return {
        "股票代號":"9999", "股票名稱":"H29測試股",
        "大盤資料日期":"2026-08-14", "K線最後交易日":"2026-08-14",
        "本輪市場最新交易日":"2026-08-14", "大盤資料新鮮度":"最新",
        "大盤橋接分數":62.7, "大盤多空分數":62.7,
        "大盤橋接狀態":"中性偏多", "大盤橋接風控":"中性",
        "大盤策略模式":"偏多輪動",
        "大盤策略建議":"大盤分數62.7，資金輪動機率高，優先看族群資金流與拉回承接。",
        "大盤風控建議":"偏多輪動，仍需依停損策略控管單檔風險。",
        "大盤風險燈號":"紅燈｜防守",
        "今日大盤結論":"不適合擴大倉位；以等突破與風控為主。",
        "加權漲跌%":-0.46, "櫃買漲跌幅%":0.0, "市場上漲家數比例%":45,
    }


def _a_minus_row(rr=1.20):
    r = _base_market_row()
    r.update({
        "最新價":100, "推薦買點_拉回":99, "實戰觸發價":102, "觸發後守價":100,
        "實戰停損距離%":5.0, "實戰壓力空間%":6.0,
        "實戰風險報酬比":rr, "風險報酬比":rr,
        "買進分數":68, "Entry進場買點分":75, "Risk風控安全分":68, "股神實戰總分":75,
        "技術結構分數":75, "起漲前兆分數":72, "交易可行分數":70,
        "推薦總分":78, "候選強度分":78,
        "主流資金分":65, "族群攻擊強度":60, "族群輪動分":60, "資金攻擊有效分":65,
        "成交額百萬":400, "20日均成交額百萬":350, "最新成交量_張":1500, "20日均量_張":1400,
        "追價風險分":45, "近5日漲幅%":1.0, "近20日漲幅%":8.0,
        "今日漲幅%":1.0, "當日量比":1.1, "當日收盤位置%":70,
        "爆發雷達分":60, "隔日爆發分":60, "強勢動能分":60, "強勢前兆分":60,
        "推薦角色":"B｜等突破確認", "族群廣度分":60,
        "K線落後交易日":0, "K線資料新鮮度":"最新",
        "官方資料完整度":100, "官方因子資料狀態":"完整",
        "每日因子來源可信度":100, "因子來源可信度":100, "來源可信度狀態":"官方高可信",
        "官方資料日期":"2026-08-14", "官方因子資料日期":"2026-08-14",
        "法人資料日期":"2026-08-14", "估值資料日期":"2026-08-14",
    })
    return r


def test_user_exact_semantics_is_defensive_not_severe():
    info = formal._market_risk_info(pd.Series(_base_market_row()))
    assert info["aligned"] is True, info
    assert info["supportive_authority"] is True, info
    assert info["breadth_defensive"] is True, info
    assert info["severe"] is False, info
    assert info["defensive"] is True, info
    assert "廣度偏弱" in info["level"], info


def test_true_bearish_authority_remains_severe():
    row = _base_market_row()
    row.update({"大盤橋接分數":35, "大盤多空分數":35, "大盤橋接狀態":"偏空", "大盤橋接風控":"空方"})
    info = formal._market_risk_info(pd.Series(row))
    assert info["severe"] is True, info
    assert info["supportive_authority"] is False, info


def test_extreme_market_lockdown_is_not_relaxed():
    row = _base_market_row()
    row["加權漲跌%"] = -4.1
    info = formal._market_risk_info(pd.Series(row))
    assert info["panic"] is True and info["severe"] is True, info
    assert info["new_position_cap_pct"] == 0, info


def test_breadth_engine_labels_its_own_red_semantics():
    df = pd.DataFrame([
        {"近5日漲幅%":-5, "近20日漲幅%":-8, "推薦總分":48, "成交額百萬":100,
         "人氣量能分":40, "大盤橋接分數":62.7, "大盤橋接狀態":"中性偏多"}
        for _ in range(8)
    ])
    out = regime.apply_market_regime_engine(df)
    assert set(out["大盤風險燈號"].astype(str)) == {"廣度紅燈｜防守"}, out.iloc[0].to_dict()


def test_a_minus_path_is_not_false_zero_under_supportive_authority():
    out = formal.apply_formal_recommendation_engine(pd.DataFrame([_a_minus_row()]))
    row = out.iloc[0]
    assert row["正式推薦分區"] == "A-｜準主推薦小量試單", row.to_dict()
    assert "廣度偏弱" in str(row["大盤風控層級"]), row.to_dict()


def test_rr_guard_still_blocks_low_rr():
    out = formal.apply_formal_recommendation_engine(pd.DataFrame([_a_minus_row(rr=0.95)]))
    assert out.iloc[0]["正式推薦分區"] not in {"正式下週主推薦", "A-｜準主推薦小量試單"}, out.iloc[0].to_dict()

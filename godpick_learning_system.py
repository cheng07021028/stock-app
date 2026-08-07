# -*- coding: utf-8 -*-
"""Phase 108｜V177 全市場 AI 發現、跨市場新強股召回與戰術風控決策核心。

設計原則：
1. 多路召回，不讓大型熱門股壟斷候選。
2. Alpha / Timing / Risk / Continuation 四引擎分離。
3. 只以已發生的績效紀錄做小幅、收縮後的經驗校正，避免單日過擬合。
4. 每次完整掃描保存不可變決策快照；正式推薦為 0 也必須記錄。
5. Champion / Challenger 欄位先建立，正式模型只以已驗證規則升格。
6. 將「結構停損」與「戰術守價」分離，避免主升股被過遠結構停損誤殺。
7. 以全市場橫斷面排名補回新主升／成長領漲股，但不繞過流動性、K線與市場硬風控。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
import hashlib
import json
import math
import os
import re

import pandas as pd

LEARNING_SYSTEM_VERSION = "phase108_full_market_discovery_ai_v1_20260807"
MODEL_VERSION = "champion_phase108_full_market_discovery_v1"
CHALLENGER_VERSION = "challenger_phase108_novelty_diversity_v1"
LEARNING_STATE_FILE = "godpick_learning_state.json"
LEARNING_ROOT = "data/godpick_learning"
DEFAULT_RECORD_FILE = "godpick_records.json"

LEARNING_COLUMNS = [
    "AI市場狀態", "AI召回路徑", "AI主要召回路徑", "AI召回分",
    "AI Alpha品質分", "AI Timing時機分", "AI Risk風控分", "AI Continuation延續分",
    "AI資料信心分", "AI經驗校正分", "AI綜合決策分", "AI排名加減分",
    "AI跨市場新強股分", "AI新證據分", "AI召回來源數", "AI召回保留旗標", "AI漏選風險分",
    "AI結構停損距離%", "AI戰術停損距離%", "AI戰術風報比", "AI趨勢延伸目標價",
    "AI領漲延續狀態", "AI過熱型態", "AI風險口徑", "AI重複證據衰減分",
    "AI預測優勢", "AI推薦資格", "AI推薦理由", "AI反對理由", "AI取消條件",
    "AI學習樣本數", "AI模型版本", "AI Challenger分", "AI Champion勝出",
    "AI發現母體", "AI舊規則軟篩選數", "AI舊規則救回旗標",
]

_NUMERIC_COLUMNS = {
    "AI召回分", "AI Alpha品質分", "AI Timing時機分", "AI Risk風控分",
    "AI Continuation延續分", "AI資料信心分", "AI經驗校正分",
    "AI綜合決策分", "AI排名加減分", "AI學習樣本數", "AI Challenger分",
    "AI跨市場新強股分", "AI新證據分", "AI召回來源數", "AI漏選風險分",
    "AI結構停損距離%", "AI戰術停損距離%", "AI戰術風報比", "AI趨勢延伸目標價",
    "AI重複證據衰減分", "AI舊規則軟篩選數",
}


def _root(base_dir: str | Path | None = None) -> Path:
    return Path(base_dir or Path(__file__).resolve().parent)


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        out = float(value)
        return float(default) if math.isnan(out) or math.isinf(out) else out
    except Exception:
        text = _safe_str(value).replace(",", "").replace("%", "")
        try:
            return float(text)
        except Exception:
            return float(default)


def _clip(value: Any, low: float = 0.0, high: float = 100.0) -> float:
    return round(max(low, min(high, _safe_float(value))), 2)


def _first_num(row: pd.Series | dict[str, Any], names: Iterable[str], default: float = 0.0, *, positive: bool = False) -> float:
    for name in names:
        try:
            value = row.get(name)
        except Exception:
            value = None
        number = _safe_float(value, float("nan"))
        if not math.isnan(number) and (not positive or number > 0):
            return number
    return float(default)


def _best_num(row: pd.Series | dict[str, Any], names: Iterable[str], default: float = 0.0, *, positive: bool = False, mode: str = "max") -> float:
    values: list[float] = []
    for name in names:
        try:
            value = row.get(name)
        except Exception:
            value = None
        number = _safe_float(value, float("nan"))
        if math.isnan(number) or (positive and number <= 0):
            continue
        values.append(number)
    if not values:
        return float(default)
    return min(values) if mode == "min" else max(values)


def _text_blob(row: pd.Series | dict[str, Any], names: Iterable[str]) -> str:
    return "｜".join(_safe_str(row.get(name)) for name in names if _safe_str(row.get(name)))


def _contains(text: Any, words: Iterable[str]) -> bool:
    blob = _safe_str(text).lower()
    return any(_safe_str(word).lower() in blob for word in words)


def _code(value: Any) -> str:
    text = _safe_str(value)
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(4) if 1 <= len(digits) <= 4 else digits or text


def _market_regime(row: pd.Series | dict[str, Any]) -> str:
    text = _text_blob(row, [
        "大盤風險燈號", "大盤策略模式", "大盤橋接風控", "大盤橋接狀態",
        "大盤風控層級", "極端市場LOCKDOWN", "市場環境", "AI市場狀態",
    ])
    if _contains(text, ["lockdown", "全面禁買", "極端風險"]):
        return "LOCKDOWN"
    if _contains(text, ["紅燈", "空頭", "崩跌", "冷卻"]):
        return "空頭防守"
    if _contains(text, ["黃燈", "震盪", "輪動"]):
        return "震盪輪動"
    if _contains(text, ["綠燈", "多頭", "攻擊"]):
        return "趨勢多頭"
    score = _first_num(row, ["大盤橋接分數", "大盤多空分數", "市場環境分數"], 50)
    if score >= 65:
        return "趨勢多頭"
    if score <= 35:
        return "空頭防守"
    return "中性震盪"


def _data_confidence(row: pd.Series | dict[str, Any]) -> float:
    complete = _first_num(row, ["官方資料完整度", "資料完整度評分", "資料完整度", "官方因子總分"], 0)
    source_trust = _first_num(row, ["因子來源可信度", "來源可信度", "樣本可信度"], 0)
    if source_trust <= 0:
        source_text = _text_blob(row, ["官方因子資料源", "因子主要來源", "因子備援來源", "籌碼資料來源", "基本面資料來源"])
        source_trust = 100 if _contains(source_text, ["twse", "tpex", "mops", "官方"]) else 82 if _contains(source_text, ["finmind"]) else 60 if _contains(source_text, ["快取"]) else 35
    kline = _safe_str(row.get("K線資料新鮮度"))
    lag = _first_num(row, ["K線落後交易日"], 999)
    kline_score = 100 if (lag == 0 and not _contains(kline, ["未知", "未驗證", "待更新"])) else 55 if lag == 1 else 15
    official_text = _safe_str(row.get("官方因子新鮮度"))
    official_lag = _first_num(row, ["官方因子落後交易日"], 999)
    official_fresh = 100 if official_lag == 0 else 82 if official_lag == 1 else 50 if _contains(official_text, ["有效"]) else 20
    return _clip(complete * 0.43 + source_trust * 0.22 + kline_score * 0.22 + official_fresh * 0.13)


def _liquidity_score(row: pd.Series | dict[str, Any]) -> float:
    amount = _first_num(row, ["成交額百萬", "流動性參考成交額百萬", "20日均成交額百萬"], 0)
    if amount >= 3000:
        base = 100
    elif amount >= 1200:
        base = 92
    elif amount >= 500:
        base = 83
    elif amount >= 200:
        base = 72
    elif amount >= 100:
        base = 60
    elif amount > 0:
        base = 35
    else:
        base = 15
    text = _text_blob(row, ["流動性等級", "流動性資料狀態", "冷門股警示"])
    if _contains(text, ["低流動", "冷門", "不足"]):
        base = min(base, 38)
    return float(base)


def _fundamental_components(row: pd.Series | dict[str, Any]) -> tuple[float, float, float, float]:
    revenue = _first_num(row, ["營收成長官方分數", "基本面成長分數", "營收成長分數", "營收動能代理分數", "營收動能代理"], 50)
    eps = _first_num(row, ["EPS成長分數", "EPS代理分數", "EPS代理", "獲利代理分數", "官方基本面成長分數"], 50)
    official = _first_num(row, ["官方因子總分", "自動因子總分", "基本面成長分數"], 50)
    per = _first_num(row, ["PER本益比", "本益比", "本益比(倍)"], 0)
    valuation_risk = _first_num(row, ["官方估值風險分數", "估值風險分數"], 50)
    if per > 0:
        per_score = 88 if 8 <= per <= 20 else 78 if 5 <= per < 8 or 20 < per <= 30 else 62 if 30 < per <= 45 else 42
        valuation = per_score * 0.65 + valuation_risk * 0.35
    else:
        valuation = valuation_risk
    return _clip(revenue), _clip(eps), _clip(official), _clip(valuation)


def _institution_score(row: pd.Series | dict[str, Any]) -> float:
    score = _first_num(row, ["法人籌碼官方分數", "法人籌碼分數", "法人分數", "法人攻擊分", "主流資金分"], 50)
    f1 = _first_num(row, ["外資近1日買賣超", "外資近1日買賣超股數", "外資買賣超"], 0)
    t1 = _first_num(row, ["投信近1日買賣超", "投信近1日買賣超股數", "投信買賣超"], 0)
    total5 = _first_num(row, ["三大法人近5日合計", "法人5日買超"], 0)
    directional = 50 + (8 if f1 > 0 else -6 if f1 < 0 else 0) + (10 if t1 > 0 else -8 if t1 < 0 else 0) + (8 if total5 > 0 else -6 if total5 < 0 else 0)
    return _clip(score * 0.65 + directional * 0.35)



def _tactical_path_metrics(row: pd.Series | dict[str, Any]) -> dict[str, Any]:
    """Separate long-horizon structural protection from an executable tactical guard.

    The previous engine could calculate reward/risk with a distant structural support
    while the actual trigger plan already had a tight guard.  That systematically
    erased strong trend leaders.  Phase107 keeps both distances and only uses the
    tactical guard for entry qualification when it is valid.
    """
    latest = _first_num(row, ["最新價", "收盤價", "參考價"], 0, positive=True)
    trigger = _first_num(row, ["實戰觸發價", "預估進場點", "進場價", "最新價"], latest, positive=True)
    guard = _first_num(row, ["觸發後守價", "隔日守價", "實戰守價"], 0, positive=True)
    structural = _first_num(row, ["停損參考", "實戰停損參考", "結構停損價"], 0, positive=True)
    pressure = _first_num(row, ["第一壓力價", "第一目標價", "壓力價"], 0, positive=True)

    def _distance(stop_price: float) -> float:
        if trigger <= 0 or stop_price <= 0 or stop_price >= trigger:
            return 99.0
        return max(0.0, (trigger - stop_price) / trigger * 100.0)

    guard_dist = _distance(guard)
    structural_dist = _distance(structural)
    if 0.20 <= guard_dist <= 8.50:
        tactical_stop = guard
        tactical_dist = guard_dist
        risk_basis = "戰術守價"
    else:
        tactical_stop = structural
        tactical_dist = structural_dist
        risk_basis = "結構停損"

    main = _first_num(row, ["主流主升優先分", "主流資金分", "族群攻擊強度"], 50)
    sector = _first_num(row, ["族群攻擊強度", "類股熱度分數"], 50)
    revenue = _first_num(row, ["營收成長官方分數", "營收動能代理分數", "營收動能代理"], 50)
    tech = _first_num(row, ["技術結構分數", "強勢動能分", "爆發雷達分"], 50)
    missed = _first_num(row, ["強勢股漏選風險分", "主流領漲回補分", "隔日爆發分"], 50)
    amount = _first_num(row, ["成交額百萬", "20日均成交額百萬"], 0)
    leader_text = _text_blob(row, ["是否領先同類股", "類股前3強", "推薦角色", "領漲回補角色"])
    leader_evidence = (
        amount >= 80
        and max(main, sector) >= 65
        and max(revenue, tech, missed) >= 65
        and (_contains(leader_text, ["是", "前3", "領漲", "回補"]) or missed >= 75)
    )

    risk_abs = max(0.0, trigger - tactical_stop) if trigger > 0 and tactical_stop > 0 else 0.0
    first_rr = (pressure - trigger) / risk_abs if pressure > trigger and risk_abs > 0 else 0.0
    runner_target = trigger + risk_abs * 1.60 if trigger > 0 and risk_abs > 0 and leader_evidence else 0.0
    target = pressure if first_rr >= 1.05 else max(pressure, runner_target)
    tactical_rr = (target - trigger) / risk_abs if target > trigger and risk_abs > 0 else 0.0
    # Historical rows and unit tests may carry only distance/RR fields without explicit prices.
    # Preserve those valid values instead of degrading them to 99% / 0R.
    fallback_stop = _best_num(row, ["實戰停損距離%", "停損距離_隔日%", "停損距離%"], 99, positive=True, mode="min")
    fallback_rr = _best_num(row, ["路徑風險報酬比", "實戰風險報酬比", "風險報酬比"], 0, positive=True, mode="max")
    if structural_dist >= 99 and fallback_stop < 99:
        structural_dist = fallback_stop
    if tactical_dist >= 99 and fallback_stop < 99:
        tactical_dist = fallback_stop
        risk_basis = "既有實戰風控距離"
    if tactical_rr <= 0 and fallback_rr > 0:
        tactical_rr = fallback_rr

    ret1 = _first_num(row, ["今日漲幅%", "區間漲跌幅%"], 0)
    ret5 = _first_num(row, ["近5日漲幅%"], 0)
    close_pos = _first_num(row, ["當日收盤位置%"], 50)
    upper = _first_num(row, ["上影線比例%"], 0)
    volume = _first_num(row, ["當日量比", "5日20日量比"], 1.0)
    blowoff = (
        (ret1 >= 8 and (close_pos < 65 or upper >= 30 or volume >= 2.5))
        or (ret5 >= 25 and ret1 >= 5 and close_pos < 70)
    )
    secondary_entry = (
        not blowoff
        and ret5 >= 15
        and -3.0 <= ret1 <= 4.5
        and tactical_dist <= 4.5
        and max(main, sector) >= 65
        and max(revenue, tech, missed) >= 60
    )
    ignition_wait = not blowoff and 3.0 < ret1 < 8.0 and close_pos >= 75
    if blowoff:
        heat_type = "爆量噴出末升"
        continuation_state = "過熱末升｜僅等大幅回測"
    elif secondary_entry:
        heat_type = "主升整理二次買點"
        continuation_state = "主升延續｜等待觸發與守價"
    elif ignition_wait:
        heat_type = "當日點火隔日只等回測"
        continuation_state = "點火有效｜禁止開盤追價"
    else:
        heat_type = "正常趨勢"
        continuation_state = "依觸發與族群同步確認"

    return {
        "AI結構停損距離%": round(structural_dist, 2),
        "AI戰術停損距離%": round(tactical_dist, 2),
        "AI戰術風報比": round(max(0.0, tactical_rr), 2),
        "AI趨勢延伸目標價": round(max(0.0, target), 4),
        "AI領漲延續狀態": continuation_state,
        "AI過熱型態": heat_type,
        "AI風險口徑": risk_basis,
        "_AI領漲證據": bool(leader_evidence),
    }

def _recall_routes(row: pd.Series | dict[str, Any]) -> dict[str, float]:
    tech = _first_num(row, ["技術結構分數", "技術趨勢分數", "Alpha選股潛力分"], 50)
    early = _first_num(row, ["起漲前兆分數", "強勢前兆分", "盤前強勢前兆分"], 50)
    breakout = _first_num(row, ["型態突破分數", "突破準備分", "強勢動能分", "爆發雷達分"], 50)
    entry = _first_num(row, ["Entry進場買點分", "進場買點分", "買進分數"], 50)
    risk = _first_num(row, ["Risk風控安全分", "風控安全分", "交易可行分數"], 50)
    main = _first_num(row, ["主流主升優先分", "主流資金分", "族群攻擊強度"], 50)
    sector = _first_num(row, ["族群攻擊強度", "類股熱度分數", "族群資金流分數"], 50)
    volume = _first_num(row, ["當日量比", "5日20日量比"], 1.0)
    close_pos = _first_num(row, ["當日收盤位置%"], 50)
    ret1 = _first_num(row, ["今日漲幅%", "區間漲跌幅%"], 0)
    ret5 = _first_num(row, ["近5日漲幅%"], 0)
    ret20 = _first_num(row, ["近20日漲幅%"], 0)
    dist_high = _first_num(row, ["距20日高點%"], -20)
    stop = _best_num(row, ["實戰停損距離%", "停損距離_隔日%", "停損距離%"], 99, positive=True, mode="min")
    rr = _best_num(row, ["路徑風險報酬比", "實戰風險報酬比", "風險報酬比"], 0, positive=True, mode="max")
    revenue, eps, official, valuation = _fundamental_components(row)
    institution = _institution_score(row)
    liquidity = _liquidity_score(row)
    catalyst_text = _text_blob(row, ["催化劑", "新聞摘要", "法說摘要", "題材", "推薦理由摘要", "推薦原因"])
    catalyst = 72 if _contains(catalyst_text, ["法說", "新產品", "新客戶", "接單", "擴產", "漲價", "政策", "報價", "轉單"]) else 48

    routes: dict[str, float] = {}
    routes["主升突破"] = _clip(tech * .18 + breakout * .22 + main * .22 + sector * .12 + close_pos * .10 + min(volume, 2.2) / 2.2 * 100 * .08 + liquidity * .08)
    if ret5 > 12 or ret20 > 28 or ret1 > 8:
        routes["主升突破"] -= min(18, max(ret5 - 12, 0) * .7 + max(ret1 - 8, 0) * 1.5)

    position_score = 90 if -7 <= dist_high <= 1.5 else 72 if -12 <= dist_high < -7 else 50
    heat_score = 92 if -3 <= ret5 <= 7 and -5 <= ret20 <= 16 else 65 if ret5 <= 12 else 35
    routes["起漲前兆"] = _clip(early * .30 + tech * .15 + position_score * .15 + heat_score * .16 + sector * .10 + institution * .07 + liquidity * .07)

    pullback = 90 if -4 <= ret1 <= 1.5 else 72 if -6 <= ret1 < -4 else 50
    routes["強勢回測"] = _clip(tech * .18 + entry * .20 + risk * .18 + pullback * .15 + (100 if stop <= 5.5 else 65 if stop <= 7 else 30) * .12 + min(100, rr * 55) * .10 + liquidity * .07)

    routes["基本面轉折"] = _clip(revenue * .28 + eps * .22 + official * .15 + institution * .12 + tech * .08 + early * .08 + liquidity * .07)
    routes["成長合理估值"] = _clip(revenue * .20 + eps * .20 + official * .14 + valuation * .25 + risk * .10 + tech * .06 + liquidity * .05)
    routes["法人布局"] = _clip(institution * .42 + main * .18 + sector * .10 + tech * .10 + entry * .08 + risk * .07 + liquidity * .05)
    routes["催化事件"] = _clip(catalyst * .30 + early * .16 + breakout * .16 + main * .12 + sector * .10 + institution * .08 + liquidity * .08)

    rebound_base = _first_num(row, ["恐慌反彈領漲分", "紅燈逆勢反轉分", "主流領漲回補分"], 45)
    routes["恐慌後領漲修復"] = _clip(rebound_base * .30 + tech * .16 + risk * .16 + close_pos * .12 + institution * .08 + main * .10 + liquidity * .08)

    return {k: round(_clip(v), 2) for k, v in routes.items()}


def _infer_route(row: pd.Series | dict[str, Any]) -> str:
    existing = _safe_str(row.get("AI主要召回路徑"))
    if existing:
        return existing
    routes = _recall_routes(row)
    return max(routes, key=routes.get) if routes else "綜合條件"


def _outcome_value(row: pd.Series | dict[str, Any]) -> float | None:
    for col in ["可執行交易3日%", "推薦後3日%", "3日績效%", "可執行交易1日%", "推薦後1日%", "觸發後收盤績效%", "隔日候選漲跌%"]:
        value = _safe_float(row.get(col), float("nan"))
        if not math.isnan(value) and -80 <= value <= 200:
            return value
    return None


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _plain_json_value(value: Any) -> Any:
    """Convert pandas/numpy/date objects into JSON-safe built-ins.

    Persistence backends may reject numpy scalars even though local json.dumps
    can stringify them.  Learning snapshots must preserve numeric semantics,
    so convert recursively before both local and remote writes.
    """
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        numeric = float(value)
        return None if math.isnan(numeric) or math.isinf(numeric) else numeric
    if isinstance(value, dict):
        return {str(k): _plain_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_plain_json_value(v) for v in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat(sep=" ")
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    # numpy scalar and other scalar-like objects
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _plain_json_value(item())
        except Exception:
            pass
    return str(value)


def _atomic_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    plain = _plain_json_value(payload)
    tmp.write_text(json.dumps(plain, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _prediction_probability(row: pd.Series | dict[str, Any]) -> float | None:
    for col in ["模型隔日上漲機率%", "AI隔日上漲機率%", "預估上漲機率%", "上漲機率%"]:
        value = _safe_float(row.get(col), float("nan"))
        if math.isnan(value):
            continue
        if 0 <= value <= 1:
            value *= 100.0
        if 0 <= value <= 100:
            return value / 100.0
    return None


def _error_category(row: pd.Series | dict[str, Any], outcome: float) -> str:
    text = _text_blob(row, [
        "績效判定", "觸發品質判定", "訊號失敗原因", "未觸發漏選標記",
        "進場阻擋原因", "正式推薦排除原因", "操作許可",
    ])
    triggered = not _contains(text, ["未觸發", "不可買", "禁止新倉", "等待"])
    if _contains(text, ["假突破", "跌破守價", "訊號失敗"]):
        return "假突破／守價失敗"
    if _contains(text, ["未觸發漏選", "漏選", "錯失"]):
        return "觸發過遠／漏選強勢"
    max_dd = _best_num(row, ["觸發後最大回撤%", "最大回撤%", "推薦後最大回撤%"], 0, mode="min")
    if max_dd <= -6:
        return "下行風險低估"
    if triggered and outcome <= -3:
        return "進場時機錯誤"
    if not triggered and outcome >= 3:
        return "候選正確但買點過嚴"
    if outcome < 0:
        return "方向判斷錯誤"
    return "成功／無重大錯誤"


def build_experience_profile(records: list[dict[str, Any]] | pd.DataFrame | None = None, *, base_dir: str | Path | None = None) -> dict[str, Any]:
    if records is None:
        records = _load_json(_root(base_dir) / DEFAULT_RECORD_FILE, [])
    if isinstance(records, pd.DataFrame):
        rows = records.to_dict("records")
    else:
        rows = records if isinstance(records, list) else []

    groups: dict[str, list[float]] = {}
    regimes: dict[str, list[float]] = {}
    all_outcomes: list[float] = []
    brier_values: list[float] = []
    error_taxonomy: dict[str, int] = {}
    total = 0
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        outcome = _outcome_value(raw)
        if outcome is None:
            continue
        # 僅採可信績效；若欄位不存在，保留歷史相容性但權重較低。
        eligible_text = _safe_str(raw.get("是否納入可執行績效"))
        if eligible_text and eligible_text not in {"是", "True", "true", "1", "納入"}:
            continue
        route = _infer_route(raw)
        regime = _market_regime(raw)
        groups.setdefault(route, []).append(outcome)
        regimes.setdefault(regime, []).append(outcome)
        all_outcomes.append(outcome)
        probability = _prediction_probability(raw)
        if probability is not None:
            actual = 1.0 if outcome > 0 else 0.0
            brier_values.append((probability - actual) ** 2)
        category = _error_category(raw, outcome)
        error_taxonomy[category] = error_taxonomy.get(category, 0) + 1
        total += 1

    def _stats(values: list[float]) -> dict[str, Any]:
        if not values:
            return {"count": 0, "mean": 0.0, "hit_rate": 0.0, "adjustment": 0.0}
        n = len(values)
        mean = sum(values) / n
        hit = sum(1 for v in values if v > 0) / n * 100
        # 收縮係數：樣本 5 以下幾乎不調整，30 筆以上才接近全權重。
        shrink = min(1.0, max(0.0, (n - 3) / 27))
        raw_adj = mean * 0.65 + (hit - 50) * 0.055
        adjustment = max(-5.0, min(5.0, raw_adj * shrink))
        return {"count": n, "mean": round(mean, 3), "hit_rate": round(hit, 2), "adjustment": round(adjustment, 2)}

    global_stats = _stats(all_outcomes)
    global_stats["brier_score"] = round(sum(brier_values) / len(brier_values), 4) if brier_values else None
    global_stats["probability_samples"] = len(brier_values)
    return _plain_json_value({
        "version": LEARNING_SYSTEM_VERSION,
        "built_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "eligible_samples": total,
        "global_metrics": global_stats,
        "route_stats": {k: _stats(v) for k, v in groups.items()},
        "regime_stats": {k: _stats(v) for k, v in regimes.items()},
        "error_taxonomy": dict(sorted(error_taxonomy.items(), key=lambda kv: (-kv[1], kv[0]))),
    })


def _experience_adjustment(row: pd.Series | dict[str, Any], profile: dict[str, Any]) -> tuple[float, int]:
    route = _infer_route(row)
    regime = _market_regime(row)
    route_stat = (profile.get("route_stats") or {}).get(route) or {}
    regime_stat = (profile.get("regime_stats") or {}).get(regime) or {}
    route_adj = _safe_float(route_stat.get("adjustment"), 0)
    regime_adj = _safe_float(regime_stat.get("adjustment"), 0)
    count = int(_safe_float(route_stat.get("count"), 0) + _safe_float(regime_stat.get("count"), 0))
    return round(max(-6, min(6, route_adj * .72 + regime_adj * .28)), 2), count


def _score_row(row: pd.Series | dict[str, Any], profile: dict[str, Any]) -> dict[str, Any]:
    tactical = _tactical_path_metrics(row)
    routes = _recall_routes(row)
    ordered = sorted(routes.items(), key=lambda kv: kv[1], reverse=True)
    main_route, recall = ordered[0] if ordered else ("綜合條件", 50.0)
    active_routes = [name for name, value in ordered if value >= 67][:4]
    route_text = "、".join(active_routes) if active_routes else f"{main_route}待確認"

    tech = _first_num(row, ["技術結構分數", "技術趨勢分數", "Alpha選股潛力分"], 50)
    sector = _first_num(row, ["族群攻擊強度", "類股熱度分數", "族群資金流分數"], 50)
    relative = _first_num(row, ["同類股領先幅度", "主流主升優先分", "強勢動能分"], 50)
    revenue, eps, official, valuation = _fundamental_components(row)
    institution = _institution_score(row)
    liquidity = _liquidity_score(row)
    alpha = _clip(revenue * .16 + eps * .14 + official * .10 + valuation * .13 + institution * .14 + tech * .16 + sector * .09 + relative * .08)

    entry = _first_num(row, ["Entry進場買點分", "進場買點分", "買進分數"], 50)
    early = _first_num(row, ["起漲前兆分數", "強勢前兆分"], 50)
    breakout = _first_num(row, ["強勢動能分", "型態突破分數", "爆發雷達分"], 50)
    close_pos = _first_num(row, ["當日收盤位置%"], 50)
    volume = _first_num(row, ["當日量比", "5日20日量比"], 1.0)
    chase = _first_num(row, ["追價風險分", "隔日耗竭風險分"], 50)
    heat_type = _safe_str(tactical.get("AI過熱型態"))
    if heat_type == "爆量噴出末升":
        heat_penalty = max(12.0, max(0, chase - 50) * .30 + max(0, _first_num(row, ["近5日漲幅%"], 0) - 15) * .55)
    elif heat_type == "主升整理二次買點":
        heat_penalty = max(0.0, chase - 70) * .10
    elif heat_type == "當日點火隔日只等回測":
        heat_penalty = 5.0 + max(0.0, chase - 65) * .12
    else:
        heat_penalty = max(0, chase - 60) * .18 + max(0, _first_num(row, ["近5日漲幅%"], 0) - 18) * .28
    timing = _clip(entry * .23 + early * .18 + breakout * .18 + close_pos * .11 + min(volume, 2.0) / 2.0 * 100 * .09 + recall * .12 + liquidity * .04 + (100 if tactical.get("AI戰術停損距離%", 99) <= 4.5 else 65) * .05 - heat_penalty)

    risk_base = _first_num(row, ["Risk風控安全分", "風控安全分", "交易可行分數"], 50)
    rr = _safe_float(tactical.get("AI戰術風報比"), 0)
    stop = _safe_float(tactical.get("AI戰術停損距離%"), 99)
    stop_score = 95 if 0 < stop <= 4.5 else 80 if stop <= 6 else 62 if stop <= 8 else 30
    rr_score = _clip(rr * 55)
    market = _market_regime(row)
    market_score = 0 if market == "LOCKDOWN" else 35 if market == "空頭防守" else 65 if market in {"中性震盪", "震盪輪動"} else 88
    risk = _clip(risk_base * .28 + rr_score * .22 + stop_score * .18 + (100 - chase) * .10 + liquidity * .12 + market_score * .10)

    main = _first_num(row, ["主流主升優先分", "主流資金分"], 50)
    continuation = _clip(main * .20 + sector * .15 + institution * .15 + tech * .14 + breakout * .10 + official * .10 + revenue * .08 + liquidity * .08)
    ret5 = _first_num(row, ["近5日漲幅%"], 0)
    ret20 = _first_num(row, ["近20日漲幅%"], 0)
    if tactical.get("AI過熱型態") == "爆量噴出末升":
        continuation = _clip(continuation - min(18, max(ret5 - 15, 0) * .65 + max(ret20 - 32, 0) * .22 + 8))
    elif tactical.get("AI過熱型態") == "主升整理二次買點":
        continuation = _clip(continuation + 5)
    elif ret5 > 20 or ret20 > 40:
        continuation = _clip(continuation - min(8, max(ret5 - 20, 0) * .25 + max(ret20 - 40, 0) * .10))

    data_conf = _data_confidence(row)
    experience, sample_count = _experience_adjustment({**dict(row), "AI主要召回路徑": main_route}, profile)
    # 幾何式弱點懲罰：任一核心引擎太弱時，總分不應被其他高分完全掩蓋。
    weighted = alpha * .30 + timing * .27 + risk * .23 + continuation * .20 + experience
    weak_floor = min(alpha, timing, risk, continuation)
    if weak_floor < 45:
        weighted -= (45 - weak_floor) * .22
    # 資料不足應降低信心與升格資格，但不應把真正優質股票的研究排序全面壓回50以下。
    # 只有低於55分的資料信心才扣分；正式/A-仍另有硬門檻。
    weighted -= max(0.0, 55.0 - data_conf) * 0.18
    decision = _clip(weighted)

    # Challenger 更重視新召回與基本面，僅作影子比較。
    challenger = _clip(recall * .24 + alpha * .31 + timing * .20 + risk * .13 + continuation * .12 + experience)
    rank_delta = max(-8.0, min(10.0, (decision - 60) * .16 + (recall - 65) * .08 + experience))
    repeat_count = _first_num(row, ["近5次入榜次數", "連續入榜次數"], 0)
    signal_fresh = _first_num(row, ["今日訊號新鮮分"], 50)
    repeat_decay = 0.0
    if repeat_count >= 3 and signal_fresh < 65:
        repeat_decay = min(6.0, (repeat_count - 2) * 1.5 + (65 - signal_fresh) * .06)
        rank_delta -= repeat_decay
    elif repeat_count <= 1 and recall >= 68 and signal_fresh >= 55:
        rank_delta += 2.0
    rank_delta = max(-8.0, min(10.0, rank_delta))

    hard_text = _text_blob(row, ["真禁買原因", "硬否決原因", "正式推薦排除原因", "進場阻擋原因", "實戰過濾狀態"])
    reasons = [f"{main_route}{recall:.0f}", f"Alpha {alpha:.0f}", f"Timing {timing:.0f}", f"Risk {risk:.0f}", f"延續 {continuation:.0f}"]
    oppose: list[str] = []
    if data_conf < 55:
        oppose.append(f"資料信心僅{data_conf:.0f}")
    if risk < 58:
        oppose.append(f"風控僅{risk:.0f}")
    if timing < 60:
        oppose.append(f"時機僅{timing:.0f}")
    if chase > 65:
        oppose.append(f"追價/耗竭風險{chase:.0f}")
    if hard_text:
        oppose.append(hard_text[:90])

    if market == "LOCKDOWN":
        qualification = "AI-LOCKDOWN｜禁止新倉"
    elif decision >= 78 and alpha >= 70 and timing >= 70 and risk >= 65 and continuation >= 64 and data_conf >= 65 and not hard_text:
        qualification = "AI-A｜多證據共振正式候選"
    elif decision >= 66 and alpha >= 62 and timing >= 62 and risk >= 60 and continuation >= 58 and data_conf >= 48 and not _contains(hard_text, ["低流動", "興櫃", "lockdown", "過熱禁買", "假突破"]):
        qualification = "AI-A-｜條件確認候選"
    elif alpha >= 70 and timing < 60:
        qualification = "AI-Q｜好股票等待好買點"
    elif recall >= 70:
        qualification = "AI-R1｜高召回核心雷達"
    else:
        qualification = "AI-R2｜研究雷達"

    return {
        "AI市場狀態": market,
        "AI召回路徑": route_text,
        "AI主要召回路徑": main_route,
        "AI召回分": recall,
        "AI Alpha品質分": alpha,
        "AI Timing時機分": timing,
        "AI Risk風控分": risk,
        "AI Continuation延續分": continuation,
        "AI資料信心分": data_conf,
        "AI經驗校正分": experience,
        "AI綜合決策分": decision,
        "AI排名加減分": round(rank_delta, 2),
        "AI跨市場新強股分": 0.0,
        "AI新證據分": 0.0,
        "AI召回來源數": len(active_routes),
        "AI召回保留旗標": "否",
        "AI漏選風險分": 0.0,
        "AI結構停損距離%": tactical.get("AI結構停損距離%", 99.0),
        "AI戰術停損距離%": tactical.get("AI戰術停損距離%", 99.0),
        "AI戰術風報比": tactical.get("AI戰術風報比", 0.0),
        "AI趨勢延伸目標價": tactical.get("AI趨勢延伸目標價", 0.0),
        "AI領漲延續狀態": tactical.get("AI領漲延續狀態", ""),
        "AI過熱型態": tactical.get("AI過熱型態", "正常趨勢"),
        "AI風險口徑": tactical.get("AI風險口徑", "結構停損"),
        "AI重複證據衰減分": round(repeat_decay, 2),
        "AI預測優勢": round((decision - 50) / 10, 2),
        "AI推薦資格": qualification,
        "AI推薦理由": "｜".join(reasons),
        "AI反對理由": "｜".join(oppose[:4]) if oppose else "未發現重大反對證據",
        "AI取消條件": "跌破實戰停損、觸發後無法守價、族群轉弱、資料來源衝突或大盤進入LOCKDOWN",
        "AI學習樣本數": sample_count,
        "AI模型版本": MODEL_VERSION,
        "AI Challenger分": challenger,
        "AI Champion勝出": "是" if decision >= challenger else "否｜Challenger影子領先",
    }



def _numeric_series(df: pd.DataFrame, names: Iterable[str], default: float = 0.0, *, mode: str = "first") -> pd.Series:
    series_list: list[pd.Series] = []
    for name in names:
        if name in df.columns:
            series_list.append(pd.to_numeric(df[name], errors="coerce"))
    if not series_list:
        return pd.Series([float(default)] * len(df), index=df.index, dtype="float64")
    work = pd.concat(series_list, axis=1)
    if mode == "max":
        out = work.max(axis=1, skipna=True)
    elif mode == "min":
        out = work.min(axis=1, skipna=True)
    else:
        out = work.bfill(axis=1).iloc[:, 0]
    return out.fillna(float(default)).astype(float)


def _percentile_rank(series: pd.Series, *, higher_is_better: bool = True) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.notna()
    if valid.sum() < 2 or numeric[valid].nunique() <= 1:
        return pd.Series([50.0] * len(series), index=series.index)
    ranks = numeric.rank(method="average", pct=True) * 100.0
    if not higher_is_better:
        ranks = 100.0 - ranks
    return ranks.fillna(50.0).clip(0, 100)


def _apply_cross_sectional_leader_overlay(df: pd.DataFrame) -> pd.DataFrame:
    """Use market-wide evidence to rescue emerging leaders without granting blind buy permission."""
    if df.empty:
        return df
    out = df.copy()
    missed = _numeric_series(out, ["強勢股漏選風險分", "主流領漲回補分", "隔日爆發分"], 50, mode="max")
    revenue = _numeric_series(out, ["營收成長官方分數", "營收動能代理分數", "營收動能代理"], 50, mode="max")
    eps = _numeric_series(out, ["EPS成長分數", "EPS代理分數", "EPS代理"], 50, mode="max")
    tech = _numeric_series(out, ["技術結構分數", "強勢動能分", "爆發雷達分"], 50, mode="max")
    main = _numeric_series(out, ["主流主升優先分", "主流資金分"], 50, mode="max")
    sector = _numeric_series(out, ["族群攻擊強度", "類股熱度分數"], 50, mode="max")
    breakout = _numeric_series(out, ["型態突破分數", "強勢動能分", "爆發雷達分"], 50, mode="max")
    early = _numeric_series(out, ["起漲前兆分數", "強勢前兆分", "盤前強勢前兆分"], 50, mode="max")
    ret1 = _numeric_series(out, ["今日漲幅%", "區間漲跌幅%"], 0)
    ret5 = _numeric_series(out, ["近5日漲幅%"], 0)
    volume = _numeric_series(out, ["當日量比", "5日20日量比"], 1.0, mode="max")
    close_pos = _numeric_series(out, ["當日收盤位置%"], 50)
    tactical_rr = _numeric_series(out, ["AI戰術風報比"], 0)
    tactical_stop = _numeric_series(out, ["AI戰術停損距離%"], 99)
    amount = _numeric_series(out, ["成交額百萬", "20日均成交額百萬"], 0, mode="max")

    cross = (
        _percentile_rank(missed) * .18
        + _percentile_rank(revenue) * .15
        + _percentile_rank(eps) * .08
        + _percentile_rank(tech) * .12
        + _percentile_rank(main) * .10
        + _percentile_rank(sector) * .08
        + _percentile_rank(breakout) * .10
        + _percentile_rank(tactical_rr.clip(upper=3.0)) * .10
        + _percentile_rank(close_pos) * .04
        + _percentile_rank(volume.clip(upper=3.0)) * .05
    ).clip(0, 100)
    fresh = (
        _percentile_rank(early) * .20
        + _percentile_rank(breakout) * .18
        + _percentile_rank(ret1.clip(-10, 10)) * .16
        + _percentile_rank(ret5.clip(-30, 30)) * .12
        + _percentile_rank(volume.clip(upper=3.0)) * .14
        + _percentile_rank(revenue) * .12
        + _percentile_rank(missed) * .08
    ).clip(0, 100)

    old_decision = _numeric_series(out, ["AI綜合決策分"], 0)
    old_recall = _numeric_series(out, ["AI召回分"], 0)
    old_rank_delta = _numeric_series(out, ["AI排名加減分"], 0)
    repeat_decay = _numeric_series(out, ["AI重複證據衰減分"], 0)
    lag = _numeric_series(out, ["K線落後交易日"], 999)
    heat = out.get("AI過熱型態", pd.Series([""] * len(out), index=out.index)).astype(str)
    all_text = (
        out.get("真禁買原因", pd.Series([""] * len(out), index=out.index)).astype(str)
        + "｜" + out.get("硬否決原因", pd.Series([""] * len(out), index=out.index)).astype(str)
        + "｜" + out.get("進場阻擋原因", pd.Series([""] * len(out), index=out.index)).astype(str)
        + "｜" + out.get("操作許可", pd.Series([""] * len(out), index=out.index)).astype(str)
    ).str.lower()
    absolute_block = all_text.str.contains("lockdown|全面禁買|低流動|興櫃|行情落後|k線落後|無歷史|資料待更新", regex=True, na=False) | lag.ne(0)
    blowoff = heat.eq("爆量噴出末升")
    retain = (((cross >= 74) & (fresh >= 60)) | (missed >= 82)) & (amount >= 80) & ~absolute_block

    bonus = ((cross - 68).clip(lower=0) * .11 + (fresh - 62).clip(lower=0) * .05).clip(upper=6.0)
    bonus = bonus.where(~blowoff, 0.0)
    adjusted_decision = (old_decision + bonus - repeat_decay * .25).clip(0, 100)
    adjusted_recall = pd.concat([old_recall, cross * .94], axis=1).max(axis=1).clip(0, 100)
    adjusted_rank = (old_rank_delta + bonus * .65 - repeat_decay * .20).clip(-8, 12)
    leak_risk = pd.concat([missed, cross], axis=1).max(axis=1)
    bucket = out.get("正式推薦分區", pd.Series([""] * len(out), index=out.index)).astype(str)
    leak_risk = (leak_risk + ((adjusted_decision < 60) | bucket.str.contains("排除", na=False)).astype(float) * 6).clip(0, 100)

    out["AI跨市場新強股分"] = cross.round(2)
    out["AI新證據分"] = fresh.round(2)
    out["AI漏選風險分"] = leak_risk.round(2)
    out["AI召回保留旗標"] = retain.map({True: "是", False: "否"})
    out["AI綜合決策分"] = adjusted_decision.round(2)
    out["AI召回分"] = adjusted_recall.round(2)
    out["AI排名加減分"] = adjusted_rank.round(2)
    out["AI預測優勢"] = ((adjusted_decision - 50) / 10).round(2)

    alpha = _numeric_series(out, ["AI Alpha品質分"], 0)
    timing = _numeric_series(out, ["AI Timing時機分"], 0)
    risk = _numeric_series(out, ["AI Risk風控分"], 0)
    continuation = _numeric_series(out, ["AI Continuation延續分"], 0)
    data_conf = _numeric_series(out, ["AI資料信心分"], 0)
    current_q = out.get("AI推薦資格", pd.Series([""] * len(out), index=out.index)).astype(str)
    confirmation_wait = heat.eq("當日點火隔日只等回測")
    formal_mask = retain & ~absolute_block & ~blowoff & ~confirmation_wait & (cross >= 84) & (alpha >= 72) & (timing >= 70) & (risk >= 66) & (continuation >= 66) & (data_conf >= 65) & (tactical_rr >= 1.45) & (tactical_stop <= 5.0)
    a_mask = retain & ~absolute_block & ~blowoff & ~confirmation_wait & (cross >= 76) & (alpha >= 63) & (timing >= 58) & (risk >= 58) & (continuation >= 58) & (data_conf >= 38) & (tactical_rr >= 1.20) & (tactical_stop <= 5.5)
    current_q = current_q.mask(formal_mask, "AI-A｜跨市場新強股多證據共振")
    current_q = current_q.mask(~formal_mask & a_mask, "AI-A-｜主升二次買點條件候選")
    current_q = current_q.mask(retain & confirmation_wait, "AI-R1P｜點火後只等回測確認")
    current_q = current_q.mask(retain & ~formal_mask & ~a_mask & ~blowoff & ~confirmation_wait, "AI-R1X｜跨市場新強股核心雷達")
    current_q = current_q.mask(retain & blowoff, "AI-R1B｜爆量過熱只等回測")
    out["AI推薦資格"] = current_q

    route = out.get("AI召回路徑", pd.Series([""] * len(out), index=out.index)).astype(str)
    route = route.where(~retain, route.apply(lambda x: x if "跨市場新強股" in x else ("跨市場新強股、" + x if x else "跨市場新強股")))
    out["AI召回路徑"] = route
    out.loc[retain & (cross >= out["AI召回分"] - 1), "AI主要召回路徑"] = "跨市場新強股"

    reason = out.get("AI推薦理由", pd.Series([""] * len(out), index=out.index)).astype(str)
    reason_add = "跨市場分" + cross.round(0).astype(int).astype(str) + "｜新證據" + fresh.round(0).astype(int).astype(str) + "｜戰術RR " + tactical_rr.round(2).astype(str)
    out["AI推薦理由"] = reason.where(~retain, reason + "｜" + reason_add)
    return out

def apply_daily_learning_overlay(df: pd.DataFrame | None, profile: dict[str, Any] | None = None, *, base_dir: str | Path | None = None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=LEARNING_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        for col in LEARNING_COLUMNS:
            if col not in out.columns:
                out[col] = pd.Series(dtype="float64" if col in _NUMERIC_COLUMNS else "object")
        return out
    profile = profile or build_experience_profile(base_dir=base_dir)
    scored = out.apply(lambda r: _score_row(r, profile), axis=1, result_type="expand")
    for col in LEARNING_COLUMNS:
        if col not in scored.columns:
            scored[col] = 0.0 if col in _NUMERIC_COLUMNS else ""
    out = out.drop(columns=[c for c in LEARNING_COLUMNS if c in out.columns], errors="ignore")
    combined = pd.concat([out, scored[LEARNING_COLUMNS]], axis=1)
    combined = _apply_cross_sectional_leader_overlay(combined)
    soft_count = pd.to_numeric(combined.get("前置軟篩選數", 0), errors="coerce")
    if not isinstance(soft_count, pd.Series):
        soft_count = pd.Series([float(soft_count or 0)] * len(combined), index=combined.index)
    soft_count = soft_count.fillna(0).clip(lower=0)
    combined["AI發現母體"] = "FULL-MARKET｜所有K線有效股票"
    combined["AI舊規則軟篩選數"] = soft_count.astype(float)
    retain = combined.get("AI召回保留旗標", pd.Series(["否"] * len(combined), index=combined.index)).astype(str).eq("是")
    combined["AI舊規則救回旗標"] = ((soft_count > 0) & retain).map({True: "是｜舊規則原會淘汰", False: "否"})
    return combined


def _hard_block(row: pd.Series | dict[str, Any]) -> bool:
    text = _text_blob(row, ["真禁買原因", "硬否決原因", "正式推薦排除原因", "進場阻擋原因", "實戰過濾狀態", "操作許可"])
    if _contains(text, ["lockdown", "全面禁買", "低流動性", "興櫃", "假突破", "行情落後", "k線落後", "資料待更新"]):
        return True
    # 舊版「過熱禁買」若經 Phase107 判定為健康主升整理，允許進入條件式評估；
    # 真正爆量末升仍保持硬封鎖。
    if _contains(text, ["過熱禁買", "禁止新倉"]) and _safe_str(row.get("AI過熱型態")) != "主升整理二次買點":
        return True
    return False


def apply_learning_admission(df: pd.DataFrame | None, *, max_formal: int = 3, max_a_minus: int = 3) -> pd.DataFrame:
    """只對多證據共振股票升格，不因名額硬湊。既有更嚴格結果優先保留。"""
    if df is None:
        return pd.DataFrame()
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        return out
    if "AI綜合決策分" not in out.columns:
        out = apply_daily_learning_overlay(out)

    if "AI升格來源" not in out.columns:
        out["AI升格來源"] = ""
    if "AI升格限制" not in out.columns:
        out["AI升格限制"] = ""

    bucket = out.get("正式推薦分區", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)
    existing_formal = int(bucket.eq("正式下週主推薦").sum())
    existing_a = int(bucket.eq("A-｜準主推薦小量試單").sum())

    eligible_rows: list[tuple[Any, float, str]] = []
    for idx, row in out.iterrows():
        if _hard_block(row):
            continue
        qualification = _safe_str(row.get("AI推薦資格"))
        if not qualification.startswith("AI-A"):
            continue
        lag = _first_num(row, ["K線落後交易日"], 999)
        if lag != 0:
            continue
        rr = _best_num(row, ["AI戰術風報比", "路徑風險報酬比", "實戰風險報酬比", "風險報酬比"], 0, positive=True, mode="max")
        stop = _best_num(row, ["AI戰術停損距離%", "實戰停損距離%", "停損距離_隔日%", "停損距離%"], 99, positive=True, mode="min")
        if qualification.startswith("AI-A｜") and (rr < 1.35 or stop > 6.0):
            continue
        if qualification.startswith("AI-A-｜") and (rr < 1.05 or stop > 7.2):
            continue
        eligible_rows.append((idx, _safe_float(row.get("AI綜合決策分")), qualification))

    eligible_rows.sort(key=lambda item: item[1], reverse=True)
    for idx, _, qualification in eligible_rows:
        row = out.loc[idx]
        market = _safe_str(row.get("AI市場狀態"))
        if market in {"LOCKDOWN", "空頭防守"}:
            continue
        if qualification.startswith("AI-A｜") and existing_formal < max_formal:
            out.at[idx, "正式推薦分區"] = "正式下週主推薦"
            out.at[idx, "是否正式推薦"] = "是"
            out.at[idx, "正式推薦資格"] = "A｜AI多證據共振"
            out.at[idx, "正式推薦等級"] = "A｜AI多證據共振"
            out.at[idx, "操作許可"] = "允許｜盤中觸發、守價與大盤同步後分批"
            out.at[idx, "正式推薦動作"] = "多路召回、基本面、技術時機、風控與延續性共振；只在觸發後守價成立時分批進場。"
            out.at[idx, "正式推薦排除原因"] = ""
            out.at[idx, "AI升格來源"] = MODEL_VERSION
            out.at[idx, "AI升格限制"] = "每日最多3檔；未觸發視為無交易"
            existing_formal += 1
        elif existing_a < max_a_minus:
            out.at[idx, "正式推薦分區"] = "A-｜準主推薦小量試單"
            out.at[idx, "是否正式推薦"] = "否"
            out.at[idx, "正式推薦資格"] = "A-｜AI條件確認"
            out.at[idx, "正式推薦等級"] = "A-｜AI條件確認"
            out.at[idx, "操作許可"] = "條件式1%｜觸發、守價、大盤同步"
            out.at[idx, "正式推薦動作"] = "AI多證據條件候選；最多1%試單，未觸發不交易，跌破守價立即取消。"
            out.at[idx, "正式推薦排除原因"] = ""
            out.at[idx, "AI升格來源"] = MODEL_VERSION
            out.at[idx, "AI升格限制"] = "每日最多3檔；單檔最多1%"
            existing_a += 1
    return out


def build_learning_summary(df: pd.DataFrame | None, profile: dict[str, Any] | None = None) -> dict[str, Any]:
    out = df if isinstance(df, pd.DataFrame) else pd.DataFrame(df or [])
    profile = profile or build_experience_profile()
    if out.empty:
        return {"version": LEARNING_SYSTEM_VERSION, "candidate_count": 0, "eligible_samples": profile.get("eligible_samples", 0)}
    q = out.get("AI推薦資格", pd.Series([""] * len(out), index=out.index)).astype(str)
    route = out.get("AI主要召回路徑", pd.Series([""] * len(out), index=out.index)).astype(str)
    route_counts = route.value_counts().head(8).to_dict()
    return _plain_json_value({
        "version": LEARNING_SYSTEM_VERSION,
        "model_version": MODEL_VERSION,
        "candidate_count": len(out),
        "formal_ai": int(q.str.startswith("AI-A｜").sum()),
        "a_minus_ai": int(q.str.startswith("AI-A-｜").sum()),
        "quality_wait": int(q.str.startswith("AI-Q").sum()),
        "recall_radar": int(q.str.startswith("AI-R1").sum()),
        "avg_decision": round(pd.to_numeric(out.get("AI綜合決策分", 0), errors="coerce").fillna(0).mean(), 2),
        "avg_data_confidence": round(pd.to_numeric(out.get("AI資料信心分", 0), errors="coerce").fillna(0).mean(), 2),
        "route_counts": route_counts,
        "full_market_soft_gate_rows": int((pd.to_numeric(out.get("AI舊規則軟篩選數", 0), errors="coerce").fillna(0) > 0).sum()) if "AI舊規則軟篩選數" in out.columns else 0,
        "legacy_gate_rescued_rows": int(out.get("AI舊規則救回旗標", pd.Series([""] * len(out), index=out.index)).astype(str).str.startswith("是").sum()) if len(out) else 0,
        "eligible_samples": int(profile.get("eligible_samples", 0)),
        "global_metrics": profile.get("global_metrics", {}),
        "error_taxonomy": profile.get("error_taxonomy", {}),
        "built_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })


def _condensed_records(df: pd.DataFrame | None, limit: int | None = None) -> list[dict[str, Any]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    cols = [
        "股票代號", "股票名稱", "市場別", "類別", "K線最後交易日", "本輪市場最新交易日",
        "正式推薦分區", "正式推薦資格", "操作許可", "股神推薦總排名", "可交易總排名",
        "推薦總分", "股神實戰總分", "Entry進場買點分", "Risk風控安全分", "風險報酬比",
        "實戰觸發價", "觸發後守價", "停損參考", "AI市場狀態", "AI召回路徑", "AI主要召回路徑",
        "AI召回分", "AI Alpha品質分", "AI Timing時機分", "AI Risk風控分", "AI Continuation延續分",
        "AI資料信心分", "AI經驗校正分", "AI綜合決策分", "AI跨市場新強股分", "AI新證據分",
        "AI召回來源數", "AI召回保留旗標", "AI漏選風險分", "AI結構停損距離%", "AI戰術停損距離%",
        "AI戰術風報比", "AI趨勢延伸目標價", "AI領漲延續狀態", "AI過熱型態", "AI風險口徑",
        "AI推薦資格", "AI推薦理由", "AI反對理由", "AI模型版本", "AI Challenger分", "AI Champion勝出",
        "AI發現母體", "AI舊規則軟篩選數", "AI舊規則救回旗標", "前置軟篩選狀態", "前置軟篩選階段", "前置軟篩選原因",
    ]
    use = [c for c in cols if c in df.columns]
    work = df.copy()
    if "AI綜合決策分" in work.columns:
        work = work.sort_values("AI綜合決策分", ascending=False, kind="mergesort")
    if limit:
        work = work.head(limit)
    records = work[use].where(pd.notna(work[use]), None).to_dict("records")
    for record in records:
        if "股票代號" in record:
            record["股票代號"] = _code(record["股票代號"])
    return records


def save_learning_run(
    candidates_df: pd.DataFrame | None,
    recommendations_df: pd.DataFrame | None,
    *,
    scan_report: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    base_dir: str | Path | None = None,
    persist_remote: bool = True,
) -> tuple[bool, list[str], dict[str, Any]]:
    root = _root(base_dir)
    now = datetime.now()
    run_date = now.strftime("%Y-%m-%d")
    run_id = now.strftime("%Y%m%d_%H%M%S_%f")
    profile = build_experience_profile(base_dir=root)
    candidates = apply_daily_learning_overlay(candidates_df, profile=profile, base_dir=root) if isinstance(candidates_df, pd.DataFrame) else pd.DataFrame()
    recommendations = apply_daily_learning_overlay(recommendations_df, profile=profile, base_dir=root) if isinstance(recommendations_df, pd.DataFrame) else pd.DataFrame()
    summary = build_learning_summary(candidates, profile)
    payload = {
        "schema_version": LEARNING_SYSTEM_VERSION,
        "run_id": run_id,
        "run_date": run_date,
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "model": {"champion": MODEL_VERSION, "challenger": CHALLENGER_VERSION},
        "metadata": metadata or {},
        "scan_report": scan_report or {},
        "summary": summary,
        "recommendations": _condensed_records(recommendations, limit=120),
        "candidates": _condensed_records(candidates, limit=None),
    }
    payload = _plain_json_value(payload)
    path_name = f"{LEARNING_ROOT}/runs/{run_date}/{run_id}.json"
    path = root / path_name
    _atomic_write(path, payload)
    messages = [f"本機不可變決策快照：{path_name}", f"候選 {len(payload['candidates'])}｜作戰名單 {len(payload['recommendations'])}"]
    permanent_ok = True
    if persist_remote:
        try:
            from godpick_persistence_service import save_named_json_permanent
            report = save_named_json_permanent(
                path_name,
                payload,
                firestore_doc=f"godpick_learning_run_{run_id}",
            )
            permanent_ok = bool(getattr(report, "permanent_ok", False))
            messages.extend([
                f"永久保存：{'成功' if permanent_ok else '待確認'}",
                _safe_str(getattr(report, "firestore_message", "")),
                _safe_str(getattr(report, "github_message", "")),
            ])
        except Exception as exc:
            permanent_ok = False
            messages.append(f"遠端永久保存例外：{exc}")

    state = {
        "version": LEARNING_SYSTEM_VERSION,
        "model_version": MODEL_VERSION,
        "last_run_id": run_id,
        "last_run_date": run_date,
        "last_run_path": path_name,
        "last_run_summary": summary,
        "experience_profile": profile,
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
    }
    state = _plain_json_value(state)
    _atomic_write(root / LEARNING_STATE_FILE, state)
    if persist_remote:
        try:
            from godpick_persistence_service import save_named_json_permanent
            state_report = save_named_json_permanent(LEARNING_STATE_FILE, state, firestore_doc="godpick_learning_state")
            messages.append(f"學習狀態永久保存：{'成功' if getattr(state_report, 'permanent_ok', False) else '待確認'}")
        except Exception as exc:
            messages.append(f"學習狀態遠端保存例外：{exc}")
    return bool(permanent_ok), [m for m in messages if m], state


def load_learning_state(*, base_dir: str | Path | None = None) -> dict[str, Any]:
    root = _root(base_dir)
    try:
        from godpick_persistence_service import load_named_json_permanent
        payload, _ = load_named_json_permanent(LEARNING_STATE_FILE, {}, firestore_doc="godpick_learning_state")
        if isinstance(payload, dict) and payload:
            return payload
    except Exception:
        pass
    return _load_json(root / LEARNING_STATE_FILE, {})


def refresh_learning_state_from_records(*, base_dir: str | Path | None = None, persist_remote: bool = True) -> tuple[dict[str, Any], list[str]]:
    root = _root(base_dir)
    profile = build_experience_profile(base_dir=root)
    state = load_learning_state(base_dir=root)
    state.update({
        "version": LEARNING_SYSTEM_VERSION,
        "model_version": MODEL_VERSION,
        "experience_profile": profile,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    state = _plain_json_value(state)
    _atomic_write(root / LEARNING_STATE_FILE, state)
    messages = [f"可用學習樣本：{profile.get('eligible_samples', 0)}"]
    if persist_remote:
        try:
            from godpick_persistence_service import save_named_json_permanent
            report = save_named_json_permanent(LEARNING_STATE_FILE, state, firestore_doc="godpick_learning_state")
            messages.append(f"學習狀態永久保存：{'成功' if getattr(report, 'permanent_ok', False) else '待確認'}")
        except Exception as exc:
            messages.append(f"學習狀態永久保存例外：{exc}")
    return state, messages


__all__ = [
    "LEARNING_SYSTEM_VERSION", "MODEL_VERSION", "CHALLENGER_VERSION", "LEARNING_COLUMNS",
    "apply_daily_learning_overlay", "apply_learning_admission", "build_experience_profile",
    "build_learning_summary", "save_learning_run", "load_learning_state", "refresh_learning_state_from_records",
]

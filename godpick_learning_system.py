# -*- coding: utf-8 -*-
"""Phase 105｜每日學習型股神決策核心。

設計原則：
1. 多路召回，不讓大型熱門股壟斷候選。
2. Alpha / Timing / Risk / Continuation 四引擎分離。
3. 只以已發生的績效紀錄做小幅、收縮後的經驗校正，避免單日過擬合。
4. 每次完整掃描保存不可變決策快照；正式推薦為 0 也必須記錄。
5. Champion / Challenger 欄位先建立，Phase 105 只讓 Champion 影響正式結果。
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

LEARNING_SYSTEM_VERSION = "phase105_daily_learning_ai_v1_20260805"
MODEL_VERSION = "champion_phase105_v1"
CHALLENGER_VERSION = "challenger_phase105_recall_v1"
LEARNING_STATE_FILE = "godpick_learning_state.json"
LEARNING_ROOT = "data/godpick_learning"
DEFAULT_RECORD_FILE = "godpick_records.json"

LEARNING_COLUMNS = [
    "AI市場狀態", "AI召回路徑", "AI主要召回路徑", "AI召回分",
    "AI Alpha品質分", "AI Timing時機分", "AI Risk風控分", "AI Continuation延續分",
    "AI資料信心分", "AI經驗校正分", "AI綜合決策分", "AI排名加減分",
    "AI預測優勢", "AI推薦資格", "AI推薦理由", "AI反對理由", "AI取消條件",
    "AI學習樣本數", "AI模型版本", "AI Challenger分", "AI Champion勝出",
]

_NUMERIC_COLUMNS = {
    "AI召回分", "AI Alpha品質分", "AI Timing時機分", "AI Risk風控分",
    "AI Continuation延續分", "AI資料信心分", "AI經驗校正分",
    "AI綜合決策分", "AI排名加減分", "AI學習樣本數", "AI Challenger分",
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
    heat_penalty = max(0, chase - 55) * .25 + max(0, _first_num(row, ["近5日漲幅%"], 0) - 12) * .6
    timing = _clip(entry * .25 + early * .18 + breakout * .18 + close_pos * .12 + min(volume, 2.0) / 2.0 * 100 * .10 + recall * .12 + liquidity * .05 - heat_penalty)

    risk_base = _first_num(row, ["Risk風控安全分", "風控安全分", "交易可行分數"], 50)
    rr = _best_num(row, ["路徑風險報酬比", "實戰風險報酬比", "風險報酬比"], 0, positive=True, mode="max")
    stop = _best_num(row, ["實戰停損距離%", "停損距離_隔日%", "停損距離%"], 99, positive=True, mode="min")
    stop_score = 95 if 0 < stop <= 4.5 else 80 if stop <= 6 else 62 if stop <= 8 else 30
    rr_score = _clip(rr * 55)
    market = _market_regime(row)
    market_score = 0 if market == "LOCKDOWN" else 35 if market == "空頭防守" else 65 if market in {"中性震盪", "震盪輪動"} else 88
    risk = _clip(risk_base * .28 + rr_score * .22 + stop_score * .18 + (100 - chase) * .10 + liquidity * .12 + market_score * .10)

    main = _first_num(row, ["主流主升優先分", "主流資金分"], 50)
    continuation = _clip(main * .20 + sector * .15 + institution * .15 + tech * .14 + breakout * .10 + official * .10 + revenue * .08 + liquidity * .08)
    ret5 = _first_num(row, ["近5日漲幅%"], 0)
    ret20 = _first_num(row, ["近20日漲幅%"], 0)
    if ret5 > 15 or ret20 > 32:
        continuation = _clip(continuation - min(16, max(ret5 - 15, 0) * .7 + max(ret20 - 32, 0) * .25))

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
    if repeat_count >= 3 and signal_fresh < 65:
        rank_delta -= min(6.0, (repeat_count - 2) * 1.5 + (65 - signal_fresh) * .06)
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
    return pd.concat([out, scored[LEARNING_COLUMNS]], axis=1)


def _hard_block(row: pd.Series | dict[str, Any]) -> bool:
    text = _text_blob(row, ["真禁買原因", "硬否決原因", "正式推薦排除原因", "進場阻擋原因", "實戰過濾狀態", "操作許可"])
    return _contains(text, ["lockdown", "全面禁買", "低流動性", "興櫃", "假突破", "過熱禁買", "行情落後", "k線落後", "禁止新倉"])


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
        rr = _best_num(row, ["路徑風險報酬比", "實戰風險報酬比", "風險報酬比"], 0, positive=True, mode="max")
        stop = _best_num(row, ["實戰停損距離%", "停損距離_隔日%", "停損距離%"], 99, positive=True, mode="min")
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
        "AI資料信心分", "AI經驗校正分", "AI綜合決策分", "AI推薦資格", "AI推薦理由", "AI反對理由",
        "AI模型版本", "AI Challenger分", "AI Champion勝出",
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

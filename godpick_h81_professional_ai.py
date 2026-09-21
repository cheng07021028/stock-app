# -*- coding: utf-8 -*-
"""H81 professional research layer for GodPick.

This layer implements seven user-requested research capabilities:
1) what the market is pricing / fundamental + valuation framing,
2) daily + weekly technical interpretation,
3) news -> market-impact translation,
4) backtest / robustness review with anti-overfit checks,
5) portfolio concentration/correlation stress testing,
6) trade-journal behavioural review,
7) timestamped daily trading-plan checklist.

Governance: H81 can refine *research ranking* only.  It never creates H64/H68
Formal authority, never fabricates a price plan, and never bypasses liquidity,
freshness, overheat, stop-distance or cost-after-RR gates.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable
import math
import re

import pandas as pd

VERSION = "v191_h81_professional_research_governance_20260921"

POSITIVE_NEWS = (
    "上修", "成長", "優於預期", "超預期", "新訂單", "得標", "擴產", "量產", "認證",
    "回購", "庫藏股", "毛利率提升", "獲利創高", "營收創高", "市占提升", "降息",
)
NEGATIVE_NEWS = (
    "下修", "衰退", "低於預期", "虧損", "減產", "停工", "延後", "召回", "訴訟",
    "罰款", "增資", "違約", "財測下修", "毛利率下降", "需求疲弱", "升息", "制裁",
)


def _text(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _num(v: Any, default: float | None = None) -> float | None:
    if v is None:
        return default
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    if isinstance(v, str):
        v = v.replace(",", "").replace("％", "%").replace("%", "").strip()
        if not v or v.lower() in {"none", "nan", "null", "--", "-"}:
            return default
    try:
        x = float(v)
    except Exception:
        return default
    return x if math.isfinite(x) else default


def _clip(x: Any, low: float = 0.0, high: float = 100.0, default: float = 50.0) -> float:
    v = _num(x, default)
    if v is None:
        v = default
    return max(low, min(high, float(v)))


def _first_num(row: dict[str, Any], names: Iterable[str]) -> float | None:
    for name in names:
        x = _num(row.get(name), None)
        if x is not None:
            return x
    return None


def _first_text(row: dict[str, Any], names: Iterable[str]) -> str:
    for name in names:
        s = _text(row.get(name))
        if s:
            return s
    return ""


def _score_from_growth(value: float | None, neutral: float = 50.0) -> float:
    if value is None:
        return neutral
    if value >= 30:
        return 90
    if value >= 15:
        return 78
    if value >= 5:
        return 65
    if value >= -5:
        return 52
    if value >= -15:
        return 38
    return 22


def _score_from_rr(rr: float | None) -> float:
    if rr is None:
        return 50
    if rr >= 2.5:
        return 90
    if rr >= 2.0:
        return 82
    if rr >= 1.5:
        return 70
    if rr >= 1.2:
        return 55
    return 30


def _score_market_pricing(row: dict[str, Any]) -> tuple[float, float, list[str], list[str], list[str]]:
    revenue = _first_num(row, ["月營收YoY%", "累計營收YoY%", "營收成長率%"])
    eps = _first_num(row, ["EPS成長率%", "EPS成長%", "EPS成長分數"])
    valuation = _first_num(row, ["官方估值風險分數", "估值風險分數"])
    per = _first_num(row, ["PER本益比", "本益比", "PER"])
    inst = _first_num(row, ["法人籌碼官方分數", "法人籌碼分數", "H64法人分"])
    sector = _first_num(row, ["H53族群共振分", "族群資金流分數", "類股熱度分數"])
    ret5 = _first_num(row, ["近5日漲幅%", "5日漲幅%"])

    parts: list[tuple[float, float]] = []
    catalysts: list[str] = []
    risks: list[str] = []
    watch: list[str] = []
    if revenue is not None:
        parts.append((_score_from_growth(revenue), .24))
        (catalysts if revenue >= 10 else risks if revenue < -5 else watch).append(f"營收YoY {revenue:.1f}%")
    if eps is not None:
        # EPS can already be a 0-100 score in the system.  Detect scale conservatively.
        eps_score = _clip(eps) if 0 <= eps <= 100 and abs(eps) > 40 else _score_from_growth(eps)
        parts.append((eps_score, .22))
        (catalysts if eps_score >= 65 else risks if eps_score < 40 else watch).append(f"EPS/獲利成長訊號 {eps:.1f}")
    if valuation is not None:
        # Existing column is a risk score: higher risk means lower research score.
        parts.append((100 - _clip(valuation), .18))
        if valuation >= 70:
            risks.append("估值風險偏高")
        elif valuation <= 35:
            catalysts.append("估值壓力相對低")
    elif per is not None:
        val_score = 75 if 0 < per <= 15 else 62 if per <= 25 else 48 if per <= 40 else 30
        parts.append((val_score, .14))
        if per > 40:
            risks.append(f"本益比偏高 {per:.1f}x")
        else:
            watch.append(f"本益比 {per:.1f}x")
    if inst is not None:
        parts.append((_clip(inst), .18))
        if inst >= 70:
            catalysts.append("法人籌碼偏正向")
        elif inst <= 35:
            risks.append("法人籌碼偏弱")
    if sector is not None:
        parts.append((_clip(sector), .18))
        if sector >= 70:
            catalysts.append("族群共振偏強")
        elif sector < 40:
            risks.append("族群共振偏弱")

    weight = sum(w for _, w in parts)
    score = sum(v * w for v, w in parts) / weight if weight else 50.0
    # "Priced-in" proxy: strong fundamentals + already extended short-term price lowers surprise potential.
    if ret5 is not None:
        if ret5 >= 15:
            score -= 8
            risks.append("近5日已大幅上漲，部分利多可能已反映")
        elif ret5 <= 2 and revenue is not None and revenue >= 10:
            score += 5
            catalysts.append("基本面改善但短線股價尚未明顯反映")
    coverage = min(1.0, len(parts) / 5.0)
    if not parts:
        watch.append("基本面/估值資料不足，維持中性而非臆測")
    return _clip(score), coverage, catalysts[:3], risks[:3], watch[:3]


def _score_technical(row: dict[str, Any]) -> tuple[float, float, list[str], list[str], list[str]]:
    strength = _first_num(row, ["H47個股相對強度分", "技術結構分數", "技術趨勢分數"])
    gap20 = _first_num(row, ["收盤距MA20%", "MA20乖離%"])
    gap60 = _first_num(row, ["收盤距MA60%", "MA60乖離%"])
    vol = _first_num(row, ["當日量比", "均量比"])
    accel = _first_num(row, ["3日動能加速度百分點", "動能翻多分"])
    breakout = _first_num(row, ["突破準備分", "型態突破分數"])
    rsi = _first_num(row, ["RSI14", "RSI"])
    parts: list[tuple[float, float]] = []
    bull: list[str] = []
    bear: list[str] = []
    watch: list[str] = []
    if strength is not None:
        parts.append((_clip(strength), .30))
        (bull if strength >= 65 else bear if strength < 40 else watch).append(f"相對/技術強度 {strength:.1f}")
    if gap20 is not None:
        if -3 <= gap20 <= 8:
            s = 76
        elif 8 < gap20 <= 12:
            s = 60
        elif gap20 > 12:
            s = 35
        elif -8 <= gap20 < -3:
            s = 48
        else:
            s = 28
        parts.append((s, .16))
        if gap20 > 12:
            bear.append(f"MA20乖離過熱 {gap20:.1f}%")
        elif -3 <= gap20 <= 8:
            bull.append("價格相對MA20位置健康")
        else:
            watch.append(f"MA20乖離 {gap20:.1f}%")
    if gap60 is not None:
        parts.append((72 if gap60 >= 0 else 38, .10))
        (bull if gap60 >= 0 else bear).append("週期趨勢在MA60之上" if gap60 >= 0 else "價格低於MA60")
    if vol is not None:
        s = 82 if 1.2 <= vol <= 2.5 else 68 if 1.0 <= vol < 1.2 else 45 if vol < 1.0 else 58
        parts.append((s, .15))
        (bull if vol >= 1.2 else watch).append(f"量比 {vol:.2f}")
    if accel is not None:
        s = _clip(50 + accel * 6 if abs(accel) <= 10 else accel)
        parts.append((s, .12))
        (bull if s >= 65 else bear if s < 40 else watch).append("動能改善" if s >= 65 else "動能轉弱" if s < 40 else "動能中性")
    if breakout is not None:
        parts.append((_clip(breakout), .12))
        (bull if breakout >= 65 else watch).append(f"突破準備 {breakout:.1f}")
    if rsi is not None:
        s = 72 if 50 <= rsi <= 70 else 58 if 40 <= rsi < 50 else 40 if rsi > 75 else 35
        parts.append((s, .05))
        if rsi > 75:
            bear.append(f"RSI過熱 {rsi:.1f}")
    weight = sum(w for _, w in parts)
    score = sum(v * w for v, w in parts) / weight if weight else 50.0
    coverage = min(1.0, len(parts) / 6.0)
    if not parts:
        watch.append("技術欄位不足，維持中性")
    return _clip(score), coverage, bull[:3], bear[:3], watch[:3]


def _score_news(row: dict[str, Any]) -> tuple[float, float, list[str], list[str], list[str]]:
    score_col = _first_num(row, ["新聞情緒分數", "事件影響分數", "新聞影響分數", "催化分數"])
    texts = []
    for key in ["最新新聞", "重大新聞", "新聞摘要", "催化因素", "事件摘要", "重大事件"]:
        s = _text(row.get(key))
        if s:
            texts.append(s)
    joined = "｜".join(texts)
    h83_news_source = ""
    if not joined:
        try:
            from godpick_h83_autofresh import get_news_context
            items = get_news_context(row, max_items=8)
            titles = [_text(x.get("title")) for x in items if isinstance(x, dict) and _text(x.get("title"))]
            if titles:
                joined = "｜".join(titles)
                h83_news_source = "H83自動新聞快取"
        except Exception:
            pass
    pos = [kw for kw in POSITIVE_NEWS if kw in joined]
    neg = [kw for kw in NEGATIVE_NEWS if kw in joined]
    if score_col is not None:
        score = _clip(score_col)
        coverage = .85
    elif joined:
        score = _clip(50 + 7 * len(pos) - 8 * len(neg))
        coverage = .65
    else:
        score = 50.0
        coverage = 0.0
    catalysts = ([f"新聞正向關鍵詞：{','.join(pos[:3])}"] if pos else [])
    risks = ([f"新聞負向關鍵詞：{','.join(neg[:3])}"] if neg else [])
    watch = [] if joined else ["本輪沒有可驗證個股新聞欄位；未知新聞不自行猜測"]
    if h83_news_source:
        watch.append("新聞來源：H83自動更新RSS快取；僅作研究事件因子，不單獨形成Formal權限")
    return score, coverage, catalysts, risks, watch


def _score_backtest(row: dict[str, Any], minimum_trades: int = 20) -> tuple[float, float, list[str], list[str], list[str]]:
    n = _first_num(row, ["績效樣本數", "H80績效樣本數"])
    corr = _first_num(row, ["績效校正分", "Feedback績效校正分", "H80績效校正原始分"])
    win = _first_num(row, ["歷史勝率%", "勝率%"])
    if n is None or n < minimum_trades:
        note = f"成熟績效樣本不足（{int(n or 0)}/{minimum_trades}），不讓小樣本主導排名"
        return 50.0, min(0.4, (n or 0) / max(minimum_trades, 1) * 0.4), [], [], [note]
    score = 50.0
    if corr is not None:
        score += max(-20, min(20, corr * 3.0))
    if win is not None:
        score += max(-10, min(10, (win - 50) * .5))
    pos = [f"成熟績效樣本 {int(n)} 筆"]
    neg = []
    if corr is not None and corr < -1.5:
        neg.append(f"歷史績效校正偏弱 {corr:+.1f}")
    if corr is not None and corr > 1.5:
        pos.append(f"歷史績效校正偏正向 {corr:+.1f}")
    return _clip(score), .90, pos[:3], neg[:3], []


def _score_risk(row: dict[str, Any]) -> tuple[float, float, list[str], list[str], list[str]]:
    rr = _first_num(row, ["H79成本後RR", "風險報酬比_決策", "風險報酬比"])
    stop = _first_num(row, ["H79停損距離%", "停損距離%"])
    chase = _first_num(row, ["追價風險分", "追高風險分數_決策"])
    vol = _first_num(row, ["20日波動率%", "波動率%"])
    parts: list[tuple[float, float]] = []
    pos: list[str] = []
    neg: list[str] = []
    watch: list[str] = []
    if rr is not None:
        s = _score_from_rr(rr)
        parts.append((s, .42))
        (pos if rr >= 1.5 else neg).append(f"成本後RR {rr:.2f}")
    if stop is not None:
        s = 82 if 0 < stop <= 5 else 68 if stop <= 8 else 35
        parts.append((s, .28))
        (pos if stop <= 8 else neg).append(f"停損距離 {stop:.1f}%")
    if chase is not None:
        s = 100 - _clip(chase)
        parts.append((s, .20))
        if chase >= 75:
            neg.append(f"追價風險高 {chase:.0f}")
    if vol is not None:
        s = 75 if vol <= 3 else 62 if vol <= 5 else 45 if vol <= 8 else 30
        parts.append((s, .10))
        if vol > 8:
            neg.append(f"波動率偏高 {vol:.1f}%")
    weight = sum(w for _, w in parts)
    score = sum(v * w for v, w in parts) / weight if weight else 50.0
    coverage = min(1.0, len(parts) / 4.0)
    if not parts:
        watch.append("風險/成本後RR欄位不足")
    return _clip(score), coverage, pos[:3], neg[:3], watch[:3]


def _score_journal(row: dict[str, Any], minimum_sample: int = 10) -> tuple[float, float, list[str], list[str], list[str]]:
    n = _first_num(row, ["績效樣本數", "H80績效樣本數"])
    corr = _first_num(row, ["績效校正分", "Feedback績效校正分"])
    if n is None or n < minimum_sample:
        return 50.0, .15 if n else 0.0, [], [], ["個人/歷史交易樣本不足，不產生過度個人化規則"]
    score = _clip(50 + max(-15, min(15, (corr or 0) * 2.5)))
    pos = [f"可用歷史樣本 {int(n)} 筆"]
    neg = [f"歷史回饋偏弱 {corr:+.1f}"] if corr is not None and corr < -1 else []
    return score, .75, pos, neg, []


def _score_execution(row: dict[str, Any]) -> tuple[float, float, list[str], list[str], list[str]]:
    entry = _first_num(row, ["H79計畫進場", "主要進場參考價", "實戰觸發價"])
    stop = _first_num(row, ["H79結構停損", "停損參考", "SuperAI動態停損價"])
    target = _first_num(row, ["H79第一目標", "第一壓力價", "SuperAI第一減碼價"])
    rr = _first_num(row, ["H79成本後RR", "風險報酬比_決策", "風險報酬比"])
    status = _first_text(row, ["H79計畫狀態", "H68次日執行狀態", "隔日建議動作"])
    complete = sum(v is not None and v > 0 for v in [entry, stop, target])
    score = 35 + complete * 15 + (_score_from_rr(rr) - 50) * .35
    if status.startswith("PASS") or status.startswith("READY"):
        score += 8
    pos = []
    neg = []
    watch = []
    if complete == 3:
        pos.append("進場/停損/目標完整")
    else:
        neg.append("交易計畫價格欄位不完整")
    if rr is not None:
        (pos if rr >= 1.5 else neg).append(f"RR {rr:.2f}")
    if not status:
        watch.append("缺執行狀態")
    return _clip(score), min(1.0, (complete + (1 if rr is not None else 0) + (1 if status else 0)) / 5.0), pos[:3], neg[:3], watch[:3]


def _settings_or_default(settings: dict[str, Any] | None) -> dict[str, Any]:
    if isinstance(settings, dict):
        try:
            from godpick_h81_professional_settings import normalize_settings
            return normalize_settings(settings)
        except Exception:
            return settings
    try:
        from godpick_h81_professional_settings import load_settings_safe
        return load_settings_safe()
    except Exception:
        from godpick_h81_professional_settings import DEFAULT_SETTINGS, normalize_settings
        return normalize_settings(DEFAULT_SETTINGS)


def analyze_candidate(row: dict[str, Any] | pd.Series, settings: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
    cfg = _settings_or_default(settings)
    min_bt = int(cfg.get("backtest", {}).get("minimum_trades", 20) or 20)
    min_journal = int(cfg.get("trade_journal", {}).get("minimum_personalized_sample", 10) or 10)
    component_fns = {
        "market_pricing": lambda: _score_market_pricing(raw),
        "technical": lambda: _score_technical(raw),
        "news": lambda: _score_news(raw),
        "backtest": lambda: _score_backtest(raw, min_bt),
        "portfolio_risk": lambda: _score_risk(raw),
        "journal": lambda: _score_journal(raw, min_journal),
        "execution_plan": lambda: _score_execution(raw),
    }
    results = {k: fn() for k, fn in component_fns.items()}
    features = cfg.get("features", {}) if isinstance(cfg.get("features", {}), dict) else {}
    weights = cfg.get("ranking_overlay", {}).get("weights", {})
    weighted, weight_sum = 0.0, 0.0
    coverage_sum, coverage_weight = 0.0, 0.0
    positives, negatives, watches = [], [], []
    for key, (score, coverage, pos, neg, watch) in results.items():
        w = max(0.0, float(weights.get(key, 0) or 0)) if bool(features.get(key, True)) else 0.0
        weighted += score * w
        weight_sum += w
        coverage_sum += coverage * w
        coverage_weight += w
        positives.extend(pos)
        negatives.extend(neg)
        watches.extend(watch)
    total = weighted / weight_sum if weight_sum else 50.0
    coverage = coverage_sum / coverage_weight if coverage_weight else 0.0
    overlay_cfg = cfg.get("ranking_overlay", {})
    max_points = float(overlay_cfg.get("max_abs_points", 2.5) or 0)
    min_coverage = float(overlay_cfg.get("min_data_coverage", .45) or .45)
    if not bool(cfg.get("enabled", True)) or not bool(overlay_cfg.get("enabled", True)) or coverage < min_coverage:
        adjustment = 0.0
    else:
        adjustment = max(-max_points, min(max_points, (total - 50.0) / 50.0 * max_points))
    def uniq(items: list[str], n: int = 3) -> list[str]:
        out = []
        for item in items:
            s = _text(item)
            if s and s not in out:
                out.append(s)
            if len(out) >= n:
                break
        return out
    bullish = uniq(positives, 5)
    bearish = uniq(negatives, 5)
    follow = uniq(watches, 5)
    if not bullish:
        bullish = ["目前沒有足夠可驗證資料形成額外利多假設"]
    if not bearish:
        bearish = ["目前沒有足夠可驗證資料形成額外風險假設"]
    if not follow:
        follow = ["等待下一個可驗證的價格、基本面、新聞或績效樣本"]
    plan_rows = build_daily_trading_plan(raw, cfg)
    plan_map = {}
    if isinstance(plan_rows, pd.DataFrame) and not plan_rows.empty and "階段" in plan_rows.columns:
        for _, _prow in plan_rows.iterrows():
            plan_map[_text(_prow.get("階段"))] = f"{_text(_prow.get('時間'))}｜{_text(_prow.get('檢查'))}｜{_text(_prow.get('成立才做'))}"
    return {
        "H81版本": VERSION,
        "H81市場定價理解分": round(results["market_pricing"][0], 2),
        "H81技術多週期分": round(results["technical"][0], 2),
        "H81新聞事件影響分": round(results["news"][0], 2),
        "H81歷史回測可信分": round(results["backtest"][0], 2),
        "H81風險管理分": round(results["portfolio_risk"][0], 2),
        "H81交易日誌回饋分": round(results["journal"][0], 2),
        "H81交易計畫完整分": round(results["execution_plan"][0], 2),
        "H81專業研究總分": round(_clip(total), 2),
        "H81資料覆蓋%": round(coverage * 100.0, 2),
        "H81排名加減分": round(adjustment, 2),
        "H81三大利多催化": "；".join(bullish[:3]),
        "H81三大風險": "；".join(bearish[:3]),
        "H81下一步關注": "；".join(follow[:3]),
        "H81多頭情境": "利多/技術/資金證據持續改善，且價格計畫與成本後RR仍成立時才提高研究信心。",
        "H81中性情境": "證據互相抵銷或資料覆蓋不足時維持觀察，不把中性解讀成買進。",
        "H81空頭情境": "趨勢轉弱、關鍵支撐失守、負面事件擴大或風險報酬惡化時降低研究順位。",
        "H81研究摘要": f"專業研究分={total:.1f}，資料覆蓋={coverage*100:.0f}%，研究排序調整={adjustment:+.2f}。不改變Formal授權。",
        "H81盤前檢查": plan_map.get("盤前", ""),
        "H81開盤策略": plan_map.get("開盤", ""),
        "H81盤中調整": plan_map.get("盤中", ""),
        "H81收盤檢討": plan_map.get("收盤", ""),
        "H81設定版本": _text(cfg.get("version")) or "default",
    }


H81_COLUMNS = [
    "H81版本", "H81市場定價理解分", "H81技術多週期分", "H81新聞事件影響分",
    "H81歷史回測可信分", "H81風險管理分", "H81交易日誌回饋分", "H81交易計畫完整分",
    "H81專業研究總分", "H81資料覆蓋%", "H81排名加減分", "H81三大利多催化",
    "H81三大風險", "H81下一步關注", "H81多頭情境", "H81中性情境", "H81空頭情境",
    "H81研究摘要", "H81盤前檢查", "H81開盤策略", "H81盤中調整", "H81收盤檢討", "H81設定版本",
]


def apply_professional_research_overlay(frame: pd.DataFrame | None, settings: dict[str, Any] | None = None) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame(frame)
    if frame.empty:
        out = frame.copy()
        for col in H81_COLUMNS:
            if col not in out.columns:
                out[col] = pd.Series(dtype="object")
        return out
    out = frame.copy(deep=True)
    analyses = [analyze_candidate(row, settings=settings) for _, row in out.iterrows()]
    analysis_df = pd.DataFrame(analyses, index=out.index)
    for col in analysis_df.columns:
        out[col] = analysis_df[col]
    return out


def analyze_price_history(history: pd.DataFrame | None) -> dict[str, Any]:
    """Day/week chart interpretation from a normalized OHLCV frame.

    Expected columns may be Chinese (日期/收盤價/最高價/最低價/成交量) or common
    English aliases.  The function describes scenarios rather than treating any
    setup as inevitable.
    """
    if history is None or not isinstance(history, pd.DataFrame) or history.empty:
        return {"available": False, "message": "沒有可分析的歷史K線資料"}
    df = history.copy()
    aliases = {
        "date": ["日期", "date", "Date"],
        "close": ["收盤價", "close", "Close"],
        "high": ["最高價", "high", "High"],
        "low": ["最低價", "low", "Low"],
        "volume": ["成交量", "volume", "Volume"],
    }
    def pick(names):
        for name in names:
            if name in df.columns:
                return name
        return None
    c_date, c_close, c_high, c_low, c_vol = [pick(aliases[k]) for k in ["date", "close", "high", "low", "volume"]]
    if c_close is None:
        return {"available": False, "message": "歷史K線缺收盤價"}
    if c_date:
        df[c_date] = pd.to_datetime(df[c_date], errors="coerce")
        df = df.sort_values(c_date)
    close = pd.to_numeric(df[c_close], errors="coerce").dropna()
    if len(close) < 20:
        return {"available": False, "message": f"有效日線只有{len(close)}根，至少需要20根"}
    ma5, ma20 = close.rolling(5).mean(), close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    ema12, ema26 = close.ewm(span=12, adjust=False).mean(), close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    latest = float(close.iloc[-1])
    support_candidates = [x for x in [ma5.iloc[-1], ma20.iloc[-1], ma60.iloc[-1], close.tail(20).min()] if pd.notna(x)]
    resistance_candidates = [x for x in [close.tail(20).max(), close.tail(60).max() if len(close) >= 60 else None] if x is not None and pd.notna(x)]
    support = max([float(x) for x in support_candidates if float(x) < latest], default=min(float(x) for x in support_candidates))
    resistance = min([float(x) for x in resistance_candidates if float(x) > latest], default=max(float(x) for x in resistance_candidates))
    vol_ratio = None
    if c_vol:
        vol = pd.to_numeric(df.loc[close.index, c_vol], errors="coerce")
        if vol.notna().sum() >= 20 and float(vol.tail(20).mean() or 0) > 0:
            vol_ratio = float(vol.iloc[-1] / vol.tail(20).mean())
    trend = "多頭" if latest > ma20.iloc[-1] and (pd.isna(ma60.iloc[-1]) or ma20.iloc[-1] > ma60.iloc[-1]) else "空頭" if latest < ma20.iloc[-1] and (pd.isna(ma60.iloc[-1]) or ma20.iloc[-1] < ma60.iloc[-1]) else "中性"
    weekly_trend, weekly_ma5, weekly_ma20, weekly_rsi = "資料不足", None, None, None
    if c_date:
        wk = pd.DataFrame({"date": df.loc[close.index, c_date], "close": close.values}).dropna().set_index("date").sort_index()
        wk_close = wk["close"].resample("W-FRI").last().dropna()
        if len(wk_close) >= 5:
            wma5 = wk_close.rolling(5).mean()
            wma20 = wk_close.rolling(20).mean()
            wdelta = wk_close.diff()
            wgain = wdelta.clip(lower=0).rolling(14).mean()
            wloss = (-wdelta.clip(upper=0)).rolling(14).mean()
            wrs = wgain / wloss.replace(0, pd.NA)
            wrsi = 100 - (100 / (1 + wrs))
            weekly_ma5 = round(float(wma5.iloc[-1]), 4) if pd.notna(wma5.iloc[-1]) else None
            weekly_ma20 = round(float(wma20.iloc[-1]), 4) if pd.notna(wma20.iloc[-1]) else None
            weekly_rsi = round(float(wrsi.iloc[-1]), 2) if pd.notna(wrsi.iloc[-1]) else None
            wc = float(wk_close.iloc[-1])
            if weekly_ma5 is not None and (weekly_ma20 is None or weekly_ma5 > weekly_ma20) and wc >= weekly_ma5:
                weekly_trend = "多頭"
            elif weekly_ma5 is not None and (weekly_ma20 is None or weekly_ma5 < weekly_ma20) and wc < weekly_ma5:
                weekly_trend = "空頭"
            else:
                weekly_trend = "中性"
    composite_signal = "偏多" if trend == "多頭" and weekly_trend in {"多頭", "資料不足"} and float(macd.iloc[-1]) >= float(signal.iloc[-1]) else "偏空" if trend == "空頭" and weekly_trend in {"空頭", "資料不足"} else "中性"
    return {
        "available": True,
        "bars": int(len(close)),
        "latest_close": round(latest, 4),
        "trend": trend,
        "weekly_trend": weekly_trend,
        "technical_signal": composite_signal,
        "weekly_ma5": weekly_ma5,
        "weekly_ma20": weekly_ma20,
        "weekly_rsi14": weekly_rsi,
        "ma5": round(float(ma5.iloc[-1]), 4) if pd.notna(ma5.iloc[-1]) else None,
        "ma20": round(float(ma20.iloc[-1]), 4) if pd.notna(ma20.iloc[-1]) else None,
        "ma60": round(float(ma60.iloc[-1]), 4) if pd.notna(ma60.iloc[-1]) else None,
        "rsi14": round(float(rsi.iloc[-1]), 2) if pd.notna(rsi.iloc[-1]) else None,
        "macd": round(float(macd.iloc[-1]), 4),
        "macd_signal": round(float(signal.iloc[-1]), 4),
        "volume_ratio_20d": round(vol_ratio, 2) if vol_ratio is not None else None,
        "support": round(support, 4),
        "resistance": round(resistance, 4),
        "bull_case": f"守住{support:.2f}且量價改善、突破{resistance:.2f}後，多頭情境才增強。",
        "neutral_case": f"在{support:.2f}~{resistance:.2f}區間震盪，視為中性整理。",
        "bear_case": f"有效跌破{support:.2f}且量能放大時，空頭情境風險增加。",
    }



def fetch_latest_news(query: str, *, max_items: int = 12, timeout: float = 5.0) -> tuple[pd.DataFrame, str]:
    """Manually triggered Google News RSS fetch with a hard timeout.

    It is intentionally never called during page load or full-market scanning.
    Network failure returns an empty frame and a diagnostic instead of blocking
    recommendations or fabricating news.
    """
    query = _text(query)
    if not query:
        return pd.DataFrame(), "新聞查詢字串為空"
    try:
        import requests
        import xml.etree.ElementTree as ET
        response = requests.get(
            "https://news.google.com/rss/search",
            params={"q": query, "hl": "zh-TW", "gl": "TW", "ceid": "TW:zh-Hant"},
            headers={"User-Agent": "Mozilla/5.0 GodPick-H81"},
            timeout=max(2.0, min(float(timeout), 10.0)),
        )
        if response.status_code != 200:
            return pd.DataFrame(), f"Google News RSS HTTP {response.status_code}"
        root = ET.fromstring(response.content)
        rows = []
        for item in root.findall(".//item")[:max(1, min(int(max_items), 30))]:
            title = _text(item.findtext("title"))
            pub = _text(item.findtext("pubDate"))
            link = _text(item.findtext("link"))
            source_node = item.find("source")
            source = _text(source_node.text if source_node is not None else "")
            rows.append({"日期": pub, "來源": source or "Google News RSS", "標題": title, "連結": link})
        return pd.DataFrame(rows), f"Google News RSS：{len(rows)} 筆"
    except Exception as exc:
        return pd.DataFrame(), f"新聞抓取失敗：{type(exc).__name__}: {exc}"

def news_impact_analysis(news: pd.DataFrame | list[dict[str, Any]] | None) -> pd.DataFrame:
    df = news.copy() if isinstance(news, pd.DataFrame) else pd.DataFrame(news or [])
    if df.empty:
        return pd.DataFrame(columns=["日期", "標題", "來源", "方向", "影響分", "短期", "中期", "長期", "需驗證假設"])
    rows = []
    for _, row in df.iterrows():
        title = _first_text(row.to_dict(), ["標題", "title", "新聞", "摘要"])
        body = _first_text(row.to_dict(), ["內容", "body", "summary", "摘要"])
        source = _first_text(row.to_dict(), ["來源", "source"])
        date_text = _first_text(row.to_dict(), ["日期", "date", "時間", "published_at"])
        text_all = f"{title} {body}"
        pos = sum(1 for k in POSITIVE_NEWS if k in text_all)
        neg = sum(1 for k in NEGATIVE_NEWS if k in text_all)
        score = _clip(50 + pos * 9 - neg * 10)
        direction = "偏正向" if score >= 60 else "偏負向" if score <= 40 else "中性/待確認"
        rows.append({
            "日期": date_text,
            "標題": title,
            "來源": source or "未標示",
            "方向": direction,
            "影響分": round(score, 1),
            "短期": "可能先反映情緒與資金流；需用價格/成交量確認。",
            "中期": "觀察事件是否改變營收、毛利、訂單或產業供需。",
            "長期": "只有能改變競爭地位、資本效率或長期現金流的事件才提高長期權重。",
            "需驗證假設": "新聞不是價格方向保證；確認市場是否已提前反映、事件是否落地。",
        })
    return pd.DataFrame(rows)


def backtest_trade_records(records: pd.DataFrame | list[dict[str, Any]] | None, *, return_col: str | None = None, train_ratio: float = .70) -> dict[str, Any]:
    df = records.copy() if isinstance(records, pd.DataFrame) else pd.DataFrame(records or [])
    if df.empty:
        return {"available": False, "message": "沒有交易/推薦績效紀錄"}
    candidates = [return_col] if return_col else []
    candidates += ["實際報酬%", "可執行交易5日%", "推薦後5日%", "推薦後3日%", "推薦後1日%", "損益幅%"]
    col = next((c for c in candidates if c and c in df.columns), None)
    if not col:
        return {"available": False, "message": "找不到可回測的報酬欄位"}
    ret = pd.to_numeric(df[col], errors="coerce").dropna()
    if ret.empty:
        return {"available": False, "message": f"{col} 沒有有效數值"}
    ret = ret.clip(-80, 200)
    wins = ret[ret > 0]
    losses = ret[ret < 0]
    gross_profit = float(wins.sum())
    gross_loss = abs(float(losses.sum()))
    pf = gross_profit / gross_loss if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)
    equity = (1 + ret / 100.0).cumprod()
    peak = equity.cummax()
    dd = equity / peak - 1
    n = len(ret)
    split = max(1, min(n - 1, int(round(n * max(.5, min(.85, train_ratio)))))) if n > 1 else 1
    train, test = ret.iloc[:split], ret.iloc[split:]
    def stats(s: pd.Series) -> dict[str, Any]:
        if s.empty:
            return {"n": 0, "win_rate": None, "avg_return": None}
        return {"n": int(len(s)), "win_rate": round(float((s > 0).mean() * 100), 2), "avg_return": round(float(s.mean()), 3)}
    by_market = {}
    market_col = next((c for c in ["大盤情境", "大盤狀態", "市場狀態", "大盤模式"] if c in df.columns), None)
    if market_col:
        tmp = df.loc[ret.index, [market_col]].copy()
        tmp["ret"] = ret
        for key, g in tmp.groupby(market_col, dropna=False):
            by_market[_text(key) or "未分類"] = stats(g["ret"])
    overfit_warning = None
    tr, te = stats(train), stats(test)
    if te["n"] >= 3 and tr["avg_return"] is not None and te["avg_return"] is not None and tr["avg_return"] > 0 and te["avg_return"] <= 0:
        overfit_warning = "訓練區間為正但外樣本轉負，策略可能過度擬合或市場環境已改變。"
    return {
        "available": True,
        "return_column": col,
        "trades": int(n),
        "win_rate_pct": round(float((ret > 0).mean() * 100), 2),
        "avg_win_pct": round(float(wins.mean()), 3) if not wins.empty else 0.0,
        "avg_loss_pct": round(float(losses.mean()), 3) if not losses.empty else 0.0,
        "profit_factor": None if math.isinf(pf) else round(pf, 3),
        "profit_factor_infinite": bool(math.isinf(pf)),
        "max_drawdown_pct": round(float(dd.min() * 100), 3),
        "train": tr,
        "out_of_sample": te,
        "market_regimes": by_market,
        "overfit_warning": overfit_warning or "未偵測到明顯訓練/外樣本反轉，但仍需持續監控。",
    }


def portfolio_stress_test(positions: pd.DataFrame | list[dict[str, Any]] | None, settings: dict[str, Any] | None = None) -> dict[str, Any]:
    df = positions.copy() if isinstance(positions, pd.DataFrame) else pd.DataFrame(positions or [])
    if df.empty:
        return {"available": False, "message": "沒有投資組合部位"}
    cfg = _settings_or_default(settings).get("portfolio_risk", {})
    weight_col = next((c for c in ["配置比例%", "權重%", "weight", "weight_pct"] if c in df.columns), None)
    code_col = next((c for c in ["股票代號", "代號", "ticker"] if c in df.columns), None)
    sector_col = next((c for c in ["類別", "產業", "sector"] if c in df.columns), None)
    geography_col = next((c for c in ["地區", "國家", "geography", "country"] if c in df.columns), None)
    beta_col = next((c for c in ["Beta", "beta", "市場Beta"] if c in df.columns), None)
    rate_col = next((c for c in ["利率敏感度", "rate_sensitivity"] if c in df.columns), None)
    if weight_col is None:
        return {"available": False, "message": "投資組合缺配置比例%欄位"}
    weights = pd.to_numeric(df[weight_col], errors="coerce").fillna(0).clip(lower=0)
    if weights.sum() <= 0:
        return {"available": False, "message": "配置比例合計為0"}
    weights = weights / weights.sum()
    df = df.copy()
    df["__w"] = weights
    betas = pd.to_numeric(df[beta_col], errors="coerce").fillna(1.0) if beta_col else pd.Series(1.0, index=df.index)
    portfolio_beta = float((weights * betas).sum())
    max_single = float(weights.max() * 100)
    hhi = float((weights.pow(2).sum()) * 10000)
    sectors = {}
    if sector_col:
        sectors = {str(k): round(float(v * 100), 2) for k, v in df.groupby(sector_col)["__w"].sum().sort_values(ascending=False).items()}
    max_sector = max(sectors.values()) if sectors else max_single
    geographies = {}
    if geography_col:
        geographies = {str(k): round(float(v * 100), 2) for k, v in df.groupby(geography_col)["__w"].sum().sort_values(ascending=False).items()}
    max_geography = max(geographies.values()) if geographies else None
    warnings = []
    if max_single > float(cfg.get("max_single_stock_weight_pct", 20)):
        warnings.append(f"單一個股集中度 {max_single:.1f}% 超過設定")
    if max_sector > float(cfg.get("max_sector_weight_pct", 35)):
        warnings.append(f"單一產業集中度 {max_sector:.1f}% 超過設定")
    if hhi >= 2500:
        warnings.append(f"HHI {hhi:.0f} 顯示組合集中度偏高")
    scenarios = {
        "市場修正": float(cfg.get("stress_market_correction_pct", -10)),
        "熊市": float(cfg.get("stress_bear_market_pct", -20)),
        "經濟衰退": float(cfg.get("stress_recession_pct", -25)),
    }
    stress = {name: round(shock * portfolio_beta, 2) for name, shock in scenarios.items()}
    rate_shock_bp = int(cfg.get("stress_rate_shock_bp", 100))
    rate_stress = None
    rate_note = "未提供利率敏感度；不捏造數值壓力結果。"
    if rate_col:
        sensitivities = pd.to_numeric(df[rate_col], errors="coerce")
        if sensitivities.notna().any():
            # Column means expected % return impact for a +100bp shock.
            rate_stress = round(float((weights * sensitivities.fillna(0)).sum()) * rate_shock_bp / 100.0, 2)
            rate_note = "利率情境依使用者輸入的每+100bp報酬敏感度線性估算。"
    return {
        "available": True,
        "positions": int(len(df)),
        "portfolio_beta_assumption": round(portfolio_beta, 3),
        "largest_position_pct": round(max_single, 2),
        "largest_sector_pct": round(max_sector, 2),
        "hhi": round(hhi, 1),
        "sector_weights_pct": sectors,
        "geography_weights_pct": geographies,
        "largest_geography_pct": round(max_geography, 2) if max_geography is not None else None,
        "stress_portfolio_return_pct": stress,
        "rate_shock_bp": rate_shock_bp,
        "rate_shock_portfolio_return_pct": rate_stress,
        "rate_shock_note": rate_note,
        "warnings": warnings or ["未觸發目前設定的集中度警示；仍需注意相關性在壓力時可能上升。"],
        "note": "壓力測試是敏感度情境，不是市場預測；beta缺值時以1.0作保守基準。",
    }



def _history_close_frame(history: pd.DataFrame | None) -> pd.DataFrame:
    if history is None or not isinstance(history, pd.DataFrame) or history.empty:
        return pd.DataFrame()
    df = history.copy()
    date_col = next((c for c in ["日期", "date", "Date"] if c in df.columns), None)
    close_col = next((c for c in ["收盤價", "close", "Close"] if c in df.columns), None)
    if close_col is None:
        return pd.DataFrame()
    out = pd.DataFrame({"close": pd.to_numeric(df[close_col], errors="coerce")})
    if date_col:
        out["date"] = pd.to_datetime(df[date_col], errors="coerce")
    else:
        out["date"] = pd.RangeIndex(len(out))
    return out.dropna(subset=["close"]).drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)


def backtest_price_strategy(
    history: pd.DataFrame | None,
    *,
    strategy: str = "MA_CROSS",
    short_ma: int = 5,
    long_ma: int = 20,
    rsi_entry: float = 35.0,
    rsi_exit: float = 55.0,
    cost_bps: float = 30.0,
    train_ratio: float = .70,
) -> dict[str, Any]:
    """Backtest a transparent long-only technical rule without look-ahead.

    Signals observed at close t are applied from t+1.  Transaction cost is
    charged on every position change.  This is a research backtest, not an
    execution simulator, and deliberately includes an out-of-sample split.
    """
    df = _history_close_frame(history)
    if len(df) < max(60, int(long_ma) + 10):
        return {"available": False, "message": f"K線只有{len(df)}根；至少需要{max(60, int(long_ma)+10)}根"}
    close = df["close"].astype(float)
    daily = close.pct_change().fillna(0.0)
    strategy_key = _text(strategy).upper()
    short_ma = max(2, int(short_ma)); long_ma = max(short_ma + 2, int(long_ma))
    if strategy_key == "MA_CROSS":
        fast = close.rolling(short_ma).mean()
        slow = close.rolling(long_ma).mean()
        raw_signal = fast.gt(slow).fillna(False)
        label = f"MA{short_ma}/MA{long_ma}多頭交叉"
    elif strategy_key in {"RSI_REVERSAL", "RSI"}:
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss.replace(0, pd.NA)
        rsi = 100 - 100 / (1 + rs)
        state = False
        signal_values = []
        for value in rsi:
            if pd.notna(value):
                if not state and float(value) <= float(rsi_entry):
                    state = True
                elif state and float(value) >= float(rsi_exit):
                    state = False
            signal_values.append(state)
        raw_signal = pd.Series(signal_values, index=df.index, dtype=bool)
        label = f"RSI14<={rsi_entry:g}進、>={rsi_exit:g}出"
    else:
        return {"available": False, "message": f"不支援策略：{strategy}"}

    # One-bar delay: close signal becomes next session's position.
    position = raw_signal.shift(1, fill_value=False).astype(float)
    turnover = position.diff().abs().fillna(position.abs())
    cost_rate = max(0.0, float(cost_bps)) / 10000.0
    strat_ret = position * daily - turnover * cost_rate
    equity = (1.0 + strat_ret).cumprod()
    drawdown = equity / equity.cummax() - 1.0

    group = (position.ne(position.shift(1))).cumsum()
    trade_returns = []
    for _, idx in position[position.gt(0)].groupby(group[position.gt(0)]).groups.items():
        r = strat_ret.loc[list(idx)]
        if not r.empty:
            trade_returns.append(float((1 + r).prod() - 1) * 100.0)
    trade_s = pd.Series(trade_returns, dtype=float)
    wins = trade_s[trade_s > 0]; losses = trade_s[trade_s < 0]
    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = abs(float(losses.sum())) if not losses.empty else 0.0
    pf = gross_profit / gross_loss if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)

    split = max(1, min(len(strat_ret)-1, int(round(len(strat_ret) * max(.5, min(.85, float(train_ratio)))))))
    def period_stats(sr: pd.Series) -> dict[str, Any]:
        if sr.empty:
            return {"bars": 0, "return_pct": None, "max_drawdown_pct": None}
        eq = (1 + sr).cumprod(); dd = eq / eq.cummax() - 1
        return {"bars": int(len(sr)), "return_pct": round(float((eq.iloc[-1]-1)*100), 3), "max_drawdown_pct": round(float(dd.min()*100), 3)}
    train_stats = period_stats(strat_ret.iloc[:split])
    test_stats = period_stats(strat_ret.iloc[split:])

    ma60 = close.rolling(60).mean()
    regime = pd.Series("中性", index=df.index, dtype=object)
    regime = regime.mask(close > ma60 * 1.01, "多頭")
    regime = regime.mask(close < ma60 * .99, "空頭")
    regime_stats = {}
    for key in ["多頭", "中性", "空頭"]:
        sr = strat_ret[regime.eq(key)]
        if not sr.empty:
            regime_stats[key] = period_stats(sr)

    warnings = []
    if test_stats.get("bars", 0) >= 10 and (train_stats.get("return_pct") or 0) > 0 and (test_stats.get("return_pct") or 0) <= 0:
        warnings.append("訓練區間為正、外樣本轉負：可能過度擬合或市場結構改變。")
    if len(trade_s) < 20:
        warnings.append(f"完整交易只有{len(trade_s)}筆，樣本偏少，不宜據此放寬正式推薦門檻。")
    if not warnings:
        warnings.append("未見明顯訓練/外樣本反轉，但仍需其他期間與市場環境驗證。")
    return {
        "available": True,
        "strategy": label,
        "bars": int(len(df)),
        "trades": int(len(trade_s)),
        "win_rate_pct": round(float((trade_s > 0).mean()*100), 2) if len(trade_s) else None,
        "avg_win_pct": round(float(wins.mean()), 3) if not wins.empty else 0.0,
        "avg_loss_pct": round(float(losses.mean()), 3) if not losses.empty else 0.0,
        "profit_factor": None if math.isinf(pf) else round(float(pf), 3),
        "profit_factor_infinite": bool(math.isinf(pf)),
        "total_return_pct": round(float((equity.iloc[-1]-1)*100), 3),
        "max_drawdown_pct": round(float(drawdown.min()*100), 3),
        "train": train_stats,
        "out_of_sample": test_stats,
        "market_regimes": regime_stats,
        "cost_bps_per_position_change": round(float(cost_bps), 2),
        "warnings": warnings,
        "governance": "訊號延後一根K線執行，避免look-ahead；新規則先外樣本/影子驗證，不直接升格Formal。",
    }


def portfolio_correlation_analysis(
    histories: dict[str, pd.DataFrame] | None,
    *,
    threshold: float = .75,
    minimum_overlap: int = 40,
) -> dict[str, Any]:
    histories = histories or {}
    series = {}
    for code, history in histories.items():
        df = _history_close_frame(history)
        if len(df) < minimum_overlap:
            continue
        if pd.api.types.is_datetime64_any_dtype(df["date"]):
            s = pd.Series(df["close"].pct_change().values, index=pd.to_datetime(df["date"]), name=str(code)).dropna()
        else:
            s = df["close"].pct_change().rename(str(code)).dropna()
        series[str(code)] = s
    if len(series) < 2:
        return {"available": False, "message": "至少需要2檔且各有足夠重疊K線才能估算隱藏相關性"}
    joined = pd.concat(series.values(), axis=1, join="inner").dropna(how="any")
    if len(joined) < minimum_overlap:
        return {"available": False, "message": f"共同交易日只有{len(joined)}日，至少需要{minimum_overlap}日"}
    corr = joined.corr()
    pairs = []
    cols = list(corr.columns)
    for i in range(len(cols)):
        for j in range(i+1, len(cols)):
            value = float(corr.iloc[i, j])
            if abs(value) >= float(threshold):
                pairs.append({"股票A": cols[i], "股票B": cols[j], "相關係數": round(value, 3), "類型": "同向集中" if value > 0 else "反向關聯"})
    pairs.sort(key=lambda x: abs(x["相關係數"]), reverse=True)
    return {
        "available": True,
        "overlap_days": int(len(joined)),
        "threshold": round(float(threshold), 2),
        "high_correlation_pairs": pairs,
        "correlation_matrix": corr.round(3).to_dict(),
        "note": "歷史相關性會隨市場壓力改變；此結果只用於辨識可能的重複押注，不是未來相關性的保證。",
    }


def news_price_scenario(candidate: dict[str, Any] | pd.Series, impacts: pd.DataFrame | None, *, horizon_days: int = 5) -> dict[str, Any]:
    raw = candidate.to_dict() if isinstance(candidate, pd.Series) else dict(candidate or {})
    price = _first_num(raw, ["最新價", "H79計畫進場", "推薦價格", "推薦日價格"])
    daily_vol = _first_num(raw, ["20日波動率%", "日波動率%", "ATR百分比%"])
    if price is None or price <= 0:
        return {"available": False, "message": "缺目前/計畫價格，不能建立新聞價格情境"}
    if daily_vol is None or daily_vol <= 0:
        return {"available": False, "message": "缺可驗證的日波動率/ATR%，不以固定百分比捏造價格區間"}
    scores = pd.to_numeric(impacts.get("影響分"), errors="coerce").dropna() if isinstance(impacts, pd.DataFrame) and "影響分" in impacts.columns else pd.Series(dtype=float)
    avg_score = float(scores.mean()) if not scores.empty else 50.0
    days = max(1, min(20, int(horizon_days)))
    sigma_pct = float(daily_vol) * math.sqrt(days)
    directional_shift_pct = (avg_score - 50.0) / 50.0 * min(sigma_pct * .35, 5.0)
    low = price * (1 + (directional_shift_pct - 1.28*sigma_pct)/100)
    high = price * (1 + (directional_shift_pct + 1.28*sigma_pct)/100)
    formal = _first_text(raw, ["H79正式可執行", "正式可執行", "是否可直接買進"])
    size = _first_num(raw, ["建議倉位%", "動態建議倉位%"])
    allocation = f"沿用正式權威既有倉位上限 {size:.1f}%" if size is not None and formal in {"是", "YES", "PASS", "FORMAL"} else "H81研究層不自行建立部位；未取得Formal授權時不產生買進配置。"
    return {
        "available": True,
        "horizon_days": days,
        "reference_price": round(price, 4),
        "news_average_impact_score": round(avg_score, 2),
        "scenario_low": round(low, 4),
        "scenario_high": round(high, 4),
        "scenario_center_shift_pct": round(directional_shift_pct, 2),
        "allocation_governance": allocation,
        "note": "區間是以既有波動率做的情境帶，不是價格預測或保證。",
    }

def analyze_trade_journal(records: pd.DataFrame | list[dict[str, Any]] | None, recent_n: int = 20) -> dict[str, Any]:
    df = records.copy() if isinstance(records, pd.DataFrame) else pd.DataFrame(records or [])
    if df.empty:
        return {"available": False, "message": "沒有交易日誌/推薦績效資料"}
    source_label = "成熟推薦績效"
    actual_col = next((c for c in ["是否已實際買進", "是否已買進"] if c in df.columns), None)
    if actual_col:
        mask_actual = df[actual_col].astype(str).str.strip().str.lower().isin(["true", "1", "yes", "y", "是", "已買進"])
        if int(mask_actual.sum()) > 0:
            df = df.loc[mask_actual].copy()
            source_label = "實際交易"
    date_col = next((c for c in ["實際賣出日期", "更新時間", "推薦日期", "建立時間"] if c in df.columns), None)
    if date_col:
        order = pd.to_datetime(df[date_col], errors="coerce")
        df = df.assign(__date=order).sort_values("__date", ascending=False)
    df = df.head(max(1, int(recent_n))).copy()
    ret_col = next((c for c in ["實際報酬%", "損益幅%", "可執行交易5日%", "推薦後5日%"] if c in df.columns), None)
    if not ret_col:
        return {"available": False, "message": "最近交易沒有可用報酬欄位"}
    ret = pd.to_numeric(df[ret_col], errors="coerce")
    valid = ret.dropna()
    if valid.empty:
        return {"available": False, "message": "最近交易報酬尚未成熟"}
    rules = []
    chase_col = next((c for c in ["追價風險分", "追高風險分數_決策"] if c in df.columns), None)
    if chase_col:
        chase = pd.to_numeric(df[chase_col], errors="coerce")
        mask = chase.ge(70) & ret.notna()
        if mask.sum() >= 3 and float(ret[mask].mean()) < float(valid.mean()):
            rules.append("追價風險≥70時不追價；只保留研究/等待拉回，除非重新通過價格與RR。")
    rr_col = next((c for c in ["H79成本後RR", "風險報酬比_決策", "風險報酬比"] if c in df.columns), None)
    if rr_col:
        rr = pd.to_numeric(df[rr_col], errors="coerce")
        low = rr.lt(1.5) & ret.notna()
        if low.sum() >= 3 and float(ret[low].mean()) < float(valid.mean()):
            rules.append("成本後RR<1.5的訊號不升級執行，避免勝率尚可但賺賠比惡化。")
    stop_col = next((c for c in ["是否達停損", "停損觸發"] if c in df.columns), None)
    if stop_col:
        stop_hits = df[stop_col].astype(str).str.lower().isin(["true", "1", "yes", "是", "達標", "已達"]).sum()
        if stop_hits >= 2:
            rules.append("同一策略若連續出現停損，下一輪先降低部位/提高驗證門檻，不立刻放寬條件補虧損。")
    if len(rules) < 3:
        rules.extend([
            "只用成熟交易結果調整規則；未觸發或研究樣本不得當成實際交易勝負。",
            "任何新規則先以影子/小權重觀察，外樣本成立後才擴大影響。",
            "保留每筆進場理由、失效條件與市場情境，避免只看最終漲跌造成結果偏誤。",
        ])
    return {
        "available": True,
        "sample": int(len(valid)),
        "source": source_label,
        "return_column": ret_col,
        "win_rate_pct": round(float((valid > 0).mean() * 100), 2),
        "avg_return_pct": round(float(valid.mean()), 3),
        "avg_win_pct": round(float(valid[valid > 0].mean()), 3) if (valid > 0).any() else 0.0,
        "avg_loss_pct": round(float(valid[valid < 0].mean()), 3) if (valid < 0).any() else 0.0,
        "personalized_rules": rules[:3],
    }


def build_daily_trading_plan(candidate: dict[str, Any] | pd.Series, settings: dict[str, Any] | None = None) -> pd.DataFrame:
    raw = candidate.to_dict() if isinstance(candidate, pd.Series) else dict(candidate or {})
    cfg = _settings_or_default(settings).get("daily_plan", {})
    code = _first_text(raw, ["股票代號", "代號"])
    name = _first_text(raw, ["股票名稱", "名稱"])
    entry = _first_num(raw, ["H79計畫進場", "主要進場參考價", "實戰觸發價"])
    stop = _first_num(raw, ["H79結構停損", "停損參考", "SuperAI動態停損價"])
    target = _first_num(raw, ["H79第一目標", "第一壓力價", "SuperAI第一減碼價"])
    rr = _first_num(raw, ["H79成本後RR", "風險報酬比_決策", "風險報酬比"])
    regime = _first_text(raw, ["大盤情境", "大盤狀態", "大盤模式"]) or "待確認"
    rows = []
    if cfg.get("preopen_enabled", True):
        rows.append({"階段": "盤前", "時間": "08:30~08:55", "檢查": f"確認大盤情境={regime}；K線/官方因子新鮮度；是否有重大新事件。", "成立才做": "只保留資料READY且研究/正式條件沒有失效的標的。"})
    if cfg.get("open_enabled", True):
        rows.append({"階段": "開盤", "時間": "09:00~09:30", "檢查": f"觀察是否接近計畫進場 {entry if entry else '未建立'}，避免跳空追價。", "成立才做": "價格/量能符合原計畫，且未觸發過熱或失效條件。"})
    if cfg.get("intraday_enabled", True):
        rows.append({"階段": "盤中", "時間": "09:30~13:20", "檢查": f"停損 {stop if stop else '未建立'}、第一目標 {target if target else '未建立'}、成本後RR {rr if rr is not None else '未建立'}。", "成立才做": "只有原始風險報酬仍成立才維持；失效即取消，不移動停損硬湊RR。"})
    if cfg.get("close_enabled", True):
        rows.append({"階段": "收盤", "時間": "13:30後", "檢查": "記錄實際觸發、最高/最低、收盤、量能、是否達停損/目標及市場情境。", "成立才做": "同步第8頁推薦紀錄，供T+1/T+3/T+5績效與H81檢討。"})
    out = pd.DataFrame(rows)
    if not out.empty:
        out.insert(0, "標的", f"{code} {name}".strip())
    return out

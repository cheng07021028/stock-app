# -*- coding: utf-8 -*-
"""V191-H34 daily 1~3 safe recommendation admission layer.

Goal
----
Improve actionable recall without turning the recommendation engine into a quota filler.
The layer runs *after* the existing formal engine and only backfills when the current
Formal/A- list is below the market-regime target.

Principles
----------
1. Target count: bullish=3, neutral/selective=2, defensive=1, hard-risk=0.
2. Existing Formal/A- recommendations always have priority.
3. Hard vetoes are immutable: stale K-line, extreme/LOCKDOWN market, illiquidity,
   excessive stop/chase risk, insufficient RR, Emerging market, explicit block labels.
4. H34-F may promote a very strong near-miss to Formal; H34-A is conditional A- only.
5. If no stock is safe enough, output 0. Never fabricate a pick to satisfy the quota.
6. H32 forecasts are optional ranking evidence, never a hard guarantee.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import pandas as pd

VERSION = "v191_h34_daily_1to3_safe_admission_20260815"

H34_COLUMNS = [
    "H34每日精選", "H34每日精選排名", "H34每日目標檔數", "H34安全精選分",
    "H34精選等級", "H34精選理由", "H34阻擋原因", "H34操作原則", "H34版本",
]

_BLANK = {"", "none", "nan", "nat", "null", "--", "-", "<na>"}


def _s(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    t = str(v).strip()
    return "" if t.lower() in _BLANK else t


def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        text = str(v).strip().replace(",", "").replace("％", "%")
        if text.endswith("%"):
            text = text[:-1].strip()
        if not text or text.lower() in _BLANK:
            return float(default)
        x = float(text)
        return x if math.isfinite(x) else float(default)
    except Exception:
        return float(default)


def _first_num(row: pd.Series, names: Iterable[str], default: float = 0.0, positive: bool = False) -> float:
    fallback = None
    for name in names:
        if name not in row.index:
            continue
        raw = row.get(name)
        if not _s(raw):
            continue
        x = _f(raw, float("nan"))
        if math.isnan(x):
            continue
        if fallback is None:
            fallback = x
        if not positive or x > 0:
            return float(x)
    return float(default if fallback is None else fallback)


def _first_text(row: pd.Series, names: Iterable[str], default: str = "") -> str:
    for name in names:
        if name in row.index:
            text = _s(row.get(name))
            if text:
                return text
    return default


def _blob(row: pd.Series, names: Iterable[str]) -> str:
    return "｜".join(_s(row.get(name)) for name in names if name in row.index and _s(row.get(name)))


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(x)))


def _market_profile(frame: pd.DataFrame) -> dict[str, Any]:
    if frame is None or frame.empty:
        return {"target": 0, "hard_block": True, "score": 0.0, "regime": "NO-DATA"}
    row = frame.iloc[0]
    text = _blob(row, [
        "大盤原始橋接狀態", "大盤橋接狀態", "大盤橋接風控", "大盤風控層級",
        "極端市場LOCKDOWN", "大盤策略模式", "大盤風險濾網", "大盤風險燈號",
    ])
    score = _first_num(row, ["大盤橋接分數", "大盤可參考分數", "大盤多空分數", "市場環境分數"], 50.0)
    hard = any(k in text for k in ["LOCKDOWN", "全面禁買", "極端風險", "崩跌後冷卻", "極端崩跌"])
    if score < 42:
        hard = True
    if hard:
        return {"target": 0, "hard_block": True, "score": score, "regime": "LOCKDOWN/極端風險"}
    if score >= 65 or any(k in text for k in ["趨勢多頭", "中性偏多", "綠燈"]):
        target, regime = 3, "攻擊/偏多"
    elif score >= 50:
        target, regime = 2, "中性/精選"
    else:
        target, regime = 1, "防守/只選最強"
    return {"target": target, "hard_block": False, "score": score, "regime": regime}


def _metrics(row: pd.Series) -> dict[str, float]:
    rr = _first_num(row, ["路徑風險報酬比", "風險報酬比", "實戰風險報酬比", "保守風報比"], 0.0, positive=True)
    stop = _first_num(row, ["停損距離_隔日%", "隔日有效風控距離%", "實戰停損距離%", "停損距離%"], 0.0, positive=True)
    amount = _first_num(row, ["流動性參考成交額百萬", "成交額百萬", "20日均成交額百萬"], 0.0, positive=True)
    gap = _first_num(row, ["距最近可執行買點%", "觸發距離%"], 99.0)
    return {
        "entry": _first_num(row, ["Entry進場買點分", "Entry進場分", "進場買點分", "買進分數"], 0.0),
        "risk": _first_num(row, ["Risk風控安全分", "Risk風控分", "風控安全分"], 0.0),
        "buy": _first_num(row, ["買進分數"], 0.0),
        "op": _first_num(row, ["可操作分", "實戰操作品質分", "進場可執行分"], 0.0),
        "ready": _first_num(row, ["進場可執行分", "隔日可執行優先分"], 0.0),
        "rr": rr,
        "stop": stop,
        "amount": amount,
        "gap": gap,
        "chase": _first_num(row, ["追價風險分", "追高風險分數_決策", "追價風險分數"], 55.0),
        "mainstream": _first_num(row, ["主流資金分"], 50.0),
        "sector": max(
            _first_num(row, ["族群攻擊強度"], 0.0),
            _first_num(row, ["族群輪動分"], 0.0),
            _first_num(row, ["類股熱度分數"], 0.0),
        ),
        "priority": _first_num(row, ["股神推薦優先分", "正式推薦排序分", "AI綜合決策分"], 50.0),
        "ai": _first_num(row, ["AI綜合決策分", "SuperAI 最終決策分", "SuperAI最終決策分"], 50.0),
        "prob": _first_num(row, ["H32隔日上漲機率%", "模型隔日上漲機率%", "SuperAI校準後上漲機率%"], 50.0),
        "t1": _first_num(row, ["H32隔日預估漲跌幅%", "模型預估超額報酬%"], 0.0),
        "swing": _first_num(row, ["H32後續波段預估漲幅%", "H32_10日預估報酬%"], 0.0),
    }


def _hard_veto(row: pd.Series, market: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if market.get("hard_block"):
        reasons.append("大盤LOCKDOWN/極端風險")
    market_type = _s(row.get("市場別")).replace(" ", "")
    if market_type in {"興櫃", "Emerging"}:
        reasons.append("興櫃不列每日作戰推薦")

    k_lag = _first_num(row, ["K線落後交易日"], 999)
    k_fresh = _first_text(row, ["K線資料新鮮度", "股神資料總新鮮度"])
    if k_lag != 0 or any(k in k_fresh for k in ["過期", "落後", "待更新", "未驗證", "未知"]):
        reasons.append("K線不是最近完成交易日")

    o_lag = _first_num(row, ["官方因子落後交易日"], 999)
    o_fresh = _first_text(row, ["官方因子新鮮度"])
    if o_lag > 1 or any(k in o_fresh for k in ["無法驗證", "過期", "失效"]):
        reasons.append("官方因子過期/無法驗證")

    text = _blob(row, [
        "正式推薦排除原因", "風控否決旗標", "真禁買原因", "硬否決原因",
        "紅燈觸發管制", "模型預測限制", "推薦輪動狀態", "掃描品質狀態",
    ])
    hard_words = [
        "LOCKDOWN", "極端", "全面禁買", "低流動性", "冷門禁追", "興櫃",
        "停損距離過大", "風控失衡", "假強", "假突破", "PRED-BLOCK",
        "STICKY-BLOCK", "禁止升格", "過熱禁買",
    ]
    for word in hard_words:
        if word in text:
            reasons.append(f"硬風控：{word}")
            break

    m = _metrics(row)
    if m["amount"] < 150:
        reasons.append("成交額不足1.5億元")
    if m["stop"] <= 0 or m["stop"] > 8.0:
        reasons.append(f"停損距離{m['stop']:.1f}%不合格")
    if m["rr"] < 1.05:
        reasons.append(f"RR {m['rr']:.2f}<1.05")
    if m["chase"] > 68:
        reasons.append(f"追價風險{m['chase']:.0f}>68")
    return list(dict.fromkeys(reasons))


def _quality_score(m: dict[str, float]) -> float:
    rr_score = _clamp(m["rr"] / 2.5 * 100.0)
    gap_score = 100.0 if m["gap"] <= 1.5 else 90.0 if m["gap"] <= 2.5 else 78.0 if m["gap"] <= 4 else 62.0 if m["gap"] <= 5.5 else 35.0
    forecast_score = _clamp(50.0 + m["t1"] * 8.0 + m["swing"] * 1.8)
    score = (
        m["priority"] * 0.15 + m["ai"] * 0.10 + m["entry"] * 0.15 + m["risk"] * 0.15
        + m["op"] * 0.10 + rr_score * 0.12 + (100.0 - m["chase"]) * 0.06
        + m["mainstream"] * 0.05 + m["sector"] * 0.04 + m["prob"] * 0.04
        + forecast_score * 0.02 + gap_score * 0.02
    )
    return round(_clamp(score), 2)


def _gate(row: pd.Series, market: dict[str, Any]) -> dict[str, Any]:
    veto = _hard_veto(row, market)
    m = _metrics(row)
    score = _quality_score(m)
    if veto:
        return {"eligible": False, "formal": False, "score": score, "reason": "、".join(veto[:5]), "m": m}

    # H34-F: safe near-miss allowed to become a formal recommendation.
    formal = bool(
        m["entry"] >= 66 and m["risk"] >= 63 and m["buy"] >= 58
        and m["op"] >= 66 and m["rr"] >= 1.50 and 0 < m["stop"] <= 6.5
        and m["amount"] >= 250 and m["chase"] <= 52 and m["gap"] <= 5.0
        and (m["mainstream"] >= 55 or m["sector"] >= 55)
        and m["prob"] >= 52 and score >= 69
    )
    # H34-A: conditional small-position pick; still significantly above generic radar.
    a_minus = bool(
        m["entry"] >= 60 and m["risk"] >= 56 and m["buy"] >= 52
        and m["op"] >= 60 and m["rr"] >= 1.20 and 0 < m["stop"] <= 7.5
        and m["amount"] >= 180 and m["chase"] <= 62 and m["gap"] <= 6.0
        and (m["mainstream"] >= 50 or m["sector"] >= 50)
        and m["prob"] >= 50 and score >= 62
    )
    eligible = formal or a_minus
    if not eligible:
        failed = []
        checks = [
            (m["entry"] >= 60, f"Entry {m['entry']:.1f}<60"),
            (m["risk"] >= 56, f"Risk {m['risk']:.1f}<56"),
            (m["buy"] >= 52, f"買進分{m['buy']:.1f}<52"),
            (m["op"] >= 60, f"可操作{m['op']:.1f}<60"),
            (m["rr"] >= 1.20, f"RR {m['rr']:.2f}<1.20"),
            (m["chase"] <= 62, f"追價風險{m['chase']:.0f}>62"),
            (m["gap"] <= 6.0, f"距買點{m['gap']:.1f}%>6"),
            (score >= 62, f"H34安全精選分{score:.1f}<62"),
        ]
        failed = [msg for ok, msg in checks if not ok]
        reason = "、".join(failed[:5]) or "未達H34安全精選條件"
    else:
        reason = ""
    return {"eligible": eligible, "formal": formal, "score": score, "reason": reason, "m": m}


def _existing_bucket(row: pd.Series) -> bool:
    bucket = _s(row.get("正式推薦分區"))
    return bucket in {"正式下週主推薦", "A-｜準主推薦小量試單"}


def apply_daily_safe_selection(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame(columns=H34_COLUMNS)
    out = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame(frame)
    for col, default in {
        "H34每日精選": "否", "H34每日精選排名": 0, "H34每日目標檔數": 0,
        "H34安全精選分": 0.0, "H34精選等級": "", "H34精選理由": "",
        "H34阻擋原因": "", "H34操作原則": "", "H34版本": VERSION,
    }.items():
        out[col] = default
    if out.empty:
        return out

    market = _market_profile(out)
    target = int(market["target"])
    out["H34每日目標檔數"] = target
    if target <= 0:
        out["H34阻擋原因"] = "大盤LOCKDOWN/極端風險：H34允許0檔，不為名額犧牲風控"
        return out

    # Score everyone for auditability.
    gates: dict[Any, dict[str, Any]] = {}
    for idx, row in out.iterrows():
        gate = _gate(row, market)
        gates[idx] = gate
        out.at[idx, "H34安全精選分"] = gate["score"]
        out.at[idx, "H34阻擋原因"] = gate["reason"]

    existing = [idx for idx, row in out.iterrows() if _existing_bucket(row)]
    existing.sort(key=lambda idx: gates[idx]["score"], reverse=True)
    selected: list[Any] = existing[:target]

    # Backfill only when standard Formal/A- is short of the market-regime target.
    if len(selected) < target:
        pool = [idx for idx in out.index if idx not in existing and gates[idx]["eligible"]]
        pool.sort(key=lambda idx: gates[idx]["score"], reverse=True)
        for idx in pool[: max(0, target - len(selected))]:
            row = out.loc[idx]
            gate = gates[idx]
            if gate["formal"]:
                out.at[idx, "正式推薦分區"] = "正式下週主推薦"
                out.at[idx, "正式推薦資格"] = "H34-F｜每日安全精選正式推薦"
                out.at[idx, "正式推薦等級"] = "H34-F｜每日安全精選"
                out.at[idx, "是否正式推薦"] = "是"
                out.at[idx, "操作許可"] = "條件式正式推薦｜觸發後守價成立才進場"
                out.at[idx, "下週是否可直接買"] = "不可開盤追價｜觸發＋守價後分批"
                out.at[idx, "正式推薦判定來源"] = "H34每日1~3檔安全精選層"
                out.at[idx, "正式推薦排除原因"] = ""
                out.at[idx, "H34精選等級"] = "H34-F｜正式精選"
            else:
                out.at[idx, "正式推薦分區"] = "A-｜準主推薦小量試單"
                out.at[idx, "正式推薦資格"] = "H34-A｜每日安全精選準主推薦"
                out.at[idx, "正式推薦等級"] = "A-｜H34安全精選"
                out.at[idx, "準主推薦等級"] = "A-｜H34安全精選"
                out.at[idx, "是否正式推薦"] = "否"
                out.at[idx, "操作許可"] = "條件式A-｜觸發＋守價後小量試單"
                out.at[idx, "下週是否可直接買"] = "不可直接買｜盤中確認後小量"
                out.at[idx, "正式推薦判定來源"] = "H34每日1~3檔安全精選層"
                out.at[idx, "正式推薦排除原因"] = ""
                out.at[idx, "H34精選等級"] = "H34-A｜準主精選"
            out.at[idx, "正式推薦動作"] = "等待實戰觸發價；站穩並守價才分批，未觸發視為沒有交易。"
            out.at[idx, "最終操作結論"] = out.at[idx, "H34精選等級"] + "｜禁止追價"
            out.at[idx, "推薦資格路徑"] = "H34｜標準Formal/A-不足時的安全近門檻補位"
            selected.append(idx)

    # Mark selected daily list. Existing standard picks receive their original grade.
    selected = selected[:target]
    for rank, idx in enumerate(selected, start=1):
        out.at[idx, "H34每日精選"] = "是"
        out.at[idx, "H34每日精選排名"] = rank
        if not _s(out.at[idx, "H34精選等級"]):
            bucket = _s(out.at[idx, "正式推薦分區"])
            out.at[idx, "H34精選等級"] = "標準正式推薦" if bucket == "正式下週主推薦" else "標準A-準主推薦"
        m = gates[idx]["m"]
        out.at[idx, "H34精選理由"] = (
            f"安全分{gates[idx]['score']:.1f}｜Entry {m['entry']:.0f}｜Risk {m['risk']:.0f}｜"
            f"RR {m['rr']:.2f}｜可操作{m['op']:.0f}｜距買點{m['gap']:.1f}%｜"
            f"追價{m['chase']:.0f}｜隔日機率{m['prob']:.1f}%"
        )
        out.at[idx, "H34操作原則"] = "只做觸發後守價；禁止開盤追價；停損失守立即取消；未觸發不交易。"
        out.at[idx, "H34阻擋原因"] = ""

    return out


_INSTALLED = False
_ORIGINAL = None


def install_daily_safe_selection_guard() -> bool:
    """Wrap the formal engine once, without editing its large source file."""
    global _INSTALLED, _ORIGINAL
    if _INSTALLED:
        return True
    try:
        import godpick_formal_recommendation_engine as engine
    except Exception:
        return False
    original = getattr(engine, "apply_formal_recommendation_engine", None)
    if not callable(original):
        return False
    if getattr(original, "_h34_daily_safe_selection", False):
        _INSTALLED = True
        return True
    _ORIGINAL = original

    def wrapped(df):
        base = original(df)
        return apply_daily_safe_selection(base)

    wrapped.__name__ = getattr(original, "__name__", "apply_formal_recommendation_engine")
    wrapped.__doc__ = getattr(original, "__doc__", None)
    wrapped._h34_daily_safe_selection = True
    wrapped._h34_original = original
    engine.apply_formal_recommendation_engine = wrapped
    _INSTALLED = True
    return True

# -*- coding: utf-8 -*-
"""V191-H42 dual-route opportunity engine.

The recommendation system previously had one main admission path.  In sharp
sell-offs that is too easy to interpret as either "buy nothing" or "lower every
threshold".  H42 keeps the authority recommendation / H41 daily-pick layers
unchanged and adds a separate *reference* layer with two different opportunity
hypotheses:

A. 強勢延續：relative strength / leadership.  Consider only when price structure
   is still healthy and the executable trigger/pullback path is close enough.
B. 跌深價差：post-selloff mean-reversion / spread opportunity.  A large decline is
   NOT a buy signal by itself.  The engine first identifies a discount candidate,
   then requires stabilization / reclaim before it may become executable.

This module never rewrites Formal/A-/V188 authority.  It adds H42 columns and a
compact focus table so Page07/Excel can show what to watch first without forcing a
trade every day.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import pandas as pd

VERSION = "v191_h45_mainstream_wave_dual_route_20260820"

H42_COLUMNS = [
    "H42強勢分", "H42強勢狀態", "H42強勢操作許可", "H42強勢理由",
    "H42價差分", "H42價差狀態", "H42價差操作許可", "H42價差理由",
    "H42跌深旗標數", "H42止穩確認", "H42落刀風險", "H42市場情境",
    "H42市場共識分", "H42重點機會類型", "H42重點狀態", "H42重點分",
    "H42重點決策", "H42建議倉位上限%", "H42重點理由", "H42版本",
]

_BLANK = {"", "none", "nan", "nat", "null", "--", "-", "<na>"}


def _s(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in _BLANK else text


def _f(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).strip().replace(",", "").replace("％", "%")
        if text.endswith("%"):
            text = text[:-1].strip()
        if not text or text.lower() in _BLANK:
            return float(default)
        out = float(text)
        return out if math.isfinite(out) else float(default)
    except Exception:
        return float(default)


def _first_num(row: pd.Series, names: Iterable[str], default: float = 0.0, positive: bool = False) -> float:
    fallback: float | None = None
    for name in names:
        if name not in row.index or not _s(row.get(name)):
            continue
        value = _f(row.get(name), float("nan"))
        if not math.isfinite(value):
            continue
        if fallback is None:
            fallback = float(value)
        if not positive or value > 0:
            return float(value)
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


def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(value)))


def _contains(text: str, words: Iterable[str]) -> bool:
    upper = str(text or "").upper()
    return any(str(word).upper() in upper for word in words)


def _market_context(frame: pd.DataFrame) -> dict[str, Any]:
    if frame is None or frame.empty:
        return {"score": 50.0, "hard": True, "defensive": True, "regime": "NO-DATA", "source_scores": []}

    scores: list[float] = []
    texts: list[str] = []
    for _, row in frame.iterrows():
        for col in ["大盤橋接分數", "大盤多空分數", "市場環境分數"]:
            if col not in row.index or not _s(row.get(col)):
                continue
            value = _f(row.get(col), float("nan"))
            if math.isfinite(value) and value > 0 and all(abs(float(value) - seen) > 1e-6 for seen in scores):
                scores.append(float(value))
        texts.append(_blob(row, [
            "極端市場LOCKDOWN", "大盤風控層級", "大盤風險燈號", "大盤橋接風控",
            "大盤策略模式", "大盤橋接狀態", "今日大盤結論", "大盤策略建議",
        ]))

    score = float(pd.Series(scores, dtype="float64").median()) if scores else 50.0
    text = "｜".join(texts)
    hard = _contains(text, ["LOCKDOWN", "全面禁買", "全面停買", "禁止所有新倉", "極端風險", "系統性風險"])
    low_votes = sum(1 for value in scores if value < 42)
    # Same H42 consensus rule as Formal/H34: one low component is defensive
    # evidence, not a universal lockdown when other authority signals disagree.
    if score < 42 and low_votes >= 2:
        hard = True
    defensive = bool(
        hard or score < 55 or _contains(text, ["紅燈", "橘燈", "黃燈", "防守", "中性偏空", "震盪", "廣度偏弱"])
    )
    if hard:
        regime = "極端風險｜只觀察，不直接接刀"
    elif score < 50 or _contains(text, ["中性偏空", "防守", "廣度偏弱"]):
        regime = "防守/震盪｜最多小量條件單"
    elif score >= 65 and _contains(text, ["偏多", "多頭", "攻擊", "綠燈"]):
        regime = "偏多/攻擊｜強勢優先"
    else:
        regime = "中性/選股｜雙路徑比較"
    return {"score": round(score, 2), "hard": hard, "defensive": defensive, "regime": regime, "source_scores": scores}


def _metrics(row: pd.Series) -> dict[str, float]:
    latest = _first_num(row, ["最新價", "收盤價", "價格"], 0.0, positive=True)
    main_entry = _first_num(row, ["主要進場參考價", "回測承接參考價", "預估進場點"], 0.0, positive=True)
    trigger = _first_num(row, ["實戰觸發價", "突破確認參考價", "觸發價"], 0.0, positive=True)
    guard = _first_num(row, ["觸發後守價", "守價回測參考價"], 0.0, positive=True)
    stop_price = _first_num(row, ["實戰停損參考", "停損參考"], 0.0, positive=True)
    stop = _first_num(row, ["隔日有效風控距離%", "停損距離_隔日%", "實戰停損距離%", "停損距離%"], 0.0, positive=True)
    if stop <= 0 and latest > 0 and stop_price > 0 and stop_price < latest:
        stop = (latest / stop_price - 1.0) * 100.0
    rr = _first_num(row, ["路徑風險報酬比", "SuperAI執行風報比", "風險報酬比", "實戰風險報酬比"], 0.0, positive=True)
    chase = _first_num(row, ["追價風險分", "追高風險分數_決策", "追價風險分數"], 55.0)
    v188 = _first_num(row, ["V188股神作戰優先分", "股神推薦優先分"], 50.0)
    trade = _first_num(row, ["SuperAI Trade分", "實戰操作品質分", "可操作分", "進場可執行分"], 50.0)
    entry = _first_num(row, ["Entry進場買點分", "進場買點分", "買進分數"], 50.0)
    risk = _first_num(row, ["Risk風控安全分", "風控安全分", "Risk風控分"], 50.0)
    mainstream = _first_num(row, ["主流主升優先分", "主流資金分"], 50.0)
    funds = _first_num(row, ["主流資金分"], 50.0)
    sector = max(
        _first_num(row, ["族群攻擊強度"], 0.0),
        _first_num(row, ["族群輪動分"], 0.0),
        _first_num(row, ["類股熱度分數"], 0.0),
    )
    ret1 = _first_num(row, ["今日漲幅%", "當日漲幅%"], 0.0)
    ret5 = _first_num(row, ["近5日漲幅%"], 0.0)
    ret20 = _first_num(row, ["近20日漲幅%"], 0.0)
    close_pos = _first_num(row, ["當日收盤位置%"], 50.0)
    upper = _first_num(row, ["上影線比例%"], 20.0)
    support = max(
        _first_num(row, ["支撐回測分數"], 0.0),
        _first_num(row, ["支撐防守分"], 0.0),
        50.0,
    )
    reversal = max(
        _first_num(row, ["止跌轉強分數"], 0.0),
        _first_num(row, ["恐慌反彈領漲分"], 0.0),
        _first_num(row, ["均線轉強分"], 0.0),
        40.0,
    )
    amount = _first_num(row, ["流動性參考成交額百萬", "成交額百萬", "20日均成交額百萬"], 0.0, positive=True)
    volume_ratio = _first_num(row, ["當日量比", "均量比"], 1.0, positive=True)

    distances: list[float] = []
    for price in [main_entry, trigger]:
        if latest > 0 and price > 0:
            distances.append(abs(price / latest - 1.0) * 100.0)
    explicit_distance = _first_num(row, ["H41最近可執行距離%", "距最近可執行買點%"], float("nan"))
    if math.isfinite(explicit_distance) and 0 <= explicit_distance < 90:
        distances.append(explicit_distance)
    exec_distance = min(distances) if distances else 99.0

    return {
        "latest": latest, "main_entry": main_entry, "trigger": trigger, "guard": guard, "stop_price": stop_price,
        "stop": stop, "rr": rr, "chase": chase, "v188": v188, "trade": trade, "entry": entry, "risk": risk,
        "mainstream": mainstream, "funds": funds, "sector": sector, "ret1": ret1, "ret5": ret5, "ret20": ret20,
        "close": close_pos, "upper": upper, "support": support, "reversal": reversal, "amount": amount,
        "volume_ratio": volume_ratio, "exec_distance": exec_distance,
    }


def _data_block(row: pd.Series) -> tuple[bool, str]:
    text = _blob(row, [
        "K線資料新鮮度", "大盤與K線對齊狀態", "股神資料總新鮮度", "股神資料警示",
        "正式推薦排除原因", "正式推薦分區", "V188交易許可", "操作許可",
    ])
    hard_words = [
        "K線過期", "資料待更新", "禁止新倉", "正式排除", "EXCLUDE", "DATA-WAIT", "STALE",
        "流動性不足", "資料未對齊", "K線日期未驗證",
    ]
    if _contains(text, hard_words):
        return True, "資料/權限硬封鎖"
    return False, ""


def _strong_profile(row: pd.Series, market: dict[str, Any]) -> dict[str, Any]:
    """H45 strong route: H45 mainstream/wave evidence first, execution second.

    When H45 columns are present, this function MUST NOT recreate the old H42
    heuristic where a one-day rebound or a high generic money score could be
    labelled strong.  H45 already decided whether the stock is a true
    mainstream wave, an early-breakout setup, or merely a watch candidate.
    """
    m = _metrics(row)
    h45_status = _first_text(row, ["H45主流波段狀態"])
    h45_score = _first_num(row, ["H45主流波段分"], 0.0)
    h45_sector = _first_num(row, ["H45族群主流分"], 0.0)
    h45_onset = _first_num(row, ["H45起漲結構分"], 0.0)
    h45_reason = _first_text(row, ["H45主流波段理由"])
    h45_available = bool(h45_status or h45_score > 0)

    data_block, data_reason = _data_block(row)
    if h45_available:
        # H45 READY still obeys market hard-block and final execution safety.
        if h45_status.startswith("M-READY") and not data_block and not market.get("hard"):
            status = "S-READY｜主流波段條件進場"
            cap = 3.0 if market.get("defensive") else 5.0
            if m["main_entry"] > 0:
                permission = (
                    f"主流強勢不追高：優先等 {m['main_entry']:.2f} 附近承接；"
                    f"若走突破，需站上 {m['trigger']:.2f} 且守住 {m['guard']:.2f}。未成立=NO-TRADE。"
                )
            else:
                permission = "主流波段條件成立後才小量；突破後必須守價，未成立=NO-TRADE。"
        elif h45_status.startswith("M-PREP"):
            status = "S-PREP｜主流起漲前兆，等量價確認"
            cap = 0.0
            permission = "主流/領先/起漲前兆已形成，但尚未取得交易許可；等量能、收盤結構與回測/突破守價確認後再升格。"
        elif h45_status.startswith("M-WATCH") or (h45_status.startswith("M-READY") and market.get("hard")):
            status = "S-WATCH｜主流/個股強勢觀察"
            cap = 0.0
            permission = "列入主流強勢觀察，不直接追價；等回測承接或突破＋守價後重新取得交易資格。"
        else:
            status = "S-NO｜非主流波段優先"
            cap = 0.0
            permission = "H45未通過主流族群＋領先股＋起漲結構，不列強勢路徑。"
        reasons = [x for x in [h45_reason, data_reason if data_block else "", "極端市場只觀察" if market.get("hard") else ""] if x]
        return {
            "score": round(_clamp(h45_score), 2), "status": status, "permission": permission, "cap": cap,
            "reason": "；".join(reasons) or f"H45主流波段{h45_score:.1f}／族群{h45_sector:.1f}／起漲{h45_onset:.1f}",
            "m": m,
        }

    # Legacy fallback only for old frames/tests that have not yet passed H45.
    rr_score = _clamp(45.0 + 14.0 * max(0.0, m["rr"] - 1.0))
    recent_score = _clamp(50.0 + 5.0 * m["ret1"] + 1.5 * m["ret5"])
    score = (
        0.20 * m["trade"] + 0.16 * m["risk"] + 0.13 * m["entry"] + 0.13 * m["mainstream"]
        + 0.09 * m["sector"] + 0.07 * m["funds"] + 0.08 * _clamp(m["close"])
        + 0.06 * _clamp(100.0 - m["chase"]) + 0.04 * rr_score + 0.04 * recent_score
    )
    score = round(_clamp(score), 2)
    hard_safety = bool(
        m["trade"] >= 70 and m["risk"] >= 62 and m["rr"] >= 1.25 and 0 < m["stop"] <= 7.0 and m["chase"] <= 60
        and (m["amount"] <= 0 or m["amount"] >= 120)
    )
    structure = bool(m["close"] >= 50 and m["upper"] <= 45 and m["ret1"] >= -1.5 and m["ret5"] >= -6)
    leadership = bool(m["mainstream"] >= 60 or m["sector"] >= 60 or m["funds"] >= 62 or m["ret1"] >= 1.5)
    reachable = bool(m["exec_distance"] <= 6.5)
    ready_threshold = 68.0 if market.get("defensive") else 65.0
    ready = bool(not data_block and not market.get("hard") and hard_safety and structure and leadership and reachable and score >= ready_threshold)
    watch = bool(not data_block and hard_safety and score >= 60 and (leadership or structure))
    if ready:
        status, cap, permission = "S-READY｜強勢條件進場", (3.0 if market.get("defensive") else 5.0), "強勢條件成立後才小量；未成立=NO-TRADE。"
    elif watch:
        status, cap, permission = "S-WATCH｜強勢但等待更好買點", 0.0, "列入強勢觀察，不直接追價。"
    else:
        status, cap, permission = "S-NO｜非強勢優先", 0.0, "目前不列強勢路徑。"
    return {"score": score, "status": status, "permission": permission, "cap": cap, "reason": "legacy fallback", "m": m}

def _bargain_profile(row: pd.Series, market: dict[str, Any]) -> dict[str, Any]:
    m = _metrics(row)
    drawdown_flags = [m["ret1"] <= -3.0, m["ret5"] <= -8.0, m["ret20"] <= -12.0]
    drawdown_count = int(sum(drawdown_flags))
    drawdown_score = _clamp(
        48.0 + 3.0 * max(0.0, -m["ret1"] - 1.0) + 1.8 * max(0.0, -m["ret5"] - 4.0) + 1.0 * max(0.0, -m["ret20"] - 8.0)
    )
    rr_score = _clamp(45.0 + 12.0 * max(0.0, m["rr"] - 1.0))
    score = (
        0.18 * m["trade"] + 0.17 * m["risk"] + 0.12 * m["support"] + 0.10 * m["reversal"]
        + 0.10 * _clamp(100.0 - m["upper"]) + 0.08 * _clamp(m["close"]) + 0.07 * _clamp(100.0 - m["chase"])
        + 0.08 * rr_score + 0.10 * drawdown_score
    )
    score = round(_clamp(score), 2)

    data_block, data_reason = _data_block(row)
    safety = bool(
        m["trade"] >= 70 and m["risk"] >= 62 and m["rr"] >= 1.40 and 0 < m["stop"] <= 6.5 and m["chase"] <= 55
        and (m["amount"] <= 0 or m["amount"] >= 120)
    )
    falling_knife = bool(m["ret1"] <= -6.0 or m["close"] < 30 or m["upper"] > 55 or m["stop"] > 7.5)
    explicit_reversal = _blob(row, ["紅燈逆勢反轉判定", "恐慌反彈領漲判定"])
    stabilized = bool(
        _contains(explicit_reversal, ["READY-R", "READY-RB"])
        or (m["ret1"] >= 1.0 and m["close"] >= 55 and m["upper"] <= 40 and m["reversal"] >= 52)
        or (m["ret1"] >= 0 and m["close"] >= 60 and m["support"] >= 68 and m["reversal"] >= 58)
    )
    # A bargain candidate needs either two independent drawdown signals, or one
    # clearly deep multi-day drawdown (5d<=-12% / 20d<=-15%).  A single red day
    # is never enough.  This lets H42 surface genuine post-crash price-spread
    # opportunities such as a -15~-20% swing without treating every -3% day as cheap.
    deep_multi_day = bool(m["ret5"] <= -12.0 or m["ret20"] <= -15.0)
    discount = bool(drawdown_count >= 2 or deep_multi_day)
    ready = bool(not data_block and not market.get("hard") and discount and safety and not falling_knife and stabilized and score >= 68)
    watch = bool(not data_block and discount and safety and score >= 62)

    if ready:
        status = "B-READY｜跌深價差條件進場"
        cap = 2.5 if market.get("defensive") else 3.5
        permission = (
            "跌深不等於便宜：只在不破前低、支撐區止穩，且15分鐘級別重新站回確認價/守價後小量；"
            "再破前低或失守=NO-TRADE。"
        )
    elif watch:
        status = "B-WAIT｜跌深但尚待止穩"
        cap = 0.0
        permission = (
            "只列價差觀察，禁止直接接刀；等次日不破前低＋盤中重新站回承接/守價後再重新評分。"
        )
    else:
        status = "B-NO｜非價差優先"
        cap = 0.0
        permission = "目前不列跌深價差路徑。"

    reasons = []
    if data_block:
        reasons.append(data_reason)
    if discount:
        if drawdown_count >= 2:
            reasons.append(f"跌深訊號{drawdown_count}/3")
        else:
            reasons.append("單一多日跌幅已達深度門檻")
    if falling_knife:
        reasons.append("仍有落刀/弱收盤風險")
    if stabilized:
        reasons.append("已出現止穩/反轉確認")
    elif discount:
        reasons.append("尚未完成止穩確認")
    if not safety:
        reasons.append("Trade/Risk/RR/停損至少一項不足")
    if market.get("hard"):
        reasons.append("極端市場只能等待確認")
    return {
        "score": score, "status": status, "permission": permission, "cap": cap, "reason": "；".join(reasons) or "跌深條件不足",
        "drawdown_count": drawdown_count, "stabilized": stabilized, "knife": falling_knife, "m": m,
    }


def apply_dual_opportunity_engine(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame):
        return pd.DataFrame(frame)
    if frame.empty:
        out = frame.copy()
        for col in H42_COLUMNS:
            if col not in out.columns:
                out[col] = pd.Series(dtype="object")
        return out

    out = frame.copy()
    market = _market_context(out)
    strong_profiles = [_strong_profile(row, market) for _, row in out.iterrows()]
    bargain_profiles = [_bargain_profile(row, market) for _, row in out.iterrows()]

    out["H42強勢分"] = [p["score"] for p in strong_profiles]
    out["H42強勢狀態"] = [p["status"] for p in strong_profiles]
    out["H42強勢操作許可"] = [p["permission"] for p in strong_profiles]
    out["H42強勢理由"] = [p["reason"] for p in strong_profiles]
    out["H42價差分"] = [p["score"] for p in bargain_profiles]
    out["H42價差狀態"] = [p["status"] for p in bargain_profiles]
    out["H42價差操作許可"] = [p["permission"] for p in bargain_profiles]
    out["H42價差理由"] = [p["reason"] for p in bargain_profiles]
    out["H42跌深旗標數"] = [p["drawdown_count"] for p in bargain_profiles]
    out["H42止穩確認"] = ["是" if p["stabilized"] else "否" for p in bargain_profiles]
    out["H42落刀風險"] = ["高" if p["knife"] else "可控" for p in bargain_profiles]
    out["H42市場情境"] = market.get("regime")
    out["H42市場共識分"] = market.get("score")

    focus_types: list[str] = []
    focus_status: list[str] = []
    focus_score: list[float] = []
    focus_decision: list[str] = []
    caps: list[float] = []
    focus_reasons: list[str] = []
    for s_profile, b_profile in zip(strong_profiles, bargain_profiles):
        s_active = not s_profile["status"].startswith("S-NO")
        b_active = not b_profile["status"].startswith("B-NO")
        if s_active and (not b_active or s_profile["status"].startswith("S-READY") or s_profile["score"] >= b_profile["score"] + 3):
            route = "強勢延續"
            status = s_profile["status"]
            score = s_profile["score"]
            decision = (
                "可考慮條件進場" if status.startswith("S-READY")
                else "主流起漲前兆｜等確認" if status.startswith("S-PREP")
                else "強勢等待買點"
            )
            cap = s_profile["cap"]
            reason = s_profile["reason"]
        elif b_active:
            route = "跌深價差"
            status = b_profile["status"]
            score = b_profile["score"]
            decision = "可考慮價差條件單" if status.startswith("B-READY") else "跌深等待止穩"
            cap = b_profile["cap"]
            reason = b_profile["reason"]
        else:
            route, status, score, decision, cap, reason = "", "", 0.0, "不列重點", 0.0, ""
        focus_types.append(route)
        focus_status.append(status)
        focus_score.append(score)
        focus_decision.append(decision)
        caps.append(cap)
        focus_reasons.append(reason)

    out["H42重點機會類型"] = focus_types
    out["H42重點狀態"] = focus_status
    out["H42重點分"] = focus_score
    out["H42重點決策"] = focus_decision
    out["H42建議倉位上限%"] = caps
    out["H42重點理由"] = focus_reasons
    out["H42版本"] = VERSION
    return out


def _focus_permission(row: pd.Series, route: str) -> str:
    if route == "強勢延續":
        return _s(row.get("H42強勢操作許可"))
    if route == "跌深價差":
        return _s(row.get("H42價差操作許可"))
    return ""


def build_focus_decision_table(frame: pd.DataFrame, strong_top: int = 2, bargain_top: int = 2) -> pd.DataFrame:
    """Return the one sheet/page the user should read first.

    It intentionally shows a small *reference* set, not every diagnostic column:
    up to two strong-route names and two bargain-route names.  WAIT is visible so
    a sell-off still produces useful watch targets, but WAIT never means buy.
    """
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame({"狀態": ["目前沒有可建立重點決策的有效候選。"]})
    work = frame.copy()
    if "H42版本" not in work.columns or not work.get("H42版本", pd.Series([], dtype=str)).astype(str).eq(VERSION).all():
        work = apply_dual_opportunity_engine(work)

    code = work.get("股票代號", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    work = work.loc[code.str.strip().ne("")].copy()
    if work.empty:
        return pd.DataFrame({"狀態": ["目前沒有可建立重點決策的有效候選。"]})

    strong_status = work.get("H42強勢狀態", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    bargain_status = work.get("H42價差狀態", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)

    strong = work.loc[strong_status.str.match(r"^(S-READY|S-PREP|S-WATCH)")].copy()
    bargain = work.loc[bargain_status.str.match(r"^(B-READY|B-WAIT)")].copy()
    for df, status_col, score_col in [
        (strong, "H42強勢狀態", "H42強勢分"), (bargain, "H42價差狀態", "H42價差分")
    ]:
        if not df.empty:
            _status_series = df[status_col].astype(str)
            df["_status_priority"] = _status_series.map(
                lambda x: 3 if "READY" in x else 2 if "PREP" in x else 1
            )
            df["_score"] = pd.to_numeric(df.get(score_col), errors="coerce").fillna(0.0)
            _v188_raw = df["V188股神作戰優先分"] if "V188股神作戰優先分" in df.columns else pd.Series([0.0] * len(df), index=df.index)
            df["_v188"] = pd.to_numeric(_v188_raw, errors="coerce").fillna(0.0)
            _sector_raw = df["H45族群主流分"] if "H45族群主流分" in df.columns else pd.Series([0.0] * len(df), index=df.index)
            df["_sector_score"] = pd.to_numeric(_sector_raw, errors="coerce").fillna(0.0)
            # Bargain WAIT should prefer a controllable structure over the largest
            # raw drawdown.  A falling knife may stay in the full diagnostic table
            # but must not crowd safer support/reclaim candidates off the first sheet.
            if status_col == "H42價差狀態":
                df["_structure_safe"] = df.get("H42落刀風險", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str).ne("高").astype(int)
                df.sort_values(["_status_priority", "_structure_safe", "_score", "_v188"], ascending=[False, False, False, False], inplace=True, kind="mergesort")
            else:
                df.sort_values(["_status_priority", "_sector_score", "_score", "_v188"], ascending=[False, False, False, False], inplace=True, kind="mergesort")

    selected: list[tuple[str, pd.Series]] = []
    used_codes: set[str] = set()
    # H45 第一張重點頁避免兩個強勢席位都被同一族群占滿。先各取一個
    # 主流族群代表，再用剩餘名額補最強個股，讓使用者同時看到「主線」
    # 與「領漲股」，而不是兩檔高度同質的股票。
    strong_limit = max(0, int(strong_top))
    used_sectors: set[str] = set()
    for _, row in strong.iterrows():
        if len([1 for route, _ in selected if route == "強勢延續"]) >= strong_limit:
            break
        c = _s(row.get("股票代號")); sector = _s(row.get("類別")) or _s(row.get("族群名稱"))
        if c and c not in used_codes and (not sector or sector not in used_sectors):
            selected.append(("強勢延續", row)); used_codes.add(c)
            if sector: used_sectors.add(sector)
    if len([1 for route, _ in selected if route == "強勢延續"]) < strong_limit:
        for _, row in strong.iterrows():
            if len([1 for route, _ in selected if route == "強勢延續"]) >= strong_limit:
                break
            c = _s(row.get("股票代號"))
            if c and c not in used_codes:
                selected.append(("強勢延續", row)); used_codes.add(c)
    for _, row in bargain.iterrows():
        if len([1 for route, _ in selected if route == "跌深價差"]) >= max(0, int(bargain_top)):
            break
        c = _s(row.get("股票代號"))
        if c and c not in used_codes:
            selected.append(("跌深價差", row))
            used_codes.add(c)

    if not selected:
        return pd.DataFrame({
            "狀態": ["今天沒有通過 H42 基本安全條件的強勢/跌深價差候選。空手不是硬規定；代表連『值得等待』的標的都不足。"],
            "操作原則": ["不要為了有推薦而買。先等資料、結構或守價條件改善。"],
        })

    rows: list[dict[str, Any]] = []
    for idx, (route, row) in enumerate(selected, start=1):
        status = _s(row.get("H42強勢狀態" if route == "強勢延續" else "H42價差狀態"))
        is_ready = "READY" in status
        entry = _first_num(row, ["主要進場參考價", "回測承接參考價", "預估進場點"], 0.0, positive=True)
        rows.append({
            "重點順位": idx,
            "機會類型": route,
            "狀態": status,
            "股票代號": _s(row.get("股票代號")),
            "股票名稱": _s(row.get("股票名稱")),
            "目前決策": ("條件成立可小量" if is_ready else "先等條件，不直接買"),
            "條件操作許可": _focus_permission(row, route),
            "最新價": _first_num(row, ["最新價"], 0.0, positive=True),
            "進場/承接參考": entry,
            "突破觸發價": _first_num(row, ["實戰觸發價", "突破確認參考價"], 0.0, positive=True),
            "觸發後守價": _first_num(row, ["觸發後守價", "守價回測參考價"], 0.0, positive=True),
            "停損價": _first_num(row, ["實戰停損參考", "停損參考"], 0.0, positive=True),
            "路徑分": _first_num(row, ["H42強勢分" if route == "強勢延續" else "H42價差分"], 0.0),
            "H45主流波段狀態": _s(row.get("H45主流波段狀態")),
            "H45主流波段分": _first_num(row, ["H45主流波段分"], 0.0),
            "H45族群主流分": _first_num(row, ["H45族群主流分"], 0.0),
            "V188/Trade/Risk": f"{_first_num(row, ['V188股神作戰優先分'], 0.0):.1f} / {_first_num(row, ['SuperAI Trade分'], 0.0):.1f} / {_first_num(row, ['Risk風控安全分'], 0.0):.1f}",
            "RR": _first_num(row, ["路徑風險報酬比", "SuperAI執行風報比"], 0.0),
            "隔日校準上漲機率%": _first_num(row, ["SuperAI校準後隔日上漲機率%", "H32隔日上漲機率%"], 0.0),
            "10日預估報酬%": _first_num(row, ["H32_10日預估報酬%", "10日預估報酬%"], 0.0),
            "今日漲幅%": _first_num(row, ["今日漲幅%"], 0.0),
            "近5日漲幅%": _first_num(row, ["近5日漲幅%"], 0.0),
            "近20日漲幅%": _first_num(row, ["近20日漲幅%"], 0.0),
            "建議倉位上限%": _first_num(row, ["H42建議倉位上限%"], 0.0),
            "大盤情境": _s(row.get("H42市場情境")) or _s(row.get("大盤風控層級")),
            "重點理由": _s(row.get("H42強勢理由" if route == "強勢延續" else "H42價差理由")),
        })
    result = pd.DataFrame(rows)
    preferred = [
        "重點順位", "股票代號", "股票名稱", "機會類型", "狀態", "目前決策", "條件操作許可",
        "最新價", "進場/承接參考", "突破觸發價", "觸發後守價", "停損價",
        "路徑分", "H45主流波段狀態", "H45主流波段分", "H45族群主流分", "V188/Trade/Risk", "RR",
        "隔日校準上漲機率%", "10日預估報酬%", "今日漲幅%", "近5日漲幅%", "近20日漲幅%",
        "建議倉位上限%", "大盤情境", "重點理由",
    ]
    return result[[c for c in preferred if c in result.columns]]


__all__ = ["VERSION", "H42_COLUMNS", "apply_dual_opportunity_engine", "build_focus_decision_table"]

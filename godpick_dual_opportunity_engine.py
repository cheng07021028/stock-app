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
import re
import pandas as pd

VERSION = "v191_h50_1_mainstream_buyable_execution_rr_20260825"

H42_COLUMNS = [
    "H42強勢分", "H42強勢狀態", "H42強勢操作許可", "H42強勢理由",
    "H42價差分", "H42價差狀態", "H42價差操作許可", "H42價差理由",
    "H42跌深旗標數", "H42止穩確認", "H42落刀風險", "H42市場情境",
    "H42市場共識分", "H42重點機會類型", "H42重點狀態", "H42重點分",
    "H42重點決策", "H42建議倉位上限%", "H42重點理由", "H42版本",
    "H49上漲潛力分", "H49潛力等級", "H49潛力階段", "H49可執行分",
    "H49交易決策", "H49交易許可", "H49波段位置分", "H49延伸風險扣分",
    "H49上漲潛力理由", "H49版本",
    "H50推薦優先分", "H50推薦等級", "H50推薦決策", "H50推薦許可",
    "H50推薦重複扣分", "H50推薦理由", "H50推薦版本", "H50版本",
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
    rr_route = _first_num(row, ["路徑風險報酬比", "SuperAI執行風報比", "風險報酬比"], 0.0, positive=True)
    rr_spot = _first_num(row, ["實戰風險報酬比", "AI戰術風報比"], 0.0, positive=True)
    # H50.1 execution authority: the first-sheet buy decision must use the RR of
    # the actual planned entry route.  A current-price/spot RR can look excellent
    # while the system is explicitly telling the user to wait for a later breakout;
    # using that spot RR would falsely upgrade an unbuyable setup.
    rr = rr_route if rr_route > 0 else rr_spot
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
        "stop": stop, "rr": rr, "rr_route": rr_route, "rr_spot": rr_spot, "chase": chase, "v188": v188, "trade": trade, "entry": entry, "risk": risk,
        "mainstream": mainstream, "funds": funds, "sector": sector, "ret1": ret1, "ret5": ret5, "ret20": ret20,
        "close": close_pos, "upper": upper, "support": support, "reversal": reversal, "amount": amount,
        "volume_ratio": volume_ratio, "exec_distance": exec_distance,
    }



def _zone_score(value: float, lo: float, hi: float, slope: float) -> float:
    """100 inside preferred swing zone, decays linearly outside it."""
    if lo <= value <= hi:
        return 100.0
    gap = (lo - value) if value < lo else (value - hi)
    return _clamp(100.0 - gap * slope)


def _h49_potential_profile(row: pd.Series, market: dict[str, Any]) -> dict[str, Any]:
    """H49 separates *upside potential* from *execution permission*.

    H47 correctly found leaders, but the first sheet still mixed early setups with
    already-extended leaders.  H49 asks a different question: among the current
    information set, which names have the best 1~10 session *structural upside*
    from their present swing position?  It deliberately rewards EARLY/PULLBACK
    and penalizes EXTENDED/chase risk.  Trade/Risk/RR decide only whether that
    potential may be acted on now.
    """
    m = _metrics(row)
    start = _first_num(row, ["H47起漲優先分", "H45起漲結構分"], 50.0)
    rs = _first_num(row, ["H47個股相對強度分", "H45個股領先分"], 50.0)
    sector = _first_num(row, ["H50族群可買主流分", "H45族群主流分"], 50.0)
    trend = _first_num(row, ["H45趨勢延續分"], 50.0)
    volume = _first_num(row, ["H45量價啟動分"], 50.0)
    onset = _first_num(row, ["H45起漲結構分"], 50.0)
    stage = _first_text(row, ["H50波段機會階段", "H47主流領先狀態", "H47波段階段"])
    trade_state = _first_text(row, ["H47交易候選狀態"])
    close_quality = _clamp(m["close"] - max(0.0, m["upper"] - 35.0) * 0.70)

    # Preferred wave position: not a falling knife, not a 20~50% extended chase.
    fit5 = _zone_score(m["ret5"], -1.0, 8.0, 7.0)
    fit20 = _zone_score(m["ret20"], 0.0, 24.0, 3.2)
    wave_position = _clamp(fit5 * 0.55 + fit20 * 0.45)

    base = (
        start * 0.24 + rs * 0.17 + sector * 0.15 + trend * 0.11
        + volume * 0.09 + onset * 0.08 + wave_position * 0.11 + close_quality * 0.05
    )

    # Stage expresses upside *from here*, not historical strength.
    stage_adj = 0.0
    if stage.startswith(("N-EARLY", "L-EARLY")):
        stage_adj += 5.0
    elif stage.startswith(("N-PULLBACK", "L-PULLBACK")):
        stage_adj += 4.0
    elif stage.startswith(("N-LEADER", "L-LEADER")):
        stage_adj += 1.0
    elif stage.startswith(("N-EXTENDED", "L-EXTENDED")):
        stage_adj -= 20.0
    elif stage.startswith(("N-MATURE", "N-RADAR", "L-WATCH")):
        stage_adj -= 6.0
    else:
        stage_adj -= 9.0

    extension_penalty = 0.0
    if m["ret5"] > 20:
        extension_penalty += 14.0
    elif m["ret5"] > 12:
        extension_penalty += 7.0
    if m["ret20"] > 50:
        extension_penalty += 12.0
    elif m["ret20"] > 35:
        extension_penalty += 6.0
    if m["chase"] >= 90:
        extension_penalty += 10.0
    elif m["chase"] >= 75:
        extension_penalty += 5.0
    # A broken medium-term trend belongs in the bargain route until it stabilizes.
    if m["ret20"] < -12:
        extension_penalty += 12.0
    elif m["ret20"] < -8:
        extension_penalty += 6.0
    if m["ret5"] < -6:
        extension_penalty += 7.0

    # Probability is not mature enough to dominate ranking.  It may only nudge
    # structural evidence by at most +/-2.5 points.
    prob = _first_num(row, ["SuperAI校準後隔日上漲機率%", "H32隔日上漲機率%", "SuperAI隔日上漲機率%"], 50.0)
    prob_nudge = max(-2.5, min(2.5, (prob - 50.0) * 0.10))
    potential = round(_clamp(base + stage_adj + prob_nudge - extension_penalty), 2)
    # H50: high potential on the first sheet must come from a fresh/mainline stage.
    # Mature/watch/no-mainstream names can remain research radar but may not be
    # labelled P1/P2 merely because their historical scores are high.
    if stage.startswith(("N-MATURE", "N-RADAR", "N-NO", "L-WATCH", "L-NO")):
        potential = min(potential, 59.9)

    if potential >= 72:
        tier = "P1｜高上漲潛力"
    elif potential >= 66:
        tier = "P2｜中高上漲潛力"
    elif potential >= 60:
        tier = "P3｜觀察潛力"
    else:
        tier = "P4｜非優先"

    data_block, data_reason = _data_block(row)
    extended = stage.startswith(("N-EXTENDED", "L-EXTENDED")) or trade_state.startswith("T-NO-CHASE")
    rr_known = m["rr"] > 0
    exec_safety = bool(
        not data_block and not market.get("hard")
        and m["risk"] >= 58 and m["trade"] >= 60
        and (not rr_known or m["rr"] >= 1.20)
        and (m["stop"] <= 0 or m["stop"] <= 7.0)
        and m["chase"] <= 62
        and (m["amount"] <= 0 or m["amount"] >= 120)
    )
    high_potential = tier.startswith(("P1", "P2"))
    if extended:
        decision = "A-NO-CHASE｜高強度但已延伸，只等回測"
        cap = 0.0
    elif high_potential and trade_state.startswith("T-READY") and exec_safety:
        decision = "A-READY｜高潛力＋交易條件完成"
        cap = 3.0 if market.get("defensive") else 5.0
    elif high_potential:
        decision = "A-PREP｜高潛力，等待觸發/回測與守價"
        cap = 0.0
    elif tier.startswith("P3"):
        decision = "A-WATCH｜有結構但不是第一優先"
        cap = 0.0
    else:
        decision = "A-NO｜上漲潛力不足"
        cap = 0.0

    permission = (
        f"回測承接約{m['main_entry']:.2f}或突破{m['trigger']:.2f}後守住{m['guard']:.2f}才執行；未成立=NO-TRADE。"
        if decision.startswith("A-READY") and (m["main_entry"] > 0 or m["trigger"] > 0)
        else "先觀察，不因潛力分直接買進；等待交易條件完成。"
    )
    if decision.startswith("A-NO-CHASE"):
        permission = "主流地位保留，但位置已延伸；禁止追價，只等明顯回測後重新評分。"
    reasons = [
        f"起漲{start:.1f}", f"相對強度{rs:.1f}", f"族群{sector:.1f}",
        f"趨勢{trend:.1f}", f"量價{volume:.1f}", f"波段位置{wave_position:.1f}",
        f"5日{m['ret5']:+.1f}%/20日{m['ret20']:+.1f}%",
    ]
    if extension_penalty > 0:
        reasons.append(f"延伸/破壞扣分-{extension_penalty:.1f}")
    if data_reason:
        reasons.append(data_reason)
    return {
        "potential": potential, "tier": tier, "stage": stage or "未分類", "exec_score": round(_clamp(m["trade"] * .35 + m["risk"] * .30 + m["entry"] * .20 + _clamp(50 + 15 * max(0, m["rr"] - 1)) * .15), 2),
        "decision": decision, "permission": permission, "wave_position": round(wave_position, 2),
        "extension_penalty": round(extension_penalty, 2), "cap": cap, "reason": "；".join(reasons), "m": m,
    }

def _h50_recommendation_profile(row: pd.Series, h49: dict[str, Any], market: dict[str, Any]) -> dict[str, Any]:
    """Final H50 buy-priority layer: mainstream freshness + setup + execution.

    It is intentionally stricter than H49 potential.  A stock with RR 0.3 or
    Trade 49 may still be interesting research, but it must never be shown as a
    first-page "worth buying" recommendation.
    """
    m = h49["m"]
    stage = _first_text(row, ["H50波段機會階段", "H47主流領先狀態"])
    life = _first_text(row, ["H50族群生命週期"])
    fresh_status = _first_text(row, ["H50主流購買狀態"])
    sector_buy = _first_num(row, ["H50族群可買主流分"], 50.0)
    stock_main = _first_num(row, ["H50主流購買優先分"], 50.0)
    start = _first_num(row, ["H47起漲優先分", "H45起漲結構分"], 50.0)
    rs = _first_num(row, ["H47個股相對強度分", "H45個股領先分"], 50.0)
    signal_fresh = _first_num(row, ["今日訊號新鮮分"], 50.0)
    repeats = _first_num(row, ["近5次入榜次數"], 0.0)
    consecutive = _first_num(row, ["連續入榜次數"], 0.0)
    repeat_pen = max(0.0, repeats - 2.0) * 4.0 + max(0.0, consecutive - 2.0) * 2.0
    if signal_fresh >= 65:
        repeat_pen *= 0.35
    repeat_pen = min(14.0, repeat_pen)
    h49_potential = float(h49.get("potential") or 0.0)
    h49_tier = str(h49.get("tier") or "")
    exec_score = float(h49.get("exec_score") or 0.0)
    base_score = _clamp(h49_potential * 0.32 + sector_buy * 0.23 + stock_main * 0.20 + start * 0.10 + rs * 0.07 + exec_score * 0.08 - repeat_pen * 0.70)

    data_block, data_reason = _data_block(row)
    stage_ok = stage.startswith(("N-EARLY", "N-PULLBACK", "N-LEADER", "L-EARLY", "L-PULLBACK", "L-LEADER"))
    sector_ok = life.startswith(("A0", "A1", "B1", "B2"))
    extended = stage.startswith(("N-EXTENDED", "L-EXTENDED"))
    rr_known = m["rr"] > 0
    ready_exec = bool(
        not data_block and not market.get("hard") and stage_ok and sector_ok
        and m["trade"] >= 62 and m["risk"] >= 60 and m["entry"] >= 55
        and (not rr_known or m["rr"] >= 1.25)
        and (m["stop"] <= 0 or m["stop"] <= 7.0) and m["chase"] <= 60
        and (m["amount"] <= 0 or m["amount"] >= 120)
    )
    prep_exec = bool(
        not data_block and not market.get("hard") and stage_ok and sector_ok
        and h49_tier.startswith(("P1", "P2")) and m["trade"] >= 52 and m["risk"] >= 52
        and (not rr_known or m["rr"] >= 1.15)
        and (m["stop"] <= 0 or m["stop"] <= 8.5) and m["chase"] <= 70
    )
    if extended:
        decision = "R-NO-CHASE｜真正主流但已延伸，只等回測"
        level = "R4｜禁止追價"
        cap = 0.0
    elif ready_exec and h49_tier.startswith(("P1", "P2")) and repeat_pen < 10:
        decision = "R-READY｜主流起漲＋交易條件完成"
        level = "R1｜優先條件推薦"
        cap = 3.0 if market.get("defensive") else 5.0
    elif prep_exec and repeat_pen < 12:
        decision = "R-PREP｜主流高潛力，等待合理買點"
        level = "R2｜重點等待"
        cap = 0.0
    elif fresh_status.startswith(("F-SETUP", "F-LEADER", "F-RADAR")) or stage_ok:
        decision = "R-RADAR｜主流/起漲存在，但風報比或交易條件不足"
        level = "R3｜主流雷達"
        cap = 0.0
    elif life.startswith("C"):
        decision = "R-MATURE｜成熟主流，不因歷史強度重複推薦"
        level = "R5｜成熟輪動"
        cap = 0.0
    else:
        decision = "R-NO｜非目前可買主流優先"
        level = "R6｜非優先"
        cap = 0.0

    permission = "先觀察，不直接買。"
    if decision.startswith("R-READY"):
        permission = (f"只做條件單：回測承接約{m['main_entry']:.2f}或突破{m['trigger']:.2f}後守住{m['guard']:.2f}；未成立=NO-TRADE。")
    elif decision.startswith("R-PREP"):
        permission = "主流與上漲潛力成立，但價格/風報比尚未完整；等回測、突破與守價確認後再升R-READY。"
    elif decision.startswith("R-NO-CHASE"):
        permission = "保留主流身份但禁止追價；只有明顯回測重新形成低風險結構才重評。"
    reasons = [
        f"{life or '生命週期未知'}", f"{stage or '階段未知'}", f"族群可買{sector_buy:.1f}",
        f"主流購買{stock_main:.1f}", f"H49潛力{h49_potential:.1f}",
        f"Trade{m['trade']:.1f}/Risk{m['risk']:.1f}/RR{m['rr']:.2f}", f"重複扣分{repeat_pen:.1f}"
    ]
    if data_reason:
        reasons.append(data_reason)
    return {"score": round(base_score, 2), "level": level, "decision": decision, "permission": permission,
            "repeat_penalty": round(repeat_pen, 2), "reason": "；".join(reasons), "cap": cap}


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
    """H47 strong route: first say *who leads*, then say *whether it is buyable*.

    H45 mixed market leadership and trade safety in one status.  That hid the
    true leaders whenever their Risk/Trade score was low, while safer-but-weaker
    names occupied the first sheet.  H47 deliberately separates the two facts:
    ``H47主流領先狀態`` is pure market structure; ``H47交易候選狀態`` is the
    execution permission.  A real leader remains visible even when it is marked
    NO-CHASE / PREP.
    """
    m = _metrics(row)
    h47_status = _first_text(row, ["H47主流領先狀態", "H47波段階段"])
    h47_trade = _first_text(row, ["H47交易候選狀態"])
    h47_start = _first_num(row, ["H47起漲優先分"], 0.0)
    h47_rs = _first_num(row, ["H47個股相對強度分"], 0.0)
    h47_reason = _first_text(row, ["H47主流領先理由"])
    h45_sector = _first_num(row, ["H45族群主流分"], 0.0)
    h47_available = bool(h47_status or h47_start > 0 or h47_rs > 0)
    data_block, data_reason = _data_block(row)

    if h47_available:
        if h47_trade.startswith("T-READY") and h47_status.startswith(("L-EARLY", "L-PULLBACK", "L-LEADER")) and not data_block and not market.get("hard"):
            status = "S-READY｜主流條件進場"
            cap = 3.0 if market.get("defensive") else 5.0
            permission = (
                f"主流股只做條件單：優先等 {m['main_entry']:.2f} 附近承接；"
                f"若走突破，需站上 {m['trigger']:.2f} 且守住 {m['guard']:.2f}。未成立=NO-TRADE。"
                if m["main_entry"] > 0 else
                "主流結構已通過，但仍須回測承接或突破＋守價確認後才小量；未成立=NO-TRADE。"
            )
        elif h47_status.startswith(("L-EARLY", "L-PULLBACK")):
            status = "S-PREP｜主流起漲/回檔再攻，等交易確認"
            cap = 0.0
            permission = "主流與起漲結構已成立，但交易條件尚未完整；等量價、回測/突破與守價確認，不直接買。"
        elif h47_status.startswith("L-EXTENDED"):
            status = "S-LEADER｜主流領漲但已延伸，禁止追價"
            cap = 0.0
            permission = "這是真正主流領漲股，但已進延伸段；只等明顯回測承接，不追高。"
        elif h47_status.startswith("L-LEADER"):
            status = "S-LEADER｜主流領漲核心，等低風險買點"
            cap = 0.0
            permission = "主流領漲地位成立；Risk/Trade/價格位置未取得安全交易許可前只觀察，等回測或重新整理。"
        elif h47_status.startswith("L-WATCH"):
            status = "S-WATCH｜主流觀察"
            cap = 0.0
            permission = "屬主流觀察股，但尚未形成起漲/可交易結構，不直接買。"
        else:
            status = "S-NO｜非主流領先優先"
            cap = 0.0
            permission = "H47未通過當前主流族群＋個股相對強度，不列強勢路徑。"
        if data_block or market.get("hard"):
            if status.startswith("S-READY"):
                status = "S-WATCH｜主流結構存在，但資料/市場硬封鎖"
                cap = 0.0
                permission = "主流結構可保留研究，但資料或市場硬封鎖存在，禁止新倉。"
        score = _clamp(h47_start * 0.50 + h47_rs * 0.30 + h45_sector * 0.20)
        reasons = [x for x in [h47_reason, data_reason if data_block else "", "極端市場只觀察" if market.get("hard") else ""] if x]
        return {
            "score": round(score, 2), "status": status, "permission": permission, "cap": cap,
            "reason": "；".join(reasons) or f"H47起漲{h47_start:.1f}／相對強度{h47_rs:.1f}／族群{h45_sector:.1f}",
            "m": m,
        }

    # Legacy fallback for frames that have not yet passed H47.
    h45_status = _first_text(row, ["H45主流波段狀態"])
    h45_score = _first_num(row, ["H45主流波段分"], 0.0)
    h45_reason = _first_text(row, ["H45主流波段理由"])
    h45_available = bool(h45_status or h45_score > 0)
    if h45_available:
        if h45_status.startswith("M-READY") and not data_block and not market.get("hard"):
            status, cap = "S-READY｜主流波段條件進場", (3.0 if market.get("defensive") else 5.0)
            permission = "主流波段條件成立後才小量；突破/回測後必須守價，未成立=NO-TRADE。"
        elif h45_status.startswith("M-PREP"):
            status, cap = "S-PREP｜主流起漲前兆，等量價確認", 0.0
            permission = "主流/領先/起漲前兆已形成，但尚未取得交易許可。"
        elif h45_status.startswith("M-WATCH"):
            status, cap = "S-WATCH｜主流/個股強勢觀察", 0.0
            permission = "列入主流觀察，不直接追價。"
        else:
            status, cap = "S-NO｜非主流波段優先", 0.0
            permission = "H45未通過主流波段條件。"
        return {"score": round(_clamp(h45_score),2), "status":status, "permission":permission, "cap":cap,
                "reason": h45_reason or data_reason, "m":m}

    rr_score = _clamp(45.0 + 14.0 * max(0.0, m["rr"] - 1.0))
    recent_score = _clamp(50.0 + 5.0 * m["ret1"] + 1.5 * m["ret5"])
    score = (0.20*m["trade"] + 0.16*m["risk"] + 0.13*m["entry"] + 0.13*m["mainstream"] +
             0.09*m["sector"] + 0.07*m["funds"] + 0.08*_clamp(m["close"]) +
             0.06*_clamp(100.0-m["chase"]) + 0.04*rr_score + 0.04*recent_score)
    score = round(_clamp(score), 2)
    hard_safety = bool(m["trade"] >= 70 and m["risk"] >= 62 and m["rr"] >= 1.25 and 0 < m["stop"] <= 7.0 and m["chase"] <= 60 and (m["amount"] <= 0 or m["amount"] >= 120))
    structure = bool(m["close"] >= 50 and m["upper"] <= 45 and m["ret1"] >= -1.5 and m["ret5"] >= -6)
    leadership = bool(m["mainstream"] >= 60 or m["sector"] >= 60 or m["funds"] >= 62 or m["ret1"] >= 1.5)
    ready = bool(not data_block and not market.get("hard") and hard_safety and structure and leadership and m["exec_distance"] <= 6.5 and score >= (68 if market.get("defensive") else 65))
    if ready:
        status, cap, permission = "S-READY｜強勢條件進場", (3.0 if market.get("defensive") else 5.0), "條件成立後才小量；未成立=NO-TRADE。"
    elif hard_safety and score >= 60 and (leadership or structure):
        status, cap, permission = "S-WATCH｜強勢但等待更好買點", 0.0, "不直接追價，等待更好的回測/突破條件。"
    else:
        status, cap, permission = "S-NO｜非強勢路徑", 0.0, "不列強勢路徑。"
    return {"score":score, "status":status, "permission":permission, "cap":cap, "reason":data_reason, "m":m}

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
    h49_profiles = [_h49_potential_profile(row, market) for _, row in out.iterrows()]
    h50_profiles = [_h50_recommendation_profile(row, p49, market) for p49, (_, row) in zip(h49_profiles, out.iterrows())]

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
    out["H49上漲潛力分"] = [p["potential"] for p in h49_profiles]
    out["H49潛力等級"] = [p["tier"] for p in h49_profiles]
    out["H49潛力階段"] = [p["stage"] for p in h49_profiles]
    out["H49可執行分"] = [p["exec_score"] for p in h49_profiles]
    out["H49交易決策"] = [p["decision"] for p in h49_profiles]
    out["H49交易許可"] = [p["permission"] for p in h49_profiles]
    out["H49波段位置分"] = [p["wave_position"] for p in h49_profiles]
    out["H49延伸風險扣分"] = [p["extension_penalty"] for p in h49_profiles]
    out["H49上漲潛力理由"] = [p["reason"] for p in h49_profiles]
    out["H49版本"] = VERSION
    out["H50推薦優先分"] = [p["score"] for p in h50_profiles]
    out["H50推薦等級"] = [p["level"] for p in h50_profiles]
    out["H50推薦決策"] = [p["decision"] for p in h50_profiles]
    out["H50推薦許可"] = [p["permission"] for p in h50_profiles]
    out["H50推薦重複扣分"] = [p["repeat_penalty"] for p in h50_profiles]
    out["H50推薦理由"] = [p["reason"] for p in h50_profiles]
    out["H50版本"] = VERSION

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


def _h50_focus_entry_reference(row: pd.Series) -> float:
    """Return a numeric pullback/entry reference for the first-sheet decision table.

    Older rows often store the useful pullback level only inside a text field such
    as ``預估進場點=突破 1080 或回測 973.70 確認``.  Returning 0 there made
    the H50 sheet look unusable even when the executable plan was actually known.
    """
    direct = _first_num(row, [
        "主要進場參考價", "回測承接參考價", "推薦買點_拉回", "拉回買點", "承接參考價"
    ], 0.0, positive=True)
    if direct > 0:
        return direct
    text = _first_text(row, ["預估進場點", "主要進場路徑", "進場路徑"])
    if not text:
        return 0.0
    for pat in [r"(?:回測|承接|拉回)\s*([0-9]+(?:\.[0-9]+)?)", r"([0-9]+(?:\.[0-9]+)?)\s*(?:附近|確認)"]:
        m = re.search(pat, text)
        if m:
            try:
                value = float(m.group(1))
                if value > 0:
                    return value
            except Exception:
                pass
    return 0.0


def build_focus_decision_table(frame: pd.DataFrame, strong_top: int = 3, bargain_top: int = 1) -> pd.DataFrame:
    """H50 first sheet: buyable mainstream first, bargain READY only as fallback.

    R-READY/R-PREP are the only mainstream names allowed on the first page.
    R-RADAR/MATURE/NO-CHASE stay in the H50/H47 radar sheets.  This prevents
    RR 0.3 / Trade 49 names from being visually presented as recommendations.
    """
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame({"狀態": ["目前沒有可建立 H50 主流可買決策的有效候選。"]})
    work = frame.copy()
    if "H50版本" not in work.columns or not work.get("H50版本", pd.Series([], dtype=str)).astype(str).eq(VERSION).all():
        work = apply_dual_opportunity_engine(work)
    code = work.get("股票代號", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str).str.strip()
    work = work.loc[code.ne("")].copy()
    dec = work.get("H50推薦決策", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    main = work.loc[dec.str.startswith(("R-READY", "R-PREP"))].copy()
    for c in ["H50推薦優先分", "H50族群可買主流分", "H50主流購買優先分", "H49上漲潛力分", "H47起漲優先分", "H47個股相對強度分", "V188股神作戰優先分"]:
        if c not in main.columns:
            main[c] = 0.0
        main[c] = pd.to_numeric(main[c], errors="coerce").fillna(0.0)
    if not main.empty:
        main["_ready"] = main["H50推薦決策"].astype(str).str.startswith("R-READY").astype(int)
        main.sort_values(["_ready", "H50推薦優先分", "H50族群可買主流分", "H50主流購買優先分", "H49上漲潛力分"], ascending=False, inplace=True, kind="mergesort")

    max_main = max(1, int(strong_top))
    selected: list[pd.Series] = []
    used_codes: set[str] = set()
    sector_count: dict[str, int] = {}
    # First pass: one per sector; second pass: allow a second only if seats remain.
    for sector_cap in (1, 2):
        if main.empty:
            break
        for _, row in main.iterrows():
            code = _s(row.get("股票代號"))
            sector = _s(row.get("類別")) or _s(row.get("族群名稱")) or "未分類"
            if not code or code in used_codes or sector_count.get(sector, 0) >= sector_cap:
                continue
            selected.append(row); used_codes.add(code); sector_count[sector] = sector_count.get(sector, 0) + 1
            if len(selected) >= max_main:
                break
        if len(selected) >= max_main:
            break

    # Keep the independent sell-off opportunity path, but only B-READY can enter
    # this first page and only as the optional last slot.
    if int(bargain_top) > 0 and len(selected) < max_main + int(bargain_top):
        bstat = work.get("H42價差狀態", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
        bargain = work.loc[bstat.str.startswith("B-READY")].copy()
        if not bargain.empty:
            for c in ["H42價差分", "Risk風控安全分", "SuperAI Trade分", "實戰操作品質分", "V188股神作戰優先分"]:
                if c not in bargain.columns: bargain[c] = 0.0
                bargain[c] = pd.to_numeric(bargain[c], errors="coerce").fillna(0.0)
            bargain["_H50_trade_sort"] = bargain.get("SuperAI Trade分", pd.Series([0.0] * len(bargain), index=bargain.index)).where(pd.to_numeric(bargain.get("SuperAI Trade分", 0), errors="coerce").fillna(0.0).gt(0), bargain.get("實戰操作品質分", 0))
            bargain["_H50_trade_sort"] = pd.to_numeric(bargain["_H50_trade_sort"], errors="coerce").fillna(0.0)
            bargain.sort_values(["H42價差分", "Risk風控安全分", "_H50_trade_sort"], ascending=False, inplace=True, kind="mergesort")
            for _, row in bargain.iterrows():
                code = _s(row.get("股票代號"))
                if code and code not in used_codes:
                    row = row.copy(); row["_H50_bargain"] = True
                    selected.append(row); used_codes.add(code)
                    if sum(bool(_s(x.get("_H50_bargain"))) for x in selected) >= int(bargain_top): break

    if not selected:
        # H50.1 usability guard: no executable recommendation does not mean the
        # first sheet should be blank.  Surface at most three *fresh mainstream*
        # R-RADAR names as explicit research references, never as buy calls.
        # Mature/extended themes remain excluded, so a previously hot sector such
        # as shipping cannot keep filling the first page simply because it stayed liquid.
        radar_text = work.get("H50推薦決策", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
        radar = work.loc[radar_text.str.startswith("R-RADAR")].copy()
        if not radar.empty:
            for c in ["H50推薦優先分", "H50主流購買優先分", "H50族群可買主流分", "H49上漲潛力分"]:
                if c not in radar.columns:
                    radar[c] = 0.0
                radar[c] = pd.to_numeric(radar[c], errors="coerce").fillna(0.0)
            radar.sort_values(["H50推薦優先分", "H50主流購買優先分", "H50族群可買主流分", "H49上漲潛力分"], ascending=False, inplace=True, kind="mergesort")
            radar_selected: list[pd.Series] = []
            radar_codes: set[str] = set()
            radar_sectors: set[str] = set()
            for _, rrw in radar.iterrows():
                code = _s(rrw.get("股票代號")); sector = _s(rrw.get("類別")) or _s(rrw.get("族群名稱")) or "未分類"
                if not code or code in radar_codes or sector in radar_sectors:
                    continue
                radar_selected.append(rrw.copy()); radar_codes.add(code); radar_sectors.add(sector)
                if len(radar_selected) >= min(3, max_main):
                    break
            if len(radar_selected) < min(3, max_main):
                for _, rrw in radar.iterrows():
                    code = _s(rrw.get("股票代號"))
                    if not code or code in radar_codes:
                        continue
                    radar_selected.append(rrw.copy()); radar_codes.add(code)
                    if len(radar_selected) >= min(3, max_main):
                        break
            for rrw in radar_selected:
                rrw["_H50_reference_only"] = True
                selected.append(rrw)
        if not selected:
            radar_n = int(radar_text.str.startswith("R-RADAR").sum())
            return pd.DataFrame({
                "狀態": ["今天沒有通過H50『主流＋起漲＋可接受執行RR』的R-READY/R-PREP，也沒有足夠的新鮮主流雷達可列。"],
                "主流雷達檔數": [radar_n],
                "操作原則": ["維持空手；成熟主流、低風報比與重複推薦不為湊名額補位。"],
            })

    rows: list[dict[str, Any]] = []
    for idx, row in enumerate(selected, 1):
        is_bargain = bool(_s(row.get("_H50_bargain")))
        is_reference = bool(_s(row.get("_H50_reference_only")))
        decision = "B-READY｜跌深價差條件進場" if is_bargain else _s(row.get("H50推薦決策"))
        route = "跌深價差" if is_bargain else "主流潛力觀察｜非買進推薦" if is_reference else "主流起漲/回檔再攻"
        rows.append({
            "重點順位": idx, "股票代號": _s(row.get("股票代號")), "股票名稱": _s(row.get("股票名稱")),
            "類別": _s(row.get("類別")) or _s(row.get("族群名稱")), "推薦類型": route,
            "H50推薦決策": decision, "H50推薦優先分": _first_num(row, ["H50推薦優先分"], 0.0),
            "H50族群生命週期": _s(row.get("H50族群生命週期")), "H50波段機會階段": _s(row.get("H50波段機會階段")),
            "H50族群可買主流分": _first_num(row, ["H50族群可買主流分"], 0.0),
            "H50主流購買優先分": _first_num(row, ["H50主流購買優先分"], 0.0),
            "H49上漲潛力分": _first_num(row, ["H49上漲潛力分"], 0.0), "H49潛力等級": _s(row.get("H49潛力等級")),
            "目前決策": ("只觀察｜不是買進推薦" if is_reference else "條件成立可小量" if decision.startswith(("R-READY", "B-READY")) else "主流高潛力｜等待合理買點"),
            "交易許可": ("非買進名單；等路徑RR、Trade/Risk與買點條件完成後才可升R-PREP/R-READY。" if is_reference else _s(row.get("H42價差操作許可")) if is_bargain else _s(row.get("H50推薦許可"))),
            "最新價": _first_num(row, ["最新價"], 0.0, positive=True),
            "進場/承接參考": _h50_focus_entry_reference(row),
            "突破觸發價": _first_num(row, ["實戰觸發價", "突破確認參考價"], 0.0, positive=True),
            "觸發後守價": _first_num(row, ["觸發後守價", "守價回測參考價"], 0.0, positive=True),
            "停損價": _first_num(row, ["實戰停損參考", "停損參考"], 0.0, positive=True),
            "RR": _first_num(row, ["路徑風險報酬比", "SuperAI執行風報比", "風險報酬比", "實戰風險報酬比"], 0.0),
            "現價RR": _first_num(row, ["實戰風險報酬比", "AI戰術風報比"], 0.0),
            "RR口徑": _s(row.get("風報比計算口徑")) or "路徑RR優先；現價RR只供參考",
            "SuperAI Trade分": _first_num(row, ["SuperAI Trade分", "實戰操作品質分", "可操作分", "進場可執行分"], 0.0), "Risk風控安全分": _first_num(row, ["Risk風控安全分", "風控安全分", "Risk風控分"], 0.0),
            "今日/5日/20日": f"{_first_num(row,['今日漲幅%'],0):+.1f}% / {_first_num(row,['近5日漲幅%'],0):+.1f}% / {_first_num(row,['近20日漲幅%'],0):+.1f}%",
            "近5次入榜次數": _first_num(row, ["近5次入榜次數"], 0.0), "今日訊號新鮮分": _first_num(row, ["今日訊號新鮮分"], 0.0),
            "AI重點理由": _s(row.get("H42價差理由")) if is_bargain else _s(row.get("H50推薦理由")),
        })
    return pd.DataFrame(rows)


__all__ = ["VERSION", "H42_COLUMNS", "apply_dual_opportunity_engine", "build_focus_decision_table"]

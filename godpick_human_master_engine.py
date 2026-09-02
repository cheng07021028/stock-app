# -*- coding: utf-8 -*-
"""V191-H51 professional leader/pivot decision engine.

This layer intentionally separates four questions that were previously mixed:
1) Is money actually concentrated in a current mainstream sector?
2) Is the stock a real leader / fresh early-stage or pullback-reclaim setup?
3) Is the setup technically ready (pivot, volume, close quality, liquidity)?
4) Is the *published entry path* tradable now (RR/risk/entry/stop/chase)?

The design is inspired by classic discretionary growth/leader trading discipline:
market/sector first, leader second, pivot/base/reclaim third, execution last.  It
never hard-codes a sector name and never upgrades an extended leader merely
because it has been strong in the past.
"""
from __future__ import annotations

from typing import Any, Iterable
from datetime import date, datetime
import math
import re
import pandas as pd

VERSION = "v191_h56_authority_preopen_two_stage_truth_20260902"

H51_COLUMNS = [
    "H51族群主線分", "H51個股領漲品質分", "H51Pivot起漲分", "H51量價確認分",
    "H51流動性分", "H51基本面資金分", "H51主線新鮮分", "H51重複推薦扣分",
    "H51發動潛力分", "H51專業參考分", "H51可執行分", "H51市場地位", "H51交易許可", "H51推薦等級",
    "H51急跌收復狀態",
    "H51路徑RR", "H51RR口徑", "H51推薦理由", "H51版本",
]

H53_COLUMNS = [
    "H53族群共振分", "H53領漲集群分", "H53隔日優先分", "H53參考層級",
    "H53族群廣度分", "H53族群攻擊分", "H53族群量能分", "H53族群資金分",
    "H53族群樣本可信度", "H53分類稀釋扣分", "H53版本",
]

H54_COLUMNS = [
    "H54主流延續分", "H54可執行確認分", "H54耗竭風險分", "H54隔夜風險扣分",
    "H54資訊空窗風險", "H54證據品質分", "H54輪動備援分", "H54隔日真相分",
    "H54決策層級", "H54決策理由", "H54版本",
]

H55_COLUMNS = [
    "H55主線延續路徑分", "H55反轉點火路徑分", "H55逆風韌性分", "H55催化代理分",
    "H55回補雷達分", "H55雙路徑隔日分", "H55機會型態", "H55參考層級",
    "H55決策理由", "H55版本",
]

H56_COLUMNS = [
    "H56上游權威層級", "H56隔夜證據狀態", "H56盤前重驗需求", "H56T1確認分",
    "H56最終參考層級", "H56決策理由", "H56版本",
]

_BROAD_PARENT_BUCKETS = {"半導體業", "電子零組件業", "光電業", "其他電子業", "電腦及週邊設備業"}

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
        t = str(v).strip().replace(",", "").replace("％", "%")
        if t.endswith("%"):
            t = t[:-1].strip()
        if not t or t.lower() in _BLANK:
            return float(default)
        x = float(t)
        return x if math.isfinite(x) else float(default)
    except Exception:
        return float(default)


def _first_num(row: pd.Series, names: Iterable[str], default: float = 0.0, positive: bool = False) -> float:
    fallback = None
    for c in names:
        if c not in row.index or not _s(row.get(c)):
            continue
        x = _f(row.get(c), float("nan"))
        if not math.isfinite(x):
            continue
        if fallback is None:
            fallback = float(x)
        if not positive or x > 0:
            return float(x)
    return float(default if fallback is None else fallback)


def _first_text(row: pd.Series, names: Iterable[str], default: str = "") -> str:
    for c in names:
        if c in row.index:
            t = _s(row.get(c))
            if t:
                return t
    return default


def _clip(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(x)))


def _zone(v: float, lo: float, hi: float, slope: float) -> float:
    if lo <= v <= hi:
        return 100.0
    gap = lo - v if v < lo else v - hi
    return _clip(100.0 - gap * slope)


def _parse_entry(text: str) -> float:
    if not text:
        return 0.0
    # Prefer pullback/reclaim prices over breakout prices for execution RR.
    for pat in [
        r"(?:回測|承接|拉回)\s*([0-9]+(?:\.[0-9]+)?)",
        r"(?:進場|買點)\s*([0-9]+(?:\.[0-9]+)?)",
        r"(?:突破|觸發)\s*([0-9]+(?:\.[0-9]+)?)",
    ]:
        m = re.search(pat, str(text))
        if m:
            try:
                x = float(m.group(1))
                if x > 0:
                    return x
            except Exception:
                pass
    return 0.0


def _route_rr(row: pd.Series) -> tuple[float, str]:
    rr = _first_num(row, ["路徑風險報酬比", "SuperAI執行風報比", "風險報酬比"], 0.0, positive=True)
    if rr > 0:
        return rr, "路徑RR"
    entry = _first_num(row, ["主要進場參考價", "回測承接參考價", "推薦買點_拉回", "拉回買點"], 0.0, positive=True)
    if entry <= 0:
        entry = _parse_entry(_first_text(row, ["預估進場點", "主要進場路徑", "進場路徑"]))
    stop = _first_num(row, ["實戰停損參考", "停損參考", "停損價_隔日"], 0.0, positive=True)
    target = _first_num(row, ["第一壓力價", "賣出目標1", "AI趨勢延伸目標價"], 0.0, positive=True)
    if entry > 0 and stop > 0 and target > entry and stop < entry:
        risk = entry - stop
        if risk > 0:
            return (target - entry) / risk, "依公開進場/停損/壓力重算"
    spot = _first_num(row, ["實戰風險報酬比", "AI戰術風報比"], 0.0, positive=True)
    return (spot, "現價RR fallback｜只供參考") if spot > 0 else (0.0, "缺路徑RR")


def _liquidity_score(amount_m: float, avg_m: float) -> float:
    a = max(amount_m, avg_m * 0.65)
    if a >= 3000: return 100.0
    if a >= 1500: return 95.0
    if a >= 800: return 88.0
    if a >= 500: return 82.0
    if a >= 300: return 74.0
    if a >= 150: return 62.0
    if a >= 80: return 48.0
    return 30.0


def _is_blocked(row: pd.Series) -> bool:
    blob = "｜".join(_s(row.get(c)) for c in [
        "操作許可", "V188交易許可", "正式推薦排除原因", "進場阻擋原因", "最終操作結論",
        "風控否決旗標", "掃描品質狀態", "K線資料新鮮度",
    ] if c in row.index)
    hard = ["LOCKDOWN", "禁止所有新倉", "全面禁買", "資料待更新", "K線落後", "WAIT-DATA"]
    return any(x in blob.upper() for x in hard)



def _score100(v: Any, default: float = 50.0) -> float:
    """Normalize common score/ratio fields to a conservative 0-100 scale."""
    x = _f(v, default)
    if 0.0 <= x <= 1.0:
        x *= 100.0
    return _clip(x)


def _row_score100(row: pd.Series, names: Iterable[str], default: float = 50.0) -> float:
    for c in names:
        if c in row.index and _s(row.get(c)):
            return _score100(row.get(c), default)
    return float(default)


def _h53_group_context(work: pd.DataFrame) -> dict[str, dict[str, float]]:
    """Build cross-sectional sector breadth/attack/flow context.

    H53 deliberately uses the whole candidate universe instead of judging a sector
    only from the average quality of its top few stocks. Small groups are shrunk
    toward neutral to avoid one-stock themes winning the ranking. Broad parent
    industry buckets remain visible but receive a dilution penalty.
    """
    if work is None or work.empty:
        return {}
    out: dict[str, dict[str, float]] = {}
    sector_series = work.get("類別", pd.Series(["未分類"] * len(work), index=work.index)).fillna("未分類").astype(str).str.strip()
    for sector, idxs in sector_series.groupby(sector_series).groups.items():
        g = work.loc[list(idxs)].copy()
        n = max(1, len(g))
        ret1 = pd.to_numeric(g.get("今日漲幅%", 0), errors="coerce").fillna(0.0)
        vr = pd.to_numeric(g.get("當日量比", g.get("均量比", 1.0)), errors="coerce").fillna(1.0)
        ignition = pd.to_numeric(g.get("H51發動潛力分", 50), errors="coerce").fillna(50.0)
        leader = pd.to_numeric(g.get("H51個股領漲品質分", 50), errors="coerce").fillna(50.0)
        h51sector = pd.to_numeric(g.get("H51族群主線分", 50), errors="coerce").fillna(50.0)
        market = g.get("H51市場地位", pd.Series([""] * n, index=g.index)).fillna("").astype(str)

        adv = float((ret1 > 0).mean() * 100.0)
        strong = float((ret1 >= 2.0).mean() * 100.0)
        volume_confirm = float((vr >= 1.15).mean() * 100.0)
        setup_breadth = float((market.str.startswith(("HM-EARLY", "HM-PULLBACK", "HM-LEADER", "HM-SETUP")) & ignition.ge(64)).mean() * 100.0)

        ext_breadth = float(pd.Series([_row_score100(r, ["族群廣度分", "同族群強勢比例", "H45族群5日上漲比例%"], 50.0) for _, r in g.iterrows()]).mean())
        attack = float(pd.Series([_row_score100(r, ["族群攻擊強度", "族群資金流分數", "資金攻擊有效分"], 50.0) for _, r in g.iterrows()]).mean())
        volume_score = float(pd.Series([_row_score100(r, ["族群成交額分", "同族群平均量能分"], 50.0) for _, r in g.iterrows()]).mean())
        fund_flow = float(pd.Series([_row_score100(r, ["族群資金流分數", "主流資金分", "主流族群回饋分"], 50.0) for _, r in g.iterrows()]).mean())

        raw_breadth = _clip(adv * 0.28 + strong * 0.24 + volume_confirm * 0.18 + setup_breadth * 0.15 + ext_breadth * 0.15)
        sample_conf = _clip(35.0 + min(n, 8) * 8.125)  # n=1 ->43.1, n>=8 ->100
        shrink = sample_conf / 100.0
        breadth = _clip(50.0 + (raw_breadth - 50.0) * shrink)
        attack_s = _clip(50.0 + (attack - 50.0) * shrink)
        volume_s = _clip(50.0 + (volume_score - 50.0) * shrink)
        fund_s = _clip(50.0 + (fund_flow - 50.0) * shrink)
        top_ign = float(ignition.nlargest(min(3, n)).mean())
        top_leader = float(leader.nlargest(min(3, n)).mean())
        base_sector = float(h51sector.mean())
        dilution_penalty = 7.0 if sector in _BROAD_PARENT_BUCKETS and n >= 5 else 3.0 if sector in _BROAD_PARENT_BUCKETS else 0.0
        resonance = _clip(
            base_sector * 0.20 + top_ign * 0.18 + top_leader * 0.12
            + breadth * 0.20 + attack_s * 0.12 + volume_s * 0.08 + fund_s * 0.10
            - dilution_penalty
        )
        out[sector or "未分類"] = {
            "sample_n": float(n), "confidence": round(sample_conf, 2),
            "breadth": round(breadth, 2), "attack": round(attack_s, 2),
            "volume": round(volume_s, 2), "fund": round(fund_s, 2),
            "top_ignition": round(top_ign, 2), "top_leader": round(top_leader, 2),
            "resonance": round(resonance, 2), "dilution": round(dilution_penalty, 2),
        }
    return out



def _date_value(v: Any) -> date | None:
    t = _s(v)
    if not t:
        return None
    try:
        return pd.to_datetime(t, errors="coerce").date()
    except Exception:
        return None


def _datetime_value(v: Any) -> datetime | None:
    t = _s(v)
    if not t:
        return None
    try:
        x = pd.to_datetime(t, errors="coerce")
        if pd.isna(x):
            return None
        return x.to_pydatetime() if hasattr(x, "to_pydatetime") else x
    except Exception:
        return None


def _truthy(v: Any) -> bool | None:
    if isinstance(v, bool):
        return v
    t = _s(v).lower()
    if t in {"true", "1", "yes", "y", "是", "正式", "通過"}:
        return True
    if t in {"false", "0", "no", "n", "否", "未通過"}:
        return False
    return None


def _authority_state(row: pd.Series) -> tuple[str, str]:
    """Read upstream Formal/V188 permission without allowing H51 to overrule it.

    UNKNOWN preserves standalone/synthetic compatibility.  Once Page07 supplies
    explicit authority fields, negative Formal/V188 evidence is a hard ceiling:
    H51/H55 may rank/research the stock but may not manufacture BUY-READY.
    """
    names = [
        "是否正式推薦", "正式推薦分區", "V188正式推薦資格", "V188交易許可",
        "操作許可", "最終操作結論", "正式推薦動作", "推薦升級判定路徑",
    ]
    present = any(c in row.index and _s(row.get(c)) for c in names)
    if not present:
        return "UNKNOWN", "未提供Formal/V188權威欄位；保留原引擎相容性"

    official = _truthy(row.get("是否正式推薦")) if "是否正式推薦" in row.index else None
    zone = _first_text(row, ["正式推薦分區"])
    qual = _first_text(row, ["V188正式推薦資格"])
    permit = _first_text(row, ["V188交易許可", "操作許可"])
    conclusion = _first_text(row, ["最終操作結論"])
    action = _first_text(row, ["正式推薦動作"])
    path = _first_text(row, ["推薦升級判定路徑"])
    text = "｜".join([zone, qual, permit, conclusion, action, path]).upper()

    explicit_formal = bool(
        official is True
        or zone.startswith(("正式主推薦", "正式推薦"))
        or (qual and (qual.startswith(("是｜", "通過｜")) or "FORMAL-READY" in qual.upper()))
        or permit.upper().startswith(("ALLOW", "FORMAL-READY"))
    )
    if explicit_formal:
        return "FORMAL", "上游Formal/V188已明確授權正式推薦"

    a_minus = ("A-" in conclusion.upper() or "準主推薦" in conclusion or "條件推薦" in conclusion or "A-" in path.upper())
    hard_nonformal = (
        official is False
        or (qual and ("否" in qual or "不越權" in qual))
        or any(k in permit.upper() for k in ["WAIT-", "RADAR", "TRACK-ONLY", "禁止", "不得"])
        or any(k in zone for k in ["盤中雷達", "研究", "觀察"])
        or any(k in action for k in ["不得建立正式新倉", "未通過交易品質", "不得正式推薦"])
    )
    if a_minus:
        return "A-MINUS", "上游僅A-/準主推薦；H51/H55不得升格正式買進"
    if hard_nonformal:
        return "RADAR", "上游Formal/V188未授權正式新倉；僅可等待/雷達"
    return "RESTRICTED", "存在上游權威欄位但未取得明確正式推薦授權"


def _overnight_evidence_state(row: pd.Series) -> tuple[str, bool, bool, str]:
    """Return (state, verified, adverse, explanation) for T+1 pre-open evidence.

    Neutral fallback values (50/50/50 and zero changes) are *unknown*, not a
    confirmation.  Evidence is verified only when the overnight snapshot has a
    fresh timestamp after the K-line session and its quality is usable.
    """
    quality = _first_text(row, ["隔夜資料品質"])
    updated = _datetime_value(row.get("隔夜更新時間")) if "隔夜更新時間" in row.index else None
    kdate = None
    for c in ["K線日期", "K線最新日期", "資料日期", "行情日期"]:
        if c in row.index:
            kdate = _date_value(row.get(c))
            if kdate:
                break
    target = None
    for c in ["隔日大盤預測日期", "下一交易日", "目標交易日", "推薦日期"]:
        if c in row.index:
            target = _date_value(row.get(c))
            if target:
                break

    bad_quality = any(k in quality.upper() for k in ["不足", "缺", "過期", "STALE", "FAIL", "PENDING", "未更新"])
    updated_date = updated.date() if updated else None
    fresh_time = bool(updated_date and ((kdate and updated_date > kdate) or (target and updated_date >= target)))
    neutral_triplet = (
        abs(_first_num(row, ["隔夜風控分數"], 50.0) - 50.0) < 0.01
        and abs(_first_num(row, ["隔日大盤分數"], 50.0) - 50.0) < 0.01
        and abs(_first_num(row, ["隔日下跌機率%"], 50.0) - 50.0) < 0.01
    )
    moves = [
        _first_num(row, ["NASDAQ漲跌%", "Nasdaq漲跌%"], 0.0),
        _first_num(row, ["S&P500漲跌%"], 0.0),
        _first_num(row, ["費半漲跌%"], 0.0),
        _first_num(row, ["台指夜盤漲跌", "台指夜盤漲跌%"], 0.0),
    ]
    all_zero = all(abs(x) < 0.01 for x in moves)
    rescan = _first_text(row, ["隔夜催化需求"])

    if bad_quality:
        return "INSUFFICIENT｜隔夜資料品質不足", False, False, f"品質={quality or '未標示'}"
    if not fresh_time:
        why = "缺隔夜更新時間" if updated is None else f"隔夜更新{updated_date}未晚於K線{kdate}"
        if neutral_triplet and all_zero:
            why += "；50/50/50與零漲跌屬中性預設，不是已確認"
        if "重掃" in rescan:
            why += "；上游亦標示盤前需重掃"
        return "PENDING｜盤前隔夜尚未確認", False, False, why

    penalty = _first_num(row, ["H54隔夜風險扣分"], 0.0)
    score = _first_num(row, ["隔夜風控分數"], 50.0)
    down_prob = _first_num(row, ["隔日下跌機率%"], 50.0)
    nasdaq, sp500, sox, tx = moves
    adverse = bool(
        penalty >= 35 or score <= 38 or down_prob >= 62
        or nasdaq <= -1.5 or sp500 <= -1.3 or sox <= -2.0 or tx <= -1.5
    )
    if adverse:
        return "VERIFIED-RISK｜盤前隔夜已更新但偏空", True, True, f"更新={updated_date}；隔夜分{score:.0f}/下跌機率{down_prob:.0f}%/費半{sox:+.1f}%"
    return "VERIFIED｜盤前隔夜證據已更新", True, False, f"更新={updated_date}；品質={quality or '可用'}"


def _h54_exhaustion(row: pd.Series) -> float:
    """Estimate next-session exhaustion/crowding risk; higher is worse.

    Prefer the existing next-day exhaustion signal when present.  Fallback uses
    extension, chase, volume, upper-shadow and optional attention/day-trading
    evidence.  It is deliberately a veto/haircut signal, never a buy signal.
    """
    existing = _first_num(row, ["隔日耗竭風險分", "耗竭風險分", "高檔耗竭風險分"], -1.0)
    if existing >= 0:
        base = _clip(existing)
    else:
        ret1 = _first_num(row, ["今日漲幅%", "當日漲跌幅%"], 0.0)
        ret5 = _first_num(row, ["近5日漲幅%", "5日績效%"], 0.0)
        ret20 = _first_num(row, ["近20日漲幅%", "20日績效%"], 0.0)
        chase = _first_num(row, ["追價風險分", "追價風險分數"], 50.0)
        vr = _first_num(row, ["當日量比", "均量比"], 1.0, positive=True)
        upper = _first_num(row, ["上影線比例%"], 20.0)
        base = 18.0
        base += max(0.0, ret1 - 4.0) * 4.0
        base += max(0.0, ret5 - 10.0) * 1.7
        base += max(0.0, ret20 - 28.0) * 0.55
        base += max(0.0, chase - 60.0) * 0.55
        base += max(0.0, vr - 2.0) * 7.0
        base += max(0.0, upper - 45.0) * 0.25
        base = _clip(base)
    daytrade = _first_num(row, ["當沖比率%", "當沖占比%", "當日沖銷比率%"], 0.0)
    if daytrade >= 65:
        base += min(16.0, (daytrade - 65.0) * 0.7 + 5.0)
    attention = "｜".join(_s(row.get(c)) for c in ["注意股狀態", "處置股狀態", "交易異常註記"] if c in row.index)
    if any(k in attention for k in ["注意", "處置", "異常"]):
        base += 8.0
    return _clip(base)


def _h54_trigger_proximity(row: pd.Series) -> float:
    direct = _first_num(row, ["隔日可執行優先分"], -1.0)
    direct_score = _clip(direct) if direct >= 0 else 50.0
    dist = _first_num(row, ["守價回測距離%", "距最近可執行買點%", "距買點%"], float("nan"))
    proximity = 50.0
    if math.isfinite(dist):
        ad = abs(dist)
        if ad <= 1.5: proximity = 94.0
        elif ad <= 3.0: proximity = 84.0
        elif ad <= 5.0: proximity = 68.0
        elif ad <= 7.0: proximity = 48.0
        else: proximity = 28.0
        if dist < -3.0:  # already above the intended pullback/guard price: chase risk
            proximity -= min(22.0, abs(dist + 3.0) * 3.0)
    else:
        spot = _first_num(row, ["最新價", "收盤價"], 0.0, positive=True)
        trigger = _first_num(row, ["實戰觸發價", "主要進場參考價", "推薦買點_突破"], 0.0, positive=True)
        if spot > 0 and trigger > 0:
            delta = (spot / trigger - 1.0) * 100.0
            ad = abs(delta)
            proximity = 92.0 if ad <= 1.5 else 82.0 if ad <= 3.0 else 64.0 if ad <= 5.0 else 42.0 if ad <= 8.0 else 25.0
            if delta > 4.0:
                proximity -= min(20.0, (delta - 4.0) * 2.5)
    return _clip(direct_score * 0.45 + proximity * 0.55)


def _h54_overnight(row: pd.Series) -> tuple[float, float, str]:
    """Return (risk penalty, information-window risk, explanation)."""
    risk_score = _first_num(row, ["隔夜風控分數"], 50.0)
    risk_level = _first_text(row, ["隔夜風險等級", "隔夜偏向"])
    next_mkt = _first_num(row, ["隔日大盤分數"], 50.0)
    down_prob = _first_num(row, ["隔日下跌機率%"], 50.0)
    forecast_adj = _first_num(row, ["隔日大盤預測加減分"], 0.0)
    nasdaq = _first_num(row, ["NASDAQ漲跌%", "Nasdaq漲跌%"], 0.0)
    sp500 = _first_num(row, ["S&P500漲跌%"], 0.0)
    sox = _first_num(row, ["費半漲跌%"], 0.0)
    tx = _first_num(row, ["台指夜盤漲跌", "台指夜盤漲跌%"], 0.0)
    penalty = max(0.0, 52.0 - risk_score) * 0.55 + max(0.0, 50.0 - next_mkt) * 0.35 + max(0.0, down_prob - 50.0) * 0.30
    penalty += max(0.0, -forecast_adj) * 0.8
    penalty += max(0.0, -nasdaq - 1.0) * 2.0 + max(0.0, -sp500 - 1.0) * 1.5 + max(0.0, -sox - 1.5) * 2.4
    penalty += max(0.0, -tx - 1.0) * 1.8
    level_upper = risk_level.upper()
    if any(k in level_upper for k in ["高", "HIGH", "RISK-OFF", "偏空"]):
        penalty += 8.0

    kdate = None
    for c in ["K線日期", "K線最新日期", "資料日期", "行情日期"]:
        if c in row.index:
            kdate = _date_value(row.get(c))
            if kdate:
                break
    target = None
    for c in ["隔日大盤預測日期", "下一交易日", "目標交易日", "推薦日期"]:
        if c in row.index:
            target = _date_value(row.get(c))
            if target:
                break
    gap_days = (target - kdate).days if (target and kdate and target >= kdate) else 0
    info_risk = 0.0
    if gap_days >= 3: info_risk = 18.0
    elif gap_days == 2: info_risk = 12.0
    elif gap_days == 1: info_risk = 3.0
    # A weekend-generated recommendation is not stale by itself, but it carries a real
    # information window before the next tradable session and must be revalidated.
    if target and target.weekday() >= 5:
        info_risk = max(info_risk, 20.0)
    # Friday -> Monday / holiday gap requires open-session reconfirmation even if the data itself is not stale.
    if kdate and target and kdate.weekday() == 4 and target.weekday() == 0 and gap_days >= 3:
        info_risk = max(info_risk, 20.0)
    reason = f"隔夜風控{risk_score:.0f}/隔日大盤{next_mkt:.0f}/下跌機率{down_prob:.0f}%/資訊空窗{gap_days}天"
    return _clip(penalty), _clip(info_risk), reason


def _apply_h54_truth(work: pd.DataFrame) -> pd.DataFrame:
    if work is None or work.empty:
        return work
    out = work.copy()
    cols = {c: [] for c in H54_COLUMNS}
    for _, row in out.iterrows():
        resonance = _first_num(row, ["H53族群共振分"], 50.0)
        cohort = _first_num(row, ["H53領漲集群分"], 50.0)
        h53next = _first_num(row, ["H53隔日優先分"], 50.0)
        ignition = _first_num(row, ["H51發動潛力分"], 50.0)
        exec51 = _first_num(row, ["H51可執行分"], 50.0)
        rr = _first_num(row, ["H51路徑RR"], 0.0)
        liq = _first_num(row, ["H51流動性分"], 50.0)
        breadth = _first_num(row, ["H53族群廣度分", "族群廣度分"], 50.0)
        attack = _first_num(row, ["H53族群攻擊分", "族群攻擊強度"], 50.0)
        vol = _first_num(row, ["H53族群量能分", "族群成交額分"], 50.0)
        fund = _first_num(row, ["H53族群資金分", "族群資金流分數"], 50.0)
        conf = _first_num(row, ["H53族群樣本可信度"], 35.0)
        evidence = _clip(breadth * 0.28 + attack * 0.24 + vol * 0.16 + fund * 0.18 + conf * 0.14)
        exhaustion = _h54_exhaustion(row)
        trigger = _h54_trigger_proximity(row)
        overnight, info_gap, overnight_reason = _h54_overnight(row)
        overnight_state, overnight_verified, overnight_adverse, overnight_evidence_reason = _overnight_evidence_state(row)
        legacy_next = _first_num(row, ["隔日可執行優先分"], trigger)
        continuation = _clip(
            resonance * 0.25 + cohort * 0.18 + ignition * 0.15 + evidence * 0.22 + _clip(legacy_next) * 0.20
            - exhaustion * 0.22
        )
        executable = _clip(
            exec51 * 0.32 + _clip(legacy_next) * 0.23 + trigger * 0.20
            + min(rr, 2.2) / 2.2 * 100.0 * 0.15 + liq * 0.10
        )
        truth = _clip(
            continuation * 0.34 + executable * 0.27 + h53next * 0.20 + evidence * 0.12
            + _clip(100.0 - overnight) * 0.07 - exhaustion * 0.18 - overnight * 0.12 - info_gap * 0.18
        )
        # Rising secondary theme with real breadth and lower crowding: useful when yesterday's hottest theme is exhausted.
        rotation = _clip(attack * 0.27 + breadth * 0.25 + fund * 0.17 + vol * 0.11 + evidence * 0.20 - exhaustion * 0.20)
        perm = _s(row.get("H51交易許可"))
        if perm.startswith("BUY-READY") and overnight_verified and not overnight_adverse and truth >= 72 and executable >= 68 and exhaustion <= 58 and overnight <= 35:
            tier = "A1｜READY-CONFIRMED｜主線延續＋盤前隔夜證據確認"
        elif perm.startswith("BUY-READY"):
            tier = "A2｜BUY-READY｜收盤條件成立，但盤前隔夜尚未確認"
        elif perm.startswith(("NO-CHASE", "WAIT-BASE")) and (exhaustion >= 55 or overnight >= 35 or info_gap >= 18):
            tier = "X1｜WAIT-COOLDOWN｜主流強但延伸/空窗，禁止把昨日強勢當隔日優先"
        elif perm.startswith("SETUP-PREP") and overnight_verified and not overnight_adverse and truth >= 72 and executable >= 65 and exhaustion <= 55 and overnight <= 32 and info_gap <= 12 and rr >= 1.25:
            tier = "P1｜PRIME-PREP｜盤前隔夜確認後的優先等待"
        elif perm.startswith("SETUP-PREP") and (exhaustion >= 65 or overnight >= 40 or info_gap >= 18 or overnight_adverse):
            tier = "P3｜WAIT-COOLDOWN｜耗竭/隔夜/空窗風險先降溫"
        elif perm.startswith("SETUP-PREP"):
            tier = "P2｜SETUP-PREP｜一般高品質等待"
        elif rotation >= 68 and evidence >= 64 and exhaustion <= 52:
            tier = "R1｜ROTATION-BACKUP｜次主流輪動備援研究"
        elif perm.startswith("LEADER-WATCH") and continuation >= 68:
            tier = "W1｜THEME-LEADER｜主線延續研究"
        else:
            tier = "W2｜RESEARCH｜一般研究"
        reason = (
            f"延續{continuation:.1f}/可執行{executable:.1f}/隔日真相{truth:.1f}；"
            f"族群證據{evidence:.1f}/耗竭{exhaustion:.1f}/隔夜扣分{overnight:.1f}/空窗{info_gap:.1f}；"
            f"RR{rr:.2f}/觸發接近{trigger:.1f}；{overnight_reason}；隔夜證據={overnight_state}({overnight_evidence_reason})"
        )
        vals = {
            "H54主流延續分": round(continuation, 2), "H54可執行確認分": round(executable, 2),
            "H54耗竭風險分": round(exhaustion, 2), "H54隔夜風險扣分": round(overnight, 2),
            "H54資訊空窗風險": round(info_gap, 2), "H54證據品質分": round(evidence, 2),
            "H54輪動備援分": round(rotation, 2), "H54隔日真相分": round(truth, 2),
            "H54決策層級": tier, "H54決策理由": reason, "H54版本": VERSION,
        }
        for c in H54_COLUMNS:
            cols[c].append(vals[c])
    for c, vals in cols.items():
        out[c] = vals
    return out


def _h55_role_bonus(row: pd.Series) -> float:
    """Small evidence bonus from already-existing internal radar roles.

    This is deliberately a proxy for catalyst/leader confirmation, not a live-news
    feed.  H55 must remain reproducible from the scan payload and must never invent
    an event that the upstream engines did not detect.
    """
    text = "｜".join([
        _first_text(row, ["領漲回補角色", "Phase6領漲回補角色"]),
        _first_text(row, ["飆股雷達角色", "爆發雷達角色"]),
        _first_text(row, ["紅燈逆勢反轉判定"]),
        _first_text(row, ["強勢前兆判定", "起漲前兆判定"]),
    ]).upper()
    bonus = 0.0
    if "L+" in text: bonus += 8.0
    elif "領漲回補" in text or "主流強勢回補" in text: bonus += 5.0
    if "題材轉強" in text: bonus += 5.0
    if "S+" in text: bonus += 5.0
    elif "爆發" in text or "飆股" in text: bonus += 3.0
    if "強" in text or "核心" in text: bonus += 3.0
    return min(14.0, bonus)


def _h55_blend(row: pd.Series, groups: list[tuple[list[str], float]], neutral: float = 50.0) -> float:
    total = 0.0
    weight = 0.0
    for names, w in groups:
        found = False
        value = neutral
        for c in names:
            if c in row.index and _s(row.get(c)):
                value = _f(row.get(c), neutral)
                if math.isfinite(value):
                    found = True
                    break
        # Missing signals are neutral rather than zero so H55 does not punish an
        # older payload merely because an optional radar engine was unavailable.
        total += _clip(value if found else neutral) * w
        weight += w
    return _clip(total / weight if weight > 0 else neutral)


def _apply_h55_dual_path(work: pd.DataFrame) -> pd.DataFrame:
    """Add a second, independent opportunity route for fresh reversal/catalyst setups.

    H54 is intentionally conservative and excels at answering whether yesterday's
    mainstream can continue.  Fast regime shifts need another question: did a stock
    already show *pre-existing* reversal/precursor/theme/leader-replay evidence that
    deserves research priority even if its old mainstream lifecycle is not mature?

    H55 never changes H51/V188/Formal trade authority.  R1/R2/R3 are research/watch
    tiers only.  BUY/SETUP remain gated by the existing execution truth.
    """
    if work is None or work.empty:
        return work
    out = work.copy()
    cols = {c: [] for c in H55_COLUMNS}
    for _, row in out.iterrows():
        continuation54 = _first_num(row, ["H54隔日真相分"], 50.0)
        mainstream54 = _first_num(row, ["H54主流延續分"], 50.0)
        executable54 = _first_num(row, ["H54可執行確認分"], 50.0)
        evidence54 = _first_num(row, ["H54證據品質分"], 50.0)
        exhaustion = _first_num(row, ["H54耗竭風險分"], 50.0)
        overnight = _first_num(row, ["H54隔夜風險扣分"], 0.0)
        info_gap = _first_num(row, ["H54資訊空窗風險"], 0.0)
        h53res = _first_num(row, ["H53族群共振分"], 50.0)
        ignition = _first_num(row, ["H51發動潛力分"], 50.0)
        rs = _first_num(row, ["H47個股相對強度分", "H45個股領先分"], 50.0)
        close = _first_num(row, ["當日收盤位置%"], 50.0)
        volume = _first_num(row, ["H51量價確認分", "強勢動能分"], 50.0)
        freshness = _first_num(row, ["今日訊號新鮮分", "H51主線新鮮分"], 50.0)
        risk = _first_num(row, ["Risk風控安全分", "SuperAI Risk分"], 50.0)

        reversal = _h55_blend(row, [
            (["紅燈逆勢反轉分", "逆勢反轉分", "回補潛力分"], 0.58),
            (["主流領漲回補分", "市場領漲相似分", "漲停回放分"], 0.42),
        ])
        precursor = _h55_blend(row, [
            (["強勢前兆分", "起漲前兆分數", "盤前強勢前兆分"], 0.62),
            (["隔日爆發分", "爆發雷達分", "飆股攻擊分"], 0.38),
        ])
        burst = _h55_blend(row, [
            (["隔日爆發分", "爆發雷達分", "飆股攻擊分"], 0.55),
            (["強勢前兆分", "起漲前兆分數", "盤前強勢前兆分"], 0.45),
        ])
        theme = _h55_blend(row, [
            (["局部題材火種分", "漲停族群相似度", "題材轉強分"], 0.60),
            (["族群攻擊強度", "H53族群攻擊分"], 0.40),
        ])
        replay = _h55_blend(row, [
            (["主流領漲回補分", "市場領漲相似分"], 0.55),
            (["漲停回放分", "強勢股漏選風險分"], 0.45),
        ])
        role_bonus = _h55_role_bonus(row)

        resilience = _clip(
            rs * 0.28 + reversal * 0.25 + close * 0.12 + volume * 0.10
            + freshness * 0.10 + risk * 0.15
        )
        catalyst = _clip(
            burst * 0.30 + precursor * 0.23 + theme * 0.22 + replay * 0.25 + role_bonus
        )
        rebound = _clip(reversal * 0.35 + replay * 0.30 + catalyst * 0.20 + freshness * 0.15)
        continuation_path = _clip(continuation54 * 0.60 + mainstream54 * 0.25 + executable54 * 0.15)
        reversal_path = _clip(
            resilience * 0.27 + catalyst * 0.30 + rebound * 0.18
            + executable54 * 0.10 + h53res * 0.07 + ignition * 0.08
            - exhaustion * 0.10 - overnight * 0.04 - info_gap * 0.04
        )
        dual = _clip(
            max(continuation_path, reversal_path) * 0.74
            + min(continuation_path, reversal_path) * 0.12
            + executable54 * 0.08 + evidence54 * 0.06
        )

        if continuation_path >= 70 and reversal_path >= 70:
            opportunity = "DUAL｜主線延續＋反轉點火雙確認"
        elif reversal_path >= max(70.0, continuation_path + 5.0) and catalyst >= 66:
            opportunity = "REVERSAL-CATALYST｜逆勢反轉／新題材點火"
        elif continuation_path >= 68:
            opportunity = "CONTINUATION｜主線延續"
        elif _first_num(row, ["H54輪動備援分"], 50.0) >= 66 and catalyst >= 62:
            opportunity = "ROTATION-IGNITION｜次主流輪動點火"
        else:
            opportunity = "RESEARCH｜一般研究"

        perm = _s(row.get("H51交易許可"))
        h54tier = _s(row.get("H54決策層級"))
        market_status = _s(row.get("H51市場地位"))
        if perm.startswith("BUY-READY") and dual >= 74 and executable54 >= 68 and exhaustion <= 62:
            tier = "A1｜READY-DUAL-CONFIRMED｜交易權威＋雙路徑確認"
        elif perm.startswith("BUY-READY"):
            tier = "A2｜BUY-READY｜交易權威成立，H55僅調整參考順位"
        elif perm.startswith("SETUP-PREP") and h54tier.startswith(("P3", "X1")):
            tier = "P3｜WAIT-COOLDOWN｜保留H54耗竭/隔夜降級"
        elif perm.startswith("SETUP-PREP") and dual >= 72 and executable54 >= 63 and exhaustion <= 62:
            tier = "P1｜PRIME-DUAL-PREP｜雙路徑隔日優先等待"
        elif perm.startswith("SETUP-PREP"):
            tier = "P2｜SETUP-PREP｜一般高品質等待"
        elif market_status.startswith("HM-RECLAIM") and reversal_path >= 72 and catalyst >= 66:
            tier = "R3｜RECLAIM-WATCH｜急跌後只列收復觀察，不升交易權威"
        elif reversal_path >= 72 and catalyst >= 68 and resilience >= 63 and exhaustion <= 70 and overnight <= 45:
            tier = "R2｜FRESH-IGNITION｜新反轉/新題材點火研究"
        elif h54tier.startswith("R1") and dual >= 64:
            tier = "R1｜ROTATION-BACKUP｜次主流輪動備援研究"
        elif perm.startswith("LEADER-WATCH") and dual >= 66:
            tier = "W1｜THEME-LEADER｜主線/點火領漲研究"
        else:
            tier = "W2｜RESEARCH｜一般研究"

        reason = (
            f"主線路徑{continuation_path:.1f}/反轉點火{reversal_path:.1f}/雙路徑{dual:.1f}；"
            f"逆風韌性{resilience:.1f}/催化代理{catalyst:.1f}/回補雷達{rebound:.1f}；"
            f"耗竭{exhaustion:.1f}/隔夜{overnight:.1f}/空窗{info_gap:.1f}。"
            "催化代理只使用系統既有前兆/爆發/題材/回放訊號，不代表已驗證新聞事件。"
        )
        vals = {
            "H55主線延續路徑分": round(continuation_path, 2),
            "H55反轉點火路徑分": round(reversal_path, 2),
            "H55逆風韌性分": round(resilience, 2),
            "H55催化代理分": round(catalyst, 2),
            "H55回補雷達分": round(rebound, 2),
            "H55雙路徑隔日分": round(dual, 2),
            "H55機會型態": opportunity,
            "H55參考層級": tier,
            "H55決策理由": reason,
            "H55版本": VERSION,
        }
        for c in H55_COLUMNS:
            cols[c].append(vals[c])
    for c, vals in cols.items():
        out[c] = vals
    return out


def _apply_h56_truth(work: pd.DataFrame) -> pd.DataFrame:
    """Final governance for authority ceiling + two-stage T+1 confirmation.

    Stage 1 (post-close): rank candidates, but never call neutral/missing overnight
    defaults "confirmed". Stage 2 (pre-open): only a fresh overnight snapshot may
    confirm or suspend the candidate. Formal/V188 remains the upper authority.
    """
    if work is None or work.empty:
        return work
    out = work.copy()
    cols = {c: [] for c in H56_COLUMNS}
    for _, row in out.iterrows():
        authority, authority_reason = _authority_state(row)
        evidence_state, verified, adverse, evidence_reason = _overnight_evidence_state(row)
        h51perm = _s(row.get("H51交易許可"))
        h55 = _first_num(row, ["H55雙路徑隔日分"], 50.0)
        h54 = _first_num(row, ["H54隔日真相分"], 50.0)
        executable = _first_num(row, ["H54可執行確認分", "H51可執行分"], 50.0)
        overnight_penalty = _first_num(row, ["H54隔夜風險扣分"], 0.0)
        authority_score = {"FORMAL": 100.0, "UNKNOWN": 70.0, "A-MINUS": 45.0, "RADAR": 28.0, "RESTRICTED": 35.0}.get(authority, 40.0)
        evidence_score = 100.0 if verified else 45.0 if evidence_state.startswith("PENDING") else 30.0
        t1 = _clip(h55 * 0.38 + h54 * 0.24 + executable * 0.16 + authority_score * 0.12 + evidence_score * 0.10 - overnight_penalty * 0.12)

        # Hard ceilings: research engines cannot create trade authority, and a
        # post-close scan cannot certify an overnight session that has not happened.
        if authority in {"A-MINUS", "RADAR", "RESTRICTED"}:
            t1 = min(t1, 62.0)
            tier = "P0｜AUTHORITY-CAPPED｜上游未正式授權，僅可等待/雷達"
            recheck = "需要｜Formal/V188先通過；若要隔日執行，盤前仍需重掃隔夜資料"
        elif adverse and verified:
            t1 = min(t1, 45.0)
            tier = "X2｜OVERNIGHT-RISK-HOLD｜盤前隔夜轉弱，暫停新倉"
            recheck = "需要｜盤前風險偏空，等待開盤後重新確認市場/守價"
        elif not verified and h51perm.startswith("BUY-READY"):
            t1 = min(t1, 68.0)
            tier = "A0｜PREOPEN-PENDING｜收盤候選成立，但隔夜尚未確認"
            recheck = "需要｜下一交易日盤前更新美股/費半/台指夜盤後才能升A1"
        elif not verified and h51perm.startswith("SETUP-PREP"):
            t1 = min(t1, 66.0)
            tier = "P0｜PREOPEN-PENDING｜高品質等待，但隔夜尚未確認"
            recheck = "需要｜盤前重掃後才決定P1/P2或降級"
        elif h51perm.startswith("BUY-READY") and verified and t1 >= 72:
            tier = "A1｜PREOPEN-CONFIRMED｜Formal/執行/隔夜三層確認"
            recheck = "已完成｜仍需依實戰觸發價與守價，不開盤追價"
        elif h51perm.startswith("BUY-READY"):
            tier = "A2｜BUY-READY｜權威成立但T+1確認不足"
            recheck = "需要｜開盤前/後再確認市場與觸發"
        elif h51perm.startswith("SETUP-PREP") and verified and t1 >= 70:
            tier = "P1｜PREOPEN-PRIME-PREP｜盤前確認後的優先等待"
            recheck = "已完成隔夜層｜仍須等Pivot/觸發守價"
        elif h51perm.startswith("SETUP-PREP"):
            tier = "P2｜SETUP-PREP｜一般等待，不視為隔日已確認"
            recheck = "需要｜依盤前/開盤後資料重驗"
        else:
            old = _s(row.get("H55參考層級"))
            tier = old if old else "W2｜RESEARCH｜一般研究"
            recheck = "研究層｜不建立正式新倉"

        reason = (
            f"上游權威={authority}；隔夜證據={evidence_state}；H55雙路徑{h55:.1f}/H54真相{h54:.1f}/"
            f"可執行{executable:.1f}/H56T1確認{t1:.1f}。{authority_reason}；{evidence_reason}"
        )
        vals = {
            "H56上游權威層級": authority,
            "H56隔夜證據狀態": evidence_state,
            "H56盤前重驗需求": recheck,
            "H56T1確認分": round(t1, 2),
            "H56最終參考層級": tier,
            "H56決策理由": reason,
            "H56版本": VERSION,
        }
        for c in H56_COLUMNS:
            cols[c].append(vals[c])
    for c, vals in cols.items():
        out[c] = vals
    return out


def _apply_h53_resonance(work: pd.DataFrame) -> pd.DataFrame:
    if work is None or work.empty:
        return work
    out = work.copy()
    context = _h53_group_context(out)
    resonance_vals = []
    cohort_vals = []
    nextday_vals = []
    tier_vals = []
    breadth_vals = []
    attack_vals = []
    volume_vals = []
    fund_vals = []
    conf_vals = []
    dilution_vals = []
    for _, row in out.iterrows():
        sector = _first_text(row, ["類別", "族群名稱"], "未分類")
        ctx = context.get(sector, {"resonance": 50.0, "breadth": 50.0, "attack": 50.0, "volume": 50.0, "fund": 50.0, "confidence": 35.0, "dilution": 0.0, "top_leader": 50.0})
        resonance = float(ctx["resonance"])
        leader = _first_num(row, ["H51個股領漲品質分"], 50.0)
        ignition = _first_num(row, ["H51發動潛力分"], 50.0)
        volume = _first_num(row, ["H51量價確認分"], 50.0)
        pro = _first_num(row, ["H51專業參考分"], 50.0)
        exec_score = _first_num(row, ["H51可執行分"], 50.0)
        rr = _first_num(row, ["H51路徑RR"], 0.0)
        chase = _first_num(row, ["追價風險分", "追價風險分數"], 55.0)
        ret1 = _first_num(row, ["今日漲幅%", "當日漲跌幅%"], 0.0)
        ret5 = _first_num(row, ["近5日漲幅%", "5日績效%"], 0.0)
        cohort = _clip(leader * 0.34 + ignition * 0.24 + volume * 0.14 + float(ctx["breadth"]) * 0.14 + float(ctx["attack"]) * 0.09 + float(ctx["top_leader"]) * 0.05)
        late_penalty = max(0.0, ret1 - 6.5) * 2.0 + max(0.0, ret5 - 14.0) * 0.8 + max(0.0, chase - 72.0) * 0.25
        nextday = _clip(ignition * 0.30 + pro * 0.20 + resonance * 0.25 + cohort * 0.15 + exec_score * 0.10 - late_penalty)
        perm = _s(row.get("H51交易許可"))
        if perm.startswith("BUY-READY") and resonance >= 68 and nextday >= 72:
            tier = "A1｜READY-CONFIRMED｜主流共振確認"
        elif perm.startswith("BUY-READY"):
            tier = "A2｜BUY-READY｜交易可執行但族群共振普通"
        elif perm.startswith("SETUP-PREP") and nextday >= 72 and resonance >= 68 and cohort >= 66 and rr >= 1.0:
            tier = "P1｜PRIME-PREP｜隔日優先等待"
        elif perm.startswith("SETUP-PREP"):
            tier = "P2｜SETUP-PREP｜一般高品質等待"
        elif perm.startswith("LEADER-WATCH") and resonance >= 70 and cohort >= 70:
            tier = "W1｜THEME-LEADER｜強主線領漲研究"
        else:
            tier = "W2｜RESEARCH｜一般研究"
        resonance_vals.append(round(resonance, 2)); cohort_vals.append(round(cohort, 2)); nextday_vals.append(round(nextday, 2)); tier_vals.append(tier)
        breadth_vals.append(float(ctx["breadth"])); attack_vals.append(float(ctx["attack"])); volume_vals.append(float(ctx["volume"])); fund_vals.append(float(ctx["fund"])); conf_vals.append(float(ctx["confidence"])); dilution_vals.append(float(ctx["dilution"]))
    out["H53族群共振分"] = resonance_vals
    out["H53領漲集群分"] = cohort_vals
    out["H53隔日優先分"] = nextday_vals
    out["H53參考層級"] = tier_vals
    out["H53族群廣度分"] = breadth_vals
    out["H53族群攻擊分"] = attack_vals
    out["H53族群量能分"] = volume_vals
    out["H53族群資金分"] = fund_vals
    out["H53族群樣本可信度"] = conf_vals
    out["H53分類稀釋扣分"] = dilution_vals
    out["H53版本"] = VERSION
    return out

def _profile(row: pd.Series) -> dict[str, Any]:
    life = _first_text(row, ["H50族群生命週期"])
    stage50 = _first_text(row, ["H50波段機會階段"])
    stage47 = _first_text(row, ["H47主流領先狀態", "H47波段階段"])
    sector50 = _first_num(row, ["H50族群可買主流分"], 50.0)
    sector45 = _first_num(row, ["H45族群主流分"], 50.0)
    fresh50 = _first_num(row, ["H50族群新鮮度分"], 50.0)
    reclaim50 = _first_num(row, ["H50族群回檔再攻分"], 50.0)
    sector = _clip(sector50 * 0.45 + sector45 * 0.20 + fresh50 * 0.18 + reclaim50 * 0.17)
    life_adj = 9 if life.startswith("A0") else 7 if life.startswith("A1") else 5 if life.startswith("B1") else 3 if life.startswith("B2") else -8 if life.startswith("C") else -14 if life.startswith("D") else 0
    sector = _clip(sector + life_adj)

    rs = _first_num(row, ["H47個股相對強度分", "H45個股領先分"], 50.0)
    pct = _first_num(row, ["H47族群內領先百分位%"], 50.0)
    onset = _first_num(row, ["H47起漲優先分", "H45起漲結構分"], 50.0)
    trend = _first_num(row, ["H45趨勢延續分"], 50.0)
    leader = _clip(rs * 0.34 + pct * 0.20 + onset * 0.25 + trend * 0.13 + sector * 0.08)

    ret1 = _first_num(row, ["今日漲幅%", "當日漲跌幅%", "當日報酬%"], 0.0)
    ret5 = _first_num(row, ["近5日漲幅%", "5日績效%"], 0.0)
    ret20 = _first_num(row, ["近20日漲幅%", "20日績效%"], 0.0)
    dist_high = abs(_first_num(row, ["距20日高點%"], 99.0))
    vr = _first_num(row, ["當日量比", "均量比"], 1.0, positive=True)
    close = _first_num(row, ["當日收盤位置%"], 50.0)
    upper = _first_num(row, ["上影線比例%"], 25.0)
    pos5 = _zone(ret5, -3.0, 8.0, 7.0)
    pos20 = _zone(ret20, 3.0, 28.0, 3.0)
    highfit = _zone(dist_high, 0.0, 8.0, 5.0)
    stage_bonus = 18 if stage50.startswith("N-EARLY") else 16 if stage50.startswith("N-PULLBACK") else 8 if stage50.startswith("N-LEADER") else 4 if stage50.startswith("N-RADAR") else -18 if stage50.startswith("N-EXTENDED") else -12 if stage50.startswith("N-MATURE") else 0
    pivot = _clip(pos5 * 0.24 + pos20 * 0.24 + highfit * 0.18 + onset * 0.22 + trend * 0.12 + stage_bonus)

    volume = _clip(35.0 + min(vr, 2.5) * 22.0 + max(0.0, close - 50.0) * 0.30 - max(0.0, upper - 35.0) * 0.35)
    amount = _first_num(row, ["成交額百萬"], 0.0, positive=True)
    avg_amount = _first_num(row, ["20日均成交額百萬"], 0.0, positive=True)
    liquidity = _liquidity_score(amount, avg_amount)
    fund = _first_num(row, ["主流資金分"], 50.0)
    eps = _first_num(row, ["EPS代理分數"], 50.0)
    revenue = _first_num(row, ["營收動能代理分數"], 50.0)
    profit = _first_num(row, ["獲利代理分數"], 50.0)
    attack = _first_num(row, ["族群攻擊強度", "資金攻擊有效分"], 50.0)
    fundamental = _clip(fund * 0.35 + attack * 0.25 + revenue * 0.18 + eps * 0.12 + profit * 0.10)
    signal_fresh = _first_num(row, ["今日訊號新鮮分"], 50.0)
    fresh = _clip(fresh50 * 0.55 + signal_fresh * 0.20 + reclaim50 * 0.25)
    repeat_penalty = _first_num(row, ["H50重複推薦扣分"], 0.0)

    chase = _first_num(row, ["追價風險分", "追價風險分數"], 55.0)
    extended = bool(stage50.startswith(("N-EXTENDED", "N-MATURE")) or stage47.startswith("L-EXTENDED") or ret5 >= 18 or ret20 >= 42 or chase >= 88)
    extension_penalty = 0.0
    if extended: extension_penalty += 18.0
    if ret5 > 20: extension_penalty += 10.0
    if ret20 > 50: extension_penalty += 9.0
    if chase >= 90: extension_penalty += 8.0
    if life.startswith("C"): extension_penalty += 5.0
    if life.startswith("D"): extension_penalty += 10.0

    shock_down = bool(ret1 <= -7.0)
    weak_breakdown = bool(ret1 <= -5.0 and close < 35.0)
    shock_penalty = 26.0 if shock_down else 14.0 if weak_breakdown else 0.0

    pro = _clip(
        sector * 0.20 + leader * 0.21 + pivot * 0.22 + volume * 0.11 + liquidity * 0.10
        + fundamental * 0.08 + fresh * 0.08 - repeat_penalty - extension_penalty - shock_penalty
    )
    ignition = _clip(
        sector * 0.19 + leader * 0.24 + pivot * 0.24 + volume * 0.10
        + fresh * 0.13 + fundamental * 0.05 + liquidity * 0.05
        - extension_penalty * 0.35 - shock_penalty
    )
    reclaim_status = (
        "SHOCK-DOWN｜當日急跌/跌停，需先收復關鍵價與量價結構" if shock_down
        else "WEAK-BREAKDOWN｜當日弱勢破壞，先等止跌收復" if weak_breakdown
        else "NORMAL｜無急跌事件否決"
    )

    # Pure market status: a fresh leader is valuable, but an event/limit-down break is not an ordinary pullback.
    if shock_down or weak_breakdown:
        market_status = "HM-RECLAIM｜急跌/事件後等待收復"
    elif extended:
        market_status = "HM-EXTENDED｜主流領漲但已延伸"
    elif life.startswith("C"):
        market_status = "HM-MATURE｜成熟主流，等新一輪基底"
    elif stage50.startswith("N-EARLY") or (sector >= 60 and pivot >= 70 and -2 <= ret5 <= 8 and 2 <= ret20 <= 26):
        market_status = "HM-EARLY｜新主流起漲候選"
    elif stage50.startswith("N-PULLBACK") or (sector >= 59 and pivot >= 68 and -6 <= ret5 <= 2.5 and 5 <= ret20 <= 32):
        market_status = "HM-PULLBACK｜主流回檔再攻"
    elif stage50.startswith("N-LEADER") or stage47.startswith("L-LEADER"):
        market_status = "HM-LEADER｜主流領漲核心"
    elif sector >= 59 and leader >= 62 and pivot >= 62:
        market_status = "HM-SETUP｜主線高品質觀察"
    else:
        market_status = "HM-NO｜非真人主線優先"

    rr, rr_basis = _route_rr(row)
    trade = _first_num(row, ["SuperAI Trade分", "實戰操作品質分", "可操作分", "進場可執行分"], 50.0)
    risk = _first_num(row, ["Risk風控安全分", "風控安全分"], 50.0)
    entry = _first_num(row, ["Entry進場買點分", "買進分數"], 50.0)
    stop_dist = _first_num(row, ["實戰停損距離%", "停損距離_隔日%", "AI戰術停損距離%"], 0.0, positive=True)
    blocked = _is_blocked(row)
    exec_score = _clip(trade * 0.25 + risk * 0.22 + entry * 0.18 + min(rr, 2.5) / 2.5 * 100 * 0.20 + liquidity * 0.10 + max(0, 100 - chase) * 0.05)

    core_market = market_status.startswith(("HM-EARLY", "HM-PULLBACK", "HM-LEADER", "HM-SETUP"))
    ready = bool(
        core_market and not blocked and not shock_down and not weak_breakdown
        and pro >= 72 and ignition >= 70 and pivot >= 68 and leader >= 56 and liquidity >= 74 and amount >= 300
        and rr >= 1.35 and trade >= 65 and risk >= 60 and entry >= 58
        and ret1 > -5.0 and (stop_dist <= 0 or stop_dist <= 7.0) and chase <= 60 and close >= 50 and upper <= 50
    )
    prep = bool(
        core_market and not blocked and not shock_down and not weak_breakdown
        and pro >= 66 and ignition >= 64 and sector >= 66 and leader >= 52 and pivot >= 62 and liquidity >= 62 and amount >= 150
        and rr >= 0.70 and risk >= 50 and trade >= 52 and ret1 > -6.0
        and close >= 40 and upper <= 60 and (stop_dist <= 0 or stop_dist <= 10.0) and chase <= 78
    )
    authority, authority_reason = _authority_state(row)
    authority_capped = bool(ready and authority in {"A-MINUS", "RADAR", "RESTRICTED"})
    if shock_down or weak_breakdown:
        permission = "WAIT-RECLAIM｜急跌/跌停後先確認收復，不列高品質等待"
    elif authority_capped:
        permission = "SETUP-PREP｜上游僅A-/雷達/未正式授權，H51不得越權升格"
    elif ready:
        permission = "BUY-READY｜主線/領漲/Pivot與執行條件完成"
    elif market_status.startswith("HM-EXTENDED"):
        permission = "NO-CHASE｜真正強股但已延伸，只等新基底/回測"
    elif market_status.startswith("HM-MATURE"):
        permission = "WAIT-BASE｜成熟主流，沒有新Pivot前不重複推薦"
    elif prep:
        permission = "SETUP-PREP｜值得盯，等Pivot/量價/路徑RR補齊"
    elif core_market:
        permission = "LEADER-WATCH｜主線成立但交易品質不足"
    else:
        permission = "NO-PRIORITY｜目前非專業主線優先"

    if ready and not authority_capped and pro >= 80:
        level = "A+｜真人主線可執行"
    elif ready and not authority_capped:
        level = "A｜真人主線可執行"
    elif (prep or authority_capped) and pro >= 72:
        level = "B+｜高品質主線等待買點"
    elif core_market:
        level = "B｜主線研究"
    else:
        level = "C｜非優先"

    reason = (
        f"族群{sector:.1f}/{life or '生命週期未知'}；領漲{leader:.1f}；Pivot{pivot:.1f}；"
        f"量價{volume:.1f}；流動性{liquidity:.1f}(成交額{amount:.0f}百萬)；"
        f"資金/基本面{fundamental:.1f}；發動潛力{ignition:.1f}；今日{ret1:+.1f}%/5日{ret5:+.1f}%/20日{ret20:+.1f}%；"
        f"路徑RR{rr:.2f}({rr_basis})；Trade{trade:.1f}/Risk{risk:.1f}/Entry{entry:.1f}/追價{chase:.0f}；"
        f"重複扣分{repeat_penalty:.1f}/延伸扣分{extension_penalty:.1f}/急跌扣分{shock_penalty:.1f}；"
        f"上游權威{authority}({authority_reason})"
    )
    return {
        "sector": round(sector, 2), "leader": round(leader, 2), "pivot": round(pivot, 2), "volume": round(volume, 2),
        "liquidity": round(liquidity, 2), "fundamental": round(fundamental, 2), "fresh": round(fresh, 2),
        "repeat": round(repeat_penalty, 2), "ignition": round(ignition, 2), "pro": round(pro, 2), "exec": round(exec_score, 2),
        "market_status": market_status, "permission": permission, "level": level, "reclaim_status": reclaim_status,
        "rr": round(rr, 3), "rr_basis": rr_basis, "reason": reason,
    }


def apply_human_master_engine(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    work = frame.copy()
    # Make sure H50/H47 context exists when the module is used standalone.
    if "H50族群生命週期" not in work.columns:
        try:
            from godpick_mainstream_wave_engine import apply_mainstream_wave_engine
            work = apply_mainstream_wave_engine(work)
        except Exception:
            pass
    profiles = [_profile(row) for _, row in work.iterrows()]
    work["H51族群主線分"] = [p["sector"] for p in profiles]
    work["H51個股領漲品質分"] = [p["leader"] for p in profiles]
    work["H51Pivot起漲分"] = [p["pivot"] for p in profiles]
    work["H51量價確認分"] = [p["volume"] for p in profiles]
    work["H51流動性分"] = [p["liquidity"] for p in profiles]
    work["H51基本面資金分"] = [p["fundamental"] for p in profiles]
    work["H51主線新鮮分"] = [p["fresh"] for p in profiles]
    work["H51重複推薦扣分"] = [p["repeat"] for p in profiles]
    work["H51發動潛力分"] = [p["ignition"] for p in profiles]
    work["H51專業參考分"] = [p["pro"] for p in profiles]
    work["H51可執行分"] = [p["exec"] for p in profiles]
    work["H51市場地位"] = [p["market_status"] for p in profiles]
    work["H51交易許可"] = [p["permission"] for p in profiles]
    work["H51推薦等級"] = [p["level"] for p in profiles]
    work["H51急跌收復狀態"] = [p["reclaim_status"] for p in profiles]
    work["H51路徑RR"] = [p["rr"] for p in profiles]
    work["H51RR口徑"] = [p["rr_basis"] for p in profiles]
    work["H51推薦理由"] = [p["reason"] for p in profiles]
    work["H51版本"] = VERSION
    work = _apply_h53_resonance(work)
    work = _apply_h54_truth(work)
    work = _apply_h55_dual_path(work)
    work = _apply_h56_truth(work)
    return work


def build_h51_final_decision_table(frame: pd.DataFrame, max_rows: int = 6) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame({"狀態": ["目前沒有可建立H51最終決策的候選資料。"]})
    work = frame if ("H51版本" in frame.columns and frame["H51版本"].astype(str).eq(VERSION).all()) else apply_human_master_engine(frame)
    code = work.get("股票代號", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str).str.strip()
    work = work.loc[code.ne("")].copy()
    perm = work.get("H51交易許可", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    h55_all_tier = work.get("H55參考層級", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    h55_fresh_count = int(h55_all_tier.str.startswith(("R1", "R2", "R3")).sum())
    # First sheet is an action/preparation sheet only. Research-only R1/R2/R3 and LEADER-WATCH
    # belong in 主流領漲股; surfacing research must never masquerade as a buy list.
    priority = perm.map(lambda x: 4 if x.startswith("BUY-READY") else 3 if x.startswith("SETUP-PREP") else 0)
    work = work.loc[priority.gt(0)].copy()
    if work.empty:
        if h55_fresh_count > 0:
            return pd.DataFrame({
                "狀態": [f"今天沒有H51可執行/高品質等待，但H55找到{h55_fresh_count}檔R1/R2/R3新點火/輪動/收復研究候選。"],
                "操作原則": ["第一頁不把研究雷達假裝成推薦；請看『主流領漲股』的H55參考層級，等H51/V188、觸發守價與路徑RR完成再升級。"],
            })
        return pd.DataFrame({
            "狀態": ["今天沒有通過H51『主流族群→領漲股→Pivot/再攻→量價→流動性→路徑RR』的高品質候選。"],
            "操作原則": ["不以成熟主流或低品質雷達補位；請看『主流族群與領漲股』了解下一批等待名單。"],
        })
    # H54 presentation guard: SETUP-PREP that has been downgraded by exhaustion,
    # overnight risk or weekend information gap must not remain on the first
    # action sheet as if it were a normal next-session priority.  This changes
    # only the reference/presentation layer, never H51/V188/Formal authority.
    h54tier = work.get("H54決策層級", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    h55tier = work.get("H55參考層級", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    h56tier = work.get("H56最終參考層級", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    cool_mask = (
        (work["H51交易許可"].astype(str).str.startswith("SETUP-PREP") & (h54tier.str.startswith(("P3", "X1")) | h55tier.str.startswith(("P3", "X1"))))
        | h56tier.str.startswith("X2")
    )
    cooled = work.loc[cool_mask].copy()
    work = work.loc[~cool_mask].copy()
    if work.empty and not cooled.empty:
        return pd.DataFrame({
            "狀態": ["H56判定：原有候選全數因耗竭／已驗證隔夜風險／資訊空窗降級，第一頁不硬塞隔日優先。"],
            "操作原則": ["下一交易日前重新驗證Formal/V188權威、隔夜資料、量價、守價與大盤；研究候選仍保留在『主流領漲股』。"],
        })
    work["_p"] = work["H51交易許可"].astype(str).map(lambda x: 4 if x.startswith("BUY-READY") else 3)
    for c in ["H56T1確認分", "H55雙路徑隔日分", "H55主線延續路徑分", "H55反轉點火路徑分", "H55逆風韌性分", "H55催化代理分", "H55回補雷達分", "H54隔日真相分", "H54主流延續分", "H54可執行確認分", "H54耗竭風險分", "H54輪動備援分", "H53隔日優先分", "H53族群共振分", "H53領漲集群分", "H51專業參考分", "H51可執行分", "H51Pivot起漲分", "H51個股領漲品質分", "H51族群主線分"]:
        work[c] = pd.to_numeric(work.get(c, 0), errors="coerce").fillna(0.0)
    work.sort_values(["_p", "H56T1確認分", "H55雙路徑隔日分", "H54隔日真相分", "H53隔日優先分", "H51專業參考分"], ascending=False, inplace=True, kind="mergesort")
    selected = []
    sector_count: dict[str, int] = {}
    for _, row in work.iterrows():
        sector = _first_text(row, ["類別", "族群名稱"], "未分類")
        permx = _s(row.get("H51交易許可"))
        if sector_count.get(sector, 0) >= 2:
            continue
        selected.append(row)
        sector_count[sector] = sector_count.get(sector, 0) + 1
        if len(selected) >= max(1, int(max_rows)):
            break
    rows = []
    for i, row in enumerate(selected, 1):
        permx = _s(row.get("H51交易許可"))
        h54tier = _s(row.get("H54決策層級"))
        h55tier = _s(row.get("H55參考層級"))
        h56tier = _s(row.get("H56最終參考層級"))
        if h56tier.startswith("A1"):
            action = "盤前已確認可執行候選｜仍依Formal/V188、實戰觸發價與守價分批，不追開盤"
        elif h56tier.startswith("A0"):
            action = "收盤候選成立｜隔夜尚未發生/未更新，盤前重掃前不得稱已確認"
        elif h56tier.startswith("P0") and "AUTHORITY-CAPPED" in h56tier:
            action = "上游僅A-/雷達｜H51/H55不得越權升格；只可盤中等待確認，不是正式買進推薦"
        elif h56tier.startswith("P0"):
            action = "高品質等待｜隔夜尚未確認，盤前重掃後再決定是否恢復P1/P2"
        elif permx.startswith("BUY-READY"):
            action = "交易條件候選｜H56尚未完成T+1盤前確認，不開盤追價"
        elif permx.startswith("SETUP-PREP") and h56tier.startswith("P1"):
            action = "盤前確認後優先等待｜仍須等Pivot/實戰觸發守價"
        elif permx.startswith("SETUP-PREP"):
            action = "一般高品質等待｜未確認買點前不進場"
        else:
            action = "主線領漲觀察｜不是買進推薦"
        rows.append({
            "決策順位": i,
            "股票代號": _s(row.get("股票代號")),
            "股票名稱": _s(row.get("股票名稱")),
            "類別": _first_text(row, ["類別", "族群名稱"]),
            "H51推薦等級": _s(row.get("H51推薦等級")),
            "H51市場地位": _s(row.get("H51市場地位")),
            "H51交易許可": permx,
            "目前決策": action,
            "H56最終參考層級": _s(row.get("H56最終參考層級")),
            "H56上游權威層級": _s(row.get("H56上游權威層級")),
            "H56隔夜證據狀態": _s(row.get("H56隔夜證據狀態")),
            "H56盤前重驗需求": _s(row.get("H56盤前重驗需求")),
            "H56T1確認分": _first_num(row, ["H56T1確認分"]),
            "H55參考層級": _s(row.get("H55參考層級")),
            "H55機會型態": _s(row.get("H55機會型態")),
            "H55雙路徑隔日分": _first_num(row, ["H55雙路徑隔日分"]),
            "H55主線延續路徑分": _first_num(row, ["H55主線延續路徑分"]),
            "H55反轉點火路徑分": _first_num(row, ["H55反轉點火路徑分"]),
            "H55逆風韌性分": _first_num(row, ["H55逆風韌性分"]),
            "H55催化代理分": _first_num(row, ["H55催化代理分"]),
            "H55回補雷達分": _first_num(row, ["H55回補雷達分"]),
            "H54決策層級": _s(row.get("H54決策層級")),
            "H54隔日真相分": _first_num(row, ["H54隔日真相分"]),
            "H54主流延續分": _first_num(row, ["H54主流延續分"]),
            "H54可執行確認分": _first_num(row, ["H54可執行確認分"]),
            "H54耗竭風險分": _first_num(row, ["H54耗竭風險分"]),
            "H54隔夜風險扣分": _first_num(row, ["H54隔夜風險扣分"]),
            "H54資訊空窗風險": _first_num(row, ["H54資訊空窗風險"]),
            "H54輪動備援分": _first_num(row, ["H54輪動備援分"]),
            "H53參考層級": _s(row.get("H53參考層級")),
            "H53隔日優先分": _first_num(row, ["H53隔日優先分"]),
            "H53族群共振分": _first_num(row, ["H53族群共振分"]),
            "H53領漲集群分": _first_num(row, ["H53領漲集群分"]),
            "H51發動潛力分": _first_num(row, ["H51發動潛力分"]),
            "H51專業參考分": _first_num(row, ["H51專業參考分"]),
            "H51族群主線分": _first_num(row, ["H51族群主線分"]),
            "H51個股領漲品質分": _first_num(row, ["H51個股領漲品質分"]),
            "H51Pivot起漲分": _first_num(row, ["H51Pivot起漲分"]),
            "H51量價確認分": _first_num(row, ["H51量價確認分"]),
            "H51流動性分": _first_num(row, ["H51流動性分"]),
            "H51路徑RR": _first_num(row, ["H51路徑RR"]),
            "H51RR口徑": _s(row.get("H51RR口徑")),
            "最新價": _first_num(row, ["最新價"], 0.0, positive=True),
            "預估進場點": _s(row.get("預估進場點")),
            "實戰觸發價": _first_num(row, ["實戰觸發價"], 0.0, positive=True),
            "觸發後守價": _first_num(row, ["觸發後守價"], 0.0, positive=True),
            "停損參考": _first_num(row, ["實戰停損參考", "停損參考"], 0.0, positive=True),
            "今日/5日/20日": f"{_first_num(row,['今日漲幅%'],0):+.1f}% / {_first_num(row,['近5日漲幅%'],0):+.1f}% / {_first_num(row,['近20日漲幅%'],0):+.1f}%",
            "AI重點理由": _s(row.get("H51推薦理由")),
            "H56T1決策理由": _s(row.get("H56決策理由")),
            "H55雙路徑理由": _s(row.get("H55決策理由")),
            "H54隔日真相理由": _s(row.get("H54決策理由")),
        })
    return pd.DataFrame(rows)


def build_h51_mainstream_leader_table(frame: pd.DataFrame, max_rows: int = 20) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    work = frame if ("H51版本" in frame.columns and frame["H51版本"].astype(str).eq(VERSION).all()) else apply_human_master_engine(frame)
    status = work.get("H51市場地位", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    h55tier = work.get("H55參考層級", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    h55_research = h55tier.str.startswith(("R1", "R2", "R3"))
    pick = work.loc[(~status.str.startswith("HM-NO")) | h55_research].copy()
    if pick.empty:
        return pick
    for c in ["H56T1確認分", "H54隔日真相分", "H54主流延續分", "H54可執行確認分", "H54耗竭風險分", "H54輪動備援分", "H53隔日優先分", "H53族群共振分", "H53領漲集群分", "H51發動潛力分", "H51專業參考分", "H51族群主線分", "H51個股領漲品質分", "H51Pivot起漲分", "H51流動性分", "H51路徑RR"]:
        pick[c] = pd.to_numeric(pick.get(c, 0), errors="coerce").fillna(0.0)
    pick["_stage"] = pick["H51市場地位"].astype(str).map(lambda x: 5 if x.startswith("HM-EARLY") else 4 if x.startswith("HM-PULLBACK") else 3 if x.startswith("HM-LEADER") else 2 if x.startswith("HM-SETUP") else 1)
    pick["_h56route"] = pick.get("H56最終參考層級", pd.Series([""] * len(pick), index=pick.index)).fillna("").astype(str).map(lambda x: 7 if x.startswith("A1") else 6 if x.startswith(("A0", "A2")) else 5 if x.startswith("P1") else 4 if x.startswith(("P0", "P2")) else 3 if x.startswith(("R1", "R2", "R3")) else 2 if x.startswith("W1") else 1)
    pick.sort_values(["_h56route", "H56T1確認分", "H55雙路徑隔日分", "H55反轉點火路徑分", "_stage", "H54隔日真相分", "H51發動潛力分"], ascending=False, inplace=True, kind="mergesort")
    cols = [c for c in [
        "股票代號", "股票名稱", "類別", "H51市場地位", "H51交易許可", "H51推薦等級", "H56最終參考層級", "H56上游權威層級", "H56隔夜證據狀態", "H56盤前重驗需求", "H56T1確認分", "H55參考層級", "H55機會型態", "H55雙路徑隔日分", "H55主線延續路徑分", "H55反轉點火路徑分", "H55逆風韌性分", "H55催化代理分", "H55回補雷達分", "H54決策層級", "H54隔日真相分", "H54主流延續分", "H54可執行確認分", "H54耗竭風險分", "H54隔夜風險扣分", "H54資訊空窗風險", "H54輪動備援分", "H53參考層級", "H53隔日優先分", "H53族群共振分", "H53領漲集群分", "H51發動潛力分", "H51專業參考分",
        "H51族群主線分", "H51個股領漲品質分", "H51Pivot起漲分", "H51量價確認分", "H51流動性分",
        "H51基本面資金分", "H51主線新鮮分", "H51急跌收復狀態", "H51路徑RR", "H51RR口徑", "今日漲幅%", "近5日漲幅%", "近20日漲幅%",
        "當日量比", "當日收盤位置%", "上影線比例%", "成交額百萬", "H56決策理由", "H55決策理由", "H51推薦理由"
    ] if c in pick.columns]
    return pick.head(max(1, int(max_rows)))[cols].reset_index(drop=True)


def build_h51_sector_table(frame: pd.DataFrame, max_rows: int = 15) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    work = frame if ("H51版本" in frame.columns and frame["H51版本"].astype(str).eq(VERSION).all() and "H53版本" in frame.columns) else apply_human_master_engine(frame)
    sec = work.get("類別", pd.Series(["未分類"] * len(work), index=work.index)).fillna("未分類").astype(str)
    tmp = pd.DataFrame({
        "類別": sec,
        "H53族群共振分": pd.to_numeric(work.get("H53族群共振分", 50), errors="coerce").fillna(50.0),
        "H53族群廣度分": pd.to_numeric(work.get("H53族群廣度分", 50), errors="coerce").fillna(50.0),
        "H53族群攻擊分": pd.to_numeric(work.get("H53族群攻擊分", 50), errors="coerce").fillna(50.0),
        "H53族群量能分": pd.to_numeric(work.get("H53族群量能分", 50), errors="coerce").fillna(50.0),
        "H53族群資金分": pd.to_numeric(work.get("H53族群資金分", 50), errors="coerce").fillna(50.0),
        "H53族群樣本可信度": pd.to_numeric(work.get("H53族群樣本可信度", 35), errors="coerce").fillna(35.0),
        "H53分類稀釋扣分": pd.to_numeric(work.get("H53分類稀釋扣分", 0), errors="coerce").fillna(0.0),
        "H53隔日優先分": pd.to_numeric(work.get("H53隔日優先分", 50), errors="coerce").fillna(50.0),
        "H51專業參考分": pd.to_numeric(work.get("H51專業參考分", 50), errors="coerce").fillna(50.0),
        "H51發動潛力分": pd.to_numeric(work.get("H51發動潛力分", 50), errors="coerce").fillna(50.0),
        "H54主流延續分": pd.to_numeric(work.get("H54主流延續分", 50), errors="coerce").fillna(50.0),
        "H54隔日真相分": pd.to_numeric(work.get("H54隔日真相分", 50), errors="coerce").fillna(50.0),
        "H54耗竭風險分": pd.to_numeric(work.get("H54耗竭風險分", 50), errors="coerce").fillna(50.0),
        "H54輪動備援分": pd.to_numeric(work.get("H54輪動備援分", 50), errors="coerce").fillna(50.0),
        "H54證據品質分": pd.to_numeric(work.get("H54證據品質分", 50), errors="coerce").fillna(50.0),
        "H55雙路徑隔日分": pd.to_numeric(work.get("H55雙路徑隔日分", 50), errors="coerce").fillna(50.0),
        "H55反轉點火路徑分": pd.to_numeric(work.get("H55反轉點火路徑分", 50), errors="coerce").fillna(50.0),
        "H55催化代理分": pd.to_numeric(work.get("H55催化代理分", 50), errors="coerce").fillna(50.0),
        "H55逆風韌性分": pd.to_numeric(work.get("H55逆風韌性分", 50), errors="coerce").fillna(50.0),
    })
    grp = tmp.groupby("類別", dropna=False).agg(
        H53族群共振分=("H53族群共振分", "mean"),
        H53族群廣度分=("H53族群廣度分", "mean"),
        H53族群攻擊分=("H53族群攻擊分", "mean"),
        H53族群量能分=("H53族群量能分", "mean"),
        H53族群資金分=("H53族群資金分", "mean"),
        H53族群前三隔日優先=("H53隔日優先分", lambda x: x.nlargest(3).mean()),
        H53族群前三發動=("H51發動潛力分", lambda x: x.nlargest(3).mean()),
        H53族群前三品質=("H51專業參考分", lambda x: x.nlargest(3).mean()),
        H53族群樣本可信度=("H53族群樣本可信度", "mean"),
        H53分類稀釋扣分=("H53分類稀釋扣分", "max"),
        H53族群樣本數=("H53族群共振分", "size"),
        H54族群延續分=("H54主流延續分", lambda x: x.nlargest(3).mean()),
        H54族群隔日真相分=("H54隔日真相分", lambda x: x.nlargest(3).mean()),
        H54族群耗竭風險=("H54耗竭風險分", "mean"),
        H54族群輪動備援=("H54輪動備援分", lambda x: x.nlargest(3).mean()),
        H54族群證據品質=("H54證據品質分", "mean"),
        H55族群雙路徑分=("H55雙路徑隔日分", lambda x: x.nlargest(3).mean()),
        H55族群反轉點火分=("H55反轉點火路徑分", lambda x: x.nlargest(3).mean()),
        H55族群催化代理分=("H55催化代理分", lambda x: x.nlargest(3).mean()),
        H55族群逆風韌性分=("H55逆風韌性分", lambda x: x.nlargest(3).mean()),
    ).reset_index()
    grp["H53族群決策分"] = (
        grp["H53族群共振分"] * 0.40 + grp["H53族群前三隔日優先"] * 0.25
        + grp["H53族群前三發動"] * 0.15 + grp["H53族群前三品質"] * 0.10
        + grp["H53族群樣本可信度"] * 0.10
    ).round(2)
    grp["H54族群決策分"] = (
        grp["H54族群延續分"] * 0.34 + grp["H54族群隔日真相分"] * 0.31
        + grp["H54族群證據品質"] * 0.18 + grp["H54族群輪動備援"] * 0.17
        - grp["H54族群耗竭風險"] * 0.15
    ).clip(0, 100).round(2)
    grp["H55族群機會分"] = (
        grp["H54族群決策分"] * 0.45 + grp["H55族群雙路徑分"] * 0.28
        + grp["H55族群反轉點火分"] * 0.15 + grp["H55族群催化代理分"] * 0.07
        + grp["H55族群逆風韌性分"] * 0.05
    ).clip(0, 100).round(2)
    grp.sort_values(["H55族群機會分", "H55族群雙路徑分", "H54族群決策分", "H55族群反轉點火分"], ascending=False, inplace=True, kind="mergesort")
    grp.insert(0, "H55族群排名", range(1, len(grp) + 1))
    return grp.head(max(1, int(max_rows))).reset_index(drop=True)


__all__ = ["VERSION", "H51_COLUMNS", "H53_COLUMNS", "H54_COLUMNS", "H55_COLUMNS", "apply_human_master_engine", "build_h51_final_decision_table", "build_h51_mainstream_leader_table", "build_h51_sector_table"]

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
import math
import re
import pandas as pd

VERSION = "v191_h52_mainstream_precision_ignition_truth_20260827"

H51_COLUMNS = [
    "H51族群主線分", "H51個股領漲品質分", "H51Pivot起漲分", "H51量價確認分",
    "H51流動性分", "H51基本面資金分", "H51主線新鮮分", "H51重複推薦扣分",
    "H51發動潛力分", "H51專業參考分", "H51可執行分", "H51市場地位", "H51交易許可", "H51推薦等級",
    "H51急跌收復狀態",
    "H51路徑RR", "H51RR口徑", "H51推薦理由", "H51版本",
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
    if shock_down or weak_breakdown:
        permission = "WAIT-RECLAIM｜急跌/跌停後先確認收復，不列高品質等待"
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

    if ready and pro >= 80:
        level = "A+｜真人主線可執行"
    elif ready:
        level = "A｜真人主線可執行"
    elif prep and pro >= 72:
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
        f"重複扣分{repeat_penalty:.1f}/延伸扣分{extension_penalty:.1f}/急跌扣分{shock_penalty:.1f}"
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
    return work


def build_h51_final_decision_table(frame: pd.DataFrame, max_rows: int = 6) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame({"狀態": ["目前沒有可建立H51最終決策的候選資料。"]})
    work = frame if ("H51版本" in frame.columns and frame["H51版本"].astype(str).eq(VERSION).all()) else apply_human_master_engine(frame)
    code = work.get("股票代號", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str).str.strip()
    work = work.loc[code.ne("")].copy()
    perm = work.get("H51交易許可", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    # First sheet is an action/preparation sheet only. Research-only LEADER-WATCH belongs in 主流領漲股.
    priority = perm.map(lambda x: 4 if x.startswith("BUY-READY") else 3 if x.startswith("SETUP-PREP") else 0)
    work = work.loc[priority.gt(0)].copy()
    if work.empty:
        return pd.DataFrame({
            "狀態": ["今天沒有通過H51『主流族群→領漲股→Pivot/再攻→量價→流動性→路徑RR』的高品質候選。"],
            "操作原則": ["不以成熟主流或低品質雷達補位；請看『主流族群與領漲股』了解下一批等待名單。"],
        })
    work["_p"] = work["H51交易許可"].astype(str).map(lambda x: 4 if x.startswith("BUY-READY") else 3)
    for c in ["H51專業參考分", "H51可執行分", "H51Pivot起漲分", "H51個股領漲品質分", "H51族群主線分"]:
        work[c] = pd.to_numeric(work.get(c, 0), errors="coerce").fillna(0.0)
    work.sort_values(["_p", "H51專業參考分", "H51可執行分", "H51Pivot起漲分", "H51個股領漲品質分"], ascending=False, inplace=True, kind="mergesort")
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
        if permx.startswith("BUY-READY"):
            action = "可執行候選｜仍依觸發/守價分批"
        elif permx.startswith("SETUP-PREP"):
            action = "高品質等待｜未確認買點前不進場"
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
        })
    return pd.DataFrame(rows)


def build_h51_mainstream_leader_table(frame: pd.DataFrame, max_rows: int = 20) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    work = frame if ("H51版本" in frame.columns and frame["H51版本"].astype(str).eq(VERSION).all()) else apply_human_master_engine(frame)
    status = work.get("H51市場地位", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    pick = work.loc[~status.str.startswith("HM-NO")].copy()
    if pick.empty:
        return pick
    for c in ["H51發動潛力分", "H51專業參考分", "H51族群主線分", "H51個股領漲品質分", "H51Pivot起漲分", "H51流動性分", "H51路徑RR"]:
        pick[c] = pd.to_numeric(pick.get(c, 0), errors="coerce").fillna(0.0)
    pick["_stage"] = pick["H51市場地位"].astype(str).map(lambda x: 5 if x.startswith("HM-EARLY") else 4 if x.startswith("HM-PULLBACK") else 3 if x.startswith("HM-LEADER") else 2 if x.startswith("HM-SETUP") else 1)
    pick.sort_values(["_stage", "H51發動潛力分", "H51專業參考分", "H51個股領漲品質分", "H51Pivot起漲分"], ascending=False, inplace=True, kind="mergesort")
    cols = [c for c in [
        "股票代號", "股票名稱", "類別", "H51市場地位", "H51交易許可", "H51推薦等級", "H51發動潛力分", "H51專業參考分",
        "H51族群主線分", "H51個股領漲品質分", "H51Pivot起漲分", "H51量價確認分", "H51流動性分",
        "H51基本面資金分", "H51主線新鮮分", "H51急跌收復狀態", "H51路徑RR", "H51RR口徑", "今日漲幅%", "近5日漲幅%", "近20日漲幅%",
        "當日量比", "當日收盤位置%", "上影線比例%", "成交額百萬", "H51推薦理由"
    ] if c in pick.columns]
    return pick.head(max(1, int(max_rows)))[cols].reset_index(drop=True)


def build_h51_sector_table(frame: pd.DataFrame, max_rows: int = 15) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    work = frame if ("H51版本" in frame.columns and frame["H51版本"].astype(str).eq(VERSION).all()) else apply_human_master_engine(frame)
    sec = work.get("類別", pd.Series(["未分類"] * len(work), index=work.index)).fillna("未分類").astype(str)
    tmp = pd.DataFrame({"類別": sec, "H51族群主線分": pd.to_numeric(work.get("H51族群主線分", 0), errors="coerce").fillna(0.0),
                        "H51專業參考分": pd.to_numeric(work.get("H51專業參考分", 0), errors="coerce").fillna(0.0),
                        "H51Pivot起漲分": pd.to_numeric(work.get("H51Pivot起漲分", 0), errors="coerce").fillna(0.0)})
    market = work.get("H51市場地位", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    tmp["_fresh"] = market.str.startswith(("HM-EARLY", "HM-PULLBACK", "HM-LEADER", "HM-SETUP")).astype(float) * 100.0
    grp = tmp.groupby("類別", dropna=False).agg(H51族群主線分=("H51族群主線分", "mean"), H51族群前三品質=("H51專業參考分", lambda s: s.nlargest(3).mean()), H51族群Pivot品質=("H51Pivot起漲分", lambda s: s.nlargest(3).mean()), H51新鮮主線比例=("_fresh", "mean"), H51族群樣本數=("_fresh", "size")).reset_index()
    grp["H51族群決策分"] = (grp["H51族群主線分"] * 0.45 + grp["H51族群前三品質"] * 0.30 + grp["H51族群Pivot品質"] * 0.15 + grp["H51新鮮主線比例"] * 0.10).round(2)
    grp.sort_values("H51族群決策分", ascending=False, inplace=True, kind="mergesort")
    grp.insert(0, "H51族群排名", range(1, len(grp) + 1))
    return grp.head(max(1, int(max_rows))).reset_index(drop=True)


__all__ = ["VERSION", "H51_COLUMNS", "apply_human_master_engine", "build_h51_final_decision_table", "build_h51_mainstream_leader_table", "build_h51_sector_table"]

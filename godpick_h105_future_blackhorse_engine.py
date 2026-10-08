# -*- coding: utf-8 -*-
"""V191-H105 Future Blackhorse / Pre-Ignition Ranking Engine.

Goal
----
Rank *future* 1~5 trading-day ignition candidates instead of simply rewarding
stocks that are already the strongest today.

H105 is a research/discovery layer only.  It never creates or relaxes Formal,
A-, R1, V188, H51, H56 or H99 execution authority.  Missing evidence remains
missing; no network request is made here.

Design principles
-----------------
1. Future ignition > current-day strength.
2. Reward controlled accumulation, institutional turn, compression and sector
   rotation before the move is fully priced in.
3. Penalize extension, blow-off volume, chase risk and already-completed moves.
4. Keep data completeness explicit and separate from the score.
5. Keep actionable execution authority unchanged.
"""
from __future__ import annotations

from typing import Any, Iterable
from pathlib import Path
import math
import pandas as pd
from godpick_h109_authority_governor import govern as h109_govern, parse_day as h109_parse_day, official_snapshot as h109_official_snapshot, official_announcements as h109_official_announcements, apply_quarantine_continuity as h109_apply_quarantine_continuity, join_official as h109_join_official, VERSION as H109_VERSION

try:
    from godpick_h104_daily_discovery import (
        save_discovery_snapshot, load_sector_history, sector_continuity, save_sector_snapshot
    )
except Exception:  # compatibility if H104/H108 continuity is not yet deployed
    save_discovery_snapshot = None
    load_sector_history = None
    sector_continuity = None
    save_sector_snapshot = None

VERSION = "v191_h109_cross_layer_governor_20261008"

H105_COLUMNS = [
    "H105版本",
    "H105黑馬預發動分",
    "H105黑馬同層順位",
    "H105未來發動窗口",
    "H105資金潛伏分",
    "H105法人轉折分",
    "H105籌碼收斂分",
    "H105技術蓄勢分",
    "H105下一波族群分",
    "H105券商分點分",
    "H105催化未反映分",
    "H105進場成熟分",
    "H105過熱追高風險分",
    "H105今日已發動程度",
    "H105證據完整度%",
    "H105核心證據完整度%",
    "H105選配證據完整度%",
    "H107新鮮機會分",
    "H107近期入選日數",
    "H107近期族群入選日數",
    "H107重複推薦懲罰",
    "H107新發現狀態",
    "H107研究池調整",
    "H108族群確認分",
    "H108族群生命週期",
    "H108個股族群角色",
    "H108族群波段加分",
    "H108族群懲罰減免",
    "H108研究優先分",
    "H108族群前次資料日",
    "H108族群前次排名",
    "H108族群排名改善",
    "H108族群資金流變化",
    "H108族群廣度變化",
    "H109版本", "H109決策權威", "H109否決原因", "H109官方營收新鮮度",
    "H109營收資料年月", "H109催化方向", "H109重大訊息狀態", "H109重大訊息標題", "H109重大訊息發布日", "H109重大訊息來源", "H109最終研究優先分",
    "H109時機窗口", "H109跨層衝突", "H109正式買進權限", "H109隔離觀察日數",
    "H105發動階段",
    "H105黑馬層級",
    "H105推薦性質",
    "H105今日強勢排除",
    "H105資料缺口",
    "H105主要理由",
    "H105建議動作",
    "H105Formal權限",
]

BLACKHORSE_OVERVIEW_COLUMNS = [
    "H105黑馬順位", "股票代號", "股票名稱", "類別", "市場別",
    "H105黑馬預發動分", "H105未來發動窗口", "H105黑馬層級", "H105發動階段",
    "H105資金潛伏分", "H105法人轉折分", "H105籌碼收斂分", "H105技術蓄勢分",
    "H105下一波族群分", "H105券商分點分", "H105催化未反映分", "H105進場成熟分",
    "H105過熱追高風險分", "H105今日已發動程度", "H105今日強勢排除", "H105證據完整度%",
    "H105核心證據完整度%", "H105選配證據完整度%", "H107新鮮機會分",
    "H107近期入選日數", "H107近期族群入選日數", "H107重複推薦懲罰", "H107新發現狀態",
    "H108族群確認分", "H108族群生命週期", "H108個股族群角色", "H108族群波段加分",
    "H108族群懲罰減免", "H108研究優先分", "H108族群前次資料日", "H108族群前次排名",
    "H108族群排名改善", "H108族群資金流變化", "H108族群廣度變化",
    "H109決策權威", "H109否決原因", "H109最終研究優先分", "H109跨層衝突",
    "H109官方營收新鮮度", "H109營收資料年月", "H109催化方向", "H109時機窗口", "H109重大訊息狀態", "H109重大訊息標題", "H109重大訊息發布日", "H109重大訊息來源",
    "H104推薦性質", "H105推薦性質", "H107研究池調整", "H105資料缺口", "H105主要理由", "H105建議動作",
    "H99主進場", "H99防守停損", "H99第一目標", "H99成本後RR", "H99目標交易日",
]

_BLANK = {"", "nan", "none", "null", "nat", "--", "-", "<na>"}


def _text(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    s = str(v).strip()
    return "" if s.lower() in _BLANK else s


def _num(v: Any, default: float | None = None) -> float | None:
    if v is None or isinstance(v, bool):
        return default
    try:
        if isinstance(v, str):
            v = v.replace(",", "").replace("％", "%").replace("%", "").replace("+", "").strip()
            if not v or v.lower() in _BLANK:
                return default
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _clip(v: float | None, lo: float = 0.0, hi: float = 100.0, default: float = 50.0) -> float:
    x = default if v is None else float(v)
    return max(lo, min(hi, x))


def _first_num(row: dict[str, Any] | pd.Series, names: Iterable[str], default: float | None = None) -> float | None:
    for name in names:
        try:
            val = row.get(name)
        except Exception:
            val = None
        x = _num(val, None)
        if x is not None:
            return x
    return default


def _first_text(row: dict[str, Any] | pd.Series, names: Iterable[str], default: str = "") -> str:
    for name in names:
        try:
            s = _text(row.get(name))
        except Exception:
            s = ""
        if s:
            return s
    return default


def _code(v: Any) -> str:
    s = _text(v)
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s.zfill(4) if s.isdigit() and len(s) < 4 else s


def _piecewise_center(value: float, ideal_lo: float, ideal_hi: float, soft_lo: float, soft_hi: float) -> float:
    """100 inside ideal band, linearly decays to 40 at soft edges, then lower."""
    v = float(value)
    if ideal_lo <= v <= ideal_hi:
        return 100.0
    if soft_lo <= v < ideal_lo:
        span = max(1e-9, ideal_lo - soft_lo)
        return 40.0 + 60.0 * (v - soft_lo) / span
    if ideal_hi < v <= soft_hi:
        span = max(1e-9, soft_hi - ideal_hi)
        return 100.0 - 60.0 * (v - ideal_hi) / span
    return 22.0 if v < soft_lo else 18.0


def _flow_sign_score(v: float | None) -> float:
    if v is None:
        return 50.0
    if v > 0:
        return 72.0
    if v < 0:
        return 28.0
    return 50.0


def _institution_turn_score(row: dict[str, Any]) -> tuple[float, list[str], bool]:
    total1 = _first_num(row, ["三大法人近1日合計_官方", "三大法人近1日合計"])
    total3 = _first_num(row, ["三大法人近3日合計"])
    total5 = _first_num(row, ["三大法人近5日合計_官方", "三大法人近5日合計"])
    foreign1 = _first_num(row, ["外資近1日買賣超_官方", "外資近1日買賣超"])
    foreign3 = _first_num(row, ["外資近3日買賣超"])
    foreign5 = _first_num(row, ["外資近5日買賣超_官方", "外資近5日買賣超"])
    trust1 = _first_num(row, ["投信近1日買賣超_官方", "投信近1日買賣超"])
    trust3 = _first_num(row, ["投信近3日買賣超"])
    trust5 = _first_num(row, ["投信近5日買賣超_官方", "投信近5日買賣超"])
    days = _first_num(row, ["法人連買天數_官方", "法人連買天數"])
    official = _first_num(row, ["法人籌碼官方分數", "法人籌碼分數", "法人連買代理分數"])
    ratio = _first_num(row, ["法人買超占量比%", "法人買超占成交量%"])

    present = [x for x in [total1,total3,total5,foreign1,foreign3,foreign5,trust1,trust3,trust5,days,official,ratio] if x is not None]
    notes: list[str] = []
    if not present:
        return 50.0, ["法人趨勢缺資料"], False

    score = _clip(official, default=50.0) * 0.30
    trend = 50.0
    # A turn from weak 5d to improving 1d/3d is more valuable than an already
    # obvious long buy streak for this *pre-ignition* objective.
    if total1 is not None and total5 is not None:
        if total1 > 0 and total5 <= 0:
            trend = 94.0; notes.append("三大法人由賣轉買")
        elif total1 > 0 and total3 is not None and total3 > 0 and total5 > 0:
            trend = 78.0; notes.append("三大法人短中期同向買超")
        elif total1 < 0 and total3 is not None and total3 > 0:
            trend = 58.0; notes.append("法人單日回吐但3日仍正")
        elif total1 < 0 and total5 < 0:
            trend = 28.0; notes.append("三大法人仍偏賣")
    else:
        trend = (_flow_sign_score(total1) * 0.6 + _flow_sign_score(total5) * 0.4)
    score += trend * 0.28

    foreign = (_flow_sign_score(foreign1) * 0.45 + _flow_sign_score(foreign3) * 0.25 + _flow_sign_score(foreign5) * 0.30)
    trust = (_flow_sign_score(trust1) * 0.50 + _flow_sign_score(trust3) * 0.20 + _flow_sign_score(trust5) * 0.30)
    if trust1 is not None and trust1 > 0 and (trust5 or 0) <= 0:
        trust = max(trust, 92.0); notes.append("投信轉買")
    if foreign1 is not None and foreign1 > 0 and (foreign5 or 0) <= 0:
        foreign = max(foreign, 88.0); notes.append("外資轉買")
    score += foreign * 0.12 + trust * 0.14

    if ratio is None:
        ratio_score = 50.0
    else:
        # Moderate participation is preferred over an already crowded one-day spike.
        ratio_score = 95.0 if 2 <= ratio <= 12 else 78.0 if 0 < ratio < 2 else 72.0 if 12 < ratio <= 20 else 38.0 if ratio < 0 else 55.0
    score += ratio_score * 0.08

    if days is None:
        days_score = 50.0
    elif 1 <= days <= 4:
        days_score = 92.0
    elif 5 <= days <= 8:
        days_score = 76.0
    elif days > 8:
        days_score = 62.0
    else:
        days_score = 45.0
    score += days_score * 0.08
    return _clip(score), notes, True


def _holder_score(row: dict[str, Any]) -> tuple[float, list[str], bool]:
    tdcc = _first_num(row, ["TDCC千張大戶週變化pp", "TDCC千張大戶週變化", "千張大戶週變化pp"])
    proxy = _first_num(row, ["大戶鎖碼分數", "大戶鎖碼代理分數", "大戶承接分"])
    notes: list[str] = []
    present = tdcc is not None or proxy is not None
    if not present:
        return 50.0, ["TDCC/大戶籌碼缺資料"], False
    score = _clip(proxy, default=50.0)
    if tdcc is not None:
        tdcc_s = _clip(50.0 + tdcc * 18.0)
        score = score * 0.55 + tdcc_s * 0.45
        notes.append(f"TDCC千張大戶週變化{tdcc:+.2f}pp")
    return _clip(score), notes, True


def _technical_setup_score(row: dict[str, Any]) -> tuple[float, list[str], bool]:
    h57 = _first_num(row, ["H57飆股發動前兆分"])
    compression = _first_num(row, ["H57波動壓縮分"])
    expansion = _first_num(row, ["H57壓縮轉擴張分"])
    rs_turn = _first_num(row, ["H57相對強度轉折分"])
    early = _first_num(row, ["H57提前視窗分"])
    pre = _first_num(row, ["起漲前兆分數", "起漲前兆分", "爆發力分數"])
    prep = _first_num(row, ["突破準備分", "型態突破分數"])
    turn = _first_num(row, ["止跌轉強分數", "均線轉強分", "動能翻多分"])
    support = _first_num(row, ["支撐回測分數", "拉回承接分數"])
    pressure = _first_num(row, ["距20日高點%", "20日壓力距離%"])
    notes: list[str] = []
    vals = [x for x in [h57,compression,expansion,rs_turn,early,pre,prep,turn,support,pressure] if x is not None]
    if not vals:
        return 50.0, ["技術蓄勢資料不足"], False
    if h57 is not None:
        score = h57 * 0.34
        score += _clip(compression, default=50) * 0.13
        score += _clip(expansion, default=50) * 0.13
        score += _clip(rs_turn, default=50) * 0.16
        score += _clip(early, default=50) * 0.14
        score += _clip(pre, default=50) * 0.10
        notes.append("沿用H57發動前兆")
    else:
        score = _clip(pre, default=50) * 0.30 + _clip(prep, default=50) * 0.25 + _clip(turn, default=50) * 0.20 + _clip(support, default=50) * 0.15
        if pressure is not None:
            score += _piecewise_center(pressure, 1.0, 7.0, 0.0, 15.0) * 0.10
        else:
            score += 50 * 0.10
    if pressure is not None and 1.0 <= pressure <= 8.0:
        notes.append("接近壓力但尚未明顯突破")
    return _clip(score), notes, True


def _sector_lifecycle_detail(row: dict[str, Any], sector_row: dict[str, Any] | None = None) -> dict[str, Any]:
    """Fuse broad sector breadth with H102 fast rotation evidence.

    H107 intentionally diversified sectors, but a fixed diversity penalty can hide a
    *real* sector ignition. H108 separates sector life-cycle from individual chase
    risk: a sector may be IGNITION while a not-yet-started member is still a future
    blackhorse.
    """
    sr = sector_row or {}
    def n(names: list[str], *, row_first: bool = False) -> float | None:
        if row_first:
            return _first_num(row, names, _first_num(sr, names))
        return _first_num(sr, names, _first_num(row, names))

    shock = n(["H102族群衝擊分"], row_first=True)
    accel = n(["類股加速度"])
    flow = n(["族群資金流分數", "主流資金分"])
    heat = n(["類股熱度分數"])
    rank = n(["類股熱度排名"])
    breadth = n(["同族群強勢比例", "H57族群點火廣度分"])
    volume = n(["同族群平均量能分"])
    avg_ret = n(["類股平均漲幅"])
    sample_conf = n(["族群樣本可信度"])
    h102_state = _first_text(row, ["H102族群動態狀態"])
    broad_state = _first_text(sr, ["族群輪動狀態", "強勢族群等級"], _first_text(row, ["族群輪動狀態", "強勢族群等級"]))
    states = f"{h102_state} {broad_state}".upper()

    rank_score = 50.0
    if rank is not None:
        if rank <= 2:
            rank_score = 90.0
        elif rank <= 6:
            rank_score = 96.0
        elif rank <= 12:
            rank_score = 86.0
        elif rank <= 20:
            rank_score = 70.0
        else:
            rank_score = 45.0

    # H102 fast-shock gets the largest weight because it already measures the
    # cross-sectional ignition that broad sector averages can dilute.
    confirm = (
        _clip(shock, default=50) * 0.30 + _clip(accel, default=50) * 0.12 +
        _clip(flow, default=50) * 0.16 + _clip(breadth, default=50) * 0.12 +
        _clip(volume, default=50) * 0.10 + _clip(heat, default=50) * 0.08 +
        _clip(sample_conf, default=50) * 0.06 + rank_score * 0.06
    )
    if "IGNITION" in states or any(k in states for k in ["點火", "加速"]):
        confirm += 8.0
    elif any(k in states for k in ["輪動轉強", "升溫", "轉強", "資金流入"]):
        confirm += 4.0
    if any(k in states for k in ["退潮", "降溫", "弱勢"]):
        confirm -= 10.0

    rank_improve = _first_num(sr, ["H108族群排名改善"])
    flow_delta = _first_num(sr, ["H108族群資金流變化"])
    breadth_delta = _first_num(sr, ["H108族群廣度變化"])
    if rank_improve is not None and rank_improve >= 3:
        confirm += 4.0
    if flow_delta is not None and flow_delta >= 8:
        confirm += 3.0
    if breadth_delta is not None and breadth_delta >= 8:
        confirm += 3.0
    if flow_delta is not None and flow_delta <= -10:
        confirm -= 4.0
    if breadth_delta is not None and breadth_delta <= -10:
        confirm -= 4.0
    confirm = _clip(confirm)

    # A high one-day sector return is not a reason to call it "future".  It is
    # instead a mature/extension warning; H108 then looks for the *next* member.
    extended = (avg_ret is not None and avg_ret >= 9.5) or (heat is not None and heat >= 84 and (rank or 99) <= 3)
    if any(k in states for k in ["退潮", "降溫", "弱勢"]):
        lifecycle = "FADE｜退潮"
    elif extended and confirm >= 68:
        lifecycle = "MATURE｜族群已大幅發動"
    elif confirm >= 78 and ("IGNITION" in states or (shock is not None and shock >= 82) or any(k in states for k in ["點火", "加速"])):
        lifecycle = "IGNITION｜族群點火"
    elif confirm >= 72:
        lifecycle = "EXPANSION｜擴散轉強"
    elif confirm >= 63:
        lifecycle = "PREHEAT｜升溫前段"
    else:
        lifecycle = "NEUTRAL｜未確認"

    return {
        "confirm": round(confirm, 2), "lifecycle": lifecycle, "shock": shock, "accel": accel,
        "flow": flow, "heat": heat, "rank": rank, "breadth": breadth, "volume": volume,
        "avg_ret": avg_ret, "state": f"{h102_state}｜{broad_state}".strip("｜"),
        "prev_day": _first_text(sr, ["H108族群前次資料日"]),
        "prev_rank": _first_num(sr, ["H108族群前次排名"]),
        "rank_improve": rank_improve, "flow_delta": flow_delta, "breadth_delta": breadth_delta,
    }


def _sector_next_wave_score(row: dict[str, Any], sector_row: dict[str, Any] | None = None) -> tuple[float, list[str], bool, dict[str, Any]]:
    d = _sector_lifecycle_detail(row, sector_row)
    vals = [d.get(k) for k in ("shock", "accel", "flow", "heat", "rank", "breadth", "volume") if d.get(k) is not None]
    if not vals and not d.get("state"):
        return 50.0, ["族群輪動資料不足"], False, d
    notes: list[str] = []
    lifecycle = _text(d.get("lifecycle"))
    rank = d.get("rank")
    if "IGNITION" in lifecycle:
        notes.append("H108確認族群點火")
    elif "EXPANSION" in lifecycle:
        notes.append("H108確認族群擴散轉強")
    elif "PREHEAT" in lifecycle:
        notes.append("H108族群升溫前段")
    elif "MATURE" in lifecycle:
        notes.append("H108族群已大幅發動，僅找未發動第二梯隊")
    if d.get("rank_improve") is not None and d["rank_improve"] >= 3:
        notes.append(f"族群排名改善+{d['rank_improve']:.0f}")
    if d.get("flow_delta") is not None and d["flow_delta"] >= 8:
        notes.append("族群資金流加速")
    if d.get("breadth_delta") is not None and d["breadth_delta"] >= 8:
        notes.append("族群強勢廣度擴張")

    if rank is None:
        rank_s = 50.0
    elif rank <= 2 and any(x in lifecycle for x in ["IGNITION", "EXPANSION"]):
        rank_s = 90.0
    elif 3 <= rank <= 12:
        rank_s = 96.0
    elif 13 <= rank <= 20:
        rank_s = 78.0
    elif rank in (1, 2):
        rank_s = 58.0
    else:
        rank_s = 48.0

    base = (
        _clip(d.get("accel"), default=50) * 0.18 + _clip(d.get("shock"), default=50) * 0.27 +
        _clip(d.get("flow"), default=50) * 0.18 + _clip(d.get("breadth"), default=50) * 0.12 +
        _clip(d.get("volume"), default=50) * 0.08 + rank_s * 0.09 + _clip(d.get("heat"), default=50) * 0.08
    )
    score = base * 0.72 + float(d.get("confirm") or 50.0) * 0.28
    if "MATURE" in lifecycle:
        score -= 6.0
    if "FADE" in lifecycle:
        score -= 10.0
    return _clip(score), notes, True, d


def _sector_wave_adjustment(detail: dict[str, Any], started: float, overheat: float, tech: float) -> tuple[float, str]:
    life = _text(detail.get("lifecycle"))
    confirm = float(_num(detail.get("confirm"), 50.0) or 50.0)
    bonus = 0.0
    role = "NEUTRAL｜一般個股"
    if started >= 72 or overheat >= 72:
        role = "LEADER-IGNITED｜個股已發動"
        return 0.0, role
    if "IGNITION" in life and confirm >= 78:
        if started < 55 and tech >= 55:
            bonus = 8.0; role = "SECOND-WAVE｜族群點火中的未發動第二梯隊"
        elif started < 65:
            bonus = 5.0; role = "EARLY-FOLLOWER｜族群點火初段跟隨"
        else:
            bonus = 2.0; role = "LATE-FOLLOWER｜族群已強但個股尚未過熱"
    elif "EXPANSION" in life and confirm >= 72:
        bonus = 4.0 if started < 60 else 1.5
        role = "EXPANSION-FOLLOWER｜族群擴散候選"
    elif "PREHEAT" in life:
        bonus = 2.0; role = "SCOUT｜族群升溫前段偵察"
    elif "MATURE" in life:
        bonus = -5.0; role = "MATURE-LAGGARD｜族群成熟期僅保留低位補漲候選"
    elif "FADE" in life:
        bonus = -8.0; role = "FADE-RISK｜族群退潮"
    return bonus, role


def _broker_flow_score(row: dict[str, Any]) -> tuple[float, list[str], bool]:
    """Use only explicit broker/branch evidence; never relabel generic 'main-force' proxies as broker data."""
    base = _first_num(row, [
        "券商分點分數", "券商分點動向分", "券商分點集中分", "券商分點集中度分",
        "分點籌碼分", "分點資金流分",
    ])
    top5 = _first_num(row, ["Top5分點淨買比%", "前五分點淨買比%", "券商Top5淨買比%", "分點Top5淨買比%"] )
    days = _first_num(row, ["券商分點連買天數", "分點連買天數", "分點連續買超天數"] )
    concentration = _first_num(row, ["分點集中度%", "券商分點集中度%", "買方分點集中度%"] )
    if base is None and top5 is None and days is None and concentration is None:
        return 50.0, ["券商分點資料未接入/不足"], False
    score = _clip(base, default=50.0)
    notes: list[str] = []
    if top5 is not None:
        score += max(-12.0, min(14.0, top5 * 0.45))
        if top5 >= 12: notes.append("買方Top5分點淨買集中")
    if days is not None:
        if 2 <= days <= 5:
            score += 8.0; notes.append("分點連續承接但尚非長期擁擠")
        elif days >= 9:
            score -= 5.0
    if concentration is not None:
        score += max(-6.0, min(10.0, (concentration - 50.0) * 0.22))
    return _clip(score), notes, True


def _catalyst_unpriced_score(row: dict[str, Any]) -> tuple[float, list[str], bool]:
    catalyst = _first_num(row, ["H102Catalyst分", "H55催化代理分", "H81新聞事件影響分", "營收成長官方分數", "官方基本面成長分數"])
    yoy = _first_num(row, ["月營收YoY%_官方", "月營收YoY%"])
    mom = _first_num(row, ["月營收MoM%_官方", "月營收MoM%"])
    news_state = _first_text(row, ["H103新聞證據", "H94新聞證據狀態"])
    ret1 = _first_num(row, ["今日漲幅%", "當日漲跌幅%", "今日漲跌幅%", "單日漲跌幅%"], 0.0) or 0.0
    ret5 = _first_num(row, ["近5日漲幅%", "5日漲幅%"], 0.0) or 0.0
    evidence = catalyst is not None or yoy is not None or mom is not None or (news_state and not news_state.startswith("MISSING"))
    if not evidence:
        return 50.0, ["可驗證催化/營收資料不足"], False
    base = _clip(catalyst, default=50.0)
    if yoy is not None:
        base += max(-8.0, min(10.0, yoy * 0.22))
    if mom is not None:
        base += max(-5.0, min(6.0, mom * 0.16))
    priced_penalty = max(0.0, ret1 - 3.0) * 3.5 + max(0.0, ret5 - 9.0) * 1.1
    notes = ["催化尚未完全反映"] if base >= 62 and priced_penalty <= 8 else []
    return _clip(base - priced_penalty), notes, True


def _entry_maturity(row: dict[str, Any]) -> float:
    entry = _first_num(row, ["進場時機分數", "Entry進場買點分", "拉回買點分數", "突破買點分數"])
    execution = _first_num(row, ["H89執行品質分", "交易可行分數"])
    rr = _first_num(row, ["H99成本後RR", "H89成本後RR1", "風險報酬_拉回", "風險報酬比_決策"])
    rr_s = 50.0 if rr is None else 90.0 if rr >= 2.0 else 78.0 if rr >= 1.5 else 62.0 if rr >= 1.2 else 35.0
    return _clip(_clip(entry, default=50) * 0.50 + _clip(execution, default=50) * 0.32 + rr_s * 0.18)


def _overheat_and_started(row: dict[str, Any]) -> tuple[float, float, list[str]]:
    # IMPORTANT: never treat generic 「區間漲跌幅%」 as today's return. In historical/cache
    # payloads that field may cover a much longer window and would falsely label most stocks as
    # already ignited. Only explicit daily-return fields may drive the "today strong" gate.
    ret1 = _first_num(row, ["今日漲幅%", "當日漲跌幅%", "今日漲跌幅%", "單日漲跌幅%"], 0.0) or 0.0
    ret5 = _first_num(row, ["近5日漲幅%", "5日漲幅%"], 0.0) or 0.0
    ret20 = _first_num(row, ["近20日漲幅%", "20日漲幅%"], 0.0) or 0.0
    chase = _first_num(row, ["追價風險分數_決策", "追價風險分", "追價風險分數", "追高風險分數"], 50.0) or 50.0
    vr = _first_num(row, ["當日量比", "均量比"], 1.0) or 1.0
    breakout = _first_num(row, ["突破20日高點%"], 0.0) or 0.0
    exhaust = _first_num(row, ["H54耗竭風險分", "隔日耗竭風險分"], 50.0) or 50.0
    strength_state = _first_text(row, ["近期強勢狀態", "主流主升判定", "主流股判定"])

    over = 15.0
    over += max(0.0, ret1 - 3.0) * 6.0
    over += max(0.0, ret5 - 10.0) * 2.0
    over += max(0.0, ret20 - 24.0) * 0.8
    over += max(0.0, chase - 55.0) * 0.35
    over += max(0.0, vr - 1.8) * 12.0
    over += max(0.0, breakout - 2.0) * 4.0
    over += max(0.0, exhaust - 60.0) * 0.3
    if "強勢主升" in strength_state and ret5 >= 10.0:
        over += 10.0
    over = _clip(over)

    started = 20.0 + max(0.0, ret1) * 7.0 + max(0.0, ret5) * 1.5
    started += max(0.0, vr - 1.0) * 18.0 + max(0.0, breakout) * 5.0
    if "強勢主升" in strength_state:
        started += 18.0
    elif "近期轉強" in strength_state:
        started += 7.0
    started = _clip(started)

    notes: list[str] = []
    if ret1 >= 5.0: notes.append("今日漲幅偏大")
    if ret5 >= 15.0: notes.append("近5日已明顯上漲")
    if vr >= 2.2: notes.append("量比偏高")
    if chase >= 75: notes.append("追價風險偏高")
    if "強勢主升" in strength_state and started >= 72: notes.append("已進入強勢主升段")
    return over, started, notes


def _capital_stealth(row: dict[str, Any], inst: float, holder: float) -> float:
    volume = _first_num(row, ["均量比", "當日量比"], 1.0) or 1.0
    amount_acc = _first_num(row, ["成交額3日加速度%", "成交額5日加速度%"])
    volume_acc = _first_num(row, ["成交量3日加速度%", "成交量5日加速度%"])
    main_money = _first_num(row, ["主流資金分", "資金攻擊有效分", "籌碼續航分"])
    volume_s = _piecewise_center(volume, 0.95, 1.65, 0.65, 2.4)
    acc = 50.0
    if amount_acc is not None or volume_acc is not None:
        a = 50.0 if amount_acc is None else _clip(50 + amount_acc * 0.7)
        v = 50.0 if volume_acc is None else _clip(50 + volume_acc * 0.7)
        acc = a * 0.58 + v * 0.42
    return _clip(inst * 0.32 + holder * 0.22 + volume_s * 0.18 + acc * 0.16 + _clip(main_money, default=50) * 0.12)


def _repeat_governor(
    raw: dict[str, Any], score: float, *, sector_lifecycle: str = "", sector_confirm: float = 50.0, started: float = 0.0
) -> tuple[float, float, int, int, str, float]:
    """Freshness governor with H108 state-dependent sector penalty.

    Same-stock repetition remains penalized.  Repeated *sector* exposure is partly
    forgiven only when an independently confirmed IGNITION/EXPANSION is present and
    the individual stock has not already fired.
    """
    repeat_days = int(_first_num(raw, ["H104先前入選日數"], 0.0) or 0)
    sector_days = int(_first_num(raw, ["H104近期族群入選日數"], 0.0) or 0)
    prior_bh = _first_num(raw, ["H104前次黑馬分"])
    bh_delta = score - prior_bh if prior_bh is not None else _first_num(raw, ["H104黑馬分變化"])
    priority_delta = _first_num(raw, ["H104優先分變化"])
    kind = _first_text(raw, ["H104推薦性質"])

    if "歷史權威尚未建立" in kind or "缺少可比較歷史" in kind:
        return round(score, 2), 0.0, repeat_days, sector_days, "BASELINE｜歷史尚未建立，不冒充新發現", 0.0

    novelty = 4.0 if repeat_days == 0 else 0.0
    stock_penalty = repeat_days * 3.5
    sector_penalty = max(0, sector_days - 2) * 1.2
    relief = 0.0
    life = _text(sector_lifecycle)
    if any(x in life for x in ["IGNITION", "EXPANSION"]) and sector_confirm >= 76 and started < 65:
        relief = sector_penalty * 0.80
        sector_penalty *= 0.20
    elif "PREHEAT" in life and sector_confirm >= 68 and started < 60:
        relief = sector_penalty * 0.35
        sector_penalty *= 0.65
    elif any(x in life for x in ["MATURE", "FADE"]):
        sector_penalty *= 1.25

    penalty = min(18.0, stock_penalty + sector_penalty)
    improved = False
    if bh_delta is not None and bh_delta >= 5.0:
        penalty = max(0.0, penalty - 6.0); improved = True
    elif priority_delta is not None and priority_delta >= 3.0:
        penalty = max(0.0, penalty - 3.0); improved = True

    if repeat_days == 0:
        state = "NEW｜近14日已知歷史未見"
    elif improved:
        state = "IMPROVED-REPEAT｜重複但證據顯著改善"
    elif repeat_days >= 3:
        state = "STALE-REPEAT｜近期重複曝光偏高"
    else:
        state = "CONTINUE｜延續追蹤"
    if relief > 0.05:
        state += "｜SECTOR-CONFIRMED"
    return round(_clip(score + novelty - penalty), 2), round(penalty, 2), repeat_days, sector_days, state, round(relief, 2)


def analyze_candidate(row: dict[str, Any] | pd.Series, *, sector_row: dict[str, Any] | None = None, pool: str = "") -> dict[str, Any]:
    raw = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
    inst, inst_notes, inst_ok = _institution_turn_score(raw)
    holder, holder_notes, holder_ok = _holder_score(raw)
    tech, tech_notes, tech_ok = _technical_setup_score(raw)
    sector, sector_notes, sector_ok, sector_detail = _sector_next_wave_score(raw, sector_row)
    broker, broker_notes, broker_ok = _broker_flow_score(raw)
    catalyst, catalyst_notes, catalyst_ok = _catalyst_unpriced_score(raw)
    entry = _entry_maturity(raw)
    overheat, started, heat_notes = _overheat_and_started(raw)
    capital = _capital_stealth(raw, inst, holder)

    core_flags = [inst_ok, holder_ok, tech_ok, sector_ok]
    optional_flags = [broker_ok, catalyst_ok]
    core_complete = round(sum(bool(x) for x in core_flags) / len(core_flags) * 100.0, 2)
    optional_complete = round(sum(bool(x) for x in optional_flags) / len(optional_flags) * 100.0, 2)
    completeness = round(core_complete * 0.80 + optional_complete * 0.20, 2)
    missing = []
    if not inst_ok: missing.append("法人")
    if not holder_ok: missing.append("TDCC/大戶")
    if not tech_ok: missing.append("技術蓄勢")
    if not sector_ok: missing.append("族群輪動")
    if not broker_ok: missing.append("券商分點(選配)")
    if not catalyst_ok: missing.append("催化/營收/新聞(選配)")

    weighted = [
        (capital, 19.0), (inst, 13.0), (holder, 10.0), (tech, 23.0),
        (sector, 17.0), (entry, 8.0),
    ]
    if catalyst_ok:
        weighted.append((catalyst, 10.0))
    if broker_ok:
        weighted.append((broker, 6.0))
    total_w = sum(w for _, w in weighted) or 100.0
    raw_score = sum(v * w for v, w in weighted) / total_w
    penalty = max(0.0, overheat - 35.0) * 0.48 + max(0.0, started - 58.0) * 0.22

    h104_kind = _first_text(raw, ["H104推薦性質"])
    if "近期首次觀察" in h104_kind:
        raw_score += 2.0
    elif "排序指標改善" in h104_kind or "黑馬分顯著改善" in h104_kind:
        raw_score += 1.0

    wave_bonus, sector_role = _sector_wave_adjustment(sector_detail, started, overheat, tech)
    score = _clip(raw_score - penalty + wave_bonus)
    sector_confirm = float(_num(sector_detail.get("confirm"), 50.0) or 50.0)
    sector_lifecycle = _text(sector_detail.get("lifecycle"))
    fresh_score, repeat_penalty, repeat_days, sector_days, fresh_state, sector_relief = _repeat_governor(
        raw, score, sector_lifecycle=sector_lifecycle, sector_confirm=sector_confirm, started=started
    )
    # H108 priority is still research-only. It lets a confirmed sector ignition rescue
    # a not-yet-started second-wave member without forgiving repeated *same-stock* exposure.
    confirm_tail = max(0.0, sector_confirm - 72.0) * 0.10 if any(x in sector_lifecycle for x in ["IGNITION", "EXPANSION"]) else 0.0
    role_tail = 2.0 if "SECOND-WAVE" in sector_role else 1.0 if "EARLY-FOLLOWER" in sector_role else 0.0
    h108_priority = round(_clip(fresh_score + min(3.0, confirm_tail) + role_tail), 2)

    already_strong = bool(started >= 72 or overheat >= 72)
    if already_strong:
        nature = "今日已發動/偏強｜不列未來黑馬主榜"
        exclude = "是"
    elif h108_priority >= 78 and core_complete >= 75 and tech >= 66 and capital >= 60:
        nature = "未來黑馬｜高優先預發動"
        exclude = "否"
    elif h108_priority >= 69:
        nature = "未來黑馬｜蓄勢觀察"
        exclude = "否"
    else:
        nature = "早期雷達｜證據仍不足"
        exclude = "否"

    if already_strong:
        stage = "IG2｜ALREADY-IGNITED｜已發動，移出純黑馬預發動主榜"
        level = "X1｜TODAY-STRONG"
        window = "已發動｜改看回測/再進場"
        action = "不要因今日強勢追價；移至已發動觀察，等待拉回或新結構。"
    elif h108_priority >= 82 and overheat < 45 and tech >= 70 and capital >= 64:
        stage = "BH3｜PRE-IGNITION-PRIME｜高品質預發動"
        level = "S0｜BLACKHORSE-PRIME"
        window = "1～3個交易日"
        action = "列最高優先黑馬雷達；次一交易日前重驗法人、量價、族群與失效條件。"
    elif h108_priority >= 74 and overheat < 58:
        stage = "BH2｜PRESSURE-BUILDING｜蓄勢接近發動"
        level = "S1｜BLACKHORSE-SETUP"
        window = "1～5個交易日"
        action = "列重點預發動觀察；未確認前不因分數直接買進。"
    elif h108_priority >= 65:
        stage = "BH1｜EARLY-BUILD｜早期資金/結構異常"
        level = "S2｜EARLY-BLACKHORSE"
        window = "2～5個交易日"
        action = "保留黑馬雷達；等待法人/量價/族群至少一項再確認。"
    else:
        stage = "BH0｜WATCH｜尚未形成足夠未來發動共振"
        level = "W｜RESEARCH"
        window = "未定"
        action = "一般研究；不因今日強勢或單一因子提高買進順位。"

    reasons = []
    reasons.extend(inst_notes[:2]); reasons.extend(holder_notes[:1]); reasons.extend(tech_notes[:2]); reasons.extend(sector_notes[:2])
    if wave_bonus > 0:
        reasons.append(f"H108族群波段加分+{wave_bonus:.1f}")
    elif wave_bonus < 0:
        reasons.append(f"H108族群成熟/退潮扣分{wave_bonus:.1f}")
    if sector_relief > 0:
        reasons.append(f"確認族群點火，減免族群重複懲罰{sector_relief:.1f}")
    if broker_ok: reasons.extend(broker_notes[:1])
    if catalyst_ok: reasons.extend(catalyst_notes[:1])
    if repeat_penalty > 0:
        reasons.append(f"重複推薦懲罰-{repeat_penalty:.1f}")
    if heat_notes:
        reasons.append("過熱警示:" + "/".join(heat_notes[:3]))
    if not reasons:
        reasons.append("多因子尚未形成明確前置共振")

    return {
        "H105版本": VERSION,
        "H105黑馬預發動分": round(score, 2),
        "H105黑馬同層順位": None,
        "H105未來發動窗口": window,
        "H105資金潛伏分": round(capital, 2),
        "H105法人轉折分": round(inst, 2),
        "H105籌碼收斂分": round(holder, 2),
        "H105技術蓄勢分": round(tech, 2),
        "H105下一波族群分": round(sector, 2),
        "H105券商分點分": round(broker, 2) if broker_ok else None,
        "H105催化未反映分": round(catalyst, 2) if catalyst_ok else None,
        "H105進場成熟分": round(entry, 2),
        "H105過熱追高風險分": round(overheat, 2),
        "H105今日已發動程度": round(started, 2),
        "H105證據完整度%": completeness,
        "H105核心證據完整度%": core_complete,
        "H105選配證據完整度%": optional_complete,
        "H107新鮮機會分": fresh_score,
        "H107近期入選日數": repeat_days,
        "H107近期族群入選日數": sector_days,
        "H107重複推薦懲罰": repeat_penalty,
        "H107新發現狀態": fresh_state,
        "H107研究池調整": "",
        "H108族群確認分": round(sector_confirm, 2),
        "H108族群生命週期": sector_lifecycle,
        "H108個股族群角色": sector_role,
        "H108族群波段加分": round(wave_bonus, 2),
        "H108族群懲罰減免": round(sector_relief, 2),
        "H108研究優先分": h108_priority,
        "H108族群前次資料日": _text(sector_detail.get("prev_day")),
        "H108族群前次排名": sector_detail.get("prev_rank"),
        "H108族群排名改善": sector_detail.get("rank_improve"),
        "H108族群資金流變化": sector_detail.get("flow_delta"),
        "H108族群廣度變化": sector_detail.get("breadth_delta"),
        "H105發動階段": stage,
        "H105黑馬層級": level,
        "H105推薦性質": nature,
        "H105今日強勢排除": exclude,
        "H105資料缺口": "、".join(missing) if missing else "無主要缺口",
        "H105主要理由": "；".join(reasons),
        "H105建議動作": action,
        "H105Formal權限": "LOCKED｜H105/H107/H108只做未來黑馬研究排序，不建立買進授權。",
    }


def _infer_market_day(candidate_df: pd.DataFrame | None, tables: dict[str, pd.DataFrame] | None = None) -> str:
    keys = ["H106資料日", "H105資料日", "H99市場資料日", "市場資料日期", "最新K線日期", "推薦日期", "資料日", "market_date"]
    frames = []
    if isinstance(candidate_df, pd.DataFrame) and not candidate_df.empty:
        frames.append(candidate_df)
    for name in ("research", "waiting", "actionable", "audit"):
        df = (tables or {}).get(name) if isinstance(tables, dict) else None
        if isinstance(df, pd.DataFrame) and not df.empty:
            frames.append(df)
    for df in frames:
        for row in df.head(5).to_dict("records"):
            for key in keys:
                s = _text(row.get(key))[:10]
                if len(s) == 10 and s[4:5] == "-" and s[7:8] == "-":
                    return s
    return ""


def _sector_lookup(
    sector_df: pd.DataFrame | None,
    *,
    sector_history: list[dict[str, Any]] | None = None,
    market_day: str = "",
) -> dict[str, dict[str, Any]]:
    if not isinstance(sector_df, pd.DataFrame) or sector_df.empty or "類別" not in sector_df.columns:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for row in sector_df.to_dict("records"):
        cat = _text(row.get("類別"))
        if not cat:
            continue
        enriched = dict(row)
        if callable(sector_continuity):
            try:
                enriched.update(sector_continuity(enriched, list(sector_history or []), market_day))
            except Exception:
                pass
        result[cat] = enriched
    return result


def _source_lookup(candidate_df: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if not isinstance(candidate_df, pd.DataFrame) or candidate_df.empty or "股票代號" not in candidate_df.columns:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for row in candidate_df.to_dict("records"):
        code = _code(row.get("股票代號"))
        if code and code not in result:
            result[code] = row
    return result


def _enrich_frame(frame: pd.DataFrame | None, *, pool: str, source_map: dict[str, dict[str, Any]], sector_map: dict[str, dict[str, Any]], official_map: dict[str, dict[str, Any]] | None = None, announcements_map: dict[str, dict[str, Any]] | None = None, market_day: str = "") -> pd.DataFrame:
    out = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame(frame or [])
    for c in H105_COLUMNS:
        if c not in out.columns:
            out[c] = None
    if out.empty or "股票代號" not in out.columns:
        return out
    records = []
    for row in out.to_dict("records"):
        code = _code(row.get("股票代號"))
        merged = dict(source_map.get(code, {}))
        merged.update(row)
        merged["股票代號"] = code
        merged = h109_join_official(merged, (official_map or {}).get(code))
        if (announcements_map or {}).get(code):
            merged.update((announcements_map or {})[code])
        sector_row = sector_map.get(_text(merged.get("類別")), {})
        merged.update(analyze_candidate(merged, sector_row=sector_row, pool=pool))
        merged.update(h109_govern(merged, asof=h109_parse_day(market_day)))
        records.append(merged)
    out = pd.DataFrame(records)
    score = pd.to_numeric(out.get("H109最終研究優先分"), errors="coerce")
    fallback = pd.to_numeric(out["H105黑馬預發動分"], errors="coerce")
    score = score.where(score.notna(), fallback)
    order = out.assign(__h108=score).sort_values(["__h108", "股票代號"], ascending=[False, True], na_position="last", kind="stable").index.tolist()
    rank_map = {idx: rank for rank, idx in enumerate(order, 1)}
    out["H105黑馬同層順位"] = [rank_map.get(i) for i in out.index]
    # Preserve the existing H79/H101/H102 row order. H105 owns only the dedicated
    # blackhorse_overview ranking; it must not silently rewrite Formal/Research
    # execution order used by legacy UI, persistence, or downstream governance.
    out = out.reset_index(drop=True)
    front = [c for c in [
        "H105黑馬同層順位", "H105黑馬預發動分", "H105未來發動窗口", "H105黑馬層級",
        "H105發動階段", "H105推薦性質", "H105今日強勢排除", "H105過熱追高風險分",
        "H105技術蓄勢分", "H105資金潛伏分", "H105法人轉折分", "H105籌碼收斂分",
        "H105下一波族群分", "H105券商分點分", "H105催化未反映分", "H105進場成熟分", "H105證據完整度%",
        "H105核心證據完整度%", "H105選配證據完整度%", "H107新鮮機會分", "H107近期入選日數",
        "H107近期族群入選日數", "H107重複推薦懲罰", "H107新發現狀態", "H107研究池調整",
        "H109最終研究優先分", "H109決策權威", "H109否決原因", "H109催化方向", "H109官方營收新鮮度", "H109時機窗口",
        "H108研究優先分", "H108族群確認分", "H108族群生命週期", "H108個股族群角色",
        "H108族群波段加分", "H108族群懲罰減免", "H108族群前次資料日", "H108族群前次排名",
        "H108族群排名改善", "H108族群資金流變化", "H108族群廣度變化",
        "股票代號", "股票名稱", "市場別", "類別", "H105主要理由", "H105建議動作",
    ] if c in out.columns]
    return out.loc[:, front + [c for c in out.columns if c not in front]].copy()


def _diversified_pick(frame: pd.DataFrame, limit: int) -> pd.DataFrame:
    """H108 dynamic diversity governor.

    Default still protects breadth (Top5 max1/sector, Top10 max2/sector), but a
    confirmed IGNITION/EXPANSION sector may contribute a second early member.
    Mature/fading sectors stay tightly capped.
    """
    if not isinstance(frame, pd.DataFrame) or frame.empty or limit <= 0:
        return frame.head(0).copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    work = frame.copy().reset_index(drop=True)
    selected: list[int] = []
    used_codes: set[str] = set()
    sector_counts: dict[str, int] = {}

    def cap_for(r: pd.Series, phase: int) -> int:
        life = _text(r.get("H108族群生命週期"))
        confirm = float(_num(r.get("H108族群確認分"), 50.0) or 50.0)
        if any(x in life for x in ["MATURE", "FADE"]):
            return 1 if phase <= 10 else 2
        if "IGNITION" in life and confirm >= 78:
            return 2 if phase <= 5 else 3 if phase <= 10 else 4
        if "EXPANSION" in life and confirm >= 72:
            return 2 if phase <= 5 else 2 if phase <= 10 else 3
        return 1 if phase <= 5 else 2 if phase <= 10 else 3

    def pass_dynamic(target: int, phase: int) -> None:
        for i, r in work.iterrows():
            if len(selected) >= target:
                return
            code = _code(r.get("股票代號"))
            cat = _text(r.get("類別")) or "__UNKNOWN__"
            cap = cap_for(r, phase)
            if not code or code in used_codes or sector_counts.get(cat, 0) >= cap:
                continue
            selected.append(i)
            used_codes.add(code)
            sector_counts[cat] = sector_counts.get(cat, 0) + 1

    pass_dynamic(min(limit, 5), 5)
    pass_dynamic(min(limit, 10), 10)
    pass_dynamic(limit, max(11, limit))
    if len(selected) < limit:
        for i, r in work.iterrows():
            if len(selected) >= limit:
                break
            code = _code(r.get("股票代號"))
            if code and code not in used_codes:
                selected.append(i); used_codes.add(code)
    return work.loc[selected].copy().reset_index(drop=True)


def _rebalance_future_research(tables: dict[str, pd.DataFrame]) -> None:
    research = tables.get("research", pd.DataFrame()).copy()
    waiting = tables.get("waiting", pd.DataFrame()).copy()
    if research.empty and waiting.empty:
        return
    target = max(0, len(research))
    if target == 0:
        return
    for df in (research, waiting):
        if "股票代號" in df.columns:
            df["股票代號"] = df["股票代號"].map(_code)
    research["__origin"] = "research"
    waiting["__origin"] = "waiting"
    pieces = [df for df in (research, waiting) if isinstance(df, pd.DataFrame) and not df.empty]
    pool = pd.concat(pieces, ignore_index=True, sort=False) if pieces else pd.DataFrame()
    if "股票代號" not in pool.columns:
        return
    pool = pool[pool["股票代號"].ne("")].drop_duplicates("股票代號", keep="first")
    started = pool.get("H105今日強勢排除", pd.Series("", index=pool.index)).fillna("").astype(str).eq("是")
    authority = pool.get("H109決策權威", pd.Series("RESEARCH_ELIGIBLE", index=pool.index)).fillna("").astype(str)
    vetoed = authority.isin({"SHOCK_QUARANTINE", "COOLDOWN_QUARANTINE", "FADE_VETO", "FAST_SHOCK_RECHECK", "FUTURE_DATA_VETO"})
    eligible = pool.loc[~started & ~vetoed].copy()
    eligible["__fresh"] = pd.to_numeric(eligible.get("H107新鮮機會分"), errors="coerce")
    eligible["__h105"] = pd.to_numeric(eligible.get("H105黑馬預發動分"), errors="coerce")
    eligible["__h108"] = pd.to_numeric(eligible.get("H109最終研究優先分"), errors="coerce")
    eligible["__h108"] = eligible["__h108"].where(eligible["__h108"].notna(), eligible["__fresh"])
    eligible["__confirm"] = pd.to_numeric(eligible.get("H108族群確認分"), errors="coerce")
    eligible["__heat"] = pd.to_numeric(eligible.get("H105過熱追高風險分"), errors="coerce")
    eligible["__tech"] = pd.to_numeric(eligible.get("H105技術蓄勢分"), errors="coerce")
    eligible["__core"] = pd.to_numeric(eligible.get("H105核心證據完整度%"), errors="coerce")
    eligible["__repeat"] = pd.to_numeric(eligible.get("H107近期入選日數"), errors="coerce").fillna(0)
    eligible = eligible.sort_values(
        ["__h108", "__fresh", "__confirm", "__h105", "__repeat", "__heat", "股票代號"],
        ascending=[False, False, False, False, True, True, True], na_position="last", kind="stable"
    )

    # Reserve a small number of "sector anchors" when the sector ignition itself is
    # independently confirmed. This fixes H107's over-diversification: a real wave
    # such as passive components may not disappear from Research solely because it
    # was also seen on prior days. The individual stock must still be not-yet-fired.
    life = eligible.get("H108族群生命週期", pd.Series("", index=eligible.index)).fillna("").astype(str)
    anchor_mask = (
        life.str.contains("IGNITION|EXPANSION", regex=True) &
        eligible["__confirm"].ge(76) & eligible["__heat"].lt(65) &
        eligible["__tech"].ge(55) & eligible["__core"].ge(75) & eligible["__h108"].ge(58)
    )
    anchor_pool = eligible.loc[anchor_mask].sort_values(
        ["__confirm", "__h108", "__tech", "股票代號"],
        ascending=[False, False, False, True], kind="stable", na_position="last"
    )
    if "類別" in anchor_pool.columns:
        anchor_pool = anchor_pool.drop_duplicates("類別", keep="first")
    reserve_n = min(target, min(2, max(1, target // 4))) if not anchor_pool.empty else 0
    anchors = anchor_pool.head(reserve_n).copy() if reserve_n else eligible.head(0).copy()
    anchor_codes = set(anchors["股票代號"].map(_code)) if not anchors.empty else set()

    remaining = eligible.loc[~eligible["股票代號"].isin(anchor_codes)].copy()
    fill_need = max(0, target - len(anchors))
    fill_pool = remaining
    # With a very small Research pool (e.g. 2 slots), one confirmed-sector anchor
    # is enough; the other slot must preserve cross-sector discovery.
    anchor_cats = set(anchors.get("類別", pd.Series(dtype=str)).fillna("").astype(str)) if not anchors.empty else set()
    if fill_need and target <= 3 and anchor_cats and "類別" in remaining.columns:
        primary = remaining.loc[~remaining["類別"].fillna("").astype(str).isin(anchor_cats)].copy()
        fill = _diversified_pick(primary, fill_need)
        if len(fill) < fill_need:
            used = set(fill.get("股票代號", pd.Series(dtype=str)).astype(str))
            fallback = remaining.loc[~remaining["股票代號"].astype(str).isin(used)].copy()
            extra = _diversified_pick(fallback, fill_need - len(fill))
            fill = pd.concat([fill, extra], ignore_index=True, sort=False)
    else:
        fill = _diversified_pick(fill_pool, fill_need)
    picked = pd.concat([anchors, fill], ignore_index=True, sort=False) if not anchors.empty else fill
    if not picked.empty:
        picked = picked.drop_duplicates("股票代號", keep="first").sort_values(
            ["__h108", "__fresh", "__confirm", "__repeat", "__heat", "股票代號"],
            ascending=[False, False, False, True, True, True], kind="stable", na_position="last"
        ).head(target).reset_index(drop=True)
    picked_codes = set(picked["股票代號"].map(_code)) if not picked.empty else set()

    if not picked.empty:
        def adjust_label(r: pd.Series) -> str:
            code = _code(r.get("股票代號"))
            if code in anchor_codes:
                return "H108保留/升級｜確認族群點火×未發動代表股"
            if r.get("__origin") == "research":
                return "保留/升級｜未來黑馬×新鮮度×動態族群分散"
            return "WAITING→RESEARCH｜補入未來黑馬"
        picked["H107研究池調整"] = picked.apply(adjust_label, axis=1)

    rest = pool.loc[~pool["股票代號"].isin(picked_codes)].copy()
    if not rest.empty:
        def reason(r: pd.Series) -> str:
            auth = _text(r.get("H109決策權威"))
            if auth in {"SHOCK_QUARANTINE", "COOLDOWN_QUARANTINE", "FADE_VETO", "FAST_SHOCK_RECHECK", "FUTURE_DATA_VETO"}:
                return "H109 RESEARCH→WAITING｜" + auth + "｜" + _text(r.get("H109否決原因"))
            if _text(r.get("H105今日強勢排除")) == "是":
                return "RESEARCH→WAITING｜今日已發動/過熱，禁止佔用未來黑馬研究位"
            life_s = _text(r.get("H108族群生命週期"))
            conf = float(_num(r.get("H108族群確認分"), 0.0) or 0.0)
            if any(x in life_s for x in ["IGNITION", "EXPANSION"]) and conf >= 76:
                return "WAITING｜族群已確認點火，但本輪研究名額由更高H108優先分個股取得"
            if int(_first_num(r, ["H107近期入選日數"], 0.0) or 0) >= 2:
                return "RESEARCH→WAITING｜近期重複曝光，等待新證據改善"
            return "WAITING｜未進本輪H108動態分散Top研究池"
        rest["H107研究池調整"] = rest.apply(reason, axis=1)

    drop_helpers = ["__origin", "__fresh", "__h105", "__h108", "__confirm", "__heat", "__tech", "__core", "__repeat"]
    tables["research"] = picked.drop(columns=[c for c in drop_helpers if c in picked.columns], errors="ignore").reset_index(drop=True)
    tables["waiting"] = rest.drop(columns=[c for c in drop_helpers if c in rest.columns], errors="ignore").reset_index(drop=True)


def _build_blackhorse_overview(tables: dict[str, pd.DataFrame], max_rows: int = 30) -> pd.DataFrame:
    frames = []
    for pool_order, name in enumerate(("research", "waiting", "actionable")):
        df = tables.get(name, pd.DataFrame())
        if not isinstance(df, pd.DataFrame) or df.empty or "股票代號" not in df.columns:
            continue
        work = df.copy(); work["__pool"] = pool_order; frames.append(work)
    if not frames:
        return pd.DataFrame(columns=BLACKHORSE_OVERVIEW_COLUMNS)
    all_rows = pd.concat(frames, ignore_index=True, sort=False)
    all_rows["股票代號"] = all_rows["股票代號"].map(_code)
    all_rows = all_rows[all_rows["股票代號"].ne("")].drop_duplicates("股票代號", keep="first")
    score = pd.to_numeric(all_rows.get("H105黑馬預發動分"), errors="coerce")
    fresh = pd.to_numeric(all_rows.get("H107新鮮機會分"), errors="coerce")
    priority = pd.to_numeric(all_rows.get("H109最終研究優先分"), errors="coerce")
    priority = priority.where(priority.notna(), fresh.where(fresh.notna(), score))
    heat = pd.to_numeric(all_rows.get("H105過熱追高風險分"), errors="coerce")
    core_complete = pd.to_numeric(all_rows.get("H105核心證據完整度%"), errors="coerce")
    exclude = all_rows.get("H105今日強勢排除", pd.Series("", index=all_rows.index)).fillna("").astype(str).eq("是")
    # Research-grade blackhorse requires core evidence; broker/news remain optional.
    authority = all_rows.get("H109決策權威", pd.Series("", index=all_rows.index)).fillna("").astype(str)
    keep = priority.ge(60) & heat.lt(72) & core_complete.ge(50) & ~exclude & authority.eq("RESEARCH_ELIGIBLE")
    out = all_rows.loc[keep].copy()
    out["__fresh"] = pd.to_numeric(out.get("H107新鮮機會分"), errors="coerce")
    out["__score"] = pd.to_numeric(out.get("H105黑馬預發動分"), errors="coerce")
    out["__h108"] = pd.to_numeric(out.get("H109最終研究優先分"), errors="coerce")
    out["__h108"] = out["__h108"].where(out["__h108"].notna(), out["__fresh"].where(out["__fresh"].notna(), out["__score"]))
    out["__confirm"] = pd.to_numeric(out.get("H108族群確認分"), errors="coerce")
    out["__heat"] = pd.to_numeric(out.get("H105過熱追高風險分"), errors="coerce")
    out["__repeat"] = pd.to_numeric(out.get("H107近期入選日數"), errors="coerce").fillna(0)
    out = out.sort_values(["__h108", "__fresh", "__confirm", "__score", "__repeat", "__heat", "__pool", "股票代號"], ascending=[False, False, False, False, True, True, True, True], na_position="last", kind="stable")
    out = _diversified_pick(out, max(1, int(max_rows)))
    out["H105黑馬順位"] = range(1, len(out) + 1)
    for c in BLACKHORSE_OVERVIEW_COLUMNS:
        if c not in out.columns:
            out[c] = None
    return out.loc[:, BLACKHORSE_OVERVIEW_COLUMNS].copy()


def decorate_decision_tables(
    tables: dict[str, Any] | None,
    *,
    candidate_df: pd.DataFrame | None = None,
    sector_df: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    src = tables if isinstance(tables, dict) else {}
    out: dict[str, pd.DataFrame] = {
        k: (v.copy() if isinstance(v, pd.DataFrame) else pd.DataFrame(v) if v is not None else pd.DataFrame())
        for k, v in src.items()
    }
    base_dir = Path(__file__).resolve().parent
    market_day = _infer_market_day(candidate_df, out)
    sector_history = []
    if callable(load_sector_history):
        try:
            sector_history = load_sector_history(base_dir)
        except Exception:
            sector_history = []
    source_map = _source_lookup(candidate_df)
    sector_map = _sector_lookup(sector_df, sector_history=sector_history, market_day=market_day)
    official_map = h109_official_snapshot(base_dir, h109_parse_day(market_day))
    announcements_map = h109_official_announcements(base_dir, h109_parse_day(market_day))
    for name in ("actionable", "research", "waiting", "audit", "emerging_watch"):
        out[name] = _enrich_frame(out.get(name), pool=name, source_map=source_map,
                                  sector_map=sector_map, official_map=official_map,
                                  announcements_map=announcements_map, market_day=market_day)

    # H107 fixes the old routing leak: an H105 "already ignited / do not chase" row
    # may not continue occupying one of the limited future-research slots.
    risk_snapshot = h109_apply_quarantine_continuity(out, base_dir, h109_parse_day(market_day))
    _rebalance_future_research(out)

    # Keep Formal authority unchanged. Only research discovery + blackhorse ranking
    # are rebalanced for repeat control and sector diversity.
    out["blackhorse_overview"] = _build_blackhorse_overview(out)

    health = out.get("health", pd.DataFrame()).copy()
    if not health.empty and "項目" in health.columns:
        item_s = health["項目"].fillna("").astype(str)
        health = health.loc[~item_s.str.startswith(("H105", "H107", "H108", "H109"))].copy()
    black = out.get("blackhorse_overview", pd.DataFrame())
    already = 0
    for name in ("research", "waiting", "actionable"):
        df = out.get(name, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and "H105今日強勢排除" in df.columns:
            already += int(df["H105今日強勢排除"].fillna("").astype(str).eq("是").sum())
    confirmed_sectors = []
    for cat, sr in sector_map.items():
        d = _sector_lifecycle_detail({}, sr)
        if any(x in _text(d.get("lifecycle")) for x in ["IGNITION", "EXPANSION"]):
            confirmed_sectors.append((cat, float(_num(d.get("confirm"), 0.0) or 0.0), _text(d.get("lifecycle"))))
    confirmed_sectors.sort(key=lambda x: (-x[1], x[0]))
    confirmed_text = "、".join(f"{c}({sc:.0f})" for c, sc, _ in confirmed_sectors[:6]) or "無"
    rows = pd.DataFrame([
        {"項目": "H105版本", "數值": VERSION},
        {"項目": "H105核心目標", "數值": "未來1～5交易日預發動 > 今日已強；今日明顯強勢/過熱會降權並移出黑馬主榜"},
        {"項目": "H105黑馬主榜列數", "數值": int(len(black)) if isinstance(black, pd.DataFrame) else 0},
        {"項目": "H105今日已發動排除列數", "數值": already},
        {"項目": "H105資料治理", "數值": "法人/TDCC/技術/族群為核心證據；券商/新聞為選配，不得因缺選配資料否決Research"},
        {"項目": "H107重複推薦治理", "數值": "同一股票仍嚴格懲罰；H108只在族群真正點火且個股未發動時減免『族群重複』部分"},
        {"項目": "H108族群生命週期", "數值": "PREHEAT→IGNITION→EXPANSION→MATURE→FADE；同時融合H102快速衝擊與04族群廣度/量能/資金"},
        {"項目": "H108確認點火/擴散族群", "數值": confirmed_text},
        {"項目": "H108第二梯隊治理", "數值": "族群已點火時，已噴出的個股仍排除；優先尋找未發動SECOND-WAVE/EARLY-FOLLOWER"},
        {"項目": "H108動態族群分散", "數值": "一般Top5每族群1檔；確認IGNITION/EXPANSION可最多2檔；MATURE/FADE維持嚴格上限"},
        {"項目": "H108研究池保留", "數值": "研究名額有限時，最多保留少量確認點火族群代表股，再以新鮮黑馬補足，避免過度分散漏掉真主流"},
        {"項目": "H109版本", "數值": H109_VERSION},
        {"項目": "H109急跌延續隔離", "數值": f"當日急跌{risk_snapshot.get('fresh_shock',0)}；延續冷卻{risk_snapshot.get('cooldown',0)}；歷史觀察{risk_snapshot.get('history',0)}"},
        {"項目": "H109跨層權威", "數值": "SHOCK_QUARANTINE/FADE_VETO/FastShock待確認不可由新鮮度補位覆寫；仍不產生買進授權"},
        {"項目": "H109營收來源", "數值": "TWSE OpenAPI (上市) 最新月營收；當日盤後3秒限時快取；回測/不同交易日不讀當前網路資料"},
        {"項目": "H109實際官方營收更新數", "數值": len(official_map)},
        {"項目": "H109實際上市重大訊息數", "數值": len(announcements_map)},
        {"項目": "H109營收資料治理", "數值": "Coverage與Freshness分開；10日前上月待公布不得直接認定過期；上櫃/未知資訊不得冒充已驗證"},
        {"項目": "H109否決統計", "數值": ", ".join(f"{k}:{sum(int((out.get(pool, pd.DataFrame()).get('H109決策權威', pd.Series(dtype=str)) == k).sum()) for pool in ('research','waiting'))}" for k in ('SHOCK_QUARANTINE','FADE_VETO','FAST_SHOCK_RECHECK'))},
        {"項目": "H105Formal權限", "數值": "LOCKED｜H105/H107/H108只改研究發現與主管排序，不放寬正式買進治理"},
    ])
    out["health"] = pd.concat([health, rows], ignore_index=True, sort=False)
    if callable(save_discovery_snapshot):
        try:
            snap = save_discovery_snapshot(base_dir, out)
            out["health"] = pd.concat([out["health"], pd.DataFrame([{
                "項目": "H107每日發現歷史保存",
                "數值": f"OK｜{snap.get('market_day','')}｜written={snap.get('written',0)}｜rows={snap.get('rows',0)}"
            }])], ignore_index=True, sort=False)
        except Exception as exc:
            out["health"] = pd.concat([out["health"], pd.DataFrame([{
                "項目": "H107每日發現歷史保存", "數值": f"WARN｜{type(exc).__name__}: {exc}"
            }])], ignore_index=True, sort=False)
    if callable(save_sector_snapshot):
        try:
            sec = save_sector_snapshot(base_dir, sector_df, market_day or _infer_market_day(candidate_df, out))
            out["health"] = pd.concat([out["health"], pd.DataFrame([{
                "項目": "H108每日族群歷史保存",
                "數值": f"OK｜{sec.get('market_day','')}｜written={sec.get('written',0)}｜rows={sec.get('rows',0)}"
            }])], ignore_index=True, sort=False)
        except Exception as exc:
            out["health"] = pd.concat([out["health"], pd.DataFrame([{
                "項目": "H108每日族群歷史保存", "數值": f"WARN｜{type(exc).__name__}: {exc}"
            }])], ignore_index=True, sort=False)
    return out


def export_contract_summary(tables: dict[str, pd.DataFrame] | None) -> dict[str, Any]:
    t = tables if isinstance(tables, dict) else {}
    overview = t.get("blackhorse_overview", pd.DataFrame())
    ok = isinstance(overview, pd.DataFrame)
    required = {"H105黑馬預發動分", "H105今日強勢排除", "H105過熱追高風險分"}
    missing = []
    if ok and not overview.empty:
        missing = sorted(required - set(overview.columns))
        ok = not missing
    return {
        "version": VERSION,
        "ok": bool(ok),
        "blackhorse_rows": int(len(overview)) if isinstance(overview, pd.DataFrame) else 0,
        "missing_columns": missing,
    }


__all__ = [
    "VERSION", "H105_COLUMNS", "BLACKHORSE_OVERVIEW_COLUMNS",
    "analyze_candidate", "decorate_decision_tables", "export_contract_summary",
]

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
import math
import pandas as pd

VERSION = "v191_h105_future_blackhorse_pre_ignition_20261004"

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
    "H104推薦性質", "H105推薦性質", "H105資料缺口", "H105主要理由", "H105建議動作",
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


def _sector_next_wave_score(row: dict[str, Any], sector_row: dict[str, Any] | None = None) -> tuple[float, list[str], bool]:
    sr = sector_row or {}
    def n(names: list[str]) -> float | None:
        return _first_num(sr, names, _first_num(row, names))
    accel = n(["類股加速度"])
    rotation = n(["族群輪動分", "H102族群衝擊分"])
    flow = n(["族群資金流分數", "主流資金分"])
    heat = n(["類股熱度分數"])
    rank = n(["類股熱度排名"])
    breadth = n(["同族群強勢比例", "H57族群點火廣度分"])
    state = _first_text(sr, ["族群輪動狀態", "強勢族群等級"], _first_text(row, ["族群輪動狀態", "H102族群動態狀態", "強勢族群等級"]))
    vals = [x for x in [accel,rotation,flow,heat,rank,breadth] if x is not None]
    if not vals and not state:
        return 50.0, ["族群輪動資料不足"], False
    notes: list[str] = []
    score = _clip(accel, default=50) * 0.25 + _clip(rotation, default=50) * 0.25 + _clip(flow, default=50) * 0.20 + _clip(breadth, default=50) * 0.12
    if rank is None:
        rank_s = 50.0
    elif 3 <= rank <= 12:
        rank_s = 96.0; notes.append("族群位於升溫區而非已霸榜第一")
    elif 13 <= rank <= 20:
        rank_s = 78.0
    elif rank in (1,2):
        rank_s = 64.0
    else:
        rank_s = 48.0
    score += rank_s * 0.10 + _clip(heat, default=50) * 0.08
    if any(k in state for k in ["升溫", "輪動", "轉強", "加速", "點火", "資金流入"]):
        score += 5.0; notes.append("族群狀態轉強")
    if any(k in state for k in ["退潮", "降溫", "弱勢", "高檔鈍化"]):
        score -= 8.0
    return _clip(score), notes, True


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


def analyze_candidate(row: dict[str, Any] | pd.Series, *, sector_row: dict[str, Any] | None = None, pool: str = "") -> dict[str, Any]:
    raw = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
    inst, inst_notes, inst_ok = _institution_turn_score(raw)
    holder, holder_notes, holder_ok = _holder_score(raw)
    tech, tech_notes, tech_ok = _technical_setup_score(raw)
    sector, sector_notes, sector_ok = _sector_next_wave_score(raw, sector_row)
    broker, broker_notes, broker_ok = _broker_flow_score(raw)
    catalyst, catalyst_notes, catalyst_ok = _catalyst_unpriced_score(raw)
    entry = _entry_maturity(raw)
    overheat, started, heat_notes = _overheat_and_started(raw)
    capital = _capital_stealth(raw, inst, holder)

    evidence_flags = [inst_ok, holder_ok, tech_ok, sector_ok, broker_ok, catalyst_ok]
    completeness = round(sum(bool(x) for x in evidence_flags) / len(evidence_flags) * 100.0, 2)
    missing = []
    if not inst_ok: missing.append("法人")
    if not holder_ok: missing.append("TDCC/大戶")
    if not tech_ok: missing.append("技術蓄勢")
    if not sector_ok: missing.append("族群輪動")
    if not broker_ok: missing.append("券商分點")
    if not catalyst_ok: missing.append("催化/營收/新聞")

    # Future score deliberately makes current-day strength a *negative* once the
    # move is visibly underway.  This is the central behavioral change in H105.
    weighted = [
        (capital, 19.0), (inst, 13.0), (holder, 10.0), (tech, 23.0),
        (sector, 17.0), (catalyst, 10.0), (entry, 8.0),
    ]
    # Broker branch data is optional/paid in many deployments. Only rebalance it
    # into the score when explicit broker/branch evidence actually exists.
    if broker_ok:
        weighted.append((broker, 6.0))
    total_w = sum(w for _, w in weighted) or 100.0
    raw_score = sum(v * w for v, w in weighted) / total_w
    penalty = max(0.0, overheat - 35.0) * 0.48 + max(0.0, started - 58.0) * 0.22

    h104_kind = _first_text(raw, ["H104推薦性質"])
    if "近期首次觀察" in h104_kind:
        raw_score += 2.0
    elif "排序指標改善" in h104_kind:
        raw_score += 1.0
    # Repeated names can remain if evidence improves, but they do not receive a
    # novelty boost merely for appearing again.

    score = _clip(raw_score - penalty)

    already_strong = bool(started >= 72 or overheat >= 72)
    if already_strong:
        nature = "今日已發動/偏強｜不列未來黑馬主榜"
        exclude = "是"
    elif score >= 78 and completeness >= 60 and tech >= 66 and capital >= 60:
        nature = "未來黑馬｜高優先預發動"
        exclude = "否"
    elif score >= 69:
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
    elif score >= 82 and overheat < 45 and tech >= 70 and capital >= 64:
        stage = "BH3｜PRE-IGNITION-PRIME｜高品質預發動"
        level = "S0｜BLACKHORSE-PRIME"
        window = "1～3個交易日"
        action = "列最高優先黑馬雷達；次一交易日前重驗法人、量價、族群與失效條件。"
    elif score >= 74 and overheat < 58:
        stage = "BH2｜PRESSURE-BUILDING｜蓄勢接近發動"
        level = "S1｜BLACKHORSE-SETUP"
        window = "1～5個交易日"
        action = "列重點預發動觀察；未確認前不因分數直接買進。"
    elif score >= 65:
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
    reasons.extend(inst_notes[:2]); reasons.extend(holder_notes[:1]); reasons.extend(tech_notes[:2]); reasons.extend(sector_notes[:2]); reasons.extend(broker_notes[:1]); reasons.extend(catalyst_notes[:1])
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
        "H105催化未反映分": round(catalyst, 2),
        "H105進場成熟分": round(entry, 2),
        "H105過熱追高風險分": round(overheat, 2),
        "H105今日已發動程度": round(started, 2),
        "H105證據完整度%": completeness,
        "H105發動階段": stage,
        "H105黑馬層級": level,
        "H105推薦性質": nature,
        "H105今日強勢排除": exclude,
        "H105資料缺口": "、".join(missing) if missing else "無主要缺口",
        "H105主要理由": "；".join(reasons),
        "H105建議動作": action,
        "H105Formal權限": "LOCKED｜H105只做未來黑馬研究排序，不建立買進授權。",
    }


def _sector_lookup(sector_df: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if not isinstance(sector_df, pd.DataFrame) or sector_df.empty or "類別" not in sector_df.columns:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for row in sector_df.to_dict("records"):
        cat = _text(row.get("類別"))
        if cat:
            result[cat] = row
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


def _enrich_frame(frame: pd.DataFrame | None, *, pool: str, source_map: dict[str, dict[str, Any]], sector_map: dict[str, dict[str, Any]]) -> pd.DataFrame:
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
        sector_row = sector_map.get(_text(merged.get("類別")), {})
        merged.update(analyze_candidate(merged, sector_row=sector_row, pool=pool))
        records.append(merged)
    out = pd.DataFrame(records)
    score = pd.to_numeric(out["H105黑馬預發動分"], errors="coerce")
    order = out.assign(__h105=score).sort_values(["__h105", "股票代號"], ascending=[False, True], na_position="last", kind="stable").index.tolist()
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
        "股票代號", "股票名稱", "市場別", "類別", "H105主要理由", "H105建議動作",
    ] if c in out.columns]
    return out.loc[:, front + [c for c in out.columns if c not in front]].copy()


def _build_blackhorse_overview(tables: dict[str, pd.DataFrame], max_rows: int = 30) -> pd.DataFrame:
    frames = []
    # Research + Waiting are the discovery universe.  Formal may be present for
    # reference, but it must not displace an earlier-stage blackhorse purely
    # because it already has execution authority.
    for pool_order, name in enumerate(("research", "waiting", "actionable")):
        df = tables.get(name, pd.DataFrame())
        if not isinstance(df, pd.DataFrame) or df.empty or "股票代號" not in df.columns:
            continue
        work = df.copy()
        work["__pool"] = pool_order
        frames.append(work)
    if not frames:
        return pd.DataFrame(columns=BLACKHORSE_OVERVIEW_COLUMNS)
    all_rows = pd.concat(frames, ignore_index=True, sort=False)
    all_rows["股票代號"] = all_rows["股票代號"].map(_code)
    all_rows = all_rows[all_rows["股票代號"].ne("")].drop_duplicates("股票代號", keep="first")
    score = pd.to_numeric(all_rows.get("H105黑馬預發動分"), errors="coerce")
    heat = pd.to_numeric(all_rows.get("H105過熱追高風險分"), errors="coerce")
    complete = pd.to_numeric(all_rows.get("H105證據完整度%"), errors="coerce")
    exclude = all_rows.get("H105今日強勢排除", pd.Series("", index=all_rows.index)).fillna("").astype(str).eq("是")
    # Main blackhorse sheet intentionally excludes today's already-fired names.
    keep = score.ge(60) & heat.lt(72) & complete.ge(40) & ~exclude
    out = all_rows.loc[keep].copy()
    if out.empty:
        # Fail transparent: show best research candidates but keep their stage and
        # exclusion evidence visible instead of returning a misleading blank.
        out = all_rows.loc[~exclude].copy()
    out["__score"] = pd.to_numeric(out.get("H105黑馬預發動分"), errors="coerce")
    out["__heat"] = pd.to_numeric(out.get("H105過熱追高風險分"), errors="coerce")
    out = out.sort_values(["__score", "__heat", "__pool", "股票代號"], ascending=[False, True, True, True], na_position="last", kind="stable").head(max(1, int(max_rows))).reset_index(drop=True)
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
    source_map = _source_lookup(candidate_df)
    sector_map = _sector_lookup(sector_df)
    for name in ("actionable", "research", "waiting", "audit", "emerging_watch"):
        out[name] = _enrich_frame(out.get(name), pool=name, source_map=source_map, sector_map=sector_map)

    # Keep H102/H101 priority overview unchanged for backward compatibility;
    # H105 gets a dedicated manager-first table with a different objective.
    out["blackhorse_overview"] = _build_blackhorse_overview(out)

    health = out.get("health", pd.DataFrame()).copy()
    if not health.empty and "項目" in health.columns:
        health = health.loc[~health["項目"].fillna("").astype(str).str.startswith("H105")].copy()
    black = out.get("blackhorse_overview", pd.DataFrame())
    already = 0
    for name in ("research", "waiting", "actionable"):
        df = out.get(name, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and "H105今日強勢排除" in df.columns:
            already += int(df["H105今日強勢排除"].fillna("").astype(str).eq("是").sum())
    rows = pd.DataFrame([
        {"項目": "H105版本", "數值": VERSION},
        {"項目": "H105核心目標", "數值": "未來1～5交易日預發動 > 今日已強；今日明顯強勢/過熱會降權並移出黑馬主榜"},
        {"項目": "H105黑馬主榜列數", "數值": int(len(black)) if isinstance(black, pd.DataFrame) else 0},
        {"項目": "H105今日已發動排除列數", "數值": already},
        {"項目": "H105資料治理", "數值": "法人/TDCC/技術/族群/催化缺漏分開標示；Missing不當成正面證據"},
        {"項目": "H105Formal權限", "數值": "LOCKED｜只改研究發現與主管排序，不放寬正式買進治理"},
    ])
    out["health"] = pd.concat([health, rows], ignore_index=True, sort=False)
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

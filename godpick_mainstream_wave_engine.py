# -*- coding: utf-8 -*-
"""V191-H45 mainstream / main-rise wave engine.

Purpose
-------
The previous recommendation stack had rich sector and momentum features, but the
final ranking was dominated by trade-quality/RR.  A one-day rebound or a high
``主流資金分`` could therefore be labelled "strong" even when the stock was down
15~20% over the last month and its sector was in capital outflow.

H45 restores the intended order of evidence:
  1) sector money/trend must be current (1/5/20d, volume and breadth),
  2) the stock must lead its sector or show a real early-breakout structure,
  3) only then do Entry/Risk/RR decide whether the opportunity is executable.

No sector name is hard-coded.  The market decides which theme is mainstream each
run.  The engine only adds H45 columns; it does not rewrite Formal/A-/V188 labels.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import pandas as pd

VERSION = "v191_h45_mainstream_wave_engine_v2_20260820"

H45_COLUMNS = [
    "H45族群主流分", "H45族群短線動能%", "H45族群5日上漲比例%", "H45族群20日上漲比例%",
    "H45族群量能分", "H45族群領先比例%", "H45族群樣本數", "H45族群主流排名", "H45族群主流百分位%", "H45族群狀態",
    "H45個股領先分", "H45起漲結構分", "H45趨勢延續分", "H45量價啟動分",
    "H45主流波段分", "H45主流波段狀態", "H45主流波段理由", "H45主流交易綜合分",
    "H45版本",
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


def _first_num(row: pd.Series, names: Iterable[str], default: float = 0.0) -> float:
    for name in names:
        if name in row.index and _s(row.get(name)):
            x = _f(row.get(name), float("nan"))
            if math.isfinite(x):
                return float(x)
    return float(default)


def _first_text(row: pd.Series, names: Iterable[str], default: str = "") -> str:
    for name in names:
        if name in row.index:
            t = _s(row.get(name))
            if t:
                return t
    return default


def _clip(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(x)))


def _num_series(frame: pd.DataFrame, names: Iterable[str], default: float = 0.0) -> pd.Series:
    out = pd.Series([float("nan")] * len(frame), index=frame.index, dtype="float64")
    for name in names:
        if name not in frame.columns:
            continue
        cur = pd.to_numeric(frame[name], errors="coerce")
        out = out.where(out.notna(), cur)
    return out.fillna(float(default))


def _bool_text_series(frame: pd.DataFrame, names: Iterable[str], positives: Iterable[str]) -> pd.Series:
    text = pd.Series([""] * len(frame), index=frame.index, dtype="object")
    for name in names:
        if name in frame.columns:
            cur = frame[name].fillna("").astype(str)
            text = text.where(text.str.strip().ne(""), cur)
    keys = tuple(str(x) for x in positives)
    return text.map(lambda x: any(k in str(x) for k in keys))


def _sector_name_series(frame: pd.DataFrame) -> pd.Series:
    for name in ["類別", "族群名稱", "產業", "V134動態族群名稱"]:
        if name in frame.columns:
            s = frame[name].fillna("").astype(str).str.strip()
            if s.ne("").any():
                return s.where(s.ne(""), "未分類")
    return pd.Series(["未分類"] * len(frame), index=frame.index)


def _build_sector_table(frame: pd.DataFrame) -> pd.DataFrame:
    work = pd.DataFrame(index=frame.index)
    work["_sector"] = _sector_name_series(frame)
    ret1 = _num_series(frame, ["今日漲幅%", "當日漲幅%"], 0.0).clip(-12, 12)
    ret5 = _num_series(frame, ["近5日漲幅%", "5日績效%"], 0.0).clip(-25, 25)
    ret20 = _num_series(frame, ["近20日漲幅%", "20日績效%"], 0.0).clip(-40, 40)
    # H45 explicitly stops using arbitrary long-window ``區間漲跌幅%`` as a
    # current sector-momentum proxy.  5d dominates; 20d confirms the swing.
    work["_short_mom"] = ret1 * 0.20 + ret5 * 0.50 + ret20 * 0.30
    work["_ret5_up"] = (ret5 > 0).astype(float) * 100.0
    work["_ret20_up"] = (ret20 > 0).astype(float) * 100.0
    vr = _num_series(frame, ["當日量比", "均量比"], 1.0).clip(0, 3)
    work["_vol_score"] = (35.0 + vr * 30.0).clip(35, 100)
    work["_mainfund"] = _num_series(frame, ["主流資金分"], 50.0).clip(0, 100)
    attack = _num_series(frame, ["族群攻擊強度"], 0.0)
    rotation = _num_series(frame, ["族群輪動分"], 0.0)
    fundflow = _num_series(frame, ["族群資金流分數"], 0.0)
    work["_sector_existing"] = pd.concat([attack, rotation, fundflow], axis=1).max(axis=1).clip(0, 100)
    top3 = _bool_text_series(frame, ["類股前3強", "是否領先同類股"], ["是", "領先", "TRUE", "True"])
    rank = _num_series(frame, ["類股內排名"], 999.0)
    work["_leader"] = (top3 | rank.le(3)).astype(float) * 100.0

    grp = work.groupby("_sector", dropna=False).agg(
        H45族群短線動能=("_short_mom", "mean"),
        H45族群5日上漲比例=("_ret5_up", "mean"),
        H45族群20日上漲比例=("_ret20_up", "mean"),
        H45族群量能分=("_vol_score", "mean"),
        H45族群主流資金=("_mainfund", "mean"),
        H45族群既有攻擊=("_sector_existing", "mean"),
        H45族群領先比例=("_leader", "mean"),
        H45族群樣本數=("_leader", "size"),
    ).reset_index().rename(columns={"_sector": "_sector_name"})

    # Convert short-return momentum into a bounded 0~100 score.  +8% blended
    # momentum is already very strong; -8% is very weak.
    mom_score = (50.0 + grp["H45族群短線動能"] * 6.0).clip(0, 100)
    breadth = (grp["H45族群5日上漲比例"] * 0.60 + grp["H45族群20日上漲比例"] * 0.40).clip(0, 100)
    # Tiny sectors must not win solely on 1/1.  Blend toward neutral 50.
    confidence = (grp["H45族群樣本數"] / (grp["H45族群樣本數"] + 2.0)).clip(0.20, 1.0)
    raw = (
        grp["H45族群主流資金"] * 0.22
        + grp["H45族群既有攻擊"] * 0.18
        + mom_score * 0.22
        + grp["H45族群量能分"] * 0.14
        + breadth * 0.14
        + grp["H45族群領先比例"] * 0.10
    )
    grp["H45族群主流分"] = (50.0 + (raw - 50.0) * confidence).clip(0, 100).round(2)
    grp["H45族群主流排名"] = grp["H45族群主流分"].rank(method="min", ascending=False).astype(int)
    _sector_n = max(1, len(grp))
    grp["H45族群主流百分位%"] = (100.0 * (1.0 - (grp["H45族群主流排名"] - 1) / _sector_n)).round(1)
    grp["H45族群短線動能%"] = grp["H45族群短線動能"].round(2)
    grp["H45族群5日上漲比例%"] = grp["H45族群5日上漲比例"].round(1)
    grp["H45族群20日上漲比例%"] = grp["H45族群20日上漲比例"].round(1)
    grp["H45族群量能分"] = grp["H45族群量能分"].round(1)
    grp["H45族群領先比例%"] = grp["H45族群領先比例"].round(1)

    def status(r: pd.Series) -> str:
        score = _f(r.get("H45族群主流分"), 0)
        mom = _f(r.get("H45族群短線動能%"), 0)
        b5 = _f(r.get("H45族群5日上漲比例%"), 0)
        mainfund = _f(r.get("H45族群主流資金"), 0)
        b20 = _f(r.get("H45族群20日上漲比例%"), 0)
        vol = _f(r.get("H45族群量能分"), 0)
        pct = _f(r.get("H45族群主流百分位%"), 0)
        # H45-v2：主流是「相對排名」概念。不同市場日分數尺度會變，
        # 因此不能只靠固定60/66。Top10~25%再搭配近端動能/廣度/資金，
        # 才能動態辨識當天真正的主線，而不是硬編碼AI、PCB或航運。
        if pct >= 90 and score >= 58 and mom >= 2.0 and b5 >= 45 and vol >= 60:
            return "A｜主流加速"
        if pct >= 75 and score >= 54 and (mom >= 0.0 or b20 >= 60) and (mainfund >= 60 or b5 >= 55):
            return "B｜主流波段/輪動"
        if pct >= 60 and score >= 50 and mom >= -3.0:
            return "C｜次主流/修復"
        return "D｜退潮/非主流"
    grp["H45族群狀態"] = grp.apply(status, axis=1)
    return grp


def apply_mainstream_wave_engine(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame):
        return pd.DataFrame(frame)
    if frame.empty:
        out = frame.copy()
        for c in H45_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="object")
        return out

    out = frame.copy()
    # Idempotent refresh: cache repair / rerun may apply H45 more than once.
    # Drop prior H45 outputs before merge so pandas never creates *_x/*_y columns.
    out.drop(columns=[c for c in H45_COLUMNS if c in out.columns], inplace=True, errors="ignore")
    out["_H45_sector_key"] = _sector_name_series(out)
    sector = _build_sector_table(out)
    out = out.merge(sector[[
        "_sector_name", "H45族群主流分", "H45族群短線動能%", "H45族群5日上漲比例%",
        "H45族群20日上漲比例%", "H45族群量能分", "H45族群領先比例%", "H45族群樣本數",
        "H45族群主流排名", "H45族群主流百分位%", "H45族群狀態"
    ]], how="left", left_on="_H45_sector_key", right_on="_sector_name")
    out.drop(columns=["_sector_name"], inplace=True, errors="ignore")

    rows = []
    for _, row in out.iterrows():
        sector_score = _first_num(row, ["H45族群主流分"], 50.0)
        mainstream = _first_num(row, ["主流資金分"], 50.0)
        mainrise = _first_num(row, ["主流主升優先分"], 50.0)
        attack = _first_num(row, ["族群攻擊強度"], 0.0)
        rotation = _first_num(row, ["族群輪動分"], 0.0)
        top3_text = _first_text(row, ["類股前3強", "是否領先同類股"])
        rank = _first_num(row, ["類股內排名"], 999.0)
        lead_amp = _first_num(row, ["同類股領先幅度"], 0.0)
        leader = 90.0 if ("是" in top3_text or rank <= 3) else 72.0 if rank <= 5 else 58.0 if lead_amp > 0 else 45.0
        breakout = _first_num(row, ["型態突破分數", "突破準備分"], 45.0)
        prebreak = max(
            _first_num(row, ["強勢前兆分", "盤前強勢前兆分"], 45.0),
            _first_num(row, ["起漲前兆分數"], 45.0),
        )
        momentum = max(
            _first_num(row, ["強勢動能分", "盤後動能救援分"], 45.0),
            _first_num(row, ["爆發力分數", "爆發雷達分", "隔日爆發分"], 45.0),
        )
        comeback = _first_num(row, ["主流領漲回補分"], 45.0)
        dist20 = _first_num(row, ["突破20日高點%"], -15.0)
        # 「波段起漲」不是只看今天漲幾%。距20日高點越近，越可能處於
        # 整理末端/突破前；離高點太遠則不應因單日反彈被稱為起漲。
        breakout_proximity = _clip(100.0 + min(0.0, dist20) * 4.0)
        vr = _first_num(row, ["當日量比", "均量比"], 1.0)
        volume_score = _clip(35 + min(max(vr, 0), 3) * 30)
        ret1 = _first_num(row, ["今日漲幅%", "當日漲幅%"], 0.0)
        ret5 = _first_num(row, ["近5日漲幅%"], 0.0)
        ret20 = _first_num(row, ["近20日漲幅%"], 0.0)
        close = _first_num(row, ["當日收盤位置%"], 50.0)
        upper = _first_num(row, ["上影線比例%"], 20.0)
        trend = _clip(52 + ret5 * 2.0 + ret20 * 0.8 + max(-5.0, min(5.0, ret1)) * 1.5)
        # A 20d collapse cannot be called "strong continuation" merely because
        # today bounced.  It belongs to bargain/reclaim logic until the medium
        # swing damage is repaired.
        if ret20 < -12:
            trend = min(trend, 35.0)
        elif ret20 < -8:
            trend = min(trend, 48.0)
        if ret5 < -6:
            trend = min(trend, 42.0)
        close_quality = _clip(close - max(0.0, upper - 35.0) * 0.7)
        onset = _clip(
            prebreak * 0.25 + momentum * 0.20 + comeback * 0.15 + breakout_proximity * 0.15
            + volume_score * 0.10 + close_quality * 0.10 + breakout * 0.05
        )
        leadership = _clip(leader * 0.55 + mainstream * 0.25 + max(attack, rotation) * 0.20)
        score = _clip(
            sector_score * 0.24 + ((mainstream + mainrise) / 2.0) * 0.20 + leadership * 0.18
            + onset * 0.18 + trend * 0.10 + volume_score * 0.10
        )
        risk = _first_num(row, ["Risk風控安全分", "風控安全分"], 50.0)
        entry = _first_num(row, ["Entry進場買點分", "進場買點分"], 50.0)
        trade = _first_num(row, ["SuperAI Trade分", "實戰操作品質分", "可操作分"], 50.0)
        v188 = _first_num(row, ["V188股神作戰優先分", "股神推薦優先分"], 50.0)
        stop = _first_num(row, ["實戰停損距離%", "隔日有效風控距離%", "停損距離%"], 0.0)
        chase = _first_num(row, ["追價風險分", "追高風險分數_決策"], 55.0)
        amount = _first_num(row, ["流動性參考成交額百萬", "成交額百萬", "20日均成交額百萬"], 0.0)
        data_blob = "｜".join(_first_text(row, [c]) for c in ["K線資料新鮮度", "正式推薦排除原因", "操作許可"])
        data_block = any(k in data_blob for k in ["過期", "待更新", "正式排除", "禁止新倉", "DATA-WAIT"])

        sector_state = _first_text(row, ["H45族群狀態"])
        sector_rank = _first_num(row, ["H45族群主流排名"], 999.0)
        sector_pct = _first_num(row, ["H45族群主流百分位%"], 0.0)
        sector_ab = sector_state.startswith(("A｜", "B｜"))
        sector_ok = sector_ab or (sector_score >= 58 and mainstream >= 75 and max(attack, rotation) >= 65)
        prep_sector_ok = sector_ab or (sector_score >= 57 and mainstream >= 70 and max(attack, rotation) >= 60)
        leader_ok = leadership >= 62 or rank <= 3
        onset_ok = onset >= 62
        trend_ok = ret5 >= -3.0 and ret20 >= -8.0 and close >= 55 and upper <= 45
        volume_ok = vr >= 0.90 or onset >= 72
        safety_ok = risk >= 58 and (stop <= 0 or stop <= 7.0) and chase <= 62 and (amount <= 0 or amount >= 150)
        ready = bool(not data_block and sector_ok and leader_ok and onset_ok and trend_ok and volume_ok and safety_ok and score >= 67)

        # M-PREP 是「主流波段起漲前兆」，不是買進許可。它刻意允許整理日
        # 收盤不夠漂亮或量尚未放大，但要求：主流族群、領先股、前兆結構、
        # 中期趨勢未破壞。這讓台燿/景碩/健鼎這種主線整理股能進入第一眼
        # 研究名單，同時不會把20日-15~-20%的單日反彈誤叫強勢延續。
        prep = bool(
            not data_block and prep_sector_ok and leadership >= 62 and onset >= 60 and score >= 60
            and ret20 >= -5.0 and ret5 >= -7.5 and close >= 30 and upper <= 70
            and risk >= 50 and (stop <= 0 or stop <= 11.0) and chase <= 75
        )
        watch = bool(
            not data_block and (
                (score >= 60 and mainstream >= 70 and leadership >= 62 and onset >= 61
                 and ret20 >= -8 and ret5 >= -5 and close >= 45 and upper <= 55
                 and (sector_score >= 54 or max(attack, rotation) >= 55))
                or
                # 主流族群領先股若今天長上影/弱收盤，保留為WATCH而不是PREP。
                # 這能看到景碩這類主線股，但不把「沖高回落」包裝成起漲。
                (sector_ab and score >= 62 and leadership >= 65 and onset >= 60
                 and ret20 >= -8 and ret5 >= -7.5 and close >= 20 and upper <= 80)
            )
        )
        if ready:
            status = "M-READY｜主流波段起漲"
        elif prep:
            status = "M-PREP｜主流起漲前兆，等量價確認"
        elif watch:
            status = "M-WATCH｜個股強勢/主流觀察"
        else:
            status = "M-NO｜非主流波段優先"

        reasons = []
        if sector_state.startswith(("A｜", "B｜")):
            reasons.append(f"族群主流{sector_score:.1f}｜排名{int(sector_rank)}｜{sector_state.split('｜',1)[1]}")
        elif sector_score >= 62:
            reasons.append(f"族群主流{sector_score:.1f}")
        else:
            reasons.append(f"族群主流僅{sector_score:.1f}")
        if leader_ok:
            reasons.append(f"個股領先{leadership:.1f}")
        if onset_ok:
            reasons.append(f"起漲結構{onset:.1f}")
        if prebreak >= 70:
            reasons.append(f"前兆{prebreak:.1f}")
        if breakout_proximity >= 70:
            reasons.append(f"距20日高點近｜突破接近度{breakout_proximity:.0f}")
        if not trend_ok:
            reasons.append(f"波段趨勢未完整｜5日{ret5:+.1f}%/20日{ret20:+.1f}%")
        if vr < 0.9:
            reasons.append(f"量比{vr:.2f}不足")
        if close < 55 or upper > 45:
            reasons.append(f"收盤結構偏弱｜位置{close:.0f}%/上影{upper:.0f}%")
        if not safety_ok:
            reasons.append("交易風控尚未通過")
        if data_block:
            reasons.append("資料/權限封鎖")
        combined = _clip(score * 0.58 + v188 * 0.27 + trade * 0.10 + entry * 0.05)
        rows.append((round(leadership,2), round(onset,2), round(trend,2), round(volume_score,2), round(score,2), status, "；".join(reasons), round(combined,2)))

    out["H45個股領先分"] = [x[0] for x in rows]
    out["H45起漲結構分"] = [x[1] for x in rows]
    out["H45趨勢延續分"] = [x[2] for x in rows]
    out["H45量價啟動分"] = [x[3] for x in rows]
    out["H45主流波段分"] = [x[4] for x in rows]
    out["H45主流波段狀態"] = [x[5] for x in rows]
    out["H45主流波段理由"] = [x[6] for x in rows]
    out["H45主流交易綜合分"] = [x[7] for x in rows]
    out["H45版本"] = VERSION
    out.drop(columns=["_H45_sector_key"], inplace=True, errors="ignore")
    return out


def build_mainstream_wave_table(frame: pd.DataFrame, top_n: int = 8) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    work = frame if ("H45版本" in frame.columns and frame["H45版本"].astype(str).eq(VERSION).all()) else apply_mainstream_wave_engine(frame)
    status = work.get("H45主流波段狀態", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    pick = work.loc[status.str.startswith(("M-READY", "M-PREP", "M-WATCH"))].copy()
    if pick.empty:
        return pick
    _status = pick["H45主流波段狀態"].astype(str)
    pick["_status_priority"] = _status.map(lambda x: 3 if x.startswith("M-READY") else 2 if x.startswith("M-PREP") else 1)
    for c in ["H45主流交易綜合分", "H45主流波段分", "H45族群主流分", "H45族群主流百分位%", "V188股神作戰優先分"]:
        raw = pick[c] if c in pick.columns else pd.Series([0.0] * len(pick), index=pick.index)
        pick[c] = pd.to_numeric(raw, errors="coerce").fillna(0.0)
    pick.sort_values(["_status_priority", "H45族群主流分", "H45主流交易綜合分", "H45主流波段分"], ascending=[False, False, False, False], inplace=True, kind="mergesort")
    return pick.head(max(1, int(top_n))).drop(columns=["_status_priority"], errors="ignore").reset_index(drop=True)

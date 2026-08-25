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

VERSION = "v191_h50_1_mainstream_rotation_lifecycle_execution_rr_20260825"

H45_COLUMNS = [
    "H45族群主流分", "H45族群短線動能%", "H45族群5日上漲比例%", "H45族群20日上漲比例%",
    "H45族群量能分", "H45族群領先比例%", "H45族群樣本數", "H45族群主流排名", "H45族群主流百分位%", "H45族群狀態",
    "H45個股領先分", "H45起漲結構分", "H45趨勢延續分", "H45量價啟動分",
    "H45主流波段分", "H45主流波段狀態", "H45主流波段理由", "H45主流交易綜合分",
    "H45版本",
    "H47個股相對強度分", "H47族群內領先排名", "H47族群內領先百分位%",
    "H47波段階段", "H47主流領先狀態", "H47起漲優先分", "H47交易候選狀態",
    "H47主流領先理由", "H47版本",
    "H50族群可買主流分", "H50族群新鮮度分", "H50族群回檔再攻分",
    "H50族群起漲候選比例%", "H50族群延伸過熱比例%", "H50族群重複壓力分",
    "H50族群生命週期", "H50波段機會階段", "H50主流購買優先分", "H50主流購買狀態",
    "H50重複推薦扣分", "H50主流購買理由", "H50主流版本", "H50版本",
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


def _h50_zone(value: pd.Series, lo: float, hi: float, slope: float) -> pd.Series:
    below = (lo - value).clip(lower=0.0)
    above = (value - hi).clip(lower=0.0)
    gap = below + above
    return (100.0 - gap * float(slope)).clip(0.0, 100.0)


def _apply_h50_lifecycle(out: pd.DataFrame) -> pd.DataFrame:
    """Second-stage mainstream lifecycle model.

    H45 answers which sectors have *persisted strength*.  H50 adds a different
    question: which sectors are fresh/buyable *now*.  A mature high-volume theme
    with no EARLY/PULLBACK/LEADER setups remains recognized as mainstream, but it
    is no longer allowed to monopolize the buy-priority list.
    """
    if out is None or out.empty:
        return out
    work = pd.DataFrame(index=out.index)
    work["_sector"] = _sector_name_series(out)
    work["_ret1"] = _num_series(out, ["今日漲幅%", "當日漲幅%"], 0.0).clip(-12, 12)
    work["_ret5"] = _num_series(out, ["近5日漲幅%", "5日績效%"], 0.0).clip(-30, 30)
    work["_ret20"] = _num_series(out, ["近20日漲幅%", "20日績效%"], 0.0).clip(-50, 60)
    work["_mainfund"] = _num_series(out, ["主流資金分"], 50.0).clip(0, 100)
    work["_reclaim"] = _num_series(out, ["主流領漲回補分"], 50.0).clip(0, 100)
    work["_ai_miss"] = _num_series(out, ["AI漏選風險分"], 50.0).clip(0, 100)
    work["_strong_miss"] = _num_series(out, ["強勢股漏選風險分"], 50.0).clip(0, 100)
    work["_vr"] = _num_series(out, ["當日量比", "均量比"], 1.0).clip(0, 3)
    work["_h45"] = _num_series(out, ["H45族群主流分"], 50.0).clip(0, 100)
    stage = out.get("H47主流領先狀態", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)
    work["_early"] = stage.str.startswith(("L-EARLY", "L-PULLBACK", "L-LEADER")).astype(float) * 100.0
    work["_pullback"] = stage.str.startswith("L-PULLBACK").astype(float) * 100.0
    work["_extended"] = stage.str.startswith("L-EXTENDED").astype(float) * 100.0
    repeats = _num_series(out, ["近5次入榜次數"], 0.0).clip(0, 5)
    freshness = _num_series(out, ["今日訊號新鮮分"], 50.0).clip(0, 100)
    consecutive = _num_series(out, ["連續入榜次數"], 0.0).clip(0, 10)
    new_text = out.get("今日新進榜", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)
    work["_new"] = new_text.str.contains("是|TRUE|True|新", regex=True).astype(float) * 100.0
    work["_fresh"] = freshness
    work["_repeat_bad"] = (((repeats >= 3) & (freshness < 60)) | ((consecutive >= 3) & (freshness < 65))).astype(float) * 100.0

    grp = work.groupby("_sector", dropna=False).agg(
        _ret1=("_ret1", "mean"), _ret5=("_ret5", "mean"), _ret20=("_ret20", "mean"),
        _mainfund=("_mainfund", "mean"), _reclaim=("_reclaim", "mean"),
        _ai_miss=("_ai_miss", "mean"), _strong_miss=("_strong_miss", "mean"),
        _vr=("_vr", "mean"), _h45=("_h45", "mean"),
        _early=("_early", "mean"), _pullback=("_pullback", "mean"), _extended=("_extended", "mean"),
        _fresh=("_fresh", "mean"), _new=("_new", "mean"), _repeat_bad=("_repeat_bad", "mean"),
        _n=("_h45", "size"),
    ).reset_index()
    trend_fit = _h50_zone(grp["_ret20"], 6.0, 30.0, 3.0)
    pullback_fit = _h50_zone(grp["_ret5"], -12.0, 3.0, 5.0)
    vol_score = (35.0 + grp["_vr"] * 30.0).clip(35, 100)
    reclaim_signal = (
        grp["_mainfund"] * 0.30 + grp["_reclaim"] * 0.25 + grp["_ai_miss"] * 0.20
        + grp["_strong_miss"] * 0.15 + vol_score * 0.10
    ).clip(0, 100)
    reclaim_score = (reclaim_signal * 0.55 + trend_fit * 0.25 + pullback_fit * 0.20).clip(0, 100)
    fresh_score = (
        reclaim_score * 0.35 + grp["_early"] * 0.25 + grp["_fresh"] * 0.18
        + grp["_new"] * 0.08 + trend_fit * 0.14
    ).clip(0, 100)
    maturity_penalty = (grp["_extended"] * 0.10 + grp["_repeat_bad"] * 0.12).clip(0, 24)
    maturity_penalty += ((grp["_early"] < 5) & (grp["_h45"] >= 58) & (reclaim_score < 68)).astype(float) * 9.0
    maturity_penalty += (grp["_ret5"] > 10).astype(float) * 5.0
    buyable = (
        grp["_h45"] * 0.20 + reclaim_score * 0.27 + fresh_score * 0.23
        + grp["_early"] * 0.15 + trend_fit * 0.15 - maturity_penalty
    ).clip(0, 100)
    # Small sectors are useful for discovery but must not dominate on one stock.
    conf = (grp["_n"] / (grp["_n"] + 2.0)).clip(0.25, 1.0)
    grp["H50族群可買主流分"] = (50.0 + (buyable - 50.0) * conf).clip(0, 100).round(2)
    grp["H50族群新鮮度分"] = (50.0 + (fresh_score - 50.0) * conf).clip(0, 100).round(2)
    grp["H50族群回檔再攻分"] = (50.0 + (reclaim_score - 50.0) * conf).clip(0, 100).round(2)
    grp["H50族群起漲候選比例%"] = grp["_early"].round(1)
    grp["H50族群延伸過熱比例%"] = grp["_extended"].round(1)
    grp["H50族群重複壓力分"] = grp["_repeat_bad"].round(1)

    def _life(r: pd.Series) -> str:
        buy = _f(r.get("H50族群可買主流分"), 0)
        fresh = _f(r.get("H50族群新鮮度分"), 0)
        reclaim = _f(r.get("H50族群回檔再攻分"), 0)
        early = _f(r.get("H50族群起漲候選比例%"), 0)
        ext = _f(r.get("H50族群延伸過熱比例%"), 0)
        repeat = _f(r.get("H50族群重複壓力分"), 0)
        h45 = _f(r.get("_h45"), 0)
        r5 = _f(r.get("_ret5"), 0)
        r20 = _f(r.get("_ret20"), 0)
        if buy >= 66 and fresh >= 64 and early >= 8 and ext < 45:
            return "A0｜新主流點火"
        if buy >= 63 and early >= 12 and r5 >= -1 and ext < 45:
            return "A1｜主流加速"
        if buy >= 54 and reclaim >= 65 and r20 >= 5 and -13 <= r5 <= 4:
            return "B1｜主流回檔蓄勢"
        if buy >= 56 and h45 >= 57 and early >= 5 and repeat < 55:
            return "B2｜主流延續"
        if h45 >= 58 or repeat >= 45 or ext >= 40:
            return "C｜成熟/高檔輪動"
        return "D｜退潮/非主流"
    grp["H50族群生命週期"] = grp.apply(_life, axis=1)

    keep = ["_sector", "H50族群可買主流分", "H50族群新鮮度分", "H50族群回檔再攻分",
            "H50族群起漲候選比例%", "H50族群延伸過熱比例%", "H50族群重複壓力分", "H50族群生命週期"]
    merged = out.merge(grp[keep], how="left", left_on=_sector_name_series(out), right_on="_sector")
    merged.drop(columns=["_sector"], inplace=True, errors="ignore")

    stage = merged.get("H47主流領先狀態", pd.Series([""] * len(merged), index=merged.index)).fillna("").astype(str)
    start = _num_series(merged, ["H47起漲優先分"], 50.0).clip(0, 100)
    rs = _num_series(merged, ["H47個股相對強度分"], 50.0).clip(0, 100)
    onset = _num_series(merged, ["H45起漲結構分"], 50.0).clip(0, 100)
    trend = _num_series(merged, ["H45趨勢延續分"], 50.0).clip(0, 100)
    sector_buy = _num_series(merged, ["H50族群可買主流分"], 50.0).clip(0, 100)
    signal_fresh = _num_series(merged, ["今日訊號新鮮分"], 50.0).clip(0, 100)
    repeats = _num_series(merged, ["近5次入榜次數"], 0.0).clip(0, 5)
    consecutive = _num_series(merged, ["連續入榜次數"], 0.0).clip(0, 10)
    repeat_penalty = (((repeats - 2).clip(lower=0) * 4.0) + ((consecutive - 2).clip(lower=0) * 2.0)).clip(0, 14)
    repeat_penalty = repeat_penalty.where(signal_fresh < 65, repeat_penalty * 0.35)
    stage_adj = pd.Series([-15.0] * len(merged), index=merged.index, dtype="float64")
    stage_adj.loc[stage.str.startswith("L-EARLY")] = 8.0
    stage_adj.loc[stage.str.startswith("L-PULLBACK")] = 7.0
    stage_adj.loc[stage.str.startswith("L-LEADER")] = 3.0
    stage_adj.loc[stage.str.startswith("L-WATCH")] = -9.0
    stage_adj.loc[stage.str.startswith("L-EXTENDED")] = -20.0
    priority = (sector_buy * 0.32 + start * 0.25 + rs * 0.18 + onset * 0.10 + trend * 0.10 + signal_fresh * 0.05 + stage_adj - repeat_penalty).clip(0, 100)
    merged["H50主流購買優先分"] = priority.round(2)
    merged["H50重複推薦扣分"] = repeat_penalty.round(2)

    lifecycle = merged.get("H50族群生命週期", pd.Series([""] * len(merged), index=merged.index)).fillna("").astype(str)
    r5s = _num_series(merged, ["近5日漲幅%", "5日績效%"], 0.0)
    r20s = _num_series(merged, ["近20日漲幅%", "20日績效%"], 0.0)
    closes = _num_series(merged, ["當日收盤位置%"], 50.0)
    uppers = _num_series(merged, ["上影線比例%"], 20.0)
    reclaim_stock = _num_series(merged, ["主流領漲回補分"], 50.0)
    ai_miss_stock = _num_series(merged, ["AI漏選風險分"], 50.0)
    stages50 = []
    statuses = []
    reasons = []
    for i in merged.index:
        stg = _s(stage.loc[i])
        life = _s(lifecycle.loc[i])
        score = _f(merged.loc[i, "H50主流購買優先分"], 0)
        sb = _f(merged.loc[i, "H50族群可買主流分"], 0)
        rep = _f(merged.loc[i, "H50重複推薦扣分"], 0)
        r5 = _f(r5s.loc[i], 0); r20 = _f(r20s.loc[i], 0)
        close = _f(closes.loc[i], 50); upper = _f(uppers.loc[i], 20)
        reclaim = _f(reclaim_stock.loc[i], 50); ai_miss = _f(ai_miss_stock.loc[i], 50)
        # H50 may discover a rotation/reclaim setup even when old H47 marked L-NO
        # because H47 required the H45 sector to already be A/B.  This breaks the
        # circular dependency and is crucial for a fresh theme returning after a
        # controlled 5-day pullback inside a healthy 20-day trend.
        reclaim_setup = bool(
            life.startswith("B1") and 5 <= r20 <= 35 and -12 <= r5 <= 4
            and (reclaim >= 60 or ai_miss >= 82) and close >= 35 and upper <= 60
        )
        fresh_ignition = bool(
            life.startswith(("A0", "A1")) and -2 <= r5 <= 9 and -2 <= r20 <= 28
            and (reclaim >= 58 or ai_miss >= 78) and close >= 45 and upper <= 55
        )
        if stg.startswith("L-EXTENDED"):
            stage50 = "N-EXTENDED｜主流已延伸"
        elif stg.startswith("L-EARLY") or fresh_ignition:
            stage50 = "N-EARLY｜新主流起漲"
        elif stg.startswith("L-PULLBACK") or reclaim_setup:
            stage50 = "N-PULLBACK｜主流回檔再攻"
        elif stg.startswith("L-LEADER"):
            stage50 = "N-LEADER｜主流領漲"
        elif life.startswith("C"):
            stage50 = "N-MATURE｜成熟主流"
        elif life.startswith(("A0", "A1", "B1", "B2")):
            stage50 = "N-RADAR｜主流輪動觀察"
        else:
            stage50 = "N-NO｜非新鮮主流"

        if stage50.startswith("N-EXTENDED"):
            status = "F-NO-CHASE｜成熟主流已延伸"
        elif stage50.startswith(("N-EARLY", "N-PULLBACK")) and score >= 58:
            status = "F-SETUP｜新鮮主流起漲/再攻"
        elif stage50.startswith("N-LEADER") and score >= 62:
            status = "F-LEADER｜主流領漲待低風險買點"
        elif stage50.startswith("N-RADAR") and score >= 55:
            status = "F-RADAR｜新主流觀察，尚未成形"
        elif stage50.startswith("N-MATURE"):
            status = "F-MATURE｜成熟主流，降低重複推薦"
        else:
            status = "F-NO｜非新鮮主流購買優先"
        # Re-score with the new H50 lifecycle stage instead of the old H47 gate.
        # This is what lets a fresh rotation/reclaim setup escape an old L-NO
        # that existed only because H45 had not yet promoted the sector to A/B.
        stage50_adj = (8.0 if stage50.startswith("N-EARLY") else 7.0 if stage50.startswith("N-PULLBACK")
                       else 3.0 if stage50.startswith("N-LEADER") else -5.0 if stage50.startswith("N-RADAR")
                       else -12.0 if stage50.startswith("N-MATURE") else -20.0 if stage50.startswith("N-EXTENDED") else -15.0)
        score50 = _clip(sb * 0.32 + _f(start.loc[i],50) * 0.25 + _f(rs.loc[i],50) * 0.18
                        + _f(onset.loc[i],50) * 0.10 + _f(trend.loc[i],50) * 0.10
                        + _f(signal_fresh.loc[i],50) * 0.05 + stage50_adj - rep)
        merged.loc[i, "H50主流購買優先分"] = round(score50, 2)
        if stage50.startswith(("N-EARLY", "N-PULLBACK")) and life.startswith(("A0", "A1", "B1", "B2")) and score50 >= 58:
            status = "F-SETUP｜新鮮主流起漲/再攻"
        elif stage50.startswith("N-LEADER") and score50 >= 62:
            status = "F-LEADER｜主流領漲待低風險買點"
        elif stage50.startswith("N-RADAR") and score50 >= 55:
            status = "F-RADAR｜新主流觀察，尚未成形"
        stages50.append(stage50)
        statuses.append(status)
        reasons.append(f"{life or '生命週期未知'}｜{stage50}｜族群可買{sb:.1f}｜H47={stg or 'NA'}｜5日{r5:+.1f}%/20日{r20:+.1f}%｜回補{reclaim:.1f}/漏選{ai_miss:.1f}｜重複扣分{rep:.1f}")
    merged["H50波段機會階段"] = stages50
    merged["H50主流購買狀態"] = statuses
    merged["H50主流購買理由"] = reasons
    merged["H50主流版本"] = VERSION
    # H50版本 is kept only for backward compatibility.  The dual-route layer
    # owns the final recommendation version and may overwrite it later.
    merged["H50版本"] = VERSION
    return merged


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

    # H47：把「市場領先」與「能不能買」拆成兩個維度。H45 的最大問題是
    # 真正領漲股會因 Risk/Trade 偏低被降成 WATCH，反而較安全但較弱的股票
    # 變成 PREP，造成第一眼名單不像主流。H47 先用純市場資料重算族群內
    # 相對強度，再於後段獨立套交易風控。
    _r1 = _num_series(out, ["今日漲幅%", "當日漲幅%"], 0.0).clip(-12, 12)
    _r5 = _num_series(out, ["近5日漲幅%", "5日績效%"], 0.0).clip(-25, 25)
    _r20 = _num_series(out, ["近20日漲幅%", "20日績效%"], 0.0).clip(-40, 40)
    _vr = _num_series(out, ["當日量比", "均量比"], 1.0).clip(0, 3)
    _close = _num_series(out, ["當日收盤位置%"], 50.0).clip(0, 100)
    _upper = _num_series(out, ["上影線比例%"], 20.0).clip(0, 100)
    _dist20 = _num_series(out, ["突破20日高點%"], -15.0).clip(-30, 20)
    _mainfund = _num_series(out, ["主流資金分"], 50.0).clip(0, 100)
    _vol_score = (35.0 + _vr * 30.0).clip(35, 100)
    _close_quality = (_close - (_upper - 35.0).clip(lower=0) * 0.7).clip(0, 100)
    _breakout_prox = (100.0 + _dist20.clip(upper=0) * 4.0).clip(0, 100)
    _ret1_score = (50.0 + _r1 * 4.0).clip(0, 100)
    _ret5_score = (50.0 + _r5 * 2.8).clip(0, 100)
    _ret20_score = (50.0 + _r20 * 1.4).clip(0, 100)
    _rs_raw = (
        _ret1_score * 0.10 + _ret5_score * 0.26 + _ret20_score * 0.22
        + _vol_score * 0.14 + _close_quality * 0.10 + _breakout_prox * 0.08
        + _mainfund * 0.10
    ).clip(0, 100)
    out["_H47_rs_raw"] = _rs_raw
    _grp = out.groupby("_H45_sector_key", dropna=False)["_H47_rs_raw"]
    _rank = _grp.rank(method="min", ascending=False)
    _size = out.groupby("_H45_sector_key", dropna=False)["_H47_rs_raw"].transform("size").astype(float)
    _pct_raw = _grp.rank(method="average", pct=True, ascending=True) * 100.0
    _confidence = (_size / (_size + 3.0)).clip(0.20, 1.0)
    out["H47個股相對強度分"] = (50.0 + (_rs_raw - 50.0) * _confidence).clip(0, 100).round(2)
    out["H47族群內領先排名"] = _rank.fillna(999).astype(int)
    out["H47族群內領先百分位%"] = (50.0 + (_pct_raw - 50.0) * _confidence).clip(0, 100).round(1)

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

        # H47 純市場領先證據：不使用 Risk/Trade 決定「是不是主流股」。
        h47_rs = _first_num(row, ["H47個股相對強度分"], 50.0)
        h47_rank = _first_num(row, ["H47族群內領先排名"], 999.0)
        h47_pct = _first_num(row, ["H47族群內領先百分位%"], 50.0)

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

        # H47 波段階段：先把「主流領漲」與「起漲候選」分開，避免真領漲股
        # 因 Risk 低被藏起來，也避免已漲30~50%的股票被叫做起漲。
        sector_ab47 = sector_state.startswith(("A｜", "B｜"))
        extended47 = bool(ret5 >= 18 or ret20 >= 35 or (ret5 >= 12 and chase >= 70))
        early_zone47 = bool(-1.5 <= ret5 <= 8.5 and -3 <= ret20 <= 20 and breakout_proximity >= 65)
        leader47 = bool(
            sector_ab47 and h47_pct >= 65 and h47_rs >= 58 and ret5 >= 3 and ret20 >= 5
            and onset >= 60 and (vr >= 0.75 or close >= 65) and close >= 40 and upper <= 65
        )
        early47 = bool(
            sector_ab47 and h47_pct >= 58 and h47_rs >= 56 and early_zone47 and onset >= 62
            and vr >= 0.90 and close >= 55 and upper <= 45
        )
        pullback47 = bool(
            sector_ab47 and h47_pct >= 58 and h47_rs >= 54 and 5 <= ret20 <= 30
            and -6 <= ret5 <= 2.5 and breakout_proximity >= 55 and close >= 45 and upper <= 55
        )
        watch47 = bool(
            sector_ab47 and h47_pct >= 50 and h47_rs >= 52 and ret20 >= -5 and onset >= 56
        )
        if leader47 and extended47:
            h47_stage = "L-EXTENDED｜主流領漲但已延伸，禁止追價"
        elif early47:
            h47_stage = "L-EARLY｜主流波段起漲候選"
        elif pullback47:
            h47_stage = "L-PULLBACK｜主流回檔再攻候選"
        elif leader47:
            h47_stage = "L-LEADER｜主流領漲核心"
        elif watch47:
            h47_stage = "L-WATCH｜主流觀察"
        else:
            h47_stage = "L-NO｜非主流領先優先"

        def _zone(v: float, lo: float, hi: float, slope: float) -> float:
            if lo <= v <= hi:
                return 100.0
            gap = lo - v if v < lo else v - hi
            return _clip(100.0 - gap * slope)
        early_fit = _zone(ret5, 0.0, 8.0, 7.0) * 0.55 + _zone(ret20, 0.0, 18.0, 3.0) * 0.45
        h47_start_score = _clip(
            sector_score * 0.18 + h47_rs * 0.20 + onset * 0.22 + trend * 0.12
            + volume_score * 0.10 + close_quality * 0.08 + breakout_proximity * 0.05 + early_fit * 0.05
        )
        rr47 = _first_num(row, ["路徑風險報酬比", "SuperAI執行風報比", "風險報酬比", "實戰風險報酬比"], 0.0)
        rr47_known = any(_s(row.get(c)) for c in ["路徑風險報酬比", "SuperAI執行風報比", "風險報酬比", "實戰風險報酬比"] if c in row.index)
        exec_safe47 = bool(
            not data_block and risk >= 58 and trade >= 60 and (not rr47_known or rr47 >= 1.20)
            and (stop <= 0 or stop <= 7.0) and chase <= 62 and (amount <= 0 or amount >= 150)
        )
        if h47_stage.startswith(("L-EARLY", "L-PULLBACK")) and exec_safe47:
            h47_trade_status = "T-READY｜主流結構＋交易條件通過"
        elif h47_stage.startswith("L-LEADER") and exec_safe47 and ret5 <= 12 and chase <= 55:
            h47_trade_status = "T-READY｜主流領漲但只准條件進場"
        elif h47_stage.startswith("L-EXTENDED"):
            h47_trade_status = "T-NO-CHASE｜主流領漲但已延伸，只等回測"
        elif h47_stage.startswith(("L-EARLY", "L-PULLBACK", "L-LEADER")):
            h47_trade_status = "T-PREP｜市場結構成立，交易條件未完成"
        elif h47_stage.startswith("L-WATCH"):
            h47_trade_status = "T-WATCH｜主流觀察"
        else:
            h47_trade_status = "T-NO｜非主流優先"

        h47_reasons = [
            f"族群{sector_score:.1f}/{sector_state or '未分類'}",
            f"族群內RS{h47_rs:.1f}｜排名{int(h47_rank) if h47_rank < 999 else '-'}｜百分位{h47_pct:.0f}",
            f"5日{ret5:+.1f}%/20日{ret20:+.1f}%",
            f"起漲{onset:.1f}｜量比{vr:.2f}｜收盤位置{close:.0f}%",
        ]
        if extended47:
            h47_reasons.append("已進入延伸段，辨識為領漲但禁止追價")
        if not exec_safe47:
            h47_reasons.append(f"交易條件未完成｜Risk{risk:.1f}/Trade{trade:.1f}/RR{rr47:.2f}/追價{chase:.0f}")

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
        combined = _clip(score * 0.48 + h47_start_score * 0.22 + v188 * 0.18 + trade * 0.07 + entry * 0.05)
        rows.append((
            round(leadership,2), round(onset,2), round(trend,2), round(volume_score,2), round(score,2), status, "；".join(reasons), round(combined,2),
            h47_stage, round(h47_start_score,2), h47_trade_status, "；".join(h47_reasons)
        ))

    out["H45個股領先分"] = [x[0] for x in rows]
    out["H45起漲結構分"] = [x[1] for x in rows]
    out["H45趨勢延續分"] = [x[2] for x in rows]
    out["H45量價啟動分"] = [x[3] for x in rows]
    out["H45主流波段分"] = [x[4] for x in rows]
    out["H45主流波段狀態"] = [x[5] for x in rows]
    out["H45主流波段理由"] = [x[6] for x in rows]
    out["H45主流交易綜合分"] = [x[7] for x in rows]
    out["H47波段階段"] = [x[8] for x in rows]
    out["H47主流領先狀態"] = out["H47波段階段"]
    out["H47起漲優先分"] = [x[9] for x in rows]
    out["H47交易候選狀態"] = [x[10] for x in rows]
    out["H47主流領先理由"] = [x[11] for x in rows]
    out["H45版本"] = VERSION
    out["H47版本"] = VERSION
    out.drop(columns=["_H45_sector_key", "_H47_rs_raw"], inplace=True, errors="ignore")
    out = _apply_h50_lifecycle(out)
    return out


def build_mainstream_wave_table(frame: pd.DataFrame, top_n: int = 8) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    work = frame if ("H45版本" in frame.columns and frame["H45版本"].astype(str).eq(VERSION).all()) else apply_mainstream_wave_engine(frame)
    status = work.get("H47主流領先狀態", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    pick = work.loc[status.str.startswith(("L-EARLY", "L-LEADER", "L-PULLBACK", "L-EXTENDED", "L-WATCH"))].copy()
    if pick.empty:
        return pick
    _status = pick["H47主流領先狀態"].astype(str)
    pick["_status_priority"] = _status.map(lambda x: 5 if x.startswith("L-EARLY") else 4 if x.startswith("L-LEADER") else 3 if x.startswith("L-PULLBACK") else 2 if x.startswith("L-EXTENDED") else 1)
    for c in ["H47起漲優先分", "H47個股相對強度分", "H47族群內領先百分位%", "H45主流交易綜合分", "H45主流波段分", "H45族群主流分", "H45族群主流百分位%", "V188股神作戰優先分"]:
        raw = pick[c] if c in pick.columns else pd.Series([0.0] * len(pick), index=pick.index)
        pick[c] = pd.to_numeric(raw, errors="coerce").fillna(0.0)
    for c in ["H50主流購買優先分", "H50族群可買主流分", "H50族群新鮮度分"]:
        if c not in pick.columns:
            pick[c] = 0.0
        pick[c] = pd.to_numeric(pick[c], errors="coerce").fillna(0.0)
    pick.sort_values(["_status_priority", "H50主流購買優先分", "H50族群可買主流分", "H47個股相對強度分", "H47起漲優先分"], ascending=[False, False, False, False, False], inplace=True, kind="mergesort")
    return pick.head(max(1, int(top_n))).drop(columns=["_status_priority"], errors="ignore").reset_index(drop=True)


def build_mainstream_sector_lifecycle_table(frame: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    work = frame if ("H50主流版本" in frame.columns and frame["H50主流版本"].astype(str).eq(VERSION).all()) else apply_mainstream_wave_engine(frame)
    sector = _sector_name_series(work)
    cols = [c for c in [
        "H50族群可買主流分", "H50族群新鮮度分", "H50族群回檔再攻分",
        "H50族群起漲候選比例%", "H50族群延伸過熱比例%", "H50族群重複壓力分", "H50族群生命週期",
        "H45族群主流分", "H45族群短線動能%", "H45族群5日上漲比例%", "H45族群20日上漲比例%",
        "H45族群量能分", "H45族群樣本數", "H45族群主流排名", "H45族群狀態"
    ] if c in work.columns]
    table = work[cols].copy()
    table.insert(0, "類別", sector.values)
    table = table.drop_duplicates(subset=["類別"], keep="first")
    for c in ["H50族群可買主流分", "H50族群新鮮度分", "H50族群回檔再攻分", "H45族群主流分"]:
        if c in table.columns:
            table[c] = pd.to_numeric(table[c], errors="coerce").fillna(0.0)
    table.sort_values([c for c in ["H50族群可買主流分", "H50族群新鮮度分", "H50族群回檔再攻分", "H45族群主流分"] if c in table.columns], ascending=False, inplace=True, kind="mergesort")
    table.insert(0, "H50可買主流排名", range(1, len(table) + 1))
    return table.head(max(1, int(top_n))).reset_index(drop=True)


__all__ = ["VERSION", "H45_COLUMNS", "apply_mainstream_wave_engine", "build_mainstream_wave_table", "build_mainstream_sector_lifecycle_table"]

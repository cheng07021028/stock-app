# -*- coding: utf-8 -*-
"""V191-H30 concise SuperAI guide for Page07 Excel exports.

This module does not promote stocks into Formal/A-.  It only gives the user a
small, decision-oriented first sheet so a 100+ column research workbook can be
read in the correct order: data authority -> Entry -> Risk -> RR -> executable
entry -> distance/chase -> AI/sector confirmation.
"""
from __future__ import annotations

from typing import Any
import math
import pandas as pd

VERSION = "v191_h30_super_ai_excel_guide_20260815"


def _blank(v: Any) -> bool:
    try:
        if v is None or pd.isna(v):
            return True
    except Exception:
        pass
    return str(v).strip().lower() in {"", "none", "nan", "nat", "null", "--", "-", "<na>"}


def _num_series(df: pd.DataFrame, names: list[str], default: float = 0.0) -> pd.Series:
    out = pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
    for name in names:
        if name not in df.columns:
            continue
        obj = df[name]
        # Duplicate column labels can make df[name] a DataFrame.  H30 must never
        # let the concise export crash because an upstream diagnostic has a
        # duplicate label; use the right-most non-empty duplicate.
        if isinstance(obj, pd.DataFrame):
            for j in range(obj.shape[1] - 1, -1, -1):
                s = pd.to_numeric(obj.iloc[:, j], errors="coerce")
                mask = out.isna() & s.notna()
                if mask.any():
                    out.loc[mask] = s.loc[mask]
            continue
        s = pd.to_numeric(obj, errors="coerce")
        mask = out.isna() & s.notna()
        if mask.any():
            out.loc[mask] = s.loc[mask]
    return out.fillna(float(default)).astype(float)


def _text_series(df: pd.DataFrame, names: list[str], default: str = "") -> pd.Series:
    out = pd.Series([default] * len(df), index=df.index, dtype="object")
    for name in names:
        if name not in df.columns:
            continue
        obj = df[name]
        frames = [obj.iloc[:, j] for j in range(obj.shape[1] - 1, -1, -1)] if isinstance(obj, pd.DataFrame) else [obj]
        for raw in frames:
            s = raw.fillna("").astype(str).str.strip()
            mask = out.map(_blank) & s.map(lambda x: not _blank(x))
            if mask.any():
                out.loc[mask] = s.loc[mask]
    return out


def _safe_ratio_score(rr: pd.Series) -> pd.Series:
    return (rr.clip(lower=0, upper=3) / 3.0 * 100.0).clip(0, 100)


def _status_tier(partition: pd.Series, permit: pd.Series) -> pd.Series:
    p = partition.fillna("").astype(str)
    a = permit.fillna("").astype(str)
    out = pd.Series([3] * len(p), index=p.index, dtype="int64")
    out.loc[p.str.contains("正式下週主推薦", regex=False)] = 0
    out.loc[p.str.contains("A-", regex=False)] = 1
    tradable = a.str.contains("可操作|條件進場|允許", regex=True)
    out.loc[(out > 1) & tradable] = 2
    return out


def build_super_ai_excel_guide(df: pd.DataFrame | None, *, max_rows: int = 20) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame({"狀態": ["本輪沒有可建立超級AI精選攻略的候選資料。"], "攻略版本": [VERSION]})

    work = df.copy()
    code = _text_series(work, ["股票代號", "代號"])
    name = _text_series(work, ["股票名稱", "名稱"])
    market = _text_series(work, ["市場別"])
    category = _text_series(work, ["類別", "產業類別"])
    partition = _text_series(work, ["正式推薦分區", "正式推薦等級"])
    permit = _text_series(work, ["操作許可", "V188交易許可", "建議動作"])

    entry = _num_series(work, ["Entry進場買點分", "Entry進場分", "進場時機分數", "買進分數"])
    risk = _num_series(work, ["Risk風控安全分", "Risk風控分", "風控安全分"])
    rr = _num_series(work, ["路徑風險報酬比", "實戰風險報酬比", "風險報酬比", "風險報酬比_決策"])
    executable = _num_series(work, ["進場可執行分", "交易可行分數", "交易可行分"])
    distance = _num_series(work, ["距最近可執行買點%", "觸發距離%"], 99.0)
    chase = _num_series(work, ["追價風險分", "追高風險分數_決策", "追價風險分數"], 55.0)
    trade = _num_series(work, ["SuperAI Trade分", "SuperAI交易分"])
    final_ai = _num_series(work, ["SuperAI 最終決策分", "SuperAI最終決策分", "V188股神作戰優先分"])
    next_up = _num_series(work, ["模型隔日上漲機率%", "隔日上漲機率%", "上漲機率%"], 50.0)
    sector = _num_series(work, ["族群攻擊強度", "族群攻擊分", "族群輪動分", "類股熱度分數"])
    mainstream = _num_series(work, ["主流資金分"])

    kline = _text_series(work, ["K線資料新鮮度", "K線最後交易日"])
    official = _text_series(work, ["官方因子新鮮度", "官方因子資料狀態", "官方資料日期"])
    market_fresh = _text_series(work, ["大盤資料新鮮度", "大盤風控層級", "大盤橋接狀態"])
    block = _text_series(work, ["正式與A近門檻說明", "正式推薦排除原因", "進場阻擋原因", "推薦漏斗阻擋主因"])
    action = _text_series(work, ["正式推薦動作", "建議動作", "進場路徑", "主要進場路徑"])

    rr_score = _safe_ratio_score(rr)
    distance_penalty = pd.Series(0.0, index=work.index)
    distance_penalty.loc[distance > 8] = 10.0
    distance_penalty.loc[(distance > 6) & (distance <= 8)] = 5.0
    guide_score = (
        entry * 0.15 + risk * 0.15 + rr_score * 0.15 + executable * 0.15
        + trade * 0.10 + final_ai * 0.10 + next_up * 0.05 + sector * 0.05
        + mainstream * 0.05 + (100.0 - chase.clip(0, 100)) * 0.05
        - distance_penalty
    ).clip(0, 100).round(1)

    tier = _status_tier(partition, permit)
    # Non-formal/A- candidates can still be a useful radar if their actual trade
    # structure is strong.  This is an observation tier only, never a promotion.
    conditional = (
        (tier >= 2) & (entry >= 60) & (risk >= 55) & (rr >= 1.05)
        & (chase <= 65) & (executable >= 40)
    )
    tier.loc[conditional] = 2

    valid_code = code.ne("")
    selected = pd.DataFrame(index=work.index)
    selected["_tier"] = tier
    selected["超級AI攻略分"] = guide_score
    selected["股票代號"] = code
    selected["股票名稱"] = name
    selected["市場別"] = market
    selected["類別"] = category
    selected["正式推薦分區"] = partition.where(partition.ne(""), "未升級")
    selected["操作許可"] = permit.where(permit.ne(""), "等待條件")
    selected["Entry"] = entry.round(1)
    selected["Risk"] = risk.round(1)
    selected["RR"] = rr.round(2)
    selected["進場可執行分"] = executable.round(1)
    selected["距最近買點%"] = distance.where(distance < 90, float("nan")).round(2)
    selected["追價風險"] = chase.round(1)
    selected["SuperAI Trade"] = trade.round(1)
    selected["SuperAI決策"] = final_ai.round(1)
    selected["隔日上漲機率%"] = next_up.round(1)
    selected["族群攻擊"] = sector.round(1)
    selected["主流資金"] = mainstream.round(1)
    selected["K線/資料狀態"] = kline
    selected["官方因子狀態"] = official
    selected["大盤狀態"] = market_fresh
    selected["主要阻擋/近門檻"] = block.where(block.ne(""), "—")
    selected["操作原則"] = action.where(action.ne(""), "未觸發前不買；只在可執行買點成立後操作")

    selected = selected.loc[valid_code].copy()
    selected = selected.sort_values(["_tier", "超級AI攻略分"], ascending=[True, False], kind="mergesort").head(max(1, int(max_rows)))
    label_map = {0: "正式推薦", 1: "A-準主推薦", 2: "條件候選", 3: "觀察雷達"}
    selected.insert(0, "超級AI定位", selected["_tier"].map(label_map).fillna("觀察雷達"))
    selected.insert(0, "攻略順位", range(1, len(selected) + 1))
    selected["攻略版本"] = VERSION
    return selected.drop(columns=["_tier"]).reset_index(drop=True)


__all__ = ["VERSION", "build_super_ai_excel_guide"]

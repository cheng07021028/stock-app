# -*- coding: utf-8 -*-
"""Phase 4 族群輪動引擎。

集中計算族群攻擊強度、續航力與領頭羊/補漲角色，避免每個模組重複 groupby。
"""
from __future__ import annotations

from typing import Any
import pandas as pd

from godpick_runtime_cache import cache_key, get_or_compute

SECTOR_ROTATION_VERSION = "phase4_sector_rotation_20260605"
SECTOR_ROTATION_COLUMNS = [
    "族群輪動分", "族群攻擊強度", "族群續航力", "族群內領頭羊", "族群內補漲股", "資金輪動角色", "族群攻擊說明", "族群輪動版本",
]
NUMERIC_SECTOR_ROTATION_COLUMNS = {"族群輪動分", "族群攻擊強度", "族群續航力"}


def _blank(v: Any) -> bool:
    try:
        if v is None or pd.isna(v):
            return True
    except Exception:
        pass
    return str(v).strip().lower() in {"", "nan", "none", "null", "--", "-", "<na>"}


def _num(df: pd.DataFrame, names: list[str], default: float = 0.0) -> pd.Series:
    out = pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
    for name in names:
        if name not in df.columns:
            continue
        s = pd.to_numeric(df[name], errors="coerce")
        mask = out.isna() & s.notna()
        if mask.any():
            out.loc[mask] = s.loc[mask]
    return out.fillna(default).astype(float)


def _cat(df: pd.DataFrame) -> pd.Series:
    if "類別" in df.columns:
        s = df["類別"].fillna("").astype(str).str.strip()
    elif "產業" in df.columns:
        s = df["產業"].fillna("").astype(str).str.strip()
    else:
        s = pd.Series(["未分類"] * len(df), index=df.index)
    return s.replace("", "未分類")


def _sector_scores(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    category = _cat(work)
    score = _num(work, ["推薦總分", "候選強度分", "股神實戰總分"], 50)
    ret5 = _num(work, ["近5日漲幅%", "5日漲幅%"], 0)
    ret20 = _num(work, ["近20日漲幅%", "20日漲幅%"], 0)
    amount = _num(work, ["成交額百萬", "20日均成交額百萬"], 0)
    vol = _num(work, ["人氣量能分", "量能啟動分", "量價動能分數"], 50)
    old_heat = _num(work, ["類股熱度分數", "族群資金流分數"], 50)

    tmp = pd.DataFrame({
        "類別": category,
        "score": score,
        "ret5": ret5,
        "ret20": ret20,
        "amount": amount,
        "vol": vol,
        "old_heat": old_heat,
    }, index=work.index)
    tmp["strong"] = ((tmp["ret5"] >= 3) | (tmp["score"] >= 84)).astype(int)
    tmp["weak"] = ((tmp["ret5"] <= -3) | (tmp["score"] < 58)).astype(int)
    grouped = tmp.groupby("類別", dropna=False)
    g = grouped.agg(
        n=("score", "size"),
        avg_score=("score", "mean"),
        avg_ret5=("ret5", "mean"),
        avg_ret20=("ret20", "mean"),
        avg_amount=("amount", "mean"),
        avg_vol=("vol", "mean"),
        avg_old_heat=("old_heat", "mean"),
        strong_count=("strong", "sum"),
        weak_count=("weak", "sum"),
    )
    g["strong_ratio"] = g["strong_count"] / g["n"].clip(lower=1)
    g["attack"] = (g["avg_score"] * 0.22 + g["avg_old_heat"] * 0.23 + g["avg_vol"] * 0.18 + g["strong_ratio"] * 100 * 0.20 + (g["avg_ret5"].clip(-5, 10) + 5) * 1.7).clip(0, 100)
    g["rotation"] = (g["attack"] * 0.62 + (g["avg_ret5"].clip(-5, 8) + 5) * 2.4 + (g["n"].clip(1, 8) / 8 * 10)).clip(0, 100)
    g["continuation"] = (g["attack"] * 0.50 + (100 - (g["avg_ret20"] - 18).clip(lower=0) * 2.2).clip(20, 100) * 0.25 + (100 - g["weak_count"] / g["n"].clip(lower=1) * 100) * 0.25).clip(0, 100)

    rank_in_sector = tmp.groupby("類別")["score"].rank(ascending=False, method="dense")
    max_score = tmp.groupby("類別")["score"].transform("max")
    avg_score = tmp.groupby("類別")["score"].transform("mean")
    attack = category.map(g["attack"]).fillna(50).astype(float)
    rotation = category.map(g["rotation"]).fillna(50).astype(float)
    continuation = category.map(g["continuation"]).fillna(50).astype(float)
    n = category.map(g["n"]).fillna(1).astype(float)

    leader = ((rank_in_sector <= 2) & (score >= avg_score) & (attack >= 62)).map({True: "是", False: "否"})
    catchup = ((score < max_score) & (score >= avg_score * 0.94) & (ret5 < tmp.groupby("類別")["ret5"].transform("max")) & (attack >= 62)).map({True: "是", False: "否"})
    role = []
    note = []
    for idx in work.index:
        a = float(attack.loc[idx]); r = float(rotation.loc[idx]); c = float(continuation.loc[idx]); nn = int(n.loc[idx])
        if a >= 78 and c >= 65:
            rr = "主流攻擊族群"
        elif a >= 66:
            rr = "輪動轉強族群"
        elif r >= 58:
            rr = "補漲觀察族群"
        else:
            rr = "非主流觀察"
        role.append(rr)
        note.append(f"{rr}｜樣本{nn}｜攻擊{a:.1f}｜續航{c:.1f}")

    out = pd.DataFrame(index=work.index)
    out["族群輪動分"] = rotation.round(1)
    out["族群攻擊強度"] = attack.round(1)
    out["族群續航力"] = continuation.round(1)
    out["族群內領頭羊"] = leader
    out["族群內補漲股"] = catchup
    out["資金輪動角色"] = role
    out["族群攻擊說明"] = note
    out["族群輪動版本"] = SECTOR_ROTATION_VERSION
    return out


def derive_sector_rotation(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=SECTOR_ROTATION_COLUMNS)
    key = cache_key("sector_rotation", df)
    return get_or_compute(key, lambda: _sector_scores(df))


def apply_sector_rotation_engine(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=SECTOR_ROTATION_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        for c in SECTOR_ROTATION_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="float64" if c in NUMERIC_SECTOR_ROTATION_COLUMNS else "object")
        return out
    ctx = derive_sector_rotation(out).reindex(out.index)
    for c in SECTOR_ROTATION_COLUMNS:
        if c not in out.columns or out[c].map(_blank).all():
            out[c] = ctx[c]
        else:
            mask = out[c].map(_blank)
            if mask.any():
                out.loc[mask, c] = ctx.loc[mask, c]
    return out

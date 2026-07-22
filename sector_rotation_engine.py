# -*- coding: utf-8 -*-
"""族群輪動與主流廣度引擎。

2026-07-22 修正重點：
1. 單一或極少數股票上漲，不得只因強勢比例 100% 就被判定為主流族群。
2. 主流族群必須同時具備廣度、成交額與續航，不再只看平均分數。
3. 所有欄位均為衍生結果，每次套用都重新計算，避免舊快取殘留舊族群分數。
"""
from __future__ import annotations

from typing import Any
import pandas as pd

from godpick_runtime_cache import cache_key, get_or_compute

SECTOR_ROTATION_VERSION = "vnext_mainstream_breadth_20260722"
SECTOR_ROTATION_COLUMNS = [
    "族群輪動分", "族群攻擊強度", "族群續航力", "族群樣本數", "族群廣度分", "族群成交額分",
    "族群主升確認", "族群內領頭羊", "族群內補漲股", "資金輪動角色", "族群攻擊說明", "族群輪動版本",
]
NUMERIC_SECTOR_ROTATION_COLUMNS = {
    "族群輪動分", "族群攻擊強度", "族群續航力", "族群樣本數", "族群廣度分", "族群成交額分",
}


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
    for col in ["類別", "產業", "產業別", "題材族群"]:
        if col in df.columns:
            s = df[col].fillna("").astype(str).str.strip()
            if s.ne("").any():
                return s.replace("", "未分類")
    return pd.Series(["未分類"] * len(df), index=df.index)


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
    tmp["positive"] = ((tmp["ret5"] > 0) | (tmp["score"] >= 72)).astype(int)
    tmp["weak"] = ((tmp["ret5"] <= -3) | (tmp["score"] < 58)).astype(int)

    grouped = tmp.groupby("類別", dropna=False)
    g = grouped.agg(
        n=("score", "size"),
        avg_score=("score", "mean"),
        avg_ret5=("ret5", "mean"),
        avg_ret20=("ret20", "mean"),
        avg_amount=("amount", "mean"),
        total_amount=("amount", "sum"),
        avg_vol=("vol", "mean"),
        avg_old_heat=("old_heat", "mean"),
        strong_count=("strong", "sum"),
        positive_count=("positive", "sum"),
        weak_count=("weak", "sum"),
    )
    g["strong_ratio"] = g["strong_count"] / g["n"].clip(lower=1)
    g["positive_ratio"] = g["positive_count"] / g["n"].clip(lower=1)
    g["weak_ratio"] = g["weak_count"] / g["n"].clip(lower=1)

    # 廣度：至少 3 檔同步轉強才接近滿分；樣本數只作輔助，避免一檔族群 100% 強勢誤判。
    g["breadth"] = (
        (g["strong_count"].clip(0, 3) / 3.0) * 55.0
        + (g["positive_count"].clip(0, 5) / 5.0) * 25.0
        + (g["n"].clip(1, 8) / 8.0) * 20.0
    ).clip(0, 100)

    # 成交額：同時看族群總成交額與平均成交額；大型主流族群可抵銷樣本較少的劣勢。
    g["amount_score"] = (
        (g["total_amount"] / 8000.0 * 100.0).clip(0, 100) * 0.58
        + (g["avg_amount"] / 1200.0 * 100.0).clip(0, 100) * 0.42
    ).clip(0, 100)
    ret5_score = ((g["avg_ret5"].clip(-5, 10) + 5.0) / 15.0 * 100.0).clip(0, 100)

    raw_attack = (
        g["avg_score"] * 0.20
        + g["avg_old_heat"] * 0.16
        + g["avg_vol"] * 0.14
        + g["strong_ratio"] * 100.0 * 0.12
        + ret5_score * 0.14
        + g["amount_score"] * 0.12
        + g["breadth"] * 0.12
    ).clip(0, 100)

    # 小樣本向中性 50 分收斂。只有成交額非常集中且強勢明確時，才允許小族群取得較高分。
    sample_confidence = (g["n"].clip(1, 6) / 6.0).pow(0.5)
    g["attack"] = (50.0 + (raw_attack - 50.0) * (0.52 + sample_confidence * 0.48)).clip(0, 100)
    one_cap = pd.Series(62.0, index=g.index)
    one_cap.loc[(g["total_amount"] >= 5000) & (g["strong_count"] >= 1) & (g["avg_ret5"] >= 3)] = 69.0
    two_cap = pd.Series(72.0, index=g.index)
    two_cap.loc[(g["total_amount"] >= 8000) & (g["strong_count"] >= 2)] = 79.0
    g.loc[g["n"] == 1, "attack"] = g.loc[g["n"] == 1, ["attack"]].iloc[:, 0].clip(upper=one_cap.loc[g["n"] == 1])
    g.loc[g["n"] == 2, "attack"] = g.loc[g["n"] == 2, ["attack"]].iloc[:, 0].clip(upper=two_cap.loc[g["n"] == 2])

    stability = (
        (100.0 - g["weak_ratio"] * 100.0) * 0.55
        + (100.0 - (g["avg_ret20"] - 24.0).clip(lower=0) * 2.2).clip(20, 100) * 0.45
    ).clip(0, 100)
    g["rotation"] = (
        g["attack"] * 0.52 + g["breadth"] * 0.18 + g["amount_score"] * 0.18 + ret5_score * 0.12
    ).clip(0, 100)
    g["continuation"] = (
        g["attack"] * 0.42 + stability * 0.25 + g["breadth"] * 0.18 + g["amount_score"] * 0.15
    ).clip(0, 100)

    rank_in_sector = tmp.groupby("類別")["score"].rank(ascending=False, method="dense")
    max_score = tmp.groupby("類別")["score"].transform("max")
    avg_score = tmp.groupby("類別")["score"].transform("mean")
    attack = category.map(g["attack"]).fillna(50).astype(float)
    rotation = category.map(g["rotation"]).fillna(50).astype(float)
    continuation = category.map(g["continuation"]).fillna(50).astype(float)
    breadth = category.map(g["breadth"]).fillna(0).astype(float)
    amount_score = category.map(g["amount_score"]).fillna(0).astype(float)
    n = category.map(g["n"]).fillna(1).astype(float)
    strong_count = category.map(g["strong_count"]).fillna(0).astype(float)

    leader = ((rank_in_sector <= 2) & (score >= avg_score) & (attack >= 62) & (amount >= 100)).map({True: "是", False: "否"})
    catchup = (
        (score < max_score) & (score >= avg_score * 0.94)
        & (ret5 < tmp.groupby("類別")["ret5"].transform("max"))
        & (attack >= 62) & (breadth >= 45)
    ).map({True: "是", False: "否"})

    role: list[str] = []
    note: list[str] = []
    mainrise: list[str] = []
    for idx in work.index:
        a = float(attack.loc[idx]); r = float(rotation.loc[idx]); c = float(continuation.loc[idx])
        b = float(breadth.loc[idx]); am = float(amount_score.loc[idx]); nn = int(n.loc[idx]); sc = int(strong_count.loc[idx])
        confirmed = a >= 70 and c >= 60 and b >= 52 and am >= 55 and sc >= 2
        concentrated = a >= 67 and c >= 58 and am >= 82 and sc >= 1
        if confirmed:
            mr = "是｜廣度、成交額與續航同步"
        elif concentrated:
            mr = "條件式｜高成交額集中領漲"
        else:
            mr = "否｜尚未形成族群主升"
        if a >= 76 and c >= 62 and (confirmed or concentrated):
            rr = "主流攻擊族群"
        elif a >= 65 and (b >= 42 or am >= 65):
            rr = "輪動轉強族群"
        elif r >= 57:
            rr = "補漲觀察族群"
        else:
            rr = "非主流觀察"
        role.append(rr)
        mainrise.append(mr)
        note.append(f"{rr}｜樣本{nn}｜廣度{b:.1f}｜成交額{am:.1f}｜攻擊{a:.1f}｜續航{c:.1f}")

    out = pd.DataFrame(index=work.index)
    out["族群輪動分"] = rotation.round(1)
    out["族群攻擊強度"] = attack.round(1)
    out["族群續航力"] = continuation.round(1)
    out["族群樣本數"] = n.round(0).astype(int)
    out["族群廣度分"] = breadth.round(1)
    out["族群成交額分"] = amount_score.round(1)
    out["族群主升確認"] = mainrise
    out["族群內領頭羊"] = leader
    out["族群內補漲股"] = catchup
    out["資金輪動角色"] = role
    out["族群攻擊說明"] = note
    out["族群輪動版本"] = SECTOR_ROTATION_VERSION
    return out


def derive_sector_rotation(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=SECTOR_ROTATION_COLUMNS)
    key = cache_key("sector_rotation_vnext_mainstream_breadth_20260722", df)
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
    # 衍生欄位一律覆寫，避免匯入/快取中的舊版族群分數繼續影響新排名。
    for c in SECTOR_ROTATION_COLUMNS:
        out[c] = ctx[c].values
    return out

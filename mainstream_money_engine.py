# -*- coding: utf-8 -*-
"""Phase 4.2 主流資金 / 流動性濾網。

目的：
- 把主流資金股、可交易股、冷門潛伏股分層，避免低成交額股票混進 S/A/B 主流追蹤。
- 集中處理成交額、成交量、均量比與法人/主力有效性，避免各頁重複計算。
- 不連網、不讀寫 JSON，只回補欄位。
"""
from __future__ import annotations

from typing import Any
import pandas as pd

MAINSTREAM_MONEY_VERSION = "phase4_3_liquidity_unknown_safe_20260712"

MAINSTREAM_MONEY_COLUMNS = [
    "主流資金分",
    "資金攻擊有效分",
    "成交額等級",
    "流動性等級",
    "流動性資料狀態",
    "流動性資料來源",
    "冷門股警示",
    "主流股判定",
    "資金攻擊有效性",
    "主流資金角色",
    "主流資金說明",
    "主流資金引擎版本",
]

NUMERIC_MAINSTREAM_MONEY_COLUMNS = {"主流資金分", "資金攻擊有效分"}

_BLANKS = {"", "nan", "none", "null", "nat", "--", "-", "<na>"}


def _blank(v: Any) -> bool:
    try:
        if v is None or pd.isna(v):
            return True
    except Exception:
        pass
    return str(v).strip().lower() in _BLANKS


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


def _txt(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name].fillna("").astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype="object")


def _amount_score(amount_m: pd.Series, avg_amount_m: pd.Series) -> pd.Series:
    a = pd.to_numeric(amount_m, errors="coerce").fillna(0.0)
    avg = pd.to_numeric(avg_amount_m, errors="coerce").fillna(0.0)
    ref = a.where(a > 0, avg)
    score = pd.Series([25.0] * len(ref), index=ref.index)
    score = score.mask(ref >= 5000, 96)
    score = score.mask((ref >= 2000) & (ref < 5000), 90)
    score = score.mask((ref >= 1000) & (ref < 2000), 82)
    score = score.mask((ref >= 500) & (ref < 1000), 72)
    score = score.mask((ref >= 250) & (ref < 500), 62)
    score = score.mask((ref >= 100) & (ref < 250), 50)
    score = score.mask((ref >= 50) & (ref < 100), 38)
    score = score.mask((ref > 0) & (ref < 50), 25)
    return score.astype(float)


def _volume_score(volume: pd.Series, avg_volume: pd.Series) -> pd.Series:
    v = pd.to_numeric(volume, errors="coerce").fillna(0.0)
    avg = pd.to_numeric(avg_volume, errors="coerce").fillna(0.0)
    ref = v.where(v > 0, avg)
    score = pd.Series([25.0] * len(ref), index=ref.index)
    score = score.mask(ref >= 20000, 94)
    score = score.mask((ref >= 10000) & (ref < 20000), 88)
    score = score.mask((ref >= 5000) & (ref < 10000), 78)
    score = score.mask((ref >= 2500) & (ref < 5000), 66)
    score = score.mask((ref >= 1000) & (ref < 2500), 54)
    score = score.mask((ref >= 500) & (ref < 1000), 40)
    score = score.mask((ref > 0) & (ref < 500), 28)
    return score.astype(float)


def _ratio_score(volume: pd.Series, avg_volume: pd.Series, amount_m: pd.Series) -> pd.Series:
    v = pd.to_numeric(volume, errors="coerce").fillna(0.0)
    avg = pd.to_numeric(avg_volume, errors="coerce").fillna(0.0)
    amount = pd.to_numeric(amount_m, errors="coerce").fillna(0.0)
    ratio = pd.Series([1.0] * len(v), index=v.index, dtype="float64")
    valid = v.gt(0) & avg.gt(0)
    ratio.loc[valid] = (v.loc[valid] / avg.loc[valid]).clip(0, 8)
    score = pd.Series([50.0] * len(v), index=v.index)
    score = score.mask(ratio >= 3.0, 82)
    score = score.mask((ratio >= 2.0) & (ratio < 3.0), 74)
    score = score.mask((ratio >= 1.3) & (ratio < 2.0), 64)
    score = score.mask((ratio >= 0.8) & (ratio < 1.3), 54)
    score = score.mask((ratio > 0) & (ratio < 0.8), 42)
    # 冷門股均量比很容易失真：成交額太低時，均量比不給高分。
    score = score.where(amount >= 100, score.clip(upper=58))
    score = score.where(amount >= 50, score.clip(upper=50))
    return score.astype(float)


def _has_real_legal_flow(df: pd.DataFrame) -> pd.Series:
    flow_cols = [
        "外資近1日買賣超", "外資買賣超", "外資買超",
        "投信近1日買賣超", "投信買賣超", "投信買超",
        "自營商近1日買賣超", "自營商買賣超", "自營商買超",
        "三大法人近1日合計", "三大法人合計", "法人合計買賣超",
        "法人買超占量比%", "法人買超占成交量%",
    ]
    has = pd.Series([False] * len(df), index=df.index)
    for col in flow_cols:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        has = has | (s.notna() & s.ne(0))
    return has


def apply_mainstream_money_engine(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=MAINSTREAM_MONEY_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        for c in MAINSTREAM_MONEY_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="float64" if c in NUMERIC_MAINSTREAM_MONEY_COLUMNS else "object")
        return out

    amount = _num(out, ["成交額百萬", "今日成交額百萬", "成交金額百萬"], 0)
    avg_amount = _num(out, ["20日均成交額百萬", "二十日均成交額百萬", "均成交額百萬"], 0)
    volume = _num(out, ["最新成交量_張", "最新成交量張", "成交量_張", "成交量"], 0)
    avg_volume = _num(out, ["20日均量_張", "20日均量張", "均量_張"], 0)
    price = _num(out, ["最新價", "推薦價格", "推薦日價格", "收盤價"], 0)

    # Phase 8.3: recover liquidity from price x volume when the direct amount field
    # is absent.  Missing data must never be silently converted into zero-volume.
    calc_amount = (price * volume / 1000.0).where(price.gt(0) & volume.gt(0), 0.0)
    calc_avg_amount = (price * avg_volume / 1000.0).where(price.gt(0) & avg_volume.gt(0), 0.0)
    amount = amount.where(amount.gt(0), calc_amount)
    avg_amount = avg_amount.where(avg_amount.gt(0), calc_avg_amount)

    liquidity_known = amount.gt(0) | avg_amount.gt(0) | volume.gt(0) | avg_volume.gt(0)
    direct_known = _num(out, ["成交額百萬", "今日成交額百萬", "成交金額百萬"], 0).gt(0)
    volume_recovered = ~direct_known & amount.gt(0)

    sector_attack = _num(out, ["族群攻擊強度", "族群輪動分", "族群資金流分數", "類股熱度分數"], 50)
    money = _num(out, ["籌碼續航分", "主力點火分", "法人攻擊分", "法人籌碼分數"], 50)
    volume_factor = _num(out, ["人氣量能分", "量能啟動分", "量價動能分數"], 50)
    ret5 = _num(out, ["近5日漲幅%", "5日漲幅%"], 0)
    real_legal = _has_real_legal_flow(out)

    amount_s = _amount_score(amount, avg_amount)
    volume_s = _volume_score(volume, avg_volume)
    ratio_s = _ratio_score(volume, avg_volume, amount)

    legal_effective = money.where(real_legal, 50.0)
    smart_effective = (legal_effective * 0.34 + volume_factor.clip(0, 100) * 0.26 + ratio_s * 0.18 + sector_attack.clip(0, 100) * 0.22).clip(0, 100)

    mainstream = (
        amount_s * 0.38
        + volume_s * 0.22
        + smart_effective * 0.20
        + sector_attack.clip(0, 100) * 0.14
        + (50 + ret5.clip(-5, 8) * 2.0).clip(35, 78) * 0.06
    ).clip(0, 100)
    # Unknown liquidity is neutral/diagnostic, not a fake low-liquidity score.
    mainstream = mainstream.where(liquidity_known, 50.0)
    mainstream = mainstream.where(~liquidity_known | (amount >= 50), mainstream.clip(upper=42))
    mainstream = mainstream.where(~liquidity_known | (amount >= 100), mainstream.clip(upper=52))
    mainstream = mainstream.where(~liquidity_known | (amount >= 250), mainstream.clip(upper=66))

    amount_grade = pd.Series("資料待補", index=out.index, dtype="object")
    amount_grade.loc[liquidity_known] = "低成交額"
    amount_grade.loc[amount >= 100] = "可觀察成交額"
    amount_grade.loc[amount >= 250] = "中等成交額"
    amount_grade.loc[amount >= 500] = "高成交額"
    amount_grade.loc[amount >= 1000] = "主流成交額"
    amount_grade.loc[amount >= 3000] = "市場焦點成交額"

    liquidity = pd.Series("資料待補", index=out.index, dtype="object")
    liquidity.loc[liquidity_known] = "低流動性"
    liquidity.loc[(amount >= 100) & ((volume >= 1000) | avg_volume.ge(1000))] = "可交易"
    liquidity.loc[(amount >= 500) | (volume >= 5000) | (avg_volume >= 5000)] = "高流動性"
    liquidity.loc[(amount >= 2000) | (volume >= 20000) | (avg_volume >= 20000)] = "主流高流動性"

    severe_cold = liquidity_known & ((amount < 50) | ((amount < 100) & (volume < 1000) & (avg_volume < 1000)))
    cold = liquidity_known & (severe_cold | ((amount < 250) & (volume < 2500) & (avg_volume < 2500) & (sector_attack < 78)))

    warning = pd.Series("", index=out.index, dtype="object")
    warning.loc[~liquidity_known] = "流動性資料缺失：禁止正式推薦，待補成交額/成交量後重評"
    warning.loc[cold] = "冷門股：成交額/成交量不足，禁止追高，只能觀察"
    warning.loc[severe_cold] = "低流動性排除：成交額過低，容易滑價與假突破"

    mainstream_label = pd.Series("流動性待補", index=out.index, dtype="object")
    mainstream_label.loc[liquidity_known] = "高流動性觀察股"
    mainstream_label.loc[liquidity_known & (mainstream >= 65)] = "主流輪動股"
    mainstream_label.loc[liquidity_known & (mainstream >= 76) & (sector_attack >= 66)] = "主流攻擊股"
    mainstream_label.loc[cold & ~severe_cold] = "冷門潛伏股"
    mainstream_label.loc[severe_cold] = "冷門禁追股"

    effective = pd.Series("資料不足｜不作資金攻擊判定", index=out.index, dtype="object")
    effective.loc[liquidity_known] = "中性｜缺法人實流資料時不加分"
    effective.loc[(smart_effective >= 66) & (amount >= 250)] = "有效｜資金與成交額同步"
    effective.loc[(smart_effective >= 72) & (amount >= 500)] = "強效｜主流資金攻擊"
    effective.loc[cold] = "無效｜低成交額放量不視為主流攻擊"
    effective.loc[real_legal & (money >= 60) & (amount >= 250)] = "有效｜法人/主力資料支持"

    role = pd.Series("流動性待補觀察", index=out.index, dtype="object")
    role.loc[liquidity_known] = "高流動性觀察"
    role.loc[mainstream_label.eq("主流攻擊股")] = "主流攻擊候選"
    role.loc[mainstream_label.eq("主流輪動股")] = "主流突破追蹤"
    role.loc[mainstream_label.eq("冷門潛伏股")] = "冷門潛伏觀察"
    role.loc[mainstream_label.eq("冷門禁追股")] = "低流動性排除"

    source = pd.Series("缺少成交額/成交量", index=out.index, dtype="object")
    source.loc[direct_known] = "歷史K線成交金額"
    source.loc[volume_recovered] = "最新價×成交量回推"
    source.loc[~direct_known & ~volume_recovered & avg_amount.gt(0)] = "20日均成交額"

    out["成交額百萬"] = amount.round(1)
    out["20日均成交額百萬"] = avg_amount.round(1)
    out["主流資金分"] = mainstream.round(1)
    out["資金攻擊有效分"] = smart_effective.round(1)
    out["成交額等級"] = amount_grade
    out["流動性等級"] = liquidity
    out["流動性資料狀態"] = pd.Series("有效", index=out.index, dtype="object").where(liquidity_known, "缺失")
    out["流動性資料來源"] = source
    out["冷門股警示"] = warning
    out["主流股判定"] = mainstream_label
    out["資金攻擊有效性"] = effective
    out["主流資金角色"] = role
    out["主流資金說明"] = [
        f"主流資金{ms:.1f}｜有效{ef:.1f}｜成交額{am:.1f}百萬｜量{vol:.0f}張｜{lbl}｜{src}"
        for ms, ef, am, vol, lbl, src in zip(out["主流資金分"], out["資金攻擊有效分"], amount, volume, mainstream_label, source)
    ]
    out["主流資金引擎版本"] = MAINSTREAM_MONEY_VERSION
    return out



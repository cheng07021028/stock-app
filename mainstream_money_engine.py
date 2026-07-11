# -*- coding: utf-8 -*-
"""Phase 8.3 evidence-aware liquidity overlay.

The proven Phase 4.2 engine is preserved under ``_phase83_core``. This public
module only replaces the liquidity-data interpretation: missing data is
``資料待補`` rather than fake zero liquidity, and turnover is reconstructed
from price x volume when possible.
"""
from __future__ import annotations

from typing import Any
import pandas as pd
from _phase83_core import mainstream_money_engine_core as _core

MAINSTREAM_MONEY_VERSION = "phase4_3_liquidity_unknown_safe_20260712"
MAINSTREAM_MONEY_COLUMNS = list(dict.fromkeys(list(_core.MAINSTREAM_MONEY_COLUMNS) + ["流動性資料狀態", "流動性資料來源"]))
NUMERIC_MAINSTREAM_MONEY_COLUMNS = set(_core.NUMERIC_MAINSTREAM_MONEY_COLUMNS)


def apply_mainstream_money_engine(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=MAINSTREAM_MONEY_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        for col in MAINSTREAM_MONEY_COLUMNS:
            if col not in out.columns:
                out[col] = pd.Series(dtype="float64" if col in NUMERIC_MAINSTREAM_MONEY_COLUMNS else "object")
        return out

    amount = _core._num(out, ["成交額百萬", "今日成交額百萬", "成交金額百萬"], 0)
    avg_amount = _core._num(out, ["20日均成交額百萬", "二十日均成交額百萬", "均成交額百萬"], 0)
    volume = _core._num(out, ["最新成交量_張", "最新成交量張", "成交量_張", "成交量"], 0)
    avg_volume = _core._num(out, ["20日均量_張", "20日均量張", "均量_張"], 0)
    price = _core._num(out, ["最新價", "推薦價格", "推薦日價格", "收盤價"], 0)

    direct_amount = amount.gt(0)
    recovered = (price * volume / 1000.0).where(price.gt(0) & volume.gt(0), 0.0)
    recovered_avg = (price * avg_volume / 1000.0).where(price.gt(0) & avg_volume.gt(0), 0.0)
    amount = amount.where(amount.gt(0), recovered)
    avg_amount = avg_amount.where(avg_amount.gt(0), recovered_avg)
    known = amount.gt(0) | avg_amount.gt(0) | volume.gt(0) | avg_volume.gt(0)

    sector = _core._num(out, ["族群攻擊強度", "族群輪動分", "族群資金流分數", "類股熱度分數"], 50)
    money = _core._num(out, ["籌碼續航分", "主力點火分", "法人攻擊分", "法人籌碼分數"], 50)
    volume_factor = _core._num(out, ["人氣量能分", "量能啟動分", "量價動能分數"], 50)
    ret5 = _core._num(out, ["近5日漲幅%", "5日漲幅%"], 0)
    real_legal = _core._has_real_legal_flow(out)

    amount_s = _core._amount_score(amount, avg_amount)
    volume_s = _core._volume_score(volume, avg_volume)
    ratio_s = _core._ratio_score(volume, avg_volume, amount)
    legal_effective = money.where(real_legal, 50.0)
    smart = (legal_effective * .34 + volume_factor.clip(0, 100) * .26 + ratio_s * .18 + sector.clip(0, 100) * .22).clip(0, 100)
    mainstream = (
        amount_s * .38 + volume_s * .22 + smart * .20 + sector.clip(0, 100) * .14
        + (50 + ret5.clip(-5, 8) * 2).clip(35, 78) * .06
    ).clip(0, 100)
    mainstream = mainstream.where(known, 50.0)
    mainstream = mainstream.where(~known | amount.ge(50), mainstream.clip(upper=42))
    mainstream = mainstream.where(~known | amount.ge(100), mainstream.clip(upper=52))
    mainstream = mainstream.where(~known | amount.ge(250), mainstream.clip(upper=66))

    amount_grade = pd.Series("資料待補", index=out.index, dtype="object")
    amount_grade.loc[known] = "低成交額"
    amount_grade.loc[amount.ge(100)] = "可觀察成交額"
    amount_grade.loc[amount.ge(250)] = "中等成交額"
    amount_grade.loc[amount.ge(500)] = "高成交額"
    amount_grade.loc[amount.ge(1000)] = "主流成交額"
    amount_grade.loc[amount.ge(3000)] = "市場焦點成交額"

    liquidity = pd.Series("資料待補", index=out.index, dtype="object")
    liquidity.loc[known] = "低流動性"
    liquidity.loc[amount.ge(100) & (volume.ge(1000) | avg_volume.ge(1000))] = "可交易"
    liquidity.loc[amount.ge(500) | volume.ge(5000) | avg_volume.ge(5000)] = "高流動性"
    liquidity.loc[amount.ge(2000) | volume.ge(20000) | avg_volume.ge(20000)] = "主流高流動性"

    severe = known & (amount.lt(50) | (amount.lt(100) & volume.lt(1000) & avg_volume.lt(1000)))
    cold = known & (severe | (amount.lt(250) & volume.lt(2500) & avg_volume.lt(2500) & sector.lt(78)))
    warning = pd.Series("", index=out.index, dtype="object")
    warning.loc[~known] = "流動性資料缺失：禁止正式推薦，待補成交額/成交量後重評"
    warning.loc[cold] = "冷門股：成交額/成交量不足，禁止追高，只能觀察"
    warning.loc[severe] = "低流動性排除：成交額過低，容易滑價與假突破"

    label = pd.Series("流動性待補", index=out.index, dtype="object")
    label.loc[known] = "高流動性觀察股"
    label.loc[known & mainstream.ge(65)] = "主流輪動股"
    label.loc[known & mainstream.ge(76) & sector.ge(66)] = "主流攻擊股"
    label.loc[cold & ~severe] = "冷門潛伏股"
    label.loc[severe] = "冷門禁追股"

    effective = pd.Series("資料不足｜不作資金攻擊判定", index=out.index, dtype="object")
    effective.loc[known] = "中性｜缺法人實流資料時不加分"
    effective.loc[smart.ge(66) & amount.ge(250)] = "有效｜資金與成交額同步"
    effective.loc[smart.ge(72) & amount.ge(500)] = "強效｜主流資金攻擊"
    effective.loc[cold] = "無效｜低成交額放量不視為主流攻擊"
    effective.loc[real_legal & money.ge(60) & amount.ge(250)] = "有效｜法人/主力資料支持"

    role = pd.Series("流動性待補觀察", index=out.index, dtype="object")
    role.loc[known] = "高流動性觀察"
    role.loc[label.eq("主流攻擊股")] = "主流攻擊候選"
    role.loc[label.eq("主流輪動股")] = "主流突破追蹤"
    role.loc[label.eq("冷門潛伏股")] = "冷門潛伏觀察"
    role.loc[label.eq("冷門禁追股")] = "低流動性排除"

    source = pd.Series("缺少成交額/成交量", index=out.index, dtype="object")
    source.loc[direct_amount] = "歷史K線成交金額"
    source.loc[~direct_amount & amount.gt(0)] = "最新價×成交量回推"
    source.loc[~direct_amount & amount.le(0) & avg_amount.gt(0)] = "20日均成交額"

    out["成交額百萬"] = amount.round(1)
    out["20日均成交額百萬"] = avg_amount.round(1)
    out["主流資金分"] = mainstream.round(1)
    out["資金攻擊有效分"] = smart.round(1)
    out["成交額等級"] = amount_grade
    out["流動性等級"] = liquidity
    out["流動性資料狀態"] = pd.Series("有效", index=out.index, dtype="object").where(known, "缺失")
    out["流動性資料來源"] = source
    out["冷門股警示"] = warning
    out["主流股判定"] = label
    out["資金攻擊有效性"] = effective
    out["主流資金角色"] = role
    out["主流資金說明"] = [
        f"主流資金{ms:.1f}｜有效{ef:.1f}｜成交額{am:.1f}百萬｜量{vol:.0f}張｜{lbl}｜{src}"
        for ms, ef, am, vol, lbl, src in zip(mainstream, smart, amount, volume, label, source)
    ]
    out["主流資金引擎版本"] = MAINSTREAM_MONEY_VERSION
    return out


for _name in dir(_core):
    if not _name.startswith("_") and _name not in globals():
        globals()[_name] = getattr(_core, _name)

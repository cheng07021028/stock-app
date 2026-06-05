# -*- coding: utf-8 -*-
"""Phase 4 飆股 / 漲停獵人引擎。

整合大盤、族群輪動與籌碼代理，輸出隔日/盤中爆發候選欄位。
不連網、不寫檔；只補欄位給 7_股神推薦、匯出與紀錄使用。
"""
from __future__ import annotations

from typing import Any
import pandas as pd

from market_regime_engine import apply_market_regime_engine, MARKET_REGIME_COLUMNS
from sector_rotation_engine import apply_sector_rotation_engine, SECTOR_ROTATION_COLUMNS
from smart_money_engine import apply_smart_money_engine, SMART_MONEY_COLUMNS

LIMITUP_HUNTER_VERSION = "phase4_limitup_hunter_20260605"
LIMITUP_HUNTER_COLUMNS = [
    "飆股攻擊分", "隔日大漲機率分", "漲停獵人觀察", "飆股獵人角色", "盤中轉強觸發價", "追漲許可", "攻擊候選原因", "飆股引擎版本",
]
NUMERIC_LIMITUP_HUNTER_COLUMNS = {"飆股攻擊分", "隔日大漲機率分", "盤中轉強觸發價"}
PHASE4_COLUMNS = MARKET_REGIME_COLUMNS + SECTOR_ROTATION_COLUMNS + SMART_MONEY_COLUMNS + LIMITUP_HUNTER_COLUMNS
PHASE4_NUMERIC_COLUMNS = set(NUMERIC_LIMITUP_HUNTER_COLUMNS)


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


def _first_text(row: pd.Series, names: list[str]) -> str:
    for name in names:
        v = row.get(name, "")
        if not _blank(v):
            return str(v).strip()
    return ""


def _limit_price(price: pd.Series, market: pd.Series) -> pd.Series:
    p = pd.to_numeric(price, errors="coerce").fillna(0.0)
    # 台股多數股票漲停約 10%；處置/特殊股不在此簡化模型處理。
    return (p * 1.10).round(2)


def _trigger_price(out: pd.DataFrame) -> pd.Series:
    price = _num(out, ["最新價", "推薦價格", "推薦日價格"], 0)
    resistance = _num(out, ["突破確認價", "突破確認價_隔日", "近端壓力", "第一壓力價"], 0)
    high = _num(out, ["最高價", "近20日高點"], 0)
    trigger = resistance.where(resistance > 0, high.where(high > 0, price * 1.025))
    trigger = trigger.where(trigger > price * 0.99, price * 1.015)
    return trigger.round(2)


def apply_limitup_hunter_engine(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=PHASE4_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        for c in PHASE4_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="float64" if c in PHASE4_NUMERIC_COLUMNS else "object")
        return out

    out = apply_market_regime_engine(out)
    out = apply_sector_rotation_engine(out)
    out = apply_smart_money_engine(out)

    score = _num(out, ["推薦總分", "候選強度分", "股神實戰總分"], 50)
    entry = _num(out, ["Entry進場買點分", "進場買點分", "買進分數"], 50)
    risk = _num(out, ["Risk風控安全分", "風控安全分", "交易可行分數"], 50)
    ret5 = _num(out, ["近5日漲幅%", "5日漲幅%"], 0)
    ret20 = _num(out, ["近20日漲幅%", "20日漲幅%"], 0)
    chase = _num(out, ["追價風險分", "追高風險分數_決策"], 50)
    volume = _num(out, ["人氣量能分", "量能啟動分", "量價動能分數"], 50)
    sector_attack = _num(out, ["族群攻擊強度", "族群資金流分數", "類股熱度分數"], 50)
    sector_cont = _num(out, ["族群續航力"], 50)
    money = _num(out, ["籌碼續航分", "法人攻擊分", "主力點火分"], 50)
    market = _num(out, ["飆股適合度", "今日可追強度"], 50)
    leader = out.get("族群內領頭羊", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)
    catchup = out.get("族群內補漲股", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)

    early_window = ((ret5 >= -2) & (ret5 <= 9)).map({True: 76.0, False: 48.0}).astype(float)
    not_exhausted = (100 - (ret20 - 22).clip(lower=0) * 2.0).clip(25, 100)
    leader_bonus = leader.eq("是").astype(float) * 5 + catchup.eq("是").astype(float) * 4
    attack = (
        sector_attack * 0.22
        + money * 0.21
        + volume * 0.18
        + score * 0.13
        + market * 0.11
        + early_window * 0.08
        + not_exhausted * 0.07
        + leader_bonus
    ).clip(0, 100).round(1)
    next_big = (attack * 0.55 + entry * 0.15 + (100 - chase).clip(0, 100) * 0.12 + sector_cont * 0.10 + risk * 0.08).clip(0, 100).round(1)

    trigger = _trigger_price(out)
    role = []
    observe = []
    allow = []
    reason = []
    for idx in out.index:
        a = float(attack.loc[idx]); nb = float(next_big.loc[idx]); r5 = float(ret5.loc[idx]); ch = float(chase.loc[idx]); m = float(market.loc[idx]); sa = float(sector_attack.loc[idx]); sm = float(money.loc[idx])
        if a >= 82 and nb >= 74 and m >= 58 and sa >= 68 and sm >= 62 and ch < 78 and r5 < 13:
            rr = "S｜飆股攻擊候選"
            ob = "高爆發觀察｜等盤中量價確認"
            al = "允許盤中觸發後小量追強"
        elif a >= 72 and nb >= 66 and sa >= 62:
            rr = "B+｜盤中突破可追"
            ob = "突破觀察｜不可預先追高"
            al = "僅突破確認後試單"
        elif a >= 62:
            rr = "B｜等突破確認"
            ob = "觀察名單｜等族群續航與買點改善"
            al = "不追價"
        else:
            rr = "觀察"
            ob = "爆發條件不足"
            al = "不追價"
        role.append(rr)
        observe.append(ob)
        allow.append(al)
        reason.append(f"攻擊{a:.1f}｜隔日大漲{nb:.1f}｜族群{sa:.1f}｜籌碼{sm:.1f}｜大盤{m:.1f}")

    out["飆股攻擊分"] = attack
    out["隔日大漲機率分"] = next_big
    out["漲停獵人觀察"] = observe
    out["飆股獵人角色"] = role
    out["盤中轉強觸發價"] = trigger
    out["追漲許可"] = allow
    out["攻擊候選原因"] = reason
    out["飆股引擎版本"] = LIMITUP_HUNTER_VERSION
    return out

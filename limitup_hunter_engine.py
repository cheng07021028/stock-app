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

try:
    from market_leader_replay_engine import (
        MARKET_LEADER_REPLAY_COLUMNS,
        NUMERIC_MARKET_LEADER_REPLAY_COLUMNS,
        apply_market_leader_replay_engine,
    )
except Exception:
    MARKET_LEADER_REPLAY_COLUMNS = []
    NUMERIC_MARKET_LEADER_REPLAY_COLUMNS = set()
    apply_market_leader_replay_engine = None

try:
    from explosive_radar_engine import (
        EXPLOSIVE_RADAR_COLUMNS,
        NUMERIC_EXPLOSIVE_RADAR_COLUMNS,
        apply_explosive_radar_engine,
    )
except Exception:
    EXPLOSIVE_RADAR_COLUMNS = []
    NUMERIC_EXPLOSIVE_RADAR_COLUMNS = set()
    apply_explosive_radar_engine = None

try:
    from godpick_miss_replay_engine import (
        MISS_REPLAY_COLUMNS,
        NUMERIC_MISS_REPLAY_COLUMNS,
        apply_godpick_miss_replay_engine,
    )
except Exception:
    MISS_REPLAY_COLUMNS = []
    NUMERIC_MISS_REPLAY_COLUMNS = set()
    apply_godpick_miss_replay_engine = None

LIMITUP_HUNTER_VERSION = "phase6_2_miss_replay_bridge_20260613"
LIMITUP_HUNTER_COLUMNS = [
    "飆股攻擊分", "隔日大漲機率分", "漲停獵人觀察", "飆股獵人角色", "盤中轉強觸發價", "追漲許可", "攻擊候選原因", "飆股引擎版本",
]
NUMERIC_LIMITUP_HUNTER_COLUMNS = {"飆股攻擊分", "隔日大漲機率分", "盤中轉強觸發價"}
PHASE4_COLUMNS = MARKET_REGIME_COLUMNS + SECTOR_ROTATION_COLUMNS + SMART_MONEY_COLUMNS + list(MARKET_LEADER_REPLAY_COLUMNS or []) + LIMITUP_HUNTER_COLUMNS + list(EXPLOSIVE_RADAR_COLUMNS or []) + list(MISS_REPLAY_COLUMNS or [])
PHASE4_NUMERIC_COLUMNS = set(NUMERIC_LIMITUP_HUNTER_COLUMNS) | set(NUMERIC_MARKET_LEADER_REPLAY_COLUMNS or set()) | set(NUMERIC_EXPLOSIVE_RADAR_COLUMNS or set()) | set(NUMERIC_MISS_REPLAY_COLUMNS or set())


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
    if callable(apply_market_leader_replay_engine):
        try:
            out = apply_market_leader_replay_engine(out)
        except Exception as _leader_err:
            out["市場領漲檢討版本"] = f"phase6_leader_replay_failed:{_leader_err}"
            for _c in list(MARKET_LEADER_REPLAY_COLUMNS or []):
                if _c not in out.columns:
                    out[_c] = ""

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
    mainstream = _num(out, ["主流資金分"], 50)
    money_effective = _num(out, ["資金攻擊有效分"], 50)
    amount = _num(out, ["成交額百萬", "20日均成交額百萬"], 0)
    leader_replay = _num(out, ["主流領漲回補分", "市場領漲相似分"], 50).clip(0, 100)
    leader_theme = _num(out, ["漲停族群相似度"], 50).clip(0, 100)
    leader_replay_role = out.get("領漲回補角色", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)
    cold_warning = out.get("冷門股警示", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)
    mainstream_label = out.get("主流股判定", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)
    is_cold = cold_warning.str.strip().ne("") | mainstream_label.str.contains("冷門|低流動性", na=False)
    leader = out.get("族群內領頭羊", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)
    catchup = out.get("族群內補漲股", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)

    early_window = ((ret5 >= -2) & (ret5 <= 9)).map({True: 76.0, False: 48.0}).astype(float)
    not_exhausted = (100 - (ret20 - 22).clip(lower=0) * 2.0).clip(25, 100)
    leader_bonus = leader.eq("是").astype(float) * 5 + catchup.eq("是").astype(float) * 4
    attack = (
        sector_attack * 0.18
        + money * 0.12
        + money_effective * 0.09
        + mainstream * 0.14
        + volume * 0.12
        + score * 0.09
        + market * 0.07
        + early_window * 0.03
        + not_exhausted * 0.02
        + leader_replay * 0.10
        + leader_theme * 0.04
        + leader_bonus
    ).clip(0, 100)
    # Phase 6：若命中 06/12 型主流領漲結構，不能只因短線風控或舊 RR 直接壓低飆股候選。
    leader_boost_floor = (leader_replay * 0.58 + leader_theme * 0.18 + sector_attack * 0.14 + volume * 0.10).clip(0, 100)
    attack = attack.where(leader_replay < 76, attack.combine(leader_boost_floor, max))

    # Phase 4.2：冷門股爆量常是低基期錯覺，不可被均量比推成 S/B+。
    attack = attack.where(~is_cold, attack.clip(upper=66))
    attack = attack.where(amount >= 50, attack.clip(upper=58))
    attack = attack.where(amount >= 100, attack.clip(upper=64))
    attack = attack.round(1)
    next_big = (attack * 0.44 + entry * 0.10 + (100 - chase).clip(0, 100) * 0.08 + sector_cont * 0.07 + risk * 0.05 + mainstream * 0.10 + leader_replay * 0.16).clip(0, 100).round(1)

    trigger = _trigger_price(out)
    role = []
    observe = []
    allow = []
    reason = []
    for idx in out.index:
        a = float(attack.loc[idx]); nb = float(next_big.loc[idx]); r5 = float(ret5.loc[idx]); ch = float(chase.loc[idx]); m = float(market.loc[idx]); sa = float(sector_attack.loc[idx]); sm = float(money.loc[idx]); ms = float(mainstream.loc[idx]); ef = float(money_effective.loc[idx]); am = float(amount.loc[idx]); lr = float(leader_replay.loc[idx]); lt = float(leader_theme.loc[idx]); lrole = str(leader_replay_role.loc[idx])
        cold = bool(is_cold.loc[idx])
        if cold:
            rr = "冷門潛伏觀察" if am >= 50 else "低流動性排除"
            ob = "冷門隔離｜低成交額不視為主流攻擊"
            al = "不追價"
        elif (a >= 82 and nb >= 74 and m >= 58 and sa >= 68 and sm >= 60 and ms >= 70 and ef >= 58 and ch < 82 and r5 < 18) or (lr >= 86 and lt >= 76 and am >= 500 and sa >= 62):
            rr = "S｜飆股攻擊候選"
            ob = "主流高爆發觀察｜等盤中量價確認"
            al = "允許盤中觸發後小量追強"
        elif (a >= 72 and nb >= 66 and sa >= 62 and ms >= 58 and ef >= 52) or (lr >= 76 and lt >= 70 and am >= 200 and (sa >= 58 or '主流強勢回補' in lrole)):
            rr = "B+｜盤中突破可追"
            ob = "主流突破觀察｜不可預先追高"
            al = "僅突破確認後試單"
        elif a >= 62 and ms >= 50:
            rr = "B｜等突破確認"
            ob = "主流觀察名單｜等族群續航與買點改善"
            al = "不追價"
        else:
            rr = "觀察"
            ob = "爆發條件不足"
            al = "不追價"
        role.append(rr)
        observe.append(ob)
        allow.append(al)
        reason.append(f"攻擊{a:.1f}｜隔日大漲{nb:.1f}｜領漲回補{lr:.1f}｜漲停族群{lt:.1f}｜族群{sa:.1f}｜籌碼{sm:.1f}｜主流{ms:.1f}｜有效{ef:.1f}｜成交額{am:.1f}百萬｜大盤{m:.1f}")

    out["飆股攻擊分"] = attack
    out["隔日大漲機率分"] = next_big
    out["漲停獵人觀察"] = observe
    out["飆股獵人角色"] = role
    out["盤中轉強觸發價"] = trigger
    out["追漲許可"] = allow
    out["攻擊候選原因"] = reason
    out["飆股引擎版本"] = LIMITUP_HUNTER_VERSION

    # Phase 5：最後套用獨立飆股雷達雙引擎。
    # 注意這條路不取代穩健推薦，只把可能被 Risk/RR 早殺的爆發股保留下來分頁追蹤。
    if callable(apply_explosive_radar_engine):
        try:
            out = apply_explosive_radar_engine(out)
        except Exception as _radar_err:
            out["飆股雷達版本"] = f"phase5_radar_failed:{_radar_err}"
            for _c in list(EXPLOSIVE_RADAR_COLUMNS or []):
                if _c not in out.columns:
                    out[_c] = ""

    # Phase 6.2：集中補漲停/強勢股回放診斷，避免 7/8/12 各頁自行重算漏選原因。
    if callable(apply_godpick_miss_replay_engine):
        try:
            out = apply_godpick_miss_replay_engine(out)
        except Exception as _miss_err:
            out["回放校正版本"] = f"phase6_2_miss_replay_failed:{_miss_err}"
            for _c in list(MISS_REPLAY_COLUMNS or []):
                if _c not in out.columns:
                    out[_c] = ""
    return out

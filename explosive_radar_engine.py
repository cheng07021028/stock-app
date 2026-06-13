# -*- coding: utf-8 -*-
"""VNext Phase 5 飆股雷達雙引擎。

這個模組只做 DataFrame 欄位補強：
- 不連網、不讀寫 JSON、不覆蓋推薦紀錄。
- 與穩健推薦引擎分開，避免 Entry/Risk/RR 太早把可能爆發股刪掉。
- 風險高的股票不升級成主推薦，而是獨立標示成高風險爆發觀察。
"""
from __future__ import annotations

from typing import Any
import pandas as pd

EXPLOSIVE_RADAR_VERSION = "phase6_explosive_leader_replay_20260612"

EXPLOSIVE_RADAR_COLUMNS = [
    "爆發雷達分",
    "隔日爆發分",
    "局部題材火種分",
    "漏網回補分",
    "飆股雷達角色",
    "飆股雷達分區",
    "盤中點火條件",
    "飆股雷達原因",
    "飆股雷達風險",
    "雙引擎決策",
    "飆股雷達版本",
]

NUMERIC_EXPLOSIVE_RADAR_COLUMNS = {
    "爆發雷達分",
    "隔日爆發分",
    "局部題材火種分",
    "漏網回補分",
}

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


def _trigger_price(df: pd.DataFrame) -> pd.Series:
    price = _num(df, ["最新價", "推薦價格", "推薦日價格", "建議價位"], 0)
    breakout = _num(df, ["盤中轉強觸發價", "突破確認價", "突破確認價_隔日", "近端壓力", "第一壓力價", "推薦買點_突破"], 0)
    high = _num(df, ["近20日高點", "最高價"], 0)
    trigger = breakout.where(breakout > 0, high.where(high > 0, price * 1.025))
    trigger = trigger.where(trigger > price * 0.99, price * 1.015)
    return trigger.round(2)


def _safe_ratio(numerator: pd.Series, denominator: pd.Series, default: float = 1.0) -> pd.Series:
    n = pd.to_numeric(numerator, errors="coerce").fillna(0.0)
    d = pd.to_numeric(denominator, errors="coerce").fillna(0.0)
    out = pd.Series([default] * len(n), index=n.index, dtype="float64")
    mask = d.gt(0)
    out.loc[mask] = (n.loc[mask] / d.loc[mask]).clip(0, 8)
    return out


def apply_explosive_radar_engine(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=EXPLOSIVE_RADAR_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        for c in EXPLOSIVE_RADAR_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="float64" if c in NUMERIC_EXPLOSIVE_RADAR_COLUMNS else "object")
        return out

    base = _num(out, ["推薦總分", "候選強度分", "股神實戰總分", "股神決策分數"], 50).clip(0, 100)
    prelaunch = _num(out, ["起漲前兆分數", "飆股起漲分數", "突破準備分", "型態突破分數"], 50).clip(0, 100)
    pattern = _num(out, ["型態突破分數", "爆發力分數", "突破準備分"], 50).clip(0, 100)
    volume_score = _num(out, ["人氣量能分", "量能啟動分", "量價動能分數", "主力點火分"], 50).clip(0, 100)
    sector = _num(out, ["族群攻擊強度", "族群輪動分", "族群資金流分數", "類股熱度分數"], 50).clip(0, 100)
    sector_cont = _num(out, ["族群續航力", "同族群強勢比例", "族群流動性分數"], 50).clip(0, 100)
    money = _num(out, ["資金攻擊有效分", "籌碼續航分", "主力點火分", "法人攻擊分"], 50).clip(0, 100)
    mainstream = _num(out, ["主流資金分", "族群流動性分數", "成交額百萬"], 50).clip(0, 100)
    market = _num(out, ["飆股適合度", "今日可追強度", "市場環境分數", "大盤橋接分數"], 50).clip(0, 100)
    old_attack = _num(out, ["飆股攻擊分", "隔日大漲機率分"], 50).clip(0, 100)
    leader_replay = _num(out, ["主流領漲回補分", "市場領漲相似分"], 50).clip(0, 100)
    leader_theme = _num(out, ["漲停族群相似度"], 50).clip(0, 100)
    leader_role = _txt(out, "領漲回補角色")
    entry = _num(out, ["Entry進場買點分", "進場買點分", "隔日進場分數", "買進分數"], 50).clip(0, 100)
    risk = _num(out, ["Risk風控安全分", "風控安全分", "交易可行分數", "實戰品質分"], 50).clip(0, 100)
    chase = _num(out, ["追價風險分", "追高風險分數_決策"], 50).clip(0, 100)
    stop_dist = _num(out, ["停損距離%", "最大風險%"], 0).clip(0, 80)
    rr = _num(out, ["風險報酬比", "風險報酬比_決策"], 0).clip(0, 8)
    ret5 = _num(out, ["近5日漲幅%", "5日漲幅%", "RET5"], 0).clip(-30, 60)
    ret20 = _num(out, ["近20日漲幅%", "20日漲幅%", "RET20"], 0).clip(-50, 120)
    amount = _num(out, ["成交額百萬", "今日成交額百萬", "成交金額百萬", "20日均成交額百萬"], 0)
    vol = _num(out, ["最新成交量_張", "最新成交量張", "成交量_張", "成交量"], 0)
    avg_vol = _num(out, ["20日均量_張", "20日均量張", "均量_張", "20日均量"], 0)
    vol_ratio = _num(out, ["均量比", "量比"], float("nan"))
    vol_ratio = vol_ratio.where(vol_ratio.notna(), _safe_ratio(vol, avg_vol, 1.0)).clip(0, 8)

    leader_text = _txt(out, "族群內領頭羊") + "｜" + _txt(out, "類股前3強") + "｜" + _txt(out, "是否領先同類股")
    leader_bonus = leader_text.str.contains("是", na=False).astype(float) * 5.0
    main_text = _txt(out, "主流股判定") + "｜" + _txt(out, "主流資金角色")
    cold_text = _txt(out, "冷門股警示") + "｜" + main_text
    severe_cold = cold_text.str.contains("低流動性排除|冷門禁追", na=False) | (amount < 50)
    cold = severe_cold | cold_text.str.contains("冷門", na=False)

    local_fire = (
        sector * 0.34
        + sector_cont * 0.18
        + volume_score * 0.18
        + market * 0.12
        + (vol_ratio.clip(0, 3) / 3 * 100) * 0.10
        + (ret5.clip(-3, 8) + 3) / 11 * 100 * 0.08
    ).clip(0, 100)
    local_fire = local_fire.where(market >= 58, (local_fire * 0.88 + sector * 0.12).clip(0, 100))

    early_momentum = pd.Series([55.0] * len(out), index=out.index, dtype="float64")
    early_momentum = early_momentum.mask((ret5 >= -2) & (ret5 <= 8), 82)
    early_momentum = early_momentum.mask((ret5 > 8) & (ret5 <= 14), 68)
    early_momentum = early_momentum.mask(ret5 > 14, 45)
    early_momentum = early_momentum.mask(ret5 < -6, 42)
    exhaustion_guard = (100 - (ret20 - 28).clip(lower=0) * 1.35 - (chase - 78).clip(lower=0) * 1.1).clip(22, 100)

    radar = (
        old_attack * 0.12
        + prelaunch * 0.14
        + pattern * 0.11
        + volume_score * 0.11
        + sector * 0.13
        + money * 0.08
        + mainstream * 0.06
        + local_fire * 0.06
        + early_momentum * 0.02
        + leader_replay * 0.13
        + leader_theme * 0.04
        + leader_bonus
    ).clip(0, 100)

    rescue = (
        prelaunch * 0.18
        + pattern * 0.15
        + sector * 0.15
        + volume_score * 0.12
        + money * 0.08
        + base * 0.06
        + local_fire * 0.08
        + leader_replay * 0.14
        + leader_theme * 0.04
    ).clip(0, 100)

    # Phase 5：飆股雷達不被 Risk/RR 直接殺掉，但仍要揭露風險與限制低流動性假強。
    # Phase 6：若符合 6/12 類型的主流領漲族群，不因舊風控/Entry 提前消失；
    # 但低流動性仍保持上限，避免冷門股被包裝成主流飆股。
    leader_floor = (leader_replay * 0.58 + leader_theme * 0.18 + sector * 0.12 + volume_score * 0.08 + mainstream * 0.04).clip(0, 100)
    radar = radar.where(leader_replay < 76, radar.combine(leader_floor, max))
    radar = radar.where(~severe_cold, radar.clip(upper=52))
    radar = radar.where(~(cold & (amount < 100)), radar.clip(upper=60))
    next_explosion = (radar * 0.42 + rescue * 0.20 + old_attack * 0.10 + local_fire * 0.08 + exhaustion_guard * 0.06 + leader_replay * 0.14).clip(0, 100)

    trigger = _trigger_price(out)
    roles: list[str] = []
    buckets: list[str] = []
    triggers: list[str] = []
    reasons: list[str] = []
    risks: list[str] = []
    dual: list[str] = []

    for idx in out.index:
        rd = float(radar.loc[idx]); nx = float(next_explosion.loc[idx]); fire = float(local_fire.loc[idx]); rsq = float(rescue.loc[idx])
        sc = float(sector.loc[idx]); ms = float(mainstream.loc[idx]); mn = float(money.loc[idx]); am = float(amount.loc[idx]); vr = float(vol_ratio.loc[idx])
        lr = float(leader_replay.loc[idx]); lt = float(leader_theme.loc[idx]); lrole = str(leader_role.loc[idx])
        ch = float(chase.loc[idx]); sd = float(stop_dist.loc[idx]); rr_v = float(rr.loc[idx]); r5 = float(ret5.loc[idx]); r20 = float(ret20.loc[idx])
        is_severe_cold = bool(severe_cold.loc[idx]); is_cold = bool(cold.loc[idx])
        risk_parts: list[str] = []
        if ch >= 78:
            risk_parts.append(f"追價風險{ch:.1f}")
        if sd >= 14:
            risk_parts.append(f"停損距離{sd:.1f}%")
        if 0 < rr_v < 0.8:
            risk_parts.append(f"RR{rr_v:.2f}")
        if r5 >= 14 or r20 >= 38:
            risk_parts.append(f"短線漲幅偏高{r5:.1f}/{r20:.1f}%")
        if is_cold:
            risk_parts.append("低流動性/冷門隔離")
        high_risk = bool(risk_parts)

        if is_severe_cold and rd < 76:
            role = "X｜假強排除"
            bucket = "假強排除"
            action = "低流動性或假突破風險，不列飆股追蹤。"
        elif (rd >= 88 and nx >= 80 and fire >= 66 and sc >= 72 and ms >= 68 and am >= 500 and ch < 84) or (lr >= 90 and lt >= 82 and sc >= 66 and am >= 700 and ch < 88):
            role = "S+｜漲停雷達"
            bucket = "飆股雷達"
            action = "盤中放量站上觸發價，且族群同步攻擊時才小量試單。"
        elif (rd >= 80 and nx >= 72 and fire >= 62 and sc >= 66 and am >= 250 and ch < 86) or (lr >= 82 and lt >= 74 and sc >= 60 and am >= 250 and ch < 90):
            role = "S｜飆股攻擊候選"
            bucket = "飆股雷達"
            action = "不可開盤無腦買；等觸發價與量能確認。"
        elif (rd >= 70 and nx >= 64 and (sc >= 62 or fire >= 65) and am >= 100) or (lr >= 74 and lt >= 68 and am >= 120 and (sc >= 56 or "題材轉強" in lrole)):
            role = "B+｜盤中點火追蹤"
            bucket = "飆股雷達"
            action = "突破前不追，點火後只小量試單。"
        elif rd >= 62 or (rsq >= 70 and sc >= 65) or (lr >= 66 and lt >= 62 and am >= 80):
            role = "R｜高風險爆發觀察"
            bucket = "高風險爆發觀察"
            action = "可能有爆發，但風險未解；只觀察觸發，不預先買。"
        else:
            role = "X｜假強排除"
            bucket = "假強排除"
            action = "爆發條件不足或只是普通觀察。"

        roles.append(role)
        buckets.append(bucket)
        triggers.append(f"站上{float(trigger.loc[idx]):.2f}且量比>1.5、同族群續強；跌回觸發價或量縮立即取消。")
        reasons.append(f"雷達{rd:.1f}｜隔日{nx:.1f}｜火種{fire:.1f}｜回補{rsq:.1f}｜領漲回補{lr:.1f}｜漲停族群{lt:.1f}｜族群{sc:.1f}｜資金{mn:.1f}｜主流{ms:.1f}｜成交額{am:.1f}百萬｜量比{vr:.2f}")
        risks.append("、".join(risk_parts) if risk_parts else "未見重大雷達風險；仍需盤中確認")
        if role.startswith("X"):
            dual.append("穩健推薦與飆股雷達皆不通過")
        elif high_risk:
            dual.append("穩健推薦不通過，但飆股雷達保留追蹤")
        else:
            dual.append("飆股雷達通過；是否進場仍看盤中觸發")

    out["爆發雷達分"] = radar.round(1)
    out["隔日爆發分"] = next_explosion.round(1)
    out["局部題材火種分"] = local_fire.round(1)
    out["漏網回補分"] = rescue.round(1)
    out["飆股雷達角色"] = roles
    out["飆股雷達分區"] = buckets
    out["盤中點火條件"] = triggers
    out["飆股雷達原因"] = reasons
    out["飆股雷達風險"] = risks
    out["雙引擎決策"] = dual
    out["飆股雷達版本"] = EXPLOSIVE_RADAR_VERSION
    return out

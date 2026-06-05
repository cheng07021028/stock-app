# -*- coding: utf-8 -*-
"""Phase 4 籌碼 / 法人 / 主力代理引擎。

不重新抓資料；只利用既有官方因子、成交量、成交額、大戶/法人欄位做代理分數。
"""
from __future__ import annotations

from typing import Any
import pandas as pd

try:
    from mainstream_money_engine import MAINSTREAM_MONEY_COLUMNS, apply_mainstream_money_engine
except Exception:
    MAINSTREAM_MONEY_COLUMNS = []
    apply_mainstream_money_engine = None

SMART_MONEY_VERSION = "phase4_2_smart_money_mainstream_20260605"
SMART_MONEY_COLUMNS = [
    "法人攻擊分", "投信鎖碼分", "主力點火分", "大戶承接分", "籌碼續航分", "資金攻擊摘要", "籌碼引擎版本",
] + list(MAINSTREAM_MONEY_COLUMNS or [])
NUMERIC_SMART_MONEY_COLUMNS = {"法人攻擊分", "投信鎖碼分", "主力點火分", "大戶承接分", "籌碼續航分", "主流資金分", "資金攻擊有效分"}


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


def _signed_flow_score(s: pd.Series, scale: float = 1200.0) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce").fillna(0.0)
    return (50 + (s / scale).clip(-1.0, 1.0) * 30).clip(0, 100)


def apply_smart_money_engine(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=SMART_MONEY_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        for c in SMART_MONEY_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="float64" if c in NUMERIC_SMART_MONEY_COLUMNS else "object")
        return out

    foreign = _num(out, ["外資近1日買賣超", "外資買賣超", "外資買超"], 0)
    trust = _num(out, ["投信近1日買賣超", "投信買賣超", "投信買超"], 0)
    dealer = _num(out, ["自營商近1日買賣超", "自營商買賣超", "自營商買超"], 0)
    total = _num(out, ["三大法人近1日合計", "三大法人合計", "法人合計買賣超"], 0)
    ratio = _num(out, ["法人買超占量比%", "法人買超占成交量%"], 0)
    lock = _num(out, ["大戶鎖碼分數", "大戶鎖碼代理分數"], 50)
    vol = _num(out, ["人氣量能分", "量能啟動分", "量價動能分數"], 50)
    amount = _num(out, ["成交額百萬", "20日均成交額百萬"], 0)
    ret5 = _num(out, ["近5日漲幅%", "5日漲幅%"], 0)

    legal_attack = (_signed_flow_score(total, 1800) * 0.44 + _signed_flow_score(foreign, 1500) * 0.20 + _signed_flow_score(trust, 550) * 0.24 + (50 + ratio.clip(-20, 20) * 1.8).clip(0, 100) * 0.12).clip(0, 100)
    trust_lock = (_signed_flow_score(trust, 450) * 0.52 + lock.clip(0, 100) * 0.32 + (50 + ratio.clip(-20, 20) * 2.0).clip(0, 100) * 0.16).clip(0, 100)
    ignite = (vol.clip(0, 100) * 0.42 + (amount.rank(pct=True).fillna(0.5) * 100) * 0.20 + _signed_flow_score(total, 1800) * 0.22 + (50 + ret5.clip(-8, 8) * 3).clip(0, 100) * 0.16).clip(0, 100)
    big_holder = (lock.clip(0, 100) * 0.55 + (50 + ratio.clip(-20, 20) * 1.5).clip(0, 100) * 0.25 + (100 - ret5.clip(lower=0) * 2.5).clip(35, 100) * 0.20).clip(0, 100)
    continuation = (legal_attack * 0.30 + trust_lock * 0.22 + ignite * 0.25 + big_holder * 0.23).clip(0, 100)

    out["法人攻擊分"] = legal_attack.round(1)
    out["投信鎖碼分"] = trust_lock.round(1)
    out["主力點火分"] = ignite.round(1)
    out["大戶承接分"] = big_holder.round(1)
    out["籌碼續航分"] = continuation.round(1)
    out["資金攻擊摘要"] = [
        f"法人{la:.1f}｜投信{tl:.1f}｜主力{ig:.1f}｜大戶{bh:.1f}｜續航{co:.1f}"
        for la, tl, ig, bh, co in zip(out["法人攻擊分"], out["投信鎖碼分"], out["主力點火分"], out["大戶承接分"], out["籌碼續航分"])
    ]
    out["籌碼引擎版本"] = SMART_MONEY_VERSION

    # Phase 4.2：集中套用主流資金/冷門股濾網，避免 7_股神推薦、匯出與飆股獵人各算一套。
    if callable(apply_mainstream_money_engine):
        try:
            out = apply_mainstream_money_engine(out)
        except Exception as _mainstream_err:
            out["主流資金引擎版本"] = f"phase4_2_mainstream_failed:{_mainstream_err}"
    return out

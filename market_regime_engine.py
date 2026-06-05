# -*- coding: utf-8 -*-
"""Phase 4 大盤情境引擎。

用既有推薦表、macro bridge 欄位與候選股廣度推估「今天適不適合追飆股」。
不連網、不讀寫 JSON，避免每個頁面重複抓大盤資料。
"""
from __future__ import annotations

from typing import Any
import math
import pandas as pd

from godpick_runtime_cache import cache_key, get_or_compute

MARKET_REGIME_VERSION = "phase4_market_regime_20260605"
MARKET_REGIME_COLUMNS = [
    "大盤攻擊模式", "飆股適合度", "今日可追強度", "中小型股風險", "今日大盤結論", "大盤風險燈號", "大盤情境版本",
]
NUMERIC_MARKET_REGIME_COLUMNS = {"飆股適合度", "今日可追強度", "中小型股風險"}


def _blank(v: Any) -> bool:
    try:
        if v is None or pd.isna(v):
            return True
    except Exception:
        pass
    return str(v).strip().lower() in {"", "nan", "none", "null", "--", "-", "<na>"}


def _num_series(df: pd.DataFrame, names: list[str], default: float = 0.0) -> pd.Series:
    out = pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
    for name in names:
        if name not in df.columns:
            continue
        s = pd.to_numeric(df[name], errors="coerce")
        mask = out.isna() & s.notna()
        if mask.any():
            out.loc[mask] = s.loc[mask]
    return out.fillna(default).astype(float)


def _text_series(df: pd.DataFrame, names: list[str], default: str = "") -> pd.Series:
    out = pd.Series([default] * len(df), index=df.index, dtype="object")
    for name in names:
        if name not in df.columns:
            continue
        s = df[name].fillna("").astype(str).str.strip()
        mask = out.map(_blank) & s.map(lambda x: not _blank(x))
        if mask.any():
            out.loc[mask] = s.loc[mask]
    return out


def _score_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=MARKET_REGIME_COLUMNS)
    work = df.copy()
    ret5 = _num_series(work, ["近5日漲幅%", "5日漲幅%"], 0)
    ret20 = _num_series(work, ["近20日漲幅%", "20日漲幅%"], 0)
    amount = _num_series(work, ["成交額百萬", "20日均成交額百萬"], 0)
    vol_score = _num_series(work, ["人氣量能分", "量能啟動分", "量價動能分數"], 50)
    score = _num_series(work, ["推薦總分", "股神實戰總分", "候選強度分"], 50)
    macro_score = _num_series(work, ["大盤橋接分數", "大盤可參考分數", "市場環境分數"], 55)
    macro_text = _text_series(work, ["大盤橋接狀態", "大盤策略模式", "市場環境", "大盤趨勢模式"], "中性")

    n = max(1, len(work))
    strong_ratio = float(((ret5 >= 3.0) | (score >= 85)).sum()) / n
    weak_ratio = float(((ret5 <= -3.0) | (score < 55)).sum()) / n
    amount_med = float(amount[amount > 0].median()) if (amount > 0).any() else 0.0
    volume_avg = float(vol_score.mean()) if len(vol_score) else 50.0
    macro_avg = float(macro_score.mean()) if len(macro_score) else 55.0
    ret5_avg = float(ret5.mean()) if len(ret5) else 0.0
    ret20_avg = float(ret20.mean()) if len(ret20) else 0.0

    suitability = 50 + strong_ratio * 30 - weak_ratio * 25 + (volume_avg - 50) * 0.25 + (macro_avg - 50) * 0.35 + max(-8, min(8, ret5_avg * 1.2))
    chase_strength = suitability + max(0, ret5_avg) * 1.5 - max(0, ret20_avg - 18) * 1.1
    small_cap_risk = 45 + weak_ratio * 30 + max(0, ret20_avg - 18) * 0.9 - strong_ratio * 10
    if amount_med < 40:
        small_cap_risk += 8
    suitability = round(max(0, min(100, suitability)), 1)
    chase_strength = round(max(0, min(100, chase_strength)), 1)
    small_cap_risk = round(max(0, min(100, small_cap_risk)), 1)

    joined_macro = "｜".join(macro_text.astype(str).head(80).tolist())
    if suitability >= 72 and chase_strength >= 70 and small_cap_risk <= 62:
        mode = "題材攻擊盤"
        conclusion = "可找強勢族群與盤中突破股，但仍需量能確認。"
        light = "綠燈｜可進攻"
    elif suitability >= 60:
        mode = "輪動選股盤"
        conclusion = "可做族群輪動與補漲，不適合盲目追高。"
        light = "黃綠燈｜精選進攻"
    elif "偏空" in joined_macro or weak_ratio > strong_ratio * 1.25:
        mode = "防守盤"
        conclusion = "不適合擴大倉位；以等突破與風控為主。"
        light = "紅燈｜防守"
    else:
        mode = "震盪盤"
        conclusion = "大盤未明確攻擊；只接受高勝率買點或盤中確認。"
        light = "黃燈｜等待確認"

    out = pd.DataFrame(index=work.index)
    out["大盤攻擊模式"] = mode
    out["飆股適合度"] = suitability
    out["今日可追強度"] = chase_strength
    out["中小型股風險"] = small_cap_risk
    out["今日大盤結論"] = conclusion
    out["大盤風險燈號"] = light
    out["大盤情境版本"] = MARKET_REGIME_VERSION
    return out


def derive_market_regime(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=MARKET_REGIME_COLUMNS)
    key = cache_key("market_regime", df)
    return get_or_compute(key, lambda: _score_frame(df))


def apply_market_regime_engine(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=MARKET_REGIME_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        for c in MARKET_REGIME_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="float64" if c in NUMERIC_MARKET_REGIME_COLUMNS else "object")
        return out
    ctx = derive_market_regime(out).reindex(out.index)
    for c in MARKET_REGIME_COLUMNS:
        if c not in out.columns or out[c].map(_blank).all():
            out[c] = ctx[c]
        else:
            mask = out[c].map(_blank)
            if mask.any():
                out.loc[mask, c] = ctx.loc[mask, c]
    return out

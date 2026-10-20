# -*- coding: utf-8 -*-
"""V191-H130: evidence-gated, read-only six-engine trading research.

No transaction placement, no optimistic default fills, no network side effects,
no writes to the production recommendation/portfolio authority.
"""
from __future__ import annotations

from datetime import date
from typing import Any
import math
import re
from urllib.parse import urlparse

import numpy as np
import pandas as pd

VERSION = "V191-H130.0"


def _num(x: Any) -> float | None:
    try:
        if isinstance(x, bool) or pd.isna(x):
            return None
        n = float(str(x).replace(",", "").replace("%", ""))
        return n if math.isfinite(n) else None
    except (TypeError, ValueError):
        return None


def _text(x: Any) -> str:
    return "" if x is None or (not isinstance(x, (list, dict)) and pd.isna(x)) else str(x).strip()


def _date(x: Any):
    """Reject ambiguous numeric dates instead of silently turning them into 1970."""
    try:
        if isinstance(x, (int, float, np.integer, np.floating)):
            raw = str(int(x))
            if len(raw) == 8 and raw.isdigit():
                ts = pd.to_datetime(raw, format="%Y%m%d", errors="coerce")
            elif 30000 <= float(x) <= 90000:  # Excel serial, explicit conversion
                ts = pd.to_datetime(x, unit="D", origin="1899-12-30", errors="coerce")
            else:
                return None
        else:
            ts = pd.to_datetime(x, errors="coerce")
        if pd.isna(ts):
            return None
        day = ts.date()
        return day if date(2000, 1, 1) <= day <= date(2100, 12, 31) else None
    except Exception:
        return None


def _pick(df: pd.DataFrame, *names: str) -> str | None:
    return next((name for name in names if name in df.columns), None)


def _code(v: Any) -> str:
    s = _text(v)
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s if re.fullmatch(r"[0-9]{4,6}", s) else ""


def _empty(status: str, message: str) -> pd.DataFrame:
    return pd.DataFrame([{"資料狀態": status, "說明": message}])


def normalize_prices(source: pd.DataFrame | None, *, asof: date | None = None) -> pd.DataFrame:
    """Canonical OHLCV. Invalid data is removed, never forward-filled."""
    if not isinstance(source, pd.DataFrame) or source.empty:
        return pd.DataFrame()
    d, c, o = (_pick(source, "日期", "date", "Date", "datetime"),
               _pick(source, "收盤價", "收盤", "close", "Close", "Adj Close"),
               _pick(source, "開盤價", "開盤", "open", "Open"))
    if not d or not c:
        return pd.DataFrame()
    h = _pick(source, "最高價", "最高", "high", "High")
    l = _pick(source, "最低價", "最低", "low", "Low")
    v = _pick(source, "成交量", "volume", "Volume")
    code = _pick(source, "股票代號", "代號", "code", "Code")
    output = pd.DataFrame(index=source.index)
    output["日期"] = pd.to_datetime(source[d], errors="coerce").dt.tz_localize(None).dt.normalize()
    for target, col in (("收盤價", c), ("開盤價", o), ("最高價", h), ("最低價", l), ("成交量", v)):
        output[target] = pd.to_numeric(source[col].astype(str).str.replace(",", "", regex=False), errors="coerce") if col else np.nan
    output["股票代號"] = source[code].map(_code) if code else ""
    output = output.dropna(subset=["日期", "收盤價"])
    output = output[output["收盤價"] > 0]
    if asof is not None:
        output = output[output["日期"].dt.date <= asof]
    # For OHLC no partial invalid OHLC can be used in next-open backtest.
    output.loc[output["開盤價"] <= 0, "開盤價"] = np.nan
    output.loc[output["最高價"] < output["最低價"], ["最高價", "最低價"]] = np.nan
    output = output.sort_values(["股票代號", "日期"], kind="stable").drop_duplicates(["股票代號", "日期"], keep="last")
    return output.reset_index(drop=True)


def trade_ideas(candidates: pd.DataFrame | None, *, asof: date | None = None, limit: int = 5) -> pd.DataFrame:
    """Rank evidence, never convert research into buy permission or invent prices."""
    if not isinstance(candidates, pd.DataFrame) or candidates.empty:
        return _empty("NO_INPUT", "無候選資料；請使用現行掃描或上傳本期研究名單。")
    code_col = _pick(candidates, "股票代號", "代號", "code")
    if not code_col:
        return _empty("INVALID_SCHEMA", "候選檔缺少股票代號。")
    rows = []
    for rec in candidates.to_dict("records"):
        code = _code(rec.get(code_col))
        if not code:
            continue
        layer = _text(rec.get("H126權威層別"))
        veto = _text(rec.get("H109決策權威"))
        excluded = (_text(rec.get("H105今日強勢排除")) == "是" or
                    layer.startswith("禁止") or
                    any(tag in veto for tag in ("VETO", "QUARANTINE", "SHOCK")))
        verified = (layer.startswith("A｜") and _text(rec.get("H126認證資格")) == "PASS")
        gap = _text(rec.get("H126缺失與處置")) or _text(rec.get("H113缺失原因"))
        cutoff = _date(rec.get("H126研究資訊截止日") or rec.get("H110研究資訊截止日"))
        stale = asof is None or cutoff is None or cutoff != asof
        rank = _num(rec.get("H126最後研究分"))
        grade = "隔離觀察" if excluded else "資料過期／未驗證" if stale else "A級認證研究（非買進）" if verified else "待驗證研究（非買進）"
        rows.append({
            "股票代號": code,
            "股票名稱": _text(rec.get("股票名稱")),
            "產業": _text(rec.get("類別")),
            "研究層別": grade,
            "研究分數_僅排序": rank,
            "研究截止日": str(cutoff) if cutoff else "UNKNOWN",
            "官方法人": _text(rec.get("H123法人實證狀態")) or "未驗證",
            "大戶持股": _text(rec.get("H124大戶實證狀態")) or "未驗證",
            "進場參考價": "未授權：需現行行情與原交易閘門",
            "目標／停損": "未授權：需當日交易計畫",
            "風險報酬比": "無有效進出場資料，無法計算",
            "缺口／否決": "硬風險否決" if excluded else "資料截止日不符" if stale else gap or "無完整正式交易授權",
        })
    if not rows:
        return _empty("NO_VALID_CODES", "找不到有效代號，不建立交易機會。")
    results = pd.DataFrame(rows).drop_duplicates("股票代號", keep="first")
    priority = {"A級認證研究（非買進）": 0, "待驗證研究（非買進）": 1,
                "資料過期／未驗證": 2, "隔離觀察": 3}
    results["_p"] = results["研究層別"].map(priority).fillna(4)
    return results.sort_values(["_p", "研究分數_僅排序"], ascending=[True, False], na_position="last").drop(columns="_p").head(limit).reset_index(drop=True)


def technical_analysis(source: pd.DataFrame | None, *, asof: date | None = None) -> pd.DataFrame:
    frame = normalize_prices(source, asof=asof)
    if len(frame) == 0:
        return _empty("NO_OHLCV", "沒有有效的日期及收盤價，無法分析技術面。")
    code = frame["股票代號"].unique()
    if len(code) != 1:
        return _empty("MULTI_TICKER", "技術分析請選擇單一代號，以免不同股票 K 線混合。")
    if len(frame) < 60:
        return _empty("INSUFFICIENT", f"日線僅 {len(frame)} 筆，至少需要 60 個交易日，不產生結論。")
    close = frame["收盤價"]
    ma5 = close.rolling(5).mean().iloc[-1]
    ma20 = close.rolling(20).mean().iloc[-1]
    ma60 = close.rolling(60).mean().iloc[-1]
    delta = close.diff()
    up = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    down = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    rsi = float(100 * up.iloc[-1] / (up.iloc[-1] + down.iloc[-1])) if (up.iloc[-1] + down.iloc[-1]) > 0 else 50.0
    highs = frame["最高價"].fillna(close)
    lows = frame["最低價"].fillna(close)
    previous = close.shift(1)
    tr = pd.concat([highs - lows, (highs - previous).abs(), (lows - previous).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14).mean().iloc[-1]
    strength = "多頭結構" if close.iloc[-1] > ma20 > ma60 else "弱勢結構" if close.iloc[-1] < ma20 < ma60 else "震盪／待確認"
    return pd.DataFrame([{
        "股票代號": code[0], "資料交易日": str(frame["日期"].max().date()),
        "收盤價": round(float(close.iloc[-1]), 3), "MA5": round(float(ma5), 3),
        "MA20": round(float(ma20), 3), "MA60": round(float(ma60), 3),
        "20日支撐參考": round(float(lows.iloc[-20:].min()), 3),
        "20日壓力參考": round(float(highs.iloc[-20:].max()), 3),
        "RSI14": round(rsi, 2), "ATR14": round(float(atr), 3),
        "趨勢解讀": strength, "研究判斷": "觀察／不構成買進授權",
        "資料狀態": "STALE_RECHECK" if asof is None or frame["日期"].max().date() != asof else "ASOF_MATCHED",
        "解讀限制": "支撐壓力為近20日統計參考；不能預測必然成交或突破",
    }])


def news_impacts(events: pd.DataFrame | None, *, asof: date | None = None) -> pd.DataFrame:
    if not isinstance(events, pd.DataFrame) or events.empty:
        return _empty("NO_SOURCE", "無已提供的新聞事件；不自動編造新聞或目標價。")
    d = _pick(events, "日期", "發布日期", "published_at")
    headline = _pick(events, "標題", "headline", "title")
    url = _pick(events, "來源網址", "網址", "source_url", "url")
    if not (d and headline and url):
        return _empty("INVALID_SCHEMA", "事件須有發布日期、標題、來源網址，不能以無來源文字作交易結論。")
    rows = []
    for item in events.to_dict("records"):
        published = _date(item.get(d))
        href = _text(item.get(url))
        parsed = urlparse(href)
        valid = published is not None and parsed.scheme in ("https", "http") and bool(parsed.netloc) and bool(_text(item.get(headline)))
        future = asof is not None and published is not None and published > asof
        age = ((asof - published).days if asof and published else None)
        status = "FUTURE_LEAKAGE" if future else "SOURCE_INVALID" if not valid else "OLD_EVENT_RECHECK" if age is not None and age > 30 else "可人工核對來源"
        rows.append({"股票代號／產業": _text(item.get("股票代號") or item.get("產業")),
                     "發布日": str(published) if published else "UNKNOWN", "事件": _text(item.get(headline)),
                     "來源網址": href, "證據狀態": status,
                     "短期情境": "無效資料，不判斷" if status in ("FUTURE_LEAKAGE", "SOURCE_INVALID") else "核實事件意外程度、盤前價量及實際公告",
                     "長期情境": "需財報／營收及訂單實證，不推測必然影響",
                     "市場已反映／未反映": "缺公告前後價格、預期差與成交量，無法判定",
                     "參考價格波動區間": "需有效OHLCV及事件研究，不提供臆測區間",
                     "失效條件": "消息更正、來源未核實或與原始財報／公告衝突"})
    return pd.DataFrame(rows)


def backtest_sma(source: pd.DataFrame | None, *, asof: date | None = None,
                 adjusted_confirmed: bool = False, fee_rate: float = 0.001425,
                 sell_tax: float = 0.003, slippage: float = 0.001,
                 hold_days: int = 5, min_test_trades: int = 5) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Point-in-time SMA20/60 crossing; signal at close t; fill at OPEN t+1.

    Predefined 5-session exit, no overlapping positions; first 70% training,
    last 30% held-out. Exit at next open, after hold_days open-to-open sessions.
    """
    frame = normalize_prices(source, asof=asof)
    if not adjusted_confirmed:
        return _empty("PRICE_ADJUSTMENT_UNCONFIRMED", "先確認除權息及拆併股已調整，否則禁止計算勝率。"), pd.DataFrame()
    if frame.empty or len(frame) < 150 or frame["股票代號"].nunique() > 1:
        return _empty("INSUFFICIENT", "至少150根單一標的日K，包含開盤與收盤價，才能執行樣本外回測。"), pd.DataFrame()
    if frame["開盤價"].isna().any() or (frame["開盤價"] <= 0).any():
        return _empty("MISSING_OPEN", "缺少實際開盤價，不能假定前一日收盤一定成交。"), pd.DataFrame()
    if not (0 <= fee_rate <= 0.02 and 0 <= sell_tax <= 0.02 and 0 <= slippage <= 0.03 and 1 <= hold_days <= 30):
        return _empty("INVALID_COST", "手續費、證交稅、滑價或持有天數不合理。"), pd.DataFrame()
    # Large discontinuities can mean unresolved splits or corrupt candles.
    jumps = frame["收盤價"].pct_change().abs()
    if (jumps > 0.45).any():
        return _empty("CORPORATE_ACTION_RECHECK", "存在單日超過45%價格跳變，須重新核對還原行情。"), pd.DataFrame()
    close = frame["收盤價"]
    fast, slow = close.rolling(20).mean(), close.rolling(60).mean()
    # Entry on cross confirmed at t; next open fill; prevents lookahead.
    cross = (fast > slow) & (fast.shift(1) <= slow.shift(1))
    test_start = int(len(frame) * 0.7)
    opens = frame["開盤價"].to_numpy(dtype=float)
    positions = []
    last_exit = -1
    for signal_ix in np.flatnonzero(cross.fillna(False).to_numpy()):
        entry = int(signal_ix) + 1
        exit_ix = entry + hold_days
        if entry <= last_exit or exit_ix >= len(frame):
            continue
        buy = opens[entry] * (1 + fee_rate + slippage)
        sell = opens[exit_ix] * (1 - fee_rate - sell_tax - slippage)
        if buy <= 0 or sell <= 0:
            continue
        ret = sell / buy - 1
        positions.append({"股票代號": frame["股票代號"].iloc[0],
                          "訊號日期": str(frame["日期"].iloc[signal_ix].date()),
                          "進場日期": str(frame["日期"].iloc[entry].date()),
                          "出場日期": str(frame["日期"].iloc[exit_ix].date()),
                          "進場開盤價": round(opens[entry], 4), "出場開盤價": round(opens[exit_ix], 4),
                          "扣成本報酬率": ret, "樣本類別": "TEST" if entry >= test_start else "TRAIN"})
        last_exit = exit_ix
    trades = pd.DataFrame(positions)
    if trades.empty:
        return _empty("NO_SIGNALS", "樣本期間無 SMA20 上穿 SMA60 訊號，不產生勝率。"), trades
    test = trades[trades["樣本類別"] == "TEST"]
    if len(test) < min_test_trades:
        return _empty("TEST_SAMPLE_SMALL", f"樣本外只有 {len(test)} 筆交易（最低 {min_test_trades}），不宣稱策略有效。"), trades
    returns = test["扣成本報酬率"]
    wins = float(returns[returns > 0].sum())
    losses = abs(float(returns[returns < 0].sum()))
    pf = wins / losses if losses > 0 else np.nan
    curve = (1 + returns).cumprod()
    drawdown = (curve / np.maximum(1, curve.cummax()) - 1).min()
    summary = pd.DataFrame([{
        "策略": "SMA20上穿SMA60，次日開盤進場，持有固定交易日，非重疊",
        "回測截止日": str(frame["日期"].iloc[-1].date()), "訓練占比": "70%", "樣本外占比": "30%",
        "訓練交易筆數": int((trades["樣本類別"] == "TRAIN").sum()),
        "樣本外交易筆數": len(test), "樣本外勝率": round(float((returns > 0).mean()), 4),
        "樣本外獲利因子": round(pf, 4) if math.isfinite(pf) else "無虧損交易，不能有限估計",
        "樣本外最大交易權益回撤": round(float(drawdown), 4),
        "樣本外平均扣成本報酬": round(float(returns.mean()), 4),
        "樣本外累積扣成本報酬": round(float(curve.iloc[-1] - 1), 4),
        "每邊手續費": fee_rate, "證交稅(賣出)": sell_tax, "每邊滑價": slippage,
        "統計聲明": "回測為歷史非重疊交易權益曲線，未計閒置現金收益／資金占用；樣本外非未來保證"
    }])
    return summary, trades


def portfolio_risk(holdings: pd.DataFrame | None, prices: pd.DataFrame | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not isinstance(holdings, pd.DataFrame) or holdings.empty:
        return _empty("NO_HOLDINGS", "未提供實際持股比例，不能推算個人曝險。"), pd.DataFrame()
    code_col = _pick(holdings, "股票代號", "代號", "code")
    weight_col = _pick(holdings, "權重%", "配置比例%", "權重", "weight_pct")
    if not (code_col and weight_col):
        return _empty("INVALID_SCHEMA", "持股必須包含股票代號及權重%（例如 25 表示 25%）。"), pd.DataFrame()
    clean = holdings.copy()
    clean["_code"] = clean[code_col].map(_code)
    clean["_weight"] = clean[weight_col].map(_num)
    if clean["_code"].eq("").any() or clean["_weight"].isna().any() or (clean["_weight"] < 0).any() or clean["_code"].duplicated().any():
        return _empty("INVALID_HOLDINGS", "代號無效、重複或權重缺漏／負數；請先修正。"), pd.DataFrame()
    total = float(clean["_weight"].sum())
    if total <= 0 or total > 100.0001:
        return _empty("INVALID_TOTAL", f"持股合計 {total:.2f}%，應落在0～100%（其餘視為現金）。"), pd.DataFrame()
    sector_col = _pick(clean, "產業", "類別", "sector")
    sector = clean.groupby(sector_col, dropna=False)["_weight"].sum().sort_values(ascending=False) if sector_col else pd.Series(dtype=float)
    nlargest = clean.nlargest(3, "_weight")
    metrics = [
        ("已知股票曝險%", round(total, 3)),
        ("推定現金%（不含其他資產）", round(100-total, 3)),
        ("最大單股權重%", round(float(clean["_weight"].max()), 3)),
        ("前三大集中度%", round(float(nlargest["_weight"].sum()), 3)),
        ("最大產業比重%", round(float(sector.iloc[0]), 3) if len(sector) else "缺產業分類"),
        ("所有股票同步下跌20%簡易壓力損失%", round(-0.20 * total, 3)),
        ("壓力測試限制", "固定等比例情境，不是VaR、不考慮槓桿／跳空／個股差異，非保證避險")]
    corr = pd.DataFrame()
    p = normalize_prices(prices)
    if not p.empty and p["股票代號"].nunique() >= 2:
        wide = p.pivot(index="日期", columns="股票代號", values="收盤價").sort_index()
        returns = wide.pct_change(fill_method=None)
        pairs = []
        owned = set(clean["_code"])
        active = sorted(owned & set(returns.columns))
        for i, a in enumerate(active):
            for b in active[i+1:]:
                valid = returns[[a,b]].dropna()
                if len(valid) >= 60 and valid[a].std() > 0 and valid[b].std() > 0:
                    rho = float(valid[a].corr(valid[b]))
                    pairs.append({"標的一": a, "標的二": b, "共同交易日數": len(valid), "日報酬相關係數": round(rho, 4),
                                  "提醒": "高正相關，留意同向下跌" if rho >= 0.75 else "參考值，非未來保證"})
        corr = pd.DataFrame(pairs)
    if corr.empty:
        metrics.append(("相關性資料", "未滿足兩檔共同有效60交易日，禁止推測隱性相關係數"))
    return pd.DataFrame(metrics, columns=["項目", "結果"]), corr


def journal_review(journal: pd.DataFrame | None, *, max_rows: int = 20) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not isinstance(journal, pd.DataFrame) or journal.empty:
        return _empty("NO_TRADES", "缺少實際交易紀錄，不得拿系統研究清單冒充已成交交易。"), pd.DataFrame()
    required = {"股票代號", "進場日", "出場日", "進場價", "出場價", "股數"}
    if not required.issubset(journal.columns):
        return _empty("INVALID_SCHEMA", "需要股票代號、進場日、出場日、進場價、出場價、股數。"), pd.DataFrame()
    if "方向" in journal.columns and journal["方向"].astype(str).str.contains("空|short", case=False, regex=True).any():
        return _empty("SHORT_NOT_SUPPORTED", "此交易日誌僅支援多單；放空交易需要獨立計算成本與盈虧。"), pd.DataFrame()
    t = journal.copy()
    t["_entry_date"] = pd.to_datetime(t["進場日"], errors="coerce")
    t["_exit_date"] = pd.to_datetime(t["出場日"], errors="coerce")
    for col in ("進場價", "出場價", "股數"):
        t[col] = t[col].map(_num)
    t = t.dropna(subset=["_entry_date", "_exit_date", "進場價", "出場價", "股數"])
    t = t[(t["進場價"] > 0) & (t["出場價"] > 0) & (t["股數"] > 0) & (t["_exit_date"] >= t["_entry_date"])]
    if t.empty:
        return _empty("NO_VALID_TRADES", "沒有有效的完整進出場紀錄。"), pd.DataFrame()
    t = t.sort_values("_exit_date").tail(max_rows).copy()
    # Never fabricate commissions; if absent, show gross return only.
    fees_known = "手續費" in t.columns and "交易稅" in t.columns
    if fees_known:
        t["_fees"] = t["手續費"].map(_num)
        t["_tax"] = t["交易稅"].map(_num)
        fees_known = t[["_fees", "_tax"]].notna().all().all() and (t[["_fees", "_tax"]] >= 0).all().all()
    gross = (t["出場價"] / t["進場價"] - 1)
    if fees_known:
        t["已知淨損益"] = (t["出場價"] - t["進場價"]) * t["股數"] - t["_fees"] - t["_tax"]
        t["單筆報酬"] = t["已知淨損益"] / (t["進場價"] * t["股數"])
    else:
        t["已知淨損益"] = np.nan
        t["單筆報酬"] = gross
    t["交易結果"] = np.where(t["單筆報酬"] > 0, "獲利", "未獲利")
    t["持有日數(日曆)"] = (t["_exit_date"] - t["_entry_date"]).dt.days
    # All claims are about the provided journal, not an inferred psychological diagnosis.
    winrate = float((t["單筆報酬"] > 0).mean())
    losing = int((t["單筆報酬"] <= 0).sum())
    rules = [
        f"紀錄樣本 {len(t)} 筆、勝率 {winrate:.1%}，其中 {losing} 筆未獲利；未滿20筆時僅作初步觀察。",
        "規則1：每筆進場前寫明失效條件與停損，結束後比對有無遵守。",
        "規則2：逐筆保留成交價、股數、手續費及證交稅，缺成本不計淨報酬。",
        "規則3：同一策略至少累積20筆真實完整交易後，再做分策略檢討；不因單筆輸贏改權重。",
    ]
    if "策略" in t.columns:
        counts = t.groupby("策略")["單筆報酬"].agg(["count", "mean"])
        weakest = counts[counts["count"] >= 3].sort_values("mean")
        if not weakest.empty:
            rules.append(f"證據：『{weakest.index[0]}』在本樣本至少3筆，平均單筆報酬偏弱，先稽核策略執行而非直接停用。")
    summary = pd.DataFrame({"交易日誌稽核／守則": rules})
    display = t.drop(columns=[c for c in t.columns if c.startswith("_")], errors="ignore")
    display["報酬型態"] = "已扣輸入成本" if fees_known else "毛報酬（未輸入完整成本）"
    return summary, display.reset_index(drop=True)


def make_template(kind: str) -> str:
    schemas = {
        "行情": "股票代號,日期,開盤價,最高價,最低價,收盤價,成交量\n2330,2026-01-05,0,0,0,0,0\n",
        "新聞": "股票代號,日期,標題,來源網址\n2330,2026-01-05,公告內容,https://example.com/source\n",
        "持股": "股票代號,產業,權重%\n2330,半導體,25\n",
        "交易": "股票代號,進場日,出場日,進場價,出場價,股數,手續費,交易稅,策略\n2330,2026-01-05,2026-01-08,100,101,1000,200,300,SMA策略\n",
    }
    return schemas[kind]

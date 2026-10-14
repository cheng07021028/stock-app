# -*- coding: utf-8 -*-
"""V191-H114.1/2: point-in-time multi-strategy research discovery.

Never grants trading permission.  Numerical strategy scores are *heuristic
research rankings*, NOT return forecasts or calibrated probabilities.  Only
verifiable, dated observations may activate a signal.  Historical series must
be provided by the caller; this module NEVER downloads current data in replay.
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping
import json
import math
import pandas as pd

VERSION = "v191_h114_parallel_growth_value_volume_engulfing_20261014"
MIN_LOTS = 1000.0  # research / certified screen; H113 underlying minimum remains untouched
MIN_DAILY_VALUE_M = 150.0
MIN_AVG20_VALUE_M = 150.0
OHLCV_MIN_BARS = 22
REVENUE_MIN_MONTHS = 13


def _t(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (ValueError, TypeError):
        pass
    s = str(v).strip()
    return "" if s.lower() in {"", "nan", "nat", "null", "none", "<na>"} else s


def _f(v: Any) -> float | None:
    try:
        if isinstance(v, bool) or not _t(v):
            return None
        x = float(_t(v).replace(",", "").replace("%", ""))
        return x if math.isfinite(x) else None
    except (ValueError, TypeError):
        return None


def _d(v: Any) -> date | None:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = _t(v).replace("T", " ").split(" ")[0].replace("/", "-")
    try:
        if len(s) == 8 and s.isdigit():
            return datetime.strptime(s, "%Y%m%d").date()
        bits = s.split("-")
        if len(bits) == 3 and bits[0].isdigit() and int(bits[0]) < 1911:
            bits[0] = str(int(bits[0]) + 1911)
            s = "-".join(bits)
        return date.fromisoformat(s)
    except (ValueError, OverflowError):
        return None


def _ym(v: Any) -> tuple[int, int] | None:
    s = _t(v).replace("-", "").replace("/", "")
    if not s.isdigit() or len(s) not in (5, 6):
        return None
    y, m = int(s[:-2]), int(s[-2:])
    if y < 1911:
        y += 1911
    return (y, m) if 1 <= m <= 12 else None


def _idx(ym: tuple[int, int]) -> int:
    return ym[0] * 12 + ym[1] - 1


def _get(r: Mapping[str, Any], *names: str) -> Any:
    for key in names:
        if key in r and _t(r[key]):
            return r[key]
    return None


def analyze_history(hist: pd.DataFrame | None, *, asof: Any = None) -> dict[str, Any]:
    """Analyze already-fetched daily OHLCV (no extra network requests).

    Units are explicit: 成交股數/成交量(股) are shares, 成交量(張) is lots.
    Ambiguous '成交量' with no unit is NOT auto-converted.
    """
    base = {
        "H114K線資料狀態": "MISSING_HISTORY", "H114K線有效根數": 0,
        "H114紅K吞噬": "未驗證", "H114量縮墊高": "未驗證",
        "H114波動收斂": "未驗證", "H114突破前蓄勢分": None,
        "H114K線證據日": "", "H114K線量比": None,
        "H114當日成交量張": None, "H11420日平均量張": None,
    }
    if not isinstance(hist, pd.DataFrame) or hist.empty:
        return base
    columns = {
        "date": ("日期", "date", "Date"),
        "open": ("開盤價", "開盤", "Open", "open"),
        "high": ("最高價", "最高", "High", "high"),
        "low": ("最低價", "最低", "Low", "low"),
        "close": ("收盤價", "收盤", "Close", "close"),
    }
    def pick(names: tuple[str, ...]) -> str | None:
        return next((x for x in names if x in hist.columns), None)
    mapped = {k: pick(v) for k, v in columns.items()}
    if not all(mapped.values()):
        return dict(base, **{"H114K線資料狀態": "MISSING_OHLC_DATE_COLUMNS"})
    df = pd.DataFrame({"day": hist[mapped["date"]].map(_d)})
    for key in ("open", "high", "low", "close"):
        df[key] = hist[mapped[key]].map(_f)
    volume_col, divisor = None, 1.0
    for key, div in (("成交股數", 1000.), ("成交量(股)", 1000.), ("VolumeShares", 1000.),
                     ("成交量_張", 1.), ("成交量(張)", 1.), ("VolumeLots", 1.)):
        if key in hist.columns:
            volume_col, divisor = key, div
            break
    if volume_col:
        df["lots"] = hist[volume_col].map(_f) / divisor
    else:
        df["lots"] = float("nan")
    df = df.dropna(subset=["day", "open", "high", "low", "close"])
    df = df[(df["open"] > 0) & (df["close"] > 0) & (df["high"] >= df[["open", "close", "low"]].max(axis=1)) &
            (df["low"] <= df[["open", "close", "high"]].min(axis=1)) & (df["low"] > 0)]
    limit = _d(asof)
    if limit:
        df = df[df["day"] <= limit]
    df = df.sort_values("day").drop_duplicates("day", keep="last")
    base["H114K線有效根數"] = len(df)
    if len(df):
        base["H114K線證據日"] = str(df["day"].iloc[-1])
    if len(df) < OHLCV_MIN_BARS:
        base["H114K線資料狀態"] = "INSUFFICIENT_DATED_BARS"
        return base
    if limit and (limit - df["day"].iloc[-1]).days > 5:
        base["H114K線資料狀態"] = "STALE_DAILY_K"
        return base
    last, prior = df.iloc[-1], df.iloc[-2]
    body_prev = prior["open"]-prior["close"]
    body_curr = last["close"]-last["open"]
    engulf = (body_prev > 0 and body_curr > 0 and last["open"] <= prior["close"] and
              last["close"] >= prior["open"] and body_curr / last["close"] >= 0.004)
    trend5 = (prior["close"] / df["close"].iloc[-6] - 1) * 100
    meaningful = bool(engulf and trend5 < -0.5)
    base["H114紅K吞噬"] = "確認形態｜需後續確認" if meaningful else "形態初現｜未有下跌背景" if engulf else "無"
    base["H114多頭吞噬需確認"] = "是" if engulf else "否"
    base["H114前段5日漲跌幅%"] = round(trend5, 2)
    base["H114K線資料狀態"] = "VALID_OHLCV" if volume_col else "VALID_OHLC_ONLY_VOLUME_UNIT_UNKNOWN"
    if volume_col and last["lots"] > 0:
        avg20 = df["lots"].iloc[-21:-1].mean()
        ratio = last["lots"] / avg20 if avg20 > 0 else None
        base["H114K線量比"] = round(ratio, 3) if ratio is not None else None
        base["H114當日成交量張"] = round(last["lots"], 1)
        base["H11420日平均量張"] = round(avg20, 1)
        if ratio is not None:
            recent5 = df["close"].iloc[-5:]
            rising_lows = df["low"].iloc[-5:].min() > df["low"].iloc[-10:-5].min()
            rising = recent5.iloc[-1] > recent5.iloc[0] and last["close"] >= df["close"].iloc[-20:].mean()
            enough = last["lots"] >= MIN_LOTS and avg20 >= MIN_LOTS
            base["H114量縮墊高"] = "是" if .60 <= ratio <= .90 and rising_lows and rising and enough else "否"
    # Volatility compression is independently useful even when raw volume missing.
    ranges = (df["high"]-df["low"])/df["close"].shift(1)
    r5 = ranges.iloc[-5:].mean()
    r20 = ranges.iloc[-21:-1].mean()
    compression = float(r5 / r20) if r20 > 0 else None
    base["H114波動收斂"] = "是" if compression is not None and compression < .80 else "否"
    prior20_hi = df["high"].iloc[-21:-1].max()
    distance = (prior20_hi-last["close"])/prior20_hi*100 if prior20_hi > 0 else None
    base["H114距20日高點%"] = round(distance, 2) if distance is not None else None
    score = 35.0
    if base["H114波動收斂"] == "是": score += 22
    if base["H114量縮墊高"] == "是": score += 24
    if meaningful: score += 16
    if distance is not None and 0 <= distance <= 4: score += 14
    if last["close"] / df["close"].iloc[-6] > 1.18: score -= 20  # already running
    base["H114突破前蓄勢分"] = round(max(0, min(100, score)), 2)
    return base


def _revenue(history: Any, asof: date | None) -> dict[str, Any]:
    out = {"H114營收資料狀態": "MISSING_REVENUE_HISTORY", "H114營收創高": "未驗證",
           "H114營收年增率%": None, "H114三月累計年增率%": None,
           "H114營收加速度pp": None, "H114營收資料年月": ""}
    if not isinstance(history, (list, tuple)) or not history:
        return out
    records = {}
    valid_sources = {"TWSE", "TPEx", "TPEX", "MOPS"}
    for v in history:
        if not isinstance(v, dict):
            continue
        ym = _ym(_get(v, "年月", "year_month", "ym"))
        release = _d(_get(v, "公告日", "released_at", "publication_date"))
        source = _t(_get(v, "來源", "source"))
        value = _f(_get(v, "營收元", "revenue_twd", "revenue"))
        if not (ym and release and value is not None and value >= 0 and source.upper() in {s.upper() for s in valid_sources}):
            continue
        if asof and release > asof:
            continue
        if asof and _idx(ym) > asof.year*12 + asof.month-1:
            continue
        records[_idx(ym)] = (ym, value)
    if not records:
        out["H114營收資料狀態"] = "NO_VERIFIED_ASOF_RECORDS"
        return out
    newest = max(records)
    ym, now = records[newest]
    out["H114營收資料年月"] = f"{ym[0]}{ym[1]:02d}"
    if asof and (asof.year * 12 + asof.month-1 - newest) > 2:
        out["H114營收資料狀態"] = "STALE_REVENUE"
        return out
    # Must be gap-free for record-high, yoy, and growth-acceleration inferences.
    if not all(newest-i in records for i in range(13)):
        out["H114營收資料狀態"] = "INSUFFICIENT_CONSECUTIVE_MONTHS"
        return out
    y = records[newest-12][1]
    out["H114營收年增率%"] = round((now/y - 1)*100, 2) if y > 0 else None
    out["H114營收創高"] = "近12月新高" if now > max(records[newest-i][1] for i in range(1, 12)) else "否"
    if all(newest-i in records for i in range(15)):
        last3 = sum(records[newest-i][1] for i in range(3))
        prev_y3 = sum(records[newest-i-12][1] for i in range(3))
        out["H114三月累計年增率%"] = round((last3/prev_y3-1)*100,2) if prev_y3>0 else None
    if all(newest-i in records for i in range(18)):
        recent = sum(records[newest-i][1] for i in range(3))
        recent_old = sum(records[newest-i-12][1] for i in range(3))
        old = sum(records[newest-i][1] for i in range(3,6))
        old_y = sum(records[newest-i-12][1] for i in range(3,6))
        if recent_old>0 and old_y>0:
            out["H114營收加速度pp"] = round((recent/recent_old - old/old_y)*100,2)
    out["H114營收資料狀態"] = "VERIFIED_ASOF"
    return out


def _valuation(row: Mapping[str, Any], asof: date | None) -> dict[str, Any]:
    out = {"H114估值資料狀態": "MISSING_PE_EPS_OR_PEER",
           "H114相對本益比折價%": None, "H114估值合理": "未驗證"}
    pe = _f(_get(row, "H114本益比實值", "本益比_官方"))
    median = _f(_get(row, "H114同業本益比中位數_官方"))
    eps = _f(_get(row, "H114近四季EPS實值", "近四季EPS_官方"))
    source = _t(row.get("H114估值資料來源"))
    day = _d(row.get("H114估值資料日期"))
    if not (pe is not None and median is not None and eps is not None and eps > 0 and pe>0 and median>0
            and source.upper() in {"TWSE", "TPEX", "MOPS", "OFFICIAL_VERIFIED"} and day):
        return out
    if asof and (day>asof or (asof-day).days>110):
        out["H114估值資料狀態"] = "STALE_OR_FUTURE_VALUATION"
        return out
    discount = (1-pe/median)*100
    out.update({"H114估值資料狀態":"VERIFIED_ASOF", "H114相對本益比折價%":round(discount,2),
                "H114估值合理":"是" if discount>=15 else "否"})
    return out


def evaluate(row: Mapping[str, Any], *, info_asof: Any = None) -> dict[str, Any]:
    """Four parallel lanes. Missing data is not a passing score.

    A growth-value: revenue x low relative valuation.
    B volume compression: sufficiently liquid volume + basing price.
    C bullish engulfing: actual two OHLC candles + context.
    D institutional: H113 strict official fund flow and TDCC.
    """
    x = dict(row)
    asof = _d(info_asof) or _d(x.get("H110研究資訊截止日")) or _d(x.get("K線最後交易日"))
    if asof is None:
        # A historical replay without a decision cutoff must NEVER read today's
        # newest revenue/valuation and claim that it was known at recommendation time.
        rev = _revenue(None, None)
        rev["H114營收資料狀態"] = "NO_INFORMATION_CUTOFF"
        val = _valuation({}, None)
        val["H114估值資料狀態"] = "NO_INFORMATION_CUTOFF"
    else:
        rev = _revenue(_get(x, "H114月營收歷史", "H114營收歷史_官方"), asof)
        val = _valuation(x, asof)
    x.update(rev);x.update(val)
    status = _t(x.get("H114K線資料狀態"))
    technical = status.startswith("VALID_")
    if not status:
        # Weak fallback indicators may support WATCH ONLY but never pretend that
        # a true two-candle engulfing or 20-day volume profile was verified.
        technical = False
        status = "NO_FULL_OHLCV"
    day_lots = _f(_get(x,"H114當日成交量張","最新成交量_張"))
    avg_lots = _f(_get(x,"H11420日平均量張","20日均量_張"))
    day_value = _f(_get(x,"H113當日成交額百萬","成交額百萬"))
    avg_value = _f(_get(x,"H113二十日均成交額百萬","20日均成交額百萬"))
    liq = (day_lots is not None and avg_lots is not None and day_value is not None and avg_value is not None
           and day_lots>=MIN_LOTS and avg_lots>=MIN_LOTS and day_value>=MIN_DAILY_VALUE_M and avg_value>=MIN_AVG20_VALUE_M)
    yoy = _f(rev["H114營收年增率%"])
    yoy3 = _f(rev["H114三月累計年增率%"])
    accel = _f(rev["H114營收加速度pp"])
    a = (rev["H114營收資料狀態"]=="VERIFIED_ASOF" and val["H114估值資料狀態"]=="VERIFIED_ASOF"
         and rev["H114營收創高"]=="近12月新高" and yoy is not None and yoy>=15
         and yoy3 is not None and yoy3>=15 and val["H114估值合理"]=="是")
    b = liq and status == "VALID_OHLCV" and x.get("H114量縮墊高")=="是" and x.get("H114波動收斂")=="是"
    c = liq and technical and x.get("H114紅K吞噬")=="確認形態｜需後續確認"
    d = liq and _t(x.get("H113法人連買合格"))=="是" and _t(x.get("H113大戶鎖碼合格"))=="是" and \
        _t(x.get("H113主升起漲合格"))=="是"
    # A numeric score only exists for actually qualified lanes. It is not a probability.
    lane_scores = {
        "GROWTH_VALUE": round(min(100,72 + min(12,max(0,yoy-15)*.35)+min(8,max(0,accel or 0)*.35)),2) if a else None,
        "VOLUME_BASE": round(min(100,75 + max(0,(_f(x.get("H114突破前蓄勢分")) or 60)-60)*.5),2) if b else None,
        "ENGULFING": 77.0 if c else None,
        "INSTITUTIONAL": 82.0 if d else None,
    }
    available = sorted([(float(s),k) for k,s in lane_scores.items() if s is not None],reverse=True)
    main = available[0][1] if available else "NO_QUALIFIED_LANE"
    score = round(min(100, available[0][0] + min(6, max(0,len(available)-1)*3)),2) if available else None
    safe = (_t(x.get("H109決策權威"))=="RESEARCH_ELIGIBLE" and
            not _t(x.get("H110證據狀態")).startswith("CONFLICT") and
            _t(x.get("H105今日強勢排除"))!="是" and
            _t(x.get("市場別")) in {"上市", "上櫃"})
    if not safe or not liq:
        tier="BLOCKED"
    elif _t(x.get("H113入榜資格"))=="QUALIFIED" and available:
        tier="A_CERTIFIED_RESEARCH"
    elif available:
        tier="B_ENHANCED_RESEARCH_ONLY"
    else:
        tier="C_WATCH_ONLY"
    if tier=="B_ENHANCED_RESEARCH_ONLY" and _t(x.get("H113入榜資格"))=="BLOCKED":
        tier="C_WATCH_ONLY"  # cannot bypass H113 block by another lane
    missing=[]
    if not liq:missing.append("成交量≥1000張及成交額≥1.5億資料/門檻未通過")
    if not technical:missing.append("有效OHLC歷史不足，K棒形態不得聲稱已驗證")
    if rev["H114營收資料狀態"]!="VERIFIED_ASOF":missing.append("12～18月逐月營收公告證據不足")
    if val["H114估值資料狀態"]!="VERIFIED_ASOF":missing.append("同業PE/EPS官方實值證據不足")
    if _t(x.get("H113入榜資格"))!="QUALIFIED":missing.append("H113法人＋TDCC嚴格認證未完成")
    return {
        "H114版本":VERSION,"H114入榜身分":tier,"H114研究優先分":score,
        "H114主導策略":main,"H114成長低估分":lane_scores["GROWTH_VALUE"],
        "H114量縮蓄勢分":lane_scores["VOLUME_BASE"],"H114K線反轉分":lane_scores["ENGULFING"],
        "H114法人資金分":lane_scores["INSTITUTIONAL"],
        "H114流動性通過":"是" if liq else "否",
        "H114日量門檻張":MIN_LOTS,"H114四策略命中數":len(available),
        "H114營收資料狀態":rev["H114營收資料狀態"],"H114營收創高":rev["H114營收創高"],
        "H114營收年增率%":yoy,"H114三月累計年增率%":yoy3,"H114營收加速度pp":accel,
        "H114營收資料年月":rev["H114營收資料年月"],
        "H114估值資料狀態":val["H114估值資料狀態"],"H114相對本益比折價%":val["H114相對本益比折價%"],
        "H114紅K吞噬":x.get("H114紅K吞噬", "未驗證"),
        "H114量縮墊高":x.get("H114量縮墊高", "未驗證"),
        "H114波動收斂":x.get("H114波動收斂", "未驗證"),
        "H114K線資料狀態":status,
        "H114缺失證據":"；".join(missing) if missing else "四策略證據已足，仍不代表可買進",
        "H114交易權限":"LOCKED｜不影響Formal，所有H114僅研究/觀察",
    }


def load_verified_evidence(root: Path) -> dict[str, dict[str, Any]]:
    """Optional read-only external evidence handoff; never writes a JSON file.

    Contract: data/h114_verified_fundamentals.json ::
      {"schema_version":1,"stocks":{"2330":{"H114月營收歷史":[...], ...}}}
    Every monthly point still requires independent official source/release date,
    and valuation requires positive EPS, peer PE, source and dated provenance.
    This file is NOT bundled, and must not be backfilled with today's snapshots.
    """
    p=Path(root)/"data"/"h114_verified_fundamentals.json"
    if not p.is_file() or p.stat().st_size>25_000_000:
        return {}
    try:
        payload=json.loads(p.read_text(encoding="utf-8-sig"))
        if payload.get("schema_version")!=1 or not isinstance(payload.get("stocks"),dict):
            return {}
        out={}
        allow={"H114月營收歷史","H114營收歷史_官方","H114本益比實值",
               "H114同業本益比中位數_官方","H114近四季EPS實值",
               "H114估值資料來源","H114估值資料日期"}
        for code,row in payload["stocks"].items():
            c=_t(code).replace(".0", "")
            if not c or not isinstance(row,dict):continue
            out[c]={k:v for k,v in row.items() if k in allow}
        return out
    except (OSError, ValueError, TypeError):
        return {}


def build_parallel_watch(tables: dict[str, pd.DataFrame], limit: int = 30) -> pd.DataFrame:
    """No promotion into Formal/Research; supplemental independent discovery."""
    frames=[]
    for source in ("research","waiting","audit"):
        f=tables.get(source)
        if isinstance(f,pd.DataFrame) and not f.empty and "股票代號" in f.columns:
            x=f.copy();x["H114候選來源"]=source.upper();frames.append(x)
    if not frames:
        return pd.DataFrame()
    joined=pd.concat(frames,ignore_index=True,sort=False)
    joined["股票代號"]=joined["股票代號"].astype(str).str.replace(r"\.0$","",regex=True)
    joined=joined.drop_duplicates("股票代號",keep="first")
    status=joined.get("H114入榜身分",pd.Series("",index=joined.index)).fillna("").astype(str)
    keep=status.isin(["A_CERTIFIED_RESEARCH","B_ENHANCED_RESEARCH_ONLY"])
    eligible=joined.loc[keep].copy()
    if eligible.empty:
        return eligible
    eligible["__h114score"]=pd.to_numeric(eligible["H114研究優先分"],errors="coerce").fillna(-1)
    eligible=eligible.sort_values(["__h114score","股票代號"],ascending=[False,True],kind="stable")
    # One representative per sector first; only after that may a second appear.
    picked=[];counts={};left=list(eligible.to_dict("records"))
    for cap in (1,2):
        for r in left:
            if len(picked)>=limit:break
            group=_t(r.get("類別")) or "未分類"
            if counts.get(group,0)>=cap or r.get("股票代號") in {v["股票代號"] for v in picked}:continue
            counts[group]=counts.get(group,0)+1; picked.append(r)
        if len(picked)>=limit:break
    result=pd.DataFrame(picked).drop(columns=["__h114score"],errors="ignore")
    result["H114顯示順位"]=range(1,len(result)+1)
    result["H114研究聲明"]="研究候選｜不得視為正式買進，須通過原有Formal治理"
    cols=[c for c in ["H114顯示順位","股票代號","股票名稱","類別","市場別","H114候選來源","H114入榜身分",
          "H114研究優先分","H114主導策略","H114成長低估分","H114量縮蓄勢分","H114K線反轉分","H114法人資金分",
          "H114營收創高","H114營收年增率%","H114估值資料狀態","H114相對本益比折價%",
          "H114紅K吞噬","H114量縮墊高","H114K線資料狀態","H114流動性通過","H114缺失證據","H114研究聲明"] if c in result]
    return result[cols]


__all__=["VERSION","MIN_LOTS","analyze_history","evaluate","build_parallel_watch","load_verified_evidence"]

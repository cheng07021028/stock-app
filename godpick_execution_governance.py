# -*- coding: utf-8 -*-
"""Phase 8.3 execution governance for the GodPick system.

This module separates four concepts that were previously mixed together:

1. Full scan coverage (did the system actually analyse the requested universe?)
2. Candidate strength (is the stock technically/structurally interesting?)
3. Final trading permission (may it be traded now, under what conditions?)
4. Diagnostic signals (why an engine noticed the stock; never a buy permission)

The functions are DataFrame-only and do not read/write JSON or call the network.
"""
from __future__ import annotations

from typing import Any, Iterable
import math

import pandas as pd

EXECUTION_GOVERNANCE_VERSION = "phase103_official_factor_effective_coverage_20260804"
_LAST_CANDIDATE_QUALITY: dict[str, float] = {}

FINAL_BUCKET_ORDER = {
    "正式下週主推薦": 10,
    "A-｜準主推薦小量試單": 20,
    "盤中雷達追蹤": 30,
    "高風險雷達觀察": 40,
    "早期潛伏觀察": 50,
    "不可直接買觀察": 60,
    "正式排除清單": 90,
}

ACTIONABLE_BUCKETS = {
    "正式下週主推薦",
    "A-｜準主推薦小量試單",
    "盤中雷達追蹤",
}

FORMAL_RECORD_BUCKETS = {
    "正式下週主推薦",
    "A-｜準主推薦小量試單",
}

_SCAN_TERMINAL_KEYS = (
    "analyzed_ok",
    "invalid_code",
    "category_filtered",
    "no_history",
    "analysis_error",
    "signal_filtered",
    "risk_filtered",
    "prelaunch_filtered",
    "trade_filtered",
)

_DIAGNOSTIC_FIELD_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("正式主推薦訊號", ("A｜股神主推薦", "正式下週主推薦", "主流攻擊候選")),
    ("準主推薦訊號", ("A-｜準主推薦", "A-｜準主推薦小量試單")),
    ("盤中點火訊號", ("B+｜盤中點火追蹤", "盤中雷達追蹤", "飆股雷達")),
    ("突破確認訊號", ("B｜等突破確認", "主流突破追蹤", "盤中突破追蹤名單")),
    ("領漲回補訊號", ("L+｜領漲回補雷達", "L｜主流強勢回補", "領漲回補雷達")),
    ("題材轉強訊號", ("T｜題材轉強追蹤", "題材轉強追蹤")),
    ("漲停回放訊號", ("M+｜漲停漏選回放", "漏選回放校正")),
    ("已覆蓋雷達訊號", ("K｜已納入雷達", "已覆蓋雷達")),
    ("早期潛伏訊號", ("C+｜早期潛伏", "早期潛伏觀察")),
    ("高風險爆發訊號", ("R｜高風險爆發觀察", "高風險雷達觀察")),
    ("假強排除訊號", ("X｜假強排除", "假強排除")),
    ("過熱禁買訊號", ("D｜過熱禁買", "過熱禁買", "BLOCK")),
    ("低流動性訊號", ("低流動性排除", "冷門禁追", "成交額不足")),
)

CANDIDATE_DIAGNOSIS_COLUMNS = [
    "股票代號", "股票名稱", "市場別", "類別", "產業",
    "最終操作結論", "正式推薦分區", "是否正式推薦", "操作許可", "正式推薦等級",
    "風控否決旗標", "決策一致性", "候選性質", "正式推薦排除原因",
    "候選強度分", "推薦總分", "股神實戰總分", "可操作分", "實戰操作品質分",
    "推薦可信度分", "資料完整度評分", "資料完整度", "官方資料完整度",
    "買進分數", "Entry進場買點分", "Risk風控安全分", "風險報酬比", "追價風險分",
    "隔日可參考分", "隔日優勢型態", "隔日風險標記", "隔日參考判定", "觸發距離%", "停損距離_隔日%",
    "進場可執行分", "進場可執行判定", "進場路徑", "距最近可執行買點%", "進場阻擋原因",
    "今日漲幅%", "開盤跳空%", "當日量比", "5日20日量比", "當日收盤位置%", "突破20日高點%", "距20日高點%", "上影線比例%", "連續上漲天數", "強勢收盤旗標",
    "盤後動能救援分", "強勢動能分", "強勢動能判定", "動能進場條件", "動能風險控制",
    "盤前強勢前兆分", "強勢前兆分", "強勢前兆判定", "強勢前兆進場條件", "強勢前兆風控", "前置保留類型", "前置保留原因",
    "K線最後交易日", "K線落後交易日", "K線資料新鮮度", "本輪市場最新交易日", "K線日期驗證基準",
    "實戰停損參考", "實戰停損距離%", "實戰壓力空間%", "實戰風險報酬比", "實戰風控來源",
    "停損距離%", "壓力空間%", "近5日漲幅%", "近20日漲幅%",
    "主流資金分", "族群攻擊強度", "資金攻擊有效分", "成交額百萬", "20日均成交額百萬", "流動性等級", "流動性資料狀態", "流動性資料來源",
    "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數",
    "類股內排名", "類股前3強", "是否領先同類股", "同類股領先幅度",
    "自動因子總分", "EPS代理分數", "營收動能代理分數", "獲利代理分數",
    "爆發雷達分", "隔日爆發分", "飆股攻擊分", "主流領漲回補分", "漲停回放分",
    "強勢股漏選風險分", "推薦角色", "飆股雷達角色", "領漲回補角色", "回放校正角色",
    "主流作戰分區", "下週作戰分區", "飆股雷達分區", "領漲回補分區", "回放校正分區",
    "引擎輔助訊號", "訊號用途", "分區互斥檢查", "最終分區唯一鍵",
    "最新價", "預估進場點", "實戰觸發價", "觸發後守價", "停損參考", "第一壓力價",
    "建議倉位上限%", "正式推薦動作", "失效條件", "開盤跳空處理",
]

ACTION_TABLE_COLUMNS = [
    "最終操作結論", "股票代號", "股票名稱", "類別", "產業",
    "是否正式推薦", "操作許可", "正式推薦等級", "候選性質",
    "可操作分", "實戰操作品質分", "推薦可信度分", "候選強度分",
    "Entry進場買點分", "Risk風控安全分", "實戰風險報酬比", "風險報酬比", "追價風險分",
    "隔日可參考分", "隔日優勢型態", "隔日風險標記", "隔日參考判定", "觸發距離%", "停損距離_隔日%",
    "進場可執行分", "進場可執行判定", "進場路徑", "距最近可執行買點%", "進場阻擋原因",
    "K線最後交易日", "K線落後交易日", "K線資料新鮮度", "本輪市場最新交易日", "K線日期驗證基準",
    "實戰停損參考", "實戰停損距離%", "實戰壓力空間%", "實戰風控來源",
    "成交額百萬", "流動性等級", "流動性資料狀態",
    "最新價", "預估進場點", "實戰觸發價", "觸發後守價", "停損參考", "第一壓力價",
    "建議倉位上限%", "正式推薦動作", "失效條件", "開盤跳空處理",
]


def _safe_str(value: Any) -> str:
    try:
        if value is None or pd.isna(value):
            return ""
    except Exception:
        if value is None:
            return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "nat", "<na>"} else text


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return float(default)
        return number
    except Exception:
        return float(default)


def _series_text(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="object")
    return df[col].map(_safe_str).astype("object")


def _numeric_series(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    """Return an index-aligned numeric Series even when an optional column is absent.

    Older recommendation caches do not contain every Phase 8 field.  Using
    ``DataFrame.get(col, 0)`` returned a scalar and then crashed on ``fillna``;
    page 7 swallowed that exception, leaving exclusions mixed into the buy list.
    """
    if col not in df.columns:
        return pd.Series([float(default)] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce").fillna(float(default)).astype(float)




def _parse_content_date(value: Any) -> pd.Timestamp | None:
    """Parse YYYYMMDD numerics and ordinary date strings without ns misreading."""
    text = _safe_str(value)
    if not text:
        return None
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    try:
        if len(digits) == 8 and digits.startswith(("19", "20")):
            ts = pd.to_datetime(digits, format="%Y%m%d", errors="coerce")
        else:
            ts = pd.to_datetime(text, errors="coerce")
        return pd.Timestamp(ts).normalize() if pd.notna(ts) else None
    except Exception:
        return None


def _business_lag(newer: Any, older: Any) -> int:
    n = _parse_content_date(newer)
    o = _parse_content_date(older)
    if n is None or o is None:
        return 999
    if n <= o:
        return 0
    try:
        return int(len(pd.bdate_range(start=o + pd.Timedelta(days=1), end=n)))
    except Exception:
        return max(0, int((n - o).days))

def _normalise_code(value: Any) -> str:
    text = _safe_str(value).replace(".0", "")
    return text.zfill(4) if text.isdigit() and len(text) < 4 else text


def build_scan_quality_report(
    summary: dict[str, Any] | None,
    *,
    universe_size: int | None = None,
    candidate_count: int = 0,
    final_count: int = 0,
    candidate_frame: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Build an execution-grade scan/data quality report.

    Coverage and data usability are separate concepts.  A symbol may be fully
    processed but still lack usable K-line or liquidity evidence.  Phase 8.3
    therefore never calls that situation an incomplete scan; it reports a
    restricted valid-data universe and only permits formal action when liquidity
    evidence is sufficiently covered.
    """
    data = summary if isinstance(summary, dict) else {}
    expected = int(_safe_float(data.get("total_count"), universe_size or 0))
    candidate_passed = int(_safe_float(data.get("analyzed_ok"), candidate_count or 0))
    analyzed = candidate_passed

    # 「有效K線」不是只有最後進入候選池的股票。signal/risk/prelaunch/trade_filtered
    # 都已完成 K 線與指標分析，只是後續條件不合格。舊版誤用 analyzed_ok 當分子，
    # 會把正常被門檻淘汰的股票誤判成抓不到K線，造成 483/1766 這類假性低覆蓋。
    inferred_history_ok = candidate_passed + sum(
        max(0, int(_safe_float(data.get(key), 0)))
        for key in ("signal_filtered", "risk_filtered", "prelaunch_filtered", "trade_filtered")
    )
    history_ok = int(_safe_float(data.get("history_ok"), inferred_history_ok))
    if expected > 0:
        history_ok = min(max(history_ok, 0), expected)
    else:
        history_ok = max(history_ok, 0)

    processed = 0
    for key in _SCAN_TERMINAL_KEYS:
        processed += max(0, int(_safe_float(data.get(key), 0)))
    if expected > 0:
        processed = min(max(processed, history_ok), expected)
    else:
        processed = max(processed, history_ok)

    coverage = (processed / expected * 100.0) if expected > 0 else 0.0
    usable_history = (history_ok / expected * 100.0) if expected > 0 else 0.0
    result_ratio = (final_count / candidate_passed * 100.0) if candidate_passed > 0 else 0.0

    liquidity_coverage = 0.0
    official_coverage = 0.0
    official_match_coverage = 0.0
    official_effective_coverage = 0.0
    official_trusted_coverage = 0.0
    official_same_day_coverage = 0.0
    official_one_day_lag_coverage = 0.0
    official_missing_date_coverage = 0.0
    data_rows = 0
    if isinstance(candidate_frame, pd.DataFrame) and not candidate_frame.empty:
        frame = candidate_frame
        data_rows = len(frame)
        amount = pd.to_numeric(frame.get("成交額百萬", pd.Series([0] * len(frame), index=frame.index)), errors="coerce").fillna(0)
        avg_amount = pd.to_numeric(frame.get("20日均成交額百萬", pd.Series([0] * len(frame), index=frame.index)), errors="coerce").fillna(0)
        volume = pd.to_numeric(frame.get("最新成交量_張", pd.Series([0] * len(frame), index=frame.index)), errors="coerce").fillna(0)
        avg_volume = pd.to_numeric(frame.get("20日均量_張", pd.Series([0] * len(frame), index=frame.index)), errors="coerce").fillna(0)
        known = amount.gt(0) | avg_amount.gt(0) | volume.gt(0) | avg_volume.gt(0)
        liquidity_coverage = float(known.mean() * 100.0) if len(known) else 0.0
        official = pd.to_numeric(frame.get("官方資料完整度", pd.Series([float("nan")] * len(frame), index=frame.index)), errors="coerce")
        official_status = _series_text(frame, "官方因子資料狀態")
        official_date_raw = frame.get("官方因子資料日期", frame.get("官方資料日期", pd.Series([None] * len(frame), index=frame.index)))
        stock_date_raw = frame.get("本輪市場最新交易日", frame.get("K線最後交易日", pd.Series([None] * len(frame), index=frame.index)))
        official_date = official_date_raw.map(_parse_content_date)
        stock_date = stock_date_raw.map(_parse_content_date)
        source_trust = pd.to_numeric(frame.get("因子來源可信度", pd.Series([0] * len(frame), index=frame.index)), errors="coerce").fillna(0)
        matched = official.notna() | official_status.ne("")
        effective = official.fillna(0).ge(45) | official_status.isin(["完整", "部分資料"])
        lag_days = pd.Series([999] * len(frame), index=frame.index, dtype="int64")
        valid_dates = official_date.notna() & stock_date.notna()
        if bool(valid_dates.any()):
            lag_days.loc[valid_dates] = [
                _business_lag(stock_date.loc[idx], official_date.loc[idx]) for idx in frame.index[valid_dates]
            ]
        fresh_enough = valid_dates & lag_days.le(1)
        trusted_source = source_trust.ge(70) | source_trust.eq(0)
        trusted = effective & fresh_enough & trusted_source
        official_match_coverage = float(matched.mean() * 100.0) if len(matched) else 0.0
        official_effective_coverage = float(effective.mean() * 100.0) if len(effective) else 0.0
        official_trusted_coverage = float(trusted.mean() * 100.0) if len(trusted) else 0.0
        official_same_day_coverage = float((effective & valid_dates & lag_days.eq(0)).mean() * 100.0) if len(effective) else 0.0
        official_one_day_lag_coverage = float((effective & valid_dates & lag_days.eq(1)).mean() * 100.0) if len(effective) else 0.0
        official_missing_date_coverage = float((effective & ~valid_dates).mean() * 100.0) if len(effective) else 0.0
        official_coverage = official_effective_coverage
    else:
        cached = _LAST_CANDIDATE_QUALITY if isinstance(_LAST_CANDIDATE_QUALITY, dict) else {}
        liquidity_coverage = _safe_float(
            data.get("liquidity_data_coverage_pct"), cached.get("liquidity_coverage", 0.0)
        )
        official_coverage = _safe_float(
            data.get("official_effective_coverage_pct", data.get("official_data_coverage_pct")), cached.get("official_coverage", 0.0)
        )
        official_match_coverage = _safe_float(data.get("official_match_coverage_pct"), official_coverage)
        official_effective_coverage = _safe_float(data.get("official_effective_coverage_pct"), official_coverage)
        official_trusted_coverage = _safe_float(data.get("official_trusted_coverage_pct"), official_effective_coverage)
        official_same_day_coverage = _safe_float(data.get("official_same_day_coverage_pct"), 0.0)
        official_one_day_lag_coverage = _safe_float(data.get("official_one_day_lag_coverage_pct"), 0.0)
        official_missing_date_coverage = _safe_float(data.get("official_missing_date_coverage_pct"), 0.0)
        data_rows = int(_safe_float(cached.get("rows"), 0))

    minimum_pool = max(100, min(300, int(expected * 0.15))) if expected > 0 else 100
    if expected <= 0:
        status, level, usable, factor = "未知｜缺少掃描證據", "unknown", False, 0.0
        scope = "不可判定"
        reason = "缺少掃描總數與成功分析數，不能用目前結果推論市場。"
    elif expected <= 10:
        complete = processed == expected and analyzed == expected and (liquidity_coverage >= 80 or data_rows == 0)
        status = "完整" if complete else "不完整｜禁止正式推薦"
        level = "complete" if complete else "invalid"
        usable = complete
        factor = 1.0 if complete else 0.0
        scope = "手動小範圍"
        reason = "小範圍掃描已逐檔完成。" if complete else "小範圍掃描必須逐檔成功，且具備流動性證據。"
    elif coverage < 95.0:
        status, level, usable, factor = "掃描未完成｜禁止正式推薦", "invalid", False, 0.0
        scope = "未完成掃描"
        reason = "仍有大量股票未完成處理，不能以局部結果代表市場。"
    elif history_ok < minimum_pool or usable_history < 10.0:
        status, level, usable, factor = "有效資料池過小｜禁止正式推薦", "invalid", False, 0.0
        scope = f"僅{history_ok}檔有效K線資料"
        reason = "成功分析樣本過少，無法形成具代表性的選股池。"
    elif data_rows > 0 and liquidity_coverage < 60.0:
        status, level, usable, factor = "流動性資料異常｜禁止正式推薦", "invalid", False, 0.0
        scope = f"有效K線{history_ok}檔，但流動性覆蓋不足"
        reason = "多數候選缺少成交額/成交量，不能把資料缺失誤判為低流動性，也不能產生正式買進結論。"
    elif data_rows > 0 and official_match_coverage >= 70.0 and official_effective_coverage < 30.0:
        status, level, usable, factor = "官方紀錄存在但有效內容不足｜只供研究雷達", "invalid", False, 0.0
        scope = f"紀錄匹配{official_match_coverage:.1f}%／有效因子僅{official_effective_coverage:.1f}%"
        reason = "官方快取可對應股票，但完整度不足或只含空白占位值；不得把『有紀錄』誤當成有效官方因子。"
    elif data_rows > 0 and official_effective_coverage < 30.0:
        status, level, usable, factor = "官方因子有效覆蓋不足｜只供研究雷達", "invalid", False, 0.0
        scope = f"有效因子覆蓋率僅{official_effective_coverage:.1f}%"
        reason = "法人、營收、估值等有效欄位未形成足夠覆蓋；技術面排名只能作研究雷達。"
    elif data_rows > 0 and official_trusted_coverage < 40.0:
        status, level, usable, factor = "官方因子日期/可信度不足｜正式推薦暫停", "warning", True, 0.35
        scope = f"有效{official_effective_coverage:.1f}%／最新可信{official_trusted_coverage:.1f}%"
        reason = "官方因子有有效內容，但日期未驗證、落後超過1日或來源可信度不足；可保留資料受限A-安全閥，正式推薦暫停。"
    elif data_rows > 0 and official_effective_coverage < 70.0:
        status, level, usable, factor = "官方因子有效覆蓋不足｜限定A-", "warning", True, 0.5
        scope = f"有效因子覆蓋率{official_effective_coverage:.1f}%／最新可信{official_trusted_coverage:.1f}%"
        reason = "有效官方因子未達70%；不得升格正式推薦，但可依資料受限A-安全閥做極小量條件評估。"
    elif data_rows > 0 and official_effective_coverage >= 70.0 and official_same_day_coverage < 40.0 and official_one_day_lag_coverage >= 40.0:
        status, level, usable, factor = "官方因子有效但落後1日｜正式暫停／A-可評估", "warning", True, 0.5
        scope = f"有效{official_effective_coverage:.1f}%／同日{official_same_day_coverage:.1f}%／落後1日{official_one_day_lag_coverage:.1f}%"
        reason = "官方因子並非缺失，而是多數落後最新K線1個交易日；研究雷達與資料受限A-可用，正式推薦待同日資料完成後再升格。"
    elif coverage >= 99.0 and usable_history >= 80.0 and (liquidity_coverage >= 90.0 or data_rows == 0):
        status, level, usable, factor = "完整", "complete", True, 1.0
        scope = "全掃描有效資料池"
        reason = "掃描、K線與流動性資料均達正式推薦標準。"
    elif coverage >= 99.0 and usable_history >= 60.0 and history_ok >= minimum_pool and (liquidity_coverage >= 80.0 or data_rows == 0):
        status, level, usable, factor = "掃描完成｜限定有效資料池", "limited", True, 0.5
        scope = f"僅適用於{history_ok}檔有效K線股票"
        reason = "全體股票已處理且有效K線達六成；推薦只代表有效資料池，倉位上限自動減半。"
    elif coverage >= 99.0 and history_ok >= minimum_pool and usable_history >= 20.0:
        status, level, usable, factor = "有效K線不足｜只供診斷雷達", "invalid", False, 0.0
        scope = f"僅{history_ok}檔有效K線，不足以形成正式作戰母體"
        reason = "全體股票雖已處理，但有效K線未達60%；只能檢視隔日優勢型態與雷達，不得宣稱正式推薦可用。"
    elif coverage >= 95.0 and usable_history >= 50.0 and (liquidity_coverage >= 75.0 or data_rows == 0):
        status, level, usable, factor = "可用但需注意", "warning", True, 0.7
        scope = "接近完整的有效資料池"
        reason = "資料大致可用，但未達最佳完整度；正式倉位自動降級。"
    else:
        status, level, usable, factor = "資料品質不足｜禁止正式推薦", "invalid", False, 0.0
        scope = "不可作為正式推薦"
        reason = "掃描雖可能完成，但有效K線或流動性資料未達操作標準。"

    return {
        "掃描品質狀態": status,
        "掃描品質等級": level,
        "正式推薦可用": bool(usable),
        "推薦適用範圍": scope,
        "倉位折減係數": float(factor),
        "預計掃描數": expected,
        "已處理數": processed,
        "成功分析數": history_ok,
        "通過推薦前置篩選數": candidate_passed,
        "完整候選診斷數": int(candidate_count or 0),
        "最終作戰候選數": int(final_count or 0),
        "掃描覆蓋率%": round(coverage, 2),
        "有效K線資料率%": round(usable_history, 2),
        "歷史資料成功率%": round(usable_history, 2),
        "流動性資料覆蓋率%": round(liquidity_coverage, 2),
        "官方因子覆蓋率%": round(official_effective_coverage, 2),
        "官方紀錄匹配率%": round(official_match_coverage, 2),
        "官方有效因子覆蓋率%": round(official_effective_coverage, 2),
        "官方最新可信覆蓋率%": round(official_trusted_coverage, 2),
        "官方同日對齊覆蓋率%": round(official_same_day_coverage, 2),
        "官方落後1日覆蓋率%": round(official_one_day_lag_coverage, 2),
        "官方日期未驗證覆蓋率%": round(official_missing_date_coverage, 2),
        "作戰候選率%": round(result_ratio, 2),
        "掃描品質說明": reason,
        "版本": EXECUTION_GOVERNANCE_VERSION,
    }


def apply_scan_quality_to_frame(df: pd.DataFrame | None, report: dict[str, Any] | None) -> pd.DataFrame:
    out = pd.DataFrame() if df is None else (df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df))
    if out.empty:
        return out
    data = report if isinstance(report, dict) else {}
    for col in [
        "掃描品質狀態", "掃描品質等級", "正式推薦可用", "推薦適用範圍", "倉位折減係數",
        "預計掃描數", "成功分析數", "掃描覆蓋率%", "有效K線資料率%",
        "歷史資料成功率%", "流動性資料覆蓋率%", "官方因子覆蓋率%",
        "官方紀錄匹配率%", "官方有效因子覆蓋率%", "官方最新可信覆蓋率%",
        "官方同日對齊覆蓋率%", "官方落後1日覆蓋率%", "官方日期未驗證覆蓋率%", "掃描品質說明",
    ]:
        out[col] = data.get(col, "")

    factor = max(0.0, min(1.0, _safe_float(data.get("倉位折減係數"), 0.0)))
    usable = bool(data.get("正式推薦可用", False))
    if "建議倉位上限%" in out.columns:
        cap = pd.to_numeric(out["建議倉位上限%"], errors="coerce").fillna(0)
        out["建議倉位上限%"] = (cap * factor).round(1) if usable else 0.0
    if "推薦可信度分" in out.columns:
        confidence = pd.to_numeric(out["推薦可信度分"], errors="coerce").fillna(0)
        penalty = 0 if factor >= 0.99 else 8 if factor >= 0.69 else 12 if factor > 0 else 25
        out["推薦可信度分"] = (confidence - penalty).clip(lower=0, upper=100).round(1)
    if usable and factor < 1 and "決策一致性" in out.columns:
        suffix = f"｜掃描範圍受限，倉位乘數{factor:.1f}"
        out["決策一致性"] = _series_text(out, "決策一致性") + suffix
    return out


def _build_signal_tags_for_row(row: pd.Series) -> str:
    blob = "｜".join(
        _safe_str(row.get(col))
        for col in [
            "推薦角色", "穩健推薦角色", "正式推薦分區", "正式推薦資格",
            "主流作戰分區", "下週作戰分區", "飆股雷達角色", "飆股雷達分區",
            "領漲回補角色", "領漲回補分區", "回放校正角色", "回放校正分區",
            "真禁買原因", "過熱原因", "正式推薦排除原因", "冷門股警示",
        ]
    )
    tags: list[str] = []
    for label, keys in _DIAGNOSTIC_FIELD_GROUPS:
        if any(key and key in blob for key in keys):
            tags.append(label)
    return "、".join(tags) if tags else "一般候選訊號"


def canonicalize_final_partition(df: pd.DataFrame | None) -> pd.DataFrame:
    """Ensure each stock has exactly one final operational bucket.

    The latest full candidate frame also records data-quality coverage. Page 7
    calls this function on the full analysed pool immediately before building the
    scan report, so older page code does not need a new function signature.
    """
    global _LAST_CANDIDATE_QUALITY
    out = pd.DataFrame() if df is None else (df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df))
    if out.empty:
        return out
    try:
        amount = pd.to_numeric(out.get("成交額百萬", pd.Series([0] * len(out), index=out.index)), errors="coerce").fillna(0)
        avg_amount = pd.to_numeric(out.get("20日均成交額百萬", pd.Series([0] * len(out), index=out.index)), errors="coerce").fillna(0)
        volume = pd.to_numeric(out.get("最新成交量_張", pd.Series([0] * len(out), index=out.index)), errors="coerce").fillna(0)
        avg_volume = pd.to_numeric(out.get("20日均量_張", pd.Series([0] * len(out), index=out.index)), errors="coerce").fillna(0)
        known = amount.gt(0) | avg_amount.gt(0) | volume.gt(0) | avg_volume.gt(0)
        official = pd.to_numeric(out.get("官方資料完整度", pd.Series([float("nan")] * len(out), index=out.index)), errors="coerce")
        snapshot = {
            "rows": float(len(out)),
            "liquidity_coverage": float(known.mean() * 100.0) if len(out) else 0.0,
            "official_coverage": float(official.notna().mean() * 100.0) if len(out) else 0.0,
        }
        # Keep the largest recently seen pool; a later call on a tiny display
        # subset must not overwrite the full-scan evidence.
        if snapshot["rows"] >= _safe_float(_LAST_CANDIDATE_QUALITY.get("rows"), 0):
            _LAST_CANDIDATE_QUALITY = snapshot
    except Exception:
        pass
    if "正式推薦分區" not in out.columns:
        try:
            from godpick_formal_recommendation_engine import apply_formal_recommendation_engine
            out = apply_formal_recommendation_engine(out)
        except Exception:
            out["正式推薦分區"] = "不可直接買觀察"
            out["正式推薦資格"] = "WATCH｜分流模組未載入"
            out["操作許可"] = "不可直接買"

    bucket = _series_text(out, "正式推薦分區")
    bucket = bucket.where(bucket.isin(FINAL_BUCKET_ORDER), "不可直接買觀察")
    out["正式推薦分區"] = bucket
    if "候選強度分" not in out.columns:
        out["候選強度分"] = _numeric_series(out, "推薦總分", 0)
    out["引擎輔助訊號"] = out.apply(_build_signal_tags_for_row, axis=1)
    out["訊號用途"] = "診斷用途｜不等於買進許可；以正式推薦分區與操作許可為唯一依據"
    codes = _series_text(out, "股票代號").map(_normalise_code)
    out["最終分區唯一鍵"] = codes + "｜" + bucket
    out["分區互斥檢查"] = "PASS｜每檔僅一個最終操作分區"
    out["最終分區優先序"] = bucket.map(FINAL_BUCKET_ORDER).fillna(999).astype(int)

    if "股票代號" in out.columns:
        # Same code can occur more than once after old merges. Keep the row with the
        # strongest final-priority/operation score, never duplicate it across final views.
        op = _numeric_series(out, "可操作分", 0)
        confidence = _numeric_series(out, "推薦可信度分", 0)
        out["_governance_sort"] = op * 0.7 + confidence * 0.3
        out = out.sort_values(
            ["最終分區優先序", "_governance_sort"],
            ascending=[True, False],
            kind="mergesort",
        ).drop_duplicates(subset=["股票代號"], keep="first")
        out = out.drop(columns=["_governance_sort"], errors="ignore")
    return out.reset_index(drop=True)


def build_candidate_diagnosis(df: pd.DataFrame | None, *, max_rows: int = 3000) -> pd.DataFrame:
    out = canonicalize_final_partition(df)
    if out.empty:
        return out
    existing = [col for col in CANDIDATE_DIAGNOSIS_COLUMNS if col in out.columns]
    extra = [
        col for col in ["掃描品質狀態", "正式推薦可用", "掃描覆蓋率%", "歷史資料成功率%"]
        if col in out.columns and col not in existing
    ]
    out = out[existing + extra].copy()
    sort_cols = [col for col in ["最終分區優先序", "可操作分", "推薦可信度分", "候選強度分"] if col in out.columns]
    if sort_cols:
        ascending = [True if col == "最終分區優先序" else False for col in sort_cols]
        out = out.sort_values(sort_cols, ascending=ascending, kind="mergesort")
    return out.head(max(1, int(max_rows))).reset_index(drop=True)


def build_action_table(df: pd.DataFrame | None, *, include_intraday: bool = True) -> pd.DataFrame:
    out = canonicalize_final_partition(df)
    if out.empty:
        return out
    allowed = {"正式下週主推薦", "A-｜準主推薦小量試單"}
    if include_intraday:
        allowed.add("盤中雷達追蹤")
    out = out[out["正式推薦分區"].isin(allowed)].copy()
    if "正式推薦分區" in out.columns and "盤中雷達優先級" in out.columns:
        radar_mask = out["正式推薦分區"].eq("盤中雷達追蹤")
        out = out[~radar_mask | _series_text(out, "盤中雷達優先級").str.startswith("R1")].copy()
    cols = [col for col in ACTION_TABLE_COLUMNS if col in out.columns]
    if cols:
        out = out[cols].copy()
    return out.reset_index(drop=True)


def build_engine_diagnostic_table(df: pd.DataFrame | None) -> pd.DataFrame:
    out = canonicalize_final_partition(df)
    if out.empty:
        return out
    cols = [
        "股票代號", "股票名稱", "類別", "正式推薦分區", "最終操作結論", "操作許可",
        "引擎輔助訊號", "訊號用途", "正式推薦排除原因", "候選強度分", "可操作分",
        "Entry進場買點分", "Risk風控安全分", "風險報酬比", "追價風險分", "成交額百萬",
        "分區互斥檢查",
    ]
    return out[[col for col in cols if col in out.columns]].copy().reset_index(drop=True)


def govern_recommend_list(df: pd.DataFrame | None, *, include_r1: bool = True) -> pd.DataFrame:
    out = canonicalize_final_partition(df)
    if out.empty:
        return out
    bucket = _series_text(out, "正式推薦分區")
    allowed = bucket.isin(FORMAL_RECORD_BUCKETS)
    if include_r1:
        radar_priority = _series_text(out, "盤中雷達優先級")
        allowed = allowed | (bucket.eq("盤中雷達追蹤") & radar_priority.str.startswith("R1"))
    return out.loc[allowed].copy().reset_index(drop=True)


def govern_recommend_records(df: pd.DataFrame | None) -> pd.DataFrame:
    out = canonicalize_final_partition(df)
    if out.empty:
        return out
    return out.loc[_series_text(out, "正式推薦分區").isin(FORMAL_RECORD_BUCKETS)].copy().reset_index(drop=True)


def report_allows_formal_action(report: dict[str, Any] | None) -> bool:
    if not isinstance(report, dict):
        return False
    return bool(report.get("正式推薦可用", False))

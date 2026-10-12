# -*- coding: utf-8 -*-
"""V191-H110 evidence integrity for future-ignition research.

Never authorises transactions. Separates trading bar date from information as-of
and refuses to promote contradictory or unverified claims as proven facts.
"""
from __future__ import annotations

import datetime as dt
import os
from typing import Any
from zoneinfo import ZoneInfo

from godpick_h109_authority_governor import f, label, parse_day, parse_month, t, revenue_freshness

VERSION = "v191_h110_future_alpha_evidence_integrity_20261012"


def research_asof(market_day: str, frames: list[Any] | None = None) -> tuple[dt.date | None, str, bool]:
    """No hindsight by default. LIVE requires explicit intent or today-dated run metadata.

    This prevents a rerun of last week's candidates from silently incorporating
    disclosures released after that week's recommended observation date.
    """
    market = parse_day(market_day)
    now = dt.datetime.now(ZoneInfo("Asia/Taipei"))
    mode = os.getenv("GODPICK_H110_SCAN_MODE", "AUTO").strip().upper()
    if mode == "LIVE":
        if market and market > now.date():
            return market, "REPLAY_FUTURE_BLOCKED", False
        if now.hour < 15:
            return market, "LIVE_BEFORE_CLOSE_BLOCKED", False
        return now.date(), "LIVE_EXPLICIT", True
    if mode not in {"SAFE", "AUTO", "REPLAY"}:
        return market, "UNKNOWN_MODE_SAFE", False
    if mode in {"SAFE", "REPLAY"}:
        return market, "HISTORICAL_SAFE", False
    # On a same-day after-close run there is no later session to leak; refresh
    # current publicly disclosed records. On weekends/holidays (stale market
    # bar), demand explicit LIVE mode or a real research timestamp.
    if market == now.date() and now.hour >= 15:
        return now.date(), "LIVE_SAME_DAY", True
    # Otherwise AUTO requires a real timestamp of today's research generation.
    # Never infer publication time from Friday/Sunday proximity.
    for frame in frames or []:
        if getattr(frame, "empty", True):
            continue
        try:
            rows = frame.head(3).to_dict("records")
        except (AttributeError, TypeError):
            continue
        for row in rows:
            for key in ("H110研究截止時間", "推薦產製時間", "研究產製時間"):
                value = t(row.get(key))
                if not value or len(value) < 16:
                    continue
                try:
                    stamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
                    if stamp.tzinfo is None:
                        stamp = stamp.replace(tzinfo=ZoneInfo("Asia/Taipei"))
                    stamp = stamp.astimezone(ZoneInfo("Asia/Taipei"))
                    if stamp.date() == now.date() and stamp <= now and now.hour >= 15:
                        return stamp.date(), "LIVE_DATED_METADATA", True
                except ValueError:
                    continue
    return market, "HISTORICAL_SAFE_NO_TIMESTAMP", False


def _number(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = f(row.get(key))
        if value is not None:
            return value
    return None


def _institution_conflict(row: dict[str, Any]) -> tuple[bool, str, bool]:
    """Detect contradictory 1/3/5 day claims, not turn missing flows into zero."""
    one = _number(row, ("三大法人近1日合計_官方", "三大法人近1日合計"))
    three = _number(row, ("三大法人近3日合計",))
    five = _number(row, ("三大法人近5日合計_官方", "三大法人近5日合計"))
    present = sum(x is not None for x in (one, three, five))
    claim = " ".join(t(row.get(k)) for k in ("H105主要理由", "法人動向", "法人趨勢說明"))
    if "三大法人短中期同向買超" in claim and present >= 2 and (three is not None and three <= 0 or five is not None and five <= 0):
        return True, f"同向買超敘述與3/5日淨額不符：3D={three}、5D={five}", True
    if "三大法人由賣轉買" in claim and one is not None and one <= 0:
        return True, f"由賣轉買敘述與當日淨額不符：1D={one}", True
    # Proxy score alone does not verify institutional buying.
    return False, "缺少逐日官方淨額，法人方向僅供研究" if present == 0 else "法人淨額已取得部分/全部", present > 0


def audit(row: dict[str, Any], *, market_day: dt.date | None,
          info_asof: dt.date | None, asof_mode: str,
          source_status: str = "") -> dict[str, Any]:
    revenue_date = parse_month(label(row, ("H109營收資料年月", "營收資料年月", "營收資料日期")))
    fresh, month = revenue_freshness(row, info_asof)
    source = label(row, ("H109營收來源",))
    if source and revenue_date is None:
        source = ""  # a URL without a parsable period cannot verify anything
    filing_date = parse_day(label(row, ("H109營收出表日期",)))
    conflict = False
    notes: list[str] = []
    if filing_date and info_asof and filing_date > info_asof:
        conflict = True
        notes.append("官方營收出表日期晚於研究截止日")
    if market_day and info_asof and info_asof < market_day:
        conflict = True
        notes.append("研究截止日早於交易資料日期")
    flow_conflict, flow_note, actual_flows = _institution_conflict(row)
    flow_date = parse_day(label(row, ("法人資料日期_官方", "三大法人官方資料日", "外資官方資料日")))
    has_official_flows = (flow_date is not None and market_day is not None and flow_date <= market_day and
                          0 <= (market_day - flow_date).days <= 4 and
                          any(f(row.get(k)) is not None for k in ("三大法人近1日合計_官方", "三大法人近5日合計_官方", "外資近5日買賣超_官方")))
    if flow_conflict:
        conflict = True
    notes.append(flow_note)
    if fresh.startswith("FUTURE"):
        conflict = True
        notes.append("月營收資料屬於未來月份")
    elif fresh.startswith(("STALE", "UNKNOWN")):
        notes.append("月營收資料未更新/缺少明確月份，不可當作當晚新催化")
    elif fresh.startswith("PENDING"):
        notes.append("官方月營收尚在揭露窗口，不代表已完成更新")
    if not has_official_flows:
        notes.append("法人方向缺少可驗證的官方資料日期與買賣超來源，不能視為官方連買")
    if not source:
        notes.append("未取得可驗證官方營收來源")
    if source_status.startswith("FAILED"):
        notes.append("官方更新失敗，不得顯示資料健康100%")
    score = f(row.get("H109最終研究優先分"))
    if score is None: score = f(row.get("H108研究優先分"))
    if score is None: score = f(row.get("H105黑馬預發動分"))
    if score is None: score = 0.0
    discount = 0.0
    if flow_conflict: discount += 12.0
    if fresh.startswith("STALE"): discount += 6.0
    elif fresh.startswith("FUTURE"): discount += 12.0
    if filing_date and info_asof and filing_date > info_asof: discount += 12.0
    if source_status.startswith("FAILED"): discount += 2.0
    # Missing optional source is not a formal veto; it is visibly unverified.
    source_filing_ok = bool(source and filing_date and info_asof and filing_date <= info_asof)
    status = "CONFLICT｜證據矛盾" if conflict else ("STALE｜關鍵資料過期" if fresh.startswith("STALE") else
             "PARTIAL｜來源/資料不足" if not source_filing_ok or not has_official_flows else "VERIFIED｜來源與方向一致")
    value = max(0, min(100, score - discount))
    return {
        "H110版本": VERSION,
        "H110行情交易日": str(market_day) if market_day else "UNKNOWN",
        "H110研究資訊截止日": str(info_asof) if info_asof else "UNKNOWN",
        "H110研究模式": asof_mode,
        "H110證據狀態": status,
        "H110營收來源驗證": "TWSE_公告日期已核對" if source_filing_ok else "NO_VERIFIED_PUBLICATION_DATE" if source else "NO_VERIFIED_SOURCE",
        "H110營收資料年月": month,
        "H110官方營收新鮮度": fresh,
        "H110法人方向驗證": "CONFLICT" if flow_conflict else "OFFICIAL_DATED" if has_official_flows else "UNVERIFIED",
        "H110證據扣分": round(discount, 2),
        "H110最終研究優先分": round(value, 2),
        "H110主榜准入": "否｜證據矛盾待複核" if conflict else "需通過原H109風險條件",
        "H110證據說明": "；".join(notes),
    }


__all__ = ["VERSION", "research_asof", "audit"]

# -*- coding: utf-8 -*-
"""Official post-close release timing helpers for GodPick V190.

The TWSE Data E-Shop documents the per-security three-institution net buy/sell
file as being produced twice on each trading day:
- 18:00: TWT86UC, excluding block trades
- 20:00: TWTAIUC, including block trades

These times describe TWSE's official file production schedule.  Public website
or OpenAPI synchronization can occur slightly later, therefore GodPick uses a
small *system* grace period after 20:00 before treating a still-T-1 cache as an
update anomaly.  The grace period is a GodPick policy, not a TWSE-published
production time.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

TAIPEI_TZ = ZoneInfo("Asia/Taipei")
TWSE_MARKET_CLOSE = time(13, 30)
TWSE_T86_FIRST_RELEASE = time(18, 0)
TWSE_T86_FINAL_RELEASE = time(20, 0)
GODPICK_FINAL_RELEASE_GRACE_MINUTES = 20
RELEASE_TIMING_VERSION = "v190_twse_t86_release_timing_20260812"


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "nat", "<na>"}:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt).date()
        except Exception:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except Exception:
        return None


def _as_taipei(now: datetime | None = None) -> datetime:
    if now is None:
        return datetime.now(TAIPEI_TZ)
    if now.tzinfo is None:
        return now.replace(tzinfo=TAIPEI_TZ)
    return now.astimezone(TAIPEI_TZ)


def _weekday_lag(older: date | None, newer: date | None) -> int:
    if older is None or newer is None:
        return 999
    if older >= newer:
        return 0
    cur = older
    count = 0
    while cur < newer:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            count += 1
    return count


def evaluate_twse_t86_release_timing(
    *,
    market_date: Any,
    official_date: Any,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a deterministic timing/governance context for TWSE T86 data.

    ``market_date`` is the latest K-line / market trading date being evaluated.
    ``official_date`` is the conservative date of the official factor snapshot.
    """
    now_tw = _as_taipei(now)
    mkt = _parse_date(market_date)
    off = _parse_date(official_date)
    lag = _weekday_lag(off, mkt)

    base = {
        "version": RELEASE_TIMING_VERSION,
        "now_taipei": now_tw.strftime("%Y-%m-%d %H:%M:%S"),
        "market_date": mkt.isoformat() if mkt else "",
        "official_date": off.isoformat() if off else "",
        "official_lag": lag,
        "twse_first_release": "18:00",
        "twse_final_release": "20:00",
        "system_grace_minutes": GODPICK_FINAL_RELEASE_GRACE_MINUTES,
        "source_note": "18:00/20:00為TWSE三大法人買賣超檔官方產製時間；公開網站/OpenAPI同步可能稍晚。",
        "t1_is_normal_now": False,
        "same_day_final_expected": False,
        "level": "warning",
        "phase": "UNKNOWN",
        "headline": "官方因子時序未驗證",
        "detail": "缺少市場日期或官方因子日期。",
        "next_milestone": "",
    }
    if mkt is None or off is None:
        return base
    if off >= mkt:
        base.update({
            "phase": "T0_READY",
            "level": "success",
            "headline": "官方因子已對齊當日交易資料",
            "detail": f"官方因子 {off.isoformat()} 已對齊市場 {mkt.isoformat()}。",
            "same_day_final_expected": now_tw.date() == mkt and now_tw.time() >= TWSE_T86_FINAL_RELEASE,
        })
        return base
    if lag >= 2:
        base.update({
            "phase": "STALE",
            "level": "error",
            "headline": "官方因子已落後超過1個交易日",
            "detail": f"官方因子 {off.isoformat()} 相對市場 {mkt.isoformat()} 落後 {lag} 個交易日。",
        })
        return base

    # T-1 can be a normal state only while evaluating today's trading date.
    if now_tw.date() != mkt:
        base.update({
            "phase": "HISTORICAL_T1",
            "level": "warning",
            "headline": "官方因子為歷史T-1資料",
            "detail": f"市場資料日為 {mkt.isoformat()}，目前已非該交易日盤後產製時窗；建議重新更新官方因子。",
        })
        return base

    t = now_tw.time()
    if t < TWSE_MARKET_CLOSE:
        base.update({
            "phase": "LIVE_SESSION_T1",
            "level": "info",
            "headline": "盤中T-1為正常最新完整基準",
            "detail": "當日交易尚未收盤，逐檔三大法人當日盤後資料尚未產製；使用前一交易日已驗證資料屬正常時序。",
            "t1_is_normal_now": True,
            "next_milestone": "18:00 TWSE TWT86UC首版（不含鉅額）",
        })
    elif t < TWSE_T86_FIRST_RELEASE:
        base.update({
            "phase": "WAIT_FIRST_T86",
            "level": "info",
            "headline": "盤後官方資料尚在首版產製前時窗",
            "detail": "TWSE逐檔三大法人首版官方檔於18:00產製；目前T-1是當下最新完整可驗證基準，不是更新失敗。",
            "t1_is_normal_now": True,
            "next_milestone": "18:00 TWT86UC（不含鉅額）",
        })
    elif t < TWSE_T86_FINAL_RELEASE:
        base.update({
            "phase": "WAIT_FINAL_T86",
            "level": "info",
            "headline": "TWSE當日法人首版已進入產製時窗",
            "detail": "18:00為不含鉅額首版；20:00才產製含鉅額完整版。若目前仍採T-1，系統視為正常降級資料，不視為錯誤。",
            "t1_is_normal_now": True,
            "next_milestone": "20:00 TWTAIUC（含鉅額）",
        })
    else:
        final_dt = datetime.combine(now_tw.date(), TWSE_T86_FINAL_RELEASE, tzinfo=TAIPEI_TZ)
        grace_until = final_dt + timedelta(minutes=GODPICK_FINAL_RELEASE_GRACE_MINUTES)
        if now_tw < grace_until:
            base.update({
                "phase": "FINAL_RELEASE_GRACE",
                "level": "info",
                "headline": "TWSE含鉅額完整版已到產製時間，系統等待同步緩衝",
                "detail": f"20:00為TWSE含鉅額版官方產製時間；GodPick另保留{GODPICK_FINAL_RELEASE_GRACE_MINUTES}分鐘同步緩衝，避免公開端點延遲被誤判為錯誤。",
                "t1_is_normal_now": True,
                "same_day_final_expected": True,
                "next_milestone": grace_until.strftime("%H:%M") + " 後若仍T-1則提示重新更新",
            })
        else:
            base.update({
                "phase": "T0_EXPECTED",
                "level": "warning",
                "headline": "當日完整法人資料已到應產製時段",
                "detail": f"TWSE 20:00含鉅額版已到官方產製時間，且已超過GodPick {GODPICK_FINAL_RELEASE_GRACE_MINUTES}分鐘同步緩衝；若仍為T-1，建議重新更新官方因子並檢查來源。",
                "t1_is_normal_now": False,
                "same_day_final_expected": True,
                "next_milestone": "立即重新更新官方因子快取",
            })
    return base

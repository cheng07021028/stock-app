# -*- coding: utf-8 -*-
"""V191-H21/H22 Page07 same-run market snapshot preflight.

H21 prevents formal/A- classification from using a stale checkout snapshot.
H22 first restores the newest business-date macro authority from runtime-data and,
after a same-run refresh, queues the newer local macro state back through the
existing durability layer.  Repository tests stay offline/deterministic.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from typing import Any


VERSION = "v191_h22_page07_same_run_market_preflight_20260814"
TAIPEI_TZ = timezone(timedelta(hours=8))
MARKET_FILE = "market_snapshot.json"
RUNTIME_FRESH_TTL_SECONDS = 45 * 60
MAX_MARKET_CALENDAR_AGE_DAYS = 10


def _now() -> datetime:
    return datetime.now(TAIPEI_TZ)


def _safe_text(value: Any) -> str:
    try:
        return "" if value is None else str(value).strip()
    except Exception:
        return ""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _parse_datetime(value: Any) -> datetime | None:
    text = _safe_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(text[:19], fmt)
            return dt.replace(tzinfo=TAIPEI_TZ)
        except Exception:
            continue
    return None


def _parse_date(value: Any):
    text = _safe_text(value)[:10]
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            continue
    return None


def _market_business_date(payload: dict[str, Any]) -> str:
    for key in ("market_date", "twse_data_date", "otc_data_date", "data_date"):
        text = _safe_text(payload.get(key))[:10]
        if _parse_date(text) is not None:
            return text
    return ""


def _runtime_snapshot_fresh(payload: dict[str, Any], *, ttl_seconds: int) -> bool:
    if not payload or not _market_business_date(payload):
        return False
    updated = _parse_datetime(
        payload.get("updated_at") or payload.get("snapshot_time") or payload.get("time")
    )
    if updated is None:
        return False
    age = (_now() - updated).total_seconds()
    return -300 <= age <= max(60, int(ttl_seconds))


def _market_date_sane(payload: dict[str, Any]) -> bool:
    market_date = _parse_date(_market_business_date(payload))
    if market_date is None:
        return False
    age_days = (_now().date() - market_date).days
    # Long exchange holidays are tolerated, but a repository snapshot that is
    # weeks/months old is never allowed to drive a new formal recommendation.
    return -1 <= age_days <= MAX_MARKET_CALENDAR_AGE_DAYS


def _repository_test_runtime() -> bool:
    try:
        if "pytest" in sys.modules:
            return True
        argv0 = str(sys.argv[0] if sys.argv else "").replace("\\", "/").lower()
        normalized = "/" + argv0.lstrip("/")
        return "/tests/" in normalized or argv0.startswith("tests/")
    except Exception:
        return False


def _restore_h22_macro_authority(*, force: bool, queue_newer_local: bool) -> dict[str, Any]:
    if _repository_test_runtime():
        return {"ok": True, "mode": "repository_test_offline"}
    try:
        from macro_runtime_authority import ensure_macro_runtime_authority_current
        return ensure_macro_runtime_authority_current(
            force=force,
            queue_newer_local=queue_newer_local,
        )
    except Exception as exc:
        return {"ok": False, "message": f"H22 macro authority restore exception: {type(exc).__name__}: {exc}"}


def ensure_page07_market_snapshot_current(
    *,
    base_dir: str | Path | None = None,
    max_runtime_seconds: int = 35,
    fresh_ttl_seconds: int = RUNTIME_FRESH_TTL_SECONDS,
) -> dict[str, Any]:
    """Hydrate and validate Page07 market data in the current process.

    H22 authority restore runs before local inspection, so a redeployed checkout
    cannot make the packaged July snapshot win merely because its filesystem mtime
    is newer.  If no usable authority exists, H21 performs a bounded same-run
    market refresh.  Failure is fail-closed: no false 0/0 recommendation is saved.
    """
    root = Path(base_dir or Path(__file__).resolve().parent).resolve()
    market_path = root / MARKET_FILE

    authority_before = _restore_h22_macro_authority(force=False, queue_newer_local=False)
    before = _read_json(market_path)
    before_date = _market_business_date(before)

    if _runtime_snapshot_fresh(before, ttl_seconds=fresh_ttl_seconds) and _market_date_sane(before):
        return {
            "version": VERSION,
            "ok": True,
            "mode": "reuse_same_runtime_snapshot",
            "market_date": before_date,
            "refreshed": False,
            "h22_authority": authority_before,
            "message": f"H22/H21 Page07大盤預檢通過：沿用最新權威快照 {before_date}",
        }

    refresh_result: dict[str, Any] = {}
    refresh_error = ""
    try:
        # Never sync runtime files to the deployment branch.  H22 durability uses
        # runtime-data/Firestore after the refresh is proven usable.
        from macro_startup_service import _run_fast_update

        raw = _run_fast_update(
            sync_github=False,
            max_runtime_seconds=max(10, min(45, int(max_runtime_seconds))),
        )
        refresh_result = raw if isinstance(raw, dict) else {"ok": bool(raw), "raw": raw}
    except Exception as exc:  # fail closed below after inspecting any usable file
        refresh_error = f"{type(exc).__name__}: {exc}"

    after = _read_json(market_path)
    after_date = _market_business_date(after)
    runtime_fresh = _runtime_snapshot_fresh(after, ttl_seconds=max(fresh_ttl_seconds, 60 * 60))
    date_sane = _market_date_sane(after)
    refresh_ok = bool(refresh_result.get("ok"))

    if runtime_fresh and date_sane and (refresh_ok or after_date != before_date or not before_date):
        authority_after = _restore_h22_macro_authority(force=True, queue_newer_local=True)
        final = _read_json(market_path)
        final_date = _market_business_date(final) or after_date
        return {
            "version": VERSION,
            "ok": True,
            "mode": "same_run_market_refresh",
            "market_date": final_date,
            "before_market_date": before_date,
            "refreshed": True,
            "refresh_ok": refresh_ok,
            "refresh_message": _safe_text(refresh_result.get("message")),
            "h22_authority_before": authority_before,
            "h22_authority_after": authority_after,
            "message": (
                f"H22/H21 Page07同輪大盤權威已重建：{before_date or '無'} → {final_date}；"
                "較新資料已排入runtime-data永久化，正式/A-仍使用原 Risk/RR/官方因子與日期對齊門檻"
            ),
        }

    details = (
        f"原快照={before_date or '無'}；更新後={after_date or '無'}；"
        f"refresh_ok={refresh_ok}；runtime_fresh={runtime_fresh}；date_sane={date_sane}"
    )
    if refresh_error:
        details += f"；refresh_error={refresh_error}"
    elif refresh_result:
        details += f"；refresh_message={_safe_text(refresh_result.get('message'))[:180]}"

    raise RuntimeError(
        "V191-H22 Page07大盤資料權威預檢失敗，拒絕執行股神推薦："
        "目前大盤快照仍不可確認為本輪可用資料。若繼續執行，正式推薦/A-準主推薦會因共同日期閘門被整批歸零。"
        f"｜{details}"
    )


__all__ = [
    "VERSION",
    "ensure_page07_market_snapshot_current",
]

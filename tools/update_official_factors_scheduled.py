# -*- coding: utf-8 -*-
"""V141 GitHub Actions / 本機排程用官方因子更新腳本。

用法：
  python tools/update_official_factors_scheduled.py

重點：
- GitHub Actions 建議每 30 分鐘觸發一次。
- 此腳本會讀取 data/config/official_factor_schedule_settings.json。
- 只有目前台灣時間符合設定的 times，才真正更新。
- workflow_dispatch 手動執行時會略過時間檢查，立即更新。
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_system_health_service import load_schedule_settings
from official_factor_service import build_official_factor_cache


def _normalize_times(value) -> list[str]:
    if isinstance(value, str):
        items = [value]
    elif isinstance(value, list):
        items = value
    else:
        items = ["23:00"]
    out: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if len(text) == 5 and text[2] == ":" and text[:2].isdigit() and text[3:].isdigit():
            out.append(text)
    return out or ["23:00"]


def _should_run_now(cfg: dict, now_tw: datetime) -> tuple[bool, str]:
    if not cfg.get("enabled", True):
        return False, "Official factor scheduled update is disabled. Skip."

    # 手動 workflow_dispatch 或本機強制執行時，不受排程時間限制。
    if os.getenv("GITHUB_EVENT_NAME") == "workflow_dispatch" or os.getenv("OFFICIAL_FACTOR_FORCE_RUN") == "1":
        return True, "Manual/forced run. Time gate skipped."

    if bool(cfg.get("weekdays_only", True)) and now_tw.weekday() >= 5:
        return False, f"Skip weekend by setting. now_tw={now_tw.strftime('%Y-%m-%d %H:%M')}"

    times = _normalize_times(cfg.get("times"))
    current_slot = now_tw.strftime("%H:%M")
    if current_slot not in times:
        return False, f"Skip by configured time. now_tw={current_slot}, configured={times}"

    return True, f"Run by configured time. now_tw={current_slot}, configured={times}"


def main() -> int:
    cfg = load_schedule_settings()
    tz_name = str(cfg.get("timezone") or "Asia/Taipei")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("Asia/Taipei")
        tz_name = "Asia/Taipei"
    now_tw = datetime.now(tz).replace(second=0, microsecond=0)

    print("Official factor scheduled update check")
    print(json.dumps({
        k: cfg.get(k)
        for k in ["enabled", "timezone", "times", "weekdays_only", "market_filter", "limit", "include_institutional", "include_revenue", "include_valuation"]
    }, ensure_ascii=False))

    should_run, reason = _should_run_now(cfg, now_tw)
    print(reason)
    if not should_run:
        return 0

    df, meta = build_official_factor_cache(
        limit=int(cfg.get("limit") or 0) or None,
        market_filter=str(cfg.get("market_filter") or "全部"),
        include_institutional=bool(cfg.get("include_institutional", True)),
        include_revenue=bool(cfg.get("include_revenue", True)),
        include_valuation=bool(cfg.get("include_valuation", True)),
        save=True,
        quick_mode=False,
        max_runtime_seconds=480,
        max_requests=260,
        finmind_bulk_only=False,
        finmind_max_stocks=200,
    )
    print(f"Updated records={len(df)}, complete_count={meta.get('complete_count', 0)}, saved={meta.get('saved')}")
    for msg in (meta.get("diagnostics") or [])[-20:]:
        print("-", msg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

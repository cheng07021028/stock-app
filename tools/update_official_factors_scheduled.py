# -*- coding: utf-8 -*-
"""V112 GitHub Actions / 本機排程用官方因子更新腳本。

用法：
  python tools/update_official_factors_scheduled.py

此腳本只更新 official_factors_cache.json 與 official_factors_update_log.json，
commit/push 由 GitHub Actions workflow 負責。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_system_health_service import load_schedule_settings
from official_factor_service import build_official_factor_cache


def main() -> int:
    cfg = load_schedule_settings()
    if not cfg.get("enabled", True):
        print("Official factor scheduled update is disabled. Skip.")
        return 0
    print("Official factor scheduled update start")
    print(json.dumps({k: cfg.get(k) for k in ["timezone", "times", "weekdays_only", "market_filter", "limit", "include_institutional", "include_revenue", "include_valuation"]}, ensure_ascii=False))
    df, meta = build_official_factor_cache(
        limit=int(cfg.get("limit") or 0) or None,
        market_filter=str(cfg.get("market_filter") or "全部"),
        include_institutional=bool(cfg.get("include_institutional", True)),
        include_revenue=bool(cfg.get("include_revenue", True)),
        include_valuation=bool(cfg.get("include_valuation", True)),
        save=True,
    )
    print(f"Updated records={len(df)}, complete_count={meta.get('complete_count', 0)}, saved={meta.get('saved')}")
    for msg in (meta.get("diagnostics") or [])[-20:]:
        print("-", msg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

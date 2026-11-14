StockApp V191-H158 | Page17 Scheduler Reliability / SuperAI Evidence & Persistence Fix
2026-11-14 | Independent minimum overlay for GitHub main 03a643e05d32ca78342a4ed9e2d037714b8b0a90

Purpose
- Fix Page17 false-overdue display on non-trading Saturday/Sunday (weekdays_only on).
- Restore SuperAI TAIFEX PCR + foreign TX futures net OI verification via official OpenAPI (strict numeric/date/contract/source checks). Legacy parsed HTML/Excel source remains an unverified research fallback and cannot be certified as complete.
- Make SuperAI batch job SUCCESS only when >100 fresh margin stock observations AND official TAIFEX PCR/TX net OI are verified. Missing data remains WARNING/FAILED; never fabricate a signal.
- Prevent failed historical-performance or background-sync jobs, after an actual attempt of the final daily slot, from permanently blocking independently verified research recomputation. Page07 gets WARNING when an advisory predecessor is bad; five H155 core data/authority gates remain hard SUCCESS requirements; H126 trade authority untouched. H156 manual full batch remains strict same-batch ALL predecessor SUCCESS.
- Enable Page08 scheduled performance persistence to request synchronous remote verification (instead of treating asynchronous queued sync as completed); if verification fails report FAILED and preserve H7 retry and old data.
- Surface SuperAI, Page08, Page07 current issue/last successful time in Page17.
- Add pinned-bounds HTML parser dependencies, preserving existing Streamlit/Starlette versions.

Files included (overwrite the SAME relative paths on an already H156/H157-equivalent main):
  godpick_auto_scheduler.py
  godpick_auto_update_tasks.py
  pages/17_系統健康檢查.py
  godpick_super_ai_market_context.py
  godpick_h158_taifex_openapi.py (NEW)
  requirements.txt
  tests/test_h156_page17_parallel_refresh.py (updated regression tests)
  tests/test_h158_scheduler_recovery.py (NEW)
  tests/test_h158_taifex_openapi.py (NEW)

Verification
- 2026-11-14 local simulated suite: 37 tests PASS, including original H155 and updated H156 tests plus H158 tests.
- Python syntax checks pass. ZIP UTF-8 Chinese relative paths and integrity verified.
- External TAIFEX APIs, Streamlit Cloud deployment, GitHub Actions worker, and remote persistence are NOT live-tested by offline simulation.

Observed incident root cause (real runtime-data, not an invented test):
  2026-11-13 super_ai_market_context.json had 1886 margin stocks but empty taifex object; diagnostics indicated missing optional lxml for PCR/futures legacy parsers. Page07 was blocked; last SUCCESS Oct 19.
  Page08 record_performance repeatedly FAILED on remote confirmation, whereas separate durability_retry later confirmed recovery. Other market provenance jobs H122/H123/H137 have separate missing verified official source/archives and are NOT made SUCCESS by this patch.

Deploy/Acceptance
1. Backup main. Keep same relative paths; do NOT remove any other files.
2. Do NOT overwrite runtime-data, user settings, Secrets, H126 trading policy, or historical recommendations.
3. On next market trading day after vendor data release, check 17: official_factors SUCCESS, SuperAI fresh margin date + OpenAPI PCR/TX dates, Page07 actual run and latest success date, Page08 permanent_ok, and remaining warnings.
4. Confirm GitHub Actions worker logs; a green workflow by itself may be routine soft-fail and is NOT data-level SUCCESS. If official TAIFEX data is missing/stale or remote sync unavailable, keep WARNING/FAILED rather than claiming success.

No live trading recommendation or probability claim. Code patch only; not auto-deployed to GitHub main.

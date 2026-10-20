StockApp V191-H139.0 | AUTO SYNC RECOVERY & RESEARCH SETTLEMENT
2026-10-20 | Incremental cumulative update for H128-H139

1. BASELINE AND DEPLOYMENT
- Intended baseline: 2026-10-15 user-uploaded stock-app-main.zip (SHA-256:
  08fcd57775091a8a1bfd5bb8448ce183408f472d2fbe7971e13a6cb6dc79b5d6).
- This H139 ZIP contains the H128-H139 necessary cumulative program overlay.
- FIRST BACK UP active code, runtime-data branch and permanent recommendations.
- If the live deployed branch has other changes, DIFF and merge; do not delete
  the existing repo or overwrite any remote authority records.
- Apply the ZIP preserving original relative paths. Do not unzip the original
  2026-10-15 raw baseline again on top of a newer live production environment.
- Requires existing GitHub runtime-data branch, GitHub Actions contents:write,
  H117 EOD source, H134 decision archives, H136/H137 existing workflows.
- Workflow h137_post_sync_audit.yml now runs H139 bounded repair BEFORE H137
  audit, H138 brief AFTER audit, then H139 settlement and publishing.
- Weekdays Taiwan 23:00; schedule may be delayed and holidays still require
  calendar/manual closure handling; H136 still runs 22:30.

2. H139 BOUNDED RECOVERY
- If H136 cache is current and internally verified, skip rebuilding.
- Otherwise rebuild ONCE from pre-existing H134/H117/H116 source files into an
  isolated temporary directory. Validate SHA, row counts, and today's EOD
  date BEFORE copying any new cache files to runtime-data.
- If input is missing/stale or source verification fails, preserve existing
  H136 cache and publish FAILED receipt. No fabricated prices, no retry loop.
- Receipt: runtime-data/data/h139/H139_recovery.json.
- Internal SHA does NOT prove official source authenticity independently.

3. H139 RESEARCH PERFORMANCE SETTLEMENT
- Accepts only current-day H136 verified derived data and H137 same-day audit.
- Fixed historical H126 Top3 / Top5 research cohorts and real benchmark sessions;
  T+1/T+3/T+5 mean stock close-to-close vs index, in percentage points.
- Missing a single constituent price invalidates the entire cohort; NEVER
  average only surviving winners or fill suspensions with stale prices.
- Reports incomplete days, coverage, newly matured cohorts, research-only
  mean alpha and fixed 15-day-vs-15-day descriptive drift advisory.
- No independently timestamped PIT proof, dividend-adjusted total return,
  fees/taxes or brokerage executions; therefore NOT real trade P&L.
- No automatic portfolio recommendation, AI weight change, trading or orders.
- Receipt: runtime-data/data/h139/H139_settlement.json; view on Page 27.

4. FILES NEW / UPDATED
New: godpick_h139_settlement.py
New: tools/h139_ops.py
New: tests/test_h139_auto_settlement.py
Changed: pages/27_AI自動資料中樞.py
Changed: .github/workflows/h137_post_sync_audit.yml

5. TESTS / ACCEPTANCE
- Offline synthetic/mocked tests; not real cloud deployment.
- Run: python -m pytest -q tests/test_h139_auto_settlement.py
- Run H128..H139 versioned tests and H115 regression; compileall.
- After deployment: manually dispatch H136, then H137/H139 workflow, inspect
  H139_recovery.json / H139_settlement.json, Page 27, and Reboot readback.
- Test a missing EOD day and a broken H136 SHA; neither may show settled P&L.
- Verify existing recommendation/history/settings files unchanged.
- NO production GitHub, Streamlit, Firestore or brokerage integrations have been
  exercised by local tests.  No external notification service is configured.

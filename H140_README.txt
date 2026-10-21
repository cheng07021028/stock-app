STOCK-APP V191-H140.0 | OPERATIONS OBSERVABILITY & LIVE CUTOVER CHECKLIST

BASELINE:
- user-uploaded stock-app-main.zip SHA256:
  08fcd57775091a8a1bfd5bb8448ce183408f472d2fbe7971e13a6cb6dc79b5d6
- This cumulative overlay contains H128..H140 necessary changes; not the entire repo.
- Always compare current GitHub main with these files before applying. The live
  repo was inspected read-only, NOT changed by the builder. Do not overwrite
  newer live code, credentials, recommendations, settings, or remote history.

H140 CORE:
1. Read current local runtime-data checkout's H136/H137/H138/H139 receipts.
   Each stage requires matching schema, exact research day, and approved status.
   Unverifiable, missing, malformed, future, old or blocked => not ready.
2. Emit data/h140/H140_monitor.json with stage-level state and short repair
   playbook. Contains no token, portfolio, private records or actual P&L.
3. Add the observer as a late stage to existing H137 audit workflow (23:00
   Asia/Taipei on weekdays); commit only the status JSON to runtime-data.
   Fail GitHub Actions clearly when any stage is blocked, rather than treating
   a stale dataset or missing settlement as a green deployment.
4. Page 27 shows the five stage states, date, diagnostic code and remedies.
   It fetches remote receipt read-only, does not run a remote job, and does not
   change ranking, recommendations or transaction records.
5. A manual market_closed flag never means official market closure proven;
   it gets a NOT_ATTESTED status. Same applies to internal SHA/PIT verification.

NEW FILES:
- godpick_h140_observability.py
- tools/h140_ops.py
- tests/test_h140_ops_observability.py
CHANGED FILES:
- pages/27_AI自動資料中樞.py
- .github/workflows/h137_post_sync_audit.yml

PREDEPLOY FINDING (2026-10-20, GitHub connector READ-ONLY):
- cheng07021028/stock-app main contained H139 modules and workflow content.
- runtime-data branch exists, but expected runtime-data/data/h136/H136_health.json,
  data/h137/H137_audit.json and data/h139/H139_settlement.json were NOT_FOUND.
- Missing receipts are not proof that an Action never ran; inspect Actions
  logs, branch permissions, H134/H117 input existence and actual job status.

LIVE ACCEPTANCE PROCEDURE:
A) Back up main, runtime-data and authoritative recommendations separately.
B) Compare changed files, apply only code overlay, and enable Actions
   contents:write for the deployment. Ensure H117 source and H134 archives.
C) Manually dispatch H136 nightly workflow, inspect generated H136 health,
   manifest and data dates in runtime-data. Do NOT fake missing EOD sessions.
D) Dispatch H137 workflow; H139 recovery and settlement then H140 run. Check
   `runtime-data/data/h140/H140_monitor.json`, its five stages, and workflow
   step summary. Missing input must lead to BLOCKED_NEEDS_OPERATOR (not PASS).
E) Open Streamlit Page 27, press H140 refresh. Confirm current-day receipt,
   match real Actions run and branch SHAs. Reboot app; compare again.
F) Test on 2+ actual trading days and one known failure (stale H136 EOD).
G) Verify H109/H126 trade gates unchanged; confirm Page8 deletions, category
   preferences and 2,490+ live records are not overwritten by this ZIP.

LIMITATIONS:
- No external Actions-run API listing, Cloud runtime E2E, Firestore, real
  forward returns, brokerage transactions, independent PIT/WORM attestation.
- Does not trigger a new scan, auto-buy, auto-approve, auto-repair or recreate
  missing H117 market prices. H139 bounded retry remains the sole repair path.
- Synthetic/mock unit test PASS is NOT remote deployment acceptance.

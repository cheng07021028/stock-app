StockApp V191-H142.0 | Sector FADE Evidence Transparency
======================================================
Prerequisite: Live code must include the V191-H141 Page07 hook. Compare
current GitHub main to the enclosed production files before merging.
This is a small incremental PATCH, not a full repository or data backup.

PRODUCTION FILES (merge same relative paths):
  godpick_h142_evidence_review.py            [NEW]
  godpick_h105_future_blackhorse_engine.py   [CHANGED]
  pages/7_股神推薦.py                         [CHANGED]
TEST:
  tests/test_h142_sector_evidence.py         [NEW]

What changes:
- H108 includes the previous-date sample confidence and state in enriched rows.
- H142 reviews each H109 FADE label against dated *internal* breadth and
  capital flow deltas. Missing/ambiguous/different-day signals remain UNVERIFIED.
- Supervisor export keeps H141 safety summary, appends H142 counts there,
  adds only one page '00D_H142風控來源稽核', and exposes H108 fields in the
  existing 05 evidence sheet. No extra interactive application page.
- Duplicates are grouped by ticker. This is not a formal trading signal.

CRITICAL FAIL-CLOSED BEHAVIOR:
- H109 FADE_VETO, H110/H113/H123/H124/H126 and original Formal trade
  authority remain unchanged. Do not treat H142 proof grades as permission
  to enter a position; they diagnose unverified assertions.
- The exported 2026-10-21 H141 workbook did NOT contain original H108
  per-sector dated momentum and breadth. 'UNVERIFIED_FADE_LABEL' means those
  observations cannot be independently inspected FROM THAT EXPORT, not that
  the underlying H108 engine had no observations at all.
- No GitHub, Firestore, secrets, token, JSON, settings, old backups or
  recommendation history are part of this ZIP.

Verification on synthetic + existing tests:
  python -m pytest -q tests/test_h125_reboot_categories.py ...
  [H125-H142 selected 394/394 PASS]
  Baseline legacy h108_full_regression.py assertion for 9991 failed BEFORE
  this patch as well as AFTER; don't claim whole repo's legacy tests pass.
  Compiled the whole project and validated clean H141 -> H142 overlay.

Deployment:
1. Backup main and runtime-data / Firestore BEFORE merging. Never replace
   runtime-data by a checkout made from stale ZIPs.
2. Compare the three production paths to current main; resolve any newer code.
3. Commit the merge to main and let Streamlit deploy.
4. Create a NEW H126 supervisor Excel on an actual open market day.
5. Check 00C_H141 output quality, 00D_H142 source audit, 05 evidence details.
6. If the live H108 source still lacks dated breadth/flow, keep STOP and fix
   the H108 upstream source before attempting trade/predictive improvements.
7. Do not claim GitHub deployment or higher stock performance until verified.

Historical source workbook available separately: StockReview_20261021_H142.xlsx
This XLSX is an analysis COPY derived from H141, not a new market recommendation.

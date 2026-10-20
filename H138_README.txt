StockApp V191-H138.0 | Unattended H126 H134 bridge / safe manager brief
2026-10-20

BASE
  stock-app-main.zip SHA256 08fcd57775091a8a1bfd5bb8448ce183408f472d2fbe7971e13a6cb6dc79b5d6
  plus H137 cumulative necessary files.

DELIVERY
  StockApp_V191_H138.zip is a cumulative H128-H138 CODE-ONLY overlay.
  Extract over the SAME verified stock-app root without deleting other files.
  Back up the true LIVE GitHub/Firestore records and settings FIRST.
  Never replace newer repository edits without a path-by-path diff/merge.
  No godpick_records.json, godpick_user_settings.json, token or secrets.

WHAT IT DOES
  1) Existing V191 central scheduler already implements Page07 background scan
     via the canonical Page07 headless callable; H138 does NOT create a second picker.
  2) After an actual successful scheduled Page07 run, the adapter checks run ID,
     execution clock, same-session H97-authorized H79 core, H126 output and three
     H117 same-date TWSE/TPEx/TAIEX official-EOD internal hash receipts.
  3) If every gate passes, attempt H134 create-only GitHub runtime-data archive
     with source_type=SCHEDULED_PAGE07_H126; only remote GET readback is confirmed.
     A Page07 scan can succeed while H138 archive remains UNCONFIRMED (warning).
  4) H135 and H136 accept valid H134 unattended source as RESEARCH ONLY (no trades).
  5) H137 23:00 workflow also builds H138_manager_brief.json from the H137 audit
     and H134 readback-verified snapshots. Page27 shows latest brief / blockers.

DEPLOYMENT CHECKLIST (important!)
  - Central V191 scheduler is DISABLED by default. Confirm settings in Page17;
    enable only after official data/source and storage health are confirmed.
  - Check main branch contains H117 official EOD archives with date-matched
    TWSE/TPEX/TAIEX; scheduler 20:55 initial slot is held until prerequisites
    complete by the existing H28 final gate. GitHub hosted fallback must run.
  - Ensure GITHUB_TOKEN contents:write on Actions and runtime-data branch exists.
  - Trigger ONE controlled real trade-day scheduled test; inspect scheduler
    godpick_recommendation job details.h138_auto_seal; must say REMOTE_CONFIRMED.
  - Confirm matching H134_YYYYMMDD... snapshot is actually on runtime-data and
    survives Streamlit Reboot; confirm H136 22:30 then H137 23:00 receipt and
    Page27 H138 brief. On a closed day no made-up same-day report is authorized.
  - GitHub Actions dispatch, real remote token and Streamlit Cloud were NOT
    tested inside the offline package build.

REMAINING LIMITATIONS
  - H134 GitHub create-only is not WORM nor independent PIT timestamp.
  - Complete H117 EOD source and CURRENT H97 core are STRICT requirements;
    incomplete/async core or source means no auto-seal (safe failure).
  - No independent holiday feed (weekday without EOD is NOT accepted as open).
  - H138 brief is operational/research readiness, not real P&L or price forecast.
  - No brokerage, automatic order, new formal buy authority or strategy weight
    modification. H109/H126 formal trade gates remain unchanged.

TESTS
  pytest -q tests/test_h128_upgrade.py tests/test_h129_wallstreet_audit.py ...
           tests/test_h137_auto_sync.py tests/test_h138_unattended.py
  Additional H115 PIT suite and full compileall run by maintainer.

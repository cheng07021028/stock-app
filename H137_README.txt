StockApp V191-H137.0 | cumulative production overlay H128-H137
===========================================================
Base: stock-app-main.zip (2026-10-15) SHA256
08fcd57775091a8a1bfd5bb8448ce183408f472d2fbe7971e13a6cb6dc79b5d6

IMPORTANT: H137 is an event-driven auto-archive + nightly audit, NOT an unattended
full-market H126 scan. Daily signals still require a real Page07 H126 report build.
No unattended buys, auto-promotion, false future prices, private portfolio import,
or retrospective claim of independent PIT proof.

NEW
  godpick_h137_auto_seal.py              admin-only new-report archiver/receipt/remote status
  tools/h137_ops_audit.py                 post-H136 GitHub runtime-data health audit
  .github/workflows/h137_post_sync_audit.yml   weekdays 23:00 Asia/Taipei
  tests/test_h137_auto_sync.py           offline/fake-GitHub failure and security tests
CHANGED
  pages/7_股神推薦.py                      H137 auto-seal after fresh fast/full workbook build
  pages/25_主管智慧決策戰情.py             H134 manual save remains backup/recovery
  pages/27_AI自動資料中樞.py             remote post-sync integrity alarm

WORKFLOW
1. When an authenticated admin builds NEW H126 manager Excel from Page07,
   H137 attempts H134 create-only upload to GitHub runtime-data and GET readback.
   If the save cannot be confirmed, the Excel remains downloadable and the UI
   clearly says the remote archive is NOT confirmed. No repeated writes on
   cached Excel download; weekend/invalid evidence/future day rejected.
2. Existing H136 workflow reads already stored H134 decisions + H117 EOD quotes,
   publishes derived research cache at 22:30 Asia/Taipei weekdays.
3. New H137 workflow runs ~23:00 and checks: valid same-day H126 live archive,
   H136 health as-of today, cache files SHA, and today's individual/benchmark
   last data date. Failure writes data/h137/H137_audit.json and marks the
   GitHub Actions run as failed. Alerts through email/Slack are NOT automatically
   delivered without configuring GitHub notification settings or an integration.
4. Page27 reads H137 status read-only and shows blockers.

OPERATION / PREDEPLOY
- Backup current GitHub deployed code and remote records/settings first.
- This ZIP contains ONLY overlay code/workflow/tests; NO user json/secrets/token.
- If current deployed branch differs from the 2026-10-15 base, DIFF and MERGE,
  never overwrite a newer branch with stale code or bundled old records.
- Apply ZIP paths at repository root, commit workflow to default branch.
- Verify GitHub Actions: permissions=contents:write, runtime-data exists, and
  the H117 + H136 production workflows actually succeed. If GitHub's org
  policy blocks Actions writes, automation will be NOT CONFIRMED.
- Open Page07 as admin and build FRESH manager Excel. Verify H137 remote readback.
- Run H136 then H137 workflow_dispatch, inspect Pages25 and 27 and Actions logs.
- Reboot Streamlit and verify H134 remote archive + H136 dataset retain status.
- Taiwan market holidays (including weekday holidays) need a separate verified
  calendar and appropriate manual override for H137 diagnostics. Current
  scheduled check is intentionally conservative and may alert on closures.
- Still NOT externally deployed or verified against live GitHub/Firestore/TWSE.
- H134 content hash / GitHub commit are not third-party immutable/PIT storage.

TESTED IN ISOLATION
- pytest/unittest offline against synthetic sources and FakeGitHub.
- Python whole-tree compileall.
- Clean overlay install must preserve packaged godpick_records.json and
  godpick_user_settings.json byte-for-byte. No real forward trading gains claimed.

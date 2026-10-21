StockApp V191-H141.0 -- records authority rollback / scan-settings / manager Excel safety fix
Date: 2026-10-20 | Incremental direct-copy bundle H128-H141, based on uploaded stock-app-main.zip.

CRITICAL OPERATIONS:
1. DO NOT overwrite runtime-data or any production godpick_records.json, existing settings, secrets, or Firestore data.
2. Cloud Secrets should include GITHUB_TOKEN (single repo Contents Read/Write),
   GITHUB_REPO_OWNER='cheng07021028', GITHUB_REPO_NAME='stock-app',
   GITHUB_REPO_BRANCH='runtime-data', GITHUB_RUNTIME_DATA_BRANCH='runtime-data'.
   Never put the token into a GitHub committed file, ZIP, chat, or screenshots.
3. Verify remote runtime-data/godpick_records_sync_state.json: as checked on 2026-10-20 22:31:49 Asia/Taipei,
   count=2491 and latest_recommendation_date=2026-10-17. This is a remote manifest summary,
   not proof of a full successful record-content roundtrip. Verify H127 shards and hashes at runtime.
4. Verify runtime-data/data/godpick_scan_settings_authority_h111.json has the 42 saved categories
   from 2026-10-19 before pressing Apply/Save in Page 7.
5. After deployment & Secrets restart, Page 8 must read 2491 remote rows (or a demonstrably newer verified authority)
   and latest >=2026-10-17. DO NOT press sync/delete/edit if it still displays July 9.
6. Page 7 settings should be loaded from the remote 'runtime-data' branch; press Apply only AFTER
   verifying 42 categories and successful remote readback.
7. Reboot app, recheck both settings and record count/date. Run a safe staging deletion/write test
   only after making a remote backup and verifying current document version.

CODE FIXES:
- H111 now defaults to runtime-data branch and honors GITHUB_RUNTIME_DATA_BRANCH.
- H141 pre-mutation remote authority check blocks stale/lower-count Page 7/8/10 snapshots from
  overwriting verified newer GitHub or Firestore history. No remote authority -> mutation blocked.
- 'Only saved locally' no longer counts as permanent/reboot-safe.
- Page 8 prominently warns when no remote authority is configured.
- Manager Excel adds '00C_H141輸出品質' and quarantines legacy H102 H99 entry/stop/target/RR.
  Large H109 FADE veto ratios are marked STOP for upstream H108/H109/official source inspection.
  H141 does NOT relax any execution risk gate, invent missing evidence, or claim restored data.
- The emergency reviewed 2026-10-20 workbook is separate: StockReview_20261020_H141.xlsx.

TESTS:
- All included selected tests pass 354/354 offline, compileall PASS (Python 3.13 environment).
- The anti-rollback tests use mock 2491-row remote/1810-row local state and no-token conditions.
- Production Streamlit Cloud, remote H127 shard readback, Firestore and post-Reboot test NOT performed.

If GitHub main has edits newer than the original 2026-10-15 uploaded base, DIFF/MERGE first;
do NOT blindly replace unrelated production files. Keep runtime-data and main branches separate.

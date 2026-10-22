StockApp H142 Excel Export Hotfix V1 (2026-10-22)
================================================
Scope: fixes Page07 manager Excel creation error:
UnboundLocalError: cannot access local variable 'h141_quality' where it is not associated with a value

REQUIRED BASE:
- StockApp H141 + H142 are already installed. Verified GitHub main prior to patch:
  pages/7_股神推薦.py blob SHA1 7602a01b08b79f08a78332e60ce15e33e02d93b4
- If remote Page07 changed after this version, do NOT overwrite it blindly; compare and merge this move only.

Production file to overwrite:
  pages/7_股神推薦.py
Optional regression test:
  tests/test_h142_excel_export_hotfix.py

Change:
- Initialize H141 manager_quality_gate() before appending H142 quality summary.
- Preserve H142 00D provenance tab, H141 00C quality, H126 trade authority, H102 legacy-price redaction.
- No adjustments to score weights, risk vetoes, GitHub/Firestore write paths, history JSON, secrets.

Recommended deployment:
1) Back up current GitHub main and runtime-data separately.
2) Upload ONLY pages/7_股神推薦.py to the same path in main (optional: regression test to tests/).
3) Wait for Streamlit deployment and choose re-run/reboot once (avoid repeated scans).
4) In Page07, press 建立主管 Excel; verify the download opens and includes
   00_H126主管決策, 00C_H141輸出品質, 00D_H142風控來源稽核,
   09_H102舊排序_非買進, with no old buy-entry/stop/target fields reinstated.
5) Verify that recommendation history, settings and remote records are unchanged.

Local validation:
- Full program compileall PASS
- 377/377 specified regression tests PASS (H115, H128 through H142, plus hotfix tests)
- Four hotfix tests execute the actual source Page07 manager export block and
  REAL Excel writer, then save/reload workbook.
- Baseline stock-app-main.zip + H141 + H142 + this fix was used for clean simulation.
- Original godpick_records.json and godpick_user_settings.json SHA256 remain unchanged.
- No live Streamlit / GitHub remote deployment or reboot performed.

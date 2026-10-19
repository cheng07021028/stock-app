StockApp V191-H129 | 2026-10-19 | Necessary files only / Direct Copy

BASELINE
- Original user upload: stock-app-main.zip SHA-256 08fcd57775091a8a1bfd5bb8448ce183408f472d2fbe7971e13a6cb6dc79b5d6
- Requires previously supplied StockApp_V191_H128.zip to have been applied to that baseline.
- Upgrade mode: NON-DESTRUCTIVE. Backup the real deployed code AND all cloud records/settings before copying.

WHAT CHANGED
1. Page7 manager export gains 00B_H129風控複核: evidence gaps vs genuine H109 market/sector veto are visible, sorted for audit attention, WITHOUT establishing buy/research certification.
2. H126 distinguishes mere H113 missing qualification evidence from a true risk veto. FADE_VETO, shock quarantine, stale/contradictory source and already-ignited veto remain hard blocks. No Formal gate is lowered.
3. Page7 T+1 report gets H129績效資料資格 / H129隔離原因 / H129有效Selection Alpha% and 08A_H129績效品質 summary; historical source numbers are preserved, not silently rewritten.
4. H102 local T+1 category score reader rejects weekend/invalid dated samples, H103-unverified paper samples, path ambiguity and extreme T+1 alpha/return before strategy weight feedback. It does not rewrite historical records.
5. Distinct cutoff and replay labels retained from H128. Report export date is not a market trading date.

INSTALL
Extract all paths into the root of the CURRENT deployed stock-app repo and overwrite matching files ONLY.
This is an incremental H129 patch; do not wipe the folder or apply over an old H79/H102-only codebase. If the deployed system does not have H128, apply H128 first or rebase this patch on current actual code.
DO NOT replace records, JSON histories, user settings, Firestore, GitHub runtime files, secrets, or backups. None are included in this archive.
Deploy code to the SAME actual GitHub/Streamlit Cloud production branch.

VERIFY
- Run python -m unittest -q tests.test_h129_wallstreet_audit tests.test_h126_authority tests.test_h128_upgrade tests.test_h125_reboot_categories tests.test_h127_sharded_records tests.h115_pit_walkforward_regression tests.h116_closed_loop_regression tests.h123_evidence_regression tests.h124_fullmarket_evidence_regression
- Offline synthetic baseline: 77 / 77 PASS; all Python files compile.
- Real 10/17 exported report: 10/16 data as-of, 240 inputs / 139 unique H126 / zero approved trades. 120 of 120 H113 audit candidates BLOCKED; H109: 116 FADE_VETO, 3 FAST_SHOCK_RECHECK, 1 SHOCK_QUARANTINE. H123/H124 120/120 PENDING. 80 legacy T+1 rows; only 2 had H103 'yes', one was Sunday 2026-10-04; 4 extreme Selection Alpha rows. This version quarantines them on newly exported report / H102 local learning, never manufactures qualified buy signals.
- Perform real cloud H126/H129 report export, inspect two new tabs and paper-truth status, and confirm post-reboot integrity; this was NOT done in this environment.

LIMITATIONS
No real GitHub/Firestore/Streamlit Cloud production session, no actual user holdings/realized P&L, no live weight changes, no verified forecast outperformance. The 10/19 market comparison is ex-post research only. This patch does not repair or backfill missing H123/H124 official historical files, nor does it force A/B research candidates to appear in a declining market.

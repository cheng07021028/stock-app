StockApp H124 TDCC Schema Guard Fix V1 (2026-10-21)
================================================
Scope: production main branch H124 official ownership collector.
Files to merge:
  godpick_h124_ownership_flow.py
  tools/h124_official_ownership_cli.py
Supporting offline regression test:
  tests/test_h124_schema_guard_fix.py

Root cause shown by GitHub Actions:
  ValueError: 官方欄位缺失或重複，禁止猜測：資料日期
  in godpick_h124_ownership_flow.py -> build_tdcc().

Changes:
  * Normalize Unicode presentation (NFKC), UTF-8 BOM, leading/trailing Unicode whitespace on field names.
  * Accept only documented date labels 資料日期 or Date; missing/ambiguous labels remain rejected.
  * Reject duplicate/empty CSV/JSON headers and rows with mismatched field counts.
  * On rejected official TDCC response, print safe header NAMES, HTTP status, MIME type, response length and SHA-256.
    Never log the raw source body, token, user credentials or arbitrary row values.
  * Disable automatic redirects for official TDCC request.
  * Preserve raw official evidence and H124 strict fail-closed, no trade authorization changes.

Offline tests (synthetic samples; live TDCC response is NOT accessible from development environment):
  python -m unittest discover -s tests -p 'test_h124_schema_guard_fix.py' -v  # 7/7
  python -m unittest discover -s tests -p 'h124_fullmarket_evidence_regression.py' -v  # 12/12

INSTALL (non-destructive):
  1. Back up the current GitHub main and runtime-data branch.
  2. Compare current main production files to patch; merge only intended changes.
  3. Commit the two .py files (test optional) to main. Do not copy/delete data/, JSON, secrets or tokens.
  4. In GitHub Actions, manually rerun "H124 TDCC Weekly and TPEx Official Provenance (Research Only)" once.
  5. If green: verify TDCC capture actually written and the remote commit SHA/HEAD verified.
  6. If red: inspect "TDCC_SCHEMA_REJECTED_NO_EVIDENCE_WRITTEN" diagnostic. Share ONLY the observed_header_names_only,
     content_type, response_bytes and error fields; never share complete raw response or credentials.

LIMITS:
  This patch is tested OFFLINE; live TDCC source has not been fetched here. It fixes presentation-character mismatch
  and adds precise safe diagnostics but cannot guarantee today's official API uses a recognized schema.
  H124 evidence still requires valid official dates, complete holder brackets and >=2 distinct weekly captures.
  H117/H136/H137 errors are separate and not fixed by this H124 patch.

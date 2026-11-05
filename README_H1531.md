# StockApp V191-H153.1 | Quality & PIT Research Repair

**Local test build (2026-11-05); NOT deployed to production.**

## What changed

1. **H109 research-state correction**: `UNKNOWN` / undocumented future enum are `PENDING_EVIDENCE`, not an explicit veto and *not* a pass. `FADE_VETO`, `FUTURE_DATA_VETO`, `SHOCK_QUARANTINE`, `COOLDOWN_QUARANTINE`, source conflicts stay blocked. No H126 formal permission changes.
2. **Official H115 raw PE/EPS vs peer comparison**: A verified timely single-company PE and TTM EPS can display even when the same-market/sector/day sample has fewer than five peers. This **never** awards low-PE/peer-discount scoring absent verified peer evidence. Financial input must come from PIT ledger with official source and availability timestamp, not the current watchlist.
3. **H115 real coverage**: 02_研究召回統計 separates successful ledger read from actual rows and per-stock 12/18/24-month coverage; missing `valuations.jsonl` is explicitly reported. No synthetic backfilled months and no future publication dates.
4. **Research sorting**: verified H109 research clearance, pending evidence, explicit veto/conflicts are distinct. C-veto group stays visible in 01 audit/research rows but is not included in 03_五檔研究計畫 or the 00 research top-five headline. A/B stay research only; ties share rank.
5. **Liquidity reasons**: Separate day turnover, 20-day mean turnover, lots and volume-ratio; missing versus below threshold never conflated. Exclusion reasons overlap, count independently.
6. **Column reconciliation**: Same-stock/date H114 actual turnover, lots, ratio, and evidence day are carried into older H148.3 columns, while original conflicting values are disclosed in an audit note.
7. **3/5-session resilience**: Optionally compare an exchange-listed stock's exact prior-session close with SHA-verified historical H117 TAIEX official index files. It does not infer an OTC benchmark, and absence of matched days leaves results blank. This is descriptive **not** a tomorrow-up probability.

## Files / deployment

This is a cumulative code overlay on **the user's H146 full source baseline** incorporating all H147 through H153 production changes. It does NOT contain recommendation-record JSON, user config, `.streamlit/secrets.toml` or the `runtime-data` authority branch.

1. Inspect diff against the actual live GitHub `main` before merging; do not overwrite newer deployed modifications or durable data.
2. Extract the ZIP at the project root, preserving path `pages/7_股神推薦.py` in UTF-8.
3. Run `python tools/h1531_verify_deploy.py --root .`.
4. Redeploy, inspect GitHub Actions H150/H115/H117 and the H140 source receipts. Do not equate a scheduled job with successful data capture.
5. Log in, confirm source data dates, run full fresh selection, then export a new workbook and verify the 12 sheets. Pay attention to `01_具名強勢研究`, `02_研究召回統計`, `03_五檔研究計畫`, and `00_H126主管決策`.
6. If actual H115 official historical records or official PE/EPS are missing, H153.1 reports the gap; it **does not silently create a historical-data importer or counterfeit figures**. Verify the H115 source publication time, historical coverage, and persistence separately.

## Invariants

- Never change H109's source verdict, H113, H123, H124, H126 authorization, saved recommendation history, saved scan choices, or credentials.
- Preserve 12 Excel sheets, 4 colored key tabs, H151.2 frozen-pane OOXML integrity, H150 login update coordinator, H152 anti-fall research and H153 growth/technical ranking.
- No guarantee of strong-stock retrieval, accurate next-day forecast, or outperforming market. T+1/T+5/T+10/T+20 live sample-out validation remains required.

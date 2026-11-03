# StockApp V191-H152 | Resilience × Next-session scenarios

## Scope

**Additive, non-trading upgrade** atop H151.2. This package preserves every
existing system and 12-sheet manager workbook, and keeps the four primary tab
colors, freeze-pane OOXML fix, H126 formal authority, recommendation archive,
settings, runtime-data and credentials unchanged.

1. H114 derives prior session close/date and same-session return from the
   existing **valid** OHLCV, never the screenshot timestamp or cached score.
2. Adds H114 *historical similar-days* sample count and observed following-day
   positive percentage when n ≥ 30. This is NOT a calibrated predictive
   probability, backtest win rate or trading permission.
3. H152 compares TWSE-listed shares to the TAIEX and TPEX-listed shares to the
   OTC index **from the same research date**. Market must have fallen at least
   1.5%, the stock outperformed by at least 1.5 percentage points and finished
   no lower than -0.5%, with liquidity verified; other stocks are not silently
   counted as resilient. Stale or unknown index → no verified resilience.
4. Optional already-existing H117 official TAIEX archives can supply the
   benchmark only when **both** dates' immutable SHA manifests are valid;
   the TAIEX is NEVER substituted for the OTC index.
5. Excel remains 12 worksheets. 01_具名強勢研究 gains named verified resilience
   research (not necessarily H148 breakout leads) and conditional scenarios;
   02_研究召回統計 includes source/rejection counts; 03_五檔研究計畫 gains
   conditional next-session scenario and relative evidence. Original H148/H149
   data and H126 sheet remain, with no columns deleted.
6. Tomorrow's possible direction: *conditional bullish / neutral / bearish*
   research descriptions, not an asserted next-day prediction or fabricated
   success probabilities. Without sufficient matched data, state explicitly
   that the direction is unavailable. No source fetch during Excel generation.

## Install / rollback safeguards

- This is one cumulative `H146 + H147..H152` overlay including all H151.2
  dependencies. Compare against GitHub's current `main` before any overwrite.
- Copy files to their exact project-relative paths. Do NOT copy H146 raw historical
  JSON/settings/secrets over your live system. Do not modify `runtime-data`.
- Verify: `python tools/h152_verify_deploy.py --root .`
- Perform a fresh post-close scan and confirm current H128/H114 market date,
  `market_snapshot.json` and/or H117 official index archives. Read 01/02/03
  manager tabs, and confirm 00_H126主管決策 never changes permission.
- The H151.2 Excel XML freeze repair remains invoked immediately before save.

## Acceptance criteria

- All Python syntax and relevant H114/H147..H151 regression tests pass.
- Independent anti-fall tests reject mismatched dates, OTC/TWSE confusion,
  stale/zero-volume rows, duplicates, fabricated ratios and price conflicts.
- No decline in legacy export structure: 12 tabs, frozen titles, colored daily
  priority tabs, formal sheet #2, independently categorized research/audits.
- **Not yet** a live broker execution, independently calibrated prediction,
  or proof of improved investing performance. Requires a truly dated market
  source in the deployment and prospective outcome tracking.

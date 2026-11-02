# StockApp V191-H150 | Login-triggered unattended data coordinator

## Base and deployment

Cumulative package: use the actual H146 full repo plus **this single H150 ZIP**; the ZIP contains all H149 production overrides and H150 additions. Always compare to current GitHub `main` before overwriting newer files. No data/history JSON, user settings, Secrets or `runtime-data` files are included.

Deploy all paths, including `.github/workflows/h150_data_orchestrator.yml` and `tools/`. Ensure GitHub Actions is enabled, and Streamlit Cloud's `GITHUB_TOKEN` has **Actions: read and write** for this repo (in addition to existing permissions). No credential is stored in the ZIP.

Verify before pushing:
```
python tools/h150_verify_deploy.py --root .
```

## Behavior

* On successful login or opening the app with authentication disabled, the Streamlit script performs **one bounded read** of recent H150 GitHub Actions runs. When no recent job is present and the token has proper rights, it sends `workflow_dispatch` once. Recent queued, running or failed jobs trigger a cooldown instead of repeat requests. The web UI stays responsive; no 2,000-stock scan runs inside the login request.
* The H150 Actions coordinator uses preexisting H117 EOD, H123 flow, H136 research cache and H137 audit workflows in guarded order, respecting Taiwan source release times, as well as independent H115 revenue and H124 weekly evidence. Workflow success **does not** certify that every underlying stock/day is verified; each collector's validation rules remain authoritative. Jobs have their own preexisting cron fallback. If launched before official release, the stage is explicitly marked `WAIT_OFFICIAL_RELEASE`; existing cron remains responsible for later completion.
* H150 also collects *public, dated RSS headlines* into `runtime-data/data/h150/H150_news.json` via its own isolated commit. Existing H83 local cache is used first; H149 can fall back to this remote public-news cache for exact company/name matches, with SHA/freshness checks. Headlines are **research clues only**, not confirmation of price causation.
* The sidebar shows queued/previous job status and offers "查核盤後資料來源" using existing H140 remote monitor. Only H140 can confirm its particular audited receipts. H126 unchanged.
* No auto-purchase, no increase of formal buy quotas, no fallback to stale prices, no trading-log invention, no silent data overwrites.

## Important limitations

* If a user logs in before an official release, new same-day data **cannot exist yet**. Scheduled updates/market-day reconciliation still apply. A recent workflow run means only that work was requested or attempted; it is not a freshness certificate.
* Private account positions and executed transactions require the owner's authorized brokerage/portfolio data integration; those are not obtainable from official public market feeds. H149 must show these as missing until real records are provided.
* H149 research only analyzes data actually available in its current scan and sources. This patch does **not** introduce a trained AI prediction model, or guarantee research picks, profitability, or a successful live deployment.
* Actual GitHub Actions permission, official EOD data date, RSS access, individual feed URLs and Streamlit Cloud must be verified after deployment. Offline tests cannot prove these external dependencies.

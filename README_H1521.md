# V191-H152.1 | Pre-breakout potential first (research only)

## Deployment
This is a cumulative **H146 base + H147 through H152.1** overlay. Check the live GitHub `main` code before merging; do not overwrite newer user changes. Extract paths into the repository root, including `pages/7_股神推薦.py`, then run:

```bash
python tools/h1521_verify_deploy.py --root .
```

Run new and existing regression tests with `pytest -q tests` (batch smaller sets when your runner times out). Redeploy Streamlit Cloud and re-export manager Excel. Check `01_具名強勢研究` and `03_五檔研究計畫`, including H126 still being the unchanged second worksheet.

## Change rationale
The original H148.3/H149 required `量比>=1.2` and a near-high / past-5-day rise to show leads, systematically excluding quiet pre-breakout setups. H152.1 uses same-run full diagnostic pool (not limited H113 top 120), with H114 >=60 dated valid K, known lots and turnover units; daily and 20-day average turnover each >=150 million TWD; lots >=500; ratio 0.55–1.45; 5-day price change -4%..+7%; daily change -2.5%..+3%; prior 20-day high gap 0–10%; at least two of volatility compression, lower-volume rising base and near-high structure, and one of the former two must be present. Excludes already-ignited names and stale quotes. These are **initial, adjustable research hypotheses**, not backtested optimal thresholds or market odds.

Potential leads (P1/P2) are placed before already-active leads in the **research** sheet. Five research plans may include them and use a `0.55` research volume-ratio threshold ONLY for P evidence; all other leads remain under their existing evidence conditions. Verified H109 vetoes are deprioritized and retained. Requires further verification of H108 group capital breadth, H123 institutional evidence, and H124 weekly TDCC; never claim whale accumulation from insufficient data. Never relax H109/H113/H123/H124/H126 formal trading authorization.

H114 now adds an AS-OF `H114當前5日漲跌幅%` derived from already fetched valid K data, not future observations. No new network calls, fabricated prices, guarantees, live predictions, model retraining, private holdings or broker orders. H151.2 OOXML freeze repair, 12 Excel tabs, 4 colored focus tabs, and H150 login jobs remain intact. ZIP excludes secrets, authoritative records, user settings, caches, and runtime-data.

## Field validation
The selection and ranking remain hypotheses. Validate true post-close official quotes, per-stock candidate inclusion, false exclusions, forward T+5/T+10/T+20 realized returns, drawdowns, turnover and slippage **out of sample** before asserting improvement. No expected rate of subsequent large rises has been established.

StockApp V191-H116 | 每日決策快照 × T+1/T+3/T+5績效閉環 × 策略人工評估
日期：2026-10-14 | H115相容必要增量包 | Research/Paper Only

【版本目標】
上個版本H115已能對外部提供之「當時快照+後續日K」計算T+1、T+3、T+5；但沒有把每次實際選股事件在決策當晚保存成獨立不可改寫的歷史批次。H116增加單次掃描真正決策core快照、GitHub遠端寫入與回讀驗證、每日成果對照及「僅供人工審核」的策略回饋。**沒有真實當時快照與未來行情時，不得報稱勝率或新策略有效。**

【正式程式】
* godpick_h116_daily_feedback.py：建立每次run_id唯一的研究快照、SHA256、明確證據時間；GitHub Contents API同步寫入+回讀；跨日重複標的去重；使用既有H115 Walkforward引擎比較策略T+1/3/5。
* tools/apply_h116_snapshot_hook.py：安全定點修補目前真正安裝中的 pages/7_股神推薦.py（先備份、檢查錨點、保留其餘頁面），在每次實際執行掃描結束處接入H116；同時把舊H85缺少的 blackhorse_overview (30列) 和 h114_parallel_watch (40列) 加進當時compact core，避免漏記新策略。
* tools/h116_closed_loop_cli.py：inspect核對原始快照、review用已供應且經使用者確認之OHLC及指數資料出報告。
* .github/workflows/h116_nightly_strategy_review.yml：可選之週一至週五19:30台北時間重算排程；**不會自行下載/虛構行情**。只有在已有每日真實價格CSV、benchmark CSV及快照時才執行，否則跳過。Actions需允許GitHub提交報告。
* tests/h116_closed_loop_regression.py：合成資料離線測試；不代表實盤績效。

【先備份與安裝】
1) 備份最新完整stock-app資料夾、第7頁、H111永久設定、推薦紀錄、原data目錄。
2) 將本ZIP解壓至目前正式stock-app專案根目錄。不要把整個舊專案與本ZIP互相覆蓋；此ZIP不含第7/8頁、任何歷史JSON或密鑰。
3) Windows Powershell在stock-app根目錄執行：
   py -3 tools\apply_h116_snapshot_hook.py --check-only
   確認 READY / PASS之後，才執行：
   py -3 tools\apply_h116_snapshot_hook.py
   程式會在pages旁留一份帶.h116backup.時間戳的備份；錨點不同則拒絕改動。
4) 本次需要提交GitHub：godpick_h116_daily_feedback.py、tools/h116_closed_loop_cli.py、tools/apply_h116_snapshot_hook.py、已修改後的 pages/7_股神推薦.py、按需求啟用 workflow。絕不能用舊版Page7代替當前Page7。
5) 正式Cloud Secrets請用私有設定（不要在公開GitHub檔案中放token）：
   H116_GITHUB_PERSIST = "1"
   H116_GITHUB_REPOSITORY = "owner/stock-app"
   H116_GITHUB_BRANCH = "main"
   GITHUB_TOKEN = "***請填自己的細緻權限金鑰***"
   Token只需要該儲存庫Contents:Read/Write，勿提供給ChatGPT。Cloud部署分支若有保護規則可能會拒絕Contents API寫入。
6) 第7頁真正完成一次新掃描後，請以顯示的「H116本輪研究快照已寫入GitHub並回讀驗證」確認；只顯示排程/本機成功不算永久保存。可到GitHub路徑 data/h116/decisions/YYYY-MM-DD/<run_id>.json 查版本與SHA256。

【資料安全】
- 每個run_id只能新增一次；已存在相同內容則安全略過，已存在不同內容則報錯且不覆寫。
- Snapshot只保存候選股票代號、模型分數、來源表與必要判斷欄位。不保存帳密或自行修改原推薦紀錄。
- GitHub使用公開儲存庫時，快照等於公開選股策略資訊；如果不希望公開，應改用私有repository且設定權限。**不允許因保密而把Token直接寫進ZIP或檔案。**
- GitHub使用者擁有版本庫修改權，故「不可覆寫」是程式規則與SHA驗證，並非不可被GitHub管理者刪改的法律級永久防竄改存證。
- 如果遠端寫入失敗會顯示未永久保存。網路等待可能增加完整掃描結尾數秒，網路不通時停止遠端記錄但不會讓正式選股崩潰。

【閉環績效運作】
先持續保存每天推薦快照（包含H114研究分、H113准入、身分與各策略；不重算昨天應選的股票）。
收盤後匯入真實個股日K成 data/h116/verified_ohlc.csv，欄位 code,date,open,high,low,close；另提供 data/h116/benchmark_ohlc.csv，代號BENCHMARK，同樣開高低收。資料請先從具可追溯官方或合法來源驗證，兩檔都採未含虛假值的同一價格基準；若股票除權息、停牌或下市須先校對，否則不可把報告當作績效權威。
- 檢查是否有推薦快照：
  py -3 tools\h116_closed_loop_cli.py inspect
- 輸出T+1/T+3/T+5走勢／日等權比較／風險／待審核建議：
  py -3 tools\h116_closed_loop_cli.py review --prices data\h116\verified_ohlc.csv --benchmark data\h116\benchmark_ohlc.csv --out reports\h116
輸出：H116_strategy_summary.json、H116_strategy_comparison.csv、H116_T1_T3_T5_details.csv、H116_approval_required.csv、H116_source_manifest.json。
期望每天晚上或下個交易日新行情輸入後再執行。若啟用可選GitHub Actions，它只在行情CSV和快照存在時重跑和提交衍生報告，不假稱已建立全市場行情爬蟲。

【回測準則】
- H115價格契約：決策後下一個交易日開盤模擬買進，持有至T+1/T+3/T+5當日收盤，扣示意交易成本/滑價。T+n為市場交易日（必須提供基準指數K來確認未停牌）。資料缺失標成失敗原因，不以0%充數。
- 依股票/推薦日/策略去重，即使一天掃描多次也不累加命中。跨不同交易日仍保留真實重複推薦，檢查重複股暴露。
- 策略比較以「每個研究日晚上的股票平均，再對各夜等權」減少每天標的多寡導致的樣本偏差。
- 股票+策略樣本>=30、獨立決策日>=20、跨至少60日曆天且每天具完整指數比較，才開始產生待人工複核的策略優先/降低風險建議。這只是初步樣本資格，並非真正樣本外alpha證明。
- 沒有精確可驗證的歷史證據時間，記錄為UNVERIFIED_ASOF_SNAPSHOT；H115嚴格績效不將其冒充有效事前回測。同日僅給出日期（未給發布時間與時區）亦不算精確證據。
- 正式H113買進資格、H111永久設定、正式推薦、Page08歷史、程式權重皆不變。**H116只有建議報告，不會自動修改分數、參數、推薦名單或執行交易。**

【已測／未測】
已測：GitHub GET/PUT+GET模擬、讀回SHA、已存在批次拒絕覆寫、版本不相容拒絕修改、歷史JSON雜湊、同日重複去重、缺證據不得給績效、T+1/3/5、前版H112/H113/H114/H115回歸、現有第7頁定點patch+編譯；原專案覆蓋可用性離線測試。
未測：正式Cloud網路及GitHub Secrets設定、外部行情每日自動擷取、個股真實公告時間補齊、60~120交易日的真正樣本外驗證、實盤績效、真實雲端Reboot。

V191-H250-R5｜手動強制全啟用工作（獨立於例行中央排程）
================================================================
本包是「鎖定正式來源、自動套用」修正工具，不是已改好的完整專案。不要只上傳工具後 Reboot 就當成完成。
本次未修改 GitHub main／runtime-data，也未觸發股票作業。以下兩種方式任選一種。

A. 建議：本機直接套用
1. 從 GitHub main 下載最新 stock-app-main.zip，解壓縮；或使用最新的本機 Git checkout。
2. 本 ZIP 解壓到不同資料夾。雙擊 RUN_H250R5.bat，貼上實際 stock-app 專案資料夾。
   亦可使用 Python 3.11+：python APPLY_H250R5.py "D:\stock-app-main" --apply
   若 H249 已整合且本機缺 requests，先安裝 requests，或使用方法 B。
3. 工具會核對 Git blob、完整定位點與正式修改檔 compile，在隔離暫存資料夾跑34項策略測試及6項真實排程原始碼（mock業務）整合測試。全部通過才備份及修改本機程式。另產生 StockApp_V191_H250R5_Overlay.zip。
4. [PASS] 後，把 Overlay 內的實際正式修改檔覆蓋/上傳 GitHub main，保留原始路徑（含 .github/workflows）。若用 Git checkout，將實際修改檔 commit/push。不要把備份或 runtime-data 上傳。
5. Reboot App。Page17 出現「H250-R5｜手動強制執行（獨立於例行排程）」才算新介面生效。
   [BLOCKED] 時停止，不強制覆蓋；若輸出 ZIP 已存在，請先改名保存，避免覆蓋。

B. 只使用 GitHub（不需 Windows Python）
1. 把本包的 APPLY_H250R5.py、H250R5_BASELINE.json、godpick_h250r5_manual.py、tests/test_h250r5_manual.py、.github/workflows/h250r5_prepare.yml 按路徑上傳到 main。
2. Actions -> H250-R5 prepare actual manual execution patch -> Run workflow (main)。
3. 完成後查看 Summary。若 publish_review_branch=true 且分支推送成功，可從 Summary 的 compare 連結建立 Pull Request、核對並合併到 main。
   若權限拒絕分支推送（例如 workflow 檔寫入限制），不得把這次當成部署成功。請下載 Artifacts 中已產出的 StockApp_V191_H250R5_Overlay，解壓並上傳真正的修改檔。
   publish_review_branch=false 時只產生 Overlay，不建立遠端分支；手動覆蓋 main。
4. 合併/覆蓋後 Reboot App。單純上傳產生工具或綠燈均不是正式部署成功。

R5 行為：
- Page17 主按鈕不再需要另外勾「部署驗證」；依已永久保存的 enabled 工作預覽數量及順序。
- Worker 在開始前讀取 restored runtime-data 設定與 sidecar，核對 SHA256；拒絕缺欄位、權威失效或 hash 不符，禁止用預設設定代替。
- 只跑啟用項目；即使總排程 OFF、週末、未到時、已執行過，也可人工強制啟動。仍保留原資料及前置驗證，07未啟用則不執行；07啟用但核心工作停用/未成功則阻擋，不強制啟用。
- 使用使用者保存的工作順序，07固定最後；非核心警示沿用 H159 規則，但整批絕不冒充全部成功。
- 每個工作 FORCE key 包含 GitHub Run ID+Attempt；人工批次結束還原例行 jobs/completed_run_keys 並獨立保存逐項回執，不占用或修改原定排程時段。
- 原 H156 核心6項／全模組更新＋重新推薦、H248 補跑（若已整合）仍保留。
- 120秒 Session 防連點；查詢同模式活動 Run，GitHub 既有 concurrency 串行（H244 queue max）。已真正整合 H249 的版本續用其 CAS 永久派工及執行日誌，不會擅自啟用尚未部署的 H249。
- 尚未整合 H249 的版本，活動 Run 查詢不是原子跨使用者去重；中斷後不應盲目重送。H249 已整合者則由原佇列負責安全接手。
- H159 最終驗收必須核對本輪 Run+Attempt、實際計畫、每個遠端 history row、零警示/失敗/阻擋。GitHub 正常例行喚醒不變。遇官方資料未發布（週末）可有警示/阻擋，不保證全部成功。
- R5 工作範圍為中央排程14項，不包括另有排程的 H195/H206/H219/H224 等所有獨立研究 workflows。
- 使用 GitHub Actions 的既有中央 workflow 亦可在 manual_job 填 force_enabled_jobs、wakeup_source 填 manual_r5 立即派工；此為部署後才可用，請勿於部署前填未知模式。

驗收限制：本機已執行41項離線測試；6項正式排程原始碼整合測試因本機未取得完整專案未執行。工具內將6項設為必跑，不通過不產出 Overlay/不套用。GitHub 正式端到端、遠端保存及真實股票工作仍需部署後實測。
既有版本：目前核對 main 的中央排程、Page17、Worker 及派送服務仍為未整合的原始基準。工具接受已知原始 Git blob 或本次包附的已知前置版本精確轉換（不覆蓋任意未知新版本）。本包不會自動把所有 H245～H250 其他修正補裝或認定其已部署。

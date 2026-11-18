V191-H168｜中央 Worker 即時真相 × 防重複自動派送
================================================
修正依據：GitHub main HEAD 179e07984bf685f3f4aaeb76eb08ce4a023f4709
事件：2026-11-18 Page17顯示13項漏點、沒有健康worker，但 GitHub Actions 
      44264974046 已在 in_progress、44267240309 pending，且多筆 pending
      已 cancelled。runtime-data status仍在10:21 SKIPPED_DISABLED，
      但排程設定已於16:12 enabled=true。

根因：
- H153.3 為避免寫入過載，工作中間只做 Worker 本機 atomic checkpoint，
  terminal checkpoint 才寫入 GitHub runtime-data，導致 Page17 顯示舊狀態。
- H19 每8秒只看 runtime-data；無遠端最新 active_run/heartbeat，會每8分鐘
  嘗試新增 workflow_dispatch。GitHub 同個 concurrency group 舊 pending
  可能被新 pending 取代或取消，派送成功不代表執行完成。

本次正式修正（只2個檔案）：
- pages/17_系統健康檢查.py：
  獨立讀取中央 GitHub Actions 的 live in_progress/pending/queued；
  清楚顯示 GitHub run ID、開始時間、來源、是否在執行／排隊；
  有活躍或待執行的官方 Worker 時禁止 H19 自動再派送。
  若 GitHub API 無法讀取，維持原本 H19 + 8分鐘 session cooldown。
  原14項設定、手動核心6項及全模組按鈕不刪除。
- godpick_h168_worker_live.py：
  僅有 GET、只讀中央 workflow，不觸碰正式推薦、H126、歷史或Secrets；
  嚴格隱藏Token，不回傳GitHub敏感回應內容。
  35秒 UI 快取避免每8秒大量查詢 Actions API。

預期操作：
1. 解壓 ZIP 並依相同路徑覆蓋 GitHub main 2個正式檔案。
2. Streamlit Reboot，開 17_系統健康檢查，觀察 H168 即時 Worker。
3. 若顯示「GitHub Actions 執行中／排隊」，不要反覆按手動強制更新；
   等該筆 Actions 完成與 runtime-data remote checkpoint 更新。
4. 若 GitHub API HTTP 403，檢查 Streamlit Secrets中的GITHUB_TOKEN對
   stock-app的 Actions: Read 權限；本次不修改任何Secrets。
5. 07推薦真正成功，仍要看 status.jobs.godpick_recommendation 的本輪SUCCESS；
   SuperAI 若未完整，仍須保留WARNING，H126權限絕不放寬。

已知限制：
- GitHub Action「執行中」不代表某個業務工作成功；最新整體執行結果
  仍由 runtime-data terminal checkpoint 確認。
- 對其他使用者／Windows/GitHub定期喚醒的既有派送行為不做變動；
  H168 防重送只作用於Page17的H19自動補送。
- 未修復 TPEx 逾時或TAIFEX落後資料；不偽裝 SuperAI 成功。
- 不包含任何業務資料或API憑證；不更改 runtime-data 或推薦歷史。

驗收：H168+H167模擬；Python語法；ZIP逐檔一致及UTF-8中文路徑。

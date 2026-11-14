V191-H159 — 中央 Worker 派送 × Page10 同步一致性 × 手動批次真相
基準：GitHub cheng07021028/stock-app main c231466fb258537de977f0806ad2cc4ee27cb164（已含 H158）

本包僅修改／新增必要檔案；請依 ZIP 內相同相對路徑直接覆蓋，勿刪除其他任何檔案。

問題核實（2026-11-14）：
1. runtime-data 的正式狀態仍記錄 11/13 交易日的核心與績效結果；11/14 是週六，例行 worker 正確 SKIPPED_WEEKEND。
2. 17 頁仍有兩個按鈕能把 14 項大型工作直接在 Streamlit UI 執行，造成頁面顯示 RUNNING 與遠端正式履歷不同步，或 UI 中止後留在半完成狀態。
3. Page10 _sync_records 原先僅要求歷史權威 upsert 本機保存，卻可憑推薦清單遠端成功回報整體同步成功；此流程的遠端保存證據不完整。
4. 排程 `_h1533_remote_failure` 將所有含「永久同步失敗」的暫時性問題都當作整批同步斷路，導致後續 Page10 N 日／命中任務直接 BLOCKED。
5. H156 手動全模組批次即使核心5項成功，只要非核心績效工作失敗仍擋住 Page07；反過來，舊版「全批次成功」摘要又未計算非核心 14 項狀態。
6. GitHub Actions 對例行喚醒採 `continue-on-error` 是為抑制每10分鐘通知，但人工強制完整更新也可能被錯誤標示綠燈。

H159 保留且修正：
- Page17「執行已到期」「強制全部」按鈕仍存在；真正工作派送到 GitHub 中央 Worker 而非 Streamlit UI。模擬鍵不變。
- 新增展開的遠端最後錯誤原因明細；受理派送不等於執行成功。
- Page10 GitHub Worker 需實際確認 godpick_records.json 歷史權威，且當前清單／同輪最新推薦快照依舊須永久化確認；保留原無資料防清空。
- 只有具明確 HTTP401/403 或憑證／權限問題才開啟跨模組同步斷路；409、timeout、待確認等暫時性失敗個別留 FAILED，下一工作仍可獨立嘗試。不可把 FAILED 假寫 SUCCESS。
- 手動全模組若核心股票主檔／大盤／官方因子／SuperAI／自選股均成功，但非核心歷史績效有錯，07 允許以 WARNING 研究重算；整批 all_success 仍為 FALSE。核心失敗仍擋 07，H126 買進授權完全不放寬。
- 僅使用者明確 workflow_dispatch manual_job 非空時，新增 H159 GitHub Actions 真實成敗門檻：同一個 run_id、非空執行清單、全批次逐項驗證、遠端驗證步驟成功。例行 GitHub/Windows 每十分鐘喚醒維持原通知行為。

部署／驗收：
(1) 僅覆蓋本包 6 個正式檔案（2個 page、2個 service、1個 workflow、1個 CLI）與測試檔。不要複製或覆蓋 runtime-data、Secrets、設定、推薦歷史、人工交易授權。
(2) 重新載入 Streamlit 第17頁，在「中央遠端最新異常原因」閱讀真正的錯誤訊息。
(3) 下一交易日（2026-11-16 週一）確認 01 大盤、16 官方、17 SuperAI 當日有效；先按「核心6項完整更新＋重新推薦」，到 GitHub Actions 查看本輪是否成功，再核對 07 最後成功更新日。
(4) 全模組模式若任何 08/10 工作有遠端同步問題應呈 FAILED/WARNING 並在 GitHub manual 工作流顯示紅燈；若核心5項成功則 07 可標警示而繼續研究。不得把 14 項當全成功。
(5) 若報 HTTP 401/403，檢查 GitHub Token／權限及 runtime-data 寫入。若報 409／timeout，可按正式工作歷史判斷是否需重新派送；不要回復舊資料覆蓋。

離線驗收：41項 pytest 相關回歸通過（H156 12? + H158 + H159）、Python 編譯通過、YAML 解析通過。
限制：未實際連到 Streamlit Cloud、期交所／TWSE/TPEx 當日網路 API，亦未對遠端 GitHub/Firestore 進行正式回寫測試。不宣稱雲端14項已全部恢復成功。

V191-H161 — 08 股神推薦紀錄：精確刪除 × 遠端權威恢復 × GitHub 403 明確診斷
版本日期：2026-11-14（台灣）

【當前實際查證】
GitHub main 原版檔案（修正前 SHA1/Git blob）：
- pages/8_股神推薦紀錄.py：3f144750b8e17ec1e128f2d3affb5e778ec43519
- godpick_persistence_service.py：dd8e091eac1f97f55676e010ff5c0dfd42922f4a
GitHub runtime-data / godpick_records_sync_state.json：2632 筆，最新推薦日期 2026-11-09。
使用者頁面顯示 2558 筆，相差 74 筆；GitHub 內容 PUT 為 HTTP 403，報「Resource not accessible by personal access token」。

【修正內容】
1. 第 08 頁的刪除/清空只依 record_id 精確刪除。新快照一律從完整原始 JSON 產生，保留畫面未顯示的歷史欄位和其他筆數。
2. H141 防縮水補強：刪除前要求與 GitHub/Firestore 的原始筆數及 payload_hash 相同，不能靠「目前筆數＋勾選數」放過局部快照；也允許刪除原權威的最新推薦日。
3. 在確認可用的遠端儲存成功之前，不改寫本機刪除結果。無遠端權限時，保留原始資料、不顯示永久刪除成功。
4. 相同資料日期、未經確認的本機局部快照（2558）若輸給遠端完整權威（2632），回讀遠端並核對 Hash 後恢復；較小局部快照在 .cache 下暫存供調查，非永久權威。
5. 403/401 Token 權限不足不再當成可重試的 CAS 衝突，不再針對相同大型分片重複嘗試；背景狀態標示 blocked_permission。
6. 第 08 頁直接顯示 GitHub HTTP 403 的授權設定指引，刪除失敗以錯誤/警告顯示；不再用成功色掩蓋失敗。

【必須由管理者完成：GitHub 設定】
1. 在 GitHub Settings > Developer settings > Personal access tokens > Fine-grained tokens 建立/更新 Token；Repository owner = cheng07021028；Only select repositories = stock-app；Repository permissions > Contents = Read and write；必要時 Actions = Read and write（第17頁手動排程）。
2. 在 Streamlit Community Cloud 股票 App > Settings > Secrets 中更新 GITHUB_TOKEN。勿公布/上傳 Token、勿在 GitHub 程式碼中硬編密鑰。
3. 設定 GITHUB_RUNTIME_DATA_BRANCH = "runtime-data"、GITHUB_CODE_BRANCH = "main"。注意：Actions 啟動權限不等於 Contents 寫入權限。
4. 重新啟動 Streamlit App／重新整理第 08 頁，按「重新載入」確認載入完整遠端權威、筆數/Hash驗證通過（以遠端當時資料為準），才重新勾選欲刪除的記錄。
5. 若 Firestore 未設定也能透過有 Contents write 權限的 GitHub H127 壓縮分片保存；但可能較慢。若兩種遠端都失敗，必須先修復至少一種，不能保證刪除後 Reboot 持續生效。

【測試與安全邊界】
新增 11 項離線測試：401/403 單次即停止、回讀筆數一致/不一致、同步前不修改本機、刪除最新日、ID 精確核對、保留隱藏欄位、舊快照回復及合法刪除不復活。
既有 H127、H128、H141 及耐久化相關測試一併執行共 33 項通過。
早期 tests/v191_hotfix8_github_cas_records_smoke.py 的「舊快照遇到較新遠端 revision 應回報成功」不符合現行 H128 防回退；此測試在未修改的原檔就失敗，不放寬安全保護去迎合過時測試。
模擬測試不等於正式 Streamlit Cloud/GitHub/Firestore 寫入成功。尚未部署 main，也未用真 Token 測試現場操作。

【部署】
僅將 ZIP 內 godpick_persistence_service.py、pages/8_股神推薦紀錄.py 依相同相對路徑上傳 GitHub main；tests/ 為驗收腳本，H161_README.txt 為說明。
不可刪除 runtime-data、godpick_records.json、godpick_records_sync_state.json、推薦歷史、Secrets 或其他現有程式。不得把本機 2558 筆直接覆蓋權威 2632 筆。

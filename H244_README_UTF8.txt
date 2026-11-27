StockApp V191-H244 — 中央排程派工與批次真實回報
日期：2026-11-27

【交付性質】
這是精確來源差異產生包，不是可直接全檔覆蓋的正式版本。
以當日 GitHub main 讀取到的 7 個正式檔案 Git blob SHA-1 為基準；
由於容器無法直接下載完整 GitHub 原始碼，不以記憶或舊版重建 Page17。
產生器只讀原始碼，核對全部雜湊、定位點與 Python compile 後才建立差異 ZIP。
不會改動原始專案、不會推送 GitHub、不會修改推薦紀錄、Secrets 或 runtime-data。

【使用方式】
1. 在 GitHub stock-app main 選 Code > Download ZIP，解壓縮至一個新資料夾。
2. 將本包解壓至另一個資料夾，雙擊 RUN_H244.bat。
   輸入實際 stock-app-main 根目錄（包含 pages 與 .github）。
   或從命令列執行：
   python Apply_H244.py "D:\stock-app-main" --out StockApp_V191_H244_Overlay.zip
3. 必須看到 [PASS]；產生 StockApp_V191_H244_Overlay.zip。
   這才是可覆蓋的差異包，保留內部路徑，包含隱藏的 .github 資料夾。
4. 備份當前正式檔，再將 Overlay 內檔案覆蓋到專案同名路徑，commit/push 到 main。
   不上傳產生器本身，不覆蓋其他檔案。確保 .github/workflows 的變更也已提交。
5. Streamlit Cloud 確認新 commit 已部署，必要時 Reboot app。
   先進 Page17 測試一次強制全模組，取得 Run ID 後不要反覆按。

【版本不符】
[BLOCKED] 代表現行 main 已更動、路徑不正確、或來源並非此次版本。
不要略過雜湊限制，不要用舊版覆蓋。提供最新 main 完整 ZIP 重新適配。
本工具容許 Windows CRLF 與 Git LF 差異，但其他內容差異一律拒絕。

【設計與驗收】
- GitHub central concurrency queue:max，仍同一群組、單 worker 順序執行，最多100個等待。
  達上限仍可能取消；長時間處理慢於兩個例行喚醒頻率時可能累積待執行工作，需監控。
- 不變更 Windows 或 GitHub cron；不變更 14 項順序、資料門檻、H159 驗收。
- GitHub API 2026-03-10 對 Page17 追蹤派送回傳 exact run ID；舊 callers 保持 2022-11-28。
  204 或格式異常時僅標示已受理但無法對應，不用最新另一個 Run 代替。
- 頁面區分人工強制／核心／到期批次，採 run ID + attempt 確認結果。
  人工派送追蹤存在 session；遠端最近 manual batch 保留原永久紀錄，重啟後可看最近批次。
  較舊批次仍須查既有 GitHub Actions 與歷史紀錄，沒有新增永久派工資料庫。
- 同 session 中相同模式已確認活躍時阻擋重複派送；非全域互斥防重複，並行群組仍序列化。
- H195/H220 補齊 lxml、beautifulsoup4、html5lib 及 import 檢查。
- 所有既有的推薦與資料安全門檻不放寬。

【實際查核】
GitHub UTC 09:34:27（台灣 17:34:27）強制批次 Run 45974835118：
14項、成功10、警示4、失敗0、阻擋0。Page10 三項警示源於57筆缺日期/ID；
Page07 推薦 WARNING，遠端封存尚未確認；H159 嚴格驗收失敗是正確行為。
UTC 10:06:00（台灣18:06）Run 45979210670 是 due_only，實際0項。
09:34強制參數本身有正確傳遞，不能把10:06零項誤判成同一筆。

【未完成／下一階段】
57筆歷史資料補證、H138封存、H206/H219/H224資料品質缺口，未在本階段更動；
不能虛構日期/ID或改掉紅燈。需核對 runtime-data 與原始推薦紀錄，才可安全補證。
測試：26本機模擬/實際服務mock檢查、24個GitHub來源定位點通過；
未進行正式雲端端到端測試，亦未部署。完整來源compile由產生器在本機執行。

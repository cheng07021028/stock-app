AI 股神 V191-H125｜「選擇類型（可多選）」Reboot 永久保存定點修正
發行日期：2026-10-14

【已確認的兩項根因】
1. 檢查 GitHub cheng07021028/stock-app 的 main 分支時，未找到 godpick_h111_scan_settings_authority.py；至少該分支尚未真正部署先前 H111 的永久權威邏輯。若 App 使用不同分支，需以該分支實檔再次確認。
2. 第7頁的 _normalize_recommend_scan_settings 會把 selected_categories 過濾成「目前行情主檔有的產業」：
     cats = [x for x in cats if x in category_options] or ["全部"]
   產業主檔載入未完整或產業名稱變動時，已儲存的勾選項會被清掉、退回「全部」。
3. 使用者新截圖有 #U80a1... 等頁面檔名；安全修補工具若只找正常中文檔名，就會漏修真正的 Page7。

【H125 修正】
- 無論當前產業選項是否完整，selected_categories 都以遠端權威值為準，不經市場主檔刪除。
- 首次遠端載入之後，再同步 session_state、multi-select 的選項與選擇值。多選勾選的順序及原有值會完整保留。
- H111 若尚未安裝：利用隨包提供的 H111 原始安全修補內容，將現有的 Page7 就地更新；只在檢查確認缺少 H111 根模組時才新增檔案，不覆蓋現有 H111。
- GitHub 同步寫入並回讀全部設定完全一致，才宣告「永久成功」。
- 若第7/8頁使用 #Uxxxx 編碼檔名，需顯式指定 --fix-page-names 才修復，且事先備份、無同名衝突。
- 不包含第7/8頁舊版整頁、不修改正式買進資格或 H124 選股因子、不包含歷史紀錄/權重JSON/Token。

【Windows PowerShell 操作】
1. 在本機先完整備份目前真正部署版本的 stock-app（含 pages/），確認 H124 最新程式已在此目錄。
2. 解壓縮 ZIP 到目前正式的 stock-app 專案根目錄，保留 tools/、h125_support/ 路徑。
3. 檢查（不修改）：
     py -3 tools\apply_h125_multiselect_authority_fix.py --check-only
4a. 如果確認第7/8頁都是正常中文字名稱，執行：
     py -3 tools\apply_h125_multiselect_authority_fix.py
4b. 如果因 #Uxxxx 亂碼檔名被阻擋，先確認沒有正常版重複頁面，然後：
     py -3 tools\apply_h125_multiselect_authority_fix.py --fix-page-names
   注意：有中文頁及亂碼頁重複存在時工具會阻擋，不會猜測哪份較新。請先人工確認，再處理重複檔。
5. 核對程式測試：
     py -3 -m unittest discover -s tests -p "test_h125*.py" -v
6. 看工具回報 H125_PASS、備份已完成；不要提交 backups/、.cache/、金鑰或舊 JSON。
7. GitHub 部署時提交：
     pages/7_股神推薦.py
     godpick_h111_scan_settings_authority.py  （如 H111 尚未上傳）
     godpick_h125_multiselect_authority.py
   如有 #U 改名：一併提交 pages/8_股神推薦紀錄.py 的「改名」，以及刪除原 #U 名稱；不要把兩套 Page7/8 一起提交。
   請確認 Streamlit Cloud 真正使用的 repo、branch 與 H111 指向的 GITHUB_REPO_BRANCH 一致。
8. 在 Streamlit Cloud Secrets 設定 GITHUB_TOKEN（GitHub Contents 讀寫權限），以及需要時的 GITHUB_REPO_OWNER、GITHUB_REPO_NAME、GITHUB_REPO_BRANCH。請勿傳送 Token。
9. 部署後進入 Page7，在「選擇類型（可多選）」選至少3個不同產業，按「套用設定」。
10. 檢查 UI 顯示從 GitHub 永久權威載入；「保存明細」應寫「GitHub 已同步寫入並回讀驗證」，並確認勾選數量。
11. Reboot App，開新瀏覽器工作階段，重新進 Page7 核對所有產業、模式及門檻沒有變動。
12. 到 GitHub 檢查 data/godpick_scan_settings_authority_h111.json：schema 為 stockapp.h111.scan-settings.v1，settings.selected_categories 應為剛才的完整產業清單。這一份才是本次永久權威檔。

【驗收失敗的判斷】
- 如果不能看到上述獨立權威 JSON：請先核對 token 權限、deploy branch、commit。
- 若顯示「未驗證」或 HTTP 403：不能當成永久成功。
- 如果 H111 JSON 內 selected_categories 正確，但 App Reboot 後錯：檢查實際部署的 Page7 是否包含 H125_SELECTED_CATEGORIES_AUTHORITY 標記，以及 UI 載入訊息。
- 如果主檔暫時缺少某些舊產業：H125 會保留已勾選的標籤，不表示該產業此刻一定有可選股票；正式掃描仍遵守原有選股篩選及風控。
- 如果發現中文頁與 #U 字串頁同時存在，請先人工核對兩份代碼差異，不得強制覆蓋。

【測試性質】
本包已於原始 Page7、亂碼檔名模擬、H111 遠端假物件及離線重啟狀況測試。
尚未在使用者實際 Streamlit Cloud App 經過 GitHub 寫入／回讀／Reboot 實測；正式永久修復要依步驟 9～12 驗證。

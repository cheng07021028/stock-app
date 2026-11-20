股神推薦系統 V191-H177｜推薦紀錄刪除部署與雙權威保護修正
日期：2026-11-20

一、已核對的事實
- 使用者先前畫面顯示 H161：本機 2617 筆、Firestore 摘要 2560 筆，差 57 筆；禁止刪除。
- 2026-11-20 讀取 GitHub cheng07021028/stock-app/main：
  godpick_h176_delete_reconcile.py 不存在，另外兩個正式程式檔也沒有 H176 標記。
  其 Git blob SHA 與使用者原始 stock-app-main(3).zip 精確一致。
- 因此 H176 ZIP 雖已提供，但當前 GitHub main 尚未部署 H176，雲端重新整理不會自行取得修正。
- 目前沒有使用者正式 Firebase 服務帳密及完整 Firestore 文件直接存取權，不能在本機證明 57 筆是物理缺漏還是摘要過期，也不會自動在正式雲端刪除任何紀錄。

二、H177 增量修正
1. 累積內含 H176 安全對帳。僅在本機與 GitHub 完整 H127 分片 Hash 同一權威、Firestore 無新版本或同 ID 衝突時，才可把缺漏 ID 補回再精確刪除。
2. H177 新增安全防護：若已設定 Firestore 但摘要查不到，不允許誤以為只要 GitHub 一致就可以完成刪除，避免重啟後從 Firestore 還原舊紀錄。
3. 第 8 頁新增清楚的「H177｜刪除修正版已載入」版本文字，方便直接確認 Streamlit Cloud 取得新程式。
4. 禁止第 8 頁在新式安全儲存服務未載入時，使用舊版整檔覆寫做刪除後備方案。
5. UI 只有本機+經確認的遠端永久保存都成功才切換到已刪除狀態；原有 H109/H126 與選股不變。
6. 保留 14 張 Excel 等既有功能；不增刪股神推薦資料；不在 ZIP 帶入 Secrets、token 或任何正式 JSON。

三、交付的三個正式檔案（必須成套同步部署至 GitHub main）
 godpick_persistence_service.py
 godpick_h176_delete_reconcile.py
 pages/8_股神推薦紀錄.py
其餘 tools/ 是安裝器，tests/ 是回歸測試，不必部署到 Streamlit Cloud 才能執行第 8 頁。

四、使用方式（請勿直接將整份舊 ZIP 覆蓋整個專案）
 A. 先備份目前 GitHub main、runtime-data 的 H127 分片、Firestore 紀錄及程式。
 B. 將本 ZIP 解壓到「專案之外」的資料夾。
 C. 本機安裝或預檢（Windows PowerShell / CMD）：
    python tools/h177_preflight.py --root "D:\stock-app"
    只有出現 READY_APPLY_H177 才執行：
    python tools/h177_preflight.py --root "D:\stock-app" --apply
    D:\stock-app 請換成真正包含 pages/7_股神推薦.py 的專案根目錄。
 D. 若用 Streamlit Cloud，請將以上三個正式檔案「同一次提交」上傳至
    GitHub repository cheng07021028/stock-app 的 main 分支，確認路徑與中文檔名保持不變。
    只在本機解壓 ZIP 不會改動 GitHub / Streamlit Cloud。
 E. 等 Streamlit Cloud 部署新 commit 並重啟，進入「08｜股神推薦紀錄」，
    頁面需明確出現「H177｜刪除修正版已載入」。
    若仍只有 H161，代表線上未部署正確的檔案，請先處理部署，不要反覆刪除。
 F. 點「重新載入」，確認本機、GitHub、Firestore 的筆數和摘要，
    選擇一筆不重要的測試歷史紀錄按「刪除勾選」，核對狀態資訊；
    成功後重整、重新開啟 App，應只少該 record_id 一筆。
 G. 如果仍顯示 H176/H177 權威不一致、HTTP 401/403、Firestore 429 或內容衝突，
    請擷取本次「同步明細」中的完整文字供後續針對真正的遠端情況修復。

五、驗收資訊
- 累積必要 3 個 production 檔；安裝器支持原始 H170 快照或已安裝 H176 的精確 SHA-256 基準；未知新版本會阻擋並避免回退。
- 189 項跨版本相關離線回歸測試通過，含 H160～H177。
- 版本安裝器驗證：原始版可安裝；已裝 H176 可升級；重複執行顯示 ALREADY_H177；未知程式 Hash 拒絕覆蓋。
- 尚未實際在使用者 Streamlit Cloud 或 Firestore 寫入／刪除、沒有聲稱線上已修復。

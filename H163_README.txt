V191-H163 | Firestore 讀取效率與 429 配額熔斷
===========================================
程式基準：GitHub cheng07021028/stock-app main@d548c52a194c7c4ecc5ee7c948dacbd0808d2938
來源檔核對：godpick_persistence_service.py SHA(blob) bf0d2ffeb92d63fbe6cefc05d976b9978848be9d
           pages/8_股神推薦紀錄.py SHA(blob) 935eb0e253a5185fecec95adb97ca04aef5376d0
本包只包含必要修改／新增檔案：
  godpick_persistence_service.py
  godpick_h163_firestore_quota_guard.py (new)
  pages/8_股神推薦紀錄.py
  tests/test_h163_firestore_read_efficiency.py (new)
  H163_README.txt

變更：
1. Firestore system/godpick_records_summary 使用程序內90秒TTL快取；
   多次 Streamlit rerun 不需要每次都向 Firestore 讀取摘要。
2. Firestore摘要與完整本機權威 Hash/筆數相同時，不再串流讀取整個
   godpick_records (數千筆) 集合。版本/ID Hash 驗證不被省略。
3. 若 GitHub H127 權威摘要與 Firestore 摘要同 Hash/筆數，優先讀取
   GitHub H127 壓縮分片；如果 GitHub 快照不可讀，才安全回退一次
   Firestore 完整讀取。FireStore 確實有更完整的權威仍可正常還原。
4. 收到 Firestore 429 / RESOURCE_EXHAUSTED / Quota exceeded 時，
   同一 Streamlit 服務程序暫停 Firestore 讀寫30分鐘，不對耗盡配額
   進行無意義重試；既有 GitHub/本機權威依原版本治理使用，未驗證的
   資料不會被標示永久成功；因非配額原因造成的全表讀取失敗，
   相同權威快照有180秒冷卻，避免反覆讀取數千筆。
5. 原完整儲存介面發現 Firestore摘要同Hash/筆數，略過 2,632 筆
   全量重寫；增量刪改、其他儲存通路沿用 H161/H127 實際確認。
6. 08推薦紀錄新增 H163 配額/摘要快取/阻止重複大讀取計數，顯示
   同一服務程序內部統計（非官方計費量）。
7. 額外保護：配額耗盡且遠端無法確認時，舊相容恢復路徑不得
   建立新的空白或更新版權威蓋掉歷史。

完整保留：H127 GitHub 壓縮分片、H141 防縮水、H161 指定 record_id
刪除與遠端先確認、H126 正式買進權限、既有 Streamlit UI/設定。

驗收：
1. 將正式程式3個檔案依相同路徑覆蓋 GitHub main，不需上傳測試檔。
2. 不可刪除 runtime-data、Firestore、GitHub Tokens/Secrets 或舊紀錄。
3. Reboot App；進入08記錄頁，確認讀取來源、權威筆數及 H163
   Firestore讀取效率狀態。正常刷新時摘要快取命中增加；同Hash
   不應執行數千筆 Firestore 全量讀取。
4. 如果 Firestore 仍在每日配額限制期間，H163 會暫停反覆請求，
   GitHub 讀寫是否正常需使用 H162 第17頁獨立驗證。
5. Firestore 配額復原後用 Firebase Usage「單日」檢查實際讀取量；
   H163 為程序內保護，不涵蓋獨立 Worker 或其他程式的所有讀取。
6. 正式刪除仍必須取得完整權威並通過 H161 GitHub/Firestore遠端
   Hash驗證；不能在配額失敗時直接刪除或強行回寫不完整歷史。

離線回歸：47項通過：H163新增20項、H161 11項、H141 9項、
H127 7項（各套件測試數量以測試輸出為準）。
本包未部署正式 main，未使用實際 Firestore 帳密，未刪除任何紀錄。

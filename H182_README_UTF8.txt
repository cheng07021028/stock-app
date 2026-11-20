V191-H182｜推薦紀錄 2560 筆內容衝突之非破壞性差異稽核

重要：此版不會繞過 H161 刪除保護，也不會直接修復正式 Firestore 資料。
目前 2617（本機）/2560（Firestore）只是筆數線索，不能證明 2560 筆既有資料可覆蓋。

【修正緣由】
H176 在比對內容時，只忽略 updated_at，任何同 ID 其他欄位不同均回報「2560筆不同」。
這無法辨識：新版新增欄位、不同欄位結構、同欄位值確實不同。
H182 新增第 8 頁「本機／Firestore 差異稽核（唯讀，不刪除）」按鈕，且對 H176 失敗理由附上欄位級分類：
  VALUE_CONFLICT：同名欄位值衝突，必須保留並人工處理
  LOCAL_EXTRA_FIELDS_ONLY：只在本機新增欄位，可能是新版資料拓展
  REMOTE_EXTRA_FIELDS_ONLY：只在 Firestore 存在，不得覆蓋
  BIDIRECTIONAL_SCHEMA_DRIFT：雙方均有對方沒有的欄位
並列出前四個相關欄位名稱、異動筆數，不輸出敏感欄位值。

【程式更新】
- godpick_h176_delete_reconcile.py（更新：原檢查不放寬，增加有用的錯誤原因）
- godpick_persistence_service.py（新增人工觸發的只讀差異查詢，沿用H163配額保護）
- pages/8_股神推薦紀錄.py（新增只讀診斷按鈕；不必再按刪除才能看差異）
- godpick_h182_record_diff_audit.py（新檔：純讀取差異分類，含選定 ID 身分核對函式）
- tools/h182_preflight.py（安裝與 SHA 防回退驗證）
- tests/test_h182_record_diff_audit.py（模擬 2617 / 2560 差異）

【本機預檢與安裝】
1. 停止執行中的 App 並備份 GitHub main 程式與資料庫；不要直接修改正式紀錄 JSON。
2. 將本 ZIP 解壓到獨立資料夾，從解壓後資料夾執行：
   python tools/h182_preflight.py --root "D:\stock-app"
3. 只有 READY_APPLY_H182 才執行：
   python tools/h182_preflight.py --root "D:\stock-app" --apply
4. 將上述四個 .py 正式程式檔一起同步上傳 GitHub main，待 Streamlit Cloud 重新部署。
5. 進入 08 推薦紀錄 → 展開「H182｜本機／Firestore 差異稽核（唯讀，不刪除）」→ 點一次檢查。
6. 擷取分類摘要（特別是 changed_fields_top 和 local_added_fields_top）；不要再按刪除測試。根據真實差異結果才可決定精準雙端刪除或非破壞性欄位補齊。

【範圍與誠實限制】
已模擬測試「2560 同 ID 只有本機新增欄位」以及真正修改推薦分數的不同狀況；前者不再和後者混淆，後者仍嚴格阻擋。
本機測試成功不代表已取得正式 Firestore 2560 筆完整內容。不能據截圖推斷所有衝突只來自新增欄位。
本版不包含正式歷史資料、Firestore/GitHub 憑證，不修改 H126 買進授權或股神選股模型。

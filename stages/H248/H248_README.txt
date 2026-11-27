V191-H248 | Selective diagnostic and retry engine
===============================================
用途：中央14項工作依失敗原因分類、同一正式時段限次補跑、Page17選擇性補跑及獨立回執。
前置：實際套用H245/H246/H247 Overlay（不是只上傳安裝器或ZIP）。H244 Page17原始版與H244完整差異版均可辨識；本包不代替H244安裝。H244已套用的功能保留。

1. 將本ZIP解壓至專案外的獨立資料夾。
2. 執行 RUN_H248.bat，輸入最新stock-app完整原始碼資料夾。或執行 python apply_h248.py "D:\stock-app"。
3. 程式核對Git blob SHA、前置實際執行定位點及全部修改定位點；不直接修改專案。成功才會產出 StockApp_V191_H248_Overlay.zip。若BLOCKED，請勿強制覆蓋。
4. 備份現有程式碼（勿將正式資料或secret納入程式碼ZIP），將Overlay內檔案覆蓋至原專案。
5. 在完整專案根目錄執行：
   python -m unittest discover -s tests -p test_h248_scheduler_contract.py -v
   須確認51項均完成且無失敗/跳過；完整專案整合測試不可當作已在交付環境驗收。
6. 確認git diff只含預期修改，提交GitHub main，等待既有Worker部署；進入Page17確認H248診斷欄及按鈕。
7. 查核正常到期、無符合補跑條件、暫時性失敗、冷卻期、失敗上限與人工派送之Run ID/attempt。不要反覆按強制全模組來驗證。不要製造正式資料異常。

策略說明：
- 新正式時段及既有手動全量/單項模式仍保留；正式自動排程同一slot只在判定可安全補跑時續跑，最多3次實際handler呼叫、間隔至少20分鐘。此為H248新增保守政策；例行排程不再於單次wake內重複呼叫；手動模式仍保留原設定上限但只允許明確暫時性且屬安全更新的立即重試。
- 原有核心五項SUCCESS前置門檻、H28順序、H153.3遠端阻擋、H245/H246權威驗證及H126授權不降低。非核心資料警示不冒充成功。
- 明示待續的追蹤/更新工作、可安全更新的暫時性失敗可補跑。未知寫入結果、憑證錯誤、程式錯誤、原始證據缺件、資料權威衝突均不進行盲目重試。遠端封存不確認只提示永久化/H246查核，不自行重跑全市場或重建歷史。
- H248選擇性補跑依真正到期的排程slot判定；不含全新到期工作、成功工作或未啟用工作。Worker重新檢查，空計畫零項執行，不能顯示完整成功。例行排程仍處理全新到期工作。
- 補跑回執單獨保存於既有排程狀態(last_h248_retry_batch)，執行明細需與計畫、Run ID、attempt一致，既有遠端checkpoint/驗證器仍為永久性確認。未通過仍保留WARNING/FAILED/BLOCKED。
- 已經有Worker排隊/執行中或GitHub狀態未知時，Page17不重送選擇性補跑；UI亦有2分鐘session冷卻。這不是跨程序永久佇列（H249後續）。
- 重試計數藉由既有狀態檔保留。若Worker中斷在終端遠端checkpoint之前，尚未持久化的嘗試可能遺失；尚未提供跨Worker exactly-once保證，H249再處理。舊狀態僅同slot保守判斷，無法重建所有過往嘗試。
- 此階段只自動化中央14項工作，其他獨立H195/H206/H219/H220/H221/H222/H224等GitHub workflow仍依既有排程和H247修正，沒有新增遠端強制派送；歷史資料尚缺證據仍待補齊。

修改內容：godpick_auto_scheduler.py、tools/run_godpick_auto_scheduler_v191.py、tools/h159_manual_verdict.py、pages/17_系統健康檢查.py、godpick_h244_dispatch_truth.py；新增godpick_h248_retry_policy.py與離線/整合測試。無資料庫migration、歷史JSON修改、排程設定覆寫或secret寫入。
回滾：以升級前git commit/備份回復上述5個原檔及移除H248新增helper與測試；不要回復runtime-data舊快照或刪除推薦紀錄。
交付限制：GitHub來源透過連接工具讀取並核對23個唯一定位點；完整原始碼未能下載到本機，因此本包是版本鎖定差異產生器。45項本機純策略測試通過，6項完整專案整合測試待執行，未部署且未聲稱正式雲端通過。

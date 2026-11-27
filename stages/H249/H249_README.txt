V191-H249｜中央永久派工佇列、工作日誌及中斷安全復原
====================================================
前置：H245、H246、H247、H248 的實際 Overlay 均已套用。H244 已套用及未套用的精確來源均可識別，不回退既有 H244。只上傳ZIP/安裝器不算安裝完成。

1. 將本ZIP解壓至專案外的資料夾，備份現有程式碼。
2. 執行 RUN_H249.bat，輸入已套用 H248 的 stock-app 根目錄。
3. 安裝器反向驗證H248差異至原Git blob、核對H248 helper SHA256，且不直接修改專案。若PASS，產生 StockApp_V191_H249_Overlay.zip；若BLOCKED勿強制覆蓋。
4. 覆蓋Overlay後，在完整專案根目錄、非GitHub Actions環境執行：
   python -m unittest discover -s tests -p test_h248_scheduler_contract.py -v
   python -m unittest discover -s tests -p test_h249_queue.py -v
   應完成原51項H248及43項H249測試。此次離線環境原H248有6項完整專案測試跳過，不能宣稱已完成。
5. 查核 git diff，提交 main，Streamlit reboot。需檢查Streamlit GitHub PAT對指定repo有 Contents read/write + Actions read/write；Worker workflow新增 actions: read，既有 contents: write 保留。不要把token貼進聊天、日誌、ZIP或Git。
6. Page17 按既有單項/選擇性補跑/強制按鈕時，先看H249永久派工ID，再看H244本次Run ID及H248狀態。測試優先採低風險單項，不要以破壞正式資料測試取消。建議隔離測試repo驗收CAS、取消、重啟、額度及完整工作相依。

機制：
- runtime-data/data/h249/queue/YYYYMMDD.json：每天獨立、SHA CAS讀回；最多100筆/日、不刪舊派工，最近兩日顯示；同模式未完結跨Session去重；24小時有效、最多3次認領。逾期不自動執行。舊日JSON保留，較早紀錄須到runtime-data查看。
- Page17手動派工先確認遠端佇列；即時workflow喚醒失敗仍可由例行Worker空閒時接手。普通到期喚醒維持原入口。例行正式工作優先，每次空閒最多接一筆佇列。現有Windows喚醒、GitHub cron、原14工作與人工全量/單項入口保留；直接在GitHub手動派送的舊入口未自動轉成永久請求。
- runtime-data/data/h249/slots/YYYYMMDD/job_HHMM.json：GitHub Worker實際handler之前永久認領，返回後記錄；同時段H248已知暫時性/待續才可補跑，最多3次且間隔20分鐘；包含可核實的舊H248嘗試次數。手動force使用manual/RunID/job.json，避免同一Run的重跑；手動H248原有可安全立即重試為同一handler執行群組，並非每次內部HTTP呼叫都單獨記錄。
- CLAIMED且無工作開始證據，確認原Run/Attempt終止後可重新認領；已有開始/返回紀錄但未證明完成則REVIEW_REQUIRED，不自動重播。尚未開始的正式到期slot由既有排程繼續。EXECUTION_RECORDED若原批次遠端status及全部history仍精確相符，可在下一wake唯讀補驗，否則保留待查，不能以新批次替代。
- VERIFIED需實際執行、同Run/Attempt、完整scope、遠端status及全部history匹配。原H159及H243/H33保留，新增H249顯式請求未驗證的GitHub失敗判定；例行喚醒仍保留原有低噪音政策。
- 永久控制層不可讀/寫時保守停止該GitHub handler，避免無證據的副作用；可能暫停正常工作，需修復GitHub權限/服務後依狀態人工查核。與H248一樣不盲目重試未知/衝突。
- H249不是任意位置續算、不是exactly-once交易事務，不會自動續接已開始的完整強制推薦批次（H155同輪前置門檻不變）；任何資料寫入未明必須先對帳。直接本機執行不受此遠端日誌保護。沒有改變原本H126選股、歷史紀錄、正式授權及資料門檻，沒有自動修補缺失財報或57筆歷史證據。
- 新增的遠端控制檔會增加GitHub commit、API請求及容量；典型工作每次需兩次CAS保存（另有佇列事件），正式耗時、限流及中斷端到端待H250實測。每次啟動先讀少量佇列；UI最多每60秒刷新一次或人工刷新。不新增獨立cron，不使用本機背景程序。
- 回滾：先停止/確認沒有Worker執行，再回復升級前4個程式/Workflow檔，移除H249新增helper。不要回滾或刪除runtime-data歷史及H249待處理佇列；回滾期間舊程式不會消費佇列，必須人工核對未完成請求。

交付限制：能透過GitHub讀取來源與核對定位點；完整main仍無法下載至本機，故為精確差異產生器，不是已套用正式原始碼的直接覆蓋ZIP。43項H249離線測試、45項H248策略回歸、精確GitHub worker blob轉換與語法檢查已通過；完整專案安裝、工作流程正式語意、正式遠端API及雲端端到端均尚未驗收。未部署、未寫入正式GitHub資料。

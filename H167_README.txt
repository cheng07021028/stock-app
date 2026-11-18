V191-H167｜第17頁遠端權威對帳 × 排程舊狀態說明 × 重複派送保護
================================================================
基準：2026-11-18 GitHub main 的 H166 正式程式；本次修改限定以下必要檔：
1. pages/17_系統健康檢查.py 〔原檔完整保留，其上局部追加 H167〕
2. godpick_h167_remote_truth.py 〔新增純資料核對模組〕
3. tests/test_h167_remote_truth.py 〔11 項新增回歸測試，部署不必複製〕

實際核對：
- 2026-11-18 16:12:06，runtime-data 中 data/config/godpick_auto_scheduler_settings.json
  的 enabled=true，14項工作皆啟用、原時段與順位不變。
- 舊 status 更新於 2026-11-18 10:21:03，當時 SKIPPED_DISABLED。
  新設定發生在舊狀態之後；不能把舊 SKIPPED_DISABLED 視為新設定失敗。
- 2026-11-18 16:12 起已有兩個 Central Auto Scheduler workflow_dispatch，
  觀察時第一個正在執行、第二個排隊；派送並非完成證據。
- GitHub runtime-data super_ai_market_context.json 更新 2026-11-17 22:03:58：
  TWSE融資券 1068檔，但 TPEx來源逾時、TPEx覆蓋0；TAIFEX PCR與外資TX為11/16。
  H166畫面顯示0檔可能是Streamlit本機舊快照，不能以此否定遠端的1068檔。
  但資料落後／上櫃缺漏是真實問題，H166仍須維持 NOT_READY 警示。

修正：
A. Page17顯示SuperAI本機/遠端快照來源及各自更新時間，從有日期的較新快照
   做H166來源驗證；GitHub無法取得時保留可用本機資料並標示不能證明遠端最新。
B. 針對永久啟用時間晚於舊 SKIPPED_DISABLED heartbeat 的情境，
   顯示「設定已啟用、等待Worker新checkpoint」，不可稱已執行成功。
C. 本頁手動 Worker 派送受理後更新既有H19 Session 8分鐘冷卻時間，
   避免手動和Page17補跑Fragment在同一瀏覽工作階段馬上重複派送。
D. SuperAI遠端JSON唯讀查詢使用Streamlit cache_data 150秒TTL，
   不在每8秒fragment重下載，也不做Firestore讀取。

保持不變：
- H126正式買進授權／H141、H161永久紀錄／H163配額保護不變。
- 原14項任務、排程順位、時間與runtime-data設定不變。
- 第07頁選股前置原始SUCCESS門檻不變；不把WARNING改SUCCESS。
- 不執行或派送任何模擬的正式工作，不修改GitHub遠端資料。

測試：
- python -m pytest tests/test_h166_operational_readiness.py tests/test_h167_remote_truth.py -q
  => 29 passed（18項原H166+11項H167）；語法檢查通過。
- 這些均屬離線模擬，不代表正式Worker或最新股票推薦已完成。

部署：
只把兩個正式.py按ZIP相對路徑複製到GitHub main。
不要覆蓋runtime-data、任何股神推薦歷史、Secrets、H126、設定檔。
重新啟動Streamlit後查看H166面板新增的「H167 SuperAI資料取用」行與
「H167排程狀態核對」訊息。讓目前已執行/排隊中的Actions完成，再以新的
runtime-data本輪履歷判斷選股是否恢復。

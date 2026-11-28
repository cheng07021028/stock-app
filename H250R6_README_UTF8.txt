V191-H250-R6｜手動執行即時戰情與遠端進度監控
本包是直接覆蓋的差異檔，不是安裝器，不需執行 BAT 或 GitHub prepare workflow。

前置版本：R5 已部署（Page17 已出現 H250-R5 手動強制執行）。已比對以下原始 Git blob SHA1：
  godpick_h250r5_manual.py d9b5828fece4d16dfbf03bdacf5d9ba8bdb87ecd
  tools/run_godpick_auto_scheduler_v191.py 4f71ed1e2c09655e99255ba23d3080dc6142fbce
  pages/17_系統健康檢查.py 5d0e1c0763ab77ce23cf267e2c7fdfa8b7c07ed5（本次不修改）
如正式檔案已有其他更新，請先比對勿盲目覆蓋。先備份上述前兩個檔案。

將 ZIP 解壓，僅以下三個正式檔案依原路徑覆蓋到 GitHub main，最好同一 commit：
  godpick_h250r5_manual.py
  godpick_h250r6_monitor.py（新增）
  tools/run_godpick_auto_scheduler_v191.py
其他 tests/、README、TEST_RESULTS、SHA256SUMS 為驗收資料，可留本機。無需修改 workflow YAML、權限或任何 runtime-data 檔案。Reboot App，Page17 主畫面會顯示 H250-R6 戰情中心。

運行說明：
- R6 使用 R5 的原有 render_button/render_status 入口，因此不覆蓋整個 Page17，保留原功能、一般例行排程區、H156 操作及 H126 門檻。上方新增人工專區，原有一般排程區維持原樣。
- 人工專區每15秒輪詢，顯示 GitHub 精確 Run/Attempt、排隊/準備步驟/工作返回/終態。等待階段的數量只是已保存設定的預覽；Worker 真正計畫優先。
- 保留原120秒防連點，新增活動批次鎖定、查核不可用時停止派送、受理不明時人工解除。跨使用者非原子查詢仍存在競爭窗口，未另建 H249 CAS 派工；既有中央 concurrency 繼續序列化。
- R6 Worker 只在 force_enabled_jobs 模式發布獨立進度檔 data/h250r6/progress/<run>.<attempt>.json 到 runtime-data。僅存工作 ID、狀態、時間及耗時，不複製原始供應商錯誤訊息或憑證。遠端終端回執沿用原R5訊息和驗收，不擅自產生結果。
- 每批14項最多30次小型 GitHub Contents PUT（開始、每項開始/返回、結束），使用同一檔案 SHA 更新；任一失敗停用該輪進度回報，不影響原業務及終端驗收。UI每輪約1至4個唯讀請求（另外原Page17仍會原樣輪詢）；多人及大量派工的API額度和效能仍需正式量測。每批保留獨立小檔，不自動刪除。
- 上傳時已在執行的R5 run 不會被改寫，僅能取得 Actions 步驟和原終端回執；逐項新進度須下一次R6 Worker。請先讓現有批次結束，不要重送或取消以驗收新版本。
- 全部成功須同Run/Attempt、完整逐項終端回執、無警示失敗阻擋及 GitHub success；原H159仍執行遠端history核對。R6不自行變更既有驗收，不把進度檔當成業務永久保存權威。若終端權威已被更新批次取代，僅保留可查進度及未取得完整驗收提示。

驗收：本機新增23項測試通過，R5回歸34項通過，6項實際完整排程測試因未取得完整專案跳過；本機未安裝Streamlit，正式頁面渲染與真實網路/回報耗時未實測。可在完整專案執行 python -m unittest discover -s tests -p 'test_h250r*.py'（或分別指定 test_h250r6_monitor.py 及 test_h250r5_manual.py）。回復：還原原先備份的兩個覆蓋檔即可，新增R6模組可保留未引用，不需刪除已存在進度資料。

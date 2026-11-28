V191-H250-R10｜歷史證據補齊、還原績效收斂與精確驗收治理
基準：使用者提供 stock-app-main (2)(1).zip + H250R9 正式修正包；R9 tracking/evidence/monitor/H241 的 Git blob 已與正式 main 核對相同。
安裝：備份後，只將下列五個正式程式檔按路徑直接覆蓋 GitHub main，保留其他檔案，不需 BAT、Overlay、workflow 或 Token 修改。
godpick_h250r7_tracking.py
godpick_h250r9_evidence.py
godpick_h250r6_monitor.py
godpick_h250r10_recovery.py
tools/h250r10_gap_repair.py

本版中央 Worker 沿用 R7 install_handlers 入口，改由 R10 歷史規劃器計算；原 R9 API 保留供回歸及比較。08/10 已部署 R9 時會自動透過 evidence 模組增加診斷表、明細、行情補證清單；H241 的 R9 _merge 由 EVIDENCE_COLUMNS 保護新增4個證據診斷欄位，無需覆蓋 H241 原始檔。
還原價：每輪預設64個市場/股票、最多128，8路並行、先未查詢再最舊已查詢。失敗6小時冷卻，HTTP429停止後續查詢波次；原價不冒充還原價，沿用08舊標籤不列為嚴格核實。既有還原證據保留，不刪除。新日期或身分路由改變仍需重新核實。例行排程依現有 H243 到期及20分鐘冷卻規則，不建立額外工作流程；manual force 不保證立即自動補跑，請使用第10頁單項派送或原有例行排程。永久性缺口不觸發無限重跑。
驗收：WORK_VERIFIED 為本輪執行/保存確認（非完整證據），DIAGNOSTICS_CONFIRMED 與指標永久確認分列；H159、H126、H138均未降低。只要仍有警示，嚴格人工批次可仍未通過。R6 UI 改為已返回/品質待補，而非混稱所有工作失敗。
行情補證：GapQueue.csv 是使用原始2385筆Excel及封存離線重算的716個股票交易日、176個市場日期查詢目標，不是確定可補回的資料。可能停牌、無交易、來源漏列。下載08/10新畫面清單可取得新快照。
本機可唯讀核對：python tools/h250r10_gap_repair.py --queue <GapQueue.csv>
若確認需取得官方補證，且有最新完整本機專案，可明確執行：python tools/h250r10_gap_repair.py --queue <GapQueue.csv> --apply-local --budget 8
預設不連網、不修改資料。明確 --apply-local 才由既有H190/H212官方全市場日資料驗證器補充本機 supplemental 封存，最多12次請求；不覆蓋原始、不自動上傳。官方未提供、無成交、既有補件或來源衝突則保留待查。正式上傳僅在確認補件通過H190讀回驗證且審查後進行；不要上傳舊 runtime JSON。
限制：47筆缺日期、14筆休市基準不一致均無足夠原始證據可自動修復；不修改原始ID、市場、日期或價格。不宣稱1690筆還原待證已清除；真實重算使用先前匯出Excel及H190本地封存，沒有向Yahoo或GitHub執行正式寫入。合成資料與H241 CAS測試不能代替正式部署。實際網路源、Streamlit畫面、遠端寫入與無人值守驗收仍待部署確認。
測試：147項unittest（141通過、6跳過）；91項pytest通過；867個Python檔編譯；真實2385筆三項離線規劃重跑零變更。

V191-H216 累積必要修正（H209.1～H216）
原始基準 stock-app-main(4).zip；H215 累積修正包。GitHub main 唯讀核對 H215 程式 blob SHA e99c81ccddb24d9e4ebacbe55837cf8662062186 與 H215 包一致；未改動 GitHub。
安裝：獨立解壓本包，先執行 python tools/h216_preflight.py --root <stock-app目錄>，確認 READY_APPLY_H216 後加 --apply；未識別版本拒絕覆蓋，備份目錄不要提交 GitHub。正式檔依原路徑上傳含 .github/workflows；不覆寫任何資料、secrets 或私人紀錄。測試可一併上傳。保留 H126、風控及95%門檻。
H216：H190 相同 cutoff 共享經檔案指紋驗證的索引；H194 不再無條件清快取；H193/H212/H213/H214/H215 同輪重用既有技術與強勢報表；報表中途若官方行情檔變更則拒絕混合世代。第32頁原H215驗收區的管線健康新增階段耗時，Excel 06_管線健康亦新增耗時。
H190 新增 today（台灣18:15後，優先官方日曆與當日雙市場行情，最多3次請求，不補歷史、不取代原 health.json），先提交 data/h190/ 的當日資料再執行既有歷史回補。H194 19:15 加一輪，盤前、日曆未確認、行情缺口時略過昂貴研究；capture_gate 提供原因，跨日不得封存。健康JSON先於Excel產生；已保存健康時即使 Excel 錯誤仍嘗試提交帳本，保留工作流錯誤。既有排程均保留。
H209 上櫃優先查詢最長14個日曆日（同月），保留單日重試及原請求預算；新增明示區間 metadata 相容解析，缺失/衝突/反序/無證據空表仍拒絕。collector.json request_log 保留各次回應欄位、日期、狀態，避免最後一次覆蓋第一次診斷。不宣稱已解決正式API根因：本機實際請求DNS失敗，web亦無法取回此JSON。
部署後：1. GitHub Actions 執行 H190 official full-market historical archive（台灣18:15後）；核對 data/h190/session_health.json，SESSION_QUOTES_VERIFIED 及兩市場筆數。2. 當晚 H194 prospective research performance，核對 data/h194/latest.json.gz capture_gate；CAPTURED/ALREADY_CAPTURED，並核對當日 signals 檔及真實筆數；非交易日不得造資料。3. H209 prospective rolling backtest，查 data/h209/collector.json request_log、pending、saved 和 errors，重點為 TPEX。4. H215 daily data proof and status audit：下載 H215_Proof.xlsx，查看 01、02、06。成熟績效需真實交易日經過，不能當日造出。
實測：H190～H216 443 項選定回歸通過；完整實檔前後相同資料、2026-11-25 17:50+08:00。第一次 7.40→2.01秒；最終重測 7.30→1.91秒、讀取 1682→439；除新增耗時欄位，整份回傳資料 SHA256一致。非網路、Excel或正式UI速度。正式來源及部署尚未實測，新股/停牌/減資分割、真實上櫃覆蓋率及成熟績效未宣稱改善。

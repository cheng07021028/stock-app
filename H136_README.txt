StockApp V191-H136.0  |  自動資料中樞（H128-H136 累積修正）
=========================================================
這是「程式碼與離線模擬交付」，不是已完成你本人 GitHub/Streamlit Cloud 部署。

一、目標
  讓原本 H133/H135 每次上傳歷史訊號、個股日K、大盤日K的流程，
  改為從既有 H134 決策封存 + H117 真實盤後行情自動彙整。
  第24頁 H133 和第26頁 H135：預設優先使用 H136 遠端資料。
  第27頁：讀取目前 H136 來源、筆數、截至日期、SHA 校驗與缺漏。

二、實際資料流
  1. 你在第7頁建立 H126 研究決策、第25頁封存一次；
     H134 封存目前仍需要操作「封存」，不會自動偽造每日推薦。
  2. 原本的 H117 排程定期產生 TWSE/TPEx/TAIEX 真實盤後日資料。
  3. 新 H136 GitHub Actions （工作日台北時間 22:30）讀取：
       runtime-data/godpick_h134_decision_vault/H134_*.json
       data/h117/daily/{TWSE,TPEX,TAIEX}/*.json.gz（首選完整來源）
     若沒有完整日封存，再從既有H116衍生的verified_ohlc.csv和benchmark_ohlc.csv讀取；
     不能補任何假收盤價、週末K線或過期訊號。
  4. 產生 runtime-data/data/h136/H136_{signals,stock,benchmark}.csv、
     H136_manifest.json 與 H136_health.json；缺資料時只更新 health，
     不覆蓋之前合格資料，畫面也不會把上一份成功快取標為今天資料。
  5. Streamlit 從 GitHub runtime-data 讀回資料並檢查 SHA256、日期及筆數。

三、安裝方式
  1. 先備份正式 GitHub 程式、runtime-data 分支的推薦權威紀錄及使用者設定。
  2. 本ZIP已有H128-H136所有必要程式。依相同路徑定點合併，不要刪除整個專案；
     如果線上版本更晚，必須先比較差異。更新包不含任何權威 JSON、使用者資料或密碼。
  3. 將 .github/workflows/h136_research_data_hub.yml 一併提交到預設程式分支。
  4. 確認 GitHub Actions 已開啟、contents:write有權限、runtime-data 分支存在。
     既有 H117 EOD 排程與 H134 的 GitHub 存取配置也必須正常。
  5. 可先從 Actions -> H136 nightly read-only research data bridge -> Run workflow
     人工觸發一輪確認，再等待下一個交易日排程。不要在GitHub網頁貼上任何Token。
  6. 先看 Actions workflow 是否成功，再看第27頁同步狀態；
     第24頁07、第26頁02的資料是否自動載入，確定後試關閉APP/重啟回讀。

四、保守保護與限制（必要閱讀）
  * PIT：H134 GitHub SHA雜湊並非獨立不可變交易時點證明，不能說回測已完成PIT核證。
  * H117封存、H116衍生CSV尚未獨立交叉確認來源及除權息調整，
    H133/H135仍應保留企業行動、成本、停牌及完整日缺價的驗證。
  * 資料太少、研究日與收盤日不對齊會標記未就緒；不能強行輸出有效勝率。
  * H126/H109正式交易風控門檻完全不變。H136不給買進許可、不調權、不下單。
  * 不會自動取得未授權券商持股與真實成交。這兩類資料仍需使用者自行授權。
  * H132當期上市公告原來的按需查詢不受影響；並未增加未經審核的新聞來源。
  * 本次未做 GitHub 連線、TWSE交易日真實連線或 Streamlit Cloud部署驗收。

五、驗收狀態
  已做合成離線測試、原始 ZIP 基準覆蓋模擬、Python編譯、資料 SHA 保護。
  下個階段可在正式權限與H117資料具備後做遠端寫入/回讀/重啟驗收。

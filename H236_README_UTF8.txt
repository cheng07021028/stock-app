V191-H236 官方XBRL財報雲端持久入庫（選擇性 Google Cloud Storage）
累積H209.1-H236，僅必要程式、測試及版本工具。不含行情、原始財報、憑證、個人資料或Secrets。安裝前 python tools/h236_preflight.py --root "實際專案目錄"，相容再加 --apply。保留.github隱藏資料夾。
正式H222 2026/11/26 15:55紀錄16/64份成功，14份月報13041筆，2份季度財報1944筆；4拒絕44待補。H190正式盤後仍11/25，H194健康檔未更新。H236尚未部署，未取得實際官方原ZIP或GCS存取授權。
【儲存與權限】由管理員建立專用私有GCS bucket，開啟防公開存取、建議版本保留；設定費用告警。發布端使用僅具指定前綴 stockapp/h236/v1/ 的 storage.objects.create 與 storage.objects.get 權限，不授予覆寫、刪除；讀取端僅 storage.objects.get。不需列舉bucket。使用專用服務帳號，勿共用現有廣權限帳號。CLI可使用Google ADC（專用帳號）或 H236_GCS_CREDENTIALS_JSON 環境變數放服務帳號JSON；另設 H236_GCS_BUCKET（bucket名稱）及 H236_GCS_PROJECT（專案ID）。憑證絕不放進GitHub檔案、ZIP或聊天。依賴google-cloud-storage已在原requirements.txt中。
【發布】在可信收集主機設定上述發布端環境變數，執行：
python tools/h236_cloud.py collect-publish --quarter 2025Q3 --official-url "從官方下載頁取得且符合H235白名單的完整HTTPS網址"
若已有H235直接下載且核實的source.zip及receipt.json，可信發布者可用：
python tools/h236_cloud.py publish --quarter 2025Q3
H234任意本機ZIP不能發布。H235網址格式仍未實際取得官方ZIP，拒絕時不可猜網址或手動偽造憑據。發布端是信任邊界，H235與H236雜湊不是官方數位簽章，需以IAM管控可信發布者。
雲端先寫以SHA256命名的原始ZIP，最後以if_generation_match=0建立固定季度manifest.json；原始物件已存在但憑據未提交時，完整下載重驗後可續跑；已提交不同證據則拒絕覆寫。未提交原始物件不能還原。保留GCS世代及原始首次核實時間。status只檢查憑據和物件世代/大小，不代表完成原始ZIP重驗。
【還原】在運行系統以專用唯讀憑證配置同一bucket：
python tools/h236_cloud.py status --quarter 2025Q3
python tools/h236_cloud.py restore --quarter 2025Q3
先驗證雲端憑據、物件世代、大小、SHA256，再以當前母體重驗原始財報；不覆寫本機既有證據，未完成的本機資料夾保留人工檢查（不刪資料）。資料還原至data/h235/YYYYQn，H192/H222既有讀取流程生效。已核實還原不再次下載原始ZIP。不要 git add data/h235 或 data/.h236-restore-*。雲端來源與本機快取非官方數位簽章。單期2GiB下載/8GB展開既有上限；大型檔案需足夠磁碟、RAM、執行時間，Streamlit Cloud限制仍需實測。
【Streamlit】管理員設定頂層Secrets環境變數 H236_GCS_BUCKET、H236_GCS_PROJECT、H236_GCS_CREDENTIALS_JSON（唯讀），H236_ENABLE_UI_RESTORE="1" 才啟用已登入頁32選定單季度還原/查詢。預設關閉，無自動下載；重啟若本機快取消失須再次還原，雲端原檔仍在。啟用後所有已登入可進入該頁者可發起唯讀還原，請僅於可信使用者環境啟用；雲端寫入只在CLI。原H222清快取，其他研究依原TTL或重新整理。部署環境無法持久保留本機快取時，需每次重建後還原。
【GitHub Actions選擇性】Repository variables: H236_GCS_BUCKET、H236_GCS_PROJECT、H236_RESTORE_QUARTERS（逗號分隔1至8個已提交YYYYQn，預設留空）。Repository secret: H236_GCS_READ_CREDENTIALS_JSON（專用唯讀JSON）。原H220、H222、H223、H224工作流在已啟用設定時先還原；缺憑證或未提交季度、超過每輪2GiB缺失原始ZIP預算、世代或雜湊不符會停止該次流程，不封存不足證據。若8季超預算不可強行放寬；可先挑較少已提交季度，或本機分季還原/使用持久運行主機。每次Actions為新環境，可能重複下載，會產生網路費用/時間；預設不連網，不新增排程。原工作流只提交既有指定公開資料範圍，不提交大型ZIP。H223補上H235異動簽章。
【驗收】原始1,966檔，0實際批次、0完整成長候選；H222核心約0.36秒，6張Excel正常。23項新測試包含模擬GCS世代/條件寫入、雲端提交中斷後重驗續跑、雜湊驗證、原時間隔離、八季雲端合成財報還原後H222具名成長候選、預算/重複還原/未知版本防護。沒有雲端SDK實機或帳號權限驗收，沒有官方ZIP真實回補，不修改H126授權或權重。未取得11/26正式盤後績效。

# V191-H153.8｜7月舊快照回退防護 × 11月runtime-data權威恢復

## 實際根因（2026-11-11）

現場第 7 頁警示：「K線=2026-11-11；大盤資料=2026-07-09；官方因子=2026-07-09；落後89交易日」。
對照 GitHub：**main** 分支內 `market_snapshot.json` 的資料日期是 **2026-07-09**；**runtime-data** 分支內 `market_snapshot.json` 的 TWSE 日期是 **2026-11-11**（更新於22:05:14）；`runtime-data/official_factors_cache.json` 業務日期是 **20261111**，2,367筆（更新於22:05:56）。第7頁在 Streamlit Reboot 後直接讀取 main/本機 packaged JSON，沒有以業務日期優選 runtime-data。原 H40 大盤備援也可能把舊橋接重新載回。

## 新增及修改

1. 新增 `godpick_h1538_source_authority.py`：只允許三個檔名 `market_snapshot.json`、`macro_mode_bridge.json`、`official_factors_cache.json`，使用**內容的交易日**比較 local/remote，絕不能使用 `updated_at`、檔案 mtime 當交易日，也不得選擇「未來日期」。
2. 對 GitHub 專案 **公開**且可被 GitHub API 證實為 public 的情況，可在沒有 GITHUB_TOKEN 時唯讀公開 `runtime-data` 分支；private repo 必須有Token。唯讀使用 HTTPS requests，單次網路逾時、12 MiB限制、TTL快取，**不寫入 GitHub**，不會更改推薦紀錄/設定/Secrets。
3. 若 remote 經業務日期證實比 local 新，允許僅在 Streamlit 執行容器內原子覆寫同名**快取副本**，令原有官方因子模組使用新內容。該動作不代表已永久寫入。若 remote 較舊，保留較新 local；無遠端且舊快照超過10天，隔離為「來源待驗證」，不得利用7月快照調權或當成今日官方因子。
4. `pages/7_股神推薦.py`：使用新讀取器；`_project_data_freshness_snapshot_v173` 不再以本輪股票欄位的大盤／官方日期冒充獨立資料源已更新；也不再把快取 `updated_at` 當資料日期；新增 UI 來源訊息。H40 備援另做業務日期有效性檢查。
5. 不改動原有 H126 正式交易門檻、H153.7 主管具名表、12 Excel、H150/H153.3排程、推薦歷史、權重與雲端 Token；完整差異包只含程式、測試與 README，不包含任何舊資料 JSON。

## 部署後驗收

- 確認 `runtime-data/market_snapshot.json` 的 `twse_data_date` 及 `runtime-data/official_factors_cache.json` 的 `data_date` 為完整盤後實際業務日；本版**不會捏造最新行情**。
- 重新啟動 Streamlit 第 7 頁，新鮮度應呈現 **2026-11-11**（若當時仍為 11/11），或下一個真正完成交易的交易日；不能再突然跳回 2026-07-09。若 runtime-data 本身停止更新，應顯示待驗證並禁止正式升格，不用假資料補日期。
- 若已更新 H150/官方因子但 `main` 仍舊7月，H153.8 可讀 runtime-data 來源；`main` 舊 snapshot 不必刪除，但未來新ZIP不應再夾帶舊資料。
- `GITHUB_TOKEN` 未設定時，**唯讀**可能正常；推薦紀錄／掃描設定永久寫入**仍必須**另設具有 Contents write 權限的 Token，以及核對 runtime-data 分支保護／Firestore 設定。勿把唯讀恢復標示成「保存成功」。
- 執行 `python tools/h1538_verify_deploy.py --root .`，再用本輪 K線/大盤/官方資料做真實新一輪測試。

## 驗收證據及限制

- `tests/test_h1538_source_authority.py` 涵蓋主分支7月＋runtime-data11月、遠端離線隔離、來源時間不能冒充業務日、未來資料拒絕、較新資料不回退、無Token公開唯讀、安全白名單、UI業務日期與H40備援。
- 自動測試環境不能直接連公開GitHub Raw，故**外部下載路徑未在本機以真網路直接連線驗證**；已透過使用者連接的 GitHub 帳號確認公開 repo 與上述官方/大盤檔案確實存在且業務日有效。正式 Streamlit Cloud 網路讀回仍待上線確認。
- 已核對 `main` 與 `runtime-data` 的 `godpick_user_settings.json`：**兩者 `weight_settings_updated_at` 均為 2026-07-11 15:21:43、權重序號80**。因此畫面中「14權重來源時間＝7/11」本身不等於錯誤回退；`runtime-data` 其他設定最新修改時間為 2026-11-02，但權重上次校正確實仍是7月。需要新權重時須透過14頁重新校正、永久保存及回讀確認，不可把它假改成11/11。
- 本版不代替 `GITHUB_TOKEN` 修復使用者設定永久寫入，也不宣稱所有官方因子欄位每日完整、H126正式已取得買進授權。這些必須依實際遠端權限及官方內容確認。

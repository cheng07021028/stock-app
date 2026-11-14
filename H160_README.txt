StockApp V191-H160 | 主管Excel H154同名次排序修復 + H111永久設定權威診斷
================================================================
基準：GitHub cheng07021028/stock-app main
基準提交：c7e009112d54c819450b9db8a7afdc72d3179a43
準備：覆蓋前請確認 main 未有其他人修改同名檔案；不要用本ZIP取代整個 repo。

必要修改（保留原有程式其他功能）：
1) godpick_h1537_manager_visible.py
   - 修復00主管表在「競賽名次並列」時按H153原研究分再排序，導致
     00 / 01 / 03 表的H153.9調整後順序不同而拋出H154 ValueError。
   - 按研究排名穩定排序，同名次保留原本已計算好的H153.9權威順序。
2) godpick_h154_export_parity.py
   - 原有嚴格一致性檢查仍保留，並報告首次差異位置、股票及行數。
   - 保留H126正式0檔禁止買進的嚴格限制。
3) godpick_h111_scan_settings_authority.py
   - H111設定僅在GITHUB_RUNTIME_DATA_BRANCH（預設runtime-data）存取，
     不再因全域GITHUB_REPO_BRANCH=main誤往主程式分支寫入。
   - 公開儲存庫可匿名讀取並驗證遠端既有設定；無Token仍絕不准寫入。
   - 錯誤辨識401/403/404/422，指出Contents權限與分支設定。
4) tests/test_h160_rank_and_settings.py
   - 排名平手、回存Excel、真差異阻擋、H126權限、68產業讀取、
     無Token禁止保存、403、GitHub回讀確認、409衝突等9項離線測試。

實測：H160 9項 + H1537/H154/H125既有16項，合計25項PASS。
本機測試非Streamlit Cloud的實際遠端寫入驗收。

【部署前請注意】
- 同路徑覆蓋三個 .py 檔案，不要刪除其他檔案、推薦紀錄、runtime-data、Secrets。
- Streamlit Cloud -> App Settings -> Secrets 必須有 GITHUB_TOKEN。
- GitHub fine-grained token 選 repository stock-app，Repository permissions:
  Contents = Read and write（H111設定寫入）；Actions = Read and write（Page17派送）。
  GITHUB_RUNTIME_DATA_BRANCH = "runtime-data"
  GITHUB_CODE_BRANCH = "main"
- GitHub runtime-data 的 data/godpick_scan_settings_authority_h111.json 已有68項產業
  （2026-11-12的遠端快照）；不要因本機提示未驗證而覆蓋或清空既有設定。
- Token不可寫進程式碼或公開聊天，若Token過期則在Secrets更新。

【上線後兩項驗收】
A. 第7頁按「套用設定」後，展開「推薦設定保存明細」：需回報
   GitHub 已同步寫入並回讀驗證；Reboot 後再次載入相同68個產業。
B. 第7頁按「建立主管研究 Excel」：核對00/01/03同輪具名排行，
   並保留00_H126正式決策禁止買進門檻；若真的有異常，H154會報第一個差異。

未修改：完整選股策略/H109/H126交易權限/第17頁14項排程/歷史推薦。

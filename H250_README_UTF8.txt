V191-H250 | 六階段整合與唯讀正式運行驗收

重要：本 ZIP 是本機差異產生與驗收包，不是可以直接上傳 GitHub 的正式程式。
11/27 查核顯示：main 已出現前置 helper / apply 工具，但實際中央排程、Page17、Worker、workflow 等仍為原始 Git blob。runtime-data 尚無 data/h249/ 檔案。19:30:08 最新 wake 是 0 項；最近人工完整批次 14 項、10 成功、4 警示。零項 wake 不代表業務成功。

1. 從 GitHub stock-app main 取得最新原始碼 ZIP（或 Git clone），解壓至獨立目錄，保留 .github 隱藏目錄與所有中文檔名。先備份目前專案；不要把 runtime-data 當作程式原始碼。
2. 將本修正包解壓至另一個目錄，不要直接上傳本包或 stages 資料夾到 GitHub。需 Python 3.11+ 及 PyYAML；若缺 PyYAML，執行 python -m pip install PyYAML。
3. Windows 執行 RUN_H250.bat，輸入原始專案資料夾。亦可執行 python BUILD_H250.py "D:\stock-app-main"。
4. 工具用 21 個既有基準 Git blob 與 78 個逐階段定位點，依序建立 H244~H249 最終結果。僅接受完全相同的原始版或可以逐一逆轉並重算原始 SHA 的已知階段結果。未知修改、缺檔、重複定位點及不相容版本一律 BLOCKED。產生工具不寫入原專案、不讀 Secrets、不接觸 runtime-data、不推送 GitHub。若已存在同名輸出，先另外保存舊包。
5. [PASS] 後得到 StockApp_V191_H250_Overlay.zip。只把這個 Overlay 解壓到原專案，確認 .github/workflows 內文件一併覆蓋，再提交 GitHub main。不要上傳 BUILD_H250.py 或 stages 或把原包當正式覆蓋包。保留其他所有功能和原本資料檔；不要以舊版整包覆蓋新版。若 H250 之後有其他修改，應使用新版本基準另行審查，不使用此包強制回退。
6. 安裝前後可檢查 h250_release_manifest.json：記錄實際最終程式的 SHA-256/Git blob，供唯讀驗收。它是產生器製作的完整性清單，不是獨立簽章。提交後 reboot Streamlit，Page17 應看到 H248/H249 區塊。
7. 在 GitHub Actions 手動選擇 H250 installed release and unattended read-only acceptance -> Run workflow，main。新 workflow 僅手動觸發，contents/actions read 權限，不寫入 runtime-data、不派工、不新增排程、不更改設定。執行指定真實專案測試、全部 Git 追蹤 Python 語法、版本清單、獨立原始證據驗證；報告在該次 Actions Artifacts H250-acceptance-<run>-<attempt>。測試在子程序停用 GITHUB_ACTIONS，避免模擬呼叫真正 GitHub 寫入。
8. 正式驗收需使用現有 Page17，選擇一次明確的 14 項完整人工批次（可能消耗行情與 GitHub API 資源，避免反覆按），保留 Run ID 和回執。再等實際例行排程自然完成至少兩個已結束平日；H250 本身不會執行/等待/代派工作。再次手動執行 H250，確認結果。若不希望強制跑完整 14 項，人工完整批次驗收仍為 PENDING。
9. H250 採保守標準：14 項均啟用，兩個已結束平日，每日所有 14 項有匹配正式 slot、H249 RETURNED/SUCCESS、remote history、Actions schedule 終態成功。最新 14 項工作須 SUCCESS。過往批次不以最新綠燈替代。近 7 日派工不可有不確定的未回報、待查或失敗回執；超出近 7 日檢視窗口不構成歷史永久驗收。這是 H250 本次驗收標準，並不要求更改使用者既有排程設定。Actions 掃描最多 300 筆及最多 60 個缺失的精確 Run/Attempt；證據不足就保留 PENDING，非認定業務失敗。
10. H127 原始分片/H246 同輪完整快照、錨點、清單及歷史紀錄由現有驗證器重驗；H237 full read-only 使用 H225/H226/H229 原始研究門檻。H221/H222 收集器健康僅為宣稱，與原始重驗分開。H138 封存依原工作成功證據，不是 H134 全部歷史封存獨立重驗。H250 不新增交易授權、不放寬 95% 規則、不推測57筆歷史日期。
11. 效能：報告輸出歷史各工作有效耗時樣本數、中位數及最大值、驗收工具耗時；沒有正式部署前後 UI 或 API 實測，不宣稱加速。驗收 workflow 35 分鐘硬上限；資料大、API 限額或原驗證器缺件均可能使驗收待完成。不可為求綠燈修改風險門檻。H250 沒有將所有中斷改為自動重播。
12. 報告狀態 ALL_GATES_PASS 才代表本工具設定的驗收門檻通過；ACCEPTANCE_PENDING 或 exit 2 代表仍有未核實項目，請下載 JSON 查看 gate。不是未來績效、完全無人值守或任意斷點 exactly-once 保證。人工或排程中途開始且副作用不明的批次仍由 H249 安全隔離。

測試：原六版離線回歸 185 pass；H250 19 pass；合計 204 pass；完整專案 H248 6 skip，未以 skip 算通過。實際 Worker 基準 SHA 2754449c19f59e90aadb6d3769bcee64e6e6cb0b 可逆轉並重新建立 H248/H249、編譯通過；H244 helper 原始與轉換 SHA 可驗證。完整主專案無法在本執行環境下載，完整 Overlay 尚未產出、未上傳、未進行 live Actions 或真實資料寫入。請以使用者本機產生器及部署後 H250 驗收結果為準。

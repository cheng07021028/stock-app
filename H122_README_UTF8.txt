AI 股神 V191-H122.1｜主動異常通報 × 官方來源安全補跑 × 管理員健康戰情
建立日期：2026-10-14｜相容基準 H121；保留 H113～H120 決策／證據政策

核心問題與修正
1. H121 有夜間健康收據，但缺少管理員一眼可讀的跨來源戰情頁、主動通知與有限補跑。
2. H119 收集工具在部分市場失敗時也可能回傳 ExitCode 0；H122 同時驗證結構化狀態、兩來源結果以及退出碼，不能誤判完整成功。
3. H117 官方行情只在三市場完整一致時封存；H122 的安全重試只處理明確的網路超時、429 與 5xx，最多兩次。HTTP 403、格式更動、交易日不一致、HASH 異常、封存衝突均禁止自動重試。嚴禁藉重試重算買進、改權重、跳過 H120 人工對帳。
4. 失敗當晚僅可提交安全的健康狀態 JSON/Markdown，不能將該次部分行情、績效報告當作完整官方證據推入遠端。
5. 新增只讀 Pages 98 管理員健康頁，強制 require_login + current_role=='admin'，未授權拒絕顯示；不覆蓋 Page7/8。
6. 可選 GitHub Issues 通知，以固定事件標題去重；預設關閉，避免公開 Repo 的誤報。即使啟用，也僅包含狀態代碼，不含推薦股票、日誌或 Secrets。H120 未核對時維持待驗證，不能自動核准策略。

正式修改項目
NEW godpick_h122_incident_center.py          健康分級、補跑守門、變更去重、唯讀戰情
NEW tools/h122_incident_cli.py                dashboard / collect-safe market_eod|corporate_raw
NEW tools/h122_notify_issue.py               可選 GitHub 事件單通知，需 GH CLI + GH_TOKEN
NEW pages/98_股神營運健康戰情.py              僅管理員可見的只讀健康頁面
UPDATED .github/workflows/h117_official_ohlcv_feedback.yml
NEW tests/h122_incident_regression.py

安裝（必須先備份目前完整程式）
- 先確認最新版 stock-app 已依次安裝 H115～H121；這是增量補丁，不是可單獨啟動的完整 App。
- 解壓 ZIP 至 stock-app 根目錄，保留既有檔案。特別注意：現有自訂 Actions workflow 需先備份／檢查差異後再覆蓋。
- 本地測試：py -3 tests\h122_incident_regression.py
- 本地快照：py -3 tools\h121_operations_cli.py inspect
- 本地戰情：py -3 tools\h122_incident_cli.py dashboard
- 本地 HTML：reports\h122\H122_dashboard.html（可用瀏覽器閱讀；無資料時顯示待驗證）
- 將本版文件提交目前部署分支。確定 GitHub Actions 已開啟並有 contents:write。
- 在 Actions 手動執行 H122 EOD Incident Response and Verified Provenance；先確認日志和 artifact，並檢查遠端 Commit。
- 如要主動事件單：Repository Settings > Secrets and variables > Actions > Variables 新增 H122_NOTIFY_GITHUB_ISSUE=1；Actions 必須有 issues:write，且儲存庫允許 Issues。未啟用時依 GitHub Actions 的執行通知處理。
- 若 Actions 安全補跑後仍缺任一來源、H116 快照、H120 證據，必須顯示 BLOCKED / PENDING，而非報成功。

驗收範圍與限制
- 提供離線測試和累積版本編譯；沒有在你的正式 GitHub 雲端連線執行，所以 GitHub Issues 通知是否實際送達、TPEx API、夜間排程與跨 Reboot 保存仍需手動驗證。
- 無法由本地 H121 收據證明 GitHub 遠端分支已保存當次結果。Actions verify-remote 會在 git push 後獨立查證，管理員頁仍保守顯示 REMOTE_UNVERIFIED。
- GitHub Issue 可能被公開檢視，請勿在通知內寫憑證、秘密、個人資料或完整交易明細。
- 假日／休市日期不會由週末推測，仍以官方來源交易日為權威。
- H113 黑馬准入、H114 四策略、H111 設定永久保存、H116/H117 行情閉環、H118～H120 證據門檻和 Formal 保護均不修改。

正式驗收建議
1. 手動測試正常資料與兩次無資料；應產生 H122 Dashboard。
2. 模擬單一市場 HTTP 503：安全重試最多2次；HTTP403：不重試。
3. 模擬一市場缺漏：健康阻擋且不得提交部分數據；H120 認證仍為待核。
4. 重啟雲端 App 驗證管理員頁，並從 Github Actions 確認遠端 Commit；不可以只有本地檔案作為成功判斷。

回歸結果（此版封裝前）
H122 新增14組離線測試 PASS；H121 7組；H115~H120既有主測試PASS；H112~H114主流程 smoke PASS；
累積還原專案Python編譯PASS。未執行正式 GitHub Actions、即時官方市場API或通知送達實測。
補跑次數：H122外層最多兩輪；H117來源抓取程式本身另有單來源內部重試，非總HTTP次數只有兩次。

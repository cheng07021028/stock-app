V191-H246 | 正式推薦與遠端封存一致性修復

【正式查核依據】
2026-11-27 中央 Worker run 45974835118：14 項執行，10 SUCCESS、4 WARNING。
07 掃描 2367 / 候選 2267 / 顯示 3 / 08 新增 0；原始訊息明確指出本輪沒有可寫入08的正式/A-/R1/H79研究推薦紀錄。零新增不可冒充故障或強制新增。
H138 status MARKET_DAY_UNVERIFIED / eod_status EOD_NOT_VERIFIED。
遠端 runtime-data 07 anchor：2026-11-27 17:56:53，recommendation_count=0，錨點摘要已更新；10 清單為 []。完整最新快照 sync-state 卻仍是 11/26 22:49:32，未取得完整 payload 逐筆查核。H117 盤後採集最低時間18:15，排程18:45及20:45（台灣時間）。

【安裝】
1. 先完成 H245 Overlay，保留 H244。此次查核時 GitHub main 尚未見 H245 原始碼，因此 H246 產生器要求 H245 已套用之專案，不能直接跳過。
2. 解壓本包，執行 RUN_H246.bat 或 python apply_h246.py "實際H245 stock-app資料夾"。
3. 確認 [PASS] 且新產生 StockApp_V191_H246_Overlay.zip。若 [BLOCKED]，停止，不可使用先前版本舊包。
4. 備份程式，將新 Overlay 覆蓋原始碼；python -m pytest -q test_h246.py（需 pytest；安裝器專屬測試在Overlay會略過）。確認無誤再提交 main 並 Reboot App。切勿將 runtime-data、secrets、歷史紀錄一併提交。
5. 在 H117 已完成當日三市場官方封存後，從 Page17 強制執行或正常排程，查 07 的 h246_consistency / h138_auto_seal 及遠端分支。不可只看 GitHub Actions 綠燈。

【程式設計】
- H246 從固定 runtime-data Git commit 同時讀取 07 完整快照、07 錨點、10 清單和各自摘要，並核對 H241 現行權威08的實際筆數及內容hash。非空清單要求唯一 ID/日期/代號/市場一致且存在現行08；空清單只允許當輪錨點與完整快照均確認為0。
- 完整快照落後時，僅中央 GitHub worker、同輪本地完整payload與已核實錨點之 full_snapshot_payload_hash 完全相同、舊遠端內容及摘要有效、時間及revision嚴格較舊，才允許安全補交。以固定父提交建立只含完整快照及其sync-state兩檔的原子Git提交；正常fast-forward push，不force、不rebase、不自動覆蓋衝突，並重讀固定提交驗證。其他分支和08/10/錨點均不改寫。若資料不足或衝突，維持WARNING。
- H138 保留原H117、H97、H126、H134驗證。18:15前明示等待官方行情；18:15後若本機缺當日官方三來源，中央worker可唯讀fetch main最新commit，將官方檔讀至暫存目錄並使用原有H138驗證器，不覆蓋main或既有資料。官方來源仍缺時不封存、不偽造。主管封存結果與推薦一致性分開回報。
- 不新增/刪除08歷史，不復活已刪除ID，不生成買進授權、不降低H126或前置品質閘。不修改Page07大型原始頁面、推薦演算法、UI、排程時段、H244/H245既有功能。

【限制】
本次已遠端查核原始碼及摘要，原始碼4個定位點各唯一匹配。完整Page07及大型遠端完整快照受工具讀取限制，未實際下載完整專案到本機，也未部署正式分支或改寫正式資料。實際完整快照能否從當輪本機找回，須在中央worker驗證；H246不承諾歷史57筆補證、不自動復活10清單。GitHub分支保護或憑證若拒絕push，安全回報未確認。原有自動排程仍需在當日有效盤後證據取得後再執行；本包不新增背景補跑、不保證當日封存成功。後續H247再處理其他workflow失敗。

【預期狀態】
REMOTE_CONSISTENT=同輪07/08/10遠端一致性確認（不代表交易授權）。
EXACT_FULL_SNAPSHOT_UNAVAILABLE=找不到與錨點完全一致的完整payload，保留警示。
LIST_IDENTITY_CONFLICT/HISTORY_IDENTITY_CONFLICT=跨檔身分不一致，禁止補交。
REMOTE_CHECK_OR_REPAIR_UNCONFIRMED=讀取/權限/推送或其他異常，未確認。
WAIT_FOR_H117_OFFICIAL_EOD_WINDOW=尚未到盤後採集時段；MARKET_DAY_UNVERIFIED仍保持未封存。

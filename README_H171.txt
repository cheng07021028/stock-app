V191-H171｜官方財報 PIT × TTM EPS × 獲利突破 × T+N 績效研究
更新日期：2026-11-19
交付類型：增量研究模組（非主程式全量 ZIP，未修改目前線上系統）

【基準與界線】
已使用 GitHub cheng07021028/stock-app 的 main 分支核對 H169/H170 來源程式存在。
本 ZIP 只包含新增模組與離線測試，不覆蓋「pages/7_股神推薦.py」、
「godpick_h169_pe_watch.py」、H109/H126 決策、永久推薦紀錄或權限。
H171 模組複製後不會自行修改原有匯出流程。套用「安全接線工具 --apply」後，
H169 原有 Excel 附件的 Sheet11 才會自動增列 H171 研究段落；
T+N 尚需另以 CLI 提供真實推薦、收盤與交易日曆檔案才能有績效。
不能將本版標示為已正式雲端部署或完成真實選股驗收。

【新增內容】
1. godpick_h171_pit_validation.py
   - TWSE/TPEx 一般業公告財報累計數標準化。
   - 具時間證據的 4 個單季 EPS 相加：單季值由同期累計 EPS 相減，
     必須有完整同期／前期官方累計資料；缺任何季度即保留空值。
   - 五季「毛利率或營益率創近五季新高 × 單季獲利較去年同期成長」研究篩選。
     須備齊五季損益表及計算所需的期初累計數；不得用缺值當零。
   - T+1/T+5/T+10 依實際交易日曆與收盤價計算歷史已實現績效，
     不跳過停牌／缺價，不把未成熟的推薦當成失敗或成功。
   - 任何 H171 研究輸出皆不變更 H126 正式交易授權。
2. godpick_h171_workbook_audit.py
   - 在現有「11_交易員重點導航」最底部新增研究段落；
     不新增工作表、不修改現有研究分數與 H126。
3. tools/h171_financial_archive.py
   - 下載官方一般業累計財報，保存首次實際觀察時間；採原子寫入與重複去重。
   - 官方資料失敗、403、舊檔損毀時不覆蓋舊紀錄。
4. tools/h171_research_cli.py
   - 在原 Excel 副本上附加 H171 稽核與 T+N 績效，原檔不覆寫。
5. tools/h171_install_excel_hook.py
   - 只針對 GitHub 已核實 H169 檔案 SHA d80461ef... 接線；
     未提供 --apply 只檢查，來源不符拒絕改寫。
   - 成功後先備份 .h171.bak，H169 原函式新增 H171 研究附錄與狀態欄。
   - 不改原 H126/研究排序；新版第7頁原流程透過原 H169 匯出函式讀取。

【安裝】
在已使用 H170 之 stock-app 專案目錄下，依 ZIP 中相對路徑直接複製新增檔。
不需要刪除任何舊檔，也不需要變更 requirements.txt（既有環境已需 openpyxl、requests）。

【讓原第7頁的 Sheet11 自動加入 H171】
先檢查：
  python tools/h171_install_excel_hook.py
確認顯示 READY_APPLY_EXACT_H169 後，才執行：
  python tools/h171_install_excel_hook.py --apply
若版本不一致，它會拒絕修改；需使用更新的基準重新適配，禁止強制覆蓋。
成功後重啟 App，原第7頁若呼叫原 H169 append_pe_audit，
既有 Excel 的 Sheet11 最後會新增 H171 財報研究段落。
原有 Excel 工作表、H126、研究排序和紀錄不變。

上述為原始碼相容性接線；仍需要在實際部署環境執行整體匯出驗收。

【先收集官方累計財報】
  python tools/h171_financial_archive.py

會建立：data/h115/official_ytd_archive.json
注意：官方接口通常只提供當期最新資料，不會自動補齊過去五季。
只要是今日才第一次收集的過去季度資料，不能宣稱昨日就已知悉；
日期型研究在同一天收集的資料，研究日當天禁止回填。
一般業財報不代表金融保險業已完成覆蓋。

【產生有 H171 稽核的 Excel】
  python tools/h171_research_cli.py --workbook old.xlsx --financial data/h115/official_ytd_archive.json --research-day 2026-11-19 --output old_H171_Audit.xlsx

若需要 T+N 回測，可再提供以下四個成套參數：
  --signals signals.json --prices prices.json --sessions trading_sessions.json --evaluated-at 2026-11-19T17:30:00+08:00

signals.json 範例（實際推薦日期、代號、run_id 均須來自原始紀錄）：
  [{"code":"3227","signal_date":"2026-11-10","run_id":"REAL_RUN_ID"}]
prices.json 範例（每日價量快照應附正式可用時間，收盤價不含除權息調整）：
  [{"code":"3227","trade_date":"2026-11-10","close":100.0,"available_at":"2026-11-10T16:00:00+08:00"}]
trading_sessions.json 範例（交易所真實開市日曆，嚴格遞增）：
  ["2026-11-10","2026-11-11","2026-11-12"]

T+N 僅為「收盤對收盤未含股利／費用／稅負」的歷史已實現漲跌；
不是下一交易日方向預測，更不是回測保證報酬。

【部署與資料真實性】
- 本次無法取得完整 4~5 季正式 PIT 財報及原系統真實交易日曆。
  因此僅測試程式邏輯與合成資料，不宣稱已取得任何個股 H171 認證。
- Streamlit Cloud 本機 JSON 檔在重啟/重新部署時未必永久保留；
  正式使用必須再串接既有永久儲存服務或版本控管可靠的資料管線。
- 既有 H126 不會因 H171 證據成立而自動放行；獲利突破只在研究層有效。
- TTM EPS 以累計 EPS 差分重建，股本變動、EPS 重編仍需官方核對；
  不可把重建值冒稱交易所直接公佈的 TTM EPS。

【測試】
  python -m pytest -q tests/test_h171_pit_validation.py tests/test_h171_workbook_and_archive.py
目前 H171 自有 29 項離線測試通過，Python compileall 與 ZIP CRC 驗證完成。
未宣稱已執行主程式第7頁的完整雲端端對端測試。

【下一次應接續】
- 完成安全接線 --apply 後於正式雲端實跑第7頁匯出並與 H169/170/H145 一同做回歸驗證。
- 將 T+N 計算直接接到現有正式永久推薦紀錄（本版僅離線輸入成套資料）。
- 建立正式歷史季度 PIT 資料來源與可靠永久存儲，避免重啟失憶。
- 串接既有永久推薦紀錄及官方交易日曆，建立真實 T+1/T+5/T+10 績效閉環。
- 已驗證的基礎面與技術面證據、選股排名及 H126 交易授權仍須分層。

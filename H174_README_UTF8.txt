V191-H174 | H154 主管 Excel「研究排名」欄位錯誤修復（只包含必要修改檔）
==================================================================

【使用者看到的實際錯誤】
主管Excel建立失敗：ValueError: H154研究表缺少：研究排名

【根因（已在原始ZIP + H173疊加後重現）】
1. H153 當輪無可認證的 A/B 研究排名，01_具名強勢研究可能為空表。
2. 舊版 H84 Excel writer 會將空 DataFrame 降成只有「狀態」一欄的說明列。
3. H154 對帳驗證器仍強制讀「研究排名／股票代號／研究證據分」，造成下載前拋 ValueError。
4. 只有 W 級核查、沒有 A/B 名次的資料，亦可能缺少排名欄位。

【修改範圍：兩個 Production 檔案】
- godpick_h151_workbook_sections.py
  * 01 表沒有研究候選時仍保留「狀態」說明，並輸出空的研究排名／股票代號／分數欄位。
  * 真實研究筆數仍是 0，絕不自動產生候選股票或分數。
  * W 級核查資料缺名次欄時僅補空的 schema，不賦予 A/B 研究排名。
  * 其餘 11 張原有分類表欄位與內容不變。
- godpick_h154_export_parity.py
  * 精確驗證空研究池／W-only，允許無 A/B 股票時合法建立 Excel。
  * 同時維持嚴格限制：若有 A/B 股票但缺代號、研究排名或研究證據分，仍阻擋匯出。
  * 00/01/03 對帳、H126 禁止買進及 H145 資料完整性阻擋不改動。

【適用版本／覆蓋前比對】
- 以使用者上傳 stock-app-main(3).zip（H170 基準）加 H173 累積包實際檔案核對。
- 本修正包不包含舊版全量程式，不包含推薦歷史、密鑰、個人設定與資料庫。
- 如你使用的專案根目錄檔案已被其他版本修改，預檢會阻擋覆蓋，不能強行複製。

【安裝（Windows）】
0. 備份現有程式資料夾與永久推薦紀錄資料，先勿在正式服務上直接修改。
1. 解壓本 ZIP 至獨立資料夾。
2. 先將 tools/h174_preflight.py 放到目前 stock-app 專案的 tools 資料夾，
   其餘 Production 檔先不要覆蓋。
3. 在專案根目錄開啟 CMD / PowerShell：
       python tools/h174_preflight.py --root .
   應顯示 status=READY_TO_APPLY_H174；若為 BLOCKED_* 請停止，不可硬蓋。
4. 將本ZIP內下列兩個檔案複製至專案根目錄、覆蓋同名檔案：
       godpick_h151_workbook_sections.py
       godpick_h154_export_parity.py
   tests/ 測試檔可選擇複製至專案 tests 資料夾。
5. 在專案根目錄檢查：
       python tools/h174_preflight.py --root . --after
   應顯示 status=ALREADY_H174_INSTALLED。
6. 若已安裝 pytest：
       python -m pytest -q tests/test_h174_manager_excel_empty_rank.py tests/test_h174_preflight.py tests/test_h154_export_parity.py tests/test_h151_excel_split.py
7. 將相同的兩個 Production 修改檔部署到**正式執行的 GitHub main / Streamlit Cloud**，
   再 Reboot App 並以同一輪資料重新點「建立主管研究 Excel」。
   注意：本機覆蓋 ZIP 不會自動更新雲端 Streamlit App。

【驗收】
- A/B=0、W=0：正常產生 Excel；01 表顯示「本輪沒有…」＋空排名欄，不是0%勝率。
- A/B=0、W>0：可匯出核查股，沒有正式買進資格。
- A/B>0：00/01/03 資料與名次仍需嚴格對帳；缺證時拒絕輸出。
- 保留原 14 張 Excel 工作表（H157 後）、H126 禁止買進、不修改 H109 或研究評分。
- 本機已完成 124 項 H157～H174 相關回歸測試；非正式雲端部署成功證明。
- 2337 預計掃描、2262 有效K線、6 作戰候選不代表已經取得 A/B 排名或 H126 交易授權。


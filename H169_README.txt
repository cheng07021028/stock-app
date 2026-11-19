V191-H169｜選股真實證據鏈修正：H145筆數、H114/H1532技術、官方PE補證
======================================================================
正式基準：GitHub main 88ff5118c3bf3495075713a7aaa977566ee1dc85
實際檢查：StockPick_H1521_Potential_20261118_095725.xlsx
研究日：2026-11-18；Excel 14 張表、01具名研究37檔；H126可買進0檔。
核心缺口：H145預期最終2檔、實際唯一代號1檔；H115正式valuations.jsonl不存在；
          H114有效K線狀態與H1532 85根技術證據不能互相代替。

重要：H169是保守的分層修復，不能宣稱3個上游根因全部解除。
- H145保留BLOCKED_FINAL_RESULT_COUNT_GAP：加入可讀的前/後去重診斷，
  「掃描宣告2、最終唯一1」無法推定缺失代號，不會拿其他研究股補湊。
  真正要排除BLOCKED仍須檢查正式產生端的同輪原始最終候選。
- H114不再只因有成交量欄位就宣稱60根VALID_OHLCV；
  近60根需正數且可轉有限值。H1532仍單獨驗證85根完整量價。
  RSI/MACD/ADX與箱型仍不會在缺來源時假設已核。
- H169新增官方上市TWSE / 上櫃TPEx PE-only擷取與一次觀察時間，
  工作流在交易日22:05台灣時間擷取到data/h115/pe_only_latest.json，
  並將來源/日期/缺漏明確顯示於既有11_交易員重點導航底部；
  不新增/刪除Excel原工作表，不改原排名。若TPEx403/逾時，
  僅採TWSE已核樣本並標PE_ONLY_PARTIAL，絕不偽稱全市場完成。
  官方這個端點無獨立EPS值，絕不用股價÷PE代替公布的近四季EPS。
  H115 valuations.jsonl仍需真正的季度已公告EPS及來源時點，否則
  PE/EPS雙證與「低本益比×高EPS」不可升格為已認證。
- 不變動H109否決、H126買進授權、H141/H161永久刪除防護、
  runtime-data、Secrets、推薦紀錄、Page17中央排程。

正式程式修改：
  godpick_h114_parallel_alpha.py
  godpick_h145_core_bridge.py
  godpick_h1536_manager_clarity.py
  godpick_h157_trader_audit.py
  godpick_h169_pe_watch.py              [新增]
  tools/h169_refresh_pe_watch.py         [新增]
  .github/workflows/h169_official_pe_only.yml [新增]
附：tests/test_h169_evidence_contract.py、H169_README.txt。

部署步驟：
1. 解開H169 ZIP，以相對路徑覆蓋GitHub main。
2. 確認GitHub Actions工作流 H169 official PE-only source observation 可手動派送，
   第一次可以手動執行；工作流在main提交PE-only快照。原有14項中央排程不修改。
3. 重新執行第07頁完整選股、輸出新的Excel；舊的已下載工作簿不會改變。
4. 檢查11_交易員重點導航底部H169區，原有14張表不變，
   H145仍為BLOCKED時絕不可據此下單。
5. 若擷取未產生數據，檢查GitHub Actions日誌及來源URL，
   不要把HTTP200但欄位不符當成PE成功。
6. 更高優先的下階段是H115季度EPS真實來源及H145正式選股產生端對帳；
   此版並未偽造完整性或解除正式交易鎖。

所有模擬測試均不會向正式資料寫入或實際交易；
雲端來源、資料覆蓋、H145後續真正根因仍須部署後驗收。
官方資料端點：
 https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL
 https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis

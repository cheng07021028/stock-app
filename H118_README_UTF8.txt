V191-H118.1｜股神績效品質權威：官方行情 × 企業行動風險隔離 × 跨重啟治理
日期：2026-10-14

一、這次真正完成的工程
  A. H115 walk-forward evaluator：新增可選 H118 STRICT 品質政策，逐筆檢查成交股票所屬市場、官方基準交易日、企業行動事件、公司行動稽核覆蓋及隔夜超過20%的異常價差。無法通過者不產出已驗證報酬。
  B. H116 策略閉環：將 H118 品質政策傳遞至回測，待核對樣本不計入策略評估或升級建議。
  C. H117 行情匯出：增加 exchange 欄位，以正確區分 TWSE／TPEx，不可把缺市場欄位當作已驗證。
  D. H116 CLI：新增 --h118-strict、--h118-actions、--h118-coverage；來源資料 SHA 記入閉環來源 manifest。資料改變時會重算而非沿用上次快取。
  E. H117 夜間工作流程／H116 手動備援：使用 --h118-strict，缺企業行動核對就只記載待核對，不得產生正面績效宣告。H116 手動排程仍為備援，不會重複每日自動寫入。
  F. 新增只讀稽核工具 tools/h118_quality_cli.py 以確認官方行情日、核對覆蓋、已知企業行動數、狀態及資料證據雜湊。

二、沒有假裝完成的項目
  1. 不會從原始價格自動推導「應配現金股利」或「應配股票股利」。目前採有企業行動即隔離，待官方數額與股份比例等可驗證後，未來再研究還原報酬。
  2. 尚未完成「每日 TWSE／TPEx 全部官方除權除息名單」的自動蒐集與完整性稽核。因此預設無覆蓋日，STRICT 不會宣稱績效已完整驗證。
  3. 只有帳面資料 SHA 與官方 URL 不能證明資料真的出自官方；審核人仍須查證與封存官方原始內容，避免假來源或遺漏股票。
  4. 企業行動「預告」若後續更改，應重新查證實際生效日，不可直接將預告視為永久真實。
  5. 未含完整歷史股利及拆併修正，不能將原始股價報酬宣稱為含息總報酬。
  6. GitHub Action 需成功提交才能跨 Streamlit Cloud Reboot；沒有線上部屬實測不能宣稱已永久。

三、安裝（必要檔案只按相同路徑覆蓋；不含Page 7/8，不含使用者JSON，不含密鑰）
  先備份目前正式 stock-app (累積安裝 H115/H116/H117)，將ZIP原路徑覆蓋至專案根目錄。
  更新以下 production 檔案：
    godpick_h115_walkforward.py
    godpick_h116_daily_feedback.py
    godpick_h117_official_market_closure.py
    godpick_h118_performance_authority.py (新增)
    tools/h116_closed_loop_cli.py
    tools/h118_quality_cli.py (新增)
    .github/workflows/h117_official_ohlcv_feedback.yml
    .github/workflows/h116_nightly_strategy_review.yml
  需將變更提交至實際部署分支；不要讓 H116 和 H117 雙夜間排程競寫。

四、操作
  py -3 tools\h118_quality_cli.py
  py -3 tools\h117_official_eod_cli.py export
  py -3 tools\h116_closed_loop_cli.py review --prices data\h116\verified_ohlc.csv --benchmark data\h116\benchmark_ohlc.csv --out reports\h116 --h118-strict

五、企業行動證據格式
  可參考 templates/H118_official_company_actions_columns.csv (僅欄位模板，無市場資料)。
  放置路徑 data/h118/corporate_actions.csv
  欄位 code,ex_date,action_type,source_url,verified_at,verification_status
  type 允許 CASH_DIVIDEND/STOCK_DIVIDEND/SPLIT/REVERSE_SPLIT/RIGHTS_ISSUE/OTHER_PRICE_ADJUSTMENT。
  verification_status 必須 OFFICIAL_DOCUMENTED。

  官方稽核覆蓋：data/h118/daily_coverage.csv
  欄位 market,date,source_url,verified_at,status
  market 僅 TWSE/TPEX；status 必須 OFFICIAL_COMPLETE。
  每個交易日必須核對官方完整企業行動清單，而不只是看個別一家公司；未核對不得填 OFFICIAL_COMPLETE。
  不可用歷史空白範本冒充已經查過「沒有公司行動」。

六、狀態語意
  CORPORATE_ACTION_QUARANTINE：持有期間發生官方已核對的公司行動；先不算原始股價績效。
  UNEXPLAINED_PRICE_DISCONTINUITY：隔夜開盤／前收落差超過20%，停用自動績效，人工核對。
  CORPORATE_ACTION_COVERAGE_UNVERIFIED：官方完整除權息清單未被證明已核對。
  MARKET_PROVENANCE_UNKNOWN：無法區分上市／上櫃股票。
  UNVERIFIED_MARKET_SESSIONS：缺少官方完整交易日證據。
  CERTIFIED_UNADJUSTED_PRICE_WINDOW：官方事件核對文件顯示期間無企業行動；僅價格報酬，不是含息報酬。

七、測試
  py -3 tests\h118_performance_authority_regression.py
  py -3 tests\h115_pit_walkforward_regression.py
  py -3 tests\h116_closed_loop_regression.py
  py -3 tests\h117_official_close_regression.py
  將上傳的修正模組疊加在實際已部署版本前請先備份，並跑完整功能。

八、權威及風險
  本版無自動買賣、無股神策略權重自動調整、無改動 H111 設定、H113 資格或正式推薦/歷史JSON。
  H118.1 目標是阻擋不可信績效污染，而非創造獲利。可記錄原始未調整股價資料，但未完成官方核對時不能當作認證Alpha。

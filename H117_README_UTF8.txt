V191-H117.1 — TWSE／TPEx 官方盤後行情 × H116 真實績效閉環（2026-10-14）
====================================================================
定位：H115/H116 後續版本；本次新增採集工具，不放寬 H113 入榜資格，不改 H114/H115/H116 決策、不更動正式買進權限。

一、交付檔案
  godpick_h117_official_market_closure.py
  tools/h117_official_eod_cli.py
  .github/workflows/h117_official_ohlcv_feedback.yml
  .github/workflows/h116_nightly_strategy_review.yml （改為手動備援，避免與H117雙排程競寫）
  tests/h117_official_close_regression.py
  H117_README_UTF8.txt / PATCH_MANIFEST.txt / SHA256SUMS.txt

二、核心處理
  1) TWSE 上市：https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL
  2) TPEx 上櫃：https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes
  3) TWSE TAIEX 開高低收：https://openapi.twse.com.tw/v1/indicesReport/MI_5MINS_HIST
  4) 自官方 Date 欄辨識民國年；拒絕多個交易日、異常K線、不正常資料量、重複代號。
  5) 台灣時間 18:15 後才允許本日 EOD；所有來源必須為同一日且等於當天交易日。
  6) 休市日三來源都停在過去日期時 SKIP；只有部分來源更新視為 FAILED/RETRY。
  7) 使用官方原始值產生不可覆寫SHA-256封存檔 data/h117/daily/{TWSE,TPEX,TAIEX}/YYYY-MM-DD.json.gz。
  8) 重跑同一日只確認內容相同；如官方更正版不同，回報 IMMUTABLE_OFFICIAL_CONFLICT，需人工稽核，不自動覆寫。
  9) 依H116不可變推薦快照只擷取必要個股，匯出 data/h116/verified_ohlc.csv 和 benchmark_ohlc.csv。
 10) H116 計算下一交易日開盤至T+1/T+3/T+5收盤的紙上績效；基準為TAIEX價格指數（非含息報酬指數）。
 11) 無完整官方資料、無當時不可變快照、證據時間未驗證或T+n未成熟 => 不得計入已驗證績效。
 12) 工作流程使用 GitHub Actions 內建 GITHUB_TOKEN (contents:write)，只在檔案真的改變時提交。

三、安裝（不要把舊 ZIP 展開去覆蓋現有第 7/8 頁）
  1. 先完整備份你目前正式 stock-app，確認 H115/H116 已安裝；將本 ZIP 按原路徑覆蓋到專案根目錄。
  2. 第一次部署請在 GitHub 主分支上提交上述程式/測試/.github/workflows/*。
  3. GitHub Settings > Actions > General > Workflow permissions 設為 Read and write permissions；避免另有排程寫同一份報告。
  4. 第7頁還需已按H116指示啟用真正的永久快照，且遠端每筆掃描必須有可驗證 latest_evidence_at，否則該筆仍只會記錄 UNVERIFIED_ASOF_SNAPSHOT。
  5. 工作流程H117：台灣時間每週一到五18:45、20:45兩次嘗試；不是即時行情更新。
  6. GitHub Actions workflow_dispatch 可手動重試當日資料；需要官方網站端點可連線。若TPEx雲端回403，不會強制改採第三方數字。
  7. H116原工作流程會轉為手動備援，由H117統一完成每日抓行情和策略評估。

四、本機命令（專案根目錄）
  py -3 tools\h117_official_eod_cli.py inspect
  py -3 tools\h117_official_eod_cli.py collect
  py -3 tools\h117_official_eod_cli.py export
  py -3 tools\h116_closed_loop_cli.py review --prices data\h116\verified_ohlc.csv --benchmark data\h116\benchmark_ohlc.csv
  py -3 tests\h117_official_close_regression.py

五、未完成/需注意
  - 測試環境無法直接發送HTTP至TWSE/TPEx；實際可達性、端點更新時刻與Cloud權限需上線核實。
  - 公開OpenAPI主要提供最新交易日，不能一晚倒填過往60-120個交易日，歷史從排程正常上線後逐日積累。
  - 未調整除權息、股票拆併或下市回補；跨公司行動的報酬可能失真，需後續企業行動核對再做正式績效歸因。
  - TPEx官方網路可能在GitHub Actions的雲端IP回403；完整全市場閉環因此會等待，不可以宣稱通過。
  - TAIEX價格指數對上櫃股票是大盤參考，不等於上櫃指數；單純股價報酬與指數本身都不是可交易策略回測證明。
  - 僅紙上驗證；不可當作買進保證，不自動修改權重或停損參數。
  - GitHub repo永久封存需先成功提交；本機成功不等於已永久保存或Reboot通過。

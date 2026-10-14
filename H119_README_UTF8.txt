V191-H119.1 — 官方企業行動擷取 × 含息權益估值 × T+1/T+3/T+5 保守績效治理
Build date: 2026-10-14

一、正式修改（僅 4 個模組 + 2 個CLI + 1個排程 + 測試/範本）
* godpick_h115_walkforward.py：增加 opt-in H119 權益估值；保留 H118 STRICT 舊行為與相容性。
* godpick_h116_daily_feedback.py：保留價格報酬主績效；現金股利與未交付股票權益分別列研究報表；不觸發自動權重變更。
* godpick_h119_total_return.py：新增官方數值核對閘、現金股利應收權益、無償配股權益估值、風險隔離。
* godpick_h119_official_source.py：TWSE/TPEx 官方來源原始JSON不可覆寫快照及SHA-256；只擷取，絕不自動升級核對狀態。
* tools/h116_closed_loop_cli.py：新增 --h119-entitlement、--h119-adjustments；輸出H119研究表及權益來源hash。
* tools/h119_corporate_action_cli.py：collect / inspect；擷取失敗、空公告、未確認欄位會明確回報。
* .github/workflows/h117_official_ohlcv_feedback.yml：接上H119原始來源擷取、保守績效檢核與版本化提交；仍保留H117行情主流程。

二、直接覆蓋
先完整備份目前部署的 stock-app（必須已有 H115～H118），保留原有選股資料與JSON，再把此ZIP內的程式依原路徑覆蓋。
注意：此ZIP不包含 pages/7_股神推薦.py、pages/8_績效.py 或任何使用者設定/紀錄JSON。
提交修改至 GitHub 正式分支後，Streamlit Cloud 才會載入此修改。

三、操作指令
py -3 tools\h119_corporate_action_cli.py inspect
py -3 tools\h119_corporate_action_cli.py collect

真正計算H119研究估值時，必須備妥三份經查核CSV（僅示範路徑）：
A) data/h118/corporate_actions.csv：H118官方企業行動類型、公告來源、timestamp。
B) data/h118/daily_coverage.csv：H118同市場逐交易日官方事件完整核對，status=OFFICIAL_COMPLETE。
C) data/h119/reviewed/official_adjustments.csv：H119實際每股現金股利、無償配股率及官方來源核對。

請勿將隨附的模板誤認為已完成官方核對；無資料時保留空白／未驗證。除權息公告只寫「權值+息值」不可當成現金股利。

py -3 tools\h116_closed_loop_cli.py review --prices data\h116\verified_ohlc.csv --benchmark data\h116\benchmark_ohlc.csv --out reports\h116 --h118-strict --h119-entitlement

四、計算方式與績效身分
進場：實際下一交易日 OPEN。出場：第 T+n 個交易日 CLOSE。
現金股利：持有期間除息日登記為每股應收權益，不是已收到現金。當天才在除息後買進，不能領該筆股利。
股票股利：若無償配股率0.1代表每持有1股增加0.1股的理論權益；但新股可能於T+3或T+5後才交付，故只輸出估算權益價值，不能稱為當天可執行現金損益。
RIGHTS_ISSUE、SPLIT、REVERSE_SPLIT、OTHER_PRICE_ADJUSTMENT：隔離，不作假設式補數。
所有收益均先以研究口徑計算、含手續費假設；不含實際稅務個人條件、未到期配股流動性成本、股利給付實際日期差異。

狀態：
- OK：官方核對的無權息價格窗口。僅這類加入H116既有策略績效比較。
- OK_CASH_ENTITLEMENT_ESTIMATE：已核對除息但僅應收權益估值，單列研究表，不參與正式策略核准、不得對TAIEX價格指數宣稱可比較的alpha。
- STOCK_RIGHTS_DELIVERY_UNVERIFIED：未確認新股交付前不得視為可售出；另列權益估值，不納入策略績效。
- CORPORATE_COMPONENTS_UNVERIFIED：知道除息，卻欠實際金額證據。
- ACTION_EVIDENCE_MISMATCH：事件與數值來源不一致。
- CORPORATE_ACTION_COVERAGE_UNVERIFIED：官方逐交易日核對不足。
- UNSUPPORTED_CORPORATE_ACTION：現金增資／分割／減資等未支援。
- UNEXPLAINED_PRICE_DISCONTINUITY、MARKET_PROVENANCE_CONFLICT：價格或市場來源不符。

五、目前限制（不可誤稱正式完成）
- 官方原始擷取器採固定TWSE/TPEx端點。因執行容器無正式API連線，尚未驗證雲端HTTP成功、TPEx目前欄位與政策；資料不符時會fail-closed並提示待核對。
- 原始公告快照 != 可直接認證的股利數值。自動把 raw 升級為OFFICIAL_COMPLETE和OFFICIAL_NUMERIC_REVIEWED尚未實作，需逐日實際官方資料與人工/可信程序核對。
- 完整含息「已結算交易績效」、上市+上櫃所有企業行動歷史、完整60～120交易日績效，目前都不能宣稱完成。
- H119不會修改H113入榜條件、H114選股引擎、第7頁、第8頁、H111設定、正式買進權限、任何永久歷史紀錄。
- 本機來源擷取只算LOCAL，必須通過GitHub Actions成功提交或使用持久資料庫，才能跨容器Reboot保存。

六、驗證方法
在原始專案根目錄先執行：
py -3 -m unittest discover -s tests -p "h119*.py" -v
py -3 -m unittest discover -s tests -p "h118_performance_authority_regression.py" -v
py -3 tools\h119_corporate_action_cli.py inspect
若原專案頁面在不同版本，先備份且不得以此ZIP覆蓋頁面。

官方參考來源：
TWSE https://www.twse.com.tw/zh/announcement/ex-right/twt49u.html
TPEx https://www.tpex.org.tw/zh-tw/announce/market/ex/cal.html
TPEx 開放資料 https://data.gov.tw/dataset/11633

V191-H185｜十二研究員「自動網路資料」修正版（累積包含 H184）
=======================================================

修正需求：不再要求每天手動上傳 Excel／CSV。系統改為官方資料排程收集＋研究頁自動讀取，手動匯入僅作進階備援。

一、交付正式必要檔
  godpick_h184_research_lab.py                 (H184原有12角色研究運算核心；若未安裝則新增)
  config/h184_role_contracts.json             (H184原有角色契約；若未安裝則新增)
  pages/28_十二研究員決策研究中心.py          (H184 -> H185 自動取數介面)
  godpick_h185_auto_sources.py                (新增官方來源、自動讀取、歷史日K及安全快照)
  config/h185_auto_role_map.json              (新增12角色自動資料契約／缺口)
  tools/h185_auto_collect.py                  (新增排程收集器)
  .github/workflows/h185_autonomous_research.yml (新增 GitHub 自動排程)
其他檔僅為安裝、測試、說明，不修改原有推薦系統。

二、真正自動抓取的網路來源
A. TWSE 股票盤後成交資訊：
   https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL
B. TPEx 上櫃每日收盤行情：
   https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes
C. TWSE 上市公司重大訊息：
   https://openapi.twse.com.tw/v1/opendata/t187ap04_L
D. TPEx 上櫃公司重大訊息：
   https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O
E. 選擇單檔股票後，自動補抓官方月度歷史日K：
   上市：TWSE STOCK_DAY（最近最多10個月份、以欄名解析）
   上櫃：TPEx afterTrading/tradingStock（自動將成交仟股*1000轉成股）
F. 重用既有每日／週期排程、不複製來源權威：
   H115 官方月營收／單季三率；H117 不可變官方收盤檔；
   H169 交易所 PE 觀察；H170 累計 EPS YTD（絕不當作 TTM）；
   H150 的公開 RSS 標題快取（非官方重大訊息，只有標題／連結，未核原文）。

三、真正的無人值守更新
新 GitHub Actions workflow 名稱：
  H185 twelve analysts autonomous official research refresh
  (1) 第一次把 H185 正式檔提交到 main，push 條件會觸發初始來源收集。
  (2) 其後每週一～五 UTC 16:15（台灣次日 00:15）定時執行。
  (3) 即使沒有人登入 Streamlit，也會自動更新 data/h185/ 官方公開研究快照。
  (4) H185 只寫 data/h185，不寫 godpick_records.json、不寫正式買進授權。
  (5) 部分來源 403／逾時：保留上次合格快照、寫入 health.json 失敗資訊；不填假數據。
  (6) 同日價格修訂未確認：拒絕靜默覆蓋。公告新項目依 FIRST_OBSERVED 追加，舊公告不改首次觀測日。
  (7) GitHub Actions 必須允許工作流程寫入 contents；如權限不足，健康狀態無法持久提交，須由管理者開啟權限。

四、使用方式（不需要手動匯入）
  1. 開啟「28｜十二研究員決策研究中心｜自動網路取數」。
  2. 系統自動顯示官方行情、重大訊息、財報／PE／EPS來源及資料日期。
  3. 選擇股票代號（可留空檢視市場／事件類），切換12個角色。
  4. 除第10角色需要明確確認估值假設外，角色於來源或角色切換時自動分析。
  5. Excel/CSV 匯入仍在「進階備援」收合區，正常使用完全不必操作。
  6. 研究結果可下載，仍不寫入既有的推薦紀錄或解除 H126 禁買。

五、明確限制：不會假裝全部12位研究員都有完整官方證據
  - 01/02/04：官方日K可自動補抓，但若官方歷史缺漏、除權息未調整、樣本不足，績效/訊號不等於實盤權威。
  - 03/11：公司公告與公開RSS可自動收集，但並非所有股東會、法說日期都已取得；RSS只代表標題存在。
  - 07/09：既有H115財報快照為單季利潤率及月營收，尚不等於全部現金流/負債/股本；H169 PE 與 H170 EPS YTD 不能推算已核實 TTM EPS。
  - 05：個人真實持股與現金資料不是公共網路資料，須有券商／授權持股來源，否則只能待補證。
  - 06：實際成交日誌同上，不得把推薦紀錄當作已成交交易。
  - 08：自動產生待驗證的反方檢核假說，不會聲稱假說已證實。
  - 10：折現率、股份數、現金流、資本支出等未經證據支持時，不能自動填入假設並產生估值。
  - 12：資料快照與稽核契約無法代替逐篇原文查證，須分級顯示待核實。

六、一次性安裝（請先備份 GitHub／工作目錄）
解壓本 ZIP 到獨立目錄，在該目錄執行：
   python tools/h185_preflight.py --root "D:\stock-app"
若顯示 READY_APPLY_H185：
   python tools/h185_preflight.py --root "D:\stock-app" --apply
若 BLOCKED_UNKNOWN_VERSION：停止，切勿強制覆蓋目前較新版本。

更新 GitHub main：提交新增/修改的7個正式檔案；勿提交 data/h185 私有紀錄、secrets.toml 或舊推薦 JSON。
GitHub Actions 需啟用 Actions 及 Read and write permissions (contents: write)；
首次合併將依 push 自動啟動，之後定時。若是本機單獨安裝而未更新 GitHub，
Streamlit Cloud 不會自動取得程式或建立雲端排程。

檢查：GitHub Actions 該工作流程結果，再查看 main/data/h185/health.json，
四個TWSE/TPEx來源各自顯示 OK／PARTIAL／FAILED 與 asof_day，
第28頁對應自動來源狀態不應永遠顯示 NOT_COLLECTED。

七、驗收／測試
  python -m pytest tests/test_h184_research_roles.py tests/test_h185_auto_sources.py tests/test_h185_preflight.py -q
本機測試包含：官方交易日／成交量、TPEX成交仟股換算、公告未來日與觀測時點、
歷史日K資料缺漏、遺失來源不覆蓋、PIT、Excel/CSV安全與版本升級。
本次本機使用離線模擬來源及實際原始H115檔案；
並未在你的 GitHub Actions / Streamlit Cloud 上執行真實網路抓取驗收。

八、安全
本ZIP不含正式推薦紀錄JSON、不含 Firestore/GitHub Token、不含 secrets。
此 H185 是研究資料與介面增量，無需重寫第7、8頁，不變更原推薦紀錄刪除系統。

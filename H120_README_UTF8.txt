V191-H120.1｜官方企業行動原始資料對帳 × 績效證據防偽 × 60/120交易日策略比較
2026-10-14 | Based on existing H119.1; install H115～H119 first.

一、修改範圍（H119 相容；不覆蓋第7／8頁）
* NEW godpick_h120_evidence_reconciliation.py：對H119原始壓縮JSON+SHA-256、指定除權息日期、TWSE/TPEx股票代碼與類別、H118事件目錄、人工核對憑據一一比對。缺資料、空回應、來源不明、事件錯配或未簽署皆保留未驗證。
* NEW godpick_h120_strategy_validation.py：近60／120個交易日、T+1/T+3/T+5、H113 vs H114共同決策日等權配對比較；僅比較原價格報酬且data_status=OK、有同日期大盤的行情。排除應收現金股利、未交付股票股利等非可比樣本。
* NEW tools/h120_validate_cli.py：read-only audit / compare。compare需檢查H116原始來源manifest（H118 STRICT+H120嚴格證據）與績效CSV、benchmark檔案SHA-256；假或舊報表拒絕。
* UPDATED tools/h116_closed_loop_cli.py：新增 --h120-proof-enforced。只把通過H120對帳之H118覆蓋紀錄交給策略績效計算；否則價格/企業行動認證維持不足。加入H120報表、來源雜湊及CSV竄改/快取防護。舊指令不加旗標時維持原行為。
* UPDATED .github/workflows/h117_official_ohlcv_feedback.yml：H119 raw抓取照舊，H120稽核資訊輸出但不自動核准；H116正式夜間閉環開啟H120嚴格核對；選擇有足夠有效快照時寫入H120策略比較。沒有有效資料即維持0樣本，不猜測利潤。
* NEW tests/h120_* ：H120來源憑據、防假證據、策略樣本、CLI端到端測試。
* NEW templates/H120_day_proofs_COLUMNS_ONLY.csv：僅欄位名稱、沒有假官方核對資料，不能當完成簽署。

二、如何安裝
(1) 完整備份目前正式 stock-app（含已部署第7／8頁與data），確認已有H119必要模組。
(2) 將這個ZIP依原路徑解壓覆蓋到 stock-app 根目錄；沒有任何真實資料JSON/CSV/密鑰，也沒有pages/7或pages/8。
(3) 先執行：
    py -3 -m unittest discover -s tests -p "h120*.py" -v
    py -3 tools\h120_validate_cli.py audit
(4) 無證據時audit回傳碼2屬正常的FAIL CLOSED，不可手工改成PASS。
(5) 依當下完整最新專案，將必要程式提交至GitHub正式部署分支，檢查Actions執行紀錄、最終git commit與遠端資料持久化。

三、人工核對所需（不能用空白範本直接取代）
需準備：
  data/h118/corporate_actions.csv         H118獨立核對、事件型別及官方來源
  data/h118/daily_coverage.csv            H118每市場交易日OFFICIAL_COMPLETE，需獨立核查
  data/h119/raw/YYYY-MM-DD/*_<SHA>.json.gz H119原始官方資料、帶原始取得時間
  data/h120/reviewed/day_proofs.csv        H120新增對帳憑據，欄位見templates
  data/h119/reviewed/official_adjustments.csv H119股利實際每股數值（與H120獨立）

H120 day_proofs欄位：market, date, raw_sha256, reviewer, reviewed_at(ISO時區), source_scope=FULL_MARKET_EX_DATE,
 expected_stock_events(逐股股票代碼數), review_status=HUMAN_ATTESTED, source_url。
每筆raw SHA須指向實際H119原始封存；reviewer為記錄識別而非密碼學驗證的簽章。
必須核對來源當次查詢是否真的涵蓋該日期完整市場清單；無明確日期的空清單不准自動表示0事件。
TWSE的「權」不等於免費股票股利，可能包含其他權益/增資，需要H118事件類別和H119數值再獨立核對。
資料修改或公告修訂時，必須重新稽核。H120只認證「人工來源對帳過程與已有事件清單吻合」，不聲稱自動完成交易所所有企業行動認證。

四、操作指令
只檢查H119 raw+人工稽核：
    py -3 tools\h120_validate_cli.py audit
以H120嚴格證據產生H116績效（沒有足夠完整每日憑據會保持UNVERIFIED）：
    py -3 tools\h116_closed_loop_cli.py review --prices data\h116\verified_ohlc.csv --benchmark data\h116\benchmark_ohlc.csv --out reports\h116 --h118-strict --h119-entitlement --h120-proof-enforced
有真實H120相符的已完成明細，再進行60／120交易日探索性比較：
    py -3 tools\h120_validate_cli.py compare
輸出：reports/h116/H120_evidence_audit.json / H116_source_manifest.json、reports/h120/H120_strategy_comparison.json。
H120比較會因缺少來源憑據、未經嚴格核對、CSV雜湊不符而返回code 2，並非執行錯誤。

五、風險與未完成限制
* 尚未在正式雲端連線驗證 TWSE/TPEx API、真正每日無事件證明、人工核對流程執行情況。
* H119 raw current/next公告並不是一整天所有企業行動歷史的保證。H120對資料缺口採fail closed，真實完備樣本可能很少甚至為零。
* 60/120交易日比較是能計算的工具，不代表已有60/120交易日真實歷史樣本；不足時不建議調策略。
* H114新策略 vs H113等日績效差異非因果證明，受不同股票池及市場因素影響；重疊持有期和高頻重測也有偏差。
* H111永久設定、H113黑馬證據准入、H114四策略、正式股票買進權限、H116快照與H119股利金額均保持不被更動。
* 本地檔案非永久權威，GitHub Actions成功提交或專門持久化資料庫後才可聲稱跨Reboot保存。
* H120未驗證個人持股、交易成本實際成交及官方含息基準指數，未自動調權重、不提供交易指令。

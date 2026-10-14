StockApp V191-H123.1｜法人連買實證 × 黑馬品質治理 × T+1/T+3/T+5 策略證據分組
2026-10-14

[一、版本及權威]
本包接續 H122，並依賴 H105/H113/H114/H116/H117/H120/H121 的既有模組。
只新增三個 root Python 模組、兩個 CLI、H105/H116 定點修補工具、夜間官方法人蒐集工作流程及測試。
不含舊版 Page 7/8、不含使用者 JSON、不含 secrets；不得拿更早的 H105/Page 7/8 覆蓋目前正式程式。
不改 Formal 買進權限、不自動改策略權重。H113 成交量、TDCC、技術及 H109/H110 風控仍是獨立必備條件。

[二、H123 實際作用]
1. TWSE T86 取回當日逐股外資/投信/三法人淨買賣股數，保存完整官方回應、來源網址、第一次取得時間和 SHA-256。
2. 5 個有效交易日必須能在 H117 完整的 TWSE/TPEx/TAIEX 官方行情封存中證明；任何一天缺來源，H123 都不能認證。
3. 每股票精確計算外資及投信連買日數、3/5交易日淨買賣超；正數表示買超，單位為股。
   H117只是已封存交易日列表、並非完整官方休市日曆：任何缺少星期一至五的日期都先標待驗證，不自行推斷休市。
4. 回測必須比對第一次實際取得時間和研究當時資訊截止時間，絕不使用事後下載的舊報表冒充當時已知資料。
5. 偵測相同日不同官方版本即停用，自動掃描不可自行選一版。重抓內容相同則使用既有不可變封存。
6. H123 MODE=STRICT 時，任何缺少五交易日法人實證的個股，都不得再被 H113 既有摘要連買分數推進嚴格黑馬主榜。
7. H116 新推薦快照新增 H123 法人證據狀態、來源雜湊與缺失原因；既有不可變快照不改寫。
8. H123 品質績效報告只用真正的 H116 快照及 H120 嚴格來源清單，依策略、T+1/T+3/T+5 和法人證據分類研究；不保證超額報酬。

[三、目前明確未完成之處]
- TPEx 官方上櫃法人每日明細自動解析與驗證：目前 PENDING，不能以 TWSE 資料代替。
- TDCC 每週千張大戶比例自動抓取及雙週對帳：目前 PENDING，仍需 H113 已有之真實、日期合格來源。
- 60/120交易日實際 H113/H114 事前推薦及含權息績效：尚未具備足夠已認證樣本。
- 本次僅完成離線假資料及累積專案測試，尚未驗證正式 Cloud / GitHub Actions 連線。

[四、安全安裝]
1. 先備份目前正式專案（建議另建 git branch）；保留使用者 JSON 與 Secrets。
2. 將本 ZIP 依路徑解壓至目前最新 stock-app，覆蓋/新增同名正式模組，但不要刪除任何既有檔案。
3. 在專案根目錄執行：
   py -3 tools\apply_h123_evidence_hook.py --check-only
   需看到 H123_READY。若出現 PATCH_COMPATIBILITY_BLOCKED，請停止並比對你最新的 H105/H116 實檔，切勿強制覆蓋舊版。
4. 執行：
   py -3 tools\apply_h123_evidence_hook.py
   此工具會建立本機 .before_h123.bak 備份。請勿將備份/密鑰提交 GitHub。
5. 執行：
   py -3 -m unittest discover -s tests -p "h123*.py" -v
   py -3 tools\h123_official_flow_cli.py inspect
6. 將正式模組、定點修正後的 H105/H116、工具與 Workflow 提交 GitHub 主分支，交由 Streamlit Cloud 部署。

[五、逐日自動蒐集]
新增 .github/workflows/h123_official_institutional_daily.yml。工作日在台灣21:45觸發，必須先有 H117 三市場相同交易日的官方行情封存，再下載該交易日 TWSE T86。
只有通過原始欄位、交易日和雜湊驗證時才提交 data/h123/twse_t86/YYYY-MM-DD/<sha>.json；遠端HEAD核對延用H121。
若主分支受保護、Actions沒有 contents:write 或 TWSE限制雲端連線，應檢查 Actions 錯誤，絕不能宣稱 Reboot 永久保存。
手動補抓單日：
   py -3 tools\h123_official_flow_cli.py collect-twse --day 2026-10-14
本命令的 first_observed_at 是實際抓取時間，不會假造原始發布時間，因此不會重寫當晚之前的歷史回測。

[六、逐步啟用]
Streamlit Cloud Secrets 的 root-level 設定：
   GODPICK_H123_MODE = "MONITOR"     # 先觀察，保持 H113 現有推薦身份
累積至少五個正常官方交易日、H117三市場完整封存、確定沒有來源衝突且 H113/TDCC來源齊全後，才可改成：
   GODPICK_H123_MODE = "STRICT"      # 逐日官方法人證據不足即 veto 嚴格黑馬
或 "OFF" 完全不啟用。STRICT 下主榜可能顯示0支，這是資料不足的安全結果。

[七、研究與績效驗收]
   py -3 tools\h123_official_flow_cli.py audit --code 2330 --market 上市 --decision-at 2026-10-14T20:00:00+08:00
   py -3 tools\h123_quality_review_cli.py
第二個指令只有在 H116 快照、H118 嚴格品質、H120 企業行動人工核對與來源檔案雜湊均齊備時才會產生有意義的群組研究。
報告路徑：reports/h123/H123_quality_attribution.json；報告不授權買賣或自動校正。

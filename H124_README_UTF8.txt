StockApp V191-H124.1｜上櫃法人與TDCC大戶實證
2026-10-14

本包是H123之後的增量；不包含舊的pages/7_股神推薦.py、pages/8、使用者JSON或金鑰。

正式功能：
1) 上櫃 TPEx 個股逐日來源轉接器：只接受官方HTTP取回的JSON、列中官方交易日期、明確以股數計的外資／投信／法人欄；欄位不合即等待認證，不做猜測。
2) TDCC官方1-5週持股分散：依級距15（持股超過1,000,001股）與合計17計算百分比週變化。注意級距14也包含剛好1,000,000股者，無法拆分，因此不能宣稱精確涵蓋所有「剛好1000張」者。
3) 缺少週資料或資料日期／首次取用時間晚於推薦時間→PENDING，絕不回填歷史。
4) H123原上市法人、H113嚴格准入、H109風控保持。H124 STRICT只否決、不新增正式買進許可。
5) 證據壓縮不可變封存、原始內容雜湊及新舊版本衝突中止，GitHub遠端確認為永久保存的必要條件。

安裝：
(1) 先備份目前真正部署的專案；解壓本ZIP到stock-app根目錄。
(2) 執行 py -3 tools\apply_h124_fullmarket_hook.py --check-only
(3) 顯示 H124_READY 後，執行 py -3 tools\apply_h124_fullmarket_hook.py
(4) 執行 py -3 -m unittest discover -s tests -p 'h124*.py' -v
(5) 提交新模組、已定點修補H105/H116及工作流程；不要提交.bak、secrets或使用者資料。
(6) 執行 py -3 tools\h124_official_ownership_cli.py inspect
(7) 執行 py -3 tools\h124_official_ownership_cli.py collect-tdcc（需可連官方網站）
(8) GitHub Actions中啟用H124工作流程；如已確認櫃買官方openapi具明確日期與股數schema，設定變數 H124_TPEX_OFFICIAL_DAILY_URL；否則不啟用該步，保持上櫃PENDING。
(9) 起初用 GODPICK_H123_MODE=MONITOR 做影子稽核。資料齊全且確認真正LIVE後，再啟用STRICT。

界線與待辦：
* 上櫃官方歷史逐日API的確切JSON欄位尚需正式環境實測；未配置、無日期、API拒絕或返回不同schema，都不能宣稱上櫃連買驗證已完成。
* TDCC首次部署只有一個週次，至少兩個可證明在推薦前已觀察的相鄰週次，才可能 VERIFIED_RISING。TDCC持股級距變化不能證明同一「主力」或特定大戶連買。
* 本機證據非跨Reboot永久保存；需GitHub真正提交、遠端SHA對齊。
* 回測不能用現在抓到的舊資料冒充當時知道；正式交易績效改善尚未驗證。
* 因為H123已採全市場掃描前一次集中載入驗證，H124也是掃描前驗證，個股路徑不重複解壓大型原始檔。

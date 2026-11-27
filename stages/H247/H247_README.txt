V191-H247 | 全模組自動執行失敗根因與資料來源修復
基準：2026-11-27 GitHub cheng07021028/stock-app main 已讀取的正式檔案 SHA。
前置：H245、H246 的 Overlay 已確實覆蓋原始碼（不是只上傳安裝包）。
H244 的 H195/H220 依賴修正若已套用，允許完全相符的舊補丁版本；不相容的其他變更一律停止。

一、已確認根因與修改
1. H124/H221：加入官方實際英文欄位別名 SecuritiesCompanyCode、外資不含自營商 Difference、投信 Difference、TotalDifference。原中文欄名保留；嚴格單一欄位、日期、筆數、重複證券、來源、封存雜湊驗證不變。英文外資合計不得替代不含自營商欄位。GitHub 健康紀錄 894 列在舊版因代號欄位不識別被拒。現行 OpenAPI 只提供最新資料；本版不假造前四日，不將單日認證升格五日。H221 追加 22:15 台灣時間採集，保留原時段，並增加 H124 修改時觸發。
2. H195/H206/H219/H220/H224：共享 HTML 解析依賴明確列出 lxml、beautifulsoup4、html5lib，增加匯入健檢；原工作與報表、驗收 exit code 完整保留。H195/H220 實際 lxml 缺件；其餘為平行環境預防。
3. H206/H208：保留原工作與 95% 門檻；補列台灣 22:05 時段（不保證上游已完成），健康 JSON 完整輸出日誌，方便辨識行情雙日與 60 日覆蓋原因。
4. H222：歷史期間有界公平輪替；未嘗試先、最久未嘗試次之；保留原 12h cooldown、每市場/資料類型失敗上限、整體請求預算及最新季度嚴格交叉補證。PENDING 明列預算／失敗上限／冷卻與最後錯誤。季報缺乏可核實年份、YTD、單位仍拒絕；需以原有 H235 明確官方 ZIP / H236 保存還原路徑補證，不將 MOPS 請求參數當報表內容證據。不自動使用未驗證第三方、也不擅自保存大型 ZIP 或改雲端設定。
5. H219/H224/H225 保留品質 gate，不將描述性結果、待補證或 exit 2 改成通過。不修改 H126、07、08、10、Page17 UI、中央排程器及 H245/H246 核心。

二、安裝
解壓至獨立資料夾，勿在 GitHub 上傳整個安裝包。
先安裝前置版本、備份或 Git 建立復原點。
Windows 執行 RUN_H247.bat，輸入最新專案根目錄。
或命令：python apply_h247.py "D:\stock-app-main"
產生 Overlay 需要 PyYAML（本機若缺少：python -m pip install PyYAML）。
僅在顯示 [PASS] 且當次產生 StockApp_V191_H247_Overlay.zip 時使用；[BLOCKED] 時勿使用舊 Overlay。
覆蓋 Overlay 中同名檔案至專案根目錄，提交 GitHub main；不需要更換 Token。
正式專案本機回歸：python -m pytest -q tests/test_h247_source_contract.py tests/test_h221_evidence_closure.py tests/test_h124_schema_guard_fix.py
（需安裝正式相依及 pytest；離線測試不連網、不寫正式資料。）
再依 H221 → H206 → H195 → H220 → H219/H224 次序在 GitHub Actions 使用 Run workflow 驗收；各工作應等待前置結束及資料更新後才啟動下一個，避免手動同時執行。
H222 自動例行照舊執行；可用官方財報 ZIP 的原有 CLI 补證，但需先核實官方完整 URL、保存權限與容量。

三、驗收及限制
本包是精確版本鎖定差異產生器，不是直接覆蓋原始碼。容器無法連 GitHub，正式全文只可透過 connector 查閱，未能執行完整專案。產生器會對每檔 Git blob 精確驗證，對 Python AST、YAML 及 ZIP 再驗證。離線測試為合約/差異/合成資料測試；完整 parser integration tests 隨 Overlay 提供，須在實際專案驗收。
11/27 正式紀錄：H219 首批 623 訊號，622 原價有效、603 淨績效有效；仍有 TPEX 雙日覆蓋不足 95%。H225 60日門檻 TWSE 1023/1029、TPEX 674/839，H222 後續 40/64，皆不是本次修正後結果。H212 本輪 8 請求新增0，不保證本版提升行情覆蓋或補全歷史季報。正式 GitHub 及 Streamlit 未部署、未驗收。H248 接續選擇性補跑與永久缺口分類。

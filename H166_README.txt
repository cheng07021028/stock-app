V191-H166｜股神系統可信度治理：排程總開關／SuperAI同交易日／TPEx覆蓋／品質監控
====================================================
依據的正式程式：GitHub main（2026-11-18查核）：
  godpick_auto_update_tasks.py = 6ef4b899a80ef5fc8e31e2ab8d408da7f1b74302
  godpick_super_ai_market_context.py = 1bed0b1a827a7c15a0fcb69cbca297bf32f85419
  pages/17_系統健康檢查.py = b6207432d0ba1d30521ff8a722cddad278f28044
本版本只修改上述三個既有程式，新增一個純診斷模組；其餘原功能、H126及資料均不修改。

【GitHub runtime-data 真實發現】
  data/config/godpick_auto_scheduler_settings.json 在 2026-11-17 09:24:56 設定 enabled=false。
  2026-11-18 10:21:03 喚醒結果 SKIPPED_DISABLED，並非 Worker 不曾啟動。
  2026-11-17 22:03:58 SuperAI 有 TWSE 融資券 1068 筆，TPEx Read timeout；
  TAIFEX PCR 及外資臺指期淨未平倉均是 2026-11-16 資料（非 11/17 同日）。
  2026-11-17 股神推薦 BLOCKED，最近成功為 2026-10-19。

【正式檔】
1. godpick_h166_operational_readiness.py（新增）
   - 分開判斷上一完成交易日、總開關、五項必要資料、Page07最後成功、
     SuperAI 日期一致性、Github備份待確認；研究就緒不等於買進或績效證明。
   - 期交所約16:30後期望當日資料；融資券按原有21:30後期望當日資料，
     各自檢查來源日期；週末往前回推，國定假日須另核交易所日曆。
   - TPEx覆蓋缺失必須列警示；歷史資料無coverage時從市場別計算。
2. godpick_auto_update_tasks.py（修改）
   - 17｜SuperAI 更新由「資料存在」升級為來源／日期／市場別覆蓋的驗證。
   - 真正資料落後才 WARNING，明確顯示缺失，不把前一日PCR偽稱最新。
3. godpick_super_ai_market_context.py（修改）
   - TPEx網路 timeout 最多兩次，不因HTTP429／403無限重試。
   - 融資券新增 TWSE／TPEx 覆蓋數與來源狀態，保存原本欄位／中文鍵。
4. pages/17_系統健康檢查.py（修改）
   - 新增 H166 系統可靠度看板，顯示總開關、成功日期、來源缺漏及實際阻擋原因。
   - 只對管理員顯示「確認恢復原排程」控制；必須明確勾選；先保存、
     再比對 runtime-data 完整設定內容與 updated_at，真正確認後才顯示成功。
   - 未經同意絕不自動修改原本停用的 enabled=false。
   - 保留 H162 GitHub 授權診斷／H159 中央Worker／H156核心六項及全部功能。

【測試】
  98 項離線整合回歸通過，含 H155/H156/H158/H159 排程/期交所、
  H161 刪除保護、H163 配額與 H166 新增的來源時序／TPEx／停用治理。
  tests/test_h166_operational_readiness.py 新增；
  tests/test_h156_page17_parallel_refresh.py 合成資料樣本增加上櫃市場來源
  （舊測試用無市場別的1000~1149股票，與新來源門檻不一致）。
  編譯與ZIP條目校驗通過。

【部署】
  將 ZIP 中四個正式 .py 依同路徑覆蓋 GitHub main，測試檔非必需部署。
  不動 runtime-data、GITHUB_TOKEN、Firestore、H126、推薦歷史、設定權威。
  Streamlit Reboot → 登入管理員 → Page17 → H166 可靠度。
  若看到排程停用，請由管理員明確勾選恢復並檢查遠端已確認。
  若仍有 SuperAI WARNING，必須確認期交所日期及 TPEx連線，
  應按 Page17「核心6項」重算取得新的本輪證據；不能宣稱本輪已成功。

【未完成／不宣稱】
  本 ZIP 尚未部署正式雲端；未取得正式資料來源完整新鮮運算與樣本外績效，
  不宣稱推薦已恢復、交易獲利、隔日方向勝率、正式買進權限已解鎖。
  第二階段應獨立驗證 T+1/T+5/T+10 的扣成本超額報酬/回撤並整理交易員Excel；
  不可用本版就緒狀態替代實際交易績效。

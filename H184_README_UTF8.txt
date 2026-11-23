V191-H184｜十二研究員決策研究中心（正式增量程式）
===========================================

目的：把原12段投資研究提示詞轉成可執行的12角色分析、統一欄位契約、來源稽核、研究Excel與事件ICS。
本次為**加法升級**：只新增3個production檔，沒有修改 pages/7、pages/8、H109、H126、H183或正式推薦記錄。

新增的正式程式檔
  godpick_h184_research_lab.py
  config/h184_role_contracts.json
  pages/28_十二研究員決策研究中心.py

其他內容：tools/h184_preflight.py（安全安裝器）、tools/build_h184_contracts.py（提示詞契約重建）、tests/*（測試）、H184_README_UTF8.txt、H184_SHA256SUMS.txt。

安裝（已備份完整程式且確認根目錄後）：
  python tools/h184_preflight.py --root "D:\stock-app"
  若顯示 READY_APPLY_H184：
  python tools/h184_preflight.py --root "D:\stock-app" --apply
  若顯示 BLOCKED：不要強制覆蓋，先核對部署版本。
  將3個新增正式檔以相同路徑提交至GitHub main；Streamlit Cloud 重新載入，側欄出現「28｜十二研究員決策研究中心」。

操作：
1. 在28頁下載8工作表資料模板，填資料或匯入自有CSV/XLSX。
2. 每個上傳工作表指定對應資料集，預覽資料後選取角色 01～12。
3. 輸入必要股票代號、截止日期、交易成本、現金比例或 DCF 假設。
4. 執行角色研究，查看計算結果、缺口與來源狀態。
5. 下載獨立14工作表研究Excel、可追溯JSON或暫定事件ICS。

現有資料重用：
第07角色會唯讀讀取 data/h115/revenue.jsonl、quarterly_margins.jsonl（如果專案已有），逐筆檢查 FIRST_OBSERVED available_at <= 研究日。
第01／02／04角色請匯入具日期、OHLCV的真實價格資料。第03／11角色請匯入事件資料；本版不會代替人員爬取官方新聞。

能力與限制（不可誤認完成）：
- 01提出5個有公式依據的價型研究情境；缺基本面時要求補證。
- 02計算日均線、週均線、RSI、ATR、區間高低；無原始資料不產生信號。
- 03整理使用者提供的事件及可核網址；不會讀取網址內文、不判定消息真實。
- 04實作固定MA5/20收盤訊號、隔日開盤成交、成本假設的簡化離線回測；不是任意策略回測器，無實盤績效保證。
- 05分析持股配置、產業／幣別／區域集中度、跌20%靜態壓測及示範再平衡；不能聲稱真的減少虧損。
- 06從輸入交易中取最後20筆做損益與遵守規則標示、列出3項可核檢規則。
- 07使用本機已有之H115官方歷史快照或上傳財報；缺財報不得補值。
- 08反方論點是待驗證假設，沒有AI假裝讀到不存在的證據。
- 09使用同期間資料比較；TTM EPS若僅使用者自稱已核實仍須原文複查。
- 10只在使用者確認DCF假設後產出3種情境，並標示簡化公式與限制。
- 11輸出待查事件清單及 ICS（STATUS:TENTATIVE），無即時推播。
- 12只稽核欄位完整與相互矛盾，尚未讀來源原文全部歸待核實。

永久性聲明：
研究結果保存在目前瀏覽器Streamlit Session；Reboot會失去本工作階段結果。請先下載研究Excel／JSON封存。未建立GitHub/Firestore正式研究權威保存，且不會把研究資訊寫入既有永久推薦紀錄。
此限制是為了避免H183同步／刪除衝突，尚待獨立規劃版本化研究帳本；不能說已完成永久化。

測試：
  python -m pytest tests/test_h184_research_roles.py tests/test_h184_install_contract.py -q
需要：pandas、numpy、openpyxl、Streamlit；沿用原有 requirements，不新增第三方套件。

注意：若完整GitHub已是更新於H183的版本，H184仍為新增頁面但請先跑預檢。不允許放寬H126買進授權或默默覆蓋任何舊紀錄。

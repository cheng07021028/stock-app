V191-H126.1 — 單一推薦權威 × 官方籌碼實證 × 主管 Excel 決策層
2026-10-14

目的
- 解決「H102舊優先排序仍顯示Research高分，H106/H113實際卻為零」的主管誤讀問題。
- 新增 00_H126主管決策、00A_H126資料缺口，並把原 H102 重新命名為 09_H102舊排序_非買進。
- 主管Excel限定九張決策工作表；05驗證與風險證據縮成24個必要欄位，原始稽核資料仍留在系統中。
- H126 僅建立研究認證與缺失證據，不建立任何 Formal 買進權限。
- 研究池、正式推薦、使用者設定、推薦紀錄 JSON 完全不改寫。

嚴格 A 級認證（所有條件同時成立）
1 H113 = QUALIFIED，且沒有各池明確相反的硬門檻證據。
2 H123 = PROVEN_FLOW、原始來源 SHA-256 格式有效、官方逐日有效天數 = 5。
3 H124 = VERIFIED_RISING、來源 SHA-256 格式有效、TDCC第15級週增 >= +0.05pp。
4 H123研究實際時間不得晚於 H110研究資訊截止日；資料時點無法查明者不得認證。
5 H109 = RESEARCH_ELIGIBLE，沒有 H110 CONFLICT、H105已發動/追高。
6 來源欄位不得從不同來源列各自湊出一個假的「聯合認證」。

注意：H126 只檢查已接線的 H123/H124 認證結果與來源欄位；並不重新
下載、重算或自行認證官方金融市場原始資料。缺少現行資料一律 PENDING，
不能用 H102 舊分數、法人連買代理分或週資料冒充每日證據。

安裝（先備份當前真實部署來源）
  py -3 tools\apply_h126_authority_hook.py --check-only
  py -3 tools\apply_h126_authority_hook.py
  py -3 -m unittest discover -s tests -p "test_h126*.py" -v
完成後請將 godpick_h126_authority_report.py、
修改後 godpick_h105_future_blackhorse_engine.py、修改後 pages/7_股神推薦.py
及 tools/apply_h126_authority_hook.py、tests/test_h126_authority.py 提交 GitHub
真正的 Streamlit Cloud 部署分支，並在 App 重建後重新推薦、重新匯出 Excel。

如果檢查結果指出 H123/H124 還未接線，先依已有升級包執行：
  py -3 tools\apply_h123_evidence_hook.py --check-only
  py -3 tools\apply_h123_evidence_hook.py
  py -3 tools\apply_h124_fullmarket_hook.py --check-only
  py -3 tools\apply_h124_fullmarket_hook.py
再執行 H126。若真實第7頁仍為 #Uxxxx 亂碼檔名，先使用 H125
安全修補流程檢查中文檔名衝突，不得隨意覆蓋正常版第7頁。

版本兼容：以先前正式 H124/H125 的實際程式為準；安裝工具以錨點驗證
拒絕不相容版本。工具不含舊 Page7/8、不含用戶資料、不含帳密。

實測與限制
- 合成資料通過 A級、低證據待驗、FADE、衝突否決、時間污染、正式權限保留。
- H113/H114/H123/H124 既有回歸與 H125 第7頁定點接線驗證完成。
- 使用者 2026/10/14 匯出表仍未包含 H123/H124實證欄位：原報表不能據此
  宣稱有正式認證黑馬。部署後需查實際雲端資料來源，不能宣稱線上已完成。
- 目前沒有真實 T+1/3/5 樣本外報酬改善證據，絕不以高分當買進機率。

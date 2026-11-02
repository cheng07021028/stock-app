# V191-H151.2 — Excel 開啟時要求修復：凍結視窗 OOXML 修正

## 修正的確切原因

H151 多表匯出沿用 H84 writer 的 `freeze_panes = "D2"`，再對主管觀察榜指定 `D8`，及對十個分類工作表改成 `A2`。目前使用的 Excel writer 在反覆賦值時會向 `sheetView.selection` 累加舊選取窗格設定；其結果可由 Python 函式讀回，但 Microsoft Excel 可能警告「我們發現部分內容有問題，需要修復」。

H151.2 在 H85 主管 Excel 真正儲存前呼叫 `godpick_h1512_excel_integrity.normalize_frozen_sheet_views(wb)`，先移除多餘選取窗格，再依據各工作表最終凍結座標重建合法的選取窗格集合。H126 決策、選股結果、工作表順序、表格資料、標籤顏色、篩選範圍、Excel 原有檔名都保持不變。

- 00_主管重點觀察榜：`D8`；在 Excel XML 中應為 `topRight,bottomLeft,bottomRight` 三個選取窗格。
- 00_H126主管決策：保留現有 `D2`（若欄位不足三欄，則保留原 `A2`）。
- 其他 10 張分類表：`A2`；在 Excel XML 中應只有 `bottomLeft` 一個選取窗格。

## 套用方式

1. 先核對 GitHub `main` 與線上最新原始碼，避免覆蓋後續其他使用者的變更。
2. 將此累積 ZIP 中的檔案依原路徑覆蓋到 **專案根目錄**；包含 H147～H151.1 需要的正式模組，無須逐版重裝。
3. 執行 `python tools/h1512_verify_deploy.py --root .`；預期出現 `PASS`。
4. 推送、確認 Streamlit Cloud 重新部署後，重新按「建立主管研究 Excel」，取得新產生的 `StockPick_H151_SplitSheets_*.xlsx`。**之前下載的檔案不會被自動修復。**
5. 用 Windows Microsoft Excel 開啟新版檔案確認不再出現修復警告。如果仍發生，請提供「新匯出的實際 XLSX」及 Excel 修復紀錄 XML，才能定位其他未知的 Excel 相容性問題。

## 驗證

`PYTHONPATH=.:tests pytest -q tests/test_h1512_excel_view_integrity.py tests/test_h1512_deploy.py tests/test_h151_excel_split.py tests/test_h1511_deploy.py`

測試包含：先用真正的頁面寫表函式重現重複的 `<selection>`，再儲存為 XLSX、讀取 OOXML、逐張確認凍結窗格與選取窗格一一對應、檢查 12 張表順序與 H126 及 tabColor 保留。

**注意**：本機 OOXML 與 Python 讀回檢查並不等同於正式 Windows Excel 的驗收；仍須正式部署後實際開啟驗證。

## 安全與資料完整性

不修改 H109/H113/H123/H124/H126 交易風控、推薦資料與紀錄、Cloud Actions、永久設定、Secrets、runtime-data。ZIP 只包含正式程式、工作流程、部署檢查、測試與說明；檔名使用純 ASCII 英文與版本號，ZIP 中 `pages/7_股神推薦.py` 保持 UTF-8 中文路徑。

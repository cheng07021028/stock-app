# V191-H151.1 | Excel Priority Worksheet Tab Colors

本版在 H151 十二張分類工作表架構上，只新增**四張主管每日重點工作表的 Excel 頁籤顏色**，不變更任何推薦計算、股票池、H126 正式交易權威、歷史資料、凍結視窗、篩選或資料排序。

| 工作表 | 目的 | Excel 頁籤色碼 |
|---|---|---|
| `01_具名強勢研究` | 每日優先研究 | 綠色 `#16A34A` |
| `00_H126主管決策` | 正式交易權威 | 紫色 `#7C3AED` |
| `03_五檔研究計畫` | 研究交易計畫 | 藍色 `#2563EB` |
| `00_主管重點觀察榜` | 主管總覽 | 金橘色 `#F59E0B` |
| 其他 8 張工作表 | 明細稽核與延伸研究 | 灰色 `#94A3B8` |

> Excel 的工作表標籤色是視覺導覽，不代表任何股票已取得買進授權。工作表維持原順序。部分 Excel 版本會把頁籤顏色呈現為標籤底部色條，屬正常顯示。

## 部署

本檔 `StockApp_V191_H1511_Tabs.zip` 是以 H151 累積包為基礎的單次覆蓋包，不含 `runtime-data`、正式推薦紀錄、使用者設定、Secrets 或憑證。將 ZIP 內檔案按原有目錄放回 GitHub 專案，**先與目前 `main` 實際程式比較，禁止覆蓋較新的客製修改**。對於已部署 H151 而且沒有其他改動的情況，只需要覆蓋 `godpick_h151_workbook_sections.py` 即可啟用四個頁籤色，其餘檔案是累積依賴／測試／說明。

驗證：

```bash
python tools/h1511_verify_deploy.py --root .
python -m pytest -q tests/test_h151_excel_split.py tests/test_h1511_deploy.py
```

部署後重新產生 `StockPick_H151_SplitSheets_*.xlsx` 即可看見四個彩色工作表頁籤；**已下載的舊 Excel 不會自動變色**。

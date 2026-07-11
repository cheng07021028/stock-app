股神系統 V158｜UTF-8 中文檔名修正版

本修補包已修正上一包的頁面檔名問題：
- 錯誤顯示：pages/8_#U80a1#U795e#U63a8#U85a6#U7d00#U9304.py
- 正式檔名：pages/8_股神推薦紀錄.py

套用方式：
1. 將本 ZIP 解壓到 stock-app-main 專案根目錄。
2. Windows 可雙擊 apply_patch_windows.bat。
3. 或在專案根目錄執行：python apply_patch.py
4. 工具會將舊的 #U 編碼頁面移到 patch_backup_v158_utf8，避免重複頁面。
5. 工具會自動執行 Python 全專案語法檢查。

本包只包含本次修改檔案與套用工具，不包含完整專案。
所有文字檔使用 UTF-8 with BOM，降低 Windows 開啟亂碼風險。

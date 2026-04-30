# -*- coding: utf-8 -*-
"""
Streamlit 多頁側邊欄顯示編號修正工具
用途：把 pages/0_大盤走勢.py 這種會被 Streamlit 隱藏數字的檔名，
改成 pages/第00_大盤走勢.py，讓側邊欄永久顯示「第00 大盤走勢」。

使用方式：
1. 把本檔放在專案根目錄，也就是跟 streamlit_app.py 同一層。
2. 執行：python rename_pages_visible_number.py
3. Commit / Push 到 GitHub。
4. Streamlit Cloud 做 Clear cache → Reboot app。
"""
from pathlib import Path

PAGES_DIR = Path("pages")

RENAMES = {
    "0_大盤走勢.py": "第00_大盤走勢.py",
    "1_儀表板.py": "第01_儀表板.py",
    "2_行情查詢.py": "第02_行情查詢.py",
    "3_歷史K線分析.py": "第03_歷史K線分析.py",
    "4_自選股中心.py": "第04_自選股中心.py",
    "5_排行榜.py": "第05_排行榜.py",
    "6_多股比較.py": "第06_多股比較.py",
    "7_股神推薦.py": "第07_股神推薦.py",
    "8_股神推薦紀錄.py": "第08_股神推薦紀錄.py",
    "9_股票主檔更新.py": "第09_股票主檔更新.py",
    "10_推薦清單.py": "第10_推薦清單.py",
    "11_資料診斷.py": "第11_資料診斷.py",
    "12_股神管理中心.py": "第12_股神管理中心.py",
    "14_股神權重校正.py": "第14_股神權重校正.py",
}


def main() -> None:
    if not PAGES_DIR.exists():
        raise SystemExit("找不到 pages 資料夾。請把本檔放在專案根目錄後再執行。")

    changed = 0
    skipped = 0
    for old_name, new_name in RENAMES.items():
        old_path = PAGES_DIR / old_name
        new_path = PAGES_DIR / new_name

        if new_path.exists():
            print(f"略過：{new_name} 已存在")
            skipped += 1
            continue

        if old_path.exists():
            old_path.rename(new_path)
            print(f"完成：{old_name}  →  {new_name}")
            changed += 1
        else:
            print(f"略過：找不到 {old_name}")
            skipped += 1

    print("-" * 60)
    print(f"改名完成：{changed} 個，略過：{skipped} 個")
    print("下一步：Commit / Push 到 GitHub，Streamlit Cloud Clear cache → Reboot app")


if __name__ == "__main__":
    main()

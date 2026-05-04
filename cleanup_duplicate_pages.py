# -*- coding: utf-8 -*-
"""
清除 Streamlit pages 重複頁面檔案。
用途：使用「第07_股神推薦.py」這種顯示編號檔名後，必須刪除舊的「7_股神推薦.py」與 #U 編碼檔，避免 Streamlit 導航衝突或側邊欄重複。
執行位置：專案根目錄，也就是與 streamlit_app.py、pages/ 同層。
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PAGES = ROOT / "pages"

DELETE_NAMES = [
    # 第07 股神推薦
    "7_股神推薦.py",
    "07_股神推薦.py",
    "7_#U80a1#U795e#U63a8#U85a6.py",
    "07_#U80a1#U795e#U63a8#U85a6.py",
    # 第08 股神推薦紀錄
    "8_股神推薦紀錄.py",
    "08_股神推薦紀錄.py",
    "8_#U80a1#U795e#U63a8#U85a6#U7d00#U9304.py",
    "08_#U80a1#U795e#U63a8#U85a6#U7d00#U9304.py",
    # 第10 推薦清單
    "10_推薦清單.py",
    "10_#U63a8#U85a6#U6e05#U55ae.py",
    # 第11 資料診斷；同時移除先前誤加的 11_推薦清單
    "11_資料診斷.py",
    "11_推薦清單.py",
    "11_#U8cc7#U6599#U8a3a#U65b7.py",
    "11_#U63a8#U85a6#U6e05#U55ae.py",
    # 第12 股神管理中心
    "12_股神管理中心.py",
    "12_#U80a1#U795e#U7ba1#U7406#U4e2d#U5fc3.py",
]

KEEP_NAMES = [
    "第07_股神推薦.py",
    "第08_股神推薦紀錄.py",
    "第10_推薦清單.py",
    "第11_資料診斷.py",
    "第12_股神管理中心.py",
]


def main():
    if not PAGES.exists():
        print("找不到 pages 資料夾，請把本檔放在專案根目錄後再執行。")
        return

    print("=== Streamlit pages 重複檔清理工具 ===")
    print(f"專案路徑：{ROOT}")
    print(f"pages 路徑：{PAGES}")

    missing_keep = [name for name in KEEP_NAMES if not (PAGES / name).exists()]
    if missing_keep:
        print("\n警告：以下新版頁面檔還不存在，請先覆蓋 v33 整包後再清理：")
        for name in missing_keep:
            print(" -", name)
        print("\n仍會繼續清理舊檔，但請確認新版檔案已放入 pages。")

    deleted = []
    skipped = []
    for name in DELETE_NAMES:
        p = PAGES / name
        if p.exists():
            p.unlink()
            deleted.append(name)
        else:
            skipped.append(name)

    print("\n已刪除舊檔：")
    if deleted:
        for name in deleted:
            print(" -", name)
    else:
        print(" - 無")

    print("\n保留新版檔：")
    for name in KEEP_NAMES:
        print(" -", name, "✅" if (PAGES / name).exists() else "❌ 不存在")

    print("\n完成。請 commit / push 到 GitHub，然後 Streamlit Cloud Clear cache → Reboot app。")


if __name__ == "__main__":
    main()

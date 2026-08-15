# -*- coding: utf-8 -*-
"""V191-H30 integrate concise SuperAI guide as the first Page07 Excel sheet."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAGE7 = ROOT / "pages" / "7_股神推薦.py"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if new in text:
        return text
    if old not in text:
        raise RuntimeError(f"H30 anchor not found: {label}")
    return text.replace(old, new, 1)


def main() -> None:
    src = PAGE7.read_text(encoding="utf-8-sig")

    anchor = '''    sheets = [
        ("股神推薦總排名", master_rank_df, "目前沒有資料新鮮且達排名門檻的推薦/觀察候選。"),
'''
    replacement = '''    # V191-H30：完整作戰表資訊很多，第一張表只留下真正的交易決策核心。
    # 這張表只做「閱讀優先級」，不改 Formal/A- 分區、不放寬 Entry/Risk/RR。
    try:
        from godpick_super_ai_excel_guide import build_super_ai_excel_guide
        _guide_source = (
            candidate_diagnosis_export
            if isinstance(candidate_diagnosis_export, pd.DataFrame) and not candidate_diagnosis_export.empty
            else master_rank_df
        )
        super_ai_guide_export = build_super_ai_excel_guide(_guide_source, max_rows=20)
    except Exception as _h30_guide_exc:
        super_ai_guide_export = pd.DataFrame({
            "狀態": [f"超級AI精選攻略建立失敗：{type(_h30_guide_exc).__name__}: {_h30_guide_exc}"],
            "說明": ["不影響其餘正式作戰表匯出；請檢查 H30 guide builder。"],
        })

    sheets = [
        ("超級AI股神精選攻略", super_ai_guide_export, "本輪沒有可建立精選攻略的候選資料。"),
        ("股神推薦總排名", master_rank_df, "目前沒有資料新鮮且達排名門檻的推薦/觀察候選。"),
'''
    src = replace_once(src, anchor, replacement, "first export sheet")

    old_diag = '''            "用途": ("第一優先" if sheet_name == "股神推薦總排名" else "使用說明" if sheet_name == "使用導航" else "操作" if sheet_name in {"股神作戰總表", "完整推薦表", "正式下週主推薦", "A-準主推薦小量試單", "盤中核心雷達", "強勢動能核心雷達", "強勢前兆核心雷達", "強勢動能完整雷達", "強勢前兆完整雷達"} else "資料待更新/禁止操作" if sheet_name == "資料待更新雷達" else "診斷/管理"),
'''
    new_diag = '''            "用途": ("第一優先｜超級AI精選攻略" if sheet_name == "超級AI股神精選攻略" else "第二優先｜完整研究排名" if sheet_name == "股神推薦總排名" else "使用說明" if sheet_name == "使用導航" else "操作" if sheet_name in {"股神作戰總表", "完整推薦表", "正式下週主推薦", "A-準主推薦小量試單", "盤中核心雷達", "強勢動能核心雷達", "強勢前兆核心雷達", "強勢動能完整雷達", "強勢前兆完整雷達"} else "資料待更新/禁止操作" if sheet_name == "資料待更新雷達" else "診斷/管理"),
'''
    src = replace_once(src, old_diag, new_diag, "export diagnostic priority")

    PAGE7.write_text(src, encoding="utf-8")
    print("PASS V191-H30 Page07 SuperAI Excel guide integrated")


if __name__ == "__main__":
    main()

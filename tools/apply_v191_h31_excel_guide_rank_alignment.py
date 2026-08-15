# -*- coding: utf-8 -*-
"""Apply V191-H31 Page07 guide-source alignment.

H30 incorrectly preferred candidate_diagnosis_export (1,700+ rows), which does not
contain the authoritative total rank / SuperAI Trade columns.  H31 makes the guide
refine master_rank_df first; candidate diagnosis is fallback only.
"""
from __future__ import annotations
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAGE7 = ROOT / "pages" / "7_股神推薦.py"

OLD = '''        _guide_source = (\n            candidate_diagnosis_export\n            if isinstance(candidate_diagnosis_export, pd.DataFrame) and not candidate_diagnosis_export.empty\n            else master_rank_df\n        )\n        super_ai_guide_export = build_super_ai_excel_guide(_guide_source, max_rows=20)\n'''
NEW = '''        # V191-H31：精選攻略是「股神推薦總排名」的濃縮閱讀層，不得另從\n        # 1,700+ 候選診斷母體重新排名，否則會與正式總排名形成兩套互相矛盾的榜單。\n        _guide_source = (\n            master_rank_df\n            if isinstance(master_rank_df, pd.DataFrame) and not master_rank_df.empty\n            else candidate_diagnosis_export\n        )\n        super_ai_guide_export = build_super_ai_excel_guide(_guide_source, max_rows=20)\n'''


def main() -> None:
    src = PAGE7.read_text(encoding="utf-8-sig")
    if NEW in src:
        print("PASS H31 already applied")
        return
    if OLD not in src:
        raise RuntimeError("H31 anchor not found in Page07")
    PAGE7.write_text(src.replace(OLD, NEW, 1), encoding="utf-8")
    print("PASS V191-H31 Page07 guide source aligned to master_rank_df")


if __name__ == "__main__":
    main()

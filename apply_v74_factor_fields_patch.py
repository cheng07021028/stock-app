# -*- coding: utf-8 -*-
from __future__ import annotations

"""
v74：修正 v73 patch 腳本 SyntaxError。
這版不使用三引號多行變數，避免 GitHub 上傳/解析時出現 invalid syntax。
"""

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent

TARGET_FILES = [
    "pages/7_股神推薦.py",
    "pages/8_股神推薦紀錄.py",
    "pages/10_推薦清單.py",
    "pages/14_股神權重校正.py",
    "godpick_record_service.py",
]

IMPORT_LINE = "from godpick_factor_schema import enrich_dataframe, ensure_factor_columns, V72_FACTOR_FIELDS\n"

HELPER_BLOCK = "\n".join([
    "",
    "# >>> V72_FACTOR_ENRICH_HELPER",
    "def _v72_enrich_recommendation_df_safe(df):",
    "    try:",
    "        return enrich_dataframe(df)",
    "    except Exception:",
    "        try:",
    "            return ensure_factor_columns(df)",
    "        except Exception:",
    "            return df",
    "# <<< V72_FACTOR_ENRICH_HELPER",
    "",
    "",
])

def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")

def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")

def _insert_import(text: str) -> str:
    if "godpick_factor_schema import" in text:
        return text

    lines = text.splitlines(keepends=True)
    insert_at = 0

    for i, line in enumerate(lines):
        if line.startswith("from __future__ import"):
            insert_at = i + 1

    if insert_at == 0:
        for i, line in enumerate(lines[:100]):
            if line.startswith("import ") or line.startswith("from "):
                insert_at = i + 1

    lines.insert(insert_at, IMPORT_LINE)
    return "".join(lines)

def _insert_helper(text: str) -> str:
    if "V72_FACTOR_ENRICH_HELPER" in text:
        return text

    m = re.search(r"(?m)^def\s+", text)
    if m:
        return text[:m.start()] + HELPER_BLOCK + text[m.start():]

    return text + "\n" + HELPER_BLOCK

def _patch_display_calls(text: str) -> str:
    if "V72_FACTOR_DISPLAY_PATCH" in text:
        return text

    vars_to_try = [
        "recommend_df",
        "result_df",
        "save_df",
        "view_df",
        "show_df",
        "filtered_df",
        "df",
    ]

    lines = text.splitlines()
    out = []
    inserted_any = False

    for line in lines:
        stripped = line.lstrip()
        indent = line[:len(line) - len(stripped)]

        matched_var = None
        for var in vars_to_try:
            if f"st.dataframe({var}" in stripped or f"st.data_editor({var}" in stripped:
                matched_var = var
                break

        if matched_var and not inserted_any:
            out.append(indent + "# >>> V72_FACTOR_DISPLAY_PATCH")
            out.append(indent + f"{matched_var} = _v72_enrich_recommendation_df_safe({matched_var})")
            out.append(indent + "# <<< V72_FACTOR_DISPLAY_PATCH")
            inserted_any = True

        out.append(line)

    return "\n".join(out) + ("\n" if text.endswith("\n") else "")

def patch_file(rel_path: str) -> None:
    path = ROOT / rel_path
    if not path.exists():
        print(f"skip missing: {rel_path}")
        return

    old = _read(path)
    text = old
    text = _insert_import(text)
    text = _insert_helper(text)
    text = _patch_display_calls(text)

    if text != old:
        backup = path.with_suffix(path.suffix + ".v74factorbak")
        backup.write_text(old, encoding="utf-8")
        _write(path, text)
        print(f"patched: {rel_path}")
    else:
        print(f"unchanged: {rel_path}")

def main() -> int:
    for rel in TARGET_FILES:
        patch_file(rel)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

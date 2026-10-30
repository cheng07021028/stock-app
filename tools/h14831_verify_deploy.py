# -*- coding: utf-8 -*-
"""H148.3.1: check local Page07 cumulative runtime dependencies (no network/data writes)."""
from __future__ import annotations
import argparse
import ast
import sys
from pathlib import Path

CUMULATIVE_MODULES = (
    "godpick_h144_focus_board",
    "godpick_h147_five_gate_review",
    "godpick_h147_scan_bridge",
    "godpick_h148_strength_provenance",
    "godpick_h1481_focus_gate",
    "godpick_h1482_source_audit",
    "godpick_h1483_research_recall",
)


def verify(project_root: Path) -> tuple[bool, list[str]]:
    root = Path(project_root)
    page = root / "pages" / "7_股神推薦.py"
    notes: list[str] = []
    if not page.is_file():
        return False, [f"MISSING: {page.relative_to(root)}"]
    try:
        tree = ast.parse(page.read_text(encoding="utf-8"), filename=str(page))
    except (OSError, UnicodeError, SyntaxError) as exc:
        return False, [f"INVALID: {page.name}: {type(exc).__name__}: {exc}"]

    referenced: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            referenced.add(node.module.split(".")[0])
        elif isinstance(node, ast.Import):
            referenced.update(alias.name.split(".")[0] for alias in node.names)

    for module in CUMULATIVE_MODULES:
        target = root / f"{module}.py"
        if module not in referenced and module != "godpick_h144_focus_board":
            notes.append(f"WARNING: no direct reference to {module} in Page07")
        if not target.is_file():
            notes.append(f"MISSING: {module}.py")
            continue
        try:
            ast.parse(target.read_text(encoding="utf-8"), filename=str(target))
        except (OSError, UnicodeError, SyntaxError) as exc:
            notes.append(f"INVALID: {module}.py: {type(exc).__name__}: {exc}")
    bad = [item for item in notes if item.startswith(("MISSING:", "INVALID:"))]
    if not bad:
        notes.insert(0, "PASS: all seven H144/H147-H148.3 dependencies present and readable")
    return not bad, notes


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Verify H148.3.1 Page07 local deployment dependencies")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args(argv)
    ok, notes = verify(args.root)
    for item in notes:
        print(item)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

# -*- coding: utf-8 -*-
"""Remove duplicate encoded page files when a proper Unicode page exists.

Some historical deployments contain both ``4_自選股中心.py`` and an old
``4_#U81ea...py`` copy. Streamlit discovers both, so users can unknowingly open
the stale copy that does not contain the current persistence fixes. This module
only removes the encoded duplicate when the decoded Unicode target already
exists; unique legacy pages are left untouched.
"""
from __future__ import annotations

import re
from pathlib import Path

_PATTERN = re.compile(r"#U([0-9a-fA-F]{4})")


def _decode_name(name: str) -> str:
    return _PATTERN.sub(lambda m: chr(int(m.group(1), 16)), name)


def cleanup_duplicate_encoded_pages(pages_dir: Path | None = None) -> list[str]:
    root = pages_dir or (Path(__file__).resolve().parent / "pages")
    removed: list[str] = []
    if not root.exists():
        return removed
    for path in sorted(root.glob("*.py")):
        if "#U" not in path.name:
            continue
        decoded_name = _decode_name(path.name)
        if decoded_name == path.name:
            continue
        target = path.with_name(decoded_name)
        if not target.exists() or target.resolve() == path.resolve():
            continue
        try:
            path.unlink()
            removed.append(path.name)
        except OSError:
            # Read-only deployments continue to work; the persistence layer still
            # targets the Unicode pages, and the cleanup can be applied in Git later.
            continue
    return removed


if __name__ == "__main__":
    for item in cleanup_duplicate_encoded_pages():
        print(f"已移除舊版重複頁面：{item}")

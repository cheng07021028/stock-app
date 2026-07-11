# -*- coding: utf-8 -*-
"""Load the verified Phase 8.2 source patch over immutable full-source bases.

The public module paths remain stable. Full pre-upgrade source is stored under
``_phase82_source`` and a human-auditable unified patch is applied in memory.
No repository file is rewritten at runtime, so Streamlit's file watcher is not
triggered and the generated source is cached for the life of the process.
"""
from __future__ import annotations

import base64
import gzip
import re
import threading
from pathlib import Path
from typing import Any, MutableMapping

ROOT = Path(__file__).resolve().parent
PAYLOAD = ROOT / "phase82_patch_payload.txt"
BASES = {
    "formal": ROOT / "_phase82_source" / "godpick_formal_recommendation_engine_base.py",
    "schema": ROOT / "_phase82_source" / "godpick_column_schema_base.py",
    "signal_hub": ROOT / "_phase82_source" / "godpick_signal_hub_base.py",
    "page7": ROOT / "_phase82_source" / "page7_godpick_base.py",
    "governance": None,
}
PATCH_PATHS = {
    "formal": "godpick_formal_recommendation_engine.py",
    "schema": "godpick_column_schema.py",
    "signal_hub": "godpick_signal_hub.py",
    "page7": "pages/7_股神推薦.py",
    "governance": "godpick_execution_governance.py",
}
VERSION = "phase8_2_source_loader_20260712"
_CACHE: dict[str, str] = {}
_LOCK = threading.RLock()


def _payload_patch() -> str:
    text = PAYLOAD.read_text(encoding="utf-8-sig")
    match = re.search(
        r"(?:PATCH_B64_GZIP_BEGIN|<<'B64'\n)([A-Za-z0-9+/=\n]+?)(?:\nPATCH_B64_GZIP_END|\nB64)",
        text,
        flags=re.S,
    )
    if not match:
        raise RuntimeError("Phase 8.2 patch payload marker not found")
    encoded = "".join(match.group(1).split())
    return gzip.decompress(base64.b64decode(encoded)).decode("utf-8")


def _file_patch(full_patch: str, path: str) -> str:
    patterns = [f"--- a/{path}\n", "--- /dev/null\n"]
    start = -1
    for marker in patterns:
        pos = full_patch.find(marker)
        if pos < 0:
            continue
        if marker == "--- /dev/null\n":
            plus_end = full_patch.find("\n", pos + len(marker))
            plus_line = full_patch[pos + len(marker):plus_end]
            if plus_line != f"+++ b/{path}":
                search_from = plus_end
                while True:
                    pos = full_patch.find(marker, search_from)
                    if pos < 0:
                        break
                    plus_end = full_patch.find("\n", pos + len(marker))
                    plus_line = full_patch[pos + len(marker):plus_end]
                    if plus_line == f"+++ b/{path}":
                        break
                    search_from = plus_end
                if pos < 0:
                    continue
        start = pos
        break
    if start < 0:
        raise RuntimeError(f"Patch section not found: {path}")
    next_section = full_patch.find("\n--- ", start + 5)
    return full_patch[start:] if next_section < 0 else full_patch[start:next_section + 1]


def _apply_unified_patch(base_text: str, patch_text: str) -> str:
    base_lines = base_text.splitlines(keepends=True)
    patch_lines = patch_text.splitlines(keepends=True)
    out: list[str] = []
    source_pos = 0
    index = 0
    while index < len(patch_lines) and not patch_lines[index].startswith("@@ "):
        index += 1
    hunk_re = re.compile(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")
    while index < len(patch_lines):
        header = patch_lines[index]
        if not header.startswith("@@ "):
            index += 1
            continue
        match = hunk_re.match(header)
        if not match:
            raise RuntimeError(f"Invalid unified-diff hunk: {header.strip()}")
        old_line = int(match.group(1))
        old_start = 0 if old_line == 0 else old_line - 1
        if old_start < source_pos:
            raise RuntimeError("Overlapping Phase 8.2 patch hunks")
        out.extend(base_lines[source_pos:old_start])
        source_pos = old_start
        index += 1
        while index < len(patch_lines) and not patch_lines[index].startswith("@@ "):
            line = patch_lines[index]
            if line.startswith("--- ") and index > 0:
                break
            prefix = line[:1]
            body = line[1:]
            if prefix == " ":
                if source_pos >= len(base_lines) or base_lines[source_pos].rstrip("\n") != body.rstrip("\n"):
                    raise RuntimeError("Phase 8.2 patch context mismatch")
                out.append(base_lines[source_pos])
                source_pos += 1
            elif prefix == "-":
                if source_pos >= len(base_lines) or base_lines[source_pos].rstrip("\n") != body.rstrip("\n"):
                    raise RuntimeError("Phase 8.2 patch removal mismatch")
                source_pos += 1
            elif prefix == "+":
                out.append(body)
            elif line.startswith("\\ No newline"):
                pass
            else:
                break
            index += 1
    out.extend(base_lines[source_pos:])
    return "".join(out)


def get_phase82_source(kind: str) -> str:
    if kind not in PATCH_PATHS:
        raise KeyError(f"Unknown Phase 8.2 source kind: {kind}")
    with _LOCK:
        cached = _CACHE.get(kind)
        if cached is not None:
            return cached
        base_path = BASES[kind]
        base_text = "" if base_path is None else base_path.read_text(encoding="utf-8-sig")
        section = _file_patch(_payload_patch(), PATCH_PATHS[kind])
        source = _apply_unified_patch(base_text, section)
        compile(source, str(base_path or PATCH_PATHS[kind]), "exec")
        checks = {
            "formal": "vnext_phase8_2_execution_governance_20260712",
            "schema": "實戰操作品質分",
            "signal_hub": "SIGNAL_HUB_LOAD_COLUMNS",
            "page7": "掃描完整性",
            "governance": "phase8_2_execution_governance_20260712",
        }
        if checks[kind] not in source:
            raise RuntimeError(f"Phase 8.2 source integrity check failed: {kind}")
        _CACHE[kind] = source
        return source


def exec_phase82_source(kind: str, namespace: MutableMapping[str, Any]) -> None:
    source = get_phase82_source(kind)
    filename = str(BASES[kind] or PATCH_PATHS[kind])
    namespace["PHASE82_SOURCE_LOADER_VERSION"] = VERSION
    exec(compile(source, filename, "exec"), namespace, namespace)

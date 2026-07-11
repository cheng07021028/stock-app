# -*- coding: utf-8 -*-
"""Phase 8 runtime source loader.

The original full modules are kept as immutable core blobs.  The one-time patch
payload is applied in an isolated temporary directory and the patched source is
then executed under the public module names.  This avoids duplicating or
truncating the very large Streamlit page while making the Phase 8 behavior part
of the deployed commit.
"""
from __future__ import annotations

import os
import tempfile
import textwrap
import threading
from pathlib import Path
from typing import Any, MutableMapping

_BASE_DIR = Path(__file__).resolve().parent
_PAYLOAD = _BASE_DIR / "phase8_actionable_patch_payload.txt"
_CORE_FILES = {
    "formal": _BASE_DIR / "_phase8_core" / "godpick_formal_recommendation_engine_core.py",
    "schema": _BASE_DIR / "_phase8_core" / "godpick_column_schema_core.py",
    "page7": _BASE_DIR / "_phase8_core" / "page7_godpick_core.py",
}
_OUTPUT_RELATIVE = {
    "formal": Path("godpick_formal_recommendation_engine.py"),
    "schema": Path("godpick_column_schema.py"),
    "page7": Path("pages") / "7_股神推薦.py",
}
_CACHE: dict[str, str] = {}
_LOCK = threading.RLock()
VERSION = "phase8_actionable_loader_20260712"


def _extract_patch_script(payload_text: str) -> str:
    marker = "python - <<'PY'\n"
    start = payload_text.find(marker)
    if start < 0:
        raise RuntimeError("Phase 8 patch payload is missing its Python block")
    start += len(marker)
    end = payload_text.find("\n          PY", start)
    if end < 0:
        end = payload_text.find("\nPY", start)
    if end < 0:
        raise RuntimeError("Phase 8 patch payload has no closing marker")
    script = textwrap.dedent(payload_text[start:end])
    if "godpick_formal_recommendation_engine.py" not in script or "_phase70_build_battle_dashboard" not in script:
        raise RuntimeError("Phase 8 patch payload failed integrity validation")
    return script


def _build_sources() -> dict[str, str]:
    payload_text = _PAYLOAD.read_text(encoding="utf-8-sig")
    script = _extract_patch_script(payload_text)
    with tempfile.TemporaryDirectory(prefix="godpick_phase8_") as temp_name:
        temp = Path(temp_name)
        (temp / "pages").mkdir(parents=True, exist_ok=True)
        for key, source_path in _CORE_FILES.items():
            if not source_path.exists():
                raise RuntimeError(f"Phase 8 core source missing: {source_path.name}")
            target = temp / _OUTPUT_RELATIVE[key]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(source_path.read_text(encoding="utf-8-sig"), encoding="utf-8")

        old_cwd = Path.cwd()
        try:
            os.chdir(temp)
            namespace: dict[str, Any] = {"__name__": "__phase8_source_patch__"}
            exec(compile(script, str(_PAYLOAD), "exec"), namespace, namespace)
        finally:
            os.chdir(old_cwd)

        built: dict[str, str] = {}
        for key, relative in _OUTPUT_RELATIVE.items():
            output = temp / relative
            if not output.exists():
                raise RuntimeError(f"Phase 8 patched source missing: {relative}")
            text = output.read_text(encoding="utf-8-sig")
            if key == "formal" and "vnext_phase8_0_actionable_governance_20260712" not in text:
                raise RuntimeError("Formal recommendation engine was not upgraded to Phase 8")
            if key == "page7" and "股神正式推薦作戰表" not in text:
                raise RuntimeError("Page 7 actionable recommendation panel was not installed")
            if key == "schema" and "最終操作結論" not in text:
                raise RuntimeError("Phase 8 shared column schema was not installed")
            built[key] = text
        return built


def get_phase8_source(kind: str) -> str:
    if kind not in _CORE_FILES:
        raise KeyError(f"Unknown Phase 8 source kind: {kind}")
    with _LOCK:
        if not _CACHE:
            _CACHE.update(_build_sources())
        return _CACHE[kind]


def exec_phase8_source(kind: str, namespace: MutableMapping[str, Any]) -> None:
    source = get_phase8_source(kind)
    core_path = _CORE_FILES[kind]
    namespace["PHASE8_RUNTIME_LOADER_VERSION"] = VERSION
    exec(compile(source, str(core_path), "exec"), namespace, namespace)

# -*- coding: utf-8 -*-
"""Safely apply H111 to the CURRENT installed Page 7, never replace it with an old copy.

Run at stock-app root: python tools/apply_h111_scan_settings_fix.py
Backs up the exact existing page; refuses unknown/missing integration hooks.
"""
from __future__ import annotations

import argparse
import ast
import datetime
import hashlib
import os
from pathlib import Path
import shutil
import tempfile

MARK = "# H111_VERIFIED_SCAN_SETTINGS_AUTHORITY"


def _replace_function(source: str, name: str, new_body: str) -> str:
    ast_tree = ast.parse(source)
    matches = [x for x in ast_tree.body if isinstance(x, (ast.FunctionDef, ast.AsyncFunctionDef)) and x.name == name]
    if len(matches) != 1:
        raise RuntimeError(f"找不到唯一的原始函式 {name}，拒絕修改；請上傳目前版本")
    node = matches[0]
    lines = source.splitlines(keepends=True)
    start = node.lineno - 1
    end = node.end_lineno
    # Do not delete decorators unexpectedly.
    if node.decorator_list:
        raise RuntimeError(f"{name} 已有 decorator，拒絕覆蓋")
    new_lines = [x + "\n" for x in new_body.rstrip("\n").split("\n")]
    return "".join(lines[:start] + new_lines + lines[end:])


NEW_LOAD = '''def _load_persistent_recommend_scan_settings(watchlist_map=None, category_options=None) -> dict[str, Any]:
    # H111_VERIFIED_SCAN_SETTINGS_AUTHORITY: remote first, never baked-in local JSON first.
    from godpick_h111_scan_settings_authority import load_scan_settings
    _legacy = _load_persistent_settings(local_first=True)
    _legacy_scan = _legacy.get("scan_settings", {}) if isinstance(_legacy, dict) else {}
    _h111_result = load_scan_settings(
        project_root=Path(__file__).resolve().parent.parent,
        legacy_settings=_legacy_scan,
    )
    st.session_state[_k("h111_scan_settings_load_status")] = {
        "ok": bool(_h111_result.ok),
        "verified": bool(_h111_result.durable_verified),
        "source": _h111_result.source,
        "revision": _h111_result.revision,
        "message": _h111_result.message,
    }
    # Preserve all saved selected sectors in the multi-select even if today's
    # master category set temporarily lacks some historical categories.
    if isinstance(category_options, list):
        for _cat in _h111_result.settings.get("selected_categories", []):
            if isinstance(_cat, str) and _cat and _cat not in category_options:
                category_options.append(_cat)
    return _normalize_recommend_scan_settings(_h111_result.settings, watchlist_map, category_options)
'''

NEW_SAVE = '''def _save_persistent_recommend_scan_settings(settings: dict[str, Any]) -> tuple[bool, list[str]]:
    # H111_VERIFIED_SCAN_SETTINGS_AUTHORITY: remote write + readback must both pass.
    from godpick_h111_scan_settings_authority import save_scan_settings
    _h111_result = save_scan_settings(
        copy.deepcopy(settings), project_root=Path(__file__).resolve().parent.parent,
    )
    st.session_state[_k("h111_scan_settings_save_status")] = {
        "ok": bool(_h111_result.ok),
        "verified": bool(_h111_result.durable_verified),
        "source": _h111_result.source,
        "revision": _h111_result.revision,
        "message": _h111_result.message,
    }
    return bool(_h111_result.durable_verified), [_h111_result.message]
'''


def rewrite(source: str) -> str:
    if MARK in source:
        return source
    for name in ("_load_persistent_recommend_scan_settings", "_save_persistent_recommend_scan_settings"):
        if source.count("def " + name + "(") != 1:
            raise RuntimeError(f"目前版本 {name} 出現次數不符，拒絕自動修改")
    if 'st.success(st.session_state.pop(_k("scan_settings_msg")))' not in source:
        raise RuntimeError("找不到掃描設定保存訊息顯示點，拒絕自動修改")
    source = _replace_function(source, "_load_persistent_recommend_scan_settings", NEW_LOAD)
    source = _replace_function(source, "_save_persistent_recommend_scan_settings", NEW_SAVE)
    old = 'st.success(st.session_state.pop(_k("scan_settings_msg")))'
    new = '''_h111_save_message = st.session_state.pop(_k("scan_settings_msg"))
        if "失敗" in _h111_save_message or "未驗證" in _h111_save_message:
            st.error(_h111_save_message)
        else:
            st.success(_h111_save_message)'''
    source = source.replace(old, new, 1)
    # Surface the remote durable load source, rather than claim it is permanent
    # based on successful ephemeral container file writes.
    anchor = '    applied_weights = _render_score_weight_panel()'
    if source.count(anchor) != 1:
        raise RuntimeError("找不到唯一的掃描設定健康訊息插入點，拒絕修改")
    panel = '''    # H111: Status is observed at cold boot, and does not expose secrets.
    _h111_load_health = st.session_state.get(_k("h111_scan_settings_load_status"), {})
    if _h111_load_health:
        if _h111_load_health.get("verified"):
            st.caption("🔒 H111 設定已從 GitHub 永久權威載入並確認；Reboot 會重新讀取。")
        else:
            st.warning("⚠️ H111 推薦設定尚未確認永久保存：" + str(_h111_load_health.get("message", "")))

'''
    source = source.replace(anchor, panel + anchor, 1)
    ast.parse(source)
    return source


def main() -> int:
    parser = argparse.ArgumentParser(description="H111 safe Page 7 in-place update")
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    root = args.project_root.resolve()
    target = root / "pages" / "7_股神推薦.py"
    helper = root / "godpick_h111_scan_settings_authority.py"
    if not target.is_file() or not helper.is_file():
        raise SystemExit("FAIL：必須先將 H111 ZIP 中的檔案合併至 stock-app 專案根目錄")
    raw = target.read_bytes()
    bom = raw.startswith(b"\xef\xbb\xbf")
    source = raw.decode("utf-8-sig")
    old_hash = hashlib.sha256(raw).hexdigest()
    output = rewrite(source)
    if output == source:
        print("ALREADY PATCHED：H111 設定修正已存在，沒有重複修改")
        return 0
    patched = (b"\xef\xbb\xbf" if bom else b"") + output.encode("utf-8")
    compile(patched, str(target), "exec")
    print("原始 Page7 SHA256", old_hash)
    print("修正 Page7 SHA256", hashlib.sha256(patched).hexdigest())
    print("H111 compile：PASS；不會修改現有推薦演算法 / Page8 / JSON")
    if args.check_only:
        return 0
    backup_dir = root / "backups" / "h111_page07"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = backup_dir / f"7_股神推薦_{stamp}_{old_hash[:12]}.py"
    shutil.copy2(target, backup)
    if backup.read_bytes() != raw:
        raise SystemExit("FAIL：Page7 備份驗證失敗，未寫入")
    fd, tmpname = tempfile.mkstemp(prefix="h111_", suffix=".tmp", dir=str(target.parent))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(patched)
        os.replace(tmpname, target)
        if target.read_bytes() != patched:
            raise RuntimeError("覆蓋後回讀驗證失敗")
    except Exception:
        try:
            os.unlink(tmpname)
        except OSError:
            pass
        shutil.copy2(backup, target)
        raise
    print("PASS：已安全修正既有 Page7，原檔備份於", backup)
    print("重新啟動 APP → 設定 → 套用設定；需能成功連線至 GitHub 並回讀驗證。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

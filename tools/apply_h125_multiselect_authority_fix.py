# -*- coding: utf-8 -*-
"""H125: safely repair Page7 selected_categories reboot roundtrip.

Never replaces installed Page7 with a historical file. Uses H111 service if
already installed; supplies H111 only when missing. Fail closed on duplicate
Unicode/#U-encoded Page7 names, unknown code anchors, or changed layouts.
"""
from __future__ import annotations
import argparse
import ast
import hashlib
import importlib.util
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
from datetime import datetime

MARK = '# H125_SELECTED_CATEGORIES_AUTHORITY'
ENC = re.compile(r'#U([0-9a-fA-F]{4})')
CANON7 = '7_股神推薦.py'
CANON8 = '8_股神推薦紀錄.py'


def decode_escaped_name(name: str) -> str:
    return ENC.sub(lambda m: chr(int(m.group(1), 16)), name)


def _page_candidates(page_dir: Path, canonical: str):
    return sorted([p for p in page_dir.iterdir() if p.is_file() and p.suffix == '.py'
                   and (p.name == canonical or decode_escaped_name(p.name) == canonical)], key=lambda p: p.name)


def _h111_rewrite(src: str, support: Path) -> str:
    spec = importlib.util.spec_from_file_location('h125_vendored_h111_patcher', support / 'apply_h111_scan_settings_fix.py')
    if spec is None or spec.loader is None:
        raise RuntimeError('H111 安全修補程式不可用')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.rewrite(src)


def _replace_one(source: str, old: str, new: str, reason: str) -> str:
    found = source.count(old)
    if found != 1:
        raise RuntimeError(f'H125 {reason}：需要唯一插入點，實際 {found} 個；停止而不覆蓋')
    return source.replace(old, new, 1)


def rewrite(src: str, support_dir: Path) -> str:
    ast.parse(src)
    if MARK in src:
        return src
    # Ensure the remote confirmed H111 authority is *actually* wired to Page7.
    if '# H111_VERIFIED_SCAN_SETTINGS_AUTHORITY' not in src:
        src = _h111_rewrite(src, support_dir)
    if ('from godpick_h111_scan_settings_authority import load_scan_settings' not in src
        or 'from godpick_h111_scan_settings_authority import save_scan_settings' not in src):
        raise RuntimeError('H111 遠端權威讀取／寫入並未同時接線，拒絕假裝永久保存')

    old_normalize = '''    category_options = category_options or ["全部"]
    cats = base.get("selected_categories", ["全部"])
    if not isinstance(cats, list):
        cats = ["全部"]
    cats = [x for x in cats if x in category_options] or ["全部"]
    base["selected_categories"] = cats'''
    new_normalize = '''    # H125_SELECTED_CATEGORIES_AUTHORITY: never prune saved categories on an incomplete market master.
    from godpick_h125_multiselect_authority import preserve_selected_categories
    cats, _available_for_widget = preserve_selected_categories(
        base.get("selected_categories", ["全部"]), category_options
    )
    base["selected_categories"] = cats'''
    src = _replace_one(src,old_normalize,new_normalize,'取消產業選項刪除機制')

    old_preload = '''    saved_categories = st.session_state.get(_k("selected_categories"), ["全部"])
    saved_categories = [x for x in saved_categories if x in category_options] or ["全部"]'''
    new_preload = '''    from godpick_h125_multiselect_authority import preserve_selected_categories
    saved_categories, category_options = preserve_selected_categories(
        st.session_state.get(_k("selected_categories"), ["全部"]), category_options
    )'''
    src = _replace_one(src, old_preload, new_preload, '初始化時保留所有已勾選產業')

    # At cold boot H111 loads *after* initial master list; reconcile again before
    # rendering widgets so delayed options cannot erase remote categories.
    anchor = '''    render_pro_section("掃描設定")'''
    addition = '''    # H125: reconcile *after* remote H111 load and before multiselect widget creation.
    _h125_saved = st.session_state.get(
        _ui_pref_key("selected_categories"), st.session_state.get(_k("selected_categories"), ["全部"])
    )
    _h125_saved, category_options = preserve_selected_categories(_h125_saved, category_options)
    st.session_state[_ui_pref_key("selected_categories")] = copy.deepcopy(_h125_saved)

'''
    src = _replace_one(src, anchor, addition+anchor, '遠端設定回載後的多選同步')

    old_widget = '''        valid_saved_categories = [x for x in st.session_state.get(_ui_pref_key("selected_categories"), saved_categories) if x in category_options] or ["全部"]
        st.session_state[_ui_pref_key("selected_categories")] = valid_saved_categories'''
    new_widget = '''        valid_saved_categories, category_options = preserve_selected_categories(
            st.session_state.get(_ui_pref_key("selected_categories"), saved_categories), category_options
        )
        st.session_state[_ui_pref_key("selected_categories")] = valid_saved_categories'''
    src = _replace_one(src,old_widget,new_widget,'多選 Widget 值保持一致')

    # Show explicit, truthful diagnostic; same remote gate as H111.
    anchor_status = '''    applied_weights = _render_score_weight_panel()'''
    status = '''    # H125: visible selected-categories count; never mislabel an unverified cache as durable.
    # A just-submitted save result takes precedence over the older cold-load health.
    # A failed save must NOT be masked by a previously successful load.
    _h125_saved_health = st.session_state.get(_k("h111_scan_settings_save_status"), {})
    _h125_health = (_h125_saved_health if isinstance(_h125_saved_health, dict) and _h125_saved_health
                    else st.session_state.get(_k("h111_scan_settings_load_status"), {}))
    _h125_count = len(st.session_state.get(_ui_pref_key("selected_categories"), []))
    if _h125_health.get("verified"):
        st.caption(f"🔒 H125：已從 GitHub 永久權威載入 {_h125_count} 個產業選擇；按套用後會同步驗證全部欄位。")
    else:
        st.warning(f"H125：目前選擇 {_h125_count} 個產業，但遠端永久權威尚未驗證；Reboot 可能無法保留，請檢查保存明細。")

'''
    src = _replace_one(src,anchor_status,status+anchor_status,'設定權威可見檢查')
    ast.parse(src)
    return src


def file_sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def main(argv=None):
    pa = argparse.ArgumentParser()
    pa.add_argument('--project-root', type=Path, default=Path(__file__).resolve().parents[1])
    pa.add_argument('--check-only', action='store_true')
    pa.add_argument('--fix-page-names', action='store_true', help='安全修復 #Uxxxx 編碼的 7/8 頁檔名')
    args = pa.parse_args(argv)
    root = args.project_root.resolve()
    page_dir = root / 'pages'
    support = root / 'h125_support'
    support_h111 = support / 'godpick_h111_scan_settings_authority.py'
    h111_target = root / 'godpick_h111_scan_settings_authority.py'
    h125_target = root / 'godpick_h125_multiselect_authority.py'
    if not page_dir.is_dir() or not h125_target.exists() or not support_h111.exists():
        raise RuntimeError('請先解壓縮 H125 ZIP 至當前正式專案根目錄')
    paths = {}
    for canonical in (CANON7,CANON8):
        found = _page_candidates(page_dir,canonical)
        if len(found)>1:
            raise RuntimeError(f'發現重複的 {canonical}（可能有 #U 編碼副本）；為避免改錯頁面，請先人工比對：{[p.name for p in found]}')
        if found:
            paths[canonical]=found[0]
    if CANON7 not in paths:
        raise RuntimeError('無法定位第7頁（含中文／#Uxxxx 格式），拒絕修改')
    for canonical,p in paths.items():
        if p.name != canonical:
            print(f'檔名異常：{p.name}  →  {canonical}')
            if not args.fix_page_names:
                raise RuntimeError('請明確加入 --fix-page-names 才能修正亂碼頁名；未修改任何檔案')

    source = paths[CANON7].read_text(encoding='utf-8-sig')
    revised = rewrite(source,support)
    compile(revised, str(paths[CANON7]),'exec')
    if h111_target.exists():
        compile(h111_target.read_text(encoding='utf-8-sig'),str(h111_target),'exec')
    else:
        compile(support_h111.read_text(encoding='utf-8-sig'),str(h111_target),'exec')
    print('H125_READY：相容性與編譯 PASS')
    print('第7頁來源:',paths[CANON7], 'SHA256:', file_sha(paths[CANON7]))
    print('H111 既有模組:',h111_target.exists(), 'H125 待修改:',revised!=source)
    if args.check_only: return 0
    backup_dir = root/'backups'/'h125_multiselect'
    backup_dir.mkdir(parents=True,exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    for canonical,p in paths.items():
        backup = backup_dir / f'{stamp}_{p.name}'
        shutil.copy2(p,backup)
        if file_sha(backup) != file_sha(p):
            raise RuntimeError('備份回讀雜湊不一致，停止操作')
        print('已備份:',backup)
    if not h111_target.exists():
        shutil.copy2(support_h111,h111_target)
        print('新增 H111 遠端永久權威模組')
    # Write the Page7 rewrite before renaming to retain old filename if failure.
    if revised != source:
        target=paths[CANON7]
        original=target.read_bytes()
        prefix=b'\xef\xbb\xbf' if original.startswith(b'\xef\xbb\xbf') else b''
        revised_bytes=prefix+revised.encode('utf-8')
        fd,name=tempfile.mkstemp(dir=target.parent,prefix='.h125_',suffix='.tmp')
        try:
            with os.fdopen(fd,'wb') as f:f.write(revised_bytes)
            os.replace(name,target)
            if target.read_bytes()!=revised_bytes:
                raise RuntimeError('Page7 重讀驗證失敗')
        except BaseException:
            try:os.unlink(name)
            except OSError:pass
            target.write_bytes(original)
            raise
    if args.fix_page_names:
        for canonical,p in paths.items():
            if p.name != canonical:
                target = page_dir / canonical
                if target.exists():
                    raise RuntimeError(f'正式檔名已存在，禁止覆寫：{target}')
                p.rename(target)
                print('修復檔名:',canonical)
    print('H125_PASS：設定回載、複選保護及遠端權威已就地修補；未修改其他頁面內容')
    print('下一步請 commit: pages/7_股神推薦.py, godpick_h111_scan_settings_authority.py, godpick_h125_multiselect_authority.py')
    return 0

if __name__ == '__main__':
    try: raise SystemExit(main())
    except Exception as e:
        print('H125_BLOCKED：',str(e));raise SystemExit(2)

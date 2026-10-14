# -*- coding: utf-8 -*-
"""H126 deterministic guarded patch to deployed Page7 + H105; backup-first.

Not a Page7 full replacement. Check-only is read-only; deployment follows git.
"""
from __future__ import annotations
import argparse, ast, shutil
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
MODULE=ROOT/'godpick_h126_authority_report.py'
H105=ROOT/'godpick_h105_future_blackhorse_engine.py'
PAGE=ROOT/'pages'/'7_股神推薦.py'


def patch_h105(src:str)->str:
    mark='    return out\n\n\ndef export_contract_summary('
    injection=('    from godpick_h126_authority_report import attach_authority as _h126_attach_authority\n'
               '    out = _h126_attach_authority(out)\n'
               '    return out\n\n\ndef export_contract_summary(')
    if '_h126_attach_authority' in src:
        ast.parse(src);return src
    if src.count(mark)!=1:
        raise ValueError('H126_H105_UNSUPPORTED: 缺少 H114/H113 相容的 decorater return 錨點')
    result=src.replace(mark,injection,1)
    ast.parse(result)
    return result


def patch_page(src:str)->str:
    if 'H126_AUTHORITY_EXPORT_HOOK_V1' in src:
        ast.parse(src);return src
    left='def _build_excel_bytes_fast_h85('
    right='\ndef _build_selected_excel_bytes_h85('
    if src.count(left)!=1 or src.count(right)!=1:raise ValueError('H126_PAGE_UNSUPPORTED: H85 Excel 函式無法唯一定位')
    start=src.index(left);end=src.index(right,start)
    chunk=src[start:end]
    target='    sheets = [\n'
    if chunk.count(target)!=1:raise ValueError('H126_PAGE_UNSUPPORTED: H85 worksheets 沒有唯一錨點')
    guard='''    # H126_AUTHORITY_EXPORT_HOOK_V1: single source for manager's decision; H102 is historical only.
    from godpick_h126_authority_report import attach_authority as _h126_attach_authority
    tables = _h126_attach_authority(tables)
    health = pd.concat([health, tables['h126_health']], ignore_index=True, sort=False)
    sheets = [
        ("00_H126主管決策", tables['h126_authority'], "本輪未能認證黑馬；不可依舊版研究分數買進。"),
        ("00A_H126資料缺口", tables['h126_health'], "H126 尚無有效權威來源資料。"),
'''
    chunk=chunk.replace(target,guard,1)
    old='("00_推薦優先總覽",'
    if chunk.count(old)!=1:
        raise ValueError('H126_PAGE_UNSUPPORTED: 舊H102排序表無法明確分辨')
    chunk=chunk.replace(old,'("09_H102舊排序_非買進",',1)
    chunk=chunk.replace('"本輪沒有Formal或核心Research可建立推薦優先總覽。"',
                        '"H102僅歷史研究/族群排序，非H126認證、亦非買進許可。"',1)
    # H126 manager workbook: only nine decision-relevant tabs; all original
    # sources remain in the App and have not been deleted or rewritten.
    end_anchor='    for name, frame, fallback in sheets:\n'
    if chunk.count(end_anchor)!=1:
        raise ValueError('H126_PAGE_UNSUPPORTED: 無法安全定位主管工作表輸出迴圈')
    limiter='''    _h126_manager_tabs = {
        '00_H126主管決策', '00A_H126資料缺口',
        '01_正式推薦與交易計畫', '02_研究推薦', '02_既有研究追蹤',
        '04_今日主流資金', '05_驗證與風險證據',
        '06_績效煞車與健康', '08_T+1推薦檢討',
        '09_H102舊排序_非買進',
    }
    sheets = [item for item in sheets if item[0] in _h126_manager_tabs]
    # 05 sheet is a concise audit, not 1,281 columns of internal engine state.
    _h126_audit_keep = (
        '股票代號','股票名稱','市場別','類別',
        'H113入榜資格','H113流動性合格','H113大戶鎖碼合格',
        'H113法人逐日來源驗證','H113法人連買合格','H113主升起漲合格',
        'H113缺失原因','H123法人實證狀態','H123來源雜湊',
        'H123研究實際時間','H124大戶實證狀態',
        'H124大戶變化pp','H124大戶來源雜湊',
        'H109決策權威','H109否決原因','H110證據狀態',
        'H110研究資訊截止日','H105發動階段',
        'H105今日強勢排除','H110最終研究優先分',
    )
    sheets = [(name, frame.loc[:, [c for c in _h126_audit_keep if c in frame.columns]]
               if name == '05_驗證與風險證據' and isinstance(frame, pd.DataFrame) and not frame.empty
               and any(c in frame.columns for c in _h126_audit_keep) else frame, fallback)
              for name, frame, fallback in sheets]
'''
    chunk=chunk.replace(end_anchor,limiter+end_anchor,1)
    result=src[:start]+chunk+src[end:]
    ast.parse(result)
    return result


def main(argv=None)->int:
    parser=argparse.ArgumentParser(description='H126 backup-first safe patch')
    parser.add_argument('--check-only',action='store_true')
    args=parser.parse_args(argv)
    if not MODULE.is_file():raise ValueError('H126_MISSING_MODULE：請先解壓完整升級包')
    if not H105.is_file() or not PAGE.is_file():raise ValueError('H126_MISSING_CURRENT_PROJECT：缺 H105 或 Pages/7 正式頁')
    before={H105:H105.read_text(encoding='utf-8-sig'),PAGE:PAGE.read_text(encoding='utf-8-sig')}
    after={H105:patch_h105(before[H105]),PAGE:patch_page(before[PAGE])}
    status=', '.join(p.name+(' PATCHABLE' if before[p]!=after[p] else ' ALREADY_PATCHED') for p in before)
    h105_current=before[H105]
    flow='from godpick_h124_runtime_bridge import' in h105_current
    oldflow='from godpick_h123_runtime_bridge import' in h105_current
    audit_message=('H123/H124正式接線已發現' if flow else
                   '僅H123接線，H124尚未套用' if oldflow else
                   'H123/H124尚未接到H105；嚴格認證將顯示待驗證，請先執行H123與H124定點修補')
    if args.check_only:
        print('H126_READY｜'+status+'｜'+audit_message)
        return 0
    # All preconditions validated BEFORE any modification.
    for path in before:
        if before[path] == after[path]: continue
        backup=path.with_suffix(path.suffix+'.before_h126.bak')
        if backup.exists() and backup.read_text(encoding='utf-8-sig')!=before[path]:
            raise ValueError('H126_BACKUP_CONFLICT：停止覆蓋 '+str(backup))
    for path in before:
        if before[path]==after[path]:continue
        backup=path.with_suffix(path.suffix+'.before_h126.bak')
        if not backup.exists():shutil.copy2(path,backup)
        tmp=path.with_suffix(path.suffix+'.h126tmp')
        tmp.write_text(after[path],encoding='utf-8')
        ast.parse(tmp.read_text(encoding='utf-8'))
        tmp.replace(path)
    print('H126_PATCH_PASS｜'+audit_message+'｜請提交兩個已修補檔及H126模組到真正的部署分支；請排除.bak')
    return 0

if __name__=='__main__':raise SystemExit(main())

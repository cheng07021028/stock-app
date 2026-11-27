# -*- coding: utf-8 -*-
"""Materialize the already audited R2 source changes as a verified ZIP. Read-only checkout."""
import argparse, ast, hashlib, json, sys, zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
R2_INPUTS={
    'BUILD_H250R2.py':'d522cc684f58f6fa33c19b6d7b8840a4bb5a6fc9',
    'previous_task_contract.py':'33216721cbd8fa36ea3791008ad20b114658ee56',
    'godpick_h250r2_nday.py':'c1f60b619095247a4cef3fc5dfbdf0cfa6a724dc',
    'godpick_h250r2_table.py':'36b1e4aca38862debd204c588bf0fcc28318da22',
    'tests/test_h250r2.py':'100c723a268e83523169f110a93e98fea51585ad',
    'test_h250r2_builder.py':'ce0008236c433a37db7ecfc35fa56f4657e91fb9',
}
REQUIRED={'godpick_column_manager.py','pages/10_推薦清單.py','godpick_auto_update_tasks.py','godpick_h241_github_records.py','godpick_h250r2_nday.py','godpick_h250r2_table.py','tests/test_h250r2.py'}

def blob(b):return hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()

def verify_inputs(root):
    for p,expected in R2_INPUTS.items():
        if blob((root/p).read_bytes())!=expected:raise ValueError('R2 source mismatch: '+p)

def verify_zip(out):
    with zipfile.ZipFile(out) as z:
        if z.testzip() or len(z.namelist())!=len(set(z.namelist())):raise ValueError('archive invalid')
        if not REQUIRED<=set(z.namelist()):raise ValueError('missing production files')
        for name in z.namelist():
            p=Path(name)
            if p.is_absolute() or '..' in p.parts or '\\' in name:raise ValueError('unsafe zip path')
            if p.suffix=='.py':compile(z.read(name),name,'exec')
        table=z.read('godpick_column_manager.py').decode('utf-8-sig');tree=ast.parse(table)
        for n in ('managed_dataframe','managed_data_editor'):
            f=[x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name==n]
            if len(f)!=1 or '_h250r2_native' not in ast.unparse(f[0]) or '_godpick_bypass' in ast.unparse(f[0]):raise ValueError('unpatched managed table: '+n)
        page=z.read('pages/10_推薦清單.py').decode('utf-8-sig');task=z.read('godpick_auto_update_tasks.py').decode('utf-8-sig');history=z.read('godpick_h241_github_records.py').decode('utf-8-sig')
        for s,markers in ((page,('_h250r2_formal_run','mask_formal','overlay_verified_history')),(task,('H250R2 正式N日績效','run_authority')),(history,('preserve_verified(old,out)',))):
            if not all(m in s for m in markers):raise ValueError('missing R2 runtime integration')
        receipt={'status':'OVERLAY_VERIFIED_NOT_DEPLOYED','files':{n:hashlib.sha256(z.read(n)).hexdigest() for n in z.namelist()},'zip_sha256':hashlib.sha256(out.read_bytes()).hexdigest()}
    return receipt

def main():
    a=argparse.ArgumentParser();a.add_argument('--out',type=Path,required=True);args=a.parse_args();out=args.out.resolve()
    verify_inputs(ROOT);sys.path.insert(0,str(ROOT));from BUILD_H250R2 import generate
    generate(ROOT,out);receipt=verify_zip(out)
    out.with_suffix('.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print('[PASS] actual production overlay generated; NOT deployed')
if __name__=='__main__':main()

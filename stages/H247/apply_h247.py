# -*- coding: utf-8 -*-
"""H247: build only verified changed files, without modifying the project."""
from pathlib import Path
import argparse, ast, hashlib, zipfile

ROOT=Path(__file__).resolve().parent
WF='.github/workflows/'
BASE={
 'godpick_h124_ownership_flow.py':'2eb61f3f0c497d919a1d4544d7eaf519732a623f',
 'godpick_h222_financial_history.py':'c7f2b2076b9dabc951bd9e3d103ab08df89936f8',
 'tools/h206_daily_review.py':'e77c1a91dae7b23881772618ffeb5b9c71513258',
 WF+'h195_daily_command.yml':'3e08ec7844ea223c640799801ac8cb331dae690b',
 WF+'h206_market_gap.yml':'f6d003964191f6096e18ebd3176405bf9b430e52',
 WF+'h219_review.yml':'400b6df0c5cd14a0bc0805b0aa193c44b539bb6a',
 WF+'h220_review.yml':'5a702a677531e976d8f56b157445a23532aebe1f',
 WF+'h221_official_chips.yml':'d2f55bba198ec33b1ce0ee8444a0a5c246a27872',
 WF+'h222_financial_history.yml':'3d1df938a7cc957aed2c3df0cdff016bb053df0d',
 WF+'h224_growth_resonance.yml':'b4beb692668e0d028ca4ec6146a14ecb2b93d934',
}
OLD_PIP="      - run: python -m pip install 'pandas==2.3.3' 'numpy>=1.26,<3' 'requests>=2.31,<3' 'openpyxl>=3.1.2,<4'"
H244_PIP=OLD_PIP+" 'lxml>=5.4,<7' 'beautifulsoup4>=4.12,<5' 'html5lib>=1.1,<2'\n      - run: python -c \"import lxml.etree, bs4, html5lib\""
PIP=OLD_PIP+" 'beautifulsoup4>=4.12,<5' 'lxml>=5.4,<7' 'html5lib>=1.1,<2'\n      - name: H247 verify shared HTML and report dependencies\n        run: python -c \"import pandas, numpy, requests, openpyxl, bs4, lxml.etree, html5lib; print('H247 dependency imports OK')\""
# Foreign aggregate MUST remain last: H221/H225 use the existing exclusive-field slice.
PATCHES={
 'godpick_h124_ownership_flow.py':[
 (" 'code':('證券代號','股票代號','SecuritiesCode','代號'),", " 'code':('證券代號','股票代號','SecuritiesCode','代號','SecuritiesCompanyCode'),"),
 ("'外資及陸資不含外資自營商買賣超股數','外資及陸資買賣超股數'),", "'外資及陸資不含外資自營商買賣超股數','Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Difference','外資及陸資買賣超股數'),"),
 (" 'trust':('投信買賣超股數','投信淨買賣超股數','投信淨買股數'),", " 'trust':('投信買賣超股數','投信淨買賣超股數','投信淨買股數','SecuritiesInvestmentTrustCompanies-Difference'),"),
 (" 'total':('三大法人買賣超股數合計','三大法人買賣超股數'),", " 'total':('三大法人買賣超股數合計','三大法人買賣超股數','TotalDifference'),"),
 ],
 'godpick_h222_financial_history.py':[
 ("    for kind,market,p in scope:\n        path=partition(root,kind,market,p);key=f'{kind}:{market}:{p}';row=", "    # H247: never-attempted partitions first; otherwise oldest attempt first.\n    # Budget, per-market failure cap and existing 12-hour cooldown stay intact.\n    from godpick_h247_source_health import fair_scope\n    for kind,market,p in fair_scope(scope, attempts, now=now, parse_time=fin.moment):\n        path=partition(root,kind,market,p);key=f'{kind}:{market}:{p}';row="),
 ("        if requests_count>=budget or failures[kind,market]>=2 or (prev and now-prev<timedelta(hours=12) and not schema_retry):\n            row['status']='PENDING';continue", "        if requests_count>=budget or failures[kind,market]>=2 or (prev and now-prev<timedelta(hours=12) and not schema_retry):\n            row['status']='PENDING'\n            row['pending_reason']=('REQUEST_BUDGET' if requests_count>=budget else 'SOURCE_FAILURE_CAP' if failures[kind,market]>=2 else 'COOLDOWN_12H')\n            row['last_error']=str(attempts.get(key,{}).get('error',''))[:200]\n            continue"),
 ],
 'tools/h206_daily_review.py':[
 ("print(b['status'],b['day'],len(b['rows']));return 0", "print(b['status'],b['day'],len(b['rows']));print(json.dumps(health,ensure_ascii=False,default=str));return 0"),
 ],
 WF+'h195_daily_command.yml':[(OLD_PIP,PIP)],
 WF+'h206_market_gap.yml':[(OLD_PIP,PIP),("    - cron: '15 1 * * *'", "    - cron: '15 1 * * *'\n    - cron: '5 14 * * 1-5' # H247 Taipei 22:05, after evening H190 collection starts")],
 WF+'h219_review.yml':[(OLD_PIP,PIP)],
 WF+'h220_review.yml':[(OLD_PIP,PIP)],
 WF+'h221_official_chips.yml':[("    - cron: '40 12 * * 1-5' # Taipei 20:40", "    - cron: '40 12 * * 1-5' # Taipei 20:40\n    - cron: '15 14 * * 1-5' # H247 Taipei 22:15: bounded current-date retry"), ("      - 'godpick_h221_evidence_closure.py'", "      - 'godpick_h221_evidence_closure.py'\n      - 'godpick_h124_ownership_flow.py'")],
 WF+'h222_financial_history.yml':[("      - 'godpick_h222_financial_history.py'", "      - 'godpick_h222_financial_history.py'\n      - 'godpick_h247_source_health.py'")],
 WF+'h224_growth_resonance.yml':[(OLD_PIP+" 'beautifulsoup4>=4.12,<5' 'lxml>=5.4,<7'",PIP)],
}

# Accept exact LF or exact raw git blob only; no blind anchor-only version upgrades.
def blob(b): return hashlib.sha1(f'blob {len(b)}\0'.encode()+b).hexdigest()
def source(root,name):
    b=(root/name).read_bytes(); normalized=b.replace(b'\r\n',b'\n')
    if BASE[name] in {blob(b),blob(normalized)}: return normalized.decode('utf-8-sig')
    # Earlier H244 can already have expanded the same two dependency steps.
    # Reverse only that exact verified edit; reject all other changes.
    if name in (WF+'h195_daily_command.yml',WF+'h220_review.yml'):
        s=normalized.decode('utf-8-sig')
        if s.count(H244_PIP)==1:
            reverted=s.replace(H244_PIP,OLD_PIP,1)
            if BASE[name] in {blob(reverted.encode()),blob(b'\xef\xbb\xbf'+reverted.encode())}:return reverted
    raise ValueError(f'{name}: Git blob differs from audited base')
def transform(name,s):
    for old,new in PATCHES[name]:
        if s.count(old)!=1: raise ValueError(f'{name}: missing/duplicate patch anchor')
        s=s.replace(old,new,1)
    if name.endswith('.py'): ast.parse(s,filename=name)
    else:
        import yaml
        doc=yaml.safe_load(s)
        if not isinstance(doc,dict) or not doc.get('jobs') or not ('on' in doc or True in doc): raise ValueError(f'{name}: invalid Actions YAML')
    return s

def create(root,out):
    if not (root/'godpick_h245_list_authority.py').is_file() or not (root/'godpick_h246_seal_consistency.py').is_file():
        raise ValueError('H245/H246 prerequisites missing; install earlier overlays first')
    task=(root/'godpick_auto_update_tasks.py').read_text('utf-8-sig')
    if 'run_check(result, BASE_DIR)' not in task: raise ValueError('H246 scheduler integration missing')
    payload={name:transform(name,source(root,name)).encode('utf-8') for name in PATCHES}
    for name in ('godpick_h247_source_health.py','tests/test_h247_source_contract.py','H247_README.txt','H247_TEST_RESULTS.txt'):
        payload[name]=(ROOT/name).read_bytes()
        if name.endswith('.py'):ast.parse(payload[name],filename=name)
    payload['H247_SHA256SUMS.txt']=''.join(f'{hashlib.sha256(v).hexdigest()}  {k}\n' for k,v in payload.items()).encode()
    tmp=out.with_suffix('.tmp')
    with zipfile.ZipFile(tmp,'w',zipfile.ZIP_DEFLATED) as z:
        for k,v in payload.items():z.writestr(k,v)
    with zipfile.ZipFile(tmp) as z:
        if z.testzip():raise ValueError('ZIP integrity check failed')
    tmp.replace(out)
    print(f'[PASS] {len(PATCHES)} audited files, {sum(map(len,PATCHES.values()))} unique anchors, Python AST, Actions YAML, ZIP');print(out)

def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('project',type=Path);a=ap.parse_args()
    try:create(a.project.resolve(),ROOT/'StockApp_V191_H247_Overlay.zip')
    except Exception as e:raise SystemExit('[BLOCKED] '+str(e))
if __name__=='__main__':main()

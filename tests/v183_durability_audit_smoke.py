# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import tempfile,json,sys
ROOT=Path(__file__).resolve().parents[1]; sys.path.insert(0,str(ROOT))
import godpick_durability_service as d

def main():
    with tempfile.TemporaryDirectory() as td:
        base=Path(td); payload={'x':1,'rows':[1,2,3]}
        (base/'market_snapshot.json').write_text(json.dumps(payload),encoding='utf-8')
        h=d._hash(payload)
        box={'market_snapshot.json':{'status':'success','payload_hash':h}}
        (base/d.OUTBOX_FILE).write_text(json.dumps(box),encoding='utf-8')
        a=d.audit_core_durability(base_dir=base,write_audit=False)
        r=next(x for x in a['rows'] if x['file']=='market_snapshot.json')
        assert r['status']=='REMOTE_CONFIRMED' and r['remote_confirmed'] is True
        # Mutating local data invalidates the old remote confirmation immediately.
        (base/'market_snapshot.json').write_text(json.dumps({'x':2}),encoding='utf-8')
        a2=d.audit_core_durability(base_dir=base,write_audit=False)
        r2=next(x for x in a2['rows'] if x['file']=='market_snapshot.json')
        assert r2['remote_confirmed'] is False and r2['status']!='REMOTE_CONFIRMED'
    print('PASS V183 durability audit｜local file existence is not misreported as remote permanent')
if __name__=='__main__': main()

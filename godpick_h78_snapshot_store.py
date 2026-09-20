"""Append-only, content-addressed evidence snapshots for later offline training.

This is supplementary research storage; it does not replace existing app records.
Capture time is real UTC time, never backdated to the quoted market date.
"""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import sqlite3
from godpick_h78_decision_engine import VERSION

def capture_snapshot(frame, db_path=None):
    if frame is None or frame.empty:
        return {'status':'EMPTY','rows':0}
    if frame.columns.duplicated().any():raise ValueError('Duplicate snapshot columns')
    db_path=Path(db_path) if db_path else Path(__file__).resolve().parent/'data'/'h78_research.sqlite3'
    db_path.parent.mkdir(parents=True,exist_ok=True)
    payload=frame.to_json(orient='records',force_ascii=False,date_format='iso')
    run_id=hashlib.sha256((VERSION+'\n'+payload).encode()).hexdigest()
    now=datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path,timeout=15) as con:
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('CREATE TABLE IF NOT EXISTS h78_snapshots (run_id TEXT PRIMARY KEY, captured_at TEXT NOT NULL, engine_version TEXT NOT NULL, row_count INTEGER NOT NULL, payload_json TEXT NOT NULL)')
        cur=con.execute('INSERT OR IGNORE INTO h78_snapshots VALUES (?,?,?,?,?)',(run_id,now,VERSION,len(frame),payload))
        first=con.execute('SELECT captured_at FROM h78_snapshots WHERE run_id=?',(run_id,)).fetchone()[0]
    return {'status':'SAVED' if cur.rowcount else 'UNCHANGED','run_id':run_id,'rows':len(frame),'captured_at':first}

def export_snapshots(db_path):
    path=Path(db_path)
    if not path.is_file():raise FileNotFoundError(path)
    with sqlite3.connect('file:'+path.resolve().as_posix()+'?mode=ro',uri=True) as con:
        return [{'run_id':r[0],'captured_at':r[1],'version':r[2],'rows':r[3],'records':json.loads(r[4])}
                for r in con.execute('SELECT * FROM h78_snapshots ORDER BY captured_at,run_id')]

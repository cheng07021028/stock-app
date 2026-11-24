#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""H196 read-only deployment evidence. No writes to production data, no network."""
import argparse, json, sys
from datetime import datetime, timedelta, time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import godpick_h190_market_archive as ar

REQUIRED={
 'H190':['godpick_h190_market_archive.py','tools/h190_market_archive.py','.github/workflows/h190_market_archive.yml'],
 'H191':['godpick_h191_opportunity_engine.py','pages/29_主流趨勢與潛力黑馬.py'],
 'H192':['godpick_h192_financial_growth.py','tools/h192_financial_collect.py','.github/workflows/h192_financial_archive.yml','pages/30_財報成長與合理估值.py'],
 'H193':['godpick_h193_joint_decision.py','tools/h193_daily_review.py','.github/workflows/h193_daily_review.yml','pages/31_十二研究員聯合決策.py'],
 'H194':['godpick_h194_performance.py','tools/h194_daily_learning.py','.github/workflows/h194_daily_learning.yml','pages/32_隔日預測與AI績效學習.py'],
 'H195':['godpick_h195_command_center.py','tools/h195_daily_command.py','.github/workflows/h195_daily_command.yml','pages/33_超級AI股神決策總指揮中心.py','pages/7_股神推薦.py'],
}
def audit(root=ROOT,now=None):
    root=Path(root);now=now or datetime.now(ar.TZ)
    if now.tzinfo is None:raise ValueError('timezone required')
    now=now.astimezone(ar.TZ); issues=[];warnings=[]
    deployment={k:[p for p in paths if not (root/p).is_file()] for k,paths in REQUIRED.items()}
    for k,missing in deployment.items():
        if missing:issues.append(k+'_DEPLOYMENT_MISSING')
    aroot=root/'data/h190'; ar._invalidate(aroot)
    summary=ar.inspect(aroot);cal,markets,rows,errors=ar._index(aroot)
    cutoff=(now.date() if now.time()>=time(18,15) else now.date()-timedelta(days=1)).isoformat()
    days=sorted({d for v in cal.values() for d in v if d<=cutoff});last=days[-1] if days else None
    calendar_current=False
    try:
        h=ar._read(aroot/'health.json');obs=datetime.fromisoformat(h['observed_at']).astimezone(ar.TZ)
        calendar_current=(h['version']==ar.VERSION and obs<=now and (obs.date().isoformat()>cutoff or (obs.date().isoformat()==cutoff and obs.time()>=time(18,15))) and cutoff[:7] not in h.get('calendar_missing',[]) and 'calendar:'+cutoff[:7] not in h.get('errors',{}) and h.get('latest_calendar_session')==last)
    except Exception as e:h={'error':type(e).__name__+': '+str(e)[:100]}
    if not calendar_current or not last:issues.append('CALENDAR_NOT_CURRENT')
    # A weekday not yet represented in official data is a gap, not a proven holiday.
    if last!=cutoff and datetime.fromisoformat(cutoff).weekday()<5:issues.append('WEEKDAY_SESSION_NOT_CONFIRMED')
    try:
        from godpick_h191_opportunity_engine import master_map, calc_features
        master=master_map(json.loads((root/'stock_master_cache.json').read_text('utf-8-sig')))
    except Exception as e:master={};issues.append('STOCK_MASTER_UNAVAILABLE')
    target=set(days[-60:]);coverage={}
    for m in ar.URLS:
        codes={c for c,r in master.items() if r['市場']==m}
        complete={c for c in codes if len(target)==60 and len({r[0] for r in rows.get((m,c),[]) if r[0] in target})==60}
        good=set(); invalid={}
        for c in complete:
            feature,why=calc_features(rows.get((m,c),[]),sorted(target))
            if feature is not None:good.add(c)
            else:invalid[c]=why
        ratio=len(good)/len(codes) if codes else None
        coverage[m]={'master_codes':len(codes),'complete_60':len(complete),'valid_60':len(good),'ratio':ratio,'missing_60_codes':sorted(codes-complete),'invalid_60_codes':invalid,'latest_day_present':bool(last and last in markets[m]),'recent_60_days':len(target & set(markets[m]))}
        if ratio is None or ratio<.95:issues.append(m+'_COVERAGE_BELOW_95_PERCENT')
        if not coverage[m]['latest_day_present']:issues.append(m+'_LATEST_DAY_MISSING')
    if errors:issues.append('H190_ARCHIVE_READ_ERRORS')
    if len(target)==60 and any(m not in cal for m in ar._months(datetime.fromisoformat(min(target)).date(),datetime.fromisoformat(max(target)).date())):issues.append('H190_CALENDAR_MONTH_GAP')
    if not summary['target_250_archived_sessions_ready']:warnings.append('H190_250_DAY_BACKFILL_PENDING')
    feeds={}
    try:
        import godpick_h192_financial_growth as f
        from datetime import date
        expm,expq=f.expected_periods(now)
        for kind,m in f.URLS:
            key=kind+'_'+m;codes={c for c,r in master.items() if r['市場']==m};rowsf=f.read_feed(root,kind,m)
            eligible=set()
            for r in rowsf:
                # Require actual first-observed evidence and exact expected period/day.
                valid_row=f.canonical(kind,r,now)
                if valid_row is None or valid_row['source']!=m:continue
                period=valid_row['_period']
                if period==(last if kind=='pe' else expm if kind=='revenue' else expq):eligible.add(valid_row['code'])
            valid=len(eligible & codes);ratio=valid/len(codes) if codes else None
            feeds[key]={'stored':len(rowsf),'current_master_codes':valid,'master_codes':len(codes),'ratio':ratio}
            # Positive PE and general-industry margins are not applicable to every company.
            if ratio is None or not rowsf or valid<f.MIN_ROWS[m]:issues.append('H192_'+key+'_SOURCE_COVERAGE_INSUFFICIENT')
            elif ratio<.95:
                (issues if kind=='revenue' else warnings).append('H192_'+key+'_BELOW_95_PERCENT')
    except Exception as e:issues.append('H192_READ_ERROR');feeds['error']=type(e).__name__+': '+str(e)[:150]
    performance={}
    try:
        import godpick_h194_performance as p
        ss,oo=p.read_ledger(root);performance={'signals':len(ss),'outcomes':len(oo),'research_days':len({s['signal_day'] for s in ss.values()})}
        if not ss:issues.append('H194_NO_PROSPECTIVE_SIGNALS')
        elif not oo:warnings.append('H194_OUTCOMES_NOT_YET_AVAILABLE')
    except Exception as e:issues.append('H194_LEDGER_ERROR');performance={'error':str(e)[:150]}
    command={}
    try:
        import godpick_h195_command_center as c
        b,status,_=c.load(root,now);command={'status':status,'asof':b.get('asof'),'market_day':b.get('market_day'),'counts':b.get('counts',{}),'rows':len(b.get('rows',[]))}
        if status!='CURRENT' or b.get('expected_session')!=last:issues.append('H195_SNAPSHOT_NOT_CURRENT')
        if status=='CURRENT' and not command['rows']:warnings.append('H195_NO_NAMED_RESEARCH_CHECK_FUNNEL')
        if b.get('errors'):issues.append('H195_SOURCE_ERRORS')
    except Exception as e:issues.append('H195_READ_ERROR');command={'error':str(e)[:150]}
    return {'version':'V191-H196','checked_at':now.isoformat(),'status':'BLOCKED' if issues else 'PARTIAL' if warnings else 'DATA_ACCEPTANCE_PASSED','issues':issues,'warnings':warnings,'deployment_missing':deployment,'expected_session':last,'calendar_current':calendar_current,'market_archive':summary,'collector_health':h,'coverage60':coverage,'financial_feeds':feeds,'performance':performance,'command':command,'trade_authority_changed':False,'note':'Data acceptance only; not verified trading results or cloud UI speed. Ordinary four-digit master codes; new listings/suspensions remain explicit gaps.'}

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--out',type=Path);a=p.parse_args();r=audit(a.root)
    blob=json.dumps(r,ensure_ascii=False,indent=2)
    if a.out:a.out.parent.mkdir(parents=True,exist_ok=True);a.out.write_text(blob,encoding='utf-8')
    print(json.dumps({k:r[k] for k in ('status','checked_at','issues','warnings','expected_session','command')},ensure_ascii=False,indent=2));return 0 if r['status']=='DATA_ACCEPTANCE_PASSED' else 2
if __name__=='__main__':raise SystemExit(main())

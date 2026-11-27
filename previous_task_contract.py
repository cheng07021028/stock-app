import hashlib
BASE='1cfe096d955a82ab7e8aff0cf3715eb82cfd8610'
TASK='godpick_auto_update_tasks.py'
p245=[('    latest = latest if isinstance(latest, dict) else {}\n    rows = _extract_named_rows_v191_h7(latest)',
  '    latest = latest if isinstance(latest, dict) else {}\n'
  '    # H245: an empty current list is not permission to resurrect historical rows.\n'
  '    # Validate publication pointers before even considering the legacy H7 fallback.\n'
  '    if payload == []:\n'
  '        from godpick_h245_list_authority import audit_empty_list, PREFIX\n'
  '        from godpick_persistence_service import (\n'
  '            read_github_json, ensure_records_local_authority_current,\n'
  '            records_authority_verification_h240,\n'
  '        )\n'
  '        def _h245_verified_history():\n'
  '            history, _, _ = ensure_records_local_authority_current()\n'
  '            return history if records_authority_verification_h240().get("status") == "VERIFIED" else None\n'
  '        audit = audit_empty_list(payload, latest, read_github_json, _h245_verified_history)\n'
  '        msgs.append(PREFIX + audit["code"] + "｜" + audit["detail"] + f"｜07快照 {audit[\'snapshot_rows\']} 筆")\n'
  '        return pd.DataFrame(), msgs\n'
  '    rows = _extract_named_rows_v191_h7(latest)'),
 ('def _h243_identity_guard(df, msgs, started):\n    from godpick_h243_tracking_evidence import eligible',
  'def _h243_identity_guard(df, msgs, started):\n'
  '    from godpick_h245_list_authority import PREFIX\n'
  '    authority = next((m for m in reversed(msgs) if isinstance(m, str) and m.startswith(PREFIX)), "")\n'
  '    if df.empty and authority:\n'
  '        code = authority[len(PREFIX):].split("｜", 1)[0]\n'
  '        result = _report(True, authority + "；未計算、未同步、不變更08歷史",\n'
  '            details={"outcome": code, "load_messages": msgs[-10:]},\n'
  '            started=started, warning=(code != "NO_CURRENT_LIST"))\n'
  '        # Missing evidence is not a transient provider failure. Retry at next slot.\n'
  '        result["retry_pending"] = False\n'
  '        return result\n'
  '    from godpick_h243_tracking_evidence import eligible'),
 ('        df,msgs=_load_recommend_list()\n        from godpick_h243_tracking_evidence import missing_ids, repair',
  '        df,msgs=_load_recommend_list()\n'
  '        if df.empty:\n'
  '            blocked=_h243_identity_guard(df,msgs,t0)\n'
  '            if blocked is not None:return blocked\n'
  '        from godpick_h243_tracking_evidence import missing_ids, repair')]
p246={'godpick_auto_update_tasks.py': [('        # H138: scheduled Page07 already runs headlessly.',
                                   '        # H246: exact same-run remote payload, anchor, list and H241 history.\n'
                                   '        # Only the central worker can atomically repair an exact full snapshot.\n'
                                   '        try:\n'
                                   '            from godpick_h246_seal_consistency import run_check\n'
                                   '            result["h246_consistency"] = run_check(result, BASE_DIR)\n'
                                   '        except Exception as _h246_exc:\n'
                                   '            result["h246_consistency"] = {"status": "H246_EXCEPTION", "confirmed": False,\n'
                                   '                                           "error_type": type(_h246_exc).__name__}\n'
                                   '        # H138: scheduled Page07 already runs headlessly.'),
                                  ('            msg += "｜H138主管決策遠端封存尚未確認（與Page07掃描成功分開）"',
                                   '            msg += "｜H138主管決策遠端封存尚未確認（與Page07掃描成功分開）" + "｜" + '
                                   'str(result["h138_auto_seal"].get("eod_status") or result["h138_auto_seal"].get("status"))\n'
                                   '        if ok:\n'
                                   '            msg += "｜H246推薦遠端一致性：" + str(result["h246_consistency"].get("status"))'),
                                  ('warning=bool(result.get("warning") or (ok and not result["h138_auto_seal"].get("confirmed")))',
                                   'warning=bool(result.get("warning") or (ok and (not result["h138_auto_seal"].get("confirmed") or not '
                                   'result["h246_consistency"].get("confirmed"))))')]}

def blob(b):return hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()
def recover_task(raw):
    for stages in ([p245,p246[TASK]],[p245]):
        try:
            t=raw.decode('utf-8-sig')
            for pairs in reversed(stages):
                for old,new in reversed(pairs):
                    if t.count(new)!=1:raise ValueError('anchor mismatch')
                    t=t.replace(new,old,1)
            if any(blob(x.encode())==BASE for x in (t,'\ufeff'+t)):return True
        except (ValueError,UnicodeError):pass
    return False

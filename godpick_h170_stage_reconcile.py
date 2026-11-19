# -*- coding: utf-8 -*-
"""H170: reconcile scanner pre-governance shortlist and final operational list.

Both must come from ONE run and one complete governed stock universe. This
module does not add candidates, rerank stocks, change H109/H126 or fetch quotes.
"""
from __future__ import annotations
import re
from collections.abc import Mapping
from typing import Any
import pandas as pd

VERSION="V191-H170-SAME-RUN-FINAL-STAGE-RECONCILIATION"


def _codes(obj: Any) -> set[str]:
    if not isinstance(obj,pd.DataFrame) or "股票代號" not in obj:
        return set()
    return {v for v in (str(x).strip().removesuffix(".0") for x in obj["股票代號"])
            if re.fullmatch(r"[1-9][0-9]{3}",v)}


def _explain(row: Mapping[str,Any]) -> str:
    """Disclose actual existing risk text; never invent a formal veto."""
    fields=("真禁買原因","硬否決原因","實戰過濾狀態","推薦角色",
            "股神實戰建議","H109決策權威","H126能否買進")
    details=[]
    for key in fields:
        v=str(row.get(key) or "").strip()
        if v and v not in {"nan","None","否","--"}:
            details.append(key+"="+v[:90])
    return "；".join(details[:5]) or "母體仍存在，但未提供具體重新篩選原因"


def build_stage_contract(*, scanner_final: Any, governed_final: Any,
                         governed_universe: Any, run_id: str, scan_signature: str) -> dict:
    """After the real post-SuperAI final selector has finished (no extra scan)."""
    pre,post,all_codes=(_codes(scanner_final),_codes(governed_final),_codes(governed_universe))
    removed=sorted(pre-post)
    new=sorted(post-pre)
    disappeared=sorted((pre|post)-all_codes)
    raw_before=len(scanner_final) if isinstance(scanner_final,pd.DataFrame) else 0
    raw_after=len(governed_final) if isinstance(governed_final,pd.DataFrame) else 0
    frame=governed_universe if isinstance(governed_universe,pd.DataFrame) else pd.DataFrame()
    reason={}
    if removed and "股票代號" in frame:
        indexed={}
        for row in frame.to_dict("records"):
            code=str(row.get("股票代號") or "").strip().removesuffix(".0")
            if code in removed:
                indexed[code]=row
        reason={code:_explain(indexed[code]) if code in indexed
                else "已不在治理後母體，不可視為正常篩選" for code in removed[:50]}
    proven=bool(run_id and scan_signature and
                isinstance(scanner_final,pd.DataFrame) and
                isinstance(governed_final,pd.DataFrame) and
                isinstance(governed_universe,pd.DataFrame) and
                "股票代號" in scanner_final and "股票代號" in governed_final and
                "股票代號" in governed_universe and not disappeared and
                len(pre)==raw_before and len(post)==raw_after)
    return {
        "version":VERSION,"stage":"PRE_SUPER_AI_TO_POST_GOVERNANCE",
        "run_id":str(run_id or ""),"scan_signature":str(scan_signature or ""),
        "pre_rows":raw_before,"pre_unique":len(pre),
        "post_rows":raw_after,"post_unique":len(post),
        "pre_codes":sorted(pre)[:300],"post_codes":sorted(post)[:300],
        "removed_codes":removed[:100],"added_codes":new[:100],
        "governed_universe_rows":len(frame),
        "disappeared_from_governed_universe":disappeared[:100],
        "removed_provenance":reason,
        "same_run_stage_reselection_proven":proven,
        "trade_permission_changed":False,
        "note":"不同階段候選不得直接互比；確認代號在同輪母體中，才可解釋重新篩選，H126仍獨立判斷。",
    }


def validate_stage_contract(contract: Any, *,
                            expected_pre: int, actual_final: Any,
                            context_run_id: str, scan_run_id: str,
                            decision_signature: str, active_signature: str) -> dict:
    """Fail closed: do not use a cache or an unrelated scan to repair H145."""
    c=contract if isinstance(contract,Mapping) else {}
    observed=_codes(actual_final)
    allowed=bool(c.get("same_run_stage_reselection_proven") is True and
        c.get("stage")=="PRE_SUPER_AI_TO_POST_GOVERNANCE" and
        context_run_id and scan_run_id and context_run_id==scan_run_id and
        context_run_id==c.get("run_id") and decision_signature and
        active_signature and decision_signature==active_signature and
        active_signature==c.get("scan_signature") and
        expected_pre>0 and c.get("pre_unique")==expected_pre and
        c.get("pre_rows")==expected_pre and
        c.get("post_unique")==len(observed) and
        c.get("post_rows")==len(actual_final) and
        len(observed)==len(actual_final) and
        sorted(observed)==c.get("post_codes") and
        not c.get("disappeared_from_governed_universe") and
        c.get("trade_permission_changed") is False)
    return {
        "stage_verified":allowed,
        "scanner_expected":expected_pre,
        "post_unique":len(observed),
        "removed_codes":c.get("removed_codes",[]) if allowed else [],
        "removed_provenance":c.get("removed_provenance",{}) if allowed else {},
        "result":("RECONCILED_POST_GOVERNANCE_RESELECTION" if allowed else
                  "BLOCKED_UNPROVEN_FINAL_STAGE_DIFFERENCE"),
        "authority_unchanged":True,
    }

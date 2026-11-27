# -*- coding: utf-8 -*-
"""Bounded H222 source scheduling. No network calls or evidence creation."""
from datetime import datetime
from collections.abc import Mapping

VERSION='V191-H247-BOUNDED-SOURCE-RECOVERY'

def fair_scope(scope, attempts, *, now, parse_time):
    """Stable oldest-first rotation. Missing/unparseable attempt => first try.

    Do not remove any target. Preserve original order for ties. Future timestamps
    are placed last; the caller's existing cooldown rejects them. No changes to
    source provenance, request limits or valid-period criteria.
    """
    if not isinstance(now,datetime) or now.tzinfo is None:raise ValueError('aware time required')
    if not isinstance(attempts,Mapping):raise ValueError('invalid attempts ledger')
    def priority(item):
        i,(kind,market,p)=item
        a=attempts.get(f'{kind}:{market}:{p}',{})
        try:t=parse_time(a.get('at')) if isinstance(a,Mapping) else None
        except (ValueError,TypeError,OverflowError):t=None
        if not isinstance(t,datetime) or t.tzinfo is None:return (0,0,i)
        if t>now:return (2,t.timestamp(),i)
        return (1,t.timestamp(),i)
    return [v for _,v in sorted(enumerate(scope),key=priority)]

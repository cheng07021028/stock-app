# -*- coding: utf-8 -*-
"""Apply V191-H29 market breadth-vs-authority risk semantics hotfix.

The candidate breadth engine may legitimately say "red / defensive" when the
candidate universe is weak.  That is a *breadth allocation warning*, not by
itself evidence that the authoritative macro regime is severe/lockdown.

Before H29 the formal engine searched a combined text blob and treated any
occurrence of ``紅燈`` as severe.  A breadth-derived ``紅燈｜防守`` could therefore
hard-block the whole market even when the durable market snapshot was 62.7,
中性偏多, 中低風險 and risk_filter=正常.
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FORMAL = ROOT / "godpick_formal_recommendation_engine.py"
REGIME = ROOT / "market_regime_engine.py"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if new in text:
        return text
    if old not in text:
        raise RuntimeError(f"H29 anchor not found: {label}")
    return text.replace(old, new, 1)


def patch_formal() -> None:
    src = FORMAL.read_text(encoding="utf-8-sig")
    src = src.replace(
        'FORMAL_RECOMMENDATION_VERSION = "vnext_phase10_9_super_ai_perf_cache_v183_20260811"',
        'FORMAL_RECOMMENDATION_VERSION = "v191_h29_market_breadth_semantics_20260815"',
        1,
    )
    old = '''    severe = _contains_any(blob, ["紅燈", "空方", "全面防守", "禁止進攻", "風險急升"])
    defensive = severe or _contains_any(blob, ["防守", "保守", "震盪控風險", "不宜全面追價"])
    panic = _contains_any(blob, ["崩盤", "極端風險", "系統性風險", "禁止所有新倉", "全面停買", "流動性危機", "LOCKDOWN"])
    twse_pct = _num(row, "加權漲跌%", _num(row, "大盤漲跌幅%", _num(row, "加權指數漲跌幅%", 0)))
    otc_pct = _num(row, "櫃買漲跌幅%", _num(row, "OTC漲跌%", _num(row, "上櫃指數漲跌幅%", 0)))
    breadth_pct = _num(row, "市場上漲家數比例%", _num(row, "上漲家數比例%", 50))
    extreme_lockdown = bool(twse_pct <= -3.5 or otc_pct <= -4.5 or (twse_pct <= -2.5 and breadth_pct <= 15))
'''
    new = '''    # V191-H29：把「候選廣度風險」與「權威大盤 severe」分開。
    # market_regime_engine 的紅燈是由候選股廣度/強弱比推導，應用於縮倉、
    # 提高 Entry/RR 與降低追價，不可單靠這個文字把全市場硬封鎖。
    breadth_light = _safe_str(row.get("大盤風險燈號"))
    breadth_conclusion = _safe_str(row.get("今日大盤結論"))
    authority_blob = _text_blob(row, [
        "大盤橋接風控", "大盤策略模式", "大盤策略建議", "大盤風控建議", "大盤橋接狀態",
    ])
    breadth_defensive = bool(
        _contains_any(breadth_light, ["紅燈", "廣度紅燈", "防守"])
        or _contains_any(breadth_conclusion, ["不適合擴大倉位", "防守", "等待確認"])
    )
    explicit_authority_hard = _contains_any(
        authority_blob,
        ["空方", "全面防守", "禁止進攻", "風險急升", "極端風險", "全面停買", "LOCKDOWN"],
    )
    supportive_authority = bool(
        score >= 55
        and _contains_any(authority_blob, ["中性偏多", "偏多", "選股偏多", "正常", "偏多輪動", "輪動"])
        and not explicit_authority_hard
    )

    # 若只拿到舊版「紅燈｜防守」而沒有可佐證的權威分數/偏多橋接，仍維持保守 severe；
    # 但像本輪 62.7 / 中性偏多 / 中低風險，廣度紅燈只能降為 defensive。
    severe = bool(explicit_authority_hard or (breadth_defensive and not supportive_authority and score < 55))
    defensive = bool(
        severe or breadth_defensive
        or _contains_any(authority_blob, ["防守", "保守", "震盪控風險", "不宜全面追價"])
    )
    panic = _contains_any(blob, ["崩盤", "極端風險", "系統性風險", "禁止所有新倉", "全面停買", "流動性危機", "LOCKDOWN"])
    twse_pct = _num(row, "加權漲跌%", _num(row, "大盤漲跌幅%", _num(row, "加權指數漲跌幅%", 0)))
    otc_pct = _num(row, "櫃買漲跌幅%", _num(row, "OTC漲跌%", _num(row, "上櫃指數漲跌幅%", 0)))
    breadth_pct = _num(row, "市場上漲家數比例%", _num(row, "上漲家數比例%", 50))
    extreme_lockdown = bool(twse_pct <= -3.5 or otc_pct <= -4.5 or (twse_pct <= -2.5 and breadth_pct <= 15))
'''
    src = replace_once(src, old, new, "formal risk semantics")

    old_level = '''    else:
        level = "極端風險｜全面禁買" if panic else "紅燈｜只准條件逆勢" if severe else "防守｜縮小倉位" if defensive else "一般｜依個股條件"
    return {
'''
    new_level = '''    else:
        if panic:
            level = "極端風險｜全面禁買"
        elif severe:
            level = "紅燈｜權威風險佐證，僅准條件逆勢"
        elif defensive and breadth_defensive and supportive_authority:
            level = "黃燈｜廣度偏弱但權威大盤可選股，縮倉精選"
        elif defensive:
            level = "防守｜縮小倉位"
        else:
            level = "一般｜依個股條件"
    return {
'''
    src = replace_once(src, old_level, new_level, "formal risk level")

    old_return = '''        "raw_severe": raw_severe,
        "raw_panic": raw_panic,
        "lockdown": bool(panic),
'''
    new_return = '''        "raw_severe": raw_severe,
        "raw_panic": raw_panic,
        "breadth_defensive": breadth_defensive,
        "supportive_authority": supportive_authority,
        "explicit_authority_hard": explicit_authority_hard,
        "market_semantic_guard": "V191-H29｜廣度防守與權威大盤分離",
        "lockdown": bool(panic),
'''
    src = replace_once(src, old_return, new_return, "formal diagnostics")
    FORMAL.write_text(src, encoding="utf-8")


def patch_regime() -> None:
    src = REGIME.read_text(encoding="utf-8-sig")
    src = src.replace(
        'MARKET_REGIME_VERSION = "phase6_market_leader_replay_20260612"',
        'MARKET_REGIME_VERSION = "v191_h29_breadth_defensive_semantics_20260815"',
        1,
    )
    src = replace_once(
        src,
        '''        mode = "防守盤"\n        conclusion = "不適合擴大倉位；以等突破與風控為主。"\n        light = "紅燈｜防守"''',
        '''        mode = "防守盤"\n        conclusion = "候選廣度偏弱，不適合擴大倉位；以等突破、縮倉與風控為主。"\n        light = "廣度紅燈｜防守"''',
        "breadth red label",
    )
    REGIME.write_text(src, encoding="utf-8")


def main() -> None:
    patch_formal()
    patch_regime()
    print("PASS V191-H29 market breadth semantics patch applied")


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAGE7 = ROOT / "pages" / "7_股神推薦.py"
AUTO = ROOT / "godpick_auto_update_tasks.py"
TRUTH = ROOT / "godpick_t1_trade_truth.py"
GUIDE = ROOT / "godpick_super_ai_excel_guide.py"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if new in text:
        return text
    if old not in text:
        raise RuntimeError(f"H32 anchor not found: {label}")
    return text.replace(old, new, 1)


def patch_page7() -> None:
    src = PAGE7.read_text(encoding="utf-8-sig")
    old = '        _save_recommend_result_to_state(rec_df, category_strength_df, hot_pick_df)\n'
    new = '''        # V191-H32：在正式分區完成後加上「機率＋報酬區間」預測層。\n        # 此層只做預測/驗證，不得改 Formal/A-、Entry/Risk/RR 或交易許可。\n        try:\n            from godpick_return_forecast_engine import (\n                apply_return_forecast as _h32_apply_return_forecast,\n                forecast_validation_summary as _h32_forecast_validation_summary,\n                FORECAST_COLUMNS as _H32_FORECAST_COLUMNS,\n            )\n            rec_df = _h32_apply_return_forecast(rec_df)\n            _h32_candidate = st.session_state.get(_k("candidate_diagnosis_store"))\n            if isinstance(_h32_candidate, pd.DataFrame) and not _h32_candidate.empty:\n                _h32_candidate = _h32_apply_return_forecast(_h32_candidate)\n                st.session_state[_k("candidate_diagnosis_store")] = _h32_candidate\n            for _h32_col in _H32_FORECAST_COLUMNS:\n                if _h32_col not in GODPICK_RECORD_COLUMNS:\n                    GODPICK_RECORD_COLUMNS.append(_h32_col)\n            st.session_state[_k("h32_return_forecast_validation")] = _h32_forecast_validation_summary()\n        except Exception as _h32_forecast_exc:\n            st.session_state[_k("h32_return_forecast_validation")] = {\n                "status": f"H32報酬預測建立失敗：{type(_h32_forecast_exc).__name__}: {_h32_forecast_exc}",\n                "samples": 0,\n            }\n        _save_recommend_result_to_state(rec_df, category_strength_df, hot_pick_df)\n'''
    src = replace_once(src, old, new, "Page07 forecast before persistence")

    old_ui = '''    _truth_async_msg = _safe_str(st.session_state.get(_k("v188_t1_truth_async_message")))\n    if _truth_async_msg:\n        st.caption(f"V188 T+1實戰真相：{_truth_async_msg}")\n\n'''
    new_ui = '''    _truth_async_msg = _safe_str(st.session_state.get(_k("v188_t1_truth_async_message")))\n    if _truth_async_msg:\n        st.caption(f"V188 T+1實戰真相：{_truth_async_msg}")\n\n    # V191-H32：明確把「90%區間覆蓋」與「方向/點預測準確率」分開。\n    try:\n        from godpick_return_forecast_engine import forecast_validation_summary as _h32_summary_fn\n        _h32_summary = st.session_state.get(_k("h32_return_forecast_validation")) or _h32_summary_fn()\n    except Exception:\n        _h32_summary = {}\n    if isinstance(_h32_summary, dict):\n        with st.expander("H32｜隔日/波段報酬預測與90%校準驗證", expanded=False):\n            _h32_n = int(_h32_summary.get("interval_samples") or _h32_summary.get("samples") or 0)\n            _h32_cov = _h32_summary.get("interval_coverage_pct")\n            _h32_dir = _h32_summary.get("direction_hit_rate_pct")\n            _h32_mae = _h32_summary.get("mae_pct")\n            render_pro_kpi_row([\n                {"label": "成熟驗證樣本", "value": _h32_n, "delta": "走勢外真相樣本", "delta_class": "pro-kpi-delta-flat"},\n                {"label": "90%區間覆蓋率", "value": f"{float(_h32_cov):.1f}%" if _h32_cov is not None else "待累積", "delta": "目標≥90%，不等於命中率", "delta_class": "pro-kpi-delta-flat"},\n                {"label": "隔日方向命中率", "value": f"{float(_h32_dir):.1f}%" if _h32_dir is not None else "待累積", "delta": "獨立追蹤，不保證90%", "delta_class": "pro-kpi-delta-flat"},\n                {"label": "隔日MAE", "value": f"{float(_h32_mae):.2f}%" if _h32_mae is not None else "待累積", "delta": "點預測平均絕對誤差", "delta_class": "pro-kpi-delta-flat"},\n            ])\n            st.caption(str(_h32_summary.get("status") or "H32 尚無足夠成熟樣本，不得宣稱90%準確率。"))\n            st.caption("H32輸出：隔日預估漲跌幅＋90%區間、5/10/20日預估報酬區間；10日作為『後續波段』摘要。Formal/A-與買進許可仍由原正式風控引擎決定。")\n\n'''
    src = replace_once(src, old_ui, new_ui, "Page07 H32 validation panel")
    PAGE7.write_text(src, encoding="utf-8")


def patch_auto_task() -> None:
    src = AUTO.read_text(encoding="utf-8-sig")
    old = '        res=refresh_t1_trade_truth(max_records=int(cfg.get("max_records",500) or 500),max_workers=int(cfg.get("max_workers",8) or 8),persist=True)\n'
    new = '''        # V191-H32：authority_rows 已經通過 Page08 權威選舉/歷史救援，必須直接\n        # 傳給 truth service；不得再讓它回頭讀 fresh runner 上的空 godpick_records.json。\n        res=refresh_t1_trade_truth(records=authority_rows,max_records=int(cfg.get("max_records",500) or 500),max_workers=int(cfg.get("max_workers",8) or 8),persist=True)\n'''
    src = replace_once(src, old, new, "T1 task authority rows")
    AUTO.write_text(src, encoding="utf-8")


def patch_truth() -> None:
    src = TRUTH.read_text(encoding="utf-8-sig")
    old_pre = '    code = _s(original.get("股票代號") or original.get("代號"))\n    return {\n'
    new_pre = '''    code = _s(original.get("股票代號") or original.get("代號"))\n\n    # V191-H32：把發布當下的報酬預測與後續真實績效放進同一筆不可變真相。\n    _h32_pred1 = _f(original.get("H32隔日預估漲跌幅%"))\n    _h32_low1 = _f(original.get("H32隔日90%區間下緣%"))\n    _h32_high1 = _f(original.get("H32隔日90%區間上緣%"))\n    _h32_actual = {\n        1: cand_ret,\n        5: _f(updated.get("推薦後5日%")),\n        10: _f(updated.get("推薦後10日%")),\n        20: _f(updated.get("推薦後20日%")),\n    }\n    def _h32_inside(actual, low, high):\n        if actual is None or low is None or high is None:\n            return None\n        return bool(min(float(low), float(high)) <= float(actual) <= max(float(low), float(high)))\n    def _h32_direction_hit(actual, pred):\n        if actual is None or pred is None:\n            return None\n        if abs(float(actual)) < 1e-12 and abs(float(pred)) < 0.15:\n            return True\n        return bool((float(actual) > 0) == (float(pred) > 0))\n\n    return {\n'''
    src = replace_once(src, old_pre, new_pre, "T1 H32 precompute")

    old_dict = '        "SuperAI Trade等級": _s(original.get("SuperAI Trade等級")),\n        "V188交易許可": _s(original.get("V188交易許可")),\n'
    new_dict = '''        "SuperAI Trade等級": _s(original.get("SuperAI Trade等級")),\n        "H32隔日預估漲跌幅%": _h32_pred1,\n        "H32隔日90%區間下緣%": _h32_low1,\n        "H32隔日90%區間上緣%": _h32_high1,\n        "H32隔日方向預測命中": _h32_direction_hit(_h32_actual[1], _h32_pred1),\n        "H32隔日90%區間命中": _h32_inside(_h32_actual[1], _h32_low1, _h32_high1),\n        "H32_5日預估報酬%": _f(original.get("H32_5日預估報酬%")),\n        "H32_5日90%區間下緣%": _f(original.get("H32_5日90%區間下緣%")),\n        "H32_5日90%區間上緣%": _f(original.get("H32_5日90%區間上緣%")),\n        "H32_10日預估報酬%": _f(original.get("H32_10日預估報酬%")),\n        "H32_10日90%區間下緣%": _f(original.get("H32_10日90%區間下緣%")),\n        "H32_10日90%區間上緣%": _f(original.get("H32_10日90%區間上緣%")),\n        "H32_20日預估報酬%": _f(original.get("H32_20日預估報酬%")),\n        "H32_20日90%區間下緣%": _f(original.get("H32_20日90%區間下緣%")),\n        "H32_20日90%區間上緣%": _f(original.get("H32_20日90%區間上緣%")),\n        "H32實際5日報酬%": _h32_actual[5],\n        "H32實際10日報酬%": _h32_actual[10],\n        "H32實際20日報酬%": _h32_actual[20],\n        "H32_5日90%區間命中": _h32_inside(_h32_actual[5], _f(original.get("H32_5日90%區間下緣%")), _f(original.get("H32_5日90%區間上緣%"))),\n        "H32_10日90%區間命中": _h32_inside(_h32_actual[10], _f(original.get("H32_10日90%區間下緣%")), _f(original.get("H32_10日90%區間上緣%"))),\n        "H32_20日90%區間命中": _h32_inside(_h32_actual[20], _f(original.get("H32_20日90%區間下緣%")), _f(original.get("H32_20日90%區間上緣%"))),\n        "H32預測版本": _s(original.get("H32預測版本")),\n        "V188交易許可": _s(original.get("V188交易許可")),\n'''
    src = replace_once(src, old_dict, new_dict, "T1 H32 truth fields")
    TRUTH.write_text(src, encoding="utf-8")


def patch_guide() -> None:
    src = GUIDE.read_text(encoding="utf-8-sig")
    old_line = '    next_up = _num_series(work, ["SuperAI校準後隔日上漲機率%", "模型隔日上漲機率%", "隔日上漲機率%", "上漲機率%"], 50.0)\n'
    new_lines = '''    next_up = _num_series(work, ["H32隔日上漲機率%", "SuperAI校準後隔日上漲機率%", "模型隔日上漲機率%", "隔日上漲機率%", "上漲機率%"], 50.0)\n    h32_t1 = _num_series(work, ["H32隔日預估漲跌幅%"], 0.0)\n    h32_t1_low = _num_series(work, ["H32隔日90%區間下緣%"], 0.0)\n    h32_t1_high = _num_series(work, ["H32隔日90%區間上緣%"], 0.0)\n    h32_swing = _num_series(work, ["H32後續波段預估漲幅%", "H32_10日預估報酬%"], 0.0)\n    h32_10_low = _num_series(work, ["H32_10日90%區間下緣%"], 0.0)\n    h32_10_high = _num_series(work, ["H32_10日90%區間上緣%"], 0.0)\n    h32_validation = _text_series(work, ["H32預測驗證狀態"], "未驗證｜不得宣稱90%準確")\n'''
    src = replace_once(src, old_line, new_lines, "H31 guide H32 extraction")

    old_selected = '    selected["隔日上漲機率%"] = next_up.round(1)\n    selected["族群攻擊"] = sector.round(1)\n'
    new_selected = '''    selected["隔日上漲機率%"] = next_up.round(1)\n    selected["隔日預估漲跌幅%"] = h32_t1.round(2)\n    selected["隔日90%區間"] = [f"{lo:+.2f}% ~ {hi:+.2f}%" for lo, hi in zip(h32_t1_low, h32_t1_high)]\n    selected["後續波段預估漲幅%"] = h32_swing.round(2)\n    selected["10日90%區間"] = [f"{lo:+.2f}% ~ {hi:+.2f}%" for lo, hi in zip(h32_10_low, h32_10_high)]\n    selected["報酬預測驗證"] = h32_validation\n    selected["族群攻擊"] = sector.round(1)\n'''
    src = replace_once(src, old_selected, new_selected, "H31 guide H32 display")
    GUIDE.write_text(src, encoding="utf-8")


def main() -> None:
    patch_page7(); patch_auto_task(); patch_truth(); patch_guide()
    print("PASS V191-H32 v2 integration applied")


if __name__ == "__main__":
    main()

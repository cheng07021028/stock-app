# -*- coding: utf-8 -*-
from pathlib import Path

page = Path(__file__).resolve().parents[1] / "pages" / "7_股神推薦.py"
s = page.read_text(encoding="utf-8")
assert "render_pro_kpis(" not in s, "H67.1 NameError regression: render_pro_kpis reintroduced"
assert "apply_h68_execution_learning_truth(_record_source)" in s
assert "H68學習快照建立時間" in s
# Critical cache guard: H68 must be re-applied after Phase93 resolves its cached decision frame.
pos_phase93 = s.index("work = _phase93_single_source_decision_frame(work, work)")
pos_guard = s.index("work = apply_h68_execution_learning_truth(work)", pos_phase93)
pos_partition = s.index('if "正式推薦分區" not in work.columns:', pos_phase93)
assert pos_phase93 < pos_guard < pos_partition
# H68 formal truth must be reconciled back into the visible/exported summary.
assert "reconcile_h68_formal_summary(summary, _h51_source_ui, scan_report, _h63_formal_ui)" in s
assert '"H68執行學習治理"' in s
print("PASS v191_h68_record_snapshot_static_smoke")

# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def patch_verifier_scope() -> None:
    path = ROOT / "tools" / "verify_godpick_scheduler_remote_v191.py"
    src = path.read_text(encoding="utf-8-sig")
    old = 'expected_run_id = str(os.environ.get("GITHUB_RUN_ID") or os.environ.get("GODPICK_WAKEUP_RUN_ID") or "").strip()'
    new = 'expected_run_id = str(os.environ.get("GODPICK_EXPECTED_WAKEUP_RUN_ID") or "").strip()'
    if old not in src:
        if new in src:
            return
        raise SystemExit("H27b verifier expected-run anchor not found")
    src = src.replace(old, new, 1)
    src = src.replace(
        '# H27: a verifier running inside GitHub Actions must confirm THIS workflow\'s',
        '# H27b: only the central scheduler workflow opts into strict current-run verification;',
        1,
    )
    path.write_text(src, encoding="utf-8")


def patch_central_workflow() -> None:
    path = ROOT / ".github" / "workflows" / "godpick_auto_scheduler_v191.yml"
    src = path.read_text(encoding="utf-8-sig")
    old = '''      - name: Verify scheduler heartbeat/history reached runtime-data\n        # Every scheduled task already persists its authority through the same\n        # GitHub CAS / durability services used by Streamlit.  H10 deliberately\n        # removes the old second bulk git-branch publish because it could fail on\n        # untracked runtime files and could overwrite a newer concurrent authority.\n        run: python tools/verify_godpick_scheduler_remote_v191.py\n'''
    new = '''      - name: Verify scheduler heartbeat/history reached runtime-data\n        # Every scheduled task already persists its authority through the same\n        # GitHub CAS / durability services used by Streamlit.  H10 deliberately\n        # removes the old second bulk git-branch publish because it could fail on\n        # untracked runtime files and could overwrite a newer concurrent authority.\n        # H27b explicitly scopes strict current-run verification to this workflow.\n        env:\n          GODPICK_EXPECTED_WAKEUP_RUN_ID: ${{ github.run_id }}\n        run: python tools/verify_godpick_scheduler_remote_v191.py\n'''
    if old not in src:
        if "GODPICK_EXPECTED_WAKEUP_RUN_ID" in src:
            return
        raise SystemExit("H27b central workflow verifier anchor not found")
    path.write_text(src.replace(old, new, 1), encoding="utf-8")


if __name__ == "__main__":
    patch_verifier_scope()
    patch_central_workflow()
    print("V191-H27b verifier scope fix applied/verified")

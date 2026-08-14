from __future__ import annotations
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
WIN = ROOT / "tools" / "windows"
INSTALL = WIN / "Install-GodPickStrictWakeupV191.ps1"
INVOKE = WIN / "Invoke-GodPickStrictWakeupV191.ps1"
WORKFLOW = ROOT / ".github" / "workflows" / "godpick_auto_scheduler_v191.yml"

def check(cond, msg):
    if not cond: raise AssertionError(msg)
    print("PASS", msg)

def main():
    install=INSTALL.read_text(encoding="utf-8-sig")
    invoke=INVOKE.read_text(encoding="utf-8-sig")
    workflow=WORKFLOW.read_text(encoding="utf-8")
    check("Set-Content -LiteralPath $TokenFile" not in install, "H11 newline bug remains eliminated")
    check("WriteAllText" in install, "token remains single-line persisted")
    check("ProtectedData" not in install + invoke, "H12 direct ProtectedData dependency removed from active path")
    check("ConvertFrom-SecureString" in install and "ConvertTo-SecureString -String $encrypted" in invoke, "native PS5.1 DPAPI path retained")
    check("GODPICK_PS_DPAPI_V6:" in install + invoke, "fresh H15 token version used")
    check("wakeup_source:" in workflow and "workflow_dispatch:" in workflow, "workflow accepts strict wake source")
    check('cron: "2-52/10 * * * *"' in workflow, "GitHub off-peak fallback remains enabled")
    print("H12 regression after H15: ALL PASS")

if __name__ == "__main__": main()

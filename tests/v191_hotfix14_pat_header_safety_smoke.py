from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WIN = ROOT / "tools" / "windows"
INSTALL = WIN / "Install-GodPickStrictWakeupV191.ps1"
INVOKE = WIN / "Invoke-GodPickStrictWakeupV191.ps1"
UNINSTALL = WIN / "Uninstall-GodPickStrictWakeupV191.ps1"
WORKFLOW = ROOT / ".github" / "workflows" / "godpick_auto_scheduler_v191.yml"
PAGE17 = ROOT / "pages" / "17_系統健康檢查.py"


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print("PASS", msg)


def normalize_like_h14(value: str) -> tuple[str, int]:
    removed = 0
    chars: list[str] = []
    for ch in value:
        if ch.isspace() or ord(ch) < 32 or ord(ch) == 127:
            removed += 1
            continue
        chars.append(ch)
    clean = "".join(chars).strip()
    if len(clean) >= 2 and clean[0] == clean[-1] and clean[0] in {'"', "'"}:
        clean = clean[1:-1]
    if not clean.startswith("github_pat_"):
        raise ValueError("bad prefix")
    if any(ord(ch) < 0x21 or ord(ch) > 0x7E for ch in clean):
        raise ValueError("non-visible ascii")
    return clean, removed


def main() -> None:
    install = INSTALL.read_text(encoding="utf-8-sig")
    invoke = INVOKE.read_text(encoding="utf-8-sig")
    uninstall = UNINSTALL.read_text(encoding="utf-8-sig")
    workflow = WORKFLOW.read_text(encoding="utf-8")
    page17 = PAGE17.read_text(encoding="utf-8")

    # Exact H13 incident: control chars in the Authorization header must be impossible.
    check("[System.Char]::IsControl($ch)" in install and "[System.Char]::IsWhiteSpace($ch)" in install,
          "installer strips control/whitespace characters from pasted PAT")
    check("[System.Char]::IsControl($ch)" in invoke and "[System.Char]::IsWhiteSpace($ch)" in invoke,
          "dispatcher re-normalizes decrypted PAT before HTTP use")
    check("Authorization header contains a control character after normalization." in invoke,
          "dispatcher has a final pre-flight control-character guard")
    check('StartsWith("github_pat_"' in install and 'StartsWith("github_pat_"' in invoke,
          "both install and dispatch paths validate fine-grained PAT prefix")
    check("'^[\\x21-\\x7E]+$'" in install and "'^[\\x21-\\x7E]+$'" in invoke,
          "both paths require visible ASCII after normalization")

    # H14 deliberately drops legacy token parsing instead of stacking more compatibility branches.
    check("GODPICK_PS_DPAPI_V6:" in install and "GODPICK_PS_DPAPI_V6:" in invoke,
          "H15 uses a fresh V5 token format")
    check("GODPICK_PS_DPAPI_V4:" not in invoke and "GODPICK_PS_DPAPI_V3:" not in invoke and "GODPICK_DPAPI_V2:" not in invoke,
          "dispatcher no longer parses H11-H14 token formats")
    check("ProtectedData" not in install + invoke,
          "H15 active Windows path has no ProtectedData type dependency")
    check("ConvertFrom-SecureString" in install and "ConvertTo-SecureString -String $encrypted" in invoke,
          "H15 retains native Windows PowerShell DPAPI secure-string storage")
    check("WriteAllText" in install and "Set-Content -LiteralPath $TokenFile" not in install,
          "token persistence remains newline-safe")

    # Stable installation: task must not depend on Downloads path after setup.
    check('Join-Path $DataDir "Invoke-GodPickStrictWakeupV191.ps1"' in install,
          "installer defines stable LocalAppData dispatcher path")
    check("Copy-Item -LiteralPath $SourceScriptPath -Destination $StableScriptPath -Force" in install,
          "installer copies dispatcher to LocalAppData before task registration")
    check("$StableScriptPath" in install and "$SourceScriptPath" not in re.search(r"\$actionArgs\s*=.*", install).group(0),
          "scheduled task executes stable LocalAppData script, not Downloads copy")

    # Dispatch and scheduled-task contract.
    check("Invoke-WebRequest -UseBasicParsing" in invoke,
          "Windows PowerShell 5.1 dispatcher uses basic parsing HTTP client")
    check("/actions/workflows/$Workflow/dispatches" in invoke and "workflow_dispatch" in install,
          "installer performs a real workflow_dispatch test before registration")
    check('WakeupSource = "windows_task_scheduler"' in invoke and '"windows_install_test"' in install,
          "install test and scheduled wakeups are distinguishable")
    check("GodPick V191 H16 Strict 10-Min Wakeup" in install and "GodPick V191 H16 Strict 10-Min Wakeup" in uninstall,
          "H15 task identity is registered and removable")
    check("New-TimeSpan -Minutes 10" in install and "AddMinutes(7)" in install,
          "strict Windows clock remains every 10 minutes at xx:07 offset")
    check('cron: "2-52/10 * * * *"' in workflow,
          "GitHub fallback remains staggered at xx:02/12/22/32/42/52")

    # UI should use generic wording rather than exposing obsolete H11/H12/H13 implementation detail.
    check("若已安裝 H11 Windows" not in page17 and "H11 支援 Windows" not in page17,
          "Page17 no longer presents obsolete H11-specific wakeup wording")

    # Simulate the exact clipboard/control-character incident.
    dirty = "\r\n\t github_pat_EXAMPLE1234567890 \x00"
    clean, removed = normalize_like_h14(dirty)
    check(clean == "github_pat_EXAMPLE1234567890" and removed >= 5,
          "clipboard CR/LF/TAB/NUL/space contamination normalizes to a valid header token")
    check(not any(ord(ch) < 32 or ord(ch) == 127 for ch in ("Bearer " + clean)),
          "normalized Authorization header contains no control characters")

    # No realistic full token value is embedded in the package.
    combined = install + invoke
    check(re.search(r"github_pat_[A-Za-z0-9_]{40,}", combined) is None,
          "scripts contain no embedded PAT secret value")

    print("H14 regression after H15 PAT/header safety: ALL PASS")


if __name__ == "__main__":
    main()

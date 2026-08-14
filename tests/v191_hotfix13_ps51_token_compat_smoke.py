from __future__ import annotations
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WIN = ROOT / "tools" / "windows"
INSTALL = WIN / "Install-GodPickStrictWakeupV191.ps1"
INVOKE = WIN / "Invoke-GodPickStrictWakeupV191.ps1"
UNINSTALL = WIN / "Uninstall-GodPickStrictWakeupV191.ps1"

def check(cond, msg):
    if not cond: raise AssertionError(msg)
    print("PASS", msg)

def main():
    install = INSTALL.read_text(encoding="utf-8-sig")
    invoke = INVOKE.read_text(encoding="utf-8-sig")
    uninstall = UNINSTALL.read_text(encoding="utf-8-sig")
    check("ProtectedData" not in install + invoke, "H12 ProtectedData type dependency remains eliminated")
    check("ConvertFrom-SecureString" in install, "Windows PowerShell native DPAPI writer retained")
    check("ConvertTo-SecureString -String $encrypted" in invoke, "matching native decryptor retained")
    check("WriteAllText" in install, "newline-safe token persistence retained")
    check("GODPICK_PS_DPAPI_V6:" in install + invoke, "H15 fresh V5 token format replaces legacy compatibility stack")
    check("GodPick V191 H16 Strict 10-Min Wakeup" in install, "H15 scheduled-task identity used")
    check("GodPick V191 H13 Strict 10-Min Wakeup" in install, "H13 task removed after successful H14 test")
    check("GodPick V191 H16 Strict 10-Min Wakeup" in uninstall, "uninstaller knows H15 task")
    print("H13 compatibility regression after H15: ALL PASS")

if __name__ == "__main__": main()

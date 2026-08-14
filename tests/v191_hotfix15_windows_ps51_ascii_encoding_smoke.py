from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
WIN = ROOT / "tools" / "windows"
INSTALL = WIN / "Install-GodPickStrictWakeupV191.ps1"
INVOKE = WIN / "Invoke-GodPickStrictWakeupV191.ps1"
UNINSTALL = WIN / "Uninstall-GodPickStrictWakeupV191.ps1"
WORKFLOW = ROOT / ".github" / "workflows" / "godpick_auto_scheduler_v191.yml"


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print("PASS", msg)


def lexical_balance(text: str) -> bool:
    # Lightweight PowerShell lexical balance checker. It is not a replacement for
    # the Windows PowerShell parser; it catches unclosed strings/braces introduced
    # by packaging or source transformation.
    stack: list[str] = []
    pairs = {")": "(", "]": "[", "}": "{"}
    i = 0
    quote: str | None = None
    while i < len(text):
        ch = text[i]
        if quote:
            if quote == '"' and ch == '`':
                i += 2
                continue
            if ch == quote:
                # PowerShell single-quoted strings escape a quote by doubling it.
                if quote == "'" and i + 1 < len(text) and text[i + 1] == "'":
                    i += 2
                    continue
                quote = None
            i += 1
            continue
        if ch == '#':
            nl = text.find('\n', i)
            if nl < 0:
                break
            i = nl + 1
            continue
        if ch in {'"', "'"}:
            quote = ch
        elif ch in '([{':
            stack.append(ch)
        elif ch in ')]}':
            if not stack or stack.pop() != pairs[ch]:
                return False
        i += 1
    return quote is None and not stack


def normalize_like_h15(value: str) -> tuple[str, int]:
    removed = 0
    out: list[str] = []
    for ch in value:
        if ch.isspace() or ord(ch) < 32 or ord(ch) == 127:
            removed += 1
            continue
        out.append(ch)
    clean = ''.join(out).strip()
    if len(clean) >= 2 and clean[0] == clean[-1] and clean[0] in {'"', "'"}:
        clean = clean[1:-1]
    if not clean.startswith('github_pat_'):
        raise ValueError('bad prefix')
    if any(ord(ch) < 0x21 or ord(ch) > 0x7E for ch in clean):
        raise ValueError('not visible ascii')
    return clean, removed


def main() -> None:
    scripts = [INSTALL, INVOKE, UNINSTALL]
    for path in scripts:
        raw = path.read_bytes()
        check(raw and max(raw) < 128, f"{path.name} is pure ASCII so Windows PowerShell 5.1 code-page decoding cannot corrupt syntax")
        check(b'\r\n' in raw and b'\n' not in raw.replace(b'\r\n', b''), f"{path.name} uses Windows CRLF line endings")
        text_ascii = raw.decode('ascii')
        check(raw.decode('cp950') == text_ascii, f"{path.name} decodes identically under CP950 and ASCII")
        check(raw.decode('utf-8') == text_ascii, f"{path.name} decodes identically under UTF-8 and ASCII")
        check(lexical_balance(text_ascii), f"{path.name} has balanced PowerShell strings/brackets after packaging")

    install = INSTALL.read_text(encoding='ascii')
    invoke = INVOKE.read_text(encoding='ascii')
    uninstall = UNINSTALL.read_text(encoding='ascii')
    workflow = WORKFLOW.read_text(encoding='utf-8')

    check('GodPick V191 H16 Strict 10-Min Wakeup' in install and 'GodPick V191 H16 Strict 10-Min Wakeup' in uninstall,
          'H16 task identity is installed and removable')
    check('strict_wakeup_token_h16.dat' in install and 'strict_wakeup_token_h16.dat' in invoke,
          'H16 uses a fresh token file instead of reusing failed H11-H15 token files')
    check('GODPICK_PS_DPAPI_V6:' in install and 'GODPICK_PS_DPAPI_V6:' in invoke,
          'H16 uses a fresh V6 DPAPI payload marker')
    check('ConvertFrom-SecureString' in install and 'ConvertTo-SecureString -String $encrypted' in invoke,
          'H15 uses Windows PowerShell native DPAPI secure-string round trip')
    check('[System.Char]::IsControl($ch)' in install and '[System.Char]::IsWhiteSpace($ch)' in install,
          'installer strips control and whitespace characters from pasted PAT')
    check('[System.Char]::IsControl($ch)' in invoke and '[System.Char]::IsWhiteSpace($ch)' in invoke,
          'dispatcher re-normalizes decrypted PAT')
    check('Authorization header contains a control character after normalization.' in invoke,
          'dispatcher blocks any residual control character before HTTP')
    check('Invoke-WebRequest -UseBasicParsing' in invoke and '/actions/workflows/$Workflow/dispatches' in invoke,
          'dispatcher calls the PowerShell 5.1-compatible GitHub workflow_dispatch endpoint')
    check('Copy-Item -LiteralPath $SourceScriptPath -Destination $StableScriptPath -Force' in install,
          'scheduled task uses stable LocalAppData dispatcher copy')
    check('New-TimeSpan -Minutes 10' in install and 'AddMinutes(7)' in install,
          'Windows wakeup cadence remains every 10 minutes at xx:07 offset')
    check('cron: "2-52/10 * * * *"' in workflow,
          'GitHub fallback remains staggered at xx:02/12/22/32/42/52')

    dirty = '\r\n\t github_pat_EXAMPLE1234567890 \x00'
    clean, removed = normalize_like_h15(dirty)
    check(clean == 'github_pat_EXAMPLE1234567890' and removed >= 5,
          'clipboard control-character contamination normalizes to a valid PAT')
    check(not any(ord(ch) < 32 or ord(ch) == 127 for ch in ('Bearer ' + clean)),
          'simulated Authorization header is control-character free')

    combined = install + invoke
    check(re.search(r'github_pat_[A-Za-z0-9_]{40,}', combined) is None,
          'package does not embed a real PAT value')

    print('H15 Windows PowerShell 5.1 ASCII encoding smoke: ALL PASS')


if __name__ == '__main__':
    main()

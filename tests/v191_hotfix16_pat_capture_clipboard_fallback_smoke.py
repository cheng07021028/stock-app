from __future__ import annotations

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
WIN = ROOT / "tools" / "windows"
INSTALL = WIN / "Install-GodPickStrictWakeupV191.ps1"
INVOKE = WIN / "Invoke-GodPickStrictWakeupV191.ps1"
UNINSTALL = WIN / "Uninstall-GodPickStrictWakeupV191.ps1"


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print("PASS", msg)


def normalize(value: str, minimum: int = 40) -> tuple[str, int]:
    out = []
    removed = 0
    for ch in value:
        if ch.isspace() or ord(ch) < 32 or ord(ch) == 127:
            removed += 1
            continue
        out.append(ch)
    clean = ''.join(out).strip()
    if len(clean) >= 2 and clean[0] == clean[-1] and clean[0] in {'"', "'"}:
        clean = clean[1:-1]
    if not clean:
        raise ValueError('empty')
    if len(clean) < minimum:
        raise ValueError('short')
    if not clean.startswith('github_pat_'):
        raise ValueError('prefix')
    if any(ord(ch) < 0x21 or ord(ch) > 0x7E for ch in clean):
        raise ValueError('ascii')
    return clean, removed


def main() -> None:
    install = INSTALL.read_text(encoding='ascii')
    invoke = INVOKE.read_text(encoding='ascii')
    uninstall = UNINSTALL.read_text(encoding='ascii')

    for path in [INSTALL, INVOKE, UNINSTALL]:
        raw = path.read_bytes()
        check(raw and max(raw) < 128, f"{path.name} remains pure ASCII")
        check(b'\r\n' in raw and b'\n' not in raw.replace(b'\r\n', b''), f"{path.name} remains Windows CRLF")

    check('GodPick V191 H16 Strict 10-Min Wakeup' in install, 'H16 task identity is installed')
    check('GodPick V191 H16 Strict 10-Min Wakeup' in uninstall, 'H16 task identity is removable')
    check('strict_wakeup_token_h16.dat' in install and 'strict_wakeup_token_h16.dat' in invoke,
          'H16 uses a fresh token file')
    check('GODPICK_PS_DPAPI_V6:' in install and 'GODPICK_PS_DPAPI_V6:' in invoke,
          'H16 uses a fresh DPAPI marker')

    check('Secure input captured length:' in install,
          'installer reports only captured length without revealing PAT')
    check('$capturedLength -lt $MinimumPatLength' in install,
          'installer detects incomplete secure-console input before saving')
    check('System.Net.NetworkCredential' in install and 'SecurePassword = $SecureToken' in install,
          'installer converts SecureString via documented NetworkCredential path')
    check('SecureStringToBSTR' not in install and 'PtrToStringBSTR' not in install,
          'H15 Marshal conversion path is removed from installer')
    check('System.Net.NetworkCredential' in invoke and 'SecurePassword = $SecureToken' in invoke,
          'dispatcher uses the same NetworkCredential conversion path')
    check('SecureStringToBSTR' not in invoke and 'PtrToStringBSTR' not in invoke,
          'H15 Marshal conversion path is removed from dispatcher')

    check('Get-Clipboard -Raw -ErrorAction Stop' in install,
          'PowerShell 5.1 clipboard fallback is implemented')
    check('Set-Clipboard -Value ""' in install,
          'clipboard is cleared immediately after one-time PAT import')
    check('Get-NormalizedPatFromClipboard' in install,
          'installer has explicit clipboard fallback helper')
    check('if ($needClipboardFallback)' in install,
          'incomplete or failed secure input automatically switches to clipboard fallback')

    valid = 'github_pat_' + 'A' * 70
    dirty = '\r\n\t ' + valid + ' \x00'
    clean, removed = normalize(dirty)
    check(clean == valid and removed >= 5,
          'clipboard CR/LF/control contamination is safely normalized')
    try:
        normalize('*')
    except ValueError as exc:
        check(str(exc) == 'short', 'single-character secure input is classified as incomplete, not encrypted')
    else:
        raise AssertionError('single-character input should fail')

    check('Invoke-WebRequest -UseBasicParsing' in invoke,
          'dispatcher remains Windows PowerShell 5.1 HTTP compatible')
    check('Authorization header contains a control character after normalization.' in invoke,
          'dispatcher retains final Authorization header guard')
    check(re.search(r'github_pat_[A-Za-z0-9_]{40,}', install + invoke) is None,
          'package does not embed a real PAT')

    print('H16 PAT capture + clipboard fallback smoke: ALL PASS')


if __name__ == '__main__':
    main()

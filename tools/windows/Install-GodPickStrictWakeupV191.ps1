param(
    [string]$Repository = "cheng07021028/stock-app",
    [string]$Workflow = "godpick_auto_scheduler_v191.yml",
    [string]$Branch = "main"
)

$ErrorActionPreference = "Stop"
$TaskName = "GodPick V191 H16 Strict 10-Min Wakeup"
$LegacyTaskNames = @(
    "GodPick V191 H15 Strict 10-Min Wakeup",
    "GodPick V191 H14 Strict 10-Min Wakeup",
    "GodPick V191 H13 Strict 10-Min Wakeup",
    "GodPick V191 H12 Strict 10-Min Wakeup",
    "GodPick V191 H11 Strict 10-Min Wakeup"
)
$SourceScriptPath = Join-Path $PSScriptRoot "Invoke-GodPickStrictWakeupV191.ps1"
$DataDir = Join-Path $env:LOCALAPPDATA "GodPickV191"
$StableScriptPath = Join-Path $DataDir "Invoke-GodPickStrictWakeupV191.ps1"
$TokenFile = Join-Path $DataDir "strict_wakeup_token_h16.dat"
$LegacyTokenFiles = @(
    (Join-Path $DataDir "strict_wakeup_token_h15.dat"),
    (Join-Path $DataDir "strict_wakeup_token_h14.dat"),
    (Join-Path $DataDir "strict_wakeup_token.dat")
)
$TokenPrefix = "GODPICK_PS_DPAPI_V6:"
$MinimumPatLength = 40

if (-not (Test-Path -LiteralPath $SourceScriptPath)) {
    throw "Strict wakeup dispatcher not found: $SourceScriptPath"
}
New-Item -ItemType Directory -Force -Path $DataDir | Out-Null

function Convert-SecureStringToPlainTextCompat {
    param([Parameter(Mandatory=$true)][System.Security.SecureString]$SecureToken)

    $credential = New-Object -TypeName System.Net.NetworkCredential
    $credential.UserName = "godpick"
    $credential.SecurePassword = $SecureToken
    return $credential.Password
}

function Normalize-GitHubFineGrainedPat {
    param([Parameter(Mandatory=$true)][string]$Value)

    $builder = New-Object System.Text.StringBuilder
    $removed = 0
    foreach ($ch in $Value.ToCharArray()) {
        if ([System.Char]::IsControl($ch) -or [System.Char]::IsWhiteSpace($ch)) {
            $removed++
            continue
        }
        [void]$builder.Append($ch)
    }
    $clean = $builder.ToString().Trim()

    if ($clean.Length -ge 2) {
        if (($clean[0] -eq '"' -and $clean[$clean.Length - 1] -eq '"') -or
            ($clean[0] -eq "'" -and $clean[$clean.Length - 1] -eq "'")) {
            $clean = $clean.Substring(1, $clean.Length - 2)
        }
    }

    if ([System.String]::IsNullOrWhiteSpace($clean)) {
        throw "PAT is empty after normalization."
    }
    if ($clean.Length -lt $MinimumPatLength) {
        throw ("PAT is too short after normalization: captured {0} characters; expected at least {1}." -f $clean.Length, $MinimumPatLength)
    }
    if (-not $clean.StartsWith("github_pat_", [System.StringComparison]::Ordinal)) {
        throw "Invalid Fine-grained PAT prefix. Expected github_pat_."
    }
    if ($clean -notmatch '^[\x21-\x7E]+$') {
        throw "PAT contains characters that are not valid visible ASCII."
    }

    return [PSCustomObject]@{
        Token = $clean
        RemovedCount = $removed
    }
}

function Save-GodPickTokenPsDpapiV6 {
    param(
        [Parameter(Mandatory=$true)][string]$PlainToken,
        [Parameter(Mandatory=$true)][string]$Path
    )

    $secureToken = ConvertTo-SecureString -String $PlainToken -AsPlainText -Force
    $encrypted = ConvertFrom-SecureString -SecureString $secureToken
    if ([System.String]::IsNullOrWhiteSpace($encrypted)) {
        throw "DPAPI token encryption returned an empty payload."
    }
    $payload = $TokenPrefix + $encrypted.Trim()
    [System.IO.File]::WriteAllText($Path, $payload, [System.Text.Encoding]::ASCII)
}

function Get-NormalizedPatFromClipboard {
    Write-Host "Secure console input did not contain a complete PAT." -ForegroundColor Yellow
    Write-Host "Copy the full github_pat_... token to the Windows clipboard now." -ForegroundColor Yellow
    [void](Read-Host "Then press ENTER here; the clipboard will be read once and cleared immediately")

    $clipboardText = $null
    try {
        $clipboardText = Get-Clipboard -Raw -ErrorAction Stop
        if ([System.String]::IsNullOrWhiteSpace([string]$clipboardText)) {
            throw "Clipboard does not contain text."
        }
        $result = Normalize-GitHubFineGrainedPat -Value ([string]$clipboardText)
        return $result
    }
    finally {
        $clipboardText = $null
        try { Set-Clipboard -Value "" -ErrorAction SilentlyContinue } catch {}
    }
}

Write-Host "GodPick V191 H16 strict wakeup installer" -ForegroundColor Cyan
Write-Host "Windows Task Scheduler will dispatch the GitHub workflow every 10 minutes." -ForegroundColor Cyan
Write-Host "Use a Fine-grained PAT limited to this repository with Actions: Read and write." -ForegroundColor Yellow
Write-Host "H16 validates how many characters Windows PowerShell actually captured before saving the PAT." -ForegroundColor DarkCyan
Write-Host "If secure console paste is incomplete, H16 automatically falls back to one-time clipboard import." -ForegroundColor DarkCyan

$secureInput = Read-Host "Enter GitHub Fine-grained PAT (input is hidden)" -AsSecureString
$capturedLength = 0
if ($null -ne $secureInput) { $capturedLength = $secureInput.Length }
Write-Host ("Secure input captured length: {0}" -f $capturedLength) -ForegroundColor DarkGray

$plainInput = $null
$normalized = $null
$needClipboardFallback = $false
try {
    if ($capturedLength -lt $MinimumPatLength) {
        $needClipboardFallback = $true
    }
    else {
        try {
            $plainInput = Convert-SecureStringToPlainTextCompat -SecureToken $secureInput
            $normalized = Normalize-GitHubFineGrainedPat -Value $plainInput
        }
        catch {
            Write-Warning ("Secure input conversion/validation failed: {0}" -f $_.Exception.Message)
            $needClipboardFallback = $true
        }
    }

    if ($needClipboardFallback) {
        $normalized = Get-NormalizedPatFromClipboard
    }

    Write-Host ("PAT local validation: OK | prefix github_pat_ | length {0} | removed chars {1}" -f $normalized.Token.Length, $normalized.RemovedCount) -ForegroundColor Green
    Save-GodPickTokenPsDpapiV6 -PlainToken $normalized.Token -Path $TokenFile
}
finally {
    $plainInput = $null
    $secureInput = $null
}

Copy-Item -LiteralPath $SourceScriptPath -Destination $StableScriptPath -Force

& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $StableScriptPath -Repository $Repository -Workflow $Workflow -Branch $Branch -TokenFile $TokenFile -WakeupSource "windows_install_test"
if ($LASTEXITCODE -ne 0) {
    throw "workflow_dispatch live test failed. Scheduled task was NOT created. Check the H16 diagnostic above."
}

foreach ($legacyTaskName in $LegacyTaskNames) {
    try {
        Unregister-ScheduledTask -TaskName $legacyTaskName -Confirm:$false -ErrorAction SilentlyContinue
    }
    catch {}
}
try {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
}
catch {}

$principal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
$actionArgs = '-NoProfile -ExecutionPolicy Bypass -File "{0}" -Repository "{1}" -Workflow "{2}" -Branch "{3}" -TokenFile "{4}" -WakeupSource "windows_task_scheduler"' -f $StableScriptPath,$Repository,$Workflow,$Branch,$TokenFile
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $actionArgs

$now = Get-Date
$start = $now.Date.AddMinutes(7)
while ($start -le $now) {
    $start = $start.AddMinutes(10)
}
$trigger = New-ScheduledTaskTrigger -Once -At $start -RepetitionInterval (New-TimeSpan -Minutes 10) -RepetitionDuration (New-TimeSpan -Days 3650)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 5)

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null

foreach ($legacyTokenFile in $LegacyTokenFiles) {
    try {
        Remove-Item -LiteralPath $legacyTokenFile -Force -ErrorAction SilentlyContinue
    }
    catch {}
}

Write-Host "SUCCESS workflow_dispatch live test passed." -ForegroundColor Green
Write-Host "Installed scheduled task: $TaskName" -ForegroundColor Green
Write-Host ("Stable dispatcher: {0}" -f $StableScriptPath) -ForegroundColor DarkGray
Write-Host ("First scheduled wakeup: {0}; repeats every 10 minutes while this Windows user is logged in." -f $start.ToString("yyyy-MM-dd HH:mm:ss"))
Write-Host "GitHub schedule remains enabled as the staggered fallback wakeup source."
Write-Host ("Wakeup log: {0}" -f (Join-Path $DataDir "strict_wakeup.log")) -ForegroundColor DarkGray

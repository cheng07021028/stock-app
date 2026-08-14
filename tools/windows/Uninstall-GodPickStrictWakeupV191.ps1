$ErrorActionPreference = "Stop"
$TaskNames = @(
    "GodPick V191 H16 Strict 10-Min Wakeup",
    "GodPick V191 H15 Strict 10-Min Wakeup",
    "GodPick V191 H14 Strict 10-Min Wakeup",
    "GodPick V191 H13 Strict 10-Min Wakeup",
    "GodPick V191 H12 Strict 10-Min Wakeup",
    "GodPick V191 H11 Strict 10-Min Wakeup"
)

foreach ($taskName in $TaskNames) {
    try {
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
        Write-Host "Removed scheduled task: $taskName"
    }
    catch {}
}

$DataDir = Join-Path $env:LOCALAPPDATA "GodPickV191"
$Files = @(
    (Join-Path $DataDir "strict_wakeup_token_h16.dat"),
    (Join-Path $DataDir "strict_wakeup_token_h15.dat"),
    (Join-Path $DataDir "strict_wakeup_token_h14.dat"),
    (Join-Path $DataDir "strict_wakeup_token.dat"),
    (Join-Path $DataDir "Invoke-GodPickStrictWakeupV191.ps1")
)
foreach ($f in $Files) {
    try {
        Remove-Item -LiteralPath $f -Force -ErrorAction SilentlyContinue
    }
    catch {}
}
Write-Host "GodPick V191 strict 10-minute wakeup tasks, local token files, and dispatcher were removed." -ForegroundColor Green

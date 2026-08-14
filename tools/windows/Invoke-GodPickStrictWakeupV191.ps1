param(
    [string]$Repository = "cheng07021028/stock-app",
    [string]$Workflow = "godpick_auto_scheduler_v191.yml",
    [string]$Branch = "main",
    [string]$TokenFile = "$env:LOCALAPPDATA\GodPickV191\strict_wakeup_token_h16.dat",
    [string]$WakeupSource = "windows_task_scheduler"
)

$ErrorActionPreference = "Stop"
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
$TokenPrefix = "GODPICK_PS_DPAPI_V6:"
$MinimumPatLength = 40

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
        throw ("PAT is too short after normalization: {0} characters." -f $clean.Length)
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

function Get-GodPickTokenPlainTextV6 {
    param([Parameter(Mandatory=$true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "H16 token file not found: $Path"
    }
    $raw = [System.IO.File]::ReadAllText($Path, [System.Text.Encoding]::ASCII)
    if ($null -eq $raw) {
        throw "H16 token file is empty."
    }
    $raw = $raw.Trim()
    if (-not $raw.StartsWith($TokenPrefix, [System.StringComparison]::Ordinal)) {
        throw "Token file is not H16 V6 format. Run the H16 installer again."
    }

    $encrypted = $raw.Substring($TokenPrefix.Length).Trim()
    if ([System.String]::IsNullOrWhiteSpace($encrypted)) {
        throw "H16 encrypted token payload is empty."
    }

    try {
        $secure = ConvertTo-SecureString -String $encrypted
        $plain = Convert-SecureStringToPlainTextCompat -SecureToken $secure
        if ([System.String]::IsNullOrWhiteSpace($plain)) {
            throw "Token decrypted to an empty value."
        }
        return $plain
    }
    catch {
        throw ("H16 token cannot be decrypted by the current Windows user. Run the installer again. Detail: {0}" -f $_.Exception.Message)
    }
}

function Get-GitHubHttpErrorMessage {
    param([Parameter(Mandatory=$true)]$ErrorRecord)

    $statusCode = $null
    $responseText = ""
    try {
        $statusCode = [int]$ErrorRecord.Exception.Response.StatusCode
    }
    catch {}
    try {
        $stream = $ErrorRecord.Exception.Response.GetResponseStream()
        if ($null -ne $stream) {
            $reader = New-Object System.IO.StreamReader($stream)
            $responseText = $reader.ReadToEnd()
            $reader.Dispose()
        }
    }
    catch {}

    switch ($statusCode) {
        401 { return "GitHub HTTP 401: PAT is invalid, expired, or revoked." }
        403 { return "GitHub HTTP 403: PAT permission is insufficient. Set repository Actions to Read and write." }
        404 { return "GitHub HTTP 404: repository/workflow is not accessible to this PAT." }
        422 { return "GitHub HTTP 422: workflow_dispatch ref or inputs do not match the workflow definition." }
        default {
            if ([System.String]::IsNullOrWhiteSpace($responseText)) {
                return $ErrorRecord.Exception.Message
            }
            return ("GitHub HTTP {0}: {1}" -f $statusCode, $responseText)
        }
    }
}

$token = $null
try {
    $rawToken = Get-GodPickTokenPlainTextV6 -Path $TokenFile
    $normalized = Normalize-GitHubFineGrainedPat -Value $rawToken
    $token = $normalized.Token
    $rawToken = $null

    $authValue = [System.String]::Concat("Bearer ", $token)
    foreach ($ch in $authValue.ToCharArray()) {
        if ([System.Char]::IsControl($ch)) {
            throw "Authorization header contains a control character after normalization."
        }
    }

    $headers = @{
        Accept = "application/vnd.github+json"
        Authorization = $authValue
        "X-GitHub-Api-Version" = "2022-11-28"
        "User-Agent" = "GodPick-V191-H16-Strict-Wakeup"
    }
    $body = @{
        ref = $Branch
        inputs = @{
            wakeup_source = $WakeupSource
        }
    } | ConvertTo-Json -Depth 4 -Compress

    $uri = "https://api.github.com/repos/$Repository/actions/workflows/$Workflow/dispatches"
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri $uri -Method Post -Headers $headers -ContentType "application/json" -Body $body
        if ($null -ne $response) {
            $okCodes = @(200, 201, 202, 204)
            if ($okCodes -notcontains [int]$response.StatusCode) {
                throw ("Unexpected workflow_dispatch HTTP status: {0}" -f $response.StatusCode)
            }
        }
    }
    catch {
        throw (Get-GitHubHttpErrorMessage -ErrorRecord $_)
    }

    $logDir = Join-Path $env:LOCALAPPDATA "GodPickV191"
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $line = "{0} SUCCESS workflow_dispatch {1} source={2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Repository, $WakeupSource
    Add-Content -LiteralPath (Join-Path $logDir "strict_wakeup.log") -Value $line -Encoding ASCII
    Write-Host $line -ForegroundColor Green
    exit 0
}
catch {
    $logDir = Join-Path $env:LOCALAPPDATA "GodPickV191"
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $line = "{0} FAILED {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $_.Exception.Message
    Add-Content -LiteralPath (Join-Path $logDir "strict_wakeup.log") -Value $line -Encoding ASCII
    Write-Error $line
    exit 1
}
finally {
    $token = $null
    Remove-Variable token -ErrorAction SilentlyContinue
}

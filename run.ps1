# DMM-CM Agent Launcher (Windows PowerShell)
# Usage: .\run.ps1

Set-Location $PSScriptRoot

$Agent = Join-Path $PSScriptRoot "agent.py"
$Venv = Join-Path $PSScriptRoot "venv"
$Requirements = Join-Path $PSScriptRoot "requirements.txt"

# ─── Setup Python environment ───

function Initialize-Env {
    if (-not (Test-Path $Venv)) {
        Write-Host "[INFO] Creating virtual environment..." -ForegroundColor Cyan
        python -m venv $Venv
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERROR] Failed to create venv. Is Python 3.10+ installed?" -ForegroundColor Red
            exit 1
        }
    }
    Write-Host "[INFO] Installing dependencies..." -ForegroundColor Cyan
    & "$Venv\Scripts\python" -m pip install --upgrade pip -q
    & "$Venv\Scripts\python" -m pip install -r $Requirements -q
    Write-Host "[INFO] Environment ready" -ForegroundColor Green
}

Initialize-Env

# ─── Check DEVICE_TOKEN ───

$ConfigFile = Join-Path $PSScriptRoot "config.json"
$Token = ""
if (Test-Path $ConfigFile) {
    try {
        $Config = Get-Content $ConfigFile -Raw | ConvertFrom-Json
        $Token = $Config.DEVICE_TOKEN
    } catch {}
}
if ($Token -and $Token.Length -ge 8) {
    $Masked = $Token.Substring(0, 4) + "***" + $Token.Substring($Token.Length - 4)
    Write-Host "[agent] Token: $Masked" -ForegroundColor Gray
} else {
    Write-Host "==========================================" -ForegroundColor Yellow
    Write-Host "  First run - device binding required" -ForegroundColor Yellow
    Write-Host "  Agent will generate a Telegram link" -ForegroundColor Yellow
    Write-Host "  Click the link in Telegram to bind" -ForegroundColor Yellow
    Write-Host "==========================================" -ForegroundColor Yellow
}

# ─── Kill existing agent ───

$LockFile = Join-Path $env:TEMP "dmm-agent.lock"
if (Test-Path $LockFile) {
    $OldPid = (Get-Content $LockFile -ErrorAction SilentlyContinue).Trim()
    if ($OldPid) {
        $Proc = Get-Process -Id $OldPid -ErrorAction SilentlyContinue
        if ($Proc) {
            Write-Host "[agent] Stopping old agent (PID $OldPid)..." -ForegroundColor Yellow
            Stop-Process -Id $OldPid -Force -ErrorAction SilentlyContinue
            Start-Sleep -Milliseconds 500
            Write-Host "[agent] Old agent stopped" -ForegroundColor Green
        }
    }
    Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
}

# ─── Clear proxy env vars ───

$ProxyVars = @("ALL_PROXY", "all_proxy", "HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy")
foreach ($v in $ProxyVars) {
    [Environment]::SetEnvironmentVariable($v, $null, "Process")
}

# ─── Launch agent ───

Write-Host ""
Write-Host "[agent] Starting DMM-CM Agent..." -ForegroundColor Cyan
Write-Host "[agent] Press Ctrl+C to stop" -ForegroundColor Gray
Write-Host ""

& "$Venv\Scripts\python" -B $Agent

# ==============================================================================
# QUANT CORTEX | Launcher Script for Windows PowerShell
# ==============================================================================
$ErrorActionPreference = "Stop"

Set-Location -Path $PSScriptRoot

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "⚡ INICIANDO QUANT CORTEX TERMINAL (Snowflake AI Data Cloud)" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan

# 1. Activar entorno virtual
if (Test-Path ".venv\Scripts\Activate.ps1") {
    Write-Host "[*] Activando entorno virtual .venv..." -ForegroundColor Green
    & .\.venv\Scripts\Activate.ps1
} elseif (Test-Path ".venv\Scripts\activate.bat") {
    Write-Host "[*] Activando entorno virtual .venv..." -ForegroundColor Green
    $env:PATH = "$PSScriptRoot\.venv\Scripts;" + $env:PATH
} else {
    Write-Host "[!] Creando entorno virtual .venv..." -ForegroundColor Yellow
    python -m venv .venv
    & .\.venv\Scripts\Activate.ps1
}

# 2. Verificar dependencias
Write-Host "[*] Verificando dependencias..." -ForegroundColor Gray
python -c "import streamlit, plotly, snowflake.connector" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[*] Instalando dependencias de requirements.txt y Streamlit..." -ForegroundColor Yellow
    pip install -r requirements.txt streamlit plotly
}

# 3. Lanzar Streamlit con recarga automática activa
Write-Host "[*] Lanzando terminal web Streamlit en http://localhost:8501 ..." -ForegroundColor Cyan
streamlit run app.py --server.port 8501 --server.runOnSave true

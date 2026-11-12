# ==============================================================================
# QUANT CORTEX | Kafka & WebSocket Real-Time Candlestick Streaming Gateway
# ==============================================================================
$ErrorActionPreference = "Continue"

Set-Location -Path $PSScriptRoot

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "⚡ INICIANDO REAL-TIME CANDLESTICK STREAMING (KAFKA + WEBSOCKET GATEWAY)" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan

# 1. Verificar si Docker está disponible para levantar Redpanda / Kafka
$dockerCheck = docker ps 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "[*] Docker está activo. Verificando clúster de Kafka (Redpanda)..." -ForegroundColor Green
    docker compose -f docker-compose.kafka.yml up -d
    Write-Host "[✓] Broker Kafka disponible en localhost:9092" -ForegroundColor Green
    Write-Host "[✓] Consola Web de Kafka disponible en http://localhost:8080" -ForegroundColor Green
} else {
    Write-Host "[!] Docker daemon no está iniciado. Operando en modo directo de alta velocidad (zero-broker fallback)." -ForegroundColor Yellow
    Write-Host "[i] Tip: Puedes iniciar Docker Desktop para activar la persistencia distribuida en Kafka." -ForegroundColor Gray
}

# 2. Lanzar Gateway WebSocket en puerto 8765
Write-Host "[*] Iniciando WebSocket Gateway en ws://localhost:8765/ws/candles ..." -ForegroundColor Cyan
python -m src.pipeline.ws_gateway

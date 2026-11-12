@echo off
title QUANT CORTEX - Real-Time Candlestick Streaming Gateway
echo ==================================================================
echo   INICIANDO KAFKA & WEBSOCKET REAL-TIME STREAMING GATEWAY
echo ==================================================================

docker ps >nul 2>&1
if %ERRORLEVEL% equ 0 (
    echo [*] Docker detectado. Levantando cluster Redpanda Kafka...
    docker compose -f docker-compose.kafka.yml up -d
) else (
    echo [!] Docker no esta en ejecucion. Operando en modo directo zero-broker.
)

echo [*] Lanzando servidor WebSocket en ws://localhost:8765 ...
python -m src.pipeline.ws_gateway
pause

"""Real-Time Institutional WebSocket Gateway.

Bridges Kafka/Exchange Candlestick Streams directly to TradingView Frontend.
Listens on port 8765.

Endpoint:
  ws://localhost:8765/ws/candles/{ticker}
  (e.g., ws://localhost:8765/ws/candles/BTCUSDT)
"""

import asyncio
import json
import logging
import signal
import sys
import time
from typing import Dict, Set
import websockets
from websockets.server import WebSocketServerProtocol

from src.pipeline.kafka_market_pipeline import KafkaCandleProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (ws_gateway) %(message)s"
)
logger = logging.getLogger("ws_gateway")

# Active WebSocket connections per symbol: {"BTCUSDT": {client1, client2}}
CONNECTED_CLIENTS: Dict[str, Set[WebSocketServerProtocol]] = {}

# Active background producers per symbol: {"BTCUSDT": KafkaCandleProducer}
ACTIVE_PRODUCERS: Dict[str, KafkaCandleProducer] = {}
PRODUCER_TASKS: Dict[str, asyncio.Task] = {}

# Latest cached candle per symbol
LATEST_CANDLES: Dict[str, dict] = {}


def broadcast_candle_update(symbol: str, candle: dict) -> None:
    """Callback invoked when a new trade tick or candle update arrives."""
    sym = symbol.upper()
    LATEST_CANDLES[sym] = candle

    clients = CONNECTED_CLIENTS.get(sym, set())
    if not clients:
        return

    # Lightweight Charts expects unix timestamp in seconds
    payload = json.dumps({
        "symbol": sym,
        "time": candle.get("time", int(time.time())),
        "open": round(float(candle.get("open", 0.0)), 2),
        "high": round(float(candle.get("high", 0.0)), 2),
        "low": round(float(candle.get("low", 0.0)), 2),
        "close": round(float(candle.get("close", 0.0)), 2),
        "volume": round(float(candle.get("volume", 0.0)), 4),
        "cvd": round(float(candle.get("cvd", 0.0)), 4),
        "trades_count": int(candle.get("trades_count", 0)),
        "is_closed": bool(candle.get("is_closed", False))
    })

    # Broadcast to all active clients
    dead_clients = set()
    for client in list(clients):
        try:
            asyncio.create_task(client.send(payload))
        except Exception:
            dead_clients.add(client)

    if dead_clients:
        clients.difference_update(dead_clients)


def ensure_producer_running(symbol: str, timeframe: str = "5m") -> None:
    """Ensure a KafkaCandleProducer is running for the given symbol."""
    sym = symbol.upper()
    if sym in ACTIVE_PRODUCERS:
        return

    logger.info(f"Spawning Kafka stream worker for {sym} ({timeframe})...")
    producer = KafkaCandleProducer(
        symbol=sym,
        timeframe=timeframe,
        on_candle_update=lambda c: broadcast_candle_update(sym, c)
    )
    ACTIVE_PRODUCERS[sym] = producer
    loop = asyncio.get_event_loop()
    task = loop.create_task(producer.run())
    PRODUCER_TASKS[sym] = task


async def handler(websocket: WebSocketServerProtocol, path: str = ""):
    """Handle incoming browser WebSocket connections."""
    # Extract symbol from path, e.g. /ws/candles/BTCUSDT or /BTCUSDT
    parts = path.strip("/").split("/")
    symbol = "BTCUSDT"
    if len(parts) >= 3 and parts[0] == "ws" and parts[1] == "candles":
        symbol = parts[2].upper()
    elif len(parts) == 1 and parts[0]:
        symbol = parts[0].upper()

    # Clean ticker name (remove slashes, etc.)
    symbol = symbol.replace("/", "").replace("-", "")

    if symbol not in CONNECTED_CLIENTS:
        CONNECTED_CLIENTS[symbol] = set()
    CONNECTED_CLIENTS[symbol].add(websocket)
    logger.info(f"[CLIENT CONNECTED] {websocket.remote_address} subscribed to {symbol} (Total: {len(CONNECTED_CLIENTS[symbol])})")

    # Start stream if not already active
    ensure_producer_running(symbol)

    # Immediately push cached candle if available
    if symbol in LATEST_CANDLES:
        try:
            last = LATEST_CANDLES[symbol]
            await websocket.send(json.dumps({
                "symbol": symbol,
                "time": last.get("time", int(time.time())),
                "open": round(float(last.get("open", 0.0)), 2),
                "high": round(float(last.get("high", 0.0)), 2),
                "low": round(float(last.get("low", 0.0)), 2),
                "close": round(float(last.get("close", 0.0)), 2),
                "volume": round(float(last.get("volume", 0.0)), 4),
                "cvd": round(float(last.get("cvd", 0.0)), 4),
                "trades_count": int(last.get("trades_count", 0)),
                "is_closed": bool(last.get("is_closed", False))
            }))
        except Exception as e:
            logger.debug(f"Error sending initial state: {e}")

    try:
        # Keep connection open and handle incoming ping/pong
        async for message in websocket:
            # Client can send {"action": "switch", "symbol": "ETHUSDT"}
            try:
                data = json.loads(message)
                if data.get("action") == "ping":
                    await websocket.send(json.dumps({"action": "pong"}))
            except Exception:
                pass
    except websockets.ConnectionClosed:
        pass
    finally:
        if symbol in CONNECTED_CLIENTS and websocket in CONNECTED_CLIENTS[symbol]:
            CONNECTED_CLIENTS[symbol].remove(websocket)
        logger.info(f"[CLIENT DISCONNECTED] {websocket.remote_address} from {symbol}")


async def main():
    host = "0.0.0.0"
    port = 8765
    logger.info(f"Starting Institutional WebSocket Gateway on ws://{host}:{port}")

    # Pre-warm BTCUSDT producer
    ensure_producer_running("BTCUSDT", "5m")

    async with websockets.serve(handler, host, port):
        logger.info(f"WebSocket Gateway READY. Listening on ws://localhost:{port}/ws/candles/<SYMBOL>")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopping WebSocket Gateway...")

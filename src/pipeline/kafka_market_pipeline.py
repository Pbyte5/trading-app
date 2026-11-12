"""Institutional Kafka Market Data Streaming Pipeline for Real-Time Candlestick Charting.

Architecture:
1. Ingests tick-by-tick trades (aggTrade) & forming klines from Binance WebSocket.
2. Publishes serialized events into Kafka topics:
   - 'market.crypto.trades.<symbol>'
   - 'market.crypto.candles.<symbol>'
3. Computes real-time Cumulative Volume Delta (CVD) and OHLC price movements.
4. Provides dual-mode execution:
   - Kafka Cluster mode (if broker at localhost:9092 is active)
   - Zero-broker local fallback (so charting works even before Kafka container starts)
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import websockets

try:
    from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (kafka_pipeline) %(message)s"
)
logger = logging.getLogger("kafka_pipeline")


def is_kafka_broker_reachable(bootstrap_servers: str = "localhost:9092", timeout_sec: float = 1.5) -> bool:
    """Check if Kafka broker is reachable without throwing unhandled exceptions."""
    if not KAFKA_AVAILABLE:
        return False
    try:
        p = Producer({'bootstrap.servers': bootstrap_servers, 'socket.timeout.ms': int(timeout_sec * 1000)})
        meta = p.list_topics(timeout=timeout_sec)
        return meta is not None and len(meta.brokers) > 0
    except Exception as e:
        logger.debug(f"Kafka broker check failed: {e}")
        return False


class KafkaCandleProducer:
    """Produces real-time tick-by-tick trades and forming candles to Kafka."""

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        timeframe: str = "5m",
        bootstrap_servers: str = "localhost:9092",
        on_candle_update: Optional[Callable[[Dict[str, Any]], None]] = None
    ):
        self.symbol = symbol.upper()
        self.timeframe = timeframe.lower()
        self.bootstrap_servers = bootstrap_servers
        self.on_candle_update = on_candle_update

        self.trade_topic = f"market.crypto.trades.{self.symbol.lower()}"
        self.candle_topic = f"market.crypto.candles.{self.symbol.lower()}"

        self.kafka_active = False
        self._producer: Optional[Any] = None

        if KAFKA_AVAILABLE and is_kafka_broker_reachable(self.bootstrap_servers):
            try:
                self._producer = Producer({
                    'bootstrap.servers': self.bootstrap_servers,
                    'client.id': f'quant-producer-{self.symbol}',
                    'acks': '1',
                    'linger.ms': 5,
                    'compression.type': 'snappy' if hasattr(Producer, 'COMPRESSION_SNAPPY') else 'none'
                })
                self.kafka_active = True
                logger.info(f"Connected to Kafka broker at {self.bootstrap_servers}. Active topics: {self.trade_topic}, {self.candle_topic}")
            except Exception as e:
                logger.warning(f"Could not initialize Kafka producer: {e}. Falling back to direct stream.")
        else:
            logger.info("Kafka broker not reachable or not running. Operating in High-Speed Local Stream mode.")

        self._running = False
        self._candle_cvd = 0.0
        self.live_candle: Dict[str, Any] = {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "time": 0,
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": 0.0,
            "volume": 0.0,
            "is_closed": False,
            "cvd": 0.0,
            "trades_count": 0,
            "timestamp_utc": ""
        }

    @property
    def stream_url(self) -> str:
        s = self.symbol.lower()
        streams = f"{s}@kline_{self.timeframe}/{s}@aggTrade"
        return f"wss://stream.binance.com:9443/stream?streams={streams}"

    def publish_kafka(self, topic: str, key: str, payload: Dict[str, Any]) -> None:
        """Publish JSON message to Kafka topic if broker is active."""
        if not self.kafka_active or self._producer is None:
            return
        try:
            val_bytes = json.dumps(payload).encode("utf-8")
            self._producer.produce(topic, key=key.encode("utf-8"), value=val_bytes)
            self._producer.poll(0)
        except Exception as e:
            logger.debug(f"Kafka produce error on {topic}: {e}")

    def handle_trade(self, data: Dict[str, Any]) -> None:
        """Handle individual trade tick and compute delta."""
        try:
            qty = float(data.get("q", 0.0))
            is_buyer_maker = bool(data.get("m", False))
            price = float(data.get("p", 0.0))
            trade_time = int(data.get("T", time.time() * 1000))

            delta = -qty if is_buyer_maker else qty
            self._candle_cvd += delta
            self.live_candle["cvd"] = round(self._candle_cvd, 4)
            self.live_candle["trades_count"] = self.live_candle.get("trades_count", 0) + 1

            # Update high/low/close with current tick
            if price > 0:
                self.live_candle["close"] = price
                if self.live_candle["open"] == 0:
                    self.live_candle["open"] = price
                if price > self.live_candle["high"]:
                    self.live_candle["high"] = price
                if self.live_candle["low"] == 0 or price < self.live_candle["low"]:
                    self.live_candle["low"] = price

            # Publish trade event to Kafka
            trade_payload = {
                "symbol": self.symbol,
                "price": price,
                "quantity": qty,
                "is_buyer_maker": is_buyer_maker,
                "trade_time": trade_time,
                "cvd": self.live_candle["cvd"]
            }
            self.publish_kafka(self.trade_topic, self.symbol, trade_payload)

            # Trigger real-time callback for WebSocket subscribers
            if self.on_candle_update and self.live_candle["time"] > 0:
                self.on_candle_update(dict(self.live_candle))
        except Exception as e:
            logger.debug(f"Error in handle_trade: {e}")

    def handle_kline(self, data: Dict[str, Any]) -> None:
        """Handle 5m/timeframe kline packet."""
        k = data.get("k", {})
        if not k:
            return

        ts_ms = k.get("t", 0)
        ts_sec = int(ts_ms / 1000)
        is_closed = bool(k.get("x", False))

        # Reset candle CVD if new candle period began
        if self.live_candle["time"] != 0 and ts_sec != self.live_candle["time"]:
            self._candle_cvd = 0.0

        self.live_candle.update({
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "time": ts_sec,
            "open": float(k.get("o", self.live_candle["open"])),
            "high": float(k.get("h", self.live_candle["high"])),
            "low": float(k.get("l", self.live_candle["low"])),
            "close": float(k.get("c", self.live_candle["close"])),
            "volume": float(k.get("v", self.live_candle["volume"])),
            "is_closed": is_closed,
            "timestamp_utc": datetime.fromtimestamp(ts_sec, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        })

        # Publish candle update to Kafka
        self.publish_kafka(self.candle_topic, self.symbol, self.live_candle)

        # Notify callback
        if self.on_candle_update:
            self.on_candle_update(dict(self.live_candle))

    async def run(self) -> None:
        """Main async loop subscribing to Binance stream."""
        self._running = True
        logger.info(f"Connecting to Binance stream for {self.symbol} ({self.timeframe})...")

        while self._running:
            try:
                async with websockets.connect(
                    self.stream_url,
                    ping_interval=20,
                    ping_timeout=15,
                    close_timeout=10
                ) as ws:
                    logger.info(f"Connected to stream. Broadcasting sub-second candle updates for {self.symbol}.")
                    while self._running:
                        try:
                            msg_raw = await ws.recv()
                            msg = json.loads(msg_raw)
                            stream_name = msg.get("stream", "")
                            payload = msg.get("data", {})

                            if "aggTrade" in stream_name:
                                self.handle_trade(payload)
                            elif "kline" in stream_name:
                                self.handle_kline(payload)
                        except websockets.ConnectionClosed:
                            logger.warning("Stream disconnected. Reconnecting in 3s...")
                            break
                        except Exception as e:
                            logger.debug(f"Error handling message: {e}")
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)

    def stop(self) -> None:
        self._running = False
        if self._producer:
            try:
                self._producer.flush(timeout=2.0)
            except Exception:
                pass
        logger.info(f"Producer for {self.symbol} stopped.")

"""Standalone Binance WebSocket Worker for Real-Time Crypto Streaming.

Consumes kline and aggTrade streams for BTCUSDT, computes real CVD,
buffers raw messages locally into partitioned compressed JSONL (Bronze MVP),
and writes live forming candle state to local IPC cache for zero-Snowflake-compute UI.
"""

import asyncio
import gzip
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (ws_worker) %(message)s"
)
logger = logging.getLogger("ws_binance_worker")


class BinanceWebSocketWorker:
    """Standalone WebSocket worker for Binance crypto market data."""

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        timeframe: str = "5m",
        buffer_dir: str = "data/bronze_buffer",
        live_state_dir: str = "data/live_state",
        batch_size: int = 500,
        flush_interval_sec: float = 60.0,
        on_candle_close: Optional[Callable[[Dict[str, Any]], None]] = None
    ):
        self.symbol = symbol.upper()
        self.timeframe = timeframe.lower()
        self.buffer_dir = Path(buffer_dir)
        self.live_state_dir = Path(live_state_dir)
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec
        self.on_candle_close = on_candle_close

        self.buffer_dir.mkdir(parents=True, exist_ok=True)
        self.live_state_dir.mkdir(parents=True, exist_ok=True)

        self._running = False
        self._buffer: List[Dict[str, Any]] = []
        self._last_flush_time = time.time()
        self._connection_start_time = 0.0

        # CVD and forming candle tracking
        self.live_state: Dict[str, Any] = {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "timestamp_utc": None,
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": 0.0,
            "volume": 0.0,
            "is_closed": False,
            "cvd": 0.0,
            "trades_count": 0,
            "last_updated_epoch": 0.0
        }
        self._candle_cvd = 0.0

    @property
    def stream_url(self) -> str:
        s = self.symbol.lower()
        streams = f"{s}@kline_{self.timeframe}/{s}@aggTrade"
        return f"wss://stream.binance.com:9443/stream?streams={streams}"

    def _flush_buffer(self) -> Optional[Path]:
        """Write in-memory raw messages to partitioned compressed JSONL file."""
        if not self._buffer:
            return None

        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")

        partition_dir = self.buffer_dir / f"symbol={self.symbol}" / f"date={date_str}" / f"hour={hour_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        file_timestamp = int(now.timestamp() * 1000)
        file_path = partition_dir / f"raw_stream_{file_timestamp}.jsonl.gz"

        records_to_write = self._buffer
        self._buffer = []
        self._last_flush_time = time.time()

        try:
            with gzip.open(file_path, "wt", encoding="utf-8") as f:
                for record in records_to_write:
                    f.write(json.dumps(record) + "\n")
            logger.info(f"[BRONZE BUFFER] Flushed {len(records_to_write)} raw messages to {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Failed to flush bronze buffer: {e}")
            # Restore records to buffer so data is not lost
            self._buffer = records_to_write + self._buffer
            return None

    def _write_live_state(self) -> None:
        """Atomically persist current forming candle state for UI consumption."""
        live_file = self.live_state_dir / f"live_{self.symbol.lower()}.json"
        tmp_file = self.live_state_dir / f".live_{self.symbol.lower()}.tmp"

        self.live_state["last_updated_epoch"] = time.time()
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(self.live_state, f, indent=2)
            os.replace(tmp_file, live_file)
        except Exception as e:
            logger.debug(f"Error updating live state file: {e}")

    def _handle_agg_trade(self, data: Dict[str, Any]) -> None:
        """Process real-time aggregated trade and update real Cumulative Volume Delta (CVD)."""
        # Binance aggTrade schema:
        # 'p': price, 'q': quantity, 'm': is buyer the market maker?
        # If m is True: maker was buyer -> taker was seller -> taker sell -> delta is negative
        # If m is False: maker was seller -> taker was buyer -> taker buy -> delta is positive
        try:
            qty = float(data.get("q", 0.0))
            is_buyer_maker = bool(data.get("m", False))
            trade_delta = -qty if is_buyer_maker else qty

            self._candle_cvd += trade_delta
            self.live_state["cvd"] = round(self._candle_cvd, 4)
            self.live_state["trades_count"] = self.live_state.get("trades_count", 0) + 1
        except Exception as e:
            logger.debug(f"Error parsing aggTrade: {e}")

    def _handle_kline(self, data: Dict[str, Any]) -> None:
        """Process 5m candlestick update."""
        k = data.get("k", {})
        if not k:
            return

        ts_ms = k.get("t", 0)
        ts_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        is_closed = bool(k.get("x", False))

        self.live_state["timestamp_utc"] = ts_utc
        self.live_state["open"] = float(k.get("o", 0.0))
        self.live_state["high"] = float(k.get("h", 0.0))
        self.live_state["low"] = float(k.get("l", 0.0))
        self.live_state["close"] = float(k.get("c", 0.0))
        self.live_state["volume"] = float(k.get("v", 0.0))
        self.live_state["is_closed"] = is_closed

        self._write_live_state()

        if is_closed:
            logger.info(
                f"[CANDLE CLOSED] {self.symbol} {self.timeframe} @ {ts_utc} "
                f"Close: {self.live_state['close']} Vol: {self.live_state['volume']} CVD: {self.live_state['cvd']}"
            )
            if self.on_candle_close:
                try:
                    self.on_candle_close(dict(self.live_state))
                except Exception as e:
                    logger.error(f"Error in on_candle_close callback: {e}")

            # Reset candle CVD after bar close
            self._candle_cvd = 0.0

    async def run(self) -> None:
        """Run the WebSocket consumer loop with auto-reconnection and exponential backoff."""
        self._running = True
        backoff_sec = 1.0
        max_backoff = 30.0
        # Binance disconnects streams after 24h; proactively reconnect at 23h50m
        max_session_duration = 23 * 3600 + 50 * 60

        logger.info(f"Starting Binance WebSocket worker for {self.symbol} ({self.timeframe})...")
        logger.info(f"Connecting to: {self.stream_url}")

        while self._running:
            try:
                self._connection_start_time = time.time()
                async with websockets.connect(self.stream_url, ping_interval=20, ping_timeout=20) as ws:
                    logger.info(f"[CONNECTED] Subscribed to {self.symbol} kline_{self.timeframe} and aggTrade streams.")
                    backoff_sec = 1.0  # Reset backoff on successful connection

                    while self._running:
                        # Check proactive reconnect
                        if time.time() - self._connection_start_time > max_session_duration:
                            logger.info("[RECONNECT] Graceful proactive 24h session renewal.")
                            break

                        try:
                            msg_raw = await asyncio.wait_for(ws.recv(), timeout=25.0)
                        except asyncio.TimeoutError:
                            # Ping/keepalive check
                            pong_waiter = await ws.ping()
                            await asyncio.wait_for(pong_waiter, timeout=10.0)
                            continue

                        msg = json.loads(msg_raw)
                        stream_name = msg.get("stream", "")
                        payload = msg.get("data", {})

                        # Buffer original raw JSON message
                        self._buffer.append({
                            "received_at": datetime.now(timezone.utc).isoformat(),
                            "stream": stream_name,
                            "raw_message": payload
                        })

                        if "aggTrade" in stream_name:
                            self._handle_agg_trade(payload)
                        elif "kline" in stream_name:
                            self._handle_kline(payload)

                        # Check buffer flush triggers
                        now_time = time.time()
                        if (
                            len(self._buffer) >= self.batch_size
                            or (now_time - self._last_flush_time >= self.flush_interval_sec)
                        ):
                            self._flush_buffer()

            except (websockets.ConnectionClosed, websockets.WebSocketException, OSError) as conn_err:
                if not self._running:
                    break
                logger.warning(f"[WS DISCONNECT] {conn_err}. Reconnecting in {backoff_sec:.1f}s...")
                self._flush_buffer()
                await asyncio.sleep(backoff_sec)
                backoff_sec = min(backoff_sec * 2.0, max_backoff)
            except Exception as unhandled_err:
                if not self._running:
                    break
                logger.error(f"[WS ERROR] Unhandled error: {unhandled_err}. Retrying in {backoff_sec:.1f}s...")
                self._flush_buffer()
                await asyncio.sleep(backoff_sec)
                backoff_sec = min(backoff_sec * 2.0, max_backoff)

        # Final flush on shutdown
        self._flush_buffer()
        logger.info("[STOPPED] WebSocket worker shut down cleanly.")

    def stop(self) -> None:
        """Signal worker to terminate loop and flush remaining buffers."""
        self._running = False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Binance WebSocket Worker (BTCUSDT)")
    parser.add_argument("--symbol", default="BTCUSDT", help="Symbol to stream (default: BTCUSDT)")
    parser.add_argument("--timeframe", default="5m", help="Candle timeframe (default: 5m)")
    parser.add_argument("--batch-size", type=int, default=500, help="Bronze buffer flush size")
    parser.add_argument("--flush-interval", type=float, default=60.0, help="Buffer flush interval (s)")
    args = parser.parse_args()

    worker = BinanceWebSocketWorker(
        symbol=args.symbol,
        timeframe=args.timeframe,
        batch_size=args.batch_size,
        flush_interval_sec=args.flush_interval
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def shutdown_signal(sig, frame):
        logger.info(f"Received signal {sig}, terminating gracefully...")
        worker.stop()

    signal.signal(signal.SIGINT, shutdown_signal)
    signal.signal(signal.SIGTERM, shutdown_signal)

    try:
        loop.run_until_complete(worker.run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()

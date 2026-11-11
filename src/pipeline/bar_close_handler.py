"""Bar-Close Event Handler and Micro-Batch Ingestion Pipeline.

Triggered strictly upon candle close (kline['x'] == True):
1. Flushes buffered Bronze raw stream into Snowflake BRONZE_RAW.
2. Upserts the closed candle into SILVER_CLEAN.MARKET_CANDLES.
3. Triggers incremental Gold recalculation (microstructure + volume profile + VWAP)
   using a bounded lookback window.
"""

import gzip
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.connectors.snow_loader import load_raw_to_bronze, upsert_candles_to_silver
from src.quant_engine.microstructure import process_quant_state_for_ticker

logger = logging.getLogger("bar_close_handler")


def flush_pending_bronze_buffers(
    buffer_dir: str = "data/bronze_buffer",
    symbol: str = "BTCUSDT",
    timeframe: str = "5m",
    max_files: int = 5
) -> int:
    """Read pending compressed JSONL buffer files and load raw batches to Snowflake Bronze."""
    buf_path = Path(buffer_dir) / f"symbol={symbol.upper()}"
    if not buf_path.exists():
        return 0

    gz_files = sorted(buf_path.rglob("*.jsonl.gz"))
    if not gz_files:
        return 0

    total_loaded = 0
    # Process up to max_files to avoid oversized transactions
    for gz_file in gz_files[:max_files]:
        try:
            records = []
            with gzip.open(gz_file, "rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))

            if records:
                load_raw_to_bronze(
                    records=records,
                    table_name="RAW_MARKET_FEED",
                    source="BINANCE_WS_BUFFER",
                    market="CRYPTO",
                    ticker=symbol.upper(),
                    timeframe=timeframe.lower()
                )
                total_loaded += len(records)

            # Delete or archive processed buffer file to prevent double-loading
            gz_file.unlink()
        except Exception as e:
            logger.error(f"Error loading buffer file {gz_file} to Bronze: {e}")
            break

    return total_loaded


def on_bar_closed_pipeline(
    closed_state: Dict[str, Any],
    flush_bronze: bool = True
) -> Dict[str, Any]:
    """Execute the atomic pipeline when a 5m bar closes."""
    symbol = str(closed_state.get("symbol", "BTCUSDT")).upper()
    timeframe = str(closed_state.get("timeframe", "5m")).lower()
    ts_utc = closed_state.get("timestamp_utc")

    logger.info(f"[PIPELINE BAR CLOSE] Processing closed candle for {symbol} ({timeframe}) @ {ts_utc}")

    # 1. Micro-batch bronze buffer flush (optional or periodic)
    bronze_records_flushed = 0
    if flush_bronze:
        try:
            bronze_records_flushed = flush_pending_bronze_buffers(symbol=symbol, timeframe=timeframe)
        except Exception as e:
            logger.warning(f"Failed to flush pending bronze buffers: {e}")

    # 2. Upsert single closed candle into Silver
    silver_candle = {
        "MARKET": "CRYPTO",
        "TICKER": symbol,
        "TIMEFRAME": timeframe,
        "TIMESTAMP_UTC": ts_utc,
        "OPEN": float(closed_state.get("open", 0.0)),
        "HIGH": float(closed_state.get("high", 0.0)),
        "LOW": float(closed_state.get("low", 0.0)),
        "CLOSE": float(closed_state.get("close", 0.0)),
        "VOLUME": float(closed_state.get("volume", 0.0)),
        "EXTRA_METRICS": {
            "cvd": float(closed_state.get("cvd", 0.0)),
            "trades_count": int(closed_state.get("trades_count", 0)),
            "source": "BINANCE_WS_CLOSED_BAR"
        }
    }

    silver_upserted = upsert_candles_to_silver([silver_candle])

    # 3. Trigger incremental Gold state recalculation with bounded window
    gold_state = {}
    try:
        gold_state = process_quant_state_for_ticker(
            market="CRYPTO",
            ticker=symbol,
            timeframe=timeframe,
            lookback=300
        )
    except Exception as e:
        logger.error(f"Error recalculating Gold state upon bar close: {e}")

    logger.info(
        f"[PIPELINE COMPLETE] {symbol} {timeframe} - Bronze rows: {bronze_records_flushed}, "
        f"Silver rows: {silver_upserted}, Gold POC: {gold_state.get('volume_profile', {}).get('poc', 'N/A')}"
    )

    return {
        "bronze_flushed": bronze_records_flushed,
        "silver_upserted": silver_upserted,
        "gold_calculated": bool(gold_state)
    }

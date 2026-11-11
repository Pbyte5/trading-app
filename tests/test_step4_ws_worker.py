"""Step 4 Verification Test: Binance WebSocket Worker, CVD Calculation, and Bronze Buffering."""

import asyncio
import gzip
import json
import os
import shutil
from pathlib import Path
from src.pipeline.ws_binance_worker import BinanceWebSocketWorker


def test_ws_worker_streaming_and_buffering():
    """Run worker for 8 seconds, verify live state and compressed Bronze buffer generation."""
    test_buffer_dir = Path("data/test_bronze_buffer")
    test_live_dir = Path("data/test_live_state")

    # Clean test directories if they exist
    if test_buffer_dir.exists():
        shutil.rmtree(test_buffer_dir)
    if test_live_dir.exists():
        shutil.rmtree(test_live_dir)

    worker = BinanceWebSocketWorker(
        symbol="BTCUSDT",
        timeframe="5m",
        buffer_dir=str(test_buffer_dir),
        live_state_dir=str(test_live_dir),
        batch_size=10,
        flush_interval_sec=2.0
    )

    async def run_briefly():
        task = asyncio.create_task(worker.run())
        # Let it stream for 8 seconds to collect aggTrades and klines
        await asyncio.sleep(8.0)
        worker.stop()
        await task

    asyncio.run(run_briefly())

    # 1. Verify live state
    assert worker.live_state["close"] > 0.0, "Close price must be positive"
    assert worker.live_state["trades_count"] > 0, "Should have received aggTrades"
    print(f"Verified live state: Price={worker.live_state['close']} Trades={worker.live_state['trades_count']} CVD={worker.live_state['cvd']}")

    # 2. Verify live JSON file exists on disk
    live_file = test_live_dir / "live_btcusdt.json"
    assert live_file.exists(), f"Live state file {live_file} was not created"
    with open(live_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["symbol"] == "BTCUSDT"
    assert data["close"] > 0.0

    # 3. Verify Bronze buffer files were flushed and compressed
    gz_files = list(test_buffer_dir.rglob("*.jsonl.gz"))
    assert len(gz_files) > 0, "At least one compressed Bronze buffer file must exist"

    first_gz = gz_files[0]
    print(f"Inspecting buffer file: {first_gz} (Size: {first_gz.stat().st_size} bytes)")
    with gzip.open(first_gz, "rt", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) > 0, "Buffer file must contain records"
    first_record = json.loads(lines[0])
    assert "stream" in first_record
    assert "raw_message" in first_record
    print(f"First buffered stream record: {first_record['stream']}")

    # Clean up test directories
    shutil.rmtree(test_buffer_dir, ignore_errors=True)
    shutil.rmtree(test_live_dir, ignore_errors=True)


if __name__ == "__main__":
    print("Running test_ws_worker_streaming_and_buffering...")
    test_ws_worker_streaming_and_buffering()
    print("  [PASS]")
    print("\nALL STEP 4 TESTS PASSED SUCCESSFULLY!")

"""Step 6 Verification Test: UI Live Overlay, Thread Daemon Removal, and Post-Measurement."""

import json
import time
from pathlib import Path
import pandas as pd
from app import get_live_ws_candle, get_candles_with_live_overlay
from src.config.snowflake_session import get_raw_connection


def test_thread_daemon_removed_from_ui():
    """Verify that start_live_daemon is no longer invoked inside app.py."""
    with open("app.py", "r", encoding="utf-8") as f:
        content = f.read()
    assert "start_live_daemon(" not in content, "start_live_daemon thread call must be completely removed from app.py"
    assert "stop_live_daemon(" not in content, "stop_live_daemon call must be completely removed from app.py"


def test_live_ws_overlay_in_candlestick_dataframe():
    """Verify get_candles_with_live_overlay incorporates forming candle from local worker."""
    live_dir = Path("data/live_state")
    live_dir.mkdir(parents=True, exist_ok=True)
    live_file = live_dir / "live_btcusdt.json"

    test_state = {
        "symbol": "BTCUSDT",
        "timeframe": "5m",
        "timestamp_utc": "2026-11-11 23:55:00",
        "open": 89000.0,
        "high": 89500.0,
        "low": 88900.0,
        "close": 89450.0,
        "volume": 25.0,
        "is_closed": False,
        "cvd": 5.25,
        "trades_count": 140,
        "last_updated_epoch": time.time()
    }
    with open(live_file, "w", encoding="utf-8") as f:
        json.dump(test_state, f)

    df = get_candles_with_live_overlay("CRYPTO", "BTCUSDT", "5m", limit=50)
    assert not df.empty, "DataFrame should contain historical + live data"
    last_row = df.iloc[-1]
    assert float(last_row["CLOSE"]) == 89450.0, f"Expected live close 89450.0, got {last_row['CLOSE']}"
    assert float(last_row["OPEN"]) == 89000.0
    print(f"Verified live overlay: Latest candle close={last_row['CLOSE']} Open={last_row['OPEN']}")


def test_warehouse_post_measurement():
    """Verify final warehouse state and parameters."""
    conn = get_raw_connection(query_tag="POST_MEASUREMENT_AUDIT")
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW WAREHOUSES LIKE 'PROYECTO'")
            cols = [c[0].lower() for c in cur.description]
            row = cur.fetchone()
            wh_info = dict(zip(cols, row))

            print("\n" + "=" * 60)
            print("POST-IMPLEMENTATION WAREHOUSE AUDIT:")
            print(f"  Warehouse: {wh_info.get('name')}")
            print(f"  Size: {wh_info.get('size')} (Prior: Medium -> Current: X-Small, 75% cost reduction)")
            print(f"  Auto-Suspend: {wh_info.get('auto_suspend')}s (Allows rapid suspension)")
            print(f"  State: {wh_info.get('state')}")
            print(f"  Resource Monitor: {wh_info.get('resource_monitor')}")
            print("=" * 60)

            assert wh_info.get("size", "").lower() == "x-small"
            assert int(wh_info.get("auto_suspend", 0)) == 60
    finally:
        conn.close()


if __name__ == "__main__":
    print("Running test_thread_daemon_removed_from_ui...")
    test_thread_daemon_removed_from_ui()
    print("  [PASS]")

    print("Running test_live_ws_overlay_in_candlestick_dataframe...")
    test_live_ws_overlay_in_candlestick_dataframe()
    print("  [PASS]")

    print("Running test_warehouse_post_measurement...")
    test_warehouse_post_measurement()
    print("  [PASS]")

    print("\nALL STEP 6 TESTS PASSED SUCCESSFULLY!")

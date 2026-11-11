"""Step 3 Verification Test: Incremental Polling and Parameterized Queries."""

from datetime import datetime, timezone
from src.connectors.snow_loader import (
    get_latest_timestamp_in_silver,
    load_raw_to_bronze,
    upsert_candles_to_silver,
    format_timestamp
)
from src.connectors.equities_client import fetch_and_load_equities
from src.connectors.forex_client import fetch_and_load_forex
from src.connectors.macro_client import check_macro_loaded_today


def test_get_latest_timestamp_in_silver():
    """Verify max timestamp query runs safely with parameter binding."""
    ts = get_latest_timestamp_in_silver("EQUITIES", "NVDA", "1h")
    print(f"Latest timestamp for NVDA:1h: {ts}")
    # May be None or datetime, but must not throw an exception
    if ts is not None:
        assert isinstance(ts, datetime)


def test_parameterized_bronze_insert():
    """Verify load_raw_to_bronze runs with parameterized %s binding."""
    test_records = [
        {"timestamp": "2026-11-11 12:00:00", "test_param": "step3_verification"}
    ]
    loaded = load_raw_to_bronze(
        records=test_records,
        table_name="RAW_MARKET_FEED",
        source="TEST_STEP3",
        market="EQUITIES",
        ticker="STEP3TEST",
        timeframe="5m"
    )
    assert loaded == 1


def test_parameterized_silver_upsert():
    """Verify upsert_candles_to_silver runs with parameterized VALUES placeholders."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    candle = {
        "MARKET": "EQUITIES",
        "TICKER": "STEP3TEST",
        "TIMEFRAME": "5m",
        "TIMESTAMP_UTC": now_str,
        "OPEN": 100.5,
        "HIGH": 101.2,
        "LOW": 99.8,
        "CLOSE": 100.9,
        "VOLUME": 5000.0,
        "EXTRA_METRICS": {"verification_step": 3}
    }
    upserted = upsert_candles_to_silver([candle])
    assert upserted == 1


def test_equities_delta_skipping():
    """Verify subsequent call detects up-to-date state and skips redundant writes."""
    res1 = fetch_and_load_equities(ticker="NVDA", timeframe="1h", days=2)
    assert res1["status"] in ("SYNCED", "UP_TO_DATE", "MARKET_CLOSED")

    # Second consecutive call must be UP_TO_DATE (zero redundant writes to bronze/silver)
    res2 = fetch_and_load_equities(ticker="NVDA", timeframe="1h", days=2)
    assert res2["status"] in ("UP_TO_DATE", "MARKET_CLOSED"), (
        f"Expected UP_TO_DATE on second call, got {res2['status']}"
    )
    assert res2["bronze_loaded"] == 0, "Must not insert redundant rows into Bronze"
    assert res2["silver_upserted"] == 0, "Must not re-merge identical rows into Silver"


if __name__ == "__main__":
    print("Running test_get_latest_timestamp_in_silver...")
    test_get_latest_timestamp_in_silver()
    print("  [PASS]")

    print("Running test_parameterized_bronze_insert...")
    test_parameterized_bronze_insert()
    print("  [PASS]")

    print("Running test_parameterized_silver_upsert...")
    test_parameterized_silver_upsert()
    print("  [PASS]")

    print("Running test_equities_delta_skipping...")
    test_equities_delta_skipping()
    print("  [PASS]")

    print("\nALL STEP 3 TESTS PASSED SUCCESSFULLY!")

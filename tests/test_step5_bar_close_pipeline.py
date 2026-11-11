"""Step 5 Verification Test: Micro-batch Bronze and Bar-Close Silver/Gold Sync."""

from datetime import datetime, timezone
from src.config.snowflake_session import get_raw_connection
from src.pipeline.bar_close_handler import on_bar_closed_pipeline


def test_on_bar_closed_pipeline_execution():
    """Verify on_bar_closed_pipeline upserts closed candle to Silver and calculates Gold."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    closed_state = {
        "symbol": "BTCUSDT",
        "timeframe": "5m",
        "timestamp_utc": now_utc,
        "open": 88000.0,
        "high": 88500.0,
        "low": 87900.0,
        "close": 88400.0,
        "volume": 75.5,
        "is_closed": True,
        "cvd": 12.34,
        "trades_count": 520
    }

    res = on_bar_closed_pipeline(closed_state, flush_bronze=False)
    assert res["silver_upserted"] == 1, "Must upsert exactly 1 closed candle to Silver"
    assert res["gold_calculated"] is True, "Must successfully recalculate Gold state"

    # Verify state in Snowflake GOLD table
    conn = get_raw_connection(query_tag="TEST_STEP5_VERIFY")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MARKET, TICKER, TIMEFRAME, VWAP, POC, CVD
                FROM QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.QUANT_MARKET_STATES
                WHERE MARKET = 'CRYPTO' AND TICKER = 'BTCUSDT' AND TIMEFRAME = '5m'
                ORDER BY TIMESTAMP_UTC DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            assert row is not None, "Gold record must exist for BTCUSDT 5m"
            print(f"Verified Snowflake Gold state: Market={row[0]} Ticker={row[1]} TF={row[2]} VWAP={row[3]} POC={row[4]} CVD={row[5]}")
            assert float(row[3]) > 0.0, "VWAP must be positive"
            assert float(row[4]) > 0.0, "POC must be positive"
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM QUANT_INSTITUTIONAL_DB.SILVER_CLEAN.MARKET_CANDLES WHERE TICKER = 'BTCUSDT' AND TIMESTAMP_UTC = '{now_utc}'")
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    print("Running test_on_bar_closed_pipeline_execution...")
    test_on_bar_closed_pipeline_execution()
    print("  [PASS]")
    print("\nALL STEP 5 TESTS PASSED SUCCESSFULLY!")

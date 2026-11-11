"""Snowflake data loader for Bronze (Raw Variant) and Silver (Normalized Candles) layers.

Implements batched loading and SQL MERGE (Upsert) to avoid duplicate candles.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from src.config.snowflake_session import get_snowpark_session, get_raw_connection


def format_timestamp(ts: Union[str, int, float, datetime]) -> str:
    """Format various timestamp formats to ISO-8601 string for Snowflake."""
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(ts, (int, float)):
        # Support milliseconds or seconds
        if ts > 1e11:  # milliseconds
            return datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return str(ts)


def get_latest_timestamp_in_silver(market: str, ticker: str, timeframe: str) -> Optional[datetime]:
    """Retrieve MAX(TIMESTAMP_UTC) from SILVER_CLEAN.MARKET_CANDLES for incremental loading."""
    conn = get_raw_connection(query_tag="CHECK_MAX_TIMESTAMP")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(TIMESTAMP_UTC)
                FROM QUANT_INSTITUTIONAL_DB.SILVER_CLEAN.MARKET_CANDLES
                WHERE MARKET = %s AND TICKER = %s AND TIMEFRAME = %s
            """, (market.upper(), ticker.upper(), timeframe.lower()))
            row = cur.fetchone()
            if row and row[0] is not None:
                val = row[0]
                if isinstance(val, datetime):
                    return val.replace(tzinfo=timezone.utc) if val.tzinfo is None else val
                try:
                    dt = datetime.fromisoformat(str(val))
                    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
                except Exception:
                    return None
            return None
    except Exception as e:
        print(f"[!] Error consultando max timestamp para {market}:{ticker}: {e}")
        return None
    finally:
        conn.close()


def load_raw_to_bronze(
    records: List[Dict[str, Any]],
    table_name: str = "RAW_MARKET_FEED",
    source: str = "GENERIC",
    market: str = "CRYPTO",
    ticker: str = "UNKNOWN",
    timeframe: str = "1h"
) -> int:
    """Insert raw JSON records into QUANT_INSTITUTIONAL_DB.BRONZE_RAW using parameterized SQL."""
    if not records:
        print(f"[!] Sin registros crudos para cargar en BRONZE_RAW.{table_name}")
        return 0

    print(f"[*] Cargando {len(records)} registros en BRONZE_RAW.{table_name} ({market}:{ticker}:{timeframe})...")

    conn = get_raw_connection(query_tag="BRONZE_RAW_LOAD")
    try:
        ingestion_id = str(uuid.uuid4())
        payload_json = json.dumps(records)

        safe_table = "RAW_MACRO_FEED" if table_name == "RAW_MACRO_FEED" else "RAW_MARKET_FEED"

        if safe_table == "RAW_MACRO_FEED":
            sql = """
            INSERT INTO QUANT_INSTITUTIONAL_DB.BRONZE_RAW.RAW_MACRO_FEED 
            (INGESTION_ID, SOURCE, SERIES_OR_CIK, PERIOD_DATE, INGESTED_AT, RAW_PAYLOAD)
            SELECT %s, %s, %s, CURRENT_DATE(), CURRENT_TIMESTAMP(), PARSE_JSON(%s)
            """
            params = (ingestion_id, source, ticker.upper(), payload_json)
        else:
            sql = """
            INSERT INTO QUANT_INSTITUTIONAL_DB.BRONZE_RAW.RAW_MARKET_FEED 
            (INGESTION_ID, SOURCE, MARKET, TICKER, TIMEFRAME, INGESTED_AT, RAW_PAYLOAD)
            SELECT %s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), PARSE_JSON(%s)
            """
            params = (ingestion_id, source, market.upper(), ticker.upper(), timeframe.lower(), payload_json)

        with conn.cursor() as cur:
            cur.execute(sql, params)

        print(f"[OK] {len(records)} registros cargados exitosamente en BRONZE_RAW.{safe_table}")
        return len(records)
    finally:
        conn.close()


def upsert_candles_to_silver(candles: List[Dict[str, Any]]) -> int:
    """Upsert normalized candles into QUANT_INSTITUTIONAL_DB.SILVER_CLEAN.MARKET_CANDLES using parameterized SQL."""
    if not candles:
        print("[!] Sin velas para actualizar en SILVER_CLEAN.MARKET_CANDLES")
        return 0

    print(f"[*] Ejecutando MERGE parametrizado de {len(candles)} velas en SILVER_CLEAN.MARKET_CANDLES...")

    conn = get_raw_connection(query_tag="SILVER_MERGE_CANDLES")
    try:
        # Batch in chunks of 250 rows to keep parameter bounds clean
        chunk_size = 250
        total_upserted = 0

        for i in range(0, len(candles), chunk_size):
            chunk = candles[i:i + chunk_size]
            row_placeholders = []
            flat_params = []

            for c in chunk:
                market = str(c["MARKET"]).upper()
                ticker = str(c["TICKER"]).upper()
                timeframe = str(c["TIMEFRAME"]).lower()
                ts = format_timestamp(c["TIMESTAMP_UTC"])
                o = float(c["OPEN"])
                h = float(c["HIGH"])
                l = float(c["LOW"])
                cl = float(c["CLOSE"])
                vol = float(c["VOLUME"])
                extra = json.dumps(c.get("EXTRA_METRICS", {}))

                row_placeholders.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                flat_params.extend([market, ticker, timeframe, ts, o, h, l, cl, vol, extra])

            values_clause = ",\n    ".join(row_placeholders)

            merge_sql = f"""
            MERGE INTO QUANT_INSTITUTIONAL_DB.SILVER_CLEAN.MARKET_CANDLES AS target
            USING (
                SELECT 
                    column1 AS MARKET,
                    column2 AS TICKER,
                    column3 AS TIMEFRAME,
                    column4::TIMESTAMP_NTZ AS TIMESTAMP_UTC,
                    column5::FLOAT AS OPEN,
                    column6::FLOAT AS HIGH,
                    column7::FLOAT AS LOW,
                    column8::FLOAT AS CLOSE,
                    column9::FLOAT AS VOLUME,
                    TRY_PARSE_JSON(column10) AS EXTRA_METRICS
                FROM VALUES
                    {values_clause}
            ) AS src
            ON target.MARKET = src.MARKET 
               AND target.TICKER = src.TICKER 
               AND target.TIMEFRAME = src.TIMEFRAME 
               AND target.TIMESTAMP_UTC = src.TIMESTAMP_UTC
            WHEN MATCHED THEN
                UPDATE SET 
                    target.OPEN = src.OPEN,
                    target.HIGH = src.HIGH,
                    target.LOW = src.LOW,
                    target.CLOSE = src.CLOSE,
                    target.VOLUME = src.VOLUME,
                    target.EXTRA_METRICS = src.EXTRA_METRICS
            WHEN NOT MATCHED THEN
                INSERT (MARKET, TICKER, TIMEFRAME, TIMESTAMP_UTC, OPEN, HIGH, LOW, CLOSE, VOLUME, EXTRA_METRICS)
                VALUES (src.MARKET, src.TICKER, src.TIMEFRAME, src.TIMESTAMP_UTC, src.OPEN, src.HIGH, src.LOW, src.CLOSE, src.VOLUME, src.EXTRA_METRICS);
            """

            with conn.cursor() as cur:
                cur.execute(merge_sql, flat_params)

            total_upserted += len(chunk)

        print(f"[OK] {total_upserted} velas sincronizadas correctamente en SILVER_CLEAN.MARKET_CANDLES")
        return total_upserted
    finally:
        conn.close()



def sync_single_asset(market: str, ticker: str, timeframe: str = "5m", days: int = 7) -> int:
    """Ingest a single asset into Bronze/Silver and calculate Gold quant state."""
    market = market.upper()
    ticker = ticker.upper()
    timeframe = timeframe.lower()
    
    print(f"[*] Sincronizando bajo demanda: {market}:{ticker} ({timeframe}, {days} días)...")
    count = 0
    
    from src.connectors.crypto_client import fetch_and_load_crypto
    from src.connectors.equities_client import fetch_and_load_equities
    from src.connectors.forex_client import fetch_and_load_forex
    from src.quant_engine.microstructure import process_quant_state_for_ticker

    if market == "CRYPTO":
        count = fetch_and_load_crypto(ticker=ticker, timeframe=timeframe, days=days)
    elif market == "EQUITIES":
        count = fetch_and_load_equities(ticker=ticker, timeframe=timeframe, days=days)
    elif market == "FOREX":
        count = fetch_and_load_forex(ticker=ticker, timeframe=timeframe, days=days)
    
    try:
        process_quant_state_for_ticker(market=market, ticker=ticker, timeframe=timeframe)
    except Exception as e:
        print(f"[!] Aviso calculando estado Gold para {ticker}: {e}")

    return count


def sync_all_markets(days: int = 7, timeframes: Optional[List[str]] = None) -> None:
    """Run data ingestion across all four asset classes (Crypto, Equities, Forex, Macro) with 7-day lookback."""
    tf_list = timeframes or ["5m", "1h"]
    print("=" * 70)
    print(f"SINCRONIZACIÓN MULTI-MERCADO: ÚLTIMOS {days} DÍAS (TEMPORALIDADES: {', '.join(tf_list)})")
    print("=" * 70)

    from src.connectors.crypto_client import fetch_and_load_crypto
    from src.connectors.equities_client import fetch_and_load_equities
    from src.connectors.forex_client import fetch_and_load_forex
    from src.connectors.macro_client import fetch_and_load_macro

    for tf in tf_list:
        # 1. Crypto (Binance Futures & On-Chain)
        try:
            fetch_and_load_crypto(ticker="BTCUSDT", timeframe=tf, days=days)
        except Exception as e:
            print(f"[ERROR EN CRIPTO {tf}]: {e}")

        # 2. Equities (Alpaca / Public data fallback)
        try:
            fetch_and_load_equities(ticker="NVDA", timeframe=tf, days=days)
        except Exception as e:
            print(f"[ERROR EN ACCIONES {tf}]: {e}")

        # 3. Forex (OANDA / Public fallback)
        try:
            fetch_and_load_forex(ticker="EURUSD", timeframe=tf, days=days)
        except Exception as e:
            print(f"[ERROR EN FOREX {tf}]: {e}")

    # 4. Macro (FRED / Economic Indicators)
    try:
        fetch_and_load_macro()
    except Exception as e:
        print(f"[ERROR EN MACRO]: {e}")

    print("=" * 70)
    print(f"[FINALIZADO] Sincronización de {days} días completada exitosamente.")
    print("=" * 70)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Snowflake Data Loader (Bronze & Silver)")
    parser.add_argument("--sync-all", action="store_true", help="Sincronizar datos de todos los mercados hacia Bronze/Silver")
    parser.add_argument("--days", type=int, default=7, help="Número de días a sincronizar (por defecto 7)")
    parser.add_argument("--timeframe", type=str, default="5m", help="Temporalidad a sincronizar (5m, 1h, 1d)")
    args = parser.parse_args()

    if args.sync_all:
        sync_all_markets(days=args.days, timeframes=[args.timeframe, "1h"] if args.timeframe != "1h" else ["1h"])
    else:
        parser.print_help()

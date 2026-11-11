"""Macroeconomic and news connector (FRED API, Economic Calendar, St. Louis Fed).

Loads interest rates, yield curves, and inflation into the Bronze layer.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import requests

from src.config.settings import get_settings
from src.config.snowflake_session import get_snowpark_session, get_raw_connection
from src.connectors.snow_loader import load_raw_to_bronze


def fetch_fred_series(
    series_id: str = "FEDFUNDS",
    limit: int = 12
) -> List[Dict[str, Any]]:
    """Fetch time series data from St. Louis Fed FRED API."""
    settings = get_settings()

    if not settings.fred_api_key:
        print(f"[!] Sin FRED_API_KEY configurada. Usando datos macro referenciales para {series_id}...")
        return get_fallback_macro_data(series_id)

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": settings.fred_api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": limit
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        observations = data.get("observations", [])

        records = []
        for obs in observations:
            val = obs.get("value")
            if val == ".":
                continue
            records.append({
                "series_id": series_id,
                "date": obs.get("date"),
                "value": float(val) if val else None
            })
        return records
    except Exception as e:
        print(f"[!] Error consultando FRED API para {series_id}: {e}")
        return get_fallback_macro_data(series_id)


def get_fallback_macro_data(series_id: str) -> List[Dict[str, Any]]:
    """Fallback macroeconomic benchmark data."""
    benchmarks = {
        "FEDFUNDS": 5.33,
        "T10Y2Y": 0.15,     # 10-Year minus 2-Year Treasury spread
        "VIXCLS": 14.80,    # CBOE Volatility Index
        "CPIAUCSL": 314.5,  # US CPI Urban Consumers
        "M2SL": 21000.0     # M2 Money Stock ($B)
    }
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return [{
        "series_id": series_id,
        "date": today,
        "value": benchmarks.get(series_id, 0.0),
        "source": "MACRO_REFERENCE_DATA"
    }]


def check_macro_loaded_today() -> bool:
    """Return True if macro data has already been ingested today into BRONZE_RAW.RAW_MACRO_FEED."""
    conn = get_raw_connection(query_tag="CHECK_MACRO_DAILY")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM QUANT_INSTITUTIONAL_DB.BRONZE_RAW.RAW_MACRO_FEED
                WHERE PERIOD_DATE >= CURRENT_DATE()
            """)
            cnt = cur.fetchone()[0]
            return cnt > 0
    except Exception as e:
        print(f"[!] Error verificando ingesta macro diaria: {e}")
        return False
    finally:
        conn.close()


def fetch_and_load_macro(force: bool = False) -> int:
    """Fetch key macroeconomic series (FEDFUNDS, T10Y2Y, VIX) 1x daily into Bronze."""
    if not force and check_macro_loaded_today():
        print("[MACRO] Datos macroeconómicos ya fueron actualizados hoy. Omitiendo ingesta para ahorrar créditos Snowflake.")
        return 0

    print("\n[MACRO] Consultando métricas macroeconómicas de FRED...")
    series_list = ["FEDFUNDS", "T10Y2Y", "VIXCLS", "CPIAUCSL"]

    total_records = 0
    all_macro_records = []

    for sid in series_list:
        data = fetch_fred_series(series_id=sid, limit=6)
        all_macro_records.extend(data)

    if all_macro_records:
        total_records = load_raw_to_bronze(
            records=all_macro_records,
            table_name="RAW_MACRO_FEED",
            source="FRED_STLOUIS",
            market="MACRO",
            ticker="US_ECONOMY",
            timeframe="1d"
        )

    return total_records


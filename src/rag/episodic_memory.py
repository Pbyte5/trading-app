"""Episodic memory management for historical trade setups and market states."""

import json
import uuid
from typing import Any, Dict, List, Optional

from src.config.snowflake_session import get_snowpark_session, get_raw_connection


def record_episodic_state(
    market: str,
    ticker: str,
    timeframe: str,
    timestamp_utc: str,
    regime: str,
    state_summary: str,
    outcome_label: str,
    metadata: Optional[Dict[str, Any]] = None
) -> str:
    """Insert a new historical trade setup into GOLD_QUANT_RAG.EPISODIC_MEMORY_STATES with Cortex vector embedding."""
    try:
        session = get_snowpark_session()
        use_snowpark = True
    except Exception:
        session = get_raw_connection()
        use_snowpark = False

    episode_id = str(uuid.uuid4())
    summary_escaped = state_summary.replace("'", "''")
    meta_json = json.dumps(metadata or {}).replace("'", "''")

    sql = f"""
    INSERT INTO QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.EPISODIC_MEMORY_STATES
    (EPISODE_ID, MARKET, TICKER, TIMEFRAME, TIMESTAMP_UTC, REGIME, STATE_SUMMARY, STATE_VECTOR, OUTCOME_LABEL, METADATA)
    SELECT
        '{episode_id}',
        '{market.upper()}',
        '{ticker.upper()}',
        '{timeframe.lower()}',
        '{timestamp_utc}'::TIMESTAMP_NTZ,
        '{regime.upper()}',
        '{summary_escaped}',
        SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', '{summary_escaped}'),
        '{outcome_label.upper()}',
        PARSE_JSON('{meta_json}')
    """

    try:
        # Try inserting with native Cortex embedding
        try:
            if use_snowpark:
                session.sql(sql).collect()
            else:
                with session.cursor() as cur:
                    cur.execute(sql)
            return episode_id
        except Exception:
            # Fallback to inserting with NULL vector if Cortex is restricted (e.g. trial accounts)
            fallback_sql = f"""
            INSERT INTO QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.EPISODIC_MEMORY_STATES
            (EPISODE_ID, MARKET, TICKER, TIMEFRAME, TIMESTAMP_UTC, REGIME, STATE_SUMMARY, STATE_VECTOR, OUTCOME_LABEL, METADATA)
            SELECT
                '{episode_id}',
                '{market.upper()}',
                '{ticker.upper()}',
                '{timeframe.lower()}',
                '{timestamp_utc}'::TIMESTAMP_NTZ,
                '{regime.upper()}',
                '{summary_escaped}',
                NULL,
                '{outcome_label.upper()}',
                PARSE_JSON('{meta_json}')
            """
            if use_snowpark:
                session.sql(fallback_sql).collect()
            else:
                with session.cursor() as cur:
                    cur.execute(fallback_sql)
            return episode_id
    finally:
        session.close()


def seed_reference_episodes() -> int:
    """Seed benchmark episodic market events for initial historical pattern matching."""
    benchmarks = [
        {
            "market": "CRYPTO",
            "ticker": "BTCUSDT",
            "timeframe": "1h",
            "timestamp_utc": "2024-03-05 14:00:00",
            "regime": "TRENDING_MOMENTUM",
            "state_summary": "Ruptura agresiva de máximo histórico previo en 69k. CVD con absorción masiva y liquidaciones de cortos masivas. Hurst=0.68.",
            "outcome_label": "BREAKOUT_SUCCESS",
            "metadata": {"risk_reward_realized": 3.4, "max_adverse_excursion_pct": 0.8}
        },
        {
            "market": "CRYPTO",
            "ticker": "BTCUSDT",
            "timeframe": "1h",
            "timestamp_utc": "2024-05-10 18:00:00",
            "regime": "MEAN_REVERSION",
            "state_summary": "Rebote exacto en banda de VWAP -2.5 sigma con divergencia alcista clara de CVD en soporte de 60k. Hurst=0.41.",
            "outcome_label": "MEAN_REVERSION_WIN",
            "metadata": {"risk_reward_realized": 2.8, "target_reached": "POC"}
        },
        {
            "market": "EQUITIES",
            "ticker": "NVDA",
            "timeframe": "1d",
            "timestamp_utc": "2024-02-22 09:30:00",
            "regime": "TRENDING_MOMENTUM",
            "state_summary": "Post-earnings gap up por encima del Value Area High previo. Fuerte aceleración de volumen institucional y compresión de volatilidad superada. Hurst=0.72.",
            "outcome_label": "TREND_CONTINUATION_WIN",
            "metadata": {"risk_reward_realized": 4.1, "post_gap_drift_days": 12}
        },
        {
            "market": "FOREX",
            "ticker": "EURUSD",
            "timeframe": "1h",
            "timestamp_utc": "2024-04-12 12:30:00",
            "regime": "TRENDING_MOMENTUM",
            "state_summary": "Barrido de liquidez de Equal Highs en sesión europea seguido por desplome tras datos de inflación de EE. UU. superiores a lo previsto. Rendimiento US10Y repuntando.",
            "outcome_label": "LIQUIDITY_SWEEP_SHORT_WIN",
            "metadata": {"risk_reward_realized": 2.6}
        }
    ]

    count = 0
    for b in benchmarks:
        try:
            record_episodic_state(**b)
            count += 1
        except Exception as e:
            print(f"[!] Error sembrando episodio {b['ticker']}: {e}")
    return count


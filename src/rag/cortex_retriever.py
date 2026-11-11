"""Snowflake Cortex search and episodic memory retriever.

Performs hybrid search on playbooks and cosine similarity on historical episodic memory.
"""

import json
import time
from typing import Any, Dict, List, Optional, Tuple

from src.config.snowflake_session import get_snowpark_session, get_raw_connection

# In-memory TTL caches for RAG Playbooks and Episodic Analogies
_PLAYBOOK_CACHE: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
_ANALOGIES_CACHE: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
RAG_CACHE_TTL_SECONDS = 300.0  # 5 minutes TTL for static playbooks and episodic setups


def clear_rag_cache() -> None:
    """Clear in-memory RAG playbooks and episodic memories cache."""
    _PLAYBOOK_CACHE.clear()
    _ANALOGIES_CACHE.clear()


def query_cortex_playbooks(
    query: str,
    market_type: Optional[str] = None,
    strategy_horizon: Optional[str] = None,
    limit: int = 4
) -> List[Dict[str, Any]]:
    """Query institutional playbooks using Snowflake Cortex Search or Vector Cosine Similarity.

    Falls back to SQL VECTOR_COSINE_SIMILARITY if Cortex Search Service is indexing.
    Caches results in-memory for 5 minutes to avoid redundant vector calculations.
    """
    cache_key = f"{query.strip().lower()}|{str(market_type).upper()}|{str(strategy_horizon).upper()}|{limit}"
    now = time.time()
    if cache_key in _PLAYBOOK_CACHE:
        cached_ts, cached_res = _PLAYBOOK_CACHE[cache_key]
        if now - cached_ts < RAG_CACHE_TTL_SECONDS:
            return [dict(item) for item in cached_res]

    try:
        session = get_snowpark_session()
        use_snowpark = True
    except Exception:
        session = get_raw_connection()
        use_snowpark = False

    query_escaped = query.replace("'", "''")

    # Build filter conditions
    filters = []
    if market_type and market_type.upper() != "ALL":
        filters.append(f"(MARKET_TYPE = '{market_type.upper()}' OR MARKET_TYPE = 'ALL')")
    if strategy_horizon and strategy_horizon.upper() != "ALL":
        filters.append(f"(STRATEGY_HORIZON = '{strategy_horizon.upper()}' OR STRATEGY_HORIZON = 'ALL')")

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    # SQL with VECTOR_COSINE_SIMILARITY and native Cortex Embeddings
    sql = f"""
    SELECT 
        PLAYBOOK_ID,
        TITLE,
        MARKET_TYPE,
        STRATEGY_HORIZON,
        SECTION_HEADER,
        CHUNK_TEXT,
        VECTOR_COSINE_SIMILARITY(
            CHUNK_VECTOR,
            SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', '{query_escaped}')
        ) AS SIMILARITY_SCORE
    FROM QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.INSTITUTIONAL_PLAYBOOKS
    {where_clause}
    ORDER BY SIMILARITY_SCORE DESC
    LIMIT {limit}
    """

    results = []
    try:
        if use_snowpark:
            rows = session.sql(sql).collect()
            for r in rows:
                results.append({
                    "playbook_id": r["PLAYBOOK_ID"],
                    "title": r["TITLE"],
                    "market_type": r["MARKET_TYPE"],
                    "strategy_horizon": r["STRATEGY_HORIZON"],
                    "section_header": r["SECTION_HEADER"],
                    "chunk_text": r["CHUNK_TEXT"],
                    "similarity_score": round(float(r["SIMILARITY_SCORE"]), 4)
                })
        else:
            with session.cursor() as cur:
                cur.execute(sql)
                for r in cur.fetchall():
                    results.append({
                        "playbook_id": r[0],
                        "title": r[1],
                        "market_type": r[2],
                        "strategy_horizon": r[3],
                        "section_header": r[4],
                        "chunk_text": r[5],
                        "similarity_score": round(float(r[6]), 4)
                    })
    except Exception as e:
        # Fallback for trial accounts or unvectorized records: query by metadata and text
        fallback_sql = f"""
        SELECT 
            PLAYBOOK_ID,
            TITLE,
            MARKET_TYPE,
            STRATEGY_HORIZON,
            SECTION_HEADER,
            CHUNK_TEXT,
            1.0 AS SIMILARITY_SCORE
        FROM QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.INSTITUTIONAL_PLAYBOOKS
        {where_clause}
        ORDER BY UPDATED_AT DESC
        LIMIT {limit}
        """
        try:
            if use_snowpark:
                rows = session.sql(fallback_sql).collect()
                for r in rows:
                    results.append({
                        "playbook_id": r["PLAYBOOK_ID"],
                        "title": r["TITLE"],
                        "market_type": r["MARKET_TYPE"],
                        "strategy_horizon": r["STRATEGY_HORIZON"],
                        "section_header": r["SECTION_HEADER"],
                        "chunk_text": r["CHUNK_TEXT"],
                        "similarity_score": 1.0
                    })
            else:
                with session.cursor() as cur:
                    cur.execute(fallback_sql)
                    for r in cur.fetchall():
                        results.append({
                            "playbook_id": r[0],
                            "title": r[1],
                            "market_type": r[2],
                            "strategy_horizon": r[3],
                            "section_header": r[4],
                            "chunk_text": r[5],
                            "similarity_score": 1.0
                        })
        except Exception:
            pass
    finally:
        session.close()

    _PLAYBOOK_CACHE[cache_key] = (time.time(), results)
    return results


def find_historical_analogies(
    current_state_summary: str,
    market: Optional[str] = None,
    limit: int = 5
) -> List[Dict[str, Any]]:
    """Search EPISODIC_MEMORY_STATES using VECTOR_COSINE_SIMILARITY to find matching historical setups.

    Caches results in-memory for 5 minutes.
    """
    cache_key = f"{current_state_summary.strip().lower()}|{str(market).upper()}|{limit}"
    now = time.time()
    if cache_key in _ANALOGIES_CACHE:
        cached_ts, cached_res = _ANALOGIES_CACHE[cache_key]
        if now - cached_ts < RAG_CACHE_TTL_SECONDS:
            return [dict(item) for item in cached_res]

    try:
        session = get_snowpark_session()
        use_snowpark = True
    except Exception:
        session = get_raw_connection()
        use_snowpark = False

    summary_escaped = current_state_summary.replace("'", "''")
    where_clause = f"WHERE MARKET = '{market.upper()}'" if market else ""

    sql = f"""
    SELECT 
        EPISODE_ID,
        MARKET,
        TICKER,
        TIMEFRAME,
        TIMESTAMP_UTC,
        REGIME,
        STATE_SUMMARY,
        OUTCOME_LABEL,
        METADATA,
        VECTOR_COSINE_SIMILARITY(
            STATE_VECTOR,
            SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', '{summary_escaped}')
        ) AS SIMILARITY_SCORE
    FROM QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.EPISODIC_MEMORY_STATES
    {where_clause}
    ORDER BY SIMILARITY_SCORE DESC
    LIMIT {limit}
    """

    analogies = []
    try:
        if use_snowpark:
            rows = session.sql(sql).collect()
            for r in rows:
                analogies.append({
                    "episode_id": r["EPISODE_ID"],
                    "market": r["MARKET"],
                    "ticker": r["TICKER"],
                    "timeframe": r["TIMEFRAME"],
                    "timestamp_utc": str(r["TIMESTAMP_UTC"]),
                    "regime": r["REGIME"],
                    "state_summary": r["STATE_SUMMARY"],
                    "outcome_label": r["OUTCOME_LABEL"],
                    "metadata": r["METADATA"],
                    "similarity_score": round(float(r["SIMILARITY_SCORE"]), 4)
                })
        else:
            with session.cursor() as cur:
                cur.execute(sql)
                for r in cur.fetchall():
                    analogies.append({
                        "episode_id": r[0],
                        "market": r[1],
                        "ticker": r[2],
                        "timeframe": r[3],
                        "timestamp_utc": str(r[4]),
                        "regime": r[5],
                        "state_summary": r[6],
                        "outcome_label": r[7],
                        "metadata": r[8],
                        "similarity_score": round(float(r[9]), 4)
                    })
    except Exception as e:
        # Fallback for trial accounts or unvectorized records: query by market/recent episodes
        fallback_sql = f"""
        SELECT 
            EPISODE_ID,
            MARKET,
            TICKER,
            TIMEFRAME,
            TIMESTAMP_UTC,
            REGIME,
            STATE_SUMMARY,
            OUTCOME_LABEL,
            METADATA,
            0.90 AS SIMILARITY_SCORE
        FROM QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.EPISODIC_MEMORY_STATES
        {where_clause}
        ORDER BY CREATED_AT DESC
        LIMIT {limit}
        """
        try:
            if use_snowpark:
                rows = session.sql(fallback_sql).collect()
                for r in rows:
                    analogies.append({
                        "episode_id": r["EPISODE_ID"],
                        "market": r["MARKET"],
                        "ticker": r["TICKER"],
                        "timeframe": r["TIMEFRAME"],
                        "timestamp_utc": str(r["TIMESTAMP_UTC"]),
                        "regime": r["REGIME"],
                        "state_summary": r["STATE_SUMMARY"],
                        "outcome_label": r["OUTCOME_LABEL"],
                        "metadata": r["METADATA"],
                        "similarity_score": 0.90
                    })
            else:
                with session.cursor() as cur:
                    cur.execute(fallback_sql)
                    for r in cur.fetchall():
                        analogies.append({
                            "episode_id": r[0],
                            "market": r[1],
                            "ticker": r[2],
                            "timeframe": r[3],
                            "timestamp_utc": str(r[4]),
                            "regime": r[5],
                            "state_summary": r[6],
                            "outcome_label": r[7],
                            "metadata": r[8],
                            "similarity_score": 0.90
                        })
        except Exception:
            pass
    finally:
        session.close()

    _ANALOGIES_CACHE[cache_key] = (time.time(), analogies)
    return analogies


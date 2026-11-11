"""Model Context Protocol (MCP) server & tool interface connected to Snowflake GOLD_QUANT_RAG."""

import json
import time
from typing import Any, Dict, List, Optional, Tuple

from src.config.snowflake_session import get_snowpark_session, get_raw_connection
from src.rag.cortex_retriever import query_cortex_playbooks, find_historical_analogies, clear_rag_cache

# In-memory tool cache for quant market states
_GOLD_STATE_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
GOLD_CACHE_TTL_SECONDS = 30.0

try:
    try:
        from mcp.server.mcpserver import MCPServer as FastMCP
    except ImportError:
        from mcp.server.fastmcp import FastMCP
    mcp = FastMCP("snowflake-quant-tools")
except Exception:
    mcp = None


def clear_gold_tools_cache() -> None:
    """Clear in-memory quant tools cache and RAG cache."""
    _GOLD_STATE_CACHE.clear()
    clear_rag_cache()


def get_gold_market_state(ticker: str, timeframe: str = "1h", market: Optional[str] = None) -> Dict[str, Any]:
    """Retrieve the latest quantitative state (VWAP, Volume Profile, Hurst, Regime) from Snowflake GOLD layer.

    Includes a 30-second tool cache to avoid redundant network roundtrips.
    """
    cache_key = f"{ticker.upper()}|{timeframe.lower()}|{str(market).upper() if market else ''}"
    now = time.time()
    if cache_key in _GOLD_STATE_CACHE:
        cached_ts, cached_state = _GOLD_STATE_CACHE[cache_key]
        if now - cached_ts < GOLD_CACHE_TTL_SECONDS:
            return dict(cached_state)

    session = None
    try:
        try:
            session = get_snowpark_session()
            use_snowpark = True
        except Exception:
            session = get_raw_connection()
            use_snowpark = False

        market_clause = f"AND MARKET = '{market.upper()}'" if market else ""
        sql = f"""
        SELECT 
            MARKET, TICKER, TIMEFRAME, TIMESTAMP_UTC,
            VWAP, VWAP_UPPER_1, VWAP_LOWER_1, VWAP_UPPER_2, VWAP_LOWER_2, VWAP_UPPER_3, VWAP_LOWER_3,
            POC, VAH, VAL, CVD, ATR, HURST_EXPONENT, REGIME, STATE_JSON
        FROM QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.QUANT_MARKET_STATES
        WHERE TICKER = '{ticker.upper()}'
          AND TIMEFRAME = '{timeframe.lower()}'
          {market_clause}
        ORDER BY TIMESTAMP_UTC DESC
        LIMIT 1
        """

        state_data = {}
        if use_snowpark:
            rows = session.sql(sql).collect()
            if rows:
                r = rows[0]
                state_data = json.loads(r["STATE_JSON"]) if isinstance(r["STATE_JSON"], str) else r["STATE_JSON"]
        else:
            with session.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                if row:
                    raw_json = row[18]
                    state_data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json

        if state_data and "error" not in state_data:
            _GOLD_STATE_CACHE[cache_key] = (time.time(), state_data)

        return state_data or {
            "error": f"No se encontró estado GOLD para {ticker}:{timeframe}.",
            "ticker": ticker,
            "timeframe": timeframe
        }
    except Exception as e:
        print(f"[!] Aviso consultando GOLD layer en Snowflake ({e}). Usando estimador de resiliencia.")
        return {}
    finally:
        if session:
            try:
                session.close()
            except Exception:
                pass


def mcp_query_playbooks(query: str, market_type: str = "ALL", strategy_horizon: str = "ALL") -> List[Dict[str, Any]]:
    """MCP tool wrapper to retrieve playbook chunks via Cortex hybrid search."""
    return query_cortex_playbooks(query=query, market_type=market_type, strategy_horizon=strategy_horizon)


def mcp_find_historical_analogies(current_state_summary: str, market: Optional[str] = None) -> List[Dict[str, Any]]:
    """MCP tool wrapper to match historical episodic analogies via Snowflake cosine vector similarity."""
    return find_historical_analogies(current_state_summary=current_state_summary, market=market)


# Register tools with FastMCP if available
if mcp is not None:
    @mcp.tool(name="get_gold_market_state")
    def tool_get_gold_market_state(ticker: str, timeframe: str = "1h", market: Optional[str] = None) -> str:
        """Consultar métricas cuantitativas calculadas en Snowflake (VWAP, Volume Profile, Hurst, Régimen)."""
        return json.dumps(get_gold_market_state(ticker=ticker, timeframe=timeframe, market=market))

    @mcp.tool(name="query_cortex_playbooks")
    def tool_query_cortex_playbooks(query: str, market_type: str = "ALL", strategy_horizon: str = "ALL") -> str:
        """Búsqueda semántica híbrida en playbooks institucionales usando Snowflake Cortex."""
        return json.dumps(mcp_query_playbooks(query=query, market_type=market_type, strategy_horizon=strategy_horizon))

    @mcp.tool(name="find_historical_analogies")
    def tool_find_historical_analogies(current_state_summary: str, market: Optional[str] = None) -> str:
        """Búsqueda de memoria episódica histórica mediante VECTOR_COSINE_SIMILARITY."""
        return json.dumps(mcp_find_historical_analogies(current_state_summary=current_state_summary, market=market))


if __name__ == "__main__":
    if mcp is not None:
        print("[*] Iniciando Servidor MCP de Herramientas Cuantitativas de Snowflake (stdio)...")
        mcp.run()
    else:
        print("[!] FastMCP no disponible.")



-- ============================================================================
-- SCRIPT 03: Snowflake Cortex Search Service (RAG Híbrido)
-- Servicio de búsqueda sobre INSTITUTIONAL_PLAYBOOKS con filtrado por metadatos
-- ============================================================================

USE DATABASE QUANT_INSTITUTIONAL_DB;
USE SCHEMA GOLD_QUANT_RAG;
USE WAREHOUSE QUANT_RAG_WH;

-- 1. Creación del Servicio de Búsqueda Cortex
-- Indexa CHUNK_TEXT y habilita filtros exactos por MARKET_TYPE y STRATEGY_HORIZON
CREATE OR REPLACE CORTEX SEARCH SERVICE PLAYBOOK_SEARCH_SERVICE
    ON CHUNK_TEXT
    ATTRIBUTES MARKET_TYPE, STRATEGY_HORIZON, TITLE
    WAREHOUSE = QUANT_RAG_WH
    TARGET_LAG = '1 hour'
    COMMENT = 'Servicio de búsqueda híbrida semántica y léxica para playbooks institucionales'
    AS (
        SELECT
            PLAYBOOK_ID,
            TITLE,
            MARKET_TYPE,
            STRATEGY_HORIZON,
            SECTION_HEADER,
            CHUNK_TEXT,
            METADATA
        FROM QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.INSTITUTIONAL_PLAYBOOKS
    );

-- ============================================================================
-- 2. Consultas de Verificación y Ejemplos de Búsqueda Vectorial Nativa
-- ============================================================================

-- Ejemplo A: Generación de embeddings con Cortex directamente en SQL
-- SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', 'Estrategia de barrido de liquidez en VWAP');

-- Ejemplo B: Búsqueda de Memoria Episódica usando similitud coseno
-- SELECT 
--     EPISODE_ID,
--     TICKER,
--     REGIME,
--     STATE_SUMMARY,
--     OUTCOME_LABEL,
--     VECTOR_COSINE_SIMILARITY(
--         STATE_VECTOR, 
--         SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', 'Alta volatilidad con Hurst > 0.60 y ruptura de VAH')
--     ) AS SIMILARITY_SCORE
-- FROM QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.EPISODIC_MEMORY_STATES
-- ORDER BY SIMILARITY_SCORE DESC
-- LIMIT 5;


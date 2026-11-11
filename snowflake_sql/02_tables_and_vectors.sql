-- ============================================================================
-- SCRIPT 02: Tablas Medallion (Bronze, Silver, Gold) y Tablas Vectoriales
-- Esquemas: BRONZE_RAW, SILVER_CLEAN, GOLD_QUANT_RAG
-- ============================================================================

USE DATABASE QUANT_INSTITUTIONAL_DB;

-- ============================================================================
-- 1. CAPA BRONZE: Ingesta cruda en columnas VARIANT
-- ============================================================================
USE SCHEMA BRONZE_RAW;

-- Tabla para almacenar payloads crudos de velas y trades (Binance, Alpaca, OANDA)
CREATE TABLE IF NOT EXISTS RAW_MARKET_FEED (
    INGESTION_ID VARCHAR DEFAULT UUID_STRING(),
    SOURCE VARCHAR(50) NOT NULL,            -- 'BINANCE', 'ALPACA', 'OANDA', etc.
    MARKET VARCHAR(20) NOT NULL,            -- 'CRYPTO', 'EQUITIES', 'FOREX'
    TICKER VARCHAR(30) NOT NULL,            -- 'BTCUSDT', 'NVDA', 'EURUSD'
    TIMEFRAME VARCHAR(10) NOT NULL,         -- '1m', '5m', '1h', '1d'
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RAW_PAYLOAD VARIANT NOT NULL,
    PRIMARY KEY (INGESTION_ID)
);

-- Tabla para profundidad de libro de órdenes (Order Book L2) y liquidaciones
CREATE TABLE IF NOT EXISTS RAW_ORDERBOOK_FEED (
    INGESTION_ID VARCHAR DEFAULT UUID_STRING(),
    SOURCE VARCHAR(50) NOT NULL,            -- 'BINANCE_FUTURES', 'COINGLASS', 'OANDA_BOOK'
    MARKET VARCHAR(20) NOT NULL,
    TICKER VARCHAR(30) NOT NULL,
    SNAPSHOT_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RAW_PAYLOAD VARIANT NOT NULL,
    PRIMARY KEY (INGESTION_ID)
);

-- Tabla para reportes macro y fundamentales (FRED, CFTC COT, SEC EDGAR)
CREATE TABLE IF NOT EXISTS RAW_MACRO_FEED (
    INGESTION_ID VARCHAR DEFAULT UUID_STRING(),
    SOURCE VARCHAR(50) NOT NULL,            -- 'FRED', 'CFTC_COT', 'SEC_EDGAR'
    SERIES_OR_CIK VARCHAR(50) NOT NULL,     -- 'FEDFUNDS', 'T10Y2Y', '0001045810'
    PERIOD_DATE DATE,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RAW_PAYLOAD VARIANT NOT NULL,
    PRIMARY KEY (INGESTION_ID)
);

-- Tabla para noticias financieras y calendario económico (Finnhub, Tavily)
CREATE TABLE IF NOT EXISTS RAW_NEWS_FEED (
    INGESTION_ID VARCHAR DEFAULT UUID_STRING(),
    SOURCE VARCHAR(50) NOT NULL,
    TICKER VARCHAR(30),
    PUBLISHED_AT TIMESTAMP_NTZ,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RAW_PAYLOAD VARIANT NOT NULL,
    PRIMARY KEY (INGESTION_ID)
);

-- ============================================================================
-- 2. CAPA SILVER: Series de tiempo normalizadas y deduplicadas
-- ============================================================================
USE SCHEMA SILVER_CLEAN;

-- Tabla principal de velas normalizadas con clustering para alta eficiencia
CREATE TABLE IF NOT EXISTS MARKET_CANDLES (
    MARKET VARCHAR(20) NOT NULL,            -- 'CRYPTO', 'EQUITIES', 'FOREX'
    TICKER VARCHAR(30) NOT NULL,
    TIMEFRAME VARCHAR(10) NOT NULL,
    TIMESTAMP_UTC TIMESTAMP_NTZ NOT NULL,
    OPEN NUMBER(18, 8) NOT NULL,
    HIGH NUMBER(18, 8) NOT NULL,
    LOW NUMBER(18, 8) NOT NULL,
    CLOSE NUMBER(18, 8) NOT NULL,
    VOLUME NUMBER(24, 8) NOT NULL,
    EXTRA_METRICS VARIANT,                  -- CVD, número de trades, open interest, etc.
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_MARKET_CANDLES PRIMARY KEY (MARKET, TICKER, TIMEFRAME, TIMESTAMP_UTC)
)
CLUSTER BY (MARKET, TICKER, TIMEFRAME, TIMESTAMP_UTC);

-- ============================================================================
-- 3. CAPA GOLD: Feature Store Cuantitativo y Base Vectorial (Cortex)
-- ============================================================================
USE SCHEMA GOLD_QUANT_RAG;

-- Tabla 1: Estados de microestructura y régimen calculados por el motor cuantitativo
CREATE TABLE IF NOT EXISTS QUANT_MARKET_STATES (
    STATE_ID VARCHAR DEFAULT UUID_STRING(),
    MARKET VARCHAR(20) NOT NULL,
    TICKER VARCHAR(30) NOT NULL,
    TIMEFRAME VARCHAR(10) NOT NULL,
    TIMESTAMP_UTC TIMESTAMP_NTZ NOT NULL,
    
    -- Métricas de Microestructura
    VWAP NUMBER(18, 8),
    VWAP_UPPER_1 NUMBER(18, 8),
    VWAP_LOWER_1 NUMBER(18, 8),
    VWAP_UPPER_2 NUMBER(18, 8),
    VWAP_LOWER_2 NUMBER(18, 8),
    VWAP_UPPER_3 NUMBER(18, 8),
    VWAP_LOWER_3 NUMBER(18, 8),
    POC NUMBER(18, 8),                     -- Point of Control (Perfil de Volumen)
    VAH NUMBER(18, 8),                     -- Value Area High (70% volumen)
    VAL NUMBER(18, 8),                     -- Value Area Low (70% volumen)
    CVD NUMBER(24, 8),                     -- Cumulative Volume Delta
    
    -- Métricas de Régimen y Riesgo
    ATR NUMBER(18, 8),
    HURST_EXPONENT NUMBER(8, 4),           -- <0.45 Reversión a media, ~0.5 Random Walk, >0.55 Tendencial
    REGIME VARCHAR(30),                    -- 'MEAN_REVERSION', 'RANDOM_WALK', 'TRENDING_MOMENTUM'
    
    -- Estado condensado para rápido consumo por el LLM / Agentes
    STATE_JSON VARIANT NOT NULL,
    CALCULATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_QUANT_MARKET_STATES PRIMARY KEY (MARKET, TICKER, TIMEFRAME, TIMESTAMP_UTC)
);

-- Tabla 2: Playbooks institucionales con embeddings de 1024 dimensiones
CREATE TABLE IF NOT EXISTS INSTITUTIONAL_PLAYBOOKS (
    PLAYBOOK_ID VARCHAR DEFAULT UUID_STRING(),
    TITLE VARCHAR(100) NOT NULL,
    MARKET_TYPE VARCHAR(30) NOT NULL,      -- 'CRYPTO', 'EQUITIES', 'FOREX', 'MACRO', 'ALL'
    STRATEGY_HORIZON VARCHAR(30) NOT NULL, -- 'INTRADAY', 'SWING_HOLDER', 'ALL'
    SECTION_HEADER VARCHAR(200) NOT NULL,
    CHUNK_TEXT VARCHAR NOT NULL,
    CHUNK_VECTOR VECTOR(FLOAT, 1024),      -- Generado con SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', ...)
    METADATA VARIANT,                      -- Tags, fórmulas, condiciones de activación
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_INSTITUTIONAL_PLAYBOOKS PRIMARY KEY (PLAYBOOK_ID)
);

-- Tabla 3: Memoria episódica de mercado con embeddings de estados históricos
CREATE TABLE IF NOT EXISTS EPISODIC_MEMORY_STATES (
    EPISODE_ID VARCHAR DEFAULT UUID_STRING(),
    MARKET VARCHAR(20) NOT NULL,
    TICKER VARCHAR(30) NOT NULL,
    TIMEFRAME VARCHAR(10) NOT NULL,
    TIMESTAMP_UTC TIMESTAMP_NTZ NOT NULL,
    REGIME VARCHAR(30) NOT NULL,
    STATE_SUMMARY VARCHAR NOT NULL,         -- Resumen textual del estado cuantitativo
    STATE_VECTOR VECTOR(FLOAT, 1024),      -- Embedding para VECTOR_COSINE_SIMILARITY
    OUTCOME_LABEL VARCHAR(50),             -- 'BREAKOUT_SUCCESS', 'MEAN_REVERSION_WIN', etc.
    METADATA VARIANT,                      -- Contexto macro, drawdown posterior, R:R alcanzado
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_EPISODIC_MEMORY PRIMARY KEY (EPISODE_ID)
);


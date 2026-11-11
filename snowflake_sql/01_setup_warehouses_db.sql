-- ============================================================================
-- SCRIPT 01: Setup Warehouses, Database y Esquemas Medallion
-- Arquitectura: QUANT_INSTITUTIONAL_DB (BRONZE_RAW, SILVER_CLEAN, GOLD_QUANT_RAG)
-- Warehouses: INGEST_WH, QUANT_RAG_WH (XSMALL, auto-suspend 60s, auto-resume)
-- ============================================================================

-- 1. Creación de Virtual Warehouses optimizados en consumo de créditos
CREATE WAREHOUSE IF NOT EXISTS INGEST_WH
    WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse dedicado a la ingesta cruda hacia la capa BRONZE_RAW y transformaciones iniciales';

CREATE WAREHOUSE IF NOT EXISTS QUANT_RAG_WH
    WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse dedicado al motor cuantitativo Snowpark, Cortex Search y consultas RAG';

-- 2. Creación de la Base de Datos Institucional
CREATE DATABASE IF NOT EXISTS QUANT_INSTITUTIONAL_DB
    COMMENT = 'Base de datos central del sistema RAG Cuantitativo Multi-Mercado';

USE DATABASE QUANT_INSTITUTIONAL_DB;

-- 3. Creación de los Esquemas de la Arquitectura Medallion
CREATE SCHEMA IF NOT EXISTS QUANT_INSTITUTIONAL_DB.BRONZE_RAW
    COMMENT = 'Capa Bronze: Ingesta de datos crudos en formato VARIANT (JSON)';

CREATE SCHEMA IF NOT EXISTS QUANT_INSTITUTIONAL_DB.SILVER_CLEAN
    COMMENT = 'Capa Silver: Series de tiempo financieras limpias, deduplicadas y normalizadas';

CREATE SCHEMA IF NOT EXISTS QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG
    COMMENT = 'Capa Gold: Feature store cuantitativo, estados de mercado, memoria episódica y playbooks vectorizados';


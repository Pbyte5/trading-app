# Plan Maestro: Sistema RAG Cuantitativo Multi-Mercado en Snowflake (Cripto, Acciones y Forex)

**Entorno de Construcción:** Antigravity IDE / Agentic Workflow  
**Arquitectura Cloud:** Snowflake AI Data Cloud (Medallion Data Warehouse + Snowpark Python + Cortex Search/Vector) + Orquestación Multi-Agente vía MCP.

---

## 1. ¿Cómo encaja Snowflake en un Sistema Quant Institucional?

Al migrar a **Snowflake**, eliminas la necesidad de mantener contenedores locales de PostgreSQL (`pgvector` + `TimescaleDB`) y unificas todo el pipeline institucional en una sola plataforma con separación de almacenamiento y cómputo (*Virtual Warehouses*):

1. **Data Warehouse (Arquitectura Medallion):**
   * **Capa `BRONZE` (Raw Ingestion):** Ingesta cruda de velas OHLCV, libros de órdenes, reportes SEC, COT y noticias en tablas con columnas tipo `VARIANT` (JSON nativo de ultra-rápida consulta) vía **Snowpipe Streaming** o conectores en Python.
   * **Capa `SILVER` (Cleaned & Normalized):** **Dynamic Tables** que transforman y normalizan automáticamente los JSON crudos a series de tiempo limpias y alineadas por zona horaria (`UTC`).
   * **Capa `GOLD` (Feature Store & Quant State):** Ejecución del motor matemático usando **Snowpark Python** directamente dentro de los nodos de Snowflake (sin mover gigabytes de datos a tu máquina local) para calcular VWAP, Volume Profile, Exponente de Hurst y ATR.
2. **Base de Datos Vectorial y RAG Nativo (Snowflake Cortex):**
   * **Tipo de dato `VECTOR(FLOAT, 768/1024)`:** Almacenamiento nativo de embeddings en las mismas tablas donde viven los datos financieros.
   * **Snowflake Cortex Search:** Motor de búsqueda híbrida gestionado (combina búsqueda semántica vectorial + búsqueda léxica BM25 + *Reranking* automático) ideal para buscar tickers exactos (`BTCUSDT`, `NVDA`), fechas y conceptos técnicos en los *Playbooks* institucionales.
   * **Funciones Vectoriales SQL:** Uso directo de `VECTOR_COSINE_SIMILARITY()` en SQL para comparar el estado actual del mercado con estados históricos (Memoria Episódica).
3. **Marketplace Financiero Nativo (Ventaja Institucional):**
   * Desde el **Snowflake Marketplace** puedes montar datasets gratuitos o institucionales (datos de la SEC, FRED, tipos de cambio y precios históricos) mediante *Zero-Copy Data Sharing* (sin construir ni pagar pipelines de descarga para datos históricos).

---

## 2. Catálogo de APIs Necesarias (Ingesta Hacia Snowflake `BRONZE`)

Para datos en tiempo real de baja latencia que complementen el Snowflake Marketplace, conectaremos cuatro flujos hacia el esquema `BRONZE`:

### A. Mercado de Criptoactivos (Spot, Futuros Perpetuos y On-Chain)
1. **Binance API (Spot & USDⓈ-M Futures) / Bybit API v5**
   * **Uso:** Velas OHLCV en tiempo real, profundidad del libro de órdenes (*Order Book L2*), historial de *Trades* para calcular el *CVD (Cumulative Volume Delta)*.
   * **Endpoints clave:** `/fapi/v1/klines`, `/fapi/v1/depth`, `/fapi/v1/premiumIndex` (Funding Rate).
2. **Coinglass API / Velo Data API**
   * **Uso:** Mapas de calor de liquidaciones, *Open Interest (OI)* agregado entre exchanges, *Long/Short Ratio* institucional y *Funding Rates* ponderados.
3. **DefiLlama API / Glassnode API**
   * **Uso (Estrategias Holder):** Métricas On-Chain ($MVRV\text{ Z-Score}$, $SOPR$, reservas en exchanges, flujos de ballenas y $TVL$). *DefiLlama es 100% gratuita sin API Key*.

### B. Mercado de Acciones (Equities & Índices)
1. **Alpaca Markets API / Polygon.io**
   * **Uso:** Datos históricos y en tiempo real de acciones de EE. UU. (NYSE, NASDAQ), *Trades & Quotes (NBBO)* para calcular el *VWAP Institucional* y perfiles de volumen.
2. **SEC EDGAR API (Oficial) / Snowflake Financial Marketplace**
   * **Uso (Estrategias Holder):** Reportes trimestrales (`10-Q`), anuales (`10-K`), múltiplos de valoración ($P/E$, $EV/EBITDA$, $FCF$). *(En Snowflake puedes conectar el dataset público de SEC Filings directamente desde el Marketplace)*.

### C. Mercado de Divisas (Forex G10 & Materias Primas)
1. **OANDA v20 API / Twelve Data**
   * **Uso:** Precios intradía e históricos de pares mayores/menores ($EUR/USD$, $USD/JPY$, $GBP/USD$, $XAU/USD$) y el **Order Book / Position Book de clientes** para estrategias institucionales de sentimiento contrario (*Contrarian Liquidity Sweep*).
2. **CFTC API (Socrata Open Data)**
   * **Uso (Estrategias Holder/Swing):** Reportes **COT (Commitment of Traders)** semanales para rastrear el posicionamiento neto de *Asset Managers* y *Leveraged Funds*. Gratuita.

### D. Macroeconomía, Bancos Centrales y Noticias (Transversal)
1. **FRED API (Federal Reserve Economic Data - St. Louis Fed)**
   * **Uso:** Tasas de interés ($FEDFUNDS$), inflación ($CPIAUCSL$), curva de rendimientos de bonos del Tesoro ($T10Y2Y$), masa monetaria ($M2SL$) e índice de volatilidad ($VIXCLS$). Gratuita.
2. **Finnhub API / Tavily Search API**
   * **Uso:** Calendario económico en tiempo real y noticias financieras de última hora limpias para ingesta en las tablas vectoriales de Snowflake.

---

## 3. Estructura del Proyecto para Antigravity (Arquitectura Snowflake)

Crea esta estructura de directorios en tu espacio de trabajo para que Antigravity construya tanto los scripts DDL de Snowflake como el motor en Python:

```
quant-rag-snowflake/
├── .env.example                      # Credenciales de Snowflake + APIs Financieras
├── requirements.txt                  # snowflake-snowpark-python, snowflake-ml-python, etc.
├── snowflake_sql/                    # Scripts de Infraestructura en Snowflake
│   ├── 01_setup_warehouses_db.sql    # Creación de Warehouses (INGEST_WH, QUANT_WH), DB y Esquemas
│   ├── 02_tables_and_vectors.sql     # Tablas Medallion (Bronze, Silver, Gold) + Tablas VECTOR
│   └── 03_cortex_search_service.sql  # Configuración de Cortex Search (RAG Híbrido)
├── src/
│   ├── config/
│   │   ├── settings.py               # Configuración con Pydantic BaseSettings
│   │   └── snowflake_session.py      # Gestor de conexión Snowpark y Key-Pair Auth
│   ├── connectors/                   # Ingesta desde APIs hacia Snowflake BRONZE
│   │   ├── snow_loader.py            # Utilidad write_pandas / Snowpipe hacia tablas BRONZE
│   │   ├── crypto_client.py          # Binance + DefiLlama + Coinglass
│   │   ├── equities_client.py        # Alpaca / Polygon + SEC EDGAR
│   │   ├── forex_client.py           # OANDA / TwelveData + CFTC (COT)
│   │   └── macro_client.py           # FRED API + Calendario Económico
│   ├── quant_engine/                 # Motor Matemático (Snowpark Python / Stored Procedures)
│   │   ├── microstructure.py         # VWAP, Volume Profile (POC, VAH, VAL), CVD, Liquidity Pools
│   │   ├── regime_detector.py        # Volatilidad ATR, GARCH, Hurst Exponent
│   │   └── risk_math.py              # Criterio de Kelly, Position Sizing, VaR
│   ├── rag/                          # Memoria Institucional con Snowflake Cortex
│   │   ├── ingest_playbooks_snow.py  # Chunking e inserción con SNOWFLAKE.CORTEX.EMBED_TEXT_1024
│   │   ├── cortex_retriever.py       # Consulta a Cortex Search + SQL Vector Similarity
│   │   └── episodic_memory.py        # Búsqueda de estados históricos con VECTOR_COSINE_SIMILARITY
│   ├── mcp_server/                   # Servidor Model Context Protocol para el LLM
│   │   └── snowflake_quant_tools.py  # Herramientas MCP conectadas al esquema GOLD y Cortex
│   └── agents/                       # Orquestación de Agentes
│       ├── prompts.py                # System Prompts Institucionales
│       ├── schemas.py                # Esquemas Pydantic de Salida Estructurada
│       └── orchestrator.py           # Flujo: Analista -> Estratega -> Risk Officer
└── playbooks/                        # Documentos base para el RAG
    ├── 01_microestructura_liquidez.md
    ├── 02_macro_carry_trade_forex.md
    └── 03_onchain_ciclos_cripto.md
```

---

## 4. Paso a Paso de Construcción en Antigravity

Copia y pega cada **Prompt de Construcción** directamente en Antigravity fase por fase.

### FASE 1: Arquitectura del Data Warehouse y Base Vectorial en Snowflake

Separaremos el cómputo en dos *Virtual Warehouses* (`X-SMALL` con auto-suspend a 60 segundos para optimizar créditos): uno para la ingesta de datos (`INGEST_WH`) y otro para el motor cuantitativo y consultas RAG (`QUANT_RAG_WH`).

* **Acción en Antigravity:** Generar los scripts SQL en `snowflake_sql/` y el gestor de sesión Snowpark en `src/config/`.
* **Prompt para Antigravity:**

  > *"Genera los scripts DDL en `snowflake_sql/`. 1) En `01_setup_warehouses_db.sql`, crea la base de datos `QUANT_INSTITUTIONAL_DB`, tres esquemas (`BRONZE_RAW`, `SILVER_CLEAN`, `GOLD_QUANT_RAG`) y dos Virtual Warehouses (`INGEST_WH` y `QUANT_RAG_WH` tamaño `XSMALL` con `AUTO_SUSPEND = 60` y `AUTO_RESUME = TRUE`). 2) En `02_tables_and_vectors.sql`, crea en `BRONZE_RAW` tablas con columna `RAW_PAYLOAD VARIANT` para ingesta JSON; en `SILVER_CLEAN` la tabla `MARKET_CANDLES` optimizada con `CLUSTER BY (MARKET, TICKER, TIMEFRAME, TIMESTAMP_UTC)`; y en `GOLD_QUANT_RAG` crea tres tablas: `QUANT_MARKET_STATES` (estados calculados de microestructura), `INSTITUTIONAL_PLAYBOOKS` (con columna `CHUNK_VECTOR VECTOR(FLOAT, 1024)`) y `EPISODIC_MEMORY_STATES` (con `STATE_VECTOR VECTOR(FLOAT, 1024)`). 3) En `03_cortex_search_service.sql`, escribe el DDL para crear un `CORTEX SEARCH SERVICE` sobre `INSTITUTIONAL_PLAYBOOKS` que indexe el texto y permita filtrado por `MARKET_TYPE` y `STRATEGY_HORIZON`. Finalmente, crea `src/config/snowflake_session.py` usando `snowflake-snowpark-python`."*

### FASE 2: Pipelines de Ingesta hacia la Capa `BRONZE` y `SILVER`

Los conectores en Python descargan los datos de las APIs y los suben en bloque a Snowflake usando `write_pandas` o inserción asíncrona sobre columnas `VARIANT`.

* **Acción en Antigravity:** Construir `src/connectors/snow_loader.py` y los 4 conectores de mercado.
* **Prompt para Antigravity:**

  > *"Implementa la capa de ingesta hacia Snowflake en `src/connectors/`. 1) Crea `snow_loader.py` con una función que reciba DataFrames de Pandas o listas de JSON y los cargue eficientemente en las tablas de `QUANT_INSTITUTIONAL_DB.BRONZE_RAW` y `SILVER_CLEAN.MARKET_CANDLES` usando `session.write_pandas(..., auto_create_table=False, overwrite=False)` con lógica `MERGE` (Upsert) para evitar velas duplicadas. 2) Crea los conectores para Binance Futures (`crypto_client.py`), Alpaca Markets (`equities_client.py`), OANDA/CFTC (`forex_client.py`) y FRED (`macro_client.py`), normalizando todos los precios al esquema `MARKET, TICKER, TIMEFRAME, TIMESTAMP_UTC, OPEN, HIGH, LOW, CLOSE, VOLUME, EXTRA_METRICS (VARIANT)`."*

### FASE 3: Motor Cuantitativo con Snowpark Python (Capa `GOLD`)

**Regla de oro:** El LLM nunca calcula indicadores. Tu código Python se ejecuta sobre los datos de `SILVER_CLEAN.MARKET_CANDLES` y escribe el resultado digerido en `GOLD_QUANT_RAG.QUANT_MARKET_STATES`.

#### Fórmulas que debe implementar este módulo:

1. **VWAP Institucional con Bandas de Desviación Estándar:**
   $$VWAP = \frac{\sum (P_i \cdot V_i)}{\sum V_i}$$
   Calcula las bandas a $\pm 1\sigma$, $\pm 2\sigma$ y $\pm 3\sigma$ para detectar agotamiento intradía.

2. **Volume Profile (Perfil de Volumen):**
   Agrupa el volumen por niveles de precio (bins) para hallar el **POC** (*Point of Control*), **VAH** (*Value Area High*) y **VAL** (*Value Area Low*, que concentran el $70\%$ del volumen total).

3. **Exponente de Hurst ($H$) para Detectar el Régimen de Mercado:**
   * Si $H < 0.45$: Mercado en reversión a la media (operar rebotes en bandas de VWAP / rango).
   * Si $H \approx 0.50$: Caminata aleatoria (No operar).
   * Si $H > 0.55$: Mercado tendencial / *Momentum* (operar rupturas y *pullbacks*).

4. **Criterio de Kelly Fraccionado (Dimensionamiento de Posición):**
   $$f^* = \frac{1}{4} \left( \frac{p \cdot b - q}{b} \right)$$
   Donde $p$ es la probabilidad estimada de éxito, $q = 1 - p$, y $b$ es el ratio Beneficio/Riesgo neto ($R:R$).

* **Prompt para Antigravity:**

  > *"Desarrolla el motor cuantitativo en `src/quant_engine/` diseñado para leer series de tiempo desde `SILVER_CLEAN.MARKET_CANDLES` vía Snowpark y guardar el estado calculado en `GOLD_QUANT_RAG.QUANT_MARKET_STATES`. En `microstructure.py` calcula: 1) VWAP anclado a la sesión con bandas ($\pm 1, 2, 3\sigma$), 2) Volume Profile exacto (POC, VAH, VAL), 3) Liquidity Pools (máximos/mínimos iguales no mitigados con tolerancia de $0.1 \times ATR$), y 4) Aproximación de Cumulative Volume Delta (CVD). En `regime_detector.py` calcula el ATR(14), el Exponente de Hurst ($H$) y el régimen de volatilidad. El resultado debe guardarse en una columna `STATE_JSON VARIANT` dentro de Snowflake lista para ser consultada en milisegundos."*

### FASE 4: RAG Nativo en Snowflake (Cortex Embeddings + Cortex Search)

En lugar de gestionar índices vectoriales externos, usamos las funciones nativas de IA de Snowflake (`SNOWFLAKE.CORTEX.EMBED_TEXT_1024`) y búsqueda de similitud coseno en SQL.

* **Acción en Antigravity:** Crear los manuales en `playbooks/` y los módulos en `src/rag/`.
* **Prompt para Antigravity:**

  > *"Primero, genera tres manuales técnicos institucionales en `playbooks/`: 1) Microestructura Intradía (Liquidity Sweeps, absorción en VWAP $\pm 2\sigma$, CVD y ORB), 2) Forex Macro y Carry Trade (diferenciales de tasas, reportes COT y correlación con US10Y), y 3) Cripto On-Chain y Derivados (Funding Squeezes y ciclos MVRV). Segundo, crea `src/rag/ingest_playbooks_snow.py` que divida los archivos Markdown por secciones, los suba a `GOLD_QUANT_RAG.INSTITUTIONAL_PLAYBOOKS` y ejecute en SQL `SNOWFLAKE.CORTEX.EMBED_TEXT_1024('voyage-multilingual-2', CHUNK_TEXT)` para generar los vectores directamente dentro de Snowflake. Tercero, crea `src/rag/cortex_retriever.py` que implemente dos funciones: búsqueda híbrida sobre los Playbooks usando Snowflake Cortex Search, y búsqueda de Memoria Episódica usando `VECTOR_COSINE_SIMILARITY(STATE_VECTOR, ...)` para encontrar las 5 ocasiones históricas en que el mercado tuvo una estructura técnica y macro idéntica a la actual."*

### FASE 5: Servidor MCP y Comité de Agentes Conectados a Snowflake

El servidor MCP expone vistas directas del esquema `GOLD_QUANT_RAG` al comité de agentes (Analista Cuantitativo, Analista Macro, Estratega y Risk Officer).

* **Prompt para Antigravity:**

  > *"Crea en `src/mcp_server/snowflake_quant_tools.py` las herramientas MCP que consulten el esquema `GOLD_QUANT_RAG` en Snowflake: `get_gold_market_state(ticker, timeframe)`, `query_cortex_playbooks(market_type, regime, query)` y `find_historical_analogies(current_state_summary)`. Luego, crea en `src/agents/schemas.py` el esquema Pydantic `InstitutionalTradePlan` (con estrategias separadas para `intraday_execution` y `holder_swing_allocation`, incluyendo entradas, Stop Loss por ATR, Take Profits en POC/VAH/VAL y fracción de Kelly). Finalmente, construye `src/agents/orchestrator.py` donde el LLM reciba los datos del esquema GOLD de Snowflake, diseñe las estrategias y pase por un filtro de Risk Officer en Python que rechace cualquier operación con Ratio Beneficio/Riesgo menor a 2.0."*

---

## 5. Variables de Entorno para Snowflake + APIs (`.env.example`)

Actualiza tu archivo `.env.example` con las credenciales del Data Warehouse y las fuentes de mercado:

```ini
# --- SNOWFLAKE AI DATA CLOUD ---
SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
SNOWFLAKE_USER="QUANT_BOT_USER"
SNOWFLAKE_PASSWORD="tu_password_o_usar_private_key"
SNOWFLAKE_ROLE="QUANT_ENGINEER_ROLE"
SNOWFLAKE_WAREHOUSE="QUANT_RAG_WH"
SNOWFLAKE_DATABASE="QUANT_INSTITUTIONAL_DB"
SNOWFLAKE_SCHEMA="GOLD_QUANT_RAG"

# --- LLM EXTERNO (Opcional si usas Snowflake Cortex Complete) ---
GEMINI_API_KEY="tu_api_key_aqui"

# --- CRIPTO (Binance / Coinglass) ---
BINANCE_API_KEY="opcional_para_datos_publicos"
BINANCE_SECRET_KEY="opcional_para_datos_publicos"
COINGLASS_API_KEY="tu_api_key_aqui"

# --- ACCIONES (Alpaca Markets / FMP) ---
ALPACA_API_KEY="tu_api_key_paper_trading"
ALPACA_SECRET_KEY="tu_secret_key_paper_trading"
ALPACA_BASE_URL="https://paper-api.alpaca.markets"
FMP_API_KEY="tu_api_key_para_fundamentales"

# --- FOREX (OANDA / Twelve Data) ---
OANDA_API_KEY="tu_api_key_oanda"
OANDA_ACCOUNT_ID="tu_account_id"
TWELVEDATA_API_KEY="tu_api_key_twelvedata"

# --- MACROECONOMÍA Y NOTICIAS ---
FRED_API_KEY="tu_api_key_gratuita_de_st_louis_fed"
FINNHUB_API_KEY="tu_api_key_noticias_y_calendario"
```

---

## 6. Flujo de Ejecución y Despliegue desde Antigravity

1. **Inicializar la infraestructura en Snowflake:**
   Ejecuta los scripts de la carpeta `snowflake_sql/` desde la extensión de Snowflake en tu IDE o mediante un script de inicialización en Python:
   ```bash
   python -m src.config.snowflake_session --init-db
   ```

2. **Ingestar y vectorizar los Playbooks Institucionales en Snowflake Cortex:**
   ```bash
   python -m src.rag.ingest_playbooks_snow
   ```

3. **Sincronizar datos de mercado (Bronze -> Silver) y calcular estado Quant (Gold):**
   ```bash
   python -m src.connectors.snow_loader --sync-all
   python -m src.quant_engine.microstructure --compute-gold
   ```

4. **Ejecutar el Orquestador Multi-Agente para generar planes de Trading (Intradía + Holder):**
   ```bash
   python -m src.agents.orchestrator --ticker "BTCUSDT" --market "CRYPTO"
   python -m src.agents.orchestrator --ticker "NVDA" --market "EQUITIES"
   python -m src.agents.orchestrator --ticker "EURUSD" --market "FOREX"
   ```
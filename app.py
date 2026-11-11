"""Plataforma de Análisis de Mercado | Trading Institucional & Microestructura.

Cutting-edge Financial Frontend for Real-Time 5m/15m/1h Candlestick Analysis,
VWAP Bands, Volume Profile (POC/VAH/VAL), Snowflake Medallion Feature Store,
and Multi-Agent Committee Orchestration with Chief Risk Officer (CRO) Filter.
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import streamlit.components.v1 as components

from src.config.settings import get_settings
from src.config.snowflake_session import get_raw_connection, get_snowpark_session
from src.mcp_server.snowflake_quant_tools import get_gold_market_state, clear_gold_tools_cache
from src.agents.orchestrator import generate_trade_plan
from src.agents.gemini_service import query_gemini_agent
from src.agents.visualizer import generate_investment_proposal_chart, generate_microstructure_radar
from src.connectors.snow_loader import sync_single_asset
from src.pipeline.live_daemon import (
    get_pipeline_state,
    start_live_daemon,
    stop_live_daemon,
    execute_pipeline_cycle
)

# ==============================================================================
# 1. PAGE CONFIGURATION & INSTITUTIONAL THEME (DARK SLATE / SKY BLUE SYSTEM)
# ==============================================================================
st.set_page_config(
    page_title="Plataforma de Análisis de Mercado",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Institutional Theme Styling (Slate #090d16, Cards #0f172a, Sky Blue #38bdf8, Emerald #10b981)
st.markdown("""
<style>
    /* 1. Base App Canvas (Deep Dark Slate) */
    .stApp {
        background-color: #090d16;
        color: #e2e8f0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }
    
    /* Transparent top header so custom buttons stay visible */
    header[data-testid="stHeader"] {
        background-color: transparent !important;
        z-index: 100 !important;
    }

    /* 2. HIGH-CONTRAST SKY BLUE HAMBURGER & SIDEBAR TOGGLE BUTTON */
    [data-testid="collapsedControl"],
    [data-testid="stSidebarCollapseButton"] button,
    button[data-testid="baseButton-header"],
    div[data-testid="stSidebarCollapsedControl"] button,
    button[kind="header"] {
        background-color: #0f172a !important;
        border: 1.5px solid #38bdf8 !important;
        color: #38bdf8 !important;
        border-radius: 8px !important;
        padding: 6px 12px !important;
        box-shadow: 0 0 14px rgba(56, 189, 248, 0.35) !important;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1) !important;
        opacity: 1 !important;
        visibility: visible !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
    }
    [data-testid="collapsedControl"]:hover,
    [data-testid="stSidebarCollapseButton"] button:hover,
    button[data-testid="baseButton-header"]:hover {
        background-color: #38bdf8 !important;
        color: #090d16 !important;
        box-shadow: 0 0 20px rgba(56, 189, 248, 0.65) !important;
        transform: scale(1.06) !important;
    }
    [data-testid="collapsedControl"] svg,
    [data-testid="stSidebarCollapseButton"] svg,
    button[data-testid="baseButton-header"] svg {
        stroke: currentColor !important;
        fill: currentColor !important;
        width: 22px !important;
        height: 22px !important;
    }

    /* 3. Accessibility Focus Indicator */
    button:focus-visible, input:focus-visible, select:focus-visible, [tabindex="0"]:focus-visible {
        outline: 2px solid #38bdf8 !important;
        outline-offset: 2px !important;
    }

    /* 4. Top Header Bar (Slate Surface #0f172a) */
    .terminal-header {
        background: #0f172a;
        border: 1px solid #1e293b;
        border-radius: 10px;
        padding: 14px 22px;
        margin-bottom: 14px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.25);
    }
    
    /* 5. Metric Cards & Strategy Containers */
    .metric-card {
        background: #0f172a;
        border: 1px solid #1e293b;
        border-radius: 10px;
        padding: 16px 20px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.25);
    }
    .metric-title {
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #94a3b8;
        margin-bottom: 5px;
    }
    .metric-value {
        font-size: 1.4rem;
        font-weight: 700;
        font-family: 'JetBrains Mono', 'Roboto Mono', monospace;
        color: #f8fafc;
    }
    .metric-sub {
        font-size: 0.75rem;
        color: #64748b;
        margin-top: 4px;
    }

    .strategy-box {
        background: #0f172a;
        border: 1px solid #1e293b;
        border-radius: 10px;
        padding: 18px;
        margin-top: 10px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.25);
    }
    
    /* 6. Badges (Institutional Semantic Coherence) */
    .cro-badge-approved {
        background-color: rgba(16, 185, 129, 0.12);
        color: #10b981;
        border: 1px solid rgba(16, 185, 129, 0.4);
        padding: 6px 14px;
        border-radius: 6px;
        font-weight: 700;
        letter-spacing: 0.05em;
        display: inline-block;
    }
    .cro-badge-rejected {
        background-color: rgba(239, 68, 68, 0.12);
        color: #ef4444;
        border: 1px solid rgba(239, 68, 68, 0.4);
        padding: 6px 14px;
        border-radius: 6px;
        font-weight: 700;
        letter-spacing: 0.05em;
        display: inline-block;
    }
    .badge-info {
        background-color: rgba(56, 189, 248, 0.12);
        color: #38bdf8;
        border: 1px solid rgba(56, 189, 248, 0.35);
        padding: 5px 12px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 0.75rem;
        display: inline-block;
    }

    /* 7. Tab Bar Navigation (Slate with Sky Active Border) */
    .stTabs [data-baseweb="tab-list"] {
        gap: 6px;
        background-color: #0f172a;
        padding: 6px;
        border-radius: 10px;
        border: 1px solid #1e293b;
        margin-bottom: 14px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 42px;
        color: #94a3b8;
        font-weight: 600;
        font-size: 0.88rem;
        padding: 8px 20px;
        border-radius: 6px;
        background: transparent;
        border: none;
        transition: all 0.15s ease-in-out;
    }
    .stTabs [aria-selected="true"] {
        color: #f8fafc !important;
        background-color: #1e293b !important;
        border: 1px solid #334155 !important;
        font-weight: 700 !important;
    }

    /* 8. Buttons (Sky Blue Primary & Slate Secondary) */
    .stButton > button[kind="primary"],
    button[data-testid="baseButton-primary"] {
        background: #0284c7 !important;
        color: #ffffff !important;
        font-weight: 700 !important;
        border: none !important;
        border-radius: 8px !important;
        box-shadow: 0 0 14px rgba(2, 132, 199, 0.25) !important;
        transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1) !important;
        min-height: 40px !important;
    }
    .stButton > button[kind="primary"]:hover,
    button[data-testid="baseButton-primary"]:hover {
        background: #0369a1 !important;
        box-shadow: 0 0 20px rgba(56, 189, 248, 0.45) !important;
        transform: translateY(-1px);
    }
    .stButton > button[kind="secondary"],
    button[data-testid="baseButton-secondary"] {
        background: #1e293b !important;
        color: #cbd5e1 !important;
        border: 1px solid #334155 !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        transition: all 0.2s ease !important;
        min-height: 40px !important;
    }
    .stButton > button[kind="secondary"]:hover,
    button[data-testid="baseButton-secondary"]:hover {
        background: #334155 !important;
        color: #ffffff !important;
        border-color: #38bdf8 !important;
    }

    /* 9. Form Elements (Select, Input, Number) */
    div[data-baseweb="select"] > div,
    div[data-baseweb="input"] > div,
    div[data-baseweb="base-input"] {
        background-color: #090d16 !important;
        border: 1px solid #1e293b !important;
        border-radius: 8px !important;
        color: #f8fafc !important;
    }
    div[data-baseweb="select"] > div:hover,
    div[data-baseweb="input"] > div:hover {
        border-color: #38bdf8 !important;
    }

    /* 10. Sidebar Background */
    section[data-testid="stSidebar"] {
        background-color: #0b1120;
        border-right: 1px solid #1e293b;
    }
</style>
""", unsafe_allow_html=True)


# ==============================================================================
# 2. DATA LOADERS & MULTI-TIER CACHING ENGINE
# ==============================================================================
@st.cache_data(ttl=60, show_spinner=False)
def load_candles_from_snowflake(market: str, ticker: str, timeframe: str, limit: int = 2016) -> pd.DataFrame:
    """Fetch normalized historical candles from SILVER_CLEAN.MARKET_CANDLES with 60s cache."""
    sql = f"""
    SELECT 
        TIMESTAMP_UTC,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        EXTRA_METRICS
    FROM QUANT_INSTITUTIONAL_DB.SILVER_CLEAN.MARKET_CANDLES
    WHERE MARKET = '{market.upper()}'
      AND TICKER = '{ticker.upper()}'
      AND TIMEFRAME = '{timeframe.lower()}'
    ORDER BY TIMESTAMP_UTC DESC
    LIMIT {limit}
    """
    conn = None
    try:
        conn = get_raw_connection()
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            if not rows:
                return pd.DataFrame()
            cols = ["TIMESTAMP_UTC", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "EXTRA_METRICS"]
            df = pd.DataFrame(rows, columns=cols)
            df["TIMESTAMP_UTC"] = pd.to_datetime(df["TIMESTAMP_UTC"])
            for c in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]:
                df[c] = pd.to_numeric(df[c])
            df = df.sort_values("TIMESTAMP_UTC").reset_index(drop=True)
            return df
    except Exception as e:
        st.warning(f"Aviso consultando Snowflake: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


@st.cache_data(ttl=60, show_spinner=False)
def load_gold_state(ticker: str, timeframe: str, market: str) -> Dict[str, Any]:
    """Retrieve the precomputed institutional quant state from GOLD_QUANT_RAG with 60s cache."""
    return get_gold_market_state(ticker=ticker, timeframe=timeframe, market=market)


@st.cache_data(ttl=120, show_spinner=False)
def cached_generate_trade_plan(ticker: str, market: str, timeframe: str, account_equity: float):
    """Generate trade plan with 120s cache to avoid repeating Cortex Search and vector analogies."""
    return generate_trade_plan(
        ticker=ticker,
        market=market,
        timeframe=timeframe,
        account_equity=account_equity
    )


@st.cache_data(ttl=180, show_spinner=False)
def cached_query_gemini_agent(
    user_query: str,
    ticker: str,
    market: str,
    timeframe: str,
    regime: str,
    last_price_rounded: float,
    api_key_override: Optional[str] = None
) -> Dict[str, Any]:
    """Cache deterministic and quick-prompt AI responses for 180s."""
    quant_state = load_gold_state(ticker=ticker, timeframe=timeframe, market=market)
    return query_gemini_agent(
        user_query=user_query,
        ticker=ticker,
        market=market,
        timeframe=timeframe,
        quant_state=quant_state,
        api_key_override=api_key_override
    )


# ==============================================================================
# 3. HIGH-PRECISION TRADINGVIEW LIGHTWEIGHT CHARTS™ ENGINE (HTML5 CANVAS)
# ==============================================================================
def render_tradingview_chart_html(
    df: pd.DataFrame,
    quant_state: Dict[str, Any],
    ticker: str,
    timeframe: str,
    market: str = "CRYPTO",
    show_vwap: bool = True,
    show_bands: bool = False,
    show_poc: bool = True,
    show_val_vah: bool = False,
    show_volume: bool = True
) -> str:
    """Generate high-precision interactive financial chart using official TradingView Lightweight Charts (HTML5 Canvas)."""
    if df.empty:
        return """
        <div style="background-color: #090d16; color: #94a3b8; padding: 50px; text-align: center; font-family: -apple-system, sans-serif; border: 1px solid #1e293b; border-radius: 8px;">
            <h3 style="color: #38bdf8; margin-bottom: 8px;">Sin velas registradas para este activo</h3>
            <p>Usa el botón de sincronización directa en la barra lateral para descargar datos de Snowflake.</p>
        </div>
        """

    # Format data for TradingView: strictly ascending unix timestamps without duplicates
    candles = []
    volumes = []
    seen = set()

    df_sorted = df.sort_values("TIMESTAMP_UTC").reset_index(drop=True)

    for _, row in df_sorted.iterrows():
        try:
            ts = int(pd.to_datetime(row["TIMESTAMP_UTC"]).timestamp())
            if ts in seen:
                continue
            seen.add(ts)
            o = float(row["OPEN"])
            h = float(row["HIGH"])
            l = float(row["LOW"])
            c = float(row["CLOSE"])
            v = float(row["VOLUME"])
            candles.append({"time": ts, "open": o, "high": h, "low": l, "close": c})
            volumes.append({
                "time": ts,
                "value": v,
                "color": "rgba(16, 185, 129, 0.45)" if c >= o else "rgba(239, 68, 68, 0.45)"
            })
        except Exception:
            continue

    if not candles:
        return "<div style='color: #ef4444; padding: 20px;'>Datos de velas insuficientes</div>"

    last_price = candles[-1]["close"]
    if last_price < 2.0:
        precision = 5
        min_move = 0.00001
    elif last_price < 500:
        precision = 2
        min_move = 0.01
    else:
        precision = 2
        min_move = 0.1

    # Extract quant state lines
    vwap_val = quant_state.get("vwap_metrics", {}).get("vwap")
    poc_val = quant_state.get("volume_profile", {}).get("poc")
    u2_val = quant_state.get("vwap_metrics", {}).get("vwap_upper_2")
    l2_val = quant_state.get("vwap_metrics", {}).get("vwap_lower_2")
    vah_val = quant_state.get("volume_profile", {}).get("vah")
    val_val = quant_state.get("volume_profile", {}).get("val")

    candles_json = json.dumps(candles)
    volumes_json = json.dumps(volumes)

    vwap_json = json.dumps(float(vwap_val)) if vwap_val is not None else "null"
    poc_json = json.dumps(float(poc_val)) if poc_val is not None else "null"
    u2_json = json.dumps(float(u2_val)) if u2_val is not None else "null"
    l2_json = json.dumps(float(l2_val)) if l2_val is not None else "null"
    vah_json = json.dumps(float(vah_val)) if vah_val is not None else "null"
    val_json = json.dumps(float(val_val)) if val_val is not None else "null"

    html = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <script src="https://unpkg.com/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
        <style>
            * {{
                box-sizing: border-box;
                margin: 0;
                padding: 0;
                user-select: none;
            }}
            body {{
                background-color: #090d16;
                color: #e2e8f0;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                overflow: hidden;
                width: 100%;
                height: 100vh;
            }}
            #wrapper {{
                position: relative;
                width: 100%;
                height: 100%;
                background: #090d16;
                border: 1px solid #1e293b;
                border-radius: 8px;
            }}
            #tv-container {{
                width: 100%;
                height: 100%;
            }}
            #hud-bar {{
                position: absolute;
                top: 10px;
                left: 12px;
                z-index: 20;
                display: flex;
                flex-wrap: wrap;
                align-items: center;
                gap: 12px;
                pointer-events: auto;
            }}
            .range-group {{
                display: flex;
                gap: 2px;
                background: rgba(15, 23, 42, 0.95);
                border: 1px solid #1e293b;
                border-radius: 6px;
                padding: 3px 5px;
            }}
            .range-btn {{
                background: none;
                border: none;
                color: #94a3b8;
                font-size: 11px;
                font-weight: 600;
                padding: 4px 10px;
                border-radius: 4px;
                cursor: pointer;
                transition: all 0.15s ease;
            }}
            .range-btn:hover {{
                background: #1e293b;
                color: #f8fafc;
            }}
            .range-btn.active {{
                background: #0284c7;
                color: #ffffff;
                font-weight: 700;
            }}
            .hud-card {{
                display: flex;
                align-items: center;
                gap: 8px;
                background: rgba(15, 23, 42, 0.95);
                border: 1px solid #1e293b;
                border-radius: 6px;
                padding: 5px 12px;
                font-family: 'JetBrains Mono', 'Roboto Mono', monospace;
                font-size: 11px;
                color: #94a3b8;
            }}
            .hud-badge {{
                color: #38bdf8;
                font-weight: 700;
            }}
            .hud-val {{
                color: #f8fafc;
                font-weight: 600;
            }}
        </style>
    </head>
    <body>
        <div id="wrapper">
            <!-- HUD & Toolbar -->
            <div id="hud-bar">
                <div class="range-group">
                    <button class="range-btn" onclick="selectHorizon('6h', this)">6H</button>
                    <button class="range-btn" onclick="selectHorizon('1d', this)">1D</button>
                    <button class="range-btn" onclick="selectHorizon('3d', this)">3D</button>
                    <button class="range-btn active" onclick="selectHorizon('all', this)">7D</button>
                    <button class="range-btn" onclick="resetScale()" style="color: #38bdf8;">Reset Escala</button>
                </div>
                <div class="hud-card">
                    <span class="hud-badge">{ticker} • {timeframe}</span>
                    <span id="ohlc-metrics">Cargando...</span>
                </div>
            </div>

            <!-- TradingView Canvas Container -->
            <div id="tv-container"></div>
        </div>

        <script>
            const container = document.getElementById('tv-container');
            const candles = {candles_json};
            const volumes = {volumes_json};
            const vwap = {vwap_json};
            const poc = {poc_json};
            const u2 = {u2_json};
            const l2 = {l2_json};
            const vah = {vah_json};
            const val = {val_json};

            // 1. Initialize TradingView Lightweight Chart
            const chart = LightweightCharts.createChart(container, {{
                width: container.clientWidth,
                height: container.clientHeight,
                layout: {{
                    background: {{ type: 'solid', color: '#090d16' }},
                    textColor: '#94a3b8',
                    fontSize: 11,
                    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
                }},
                grid: {{
                    vertLines: {{ color: 'rgba(30, 41, 59, 0.45)' }},
                    horzLines: {{ color: 'rgba(30, 41, 59, 0.45)' }}
                }},
                crosshair: {{
                    mode: LightweightCharts.CrosshairMode.Normal,
                    vertLine: {{
                        color: '#64748b',
                        width: 1,
                        style: LightweightCharts.LineStyle.Dashed,
                        labelBackgroundColor: '#1e293b'
                    }},
                    horzLine: {{
                        color: '#64748b',
                        width: 1,
                        style: LightweightCharts.LineStyle.Dashed,
                        labelBackgroundColor: '#1e293b'
                    }}
                }},
                rightPriceScale: {{
                    borderColor: '#1e293b',
                    autoScale: true,
                    scaleMargins: {{
                        top: 0.08,
                        bottom: 0.20
                    }}
                }},
                timeScale: {{
                    borderColor: '#1e293b',
                    timeVisible: true,
                    secondsVisible: false,
                    barSpacing: 10,
                    minBarSpacing: 1,
                    rightOffset: 5
                }},
                handleScroll: {{
                    mouseWheel: true,
                    pressedMouseMove: true,
                    horzTouchDrag: true,
                    vertTouchDrag: true
                }},
                handleScale: {{
                    axisPressedMouseMove: true,
                    mouseWheel: true,
                    pinch: true
                }}
            }});

            // 2. Candlestick Series (Emerald & Red Standard)
            const candleSeries = chart.addCandlestickSeries({{
                upColor: '#10b981',
                downColor: '#ef4444',
                borderVisible: false,
                wickUpColor: '#10b981',
                wickDownColor: '#ef4444',
                priceFormat: {{
                    type: 'price',
                    precision: {precision},
                    minMove: {min_move}
                }}
            }});
            candleSeries.setData(candles);

            // 3. Volume Overlay
            if ({str(show_volume).lower()}) {{
                const volumeSeries = chart.addHistogramSeries({{
                    priceFormat: {{ type: 'volume' }},
                    priceScaleId: ''
                }});
                volumeSeries.priceScale().applyOptions({{
                    scaleMargins: {{
                        top: 0.82,
                        bottom: 0
                    }}
                }});
                volumeSeries.setData(volumes);
            }}

            // 4. Institutional Price Lines
            if ({str(show_vwap).lower()} && vwap !== null) {{
                candleSeries.createPriceLine({{
                    price: vwap,
                    color: '#818cf8',
                    lineWidth: 2,
                    lineStyle: LightweightCharts.LineStyle.Solid,
                    axisLabelVisible: true,
                    title: 'VWAP'
                }});
            }}

            if ({str(show_poc).lower()} && poc !== null) {{
                candleSeries.createPriceLine({{
                    price: poc,
                    color: '#f59e0b',
                    lineWidth: 2,
                    lineStyle: LightweightCharts.LineStyle.Solid,
                    axisLabelVisible: true,
                    title: 'POC'
                }});
            }}

            if ({str(show_bands).lower()} && u2 !== null && l2 !== null) {{
                candleSeries.createPriceLine({{
                    price: u2,
                    color: 'rgba(129, 140, 248, 0.5)',
                    lineWidth: 1,
                    lineStyle: LightweightCharts.LineStyle.Dotted,
                    axisLabelVisible: true,
                    title: '+2s VWAP'
                }});
                candleSeries.createPriceLine({{
                    price: l2,
                    color: 'rgba(129, 140, 248, 0.5)',
                    lineWidth: 1,
                    lineStyle: LightweightCharts.LineStyle.Dotted,
                    axisLabelVisible: true,
                    title: '-2s VWAP'
                }});
            }}

            if ({str(show_val_vah).lower()} && vah !== null && val !== null) {{
                candleSeries.createPriceLine({{
                    price: vah,
                    color: '#06b6d4',
                    lineWidth: 1,
                    lineStyle: LightweightCharts.LineStyle.Dashed,
                    axisLabelVisible: true,
                    title: 'VAH 70%'
                }});
                candleSeries.createPriceLine({{
                    price: val,
                    color: '#06b6d4',
                    lineWidth: 1,
                    lineStyle: LightweightCharts.LineStyle.Dashed,
                    axisLabelVisible: true,
                    title: 'VAL 70%'
                }});
            }}

            // 5. Dynamic HUD Updates on Hover
            const ohlcText = document.getElementById('ohlc-metrics');
            function renderHUD(bar) {{
                if (!bar) return;
                const pct = (((bar.close - bar.open) / bar.open) * 100).toFixed(2);
                const col = pct >= 0 ? '#10b981' : '#ef4444';
                const sign = pct >= 0 ? '+' : '';
                ohlcText.innerHTML = `O: <b class="hud-val">${{bar.open.toFixed({precision})}}</b> ` +
                                    `H: <b class="hud-val">${{bar.high.toFixed({precision})}}</b> ` +
                                    `L: <b class="hud-val">${{bar.low.toFixed({precision})}}</b> ` +
                                    `C: <b style="color:${{col}}">${{bar.close.toFixed({precision})}}</b> ` +
                                    `<span style="color:${{col}}">(${{sign}}${{pct}}%)</span>`;
            }}

            if (candles.length > 0) {{
                renderHUD(candles[candles.length - 1]);
            }}

            chart.subscribeCrosshairMove(param => {{
                if (!param.time || !param.seriesData.get(candleSeries)) {{
                    if (candles.length > 0) renderHUD(candles[candles.length - 1]);
                    return;
                }}
                const bar = param.seriesData.get(candleSeries);
                renderHUD(bar);
            }});

            // 6. Timeframe Range Navigator
            window.selectHorizon = function(horizon, btn) {{
                document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
                if (btn) btn.classList.add('active');
                if (!candles.length) return;
                const last = candles[candles.length - 1].time;
                let secs = 0;
                if (horizon === '6h') secs = 6 * 3600;
                else if (horizon === '1d') secs = 24 * 3600;
                else if (horizon === '3d') secs = 3 * 24 * 3600;
                else if (horizon === 'all') {{
                    chart.timeScale().fitContent();
                    return;
                }}
                chart.timeScale().setVisibleRange({{
                    from: last - secs,
                    to: last + 1200
                }});
            }};

            window.resetScale = function() {{
                chart.timeScale().resetTimeScale();
                chart.priceScale('right').applyOptions({{ autoScale: true }});
            }};

            if (candles.length > 120) {{
                const step = candles[1].time - candles[0].time || 300;
                const last = candles[candles.length - 1].time;
                chart.timeScale().setVisibleRange({{
                    from: last - (120 * step),
                    to: last + (step * 5)
                }});
            }} else {{
                chart.timeScale().fitContent();
            }}

            new ResizeObserver(entries => {{
                if (entries.length && entries[0].contentRect) {{
                    const rect = entries[0].contentRect;
                    chart.applyOptions({{ width: rect.width, height: rect.height }});
                }}
            }}).observe(container);
        </script>
    </body>
    </html>
    """
    return html


# ==============================================================================
# 4. MAIN APPLICATION CONTROLLER
# ==============================================================================
def main():
    pipe_state = get_pipeline_state()
    is_live = pipe_state.get("is_running", False)

    # Initialize session state variables
    if "chat_messages" not in st.session_state:
        st.session_state["chat_messages"] = []
    if "gemini_api_key" not in st.session_state:
        st.session_state["gemini_api_key"] = get_settings().gemini_api_key or ""

    # 1. Header Bar: Institutional Branding & Status Badges
    st.markdown(f"""
    <div class="terminal-header">
        <div>
            <span style="font-size: 1.2rem; font-weight: 800; color: #38bdf8; letter-spacing: 0.05em;">PLATAFORMA DE ANÁLISIS DE MERCADO</span>
            <span style="font-size: 0.85rem; color: #94a3b8; margin-left: 12px;">Trading Institucional & Microestructura</span>
        </div>
        <div style="display: flex; gap: 10px; align-items: center;">
            <span class="badge-info">
                SNOWFLAKE: QUANT_INSTITUTIONAL_DB
            </span>
            <span class="{'cro-badge-approved' if is_live else 'cro-badge-rejected'}" style="font-size: 0.75rem; padding: 5px 12px;">
                {'● PIPELINE EN VIVO' if is_live else '● PIPELINE DETENIDO'}
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 2. Sidebar Controls
    with st.sidebar:
        st.markdown("""
        <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid #1e293b;">
            <span style="font-size: 0.95rem; font-weight: 800; color: #ffffff; letter-spacing: 0.04em;">MERCADO & ACTIVOS</span>
            <span style="font-size: 0.7rem; background: rgba(56, 189, 248, 0.12); color: #38bdf8; padding: 2px 8px; border-radius: 4px; font-weight: 700;">LIVE</span>
        </div>
        """, unsafe_allow_html=True)
        
        ASSET_REGISTRY = {
            "BTC/USDT — Bitcoin (Crypto)": ("CRYPTO", "BTCUSDT"),
            "ETH/USDT — Ethereum (Crypto)": ("CRYPTO", "ETHUSDT"),
            "SOL/USDT — Solana (Crypto)": ("CRYPTO", "SOLUSDT"),
            "NVDA — Nvidia Corp (Equity)": ("EQUITIES", "NVDA"),
            "AAPL — Apple Inc (Equity)": ("EQUITIES", "AAPL"),
            "MSFT — Microsoft Corp (Equity)": ("EQUITIES", "MSFT"),
            "EUR/USD — Euro / Dólar (Forex)": ("FOREX", "EURUSD"),
            "GBP/USD — Libra / Dólar (Forex)": ("FOREX", "GBPUSD"),
            "USD/JPY — Dólar / Yen (Forex)": ("FOREX", "USDJPY"),
        }

        selected_asset_label = st.selectbox(
            "Activo Financiero",
            options=list(ASSET_REGISTRY.keys()),
            index=0
        )
        market_choice, ticker = ASSET_REGISTRY[selected_asset_label]

        col_tf1, col_tf2 = st.columns([1.4, 1.0])
        with col_tf1:
            timeframe = st.selectbox("Temporalidad", options=["5m", "15m", "1h", "1d"], index=0)
        with col_tf2:
            days_lookback = st.number_input("Ventana (Días)", min_value=1, max_value=7, value=7, step=1)

        # 1-Click Synchronization & Explicit Cache Purge Controls
        col_btn1, col_btn2 = st.columns([1.4, 1.0])
        with col_btn1:
            if st.button("Sincronizar Activo", use_container_width=True, type="primary"):
                with st.spinner(f"Sincronizando {ticker} ({timeframe}) con Snowflake..."):
                    sync_single_asset(market=market_choice, ticker=ticker, timeframe=timeframe, days=days_lookback)
                    st.cache_data.clear()
                    clear_gold_tools_cache()
                    st.success("Datos actualizados correctamente.")
                    st.rerun()
        with col_btn2:
            if st.button("Purgar Caché", use_container_width=True, help="Limpia la memoria caché de consultas y RAG."):
                st.cache_data.clear()
                clear_gold_tools_cache()
                st.toast("Caché purgada con éxito.", icon="✅")
                st.rerun()

        st.markdown("---")

        with st.expander("Capas de Microestructura", expanded=True):
            show_vwap = st.checkbox("Línea VWAP de Sesión", value=True)
            show_poc = st.checkbox("Nivel POC (Point of Control)", value=True)
            show_bands = st.checkbox("Bandas VWAP (±2s)", value=False)
            show_val_vah = st.checkbox("Áreas de Valor (VAH / VAL)", value=False)
            show_volume = st.checkbox("Histograma de Volumen", value=True)

        with st.expander("Control de Pipeline en Vivo", expanded=False):
            col_s1, col_s2 = st.columns(2)
            with col_s1:
                if st.button("Iniciar Live", use_container_width=True):
                    start_live_daemon(interval_seconds=60, days=days_lookback, timeframes=[timeframe, "1h"])
                    st.rerun()
            with col_s2:
                if st.button("Detener", use_container_width=True):
                    stop_live_daemon()
                    st.rerun()

        with st.expander("Gestión de Capital", expanded=False):
            equity = st.number_input("Capital Institucional (USD)", min_value=1000.0, value=100000.0, step=10000.0)

        with st.expander("Configuración Gemini API", expanded=False):
            gemini_key_input = st.text_input(
                "Gemini API Key:",
                value=st.session_state.get("gemini_api_key", ""),
                type="password",
                placeholder="AIzaSy...",
                help="Clave API de Google Gemini para habilitar el razonamiento institucional."
            )
            if gemini_key_input != st.session_state.get("gemini_api_key", ""):
                st.session_state["gemini_api_key"] = gemini_key_input
                st.success("Clave Gemini actualizada.")

    # 3. Load Candlestick Data & Quant State from Snowflake (Multi-tier Cache)
    df_candles = load_candles_from_snowflake(market=market_choice, ticker=ticker, timeframe=timeframe, limit=2016)
    
    if df_candles.empty:
        with st.spinner(f"Sincronizando {market_choice}:{ticker} ({timeframe}) directamente en Snowflake..."):
            sync_single_asset(market=market_choice, ticker=ticker, timeframe=timeframe, days=days_lookback)
            st.cache_data.clear()
            clear_gold_tools_cache()
            df_candles = load_candles_from_snowflake(market=market_choice, ticker=ticker, timeframe=timeframe, limit=2016)

    quant_state = load_gold_state(ticker=ticker, timeframe=timeframe, market=market_choice)
    last_price = quant_state.get("last_price", df_candles["CLOSE"].iloc[-1] if not df_candles.empty else 0.0)
    reg_data = quant_state.get("regime", {})
    regime = reg_data.get("regime", "TRENDING_MOMENTUM")
    hurst = reg_data.get("hurst", 0.55)

    # ==============================================================================
    # 4. TAB NAVIGATION & MAIN VIEWS (No unnecessary reruns, 100% accessible)
    # ==============================================================================
    tab_chart, tab_committee, tab_chat = st.tabs([
        "Terminal Gráfica",
        "Comité de Riesgo (CRO)",
        "Asesor Cuantitativo"
    ])

    # ------------------------------------------------------------------------------
    # TAB 1: TRADINGVIEW CANDLESTICK CHART
    # ------------------------------------------------------------------------------
    with tab_chart:
        chart_html = render_tradingview_chart_html(
            df=df_candles,
            quant_state=quant_state,
            ticker=ticker,
            timeframe=timeframe,
            market=market_choice,
            show_vwap=show_vwap,
            show_bands=show_bands,
            show_poc=show_poc,
            show_val_vah=show_val_vah,
            show_volume=show_volume
        )
        components.html(chart_html, height=740)

    # ------------------------------------------------------------------------------
    # TAB 2: COMITÉ AGÉNTICO & VEREDICTO CRO (CACHED ANALYSIS)
    # ------------------------------------------------------------------------------
    with tab_committee:
        col_head, col_btn = st.columns([4, 1.2])
        with col_btn:
            if st.button("Actualizar Análisis", use_container_width=True, type="primary", key="btn_refresh_plan"):
                st.cache_data.clear()
                clear_gold_tools_cache()
                st.rerun()

        plan = cached_generate_trade_plan(
            ticker=ticker,
            market=market_choice,
            timeframe=timeframe,
            account_equity=equity
        )

        verdict = plan.intraday_execution.risk_officer_verdict
        is_approved = (verdict == "APPROVED")

        # Top Executive CRO Banner
        st.markdown(f"""
        <div style="background: #0f172a; border: 1px solid #1e293b; border-radius: 10px; padding: 18px 22px; margin-bottom: 16px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h2 style="margin: 0; color: #f8fafc; font-size: 1.35rem; font-weight: 700;">Comité Agéntico de Inversión & Control de Riesgo</h2>
                    <p style="margin: 4px 0 0 0; color: #94a3b8; font-size: 0.88rem;">
                        Evaluación para <strong>{ticker}</strong> ({market_choice} • {timeframe}) | Filosofía de Preservación de Capital
                    </p>
                </div>
                <div>
                    <span class="{'cro-badge-approved' if is_approved else 'cro-badge-rejected'}" style="font-size: 1.05rem; padding: 8px 18px;">
                        VEREDICTO CRO: {verdict}
                    </span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # 4 Key Metrics Bar (Consistent Slate & Sky Blue System)
        mk1, mk2, mk3, mk4 = st.columns(4)
        with mk1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Ratio R:R Proyectado</div>
                <div class="metric-value" style="color: {'#10b981' if plan.intraday_execution.reward_to_risk_ratio >= 2.0 else '#ef4444'};">
                    {plan.intraday_execution.reward_to_risk_ratio:.2f}
                </div>
                <div class="metric-sub">Umbral Obligatorio CRO: >= 2.00</div>
            </div>
            """, unsafe_allow_html=True)

        with mk2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Criterio Kelly Fraccionado</div>
                <div class="metric-value" style="color: #38bdf8;">
                    {plan.intraday_execution.fractional_kelly_fraction * 100:.2f}%
                </div>
                <div class="metric-sub">1/4 Kelly (Gestión de Ruina)</div>
            </div>
            """, unsafe_allow_html=True)

        with mk3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Capital Máximo Asignado</div>
                <div class="metric-value" style="color: #f8fafc;">
                    ${plan.intraday_execution.suggested_risk_capital_usd:,.2f} USD
                </div>
                <div class="metric-sub">Sobre Equity: ${equity:,.2f} USD</div>
            </div>
            """, unsafe_allow_html=True)

        with mk4:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Régimen & Microestructura</div>
                <div class="metric-value" style="font-size: 1.15rem; color: #f59e0b;">
                    {plan.market_regime}
                </div>
                <div class="metric-sub">Hurst: {plan.hurst_exponent:.3f} | ATR: {plan.atr:,.4f}</div>
            </div>
            """, unsafe_allow_html=True)

        if not is_approved and plan.intraday_execution.risk_rejection_reason:
            st.markdown(f"""
            <div style="background: rgba(239, 68, 68, 0.12); border: 1px solid #ef4444; border-radius: 6px; padding: 12px 16px; margin: 14px 0; color: #fca5a5;">
                <strong>Veto del Chief Risk Officer (CRO):</strong> {plan.intraday_execution.risk_rejection_reason}
            </div>
            """, unsafe_allow_html=True)

        # 2 Wide Columns for Strategies
        col_st1, col_st2 = st.columns(2)
        with col_st1:
            s1 = plan.intraday_execution
            act_col = "#10b981" if s1.action == "BUY" else "#ef4444" if s1.action == "SELL" else "#94a3b8"
            st.markdown(f"""
            <div class="strategy-box">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-size: 1.05rem; font-weight: 700; color: #38bdf8;">Estrategia 1: Ejecución Intradía ({timeframe})</span>
                    <span style="font-size: 1.25rem; font-weight: 800; color: {act_col};">{s1.action}</span>
                </div>
                <hr style="border-color: #1e293b; margin: 10px 0;">
                <table style="width: 100%; font-size: 0.92rem; color: #cbd5e1; border-collapse: separate; border-spacing: 0 6px;">
                    <tr><td><strong>Precio Entrada:</strong></td><td align="right"><b>{s1.entry_price:,.4f}</b></td></tr>
                    <tr><td><strong>Stop Loss (ATR):</strong></td><td align="right" style="color: #ef4444;"><b>{s1.stop_loss_price:,.4f}</b></td></tr>
                    <tr><td><strong>Take Profit 1 / 2:</strong></td><td align="right" style="color: #10b981;"><b>{s1.take_profit_levels}</b></td></tr>
                    <tr><td><strong>Ratio R:R:</strong></td><td align="right"><b>{s1.reward_to_risk_ratio:.2f}</b></td></tr>
                    <tr><td><strong>Regla Playbook:</strong></td><td align="right" style="color: #38bdf8;"><em>{s1.matched_playbook_rule}</em></td></tr>
                </table>
                <p style="font-size: 0.85rem; color: #94a3b8; margin-top: 10px; line-height: 1.4;">
                    <strong>Tesis Técnica:</strong> {s1.technical_rationale}
                </p>
            </div>
            """, unsafe_allow_html=True)

        with col_st2:
            s2 = plan.holder_swing_allocation
            act_col2 = "#10b981" if s2.action == "BUY" else "#ef4444" if s2.action == "SELL" else "#94a3b8"
            st.markdown(f"""
            <div class="strategy-box">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-size: 1.05rem; font-weight: 700; color: #818cf8;">Estrategia 2: Asignación Swing / Macro</span>
                    <span style="font-size: 1.25rem; font-weight: 800; color: {act_col2};">{s2.action}</span>
                </div>
                <hr style="border-color: #1e293b; margin: 10px 0;">
                <table style="width: 100%; font-size: 0.92rem; color: #cbd5e1; border-collapse: separate; border-spacing: 0 6px;">
                    <tr><td><strong>Precio Acumulación:</strong></td><td align="right"><b>{s2.entry_price:,.4f}</b></td></tr>
                    <tr><td><strong>Stop Estructural:</strong></td><td align="right" style="color: #ef4444;"><b>{s2.stop_loss_price:,.4f}</b></td></tr>
                    <tr><td><strong>Target Plurianual:</strong></td><td align="right" style="color: #10b981;"><b>{s2.take_profit_levels}</b></td></tr>
                    <tr><td><strong>R:R Proyectado:</strong></td><td align="right"><b>{s2.reward_to_risk_ratio:.2f}</b></td></tr>
                    <tr><td><strong>Memoria Episódica:</strong></td><td align="right" style="color: #818cf8;"><em>Cortex Vector RAG</em></td></tr>
                </table>
                <p style="font-size: 0.85rem; color: #94a3b8; margin-top: 10px; line-height: 1.4;">
                    <strong>Analogía Histórica:</strong> {plan.historical_analogy_summary}
                </p>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
        chart_comm = generate_investment_proposal_chart(
            ticker=ticker,
            action=plan.intraday_execution.action if plan.intraday_execution.action != "NO_TRADE" else "BUY",
            entry_price=plan.intraday_execution.entry_price,
            stop_loss=plan.intraday_execution.stop_loss_price,
            take_profit_levels=plan.intraday_execution.take_profit_levels,
            vwap=plan.vwap,
            poc=plan.poc,
            rr_ratio=plan.intraday_execution.reward_to_risk_ratio,
            timeframe=timeframe
        )
        st.plotly_chart(chart_comm, use_container_width=True)

    # ------------------------------------------------------------------------------
    # TAB 3: ASESOR CUANTITATIVO IA (GEMINI - CACHED QUERY PIPELINE)
    # ------------------------------------------------------------------------------
    with tab_chat:
        if not st.session_state["chat_messages"]:
            welcome_msg = (
                f"Asesor Cuantitativo conectado a la capa GOLD de Snowflake y Google Gemini.\n\n"
                f"Activo monitoreado: **{ticker}** ({market_choice} • {timeframe}) | Último precio: **{last_price:,.4f}** "
                f"| Régimen: **{regime}** (Hurst: `{hurst:.3f}`).\n\n"
                f"Capacidades disponibles:\n"
                f"- **Propuesta estructurada de inversión**: Niveles de Entrada, Stop Loss por ATR, Take Profit y ratio R:R >= 2.0.\n"
                f"- **Explicación técnica con gráficos**: Desglose de VWAP, POC y perfiles de liquidez.\n"
                f"- **Evaluación del modelo de riesgo**: Gestión de capital por 1/4 Kelly y política de preservación de capital del CRO."
            )
            st.session_state["chat_messages"].append({"role": "assistant", "content": welcome_msg, "chart": None})

        st.markdown(f"""
        <div style="background: #0f172a; border: 1px solid #1e293b; border-radius: 10px; padding: 14px 18px; margin-bottom: 12px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h3 style="margin: 0; color: #f8fafc; font-size: 1.15rem; font-weight: 700;">Asesor Cuantitativo & de Negocio (Gemini)</h3>
                    <p style="margin: 3px 0 0 0; color: #94a3b8; font-size: 0.85rem;">
                        Activo en Foco: <strong style="color: #38bdf8;">{ticker}</strong> ({market_choice} • {timeframe}) | 
                        Precio: <strong>{last_price:,.4f}</strong> | Régimen: <strong style="color: #f59e0b;">{regime}</strong>
                    </p>
                </div>
                <div>
                    <span class="cro-badge-approved" style="font-size: 0.75rem; padding: 4px 12px;">
                        AGENTE ACTIVO
                    </span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Quick Prompt Buttons
        q_col1, q_col2, q_col3, q_col4 = st.columns([2.2, 2.2, 2.2, 1.2])
        trigger_prompt = None
        with q_col1:
            if st.button(f"Proponer Inversión en {ticker}", use_container_width=True, key="btn_qp_prop"):
                trigger_prompt = f"Proponme una inversión estructurada y fundamentada para {ticker} ({timeframe}) evaluando VWAP, POC, ATR y el ratio R:R."
        with q_col2:
            if st.button("Explicar Modelo y CRO", use_container_width=True, key="btn_qp_model"):
                trigger_prompt = "¿Cómo funciona el modelo de negocio institucional de la plataforma y por qué el CRO aplica veto obligatorio?"
        with q_col3:
            if st.button("Analizar Microestructura", use_container_width=True, key="btn_qp_micro"):
                trigger_prompt = f"Analiza en detalle la microestructura actual de {ticker}: ¿Dónde está el POC, el VWAP y qué indica el CVD?"
        with q_col4:
            if st.button("Limpiar Historial", use_container_width=True, key="btn_qp_clear"):
                st.session_state["chat_messages"] = []
                st.rerun()

        # Render Conversation History
        for msg in st.session_state["chat_messages"]:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])
                if msg.get("chart") is not None:
                    st.plotly_chart(msg["chart"], use_container_width=True)

        user_input = st.chat_input("Escribe tu consulta sobre inversión, microestructura o modelo de negocio...")
        active_query = trigger_prompt or user_input

        if active_query:
            st.session_state["chat_messages"].append({"role": "user", "content": active_query, "chart": None})
            with st.spinner("Analizando Feature Store y consultando Gemini..."):
                agent_res = cached_query_gemini_agent(
                    user_query=active_query,
                    ticker=ticker,
                    market=market_choice,
                    timeframe=timeframe,
                    regime=regime,
                    last_price_rounded=round(last_price, 2),
                    api_key_override=st.session_state.get("gemini_api_key")
                )
            st.session_state["chat_messages"].append({
                "role": "assistant",
                "content": agent_res["text"],
                "chart": agent_res.get("chart")
            })
            st.rerun()


if __name__ == "__main__":
    main()

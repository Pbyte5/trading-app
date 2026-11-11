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
from src.mcp_server.snowflake_quant_tools import get_gold_market_state
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
# 1. PAGE CONFIGURATION & INSTITUTIONAL THEME
# ==============================================================================
st.set_page_config(
    page_title="Plataforma de Análisis de Mercado",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# High-End TradingView/Bloomberg Dark Styling (Low saturation, clean contrasts)
st.markdown("""
<style>
    /* Dark Terminal Background */
    .stApp {
        background-color: #080c14;
        color: #e2e8f0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }
    
    /* Top Header Bar */
    .terminal-header {
        background: linear-gradient(90deg, #0d1527 0%, #111c35 100%);
        border: 1px solid #1e293b;
        border-radius: 8px;
        padding: 12px 18px;
        margin-bottom: 16px;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    
    /* KPI Metric Cards */
    .metric-card {
        background: #0d1424;
        border: 1px solid #1e293b;
        border-radius: 8px;
        padding: 12px 16px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
    }
    .metric-title {
        font-size: 0.72rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #94a3b8;
        margin-bottom: 4px;
    }
    .metric-value {
        font-size: 1.35rem;
        font-weight: 700;
        font-family: 'JetBrains Mono', 'Roboto Mono', monospace;
        color: #f8fafc;
    }
    .metric-sub {
        font-size: 0.72rem;
        color: #64748b;
        margin-top: 2px;
    }
    
    /* CRO Risk Badges */
    .cro-badge-approved {
        background-color: rgba(0, 192, 118, 0.15);
        color: #00c076;
        border: 1px solid #00c076;
        padding: 6px 14px;
        border-radius: 6px;
        font-weight: 700;
        letter-spacing: 0.05em;
        display: inline-block;
    }
    .cro-badge-rejected {
        background-color: rgba(255, 59, 48, 0.15);
        color: #ff3b30;
        border: 1px solid #ff3b30;
        padding: 6px 14px;
        border-radius: 6px;
        font-weight: 700;
        letter-spacing: 0.05em;
        display: inline-block;
    }
    
    /* Strategy Containers */
    .strategy-box {
        background: #0d1527;
        border: 1px solid #1e293b;
        border-radius: 8px;
        padding: 16px;
        margin-top: 10px;
    }
    
    /* Sidebar Styling */
    section[data-testid="stSidebar"] {
        background-color: #0a0f1d;
        border-right: 1px solid #1e293b;
    }
</style>
""", unsafe_allow_html=True)


# ==============================================================================
# 2. DATA LOADERS & SNOWFLAKE INTEGRATION
# ==============================================================================
@st.cache_data(ttl=20)
def load_candles_from_snowflake(market: str, ticker: str, timeframe: str, limit: int = 2016) -> pd.DataFrame:
    """Fetch normalized historical candles from SILVER_CLEAN.MARKET_CANDLES in Snowflake."""
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


def load_gold_state(ticker: str, timeframe: str, market: str) -> Dict[str, Any]:
    """Retrieve the precomputed institutional quant state from GOLD_QUANT_RAG."""
    return get_gold_market_state(ticker=ticker, timeframe=timeframe, market=market)


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
        <div style="background-color: #080c14; color: #94a3b8; padding: 50px; text-align: center; font-family: -apple-system, sans-serif; border: 1px solid #1e293b; border-radius: 8px;">
            <h3 style="color: #38bdf8; margin-bottom: 8px;">Sin velas registradas para este activo</h3>
            <p>Usa el botón de sincronización en la barra lateral para descargar datos de Snowflake.</p>
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
                "color": "rgba(0, 192, 118, 0.45)" if c >= o else "rgba(255, 59, 48, 0.45)"
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
                background-color: #080c14;
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
                background: #080c14;
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
                background: rgba(13, 21, 39, 0.9);
                border: 1px solid #1e293b;
                border-radius: 6px;
                padding: 2px 4px;
            }}
            .range-btn {{
                background: none;
                border: none;
                color: #94a3b8;
                font-size: 11px;
                font-weight: 600;
                padding: 3px 8px;
                border-radius: 4px;
                cursor: pointer;
                transition: all 0.15s ease;
            }}
            .range-btn:hover {{
                background: #1e293b;
                color: #f8fafc;
            }}
            .range-btn.active {{
                background: #00c076;
                color: #080c14;
                font-weight: 700;
            }}
            .hud-card {{
                display: flex;
                align-items: center;
                gap: 8px;
                background: rgba(13, 21, 39, 0.9);
                border: 1px solid #1e293b;
                border-radius: 6px;
                padding: 4px 10px;
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
                    <button class="range-btn active" onclick="selectHorizon('all', this)">7D (Todo)</button>
                    <button class="range-btn" onclick="resetScale()" style="color: #38bdf8;">⟲ Reset</button>
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
                    background: {{ type: 'solid', color: '#080c14' }},
                    textColor: '#94a3b8',
                    fontSize: 11,
                    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
                }},
                grid: {{
                    vertLines: {{ color: 'rgba(30, 41, 59, 0.4)' }},
                    horzLines: {{ color: 'rgba(30, 41, 59, 0.4)' }}
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
                    axisPressedMouseMove: true, // Drag price axis vertically to stretch!
                    mouseWheel: true,
                    pinch: true
                }}
            }});

            // 2. Candlestick Series (Pixel-Perfect)
            const candleSeries = chart.addCandlestickSeries({{
                upColor: '#00c076',
                downColor: '#ff3b30',
                borderVisible: false,
                wickUpColor: '#00c076',
                wickDownColor: '#ff3b30',
                priceFormat: {{
                    type: 'price',
                    precision: {precision},
                    minMove: {min_move}
                }}
            }});
            candleSeries.setData(candles);

            // 3. Volume Overlay (Bottom 18%)
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
                    color: '#a855f7',
                    lineWidth: 2,
                    lineStyle: LightweightCharts.LineStyle.Solid,
                    axisLabelVisible: true,
                    title: 'VWAP'
                }});
            }}

            if ({str(show_poc).lower()} && poc !== null) {{
                candleSeries.createPriceLine({{
                    price: poc,
                    color: '#fbbf24',
                    lineWidth: 2,
                    lineStyle: LightweightCharts.LineStyle.Solid,
                    axisLabelVisible: true,
                    title: 'POC'
                }});
            }}

            if ({str(show_bands).lower()} && u2 !== null && l2 !== null) {{
                candleSeries.createPriceLine({{
                    price: u2,
                    color: '#38bdf8',
                    lineWidth: 1,
                    lineStyle: LightweightCharts.LineStyle.Dotted,
                    axisLabelVisible: true,
                    title: '+2σ VWAP'
                }});
                candleSeries.createPriceLine({{
                    price: l2,
                    color: '#38bdf8',
                    lineWidth: 1,
                    lineStyle: LightweightCharts.LineStyle.Dotted,
                    axisLabelVisible: true,
                    title: '-2σ VWAP'
                }});
            }}

            if ({str(show_val_vah).lower()} && vah !== null && val !== null) {{
                candleSeries.createPriceLine({{
                    price: vah,
                    color: '#0ea5e9',
                    lineWidth: 1,
                    lineStyle: LightweightCharts.LineStyle.Dashed,
                    axisLabelVisible: true,
                    title: 'VAH 70%'
                }});
                candleSeries.createPriceLine({{
                    price: val,
                    color: '#0ea5e9',
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
                const col = pct >= 0 ? '#00c076' : '#ff3b30';
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

            // Initial view: default to last 120 candles with clear spacing
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

            // Resize Observer for 100% responsiveness
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
    if "main_view" not in st.session_state:
        st.session_state["main_view"] = "chart"  # Options: "chart", "committee", "chat"
    if "chat_messages" not in st.session_state:
        st.session_state["chat_messages"] = []
    if "gemini_api_key" not in st.session_state:
        st.session_state["gemini_api_key"] = get_settings().gemini_api_key or ""

    # 1. Header Bar: Plataforma de Análisis de Mercado
    st.markdown(f"""
    <div class="terminal-header">
        <div>
            <span style="font-size: 1.25rem; font-weight: 800; color: #00f2fe; letter-spacing: 0.05em;">Plataforma de Análisis de Mercado</span>
            <span style="font-size: 0.85rem; color: #94a3b8; margin-left: 10px;">| Trading & Microestructura Institucional</span>
        </div>
        <div style="display: flex; gap: 12px; align-items: center;">
            <span style="font-size: 0.78rem; background: #1e293b; padding: 4px 10px; border-radius: 4px; color: #38bdf8;">
                ❄️ Snowflake DB: QUANT_INSTITUTIONAL_DB
            </span>
            <span style="font-size: 0.78rem; background: {'rgba(0, 192, 118, 0.2)' if is_live else 'rgba(255, 59, 48, 0.2)'}; 
                         color: {'#00c076' if is_live else '#ff3b30'}; padding: 4px 10px; border-radius: 4px; font-weight: 700;">
                ● {'PIPELINE EN VIVO' if is_live else 'PIPELINE DETENIDO'}
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 2. View Switcher Bar (Instant full-screen toggle)
    col_v1, col_v2, col_v3, col_v_back = st.columns([1.5, 1.8, 1.8, 3.2])
    with col_v1:
        if st.button("📈 Terminal Gráfica", use_container_width=True, type="primary" if st.session_state["main_view"] == "chart" else "secondary"):
            st.session_state["main_view"] = "chart"
            st.rerun()
    with col_v2:
        if st.button("🏛️ Comité Agéntico (Full)", use_container_width=True, type="primary" if st.session_state["main_view"] == "committee" else "secondary"):
            st.session_state["main_view"] = "committee"
            st.rerun()
    with col_v3:
        if st.button("🤖 Asesor Cuantitativo IA", use_container_width=True, type="primary" if st.session_state["main_view"] == "chat" else "secondary"):
            st.session_state["main_view"] = "chat"
            st.rerun()
    with col_v_back:
        if st.session_state["main_view"] != "chart":
            if st.button("⬅️ Volver a Pantalla Completa de Gráfico", use_container_width=False):
                st.session_state["main_view"] = "chart"
                st.rerun()

    st.markdown("<div style='height: 6px;'></div>", unsafe_allow_html=True)

    # 3. Sidebar Controls
    with st.sidebar:
        st.markdown("### 📊 Activos del Mercado")
        
        # Complete asset registry with all assets visible simultaneously
        ASSET_REGISTRY = {
            "🪙 BTCUSDT (Bitcoin • Crypto)": ("CRYPTO", "BTCUSDT"),
            "🪙 ETHUSDT (Ethereum • Crypto)": ("CRYPTO", "ETHUSDT"),
            "🪙 SOLUSDT (Solana • Crypto)": ("CRYPTO", "SOLUSDT"),
            "📈 NVDA (Nvidia Corp • Equity)": ("EQUITIES", "NVDA"),
            "📈 AAPL (Apple Inc • Equity)": ("EQUITIES", "AAPL"),
            "📈 MSFT (Microsoft • Equity)": ("EQUITIES", "MSFT"),
            "💱 EURUSD (Euro / Dólar • Forex)": ("FOREX", "EURUSD"),
            "💱 GBPUSD (Libra / Dólar • Forex)": ("FOREX", "GBPUSD"),
            "💱 USDJPY (Dólar / Yen • Forex)": ("FOREX", "USDJPY"),
        }

        selected_asset_label = st.radio(
            "Activo",
            options=list(ASSET_REGISTRY.keys()),
            index=0,
            label_visibility="collapsed"
        )
        market_choice, ticker = ASSET_REGISTRY[selected_asset_label]

        st.markdown("---")
        st.markdown("### ⏱️ Temporalidad")
        timeframe = st.select_slider(
            "Temporalidad (Velas)",
            options=["5m", "15m", "1h", "1d"],
            value="5m",
            label_visibility="collapsed"
        )

        days_lookback = st.slider("Ventana Histórica (Días)", min_value=1, max_value=7, value=7)

        with st.expander("👁️ Capas de Microestructura", expanded=True):
            show_vwap = st.checkbox("Línea VWAP de Sesión", value=True)
            show_poc = st.checkbox("Nivel POC (Point of Control)", value=True)
            show_bands = st.checkbox("Bandas VWAP (±2σ)", value=False)
            show_val_vah = st.checkbox("Áreas de Valor (VAH / VAL)", value=False)
            show_volume = st.checkbox("Volumen Overlay (Inferior)", value=True)
            st.caption("💡 *Tip:* Usa rueda del ratón para zoom temporal y arrastra el eje vertical derecho de precios para estirar velas.")

        with st.expander("⚡ Pipeline & Sincronización", expanded=False):
            col_s1, col_s2 = st.columns(2)
            with col_s1:
                 if st.button("▶️ Iniciar Live", use_container_width=True):
                     start_live_daemon(interval_seconds=60, days=days_lookback, timeframes=[timeframe, "1h"])
                     st.rerun()
            with col_s2:
                 if st.button("⏹️ Detener", use_container_width=True):
                     stop_live_daemon()
                     st.rerun()

            if st.button("🔄 Sincronizar Activo Ahora", use_container_width=True):
                 with st.spinner(f"Descargando {ticker} ({timeframe}) y actualizando Snowflake..."):
                     sync_single_asset(market=market_choice, ticker=ticker, timeframe=timeframe, days=days_lookback)
                     st.cache_data.clear()
                     st.success("¡Datos actualizados!")
                     st.rerun()

        with st.expander("💼 Gestión de Capital", expanded=False):
            equity = st.number_input("Capital Institucional (USD)", min_value=1000.0, value=100000.0, step=10000.0)

        with st.expander("🔑 Configuración Gemini API", expanded=False):
            gemini_key_input = st.text_input(
                "Gemini API Key:",
                value=st.session_state.get("gemini_api_key", ""),
                type="password",
                placeholder="AIzaSy...",
                help="Ingresa tu clave de Google Gemini para activar el razonamiento con IA. También se detecta automáticamente desde .env."
            )
            if gemini_key_input != st.session_state.get("gemini_api_key", ""):
                st.session_state["gemini_api_key"] = gemini_key_input
                st.success("Clave Gemini actualizada.")

    # 4. Load Candlestick Data & Quant State from Snowflake
    df_candles = load_candles_from_snowflake(market=market_choice, ticker=ticker, timeframe=timeframe, limit=2016)
    
    if df_candles.empty:
        with st.spinner(f"Sincronizando {market_choice}:{ticker} ({timeframe}) directamente en Snowflake..."):
            sync_single_asset(market=market_choice, ticker=ticker, timeframe=timeframe, days=days_lookback)
            st.cache_data.clear()
            df_candles = load_candles_from_snowflake(market=market_choice, ticker=ticker, timeframe=timeframe, limit=2016)

    quant_state = load_gold_state(ticker=ticker, timeframe=timeframe, market=market_choice)
    last_price = quant_state.get("last_price", df_candles["CLOSE"].iloc[-1] if not df_candles.empty else 0.0)
    reg_data = quant_state.get("regime", {})
    regime = reg_data.get("regime", "TRENDING_MOMENTUM")
    hurst = reg_data.get("hurst", 0.55)

    # 5. Sidebar Actions: Comité Agéntico & Consultas Rápidas
    with st.sidebar:
        st.markdown("---")
        with st.expander("🤖 Comité Agéntico & Control CRO", expanded=False):
            plan_cache_key = f"plan_{ticker}_{timeframe}_{equity}"
            if st.button("🧠 Ejecutar Análisis a Pantalla Completa", type="primary", use_container_width=True):
                with st.spinner(f"Analizando {ticker} ({timeframe}) con Comité Agéntico y CRO..."):
                    plan = generate_trade_plan(
                        ticker=ticker,
                        market=market_choice,
                        timeframe=timeframe,
                        account_equity=equity
                    )
                    st.session_state[plan_cache_key] = plan
                    st.session_state["main_view"] = "committee"
                    st.rerun()

            plan = st.session_state.get(plan_cache_key)
            if plan:
                verdict = plan.intraday_execution.risk_officer_verdict
                is_approved = (verdict == "APPROVED")
                st.markdown(f"""
                <div class="strategy-box" style="padding: 10px; margin-top: 6px;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                        <span style="font-size: 0.85rem; font-weight: 700;">Veredicto CRO:</span>
                        <span class="{'cro-badge-approved' if is_approved else 'cro-badge-rejected'}" style="font-size: 0.72rem; padding: 2px 8px;">{verdict}</span>
                    </div>
                    <div style="font-size: 0.78rem; color: #cbd5e1; line-height: 1.4;">
                        <strong>R:R:</strong> {plan.intraday_execution.reward_to_risk_ratio:.2f} (Mín: 2.0)<br>
                        <strong>Kelly (1/4):</strong> {plan.intraday_execution.fractional_kelly_fraction * 100:.1f}% capital<br>
                        <strong>Riesgo Máx:</strong> ${plan.intraday_execution.suggested_risk_capital_usd:,.2f} USD
                    </div>
                </div>
                """, unsafe_allow_html=True)
                if st.button("🔍 Ver Informe Detallado", use_container_width=True):
                    st.session_state["main_view"] = "committee"
                    st.rerun()
            else:
                st.caption("Pulsa el botón para ejecutar análisis multi-agente en pantalla completa.")

        with st.expander("💬 Consultas al Agente Cuantitativo", expanded=False):
            sidebar_query = st.text_input("Pregunta al Agente:", placeholder="Ej: Proponme una inversión...", key="sidebar_agent_input")
            if st.button("Consultar en Pantalla Completa", use_container_width=True, key="sidebar_agent_btn"):
                if sidebar_query:
                    # Execute agent query
                    with st.spinner("Procesando consulta con Gemini y Snowflake..."):
                        res = query_gemini_agent(
                            user_query=sidebar_query,
                            ticker=ticker,
                            market=market_choice,
                            timeframe=timeframe,
                            quant_state=quant_state,
                            api_key_override=st.session_state.get("gemini_api_key")
                        )
                    st.session_state["chat_messages"].append({"role": "user", "content": sidebar_query, "chart": None})
                    st.session_state["chat_messages"].append({"role": "assistant", "content": res["text"], "chart": res.get("chart")})
                    st.session_state["main_view"] = "chat"
                    st.rerun()

    # ==============================================================================
    # 6. DYNAMIC MAIN VIEW ROUTER
    # ==============================================================================

    # VIEW A: TRADINGVIEW CANDLESTICK CHART (60 FPS CANVAS)
    if st.session_state["main_view"] == "chart":
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

    # VIEW B: FULL-SCREEN COMITÉ AGÉNTICO & VEREDICTO CRO
    elif st.session_state["main_view"] == "committee":
        plan_cache_key = f"plan_{ticker}_{timeframe}_{equity}"
        plan = st.session_state.get(plan_cache_key)

        if not plan:
            with st.spinner(f"Analizando {ticker} ({timeframe}) con el Comité Agéntico y evaluando con el CRO..."):
                plan = generate_trade_plan(
                    ticker=ticker,
                    market=market_choice,
                    timeframe=timeframe,
                    account_equity=equity
                )
                st.session_state[plan_cache_key] = plan

        verdict = plan.intraday_execution.risk_officer_verdict
        is_approved = (verdict == "APPROVED")

        # Top Executive CRO Banner
        st.markdown(f"""
        <div style="background: linear-gradient(90deg, #0d1527 0%, #152244 100%); border: 1px solid #1e293b; border-radius: 8px; padding: 18px 22px; margin-bottom: 16px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h2 style="margin: 0; color: #f8fafc; font-size: 1.4rem;">🏛️ Comité Agéntico de Inversión & Control de Riesgo</h2>
                    <p style="margin: 4px 0 0 0; color: #94a3b8; font-size: 0.88rem;">
                        Evaluación para <strong>{ticker}</strong> ({market_choice} • {timeframe}) | Filosofía de Preservación de Capital
                    </p>
                </div>
                <div>
                    <span class="{'cro-badge-approved' if is_approved else 'cro-badge-rejected'}" style="font-size: 1.1rem; padding: 8px 20px;">
                        VEREDICTO CRO: {verdict}
                    </span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # 4 Key Metrics Bar
        mk1, mk2, mk3, mk4 = st.columns(4)
        with mk1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Ratio R:R Proyectado</div>
                <div class="metric-value" style="color: {'#00c076' if plan.intraday_execution.reward_to_risk_ratio >= 2.0 else '#ff3b30'};">
                    {plan.intraday_execution.reward_to_risk_ratio:.2f}
                </div>
                <div class="metric-sub">Umbral Obligatorio CRO: ≥ 2.00</div>
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
                <div class="metric-value" style="font-size: 1.15rem; color: #fbbf24;">
                    {plan.market_regime}
                </div>
                <div class="metric-sub">Hurst: {plan.hurst_exponent:.3f} | ATR: {plan.atr:,.4f}</div>
            </div>
            """, unsafe_allow_html=True)

        if not is_approved and plan.intraday_execution.risk_rejection_reason:
            st.markdown(f"""
            <div style="background: rgba(255, 59, 48, 0.12); border: 1px solid #ff3b30; border-radius: 6px; padding: 12px 16px; margin: 14px 0; color: #fca5a5;">
                <strong>🛑 Veto del Chief Risk Officer (CRO):</strong> {plan.intraday_execution.risk_rejection_reason}
            </div>
            """, unsafe_allow_html=True)

        # 2 Wide Columns for Strategies
        col_st1, col_st2 = st.columns(2)
        with col_st1:
            s1 = plan.intraday_execution
            act_col = "#00c076" if s1.action == "BUY" else "#ff3b30" if s1.action == "SELL" else "#94a3b8"
            st.markdown(f"""
            <div class="strategy-box">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-size: 1.05rem; font-weight: 700; color: #38bdf8;">⚡ Estrategia 1: Ejecución Intradía ({timeframe})</span>
                    <span style="font-size: 1.25rem; font-weight: 800; color: {act_col};">{s1.action}</span>
                </div>
                <hr style="border-color: #1e293b; margin: 10px 0;">
                <table style="width: 100%; font-size: 0.92rem; color: #cbd5e1; border-collapse: separate; border-spacing: 0 6px;">
                    <tr><td><strong>Precio Entrada:</strong></td><td align="right"><b>{s1.entry_price:,.4f}</b></td></tr>
                    <tr><td><strong>Stop Loss (ATR):</strong></td><td align="right" style="color: #ff3b30;"><b>{s1.stop_loss_price:,.4f}</b></td></tr>
                    <tr><td><strong>Take Profit 1 / 2:</strong></td><td align="right" style="color: #00c076;"><b>{s1.take_profit_levels}</b></td></tr>
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
            act_col2 = "#00c076" if s2.action == "BUY" else "#ff3b30" if s2.action == "SELL" else "#94a3b8"
            st.markdown(f"""
            <div class="strategy-box">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-size: 1.05rem; font-weight: 700; color: #a855f7;">🌐 Estrategia 2: Asignación Swing / Macro</span>
                    <span style="font-size: 1.25rem; font-weight: 800; color: {act_col2};">{s2.action}</span>
                </div>
                <hr style="border-color: #1e293b; margin: 10px 0;">
                <table style="width: 100%; font-size: 0.92rem; color: #cbd5e1; border-collapse: separate; border-spacing: 0 6px;">
                    <tr><td><strong>Precio Acumulación:</strong></td><td align="right"><b>{s2.entry_price:,.4f}</b></td></tr>
                    <tr><td><strong>Stop Estructural:</strong></td><td align="right" style="color: #ff3b30;"><b>{s2.stop_loss_price:,.4f}</b></td></tr>
                    <tr><td><strong>Target Plurianual:</strong></td><td align="right" style="color: #00c076;"><b>{s2.take_profit_levels}</b></td></tr>
                    <tr><td><strong>R:R Proyectado:</strong></td><td align="right"><b>{s2.reward_to_risk_ratio:.2f}</b></td></tr>
                    <tr><td><strong>Memoria Episódica:</strong></td><td align="right" style="color: #a855f7;"><em>Cortex Vector RAG</em></td></tr>
                </table>
                <p style="font-size: 0.85rem; color: #94a3b8; margin-top: 10px; line-height: 1.4;">
                    <strong>Analogía Histórica:</strong> {plan.historical_analogy_summary}
                </p>
            </div>
            """, unsafe_allow_html=True)

        # Visual Explanatory Chart of the Strategy Decision
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

    # VIEW C: FULL-SCREEN CHAT ASESOR CUANTITATIVO IA (GEMINI 2.5)
    elif st.session_state["main_view"] == "chat":
        # Welcome message on fresh conversation
        if not st.session_state["chat_messages"]:
            welcome_msg = (
                f"¡Hola! Soy tu **Asesor Cuantitativo & de Negocio**, conectado en tiempo real a la capa GOLD "
                f"de Snowflake y potenciado por la inteligencia de **Google Gemini 2.5**.\n\n"
                f"Actualmente monitoreando **{ticker}** ({market_choice} • {timeframe}) con último precio en **{last_price:,.4f}** "
                f"en régimen **{regime}** (Hurst: `{hurst:.3f}`).\n\n"
                f"Estoy listo para:\n"
                f"- 💡 **Proponerte una inversión estructurada** con niveles de Entrada, Stop Loss por ATR, Take Profit y ratio R:R $\\ge 2.0$.\n"
                f"- 📊 **Explicarte con gráficos interactivos** por qué se toma cada decisión técnica.\n"
                f"- ⚖️ **Responder preguntas sobre nuestro modelo de negocio**, gestión del riesgo por 1/4 Kelly y veto del CRO."
            )
            st.session_state["chat_messages"].append({"role": "assistant", "content": welcome_msg, "chart": None})

        # Top Agent Status Banner
        st.markdown(f"""
        <div style="background: linear-gradient(90deg, #0d1527 0%, #152244 100%); border: 1px solid #1e293b; border-radius: 8px; padding: 14px 18px; margin-bottom: 12px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h3 style="margin: 0; color: #f8fafc; font-size: 1.25rem;">🤖 Asesor Cuantitativo & de Negocio (Gemini 2.5)</h3>
                    <p style="margin: 3px 0 0 0; color: #94a3b8; font-size: 0.85rem;">
                        Activo en Foco: <strong style="color: #38bdf8;">{ticker}</strong> ({market_choice} • {timeframe}) | 
                        Precio: <strong>{last_price:,.4f}</strong> | Régimen: <strong style="color: #fbbf24;">{regime}</strong>
                    </p>
                </div>
                <div>
                    <span style="font-size: 0.78rem; background: rgba(0, 192, 118, 0.15); border: 1px solid #00c076; color: #00c076; padding: 4px 10px; border-radius: 4px; font-weight: 600;">
                        ● AGENTE ACTIVO & BLINDADO
                    </span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Quick Prompt Suggestion Pills
        q_col1, q_col2, q_col3, q_col4 = st.columns([2.3, 2.3, 2.3, 1.1])
        trigger_prompt = None
        with q_col1:
            if st.button(f"💡 Proponer Inversión en {ticker}", use_container_width=True):
                trigger_prompt = f"Proponme una inversión estructurada y fundamentada para {ticker} ({timeframe}) evaluando VWAP, POC, ATR y el ratio R:R."
        with q_col2:
            if st.button("⚖️ Explicar Modelo y CRO", use_container_width=True):
                trigger_prompt = "¿Cómo funciona el modelo de negocio institucional de la plataforma y por qué el CRO aplica veto obligatorio?"
        with q_col3:
            if st.button("📊 Analizar Microestructura", use_container_width=True):
                trigger_prompt = f"Analiza en detalle la microestructura actual de {ticker}: ¿Dónde está el POC, el VWAP y qué indica el CVD?"
        with q_col4:
            if st.button("🗑️ Limpiar", use_container_width=True):
                st.session_state["chat_messages"] = []
                st.rerun()

        # Render Conversation History
        for msg in st.session_state["chat_messages"]:
            with st.chat_message(msg["role"], avatar="🧑‍💻" if msg["role"] == "user" else "🤖"):
                st.markdown(msg["content"])
                if msg.get("chart") is not None:
                    st.plotly_chart(msg["chart"], use_container_width=True)

        # Bottom Input Box
        user_input = st.chat_input("Escribe tu consulta sobre inversión, microestructura o modelo de negocio...")
        active_query = trigger_prompt or user_input

        if active_query:
            # 1. Add user message
            st.session_state["chat_messages"].append({"role": "user", "content": active_query, "chart": None})
            
            # 2. Query Gemini Agent with Security & Feature Store Context
            with st.spinner("El Agente Cuantitativo está analizando el Feature Store de Snowflake y generando respuesta..."):
                agent_res = query_gemini_agent(
                    user_query=active_query,
                    ticker=ticker,
                    market=market_choice,
                    timeframe=timeframe,
                    quant_state=quant_state,
                    api_key_override=st.session_state.get("gemini_api_key")
                )
            
            # 3. Add assistant response
            st.session_state["chat_messages"].append({
                "role": "assistant",
                "content": agent_res["text"],
                "chart": agent_res.get("chart")
            })
            st.rerun()


if __name__ == "__main__":
    main()

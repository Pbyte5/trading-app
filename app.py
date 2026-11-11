"""Plataforma de Análisis de Mercado | Trading Institucional & Microestructura.

Cutting-edge Financial Frontend for Real-Time 5m/15m/1h Candlestick Analysis,
VWAP Bands, Volume Profile (POC/VAH/VAL), Snowflake Medallion Feature Store,
and Multi-Agent Committee Orchestration with Chief Risk Officer (CRO) Filter.
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import streamlit.components.v1 as components

logger = logging.getLogger("trading_terminal")

from src.config.settings import get_settings
from src.config.snowflake_session import get_raw_connection, get_snowpark_session
from src.mcp_server.snowflake_quant_tools import get_gold_market_state, clear_gold_tools_cache
from src.agents.orchestrator import generate_trade_plan
from src.agents.gemini_service import query_gemini_agent
from src.agents.visualizer import generate_investment_proposal_chart, generate_microstructure_radar
from src.connectors.snow_loader import sync_single_asset
from src.pipeline.live_daemon import get_pipeline_state, execute_pipeline_cycle

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
    /* 1. Base Terminal Canvas & High-DPI Theme */
    .stApp {
        background-color: #070a13;
        color: #e2e8f0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
        -webkit-font-smoothing: antialiased;
    }

    /* Full-screen terminal canvas maximized for near-full screen trading view */
    .block-container {
        padding-top: 0.40rem !important;
        padding-bottom: 0.25rem !important;
        padding-left: 1rem !important;
        padding-right: 1rem !important;
        max-width: 100% !important;
        position: relative !important;
    }

    /* Streamlit Header & Toolbar Decoupling: Non-blocking pointer events, zero physical collision */
    header[data-testid="stHeader"] {
        background-color: transparent !important;
        z-index: 9999 !important;
        pointer-events: none !important;
    }
    header[data-testid="stHeader"] button,
    header[data-testid="stHeader"] a {
        pointer-events: auto !important;
    }
    div[data-testid="stToolbar"] {
        background-color: transparent !important;
        pointer-events: none !important;
    }
    div[data-testid="stToolbar"] > * {
        pointer-events: auto !important;
    }

    /* Hide Deploy button and right-side Streamlit toolbar items */
    .stAppDeployButton,
    [data-testid="stAppDeployButton"],
    [data-testid="stToolbarActions"],
    [data-testid="stMainMenu"],
    [data-testid="stStatusWidget"],
    [data-testid="stToolbarActionButton"] {
        display: none !important;
        visibility: hidden !important;
        width: 0 !important;
        height: 0 !important;
        opacity: 0 !important;
        pointer-events: none !important;
    }

    /* High-Visibility Floating Sidebar Toggle Buttons (Expand & Collapse) */
    [data-testid="collapsedControl"],
    [data-testid="stSidebarCollapseButton"] button,
    div[data-testid="stSidebarCollapsedControl"] button,
    [data-testid="stExpandSidebarButton"],
    button[data-testid="stExpandSidebarButton"] {
        background-color: rgba(15, 23, 42, 0.95) !important;
        backdrop-filter: blur(10px) !important;
        -webkit-backdrop-filter: blur(10px) !important;
        border: 1.5px solid #38bdf8 !important;
        color: #38bdf8 !important;
        border-radius: 8px !important;
        padding: 6px 11px !important;
        box-shadow: 0 0 14px rgba(56, 189, 248, 0.3), inset 0 1px 0 rgba(255, 255, 255, 0.1) !important;
        transition: all 0.25s cubic-bezier(0.16, 1, 0.3, 1) !important;
        opacity: 1 !important;
        visibility: visible !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        cursor: pointer !important;
        pointer-events: auto !important;
        z-index: 100000 !important;
    }
    [data-testid="collapsedControl"]:hover,
    [data-testid="stSidebarCollapseButton"] button:hover,
    [data-testid="stExpandSidebarButton"]:hover,
    button[data-testid="stExpandSidebarButton"]:hover {
        background-color: #38bdf8 !important;
        color: #070a13 !important;
        box-shadow: 0 0 20px rgba(56, 189, 248, 0.55) !important;
        transform: scale(1.05) !important;
    }
    [data-testid="collapsedControl"]:active,
    [data-testid="stSidebarCollapseButton"] button:active,
    [data-testid="stExpandSidebarButton"]:active,
    button[data-testid="stExpandSidebarButton"]:active {
        transform: scale(0.96) !important;
    }
    [data-testid="collapsedControl"] svg,
    [data-testid="stSidebarCollapseButton"] svg,
    [data-testid="stExpandSidebarButton"] svg,
    button[data-testid="stExpandSidebarButton"] svg {
        stroke: currentColor !important;
        fill: currentColor !important;
        width: 18px !important;
        height: 18px !important;
    }

    /* 2. Compact Top-Right Settings Popover Button (Zero Overlap, Spring Physics) */
    div[data-testid="stPopover"] {
        display: flex;
        align-items: center;
        justify-content: flex-end;
    }
    div[data-testid="stPopover"] > button {
        background-color: #0f172a !important;
        border: 1px solid rgba(255, 255, 255, 0.12) !important;
        color: #38bdf8 !important;
        border-radius: 8px !important;
        padding: 5px 10px !important;
        font-size: 1.15rem !important;
        line-height: 1 !important;
        min-height: 38px !important;
        width: 100% !important;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.08), 0 2px 8px rgba(0, 0, 0, 0.3) !important;
        transition: all 0.25s cubic-bezier(0.16, 1, 0.3, 1) !important;
    }
    div[data-testid="stPopover"] > button:hover {
        border-color: #38bdf8 !important;
        background-color: rgba(56, 189, 248, 0.15) !important;
        box-shadow: 0 0 16px rgba(56, 189, 248, 0.4) !important;
        transform: translateY(-1.5px) !important;
    }
    div[data-testid="stPopover"] > button:active {
        transform: scale(0.95) translateY(1px) !important;
    }

    /* 3. Perpetual Micro-Interactions: Authentic Pulsing Indicators */
    @keyframes pulse-ring-green {
        0% {
            box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
        }
        70% {
            box-shadow: 0 0 0 7px rgba(16, 185, 129, 0);
        }
        100% {
            box-shadow: 0 0 0 0 rgba(16, 185, 129, 0);
        }
    }
    @keyframes pulse-ring-red {
        0% {
            box-shadow: 0 0 0 0 rgba(239, 68, 68, 0.7);
        }
        70% {
            box-shadow: 0 0 0 7px rgba(239, 68, 68, 0);
        }
        100% {
            box-shadow: 0 0 0 0 rgba(239, 68, 68, 0);
        }
    }

    .status-dot-pulse-green {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background-color: #10b981;
        display: inline-block;
        animation: pulse-ring-green 2s infinite cubic-bezier(0.4, 0, 0.6, 1);
        margin-right: 6px;
        vertical-align: middle;
    }
    .status-dot-pulse-red {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background-color: #ef4444;
        display: inline-block;
        animation: pulse-ring-red 2s infinite cubic-bezier(0.4, 0, 0.6, 1);
        margin-right: 6px;
        vertical-align: middle;
    }

    /* 4. Badges (Institutional Semantic Coherence & Glass Refraction) */
    .cro-badge-approved {
        background-color: rgba(16, 185, 129, 0.12);
        color: #10b981;
        border: 1px solid rgba(16, 185, 129, 0.35);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05);
        padding: 5px 12px;
        border-radius: 6px;
        font-weight: 700;
        font-size: 0.76rem;
        letter-spacing: 0.04em;
        display: inline-flex;
        align-items: center;
        white-space: nowrap;
        transition: all 0.2s ease;
    }
    .cro-badge-rejected {
        background-color: rgba(239, 68, 68, 0.12);
        color: #ef4444;
        border: 1px solid rgba(239, 68, 68, 0.35);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05);
        padding: 5px 12px;
        border-radius: 6px;
        font-weight: 700;
        font-size: 0.76rem;
        letter-spacing: 0.04em;
        display: inline-flex;
        align-items: center;
        white-space: nowrap;
        transition: all 0.2s ease;
    }
    .badge-info {
        background-color: rgba(56, 189, 248, 0.10);
        color: #38bdf8;
        border: 1px solid rgba(56, 189, 248, 0.28);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05);
        padding: 5px 12px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 0.75rem;
        letter-spacing: 0.03em;
        display: inline-flex;
        align-items: center;
        gap: 6px;
        white-space: nowrap;
    }
    .badge-ticker {
        background: #0f172a;
        color: #f8fafc;
        border: 1px solid #1e293b;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05);
        padding: 4px 10px;
        border-radius: 6px;
        font-family: 'JetBrains Mono', 'Roboto Mono', monospace;
        font-size: 0.82rem;
        font-weight: 700;
        display: inline-flex;
        align-items: center;
        white-space: nowrap;
    }

    /* 5. Spacious High-Agency Sidebar (480px Expanded Width for Rich Chat) */
    section[data-testid="stSidebar"],
    div[data-testid="stSidebar"] {
        background-color: #090e1a !important;
        border-right: 1px solid #1e293b !important;
    }
    
    /* Enforce generous width when sidebar is open */
    section[data-testid="stSidebar"]:not([data-collapsed="true"]),
    div[data-testid="stSidebar"]:not([data-collapsed="true"]) {
        width: 480px !important;
        min-width: 480px !important;
        max-width: 520px !important;
    }
    section[data-testid="stSidebar"]:not([data-collapsed="true"]) > div:first-child,
    div[data-testid="stSidebar"]:not([data-collapsed="true"]) > div:first-child {
        width: 480px !important;
        min-width: 480px !important;
    }
    section[data-testid="stSidebar"]:not([data-collapsed="true"]) [data-testid="stSidebarContent"],
    div[data-testid="stSidebar"]:not([data-collapsed="true"]) [data-testid="stSidebarContent"] {
        width: 480px !important;
        min-width: 480px !important;
        scrollbar-gutter: auto !important;
        overflow-y: hidden !important;
        -ms-overflow-style: none !important;
        scrollbar-width: none !important;
    }
    section[data-testid="stSidebar"] [data-testid="stSidebarContent"]::-webkit-scrollbar {
        display: none !important;
        width: 0 !important;
        height: 0 !important;
    }
    div[data-testid="stSidebarUserContent"] {
        max-width: 100% !important;
        padding-top: 0 !important;
        padding-bottom: 0.5rem !important;
        padding-left: 1.25rem !important;
        padding-right: 1.25rem !important;
    }

    section[data-testid="stSidebar"] .stChatMessage {
        background-color: rgba(15, 23, 42, 0.45) !important;
        backdrop-filter: blur(10px) !important;
        -webkit-backdrop-filter: blur(10px) !important;
        border: 1px solid rgba(255, 255, 255, 0.08) !important;
        border-radius: 10px !important;
        padding: 10px 14px !important;
        margin-bottom: 6px !important;
        font-size: 0.86rem !important;
        line-height: 1.45 !important;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03), 0 2px 6px rgba(0, 0, 0, 0.2) !important;
    }
    /* Ultra-translucent glassmorphic suggestion bubbles */
    section[data-testid="stSidebar"] div[data-testid="stButton"] button,
    section[data-testid="stSidebar"] button[data-testid="baseButton-secondary"],
    section[data-testid="stSidebar"] button[kind="secondary"] {
        border-radius: 9999px !important;
        font-size: 0.74rem !important;
        font-weight: 600 !important;
        padding: 5px 12px !important;
        min-height: 32px !important;
        background: rgba(18, 24, 46, 0.5) !important;
        border: 1px solid rgba(56, 189, 248, 0.3) !important;
        color: #e2e8f0 !important;
        backdrop-filter: blur(12px) !important;
        -webkit-backdrop-filter: blur(12px) !important;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05), 0 2px 6px rgba(0, 0, 0, 0.15) !important;
        transition: all 0.22s cubic-bezier(0.16, 1, 0.3, 1) !important;
        display: inline-flex !important;
        align-items: center !important;
        justify-content: center !important;
        white-space: normal !important;
    }
    section[data-testid="stSidebar"] div[data-testid="stButton"] button p,
    section[data-testid="stSidebar"] div[data-testid="stButton"] button span:not([data-testid="stIconMaterial"]) {
        color: inherit !important;
        font-size: inherit !important;
    }
    section[data-testid="stSidebar"] div[data-testid="stButton"] button span[data-testid="stIconMaterial"],
    section[data-testid="stSidebar"] span[data-testid="stIconMaterial"] {
        font-size: 1.15rem !important;
        line-height: 1 !important;
        display: inline-flex !important;
        align-items: center !important;
        justify-content: center !important;
    }
    /* Clear chat button styling */
    section[data-testid="stSidebar"] div[data-testid="stButton"]:has(button[key="sb_btn_clear"]) button {
        border-radius: 8px !important;
        padding: 4px 6px !important;
        min-height: 32px !important;
        background: rgba(18, 24, 46, 0.45) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        color: #94a3b8 !important;
    }
    section[data-testid="stSidebar"] div[data-testid="stButton"]:has(button[key="sb_btn_clear"]) button:hover {
        background: rgba(239, 68, 68, 0.15) !important;
        border-color: rgba(239, 68, 68, 0.45) !important;
        color: #fca5a5 !important;
        box-shadow: 0 0 12px rgba(239, 68, 68, 0.25) !important;
    }
    section[data-testid="stSidebar"] div[data-testid="stButton"] button:hover,
    section[data-testid="stSidebar"] button[data-testid="baseButton-secondary"]:hover,
    section[data-testid="stSidebar"] button[kind="secondary"]:hover {
        border-color: #38bdf8 !important;
        color: #38bdf8 !important;
        background: rgba(56, 189, 248, 0.15) !important;
        transform: translateY(-1.5px) !important;
        box-shadow: 0 0 14px rgba(56, 189, 248, 0.25), inset 0 1px 0 rgba(255, 255, 255, 0.1) !important;
    }
    section[data-testid="stSidebar"] div[data-testid="stButton"] button:active,
    section[data-testid="stSidebar"] button[data-testid="baseButton-secondary"]:active,
    section[data-testid="stSidebar"] button[kind="secondary"]:active {
        transform: scale(0.96) !important;
        background: rgba(56, 189, 248, 0.2) !important;
    }

    /* 6. Unified Single-Bar Navigation & Integrated Settings Icon */
    div[data-testid="stPopover"] {
        position: absolute !important;
        top: 0.44rem !important;
        right: calc(1rem + 8px) !important;
        z-index: 100 !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    div[data-testid="stPopover"] button[data-testid="stPopoverButton"],
    div[data-testid="stPopover"] > div > div > button {
        height: 28px !important;
        min-height: 28px !important;
        width: 32px !important;
        min-width: 32px !important;
        padding: 0 !important;
        background: transparent !important;
        border: 1px solid transparent !important;
        border-radius: 6px !important;
        display: inline-flex !important;
        align-items: center !important;
        justify-content: center !important;
        color: #94a3b8 !important;
        box-shadow: none !important;
        transition: all 0.2s cubic-bezier(0.16, 1, 0.3, 1) !important;
    }
    div[data-testid="stPopover"] button[data-testid="stPopoverButton"]:hover,
    div[data-testid="stPopover"] > div > div > button:hover {
        border-color: rgba(56, 189, 248, 0.4) !important;
        background: #1e293b !important;
        color: #38bdf8 !important;
        box-shadow: 0 0 10px rgba(56, 189, 248, 0.25) !important;
    }
    /* Hide the drop arrow inside popover button so it acts as an integrated icon */
    button[data-testid="stPopoverButton"] [data-testid="stIconMaterial"] {
        display: none !important;
    }

    div[data-testid="stTabs"] div[role="tablist"],
    .stTabs [data-baseweb="tab-list"] {
        display: flex !important;
        width: 100% !important;
        gap: 6px !important;
        background-color: #0f172a !important;
        padding: 3px 6px !important;
        border-radius: 8px !important;
        border: 1px solid #1e293b !important;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04) !important;
        margin-top: 0px !important;
        margin-bottom: 4px !important;
        box-sizing: border-box !important;
        align-items: center !important;
        padding-right: 44px !important; /* Space for the seamlessly integrated ⚙️ inside this same bar */
    }
    div[data-testid="stTabs"] div[role="tab"],
    div[data-testid="stTab"],
    .stTabs [data-baseweb="tab"] {
        height: 28px !important;
        color: #94a3b8 !important;
        font-weight: 600 !important;
        font-size: 0.80rem !important;
        padding: 3px 14px !important;
        border-radius: 5px !important;
        background: transparent !important;
        border: 1px solid transparent !important;
        transition: all 0.2s cubic-bezier(0.16, 1, 0.3, 1) !important;
    }
    div[data-testid="stTabs"] div[role="tab"]:hover,
    div[data-testid="stTab"]:hover,
    .stTabs [data-baseweb="tab"]:hover {
        color: #f8fafc !important;
        background-color: rgba(255, 255, 255, 0.04) !important;
    }
    div[data-testid="stTabs"] div[role="tab"][aria-selected="true"],
    div[data-testid="stTab"][aria-selected="true"],
    .stTabs [aria-selected="true"] {
        color: #38bdf8 !important;
        background-color: #1e293b !important;
        border: 1px solid rgba(56, 189, 248, 0.35) !important;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.08), 0 2px 8px rgba(0, 0, 0, 0.25) !important;
        font-weight: 700 !important;
    }

    /* 7. Metric Cards & Strategy Containers (Liquid Glass Elevation) */
    .metric-card {
        background: linear-gradient(180deg, rgba(15, 23, 42, 0.95) 0%, rgba(11, 17, 32, 0.9) 100%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 10px;
        padding: 18px 22px;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06), 0 4px 14px rgba(0, 0, 0, 0.3);
        transition: all 0.25s cubic-bezier(0.16, 1, 0.3, 1);
    }
    .metric-card:hover {
        transform: translateY(-2px);
        border-color: rgba(56, 189, 248, 0.3);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.08), 0 8px 22px rgba(0, 0, 0, 0.4);
    }
    .metric-title {
        font-size: 0.74rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #94a3b8;
        margin-bottom: 6px;
    }
    .metric-value {
        font-size: 1.45rem;
        font-weight: 700;
        font-family: 'JetBrains Mono', 'Roboto Mono', monospace;
        color: #f8fafc;
        letter-spacing: -0.02em;
    }
    .metric-sub {
        font-size: 0.75rem;
        color: #64748b;
        margin-top: 5px;
    }

    .strategy-box {
        background: linear-gradient(180deg, rgba(15, 23, 42, 0.95) 0%, rgba(11, 17, 32, 0.9) 100%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 10px;
        padding: 20px 24px;
        margin-top: 12px;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06), 0 4px 14px rgba(0, 0, 0, 0.3);
        transition: all 0.25s cubic-bezier(0.16, 1, 0.3, 1);
    }
    .strategy-box:hover {
        border-color: rgba(255, 255, 255, 0.14);
    }

    /* 8. Buttons (Tactile Feedback & Spring Physics) */
    .stButton > button[kind="primary"],
    button[data-testid="baseButton-primary"] {
        background: #0284c7 !important;
        color: #ffffff !important;
        font-weight: 700 !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-radius: 8px !important;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.2), 0 0 16px rgba(2, 132, 199, 0.3) !important;
        transition: all 0.22s cubic-bezier(0.16, 1, 0.3, 1) !important;
        min-height: 40px !important;
    }
    .stButton > button[kind="primary"]:hover,
    button[data-testid="baseButton-primary"]:hover {
        background: #0369a1 !important;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.25), 0 0 22px rgba(56, 189, 248, 0.45) !important;
        transform: translateY(-1.5px) !important;
    }
    .stButton > button[kind="primary"]:active,
    button[data-testid="baseButton-primary"]:active {
        transform: scale(0.97) translateY(1px) !important;
    }
    .stMain .stButton > button[kind="secondary"],
    div[data-testid="stMain"] button[data-testid="baseButton-secondary"] {
        background: #1e293b !important;
        color: #cbd5e1 !important;
        border: 1px solid rgba(255, 255, 255, 0.08) !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04) !important;
        transition: all 0.22s cubic-bezier(0.16, 1, 0.3, 1) !important;
        min-height: 40px !important;
    }
    .stMain .stButton > button[kind="secondary"]:hover,
    div[data-testid="stMain"] button[data-testid="baseButton-secondary"]:hover {
        background: #273549 !important;
        color: #ffffff !important;
        border-color: #38bdf8 !important;
        transform: translateY(-1.5px) !important;
    }
    .stMain .stButton > button[kind="secondary"]:active,
    div[data-testid="stMain"] button[data-testid="baseButton-secondary"]:active {
        transform: scale(0.97) translateY(1px) !important;
    }

    /* 9. Form Inputs (Selectores de Activo, Temporalidad, Ventana - Streamlit 1.67+ React Aria) */
    
    /* Contenedores de las casillas: Activo, Temporalidad y Ventana */
    div[data-testid="stSelectbox"] div[role="group"],
    div[data-testid="stSelectbox"] div[data-rac][role="group"],
    div[data-testid="stNumberInputContainer"] {
        background-color: #0d1324 !important;  /* <- 1. COLOR DE FONDO DE LAS CASILLAS */
        border: 1px solid #1e293b !important;  /* <- 2. COLOR DEL BORDE */
        border-radius: 8px !important;
        transition: border-color 0.2s ease, background-color 0.2s ease !important;
    }

    /* Hover en los contenedores */
    div[data-testid="stSelectbox"] div[role="group"]:hover,
    div[data-testid="stSelectbox"] div[data-rac][role="group"]:hover,
    div[data-testid="stNumberInputContainer"]:hover {
        border-color: #38bdf8 !important;     /* <- COLOR DEL BORDE AL PASAR EL CURSOR */
    }

    /* Texto interno y campos editables de los inputs */
    div[data-testid="stSelectbox"] input,
    input[data-testid="stNumberInputField"],
    div[data-testid="stNumberInput"] input {
        color: #f8fafc !important;             /* <- 3. COLOR DEL TEXTO */
        background-color: transparent !important;
    }

    /* Botones de incremento (+ y -) de Ventana (Días) */
    button[data-testid="stNumberInputStepDown"],
    button[data-testid="stNumberInputStepUp"] {
        color: #94a3b8 !important;             /* <- 4. COLOR DE LOS SIGNOS + Y - */
        background-color: transparent !important;
        border: none !important;
    }
    button[data-testid="stNumberInputStepDown"]:hover,
    button[data-testid="stNumberInputStepUp"]:hover {
        color: #38bdf8 !important;             /* <- Color al pasar el cursor sobre + o - */
    }

    /* Flecha desplegable (chevron) de los Selectbox */
    div[data-testid="stSelectbox"] button svg,
    div[data-testid="stSelectbox"] svg {
        color: #94a3b8 !important;             /* <- 5. COLOR DE LA FLECHA DESPLEGABLE */
        fill: currentColor !important;
    }
    div[data-testid="stSelectbox"] button:hover svg {
        color: #38bdf8 !important;
    }

    /* Títulos / Etiquetas de los inputs ("Temporalidad", "Ventana (Días)") */
    div[data-testid="stSelectbox"] label[data-testid="stWidgetLabel"] p,
    div[data-testid="stNumberInput"] label[data-testid="stWidgetLabel"] p {
        color: #94a3b8 !important;             /* <- 6. COLOR DE LOS TÍTULOS SUPERIORES */
        font-size: 0.76rem !important;
        font-weight: 600 !important;
    }

    /* Menú desplegable cuando se abre la lista de opciones */
    div[data-testid="stSelectboxVirtualDropdown"] {
        background-color: #0b1120 !important;  /* <- 7. FONDO DE LA LISTA DESPLEGABLE */
        border: 1px solid #1e293b !important;
        border-radius: 8px !important;
        box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.5) !important;
    }
    div[data-testid="stSelectboxVirtualDropdown"] [role="option"] {
        color: #f8fafc !important;
    }
    div[data-testid="stSelectboxVirtualDropdown"] [role="option"]:hover,
    div[data-testid="stSelectboxVirtualDropdown"] [role="option"][aria-selected="true"] {
        background-color: rgba(56, 189, 248, 0.15) !important;
        color: #38bdf8 !important;
    }

    /* Accessibility Focus Indicator */
    button:focus-visible, input:focus-visible, select:focus-visible, [tabindex="0"]:focus-visible {
        outline: 2px solid #38bdf8 !important;
        outline-offset: 2px !important;
    }
</style>
""", unsafe_allow_html=True)


# ==============================================================================
# 2. DATA LOADERS & MULTI-TIER CACHING ENGINE
# ==============================================================================
@st.cache_data(ttl=300, show_spinner=False)
def load_candles_from_snowflake(market: str, ticker: str, timeframe: str, limit: int = 2016) -> pd.DataFrame:
    """Fetch closed historical candles from SILVER_CLEAN.MARKET_CANDLES with 300s cache."""
    sql = """
    SELECT 
        TIMESTAMP_UTC,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        VOLUME,
        EXTRA_METRICS
    FROM QUANT_INSTITUTIONAL_DB.SILVER_CLEAN.MARKET_CANDLES
    WHERE MARKET = %s
      AND TICKER = %s
      AND TIMEFRAME = %s
    ORDER BY TIMESTAMP_UTC DESC
    LIMIT %s
    """
    conn = None
    try:
        conn = get_raw_connection(query_tag="STREAMLIT_UI_CANDLES")
        with conn.cursor() as cur:
            cur.execute(sql, (market.upper(), ticker.upper(), timeframe.lower(), limit))
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


def get_live_ws_candle(symbol: str) -> Optional[Dict[str, Any]]:
    """Read live forming candle and CVD from the WebSocket worker's local state file."""
    state_file = Path("data/live_state") / f"live_{symbol.lower()}.json"
    if not state_file.exists():
        return None
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Check if the state was updated within the last 120 seconds
        if time.time() - data.get("last_updated_epoch", 0) < 120:
            return data
        return None
    except Exception:
        return None


def get_candles_with_live_overlay(market: str, ticker: str, timeframe: str, limit: int = 2016) -> pd.DataFrame:
    """Combine closed historical bars from Snowflake with the live forming bar from the WebSocket worker."""
    df = load_candles_from_snowflake(market=market, ticker=ticker, timeframe=timeframe, limit=limit)

    live_candle = get_live_ws_candle(ticker)
    if live_candle and live_candle.get("timestamp_utc"):
        live_ts = pd.to_datetime(live_candle["timestamp_utc"])
        live_close = float(live_candle.get("close", 0.0))
        # Sanity check: prevent synthetic/outlier test candle from distorting live chart
        is_sane = True
        if not df.empty and live_close > 0:
            last_hist_close = float(df["CLOSE"].iloc[-1])
            max_dev = 0.25 if timeframe == "1d" else 0.12
            if last_hist_close > 0 and abs(live_close - last_hist_close) / last_hist_close > max_dev:
                logger.warning(f"Live candle price {live_close} deviates excessively (>{int(max_dev*100)}%) from historical close {last_hist_close}. Ignoring rogue live candle.")
                is_sane = False

        if is_sane:
            live_row = {
                "TIMESTAMP_UTC": live_ts,
                "OPEN": float(live_candle.get("open", 0.0)),
                "HIGH": float(live_candle.get("high", 0.0)),
                "LOW": float(live_candle.get("low", 0.0)),
                "CLOSE": live_close,
                "VOLUME": float(live_candle.get("volume", 0.0)),
                "EXTRA_METRICS": {"cvd": live_candle.get("cvd", 0.0), "is_live_ws": True}
            }

            if df.empty:
                df = pd.DataFrame([live_row])
            else:
                last_ts = df["TIMESTAMP_UTC"].iloc[-1]
                if live_ts == last_ts:
                    for col in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "EXTRA_METRICS"]:
                        df.at[df.index[-1], col] = live_row[col]
                elif live_ts > last_ts:
                    df = pd.concat([df, pd.DataFrame([live_row])], ignore_index=True)

    return df



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

    # Sanity filter: remove any extreme outlier / rogue records that distort scale
    if not df_sorted.empty and "CLOSE" in df_sorted.columns:
        price_cols = [c for c in ["OPEN", "HIGH", "LOW", "CLOSE"] if c in df_sorted.columns]
        for c in price_cols:
            df_sorted[c] = pd.to_numeric(df_sorted[c], errors="coerce")
        median_price = float(df_sorted["CLOSE"].median())
        q25 = float(df_sorted["CLOSE"].quantile(0.25))
        q75 = float(df_sorted["CLOSE"].quantile(0.75))
        iqr = q75 - q25
        if median_price > 0:
            dev_ratio = 0.35 if timeframe == "1d" else 0.12
            bound = max(median_price * dev_ratio, 3.5 * (iqr if iqr > 0 else median_price * 0.03))
            lower_b = max(0.000001, median_price - bound)
            upper_b = median_price + bound

            mask = pd.Series(True, index=df_sorted.index)
            for c in price_cols:
                mask = mask & (df_sorted[c] >= lower_b) & (df_sorted[c] <= upper_b)
            df_sorted = df_sorted[mask].reset_index(drop=True)

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
                background: #070a13;
                border: 1px solid #1e293b;
                border-radius: 12px;
                overflow: hidden;
            }}
            #tv-container {{
                width: 100%;
                height: 100%;
            }}
            #hud-bar {{
                position: absolute;
                top: 14px;
                left: 14px;
                z-index: 25;
                display: flex;
                flex-wrap: wrap;
                align-items: center;
                gap: 12px;
                row-gap: 8px;
                pointer-events: auto;
            }}
            .range-group {{
                display: flex;
                align-items: center;
                gap: 3px;
                background: rgba(15, 23, 42, 0.88);
                backdrop-filter: blur(12px);
                -webkit-backdrop-filter: blur(12px);
                border: 1px solid rgba(255, 255, 255, 0.09);
                border-radius: 8px;
                padding: 4px 6px;
                box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06), 0 4px 12px rgba(0, 0, 0, 0.35);
            }}
            .range-btn {{
                background: transparent;
                border: 1px solid transparent;
                color: #94a3b8;
                font-size: 11px;
                font-weight: 600;
                padding: 4px 11px;
                border-radius: 5px;
                cursor: pointer;
                transition: all 0.2s cubic-bezier(0.16, 1, 0.3, 1);
            }}
            .range-btn:hover {{
                background: rgba(255, 255, 255, 0.06);
                color: #f8fafc;
            }}
            .range-btn.active {{
                background: #0284c7;
                color: #ffffff;
                font-weight: 700;
                border-color: rgba(255, 255, 255, 0.15);
                box-shadow: 0 0 12px rgba(2, 132, 199, 0.45);
            }}
            .range-btn:active {{
                transform: scale(0.95);
            }}
            .hud-card {{
                display: flex;
                align-items: center;
                gap: 10px;
                background: rgba(15, 23, 42, 0.88);
                backdrop-filter: blur(12px);
                -webkit-backdrop-filter: blur(12px);
                border: 1px solid rgba(255, 255, 255, 0.09);
                border-radius: 8px;
                padding: 5px 14px;
                font-family: 'JetBrains Mono', 'Roboto Mono', monospace;
                font-size: 11px;
                color: #94a3b8;
                box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06), 0 4px 12px rgba(0, 0, 0, 0.35);
            }}
            .hud-badge {{
                color: #38bdf8;
                font-weight: 700;
                letter-spacing: 0.02em;
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
                    <button class="range-btn active" onclick="selectHorizon('1d', this)">1D</button>
                    <button class="range-btn" onclick="selectHorizon('3d', this)">3D</button>
                    <button class="range-btn" onclick="selectHorizon('7d', this)">7D</button>
                    <button class="range-btn" onclick="resetScale()" style="color: #38bdf8;">Reset</button>
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
                    minBarSpacing: 2.5,
                    maxBarSpacing: 45,
                    rightOffset: 10,
                    fixLeftEdge: false,
                    fixRightEdge: false,
                    lockVisibleTimeRangeOnResize: true
                }},
                handleScroll: {{
                    mouseWheel: false,
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

            // 6. Timeframe Range Navigator & Initial Scaling
            function setInitialView() {{
                if (!candles || candles.length === 0) return;
                const total = candles.length;
                const visibleCount = Math.min(total, 95);
                chart.timeScale().setVisibleLogicalRange({{
                    from: total - visibleCount,
                    to: total + 6
                }});
            }}

            window.resetScale = function() {{
                setInitialView();
                chart.priceScale('right').applyOptions({{ autoScale: true }});
            }};

            window.selectHorizon = function(horizon, btn) {{
                document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
                if (btn) btn.classList.add('active');
                if (!candles || !candles.length) return;
                
                const total = candles.length;
                const lastTime = candles[total - 1].time;
                let secs = 0;
                if (horizon === '6h') secs = 6 * 3600;
                else if (horizon === '1d') secs = 24 * 3600;
                else if (horizon === '3d') secs = 3 * 24 * 3600;
                else if (horizon === '7d' || horizon === 'all') secs = 7 * 24 * 3600;

                if (secs > 0) {{
                    const targetTime = lastTime - secs;
                    let targetIndex = 0;
                    for (let i = total - 1; i >= 0; i--) {{
                        if (candles[i].time <= targetTime) {{
                            targetIndex = i;
                            break;
                        }}
                    }}
                    chart.timeScale().setVisibleLogicalRange({{
                        from: targetIndex,
                        to: total + 6
                    }});
                }} else {{
                    window.resetScale();
                }}
            }};

            // Comfortable default initial view on load
            setInitialView();

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

    # 1. Sidebar - Asset Selection
    with st.sidebar:
        st.markdown("""
        <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; padding-bottom: 6px; border-bottom: 1px solid #1e293b;">
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
            index=0,
            label_visibility="collapsed"
        )
        market_choice, ticker = ASSET_REGISTRY[selected_asset_label]

        col_tf1, col_tf2 = st.columns([1.3, 1.0])
        with col_tf1:
            timeframe = st.selectbox("Temporalidad", options=["5m", "15m", "1h", "1d"], index=0)
        with col_tf2:
            days_lookback = st.number_input("Ventana (Días)", min_value=1, max_value=7, value=7, step=1)

    # 2. Load Candlestick Data & Quant State from Snowflake (Multi-tier Cache)
    df_candles = get_candles_with_live_overlay(market=market_choice, ticker=ticker, timeframe=timeframe, limit=2016)
    
    sync_warning = None
    if df_candles.empty:
        with st.spinner(f"Sincronizando {market_choice}:{ticker} ({timeframe}) directamente en Snowflake..."):
            try:
                sync_single_asset(market=market_choice, ticker=ticker, timeframe=timeframe, days=days_lookback)
                st.cache_data.clear()
                clear_gold_tools_cache()
                df_candles = get_candles_with_live_overlay(market=market_choice, ticker=ticker, timeframe=timeframe, limit=2016)
            except Exception as e:
                logger.warning(f"Error sincronizando {ticker} con Snowflake: {e}")
                sync_warning = str(e)

    quant_state = load_gold_state(ticker=ticker, timeframe=timeframe, market=market_choice)
    last_price = quant_state.get("last_price", df_candles["CLOSE"].iloc[-1] if not df_candles.empty else 0.0)
    reg_data = quant_state.get("regime", {})
    regime = reg_data.get("regime", "TRENDING_MOMENTUM")
    hurst = reg_data.get("hurst", 0.55)

    # 3. Sidebar - Asesor Cuantitativo (Chat IA con Burbujas de Sugerencias)
    with st.sidebar:
        st.markdown("<div style='height: 1px; background: rgba(255, 255, 255, 0.08); margin: 6px 0 8px 0;'></div>", unsafe_allow_html=True)
        col_sb_title, col_sb_clear = st.columns([0.84, 0.16])
        with col_sb_title:
            st.markdown(f"""
            <div style="display: flex; align-items: center; gap: 6px;">
                <span style="font-size: 0.88rem; font-weight: 800; color: #ffffff; letter-spacing: 0.03em;"> ASESOR CUANTITATIVO</span>
                
            </div>
            <div style="font-size: 0.70rem; color: #94a3b8; margin-top: 2px;">
                Foco: <b style="color:#38bdf8;">{ticker}</b> ({timeframe}) | <b>${last_price:,.2f}</b>
            </div>
            """, unsafe_allow_html=True)
        with col_sb_clear:
            if st.button("", icon=":material/mop:", help="Limpiar historial de la conversación", key="sb_btn_clear", use_container_width=True):
                st.session_state["chat_messages"] = []
                st.rerun()

        trigger_prompt = None
        chat_box = st.container(height=570)
        with chat_box:
            if not st.session_state["chat_messages"]:
                st.markdown(
                    f"""
                    <div style='background: rgba(255, 255, 25   5, 0.025); backdrop-filter: blur(8px); border: 1px solid rgba(255, 255, 255, 0.08); border-radius: 8px; padding: 10px 12px; margin-bottom: 8px; font-size: 0.78rem; line-height: 1.45; color: #94a3b8;'>
                        <div style='color: #38bdf8; font-weight: 700; margin-bottom: 3px;'> Asesor Cuantitativo listo</div>
                        Monitoreando <b>{ticker}</b> ({timeframe}). Escribe tu consulta abajo o toca una sugerencia rápida:
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                st.markdown("<p style='font-size: 0.68rem; color: #64748b; font-weight: 700; text-transform: uppercase; margin: 4px 0 6px 2px; letter-spacing: 0.05em;'>💡 Sugerencias Rápidas:</p>", unsafe_allow_html=True)

                col_chip1, col_chip2 = st.columns(2)
                with col_chip1:
                    if st.button("📋 Plan Trading", use_container_width=True, key="chip_plan", help="Genera plan estructurado con R:R"):
                        trigger_prompt = f"Genera un plan de trading institucional detallado para {ticker} ({timeframe}) con niveles de entrada, Stop Loss por ATR, Take Profit y ratio R:R."
                    if st.button("🔍 Microestructura", use_container_width=True, key="chip_micro", help="VWAP, POC y CVD"):
                        trigger_prompt = f"Analiza en detalle la microestructura actual de {ticker} ({timeframe}): VWAP, POC, áreas de valor (VAH/VAL) y flujo CVD."
                with col_chip2:
                    if st.button("🛡️ Veredicto CRO", use_container_width=True, key="chip_cro", help="Filtro determinista de riesgo"):
                        trigger_prompt = f"Explica el veredicto del Chief Risk Officer (CRO) sobre {ticker}. ¿Autoriza o veta la operación y cuáles son los criterios de preservación de capital?"
                    if st.button("💧 Zonas Liquidez", use_container_width=True, key="chip_liq", help="Pools de liquidez institucional"):
                        trigger_prompt = f"Identifica los liquidity pools y zonas de alta liquidez para {ticker} ({timeframe}) y dónde están los stops institucionales."
            else:
                for msg in st.session_state["chat_messages"]:
                    with st.chat_message(msg["role"]):
                        st.markdown(msg["content"])
                        if msg.get("chart") is not None:
                            st.plotly_chart(msg["chart"], use_container_width=True)

                with st.expander("💡 Sugerencias rápidas", expanded=False):
                    ec1, ec2 = st.columns(2)
                    with ec1:
                        if st.button("📋 Plan Trading", use_container_width=True, key="exp_plan"):
                            trigger_prompt = f"Genera un plan de trading institucional detallado para {ticker} ({timeframe}) con niveles de entrada, Stop Loss por ATR, Take Profit y ratio R:R."
                        if st.button("🔍 Microestructura", use_container_width=True, key="exp_micro"):
                            trigger_prompt = f"Analiza en detalle la microestructura actual de {ticker} ({timeframe}): VWAP, POC, áreas de valor (VAH/VAL) y flujo CVD."
                    with ec2:
                        if st.button("🛡️ Veredicto CRO", use_container_width=True, key="exp_cro"):
                            trigger_prompt = f"Explica el veredicto del Chief Risk Officer (CRO) sobre {ticker}."
                        if st.button("💧 Zonas Liquidez", use_container_width=True, key="exp_liq"):
                            trigger_prompt = f"Identifica los liquidity pools y zonas de alta liquidez para {ticker} ({timeframe})."

        user_input = st.chat_input("Consulta al Asesor IA...", key="sidebar_chat_input")
        active_query = trigger_prompt or user_input

        if active_query:
            st.session_state["chat_messages"].append({"role": "user", "content": active_query, "chart": None})
            with st.spinner("Analizando con Gemini..."):
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

    # 4. Main Canvas: Menú de Configuración Unificado (Ultra-Compacto Flotante Superior)
    with st.popover("⚙️", help="Configuración del Terminal y Parámetros"):
        st.markdown("<h4 style='margin-top:0; color:#38bdf8;'>⚙️ Panel de Configuración</h4>", unsafe_allow_html=True)
        
        st.markdown("##### 1. Capas de Microestructura")
        show_vwap = st.checkbox("Línea VWAP de Sesión", value=True, key="cfg_vwap")
        show_poc = st.checkbox("Nivel POC (Point of Control)", value=True, key="cfg_poc")
        show_bands = st.checkbox("Bandas VWAP (±2s)", value=False, key="cfg_bands")
        show_val_vah = st.checkbox("Áreas de Valor (VAH / VAL)", value=False, key="cfg_val_vah")
        show_volume = st.checkbox("Histograma de Volumen", value=True, key="cfg_volume")
        
        st.markdown("---")
        st.markdown("##### 2. Pipeline en Vivo & Sincronización")
        live_ws = get_live_ws_candle(ticker)
        if live_ws:
            st.markdown(
                f"<div style='background:rgba(16,185,129,0.15); border:1px solid #10b981; border-radius:6px; padding:8px; font-size:0.8rem;'>"
                f"🟢 <b>WebSocket Binance Conectado</b><br>"
                f"Precio vivo: <b>${live_ws.get('close', 0.0):,.2f}</b><br>"
                f"CVD tiempo real: <b>{live_ws.get('cvd', 0.0):+,.2f}</b><br>"
                f"Trades vela 5m: <b>{live_ws.get('trades_count', 0)}</b>"
                f"</div>",
                unsafe_allow_html=True
            )
        else:
            st.markdown(
                "<div style='background:rgba(148,163,184,0.1); border:1px solid #334155; border-radius:6px; padding:8px; font-size:0.8rem; color:#94a3b8;'>"
                "⚪ <b>Worker Offline</b><br>"
                "Ejecuta en terminal:<br><code>python3 -m src.pipeline.ws_binance_worker</code>"
                "</div>",
                unsafe_allow_html=True
            )
        
        col_s1, col_s2 = st.columns(2)
        with col_s1:
            if st.button("Sincronizar Activo", use_container_width=True, type="primary", key="pop_sync"):
                with st.spinner(f"Sincronizando {ticker} con Snowflake..."):
                    try:
                        sync_single_asset(market=market_choice, ticker=ticker, timeframe=timeframe, days=days_lookback)
                        st.cache_data.clear()
                        clear_gold_tools_cache()
                        st.success("Sincronizado.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al sincronizar con Snowflake: {e}")
        with col_s2:
            if st.button("Purgar Caché", use_container_width=True, key="pop_purge"):
                st.cache_data.clear()
                clear_gold_tools_cache()
                st.toast("Caché purgada.", icon="✅")
                st.rerun()

        st.markdown("---")
        st.markdown("##### 3. Gestión de Capital")
        equity = st.number_input("Capital Institucional (USD)", min_value=1000.0, value=100000.0, step=10000.0, key="pop_equity")

        st.markdown("---")
        st.markdown("##### 4. Configuración Gemini API")
        gemini_key_input = st.text_input(
            "Gemini API Key:",
            value=st.session_state.get("gemini_api_key", ""),
            type="password",
            placeholder="AIzaSy...",
            key="pop_gemini_key",
            help="Clave API de Google Gemini para habilitar el razonamiento institucional."
        )
        if gemini_key_input != st.session_state.get("gemini_api_key", ""):
            st.session_state["gemini_api_key"] = gemini_key_input
            st.success("Clave Gemini actualizada.")

    # 5. TAB NAVIGATION (Terminal Gráfica a Pantalla Completa & Comité CRO)
    tab_chart, tab_committee = st.tabs([
        "Terminal Gráfica Institucional",
        "Comité de Riesgo (CRO) & Propuesta"
    ])

    # ------------------------------------------------------------------------------
    # TAB 1: TRADINGVIEW CANDLESTICK CHART (PANTALLA COMPLETA)
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
        components.html(chart_html, height=860)

    # ------------------------------------------------------------------------------
    # TAB 2: COMITÉ AGÉNTICO & VEREDICTO CRO (CACHED ANALYSIS)
    # ------------------------------------------------------------------------------
    with tab_committee:
        col_head, col_btn = st.columns([0.99, 0.18], vertical_alignment="center")
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
        <div style="background: linear-gradient(180deg, rgba(15, 23, 42, 0.95) 0%, rgba(11, 17, 32, 0.9) 100%); border: 1px solid rgba(255, 255, 255, 0.08); border-radius: 12px; padding: 20px 26px; margin-bottom: 20px; box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06), 0 4px 16px rgba(0, 0, 0, 0.35);">
            <div style="display: flex; justify-content: space-between; align-items: center; gap: 16px; flex-wrap: wrap;">
                <div>
                    <h2 style="margin: 0; color: #f8fafc; font-size: 1.35rem; font-weight: 700; letter-spacing: -0.01em;">Comité Agéntico de Inversión & Control de Riesgo</h2>
                    <p style="margin: 6px 0 0 0; color: #94a3b8; font-size: 0.88rem;">
                        Evaluación para <strong>{ticker}</strong> ({market_choice} • {timeframe}) | Filosofía de Preservación de Capital
                    </p>
                </div>
                <div>
                    <span class="{'cro-badge-approved' if is_approved else 'cro-badge-rejected'}" style="font-size: 0.95rem; padding: 8px 20px; border-radius: 8px;">
                        <span class="{'status-dot-pulse-green' if is_approved else 'status-dot-pulse-red'}"></span>
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


if __name__ == "__main__":
    main()

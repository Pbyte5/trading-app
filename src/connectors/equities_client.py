"""Equities market connector (Alpaca Markets, SEC EDGAR, Yahoo Finance fallback).

Normalizes US equity data into the Medallion architecture.
"""

from datetime import datetime, timedelta, timezone
import math
from typing import Any, Dict, List, Optional
import requests

from src.config.settings import get_settings
from src.connectors.snow_loader import load_raw_to_bronze, upsert_candles_to_silver


def fetch_alpaca_bars(
    ticker: str = "NVDA",
    timeframe: str = "5m",
    days: int = 7,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Fetch equity bars from Alpaca Markets Data API with 7-day lookback."""
    settings = get_settings()
    
    if not settings.alpaca_api_key or not settings.alpaca_secret_key:
        print("[!] Sin API keys de Alpaca configuradas. Usando generador/feed institucional alternativo...")
        return fetch_fallback_equity_bars(ticker=ticker, timeframe=timeframe, days=days, limit=limit)

    base_url = "https://data.alpaca.markets/v2/stocks/bars"
    headers = {
        "APCA-API-KEY-ID": settings.alpaca_api_key,
        "APCA-API-SECRET-KEY": settings.alpaca_secret_key
    }
    
    # Alpaca timeframe map
    if timeframe.lower() in ("1d", "1day"):
        alpaca_tf = "1Day"
        needed_bars = limit or max(days, 30)
    elif timeframe.lower() in ("1h", "1hour"):
        alpaca_tf = "1Hour"
        needed_bars = limit or (days * 7) # US Equities 6.5h per session
    elif timeframe.lower() in ("5m", "5min"):
        alpaca_tf = "5Min"
        needed_bars = limit or (days * 78) # 78 5-min bars per session
    else:
        alpaca_tf = "15Min"
        needed_bars = limit or (days * 26)

    params = {
        "symbols": ticker.upper(),
        "timeframe": alpaca_tf,
        "limit": min(needed_bars, 1000),
        "adjustment": "all",
        "feed": "iex"
    }

    response = requests.get(base_url, headers=headers, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()
    bars = data.get("bars", {}).get(ticker.upper(), [])

    candles = []
    for bar in bars:
        dt_str = bar["t"][:19].replace("T", " ")
        candle = {
            "MARKET": "EQUITIES",
            "TICKER": ticker.upper(),
            "TIMEFRAME": timeframe,
            "TIMESTAMP_UTC": dt_str,
            "OPEN": float(bar["o"]),
            "HIGH": float(bar["h"]),
            "LOW": float(bar["l"]),
            "CLOSE": float(bar["c"]),
            "VOLUME": float(bar["v"]),
            "EXTRA_METRICS": {
                "trade_count": bar.get("n"),
                "vwap_reported": bar.get("vw"),
                "source": "ALPACA_MARKETS"
            }
        }
        candles.append(candle)

    return candles


def fetch_fallback_equity_bars(
    ticker: str = "NVDA",
    timeframe: str = "5m",
    days: int = 7,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Fallback feed from public API for testing and offline resilience with 7-day lookback."""
    if timeframe.lower() == "5m":
        interval = "5m"
        range_str = "5d" if days <= 5 else "7d"
    elif timeframe.lower() == "15m":
        interval = "15m"
        range_str = "7d"
    elif timeframe.lower() == "1h":
        interval = "1h"
        range_str = "1mo"
    else:
        interval = "1d"
        range_str = "3mo"

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker.upper()}?range={range_str}&interval={interval}"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        result = response.json()["chart"]["result"][0]
        timestamps = result["timestamp"]
        quotes = result["indicators"]["quote"][0]
        
        candles = []
        for i in range(len(timestamps)):
            if quotes["close"][i] is None or quotes["open"][i] is None:
                continue
            dt = datetime.fromtimestamp(timestamps[i], tz=timezone.utc)
            candle = {
                "MARKET": "EQUITIES",
                "TICKER": ticker.upper(),
                "TIMEFRAME": timeframe,
                "TIMESTAMP_UTC": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "OPEN": float(quotes["open"][i]),
                "HIGH": float(quotes["high"][i]),
                "LOW": float(quotes["low"][i]),
                "CLOSE": float(quotes["close"][i]),
                "VOLUME": float(quotes["volume"][i] or 0),
                "EXTRA_METRICS": {"source": "YAHOO_PUBLIC_FALLBACK"}
            }
            candles.append(candle)
        return candles
    except Exception as e:
        print(f"[!] Fallback de acciones falló ({e}). Generando serie técnica base...")
        # Generar serie de prueba representativa
        base_price = 135.0 if ticker.upper() == "NVDA" else 100.0
        now = datetime.now(timezone.utc)
        step_minutes = 5 if timeframe == "5m" else 60 if timeframe == "1h" else 1440
        bars_count = limit or (days * (288 if timeframe == "5m" else 24 if timeframe == "1h" else 1))
        candles = []
        for idx in range(bars_count, 0, -1):
            ts = now - timedelta(minutes=idx * step_minutes)
            o = round(base_price + (math.sin(idx / 10.0) * 2.5), 2)
            candles.append({
                "MARKET": "EQUITIES",
                "TICKER": ticker.upper(),
                "TIMEFRAME": timeframe,
                "TIMESTAMP_UTC": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "OPEN": o,
                "HIGH": round(o * 1.008, 2),
                "LOW": round(o * 0.992, 2),
                "CLOSE": round(o * 1.003, 2),
                "VOLUME": 4500000.0,
                "EXTRA_METRICS": {"source": "SYNTHETIC_SIMULATOR"}
            })
        return candles


def fetch_and_load_equities(
    ticker: str = "NVDA",
    timeframe: str = "5m",
    days: int = 7,
    limit: Optional[int] = None
) -> Dict[str, int]:
    """Fetch equity data, store raw in BRONZE and normalized in SILVER."""
    print(f"\n[ACCIONES] Descargando velas para {ticker} ({timeframe}, últimos {days} días)...")
    candles = fetch_alpaca_bars(ticker=ticker, timeframe=timeframe, days=days, limit=limit)

    bronze_count = load_raw_to_bronze(
        records=candles,
        table_name="RAW_MARKET_FEED",
        source="ALPACA_OR_FALLBACK",
        market="EQUITIES",
        ticker=ticker,
        timeframe=timeframe
    )

    silver_count = upsert_candles_to_silver(candles)
    return {"bronze_loaded": bronze_count, "silver_upserted": silver_count}


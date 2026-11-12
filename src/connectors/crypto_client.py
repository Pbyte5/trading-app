"""Crypto market connector (Binance Futures & Spot, DefiLlama, Coinglass).

Normalizes crypto OHLCV and on-chain metrics into the Medallion architecture.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import requests

from src.config.settings import get_settings
from src.connectors.snow_loader import (
    load_raw_to_bronze,
    upsert_candles_to_silver,
    get_latest_timestamp_in_silver,
    format_timestamp
)


def fetch_binance_klines(
    ticker: str = "BTCUSDT",
    timeframe: str = "1h",
    days: int = 7,
    limit: Optional[int] = None,
    is_futures: bool = False
) -> List[Dict[str, Any]]:
    """Fetch candles from Binance Spot or Futures REST API with 7-day lookback support."""
    settings = get_settings()
    base_url = "https://fapi.binance.com" if is_futures else "https://api.binance.com"
    endpoint = "/fapi/v1/klines" if is_futures else "/api/v3/klines"
    
    headers = {}
    if settings.binance_api_key:
        headers["X-MBX-APIKEY"] = settings.binance_api_key

    # Calculate required bars for the days lookback
    bars_per_day = {
        "1m": 1440,
        "5m": 288,
        "15m": 96,
        "1h": 24,
        "4h": 6,
        "1d": 1,
    }.get(timeframe.lower(), 24)

    total_bars_needed = limit if limit is not None else max(days * bars_per_day, 50)
    max_chunk = 1000

    all_raw_data = []
    end_time: Optional[int] = None

    while len(all_raw_data) < total_bars_needed:
        fetch_limit = min(max_chunk, total_bars_needed - len(all_raw_data))
        params: Dict[str, Any] = {
            "symbol": ticker.upper(),
            "interval": timeframe,
            "limit": fetch_limit
        }
        if end_time:
            params["endTime"] = end_time

        try:
            response = requests.get(f"{base_url}{endpoint}", params=params, headers=headers, timeout=15)
            response.raise_for_status()
            batch = response.json()
        except Exception as e:
            print(f"[!] Error descargando lote de velas Binance ({e})")
            break

        if not batch:
            break

        # Prepend older bars
        all_raw_data = batch + all_raw_data
        # Update end_time to oldest candle in batch - 1ms
        oldest_time = int(batch[0][0])
        end_time = oldest_time - 1

        if len(batch) < fetch_limit or end_time <= 0:
            break

    # Deduplicate by open time
    seen_times = set()
    deduped_raw = []
    for item in all_raw_data:
        open_time = int(item[0])
        if open_time not in seen_times:
            seen_times.add(open_time)
            deduped_raw.append(item)

    # Sort ascending by timestamp
    deduped_raw.sort(key=lambda x: int(x[0]))
    # Slice only the required amount
    if len(deduped_raw) > total_bars_needed:
        deduped_raw = deduped_raw[-total_bars_needed:]

    candles = []
    for item in deduped_raw:
        # Binance kline format:
        # 0: Open time, 1: Open, 2: High, 3: Low, 4: Close, 5: Volume,
        # 6: Close time, 7: Quote asset volume, 8: Number of trades,
        # 9: Taker buy base asset volume, 10: Taker buy quote asset volume
        open_time_ms = int(item[0])
        dt_utc = datetime.fromtimestamp(open_time_ms / 1000.0, tz=timezone.utc)

        candle = {
            "MARKET": "CRYPTO",
            "TICKER": ticker.upper(),
            "TIMEFRAME": timeframe,
            "TIMESTAMP_UTC": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "OPEN": float(item[1]),
            "HIGH": float(item[2]),
            "LOW": float(item[3]),
            "CLOSE": float(item[4]),
            "VOLUME": float(item[5]),
            "EXTRA_METRICS": {
                "trades_count": int(item[8]),
                "quote_volume": float(item[7]),
                "taker_buy_base_vol": float(item[9]),
                "taker_buy_quote_vol": float(item[10]),
                "source": "BINANCE_FUTURES" if is_futures else "BINANCE_SPOT"
            }
        }
        candles.append(candle)

    return candles


def fetch_defillama_tvl(protocol: str = "ethereum") -> Dict[str, Any]:
    """Fetch TVL and on-chain protocol metrics from DefiLlama (100% free, no API key)."""
    url = f"https://api.llama.fi/charts/{protocol}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        latest = data[-1] if data else {}
        return {
            "protocol": protocol,
            "timestamp": latest.get("date"),
            "totalLiquidityUSD": latest.get("totalLiquidityUSD")
        }
    except Exception as e:
        print(f"[!] Error fetching DefiLlama data: {e}")
        return {}


def fetch_and_load_crypto(
    ticker: str = "BTCUSDT",
    timeframe: str = "5m",
    days: int = 7,
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """Download crypto data incrementally, store raw in BRONZE and normalized in SILVER."""
    last_ts = get_latest_timestamp_in_silver("CRYPTO", ticker, timeframe)
    last_ts_str = format_timestamp(last_ts) if last_ts else None

    effective_days = 2 if last_ts else days
    print(f"\n[CRIPTO] Consultando velas para {ticker} ({timeframe}, delta desde {last_ts_str or 'inicio'})...")
    candles = fetch_binance_klines(ticker=ticker, timeframe=timeframe, days=effective_days, limit=limit)

    if last_ts_str:
        new_candles = [c for c in candles if format_timestamp(c["TIMESTAMP_UTC"]) > last_ts_str]
    else:
        new_candles = candles

    if not new_candles:
        print(f"[CRIPTO] {ticker} ({timeframe}) al día hasta {last_ts_str}. Sin nuevas velas. Cero cómputo Snowflake.")
        return {"bronze_loaded": 0, "silver_upserted": 0, "status": "UP_TO_DATE"}

    print(f"[CRIPTO] {len(new_candles)} nuevas velas encontradas para {ticker} ({timeframe}).")

    bronze_count = load_raw_to_bronze(
        records=new_candles,
        table_name="RAW_MARKET_FEED",
        source="BINANCE_SPOT",
        market="CRYPTO",
        ticker=ticker,
        timeframe=timeframe
    )

    silver_count = upsert_candles_to_silver(new_candles)
    return {"bronze_loaded": bronze_count, "silver_upserted": silver_count, "status": "SYNCED"}



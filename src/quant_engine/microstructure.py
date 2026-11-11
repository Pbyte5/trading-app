"""Microstructure engine: Session-anchored VWAP, Volume Profile, Liquidity Pools and CVD.

Reads from SILVER_CLEAN.MARKET_CANDLES and writes to GOLD_QUANT_RAG.QUANT_MARKET_STATES.
"""

import json
import math
from typing import Any, Dict, List, Optional, Tuple
import numpy as np

from src.config.snowflake_session import get_snowpark_session, get_raw_connection
from src.quant_engine.regime_detector import detect_regime, compute_atr


def compute_vwap_and_bands(candles: List[Dict[str, Any]]) -> Dict[str, float]:
    """Calculate session-anchored Volume Weighted Average Price (VWAP) and ±1σ, ±2σ, ±3σ bands."""
    if not candles:
        return {}

    highs = np.array([float(c["HIGH"]) for c in candles], dtype=np.float64)
    lows = np.array([float(c["LOW"]) for c in candles], dtype=np.float64)
    closes = np.array([float(c["CLOSE"]) for c in candles], dtype=np.float64)
    volumes = np.array([float(c["VOLUME"]) for c in candles], dtype=np.float64)

    typical_prices = (highs + lows + closes) / 3.0
    cum_vol = float(np.sum(volumes))

    if cum_vol == 0:
        last_close = float(closes[-1])
        return {
            "vwap": last_close,
            "vwap_upper_1": last_close,
            "vwap_lower_1": last_close,
            "vwap_upper_2": last_close,
            "vwap_lower_2": last_close,
            "vwap_upper_3": last_close,
            "vwap_lower_3": last_close,
            "vwap_std_dev": 0.0
        }

    vwap = float(np.sum(typical_prices * volumes) / cum_vol)
    sum_sq_diff = float(np.sum(volumes * ((typical_prices - vwap) ** 2)))
    variance = sum_sq_diff / cum_vol
    std_dev = math.sqrt(variance)

    return {
        "vwap": round(vwap, 4),
        "vwap_upper_1": round(vwap + 1.0 * std_dev, 4),
        "vwap_lower_1": round(vwap - 1.0 * std_dev, 4),
        "vwap_upper_2": round(vwap + 2.0 * std_dev, 4),
        "vwap_lower_2": round(vwap - 2.0 * std_dev, 4),
        "vwap_upper_3": round(vwap + 3.0 * std_dev, 4),
        "vwap_lower_3": round(vwap - 3.0 * std_dev, 4),
        "vwap_std_dev": round(std_dev, 4)
    }


def compute_volume_profile(candles: List[Dict[str, Any]], num_bins: int = 50) -> Dict[str, float]:
    """Calculate exact Volume Profile: POC (Point of Control), VAH (70%), and VAL (70%)."""
    if not candles:
        return {"poc": 0.0, "vah": 0.0, "val": 0.0}

    lows = np.array([float(c["LOW"]) for c in candles], dtype=np.float64)
    highs = np.array([float(c["HIGH"]) for c in candles], dtype=np.float64)
    volumes = np.array([float(c["VOLUME"]) for c in candles], dtype=np.float64)
    closes = np.array([float(c["CLOSE"]) for c in candles], dtype=np.float64)

    min_p = float(np.min(lows))
    max_p = float(np.max(highs))

    if max_p == min_p:
        p = float(closes[-1])
        return {"poc": p, "vah": p, "val": p}

    bin_size = (max_p - min_p) / num_bins
    bins = np.array([min_p + i * bin_size for i in range(num_bins + 1)], dtype=np.float64)

    c_ranges = np.maximum(highs - lows, 1e-8)
    b_low = bins[:-1]
    b_high = bins[1:]

    # Broadcast (N_candles, 1) with (1, num_bins) for vectorized overlap calculation
    overlap = np.maximum(0.0, np.minimum(highs[:, None], b_high[None, :]) - np.maximum(lows[:, None], b_low[None, :]))
    fractions = overlap / c_ranges[:, None]
    volume_by_bin = np.sum(volumes[:, None] * fractions, axis=0)

    total_vol = float(np.sum(volume_by_bin))
    if total_vol == 0:
        p = float(closes[-1])
        return {"poc": p, "vah": p, "val": p}

    # POC = price level of bin with max volume
    max_bin_idx = int(np.argmax(volume_by_bin))
    poc_price = (bins[max_bin_idx] + bins[max_bin_idx + 1]) / 2.0

    # Value Area = 70% of total volume expanding outward from POC
    target_vol = 0.70 * total_vol
    current_vol = float(volume_by_bin[max_bin_idx])

    low_idx = max_bin_idx
    high_idx = max_bin_idx

    while current_vol < target_vol and (low_idx > 0 or high_idx < num_bins - 1):
        vol_below = volume_by_bin[low_idx - 1] if low_idx > 0 else -1.0
        vol_above = volume_by_bin[high_idx + 1] if high_idx < num_bins - 1 else -1.0

        if vol_above >= vol_below and high_idx < num_bins - 1:
            high_idx += 1
            current_vol += volume_by_bin[high_idx]
        elif low_idx > 0:
            low_idx -= 1
            current_vol += volume_by_bin[low_idx]
        else:
            break

    val_price = bins[low_idx]
    vah_price = bins[high_idx + 1]

    return {
        "poc": round(float(poc_price), 4),
        "vah": round(float(vah_price), 4),
        "val": round(float(val_price), 4),
        "value_area_volume_ratio": round(float(current_vol / total_vol), 3)
    }


def find_liquidity_pools(candles: List[Dict[str, Any]], atr: float, max_window: Optional[int] = None) -> Dict[str, List[float]]:
    """Detect unmitigated equal highs and equal lows with 0.1 * ATR tolerance using vectorized NumPy candidate filtering."""
    if len(candles) < 5 or atr <= 0:
        return {"equal_highs": [], "equal_lows": []}

    candles_window = candles[-max_window:] if (max_window and len(candles) > max_window) else candles
    highs = np.array([float(c["HIGH"]) for c in candles_window], dtype=np.float64)
    lows = np.array([float(c["LOW"]) for c in candles_window], dtype=np.float64)
    n = len(candles_window)
    tolerance = 0.10 * atr

    # Vectorized upper-triangular pair index with j >= i + 2
    i_idx, j_idx = np.triu_indices(n, k=2)

    # Check Equal Highs candidate mask
    h_diff = np.abs(highs[i_idx] - highs[j_idx])
    h_mask = h_diff <= tolerance

    # Check Equal Lows candidate mask
    l_diff = np.abs(lows[i_idx] - lows[j_idx])
    l_mask = l_diff <= tolerance

    equal_highs = []
    for i, j in zip(i_idx[h_mask], j_idx[h_mask]):
        max_inter = np.max(highs[i:j])
        if max_inter <= max(highs[i], highs[j]) + tolerance:
            equal_highs.append(round(float((highs[i] + highs[j]) / 2.0), 4))

    equal_lows = []
    for i, j in zip(i_idx[l_mask], j_idx[l_mask]):
        min_inter = np.min(lows[i:j])
        if min_inter >= min(lows[i], lows[j]) - tolerance:
            equal_lows.append(round(float((lows[i] + lows[j]) / 2.0), 4))

    return {
        "equal_highs": list(set(equal_highs))[-3:],
        "equal_lows": list(set(equal_lows))[-3:]
    }



def compute_cvd_approximation(candles: List[Dict[str, Any]]) -> float:
    """Approximate Cumulative Volume Delta (CVD) based on candle bar metrics or close location."""
    cvd = 0.0
    for c in candles:
        extra = c.get("EXTRA_METRICS") or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except Exception:
                extra = {}

        # If taker buy/sell volume exists (e.g. Binance)
        taker_buy = extra.get("taker_buy_base_vol")
        total_vol = float(c["VOLUME"])

        if taker_buy is not None:
            taker_sell = total_vol - float(taker_buy)
            cvd += (float(taker_buy) - taker_sell)
        else:
            # Bar-range delta approximation: ((Close - Low) - (High - Close)) / Range * Volume
            h = float(c["HIGH"])
            l = float(c["LOW"])
            cl = float(c["CLOSE"])
            rng = max(h - l, 1e-8)
            buy_pressure = (cl - l) / rng
            sell_pressure = (h - cl) / rng
            cvd += (buy_pressure - sell_pressure) * total_vol

    return round(cvd, 2)


def process_quant_state_for_ticker(
    market: str,
    ticker: str,
    timeframe: str = "1h",
    lookback: int = 300
) -> Dict[str, Any]:
    """Fetch candles from SILVER, calculate all quantitative metrics, and save to GOLD."""
    print(f"\n[*] Calculando estado cuantitativo para {market}:{ticker} ({timeframe}) [lookback={lookback}]...")

    conn = get_raw_connection(query_tag="QUANT_MICROSTRUCTURE")

    query = """
    SELECT 
        MARKET, TICKER, TIMEFRAME, TIMESTAMP_UTC, OPEN, HIGH, LOW, CLOSE, VOLUME, EXTRA_METRICS
    FROM QUANT_INSTITUTIONAL_DB.SILVER_CLEAN.MARKET_CANDLES
    WHERE MARKET = %s 
      AND TICKER = %s 
      AND TIMEFRAME = %s
    ORDER BY TIMESTAMP_UTC DESC
    LIMIT %s
    """

    candles = []
    try:
        with conn.cursor() as cur:
            cur.execute(query, (market.upper(), ticker.upper(), timeframe.lower(), lookback))
            for r in cur.fetchall():
                candles.append({
                    "MARKET": r[0],
                    "TICKER": r[1],
                    "TIMEFRAME": r[2],
                    "TIMESTAMP_UTC": str(r[3]),
                    "OPEN": float(r[4]),
                    "HIGH": float(r[5]),
                    "LOW": float(r[6]),
                    "CLOSE": float(r[7]),
                    "VOLUME": float(r[8]),
                    "EXTRA_METRICS": r[9]
                })
    finally:
        conn.close()

    if not candles:
        print(f"[!] No hay velas registradas en SILVER_CLEAN para {market}:{ticker}:{timeframe}")
        return {}

    # Reverse to chronological order (oldest to newest)
    candles.reverse()

    # Calculate indicators
    vwap_res = compute_vwap_and_bands(candles)
    vp_res = compute_volume_profile(candles)
    regime_res = detect_regime(candles)
    atr_val = regime_res["atr"]
    pools = find_liquidity_pools(candles, atr=atr_val, max_window=lookback)
    cvd_val = compute_cvd_approximation(candles)

    latest = candles[-1]
    last_price = latest["CLOSE"]

    state_json = {
        "market": market.upper(),
        "ticker": ticker.upper(),
        "timeframe": timeframe.lower(),
        "latest_timestamp_utc": latest["TIMESTAMP_UTC"],
        "last_price": last_price,
        "vwap_metrics": vwap_res,
        "volume_profile": vp_res,
        "regime": regime_res,
        "liquidity_pools": pools,
        "cvd": cvd_val
    }

    # Upsert into GOLD_QUANT_RAG.QUANT_MARKET_STATES
    save_gold_market_state(state_json)
    return state_json


def save_gold_market_state(state: Dict[str, Any]) -> None:
    """Save calculated state into QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.QUANT_MARKET_STATES with parameterized SQL."""
    conn = get_raw_connection(query_tag="GOLD_MERGE_STATE")

    try:
        m = state["market"]
        tk = state["ticker"]
        tf = state["timeframe"]
        ts = state["latest_timestamp_utc"]

        vwap = float(state["vwap_metrics"].get("vwap", 0.0))
        u1 = float(state["vwap_metrics"].get("vwap_upper_1", 0.0))
        l1 = float(state["vwap_metrics"].get("vwap_lower_1", 0.0))
        u2 = float(state["vwap_metrics"].get("vwap_upper_2", 0.0))
        l2 = float(state["vwap_metrics"].get("vwap_lower_2", 0.0))
        u3 = float(state["vwap_metrics"].get("vwap_upper_3", 0.0))
        l3 = float(state["vwap_metrics"].get("vwap_lower_3", 0.0))

        poc = float(state["volume_profile"].get("poc", 0.0))
        vah = float(state["volume_profile"].get("vah", 0.0))
        val = float(state["volume_profile"].get("val", 0.0))
        cvd = float(state.get("cvd", 0.0))

        atr = float(state["regime"].get("atr", 0.0))
        hurst = float(state["regime"].get("hurst", 0.50))
        regime = str(state["regime"].get("regime", "RANDOM_WALK"))

        payload = json.dumps(state)

        merge_sql = """
        MERGE INTO QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.QUANT_MARKET_STATES AS target
        USING (
            SELECT 
                %s AS MARKET,
                %s AS TICKER,
                %s AS TIMEFRAME,
                %s::TIMESTAMP_NTZ AS TIMESTAMP_UTC,
                %s AS VWAP,
                %s AS VWAP_UPPER_1,
                %s AS VWAP_LOWER_1,
                %s AS VWAP_UPPER_2,
                %s AS VWAP_LOWER_2,
                %s AS VWAP_UPPER_3,
                %s AS VWAP_LOWER_3,
                %s AS POC,
                %s AS VAH,
                %s AS VAL,
                %s AS CVD,
                %s AS ATR,
                %s AS HURST_EXPONENT,
                %s AS REGIME,
                PARSE_JSON(%s) AS STATE_JSON
        ) AS src
        ON target.MARKET = src.MARKET 
           AND target.TICKER = src.TICKER 
           AND target.TIMEFRAME = src.TIMEFRAME 
           AND target.TIMESTAMP_UTC = src.TIMESTAMP_UTC
        WHEN MATCHED THEN
            UPDATE SET 
                target.VWAP = src.VWAP,
                target.POC = src.POC,
                target.VAH = src.VAH,
                target.VAL = src.VAL,
                target.CVD = src.CVD,
                target.ATR = src.ATR,
                target.HURST_EXPONENT = src.HURST_EXPONENT,
                target.REGIME = src.REGIME,
                target.STATE_JSON = src.STATE_JSON,
                target.CALCULATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (
                MARKET, TICKER, TIMEFRAME, TIMESTAMP_UTC,
                VWAP, VWAP_UPPER_1, VWAP_LOWER_1, VWAP_UPPER_2, VWAP_LOWER_2, VWAP_UPPER_3, VWAP_LOWER_3,
                POC, VAH, VAL, CVD, ATR, HURST_EXPONENT, REGIME, STATE_JSON
            )
            VALUES (
                src.MARKET, src.TICKER, src.TIMEFRAME, src.TIMESTAMP_UTC,
                src.VWAP, src.VWAP_UPPER_1, src.VWAP_LOWER_1, src.VWAP_UPPER_2, src.VWAP_LOWER_2, src.VWAP_UPPER_3, src.VWAP_LOWER_3,
                src.POC, src.VAH, src.VAL, src.CVD, src.ATR, src.HURST_EXPONENT, src.REGIME, src.STATE_JSON
            );
        """
        params = (
            m, tk, tf, ts,
            vwap, u1, l1, u2, l2, u3, l3,
            poc, vah, val, cvd,
            atr, hurst, regime, payload
        )

        with conn.cursor() as cur:
            cur.execute(merge_sql, params)

        print(f"[OK] Estado guardado en GOLD_QUANT_RAG.QUANT_MARKET_STATES para {m}:{tk}:{tf}")
    finally:
        conn.close()


def compute_all_gold_states(timeframes: Optional[List[str]] = None) -> None:
    """Compute gold state across all active tickers in Silver."""
    print("=" * 70)
    print("MOTOR CUÁNTICO SNOWPARK: CÁLCULO DE LA CAPA GOLD_QUANT_RAG")
    print("=" * 70)

    target_tfs = timeframes or ["5m", "1h"]
    assets = []
    for tf in target_tfs:
        assets.append(("CRYPTO", "BTCUSDT", tf))
        assets.append(("EQUITIES", "NVDA", tf))
        assets.append(("FOREX", "EURUSD", tf))
    assets.append(("EQUITIES", "NVDA", "1d"))

    for m, tk, tf in assets:
        try:
            process_quant_state_for_ticker(m, tk, tf)
        except Exception as e:
            print(f"[!] Error procesando estado para {tk}: {e}")

    print("=" * 70)
    print("[FINALIZADO] Capa GOLD actualizada con éxito.")
    print("=" * 70)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Motor Cuantitativo Microestructura (Snowpark)")
    parser.add_argument("--compute-gold", action="store_true", help="Calcular indicadores y actualizar esquema GOLD en Snowflake")
    args = parser.parse_args()

    if args.compute_gold:
        compute_all_gold_states()
    else:
        parser.print_help()

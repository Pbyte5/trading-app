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

    cum_vol = 0.0
    cum_pv = 0.0

    typical_prices = []
    volumes = []

    for c in candles:
        h = float(c["HIGH"])
        l = float(c["LOW"])
        cl = float(c["CLOSE"])
        v = float(c["VOLUME"])
        tp = (h + l + cl) / 3.0

        typical_prices.append(tp)
        volumes.append(v)

        cum_vol += v
        cum_pv += tp * v

    if cum_vol == 0:
        last_close = float(candles[-1]["CLOSE"])
        return {
            "vwap": last_close,
            "vwap_upper_1": last_close,
            "vwap_lower_1": last_close,
            "vwap_upper_2": last_close,
            "vwap_lower_2": last_close,
            "vwap_upper_3": last_close,
            "vwap_lower_3": last_close,
        }

    vwap = cum_pv / cum_vol

    # Weighted standard deviation: sqrt( sum( v * (tp - vwap)^2 ) / sum(v) )
    sum_sq_diff = sum(v * ((tp - vwap) ** 2) for tp, v in zip(typical_prices, volumes))
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

    min_p = min(float(c["LOW"]) for c in candles)
    max_p = max(float(c["HIGH"]) for c in candles)

    if max_p == min_p:
        p = float(candles[-1]["CLOSE"])
        return {"poc": p, "vah": p, "val": p}

    bin_size = (max_p - min_p) / num_bins
    bins = [min_p + i * bin_size for i in range(num_bins + 1)]
    volume_by_bin = [0.0] * num_bins

    # Allocate candle volume proportionally across overlapping bins
    for c in candles:
        c_low = float(c["LOW"])
        c_high = float(c["HIGH"])
        vol = float(c["VOLUME"])
        c_range = max(c_high - c_low, 1e-8)

        for b_idx in range(num_bins):
            b_low = bins[b_idx]
            b_high = bins[b_idx + 1]

            overlap = max(0.0, min(c_high, b_high) - max(c_low, b_low))
            if overlap > 0:
                fraction = overlap / c_range
                volume_by_bin[b_idx] += vol * fraction

    total_vol = sum(volume_by_bin)
    if total_vol == 0:
        p = float(candles[-1]["CLOSE"])
        return {"poc": p, "vah": p, "val": p}

    # POC = price level of bin with max volume
    max_bin_idx = int(np.argmax(volume_by_bin))
    poc_price = (bins[max_bin_idx] + bins[max_bin_idx + 1]) / 2.0

    # Value Area = 70% of total volume expanding outward from POC
    target_vol = 0.70 * total_vol
    current_vol = volume_by_bin[max_bin_idx]

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
        "poc": round(poc_price, 4),
        "vah": round(vah_price, 4),
        "val": round(val_price, 4),
        "value_area_volume_ratio": round(current_vol / total_vol, 3)
    }


def find_liquidity_pools(candles: List[Dict[str, Any]], atr: float) -> Dict[str, List[float]]:
    """Detect unmitigated equal highs and equal lows with 0.1 * ATR tolerance."""
    if len(candles) < 5 or atr <= 0:
        return {"equal_highs": [], "equal_lows": []}

    tolerance = 0.10 * atr
    highs = [float(c["HIGH"]) for c in candles]
    lows = [float(c["LOW"]) for c in candles]

    equal_highs = []
    equal_lows = []

    for i in range(len(candles) - 1):
        for j in range(i + 2, len(candles)):
            # Check Equal Highs
            if abs(highs[i] - highs[j]) <= tolerance:
                # Verify intermediate prices did not breach this level
                max_inter = max(highs[i:j])
                if max_inter <= max(highs[i], highs[j]) + tolerance:
                    equal_highs.append(round((highs[i] + highs[j]) / 2.0, 4))

            # Check Equal Lows
            if abs(lows[i] - lows[j]) <= tolerance:
                min_inter = min(lows[i:j])
                if min_inter >= min(lows[i], lows[j]) - tolerance:
                    equal_lows.append(round((lows[i] + lows[j]) / 2.0, 4))

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
    timeframe: str = "1h"
) -> Dict[str, Any]:
    """Fetch candles from SILVER, calculate all quantitative metrics, and save to GOLD."""
    print(f"\n[*] Calculando estado cuantitativo para {market}:{ticker} ({timeframe})...")

    try:
        session = get_snowpark_session()
        use_snowpark = True
    except Exception:
        session = get_raw_connection()
        use_snowpark = False

    query = f"""
    SELECT 
        MARKET, TICKER, TIMEFRAME, TIMESTAMP_UTC, OPEN, HIGH, LOW, CLOSE, VOLUME, EXTRA_METRICS
    FROM QUANT_INSTITUTIONAL_DB.SILVER_CLEAN.MARKET_CANDLES
    WHERE MARKET = '{market.upper()}' 
      AND TICKER = '{ticker.upper()}' 
      AND TIMEFRAME = '{timeframe.lower()}'
    ORDER BY TIMESTAMP_UTC DESC
    LIMIT 2500
    """

    candles = []
    try:
        if use_snowpark:
            rows = session.sql(query).collect()
            for r in rows:
                candles.append({
                    "MARKET": r["MARKET"],
                    "TICKER": r["TICKER"],
                    "TIMEFRAME": r["TIMEFRAME"],
                    "TIMESTAMP_UTC": str(r["TIMESTAMP_UTC"]),
                    "OPEN": float(r["OPEN"]),
                    "HIGH": float(r["HIGH"]),
                    "LOW": float(r["LOW"]),
                    "CLOSE": float(r["CLOSE"]),
                    "VOLUME": float(r["VOLUME"]),
                    "EXTRA_METRICS": r["EXTRA_METRICS"]
                })
        else:
            with session.cursor() as cur:
                cur.execute(query)
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
        session.close()

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
    pools = find_liquidity_pools(candles, atr=atr_val)
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
    """Save calculated state into QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.QUANT_MARKET_STATES."""
    try:
        session = get_snowpark_session()
        use_snowpark = True
    except Exception:
        session = get_raw_connection()
        use_snowpark = False

    try:
        m = state["market"]
        tk = state["ticker"]
        tf = state["timeframe"]
        ts = state["latest_timestamp_utc"]

        vwap = state["vwap_metrics"].get("vwap", 0.0)
        u1 = state["vwap_metrics"].get("vwap_upper_1", 0.0)
        l1 = state["vwap_metrics"].get("vwap_lower_1", 0.0)
        u2 = state["vwap_metrics"].get("vwap_upper_2", 0.0)
        l2 = state["vwap_metrics"].get("vwap_lower_2", 0.0)
        u3 = state["vwap_metrics"].get("vwap_upper_3", 0.0)
        l3 = state["vwap_metrics"].get("vwap_lower_3", 0.0)

        poc = state["volume_profile"].get("poc", 0.0)
        vah = state["volume_profile"].get("vah", 0.0)
        val = state["volume_profile"].get("val", 0.0)
        cvd = state.get("cvd", 0.0)

        atr = state["regime"].get("atr", 0.0)
        hurst = state["regime"].get("hurst", 0.50)
        regime = state["regime"].get("regime", "RANDOM_WALK")

        payload = json.dumps(state).replace("'", "''")

        merge_sql = f"""
        MERGE INTO QUANT_INSTITUTIONAL_DB.GOLD_QUANT_RAG.QUANT_MARKET_STATES AS target
        USING (
            SELECT 
                '{m}' AS MARKET,
                '{tk}' AS TICKER,
                '{tf}' AS TIMEFRAME,
                '{ts}'::TIMESTAMP_NTZ AS TIMESTAMP_UTC,
                {vwap} AS VWAP,
                {u1} AS VWAP_UPPER_1,
                {l1} AS VWAP_LOWER_1,
                {u2} AS VWAP_UPPER_2,
                {l2} AS VWAP_LOWER_2,
                {u3} AS VWAP_UPPER_3,
                {l3} AS VWAP_LOWER_3,
                {poc} AS POC,
                {vah} AS VAH,
                {val} AS VAL,
                {cvd} AS CVD,
                {atr} AS ATR,
                {hurst} AS HURST_EXPONENT,
                '{regime}' AS REGIME,
                PARSE_JSON('{payload}') AS STATE_JSON
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

        if use_snowpark:
            session.sql(merge_sql).collect()
        else:
            with session.cursor() as cur:
                cur.execute(merge_sql)

        print(f"[OK] Estado guardado en GOLD_QUANT_RAG.QUANT_MARKET_STATES para {m}:{tk}:{tf}")
    finally:
        session.close()


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

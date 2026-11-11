"""Market regime detection: Hurst Exponent (H), ATR(14) and volatility regime."""

import math
from typing import Dict, List, Tuple
import numpy as np


def compute_atr(candles: List[Dict[str, float]], period: int = 14) -> float:
    """Calculate Average True Range (ATR) over N periods."""
    if len(candles) < 2:
        return 0.0

    true_ranges = []
    for i in range(1, len(candles)):
        high = candles[i]["HIGH"]
        low = candles[i]["LOW"]
        prev_close = candles[i - 1]["CLOSE"]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        true_ranges.append(tr)

    if not true_ranges:
        return 0.0

    subset = true_ranges[-period:]
    return float(np.mean(subset))


def compute_hurst_exponent(prices: List[float], max_lags: int = 20) -> float:
    """Calculate Hurst Exponent (H) via Rescaled Range (R/S) analysis.

    Interpretation:
    - H < 0.45: Mean-reverting series (anti-persistent, range-bound).
    - 0.45 <= H <= 0.55: Geometric Brownian Motion / Random walk (no edge).
    - H > 0.55: Persistent / Trending / Momentum series.
    """
    if len(prices) < 30:
        return 0.50  # Default neutral random walk if sample size is insufficient

    ts = np.array(prices, dtype=np.float64)
    # Log returns
    returns = np.diff(np.log(ts))
    n = len(returns)

    # Sub-series sizes (powers of 2 or logarithmic steps)
    lags = [l for l in [8, 16, 32, 64, 128] if l <= n // 2]
    if len(lags) < 2:
        lags = [8, max(9, n // 2)]

    rs_values = []
    for lag in lags:
        sub_samples = n // lag
        rs_sub = []
        for s in range(sub_samples):
            sub = returns[s * lag:(s + 1) * lag]
            mean_sub = np.mean(sub)
            dev = sub - mean_sub
            cum_dev = np.cumsum(dev)
            r = np.max(cum_dev) - np.min(cum_dev)
            s_std = np.std(sub, ddof=1)
            if s_std > 1e-9:
                rs_sub.append(r / s_std)

        if rs_sub:
            rs_values.append(np.mean(rs_sub))
        else:
            rs_values.append(1.0)

    # Fit line: log(R/S) = H * log(lag) + C
    log_lags = np.log(lags)
    log_rs = np.log(rs_values)

    try:
        poly = np.polyfit(log_lags, log_rs, 1)
        hurst = float(poly[0])
        # Clip to realistic range [0.05, 0.95]
        return float(np.clip(hurst, 0.05, 0.95))
    except Exception:
        return 0.50


def detect_regime(candles: List[Dict[str, float]]) -> Dict[str, Any]:
    """Detect volatility and directional regime from candle series."""
    if not candles:
        return {
            "atr": 0.0,
            "hurst": 0.50,
            "regime": "RANDOM_WALK",
            "regime_description": "Datos insuficientes para determinar régimen."
        }

    close_prices = [float(c["CLOSE"]) for c in candles]
    atr_val = compute_atr(candles, period=14)
    hurst_val = compute_hurst_exponent(close_prices)

    if hurst_val < 0.45:
        regime = "MEAN_REVERSION"
        desc = f"Régimen de Reversión a la Media (H={hurst_val:.3f}). Favorece operaciones de rebote en bandas de VWAP y canales."
    elif hurst_val > 0.55:
        regime = "TRENDING_MOMENTUM"
        desc = f"Régimen Tendencial / Momentum (H={hurst_val:.3f}). Favorece compras en rupturas de POC/VAH y pullbacks a favor de tendencia."
    else:
        regime = "RANDOM_WALK"
        desc = f"Régimen de Caminata Aleatoria (H={hurst_val:.3f}). Sin edge direccional claro, se aconseja prudencia o no operar."

    return {
        "atr": round(atr_val, 6),
        "hurst": round(hurst_val, 4),
        "regime": regime,
        "regime_description": desc
    }


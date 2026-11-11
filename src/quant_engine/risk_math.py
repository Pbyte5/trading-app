"""Quantitative risk mathematics: Fractional Kelly Criterion, Position Sizing, and VaR."""

import math
from typing import Dict, Any


def calculate_fractional_kelly(
    win_rate: float,
    reward_risk_ratio: float,
    fraction: float = 0.25
) -> float:
    """Calculate Fractional Kelly Criterion allocation percentage.

    Formula:
        f* = fraction * ((p * b - q) / b)
    where:
        p = win_rate (e.g. 0.55)
        q = 1 - p (e.g. 0.45)
        b = reward_risk_ratio (e.g. 2.0)
        fraction = 0.25 (Quarter-Kelly for conservative institutional sizing)
    """
    if reward_risk_ratio <= 0.0 or win_rate <= 0.0:
        return 0.0

    p = max(0.0, min(1.0, win_rate))
    q = 1.0 - p
    b = reward_risk_ratio

    full_kelly = (p * b - q) / b

    # If edge is negative, allocate 0%
    if full_kelly <= 0.0:
        return 0.0

    quarter_kelly = fraction * full_kelly
    # Institutional safety cap: max 10% of equity per trade
    return float(min(quarter_kelly, 0.10))


def calculate_position_size(
    account_equity: float,
    entry_price: float,
    stop_loss_price: float,
    win_rate: float = 0.55,
    take_profit_price: float = None,
    max_risk_pct: float = 0.02
) -> Dict[str, Any]:
    """Calculate institutional position size based on ATR/Price Stop Loss and Kelly."""
    if entry_price <= 0.0 or stop_loss_price <= 0.0:
        raise ValueError("Los precios de entrada y Stop Loss deben ser positivos.")

    risk_per_unit = abs(entry_price - stop_loss_price)
    if risk_per_unit < 1e-8:
        raise ValueError("La distancia de Stop Loss es cero.")

    if take_profit_price:
        reward_per_unit = abs(take_profit_price - entry_price)
        rr_ratio = reward_per_unit / risk_per_unit
    else:
        rr_ratio = 2.0  # Mínimo institucional por defecto

    kelly_pct = calculate_fractional_kelly(win_rate=win_rate, reward_risk_ratio=rr_ratio)

    # Determine maximum dollar risk allowed
    risk_pct = min(kelly_pct, max_risk_pct) if kelly_pct > 0 else 0.0
    capital_at_risk = account_equity * risk_pct

    # Units = capital_at_risk / risk_per_unit
    units = (capital_at_risk / risk_per_unit) if risk_per_unit > 0 else 0.0
    total_position_value = units * entry_price

    return {
        "account_equity": account_equity,
        "entry_price": entry_price,
        "stop_loss_price": stop_loss_price,
        "reward_risk_ratio": round(rr_ratio, 2),
        "is_rr_valid": rr_ratio >= 2.0,
        "quarter_kelly_fraction": round(kelly_pct, 4),
        "risk_percentage_applied": round(risk_pct, 4),
        "capital_at_risk_usd": round(capital_at_risk, 2),
        "position_units": round(units, 4),
        "total_position_value_usd": round(total_position_value, 2)
    }


def calculate_parametric_var(
    position_value: float,
    daily_volatility: float,
    confidence_level: float = 0.95,
    time_horizon_days: int = 1
) -> Dict[str, float]:
    """Parametric Value at Risk (VaR) using normal distribution approximation."""
    z_scores = {0.90: 1.282, 0.95: 1.645, 0.99: 2.326}
    z = z_scores.get(confidence_level, 1.645)

    var_usd = position_value * daily_volatility * z * math.sqrt(time_horizon_days)
    return {
        "confidence_level": confidence_level,
        "time_horizon_days": time_horizon_days,
        "var_usd": round(var_usd, 2),
        "var_percentage": round((var_usd / position_value) * 100, 2) if position_value > 0 else 0.0
    }


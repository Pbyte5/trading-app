"""Pydantic schemas for structured institutional trading plans and risk validation."""

from typing import List, Literal, Optional
from pydantic import BaseModel, Field


class TradeStrategy(BaseModel):
    """Detailed parameters and risk evaluation for an execution strategy."""
    action: Literal["BUY", "SELL", "NO_TRADE"] = Field(
        description="Dirección operativa institucional."
    )
    horizon: Literal["INTRADAY", "SWING_HOLDER"] = Field(
        description="Horizonte temporal de la estrategia."
    )
    entry_price: float = Field(
        description="Precio exacto de entrada."
    )
    stop_loss_price: float = Field(
        description="Precio de corte de pérdida basado en múltiplos de ATR o bandas de VWAP."
    )
    take_profit_levels: List[float] = Field(
        default_factory=list,
        description="Niveles de toma de beneficio anclados a POC, VAH, VAL o bandas de VWAP."
    )
    reward_to_risk_ratio: float = Field(
        description="Ratio Beneficio / Riesgo proyectado (mínimo institucional 2.0)."
    )
    fractional_kelly_fraction: float = Field(
        default=0.0,
        description="Fracción de Kelly Fraccionado (1/4 Kelly) sugerida."
    )
    suggested_risk_capital_usd: float = Field(
        default=0.0,
        description="Capital máximo en dólares a arriesgar."
    )
    technical_rationale: str = Field(
        description="Justificación técnica cuantitativa y microestructural."
    )
    matched_playbook_rule: str = Field(
        description="Regla específica del Playbook Institucional referenciada."
    )
    risk_officer_verdict: Literal["APPROVED", "REJECTED", "PENDING"] = Field(
        default="PENDING",
        description="Veredicto del filtro estricto de riesgo."
    )
    risk_rejection_reason: Optional[str] = Field(
        default=None,
        description="Motivo del rechazo en caso de no superar los filtros de riesgo institucional."
    )


class InstitutionalTradePlan(BaseModel):
    """Unified Institutional Trading Plan containing Intraday and Swing/Holder allocations."""
    ticker: str
    market: Literal["CRYPTO", "EQUITIES", "FOREX", "MACRO"]
    timeframe: str
    timestamp_utc: str
    market_regime: Literal["MEAN_REVERSION", "RANDOM_WALK", "TRENDING_MOMENTUM"]
    hurst_exponent: float
    atr: float
    vwap: float
    poc: float
    vah: float
    val: float
    cvd: float
    
    # Dual strategies as required by the Master Plan
    intraday_execution: TradeStrategy
    holder_swing_allocation: TradeStrategy

    historical_analogy_summary: Optional[str] = None
    overall_portfolio_risk_status: Literal["HEALTHY", "RESTRICTED", "REJECTED"] = "HEALTHY"


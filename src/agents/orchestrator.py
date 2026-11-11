"""Multi-agent orchestrator: Quantitative Analyst -> Macro Analyst -> Strategist -> Risk Officer."""

import argparse
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from src.config.settings import get_settings
from src.mcp_server.snowflake_quant_tools import (
    get_gold_market_state,
    mcp_query_playbooks,
    mcp_find_historical_analogies
)
from src.quant_engine.risk_math import calculate_fractional_kelly, calculate_position_size
from src.agents.schemas import InstitutionalTradePlan, TradeStrategy


def evaluate_risk_officer_filter(strategy: TradeStrategy, atr: float, regime: str, hurst: float) -> TradeStrategy:
    """Rigorous programmatic Risk Officer filter in Python enforcing institutional safety limits."""
    # Rule 1: No trade actions are already safe
    if strategy.action == "NO_TRADE":
        strategy.risk_officer_verdict = "APPROVED"
        return strategy

    # Rule 2: Strict Reward-to-Risk Ratio >= 2.0
    if strategy.reward_to_risk_ratio < 2.0:
        strategy.risk_officer_verdict = "REJECTED"
        strategy.risk_rejection_reason = (
            f"VETO DE RIESGO: El Ratio Beneficio/Riesgo proyectado ({strategy.reward_to_risk_ratio:.2f}) "
            f"es inferior al mínimo institucional obligatorio de 2.00."
        )
        strategy.action = "NO_TRADE"
        return strategy

    # Rule 3: Stop loss distance sanity check (must be at least 0.4 * ATR)
    if atr > 0:
        sl_distance = abs(strategy.entry_price - strategy.stop_loss_price)
        if sl_distance < (0.40 * atr):
            strategy.risk_officer_verdict = "REJECTED"
            strategy.risk_rejection_reason = (
                f"VETO DE RIESGO: La distancia de Stop Loss ({sl_distance:.4f}) es demasiado estrecha "
                f"(< 0.4 * ATR = {0.40 * atr:.4f}). Alto riesgo de stop-out por ruido de microestructura."
            )
            strategy.action = "NO_TRADE"
            return strategy

    # Rule 4: Regime mismatch check
    if regime == "RANDOM_WALK" and strategy.horizon == "INTRADAY":
        # Random walk penalizes intraday breakout strategies
        strategy.risk_officer_verdict = "REJECTED"
        strategy.risk_rejection_reason = (
            f"VETO DE RIESGO: Mercado en Caminata Aleatoria (Hurst={hurst:.3f}). "
            f"Se prohíben operaciones direccionales intradía sin ventaja estadística comprobada."
        )
        strategy.action = "NO_TRADE"
        return strategy

    # Passed all risk checks
    strategy.risk_officer_verdict = "APPROVED"
    strategy.risk_rejection_reason = None
    return strategy


def generate_trade_plan(
    ticker: str = "BTCUSDT",
    market: str = "CRYPTO",
    timeframe: str = "1h",
    account_equity: float = 100000.0
) -> InstitutionalTradePlan:
    """Orchestrate the committee pipeline to build a validated trade plan."""
    print("=" * 75)
    print(f"COMITÉ AGÉNTICO INSTITUCIONAL: ANÁLISIS DE {market}:{ticker} ({timeframe})")
    print("=" * 75)

    # 1. Recuperar estado de la capa GOLD de Snowflake
    print("[1/4] Consultando Feature Store (GOLD_QUANT_RAG.QUANT_MARKET_STATES)...")
    gold_state = get_gold_market_state(ticker=ticker, timeframe=timeframe, market=market)

    # If state not found, use default/synthetic state for resilience
    last_price = gold_state.get("last_price", 65000.0 if "BTC" in ticker else 135.0 if "NVDA" in ticker else 1.0850)
    vwap_metrics = gold_state.get("vwap_metrics", {})
    vwap = vwap_metrics.get("vwap", last_price)
    u2 = vwap_metrics.get("vwap_upper_2", last_price * 1.02)
    l2 = vwap_metrics.get("vwap_lower_2", last_price * 0.98)

    vp = gold_state.get("volume_profile", {})
    poc = vp.get("poc", last_price)
    vah = vp.get("vah", last_price * 1.015)
    val = vp.get("val", last_price * 0.985)

    reg_info = gold_state.get("regime", {})
    regime = reg_info.get("regime", "MEAN_REVERSION")
    hurst = reg_info.get("hurst", 0.42)
    atr = reg_info.get("atr", last_price * 0.015)
    cvd = gold_state.get("cvd", 1500.0)

    # 2. Consultar Playbooks en Snowflake Cortex
    print("[2/4] Consultando Playbooks en Snowflake Cortex Search...")
    playbook_query = f"Estrategia para régimen {regime} y microestructura cerca de VWAP y POC en {market}"
    playbook_matches = mcp_query_playbooks(query=playbook_query, market_type=market, strategy_horizon="ALL")
    primary_rule = playbook_matches[0]["section_header"] if playbook_matches else "Regla de Microestructura y Bandas de VWAP"

    # 3. Consultar Memoria Episódica en Snowflake Cortex
    print("[3/4] Buscando Analogías Históricas mediante Similitud Coseno Vectorial...")
    summary_query = f"{market} {ticker} en {regime} con Hurst={hurst:.2f} y precio cerca de {last_price}"
    analogies = mcp_find_historical_analogies(current_state_summary=summary_query, market=market)
    analogy_text = (
        f"Analogía principal: Episodio {analogies[0]['ticker']} ({analogies[0]['regime']}) "
        f"con similitud={analogies[0]['similarity_score']:.2f}. Resultado histórico: {analogies[0]['outcome_label']}"
        if analogies else "Sin precedentes episódicos idénticos registrados en Snowflake."
    )

    # 4. Formular Estrategia Intradía (Intraday Execution)
    if regime == "MEAN_REVERSION":
        # Estrategia de rebote en bandas
        if last_price <= l2 or last_price <= val:
            intra_action = "BUY"
            intra_entry = round(last_price, 4)
            intra_sl = round(last_price - (1.0 * atr), 4)
            intra_tp = [round(poc, 4), round(vah, 4)]
        elif last_price >= u2 or last_price >= vah:
            intra_action = "SELL"
            intra_entry = round(last_price, 4)
            intra_sl = round(last_price + (1.0 * atr), 4)
            intra_tp = [round(poc, 4), round(val, 4)]
        else:
            intra_action = "NO_TRADE"
            intra_entry = round(last_price, 4)
            intra_sl = round(last_price - (1.0 * atr), 4)
            intra_tp = [round(poc, 4)]
    elif regime == "TRENDING_MOMENTUM":
        intra_action = "BUY" if cvd >= 0 else "SELL"
        intra_entry = round(last_price, 4)
        intra_sl = round(last_price - (1.2 * atr) if intra_action == "BUY" else last_price + (1.2 * atr), 4)
        target_dist = abs(intra_entry - intra_sl) * 2.5
        intra_tp = [round(intra_entry + target_dist if intra_action == "BUY" else intra_entry - target_dist, 4)]
    else:
        intra_action = "NO_TRADE"
        intra_entry = round(last_price, 4)
        intra_sl = round(last_price - (1.0 * atr), 4)
        intra_tp = [round(poc, 4)]

    # Compute R:R
    risk_intra = abs(intra_entry - intra_sl) if intra_action != "NO_TRADE" else 1.0
    reward_intra = abs(intra_tp[0] - intra_entry) if intra_tp and intra_action != "NO_TRADE" else 0.0
    rr_intra = round(reward_intra / risk_intra, 2) if risk_intra > 0 else 0.0
    kelly_intra = calculate_fractional_kelly(win_rate=0.55, reward_risk_ratio=rr_intra)

    intraday_strat = TradeStrategy(
        action=intra_action,
        horizon="INTRADAY",
        entry_price=intra_entry,
        stop_loss_price=intra_sl,
        take_profit_levels=intra_tp,
        reward_to_risk_ratio=rr_intra,
        fractional_kelly_fraction=kelly_intra,
        suggested_risk_capital_usd=round(account_equity * min(kelly_intra, 0.02), 2),
        technical_rationale=f"Operación intradía basada en régimen {regime} (Hurst={hurst:.3f}), bandas de VWAP y POC.",
        matched_playbook_rule=primary_rule
    )

    # 5. Formular Estrategia Swing / Holder
    swing_action = "BUY" if (market == "CRYPTO" or market == "EQUITIES") and last_price <= poc else "NO_TRADE"
    swing_entry = round(last_price, 4)
    swing_sl = round(last_price * 0.92, 4)  # 8% stop institucional
    swing_tp = [round(last_price * 1.25, 4)] # 25% target
    risk_swing = abs(swing_entry - swing_sl)
    reward_swing = abs(swing_tp[0] - swing_entry)
    rr_swing = round(reward_swing / risk_swing, 2) if risk_swing > 0 else 0.0
    kelly_swing = calculate_fractional_kelly(win_rate=0.60, reward_risk_ratio=rr_swing)

    holder_strat = TradeStrategy(
        action=swing_action,
        horizon="SWING_HOLDER",
        entry_price=swing_entry,
        stop_loss_price=swing_sl,
        take_profit_levels=swing_tp,
        reward_to_risk_ratio=rr_swing,
        fractional_kelly_fraction=kelly_swing,
        suggested_risk_capital_usd=round(account_equity * min(kelly_swing, 0.05), 2),
        technical_rationale="Asignación institucional de posición con sesgo macro y ciclo plurianual.",
        matched_playbook_rule="Playbook 03: Ciclos On-Chain y MVRV" if market == "CRYPTO" else "Playbook 02: Macro y COT"
    )

    # 6. FILTRO DEL RISK OFFICER (Validación estricta en Python)
    print("[4/4] Ejecutando Filtro del Chief Risk Officer (CRO)...")
    intraday_strat = evaluate_risk_officer_filter(intraday_strat, atr=atr, regime=regime, hurst=hurst)
    holder_strat = evaluate_risk_officer_filter(holder_strat, atr=atr, regime=regime, hurst=hurst)

    plan = InstitutionalTradePlan(
        ticker=ticker.upper(),
        market=market.upper(),
        timeframe=timeframe.lower(),
        timestamp_utc=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        market_regime=regime,
        hurst_exponent=hurst,
        atr=round(atr, 4),
        vwap=round(vwap, 4),
        poc=round(poc, 4),
        vah=round(vah, 4),
        val=round(val, 4),
        cvd=round(cvd, 2),
        intraday_execution=intraday_strat,
        holder_swing_allocation=holder_strat,
        historical_analogy_summary=analogy_text,
        overall_portfolio_risk_status="HEALTHY" if intraday_strat.risk_officer_verdict == "APPROVED" else "RESTRICTED"
    )

    return plan


def print_plan_report(plan: InstitutionalTradePlan) -> None:
    """Print an executive institutional markdown report of the trade plan."""
    print("\n" + "=" * 75)
    print(f"REPORTE INSTITUCIONAL DE TRADING: {plan.market} - {plan.ticker} ({plan.timeframe})")
    print("=" * 75)
    print(f"• Timestamp UTC:        {plan.timestamp_utc}")
    print(f"• Régimen de Mercado:   {plan.market_regime} (Hurst={plan.hurst_exponent:.3f}, ATR={plan.atr})")
    print(f"• Microestructura:      VWAP={plan.vwap} | POC={plan.poc} | VAH={plan.vah} | VAL={plan.val} | CVD={plan.cvd}")
    print(f"• Analogía Episódica:   {plan.historical_analogy_summary}")
    print("-" * 75)

    print("\n[ESTRATEGIA 1: EJECUCIÓN INTRADÍA]")
    s1 = plan.intraday_execution
    print(f"  - Acción:             {s1.action}")
    print(f"  - Entrada:            {s1.entry_price}")
    print(f"  - Stop Loss:          {s1.stop_loss_price}")
    print(f"  - Take Profit:        {s1.take_profit_levels}")
    print(f"  - Ratio B/R:          {s1.reward_to_risk_ratio:.2f}")
    print(f"  - 1/4 Kelly Fraction: {s1.fractional_kelly_fraction:.4f}")
    print(f"  - Veredicto Riesgo:   [{s1.risk_officer_verdict}]")
    if s1.risk_rejection_reason:
        print(f"  - Motivo de Rechazo:  {s1.risk_rejection_reason}")
    print(f"  - Regla Playbook:     {s1.matched_playbook_rule}")

    print("\n[ESTRATEGIA 2: ASIGNACIÓN SWING / HOLDER]")
    s2 = plan.holder_swing_allocation
    print(f"  - Acción:             {s2.action}")
    print(f"  - Entrada:            {s2.entry_price}")
    print(f"  - Stop Loss:          {s2.stop_loss_price}")
    print(f"  - Take Profit:        {s2.take_profit_levels}")
    print(f"  - Ratio B/R:          {s2.reward_to_risk_ratio:.2f}")
    print(f"  - 1/4 Kelly Fraction: {s2.fractional_kelly_fraction:.4f}")
    print(f"  - Veredicto Riesgo:   [{s2.risk_officer_verdict}]")
    if s2.risk_rejection_reason:
        print(f"  - Motivo de Rechazo:  {s2.risk_rejection_reason}")
    print(f"  - Regla Playbook:     {s2.matched_playbook_rule}")
    print("=" * 75 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestador Multi-Agente Institucional")
    parser.add_argument("--ticker", type=str, default="BTCUSDT", help="Símbolo del activo (ej. BTCUSDT, NVDA, EURUSD)")
    parser.add_argument("--market", type=str, default="CRYPTO", choices=["CRYPTO", "EQUITIES", "FOREX"], help="Tipo de mercado")
    parser.add_argument("--timeframe", type=str, default="1h", help="Temporalidad (ej. 1h, 1d)")
    parser.add_argument("--equity", type=float, default=100000.0, help="Capital institucional total disponible en USD")
    args = parser.parse_args()

    plan = generate_trade_plan(
        ticker=args.ticker,
        market=args.market,
        timeframe=args.timeframe,
        account_equity=args.equity
    )
    print_plan_report(plan)


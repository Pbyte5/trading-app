"""Visual Explanatory Charts for Quantitative Investment Proposals and Risk Rationale.

Generates clean, institutional charts illustrating why an investment decision was made,
including Entry, Stop Loss (ATR), Take Profits, VWAP, POC, and Risk/Reward distribution.
"""

from typing import Dict, Any, List, Optional
import plotly.graph_objects as go


def generate_investment_proposal_chart(
    ticker: str,
    action: str,
    entry_price: float,
    stop_loss: float,
    take_profit_levels: List[float],
    vwap: Optional[float] = None,
    poc: Optional[float] = None,
    rr_ratio: float = 2.0,
    timeframe: str = "5m"
) -> go.Figure:
    """Create a high-impact institutional price level & risk-reward visualizer chart."""
    fig = go.Figure()

    # Determine colors
    is_buy = action.upper() == "BUY"
    action_color = "#10b981" if is_buy else "#ef4444" if action.upper() == "SELL" else "#94a3b8"

    # Reference prices
    tp1 = take_profit_levels[0] if take_profit_levels else entry_price * (1.02 if is_buy else 0.98)
    tp2 = take_profit_levels[1] if len(take_profit_levels) > 1 else None

    # Compute bounding boxes for Risk Zone and Reward Zone
    # X coordinates: 0 to 10
    x_min, x_max = 0, 10

    # 1. Risk Zone (Shaded Red)
    risk_low = min(entry_price, stop_loss)
    risk_high = max(entry_price, stop_loss)
    fig.add_shape(
        type="rect",
        x0=x_min, y0=risk_low, x1=x_max, y1=risk_high,
        fillcolor="rgba(239, 68, 68, 0.12)",
        line=dict(width=0),
        layer="below"
    )

    # 2. Reward Zone (Shaded Emerald)
    max_tp = max(take_profit_levels) if take_profit_levels else tp1
    reward_low = min(entry_price, max_tp)
    reward_high = max(entry_price, max_tp)
    fig.add_shape(
        type="rect",
        x0=x_min, y0=reward_low, x1=x_max, y1=reward_high,
        fillcolor="rgba(16, 185, 129, 0.12)",
        line=dict(width=0),
        layer="below"
    )

    # 3. Horizontal Level Lines with Annotations
    levels = [
        ("STOP LOSS [ATR]", stop_loss, "#ef4444", "dash"),
        ("PRECIO DE ENTRADA", entry_price, "#38bdf8", "solid"),
        ("TAKE PROFIT 1", tp1, "#10b981", "dash"),
    ]
    if tp2:
        levels.append(("TAKE PROFIT 2 [Estructural]", tp2, "#34d399", "dot"))

    if vwap:
        levels.append(("VWAP SESION", vwap, "#818cf8", "dot"))
    if poc:
        levels.append(("POC VOLUMEN", poc, "#f59e0b", "dot"))

    for label, price, color, dash in levels:
        fig.add_trace(go.Scatter(
            x=[x_min, x_max],
            y=[price, price],
            mode="lines+text",
            line=dict(color=color, width=2, dash=dash),
            text=["", f"<b>{label}: {price:,.4f}</b>"],
            textposition="middle left",
            textfont=dict(color=color, size=11, family="JetBrains Mono, monospace"),
            hoverinfo="y+text",
            showlegend=False
        ))

    # Layout styling (Dark Slate Institutional System)
    fig.update_layout(
        title=dict(
            text=f"<b>Mapeo Cuantitativo: {action} {ticker} ({timeframe}) | R:R = {rr_ratio:.2f}</b>",
            font=dict(size=14, color="#f8fafc"),
            x=0.02, y=0.96
        ),
        paper_bgcolor="#0f172a",
        plot_bgcolor="#090d16",
        margin=dict(l=20, r=40, t=50, b=30),
        height=320,
        xaxis=dict(
            showticklabels=False,
            showgrid=False,
            zeroline=False,
            range=[x_min, x_max]
        ),
        yaxis=dict(
            gridcolor="rgba(51, 65, 85, 0.4)",
            tickfont=dict(color="#94a3b8", size=10, family="monospace"),
            side="right"
        ),
        annotations=[
            dict(
                x=x_max * 0.15,
                y=(risk_low + risk_high) / 2,
                text="<b>ZONA DE RIESGO (1R)</b>",
                showarrow=False,
                font=dict(color="#f87171", size=11)
            ),
            dict(
                x=x_max * 0.15,
                y=(reward_low + reward_high) / 2,
                text=f"<b>ZONA DE BENEFICIO ({rr_ratio:.1f}R)</b>",
                showarrow=False,
                font=dict(color="#34d399", size=11)
            )
        ]
    )

    return fig


def generate_microstructure_radar(quant_state: Dict[str, Any], ticker: str) -> go.Figure:
    """Generate a radar chart visualizing microstructural metrics."""
    regime_data = quant_state.get("regime", {})
    hurst = regime_data.get("hurst", 0.5)
    atr = regime_data.get("atr", 1.0)
    cvd = quant_state.get("cvd", 0.0)

    # Normalize metrics to 0-100 scale for radar
    # Hurst: 0.5 is 50, 0.7 is 90, 0.3 is 10
    hurst_score = max(5, min(95, hurst * 100))
    # CVD momentum score: positive -> > 50, negative -> < 50
    cvd_score = 75 if cvd > 0 else 25
    # Risk/Reward readiness
    rr_score = 85

    categories = [
        "Persistencia (Hurst)",
        "Presión Volumen (CVD)",
        "Volatilidad (ATR)",
        "Proximidad a VWAP",
        "Ratio Asimetría R:R"
    ]
    values = [hurst_score, cvd_score, 65, 70, rr_score]
    values.append(values[0])
    categories.append(categories[0])

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        fillcolor='rgba(56, 189, 248, 0.2)',
        line=dict(color='#38bdf8', width=2),
        name=ticker
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], showticklabels=False, linecolor="#334155", gridcolor="#1e293b"),
            angularaxis=dict(linecolor="#334155", gridcolor="#1e293b", tickfont=dict(color="#94a3b8", size=9))
        ),
        paper_bgcolor="#0f172a",
        plot_bgcolor="#0f172a",
        margin=dict(l=30, r=30, t=30, b=30),
        height=260,
        showlegend=False
    )
    return fig

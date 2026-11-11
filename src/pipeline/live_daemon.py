"""Continuous Live Pipeline Daemon for Multi-Market Ingestion and Quant Computation.

Maintains a 7-day rolling window of 5-minute and 1-hour candles in Snowflake Bronze & Silver,
and continuously recomputes the Gold feature store (VWAP, Volume Profile, Hurst, Regime).
"""

import argparse
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.connectors.crypto_client import fetch_and_load_crypto
from src.connectors.equities_client import fetch_and_load_equities
from src.connectors.forex_client import fetch_and_load_forex
from src.connectors.macro_client import fetch_and_load_macro
from src.quant_engine.microstructure import process_quant_state_for_ticker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (LivePipeline) %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("LivePipeline")

STATE_FILE = Path(__file__).resolve().parent.parent.parent / "live_daemon_state.json"

_stop_event = threading.Event()
_daemon_thread: Optional[threading.Thread] = None


def get_pipeline_state() -> Dict[str, Any]:
    """Read the current status of the live pipeline from disk."""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "is_running": False,
        "status": "STOPPED",
        "last_sync_utc": None,
        "cycles_completed": 0,
        "days_lookback": 7,
        "timeframes": ["5m", "1h"],
        "assets_tracked": ["BTCUSDT (CRYPTO)", "NVDA (EQUITIES)", "EURUSD (FOREX)"],
        "last_error": None
    }


def save_pipeline_state(state: Dict[str, Any]) -> None:
    """Save the pipeline state to disk."""
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning(f"No se pudo guardar el estado del daemon: {e}")


def execute_pipeline_cycle(days: int = 7, timeframes: Optional[List[str]] = None) -> Dict[str, Any]:
    """Execute a single end-to-end sync and quant computation cycle."""
    tfs = timeframes or ["5m", "1h"]
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"=== INICIANDO CICLO EN VIVO: Ventana {days} días | Temporalidades: {tfs} ===")

    stats = {"crypto": 0, "equities": 0, "forex": 0, "macro": 0, "quant_states": 0}

    # 1. Ingest Crypto (BTCUSDT)
    for tf in tfs:
        try:
            res = fetch_and_load_crypto(ticker="BTCUSDT", timeframe=tf, days=days)
            stats["crypto"] += res.get("silver_upserted", 0)
        except Exception as e:
            logger.error(f"Error sincronizando Cripto ({tf}): {e}")

    # 2. Ingest Equities (NVDA)
    for tf in tfs:
        try:
            res = fetch_and_load_equities(ticker="NVDA", timeframe=tf, days=days)
            stats["equities"] += res.get("silver_upserted", 0)
        except Exception as e:
            logger.error(f"Error sincronizando Acciones ({tf}): {e}")

    # 3. Ingest Forex (EURUSD)
    for tf in tfs:
        try:
            res = fetch_and_load_forex(ticker="EURUSD", timeframe=tf, days=days)
            stats["forex"] += res.get("silver_upserted", 0)
        except Exception as e:
            logger.error(f"Error sincronizando Forex ({tf}): {e}")

    # 4. Ingest Macro Indicators
    try:
        m_count = fetch_and_load_macro()
        stats["macro"] = m_count
    except Exception as e:
        logger.error(f"Error sincronizando Macro: {e}")

    # 5. Compute Gold Quant States (Microstructure, Hurst, ATR, VWAP)
    for tf in tfs:
        for market, ticker in [("CRYPTO", "BTCUSDT"), ("EQUITIES", "NVDA"), ("FOREX", "EURUSD")]:
            try:
                state = process_quant_state_for_ticker(market=market, ticker=ticker, timeframe=tf)
                if state:
                    stats["quant_states"] += 1
            except Exception as e:
                logger.error(f"Error calculando estado cuantitativo para {ticker} ({tf}): {e}")

    logger.info(f"=== CICLO COMPLETADO: {stats} ===")
    return stats


def _daemon_worker(interval_seconds: int = 60, days: int = 7, timeframes: Optional[List[str]] = None):
    """Background thread function running continuous live cycles."""
    tfs = timeframes or ["5m", "1h"]
    state = get_pipeline_state()
    state["is_running"] = True
    state["status"] = "BOOTSTRAPPING"
    state["days_lookback"] = days
    state["timeframes"] = tfs
    save_pipeline_state(state)

    logger.info(f"Daemon iniciado. Refresco cada {interval_seconds}s. Ventana de 7 días activa.")

    cycles = 0
    # First cycle: Full 7-day bootstrap
    try:
        execute_pipeline_cycle(days=days, timeframes=tfs)
        cycles += 1
        state["cycles_completed"] = cycles
        state["last_sync_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        state["status"] = "LIVE_RUNNING"
        save_pipeline_state(state)
    except Exception as e:
        state["last_error"] = str(e)
        state["status"] = "ERROR"
        save_pipeline_state(state)
        logger.error(f"Falla en ciclo bootstrap: {e}")

    # Subsequent continuous cycles (refreshing rolling bars)
    while not _stop_event.is_set():
        # Sleep in small increments for fast shutdown
        for _ in range(interval_seconds):
            if _stop_event.is_set():
                break
            time.sleep(1)

        if _stop_event.is_set():
            break

        try:
            logger.info("Ejecutando ciclo de actualización periódica en Snowflake...")
            # For rolling live updates, syncing 1 day is sufficient and fast
            execute_pipeline_cycle(days=2, timeframes=tfs)
            cycles += 1
            state = get_pipeline_state()
            state["cycles_completed"] = cycles
            state["last_sync_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            state["status"] = "LIVE_RUNNING"
            save_pipeline_state(state)
        except Exception as e:
            logger.error(f"Error en ciclo periódico: {e}")
            state = get_pipeline_state()
            state["last_error"] = str(e)
            save_pipeline_state(state)

    state = get_pipeline_state()
    state["is_running"] = False
    state["status"] = "STOPPED"
    save_pipeline_state(state)
    logger.info("Daemon detenido ordenadamente.")


def start_live_daemon(interval_seconds: int = 60, days: int = 7, timeframes: Optional[List[str]] = None) -> bool:
    """Start the live background daemon thread if not already running."""
    global _daemon_thread, _stop_event
    if _daemon_thread and _daemon_thread.is_alive():
        logger.info("El daemon ya se encuentra en ejecución.")
        return False

    _stop_event.clear()
    _daemon_thread = threading.Thread(
        target=_daemon_worker,
        kwargs={"interval_seconds": interval_seconds, "days": days, "timeframes": timeframes},
        daemon=True,
        name="LiveQuantDaemon"
    )
    _daemon_thread.start()
    return True


def stop_live_daemon() -> None:
    """Stop the live background daemon."""
    global _stop_event
    _stop_event.set()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daemon de Ingesta Continua y Cuantificación en Vivo")
    parser.add_argument("--interval", type=int, default=60, help="Intervalo en segundos entre ciclos (por defecto 60s)")
    parser.add_argument("--days", type=int, default=7, help="Días de histórico para la ventana móvil (por defecto 7)")
    parser.add_argument("--timeframe", type=str, default="5m", help="Temporalidad prioritaria (5m, 1h)")
    parser.add_argument("--once", action="store_true", help="Ejecutar un solo ciclo y salir")
    args = parser.parse_args()

    tfs = [args.timeframe] if args.timeframe == "1h" else [args.timeframe, "1h"]

    if args.once:
        execute_pipeline_cycle(days=args.days, timeframes=tfs)
    else:
        print("Iniciando servicio continuo en primer plano (Presiona Ctrl + C para detener)...")
        start_live_daemon(interval_seconds=args.interval, days=args.days, timeframes=tfs)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nDeteniendo daemon...")
            stop_live_daemon()
            if _daemon_thread:
                _daemon_thread.join(timeout=5)
            print("Daemon finalizado.")

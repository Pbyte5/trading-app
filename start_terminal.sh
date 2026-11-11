#!/usr/bin/env bash
# ==============================================================================
# QUANT CORTEX | Launcher Script for Institutional Trading Terminal & Live Daemon
# ==============================================================================

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

echo "=================================================================="
echo "⚡ INICIANDO QUANT CORTEX TERMINAL (Snowflake AI Data Cloud)"
echo "=================================================================="

# Activar entorno virtual
if [ -d ".venv" ]; then
    source .venv/bin/activate
else
    echo "❌ Error: Entorno virtual .venv no encontrado."
    exit 1
fi

echo "Verificando dependencias..."
python3 -c "import streamlit, plotly, snowflake.connector" 2>/dev/null || {
    echo "Instalando paquetes pendientes..."
    pip install streamlit plotly
}

echo "Lanzando terminal web Streamlit en http://localhost:8501 ..."
streamlit run app.py --server.port 8501 --server.headless false

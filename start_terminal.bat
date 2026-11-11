@echo off
setlocal
cd /d "%~dp0"

echo ==================================================================
echo ⚡ INICIANDO QUANT CORTEX TERMINAL (Snowflake AI Data Cloud)
echo ==================================================================

if not exist ".venv\Scripts\activate.bat" (
    echo [!] Entorno virtual .venv no encontrado. Creando entorno virtual...
    python -m venv .venv
)

echo [*] Activando entorno virtual .venv...
call .venv\Scripts\activate.bat

echo [*] Verificando dependencias...
python -c "import streamlit, plotly, snowflake.connector" 2>nul
if %errorlevel% neq 0 (
    echo [*] Instalando dependencias necesarias...
    pip install -r requirements.txt streamlit plotly
)

echo [*] Lanzando terminal Streamlit en http://localhost:8501 (Auto-reload habilitado)...
streamlit run app.py --server.port 8501 --server.runOnSave true
pause

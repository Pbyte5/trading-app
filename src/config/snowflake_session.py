"""Snowflake session management with Snowpark Python and Key-Pair / Password auth.

Provides connection utilities and database initialization (--init-db).
"""

import argparse
import logging
import os
import sys
import threading
from pathlib import Path
from typing import Dict, Any, Optional

from src.config.settings import get_settings

logger = logging.getLogger("snowflake_session")

# Thread-safe singletons for connection pooling across the process
_session_lock = threading.Lock()
_shared_raw_conn = None
_shared_snowpark_session = None
_current_raw_tag: Optional[str] = None
_current_snowpark_tag: Optional[str] = None


class SharedSnowflakeConnectionProxy:
    """Wrapper around SnowflakeConnection that prevents callers from closing the shared connection.
    
    Callers that execute 'finally: conn.close()' will not terminate the underlying
    Snowflake session, allowing warehouse auto-suspend and connection reuse.
    """
    def __init__(self, target_conn):
        self._target = target_conn

    def close(self) -> None:
        # Intercept close() to preserve the shared singleton session
        pass

    def force_close(self) -> None:
        """Explicitly terminate the underlying Snowflake connection."""
        try:
            self._target.close()
        except Exception as e:
            logger.debug(f"Error closing underlying Snowflake connection: {e}")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._target, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class SharedSnowparkSessionProxy:
    """Wrapper around Snowpark Session that prevents callers from closing the shared session."""
    def __init__(self, target_session):
        self._target = target_session

    def close(self) -> None:
        # Intercept close() to preserve the shared singleton session
        pass

    def force_close(self) -> None:
        """Explicitly terminate the underlying Snowpark session."""
        try:
            self._target.close()
        except Exception as e:
            logger.debug(f"Error closing underlying Snowpark session: {e}")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._target, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def get_private_key_bytes(key_path: str, passphrase: Optional[str] = None) -> bytes:
    """Read and decrypt an RSA private key for Snowflake Key-Pair authentication."""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    with open(key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend()
        )

    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )


def build_connection_params(use_initial_warehouse: bool = False) -> Dict[str, Any]:
    """Build connection parameters dictionary from settings."""
    settings = get_settings()

    if not settings.snowflake_account or not settings.snowflake_user:
        raise ValueError(
            "Faltan credenciales de Snowflake. Configura SNOWFLAKE_ACCOUNT y SNOWFLAKE_USER en tu archivo .env"
        )

    if settings.snowflake_account in ("xy12345.us-east-1", "MI_ORGANIZACION-MI_CUENTA", "tu_cuenta", "xy12345"):
        raise ValueError(
            f"El valor actual de SNOWFLAKE_ACCOUNT ('{settings.snowflake_account}') es una plantilla de ejemplo. "
            "Debes configurar tu identificador de cuenta real de Snowflake en el archivo .env (ej. 'orgname-accountname')."
        )

    params: Dict[str, Any] = {
        "account": settings.snowflake_account,
        "user": settings.snowflake_user,
        "role": settings.snowflake_role,
    }

    if settings.snowflake_authenticator:
        params["authenticator"] = settings.snowflake_authenticator

    # Optional initial warehouse and db (can be omitted during initial setup)
    if not use_initial_warehouse:
        if settings.snowflake_warehouse:
            params["warehouse"] = settings.snowflake_warehouse
        if settings.snowflake_database:
            params["database"] = settings.snowflake_database
        if settings.snowflake_schema:
            params["schema"] = settings.snowflake_schema
    else:
        # For initial setup, use user-configured warehouse (e.g. COMPUTE_WH) if available
        if settings.snowflake_warehouse:
            params["warehouse"] = settings.snowflake_warehouse

    if settings.snowflake_private_key_path and os.path.exists(settings.snowflake_private_key_path):
        params["private_key"] = get_private_key_bytes(
            settings.snowflake_private_key_path,
            settings.snowflake_private_key_passphrase
        )
    elif settings.snowflake_password:
        params["password"] = settings.snowflake_password
    elif settings.snowflake_authenticator == "externalbrowser":
        pass  # Browser SSO login does not need static password
    else:
        raise ValueError(
            "Debes proporcionar SNOWFLAKE_PASSWORD, SNOWFLAKE_PRIVATE_KEY_PATH o usar SNOWFLAKE_AUTHENTICATOR='externalbrowser' en .env"
        )

    return params


def get_snowpark_session(use_initial_warehouse: bool = False, query_tag: Optional[str] = None):
    """Return a shared Snowpark Session instance (or fresh one if use_initial_warehouse=True)."""
    try:
        from snowflake.snowpark import Session
    except ImportError as e:
        raise ImportError(
            "El paquete 'snowflake-snowpark-python' no está instalado. "
            "Instálalo con 'pip install snowflake-snowpark-python'."
        ) from e

    if use_initial_warehouse:
        params = build_connection_params(use_initial_warehouse=True)
        return Session.builder.configs(params).create()

    global _shared_snowpark_session, _current_snowpark_tag

    with _session_lock:
        tag = query_tag or os.getenv("SNOWFLAKE_QUERY_TAG", "PROJECT_FINANCIAL_AGENT")
        is_healthy = False
        if _shared_snowpark_session is not None:
            try:
                # Snowpark session validity check
                if hasattr(_shared_snowpark_session, "_conn") and hasattr(_shared_snowpark_session._conn, "is_closed"):
                    is_healthy = not _shared_snowpark_session._conn.is_closed()
                else:
                    is_healthy = True
            except Exception:
                is_healthy = False

        if not is_healthy:
            params = build_connection_params(use_initial_warehouse=False)
            params["session_parameters"] = {"QUERY_TAG": tag}
            _shared_snowpark_session = Session.builder.configs(params).create()
            _current_snowpark_tag = tag
        elif query_tag and query_tag != _current_snowpark_tag:
            try:
                safe_tag = "".join(c for c in query_tag if c.isalnum() or c in "_-:")
                _shared_snowpark_session.sql(f"ALTER SESSION SET QUERY_TAG = '{safe_tag}'").collect()
                _current_snowpark_tag = safe_tag
            except Exception as e:
                logger.warning(f"Error setting Snowpark QUERY_TAG: {e}")

        return SharedSnowparkSessionProxy(_shared_snowpark_session)


def get_raw_connection(use_initial_warehouse: bool = False, query_tag: Optional[str] = None, *args, **kwargs):
    """Return a shared raw snowflake connection (or fresh one if use_initial_warehouse=True)."""
    if "query_tag" in kwargs and not query_tag:
        query_tag = kwargs["query_tag"]
    try:
        import snowflake.connector
    except ImportError as e:
        raise ImportError(
            "El paquete 'snowflake-connector-python' no está instalado. "
            "Instálalo con 'pip install snowflake-connector-python'."
        ) from e

    if use_initial_warehouse:
        params = build_connection_params(use_initial_warehouse=True)
        return snowflake.connector.connect(**params)

    global _shared_raw_conn, _current_raw_tag

    with _session_lock:
        tag = query_tag or os.getenv("SNOWFLAKE_QUERY_TAG", "PROJECT_FINANCIAL_AGENT")
        is_healthy = False
        if _shared_raw_conn is not None:
            try:
                is_healthy = not _shared_raw_conn.is_closed()
            except Exception:
                is_healthy = False

        if not is_healthy:
            params = build_connection_params(use_initial_warehouse=False)
            params["session_parameters"] = {"QUERY_TAG": tag}
            _shared_raw_conn = snowflake.connector.connect(**params)
            _current_raw_tag = tag
        elif query_tag and query_tag != _current_raw_tag:
            try:
                safe_tag = "".join(c for c in query_tag if c.isalnum() or c in "_-:")
                with _shared_raw_conn.cursor() as cur:
                    cur.execute(f"ALTER SESSION SET QUERY_TAG = '{safe_tag}'")
                _current_raw_tag = safe_tag
            except Exception as e:
                logger.warning(f"Error setting raw connection QUERY_TAG: {e}")

        return SharedSnowflakeConnectionProxy(_shared_raw_conn)


def close_all_shared_connections() -> None:
    """Explicitly terminate all shared connections (call at shutdown)."""
    global _shared_raw_conn, _shared_snowpark_session, _current_raw_tag, _current_snowpark_tag
    with _session_lock:
        if _shared_raw_conn is not None:
            try:
                _shared_raw_conn.close()
            except Exception:
                pass
            _shared_raw_conn = None
            _current_raw_tag = None

        if _shared_snowpark_session is not None:
            try:
                _shared_snowpark_session.close()
            except Exception:
                pass
            _shared_snowpark_session = None
            _current_snowpark_tag = None


def set_active_query_tag(tag: str) -> None:
    """Set QUERY_TAG on active shared connections."""
    global _current_raw_tag, _current_snowpark_tag
    safe_tag = "".join(c for c in tag if c.isalnum() or c in "_-:")
    with _session_lock:
        if _shared_raw_conn is not None and not _shared_raw_conn.is_closed():
            try:
                with _shared_raw_conn.cursor() as cur:
                    cur.execute(f"ALTER SESSION SET QUERY_TAG = '{safe_tag}'")
                _current_raw_tag = safe_tag
            except Exception as e:
                logger.warning(f"Failed to update raw connection QUERY_TAG: {e}")

        if _shared_snowpark_session is not None:
            try:
                _shared_snowpark_session.sql(f"ALTER SESSION SET QUERY_TAG = '{safe_tag}'").collect()
                _current_snowpark_tag = safe_tag
            except Exception as e:
                logger.warning(f"Failed to update Snowpark QUERY_TAG: {e}")



def execute_sql_file(conn_or_session, sql_file_path: Path) -> None:
    """Execute all SQL statements in a given SQL script file."""
    if not sql_file_path.exists():
        raise FileNotFoundError(f"Archivo SQL no encontrado: {sql_file_path}")

    print(f"\n[*] Ejecutando script: {sql_file_path.name}")
    content = sql_file_path.read_text(encoding="utf-8")

    # Split statements by semicolon while ignoring comments and empty lines
    raw_statements = content.split(";")
    statements = []
    for stmt in raw_statements:
        cleaned = stmt.strip()
        if cleaned:
            # Remove full-line comments to verify if statement has executable code
            lines = [l for l in cleaned.splitlines() if not l.strip().startswith("--")]
            if "".join(lines).strip():
                statements.append(cleaned)

    # Detect if we have a Snowpark session or a standard connector
    is_snowpark = hasattr(conn_or_session, "sql")

    cursor = None
    if not is_snowpark:
        cursor = conn_or_session.cursor()

    for idx, stmt in enumerate(statements, 1):
        # Preview first line for logging
        first_line = stmt.strip().splitlines()[0][:70]
        try:
            if is_snowpark:
                conn_or_session.sql(stmt).collect()
            else:
                cursor.execute(stmt)
            print(f"  [OK] ({idx}/{len(statements)}) {first_line}...")
        except Exception as err:
            print(f"  [ERROR] en sentencia {idx}: {first_line}...")
            print(f"          Detalle: {err}")
            raise err


def init_database() -> None:
    """Initialize warehouses, database, schemas, tables and Cortex Search."""
    print("=" * 70)
    print("INICIALIZACIÓN DE INFRAESTRUCTURA SNOWFLAKE (FASE 1)")
    print("=" * 70)

    project_root = Path(__file__).resolve().parent.parent.parent
    sql_dir = project_root / "snowflake_sql"

    scripts = [
        sql_dir / "01_setup_warehouses_db.sql",
        sql_dir / "02_tables_and_vectors.sql",
        sql_dir / "03_cortex_search_service.sql",
    ]

    # Use initial warehouse (e.g. COMPUTE_WH) to create databases and new warehouses
    print("[*] Conectando a Snowflake con credenciales de inicialización...")
    try:
        session = get_snowpark_session(use_initial_warehouse=True)
        active_session = session
        use_snowpark = True
    except Exception as err_snowpark:
        print(f"[!] Snowpark no disponible o error al conectar con Snowpark: {err_snowpark}")
        print("[*] Intentando conexión alternativa con snowflake-connector-python...")
        active_session = get_raw_connection(use_initial_warehouse=True)
        use_snowpark = False

    try:
        for script in scripts:
            try:
                execute_sql_file(active_session, script)
            except Exception as err:
                if "03_cortex_search_service.sql" in str(script) and "trial accounts" in str(err).lower():
                    print(f"\n[AVISO SNOWFLAKE] Servicio Cortex Search omitido en cuenta de prueba:")
                    print("  -> Snowflake restringe los servicios Cortex Search en cuentas tipo 'Trial'.")
                    print("  -> La base de datos, warehouses y todas las tablas Medallion quedaron creadas al 100%.")
                else:
                    raise err
        print("\n" + "=" * 70)
        print("[EXITO] Toda la infraestructura de Snowflake ha sido creada correctamente:")
        print("  - Base de Datos: QUANT_INSTITUTIONAL_DB")
        print("  - Esquemas: BRONZE_RAW, SILVER_CLEAN, GOLD_QUANT_RAG")
        print("  - Warehouses: INGEST_WH, QUANT_RAG_WH (XSMALL)")
        print("  - Tablas: RAW_*, MARKET_CANDLES, QUANT_MARKET_STATES, INSTITUTIONAL_PLAYBOOKS, EPISODIC_MEMORY_STATES")
        print("  - Servicio Cortex: PLAYBOOK_SEARCH_SERVICE")
        print("=" * 70)
    finally:
        if use_snowpark:
            active_session.close()
        else:
            active_session.close()


def test_connection() -> None:
    """Test connection to Snowflake using current .env parameters."""
    print("[*] Probando conexión con Snowflake...")
    try:
        try:
            session = get_snowpark_session(use_initial_warehouse=True)
            row = session.sql("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()").collect()
            print(f"[CONEXION EXITOSA VÍA SNOWPARK] Sesión activa: {row[0]}")
            session.close()
            return
        except ImportError:
            pass

        conn = get_raw_connection(use_initial_warehouse=True)
        with conn.cursor() as cur:
            cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
            row = cur.fetchone()
            print(f"[CONEXION EXITOSA VÍA CONNECTOR] Usuario: {row[0]} | Rol: {row[1]} | Warehouse: {row[2]}")
        conn.close()
    except Exception as e:
        print(f"[ERROR DE CONEXION] {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gestor de sesión y despliegue en Snowflake")
    parser.add_argument("--init-db", action="store_true", help="Crear Warehouses, BD, Esquemas, Tablas y Cortex Search")
    parser.add_argument("--test-connection", action="store_true", help="Verificar la conectividad con Snowflake")
    args = parser.parse_args()

    if args.init_db:
        init_database()
    elif args.test_connection:
        test_connection()
    else:
        parser.print_help()


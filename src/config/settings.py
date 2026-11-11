"""Application configuration loaded from environment variables and .env file."""

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional
from pydantic import BaseModel, Field

# Helper to read .env file manually if python-dotenv or pydantic-settings is missing
def load_env_file(env_path: Path) -> dict:
    values = {}
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            values[k] = v
    return values


class Settings(BaseModel):
    """Central settings for Snowflake and financial market APIs."""

    # Snowflake AI Data Cloud
    snowflake_account: str = Field(default="")
    snowflake_user: str = Field(default="")
    snowflake_password: Optional[str] = Field(default=None)
    snowflake_authenticator: str = Field(default="snowflake")  # 'snowflake', 'externalbrowser', or 'username_password_mfa'
    snowflake_role: str = Field(default="ACCOUNTADMIN")
    snowflake_warehouse: str = Field(default="QUANT_RAG_WH")
    snowflake_database: str = Field(default="QUANT_INSTITUTIONAL_DB")
    snowflake_schema: str = Field(default="GOLD_QUANT_RAG")

    # Key-pair authentication (alternative to password)
    snowflake_private_key_path: Optional[str] = Field(default=None)
    snowflake_private_key_passphrase: Optional[str] = Field(default=None)

    # LLM Settings
    gemini_api_key: Optional[str] = Field(default=None)

    # Market APIs - Crypto
    binance_api_key: Optional[str] = Field(default=None)
    binance_secret_key: Optional[str] = Field(default=None)
    coinglass_api_key: Optional[str] = Field(default=None)

    # Market APIs - Equities
    alpaca_api_key: Optional[str] = Field(default=None)
    alpaca_secret_key: Optional[str] = Field(default=None)
    alpaca_base_url: str = Field(default="https://paper-api.alpaca.markets")
    fmp_api_key: Optional[str] = Field(default=None)

    # Market APIs - Forex
    oanda_api_key: Optional[str] = Field(default=None)
    oanda_account_id: Optional[str] = Field(default=None)
    twelvedata_api_key: Optional[str] = Field(default=None)

    # Macro & News
    fred_api_key: Optional[str] = Field(default=None)
    finnhub_api_key: Optional[str] = Field(default=None)


@lru_cache()
def get_settings() -> Settings:
    """Return an application settings instance loaded from .env and environment variables."""
    env_file = Path(__file__).resolve().parent.parent.parent / ".env"
    file_vars = load_env_file(env_file)

    def get_val(key: str, default: Any = None):
        return os.environ.get(key, file_vars.get(key, default))

    return Settings(
        snowflake_account=get_val("SNOWFLAKE_ACCOUNT", ""),
        snowflake_user=get_val("SNOWFLAKE_USER", ""),
        snowflake_password=get_val("SNOWFLAKE_PASSWORD", None),
        snowflake_authenticator=get_val("SNOWFLAKE_AUTHENTICATOR", "snowflake"),
        snowflake_role=get_val("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        snowflake_warehouse=get_val("SNOWFLAKE_WAREHOUSE", "QUANT_RAG_WH"),
        snowflake_database=get_val("SNOWFLAKE_DATABASE", "QUANT_INSTITUTIONAL_DB"),
        snowflake_schema=get_val("SNOWFLAKE_SCHEMA", "GOLD_QUANT_RAG"),
        snowflake_private_key_path=get_val("SNOWFLAKE_PRIVATE_KEY_PATH", None),
        snowflake_private_key_passphrase=get_val("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", None),
        gemini_api_key=get_val("GEMINI_API_KEY", None),
        binance_api_key=get_val("BINANCE_API_KEY", None),
        binance_secret_key=get_val("BINANCE_SECRET_KEY", None),
        coinglass_api_key=get_val("COINGLASS_API_KEY", None),
        alpaca_api_key=get_val("ALPACA_API_KEY", None),
        alpaca_secret_key=get_val("ALPACA_SECRET_KEY", None),
        alpaca_base_url=get_val("ALPACA_BASE_URL", "https://paper-api.alpaca.markets"),
        fmp_api_key=get_val("FMP_API_KEY", None),
        oanda_api_key=get_val("OANDA_API_KEY", None),
        oanda_account_id=get_val("OANDA_ACCOUNT_ID", None),
        twelvedata_api_key=get_val("TWELVEDATA_API_KEY", None),
        fred_api_key=get_val("FRED_API_KEY", None),
        finnhub_api_key=get_val("FINNHUB_API_KEY", None),
    )

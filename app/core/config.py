"""Configuracao da aplicacao.

Todos os limites usados pelo motor de Data Quality moram aqui: nenhum numero
magico deve aparecer inline nas regras ou nos servicos.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuracao tipada, carregada de variaveis de ambiente ou de `.env`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Identidade da aplicacao -------------------------------------------
    app_name: str = "Real-time Event Ingestion & Data Quality Engine"
    app_version: str = "1.0.0"
    environment: str = "local"

    # --- Persistencia ------------------------------------------------------
    database_url: str = "postgresql+asyncpg://dq:dq@postgres:5432/dq"
    db_pool_size: int = 5
    db_max_overflow: int = 10

    # --- OpenTelemetry -----------------------------------------------------
    otel_sdk_disabled: bool = False
    otel_exporter_otlp_endpoint: str = "http://jaeger:4317"
    otel_service_name: str = "data-quality-engine"

    # --- Motor de Data Quality --------------------------------------------
    strict_mode: bool = False
    future_timestamp_tolerance_seconds: int = 300
    retention_window_days: int = 30
    temperature_min: float = -90.0
    temperature_max: float = 60.0
    humidity_min: float = 0.0
    humidity_max: float = 100.0
    value_min: float = 0.0
    max_string_length: int = 512
    max_envelope_string_length: int = 128
    """Teto das strings do envelope; deve acompanhar o `String(128)` das colunas
    `event_id`, `event_type` e `source` para que o limite seja adjudicado pelo
    motor e nunca pelo banco."""
    dedup_window_size: int = 10000

    # --- Limites de requisicao --------------------------------------------
    max_batch_size: int = 1000
    """Maximo de elementos aceitos em um array JSON numa unica requisicao."""

    # --- Observabilidade ---------------------------------------------------
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """Retorna a instancia unica (cacheada) de `Settings`."""
    return Settings()

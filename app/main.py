"""Fabrica da aplicacao FastAPI: lifespan, rotas, telemetria e handlers.

O lifespan cria o engine, cria as tabelas com `init_models` (decisao D5: sem
Alembic, `docker compose up` sobe o servico com o schema pronto) e configura a
telemetria; no shutdown o pool e fechado.
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.api.v1 import health
from app.api.v1.router import TAG_EVENTS, TAG_HEALTH, api_router
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.core.telemetry import (
    configure_telemetry,
    current_trace_id,
    instrument_database,
)
from app.domain.errors import PersistenceError
from app.domain.schemas import ErrorResponse
from app.infrastructure.database import (
    create_engine_from_settings,
    dispose_engine,
    init_models,
    set_engine,
)

logger = logging.getLogger(__name__)

API_V1_PREFIX = "/api/v1"

DESCRIPTION = """
Ingestao de eventos em tempo real com um motor de Data Quality componivel.

* `POST /api/v1/events` aceita um objeto JSON ou um array de objetos.
* Eventos aprovados (com ou sem sanitizacao) vao para `valid_events`.
* Eventos rejeitados vao para o dead letter `invalid_events` com a razao.
* Toda requisicao processada responde **202 Accepted**: a rejeicao e um
  resultado de negocio, nao um erro de transporte.
""".strip()

OPENAPI_TAGS: list[dict[str, str]] = [
    {"name": TAG_EVENTS, "description": "Recebimento e adjudicacao de eventos."},
    {"name": TAG_HEALTH, "description": "Liveness e readiness do servico."},
]

DATABASE_ERROR_DETAIL = (
    "Falha ao persistir o evento; nenhum evento foi descartado em silencio."
)
INTERNAL_ERROR_DETAIL = "Erro interno inesperado."


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Cria o banco e instrumenta o engine; libera o pool no shutdown.

    O logging e a instrumentacao da aplicacao ficam em `create_app`: o
    `FastAPIInstrumentor` precisa rodar antes do startup (decisao D19).
    """
    settings = get_settings()

    engine = create_engine_from_settings(settings)
    set_engine(engine)
    await init_models(engine)
    instrument_database(engine, settings)

    logger.info(
        "Aplicacao iniciada",
        extra={"app_version": settings.app_version, "environment": settings.environment},
    )
    try:
        yield
    finally:
        await dispose_engine()
        logger.info("Aplicacao encerrada")


async def persistence_error_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    """Mapeia `PersistenceError` para 503 com o trace correlacionado (D12)."""
    trace_id = current_trace_id()
    logger.error(
        "Falha de persistencia na ingestao",
        exc_info=exc,
        extra={"path": request.url.path, "trace_id_ref": trace_id},
    )
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content=ErrorResponse(
            detail=DATABASE_ERROR_DETAIL, trace_id=trace_id
        ).model_dump(),
    )


async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Ultima rede de seguranca: 500 com o trace para correlacionar no Jaeger."""
    trace_id = current_trace_id()
    logger.error(
        "Excecao nao tratada",
        exc_info=exc,
        extra={"path": request.url.path, "trace_id_ref": trace_id},
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ErrorResponse(
            detail=INTERNAL_ERROR_DETAIL, trace_id=trace_id
        ).model_dump(),
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    """Constroi a aplicacao FastAPI com rotas, handlers, logging e telemetria."""
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description=DESCRIPTION,
        openapi_tags=OPENAPI_TAGS,
        lifespan=lifespan,
    )

    app.include_router(api_router, prefix=API_V1_PREFIX)
    # Health tambem na raiz: e o caminho que orquestradores e healthchecks de
    # container esperam, sem versionamento.
    app.include_router(health.router, tags=[TAG_HEALTH])

    app.add_exception_handler(PersistenceError, persistence_error_handler)
    app.add_exception_handler(Exception, unhandled_error_handler)

    # Antes do startup, enquanto a pilha de middlewares ainda aceita adicoes
    # (decisao D19).
    configure_telemetry(app, settings)

    return app


app = create_app()

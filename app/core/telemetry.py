"""Configuracao do OpenTelemetry: traces, metricas e instrumentacao.

Decisao D6: exportacao OTLP sobre gRPC na porta 4317 (padrao do exportador e
do Jaeger all-in-one com `COLLECTOR_OTLP_ENABLED=true`).

Decisao D15: telemetria degrada com elegancia. Toda a montagem do SDK esta
dentro de um `try/except` que apenas registra um aviso, e o
`BatchSpanProcessor` exporta em thread separada - ou seja, um collector
inacessivel nao derruba o startup nem falha uma requisicao. Com
`settings.otel_sdk_disabled=True` nenhum provider e instalado: e assim que a
suite de testes e as execucoes locais sem Jaeger rodam.

Decisao D17: os instrumentos de metrica existem sempre, mas a exportacao OTLP
de metricas e opcional (`OTEL_METRICS_ENABLED`, desligada por padrao). O
collector da stack e o Jaeger, que ingere apenas traces e responde
`UNIMPLEMENTED` ao servico OTLP de metricas - sem o interruptor o exportador
registraria um ERROR a cada intervalo, sem nenhum metrico chegar a lugar algum.

Decisao D19: a instrumentacao e dividida em dois momentos porque os dois
instrumentadores tem pre-requisitos diferentes:

* `configure_telemetry` instala os providers e o `FastAPIInstrumentor` e roda
  na **fabrica da aplicacao**. O instrumentador adiciona um middleware ASGI, e
  o Starlette congela a pilha de middlewares no primeiro `__call__` - que e o
  proprio evento de `lifespan`. Chamado de dentro do lifespan, ele levanta
  "Cannot add middleware after an application has started" e a aplicacao fica
  sem span de servidor: os spans explicitos viram traces orfaos de um span so.
* `instrument_database` depende do engine, que so existe no lifespan.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import (
    DEPLOYMENT_ENVIRONMENT,
    SERVICE_NAME,
    SERVICE_VERSION,
    Resource,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.config import Settings

if TYPE_CHECKING:  # pragma: no cover - apenas para anotacao de tipo
    # `app/api` e `app/main.py` sao as unicas partes do codigo que usam as
    # primitivas do FastAPI em tempo de execucao; aqui o tipo serve so para
    # anotar o parametro que sera entregue ao FastAPIInstrumentor.
    from fastapi import FastAPI

logger = logging.getLogger(__name__)

TRACE_ID_HEX_WIDTH = 32
"""Largura do `trace_id` em hexadecimal, igual a coluna `trace_id` das tabelas."""


def configure_telemetry(app: FastAPI, settings: Settings) -> None:
    """Instala provider de traces/metricas e instrumenta a aplicacao FastAPI.

    Precisa ser chamada na fabrica da aplicacao, antes do startup: o
    `FastAPIInstrumentor` adiciona um middleware e isso e impossivel depois que
    a pilha de middlewares foi montada (decisao D19).

    Nao levanta excecao em nenhuma circunstancia: qualquer falha de montagem
    do SDK ou de contato com o collector e registrada como aviso e a aplicacao
    segue servindo requisicoes sem telemetria (decisao D15).
    """
    if settings.otel_sdk_disabled:
        logger.info("OpenTelemetry desabilitado por configuracao (OTEL_SDK_DISABLED)")
        return

    try:
        resource = Resource.create(
            {
                SERVICE_NAME: settings.otel_service_name,
                SERVICE_VERSION: settings.app_version,
                DEPLOYMENT_ENVIRONMENT: settings.environment,
            }
        )

        tracer_provider = TracerProvider(resource=resource)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint)
            )
        )
        trace.set_tracer_provider(tracer_provider)

        if settings.otel_metrics_enabled:
            meter_provider = MeterProvider(
                resource=resource,
                metric_readers=[
                    PeriodicExportingMetricReader(
                        OTLPMetricExporter(
                            endpoint=settings.otel_exporter_otlp_endpoint
                        )
                    )
                ],
            )
            metrics.set_meter_provider(meter_provider)

        FastAPIInstrumentor.instrument_app(app)

        logger.info(
            "OpenTelemetry configurado",
            extra={
                "otel_endpoint": settings.otel_exporter_otlp_endpoint,
                "otel_service_name": settings.otel_service_name,
                "otel_metrics_enabled": settings.otel_metrics_enabled,
            },
        )
    except Exception as exc:  # noqa: BLE001 - telemetria nunca derruba o app
        logger.warning(
            "Falha ao configurar o OpenTelemetry; a aplicacao segue sem telemetria",
            extra={"otel_error": str(exc)},
        )


def instrument_database(engine: AsyncEngine, settings: Settings) -> None:
    """Instrumenta o engine SQLAlchemy; chamada no lifespan (decisao D19).

    Como `configure_telemetry`, nunca levanta excecao: sem instrumentacao de
    banco a aplicacao continua servindo requisicoes.
    """
    if settings.otel_sdk_disabled:
        return

    try:
        SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)
        logger.info("SQLAlchemy instrumentado")
    except Exception as exc:  # noqa: BLE001 - telemetria nunca derruba o app
        logger.warning(
            "Falha ao instrumentar o SQLAlchemy; a aplicacao segue sem esses spans",
            extra={"otel_error": str(exc)},
        )


def current_trace_id() -> str | None:
    """`trace_id` do span corrente em hexadecimal de 32 caracteres, ou `None`."""
    span_context = trace.get_current_span().get_span_context()
    if not span_context.is_valid:
        return None
    return format(span_context.trace_id, f"0{TRACE_ID_HEX_WIDTH}x")

"""Orquestracao da ingestao: adjudicar, persistir e resumir.

Eventos aprovados vao para `valid_events` com suas sanitizacoes; eventos
rejeitados vao para o dead letter `invalid_events` carregando o elemento
ORIGINAL do corpo da requisicao (decisao D13). Falhas de banco sobem como
`PersistenceError` (decisao D12) - nenhum evento e descartado em silencio.

Este modulo nao conhece FastAPI: ele recebe uma lista de dicts e devolve um
`IngestionSummary`.
"""

import logging
from typing import Any

from opentelemetry import trace
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import EventStatus
from app.domain.errors import PersistenceError
from app.domain.rules import ValidationResult, Violation
from app.domain.schemas import EventResult, IngestionSummary, ViolationOut
from app.infrastructure.repositories import InvalidEventRepository, ValidEventRepository
from app.services.data_quality import (
    EVENT_ID_FIELD,
    EVENT_TYPE_FIELD,
    PAYLOAD_FIELD,
    SOURCE_FIELD,
    DataQualityEngine,
    utcnow,
)

logger = logging.getLogger(__name__)

SPAN_PERSIST_VALID = "ingestion.persist_valid"
SPAN_PERSIST_INVALID = "ingestion.persist_invalid"
ATTR_EVENT_ID = "event.id"
ATTR_TABLE = "db.table"
TABLE_VALID = "valid_events"
TABLE_INVALID = "invalid_events"

_tracer = trace.get_tracer(__name__)


def _as_optional_str(value: Any) -> str | None:
    """Devolve a string quando o valor e uma string utilizavel, senao `None`."""
    return value if isinstance(value, str) and value else None


def _to_violation_out(violation: Violation) -> ViolationOut:
    """Converte uma violacao de dominio na sua forma de resposta da API."""
    return ViolationOut(
        code=violation.code,
        field=violation.field,
        message=violation.message,
        severity=violation.severity,
    )


class IngestionService:
    """Caso de uso de ingestao de eventos.

    Recebe o motor de Data Quality, os dois repositorios e a sessao; o commit
    acontece uma unica vez por requisicao, depois de todos os eventos.
    """

    def __init__(
        self,
        engine: DataQualityEngine,
        valid_repo: ValidEventRepository,
        invalid_repo: InvalidEventRepository,
        session: AsyncSession,
    ) -> None:
        self._engine = engine
        self._valid_repo = valid_repo
        self._invalid_repo = invalid_repo
        self._session = session

    async def ingest(
        self, items: list[dict[str, Any]], trace_id: str | None = None
    ) -> IngestionSummary:
        """Adjudica e persiste cada item, devolvendo o resumo da requisicao."""
        results: list[EventResult] = []
        reserved: list[str] = []
        accepted = 0
        rejected = 0

        try:
            for item in items:
                result = self._engine.validate(item)
                if result.status is EventStatus.SUCCESS:
                    reserved.extend(self._reserved_event_id(result))
                    await self._persist_valid(result, trace_id)
                    accepted += 1
                else:
                    await self._persist_invalid(item, result, trace_id)
                    rejected += 1
                results.append(self._to_event_result(item, result))
            await self._commit()
        except BaseException:
            # A transacao nao vingou: as reservas de event_id saem da janela
            # para que a retentativa nao volte como DUPLICATE_EVENT_ID (D10).
            self._engine.release_dedup(reserved)
            raise

        self._engine.confirm_dedup(reserved)
        return IngestionSummary(
            total=len(items),
            accepted=accepted,
            rejected=rejected,
            results=results,
        )

    # -- internals ---------------------------------------------------------
    async def _commit(self) -> None:
        """Confirma a transacao traduzindo falha em `PersistenceError` (D12).

        O commit e a ultima chamada de banco da requisicao e precisa estar
        sob o mesmo contrato dos repositorios: um flush que passa seguido de
        um commit que falha e a forma normal de uma conexao caindo no meio da
        requisicao, e isso tem de virar 503, nunca 500.
        """
        try:
            await self._session.commit()
        except SQLAlchemyError as exc:
            raise PersistenceError(
                "Falha ao confirmar a transacao da ingestao", cause=exc
            ) from exc

    @staticmethod
    def _reserved_event_id(result: ValidationResult) -> tuple[str, ...]:
        """`event_id` reservado na janela de deduplicacao por este resultado."""
        event_id = _as_optional_str(result.sanitized_event.get(EVENT_ID_FIELD))
        return (event_id,) if event_id is not None else ()

    async def _persist_valid(
        self, result: ValidationResult, trace_id: str | None
    ) -> None:
        """Grava o evento sanitizado e a lista de sanitizacoes aplicadas."""
        event = result.sanitized_event
        payload = event.get(PAYLOAD_FIELD)
        sanitizations = [item.to_dict() for item in result.sanitizations]
        with _tracer.start_as_current_span(SPAN_PERSIST_VALID) as span:
            span.set_attribute(ATTR_TABLE, TABLE_VALID)
            span.set_attribute(ATTR_EVENT_ID, str(event.get(EVENT_ID_FIELD)))
            await self._valid_repo.add(
                event_id=str(event[EVENT_ID_FIELD]),
                event_type=str(event[EVENT_TYPE_FIELD]),
                source=str(event[SOURCE_FIELD]),
                event_timestamp=result.parsed_timestamp or utcnow(),
                payload=payload if isinstance(payload, dict) else {},
                sanitizations=sanitizations or None,
                trace_id=trace_id,
            )

    async def _persist_invalid(
        self,
        original: dict[str, Any],
        result: ValidationResult,
        trace_id: str | None,
    ) -> None:
        """Grava a trilha de auditoria preservando o elemento original (D13)."""
        reasons = [item.to_dict() for item in result.violations]
        event_id = self._event_id_of(original, result)
        with _tracer.start_as_current_span(SPAN_PERSIST_INVALID) as span:
            span.set_attribute(ATTR_TABLE, TABLE_INVALID)
            span.set_attribute(ATTR_EVENT_ID, str(event_id))
            await self._invalid_repo.add(
                raw_payload=original,
                rejection_reasons=reasons,
                event_id=event_id,
                trace_id=trace_id,
            )

    def _to_event_result(
        self, original: dict[str, Any], result: ValidationResult
    ) -> EventResult:
        """Monta o desfecho individual que vai no corpo da resposta."""
        return EventResult(
            event_id=self._event_id_of(original, result),
            status=result.status,
            violations=[_to_violation_out(item) for item in result.violations],
            sanitizations=[_to_violation_out(item) for item in result.sanitizations],
        )

    @staticmethod
    def _event_id_of(original: dict[str, Any], result: ValidationResult) -> str | None:
        """Melhor `event_id` disponivel: o sanitizado e, na falta, o original."""
        sanitized = _as_optional_str(result.sanitized_event.get(EVENT_ID_FIELD))
        if sanitized is not None:
            return sanitized
        if isinstance(original, dict):
            return _as_optional_str(original.get(EVENT_ID_FIELD))
        return None

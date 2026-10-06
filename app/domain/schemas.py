"""Schemas Pydantic v2 de entrada e saida da API."""

from typing import Any

from pydantic import BaseModel, ConfigDict

from app.domain.enums import EventStatus, RejectionReason, Severity


class RawEventPayload(BaseModel):
    """Envelope permissivo de um evento recebido (decisao D3).

    Todos os campos sao opcionais e chaves desconhecidas sao preservadas em
    `model_extra`. Este modelo e validado DENTRO do motor, via
    `model_validate` protegido por try/except, e NUNCA e usado como anotacao
    de body no FastAPI: um body declarado devolveria 422 e o evento invalido
    jamais chegaria a tabela de auditoria.
    """

    model_config = ConfigDict(extra="allow")

    event_id: str | None = None
    event_type: str | None = None
    source: str | None = None
    timestamp: str | None = None
    payload: Any = None
    """Deliberadamente sem tipo: um `payload` que nao e objeto JSON e uma
    violacao de Data Quality com codigo proprio (`PAYLOAD_NOT_OBJECT`) e deve
    chegar ao dead letter, nao morrer aqui como envelope ilegivel."""


class ViolationOut(BaseModel):
    """Representacao publica de uma violacao ou sanitizacao."""

    code: RejectionReason
    field: str | None = None
    message: str
    severity: Severity


class EventResult(BaseModel):
    """Desfecho individual de um evento dentro da requisicao."""

    event_id: str | None = None
    status: EventStatus
    violations: list[ViolationOut] = []
    sanitizations: list[ViolationOut] = []


class IngestionSummary(BaseModel):
    """Resposta de `POST /api/v1/events` (sempre HTTP 202, decisao D4)."""

    total: int
    accepted: int
    rejected: int
    results: list[EventResult] = []


class HealthResponse(BaseModel):
    """Resposta dos endpoints de liveness e readiness."""

    status: str
    database: str | None = None


class ErrorResponse(BaseModel):
    """Corpo padrao de erro, com o trace correlacionado quando disponivel."""

    detail: str
    trace_id: str | None = None

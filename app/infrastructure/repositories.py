"""Repositorios de escrita das tabelas de eventos validos e invalidos.

Ambos traduzem `SQLAlchemyError` em `PersistenceError` (decisoes D12 e D13):
uma falha de gravacao nunca e engolida, nem na trilha de auditoria.
"""

from datetime import datetime
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.errors import PersistenceError
from app.domain.models import InvalidEvent, ValidEvent


class ValidEventRepository:
    """Grava eventos aprovados na tabela `valid_events`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(
        self,
        *,
        event_id: str,
        event_type: str,
        source: str,
        event_timestamp: datetime,
        payload: dict[str, Any],
        sanitizations: list[dict[str, Any]] | None = None,
        trace_id: str | None = None,
    ) -> ValidEvent:
        """Insere um evento valido e devolve a entidade com os defaults do banco.

        Levanta `PersistenceError` se o banco recusar a gravacao.
        """
        row = ValidEvent(
            event_id=event_id,
            event_type=event_type,
            source=source,
            event_timestamp=event_timestamp,
            payload=payload,
            sanitizations=sanitizations,
            trace_id=trace_id,
        )
        try:
            self._session.add(row)
            await self._session.flush()
            await self._session.refresh(row)
        except SQLAlchemyError as exc:
            raise PersistenceError(
                "Falha ao gravar o evento em valid_events", cause=exc
            ) from exc
        return row


class InvalidEventRepository:
    """Grava eventos rejeitados na tabela de dead letter `invalid_events`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(
        self,
        *,
        raw_payload: Any,
        rejection_reasons: list[dict[str, Any]],
        event_id: str | None = None,
        trace_id: str | None = None,
    ) -> InvalidEvent:
        """Insere a linha de auditoria preservando `raw_payload` intacto.

        Levanta `PersistenceError` se o banco recusar a gravacao: nunca
        confirmamos uma requisicao cuja trilha de auditoria foi perdida.
        """
        row = InvalidEvent(
            event_id=event_id,
            raw_payload=raw_payload,
            rejection_reasons=rejection_reasons,
            trace_id=trace_id,
        )
        try:
            self._session.add(row)
            await self._session.flush()
            await self._session.refresh(row)
        except SQLAlchemyError as exc:
            raise PersistenceError(
                "Falha ao gravar o evento em invalid_events", cause=exc
            ) from exc
        return row

"""Testes do servico de ingestao: roteamento valido / dead letter."""

from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.domain.enums import EventStatus
from app.domain.errors import PersistenceError
from app.domain.models import InvalidEvent, ValidEvent
from app.infrastructure.repositories import InvalidEventRepository, ValidEventRepository
from app.services.data_quality import DataQualityEngine, utcnow
from app.services.ingestion import IngestionService

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"


def _event(**overrides: Any) -> dict[str, Any]:
    """Envelope valido, com os campos sobrescritos pelo teste."""
    event: dict[str, Any] = {
        "event_id": str(uuid4()),
        "event_type": "sensor.reading",
        "source": "gateway-01",
        "timestamp": utcnow().isoformat(),
        "payload": {"temperature": 21.5, "humidity": 48.0, "value": 3},
    }
    event.update(overrides)
    return event


@pytest.fixture
def service(settings: Settings, session: AsyncSession) -> IngestionService:
    """Servico de ingestao ligado ao banco aiosqlite em memoria."""
    return IngestionService(
        engine=DataQualityEngine(settings),
        valid_repo=ValidEventRepository(session),
        invalid_repo=InvalidEventRepository(session),
        session=session,
    )


async def test_valid_event_is_persisted_with_sanitizations(
    service: IngestionService, session: AsyncSession, settings: Settings
) -> None:
    """Caminho de SANITIZACAO: 150 grava 60.0 em `valid_events` com o registro."""
    original = _event(payload={"temperature": 150.0, "value": 2})

    summary = await service.ingest([original], trace_id=TRACE_ID)

    assert summary.total == 1
    assert summary.accepted == 1
    assert summary.rejected == 0
    assert summary.results[0].status is EventStatus.SUCCESS

    stored = (await session.execute(select(ValidEvent))).scalar_one()
    assert stored.payload["temperature"] == 60.0
    assert stored.payload["temperature"] == settings.temperature_max
    assert stored.event_id == original["event_id"]
    assert stored.trace_id == TRACE_ID
    assert stored.sanitizations is not None
    assert [item["code"] for item in stored.sanitizations] == [
        "TEMPERATURE_OUT_OF_RANGE"
    ]
    assert stored.sanitizations[0]["original"] == 150.0
    assert stored.sanitizations[0]["applied"] == settings.temperature_max

    assert (await session.execute(select(func.count(InvalidEvent.id)))).scalar() == 0


async def test_clean_event_stores_no_sanitizations(
    service: IngestionService, session: AsyncSession
) -> None:
    """Sem sanitizacao aplicada, a coluna fica nula."""
    await service.ingest([_event()], trace_id=None)

    stored = (await session.execute(select(ValidEvent))).scalar_one()
    assert stored.sanitizations is None
    assert stored.trace_id is None


async def test_rejected_event_lands_in_dead_letter_with_original_body(
    service: IngestionService, session: AsyncSession
) -> None:
    """Caminho de REJEICAO: `value: -1` vai para `invalid_events` com o corpo original."""
    original = _event(payload={"value": -1, "temperature": 21.5})
    snapshot = {key: value for key, value in original.items()}

    summary = await service.ingest([original], trace_id=TRACE_ID)

    assert summary.accepted == 0
    assert summary.rejected == 1
    assert summary.results[0].status is EventStatus.REJECTED

    stored = (await session.execute(select(InvalidEvent))).scalar_one()
    assert stored.raw_payload == snapshot
    assert stored.event_id == original["event_id"]
    assert stored.trace_id == TRACE_ID
    assert [item["code"] for item in stored.rejection_reasons] == ["NEGATIVE_VALUE"]
    assert stored.rejection_reasons[0]["severity"] == "ERROR"
    assert stored.rejection_reasons[0]["field"] == "payload.value"

    assert (await session.execute(select(func.count(ValidEvent.id)))).scalar() == 0


async def test_dead_letter_preserves_unsanitized_payload(
    service: IngestionService, session: AsyncSession
) -> None:
    """O dead letter nunca guarda a versao sanitizada do payload (D13)."""
    original = _event(payload={"temperature": 150.0, "value": -1})

    await service.ingest([original], trace_id=None)

    stored = (await session.execute(select(InvalidEvent))).scalar_one()
    assert stored.raw_payload["payload"]["temperature"] == 150.0


async def test_mixed_batch_counts_each_outcome(
    service: IngestionService, session: AsyncSession
) -> None:
    """Lote misto: contagens e desfechos por evento no resumo."""
    accepted = _event()
    sanitized = _event(payload={"humidity": -5.0})
    rejected = _event(payload={"value": -1})
    broken = _event()
    del broken["source"]

    summary = await service.ingest(
        [accepted, sanitized, rejected, broken], trace_id=TRACE_ID
    )

    assert summary.total == 4
    assert summary.accepted == 2
    assert summary.rejected == 2
    assert [result.status for result in summary.results] == [
        EventStatus.SUCCESS,
        EventStatus.SUCCESS,
        EventStatus.REJECTED,
        EventStatus.REJECTED,
    ]
    assert [result.event_id for result in summary.results] == [
        accepted["event_id"],
        sanitized["event_id"],
        rejected["event_id"],
        broken["event_id"],
    ]
    assert summary.results[1].sanitizations[0].code.value == "HUMIDITY_OUT_OF_RANGE"
    assert summary.results[2].violations[0].code.value == "NEGATIVE_VALUE"

    assert (await session.execute(select(func.count(ValidEvent.id)))).scalar() == 2
    assert (await session.execute(select(func.count(InvalidEvent.id)))).scalar() == 2


async def test_persistence_error_propagates_from_valid_path(
    service: IngestionService,
    session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Falha de banco no caminho valido sobe como `PersistenceError` (D12)."""

    async def _boom() -> None:
        raise SQLAlchemyError("flush falhou")

    monkeypatch.setattr(session, "flush", _boom)

    with pytest.raises(PersistenceError):
        await service.ingest([_event()], trace_id=None)


async def test_persistence_error_propagates_from_dead_letter_path(
    service: IngestionService,
    session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Falha ao gravar a auditoria tambem sobe: nao confirmamos o que nao gravamos (D13)."""

    async def _boom() -> None:
        raise SQLAlchemyError("flush falhou")

    monkeypatch.setattr(session, "flush", _boom)

    with pytest.raises(PersistenceError):
        await service.ingest([_event(payload={"value": -1})], trace_id=None)

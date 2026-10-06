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


async def test_commit_failure_becomes_a_persistence_error(
    service: IngestionService,
    session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """O commit esta sob o mesmo contrato dos repositorios (D12).

    Um flush que passa seguido de um commit que falha e a forma normal de uma
    conexao caindo no meio da requisicao: precisa virar `PersistenceError` -
    e portanto 503 - e nunca escapar como erro inesperado.
    """

    async def _boom() -> None:
        raise SQLAlchemyError("commit falhou")

    monkeypatch.setattr(session, "commit", _boom)

    with pytest.raises(PersistenceError):
        await service.ingest([_event()], trace_id=None)


async def test_event_id_is_reusable_after_a_failed_transaction(
    service: IngestionService,
    session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retentativa de um lote que tomou 503 nao volta como DUPLICATE_EVENT_ID."""
    event = _event()

    async def _boom() -> None:
        raise SQLAlchemyError("commit falhou")

    monkeypatch.setattr(session, "commit", _boom)
    with pytest.raises(PersistenceError):
        await service.ingest([event], trace_id=None)

    monkeypatch.undo()
    await session.rollback()

    summary = await service.ingest([event], trace_id=None)

    assert summary.accepted == 1
    assert summary.results[0].status is EventStatus.SUCCESS
    stored = (await session.execute(select(ValidEvent))).scalar_one()
    assert stored.event_id == event["event_id"]


async def test_duplicate_inside_the_same_batch_is_still_rejected(
    service: IngestionService, session: AsyncSession
) -> None:
    """A reserva vale dentro do lote: a segunda copia vai para o dead letter."""
    event = _event()

    summary = await service.ingest([event, dict(event)], trace_id=None)

    assert summary.accepted == 1
    assert summary.rejected == 1
    assert [item.code.value for item in summary.results[1].violations] == [
        "DUPLICATE_EVENT_ID"
    ]


async def test_committed_event_id_is_rejected_on_a_later_request(
    service: IngestionService, session: AsyncSession
) -> None:
    """Depois do commit o `event_id` esta consumido: reenvio e duplicado."""
    event = _event()

    await service.ingest([event], trace_id=None)
    summary = await service.ingest([event], trace_id=None)

    assert summary.rejected == 1
    assert [item.code.value for item in summary.results[0].violations] == [
        "DUPLICATE_EVENT_ID"
    ]


async def test_long_envelope_strings_fit_the_columns(
    service: IngestionService, session: AsyncSession, settings: Settings
) -> None:
    """O truncamento do envelope mantem os valores dentro de `String(128)`."""
    limit = settings.max_envelope_string_length
    event = _event(event_type="t" * (limit + 50), source="s" * (limit + 50))

    summary = await service.ingest([event], trace_id=None)

    assert summary.accepted == 1
    stored = (await session.execute(select(ValidEvent))).scalar_one()
    assert len(stored.event_type) == limit
    assert len(stored.source) == limit

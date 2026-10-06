"""Testes dos repositorios sobre sqlite+aiosqlite em memoria."""

from datetime import datetime, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.errors import PersistenceError
from app.domain.models import InvalidEvent, ValidEvent
from app.infrastructure.repositories import InvalidEventRepository, ValidEventRepository

EVENT_TIMESTAMP = datetime(2024, 5, 17, 12, 30, tzinfo=timezone.utc)


async def test_valid_event_round_trip(session: AsyncSession) -> None:
    """Payload (dict) e sanitizations (lista de dicts) voltam do banco iguais."""
    repo = ValidEventRepository(session)
    payload = {"temperature": 21.5, "humidity": 48.0, "label": "sensor-a"}
    sanitizations = [
        {
            "code": "TEMPERATURE_OUT_OF_RANGE",
            "field": "payload.temperature",
            "message": "clampado",
            "severity": "WARNING",
            "original": 150.0,
            "applied": 60.0,
        }
    ]

    row = await repo.add(
        event_id="evt-1",
        event_type="sensor.reading",
        source="gateway-01",
        event_timestamp=EVENT_TIMESTAMP,
        payload=payload,
        sanitizations=sanitizations,
        trace_id="0af7651916cd43dd8448eb211c80319c",
    )
    await session.commit()

    stored = (
        await session.execute(select(ValidEvent).where(ValidEvent.id == row.id))
    ).scalar_one()

    assert stored.payload == payload
    assert stored.sanitizations == sanitizations
    assert stored.event_id == "evt-1"
    assert stored.trace_id == "0af7651916cd43dd8448eb211c80319c"
    assert stored.ingested_at is not None


async def test_valid_event_accepts_absent_sanitizations(session: AsyncSession) -> None:
    """Um evento sem sanitizacoes grava `sanitizations` nulo."""
    repo = ValidEventRepository(session)

    row = await repo.add(
        event_id="evt-2",
        event_type="sensor.reading",
        source="gateway-01",
        event_timestamp=EVENT_TIMESTAMP,
        payload={"value": 1},
    )
    await session.commit()

    assert row.sanitizations is None
    assert row.ingested_at is not None


async def test_invalid_event_preserves_raw_payload(session: AsyncSession) -> None:
    """O dead letter guarda o elemento original, sem nenhuma sanitizacao."""
    repo = InvalidEventRepository(session)
    original = {
        "event_id": "evt-3",
        "event_type": " sensor.reading ",
        "payload": {"value": -1},
        "tenant": "acme",
    }
    reasons = [
        {
            "code": "NEGATIVE_VALUE",
            "field": "payload.value",
            "message": "valor negativo",
            "severity": "ERROR",
            "original": -1,
            "applied": None,
        },
        {
            "code": "MISSING_REQUIRED_FIELD",
            "field": "source",
            "message": "campo obrigatorio ausente",
            "severity": "ERROR",
            "original": None,
            "applied": None,
        },
    ]

    row = await repo.add(
        raw_payload=original,
        rejection_reasons=reasons,
        event_id="evt-3",
    )
    await session.commit()

    stored = (
        await session.execute(select(InvalidEvent).where(InvalidEvent.id == row.id))
    ).scalar_one()

    assert stored.raw_payload == original
    assert stored.rejection_reasons == reasons
    assert stored.event_id == "evt-3"
    assert stored.rejected_at is not None


async def test_invalid_event_accepts_non_object_raw_payload(
    session: AsyncSession,
) -> None:
    """Elementos que nao sao objeto tambem viram linha de auditoria."""
    repo = InvalidEventRepository(session)

    row = await repo.add(
        raw_payload="nao-sou-um-objeto",
        rejection_reasons=[
            {
                "code": "UNPARSEABLE_ENVELOPE",
                "field": None,
                "message": "elemento nao e um objeto JSON",
                "severity": "ERROR",
                "original": None,
                "applied": None,
            }
        ],
    )
    await session.commit()

    assert row.raw_payload == "nao-sou-um-objeto"
    assert row.event_id is None


async def test_valid_repository_wraps_sqlalchemy_error(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Uma falha do SQLAlchemy sobe como `PersistenceError` (decisao D12)."""

    async def _boom() -> None:
        raise SQLAlchemyError("flush falhou")

    monkeypatch.setattr(session, "flush", _boom)
    repo = ValidEventRepository(session)

    with pytest.raises(PersistenceError) as excinfo:
        await repo.add(
            event_id="evt-4",
            event_type="sensor.reading",
            source="gateway-01",
            event_timestamp=EVENT_TIMESTAMP,
            payload={"value": 1},
        )

    assert isinstance(excinfo.value.cause, SQLAlchemyError)


async def test_invalid_repository_wraps_sqlalchemy_error(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Falha ao gravar a trilha de auditoria tambem sobe como erro (D13)."""

    async def _boom() -> None:
        raise SQLAlchemyError("flush falhou")

    monkeypatch.setattr(session, "flush", _boom)
    repo = InvalidEventRepository(session)

    with pytest.raises(PersistenceError) as excinfo:
        await repo.add(raw_payload={"a": 1}, rejection_reasons=[])

    assert isinstance(excinfo.value.cause, SQLAlchemyError)

"""Testes de ponta a ponta da API sobre `httpx.ASGITransport`.

O teste mais importante deste modulo e
`test_business_invalid_payload_never_returns_422`: ele guarda a decisao D2.
Se alguem trocar o `await request.json()` manual por um modelo Pydantic de
body, o FastAPI passa a devolver 422 e o evento invalido nunca chega ao dead
letter - o produto perde exatamente a funcionalidade que justifica o servico.
"""

import json
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI, Request
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1 import health as health_module
from app.core.config import Settings, get_settings
from app.domain.errors import PersistenceError
from app.domain.models import InvalidEvent, ValidEvent
from app.infrastructure.repositories import InvalidEventRepository, ValidEventRepository
from app.main import unhandled_error_handler
from app.services.data_quality import utcnow

EVENTS_URL = "/api/v1/events"
HEALTH_URL = "/health"
READY_URL = "/health/ready"
V1_HEALTH_URL = "/api/v1/health"

HTTP_ACCEPTED = 202
HTTP_BAD_REQUEST = 400
HTTP_OK = 200
HTTP_PAYLOAD_TOO_LARGE = 413
HTTP_UNPROCESSABLE = 422
HTTP_INTERNAL_ERROR = 500
HTTP_UNAVAILABLE = 503


def _event(**overrides: Any) -> dict[str, Any]:
    """Envelope valido (event_id em UUID, sem sanitizacao), com sobrescritas."""
    event: dict[str, Any] = {
        "event_id": str(uuid4()),
        "event_type": "sensor.reading",
        "source": "gateway-01",
        "timestamp": utcnow().isoformat(),
        "payload": {"temperature": 21.5, "humidity": 48.0, "value": 3},
    }
    event.update(overrides)
    return event


def _codes(entries: list[dict[str, Any]]) -> list[str]:
    """Codigos de uma lista de violacoes/sanitizacoes do corpo da resposta."""
    return [entry["code"] for entry in entries]


async def test_single_valid_event_is_accepted_and_persisted(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """Um evento limpo responde 202 com accepted=1 e grava em `valid_events`."""
    event = _event()

    response = await client.post(EVENTS_URL, json=event)

    assert response.status_code == HTTP_ACCEPTED
    body = response.json()
    assert body["total"] == 1
    assert body["accepted"] == 1
    assert body["rejected"] == 0
    assert body["results"][0]["event_id"] == event["event_id"]
    assert body["results"][0]["status"] == "SUCCESS"
    assert body["results"][0]["violations"] == []
    assert body["results"][0]["sanitizations"] == []

    stored = (await session.execute(select(ValidEvent))).scalar_one()
    assert stored.event_id == event["event_id"]
    assert stored.event_type == "sensor.reading"
    assert stored.payload["temperature"] == 21.5


async def test_mixed_batch_reports_every_outcome(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """Lote misto: 202 com contagens certas e desfecho por evento."""
    valid = _event()
    sanitized = _event(payload={"temperature": 150.0})
    schema_rejected = _event()
    del schema_rejected["source"]
    quality_rejected = _event(payload={"value": -1})

    response = await client.post(
        EVENTS_URL, json=[valid, sanitized, schema_rejected, quality_rejected]
    )

    assert response.status_code == HTTP_ACCEPTED
    body = response.json()
    assert body["total"] == 4
    assert body["accepted"] == 2
    assert body["rejected"] == 2

    results = body["results"]
    assert [item["status"] for item in results] == [
        "SUCCESS",
        "SUCCESS",
        "REJECTED",
        "REJECTED",
    ]
    assert [item["event_id"] for item in results] == [
        valid["event_id"],
        sanitized["event_id"],
        schema_rejected["event_id"],
        quality_rejected["event_id"],
    ]
    assert _codes(results[1]["sanitizations"]) == ["TEMPERATURE_OUT_OF_RANGE"]
    assert _codes(results[2]["violations"]) == ["MISSING_REQUIRED_FIELD"]
    assert _codes(results[3]["violations"]) == ["NEGATIVE_VALUE"]

    assert (await session.execute(select(func.count(ValidEvent.id)))).scalar() == 2
    assert (await session.execute(select(func.count(InvalidEvent.id)))).scalar() == 2


async def test_schema_rejected_event_is_dead_lettered_with_original_body(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """Falta de campo obrigatorio: 202 e linha em `invalid_events` com o corpo original."""
    event = _event()
    del event["source"]
    snapshot = dict(event)

    response = await client.post(EVENTS_URL, json=event)

    assert response.status_code == HTTP_ACCEPTED
    assert response.json()["rejected"] == 1

    stored = (await session.execute(select(InvalidEvent))).scalar_one()
    assert stored.raw_payload == snapshot
    assert stored.event_id == event["event_id"]
    assert _codes(stored.rejection_reasons) == ["MISSING_REQUIRED_FIELD"]
    assert stored.rejection_reasons[0]["severity"] == "ERROR"

    assert (await session.execute(select(func.count(ValidEvent.id)))).scalar() == 0


async def test_business_invalid_payload_never_returns_422(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """DECISAO D2: payload invalido de negocio e 202 + dead letter, nunca 422.

    Um body Pydantic declarado na rota devolveria 422 aqui e o evento seria
    perdido; por isso a rota parseia `request.json()` a mao.
    """
    bodies: list[dict[str, Any]] = [
        {},
        {"event_id": str(uuid4())},
        {"event_type": "sensor.reading", "source": "gateway-01"},
        _event(timestamp="nao-e-uma-data"),
        _event(payload="nao-e-um-objeto"),
        _event(event_id=""),
    ]

    for body in bodies:
        response = await client.post(EVENTS_URL, json=body)
        assert response.status_code != HTTP_UNPROCESSABLE, body
        assert response.status_code == HTTP_ACCEPTED, body
        assert response.json()["rejected"] == 1, body

    dead_lettered = (
        await session.execute(select(func.count(InvalidEvent.id)))
    ).scalar()
    assert dead_lettered == len(bodies)


async def test_sanitized_event_is_accepted_with_clamped_payload(
    client: httpx.AsyncClient, session: AsyncSession, settings: Settings
) -> None:
    """Caminho de SANITIZACAO ponta a ponta: 150 vira 60.0 em `valid_events`."""
    event = _event(payload={"temperature": 150.0, "humidity": -5.0, "value": 1})

    response = await client.post(EVENTS_URL, json=event)

    assert response.status_code == HTTP_ACCEPTED
    result = response.json()["results"][0]
    assert result["status"] == "SUCCESS"
    assert sorted(_codes(result["sanitizations"])) == [
        "HUMIDITY_OUT_OF_RANGE",
        "TEMPERATURE_OUT_OF_RANGE",
    ]

    stored = (await session.execute(select(ValidEvent))).scalar_one()
    assert stored.payload["temperature"] == settings.temperature_max
    assert stored.payload["humidity"] == settings.humidity_min
    assert stored.sanitizations is not None


async def test_non_json_body_is_rejected_with_400(client: httpx.AsyncClient) -> None:
    """Corpo ilegivel e erro de TRANSPORTE: 400, nao 202."""
    response = await client.post(
        EVENTS_URL,
        content=b"isto nao e json {",
        headers={"content-type": "application/json"},
    )

    assert response.status_code == HTTP_BAD_REQUEST
    assert "detail" in response.json()


@pytest.mark.parametrize("body", ["apenas-uma-string", 42, True, None])
async def test_top_level_scalar_is_rejected_with_400(
    client: httpx.AsyncClient, body: Any
) -> None:
    """JSON escalar no topo nao e um evento nem um lote: 400."""
    response = await client.post(EVENTS_URL, json=body)

    assert response.status_code == HTTP_BAD_REQUEST


async def test_non_object_array_element_is_dead_lettered_not_400(
    client: httpx.AsyncClient, session: AsyncSession
) -> None:
    """Elemento nao-objeto dentro do array vai para o dead letter, nao gera 400."""
    valid = _event()

    response = await client.post(EVENTS_URL, json=[valid, "nao-sou-um-objeto"])

    assert response.status_code == HTTP_ACCEPTED
    body = response.json()
    assert body["total"] == 2
    assert body["accepted"] == 1
    assert body["rejected"] == 1
    assert body["results"][1]["status"] == "REJECTED"
    assert _codes(body["results"][1]["violations"]) == ["UNPARSEABLE_ENVELOPE"]

    stored = (await session.execute(select(InvalidEvent))).scalar_one()
    assert stored.raw_payload == "nao-sou-um-objeto"
    assert stored.event_id is None


async def test_health_is_live_without_touching_the_database(
    client: httpx.AsyncClient,
) -> None:
    """Liveness responde 200 sem consultar o banco."""
    response = await client.get(HEALTH_URL)

    assert response.status_code == HTTP_OK
    assert response.json() == {"status": "ok", "database": None}


async def test_health_is_also_mounted_under_the_v1_prefix(
    client: httpx.AsyncClient,
) -> None:
    """O mesmo liveness tambem responde sob `/api/v1` (documentado no README)."""
    response = await client.get(V1_HEALTH_URL)

    assert response.status_code == HTTP_OK
    assert response.json()["status"] == "ok"


async def test_readiness_reports_the_database_as_ok(
    client: httpx.AsyncClient,
) -> None:
    """Readiness executa `SELECT 1` no banco de teste e responde 200."""
    response = await client.get(READY_URL)

    assert response.status_code == HTTP_OK
    assert response.json() == {"status": "ok", "database": "ok"}


async def test_readiness_reports_503_when_the_database_is_down(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`SELECT 1` falhando faz o readiness responder 503 com `ErrorResponse`."""

    async def _boom(session: AsyncSession) -> None:
        raise SQLAlchemyError("banco fora do ar")

    monkeypatch.setattr(health_module, "ping", _boom)

    response = await client.get(READY_URL)

    assert response.status_code == HTTP_UNAVAILABLE
    body = response.json()
    assert body["detail"] == health_module.DATABASE_UNAVAILABLE_DETAIL
    assert "trace_id" in body


# --------------------------------------------------------------------------
# Falhas de persistencia: 503, nunca 500 e nunca silencio (D12/D13)
# --------------------------------------------------------------------------
async def test_persistence_error_on_the_valid_path_returns_503(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Falha ao gravar em `valid_events`: 503 com detalhe e `trace_id`."""

    async def _boom(*args: Any, **kwargs: Any) -> None:
        raise PersistenceError("Falha ao gravar o evento em valid_events")

    monkeypatch.setattr(ValidEventRepository, "add", _boom)

    response = await client.post(EVENTS_URL, json=_event())

    assert response.status_code == HTTP_UNAVAILABLE
    body = response.json()
    assert body["detail"]
    assert "trace_id" in body


async def test_persistence_error_on_the_dead_letter_path_returns_503(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Falha ao gravar a auditoria tambem e 503: nao confirmamos o que nao gravamos."""

    async def _boom(*args: Any, **kwargs: Any) -> None:
        raise PersistenceError("Falha ao gravar o evento em invalid_events")

    monkeypatch.setattr(InvalidEventRepository, "add", _boom)

    response = await client.post(EVENTS_URL, json=_event(payload={"value": -1}))

    assert response.status_code == HTTP_UNAVAILABLE
    assert response.json()["detail"]


async def test_commit_failure_returns_503_not_500(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Flush que passa e commit que falha e 503: o commit tambem e `PersistenceError`."""

    async def _boom(self: AsyncSession) -> None:
        raise SQLAlchemyError("commit falhou")

    monkeypatch.setattr(AsyncSession, "commit", _boom)

    response = await client.post(EVENTS_URL, json=_event())

    assert response.status_code == HTTP_UNAVAILABLE
    body = response.json()
    assert body["detail"]
    assert "trace_id" in body


async def test_unhandled_exception_is_answered_with_500_and_trace_id() -> None:
    """O handler de ultima instancia responde 500 com `ErrorResponse`.

    Chamado direto porque o `ServerErrorMiddleware` do Starlette re-levanta a
    excecao depois de enviar a resposta, e o `ASGITransport` a propagaria para
    o teste em vez de devolver um `Response`.
    """
    scope: dict[str, Any] = {
        "type": "http",
        "method": "POST",
        "path": EVENTS_URL,
        "query_string": b"",
        "headers": [],
        "scheme": "http",
        "server": ("test", 80),
        "root_path": "",
    }

    response = await unhandled_error_handler(
        Request(scope), RuntimeError("falha inesperada")
    )

    assert response.status_code == HTTP_INTERNAL_ERROR
    body = json.loads(bytes(response.body))
    assert body["detail"]
    assert "trace_id" in body


# --------------------------------------------------------------------------
# Limite de tamanho do lote
# --------------------------------------------------------------------------
async def test_batch_above_the_limit_is_rejected_before_any_write(
    client: httpx.AsyncClient,
    app: FastAPI,
    settings: Settings,
    session: AsyncSession,
) -> None:
    """Array maior que `MAX_BATCH_SIZE`: 413 e nenhuma linha gravada."""
    app.dependency_overrides[get_settings] = lambda: settings.model_copy(
        update={"max_batch_size": 2}
    )

    response = await client.post(EVENTS_URL, json=[_event(), _event(), _event()])

    assert response.status_code == HTTP_PAYLOAD_TOO_LARGE
    assert "2" in response.json()["detail"]
    assert (await session.execute(select(func.count(ValidEvent.id)))).scalar() == 0


async def test_batch_at_the_limit_is_accepted(
    client: httpx.AsyncClient, app: FastAPI, settings: Settings
) -> None:
    """Exatamente `MAX_BATCH_SIZE` elementos ainda passa."""
    app.dependency_overrides[get_settings] = lambda: settings.model_copy(
        update={"max_batch_size": 2}
    )

    response = await client.post(EVENTS_URL, json=[_event(), _event()])

    assert response.status_code == HTTP_ACCEPTED
    assert response.json()["accepted"] == 2

"""Endpoint de ingestao de eventos.

Decisao D1: um unico endpoint aceita um objeto JSON OU um array de objetos;
nao existe `/batch` irmao - um lote e simplesmente `len(items) > 1`.

Decisao D2: a rota recebe `Request` e faz `await request.json()` a MAO. Ela
deliberadamente NAO declara um modelo Pydantic de body, porque um body
declarado - mesmo permissivo - devolveria um 422 do proprio FastAPI para um
payload invalido do ponto de vista de negocio, e o evento nunca chegaria a
tabela de auditoria `invalid_events`. Os unicos 400 possiveis sao de
transporte: corpo que nao e JSON e JSON escalar no topo. Um elemento do array
que nao e objeto NAO e 400: ele vai para o dead letter.

Decisao D4: toda requisicao processada responde HTTP 202 Accepted, inclusive
um lote 100% rejeitado - o desfecho de cada evento esta no corpo da resposta.
"""

import logging
from typing import Any, cast

from fastapi import APIRouter, HTTPException, Request, status

from app.api.deps import IngestionServiceDep, SettingsDep
from app.core.telemetry import current_trace_id
from app.domain.schemas import ErrorResponse, IngestionSummary

logger = logging.getLogger(__name__)

router = APIRouter()

INVALID_JSON_DETAIL = "Corpo da requisicao nao e um JSON valido."
INVALID_ROOT_DETAIL = (
    "O corpo deve ser um objeto JSON ou um array de objetos JSON."
)
BATCH_TOO_LARGE_DETAIL = (
    "Lote acima do maximo permitido de {limit} eventos por requisicao."
)

_ACCEPTED_EXAMPLE: dict[str, Any] = {
    "total": 2,
    "accepted": 1,
    "rejected": 1,
    "results": [
        {
            "event_id": "7f1f1a3e-6a5a-4d6f-9b1e-2c0a9d4e5f60",
            "status": "SUCCESS",
            "violations": [],
            "sanitizations": [
                {
                    "code": "TEMPERATURE_OUT_OF_RANGE",
                    "field": "payload.temperature",
                    "message": "temperature fora da faixa [-90.0, 60.0]; clampado.",
                    "severity": "WARNING",
                }
            ],
        },
        {
            "event_id": None,
            "status": "REJECTED",
            "violations": [
                {
                    "code": "MISSING_REQUIRED_FIELD",
                    "field": "event_id",
                    "message": "Campos obrigatorios ausentes ou nulos: event_id.",
                    "severity": "ERROR",
                }
            ],
            "sanitizations": [],
        },
    ],
}


@router.post(
    "/events",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=IngestionSummary,
    summary="Recebe um evento ou um lote de eventos",
    responses={
        status.HTTP_202_ACCEPTED: {
            "description": (
                "Corpo processado e adjudicado. Sempre 202, mesmo quando todos "
                "os eventos foram rejeitados (decisao D4)."
            ),
            "content": {"application/json": {"example": _ACCEPTED_EXAMPLE}},
        },
        status.HTTP_400_BAD_REQUEST: {
            "model": ErrorResponse,
            "description": (
                "Erro de transporte: corpo ilegivel como JSON ou JSON escalar "
                "no topo. Payload invalido de negocio NAO cai aqui - ele e "
                "aceito com 202 e registrado em invalid_events."
            ),
        },
        status.HTTP_413_REQUEST_ENTITY_TOO_LARGE: {
            "model": ErrorResponse,
            "description": (
                "O array enviado tem mais elementos que MAX_BATCH_SIZE; nada "
                "foi adjudicado nem gravado."
            ),
        },
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "Falha de persistencia: nenhum evento foi descartado.",
        },
    },
)
async def ingest_events(
    request: Request, service: IngestionServiceDep, settings: SettingsDep
) -> IngestionSummary:
    """Valida e persiste um evento ou um lote, devolvendo o desfecho de cada um.

    Eventos aprovados (eventualmente sanitizados) vao para `valid_events`;
    eventos rejeitados vao para o dead letter `invalid_events` com a razao da
    rejeicao.
    """
    try:
        body = await request.json()
    except Exception as exc:  # noqa: BLE001 - qualquer falha de decode e 400
        logger.warning("Corpo da requisicao ilegivel", extra={"parse_error": str(exc)})
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=INVALID_JSON_DETAIL
        ) from exc

    items = _normalise(body)
    _check_batch_size(items, settings.max_batch_size)
    # Um elemento que nao e objeto segue de proposito para o motor: ele sera
    # rejeitado como UNPARSEABLE_ENVELOPE e preservado no dead letter.
    return await service.ingest(
        cast(list[dict[str, Any]], items), trace_id=current_trace_id()
    )


def _normalise(body: Any) -> list[Any]:
    """Normaliza o corpo em uma lista de elementos a adjudicar.

    Um objeto JSON vira uma lista de um elemento, um array e usado como esta e
    qualquer escalar no topo (string, numero, booleano, `null`) e 400.
    """
    if isinstance(body, dict):
        return [body]
    if isinstance(body, list):
        return body
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail=INVALID_ROOT_DETAIL
    )


def _check_batch_size(items: list[Any], limit: int) -> None:
    """Recusa um lote maior que `MAX_BATCH_SIZE` antes de qualquer adjudicacao.

    O lote inteiro e validado e gravado em uma unica transacao, com todos os
    resultados em memoria: um array sem limite seria um vetor de exaustao de
    memoria em um endpoint exposto na rede.
    """
    if len(items) <= limit:
        return
    detail = BATCH_TOO_LARGE_DETAIL.format(limit=limit)
    logger.warning("Lote recusado por tamanho", extra={"batch_size": len(items)})
    raise HTTPException(
        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail=detail
    )

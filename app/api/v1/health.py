"""Endpoints de liveness e readiness.

`GET /health` nao toca o banco: ele responde se o processo esta de pe.
`GET /health/ready` faz um `SELECT 1` e responde 503 quando o banco nao
responde - e esse o sinal que um orquestrador usa para nao enviar trafego.
"""

import logging

from fastapi import APIRouter, Response, status
from fastapi.responses import JSONResponse

from app.api.deps import SessionDep
from app.core.telemetry import current_trace_id
from app.domain.schemas import ErrorResponse, HealthResponse
from app.infrastructure.database import ping

logger = logging.getLogger(__name__)

router = APIRouter()

STATUS_OK = "ok"
DATABASE_UNAVAILABLE_DETAIL = "Banco de dados indisponivel."


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness: o processo esta de pe",
)
async def health() -> HealthResponse:
    """Responde 200 sem acessar o banco."""
    return HealthResponse(status=STATUS_OK)


@router.get(
    "/health/ready",
    response_model=HealthResponse,
    summary="Readiness: o processo esta de pe e o banco responde",
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "O banco nao respondeu ao SELECT 1.",
        }
    },
)
async def readiness(session: SessionDep) -> Response:
    """Responde 200 quando o `SELECT 1` passa e 503 quando ele falha."""
    try:
        await ping(session)
    except Exception as exc:  # noqa: BLE001 - readiness nunca propaga
        logger.error(
            "Readiness falhou: banco indisponivel",
            extra={"database_error": str(exc)},
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=ErrorResponse(
                detail=DATABASE_UNAVAILABLE_DETAIL, trace_id=current_trace_id()
            ).model_dump(),
        )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=HealthResponse(status=STATUS_OK, database=STATUS_OK).model_dump(),
    )

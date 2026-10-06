"""Injecao de dependencias da API.

O motor de Data Quality e um SINGLETON por processo: ele carrega a LRU de
deduplicacao (decisao D10), que precisa sobreviver entre requisicoes. Sessao e
repositorios, ao contrario, vivem apenas durante uma requisicao.
"""

from functools import lru_cache
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings, get_settings
from app.infrastructure.database import get_session
from app.infrastructure.repositories import InvalidEventRepository, ValidEventRepository
from app.services.data_quality import DataQualityEngine
from app.services.ingestion import IngestionService

SessionDep = Annotated[AsyncSession, Depends(get_session)]
"""Sessao assincrona por requisicao."""

SettingsDep = Annotated[Settings, Depends(get_settings)]
"""Configuracao da aplicacao, injetada para que os testes possam sobrescreve-la."""


@lru_cache
def get_data_quality_engine() -> DataQualityEngine:
    """Retorna o motor de Data Quality unico do processo.

    Cacheado de proposito: a janela de deduplicacao por `event_id` e
    in-process e seria inutil se um motor novo fosse criado por requisicao.
    """
    return DataQualityEngine(get_settings())


DataQualityEngineDep = Annotated[DataQualityEngine, Depends(get_data_quality_engine)]
"""Motor de Data Quality compartilhado."""


def get_ingestion_service(
    session: SessionDep, engine: DataQualityEngineDep
) -> IngestionService:
    """Monta o caso de uso de ingestao com os repositorios da sessao atual."""
    return IngestionService(
        engine=engine,
        valid_repo=ValidEventRepository(session),
        invalid_repo=InvalidEventRepository(session),
        session=session,
    )


IngestionServiceDep = Annotated[IngestionService, Depends(get_ingestion_service)]
"""Caso de uso de ingestao pronto para a rota."""

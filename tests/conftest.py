"""Fixtures compartilhadas: configuracao de teste, engine, sessao e cliente HTTP."""

from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import StaticPool

from app.api.deps import get_data_quality_engine
from app.core.config import Settings
from app.infrastructure.database import get_session, init_models
from app.main import create_app
from app.services.data_quality import DataQualityEngine

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture
def settings() -> Settings:
    """`Settings` de teste: sem SDK de telemetria e com SQLite em memoria."""
    return Settings(
        database_url=TEST_DATABASE_URL,
        otel_sdk_disabled=True,
        environment="test",
        _env_file=None,
    )


@pytest.fixture
async def engine(settings: Settings) -> AsyncIterator[AsyncEngine]:
    """Engine aiosqlite com `StaticPool` para que todas as sessoes vejam o mesmo banco."""
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(
        settings.database_url,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    await init_models(engine)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def session(engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    """Sessao assincrona ligada ao engine de teste."""
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest.fixture
def app(settings: Settings, engine: AsyncEngine) -> FastAPI:
    """Aplicacao real com as dependencias apontadas para o banco de teste.

    O lifespan nao roda sob `ASGITransport`, por isso o engine aiosqlite e
    injetado via `dependency_overrides`. O motor de Data Quality tambem e
    sobrescrito: em producao ele e um singleton por processo (a LRU de
    deduplicacao, decisao D10), e um singleton cacheado vazaria `event_id`s
    entre testes.
    """
    application = create_app(settings)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def override_get_session() -> AsyncIterator[AsyncSession]:
        async with factory() as session:
            yield session

    application.dependency_overrides[get_session] = override_get_session
    application.dependency_overrides[get_data_quality_engine] = (
        lambda: DataQualityEngine(settings)
    )
    return application


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """Cliente HTTP falando ASGI direto com a aplicacao, sem rede."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test"
    ) as client:
        yield client

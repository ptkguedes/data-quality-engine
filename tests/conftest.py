"""Fixtures compartilhadas: configuracao de teste, engine e sessao aiosqlite."""

from collections.abc import AsyncIterator

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.config import Settings
from app.infrastructure.database import init_models

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

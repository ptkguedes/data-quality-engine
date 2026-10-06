"""Engine assincrono, sessionmaker e ciclo de vida do schema.

As tabelas sao criadas por `init_models` no lifespan da aplicacao (decisao
D5): sem Alembic, `docker compose up` sobe o servico com o schema pronto e
zero passos manuais.
"""

from collections.abc import AsyncIterator

from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings, get_settings
from app.domain.models import Base

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def create_engine_from_settings(settings: Settings) -> AsyncEngine:
    """Cria um `AsyncEngine` com o dimensionamento de pool da configuracao.

    Pools explicitos nao se aplicam ao SQLite em memoria usado nos testes, por
    isso o dimensionamento so e passado para os demais drivers.
    """
    if settings.database_url.startswith("sqlite"):
        return create_async_engine(settings.database_url, pool_pre_ping=True)

    return create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )


def get_engine() -> AsyncEngine:
    """Retorna o engine global, construindo-o na primeira chamada."""
    global _engine
    if _engine is None:
        _engine = create_engine_from_settings(get_settings())
    return _engine


def set_engine(engine: AsyncEngine) -> None:
    """Define o engine global e refaz o `async_sessionmaker` associado."""
    global _engine, _session_factory
    _engine = engine
    _session_factory = async_sessionmaker(engine, expire_on_commit=False)


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Retorna o `async_sessionmaker` global, construindo-o se necessario."""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _session_factory


async def get_session() -> AsyncIterator[AsyncSession]:
    """Gera uma `AsyncSession` por requisicao (dependencia do FastAPI)."""
    factory = get_session_factory()
    async with factory() as session:
        yield session


async def init_models(engine: AsyncEngine) -> None:
    """Cria as tabelas declaradas em `Base.metadata` se ainda nao existirem."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def ping(session: AsyncSession) -> bool:
    """Executa `SELECT 1` para comprovar que o banco responde."""
    result = await session.execute(select(1))
    return result.scalar_one() == 1


async def dispose_engine() -> None:
    """Fecha o pool do engine global e zera o estado de modulo."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None

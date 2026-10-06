"""Modelos ORM (SQLAlchemy 2.0, declarativo tipado).

`JSONB_VARIANT` usa `JSONB` real no PostgreSQL e cai para `JSON` no SQLite,
o que permite rodar toda a suite de testes em `sqlite+aiosqlite` em memoria,
sem PostgreSQL nem Docker.
"""

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Index, Integer, String, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON

JSONB_VARIANT = postgresql.JSONB().with_variant(JSON(), "sqlite")
"""`JSONB` no PostgreSQL, `JSON` no SQLite (suite de testes sem Postgres)."""

PK_VARIANT = BigInteger().with_variant(Integer(), "sqlite")
"""`BIGINT` no PostgreSQL; no SQLite so `INTEGER PRIMARY KEY` autoincrementa."""


class Base(AsyncAttrs, DeclarativeBase):
    """Base declarativa com suporte a atributos assincronos."""


class ValidEvent(Base):
    """Evento aprovado pelo motor de Data Quality.

    `payload` guarda o payload de negocio APOS as sanitizacoes; a lista de
    sanitizacoes aplicadas fica em `sanitizations` para auditoria.
    """

    __tablename__ = "valid_events"
    __table_args__ = (
        Index("ix_valid_events_event_type", "event_type"),
        Index("ix_valid_events_ingested_at", "ingested_at"),
        Index("ix_valid_events_type_ingested", "event_type", "ingested_at"),
    )

    id: Mapped[int] = mapped_column(PK_VARIANT, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    source: Mapped[str] = mapped_column(String(128), nullable=False)
    event_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB_VARIANT, nullable=False)
    sanitizations: Mapped[list[dict[str, Any]] | None] = mapped_column(
        JSONB_VARIANT, nullable=True
    )
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    trace_id: Mapped[str | None] = mapped_column(String(32), nullable=True)


class InvalidEvent(Base):
    """Linha de dead letter / auditoria de um evento rejeitado.

    `raw_payload` guarda o elemento ORIGINAL do corpo da requisicao, sem
    nenhuma sanitizacao, e nunca deve ser sobrescrito por uma versao tratada.
    """

    __tablename__ = "invalid_events"
    __table_args__ = (
        Index("ix_invalid_events_event_id", "event_id"),
        Index("ix_invalid_events_rejected_at", "rejected_at"),
    )

    id: Mapped[int] = mapped_column(PK_VARIANT, primary_key=True, autoincrement=True)
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    raw_payload: Mapped[Any] = mapped_column(JSONB_VARIANT, nullable=False)
    rejection_reasons: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB_VARIANT, nullable=False
    )
    rejected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    trace_id: Mapped[str | None] = mapped_column(String(32), nullable=True)

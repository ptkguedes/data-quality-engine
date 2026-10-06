"""Testes da fiacao da telemetria (decisoes D15 e D19).

Nenhum destes testes instala providers do SDK: eles cobrem o *momento* em que
a instrumentacao acontece e o curto-circuito de `OTEL_SDK_DISABLED`.
"""

from typing import Any

import pytest
from fastapi import FastAPI

from app import main
from app.core import telemetry
from app.core.config import Settings


def test_create_app_configura_telemetria_antes_de_montar_middlewares(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    """Regressao: o `FastAPIInstrumentor` nao pode rodar depois do startup.

    O instrumentador adiciona um middleware ASGI e o Starlette congela a pilha
    no primeiro `__call__` - que e o evento de `lifespan`. Chamar
    `configure_telemetry` de dentro do lifespan levantava "Cannot add
    middleware after an application has started", a excecao era engolida pelo
    try/except da decisao D15 e a aplicacao ficava sem span de servidor e sem
    spans de SQLAlchemy. O invariante abaixo e que a pilha ainda esteja sem
    montar (`middleware_stack is None`) quando a telemetria e configurada.
    """
    stacks: list[Any] = []

    def spy(application: FastAPI, _settings: Settings) -> None:
        stacks.append(application.middleware_stack)
        # Se a pilha ja estivesse montada, isto levantaria RuntimeError.
        application.add_middleware(_NoopMiddleware)

    monkeypatch.setattr(main, "configure_telemetry", spy)

    app = main.create_app(settings)

    assert stacks == [None]
    assert any(item.cls is _NoopMiddleware for item in app.user_middleware)


async def test_lifespan_instrumenta_o_engine_e_nao_a_aplicacao(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    """O lifespan cuida do engine: a app ja foi instrumentada na fabrica."""
    engine = object()
    instrumented: list[Any] = []

    async def fake_init_models(_engine: Any) -> None:
        return None

    async def fake_dispose_engine() -> None:
        return None

    monkeypatch.setattr(main, "get_settings", lambda: settings)
    monkeypatch.setattr(main, "create_engine_from_settings", lambda _s: engine)
    monkeypatch.setattr(main, "set_engine", lambda _e: None)
    monkeypatch.setattr(main, "init_models", fake_init_models)
    monkeypatch.setattr(main, "dispose_engine", fake_dispose_engine)
    monkeypatch.setattr(
        main, "instrument_database", lambda e, _s: instrumented.append(e)
    )
    monkeypatch.setattr(
        main,
        "configure_telemetry",
        lambda *_args: pytest.fail(
            "configure_telemetry nao deve ser chamada pelo lifespan"
        ),
    )

    app = FastAPI()
    async with main.lifespan(app):
        pass

    assert instrumented == [engine]


def test_configure_telemetry_nao_toca_na_app_com_sdk_desabilitado(
    settings: Settings,
) -> None:
    """`OTEL_SDK_DISABLED` sai antes de instalar qualquer provider (D15)."""
    sentinel = object()
    # Um objeto que nao e FastAPI: se a funcao tentasse instrumentar, falharia.
    telemetry.configure_telemetry(sentinel, settings)  # type: ignore[arg-type]


def test_instrument_database_nao_toca_no_engine_com_sdk_desabilitado(
    settings: Settings,
) -> None:
    """Idem para o engine: sem SDK, `instrument_database` e um no-op."""
    sentinel = object()
    telemetry.instrument_database(sentinel, settings)  # type: ignore[arg-type]


class _NoopMiddleware:
    """Middleware ASGI inerte, usado so para provar que a pilha aceita adicoes."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        await self.app(scope, receive, send)

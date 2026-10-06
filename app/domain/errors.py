"""Excecoes de dominio."""


class DomainError(Exception):
    """Classe base de todos os erros de dominio da aplicacao."""


class PersistenceError(DomainError):
    """Falha ao gravar no banco de dados (decisoes D12 e D13).

    Encapsula o erro original do SQLAlchemy para que nenhuma falha de
    persistencia seja silenciosamente engolida: a API a traduz em HTTP 503.
    """

    def __init__(self, message: str, cause: Exception | None = None) -> None:
        super().__init__(message)
        self.cause = cause


class UnparseableBodyError(DomainError):
    """O corpo da requisicao nao e JSON valido ou nao e objeto/array."""

"""Objetos de valor do contrato de regras de Data Quality (decisao D8).

Uma regra recebe um `EvaluationContext` que carrega o dicionario original
imutavel (`raw`) e uma copia de trabalho MUTAVEL (`working`). E isso que
permite a uma regra de severidade `WARNING` sanitizar (clampar, truncar,
aparar) um valor mantendo a assinatura `evaluate(...) -> Violation | None`.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from app.domain.enums import EventStatus, RejectionReason, Severity


@dataclass(frozen=True, slots=True)
class Violation:
    """Uma unica infracao detectada por uma regra.

    `original` e `applied` sao preenchidos pelas regras de sanitizacao para
    registrar no banco o valor que entrou e o valor que ficou.
    """

    code: RejectionReason
    field: str | None
    message: str
    severity: Severity
    original: Any | None = None
    applied: Any | None = None

    def to_dict(self) -> dict[str, Any]:
        """Forma serializavel em JSON, usada nas colunas JSONB de auditoria."""
        return {
            "code": self.code.value,
            "field": self.field,
            "message": self.message,
            "severity": self.severity.value,
            "original": self.original,
            "applied": self.applied,
        }


@dataclass(slots=True)
class EvaluationContext:
    """Estado compartilhado por todas as regras durante a validacao.

    `raw` deve ser tratado como imutavel (e o que vai para o dead letter);
    `working` e mutavel e acumula as sanitizacoes aplicadas pelas regras.
    """

    raw: dict[str, Any]
    working: dict[str, Any]
    parsed_timestamp: datetime | None = None


@runtime_checkable
class QualityRule(Protocol):
    """Contrato de uma regra de Data Quality."""

    code: RejectionReason
    severity: Severity

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Avalia o contexto e retorna uma `Violation` ou `None` se conforme."""
        ...


@dataclass(slots=True)
class ValidationResult:
    """Resultado da adjudicacao de um evento pelo motor de Data Quality."""

    status: EventStatus
    sanitized_event: dict[str, Any]
    violations: list[Violation] = field(default_factory=list)
    sanitizations: list[Violation] = field(default_factory=list)
    rules_evaluated: int = 0

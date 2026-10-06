"""Testes da camada de dominio pura."""

from app.domain.enums import EventStatus, RejectionReason, Severity
from app.domain.rules import EvaluationContext, QualityRule, Violation
from app.domain.schemas import RawEventPayload


def test_raw_event_payload_accepts_empty_object() -> None:
    """Um objeto vazio e aceito, com todos os campos do envelope em None."""
    model = RawEventPayload.model_validate({})

    assert model.event_id is None
    assert model.event_type is None
    assert model.source is None
    assert model.timestamp is None
    assert model.payload is None


def test_raw_event_payload_preserves_extra_keys() -> None:
    """Chaves desconhecidas do envelope sobrevivem em `model_extra` (D3/D11)."""
    model = RawEventPayload.model_validate(
        {"event_id": "abc", "tenant": "acme", "region": "sa-east-1"}
    )

    assert model.event_id == "abc"
    assert model.model_extra == {"tenant": "acme", "region": "sa-east-1"}


def test_violation_to_dict_is_json_serializable() -> None:
    """`to_dict` expoe codigo e severidade como strings, prontos para JSONB."""
    violation = Violation(
        code=RejectionReason.TEMPERATURE_OUT_OF_RANGE,
        field="payload.temperature",
        message="temperatura acima do limite",
        severity=Severity.WARNING,
        original=150.0,
        applied=60.0,
    )

    assert violation.to_dict() == {
        "code": "TEMPERATURE_OUT_OF_RANGE",
        "field": "payload.temperature",
        "message": "temperatura acima do limite",
        "severity": "WARNING",
        "original": 150.0,
        "applied": 60.0,
    }


def test_evaluation_context_working_copy_is_mutable() -> None:
    """`working` e mutavel e `raw` permanece intocado (decisao D8)."""
    raw = {"payload": {"temperature": 150.0}}
    ctx = EvaluationContext(raw=raw, working={"payload": {"temperature": 150.0}})

    ctx.working["payload"]["temperature"] = 60.0

    assert raw["payload"]["temperature"] == 150.0
    assert ctx.working["payload"]["temperature"] == 60.0


def test_quality_rule_protocol_is_structurally_satisfiable() -> None:
    """Uma classe com code/severity/evaluate satisfaz o Protocol."""

    class _AlwaysConform:
        code = RejectionReason.NEGATIVE_VALUE
        severity = Severity.ERROR

        def evaluate(self, ctx: EvaluationContext) -> Violation | None:
            return None

    rule = _AlwaysConform()

    assert isinstance(rule, QualityRule)
    assert rule.evaluate(EvaluationContext(raw={}, working={})) is None


def test_enum_values_are_stable_strings() -> None:
    """Os enums sao `str` para serializar direto em JSON e no banco."""
    assert EventStatus.SUCCESS.value == "SUCCESS"
    assert EventStatus.REJECTED.value == "REJECTED"
    assert Severity.WARNING.value == "WARNING"
    assert Severity.ERROR.value == "ERROR"
    assert RejectionReason.DUPLICATE_EVENT_ID.value == "DUPLICATE_EVENT_ID"

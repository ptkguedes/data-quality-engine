"""Testes do motor de Data Quality: caminho de sanitizacao e de rejeicao."""

from datetime import timedelta
from typing import Any
from uuid import uuid4

from app.core.config import Settings
from app.domain.enums import EventStatus, RejectionReason, Severity
from app.domain.rules import QualityRule, ValidationResult
from app.services.data_quality import DataQualityEngine, utcnow

FIXED_UUID = "3f2504e0-4f89-11d3-9a0c-0305e82c3301"


def _event(**overrides: Any) -> dict[str, Any]:
    """Envelope totalmente valido, com os campos sobrescritos pelo teste."""
    event: dict[str, Any] = {
        "event_id": str(uuid4()),
        "event_type": "sensor.reading",
        "source": "gateway-01",
        "timestamp": utcnow().isoformat(),
        "payload": {"temperature": 21.5, "humidity": 48.0, "value": 3},
    }
    event.update(overrides)
    return event


def _codes(result: ValidationResult) -> set[RejectionReason]:
    """Todos os codigos produzidos, de rejeicao e de sanitizacao."""
    return {item.code for item in result.violations} | {
        item.code for item in result.sanitizations
    }


def _violation_codes(result: ValidationResult) -> set[RejectionReason]:
    return {item.code for item in result.violations}


def _sanitization_codes(result: ValidationResult) -> set[RejectionReason]:
    return {item.code for item in result.sanitizations}


def _engine(settings: Settings) -> DataQualityEngine:
    return DataQualityEngine(settings)


# --------------------------------------------------------------------------
# Caminho feliz
# --------------------------------------------------------------------------
def test_valid_event_is_accepted_without_sanitizations(settings: Settings) -> None:
    """Um evento conforme passa sem violacoes nem sanitizacoes."""
    result = _engine(settings).validate(_event())

    assert result.status is EventStatus.SUCCESS
    assert result.violations == []
    assert result.sanitizations == []
    assert result.parsed_timestamp is not None
    assert result.rules_evaluated > 0


def test_raw_input_is_never_mutated(settings: Settings) -> None:
    """O dict recebido fica intocado: e ele que vai para o dead letter (D8/D13)."""
    raw = _event(payload={"temperature": 150.0})
    snapshot = {"temperature": 150.0}

    result = _engine(settings).validate(raw)

    assert raw["payload"] == snapshot
    assert result.sanitized_event["payload"]["temperature"] == settings.temperature_max


def test_every_rule_satisfies_the_protocol(settings: Settings) -> None:
    """A lista de regras e composta de objetos que cumprem `QualityRule`."""
    engine = _engine(settings)

    assert len(engine.schema_rules) > 0
    assert len(engine.quality_rules) > 0
    for rule in engine.schema_rules + engine.quality_rules:
        assert isinstance(rule, QualityRule)


# --------------------------------------------------------------------------
# Conformidade de schema (caminho de rejeicao)
# --------------------------------------------------------------------------
def test_missing_event_id_is_rejected(settings: Settings) -> None:
    """Campo obrigatorio ausente e ERROR e manda o evento ao dead letter."""
    raw = _event()
    del raw["event_id"]

    result = _engine(settings).validate(raw)

    assert result.status is EventStatus.REJECTED
    assert RejectionReason.MISSING_REQUIRED_FIELD in _violation_codes(result)


def test_empty_event_id_is_rejected(settings: Settings) -> None:
    """Um `event_id` vazio e inutilizavel: ERROR INVALID_EVENT_ID."""
    result = _engine(settings).validate(_event(event_id="   "))

    assert result.status is EventStatus.REJECTED
    assert RejectionReason.INVALID_EVENT_ID in _violation_codes(result)


def test_non_uuid_event_id_is_sanitized(settings: Settings) -> None:
    """Um `event_id` que nao e UUID e aceito como sanitizacao e normalizado."""
    result = _engine(settings).validate(_event(event_id="  evt-42  "))

    assert result.status is EventStatus.SUCCESS
    assert RejectionReason.INVALID_EVENT_ID in _sanitization_codes(result)
    assert result.sanitized_event["event_id"] == "evt-42"


def test_long_envelope_strings_are_truncated_to_the_column_width(
    settings: Settings,
) -> None:
    """Strings do envelope sao truncadas pelo motor, nao pelo `String(128)`.

    Sem isso o INSERT falharia no PostgreSQL - inclusive o do dead letter, que
    e a trilha que a arquitetura promete nunca perder.
    """
    limit = settings.max_envelope_string_length
    result = _engine(settings).validate(
        _event(event_id="e" * (limit + 10), event_type="t" * (limit + 10))
    )

    assert result.status is EventStatus.SUCCESS
    assert RejectionReason.ENVELOPE_STRING_TOO_LONG in _sanitization_codes(result)
    assert len(result.sanitized_event["event_id"]) == limit
    assert len(result.sanitized_event["event_type"]) == limit


def test_long_envelope_strings_are_truncated_on_the_rejected_path(
    settings: Settings,
) -> None:
    """Mesmo rejeitado, o `event_id` sai curto o suficiente para o dead letter."""
    limit = settings.max_envelope_string_length
    raw = _event(source="s" * (limit + 10), payload={"value": -1})

    result = _engine(settings).validate(raw)

    assert result.status is EventStatus.REJECTED
    assert len(result.sanitized_event["source"]) == limit
    assert len(raw["source"]) == limit + 10  # o original segue intocado


def test_unparseable_timestamp_is_rejected(settings: Settings) -> None:
    """Timestamp que nao e ISO-8601 e ERROR INVALID_TIMESTAMP."""
    result = _engine(settings).validate(_event(timestamp="ontem de manha"))

    assert result.status is EventStatus.REJECTED
    assert RejectionReason.INVALID_TIMESTAMP in _violation_codes(result)


def test_naive_timestamp_is_sanitized_as_utc(settings: Settings) -> None:
    """Timestamp sem fuso e assumido UTC e registrado como sanitizacao."""
    naive = utcnow().replace(tzinfo=None).isoformat()

    result = _engine(settings).validate(_event(timestamp=naive))

    assert result.status is EventStatus.SUCCESS
    assert RejectionReason.INVALID_TIMESTAMP in _sanitization_codes(result)
    assert result.parsed_timestamp is not None
    assert result.parsed_timestamp.utcoffset() == timedelta()


def test_missing_payload_is_sanitized_to_empty_object(settings: Settings) -> None:
    """Payload ausente vira `{}` com WARNING, sem rejeitar o evento."""
    raw = _event()
    del raw["payload"]

    result = _engine(settings).validate(raw)

    assert result.status is EventStatus.SUCCESS
    assert RejectionReason.PAYLOAD_NOT_OBJECT in _sanitization_codes(result)
    assert result.sanitized_event["payload"] == {}


def test_payload_that_is_not_an_object_is_rejected(settings: Settings) -> None:
    """Payload de outro tipo JSON e ERROR PAYLOAD_NOT_OBJECT."""
    result = _engine(settings).validate(_event(payload=[1, 2, 3]))

    assert result.status is EventStatus.REJECTED
    assert RejectionReason.PAYLOAD_NOT_OBJECT in _violation_codes(result)


def test_unknown_envelope_key_is_warning_in_permissive_mode(
    settings: Settings,
) -> None:
    """Modo permissivo (padrao): a chave desconhecida vai para `payload._extra`."""
    result = _engine(settings).validate(_event(tenant="acme"))

    assert result.status is EventStatus.SUCCESS
    assert RejectionReason.UNKNOWN_FIELD in _sanitization_codes(result)
    assert "tenant" not in result.sanitized_event
    assert result.sanitized_event["payload"]["_extra"] == {"tenant": "acme"}


def test_unknown_envelope_key_is_error_in_strict_mode(settings: Settings) -> None:
    """`strict_mode=True`: a chave desconhecida e ERROR UNKNOWN_FIELD (D11)."""
    strict = settings.model_copy(update={"strict_mode": True})

    result = _engine(strict).validate(_event(tenant="acme"))

    assert result.status is EventStatus.REJECTED
    assert RejectionReason.UNKNOWN_FIELD in _violation_codes(result)


def test_non_object_envelope_is_rejected_without_raising(settings: Settings) -> None:
    """Um elemento que nao e objeto JSON e rejeitado, nao explode (D3/D15)."""
    result = _engine(settings).validate("nao-sou-um-objeto")  # type: ignore[arg-type]

    assert result.status is EventStatus.REJECTED
    assert RejectionReason.UNPARSEABLE_ENVELOPE in _violation_codes(result)


def test_envelope_with_wrong_scalar_type_is_rejected(settings: Settings) -> None:
    """Um campo de envelope com tipo errado nao chega ao banco: ERROR."""
    result = _engine(settings).validate(_event(event_type=42))

    assert result.status is EventStatus.REJECTED
    assert RejectionReason.UNPARSEABLE_ENVELOPE in _violation_codes(result)


def test_schema_error_short_circuits_quality_rules(settings: Settings) -> None:
    """Com ERROR de schema, as regras de qualidade nao rodam (decisao D9)."""
    engine = _engine(settings)
    raw = _event(payload={"temperature": 150.0})
    del raw["source"]

    result = engine.validate(raw)

    assert result.status is EventStatus.REJECTED
    assert result.rules_evaluated == len(engine.schema_rules)
    assert RejectionReason.TEMPERATURE_OUT_OF_RANGE not in _codes(result)


# --------------------------------------------------------------------------
# Regras de qualidade: caminho de sanitizacao
# --------------------------------------------------------------------------
def test_temperature_above_maximum_is_clamped(settings: Settings) -> None:
    """Temperatura 150 vira 60.0 e o evento segue valido com a sanitizacao."""
    result = _engine(settings).validate(_event(payload={"temperature": 150.0}))

    assert result.status is EventStatus.SUCCESS
    assert result.violations == []
    assert result.sanitized_event["payload"]["temperature"] == 60.0
    assert result.sanitized_event["payload"]["temperature"] == settings.temperature_max

    sanitization = next(
        item
        for item in result.sanitizations
        if item.code is RejectionReason.TEMPERATURE_OUT_OF_RANGE
    )
    assert sanitization.severity is Severity.WARNING
    assert sanitization.original == 150.0
    assert sanitization.applied == settings.temperature_max
    assert sanitization.field == "payload.temperature"


def test_temperature_below_minimum_is_clamped(settings: Settings) -> None:
    """Temperatura abaixo do piso e elevada ao `temperature_min`."""
    result = _engine(settings).validate(_event(payload={"temperature": -300.0}))

    assert result.status is EventStatus.SUCCESS
    assert result.sanitized_event["payload"]["temperature"] == settings.temperature_min


def test_humidity_below_minimum_is_clamped(settings: Settings) -> None:
    """Umidade -5 vira 0.0 com sanitizacao HUMIDITY_OUT_OF_RANGE."""
    result = _engine(settings).validate(_event(payload={"humidity": -5.0}))

    assert result.status is EventStatus.SUCCESS
    assert result.sanitized_event["payload"]["humidity"] == 0.0
    assert result.sanitized_event["payload"]["humidity"] == settings.humidity_min
    assert RejectionReason.HUMIDITY_OUT_OF_RANGE in _sanitization_codes(result)


def test_long_payload_string_is_truncated(settings: Settings) -> None:
    """String acima de `max_string_length` e truncada no limite configurado."""
    long_value = "x" * (settings.max_string_length * 2)

    result = _engine(settings).validate(_event(payload={"label": long_value}))

    assert result.status is EventStatus.SUCCESS
    assert len(result.sanitized_event["payload"]["label"]) == settings.max_string_length
    assert RejectionReason.STRING_TOO_LONG in _sanitization_codes(result)


def test_untrimmed_envelope_strings_are_trimmed(settings: Settings) -> None:
    """Espacos nas pontas de `event_type` e `source` sao aparados (WARNING)."""
    result = _engine(settings).validate(
        _event(event_type="  sensor.reading  ", source=" gateway-01 ")
    )

    assert result.status is EventStatus.SUCCESS
    assert result.sanitized_event["event_type"] == "sensor.reading"
    assert result.sanitized_event["source"] == "gateway-01"
    assert RejectionReason.UNTRIMMED_STRING in _sanitization_codes(result)


def test_numeric_string_metric_is_coerced_to_float(settings: Settings) -> None:
    """Metrica em string numerica e normalizada e ainda passa pela faixa."""
    result = _engine(settings).validate(_event(payload={"temperature": "150.0"}))

    assert result.status is EventStatus.SUCCESS
    assert result.sanitized_event["payload"]["temperature"] == settings.temperature_max


# --------------------------------------------------------------------------
# Regras de qualidade: caminho de rejeicao
# --------------------------------------------------------------------------
def test_negative_value_is_rejected(settings: Settings) -> None:
    """`payload.value` negativo e ERROR NEGATIVE_VALUE."""
    result = _engine(settings).validate(_event(payload={"value": -1}))

    assert result.status is EventStatus.REJECTED
    assert RejectionReason.NEGATIVE_VALUE in _violation_codes(result)


def test_non_numeric_metric_is_rejected(settings: Settings) -> None:
    """Metrica nao convertivel para numero e ERROR NON_NUMERIC_METRIC."""
    result = _engine(settings).validate(_event(payload={"temperature": "quente"}))

    assert result.status is EventStatus.REJECTED
    assert RejectionReason.NON_NUMERIC_METRIC in _violation_codes(result)


def test_future_timestamp_is_rejected(settings: Settings) -> None:
    """Um dia no futuro excede a tolerancia configurada."""
    future = (utcnow() + timedelta(days=1)).isoformat()

    result = _engine(settings).validate(_event(timestamp=future))

    assert result.status is EventStatus.REJECTED
    assert RejectionReason.TIMESTAMP_IN_FUTURE in _violation_codes(result)


def test_timestamp_within_future_tolerance_is_accepted(settings: Settings) -> None:
    """Dentro de `future_timestamp_tolerance_seconds` o evento e aceito."""
    tolerated = (
        utcnow()
        + timedelta(seconds=settings.future_timestamp_tolerance_seconds // 2)
    ).isoformat()

    result = _engine(settings).validate(_event(timestamp=tolerated))

    assert result.status is EventStatus.SUCCESS


def test_stale_timestamp_is_rejected(settings: Settings) -> None:
    """400 dias de idade ultrapassa a janela de retencao."""
    stale = (utcnow() - timedelta(days=400)).isoformat()

    result = _engine(settings).validate(_event(timestamp=stale))

    assert result.status is EventStatus.REJECTED
    assert RejectionReason.TIMESTAMP_TOO_OLD in _violation_codes(result)


def test_value_floor_comes_from_settings(settings: Settings) -> None:
    """`VALUE_MIN` e configuravel: com piso -10, `value: -5` passa."""
    permissive = settings.model_copy(update={"value_min": -10.0})

    rejected = _engine(settings).validate(_event(payload={"value": -5}))
    accepted = _engine(permissive).validate(_event(payload={"value": -5}))

    assert rejected.status is EventStatus.REJECTED
    assert RejectionReason.NEGATIVE_VALUE in _violation_codes(rejected)
    assert accepted.status is EventStatus.SUCCESS


def test_duplicate_event_id_is_rejected_on_second_sight(settings: Settings) -> None:
    """O mesmo `event_id` duas vezes: a segunda e DUPLICATE_EVENT_ID (D10)."""
    engine = _engine(settings)
    raw = _event(event_id=FIXED_UUID)

    first = engine.validate(raw)
    second = engine.validate(raw)

    assert first.status is EventStatus.SUCCESS
    assert second.status is EventStatus.REJECTED
    assert RejectionReason.DUPLICATE_EVENT_ID in _violation_codes(second)


def test_rejected_event_id_never_enters_the_dedup_window(settings: Settings) -> None:
    """Um evento rejeitado nao consome o `event_id`: a correcao pode voltar."""
    engine = _engine(settings)

    rejected = engine.validate(_event(event_id=FIXED_UUID, payload={"value": -1}))
    corrected = engine.validate(_event(event_id=FIXED_UUID, payload={"value": 1}))

    assert rejected.status is EventStatus.REJECTED
    assert corrected.status is EventStatus.SUCCESS


def test_released_reservation_frees_the_event_id_for_a_retry(
    settings: Settings,
) -> None:
    """`release_dedup` desfaz a reserva de uma transacao que nao commitou."""
    engine = _engine(settings)

    first = engine.validate(_event(event_id=FIXED_UUID))
    engine.release_dedup([FIXED_UUID])
    retried = engine.validate(_event(event_id=FIXED_UUID))

    assert first.status is EventStatus.SUCCESS
    assert retried.status is EventStatus.SUCCESS


def test_confirmed_event_id_stays_in_the_window(settings: Settings) -> None:
    """Depois de `confirm_dedup` a entrada e definitiva: `release` nao a apaga."""
    engine = _engine(settings)

    engine.validate(_event(event_id=FIXED_UUID))
    engine.confirm_dedup([FIXED_UUID])
    engine.release_dedup([FIXED_UUID])

    assert engine.validate(_event(event_id=FIXED_UUID)).status is (
        EventStatus.REJECTED
    )


def test_dedup_window_is_bounded(settings: Settings) -> None:
    """A LRU respeita `dedup_window_size` e nao cresce sem limite."""
    bounded = settings.model_copy(update={"dedup_window_size": 1})
    engine = _engine(bounded)

    engine.validate(_event(event_id=FIXED_UUID))
    engine.validate(_event(event_id=str(uuid4())))
    evicted = engine.validate(_event(event_id=FIXED_UUID))

    assert evicted.status is EventStatus.SUCCESS


def test_all_simultaneous_violations_are_collected(settings: Settings) -> None:
    """Varias violacoes e sanitizacoes no mesmo evento: todas sao coletadas."""
    result = _engine(settings).validate(
        _event(
            event_type="  sensor.reading  ",
            timestamp=(utcnow() + timedelta(days=1)).isoformat(),
            payload={
                "temperature": 150.0,
                "humidity": -5.0,
                "value": -1,
                "label": "x" * (settings.max_string_length + 1),
            },
        )
    )

    assert result.status is EventStatus.REJECTED
    assert _violation_codes(result) == {
        RejectionReason.NEGATIVE_VALUE,
        RejectionReason.TIMESTAMP_IN_FUTURE,
    }
    assert _sanitization_codes(result) == {
        RejectionReason.TEMPERATURE_OUT_OF_RANGE,
        RejectionReason.HUMIDITY_OUT_OF_RANGE,
        RejectionReason.STRING_TOO_LONG,
        RejectionReason.UNTRIMMED_STRING,
    }


def test_every_rejection_reason_is_produced_by_some_rule(settings: Settings) -> None:
    """Todo codigo do catalogo e alcancavel por pelo menos um cenario."""
    produced: set[RejectionReason] = set()

    no_source = _event()
    del no_source["source"]
    no_payload = _event()
    del no_payload["payload"]

    scenarios: list[Any] = [
        no_source,
        no_payload,
        _event(event_id="   "),
        _event(event_id="evt-42"),
        _event(timestamp="ontem de manha"),
        _event(timestamp="2024-05-17T12:30:00"),
        _event(payload=[1, 2, 3]),
        _event(tenant="acme"),
        "nao-sou-um-objeto",
        _event(payload={"temperature": 150.0}),
        _event(payload={"humidity": -5.0}),
        _event(payload={"value": -1}),
        _event(payload={"temperature": "quente"}),
        _event(payload={"label": "x" * (settings.max_string_length + 1)}),
        _event(source="s" * (settings.max_envelope_string_length + 1)),
        _event(source="  gateway-01  "),
        _event(timestamp=(utcnow() + timedelta(days=1)).isoformat()),
        _event(timestamp=(utcnow() - timedelta(days=400)).isoformat()),
    ]
    for raw in scenarios:
        produced |= _codes(_engine(settings).validate(raw))

    strict = settings.model_copy(update={"strict_mode": True})
    produced |= _codes(_engine(strict).validate(_event(tenant="acme")))

    engine = _engine(settings)
    duplicated = _event(event_id=FIXED_UUID)
    engine.validate(duplicated)
    produced |= _codes(engine.validate(duplicated))

    assert produced == set(RejectionReason)

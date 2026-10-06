"""Motor de Data Quality: catalogo de regras componiveis + adjudicacao.

O motor e uma LISTA ORDENADA de regras, nao uma pilha de `if`s. Cada regra e
uma classe pequena que cumpre o Protocol `QualityRule`
(`evaluate(ctx) -> Violation | None`), declara seu `code` e sua `severity` e
recebe todos os seus limites via `Settings` - nenhum numero magico aparece
inline neste modulo.

Roteamento por severidade (decisao D7):

* `Severity.WARNING` -> a regra MUTA `ctx.working` (clampa, trunca, apara,
  normaliza) e a violacao e registrada como SANITIZACAO; o evento continua
  valido e vai para `valid_events` com o registro do que foi alterado.
* `Severity.ERROR`   -> o evento e REJEITADO e vai para `invalid_events`.

Nenhuma regra decide seu proprio roteamento: quem particiona por severidade e
o motor, em `validate()`.
"""

import logging
from collections import OrderedDict
from collections.abc import Callable, Iterable, Sequence
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any
from uuid import UUID

from opentelemetry import metrics, trace
from opentelemetry.trace import Span, Status, StatusCode
from pydantic import ValidationError

from app.core.config import Settings
from app.domain.enums import EventStatus, RejectionReason, Severity
from app.domain.rules import EvaluationContext, QualityRule, ValidationResult, Violation
from app.domain.schemas import RawEventPayload

logger = logging.getLogger(__name__)

# --- Nomes de span, de evento de span e de atributos ----------------------
SPAN_VALIDATE = "data_quality.validate"
SPAN_EVENT_REJECTED = "event.rejected"
ATTR_STATUS = "dq.status"
ATTR_VIOLATION_COUNT = "dq.violation_count"
ATTR_SANITIZATION_COUNT = "dq.sanitization_count"
ATTR_RULES_EVALUATED = "dq.rules_evaluated"
ATTR_VIOLATION_CODES = "dq.violation_codes"
ATTR_EVENT_TYPE = "event.type"
ATTR_EVENT_ID = "event.id"

# --- Instrumentos de metrica (decisao D17) --------------------------------
METRIC_EVENTS_PROCESSED = "dq.events.processed"
METRIC_VALIDATION_DURATION = "dq.validation.duration_ms"

# --- Chaves do envelope e do payload --------------------------------------
EVENT_ID_FIELD = "event_id"
EVENT_TYPE_FIELD = "event_type"
SOURCE_FIELD = "source"
TIMESTAMP_FIELD = "timestamp"
PAYLOAD_FIELD = "payload"
EXTRA_FIELD = "_extra"

ENVELOPE_FIELDS: frozenset[str] = frozenset(RawEventPayload.model_fields)
"""Chaves conhecidas do envelope; qualquer outra e tratada por `UnknownFieldRule`."""

REQUIRED_FIELDS: tuple[str, ...] = (
    EVENT_ID_FIELD,
    EVENT_TYPE_FIELD,
    SOURCE_FIELD,
    TIMESTAMP_FIELD,
)
TRIMMED_FIELDS: tuple[str, ...] = (EVENT_TYPE_FIELD, SOURCE_FIELD)
METRIC_FIELDS: tuple[str, ...] = ("temperature", "humidity", "value")

MINIMUM_ALLOWED_VALUE = 0.0
"""Piso de `payload.value`: e a definicao de "nao negativo", nao um limite ajustavel."""

MILLISECONDS_PER_SECOND = 1000.0
UTC_OFFSET_SUFFIX = "+00:00"

_tracer = trace.get_tracer(__name__)
_meter = metrics.get_meter(__name__)
_events_processed = _meter.create_counter(
    METRIC_EVENTS_PROCESSED,
    unit="1",
    description="Eventos adjudicados pelo motor de Data Quality, por status.",
)
_validation_duration = _meter.create_histogram(
    METRIC_VALIDATION_DURATION,
    unit="ms",
    description="Duracao da validacao de Data Quality por evento.",
)


# --------------------------------------------------------------------------
# Auxiliares puros
# --------------------------------------------------------------------------
def parse_timestamp(value: Any) -> datetime | None:
    """Converte um valor ISO-8601 em `datetime` (possivelmente naive).

    Aceita o sufixo `Z`. Retorna `None` quando o valor nao e parseavel - quem
    decide a severidade disso e `TimestampParseRule`.
    """
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text.removesuffix("Z").removesuffix("z") + UTC_OFFSET_SUFFIX
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def utcnow() -> datetime:
    """Agora, sempre com fuso UTC explicito."""
    return datetime.now(timezone.utc)


def _coerce_float(value: Any) -> float | None:
    """Converte para `float` quando possivel; `None` caso contrario.

    `bool` e deliberadamente recusado: `True` nao e uma metrica numerica.
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _is_uuid(value: str) -> bool:
    """Indica se a string e um UUID valido."""
    try:
        UUID(value)
    except ValueError:
        return False
    return True


def _payload_object(ctx: EvaluationContext) -> dict[str, Any]:
    """Payload mutavel do contexto, ou um dict descartavel se ele nao for objeto.

    Um payload que nao e objeto JSON ja foi marcado como ERROR por
    `PayloadObjectRule`; devolver um dict vazio evita que cada regra de
    qualidade precise repetir essa checagem.
    """
    payload = ctx.working.get(PAYLOAD_FIELD)
    return payload if isinstance(payload, dict) else {}


def _join(names: Iterable[str]) -> str:
    """Junta nomes de campo em um unico rotulo estavel."""
    return ", ".join(names)


# --------------------------------------------------------------------------
# Regras de conformidade de schema
# --------------------------------------------------------------------------
class RequiredFieldsRule:
    """Exige `event_id`, `event_type`, `source` e `timestamp` presentes e nao nulos."""

    code = RejectionReason.MISSING_REQUIRED_FIELD
    severity = Severity.ERROR

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Reporta de uma vez todos os campos obrigatorios ausentes."""
        missing = [name for name in REQUIRED_FIELDS if ctx.working.get(name) is None]
        if not missing:
            return None
        return Violation(
            code=self.code,
            field=_join(missing),
            message=f"Campos obrigatorios ausentes ou nulos: {_join(missing)}.",
            severity=self.severity,
        )


class EventIdRule:
    """Valida e normaliza `event_id`.

    Vazio ou nao-string e ERROR; uma string que nao e UUID e aceita como
    WARNING, com o valor aparado gravado em `ctx.working`.
    """

    code = RejectionReason.INVALID_EVENT_ID
    severity = Severity.ERROR

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Rejeita um identificador inutilizavel; normaliza um utilizavel."""
        value = ctx.working.get(EVENT_ID_FIELD)
        if value is None:
            return None  # ausencia e assunto de RequiredFieldsRule
        if not isinstance(value, str) or not value.strip():
            return Violation(
                code=self.code,
                field=EVENT_ID_FIELD,
                message="event_id deve ser uma string nao vazia.",
                severity=Severity.ERROR,
                original=value,
            )
        normalised = value.strip()
        is_uuid = _is_uuid(normalised)
        if is_uuid and normalised == value:
            return None
        ctx.working[EVENT_ID_FIELD] = normalised
        detail = (
            "event_id aparado."
            if is_uuid
            else "event_id nao e um UUID; aceito e normalizado."
        )
        return Violation(
            code=self.code,
            field=EVENT_ID_FIELD,
            message=detail,
            severity=Severity.WARNING,
            original=value,
            applied=normalised,
        )


class TimestampParseRule:
    """Converte `timestamp` em um `datetime` UTC com fuso.

    Valor nao parseavel e ERROR; valor naive e assumido como UTC e registrado
    como sanitizacao (WARNING).
    """

    code = RejectionReason.INVALID_TIMESTAMP
    severity = Severity.ERROR

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Preenche `ctx.parsed_timestamp` ou reporta o timestamp invalido."""
        value = ctx.working.get(TIMESTAMP_FIELD)
        if value is None:
            return None  # ausencia e assunto de RequiredFieldsRule
        parsed = parse_timestamp(value)
        if parsed is None:
            return Violation(
                code=self.code,
                field=TIMESTAMP_FIELD,
                message="timestamp nao e uma data ISO-8601 valida.",
                severity=Severity.ERROR,
                original=value,
            )
        if parsed.tzinfo is not None:
            ctx.parsed_timestamp = parsed.astimezone(timezone.utc)
            return None
        aware = parsed.replace(tzinfo=timezone.utc)
        ctx.parsed_timestamp = aware
        applied = aware.isoformat()
        ctx.working[TIMESTAMP_FIELD] = applied
        return Violation(
            code=self.code,
            field=TIMESTAMP_FIELD,
            message="timestamp sem fuso horario; assumido UTC.",
            severity=Severity.WARNING,
            original=value,
            applied=applied,
        )


class PayloadObjectRule:
    """Garante que `payload` seja um objeto JSON.

    Payload ausente ou nulo e sanitizado para `{}` (WARNING); payload de outro
    tipo e ERROR.
    """

    code = RejectionReason.PAYLOAD_NOT_OBJECT
    severity = Severity.ERROR

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Normaliza a ausencia do payload e rejeita um payload de tipo errado."""
        value = ctx.working.get(PAYLOAD_FIELD)
        if value is None:
            ctx.working[PAYLOAD_FIELD] = {}
            return Violation(
                code=self.code,
                field=PAYLOAD_FIELD,
                message="payload ausente; sanitizado para objeto vazio.",
                severity=Severity.WARNING,
                original=value,
                applied={},
            )
        if not isinstance(value, dict):
            return Violation(
                code=self.code,
                field=PAYLOAD_FIELD,
                message="payload deve ser um objeto JSON.",
                severity=Severity.ERROR,
                original=value,
            )
        return None


class UnknownFieldRule:
    """Politica de chaves desconhecidas no envelope (decisao D11).

    `strict_mode=True` -> ERROR. `strict_mode=False` (padrao) -> WARNING e as
    chaves sao movidas para `payload._extra`, para que um produtor que adiciona
    um campo novo nao perca dado.
    """

    code = RejectionReason.UNKNOWN_FIELD
    severity: Severity

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.severity = (
            Severity.ERROR if settings.strict_mode else Severity.WARNING
        )

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Rejeita ou reacomoda as chaves fora do envelope conhecido."""
        extras = sorted(key for key in ctx.working if key not in ENVELOPE_FIELDS)
        if not extras:
            return None
        if self._settings.strict_mode:
            return Violation(
                code=self.code,
                field=_join(extras),
                message=f"Chaves desconhecidas no envelope: {_join(extras)}.",
                severity=Severity.ERROR,
                original=extras,
            )
        moved = {key: ctx.working.pop(key) for key in extras}
        payload = _payload_object(ctx)
        container = payload.get(EXTRA_FIELD)
        if not isinstance(container, dict):
            container = {}
            payload[EXTRA_FIELD] = container
        container.update(moved)
        return Violation(
            code=self.code,
            field=_join(extras),
            message=(
                f"Chaves desconhecidas movidas para {PAYLOAD_FIELD}.{EXTRA_FIELD}: "
                f"{_join(extras)}."
            ),
            severity=Severity.WARNING,
            original=moved,
            applied=f"{PAYLOAD_FIELD}.{EXTRA_FIELD}",
        )


# --------------------------------------------------------------------------
# Regras de qualidade
# --------------------------------------------------------------------------
class NumericMetricRule:
    """Exige que as metricas presentes sejam numeros.

    Roda antes das regras de faixa: metricas em string numerica ("21.5") sao
    normalizadas para `float` no payload de trabalho e metricas nao
    convertiveis sao ERROR.
    """

    code = RejectionReason.NON_NUMERIC_METRIC
    severity = Severity.ERROR

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Reporta de uma vez todas as metricas nao numericas do payload."""
        payload = _payload_object(ctx)
        offenders: dict[str, Any] = {}
        for name in METRIC_FIELDS:
            if payload.get(name) is None:
                continue
            value = payload[name]
            coerced = _coerce_float(value)
            if coerced is None:
                offenders[name] = value
            elif isinstance(value, str):
                payload[name] = coerced
        if not offenders:
            return None
        fields = [f"{PAYLOAD_FIELD}.{name}" for name in offenders]
        return Violation(
            code=self.code,
            field=_join(fields),
            message=f"Metricas nao numericas: {_join(fields)}.",
            severity=self.severity,
            original=offenders,
        )


class _ClampRangeRule:
    """Base das regras de faixa: clampa um numero do payload (WARNING)."""

    code: RejectionReason
    severity = Severity.WARNING
    metric: str

    def __init__(self, minimum: float, maximum: float) -> None:
        self._minimum = minimum
        self._maximum = maximum

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Clampa o valor na faixa configurada e registra o que foi alterado."""
        payload = _payload_object(ctx)
        if payload.get(self.metric) is None:
            return None
        original = payload[self.metric]
        current = _coerce_float(original)
        if current is None:
            return None  # NumericMetricRule ja reportou
        clamped = min(max(current, self._minimum), self._maximum)
        if clamped == current:
            return None
        payload[self.metric] = clamped
        return Violation(
            code=self.code,
            field=f"{PAYLOAD_FIELD}.{self.metric}",
            message=(
                f"{self.metric} fora da faixa "
                f"[{self._minimum}, {self._maximum}]; clampado."
            ),
            severity=self.severity,
            original=original,
            applied=clamped,
        )


class TemperatureRangeRule(_ClampRangeRule):
    """Clampa `payload.temperature` em `[temperature_min, temperature_max]`."""

    code = RejectionReason.TEMPERATURE_OUT_OF_RANGE
    metric = "temperature"

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings.temperature_min, settings.temperature_max)


class HumidityRangeRule(_ClampRangeRule):
    """Clampa `payload.humidity` em `[humidity_min, humidity_max]`."""

    code = RejectionReason.HUMIDITY_OUT_OF_RANGE
    metric = "humidity"

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings.humidity_min, settings.humidity_max)


class NonNegativeValueRule:
    """Rejeita `payload.value` negativo."""

    code = RejectionReason.NEGATIVE_VALUE
    severity = Severity.ERROR

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Reporta ERROR quando `payload.value` e menor que o piso permitido."""
        payload = _payload_object(ctx)
        if payload.get("value") is None:
            return None
        current = _coerce_float(payload["value"])
        if current is None or current >= MINIMUM_ALLOWED_VALUE:
            return None
        return Violation(
            code=self.code,
            field=f"{PAYLOAD_FIELD}.value",
            message=f"value deve ser maior ou igual a {MINIMUM_ALLOWED_VALUE}.",
            severity=self.severity,
            original=payload["value"],
        )


class StringLengthRule:
    """Trunca qualquer string do payload maior que `max_string_length`."""

    code = RejectionReason.STRING_TOO_LONG
    severity = Severity.WARNING

    def __init__(self, settings: Settings) -> None:
        self._max_length = settings.max_string_length

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Trunca todas as strings longas e registra tamanhos antes e depois."""
        payload = _payload_object(ctx)
        original: dict[str, int] = {}
        for name, value in payload.items():
            if isinstance(value, str) and len(value) > self._max_length:
                original[name] = len(value)
                payload[name] = value[: self._max_length]
        if not original:
            return None
        fields = [f"{PAYLOAD_FIELD}.{name}" for name in original]
        return Violation(
            code=self.code,
            field=_join(fields),
            message=(
                f"Strings acima de {self._max_length} caracteres truncadas: "
                f"{_join(fields)}."
            ),
            severity=self.severity,
            original=original,
            applied=self._max_length,
        )


class TrimStringRule:
    """Apara espacos em branco nas pontas de `event_type` e `source`."""

    code = RejectionReason.UNTRIMMED_STRING
    severity = Severity.WARNING

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Apara os campos do envelope e registra os valores antes e depois."""
        original: dict[str, str] = {}
        applied: dict[str, str] = {}
        for name in TRIMMED_FIELDS:
            value = ctx.working.get(name)
            if isinstance(value, str) and value != value.strip():
                original[name] = value
                applied[name] = value.strip()
                ctx.working[name] = applied[name]
        if not original:
            return None
        return Violation(
            code=self.code,
            field=_join(original),
            message=f"Espacos nas pontas removidos de: {_join(original)}.",
            severity=self.severity,
            original=original,
            applied=applied,
        )


class FutureTimestampRule:
    """Rejeita eventos a frente de `future_timestamp_tolerance_seconds`."""

    code = RejectionReason.TIMESTAMP_IN_FUTURE
    severity = Severity.ERROR

    def __init__(self, settings: Settings) -> None:
        self._tolerance = timedelta(
            seconds=settings.future_timestamp_tolerance_seconds
        )

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Compara o timestamp parseado com agora mais a tolerancia."""
        moment = ctx.parsed_timestamp
        if moment is None:
            return None
        limit = utcnow() + self._tolerance
        if moment <= limit:
            return None
        return Violation(
            code=self.code,
            field=TIMESTAMP_FIELD,
            message=(
                "timestamp no futuro alem da tolerancia de "
                f"{self._tolerance.total_seconds()} segundos."
            ),
            severity=self.severity,
            original=moment.isoformat(),
        )


class StaleTimestampRule:
    """Rejeita eventos mais antigos que `retention_window_days`."""

    code = RejectionReason.TIMESTAMP_TOO_OLD
    severity = Severity.ERROR

    def __init__(self, settings: Settings) -> None:
        self._window = timedelta(days=settings.retention_window_days)

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Compara o timestamp parseado com o inicio da janela de retencao."""
        moment = ctx.parsed_timestamp
        if moment is None:
            return None
        limit = utcnow() - self._window
        if moment >= limit:
            return None
        return Violation(
            code=self.code,
            field=TIMESTAMP_FIELD,
            message=(
                "timestamp anterior a janela de retencao de "
                f"{self._window.days} dias."
            ),
            severity=self.severity,
            original=moment.isoformat(),
        )


class DuplicateEventIdRule:
    """Deduplicacao em processo por `event_id` (decisao D10).

    LRU limitada a `dedup_window_size` entradas, em memoria: e uma checagem
    barata de sanidade, nao uma garantia distribuida - e deliberadamente NAO
    faz um `SELECT` por evento.
    """

    code = RejectionReason.DUPLICATE_EVENT_ID
    severity = Severity.ERROR

    def __init__(self, settings: Settings) -> None:
        self._window_size = settings.dedup_window_size
        self._seen: OrderedDict[str, None] = OrderedDict()

    def evaluate(self, ctx: EvaluationContext) -> Violation | None:
        """Registra o `event_id` ou reporta ERROR se ele ja foi visto."""
        event_id = ctx.working.get(EVENT_ID_FIELD)
        if not isinstance(event_id, str) or not event_id:
            return None
        if event_id in self._seen:
            self._seen.move_to_end(event_id)
            return Violation(
                code=self.code,
                field=EVENT_ID_FIELD,
                message=f"event_id ja visto na janela de deduplicacao: {event_id}.",
                severity=self.severity,
                original=event_id,
            )
        self._seen[event_id] = None
        while len(self._seen) > self._window_size:
            self._seen.popitem(last=False)
        return None


# --------------------------------------------------------------------------
# Composicao: a lista ordenada de regras
# --------------------------------------------------------------------------
RuleFactory = Callable[[Settings], QualityRule]

SCHEMA_RULE_FACTORIES: tuple[RuleFactory, ...] = (
    RequiredFieldsRule,
    EventIdRule,
    TimestampParseRule,
    PayloadObjectRule,
    UnknownFieldRule,
)
"""Conformidade de schema; roda primeiro e pode curto-circuitar o resto (D9)."""

QUALITY_RULE_FACTORIES: tuple[RuleFactory, ...] = (
    NumericMetricRule,
    TemperatureRangeRule,
    HumidityRangeRule,
    NonNegativeValueRule,
    StringLengthRule,
    TrimStringRule,
    FutureTimestampRule,
    StaleTimestampRule,
    DuplicateEventIdRule,
)
"""Regras de negocio; rodam todas e acumulam todas as violacoes."""


class DataQualityEngine:
    """Adjudica eventos rodando a lista ordenada de regras.

    O motor nao conhece HTTP nem banco: recebe um `dict` cru e devolve um
    `ValidationResult` com o veredito, o evento sanitizado, as violacoes
    (ERROR) e as sanitizacoes (WARNING).
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._schema_rules: tuple[QualityRule, ...] = tuple(
            factory(settings) for factory in SCHEMA_RULE_FACTORIES
        )
        self._quality_rules: tuple[QualityRule, ...] = tuple(
            factory(settings) for factory in QUALITY_RULE_FACTORIES
        )

    @property
    def schema_rules(self) -> tuple[QualityRule, ...]:
        """Regras de conformidade de schema, na ordem de execucao."""
        return self._schema_rules

    @property
    def quality_rules(self) -> tuple[QualityRule, ...]:
        """Regras de qualidade de negocio, na ordem de execucao."""
        return self._quality_rules

    def validate(self, raw: dict[str, Any]) -> ValidationResult:
        """Valida um evento cru e devolve o veredito - nunca levanta excecao.

        Uma rejeicao e um resultado de negocio: ela aparece no
        `ValidationResult`, no status do span e no evento de span
        `event.rejected` (decisao D15).
        """
        started = perf_counter()
        with _tracer.start_as_current_span(SPAN_VALIDATE) as span:
            result = self._adjudicate(raw)
            self._annotate(span, result)
            elapsed_ms = (perf_counter() - started) * MILLISECONDS_PER_SECOND
            self._record_metrics(result.status, elapsed_ms)
            return result

    # -- internals ---------------------------------------------------------
    def _adjudicate(self, raw: dict[str, Any]) -> ValidationResult:
        """Roda as regras e particiona as violacoes por severidade (D7/D9)."""
        working: dict[str, Any] = deepcopy(raw) if isinstance(raw, dict) else {}
        ctx = EvaluationContext(raw=raw, working=working)

        try:
            RawEventPayload.model_validate(raw)
        except ValidationError as exc:
            return self._unparseable(ctx, exc)

        schema_violations = self._run(self._schema_rules, ctx)
        rules_evaluated = len(self._schema_rules)
        collected = list(schema_violations)
        if not any(item.severity is Severity.ERROR for item in schema_violations):
            collected.extend(self._run(self._quality_rules, ctx))
            rules_evaluated += len(self._quality_rules)

        violations = [item for item in collected if item.severity is Severity.ERROR]
        sanitizations = [
            item for item in collected if item.severity is Severity.WARNING
        ]
        status = EventStatus.REJECTED if violations else EventStatus.SUCCESS
        return ValidationResult(
            status=status,
            sanitized_event=ctx.working,
            violations=violations,
            sanitizations=sanitizations,
            rules_evaluated=rules_evaluated,
            parsed_timestamp=ctx.parsed_timestamp,
        )

    @staticmethod
    def _unparseable(
        ctx: EvaluationContext, exc: ValidationError
    ) -> ValidationResult:
        """Envelope estruturalmente invalido: rejeita sem propagar a excecao (D3)."""
        fields = _join(
            sorted(
                {
                    ".".join(str(part) for part in error["loc"]) or "<raiz>"
                    for error in exc.errors()
                }
            )
        )
        violation = Violation(
            code=RejectionReason.UNPARSEABLE_ENVELOPE,
            field=fields or None,
            message=f"Envelope do evento nao pode ser interpretado: {fields}.",
            severity=Severity.ERROR,
        )
        return ValidationResult(
            status=EventStatus.REJECTED,
            sanitized_event=ctx.working,
            violations=[violation],
        )

    @staticmethod
    def _run(
        rules: Sequence[QualityRule], ctx: EvaluationContext
    ) -> list[Violation]:
        """Executa as regras na ordem e coleta todas as violacoes retornadas."""
        found: list[Violation] = []
        for rule in rules:
            violation = rule.evaluate(ctx)
            if violation is not None:
                found.append(violation)
        return found

    @staticmethod
    def _annotate(span: Span, result: ValidationResult) -> None:
        """Escreve os atributos `dq.*` e o desfecho do evento no span."""
        codes = [item.code.value for item in result.violations]
        span.set_attribute(ATTR_STATUS, result.status.value)
        span.set_attribute(ATTR_VIOLATION_COUNT, len(result.violations))
        span.set_attribute(ATTR_SANITIZATION_COUNT, len(result.sanitizations))
        span.set_attribute(ATTR_RULES_EVALUATED, result.rules_evaluated)
        span.set_attribute(ATTR_VIOLATION_CODES, codes)

        event_id = result.sanitized_event.get(EVENT_ID_FIELD)
        if isinstance(event_id, str):
            span.set_attribute(ATTR_EVENT_ID, event_id)
        event_type = result.sanitized_event.get(EVENT_TYPE_FIELD)
        if isinstance(event_type, str):
            span.set_attribute(ATTR_EVENT_TYPE, event_type)

        if result.status is EventStatus.REJECTED:
            span.set_status(Status(StatusCode.ERROR, _join(codes)))
            span.add_event(SPAN_EVENT_REJECTED, {ATTR_VIOLATION_CODES: codes})
            logger.warning(
                "Evento rejeitado pelo motor de Data Quality",
                extra={"dq_violation_codes": codes},
            )
        else:
            span.set_status(Status(StatusCode.OK))

    @staticmethod
    def _record_metrics(status: EventStatus, elapsed_ms: float) -> None:
        """Alimenta o contador e o histograma da decisao D17."""
        attributes = {ATTR_STATUS: status.value}
        _events_processed.add(1, attributes)
        _validation_duration.record(elapsed_ms, attributes)

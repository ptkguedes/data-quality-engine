"""Enumeracoes do dominio."""

from enum import Enum


class EventStatus(str, Enum):
    """Resultado da adjudicacao de um evento pelo motor de Data Quality."""

    SUCCESS = "SUCCESS"
    REJECTED = "REJECTED"


class Severity(str, Enum):
    """Severidade de uma violacao; e ela que determina o roteamento (D7).

    `WARNING` resulta em sanitizacao (o evento segue para `valid_events`);
    `ERROR` manda o evento para `invalid_events`.
    """

    WARNING = "WARNING"
    ERROR = "ERROR"


class RejectionReason(str, Enum):
    """Codigo estavel de cada violacao do catalogo de regras."""

    # Conformidade de schema
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    INVALID_EVENT_ID = "INVALID_EVENT_ID"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    PAYLOAD_NOT_OBJECT = "PAYLOAD_NOT_OBJECT"
    ENVELOPE_STRING_TOO_LONG = "ENVELOPE_STRING_TOO_LONG"
    UNKNOWN_FIELD = "UNKNOWN_FIELD"
    UNPARSEABLE_ENVELOPE = "UNPARSEABLE_ENVELOPE"

    # Regras de qualidade
    TEMPERATURE_OUT_OF_RANGE = "TEMPERATURE_OUT_OF_RANGE"
    HUMIDITY_OUT_OF_RANGE = "HUMIDITY_OUT_OF_RANGE"
    NEGATIVE_VALUE = "NEGATIVE_VALUE"
    NON_NUMERIC_METRIC = "NON_NUMERIC_METRIC"
    STRING_TOO_LONG = "STRING_TOO_LONG"
    UNTRIMMED_STRING = "UNTRIMMED_STRING"
    TIMESTAMP_IN_FUTURE = "TIMESTAMP_IN_FUTURE"
    TIMESTAMP_TOO_OLD = "TIMESTAMP_TOO_OLD"
    DUPLICATE_EVENT_ID = "DUPLICATE_EVENT_ID"
